"""
intent_detection.py

LLM fallback for tickets not matched by high-confidence regex_engine.py.
The LLM extracts requested_data, not a single intent.

The pipeline is conservative:
- regex hard guardrails are converted directly to human intervention;
- LLM output is auto-usable only when confidence is above the configured
  threshold;
- when regex candidates and LLM extraction disagree, the row is routed to
  human review instead of risking a wrong answer.

Creates:
- output/request_intent_results.jsonl.gz
"""

import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from typing import Dict, List

import pandas as pd
from google import genai

try:  # google-genai >= 0.3 ships the types module; guard older/partial installs
    from google.genai import types as genai_types
except ImportError:  # pragma: no cover - defensive, keeps the old call shape
    genai_types = None

from pipeline_io import (
    REGEX_MATCHES_PATH,
    REQUEST_INTENT_RESULTS_PATH,
    UNMATCHED_TICKETS_PATH,
    build_request_id,
    read_dataframe,
    write_dataframe,
)
from order_reference_resolver import contains_blocked_order_reference
from customs_rules import (
    BLOCKED_ORDER_PREFIX_SKIPPED,
    HUMAN_INTERVENTION_REQUIRED,
    UNKNOWN_REQUEST,
    PURELY_INFORMATIVE_SOLVED,
    clean_latest_request_text,
    collapse_document_embedded_requested_data,
    expand_first_returns_customs_clearance_bundle,
    detect_language_with_dictionary,
    is_acknowledgement_only,
    is_no_action_carrier_notification,
    is_missing_extracted_tracking_number,
    is_purely_informative_request,
    is_request_number_2_or_higher,
    is_request_number_3_or_higher,
    normalize_request_number,
    normalize_requested_data,
    requested_data_already_answered_by_first_reply,
)

PROMPT_PATH = "prompts/requested_data_extractor.md"
DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
FALLBACK_MODEL = os.getenv(
    "GEMINI_RATE_LIMIT_FALLBACK_MODEL",
    os.getenv("GEMINI_FALLBACK_MODEL", "gemini-3.1-flash-lite"),
)
FALLBACK_MODEL_RATE_LIMIT_ATTEMPTS = int(os.getenv("GEMINI_FALLBACK_RATE_LIMIT_ATTEMPTS", "3"))
BATCH_SIZE = int(os.getenv("LLM_BATCH_SIZE", "20"))
LLM_BATCH_WORKERS_ENV = "LLM_BATCH_WORKERS"
# Conservative: Gemini batches are the one place a 429 costs a retry with a
# sleep, and the detector already falls back to a lighter model under rate
# limiting. Set to 1 to restore exactly sequential behaviour.
DEFAULT_LLM_BATCH_WORKERS = 4


def _llm_batch_workers(batch_count):
    if batch_count <= 1:
        return 1
    try:
        configured = int(os.getenv(LLM_BATCH_WORKERS_ENV, "") or DEFAULT_LLM_BATCH_WORKERS)
    except ValueError:
        configured = DEFAULT_LLM_BATCH_WORKERS
    return max(1, min(configured, 16, batch_count))
LLM_CONFIDENCE_MIN = float(os.getenv("LLM_CONFIDENCE_MIN", "0.85"))

ALLOWED_REQUESTED_DATA = [
    "commercial_invoice",
    "return_proforma_invoice",
    "corrected_invoice",
    "export_tracking_number",
    "ups_account_number",
    "value_confirmation",
    "returned_items_confirmation",
    "dichiarazione_di_libera_esportazione",
    "eori_number",
    "power_of_attorney",
    "address_translation",
    "exporter_ein",
    "customer_phone",
    "customer_email",
    "customer_name",
    "shipping_address",
    "authorization_letter",
    "shipment_instructions",
    "address_correction",
    "previously_requested_documentation",
    "human_intervention_required",
    "unknown_request",
    "purely_informative_solved",
]

LEGACY_REQUESTED_DATA_ALIASES = {
    "declaration_of_intent": "dichiarazione_di_libera_esportazione",
}

# Kept as internal aliases only. They are collapsed later by customs_rules.
# tax/country/product collapse to commercial_invoice or return_proforma_invoice
# depending on context. customs_description/importer_details always collapse to
# return_proforma_invoice.
DEPRECATED_DOCUMENT_FIELD_KEYS = {
    "tax_information",
    "country_of_origin",
    "product_description",
    "customs_description",
    "importer_details",
}


def is_rate_limit_error(exc):
    text = str(exc or "").lower()
    return (
        "429" in text
        or "resource_exhausted" in text
        or "quota exceeded" in text
        or "quota_exceeded" in text
        or "rate limit" in text
        or "rate_limit" in text
    )


def normalized_model_name(model):
    return str(model or "").strip().lower().rsplit("/", 1)[-1]


def is_gemini_25_flash_model(model):
    return normalized_model_name(model) == "gemini-2.5-flash"


def summarize_model_attempts(attempted_models):
    summary = []
    for model in attempted_models:
        model = str(model or "").strip()
        if model and model not in summary:
            summary.append(model)
    return " -> ".join(summary)


class GeminiJsonHelper:

    @staticmethod
    def clean_json_response(text: str):
        text = (text or "").strip()

        if text.startswith("```json"):
            text = text.replace("```json", "", 1).strip()

        if text.startswith("```"):
            text = text.replace("```", "", 1).strip()

        if text.endswith("```"):
            text = text[:-3].strip()

        return text

    @staticmethod
    def afc_disabled_config():
        """Config that turns automatic function calling (AFC) off.

        No tools/functions are ever declared here: every call is a single
        stateless prompt -> JSON response. google-genai nevertheless enables
        AFC by default in Models.generate_content and logs the warning
        recommending Chat.send_message. Switching AFC off explicitly removes
        that warning; the request payload and the response are unchanged,
        because the SDK simply skips the (unused) function-calling loop.
        """
        if genai_types is None:
            return None

        try:
            return genai_types.GenerateContentConfig(
                automatic_function_calling=genai_types.AutomaticFunctionCallingConfig(
                    disable=True,
                ),
            )
        except (AttributeError, TypeError, ValueError):
            # Older/newer SDK without this config shape: fall back to the
            # previous call without a config.
            return None

    @staticmethod
    def generate_json_list(client, model, prompt):
        config = GeminiJsonHelper.afc_disabled_config()

        if config is None:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
            )
        else:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=config,
            )

        response_text = GeminiJsonHelper.clean_json_response(response.text)
        results = json.loads(response_text)

        if not isinstance(results, list):
            raise ValueError("LLM response was not a JSON list")

        return results


class RequestedDataDetector:

    def __init__(self, model=DEFAULT_MODEL, fallback_model=FALLBACK_MODEL):
        api_key = os.getenv("GEMINI_API_KEY")

        if not api_key:
            raise ValueError("Missing GEMINI_API_KEY")

        self.client = genai.Client(api_key=api_key)
        self.model = str(model or DEFAULT_MODEL).strip()
        self.fallback_model = str(fallback_model or "").strip()
        self.last_model_used = ""
        self.last_model_attempts = ""
        self.system_prompt = self._load_prompt(PROMPT_PATH)

    @staticmethod
    def _load_prompt(path):
        with open(path, "r", encoding="utf-8") as file:
            return file.read()

    @staticmethod
    def _normalize_requested_data(values):
        if not isinstance(values, list):
            return [UNKNOWN_REQUEST]

        cleaned = []

        for value in values:
            value = str(value).strip()
            value = LEGACY_REQUESTED_DATA_ALIASES.get(value, value)

            if (
                value in ALLOWED_REQUESTED_DATA
                or value in DEPRECATED_DOCUMENT_FIELD_KEYS
            ) and value not in cleaned:
                cleaned.append(value)

        return cleaned or [UNKNOWN_REQUEST]

    def _normalize_results(self, results, model_used, attempted_models):
        self.last_model_used = str(model_used or "").strip()
        self.last_model_attempts = summarize_model_attempts(attempted_models)

        for result in results:
            result["requested_data"] = self._normalize_requested_data(
                result.get("requested_data", [])
            )

            if "confidence" not in result:
                result["confidence"] = 0.0

            if "notes" not in result:
                result["notes"] = ""

            result["human_intervention_draft_response"] = str(
                result.get("human_intervention_draft_response", "") or ""
            ).strip()
            result["llm_model_used"] = self.last_model_used
            result["llm_model_attempts"] = self.last_model_attempts

        return results

    def _generate_with_retries(
        self,
        *,
        model,
        prompt,
        max_attempts,
        attempted_models,
        wait_on_rate_limit,
    ):
        last_error = None

        for attempt in range(max_attempts):
            attempted_models.append(model)
            try:
                return GeminiJsonHelper.generate_json_list(
                    self.client,
                    model,
                    prompt,
                )
            except Exception as exc:
                last_error = exc

                if not is_rate_limit_error(exc):
                    raise

                if wait_on_rate_limit and attempt < max_attempts - 1:
                    wait_seconds = 30 * (attempt + 1)
                    print(
                        f"Rate limit reached for {model}. "
                        f"Waiting {wait_seconds}s before retry."
                    )
                    time.sleep(wait_seconds)

        raise last_error

    def _should_fallback_after_rate_limit(self, exc, model):
        return (
            is_rate_limit_error(exc)
            and is_gemini_25_flash_model(model)
            and bool(self.fallback_model)
            and normalized_model_name(self.fallback_model) != normalized_model_name(model)
        )

    def detect_batch(self, batch_payload):
        prompt = f"""
{self.system_prompt}

Allowed requested_data values:
{json.dumps(ALLOWED_REQUESTED_DATA, ensure_ascii=False, indent=2)}

Classify ALL requests below.

Return JSON ONLY.

Return EXACTLY this structure:

[
  {{
    "source_id": "...",
    "requested_data": ["..."],
    "confidence": 0.0,
    "notes": "short reason",
    "human_intervention_draft_response": ""
  }}
]

REQUESTS:
{json.dumps(batch_payload, ensure_ascii=False, indent=2)}
"""

        attempted_models = []
        self.last_model_used = ""
        self.last_model_attempts = ""
        primary_has_fallback = (
            is_gemini_25_flash_model(self.model)
            and self.fallback_model
            and normalized_model_name(self.fallback_model) != normalized_model_name(self.model)
        )
        primary_attempts = 1 if primary_has_fallback else 3

        try:
            results = self._generate_with_retries(
                model=self.model,
                prompt=prompt,
                max_attempts=primary_attempts,
                attempted_models=attempted_models,
                wait_on_rate_limit=primary_attempts > 1,
            )
            return self._normalize_results(results, self.model, attempted_models)
        except Exception as exc:
            self.last_model_attempts = summarize_model_attempts(attempted_models)

            if not self._should_fallback_after_rate_limit(exc, self.model):
                raise

            print(
                f"Rate limit reached for {self.model}; "
                f"switching to fallback model {self.fallback_model}."
            )

            try:
                results = self._generate_with_retries(
                    model=self.fallback_model,
                    prompt=prompt,
                    max_attempts=max(1, FALLBACK_MODEL_RATE_LIMIT_ATTEMPTS),
                    attempted_models=attempted_models,
                    wait_on_rate_limit=True,
                )
                return self._normalize_results(
                    results,
                    self.fallback_model,
                    attempted_models,
                )
            except Exception:
                self.last_model_attempts = summarize_model_attempts(attempted_models)
                raise


def build_source_id(row):
    return row.get("request_id") or build_request_id(
        row.get("zendesk_ticket_id"),
        row.get("request_number", 1),
    )


def as_bool(value):
    if isinstance(value, bool):
        return value

    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass

    if isinstance(value, (int, float)):
        return value != 0

    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def list_or_empty(value):
    return normalize_requested_data(value)


def requested_data_set(value):
    return {
        item
        for item in normalize_requested_data(value)
        if item not in {UNKNOWN_REQUEST, HUMAN_INTERVENTION_REQUIRED}
    }


def source_text_for_llm(row) -> str:
    cleaned = str(row.get("cleaned_request_body", "") or "").strip()
    raw = str(row.get("request_body", "") or "").strip()

    if raw:
        refreshed_cleaned = str(
            clean_latest_request_text(raw).get("cleaned_request_text", "") or ""
        ).strip()
        # Rows produced before the latest cleaner change may already have a
        # shortened cleaned_request_body.  Rebuild from request_body and prefer
        # the refreshed text only when it clearly restores useful context.
        if refreshed_cleaned and (not cleaned or len(refreshed_cleaned) > len(cleaned) + 80):
            return refreshed_cleaned

    return cleaned or raw


def base_output_row(source_row) -> Dict[str, object]:
    return {
        "request_id": source_row.get("request_id") or build_request_id(
            source_row.get("zendesk_ticket_id"),
            source_row.get("request_number"),
        ),
        "request_submission_timestamp": source_row.get("request_submission_timestamp"),
        "ticket_submission_timestamp": source_row.get("ticket_submission_timestamp"),
        "zendesk_ticket_id": source_row.get("zendesk_ticket_id"),
        "request_number": source_row.get("request_number"),
        "requester_email": source_row.get("requester_email"),
        "subject": source_row.get("subject"),
        "request_body": source_row.get("request_body"),
        "cleaned_request_body": source_row.get("cleaned_request_body"),
        "ticket_category": source_row.get("ticket_category"),
        "extracted_tracking_number": source_row.get("extracted_tracking_number"),
        "shipment_order_number": source_row.get("shipment_order_number"),
        "shipment_tracking_number": source_row.get("shipment_tracking_number"),
        "return_tracking_number": source_row.get("return_tracking_number"),
        "shipment_carrier_code": source_row.get("shipment_carrier_code"),
        "return_carrier_code": source_row.get("return_carrier_code"),
        "tracking_not_found_in_shipping_platform_shipments": as_bool(
            source_row.get("tracking_not_found_in_shipping_platform_shipments")
        ),
        "extracted_order_reference": source_row.get("extracted_order_reference"),
        "order_reference_source": source_row.get("order_reference_source"),
        "shipment_selection_mode": source_row.get("shipment_selection_mode"),
        "order_processing_blocked": as_bool(source_row.get("order_processing_blocked")),
        "order_resolution_complete": source_row.get("order_resolution_complete"),
        "llm_human_intervention_draft_response": source_row.get(
            "llm_human_intervention_draft_response",
            "",
        ),
        "regex_confidence": source_row.get(
            "regex_confidence",
            source_row.get("confidence"),
        ),
        "needs_standard_reply_confirmation": as_bool(
            source_row.get("needs_standard_reply_confirmation")
        ),
        "standard_reply_requested_data": list_or_empty(
            source_row.get("standard_reply_requested_data")
        ),
        "regex_request_types": list_or_empty(source_row.get("regex_request_types")),
        "regex_requested_data": list_or_empty(source_row.get("regex_requested_data")),
        "matched_spans": source_row.get("matched_spans", ""),
        "quoted_history_removed": as_bool(source_row.get("quoted_history_removed")),
        "signature_removed": as_bool(source_row.get("signature_removed")),
    }


def build_human_output_row(source_row, reason, engine="human_guardrail"):
    output = base_output_row(source_row)
    output.update(
        {
            "engine": engine,
            "matched": True,
            "excluded": False,
            "request_types": ["human_intervention_required"],
            "requested_data": [HUMAN_INTERVENTION_REQUIRED],
            "confidence": 0.0,
            "llm_confidence": None,
            "notes": reason,
            "human_intervention_required": True,
            "llm_was_used": False,
            "llm_model_used": "",
            "llm_model_attempts": "",
        }
    )
    return output


def build_exclude_output_row(source_row, reason, engine="no_action_guardrail"):
    output = base_output_row(source_row)
    output.update(
        {
            "engine": engine,
            "matched": True,
            "excluded": True,
            "request_types": ["exclude_from_processing"],
            "requested_data": [],
            "confidence": 0.99,
            "llm_confidence": None,
            "notes": reason,
            "human_intervention_required": False,
            "llm_was_used": False,
            "llm_model_used": "",
            "llm_model_attempts": "",
        }
    )
    return output


def is_tracking_lookup_not_found(row) -> bool:
    return as_bool(row.get("tracking_not_found_in_shipping_platform_shipments"))


def has_resolved_order(row) -> bool:
    """True when the request is tied to an order by any resolution route.

    The order may come from the tracking number via
    bi.shipping_platform_shipments, or from an order reference found in the
    ticket text or inside a PDF attachment. Either way the tracking-number
    guards below no longer apply, because GET_FULL_ORDER can be queried.
    """

    if not str(row.get("shipment_order_number") or "").strip():
        return False
    # On a ticket with several tracking numbers, only some of which resolved,
    # order_resolution_complete is False and the request must stay in human
    # review instead of being answered with partial data.
    flag = row.get("order_resolution_complete")
    if flag is None or (isinstance(flag, str) and not flag.strip()):
        return True
    return as_bool(flag)


def is_order_processing_blocked(row) -> bool:
    return as_bool(row.get("order_processing_blocked")) or contains_blocked_order_reference(
        row.get("shipment_order_number"),
        row.get("extracted_order_reference"),
    )


def build_blocked_order_output_row(source_row):
    output = base_output_row(source_row)
    output.update(
        {
            "engine": "blocked_order_prefix_guard",
            "matched": True,
            "excluded": True,
            "request_types": [BLOCKED_ORDER_PREFIX_SKIPPED],
            "requested_data": [BLOCKED_ORDER_PREFIX_SKIPPED],
            "confidence": 0.99,
            "llm_confidence": None,
            "notes": (
                "The order number is on a blocked prefix (RC-US, RC-EU, LN-EU "
                "or LN-US). The request is not processed and the ticket is "
                "left untouched."
            ),
            "human_intervention_required": False,
            "llm_was_used": False,
            "llm_model_used": "",
            "llm_model_attempts": "",
        }
    )
    return output


def requested_data_summary(values: List[str]) -> str:
    understood = [
        value.replace("_", " ")
        for value in values
        if value not in {UNKNOWN_REQUEST, HUMAN_INTERVENTION_REQUIRED}
    ]
    return ", ".join(understood) if understood else "the sender's request"


def build_tracking_lookup_fallback_draft(source_row, requested_data, llm_notes: object) -> str:
    language = detect_language_with_dictionary(
        source_row.get("subject", ""),
        source_text_for_llm(source_row),
    )
    tracking_number = str(source_row.get("extracted_tracking_number") or "N/A").strip()
    understood = requested_data_summary(requested_data)
    notes = str(llm_notes or "").strip()

    if language == "it":
        detail = f" Ho interpretato la richiesta come relativa a: {understood}."
        if notes:
            detail += f" Dettagli LLM: {notes}"
        return (
            "Buongiorno,\n\n"
            f"Abbiamo ricevuto la vostra richiesta relativa al tracking {tracking_number}."
            f"{detail}\n\n"
            "Non sono riuscito a trovare questo tracking nella tabella spedizioni, "
            "quindi è necessario l'intervento di un operatore per verificare la pratica "
            "e confermare la documentazione o le informazioni corrette.\n\n"
            "Cordiali saluti,"
        )

    detail = f" I understood the request as related to: {understood}."
    if notes:
        detail += f" LLM details: {notes}"
    return (
        "Hello,\n\n"
        f"We received your request regarding tracking number {tracking_number}."
        f"{detail}\n\n"
        "I could not find this tracking number in the shipment table, so human "
        "intervention is required to verify the case and confirm the correct "
        "documentation or information.\n\n"
        "Kind regards,"
    )


def build_llm_output_row(source_row, result):
    requested_data = result.get("requested_data", [UNKNOWN_REQUEST])
    requested_data = normalize_requested_data(requested_data) or [UNKNOWN_REQUEST]
    requested_data = collapse_document_embedded_requested_data(
        requested_data,
        ticket_category=source_row.get("ticket_category"),
        request_number=source_row.get("request_number", 1),
        requester_email=source_row.get("requester_email"),
        request_text=source_text_for_llm(source_row),
    ) or [UNKNOWN_REQUEST]

    engine = "llm"
    request_types: List[str] = []
    human_intervention_required = False
    confidence = safe_float(result.get("confidence", 0.0))
    notes = result.get("notes", "")

    collapsed_regex_requested_data = collapse_document_embedded_requested_data(
        source_row.get("regex_requested_data"),
        ticket_category=source_row.get("ticket_category"),
        request_number=source_row.get("request_number", 1),
        requester_email=source_row.get("requester_email"),
        request_text=source_text_for_llm(source_row),
    )
    collapsed_regex_requested_data = expand_first_returns_customs_clearance_bundle(
        collapsed_regex_requested_data,
        ticket_category=source_row.get("ticket_category"),
        request_number=source_row.get("request_number", 1),
        requester_email=source_row.get("requester_email"),
        trigger_requested_data=collapsed_regex_requested_data,
    )
    requested_data = expand_first_returns_customs_clearance_bundle(
        requested_data,
        ticket_category=source_row.get("ticket_category"),
        request_number=source_row.get("request_number", 1),
        requester_email=source_row.get("requester_email"),
        trigger_requested_data=collapsed_regex_requested_data,
    ) or requested_data
    regex_candidates = requested_data_set(collapsed_regex_requested_data)
    llm_candidates = requested_data_set(requested_data)
    llm_human_intervention_draft_response = str(
        result.get("human_intervention_draft_response", "") or ""
    ).strip()
    llm_model_used = str(result.get("llm_model_used", "") or "").strip()
    llm_model_attempts = str(result.get("llm_model_attempts", "") or "").strip()

    if is_request_number_2_or_higher(
        source_row.get("request_number", 1)
    ) and is_acknowledgement_only(source_text_for_llm(source_row)):
        # Backstop for the stale-subject trap. The subject never changes down
        # an email thread, so a follow-up whose body is only "Thank you."
        # still arrives under "Needing Commercial Invoice - Tracking# ...",
        # and the model answered the subject: it returned commercial_invoice
        # at 0.95 with the note "Body is an acknowledgement", and the invoice
        # was sent to someone who had just confirmed receiving it.
        #
        # The LLM is still called and still consulted -- this only overrides
        # it when the body, on its own, is nothing but an acknowledgement
        # (is_acknowledgement_only requires a full match, so "thanks, but the
        # invoice is unreadable" is not caught here and is classified
        # normally).
        engine = "acknowledgement_only_follow_up_guard"
        request_types = [PURELY_INFORMATIVE_SOLVED]
        requested_data = [PURELY_INFORMATIVE_SOLVED]
        human_intervention_required = False
        notes = (
            "Follow-up message whose body is a bare acknowledgement, with no "
            "new request. Solved without a response; the ticket subject still "
            "names the original request and is not treated as a new one. "
            f"LLM notes: {notes}"
        )

    elif requested_data == [PURELY_INFORMATIVE_SOLVED]:
        # Deliberately ahead of the request-number guard below: a message that
        # asks for nothing -- "thanks, received", an acknowledgement of the
        # previous reply -- is solved with no public comment at ANY request
        # number. Sending a third-exchange thank-you to a human only to be
        # closed by hand is work for nobody. Tracking-number status, in the
        # ticket or in the shipment table, does not change this either.
        engine = "llm_purely_informative_guard"
        request_types = [PURELY_INFORMATIVE_SOLVED]
        human_intervention_required = False
        notes = (
            "LLM classified this request as purely informative, with no "
            "data or action requested. Submitted as solved without a "
            f"response. LLM notes: {notes}"
        )

    elif is_request_number_3_or_higher(source_row.get("request_number", 1)):
        # Request number 3 or higher never receives an automatic answer to an
        # actual data/action request, no matter how confident the LLM is or
        # whether regex and LLM agree. The only automatic outcome allowed at
        # 3+ is the purely-informative solve above.
        engine = "llm_request_number_3_or_higher_guard"
        request_types = ["request_number_3_or_higher_guard"]
        requested_data = [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        notes = (
            "Request number is 3 or higher. The message was evaluated by "
            "the LLM but is not purely informative, so an automatic answer "
            "is never sent regardless of LLM confidence; human intervention "
            f"is required. LLM notes: {notes}"
        )

    elif is_tracking_lookup_not_found(source_row) and not has_resolved_order(source_row):
        # Qualified exactly as the dispatch in main() and as
        # response_generator.tracking_lookup_requires_human() already do. An
        # unknown tracking number only blocks the reply when nothing else
        # identifies the order; once an order reference has been resolved from
        # the subject/description or from a PDF attachment, GET_FULL_ORDER can
        # be queried and the request is answerable. Without this qualifier a
        # request whose order WAS resolved still reached the LLM through the
        # final else-branch in main() (request number 2+, or no other guard)
        # and was then escalated here regardless of what the LLM understood.
        engine = "llm_tracking_lookup_not_found_guard"
        request_types = ["tracking_lookup_not_found_guard"]
        understood_requested_data = [
            item
            for item in requested_data
            if item not in {UNKNOWN_REQUEST, HUMAN_INTERVENTION_REQUIRED}
        ]
        requested_data = understood_requested_data or [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        if not llm_human_intervention_draft_response:
            llm_human_intervention_draft_response = build_tracking_lookup_fallback_draft(
                source_row,
                requested_data,
                notes,
            )
        notes = (
            "Tracking number extracted from the ticket was not found in "
            "tlg-business-intelligence-prd.bi.shipping_platform_shipments. "
            "Regex processing was skipped and this request was handled only by the LLM. "
            f"LLM understood requested_data={requested_data}. "
            "Human intervention is required. "
            f"LLM notes: {notes}"
        )

    elif requested_data == [HUMAN_INTERVENTION_REQUIRED]:
        engine = "llm_human_intervention_guard"
        request_types = [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        notes = (
            "LLM classified this request as requiring human intervention. "
            f"LLM notes: {notes}"
        )

    elif requested_data == [UNKNOWN_REQUEST]:
        engine = "llm_unknown_request_guard"
        request_types = ["unknown_request_guard"]
        requested_data = [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        notes = (
            "LLM could not identify actionable requested_data. "
            "Human intervention is required. "
            f"LLM notes: {notes}"
        )

    elif confidence < LLM_CONFIDENCE_MIN:
        engine = "llm_low_confidence_guard"
        request_types = ["low_confidence_guard"]
        requested_data = [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        notes = (
            f"LLM confidence {confidence:.2f} is below threshold "
            f"{LLM_CONFIDENCE_MIN:.2f}. Human intervention is required. "
            f"LLM notes: {notes}"
        )

    elif (
        regex_candidates
        and llm_candidates
        and regex_candidates != llm_candidates
        and not (regex_candidates & llm_candidates)
    ):
        engine = "regex_llm_disagreement_guard"
        request_types = ["regex_llm_disagreement_guard"]
        requested_data = [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        notes = (
            "Regex candidates and LLM requested_data disagree. "
            f"regex={sorted(regex_candidates)}; llm={sorted(llm_candidates)}. "
            "Human intervention is required. "
            f"LLM notes: {notes}"
        )

    elif (
        regex_candidates
        and llm_candidates
        and regex_candidates != llm_candidates
        and (regex_candidates & llm_candidates)
    ):
        # Regex and LLM disagree on the full set, but they agree on at least
        # one requested_data item. Rather than blocking the whole reply,
        # respond with the union of both extractions.
        union_candidates = sorted(regex_candidates | llm_candidates)
        engine = "regex_llm_union_guard"
        request_types = ["regex_llm_union_guard"]
        requested_data = union_candidates
        notes = (
            "Regex candidates and LLM requested_data partially disagree but "
            f"share at least one requested_data. regex={sorted(regex_candidates)}; "
            f"llm={sorted(llm_candidates)}. Responding with the union: "
            f"{union_candidates}. "
            f"LLM notes: {notes}"
        )

    elif (
        as_bool(source_row.get("needs_standard_reply_confirmation"))
        and requested_data_already_answered_by_first_reply(
            requested_data,
            source_row.get("requester_email"),
        )
    ):
        engine = "llm_standard_reply_repeat_guard"
        request_types = ["standard_reply_repeat_guard"]
        requested_data = [HUMAN_INTERVENTION_REQUIRED]
        human_intervention_required = True
        notes = (
            "Regex/LLM follow-up guard: the request appears to ask only for "
            "data already covered by the first standard reply. Do not send an "
            "automatic duplicate answer; human intervention is required. "
            f"LLM notes: {notes}"
        )

    output = base_output_row(source_row)
    output.update(
        {
            "engine": engine,
            "matched": True,
            "excluded": False,
            "request_types": request_types,
            "requested_data": requested_data,
            "confidence": confidence,
            "llm_confidence": confidence,
            "notes": notes,
            "human_intervention_required": human_intervention_required,
            "llm_was_used": True,
            "llm_model_used": llm_model_used,
            "llm_model_attempts": llm_model_attempts,
            "llm_human_intervention_draft_response": llm_human_intervention_draft_response,
        }
    )
    return output


def add_language_detection(final_df):
    if final_df.empty:
        final_df["request_language"] = []
        final_df["language_confidence"] = []
        final_df["language_notes"] = []
        return final_df

    language_details = final_df.apply(
        lambda row: detect_language_with_dictionary(
            row.get("subject", ""),
            row.get("cleaned_request_body", "") or row.get("request_body", ""),
            return_details=True,
        ),
        axis=1,
    )

    final_df["request_language"] = language_details.apply(
        lambda details: details["request_language"]
    )
    final_df["language_confidence"] = language_details.apply(
        lambda details: details["language_confidence"]
    )
    final_df["language_notes"] = language_details.apply(
        lambda details: details["language_notes"]
    )

    return final_df


def main():
    detector = RequestedDataDetector()

    regex_df = read_dataframe(REGEX_MATCHES_PATH)
    unmatched_df = read_dataframe(UNMATCHED_TICKETS_PATH)

    llm_results = []
    rows_for_llm = []

    for _, row in unmatched_df.iterrows():
        if is_order_processing_blocked(row):
            # RC-US / RC-EU / LN-EU / LN-US: never analyzed, never answered,
            # and the ticket is left exactly as it is.
            llm_results.append(build_blocked_order_output_row(row))
        elif is_request_number_2_or_higher(
            row.get("request_number")
        ) and is_no_action_carrier_notification(source_text_for_llm(row)):
            # A pure carrier status notification is solved with no comment and
            # no LLM call at any request number. The generic exclusion below
            # already does this, but only after the tracking-number guards,
            # which from the second exchange on would divert the row to the
            # LLM first; checking it up here keeps a follow-up notification
            # off the LLM entirely.
            llm_results.append(
                build_exclude_output_row(
                    row,
                    "Carrier notification/status message only; no actionable "
                    "customer-data request was detected. Request number 2 or "
                    "higher: solved without a response and without an LLM call.",
                    engine="no_action_guardrail_request_number_2_or_higher",
                )
            )
        elif is_missing_extracted_tracking_number(
            row.get("extracted_tracking_number")
        ) and not has_resolved_order(row) and not is_request_number_2_or_higher(
            row.get("request_number")
        ):
            # This guard only applies to request number 1. From request
            # number 2 onward, policy requires always sending the ticket to
            # the LLM regardless of tracking-number status (see
            # is_request_number_2_or_higher branch in regex_engine.py).
            if is_purely_informative_request(source_text_for_llm(row)):
                llm_results.append(
                    build_exclude_output_row(
                        row,
                        "The tracking number was not found in the ticket, but the "
                        "message is purely informative with no data or action "
                        "requested. Submitted as solved without a response.",
                        engine="missing_tracking_informative_solved",
                    )
                )
            else:
                llm_results.append(
                    build_human_output_row(
                        row,
                        "The tracking number was not found in the ticket. The request was not analyzed.",
                        engine="missing_tracking_number_guard",
                    )
                )
        elif is_tracking_lookup_not_found(row) and not has_resolved_order(row):
            # User requirement: when the extracted tracking number is missing from
            # shipping_platform_shipments, bypass regex/hard guards and let the
            # LLM summarize what it understood before routing to human review.
            rows_for_llm.append(row)
        elif is_no_action_carrier_notification(source_text_for_llm(row)):
            llm_results.append(
                build_exclude_output_row(
                    row,
                    "Carrier notification/status message only; no actionable customer-data request was detected.",
                )
            )
        elif is_request_number_2_or_higher(row.get("request_number")):
            # From the second exchange on, only the LLM decides. It is placed
            # after the carrier-notification exclusion above -- a pure status
            # notification is recognised by regex and solved with no comment
            # at any request number, and spending an LLM call on it buys
            # nothing -- but ahead of the regex safety guard below, which
            # would otherwise send a follow-up straight to a human without
            # anyone reading it.
            #
            # What the LLM is there to tell apart: a sender who is just
            # acknowledging the previous reply ("thanks, received"), which is
            # solved with no public comment whatever the request number, from
            # a sender who wants something further. Only the latter is
            # answered (request number 2, data permitting) or handed to the
            # second-level group (request number 3+, always).
            rows_for_llm.append(row)
        elif as_bool(row.get("force_human_intervention")) or as_bool(
            row.get("human_intervention_required")
        ):
            reason = row.get("notes") or "Regex safety guard required human review."
            llm_results.append(build_human_output_row(row, reason))
        else:
            # Includes request number 2 and higher unconditionally (policy:
            # always send to the LLM regardless of tracking-number status).
            # build_llm_output_row() applies the purely-informative and
            # request-number-3+ guards after the LLM call.
            rows_for_llm.append(row)

    total_batches = ceil(len(rows_for_llm) / BATCH_SIZE) if rows_for_llm else 0

    print(
        f"Processing {len(rows_for_llm)} LLM-review tickets "
        f"in {total_batches} Gemini calls; "
        f"{len(llm_results)} tickets sent directly to human review"
    )

    # Batches are independent: each is one stateless prompt -> JSON response,
    # and results are matched back by source_id. Running them concurrently
    # changes nothing about what is asked or how answers are applied; only the
    # waiting overlaps. Results are reassembled in batch order below, so
    # llm_results ends up exactly as the sequential version produced it.
    #
    # Each worker gets its OWN detector: last_model_used/last_model_attempts
    # are per-call state read straight after the call, so a shared detector
    # would attribute the wrong model to a batch under concurrency.
    batch_workers = _llm_batch_workers(total_batches)
    detector_local = threading.local()

    def worker_detector():
        existing = getattr(detector_local, "detector", None)
        if existing is None:
            existing = RequestedDataDetector(
                model=detector.model,
                fallback_model=detector.fallback_model,
            )
            detector_local.detector = existing
        return existing

    def run_batch(batch_number):
        start_idx = batch_number * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_rows = rows_for_llm[start_idx:end_idx]
        active_detector = detector if batch_workers <= 1 else worker_detector()

        batch_payload, lookup = _build_batch_payload(batch_rows)

        messages = []
        try:
            batch_results = active_detector.detect_batch(batch_payload)
        except Exception as exc:
            messages.append(f"Batch {batch_number + 1} failed: {exc}")
            batch_results = [
                {
                    "source_id": payload["source_id"],
                    "requested_data": [UNKNOWN_REQUEST],
                    "confidence": 0.0,
                    "notes": f"LLM batch failed: {exc}",
                    "llm_model_used": active_detector.last_model_used,
                    "llm_model_attempts": active_detector.last_model_attempts,
                }
                for payload in batch_payload
            ]
        return batch_number, batch_results, lookup, messages

    if batch_workers <= 1:
        completed = [run_batch(number) for number in range(total_batches)]
    else:
        print(f"Running {total_batches} Gemini batch(es) with {batch_workers} worker(s).")
        completed = []
        with ThreadPoolExecutor(max_workers=batch_workers) as executor:
            futures = [
                executor.submit(run_batch, number) for number in range(total_batches)
            ]
            for future in as_completed(futures):
                completed.append(future.result())
                print(f"  ...completed {len(completed)}/{total_batches} batch(es).")

    # Reassemble in batch order so llm_results, and therefore the output file,
    # is byte-identical to the sequential run.
    for _, batch_results, lookup, messages in sorted(completed, key=lambda item: item[0]):
        for message in messages:
            print(message)

        for result in batch_results:
            source_id = result.get("source_id")
            source_row = lookup.get(source_id)

            if source_row is None:
                print(f"WARNING: source_id {source_id} not found")
                continue

            llm_results.append(build_llm_output_row(source_row, result))

    llm_df = pd.DataFrame(llm_results)

    final_df = pd.concat(
        [regex_df, llm_df],
        ignore_index=True,
    )

    final_df = add_language_detection(final_df)

    os.makedirs("output", exist_ok=True)
    write_dataframe(final_df, REQUEST_INTENT_RESULTS_PATH)

    print(
        f"Saved {len(final_df)} rows to {REQUEST_INTENT_RESULTS_PATH}"
    )


def _build_batch_payload(batch_rows):
    """Build the Gemini payload and the source_id -> row lookup for one batch."""

    batch_payload = []
    lookup = {}

    for row in batch_rows:
        source_id = build_source_id(row)
        lookup[source_id] = row

        batch_payload.append(
            {
                "source_id": source_id,
                "subject": str(row.get("subject", "") or ""),
                # The subject never changes down an email thread, so on a
                # follow-up it still advertises the ORIGINAL request ("Needing
                # Commercial Invoice - Tracking# ...") even when the new
                # message says only "Thank you.". Without these two fields the
                # model cannot tell the first message from the fifth, and it
                # read the stale subject as a fresh request.
                "request_number": normalize_request_number(row.get("request_number")),
                "is_follow_up": is_request_number_2_or_higher(row.get("request_number")),
                "request_body": source_text_for_llm(row),
                "ticket_category": str(row.get("ticket_category", "") or ""),
                "regex_candidates": list_or_empty(row.get("regex_requested_data")),
                "regex_notes": str(row.get("notes", "") or ""),
                "extracted_tracking_number": str(
                    row.get("extracted_tracking_number", "") or ""
                ),
                "tracking_not_found_in_shipping_platform_shipments": as_bool(
                    row.get("tracking_not_found_in_shipping_platform_shipments")
                ),
            }
        )

    return batch_payload, lookup


if __name__ == "__main__":
    main()
