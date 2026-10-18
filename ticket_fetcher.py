"""
ticket_fetcher.py

Fetch active Zendesk requests into the BigQuery active-ticket table and expose
BigQuery history logging helpers used by the final response-generation stage.

The history logging logic intentionally lives here so there is no separate
history-logging script.  Importing this module is safe: Zendesk fetching only
runs when this file is executed as a script.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import re
import unicodedata
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from pipeline_io import (
    REQUEST_INTENT_RESULTS_PATH,
    build_request_id,
    get_workflow_run_id,
    read_dataframe,
)
from customs_rules import (
    CARRIER_EMAIL_DOMAINS,
    row_order_processing_blocked_final,
    PURELY_INFORMATIVE_SOLVED,
    VALID_TICKET_CATEGORIES,
    carrier_code_from_email,
    classify_ticket_category_from_content,
    email_matches_any_carrier_domain,
    is_no_action_carrier_notification,
    is_noreply_requester_email,
    is_noreply_fedex_requester_email,
    is_noreply_autoreply_first_request,
    is_missing_extracted_tracking_number,
    is_purely_informative_request,
    normalize_email,
    normalize_requested_data,
)
from order_reference_resolver import (
    ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIXES,
    BLOCKED_DESTINATION_COUNTRY_CODES,
    BLOCKED_ORDER_REFERENCE_PREFIXES,
    DESTINATION_CHECKED_ORDER_REFERENCE_PREFIXES,
    ORDER_REFERENCE_SOURCE_PDF,
    ORDER_REFERENCE_SOURCE_CROSS_REQUEST,
    ORDER_REFERENCE_SOURCE_PDF_TRACKING,
    ORDER_REFERENCE_SOURCE_SHIPMENT_STATUS,
    ORDER_REFERENCE_SOURCE_TEXT,
    ORDER_REFERENCE_SOURCE_TIL_EVENTS,
    ORDER_REFERENCE_SOURCE_TRACKING,
    order_number_from_order_reference,
    SHIPMENT_SELECTION_ALL,
    SHIPMENT_SELECTION_BY_TRACKING,
    SHIPMENT_SELECTION_EXACT,
    contains_always_blocked_order_reference,
    contains_blocked_order_reference,
    find_order_references,
    find_identifiers_in_pdfs,
    is_pdf_attachment,
    is_shipment_shaped_order_reference,
    split_order_references,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ID = os.getenv("PROJECT_ID", "tlg-business-intelligence-prd")

# til.prd_shipmentStatus is in its own GCP project with its own service
# account; see til_bigquery_client().
TIL_PROJECT_ID = os.getenv("TIL_PROJECT_ID", "tlg-wlfs-prd")
TIL_BIGQUERY_CREDS_ENV = "TIL_BIGQUERY_CREDS"

CONFIG_TABLE = os.getenv(
    "LOG_CONFIG_TABLE",
    "tlg-business-intelligence-prd.til.log_one_tool_army_config",
)

TARGET_TABLE = os.getenv(
    "ACTIVE_TICKETS_TABLE",
    "tlg-business-intelligence-prd.til.log_one_tool_army_active_tickets",
)

HISTORY_TABLE = os.getenv(
    "LOG_HISTORY_TABLE",
    "tlg-business-intelligence-prd.til.log_one_tool_army_logs_history",
)

BQ_STORAGE = False

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN", "thelevelgroup")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL", "beatrice.bettini@thelevelgroup.com")

ZENDESK_RESPONSE_SUBMISSION_ENV = "SUBMIT_ZENDESK_RESPONSES"
# Safety default: never submit public ticket replies unless the workflow/env flag
# explicitly opts in with true/1/yes/y/on.
DEFAULT_SUBMIT_ZENDESK_RESPONSES = False
ZENDESK_STATUS_AFTER_REPLY_ENV = "ZENDESK_STATUS_AFTER_REPLY"
DEFAULT_ZENDESK_STATUS_AFTER_REPLY = "solved"

ACTIVE_TICKET_COLUMNS = [
    "ingestion_timestamp",
    "request_submission_timestamp",
    "ticket_submission_timestamp",
    "request_id",
    "zendesk_ticket_id",
    "request_number",
    "requester_email",
    "subject",
    "request_body",
    "ticket_category",
    "extracted_tracking_number",
    "shipment_order_number",
    "shipment_tracking_number",
    "return_tracking_number",
    "shipment_carrier_code",
    "return_carrier_code",
    "tracking_not_found_in_shipping_platform_shipments",
    "extracted_order_reference",
    "order_reference_source",
    "shipment_selection_mode",
    "order_processing_blocked",
    "order_resolution_complete",
]

HISTORY_COLUMNS = [
    "ingestion_timestamp",
    "request_submission_timestamp",
    "ticket_submission_timestamp",
    "workflow_run_id",
    "request_run_id",
    "zendesk_ticket_id",
    "request_number",
    "request_id",
    "requester_email",
    "subject",
    "request_body",
    "cleaned_request_body",
    "ticket_category",
    "extracted_tracking_number",
    "shipment_order_number",
    "shipment_tracking_number",
    "return_tracking_number",
    "full_order_export_tracking_number",
    "extracted_order_reference",
    "order_reference_source",
    "shipment_selection_mode",
    "order_processing_blocked",
    "order_resolution_complete",
    "notes",
    "human_intervention_required",
    "regex_request_types",
    "regex_requested_data",
    "regex_confidence",
    "matched_spans",
    "request_language",
    "language_confidence",
    "language_notes",
    "llm_was_used",
    "llm_confidence",
    "requested_data",
    "draft_response",
    "final_response",
    "zendesk_submission_status",
]

# ---------------------------------------------------------------------------
# Zendesk submission outcome, logged per request in the history table.
#
# The fetcher used to skip every request_id already present in the history
# table. History rows are written in the same run that builds the reply, so a
# request whose reply was built but never actually posted (dry run, Zendesk
# error, document not downloaded yet, older buggy code version that logged an
# empty final_response...) was logged once and then never looked at again,
# while the ticket stayed open in Zendesk. Now only requests whose latest
# history row has a *terminal* status are skipped; everything else on a still
# active ticket is re-processed on the next run.
# ---------------------------------------------------------------------------
SUBMISSION_STATUS_SUBMITTED = "submitted"
SUBMISSION_STATUS_ALREADY_PRESENT = "already_present"
SUBMISSION_STATUS_SOLVED_WITHOUT_RESPONSE = "solved_without_response"
SUBMISSION_STATUS_HUMAN = "human_intervention"
SUBMISSION_STATUS_AWAITING_ORDER_DATA = "awaiting_order_data"
SUBMISSION_STATUS_EXCLUDED = "excluded"
SUBMISSION_STATUS_NOREPLY = "noreply"
SUBMISSION_STATUS_NO_FINAL_RESPONSE = "no_final_response"
SUBMISSION_STATUS_PENDING = "pending_submission"
SUBMISSION_STATUS_DRY_RUN = "dry_run"
SUBMISSION_STATUS_FAILED = "failed"
# The workflow could not find the data / took no action on the ticket, so the
# ticket was handed to the second-level logistics group instead.
SUBMISSION_STATUS_ASSIGNED_TO_GROUP = "assigned_to_group"
# RC-US / RC-EU / LN-EU / LN-US order: the ticket is deliberately left exactly
# as it is, with no reply, no solve and no group re-assignment.
SUBMISSION_STATUS_BLOCKED_ORDER_PREFIX = "blocked_order_prefix"

TERMINAL_SUBMISSION_STATUSES = {
    SUBMISSION_STATUS_SUBMITTED,
    SUBMISSION_STATUS_ALREADY_PRESENT,
    SUBMISSION_STATUS_SOLVED_WITHOUT_RESPONSE,
    SUBMISSION_STATUS_HUMAN,
    SUBMISSION_STATUS_EXCLUDED,
    SUBMISSION_STATUS_NOREPLY,
    SUBMISSION_STATUS_NO_FINAL_RESPONSE,
    SUBMISSION_STATUS_ASSIGNED_TO_GROUP,
    SUBMISSION_STATUS_BLOCKED_ORDER_PREFIX,
}

# ---------------------------------------------------------------------------
# Second-level logistics escalation group.
#
# Every ticket the workflow could not resolve -- no data found and no action
# taken on the ticket, not even a solve -- is assigned to this Zendesk group so
# a human picks it up, following the same approach as the standalone
# assign_tickets_to_group.py script. Tickets on a blocked order prefix are the
# one exception: they are left completely untouched.
# ---------------------------------------------------------------------------
ZENDESK_ESCALATION_GROUP_NAME = os.getenv(
    "ZENDESK_ESCALATION_GROUP_NAME",
    "LOG II lvl",
).strip()
ZENDESK_ESCALATION_GROUP_ID_ENV = "ZENDESK_ESCALATION_GROUP_ID"
ZENDESK_ESCALATION_CLEAR_ASSIGNEE_ENV = "ZENDESK_ESCALATION_CLEAR_ASSIGNEE"
ASSIGN_UNHANDLED_TICKETS_TO_GROUP_ENV = "ASSIGN_UNHANDLED_TICKETS_TO_GROUP"
DEFAULT_ASSIGN_UNHANDLED_TICKETS_TO_GROUP = True
# A ticket already sitting with the escalation group has been handed to a human
# on an earlier run. It is dropped at fetch time: not analyzed, not answered,
# and not written to the history table at all.
SKIP_TICKETS_IN_ESCALATION_GROUP_ENV = "SKIP_TICKETS_IN_ESCALATION_GROUP"
DEFAULT_SKIP_TICKETS_IN_ESCALATION_GROUP = True
# Anything else (dry_run, failed, pending_submission, awaiting_order_data) is
# retried on the next run while the ticket is still new/open/pending.

# Safety valve: stop retrying a request after this many logged runs.
HISTORY_MAX_RETRY_RUNS = int(os.getenv("HISTORY_MAX_RETRY_RUNS", "12"))

LEGACY_RETRYABLE_HUMAN_DRAFT_MARKERS = (
    "GET_FULL_ORDER API data is missing",
)

VALID_CATEGORIES = list(VALID_TICKET_CATEGORIES)
# Zendesk statuses the workflow should process. The previous broad query
# (-closed/-solved) also included hold tickets, which can be very large and
# are intentionally not considered active for this workflow.
ACTIVE_ZENDESK_STATUSES = ("new", "open", "pending")
ZENDESK_FETCH_CONFIGURED_REQUESTERS_ENV = "ZENDESK_FETCH_CONFIGURED_REQUESTERS"
ZENDESK_FETCH_CARRIER_DOMAIN_TERMS_ENV = "ZENDESK_FETCH_CARRIER_DOMAIN_TERMS"
ZENDESK_BROAD_ACTIVE_MAIL_FALLBACK_ENV = "ZENDESK_BROAD_ACTIVE_MAIL_FALLBACK"
ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE_ENV = "ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE"
DEFAULT_ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE = 45
ZENDESK_FETCH_STRATEGY_ENV = "ZENDESK_FETCH_STRATEGY"
DEFAULT_ZENDESK_FETCH_STRATEGY = "broad_active"
ZENDESK_SEARCH_EXPORT_PAGE_SIZE_ENV = "ZENDESK_SEARCH_EXPORT_PAGE_SIZE"
DEFAULT_ZENDESK_SEARCH_EXPORT_PAGE_SIZE = 500
ZENDESK_USER_FETCH_BATCH_SIZE_ENV = "ZENDESK_USER_FETCH_BATCH_SIZE"
DEFAULT_ZENDESK_USER_FETCH_BATCH_SIZE = 100
ZENDESK_COMMENT_FETCH_WORKERS_ENV = "ZENDESK_COMMENT_FETCH_WORKERS"
DEFAULT_ZENDESK_COMMENT_FETCH_WORKERS = 20
ZENDESK_MAX_RETRIES_ENV = "ZENDESK_MAX_RETRIES"
DEFAULT_ZENDESK_MAX_RETRIES = 5
MAX_TICKETS_PER_RUN_ENV = "MAX_TICKETS_PER_RUN"
DEFAULT_MAX_TICKETS_PER_RUN = 200
SKIP_PDF_SCAN_FOR_NO_REPLY_ENV = "SKIP_PDF_SCAN_FOR_NO_REPLY_REQUESTS"
DEFAULT_SKIP_PDF_SCAN_FOR_NO_REPLY = True
TICKET_OVERFETCH_MULTIPLIER_ENV = "TICKET_OVERFETCH_MULTIPLIER"
# Comments are fetched for MAX_TICKETS_PER_RUN * this many tickets. It has to
# exceed 1: whether a request is already completed is only knowable after its
# comments are read (the check is per request_id, which needs request_number,
# which needs the comments). Fetching exactly the cap would let a run whose
# selection happens to be all-completed do no work at all -- and, because the
# selection is deterministic, do no work on the next run either.
DEFAULT_TICKET_OVERFETCH_MULTIPLIER = 3
ORDER_REFERENCE_PDF_SCAN_WORKERS_ENV = "ORDER_REFERENCE_PDF_SCAN_WORKERS"
# PDF scanning is download + Tesseract OCR. Both release the GIL (one is a
# socket wait, the other a subprocess wait), so threads give real parallelism.
# Kept well below the comment-fetch pool: each worker holds a whole rendered
# page in memory and Zendesk still rate-limits the attachment listing call.
DEFAULT_ORDER_REFERENCE_PDF_SCAN_WORKERS = 8

LLM_ENGINE_PREFIXES = ("llm",)
LLM_ENGINE_NAMES = {
    "regex_llm_disagreement_guard",
}


# ============================================================================
# CLIENT AND AUTH HELPERS
# ============================================================================


def bigquery_client():
    """Create a BigQuery client from BI_BIGQUERY_CREDS."""

    from google.cloud import bigquery
    from google.oauth2 import service_account

    bq_credentials = json.loads(os.environ["BI_BIGQUERY_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(
        bq_credentials
    )

    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return client, bigquery


def til_bigquery_client():
    """Create a BigQuery client for the TIL project from TIL_BIGQUERY_CREDS.

    til.prd_shipmentStatus lives in a different GCP project
    (tlg-wlfs-prd) that the BI service account cannot read, so the
    shipment-status lookup needs its own credentials. Returns ``None`` when
    the secret is absent or unusable instead of raising: that lookup is the
    last resort for rows already heading to human review, so the run must
    continue without it rather than fail outright.
    """

    from google.cloud import bigquery
    from google.oauth2 import service_account

    raw = os.environ.get(TIL_BIGQUERY_CREDS_ENV)
    if not raw or not raw.strip():
        print(
            f"{TIL_BIGQUERY_CREDS_ENV} is not set; the prd_shipmentStatus "
            "lookup will be skipped."
        )
        return None

    try:
        til_credentials = json.loads(raw)
        credentials = service_account.Credentials.from_service_account_info(
            til_credentials
        )
        # project_id comes from the key itself, so a key issued for a
        # different project keeps working without a code change.
        project = til_credentials.get("project_id") or TIL_PROJECT_ID
        return bigquery.Client(project=project, credentials=credentials)
    except Exception as error:  # noqa: BLE001 - see the docstring
        print(
            f"Could not build the TIL BigQuery client from "
            f"{TIL_BIGQUERY_CREDS_ENV}: {error}. The prd_shipmentStatus "
            "lookup will be skipped."
        )
        return None


def zendesk_auth() -> HTTPBasicAuth:
    zendesk_config = json.loads(os.environ["ZENDESK_API_CREDENTIALS"])
    zendesk_api_token = zendesk_config["ZENDESK_API_TOKEN"]

    return HTTPBasicAuth(
        f"{ZENDESK_EMAIL}/token",
        zendesk_api_token,
    )


def zendesk_base_url() -> str:
    return f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2"


# ============================================================================
# ACTIVE TABLE HELPERS
# ============================================================================


def active_tickets_schema(bigquery):
    return [
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("request_submission_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("ticket_submission_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("request_id", "STRING"),
        bigquery.SchemaField("zendesk_ticket_id", "INT64"),
        bigquery.SchemaField("request_number", "INT64"),
        bigquery.SchemaField("requester_email", "STRING"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("request_body", "STRING"),
        bigquery.SchemaField("ticket_category", "STRING"),
        bigquery.SchemaField("extracted_tracking_number", "STRING"),
        bigquery.SchemaField("shipment_order_number", "STRING"),
        bigquery.SchemaField("shipment_tracking_number", "STRING"),
        bigquery.SchemaField("return_tracking_number", "STRING"),
        bigquery.SchemaField("shipment_carrier_code", "STRING"),
        bigquery.SchemaField("return_carrier_code", "STRING"),
        bigquery.SchemaField(
            "tracking_not_found_in_shipping_platform_shipments",
            "BOOL",
        ),
        bigquery.SchemaField("extracted_order_reference", "STRING"),
        bigquery.SchemaField("order_reference_source", "STRING"),
        bigquery.SchemaField("shipment_selection_mode", "STRING"),
        bigquery.SchemaField("order_processing_blocked", "BOOL"),
        bigquery.SchemaField("order_resolution_complete", "BOOL"),
    ]


def empty_active_tickets_table(client) -> None:
    client.query(
        f"""
        CREATE OR REPLACE TABLE `{TARGET_TABLE}` (
            ingestion_timestamp TIMESTAMP,
            request_submission_timestamp TIMESTAMP,
            ticket_submission_timestamp TIMESTAMP,
            request_id STRING,
            zendesk_ticket_id INT64,
            request_number INT64,
            requester_email STRING,
            subject STRING,
            request_body STRING,
            ticket_category STRING,
            extracted_tracking_number STRING,
            shipment_order_number STRING,
            shipment_tracking_number STRING,
            return_tracking_number STRING,
            shipment_carrier_code STRING,
            return_carrier_code STRING,
            tracking_not_found_in_shipping_platform_shipments BOOL,
            extracted_order_reference STRING,
            order_reference_source STRING,
            shipment_selection_mode STRING,
            order_processing_blocked BOOL,
            order_resolution_complete BOOL
        )
        """
    ).result()


def replace_active_tickets(df: pd.DataFrame, client=None, bigquery=None) -> None:
    if client is None or bigquery is None:
        client, bigquery = bigquery_client()

    df = df.reindex(columns=ACTIVE_TICKET_COLUMNS)

    if df.empty:
        empty_active_tickets_table(client)
        print("Successfully replaced active ticket table with 0 rows.")
        return

    for timestamp_column in [
        "ingestion_timestamp",
        "request_submission_timestamp",
        "ticket_submission_timestamp",
    ]:
        df[timestamp_column] = pd.to_datetime(
            df[timestamp_column],
            utc=True,
            errors="coerce",
        )

    job_config = bigquery.LoadJobConfig(
        schema=active_tickets_schema(bigquery),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_dataframe(
        df,
        TARGET_TABLE,
        job_config=job_config,
    )
    load_job.result()

    print(f"Successfully replaced active ticket table with {len(df)} rows.")


def _existing_request_ids(client, bigquery, request_ids: Iterable[str]) -> set[str]:
    request_ids = sorted({str(request_id) for request_id in request_ids if request_id})
    if not request_ids:
        return set()

    query = f"""
    SELECT request_id
    FROM `{HISTORY_TABLE}`
    WHERE request_id IN UNNEST(@request_ids)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "request_ids",
                "STRING",
                request_ids,
            )
        ]
    )

    return {
        row["request_id"]
        for row in client.query(query, job_config=job_config).result()
    }


def existing_history_request_ids(
    request_ids: Iterable[str],
    client=None,
    bigquery=None,
) -> set[str]:
    if client is None or bigquery is None:
        client, bigquery = bigquery_client()
    return _existing_request_ids(client, bigquery, request_ids)


def history_row_is_completed(
    *,
    status: Any,
    human_intervention_required: Any,
    draft_response: Any,
    run_count: Any = 1,
) -> bool:
    """Decide from the latest history row whether a request is finished.

    Rows written before zendesk_submission_status existed have a NULL status.
    For those, only human-intervention rows (a person owns them now) and
    excluded rows are treated as finished; a human row that was blocked only
    because GET_FULL_ORDER data/PDFs were missing is retried. Any other legacy
    row on a ticket that is still active was evidently never answered.
    """

    count = _to_int(run_count) or 1
    if HISTORY_MAX_RETRY_RUNS > 0 and count >= HISTORY_MAX_RETRY_RUNS:
        return True

    status_text = (_to_str(status) or "").strip().lower()
    if status_text:
        return status_text in TERMINAL_SUBMISSION_STATUSES

    draft = _to_str(draft_response) or ""
    if draft.lstrip().startswith("Excluded from processing"):
        return True
    if bool(_to_bool(human_intervention_required)):
        return not any(marker in draft for marker in LEGACY_RETRYABLE_HUMAN_DRAFT_MARKERS)
    return False


def _completed_request_ids(client, bigquery, request_ids: Iterable[str]) -> set[str]:
    request_ids = sorted({str(request_id) for request_id in request_ids if request_id})
    if not request_ids:
        return set()

    _ensure_history_schema(client)

    query = f"""
    SELECT
        request_id,
        zendesk_submission_status,
        human_intervention_required,
        draft_response,
        COUNT(*) OVER (PARTITION BY request_id) AS run_count
    FROM `{HISTORY_TABLE}`
    WHERE request_id IN UNNEST(@request_ids)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY request_id
        ORDER BY ingestion_timestamp DESC
    ) = 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("request_ids", "STRING", request_ids)
        ]
    )

    completed: set[str] = set()
    for row in client.query(query, job_config=job_config).result():
        if history_row_is_completed(
            status=row["zendesk_submission_status"],
            human_intervention_required=row["human_intervention_required"],
            draft_response=row["draft_response"],
            run_count=row["run_count"],
        ):
            completed.add(row["request_id"])
    return completed


def completed_history_request_ids(
    request_ids: Iterable[str],
    client=None,
    bigquery=None,
) -> set[str]:
    """Return request_ids whose latest history row means "nothing left to do"."""

    if client is None or bigquery is None:
        client, bigquery = bigquery_client()
    return _completed_request_ids(client, bigquery, request_ids)


# ============================================================================
# HISTORY TABLE LOGGING HELPERS
# ============================================================================


def _is_missing(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, float) and math.isnan(value):
        return True

    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _to_int(value: Any) -> int | None:
    if _is_missing(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _to_str(value: Any) -> str | None:
    if _is_missing(value):
        return None
    return str(value)


def _to_timestamp(value: Any) -> str | None:
    if _is_missing(value):
        return None

    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None

    return timestamp.to_pydatetime().isoformat().replace("+00:00", "Z")


def _parse_collection(value: Any) -> list[Any]:
    if _is_missing(value):
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    if isinstance(value, set):
        return list(value)

    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw.lower() in {"nan", "none", "null", "n/a", "na"}:
            return []

        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(raw)
                if isinstance(parsed, list):
                    return parsed
                if isinstance(parsed, tuple):
                    return list(parsed)
                if isinstance(parsed, set):
                    return list(parsed)
                if _is_missing(parsed):
                    return []
                return [parsed]
            except Exception:
                continue

        return [raw]

    return [value]


def _string_list(value: Any) -> list[str]:
    result: list[str] = []
    for item in _parse_collection(value):
        if _is_missing(item):
            continue
        item_str = str(item).strip()
        if item_str and item_str not in result:
            result.append(item_str)
    return result


def _matched_spans(value: Any) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []

    for item in _parse_collection(value):
        if not isinstance(item, Mapping):
            continue

        span = {
            "request_type": _to_str(item.get("request_type")),
            "span": _to_str(item.get("span")),
            "start": _to_int(item.get("start")),
            "end_pos": _to_int(item.get("end_pos", item.get("end"))),
        }
        spans.append(span)

    return spans


def _infer_llm_was_used(row: Mapping[str, Any]) -> bool:
    if "llm_was_used" in row and not _is_missing(row.get("llm_was_used")):
        return bool(_to_bool(row.get("llm_was_used")))

    engine = str(row.get("engine", "") or "").strip().lower()
    return engine.startswith(LLM_ENGINE_PREFIXES) or engine in LLM_ENGINE_NAMES


def history_schema(bigquery):
    return [
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("request_submission_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("ticket_submission_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("workflow_run_id", "STRING"),
        bigquery.SchemaField("request_run_id", "STRING"),
        bigquery.SchemaField("zendesk_ticket_id", "INT64"),
        bigquery.SchemaField("request_number", "INT64"),
        bigquery.SchemaField("request_id", "STRING"),
        bigquery.SchemaField("requester_email", "STRING"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("request_body", "STRING"),
        bigquery.SchemaField("cleaned_request_body", "STRING"),
        bigquery.SchemaField("ticket_category", "STRING"),
        bigquery.SchemaField("extracted_tracking_number", "STRING"),
        bigquery.SchemaField("shipment_order_number", "STRING"),
        bigquery.SchemaField("shipment_tracking_number", "STRING"),
        bigquery.SchemaField("return_tracking_number", "STRING"),
        bigquery.SchemaField("extracted_order_reference", "STRING"),
        bigquery.SchemaField("order_reference_source", "STRING"),
        bigquery.SchemaField("shipment_selection_mode", "STRING"),
        bigquery.SchemaField("order_processing_blocked", "BOOL"),
        bigquery.SchemaField("order_resolution_complete", "BOOL"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("human_intervention_required", "BOOL"),
        bigquery.SchemaField("regex_request_types", "STRING", mode="REPEATED"),
        bigquery.SchemaField("regex_requested_data", "STRING", mode="REPEATED"),
        bigquery.SchemaField("regex_confidence", "FLOAT64"),
        bigquery.SchemaField(
            "matched_spans",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("request_type", "STRING"),
                bigquery.SchemaField("span", "STRING"),
                bigquery.SchemaField("start", "INT64"),
                bigquery.SchemaField("end_pos", "INT64"),
            ],
        ),
        bigquery.SchemaField("request_language", "STRING"),
        bigquery.SchemaField("language_confidence", "FLOAT64"),
        bigquery.SchemaField("language_notes", "STRING"),
        bigquery.SchemaField("llm_was_used", "BOOL"),
        bigquery.SchemaField("llm_confidence", "FLOAT64"),
        bigquery.SchemaField("requested_data", "STRING", mode="REPEATED"),
        bigquery.SchemaField("draft_response", "STRING"),
        bigquery.SchemaField("final_response", "STRING"),
        bigquery.SchemaField("zendesk_submission_status", "STRING"),
    ]


# Columns added to the history table after it was first created.
_ADDITIVE_HISTORY_COLUMNS = (
    ("final_response", "STRING"),
    ("zendesk_submission_status", "STRING"),
    ("extracted_order_reference", "STRING"),
    ("order_reference_source", "STRING"),
    ("shipment_selection_mode", "STRING"),
    ("order_processing_blocked", "BOOL"),
    ("order_resolution_complete", "BOOL"),
    ("full_order_export_tracking_number", "STRING"),
)
_HISTORY_SCHEMA_ENSURED = False


def _ensure_history_schema(client) -> None:
    """Apply additive history-table schema changes required by this version.

    BigQuery rate-limits table metadata updates (about 5 per 10 seconds per
    table), and every ALTER TABLE counts as one even with ADD COLUMN IF NOT
    EXISTS when the column already exists. So: read the schema first (a read,
    not an update), ALTER only for columns that are really missing, and do
    this at most once per process.
    """

    global _HISTORY_SCHEMA_ENSURED
    if _HISTORY_SCHEMA_ENSURED:
        return

    existing = {field.name for field in client.get_table(HISTORY_TABLE).schema}
    for column, column_type in _ADDITIVE_HISTORY_COLUMNS:
        if column in existing:
            continue
        print(f"Adding missing column {column} to {HISTORY_TABLE}.")
        client.query(
            f"""
            ALTER TABLE `{HISTORY_TABLE}`
            ADD COLUMN IF NOT EXISTS {column} {column_type}
            """
        ).result()

    _HISTORY_SCHEMA_ENSURED = True


def _is_bigquery_rate_limit_error(exc: BaseException) -> bool:
    text = str(exc)
    return (
        getattr(exc, "code", None) == 429
        or "rateLimitExceeded" in text
        or "Exceeded rate limits" in text
    )


def _run_history_load_job(client, rows, job_config, *, max_attempts: int | None = None):
    """Run the history append, retrying BigQuery 429 rate-limit errors.

    A load job that failed with rateLimitExceeded wrote nothing, so a fresh
    job with the same rows cannot create duplicates.
    """

    attempts = max_attempts or int(os.getenv("BIGQUERY_HISTORY_LOAD_MAX_ATTEMPTS", "6"))
    delay = 5.0
    for attempt in range(1, attempts + 1):
        try:
            load_job = client.load_table_from_json(
                rows,
                HISTORY_TABLE,
                job_config=job_config,
            )
            return load_job.result()
        except Exception as exc:  # noqa: BLE001 - only rate limits are retried
            if attempt >= attempts or not _is_bigquery_rate_limit_error(exc):
                raise
            print(
                f"BigQuery rate limit on history append (attempt {attempt}/{attempts}); "
                f"retrying in {delay:.0f}s: {exc}"
            )
            time.sleep(delay)
            delay = min(delay * 2, 60.0)


def _target_history_schema(client):
    """
    Use the live destination schema for history appends.

    This keeps logging independent of the physical column order in BigQuery and
    allows future nullable columns to exist in the table even when this pipeline
    does not populate them yet.
    """

    _ensure_history_schema(client)

    table = client.get_table(HISTORY_TABLE)
    actual_columns = {field.name for field in table.schema}
    missing_columns = [column for column in HISTORY_COLUMNS if column not in actual_columns]

    if missing_columns:
        raise RuntimeError(
            "BigQuery history table is missing expected columns: "
            + ", ".join(missing_columns)
        )

    return table.schema


def prepare_history_row(
    row: Mapping[str, Any],
    *,
    workflow_run_id: str,
    ingestion_timestamp: str,
) -> dict[str, Any]:
    zendesk_ticket_id = _to_int(row.get("zendesk_ticket_id"))
    request_number = _to_int(row.get("request_number"))
    request_id = _to_str(row.get("request_id")) or build_request_id(
        zendesk_ticket_id,
        request_number,
    )

    llm_was_used = _infer_llm_was_used(row)
    regex_confidence = _to_float(row.get("regex_confidence"))
    llm_confidence = _to_float(row.get("llm_confidence"))

    # Backward-compatible fallbacks for older local handoff files that only
    # contained a generic confidence column.  For regex-only rows the generic
    # value belongs to regex_confidence; for LLM rows it belongs to llm_confidence.
    if regex_confidence is None and not llm_was_used:
        regex_confidence = _to_float(row.get("confidence"))

    if llm_confidence is None and llm_was_used:
        llm_confidence = _to_float(row.get("confidence"))

    prepared = {
        "ingestion_timestamp": ingestion_timestamp,
        "request_submission_timestamp": _to_timestamp(
            row.get("request_submission_timestamp")
        ),
        "ticket_submission_timestamp": _to_timestamp(
            row.get("ticket_submission_timestamp")
        ),
        "workflow_run_id": workflow_run_id,
        "request_run_id": str(uuid.uuid4()),
        "zendesk_ticket_id": zendesk_ticket_id,
        "request_number": request_number,
        "request_id": request_id,
        "requester_email": _to_str(row.get("requester_email")),
        "subject": _to_str(row.get("subject")),
        "request_body": _to_str(row.get("request_body")),
        "cleaned_request_body": _to_str(row.get("cleaned_request_body")),
        "ticket_category": _to_str(row.get("ticket_category")),
        "extracted_tracking_number": _to_str(row.get("extracted_tracking_number")),
        "shipment_order_number": _to_str(row.get("shipment_order_number")),
        "shipment_tracking_number": _to_str(row.get("shipment_tracking_number")),
        "return_tracking_number": _to_str(row.get("return_tracking_number")),
        "extracted_order_reference": _to_str(row.get("extracted_order_reference")),
        "order_reference_source": _to_str(row.get("order_reference_source")),
        "shipment_selection_mode": _to_str(row.get("shipment_selection_mode")),
        "order_processing_blocked": bool(_to_bool(row.get("order_processing_blocked"))),
        "order_resolution_complete": bool(_to_bool(row.get("order_resolution_complete"))),
        "notes": _to_str(row.get("notes")),
        "human_intervention_required": bool(
            _to_bool(row.get("human_intervention_required"))
        ),
        "regex_request_types": _string_list(row.get("regex_request_types")),
        "regex_requested_data": _string_list(row.get("regex_requested_data")),
        "regex_confidence": regex_confidence,
        "matched_spans": _matched_spans(row.get("matched_spans")),
        "request_language": _to_str(row.get("request_language")),
        "language_confidence": _to_float(row.get("language_confidence")),
        "language_notes": _to_str(row.get("language_notes")),
        "llm_was_used": llm_was_used,
        "llm_confidence": llm_confidence,
        "requested_data": _string_list(row.get("requested_data")),
        "draft_response": _to_str(row.get("draft_response")),
        "final_response": _to_str(row.get("final_response")),
        "zendesk_submission_status": _to_str(row.get("zendesk_submission_status")),
    }

    return {column: prepared.get(column) for column in HISTORY_COLUMNS}


def append_history_rows(
    rows: Iterable[Mapping[str, Any]] | pd.DataFrame,
    *,
    return_inserted_request_ids: bool = False,
) -> int | tuple[int, list[str]]:
    """Append final workflow rows to the BigQuery history table."""

    def _return(count: int, inserted_request_ids: list[str] | None = None):
        if return_inserted_request_ids:
            return count, list(inserted_request_ids or [])
        return count

    if isinstance(rows, pd.DataFrame):
        raw_rows = rows.to_dict(orient="records")
    else:
        raw_rows = list(rows)

    if not raw_rows:
        print("No final rows to log to BigQuery history.")
        return _return(0)

    workflow_run_id = get_workflow_run_id()
    ingestion_timestamp = datetime.now(timezone.utc).isoformat().replace(
        "+00:00",
        "Z",
    )

    prepared_rows = [
        prepare_history_row(
            row,
            workflow_run_id=workflow_run_id,
            ingestion_timestamp=ingestion_timestamp,
        )
        for row in raw_rows
    ]
    prepared_rows = [row for row in prepared_rows if row.get("request_id")]

    unique_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in prepared_rows:
        request_id = row["request_id"]
        if request_id in seen_ids:
            continue
        seen_ids.add(request_id)
        unique_rows.append(row)

    if not unique_rows:
        print("No rows with a valid request_id to log to BigQuery history.")
        return _return(0)

    client, bigquery = bigquery_client()
    target_schema = _target_history_schema(client)

    # Only requests whose latest history row is terminal are skipped. Retried
    # requests (dry run, failed submission, awaiting documents, legacy rows
    # that were never answered) get a new row for this run, so the latest row
    # always reflects the latest outcome.
    existing_ids = _completed_request_ids(
        client,
        bigquery,
        [row["request_id"] for row in unique_rows],
    )

    rows_to_insert = [
        row for row in unique_rows if row["request_id"] not in existing_ids
    ]

    skipped_count = len(unique_rows) - len(rows_to_insert)
    if skipped_count:
        print(f"Skipped {skipped_count} rows already completed in {HISTORY_TABLE}.")

    if not rows_to_insert:
        print("No new rows to append to BigQuery history.")
        return _return(0)

    job_config = bigquery.LoadJobConfig(
        schema=target_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    _run_history_load_job(client, rows_to_insert, job_config)

    print(
        f"Logged {len(rows_to_insert)} rows to {HISTORY_TABLE} "
        f"with workflow_run_id={workflow_run_id}."
    )

    return _return(
        len(rows_to_insert),
        [str(row["request_id"]) for row in rows_to_insert if row.get("request_id")],
    )


def new_history_request_ids(rows: Iterable[Mapping[str, Any]] | pd.DataFrame) -> list[str]:
    """Return request_ids that still need a history row (not yet completed)."""

    if isinstance(rows, pd.DataFrame):
        raw_rows = rows.to_dict(orient="records")
    else:
        raw_rows = list(rows)

    if not raw_rows:
        return []

    workflow_run_id = get_workflow_run_id()
    ingestion_timestamp = datetime.now(timezone.utc).isoformat().replace(
        "+00:00",
        "Z",
    )
    prepared_rows = [
        prepare_history_row(
            row,
            workflow_run_id=workflow_run_id,
            ingestion_timestamp=ingestion_timestamp,
        )
        for row in raw_rows
    ]

    request_ids: list[str] = []
    for row in prepared_rows:
        request_id = row.get("request_id")
        if request_id and request_id not in request_ids:
            request_ids.append(str(request_id))

    if not request_ids:
        return []

    client, bigquery = bigquery_client()
    _target_history_schema(client)
    existing_ids = _completed_request_ids(client, bigquery, request_ids)
    return [request_id for request_id in request_ids if request_id not in existing_ids]


def append_history_from_file(path=REQUEST_INTENT_RESULTS_PATH) -> int:
    """Convenience CLI helper to append output/request_intent_results.jsonl.gz."""

    df = read_dataframe(path)
    return append_history_rows(df)


# ============================================================================
# ZENDESK RESPONSE HELPERS
# ============================================================================


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    raw = str(os.getenv(name, str(default)) or "").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc

    value = max(value, minimum)
    if maximum is not None:
        value = min(value, maximum)
    return value


class ZendeskUpdateAlreadyApplied(Exception):
    """Raised when a retried write turns out to have landed already.

    Zendesk sometimes drops the TCP connection *after* it has processed a
    ticket update (``Connection reset by peer``). Blindly retrying such a
    PUT would post the same public comment twice, so the retry loop asks
    the caller-supplied probe whether the write is already visible on the
    ticket and, if it is, stops instead of sending it again.
    """


# Transport-level failures that are always worth retrying: the request either
# never reached Zendesk or the answer never came back.
_ZENDESK_TRANSPORT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.Timeout,
)


def _zendesk_retry_backoff_seconds(attempt: int) -> float:
    """Exponential backoff (2s, 4s, 8s, ...) capped at 30 seconds."""

    return float(min(2 ** attempt, 30))


def zendesk_request(
    method: str,
    url: str,
    *,
    auth,
    session: requests.Session | None = None,
    already_applied_probe=None,
    **kwargs: Any,
) -> requests.Response:
    """Zendesk HTTP call with retries for resets, rate limits and 5xx errors.

    The response is returned as-is (no ``raise_for_status()``) so callers keep
    their existing status-code handling, such as the 422 required-field
    fallbacks in submit_ticket_response().

    ``already_applied_probe`` is an optional callable used only for
    non-idempotent writes: after a dropped connection it is asked whether the
    update is already on the ticket, and ZendeskUpdateAlreadyApplied is raised
    instead of resending the request.
    """

    http = session or requests
    max_retries = _env_int(
        ZENDESK_MAX_RETRIES_ENV,
        DEFAULT_ZENDESK_MAX_RETRIES,
        minimum=1,
        maximum=10,
    )
    last_response: requests.Response | None = None
    last_transport_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = http.request(method, url, auth=auth, **kwargs)
        except _ZENDESK_TRANSPORT_ERRORS as exc:
            last_transport_error = exc
            if attempt >= max_retries:
                break

            if already_applied_probe is not None:
                try:
                    if already_applied_probe():
                        raise ZendeskUpdateAlreadyApplied(
                            "Zendesk closed the connection after applying the "
                            "update; not resending it."
                        )
                except ZendeskUpdateAlreadyApplied:
                    raise
                except Exception as probe_exc:  # noqa: BLE001 - the probe is
                    # best-effort; if it fails we still retry the write.
                    print(
                        "Could not verify whether the Zendesk update was already "
                        f"applied ({probe_exc}). Retrying the request."
                    )

            sleep_seconds = _zendesk_retry_backoff_seconds(attempt)
            print(
                f"Zendesk connection error on {method.upper()} ({exc}). "
                f"Retrying in {sleep_seconds:.0f} seconds "
                f"(attempt {attempt + 1}/{max_retries})."
            )
            time.sleep(sleep_seconds)
            continue

        last_response = response
        last_transport_error = None

        if response.status_code == 429 and attempt < max_retries:
            retry_after_raw = response.headers.get("Retry-After", "60")
            try:
                retry_after = int(float(retry_after_raw))
            except ValueError:
                retry_after = 60
            sleep_seconds = max(retry_after + 1, 1)
            print(
                "Rate limited by Zendesk. "
                f"Sleeping for {sleep_seconds} seconds before retrying."
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code in {500, 502, 503, 504} and attempt < max_retries:
            sleep_seconds = _zendesk_retry_backoff_seconds(attempt)
            print(
                f"Transient Zendesk error {response.status_code}. "
                f"Retrying in {sleep_seconds:.0f} seconds."
            )
            time.sleep(sleep_seconds)
            continue

        return response

    if last_response is not None:
        return last_response
    if last_transport_error is not None:
        raise last_transport_error
    raise RuntimeError("Zendesk request failed without a response.")


def zendesk_get(
    url: str,
    *,
    auth,
    session: requests.Session | None = None,
    params: Mapping[str, Any] | None = None,
    timeout: int = 60,
) -> requests.Response:
    """GET wrapper with retry handling for Zendesk rate limits/errors."""

    response = zendesk_request(
        "GET",
        url,
        auth=auth,
        session=session,
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response


def zendesk_response_submission_enabled() -> bool:
    """Return True only when public Zendesk replies are explicitly enabled."""

    return _env_bool(
        ZENDESK_RESPONSE_SUBMISSION_ENV,
        DEFAULT_SUBMIT_ZENDESK_RESPONSES,
    )


def zendesk_status_after_reply() -> str:
    """Return the ticket status to set when posting an automatic Zendesk reply."""

    status = str(
        os.getenv(
            ZENDESK_STATUS_AFTER_REPLY_ENV,
            DEFAULT_ZENDESK_STATUS_AFTER_REPLY,
        )
        or ""
    ).strip().lower()
    if not status:
        return ""

    if status == "closed":
        print(
            f"{ZENDESK_STATUS_AFTER_REPLY_ENV}=closed would make the ticket "
            "non-editable/non-reopenable; using 'solved' instead."
        )
        status = "solved"

    valid_statuses = {"new", "open", "pending", "hold", "solved"}
    if status not in valid_statuses:
        raise ValueError(
            f"Unsupported {ZENDESK_STATUS_AFTER_REPLY_ENV}={status!r}; "
            "use one of: new, open, pending, hold, solved"
        )
    return status


def _response_is_blank(value: Any) -> bool:
    if _is_missing(value):
        return True
    return str(value).strip().lower() in {"", "nan", "none", "null", "n/a", "na"}


def _attachment_paths(value: Any) -> list[str]:
    paths: list[str] = []
    for item in _parse_collection(value):
        if _is_missing(item):
            continue
        path = str(item).strip()
        if path and path not in paths:
            paths.append(path)
    return paths


def upload_zendesk_attachment(
    path: str | Path,
    *,
    auth: HTTPBasicAuth | None = None,
    base_url: str | None = None,
) -> str:
    """Upload one local file to Zendesk and return its upload token."""

    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"Zendesk attachment not found: {file_path}")

    auth = auth or zendesk_auth()
    base_url = (base_url or zendesk_base_url()).rstrip("/")
    url = f"{base_url}/uploads.json?filename={quote(file_path.name)}"

    # The bytes are read up front (instead of streaming the open file handle)
    # so the upload can be retried after a dropped connection: a consumed file
    # object cannot be replayed on a second attempt.
    file_bytes = file_path.read_bytes()
    response = zendesk_request(
        "POST",
        url,
        auth=auth,
        data=file_bytes,
        headers={"Content-Type": "application/pdf"},
        timeout=60,
    )
    response.raise_for_status()
    token = str(response.json().get("upload", {}).get("token", "") or "").strip()
    if not token:
        raise RuntimeError(f"Zendesk upload did not return a token for {file_path}")
    return token


def _normalize_comment_body(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\r\n", "\n")).strip()


_IMAGE_LINK_LINE_RE = re.compile(r"\bimage\s*:", re.IGNORECASE)


def _strip_links_from_line(line: str) -> str:
    line = re.sub(
        r":\s*\[[^\]\n]+\]\([^\)\n]+\)",
        "",
        line,
        flags=re.IGNORECASE,
    )
    line = re.sub(
        r":\s*(?:https?://\S+|generated_documents/\S+|/[^\s:]+/generated_documents/\S+)",
        "",
        line,
        flags=re.IGNORECASE,
    )
    line = re.sub(
        r"\[[^\]\n]+\]\([^\)\n]+\)",
        "",
        line,
        flags=re.IGNORECASE,
    )
    line = re.sub(r"https?://\S+", "", line, flags=re.IGNORECASE)
    line = re.sub(
        r"(?:generated_documents/\S+|/[^\s:]+/generated_documents/\S+)",
        "",
        line,
        flags=re.IGNORECASE,
    )
    return line


def strip_links_from_public_response(value: Any) -> str:
    """Defense-in-depth guard: public Zendesk replies must not contain links.

    Returned-item image links (lines containing "Image:") are kept intact so
    the customer-facing reply still shows what was confirmed as returned.
    """

    sanitized = str(value or "")
    lines = sanitized.split("\n")
    processed_lines = [
        line if _IMAGE_LINK_LINE_RE.search(line) else _strip_links_from_line(line)
        for line in lines
    ]
    lines = [re.sub(r"[ \t]+$", "", line) for line in processed_lines]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def zendesk_public_comment_exists(
    ticket_id: int,
    body: str,
    *,
    auth: HTTPBasicAuth | None = None,
    base_url: str | None = None,
) -> bool:
    """Return True when this exact reply is still the last word on the ticket.

    The guard exists for one case: a reply that was posted but whose run did
    not record it (status update rejected, history write lost, dry run turned
    off mid-flight), which would otherwise be posted a second time.

    It used to scan the whole comment history, which made it fire on a
    perfectly legitimate second request. A customer who writes back after
    being answered -- "the invoice is unreadable", "please resend" -- gets a
    new request_number, is analyzed normally, and can quite reasonably need
    the same documents again; refusing to post because that text appears
    somewhere above in the thread leaves the ticket open with the customer's
    question unanswered, which is what happened to ticket 5004054.

    So only the *last* public comment is compared. If anyone has spoken since
    our reply -- the requester above all -- this is a new exchange and the
    answer is posted again.
    """

    auth = auth or zendesk_auth()
    base_url = (base_url or zendesk_base_url()).rstrip("/")
    wanted_body = _normalize_comment_body(body)
    if not wanted_body:
        return False

    last_public_comment = None
    for comment in get_ticket_comments(ticket_id, auth=auth, base_url=base_url):
        if comment.get("public", True):
            last_public_comment = comment

    if last_public_comment is None:
        return False

    return any(
        _normalize_comment_body(last_public_comment.get(body_key)) == wanted_body
        for body_key in ("body", "plain_body")
    )



_ZENDESK_FIELD_CACHE={}
def _zendesk_ticket_field(field_id:int,auth,base_url):
    if field_id in _ZENDESK_FIELD_CACHE:
        return _ZENDESK_FIELD_CACHE[field_id]
    r=zendesk_get(f"{base_url}/ticket_fields/{field_id}.json",auth=auth,timeout=60)
    fld=r.json()["ticket_field"]
    _ZENDESK_FIELD_CACHE[field_id]=fld
    return fld

def _zendesk_field_value(field_id:int,display_name:str,auth,base_url):
    fld=_zendesk_ticket_field(field_id,auth,base_url)
    for opt in fld.get("custom_field_options",[]):
        if str(opt.get("name","")).strip().lower()==str(display_name).strip().lower():
            return opt.get("value")
    return fld.get("custom_field_options",[{}])[0].get("value")
def _brand_from_country_tag(tag: str) -> str:
    """Extract the brand code from a country tag like 'country_dj_us' -> 'DJ'."""
    # Tags are always: country_{brand}_{iso2}
    parts = str(tag or "").split("_")
    # parts[0] = "country", parts[1] = brand, parts[2] = iso2
    if len(parts) >= 3:
        return parts[1].upper()
    return ""


def _iso_code_from(country_code: object, country_tag: object) -> str:
    """Return the lowercase ISO 3166-1 alpha-2 code for a row.

    Prefers the GET_FULL_ORDER shipping-address country code and falls back to
    the ISO suffix of an already-built tag ("country_dj_us" -> "us").
    """
    iso = str(country_code or "").strip().lower()
    if iso:
        return iso

    parts = str(country_tag or "").strip().lower().rsplit("_", 1)
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return ""


def submit_ticket_response(
    ticket_id: int,
    body: str,
    *,
    attachment_paths: Iterable[str] = (),
    reason_of_contact: str = 'Altro',
    order_note: str = '-',
    country_tag: str | None = None,
    country_code: str | None = None,
    auth: HTTPBasicAuth | None = None,
    base_url: str | None = None,
) -> bool:
    """Submit one public Zendesk ticket comment with optional uploaded files."""

    if _response_is_blank(body):
        raise ValueError("Cannot submit an empty Zendesk response body")

    auth = auth or zendesk_auth()
    base_url = (base_url or zendesk_base_url()).rstrip("/")

    if _env_bool("ZENDESK_SKIP_DUPLICATE_FINAL_RESPONSE", True) and zendesk_public_comment_exists(
        ticket_id,
        body,
        auth=auth,
        base_url=base_url,
    ):
        print(f"Skipped Zendesk ticket {ticket_id}: final_response is already present.")
        return False

    upload_tokens = [
        upload_zendesk_attachment(path, auth=auth, base_url=base_url)
        for path in attachment_paths
    ]

    comment: dict[str, Any] = {
        "body": str(body).strip(),
        "public": True,
    }
    if upload_tokens:
        comment["uploads"] = upload_tokens

    ticket_update: dict[str, Any] = {"comment": comment}
    status_after_reply = zendesk_status_after_reply()
    if status_after_reply:
        ticket_update["status"] = status_after_reply
    channel_value=_zendesk_field_value(360000381300,"online",auth,base_url)
    reason_value=_zendesk_field_value(23910471,reason_of_contact,auth,base_url)
    custom_fields: list[dict[str, Any]] = [
        {"id": 360000381300, "value": channel_value},
        {"id": 23910471, "value": reason_value},
        {"id": 36951465, "value": order_note or "-"},
    ]

    # Populate the per-brand "Country <BRAND>" field when the country tag was
    # resolved from the GET_FULL_ORDER shippingAddress (e.g. "country_dj_us").
    # The tag encodes both the brand and the ISO country code so we can derive
    # the Zendesk field id from the brand prefix without an extra API call.
    if country_tag or country_code:
        brand_code = _brand_from_country_tag(country_tag)
        from customs_rules import country_ticket_field_id_for_brand
        field_id = country_ticket_field_id_for_brand(brand_code)
        if field_id:
            # The tag shape is only a guess; Zendesk stores a FieldTagger value
            # only when it exists in the field's own option list, so resolve
            # the real option value and fall back to the tag if the lookup
            # yields nothing.
            iso_code = _iso_code_from(country_code, country_tag)
            option_value = _country_field_option_value(
                field_id,
                iso_code,
                brand_code,
                auth,
                base_url,
            )
            if option_value or country_tag:
                custom_fields.append(
                    {"id": field_id, "value": option_value or country_tag}
                )

    ticket_update["custom_fields"] = custom_fields

    ticket_url = f"{base_url}/tickets/{int(ticket_id)}.json"

    # Zendesk sometimes resets the connection after it has already applied the
    # update. Before resending the comment, check whether it is already on the
    # ticket, so a dropped connection can never produce a duplicate reply.
    def _comment_already_posted() -> bool:
        return zendesk_public_comment_exists(
            ticket_id,
            body,
            auth=auth,
            base_url=base_url,
        )

    try:
        response = zendesk_request(
            "PUT",
            ticket_url,
            auth=auth,
            json={"ticket": ticket_update},
            timeout=60,
            already_applied_probe=_comment_already_posted,
        )
    except ZendeskUpdateAlreadyApplied as exc:
        print(f"Zendesk ticket {ticket_id}: {exc}")
        return True

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        error_detail = _zendesk_error_detail(response)

        # Zendesk rejects ANY update (comment or status) to a ticket whose
        # current status is already "closed" -- this is unrelated to the
        # status we're trying to set, so retrying with a different status
        # value will not help. Surface this as a clear, specific failure
        # instead of a bare "422 Client Error".
        if response.status_code == 422 and _is_zendesk_closed_ticket_error(error_detail):
            raise ZendeskTicketClosedError(
                f"Zendesk ticket {ticket_id} is closed and cannot be updated "
                f"via the API. Skipping this ticket. Zendesk said: {error_detail}"
            ) from exc

        # Some brands require extra custom fields to be set before a ticket
        # can be moved to "solved" (e.g. "Country <BRAND>", "Amazon Order
        # Number"). When Zendesk returns a list of missing field IDs, try to
        # populate them automatically from known shipment data and retry the
        # full update (comment + status) in one call. Fields that cannot be
        # resolved are skipped; if the retry still fails, fall back to posting
        # the comment without the status change so the reply is still delivered
        # and the ticket is left for manual solving.
        if (
            response.status_code == 422
            and "status" in ticket_update
            and _is_zendesk_required_field_for_solve_error(error_detail)
        ):
            missing_field_ids = _extract_missing_field_ids(error_detail)
            order_note_value = str(order_note or "-")
            extra_fields = _resolve_missing_fields(
                missing_field_ids,
                order_note_value,
                country_tag=country_tag,
                country_code=country_code,
                auth=auth,
                base_url=base_url,
            )
            if extra_fields:
                # Merge the newly resolved fields into custom_fields and retry
                # the original update (with status) so the ticket is solved in
                # one call without any separate follow-up request.
                # Resolved values override whatever was sent the first time:
                # the original attempt may have carried a constructed country
                # tag that Zendesk does not accept as an option.
                resolved_by_id = {f["id"]: f for f in extra_fields}
                merged_fields = [
                    resolved_by_id.get(f["id"], f)
                    for f in ticket_update.get("custom_fields", [])
                ]
                existing_ids = {f["id"] for f in merged_fields}
                for field in extra_fields:
                    if field["id"] not in existing_ids:
                        merged_fields.append(field)
                retry_update = {**ticket_update, "custom_fields": merged_fields}
                print(
                    f"Zendesk ticket {ticket_id}: retrying with resolved fields "
                    f"{[f['id'] for f in extra_fields]} populated from shipment data."
                )
                retry_response = zendesk_request(
                    "PUT",
                    ticket_url,
                    auth=auth,
                    json={"ticket": retry_update},
                    timeout=60,
                    already_applied_probe=_comment_already_posted,
                )
                try:
                    retry_response.raise_for_status()
                    return True
                except requests.HTTPError:
                    # Field injection did not resolve the issue; fall through
                    # to the comment-only retry below.
                    retry_error_detail = _zendesk_error_detail(retry_response)
                    print(
                        f"Zendesk ticket {ticket_id}: field-injection retry failed "
                        f"({retry_error_detail}). Falling back to comment-only update; "
                        "ticket needs manual solving."
                    )

            print(
                f"Zendesk ticket {ticket_id}: cannot set status to "
                f"'{ticket_update['status']}' because a required field is "
                f"missing ({error_detail}). Retrying without the status "
                "change so the reply is still posted; ticket needs manual "
                "solving."
            )
            retry_update = {k: v for k, v in ticket_update.items() if k != "status"}
            retry_response = zendesk_request(
                "PUT",
                ticket_url,
                auth=auth,
                json={"ticket": retry_update},
                timeout=60,
                already_applied_probe=_comment_already_posted,
            )
            try:
                retry_response.raise_for_status()
            except requests.HTTPError as retry_exc:
                retry_error_detail = _zendesk_error_detail(retry_response)
                raise requests.HTTPError(
                    f"{retry_exc} | Zendesk ticket {ticket_id} update failed "
                    f"(even after retrying without status change). Zendesk "
                    f"error detail: {retry_error_detail}",
                    response=retry_response,
                ) from retry_exc
            return True

        raise requests.HTTPError(
            f"{exc} | Zendesk ticket {ticket_id} update failed. "
            f"Zendesk error detail: {error_detail}",
            response=response,
        ) from exc
    return True


def submit_ticket_solved_without_response(
    ticket_id: int,
    *,
    reason_of_contact: str = 'Altro',
    order_note: str = '-',
    country_tag: str | None = None,
    country_code: str | None = None,
    auth: HTTPBasicAuth | None = None,
    base_url: str | None = None,
) -> bool:
    """Move a ticket straight to ``solved`` with no Zendesk comment at all.

    Used exclusively for automated ``noreply@fedex.com`` requests: FedEx's
    notification mailbox never reads replies, so posting a public comment
    would be pointless. The ticket is still solved so it doesn't linger in
    the active queue, but `body`/`comment` is intentionally omitted from the
    Zendesk ticket update.

    Even without a comment, Zendesk still requires the same mandatory
    ticket fields as any other solve: Channel, Reason of contact, and Order
    Number. These are filled in exactly like submit_ticket_response() does,
    otherwise Zendesk rejects the solve with a 422 "is required" error.
    """

    auth = auth or zendesk_auth()
    base_url = (base_url or zendesk_base_url()).rstrip("/")

    ticket_update: dict[str, Any] = {"status": "solved"}

    channel_value = _zendesk_field_value(360000381300, "online", auth, base_url)
    reason_value = _zendesk_field_value(23910471, reason_of_contact, auth, base_url)
    custom_fields: list[dict[str, Any]] = [
        {"id": 360000381300, "value": channel_value},
        {"id": 23910471, "value": reason_value},
        {"id": 36951465, "value": order_note or "-"},
    ]

    # Same per-brand "Country <BRAND>" field handling as submit_ticket_response().
    if country_tag or country_code:
        brand_code = _brand_from_country_tag(country_tag)
        from customs_rules import country_ticket_field_id_for_brand
        field_id = country_ticket_field_id_for_brand(brand_code)
        if field_id:
            # The tag shape is only a guess; Zendesk stores a FieldTagger value
            # only when it exists in the field's own option list, so resolve
            # the real option value and fall back to the tag if the lookup
            # yields nothing.
            iso_code = _iso_code_from(country_code, country_tag)
            option_value = _country_field_option_value(
                field_id,
                iso_code,
                brand_code,
                auth,
                base_url,
            )
            if option_value or country_tag:
                custom_fields.append(
                    {"id": field_id, "value": option_value or country_tag}
                )

    ticket_update["custom_fields"] = custom_fields

    ticket_url = f"{base_url}/tickets/{int(ticket_id)}.json"

    # This update carries no comment: re-sending the same status/custom-field
    # values after a dropped connection is idempotent.
    response = zendesk_request(
        "PUT",
        ticket_url,
        auth=auth,
        json={"ticket": ticket_update},
        timeout=60,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        error_detail = _zendesk_error_detail(response)

        if response.status_code == 422 and _is_zendesk_closed_ticket_error(error_detail):
            raise ZendeskTicketClosedError(
                f"Zendesk ticket {ticket_id} is closed and cannot be updated "
                f"via the API. Skipping this ticket. Zendesk said: {error_detail}"
            ) from exc

        # Same "Country <BRAND>" / required-field-on-solve situation as
        # submit_ticket_response(): try to resolve unknown required fields
        # from shipment data and retry with the status included. If the
        # field cannot be resolved, fall back to retrying without the status
        # change so mandatory-field values are still saved; ticket is left
        # for manual solving.
        if response.status_code == 422 and _is_zendesk_required_field_for_solve_error(error_detail):
            missing_field_ids = _extract_missing_field_ids(error_detail)
            order_note_value = str(order_note or "-")
            extra_fields = _resolve_missing_fields(
                missing_field_ids,
                order_note_value,
                country_tag=country_tag,
                country_code=country_code,
                auth=auth,
                base_url=base_url,
            )
            if extra_fields:
                # Resolved values override whatever was sent the first time:
                # the original attempt may have carried a constructed country
                # tag that Zendesk does not accept as an option.
                resolved_by_id = {f["id"]: f for f in extra_fields}
                merged_fields = [
                    resolved_by_id.get(f["id"], f)
                    for f in ticket_update.get("custom_fields", [])
                ]
                existing_ids = {f["id"] for f in merged_fields}
                for field in extra_fields:
                    if field["id"] not in existing_ids:
                        merged_fields.append(field)
                retry_update = {**ticket_update, "custom_fields": merged_fields}
                print(
                    f"Zendesk ticket {ticket_id}: retrying solve-without-response with "
                    f"resolved fields {[f['id'] for f in extra_fields]} populated from shipment data."
                )
                retry_response = zendesk_request(
                    "PUT",
                    ticket_url,
                    auth=auth,
                    json={"ticket": retry_update},
                    timeout=60,
                )
                try:
                    retry_response.raise_for_status()
                    return True
                except requests.HTTPError:
                    retry_error_detail = _zendesk_error_detail(retry_response)
                    print(
                        f"Zendesk ticket {ticket_id}: field-injection retry failed "
                        f"({retry_error_detail}). Falling back to status-less update; "
                        "ticket needs manual solving."
                    )

            print(
                f"Zendesk ticket {ticket_id}: cannot set status to 'solved' "
                f"because a required field is missing ({error_detail}). "
                "Retrying without the status change so the mandatory fields "
                "are still saved; ticket needs manual solving."
            )
            retry_update = {k: v for k, v in ticket_update.items() if k != "status"}
            retry_response = zendesk_request(
                "PUT",
                ticket_url,
                auth=auth,
                json={"ticket": retry_update},
                timeout=60,
            )
            try:
                retry_response.raise_for_status()
            except requests.HTTPError as retry_exc:
                retry_error_detail = _zendesk_error_detail(retry_response)
                raise requests.HTTPError(
                    f"{retry_exc} | Zendesk ticket {ticket_id} solve-without-response "
                    f"update failed (even after retrying without status change). "
                    f"Zendesk error detail: {retry_error_detail}",
                    response=retry_response,
                ) from retry_exc
            return True

        raise requests.HTTPError(
            f"{exc} | Zendesk ticket {ticket_id} solve-without-response update failed. "
            f"Zendesk error detail: {error_detail}",
            response=response,
        ) from exc
    return True


class ZendeskTicketClosedError(RuntimeError):
    """Raised when Zendesk refuses an update because the ticket is closed."""


def _zendesk_error_detail(response: requests.Response) -> str:
    """Best-effort extraction of Zendesk's JSON error body for diagnostics."""

    try:
        payload = response.json()
    except ValueError:
        return (response.text or "").strip()[:500]
    return json.dumps(payload)[:1000]


def _is_zendesk_closed_ticket_error(error_detail: str) -> bool:
    """Detect Zendesk's "ticket is closed" validation error from its body."""

    lowered = error_detail.lower()
    return "closed" in lowered and (
        "ticketblockederror" in lowered
        or "prevents ticket update" in lowered
        or "not valid for ticket update" in lowered
    )


def _extract_missing_field_ids(error_detail: str) -> list[int]:
    """Parse Zendesk's 422 error body and return ticket_field_id values listed
    under details.base[].ticket_field_id for fields that are required on solve.

    Example payload fragment:
        {"details": {"base": [{"ticket_field_id": 28946463797660, ...}]}}
    """
    try:
        payload = json.loads(error_detail)
        base_errors = payload.get("details", {}).get("base", [])
        return [
            int(e["ticket_field_id"])
            for e in base_errors
            if e.get("ticket_field_id") is not None
        ]
    except (ValueError, KeyError, TypeError):
        return []


# Map of Zendesk ticket_field_id values that can be auto-populated from
# shipment data available at response-submission time.  Add new entries here
# whenever a brand-specific required-on-solve field is identified.
#
# 28946463797660  – "Amazon Order Number" field: populated with the shipment
#                   order number (already used as the order_note value).
_FIELD_ID_TO_SHIPMENT_DATA: dict[int, str] = {
    28946463797660: "order_note",
}


def _normalize_country_label(value: object) -> str:
    """Fold a country name or option value to a comparable form.

    Lower-cased, accents stripped, every run of non-alphanumerics collapsed
    to a single space: "Côte d\'Ivoire", "cote_d_ivoire" and "COTE D IVOIRE"
    all become "cote d ivoire".
    """

    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return text.strip()


def _option_value_label_variants(value: str, brand_key: str) -> set:
    """Comparable forms of an option *value*, brand decoration removed.

    The live drop-downs store name-shaped values with a brand suffix
    ("united_kingdom_ln", "korea_ln"). Comparing those against the plain
    country name never matched, which is why every country whose option name
    was not spelled exactly like the ISO name fell through to "no option
    matches".
    """

    label = _normalize_country_label(value)
    variants = {label}
    if not label:
        return variants

    parts = label.split(" ")
    # Trailing brand token: "united kingdom ln" -> "united kingdom".
    if brand_key and len(parts) > 1 and parts[-1] == brand_key:
        variants.add(" ".join(parts[:-1]))
    # Leading decoration: "country united kingdom" / "country ln united kingdom".
    if parts[0] == "country":
        rest = parts[1:]
        if brand_key and rest[:1] == [brand_key]:
            rest = rest[1:]
        if rest:
            variants.add(" ".join(rest))
    return {variant for variant in variants if variant}


def split_destination_country_codes(value: object) -> list:
    """Every ISO alpha-2 code in a country column, in order, de-duplicated.

    `full_order_country_code` holds one code per order normally, but an order
    whose shipments go to different places arrives as "CH;GB". Treating that
    as a single code matched nothing and left the Zendesk country field empty,
    which then made the solve fail.
    """

    codes = []
    for part in re.split(r"[^A-Za-z]+", str(value or "")):
        code = part.strip().upper()
        if len(code) == 2 and code not in codes:
            codes.append(code)
    return codes


def _country_field_option_value(
    field_id: int,
    iso_code: str,
    brand: str,
    auth,
    base_url: str,
) -> str | None:
    """Return the option value the Zendesk country drop-down actually accepts.

    "Country <BRAND>" is a FieldTagger: Zendesk only stores values that exist
    in the field\'s own ``custom_field_options`` list. A constructed tag such
    as ``country_dj_us`` is silently discarded when it is not one of those
    options, which leaves the field empty and makes Zendesk reject the solve
    with "Country <BRAND>: is required when solving a ticket" even though we
    did send a value. So the real option list is fetched (and cached) and the
    ISO code is matched against it.

    ``iso_code`` may carry several codes ("CH;GB") when an order ships to more
    than one country; each is tried in order and the first that matches an
    option wins.

    Returns None when no option matches; the caller then falls back to
    posting the comment without the status change, as before. The first
    option is deliberately never used as a fallback: that would file the
    ticket under an arbitrary wrong country.
    """
    from customs_rules import country_names_for_iso2

    codes = split_destination_country_codes(iso_code)
    if not codes:
        return None
    brand_key = str(brand or "").strip().lower()

    try:
        field = _zendesk_ticket_field(int(field_id), auth, base_url)
    except Exception as exc:  # noqa: BLE001 - diagnostics only; the caller
        # still falls back to the comment-only retry.
        print(f"Could not read Zendesk ticket field {field_id}: {exc}")
        return None

    options = field.get("custom_field_options") or []
    if not options:
        return None

    def _value(option) -> str:
        return str(option.get("value") or "").strip()

    def _name(option) -> str:
        return str(option.get("name") or "").strip()

    for iso in (code.lower() for code in codes):
        # 1. Exact value match against the shapes we know are in use.
        exact_candidates = {f"country_{iso}", iso}
        if brand_key:
            exact_candidates.update({f"country_{brand_key}_{iso}", f"{brand_key}_{iso}", f"{iso}_{brand_key}"})
        for option in options:
            if _value(option).lower() in exact_candidates:
                return _value(option)

        # 2. Any option value whose last meaningful segment is the ISO code
        #    (e.g. "dj::country_us", "country-dj-us", "us_ln").
        for option in options:
            segments = [part for part in re.split(r"[^a-z0-9]+", _value(option).lower()) if part]
            if not segments:
                continue
            if segments[-1] == iso:
                return _value(option)
            if brand_key and len(segments) > 1 and segments[-1] == brand_key and segments[-2] == iso:
                return _value(option)

        # 3. Match on the human-readable option name, or on a name-shaped
        #    value once its brand decoration is stripped.
        wanted = {_normalize_country_label(iso)}
        for country_name in country_names_for_iso2(iso):
            wanted.add(_normalize_country_label(country_name))
        wanted.discard("")

        for option in options:
            if _normalize_country_label(_name(option)) in wanted:
                return _value(option)
            if _option_value_label_variants(_value(option), brand_key) & wanted:
                return _value(option)

    sample = ", ".join(
        f"{_name(option)!r}={_value(option)!r}" for option in options[:10]
    )
    print(
        f"Zendesk ticket field {field_id}: no option matches country "
        f"{'/'.join(codes)} (brand {brand_key.upper() or 'unknown'}). "
        f"First options: {sample}"
    )
    return None


def _resolve_missing_fields(
    field_ids: list[int],
    order_note: str,
    *,
    country_tag: str | None = None,
    country_code: str | None = None,
    auth=None,
    base_url: str | None = None,
) -> list[dict[str, Any]]:
    """Return custom-field dicts for every field_id we know how to populate.

    Fields whose IDs are neither in ``_FIELD_ID_TO_SHIPMENT_DATA`` nor a known
    per-brand "Country <BRAND>" field are silently skipped; the caller falls
    back to a comment-only retry for those.
    """
    from customs_rules import brand_for_country_ticket_field_id

    resolved = []
    context = {"order_note": order_note}
    for fid in field_ids:
        data_key = _FIELD_ID_TO_SHIPMENT_DATA.get(fid)
        if data_key and context.get(data_key):
            resolved.append({"id": fid, "value": context[data_key]})
            continue

        brand = brand_for_country_ticket_field_id(fid)
        if not brand:
            continue

        iso = _iso_code_from(country_code, country_tag)
        if not iso:
            continue

        if auth is not None and base_url:
            option_value = _country_field_option_value(fid, iso, brand, auth, base_url)
            if option_value:
                resolved.append({"id": fid, "value": option_value})
            continue

        # No API access to the option list: fall back to the constructed tag.
        resolved.append({"id": fid, "value": f"country_{brand.lower()}_{iso}"})
    return resolved


def _is_zendesk_required_field_for_solve_error(error_detail: str) -> bool:
    """Detect Zendesk's "<field>: is required when solving a ticket" error.

    This happens when a brand-specific conditional field (e.g. "Country DJ")
    is configured in Zendesk as required-on-solve, but we didn't have a
    value to send for it (unmapped brand, missing shipping country, etc.).
    """

    lowered = error_detail.lower()
    return "is required when solving a ticket" in lowered


def assign_unhandled_tickets_to_group_enabled() -> bool:
    return _env_bool(
        ASSIGN_UNHANDLED_TICKETS_TO_GROUP_ENV,
        DEFAULT_ASSIGN_UNHANDLED_TICKETS_TO_GROUP,
    )


_ESCALATION_GROUP_ID_CACHE: int | None = None


def resolve_escalation_group_id(
    *,
    auth,
    base_url: str,
    session: requests.Session | None = None,
) -> int | None:
    """Resolve ZENDESK_ESCALATION_GROUP_NAME to a Zendesk group id, cached.

    Same approach as assign_tickets_to_group.py: an explicit id wins, otherwise
    the group list is paged through and matched on name, case-insensitively.
    Returns None (and logs the available group names) when nothing matches, so
    an unknown group name degrades to "no assignment" instead of an exception.
    """

    global _ESCALATION_GROUP_ID_CACHE

    if _ESCALATION_GROUP_ID_CACHE is not None:
        return _ESCALATION_GROUP_ID_CACHE

    configured_id = os.getenv(ZENDESK_ESCALATION_GROUP_ID_ENV, "").strip()
    if configured_id:
        try:
            _ESCALATION_GROUP_ID_CACHE = int(configured_id)
            return _ESCALATION_GROUP_ID_CACHE
        except ValueError:
            print(
                f"Ignoring non-numeric {ZENDESK_ESCALATION_GROUP_ID_ENV}="
                f"{configured_id!r}; resolving the group by name instead."
            )

    wanted = ZENDESK_ESCALATION_GROUP_NAME.strip().casefold()
    if not wanted:
        return None

    url = f"{base_url}/groups.json?page[size]=100"
    names: list[str] = []

    while url:
        try:
            payload = zendesk_get(url, auth=auth, session=session, timeout=60).json() or {}
        except Exception as exc:  # noqa: BLE001 - never abort the run over this
            print(f"Could not list Zendesk groups: {exc}")
            return None

        for group in payload.get("groups") or []:
            name = str(group.get("name") or "").strip()
            names.append(name)
            if name.casefold() == wanted:
                print(f"Zendesk escalation group {name!r} -> id {group['id']}")
                _ESCALATION_GROUP_ID_CACHE = int(group["id"])
                return _ESCALATION_GROUP_ID_CACHE

        meta = payload.get("meta") or {}
        links = payload.get("links") or {}
        next_url = links.get("next") if _zendesk_meta_has_more(meta) else payload.get("next_page")
        url = _absolute_zendesk_url(base_url, next_url) if next_url else None

    print(
        f"Zendesk group {ZENDESK_ESCALATION_GROUP_NAME!r} not found. "
        "Available groups: " + ", ".join(sorted(name for name in names if name))
    )
    return None


def assign_ticket_to_group(
    ticket_id: int,
    group_id: int,
    *,
    auth,
    base_url: str,
    session: requests.Session | None = None,
) -> bool:
    """Move one ticket to a group, leaving its status untouched.

    Mirrors assign_tickets_to_group.py: the group is set and (by default) the
    individual assignee is cleared so the ticket sits with the group unclaimed.
    The status is deliberately not changed -- these are exactly the tickets the
    workflow did not resolve, so they must stay actionable for a human. Already
    closed/archived tickets are skipped.
    """

    try:
        current = zendesk_get(
            f"{base_url}/tickets/{ticket_id}.json",
            auth=auth,
            session=session,
            timeout=60,
        ).json() or {}
    except Exception as exc:  # noqa: BLE001
        print(f"Could not read Zendesk ticket {ticket_id} before assignment: {exc}")
        return False

    ticket = current.get("ticket") or {}
    if not ticket:
        print(f"Zendesk ticket {ticket_id} not found; skipped group assignment.")
        return False

    if str(ticket.get("status") or "").strip().lower() in {"closed", "archived"}:
        print(f"Zendesk ticket {ticket_id} is closed; skipped group assignment.")
        return False

    if _to_int(ticket.get("group_id")) == int(group_id):
        # Already with the group; nothing to do, and re-sending would only add
        # a pointless audit event.
        return True

    fields: dict[str, Any] = {"group_id": int(group_id)}
    if _env_bool(ZENDESK_ESCALATION_CLEAR_ASSIGNEE_ENV, True):
        fields["assignee_id"] = None

    response = zendesk_request(
        "PUT",
        f"{base_url}/tickets/{ticket_id}.json",
        auth=auth,
        session=session,
        json={"ticket": fields},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )

    if not response.ok:
        print(
            f"Failed to assign Zendesk ticket {ticket_id} to group {group_id}: "
            f"{_zendesk_error_detail(response)}"
        )
        return False

    return True


def assign_unhandled_tickets_to_escalation_group(
    rows: Iterable[Mapping[str, Any]],
    *,
    auth,
    base_url: str,
    outcomes: dict[str, str] | None = None,
) -> int:
    """Assign every unresolved ticket to the second-level logistics group."""

    rows = list(rows)
    if not rows:
        return 0

    if not assign_unhandled_tickets_to_group_enabled():
        print(
            f"{ASSIGN_UNHANDLED_TICKETS_TO_GROUP_ENV}=false; skipped "
            f"{len(rows)} unresolved ticket(s) that would have been assigned to "
            f"{ZENDESK_ESCALATION_GROUP_NAME!r}."
        )
        return 0

    assigned = 0

    with requests.Session() as session:
        group_id = resolve_escalation_group_id(
            auth=auth,
            base_url=base_url,
            session=session,
        )
        if group_id is None:
            print(
                "No Zendesk escalation group id available; "
                f"{len(rows)} unresolved ticket(s) were left untouched."
            )
            return 0

        seen_ticket_ids: set[int] = set()

        for row in rows:
            ticket_id = _to_int(row.get("zendesk_ticket_id"))
            if ticket_id is None:
                continue

            if ticket_id in seen_ticket_ids:
                # Several requests can belong to the same ticket.
                if outcomes is not None:
                    outcomes[_outcome_key(row)] = SUBMISSION_STATUS_ASSIGNED_TO_GROUP
                continue
            seen_ticket_ids.add(ticket_id)

            try:
                if assign_ticket_to_group(
                    ticket_id,
                    group_id,
                    auth=auth,
                    base_url=base_url,
                    session=session,
                ):
                    assigned += 1
                    if outcomes is not None:
                        outcomes[_outcome_key(row)] = SUBMISSION_STATUS_ASSIGNED_TO_GROUP
            except Exception as exc:  # noqa: BLE001 - one bad ticket must not
                # stop the rest of the batch.
                print(f"Failed to assign Zendesk ticket {ticket_id} to a group: {exc}")

    print(
        f"Assigned {assigned} unresolved ticket(s) to Zendesk group "
        f"{ZENDESK_ESCALATION_GROUP_NAME!r}."
    )
    return assigned


def _outcome_key(row: Mapping[str, Any]) -> str:
    return _to_str(row.get("request_id")) or build_request_id(
        _to_int(row.get("zendesk_ticket_id")),
        _to_int(row.get("request_number")),
    ) or ""


def submit_final_responses(
    rows: Iterable[Mapping[str, Any]] | pd.DataFrame,
    outcomes: dict[str, str] | None = None,
) -> int:
    """Submit non-empty final_response values to Zendesk when enabled.

    When ``outcomes`` is given it is filled with request_id -> submission
    status (see SUBMISSION_STATUS_*) for every row that was a submission
    candidate, so the caller can log what really happened in Zendesk.

    Human-intervention rows and blank final responses are intentionally skipped.
    The actual public Zendesk side effect is controlled by the
    SUBMIT_ZENDESK_RESPONSES environment/workflow flag. Set it to true to
    submit; false or an unset value logs only and performs no ticket update.
    """

    if isinstance(rows, pd.DataFrame):
        raw_rows = rows.to_dict(orient="records")
    else:
        raw_rows = list(rows)

    response_rows = []
    solve_without_response_rows = []
    # Rows the workflow takes no Zendesk action on at all. They are handed to
    # the second-level logistics group at the end of this function.
    unhandled_rows: list[Mapping[str, Any]] = []
    blocked_rows: list[Mapping[str, Any]] = []

    for row in raw_rows:
        requester_email = row.get("requester_email")

        # Request number 1 from a noreply*/autoreply* sender: solved with no
        # public comment and no analysis. Checked before every other rule,
        # including the blocked order prefix below, for the same reason
        # response_generator._build_response_body() checks it first: the
        # request was never analyzed, so nothing about the order it happens
        # to reference changes the outcome. An automated FedEx delivery
        # notification whose order sits on LN-EU/RC-EU used to be caught by
        # the blocked-prefix branch and handed to the logistics group, even
        # though its own draft already said the ticket had been solved
        # without a response.
        if is_noreply_autoreply_first_request(requester_email, row.get("request_number", 1)):
            ticket_id = _to_int(row.get("zendesk_ticket_id"))
            if ticket_id is not None:
                solve_without_response_rows.append((ticket_id, row))
            continue

        # RC-US / LN-US are never answered. RC-EU / LN-EU are answered
        # normally unless the GET_FULL_ORDER destination country is US or GB
        # (or could not be determined at all). Either way the ticket is not
        # dropped: like every other request the workflow cannot serve, it is
        # handed to the second-level logistics group.
        if row_order_processing_blocked_final(row):
            blocked_rows.append(row)
            unhandled_rows.append(row)
            continue

        # noreply@fedex.com is a special case: FedEx's automated mailbox
        # never reads replies, so instead of leaving the ticket untouched
        # (the general noreply* behavior below) the agent solves it with no
        # public comment at all. This must be checked before the general
        # is_noreply_requester_email() skip, which would otherwise silently
        # swallow it with no ticket update at all.
        if is_noreply_fedex_requester_email(requester_email):
            ticket_id = _to_int(row.get("zendesk_ticket_id"))
            if ticket_id is not None:
                solve_without_response_rows.append((ticket_id, row))
            continue

        # The message was classified (by regex or LLM) as purely informative,
        # with no data or action requested. This applies regardless of
        # request number, and regardless of whether the tracking number was
        # found in the ticket or in shipping_platform_shipments. Treat this
        # the same way as noreply@fedex.com above: solve the ticket with no
        # public comment, instead of leaving it for human review. Must be
        # checked before the general noreply*/human intervention skips
        # below, which would otherwise silently swallow it with no ticket
        # update at all.
        if normalize_requested_data(row.get("requested_data")) == [PURELY_INFORMATIVE_SOLVED]:
            ticket_id = _to_int(row.get("zendesk_ticket_id"))
            if ticket_id is not None:
                solve_without_response_rows.append((ticket_id, row))
            continue

        # The tracking number could not be extracted from the ticket, but the
        # message is purely informative with no data or action requested.
        # Treat this the same way as noreply@fedex.com above: solve the
        # ticket with no public comment, instead of leaving it for human
        # review. Must be checked before the general noreply*/human
        # intervention skips below, which would otherwise silently swallow
        # it with no ticket update at all.
        if is_missing_extracted_tracking_number(
            row.get("extracted_tracking_number")
        ) and is_purely_informative_request(
            "\n".join(
                str(row.get(column) or "")
                for column in ("subject", "cleaned_request_body", "request_body")
            )
        ):
            ticket_id = _to_int(row.get("zendesk_ticket_id"))
            if ticket_id is not None:
                solve_without_response_rows.append((ticket_id, row))
            continue

        if is_noreply_requester_email(requester_email):
            # No public reply is ever sent to an automated mailbox, and the
            # ticket is not solved either: hand it to the logistics group.
            unhandled_rows.append(row)
            continue
        if bool(_to_bool(row.get("human_intervention_required"))):
            unhandled_rows.append(row)
            continue
        request_text = "\n".join(
            str(row.get(column) or "")
            for column in ("subject", "cleaned_request_body", "request_body")
        )
        if is_no_action_carrier_notification(request_text):
            # No actionable customer-data request detected: solve the ticket
            # with no public comment, same as noreply@fedex.com above.
            ticket_id = _to_int(row.get("zendesk_ticket_id"))
            if ticket_id is not None:
                solve_without_response_rows.append((ticket_id, row))
            continue
        final_response = strip_links_from_public_response(row.get("final_response"))
        if _response_is_blank(final_response):
            # Nothing was found to answer with and nothing will be posted:
            # hand the ticket to the logistics group.
            unhandled_rows.append(row)
            continue
        ticket_id = _to_int(row.get("zendesk_ticket_id"))
        if ticket_id is None:
            raise ValueError(
                "Cannot submit Zendesk response without zendesk_ticket_id "
                f"for request_id={row.get('request_id')}"
            )
        response_rows.append((ticket_id, str(final_response).strip(), row))

    if outcomes is None:
        outcomes = {}

    # No outcome is forced for blocked rows any more: they go through the
    # escalation path below like any other unserved request, which records
    # SUBMISSION_STATUS_ASSIGNED_TO_GROUP once the group assignment lands.
    if blocked_rows:
        always_blocked = sum(
            1
            for row in blocked_rows
            if contains_always_blocked_order_reference(
                row.get("shipment_order_number"),
                row.get("extracted_order_reference"),
            )
        )
        print(
            f"{len(blocked_rows)} request(s) not processed: "
            f"{always_blocked} on an always-blocked prefix "
            f"({', '.join(ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIXES)}) and "
            f"{len(blocked_rows) - always_blocked} on "
            f"{'/'.join(DESTINATION_CHECKED_ORDER_REFERENCE_PREFIXES)} shipping "
            f"to {'/'.join(sorted(BLOCKED_DESTINATION_COUNTRY_CODES))} or with no "
            f"known destination. No reply, no solve, assigned to "
            f"{ZENDESK_ESCALATION_GROUP_NAME!r}."
        )

    if not response_rows and not solve_without_response_rows:
        print("No Zendesk final responses to submit.")
        if unhandled_rows and zendesk_response_submission_enabled():
            assign_unhandled_tickets_to_escalation_group(
                unhandled_rows,
                auth=zendesk_auth(),
                base_url=zendesk_base_url(),
                outcomes=outcomes,
            )
        elif unhandled_rows:
            print(
                f"{ZENDESK_RESPONSE_SUBMISSION_ENV}=false; skipped group assignment "
                f"for {len(unhandled_rows)} unresolved ticket(s)."
            )
        return 0

    if not zendesk_response_submission_enabled():
        for _ticket_id, row in solve_without_response_rows:
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_DRY_RUN
        for _ticket_id, _body, row in response_rows:
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_DRY_RUN
        print(
            f"{ZENDESK_RESPONSE_SUBMISSION_ENV}=false; skipped "
            f"{len(response_rows)} Zendesk response(s), "
            f"{len(solve_without_response_rows)} solve-without-response ticket(s) "
            "(noreply/autoreply first request / FedEx noreply / informative), and "
            f"group assignment for {len(unhandled_rows)} unresolved ticket(s)."
        )
        return 0

    auth = zendesk_auth()
    base_url = zendesk_base_url()
    submitted = 0
    failed: list[tuple[int, str]] = []

    # Solve-without-response tickets: FedEx noreply rows, and rows where the
    # tracking number could not be extracted but the message is purely
    # informative. Move straight to solved, no public comment. Zendesk still
    # requires Channel/Reason of contact/Order Number to solve a ticket even
    # with no comment, so these are filled in the same way as for a normal
    # automatic reply.
    for ticket_id, row in solve_without_response_rows:
        try:
            if submit_ticket_solved_without_response(
                ticket_id,
                reason_of_contact=str(
                    row.get('reason_of_contact')
                    or row.get('contact_reason')
                    or row.get('zendesk_reason_of_contact')
                    or 'Altro'
                ),
                order_note=str(row.get('shipment_order_number') or row.get('order_number') or '-'),
                country_tag=row.get('zendesk_country_tag') or None,
                # Used only if Zendesk rejects the solve with a missing
                # "Country <BRAND>" field: the 422 names the field id, which
                # gives the brand, and this gives the ISO country code.
                country_code=row.get('full_order_country_code') or None,
                auth=auth,
                base_url=base_url,
            ):
                submitted += 1
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_SOLVED_WITHOUT_RESPONSE
        except ZendeskTicketClosedError as exc:
            print(f"Skipped Zendesk ticket {ticket_id}: {exc}")
            # A closed ticket can never be updated again: nothing left to do.
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_ALREADY_PRESENT
            failed.append((ticket_id, str(exc)))
        except Exception as exc:  # noqa: BLE001 - one bad ticket must not
            # abort the rest of the batch.
            print(f"Failed to solve-without-response Zendesk ticket {ticket_id}: {exc}")
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_FAILED
            failed.append((ticket_id, str(exc)))

    for ticket_id, final_response, row in response_rows:
        attachment_paths = _attachment_paths(row.get("zendesk_attachment_paths"))
        try:
            if submit_ticket_response(
                ticket_id,
                final_response,
                attachment_paths=attachment_paths,
                reason_of_contact=str(row.get('reason_of_contact') or row.get('contact_reason') or row.get('zendesk_reason_of_contact') or 'Altro'),
                order_note=str(row.get('shipment_order_number') or row.get('order_number') or '-'),
                country_tag=row.get('zendesk_country_tag') or None,
                # Used only if Zendesk rejects the solve with a missing
                # "Country <BRAND>" field: the 422 names the field id, which
                # gives the brand, and this gives the ISO country code.
                country_code=row.get('full_order_country_code') or None,
                auth=auth,
                base_url=base_url,
            ):
                submitted += 1
                outcomes[_outcome_key(row)] = SUBMISSION_STATUS_SUBMITTED
            else:
                # submit_ticket_response() returns False when the exact same
                # public comment is already on the ticket.
                outcomes[_outcome_key(row)] = SUBMISSION_STATUS_ALREADY_PRESENT
        except ZendeskTicketClosedError as exc:
            # Already-closed tickets can never be fixed by retrying this
            # same request, so log it and keep going with the rest of the
            # batch instead of aborting every remaining submission.
            print(f"Skipped Zendesk ticket {ticket_id}: {exc}")
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_ALREADY_PRESENT
            failed.append((ticket_id, str(exc)))
        except Exception as exc:  # noqa: BLE001 - intentionally broad: one
            # bad ticket (network blip, validation error, etc.) must not
            # prevent the rest of the batch from being submitted.
            print(f"Failed to submit Zendesk ticket {ticket_id}: {exc}")
            outcomes[_outcome_key(row)] = SUBMISSION_STATUS_FAILED
            failed.append((ticket_id, str(exc)))

    print(f"Submitted {submitted} Zendesk final response(s).")

    # Tickets whose submission failed also end up with no action applied, so
    # they are escalated alongside the rows that never produced a response.
    failed_ticket_ids = {ticket_id for ticket_id, _ in failed}
    for _ticket_id, row in solve_without_response_rows:
        if _to_int(row.get("zendesk_ticket_id")) in failed_ticket_ids:
            unhandled_rows.append(row)
    for _ticket_id, _body, row in response_rows:
        if _to_int(row.get("zendesk_ticket_id")) in failed_ticket_ids:
            unhandled_rows.append(row)

    assign_unhandled_tickets_to_escalation_group(
        unhandled_rows,
        auth=auth,
        base_url=base_url,
        outcomes=outcomes,
    )

    if failed:
        failed_ids = ", ".join(str(ticket_id) for ticket_id, _ in failed)
        print(
            f"WARNING: {len(failed)} Zendesk response(s) failed to submit "
            f"(ticket id(s): {failed_ids}). See messages above for details."
        )
        total_attempted = len(response_rows) + len(solve_without_response_rows)
        raise RuntimeError(
            f"{len(failed)} of {total_attempted} Zendesk response(s)/solve(s) failed "
            f"to submit after submitting {submitted} successfully. "
            f"Failed ticket id(s): {failed_ids}."
        )

    return submitted


# ============================================================================
# ZENDESK FETCH HELPERS
# ============================================================================


def load_requester_configuration(client, bigquery) -> dict[str, str]:
    """Load exact requester-email to ticket_category overrides from BigQuery.

    The fetcher no longer limits Zendesk retrieval to these emails. They are
    still authoritative: when a carrier-domain requester exists in this config
    table, its ticket_category is taken from BigQuery exactly as before.
    """

    print("Loading requester configuration table...")

    config_query = f"""
    SELECT DISTINCT
        LOWER(TRIM(requester_email)) AS requester_email,
        ticket_category
    FROM `{CONFIG_TABLE}`
    WHERE ticket_category IN UNNEST(@categories)
      AND requester_email IS NOT NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "categories",
                "STRING",
                VALID_CATEGORIES,
            )
        ]
    )

    config_df = (
        client.query(config_query, job_config=job_config)
        .to_dataframe(create_bqstorage_client=BQ_STORAGE)
    )

    if config_df.empty:
        raise RuntimeError("No requester emails found in configuration table.")

    email_to_category: dict[str, str] = {}
    for _, row in config_df.iterrows():
        requester_email = normalize_email(row.get("requester_email"))
        ticket_category = str(row.get("ticket_category") or "").strip()
        if requester_email and ticket_category:
            email_to_category[requester_email] = ticket_category

    print(
        f"Loaded {len(email_to_category)} exact requester-email category overrides."
    )
    return email_to_category


def get_ticket_comments(
    ticket_id,
    *,
    auth,
    base_url: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Retrieve all comments for a Zendesk ticket."""

    comments: list[dict[str, Any]] = []
    url = f"{base_url}/tickets/{ticket_id}/comments.json"

    while url:
        response = zendesk_get(url, auth=auth, session=session, timeout=60)

        payload = response.json()
        comments.extend(payload.get("comments", []))
        url = payload.get("next_page")

    return comments


def get_zendesk_user_email(
    user_id: object,
    *,
    auth,
    base_url: str,
    cache: dict[int, str] | None = None,
    session: requests.Session | None = None,
) -> str:
    """Return a Zendesk user's normalized email address, cached per run."""

    if user_id is None:
        return ""

    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return ""

    if cache is not None and user_id_int in cache:
        return cache[user_id_int]

    response = zendesk_get(
        f"{base_url}/users/{user_id_int}.json",
        auth=auth,
        session=session,
        timeout=60,
    )
    requester_email = normalize_email(response.json().get("user", {}).get("email"))

    if cache is not None:
        cache[user_id_int] = requester_email

    return requester_email


def requester_email_from_ticket(
    ticket: Mapping[str, Any],
    *,
    auth,
    base_url: str,
    user_email_cache: dict[int, str],
    session: requests.Session | None = None,
) -> str:
    """Extract the requester email used for domain filtering and config lookup.

    Zendesk search results sometimes expose the email in via.source.from.address;
    when that is absent or not a carrier-domain address, fall back to the
    requester user record.
    """

    via_email = normalize_email(
        ticket.get("via", {})
        .get("source", {})
        .get("from", {})
        .get("address", "")
    )
    if email_matches_any_carrier_domain(via_email):
        return via_email

    requester_email = get_zendesk_user_email(
        ticket.get("requester_id"),
        auth=auth,
        base_url=base_url,
        cache=user_email_cache,
        session=session,
    )
    if requester_email:
        return requester_email

    return via_email


def _zendesk_user_fetch_batch_size() -> int:
    return _env_int(
        ZENDESK_USER_FETCH_BATCH_SIZE_ENV,
        DEFAULT_ZENDESK_USER_FETCH_BATCH_SIZE,
        minimum=1,
        maximum=100,
    )


def get_zendesk_users_by_ids(
    user_ids: Iterable[object],
    *,
    auth,
    base_url: str,
    cache: dict[int, str],
    session: requests.Session | None = None,
) -> dict[int, str]:
    """Batch-load Zendesk requester emails into cache using users/show_many."""

    clean_user_ids: list[int] = []
    for user_id in user_ids:
        try:
            user_id_int = int(user_id)
        except (TypeError, ValueError):
            continue
        if user_id_int not in cache and user_id_int not in clean_user_ids:
            clean_user_ids.append(user_id_int)

    if not clean_user_ids:
        return cache

    base_url = base_url.rstrip("/")
    batch_size = _zendesk_user_fetch_batch_size()

    for batch in _chunked(clean_user_ids, batch_size):
        response = zendesk_get(
            f"{base_url}/users/show_many.json",
            auth=auth,
            session=session,
            params={"ids": ",".join(str(user_id) for user_id in batch)},
            timeout=60,
        )
        payload = response.json()
        returned_ids: set[int] = set()

        for user in payload.get("users", []):
            try:
                user_id_int = int(user.get("id"))
            except (TypeError, ValueError):
                continue
            returned_ids.add(user_id_int)
            cache[user_id_int] = normalize_email(user.get("email"))

        for missing_user_id in set(batch) - returned_ids:
            cache.setdefault(missing_user_id, "")

    return cache


def _ticket_via_source_email(ticket: Mapping[str, Any]) -> str:
    return normalize_email(
        ticket.get("via", {})
        .get("source", {})
        .get("from", {})
        .get("address", "")
    )


def requester_email_from_ticket_cache(
    ticket: Mapping[str, Any],
    *,
    user_email_cache: Mapping[int, str],
) -> str:
    """Return requester email from via.source first, then batched user cache."""

    via_email = _ticket_via_source_email(ticket)
    if email_matches_any_carrier_domain(via_email):
        return via_email

    try:
        requester_id = int(ticket.get("requester_id"))
    except (TypeError, ValueError):
        requester_id = None

    requester_email = user_email_cache.get(requester_id, "") if requester_id else ""
    return requester_email or via_email


def _zendesk_comment_fetch_workers(ticket_count: int) -> int:
    if ticket_count <= 0:
        return 0
    configured = _env_int(
        ZENDESK_COMMENT_FETCH_WORKERS_ENV,
        DEFAULT_ZENDESK_COMMENT_FETCH_WORKERS,
        minimum=1,
        maximum=50,
    )
    return min(configured, ticket_count)


def get_comments_for_ticket(
    ticket: Mapping[str, Any],
    *,
    auth,
    base_url: str,
) -> dict[str, Any]:
    """Fetch comments for one ticket; safe for ThreadPoolExecutor workers."""

    ticket_id = ticket.get("id")
    try:
        with requests.Session() as session:
            comments = get_ticket_comments(
                ticket_id,
                auth=auth,
                base_url=base_url,
                session=session,
            )
        return {"ticket_id": int(ticket_id), "comments": comments, "error": None}
    except Exception as exc:
        return {"ticket_id": ticket_id, "comments": [], "error": str(exc)}


def extract_tracking_number(subject, description, carrier_code):
    """Extract the most likely tracking/AWB number for the requester carrier."""

    subject = subject or ""
    description = description or ""
    carrier_code = str(carrier_code or "").strip().upper()

    patterns_by_carrier = {
        "FEDEX": [
            r"\b\d{12}\b",
            r"\b\d{15}\b",
            r"\b(?:\d{20}|\d{22})\b",
        ],
        "UPS": [
            r"\b1Z[0-9A-Z]{16}\b",
            r"\bW\d{10}\b",
        ],
        "DHL": [
            r"\bJJD\d{12,24}\b",
            r"\b\d{10}\b",
        ],
    }

    def matches(pattern):
        return re.findall(
            pattern,
            f"{subject}\n{description}",
            flags=re.IGNORECASE,
        )

    if carrier_code in patterns_by_carrier:
        values: list[str] = []
        for pattern in patterns_by_carrier[carrier_code]:
            values.extend(matches(pattern))
        deduped = list(dict.fromkeys(values))
        return ";".join(deduped) if deduped else "N/A"

    # Conservative fallback for malformed/missing requester emails.
    values = []
    for patterns in patterns_by_carrier.values():
        for pattern in patterns:
            values.extend(matches(pattern))
    deduped = list(dict.fromkeys(values))
    return ";".join(deduped) if deduped else "N/A"


def _dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    """Return non-empty strings once, preserving first-seen order."""

    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _chunked(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _configured_carrier_requester_emails(
    email_to_category: Mapping[str, str],
) -> list[str]:
    """Return configured requester emails that belong to the carrier domains."""

    return _dedupe_preserve_order(
        normalize_email(email)
        for email in email_to_category.keys()
        if email_matches_any_carrier_domain(normalize_email(email))
    )


def _zendesk_config_requester_query_batch_size() -> int:
    raw = str(
        os.getenv(
            ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE_ENV,
            str(DEFAULT_ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE),
        )
        or ""
    ).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE_ENV} must be an integer"
        ) from exc

    # Keep the generated query comfortably below Zendesk's documented
    # 64-word search-query limit, while still batching the 64 known requesters
    # into only a couple of API calls.
    return min(max(value, 1), 50)


def _active_zendesk_search_query(
    *,
    status: str | None = None,
    statuses: Iterable[str] | None = None,
    requester_emails: Iterable[str] = (),
    domain_term: str | None = None,
    include_type_term: bool = False,
) -> str:
    """Build a narrow Zendesk Search Export query for active mail tickets.

    By default the query includes all workflow-active statuses at API level:
    ``status:new status:open status:pending``. Zendesk treats repeated values
    for the same property as OR matches, so this fetches only those three
    statuses and does not fetch solved/closed tickets for local filtering.

    The main fetch path uses Zendesk's cursor-based Search Export endpoint.
    That endpoint requires the object type in the separate filter[type]
    parameter and rejects type:ticket inside the query string, so the default
    query excludes the type term. include_type_term exists only for backward-
    compatible tests/debugging against the regular search endpoint.
    """

    if status is not None and statuses is not None:
        raise ValueError("Pass either status or statuses, not both")

    raw_statuses = [status] if status is not None else list(statuses or ACTIVE_ZENDESK_STATUSES)
    normalized_statuses: list[str] = []
    for raw_status in raw_statuses:
        normalized_status = str(raw_status or "").strip().lower()
        if normalized_status not in ACTIVE_ZENDESK_STATUSES:
            raise ValueError(
                f"Unsupported Zendesk active status {raw_status!r}; use one of: "
                + ", ".join(ACTIVE_ZENDESK_STATUSES)
            )
        if normalized_status not in normalized_statuses:
            normalized_statuses.append(normalized_status)

    search_terms = []
    if include_type_term:
        search_terms.append("type:ticket")

    search_terms.extend(f"status:{value}" for value in normalized_statuses)
    search_terms.append("via:mail")

    requester_terms = _dedupe_preserve_order(
        normalize_email(email) for email in requester_emails
    )
    search_terms.extend(f"requester:{email}" for email in requester_terms)

    normalized_domain = str(domain_term or "").strip().lower().lstrip("@")
    if normalized_domain:
        search_terms.append(normalized_domain)

    lookback_days_raw = str(os.getenv("ZENDESK_ACTIVE_TICKET_LOOKBACK_DAYS", "") or "").strip()
    if lookback_days_raw:
        try:
            lookback_days = int(lookback_days_raw)
        except ValueError as exc:
            raise ValueError(
                "ZENDESK_ACTIVE_TICKET_LOOKBACK_DAYS must be an integer number of days"
            ) from exc
        if lookback_days > 0:
            start_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            search_terms.append(f"created>={start_date.strftime('%Y-%m-%d')}")

    return " ".join(search_terms)


def _zendesk_fetch_query_specs(
    email_to_category: Mapping[str, str],
) -> list[tuple[str, str]]:
    """Build Zendesk Search Export queries for this run.

    Default strategy is one broad active-mail query:
    ``status:new status:open status:pending via:mail``. It is normally faster
    than several domain/full-text searches because requester emails are loaded
    in batched ``users/show_many`` calls and comments are fetched only for the
    matching carrier-domain tickets.

    Set ``ZENDESK_FETCH_STRATEGY=narrow`` only for diagnostics if you need the
    older configured-requester/domain-term search behavior.
    """

    strategy = str(
        os.getenv(ZENDESK_FETCH_STRATEGY_ENV, DEFAULT_ZENDESK_FETCH_STRATEGY)
        or DEFAULT_ZENDESK_FETCH_STRATEGY
    ).strip().lower()

    if strategy in {"", "broad", "broad_active", "single", "single_active"}:
        return [
            (
                "active mail tickets",
                _active_zendesk_search_query(),
            )
        ]

    if strategy not in {"narrow", "domain", "domain_terms"}:
        raise ValueError(
            f"Unsupported {ZENDESK_FETCH_STRATEGY_ENV}={strategy!r}; "
            "use broad_active or narrow"
        )

    specs: list[tuple[str, str]] = []

    if _env_bool(ZENDESK_FETCH_CONFIGURED_REQUESTERS_ENV, True):
        configured_emails = _configured_carrier_requester_emails(email_to_category)
        batches = list(
            _chunked(
                configured_emails,
                _zendesk_config_requester_query_batch_size(),
            )
        )
        for index, batch in enumerate(batches, start=1):
            label = f"configured carrier requesters batch {index}/{len(batches)}"
            specs.append(
                (
                    label,
                    _active_zendesk_search_query(requester_emails=batch),
                )
            )

    if _env_bool(ZENDESK_FETCH_CARRIER_DOMAIN_TERMS_ENV, True):
        for domain in CARRIER_EMAIL_DOMAINS:
            specs.append(
                (
                    f"carrier domain term {domain}",
                    _active_zendesk_search_query(domain_term=domain),
                )
            )

    if _env_bool(ZENDESK_BROAD_ACTIVE_MAIL_FALLBACK_ENV, False):
        specs.append(
            (
                "optional broad active-mail fallback",
                _active_zendesk_search_query(),
            )
        )

    deduped_specs: list[tuple[str, str]] = []
    seen_queries: set[str] = set()
    for label, query in specs:
        if query in seen_queries:
            continue
        seen_queries.add(query)
        deduped_specs.append((label, query))

    if not deduped_specs:
        raise RuntimeError(
            "No Zendesk fetch queries were generated. Enable at least one of "
            f"{ZENDESK_FETCH_CONFIGURED_REQUESTERS_ENV}, "
            f"{ZENDESK_FETCH_CARRIER_DOMAIN_TERMS_ENV}, or "
            f"{ZENDESK_BROAD_ACTIVE_MAIL_FALLBACK_ENV}."
        )

    return deduped_specs


def _zendesk_search_export_url(
    base_url: str,
    query: str,
    *,
    after_cursor: str | None = None,
) -> str:
    """Return a cursor-paginated Search Export URL for tickets."""

    page_size = _env_int(
        ZENDESK_SEARCH_EXPORT_PAGE_SIZE_ENV,
        DEFAULT_ZENDESK_SEARCH_EXPORT_PAGE_SIZE,
        minimum=1,
        maximum=1000,
    )

    # Zendesk allows up to 1000 records per page. Active-ticket searches are not
    # archived-ticket exports, so the workflow defaults to 500 to reduce page
    # round trips while keeping an env override available.

    params = {
        "query": query,
        "filter[type]": "ticket",
        "page[size]": str(page_size),
    }
    if after_cursor:
        params["page[after]"] = after_cursor

    return f"{base_url}/search/export.json?{urlencode(params)}"


def _zendesk_meta_has_more(meta: Mapping[str, Any]) -> bool:
    value = meta.get("has_more")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _absolute_zendesk_url(base_url: str, url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        parsed_base = urlparse(base_url)
        if parsed_base.scheme and parsed_base.netloc:
            return f"{parsed_base.scheme}://{parsed_base.netloc}{url}"
    return url


def iter_zendesk_search_export_tickets(
    *,
    auth,
    base_url: str,
    query: str,
    label: str | None = None,
    session: requests.Session | None = None,
) -> Iterable[list[dict[str, Any]]]:
    """Yield pages from Zendesk's cursor-paginated Search Export endpoint.

    The regular /search.json endpoint is offset-paginated and fails with HTTP
    422 when page 11 is requested at 100 results/page. Search Export avoids
    that 1,000-result ceiling. The caller now keeps queries narrow before
    local validation by including active statuses and carrier/requester terms
    in the Zendesk query itself.
    """

    base_url = base_url.rstrip("/")
    url = _zendesk_search_export_url(base_url, query)
    page = 1
    query_label = label or query

    while url:
        response = zendesk_get(url, auth=auth, session=session, timeout=60)
        payload = response.json()

        tickets = payload.get("results", [])
        print(
            f"Found {len(tickets)} ticket(s) on Zendesk Search Export page {page} "
            f"for {query_label!r}."
        )
        yield tickets

        links = payload.get("links") or {}
        meta = payload.get("meta") or {}
        next_url = links.get("next")
        if _zendesk_meta_has_more(meta):
            if next_url:
                url = _absolute_zendesk_url(base_url, str(next_url))
            elif meta.get("after_cursor"):
                url = _zendesk_search_export_url(
                    base_url,
                    query,
                    after_cursor=str(meta["after_cursor"]),
                )
            else:
                raise RuntimeError(
                    "Zendesk Search Export indicated more results but did not "
                    "return links.next or meta.after_cursor."
                )
            page += 1
        else:
            url = None


def _ticket_overfetch_multiplier() -> int:
    return _env_int(
        TICKET_OVERFETCH_MULTIPLIER_ENV,
        DEFAULT_TICKET_OVERFETCH_MULTIPLIER,
        minimum=1,
        maximum=100,
    )


def _ticket_age_sort_key(ticket: Mapping[str, Any]) -> tuple[int, int, int]:
    """Sort key putting the oldest ticket first, unknown ages last."""

    created_at = pd.to_datetime(ticket.get("created_at"), utc=True, errors="coerce")
    ticket_id = _to_int(ticket.get("id")) or 0
    if pd.isna(created_at):
        return (1, 0, ticket_id)
    return (0, int(created_at.value), ticket_id)


def limit_tickets_for_comment_fetch(
    tickets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Trim the ticket list before the expensive per-ticket comment fetch.

    Comments are one rate-limited Zendesk call per ticket and are the reason a
    run spends minutes sleeping on 429s, so fetching them for tickets that will
    be discarded by the MAX_TICKETS_PER_RUN cap is pure waste.

    The budget is a multiple of that cap rather than the cap itself, because
    completed requests can only be recognised after their comments are read.
    With a multiplier of N, a run still makes progress as long as fewer than
    (N-1)/N of the oldest selected tickets are already finished.
    """

    budget = _max_tickets_per_run() * _ticket_overfetch_multiplier()
    if len(tickets) <= budget:
        return tickets

    ordered = sorted(tickets, key=_ticket_age_sort_key)
    kept = ordered[:budget]
    print(
        f"Fetching comments for the {len(kept)} oldest of {len(tickets)} "
        f"matched ticket(s) ({_max_tickets_per_run()} cap x "
        f"{_ticket_overfetch_multiplier()} over-fetch); the rest wait for the "
        f"next run. Set {TICKET_OVERFETCH_MULTIPLIER_ENV} to widen the margin "
        "if too many of these turn out to be already completed."
    )
    return kept


def fetch_ticket_rows(
    *,
    client,
    bigquery,
    auth,
    base_url: str,
    email_to_category: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Fetch new/open/pending Zendesk tickets from UPS/DHL/FedEx domains.

    The slow network operations are deliberately batched/parallelized:
    requester users are loaded with users/show_many, and comments are fetched
    concurrently only for tickets whose requester email matches a carrier
    domain. Exact requester emails found in BigQuery keep their configured
    ticket_category; unconfigured carrier-domain requesters are classified from
    the current requester message.
    """

    rows: list[dict[str, Any]] = []
    seen_ticket_ids: set[int] = set()
    # Phase A accumulates across every page/query; comments are fetched once,
    # in phase B, for the subset this run will actually process.
    all_matching_tickets: list[dict[str, Any]] = []
    requester_email_by_ticket_id: dict[int, str] = {}
    user_email_cache: dict[int, str] = {}
    skipped_non_carrier = 0
    skipped_unexpected_status = 0
    skipped_escalation_group = 0
    comment_error_count = 0

    # Tickets already assigned to the escalation group are waiting on a human,
    # so the agent must not touch them again -- and must not log them either.
    escalation_group_id: int | None = None
    if _env_bool(
        SKIP_TICKETS_IN_ESCALATION_GROUP_ENV,
        DEFAULT_SKIP_TICKETS_IN_ESCALATION_GROUP,
    ):
        escalation_group_id = resolve_escalation_group_id(auth=auth, base_url=base_url)
        if escalation_group_id is None:
            print(
                "Could not resolve the escalation group id; tickets already "
                "assigned to it cannot be filtered out this run."
            )
        else:
            print(
                f"Tickets already in group {ZENDESK_ESCALATION_GROUP_NAME!r} "
                f"(id {escalation_group_id}) will be skipped entirely."
            )

    query_specs = _zendesk_fetch_query_specs(email_to_category)

    print(
        "Fetching Zendesk mail tickets with API-level active-status filters: "
        + ", ".join(ACTIVE_ZENDESK_STATUSES)
    )
    print(
        "Default fetch strategy uses a single active-mail Zendesk query, then "
        "batch-loads requester users and filters carrier domains locally: "
        + ", ".join(CARRIER_EMAIL_DOMAINS)
    )
    print(
        f"Prepared {len(query_specs)} Zendesk Search Export query/queries "
        f"using {ZENDESK_FETCH_STRATEGY_ENV}="
        f"{os.getenv(ZENDESK_FETCH_STRATEGY_ENV, DEFAULT_ZENDESK_FETCH_STRATEGY)}."
    )

    with requests.Session() as session:
        for query_label, query in query_specs:
            print(f"Fetching {query_label} with query: {query!r}")

            for tickets in iter_zendesk_search_export_tickets(
                auth=auth,
                base_url=base_url,
                query=query,
                label=query_label,
                session=session,
            ):
                candidate_tickets: list[dict[str, Any]] = []

                for ticket in tickets:
                    try:
                        ticket_id = int(ticket["id"])
                    except Exception:
                        print(f"Skipped Zendesk result without numeric id: {ticket!r}")
                        continue

                    if ticket_id in seen_ticket_ids:
                        continue
                    seen_ticket_ids.add(ticket_id)

                    ticket_status = str(ticket.get("status") or "").strip().lower()
                    if ticket_status not in ACTIVE_ZENDESK_STATUSES:
                        skipped_unexpected_status += 1
                        continue

                    # Already handed to the second-level logistics group on an
                    # earlier run: skip before any further work, so the request
                    # is never analyzed and never reaches the history table.
                    if (
                        escalation_group_id is not None
                        and _to_int(ticket.get("group_id")) == escalation_group_id
                    ):
                        skipped_escalation_group += 1
                        continue

                    candidate_tickets.append(ticket)

                if not candidate_tickets:
                    continue

                # Zendesk search results normally have requester_id but not a
                # normalized requester email. Batch-load only the user records
                # needed for tickets whose via.source email is not already a
                # carrier-domain address.
                user_ids_to_load = []
                for ticket in candidate_tickets:
                    if email_matches_any_carrier_domain(_ticket_via_source_email(ticket)):
                        continue
                    requester_id = ticket.get("requester_id")
                    try:
                        requester_id_int = int(requester_id)
                    except (TypeError, ValueError):
                        continue
                    if requester_id_int not in user_email_cache:
                        user_ids_to_load.append(requester_id_int)

                get_zendesk_users_by_ids(
                    user_ids_to_load,
                    auth=auth,
                    base_url=base_url,
                    cache=user_email_cache,
                    session=session,
                )

                matching_tickets: list[dict[str, Any]] = []

                for ticket in candidate_tickets:
                    ticket_id = int(ticket["id"])
                    requester_email = requester_email_from_ticket_cache(
                        ticket,
                        user_email_cache=user_email_cache,
                    )
                    if not email_matches_any_carrier_domain(requester_email):
                        skipped_non_carrier += 1
                        continue

                    matching_tickets.append(ticket)
                    requester_email_by_ticket_id[ticket_id] = requester_email

                print(
                    f"Matched {len(matching_tickets)} carrier-domain ticket(s) "
                    f"from {len(candidate_tickets)} active candidate ticket(s) "
                    "on this page."
                )

                all_matching_tickets.extend(matching_tickets)


    # Phase B: one selection, one comment-fetch pool. Comments are the
    # rate-limited call, so only the tickets this run can actually process
    # are fetched -- see limit_tickets_for_comment_fetch() for why the
    # budget is deliberately larger than MAX_TICKETS_PER_RUN.
    selected_tickets = limit_tickets_for_comment_fetch(all_matching_tickets)

    ticket_comment_map: dict[int, list[dict[str, Any]]] = {}
    workers = _zendesk_comment_fetch_workers(len(selected_tickets))

    if workers <= 1:
        comment_results = [
            get_comments_for_ticket(
                ticket,
                auth=auth,
                base_url=base_url,
            )
            for ticket in selected_tickets
        ]
    else:
        comment_results = []
        # This used to run once per search page, so the per-page "Matched ..."
        # lines doubled as progress. Fetching every ticket in one pool is one
        # silent multi-minute block without a counter, and this is the stage
        # that sleeps on Zendesk rate limits -- exactly when you need to see
        # that the run is still moving.
        total_to_fetch = len(selected_tickets)
        progress_step = max(1, total_to_fetch // 10)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    get_comments_for_ticket,
                    ticket,
                    auth=auth,
                    base_url=base_url,
                )
                for ticket in selected_tickets
            ]
            print(
                f"Fetching comments for {total_to_fetch} ticket(s) "
                f"with {workers} worker(s)."
            )
            for future in as_completed(futures):
                comment_results.append(future.result())
                done = len(comment_results)
                if done % progress_step == 0 or done == total_to_fetch:
                    print(f"  ...fetched comments for {done}/{total_to_fetch} ticket(s).")

    for result in comment_results:
        if result.get("error"):
            comment_error_count += 1
            print(
                f"Error fetching comments for ticket "
                f"{result.get('ticket_id')}: {result.get('error')}"
            )
            continue
        try:
            result_ticket_id = int(result["ticket_id"])
        except (TypeError, ValueError):
            continue
        ticket_comment_map[result_ticket_id] = result.get("comments", [])

    for ticket in selected_tickets:
        try:
            ticket_id = int(ticket["id"])
            requester_id = ticket.get("requester_id")
            requester_email = requester_email_by_ticket_id.get(ticket_id, "")
            configured_category = email_to_category.get(requester_email)
            carrier = carrier_code_from_email(requester_email)
            comments = ticket_comment_map.get(ticket_id, [])
            if not comments:
                continue

            requester_comment_counter = 0
            requester_requests = []

            for idx, comment in enumerate(comments):
                if comment.get("author_id") != requester_id:
                    continue

                requester_comment_counter += 1
                requester_requests.append(
                    {
                        "request_number": requester_comment_counter,
                        "body": comment.get("body", ""),
                        "comment_index": idx,
                        "request_submission_timestamp": comment.get("created_at"),
                    }
                )

            active_requests = []

            for idx in range(len(comments) - 1, -1, -1):
                comment = comments[idx]

                # Ignore internal notes.
                if not comment.get("public", True):
                    continue

                if comment.get("author_id") == requester_id:
                    request = next(
                        (
                            candidate
                            for candidate in requester_requests
                            if candidate["comment_index"] == idx
                        ),
                        None,
                    )

                    if request:
                        active_requests.append(request)
                else:
                    # First public reply after the latest requester block.
                    break

            active_requests.reverse()

            if not active_requests:
                continue

            subject = ticket.get("subject", "")
            description = ticket.get("description", "")

            for request in active_requests:
                request_body = request["body"]
                ticket_category = configured_category or classify_ticket_category_from_content(
                    subject=subject,
                    request_body=request_body,
                    requester_email=requester_email,
                )
                tracking_number = extract_tracking_number(
                    subject,
                    f"{description}\n{request_body}",
                    carrier,
                )
                # Fallback identifier used when the tracking number
                # is missing or unknown to
                # bi.shipping_platform_shipments. Collected here
                # because the ticket description is available only
                # at fetch time; PDF attachments are scanned later,
                # and only for the rows that still need an order.
                order_references = find_order_references(
                    subject,
                    description,
                    request_body,
                )

                rows.append(
                    {
                        "ingestion_timestamp": datetime.now(timezone.utc),
                        "request_id": build_request_id(
                            ticket_id,
                            request["request_number"],
                        ),
                        "request_submission_timestamp": request.get(
                            "request_submission_timestamp"
                        ),
                        "ticket_submission_timestamp": ticket.get("created_at"),
                        "zendesk_ticket_id": ticket_id,
                        "requester_email": requester_email,
                        "subject": subject,
                        "request_body": request_body,
                        "request_number": request["request_number"],
                        "ticket_category": ticket_category,
                        "extracted_tracking_number": tracking_number,
                        "extracted_order_reference": ";".join(
                            order_references
                        ),
                    }
                )

        except Exception as exc:
            print(f"Error processing ticket {ticket.get('id')}: {str(exc)}")

    print(
        f"Fetched {len(rows)} active requester message(s) from carrier-domain tickets; "
        f"skipped {skipped_non_carrier} non-carrier ticket(s), "
        f"{skipped_unexpected_status} ticket(s) with unexpected statuses, "
        f"{skipped_escalation_group} ticket(s) already assigned to "
        f"{ZENDESK_ESCALATION_GROUP_NAME!r}, and "
        f"{comment_error_count} ticket(s) with comment-fetch errors."
    )
    return rows


def enrich_with_shipment_numbers(
    df: pd.DataFrame,
    client,
    bigquery,
    *,
    auth=None,
    base_url: str | None = None,
    til_client=None,
) -> pd.DataFrame:
    """Resolve each request to a shipment/order.

    The tracking number is tried first against
    bi.shipping_platform_shipments.  Rows it cannot resolve then go through
    apply_order_reference_fallback(), which looks for an order reference in the
    subject/description and, failing that, inside the ticket's PDF attachments.
    ``auth``/``base_url`` are required only for that PDF step.

    ``til_client`` is the separate BigQuery client for the TIL project (see
    til_bigquery_client()); without it the final prd_shipmentStatus lookup is
    skipped and those rows go to human review exactly as they did before.
    """

    tracking_numbers = []

    for value in df["extracted_tracking_number"].dropna():
        for tracking in re.split(r"[;,]", str(value)):
            tracking = tracking.strip()
            if tracking and tracking.upper() != "N/A":
                tracking_numbers.append(tracking)

    tracking_numbers = list(dict.fromkeys(tracking_numbers))

    shipment_map = {}

    if tracking_numbers:
        # shipment_order_number holds two different shapes: the ORDER number
        # (CV-US-00041347, dash in position 6) and the SHIPMENT number
        # (CV-USA00041347, a letter there). Looking a tracking number up can
        # land on either. Joining on the literal value therefore missed every
        # shipment of the order whenever the matched row carried the order
        # shape -- the "tracking number not found" case that is really "found,
        # but its siblings were not".
        #
        # Dropping character 6 collapses both shapes to one key, which is the
        # single-character wildcard in `shipment_order_number LIKE
        # 'CV-US%00041347'`, so every shipment of the order is returned: A, B,
        # C and the order row itself. Rows without a tracking number carry no
        # shipment to report, and deleted rows must never be considered.
        shipment_query = """
        WITH live AS (

            SELECT
                shipment_order_number,
                tracking_number,
                is_return,
                carrier_code,
                -- NULL for a blank order number, never '' : the table holds
                -- rows with no shipment_order_number, and an equality join on
                -- a blank key matches every one of them to every other. That
                -- is how a ticket ends up reporting an export tracking number
                -- belonging to a completely unrelated shipment. A NULL key
                -- joins to nothing, and those rows fall back to the
                -- tracking-number match below.
                -- Also NULL for anything that is not shaped like an order
                -- reference. The table carries UPS reference strings such as
                -- 'LN_SUP_DTM' and 'LN_SUP_STR1' in this column, and those
                -- repeat across unrelated shipments. Collapsing character 6
                -- would make every shipment sharing one of those sentinels a
                -- join partner of every other, exactly as the blank value did.
                IF(
                    shipment_order_number IS NULL
                    OR TRIM(shipment_order_number) = ''
                    OR NOT REGEXP_CONTAINS(
                        TRIM(shipment_order_number),
                        r'^[A-Za-z]{2}-[A-Za-z]{2}([A-Za-z]|-)[A-Za-z0-9]{4,20}$'
                    ),
                    NULL,
                    IF(
                        LENGTH(TRIM(shipment_order_number)) >= 7,
                        CONCAT(
                            SUBSTR(TRIM(shipment_order_number), 1, 5),
                            SUBSTR(TRIM(shipment_order_number), 7)
                        ),
                        TRIM(shipment_order_number)
                    )
                ) AS order_key

            FROM `tlg-business-intelligence-prd.bi.shipping_platform_shipments`

            WHERE is_deleted = false
        ),

        matched AS (

            SELECT DISTINCT
                tracking_number AS source_tracking_number,
                order_key

            FROM live

            WHERE tracking_number IN UNNEST(@tracking_numbers)
        )

        -- Rows whose order number is known: every shipment of that order.
        SELECT
            matched.source_tracking_number,
            s.shipment_order_number,
            s.tracking_number,
            s.is_return,
            s.carrier_code

        FROM matched

        JOIN live s
          ON matched.order_key = s.order_key

        WHERE matched.order_key IS NOT NULL
          AND s.tracking_number IS NOT NULL

        UNION DISTINCT

        -- Rows with no order number: only the matched row itself. Nothing
        -- ties it to any other shipment, so nothing else may be reported.
        SELECT
            matched.source_tracking_number,
            s.shipment_order_number,
            s.tracking_number,
            s.is_return,
            s.carrier_code

        FROM matched

        JOIN live s
          ON matched.source_tracking_number = s.tracking_number

        WHERE matched.order_key IS NULL
          AND s.tracking_number IS NOT NULL
        """

        shipment_df = (
            client.query(
                shipment_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter(
                            "tracking_numbers",
                            "STRING",
                            tracking_numbers,
                        )
                    ]
                ),
            )
            .to_dataframe(create_bqstorage_client=BQ_STORAGE)
        )

        def _joined(values: list[Any]) -> str | None:
            """De-duplicate, preserve order, join the way the pipeline does."""

            seen: list[str] = []
            for value in values:
                text = _to_str(value)
                if text is None:
                    continue
                text = text.strip()
                if text and text not in seen:
                    seen.append(text)
            return ";".join(seen) if seen else None

        for tracking in shipment_df["source_tracking_number"].unique():
            subset = shipment_df[shipment_df["source_tracking_number"] == tracking]

            # One order can have several shipments (CV-USA…, CV-USB…). All of
            # them are reported; the previous code kept only whichever row the
            # scan happened to end on. Shipment-shaped values are preferred
            # over the order-shaped row, which names no single shipment.
            order_numbers = [
                value
                for value in subset["shipment_order_number"].tolist()
                if _to_str(value)
            ]
            shipment_shaped = [
                value
                for value in order_numbers
                if is_shipment_shaped_order_reference(_to_str(value))
            ]

            shipment_trackings: list[Any] = []
            return_trackings: list[Any] = []
            shipment_carriers: list[Any] = []
            return_carriers: list[Any] = []

            for _, row in subset.iterrows():
                if bool(row["is_return"]):
                    return_trackings.append(row["tracking_number"])
                    return_carriers.append(row.get("carrier_code"))
                else:
                    shipment_trackings.append(row["tracking_number"])
                    shipment_carriers.append(row.get("carrier_code"))

            # A single carrier_code is reported when every row agrees. If the
            # shipments disagree there is no one answer, and leaving it unset
            # makes return_export_carriers_match() treat the legs as being on
            # different carriers, which is the safe reading.
            def _single(values: list[Any]) -> Any:
                joined = _joined(values)
                if joined is None or ";" in joined:
                    return None
                return joined

            shipment_map[tracking] = {
                "shipment_order_number": _joined(shipment_shaped or order_numbers),
                "shipment_tracking_number": _joined(shipment_trackings),
                "return_tracking_number": _joined(return_trackings),
                "shipment_carrier_code": _single(shipment_carriers),
                "return_carrier_code": _single(return_carriers),
            }

    df = df.copy()
    normalized_tracking = df["extracted_tracking_number"].map(
        lambda x: str(x).strip() if pd.notna(x) else ""
    )

    def enrich_tracking_field(value, field):
        results = []

        for tracking in re.split(r"[;,]", str(value or "")):
            tracking = tracking.strip()
            if not tracking:
                continue

            mapped = shipment_map.get(tracking, {}).get(field)

            if pd.notna(mapped):
                mapped = str(mapped)
                if mapped not in results:
                    results.append(mapped)

        return ";".join(results) if results else None


    df["shipment_order_number"] = df["extracted_tracking_number"].apply(
        lambda x: enrich_tracking_field(x, "shipment_order_number")
    )

    df["shipment_tracking_number"] = df["extracted_tracking_number"].apply(
        lambda x: enrich_tracking_field(x, "shipment_tracking_number")
    )

    df["return_tracking_number"] = df["extracted_tracking_number"].apply(
        lambda x: enrich_tracking_field(x, "return_tracking_number")
    )

    df["shipment_carrier_code"] = df["extracted_tracking_number"].apply(
        lambda x: enrich_tracking_field(x, "shipment_carrier_code")
    )

    df["return_carrier_code"] = df["extracted_tracking_number"].apply(
        lambda x: enrich_tracking_field(x, "return_carrier_code")
    )

    df["tracking_not_found_in_shipping_platform_shipments"] = df[
        "extracted_tracking_number"
    ].apply(
        lambda value: any(
            tracking.strip()
            and tracking.strip().upper() != "N/A"
            and tracking.strip() not in shipment_map
            for tracking in re.split(r"[;,]", str(value or ""))
        )
    )

    # Order resolution, cheapest and most direct source first:
    #   1. subject_or_description  (the sender named the order)
    #   2. tracking_lookup         (bi.shipping_platform_shipments)
    #      -- 1 and 2 are both decided inside apply_order_reference_fallback
    #   3. shipment_status_events  (til.prd_shipmentStatus)
    #   4. til_events              (til.prd_tilEvents)
    #   5. pdf_attachment          (download + OCR, so it goes last)
    # Each pass only looks at rows the previous ones left without an order.
    df = apply_order_reference_fallback(df, auth=auth, base_url=base_url)
    df = apply_shipment_status_order_lookup(df, til_client, bigquery)
    df = apply_til_events_order_lookup(df, til_client, bigquery)
    df = apply_pdf_attachment_order_lookup(
        df,
        auth=auth,
        base_url=base_url,
        client=client,
        til_client=til_client,
        bigquery=bigquery,
    )
    #   7. cross_request_tracking  (free: reuse what this run already resolved)
    df = apply_cross_request_order_propagation(df)

    return df


# ============================================================================
# SHIPMENT-STATUS ORDER LOOKUP (last resort)
# ============================================================================


SHIPMENT_STATUS_TABLE = "`tlg-wlfs-prd`.til.prd_shipmentStatus"

# The carrier events carry the shipment order number next to the tracking
# number, so a tracking number bi.shipping_platform_shipments never received
# can still be tied to an order. PACKED and SHIPPED are the only event types
# that carry shippingData.
SHIPMENT_STATUS_QUERY = f"""
WITH shipments AS (

    SELECT
        JSON_VALUE(ship, '$.trackingNumber')                        AS tracking_number,
        JSON_VALUE(ship, '$.carrierCode')                           AS carrier_code,
        JSON_VALUE(s.requestPayload, '$.data.shipmentOrderNumber')  AS shipment_order_number,
        JSON_VALUE(s.requestPayload, '$.data.logisticOrderNumber')  AS logistic_order_number,
        TIMESTAMP(JSON_VALUE(s.requestPayload, '$.eventTimestamp')) AS event_timestamp

    FROM {SHIPMENT_STATUS_TABLE} AS s,
         UNNEST(JSON_EXTRACT_ARRAY(s.requestPayload, '$.data.shippingData')) AS ship

    WHERE s.eventType IN ('PACKED', 'SHIPPED')
      AND JSON_VALUE(ship, '$.trackingNumber') IN UNNEST(@tracking_numbers)
)

-- One row per (tracking number, order). A tracking number normally produces
-- several events -- PACKED then SHIPPED -- all naming the same order, and
-- those collapse here. A tracking number that genuinely names two different
-- orders survives as two rows, and the caller refuses to guess between them
-- rather than reporting an arbitrary one.
SELECT
    tracking_number,
    shipment_order_number,
    ANY_VALUE(logistic_order_number) AS logistic_order_number,
    ANY_VALUE(carrier_code)          AS carrier_code,
    MAX(event_timestamp)             AS last_event_at

FROM shipments

WHERE tracking_number IS NOT NULL
  AND shipment_order_number IS NOT NULL
  AND TRIM(shipment_order_number) != ''

GROUP BY tracking_number, shipment_order_number
"""


def _row_tracking_numbers(value: object) -> list[str]:
    """The usable tracking numbers of one row, in order."""

    numbers: list[str] = []
    for tracking in re.split(r"[;,]", _to_str(value) or ""):
        tracking = tracking.strip()
        if tracking and tracking.upper() != "N/A" and tracking not in numbers:
            numbers.append(tracking)
    return numbers


def shipment_status_orders_for_trackings(
    tracking_numbers: list[str],
    client,
    bigquery,
) -> dict[str, str]:
    """Map tracking number -> shipment order number, from the carrier events.

    A tracking number that names more than one order is left out: there is no
    way to tell which one the ticket means, and answering with the wrong
    order is worse than routing the request to a human.
    """

    if not tracking_numbers:
        return {}

    status_df = client.query(
        SHIPMENT_STATUS_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "tracking_numbers",
                    "STRING",
                    tracking_numbers,
                )
            ]
        ),
    ).to_dataframe(create_bqstorage_client=BQ_STORAGE)

    candidates: dict[str, list[str]] = {}
    for _, row in status_df.iterrows():
        tracking = (_to_str(row.get("tracking_number")) or "").strip()
        order = (_to_str(row.get("shipment_order_number")) or "").strip()
        if not tracking or not order:
            continue
        # Validated exactly like every other order the pipeline accepts, so a
        # payload carrying something that is not a reference cannot become a
        # phantom order.
        references = split_order_references(order)
        if not references:
            continue
        for reference in references:
            if reference not in candidates.setdefault(tracking, []):
                candidates[tracking].append(reference)

    resolved: dict[str, str] = {}
    for tracking, references in candidates.items():
        if len(references) == 1:
            resolved[tracking] = references[0]
        else:
            print(
                f"  Tracking {tracking} maps to {len(references)} different "
                f"orders in prd_shipmentStatus ({', '.join(references)}); "
                "not resolving it automatically."
            )
    return resolved


def apply_shipment_status_order_lookup(
    df: pd.DataFrame,
    client,
    bigquery,
) -> pd.DataFrame:
    """Fourth and last resolution step, after the order-reference fallback.

    ``client`` is the TIL-project BigQuery client, NOT the BI one: the table
    lives in another project. ``None`` skips the step.

    Applies only to rows that reached the end of
    apply_order_reference_fallback() with a tracking number, no order at all,
    and the tracking number missing from bi.shipping_platform_shipments --
    i.e. exactly the rows that would otherwise go to human review with "no
    order reference found". Rows that already have an order are untouched:
    a reference the ticket itself carried is better evidence of what the
    sender is asking about than a carrier event.
    """

    if df.empty or "extracted_tracking_number" not in df.columns:
        return df

    if client is None:
        # No TIL credentials: the rows this step would have rescued simply
        # stay unresolved, which is the pre-existing behavior.
        return df

    df = df.copy()

    def _needs_lookup(row: Mapping[str, Any]) -> bool:
        if not _to_bool(row.get("tracking_not_found_in_shipping_platform_shipments")):
            return False
        if split_order_references(row.get("shipment_order_number")):
            return False
        return bool(_row_tracking_numbers(row.get("extracted_tracking_number")))

    pending = [row for _, row in df.iterrows() if _needs_lookup(row)]
    if not pending:
        return df

    tracking_numbers: list[str] = []
    for row in pending:
        for tracking in _row_tracking_numbers(row.get("extracted_tracking_number")):
            if tracking not in tracking_numbers:
                tracking_numbers.append(tracking)

    print(
        f"Shipment-status lookup: {len(pending)} request(s) still without an "
        f"order; querying prd_shipmentStatus for {len(tracking_numbers)} "
        "tracking number(s)."
    )

    try:
        resolved_map = shipment_status_orders_for_trackings(
            tracking_numbers, client, bigquery
        )
    except Exception as error:  # noqa: BLE001 - a failed last resort must not
        # break the run: these rows were already heading for human review.
        print(f"  prd_shipmentStatus lookup failed: {error}")
        return df

    if not resolved_map:
        print("  No order found in prd_shipmentStatus for any of them.")
        return df

    resolved_count = 0

    for index, row in df.iterrows():
        if not _needs_lookup(row):
            continue

        references: list[str] = []
        covered = 0
        for tracking in _row_tracking_numbers(row.get("extracted_tracking_number")):
            reference = resolved_map.get(tracking)
            if not reference:
                continue
            covered += 1
            if reference not in references:
                references.append(reference)

        if not references:
            continue

        resolved_order = ";".join(references)
        df.at[index, "shipment_order_number"] = resolved_order
        df.at[index, "extracted_order_reference"] = resolved_order
        df.at[index, "order_reference_source"] = ORDER_REFERENCE_SOURCE_SHIPMENT_STATUS
        # The event payload names one specific shipment, so only that block of
        # the GET_FULL_ORDER payload is read -- the same rule the fallback
        # applies to a shipment-shaped reference found in the ticket.
        df.at[index, "shipment_selection_mode"] = (
            SHIPMENT_SELECTION_EXACT
            if all(
                is_shipment_shaped_order_reference(reference)
                for reference in references
            )
            else SHIPMENT_SELECTION_BY_TRACKING
        )
        # Complete only when every tracking number of the row found an order;
        # a partially resolved row must still go to a human, as before.
        df.at[index, "order_resolution_complete"] = covered == len(
            _row_tracking_numbers(row.get("extracted_tracking_number"))
        )
        df.at[index, "order_processing_blocked"] = contains_blocked_order_reference(
            resolved_order
        )
        resolved_count += 1

    print(
        f"  Resolved {resolved_count} request(s) from prd_shipmentStatus "
        "carrier events."
    )

    return df


TIL_EVENTS_TABLE = "`tlg-wlfs-prd`.til.prd_tilEvents"

# The TIL event stream carries each shipment block under
# requestPayload.data[].shipment, and the order number with it. The tracking
# number can appear in three different places depending on the event, so all
# three are matched; a ticket tracking number that bi.shipping_platform_shipments
# and prd_shipmentStatus both missed can still be tied to an order here.
TIL_EVENTS_QUERY = f"""
WITH wanted AS (

    SELECT DISTINCT TRIM(tracking) AS tracking_number
    FROM UNNEST(@tracking_numbers) AS raw,
         UNNEST(SPLIT(raw, ';')) AS tracking
    WHERE TRIM(tracking) != ''
),

matches AS (

    SELECT
        w.tracking_number                                  AS tracking_number,
        JSON_VALUE(item, '$.shipment.orderNumber')         AS shipment_order_number,
        JSON_VALUE(item, '$.shipment.carrierCode')         AS carrier_code,
        JSON_VALUE(item, '$.shipment.isReturn')            AS is_return,
        e.ingestionTimestamp                               AS event_timestamp

    FROM {TIL_EVENTS_TABLE} AS e,
         UNNEST(JSON_EXTRACT_ARRAY(e.requestPayload, '$.data')) AS item
    JOIN wanted AS w
      ON w.tracking_number IN (
             JSON_VALUE(item, '$.trackingNumber'),
             JSON_VALUE(item, '$.shipment.trackingNumber'),
             JSON_VALUE(item, '$.carrierShipmentIdentificationNumber')
         )
)

-- One row per (tracking number, order). A tracking number normally appears in
-- several events naming the same order and those collapse here; one that
-- genuinely names two different orders survives as two rows, and the caller
-- refuses to guess between them rather than reporting an arbitrary one.
SELECT
    tracking_number,
    shipment_order_number,
    ANY_VALUE(carrier_code) AS carrier_code,
    ANY_VALUE(is_return)    AS is_return,
    MAX(event_timestamp)    AS last_event_at

FROM matches

WHERE shipment_order_number IS NOT NULL
  AND TRIM(shipment_order_number) != ''

GROUP BY tracking_number, shipment_order_number
"""


def til_events_orders_for_trackings(
    tracking_numbers: list[str],
    client,
    bigquery,
) -> dict[str, str]:
    """Map tracking number -> shipment order number, from til.prd_tilEvents.

    Mirrors shipment_status_orders_for_trackings(): a tracking number that
    names more than one order is dropped rather than resolved to an arbitrary
    one of them.
    """

    if not tracking_numbers or client is None:
        return {}

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "tracking_numbers", "STRING", tracking_numbers
            )
        ]
    )

    rows = list(client.query(TIL_EVENTS_QUERY, job_config=job_config).result())

    orders_by_tracking: dict[str, set] = {}
    for row in rows:
        tracking = _to_str(row.get("tracking_number")).strip()
        reference = _to_str(row.get("shipment_order_number")).strip()
        if not tracking or not reference:
            continue
        orders_by_tracking.setdefault(tracking, set()).add(reference)

    resolved: dict[str, str] = {}
    for tracking, references in orders_by_tracking.items():
        if len(references) != 1:
            print(
                f"  Tracking {tracking} names {len(references)} different "
                f"orders in prd_tilEvents ({', '.join(sorted(references))}); "
                "not resolved."
            )
            continue
        reference = next(iter(references))
        if split_order_references(reference):
            resolved[tracking] = reference

    return resolved


def apply_til_events_order_lookup(
    df: pd.DataFrame,
    client,
    bigquery,
) -> pd.DataFrame:
    """Fourth resolution step, after prd_shipmentStatus and before the PDFs.

    ``client`` is the TIL-project BigQuery client, NOT the BI one: the table
    lives in another project. ``None`` skips the step.

    Applies only to rows that still have no order after the tracking lookup
    and the prd_shipmentStatus events -- i.e. the rows that would otherwise
    reach the PDF scan, and failing that human review.
    """

    if df.empty or "extracted_tracking_number" not in df.columns:
        return df

    if client is None:
        # No TIL credentials: the rows this step would have rescued simply
        # stay unresolved, which is the pre-existing behavior.
        return df

    df = df.copy()

    def _needs_lookup(row: Mapping[str, Any]) -> bool:
        if split_order_references(row.get("shipment_order_number")):
            return False
        return bool(_row_tracking_numbers(row.get("extracted_tracking_number")))

    pending = [row for _, row in df.iterrows() if _needs_lookup(row)]
    if not pending:
        return df

    tracking_numbers: list[str] = []
    for row in pending:
        for tracking in _row_tracking_numbers(row.get("extracted_tracking_number")):
            if tracking not in tracking_numbers:
                tracking_numbers.append(tracking)

    print(
        f"TIL-events lookup: {len(pending)} request(s) still without an order; "
        f"querying prd_tilEvents for {len(tracking_numbers)} tracking number(s)."
    )

    try:
        resolved_map = til_events_orders_for_trackings(
            tracking_numbers, client, bigquery
        )
    except Exception as error:  # noqa: BLE001 - a failed fallback must not
        # break the run: these rows were already heading for the PDF scan.
        print(f"  prd_tilEvents lookup failed: {error}")
        return df

    if not resolved_map:
        print("  No order found in prd_tilEvents for any of them.")
        return df

    resolved_count = 0

    for index, row in df.iterrows():
        if not _needs_lookup(row):
            continue

        references: list[str] = []
        covered = 0
        for tracking in _row_tracking_numbers(row.get("extracted_tracking_number")):
            reference = resolved_map.get(tracking)
            if not reference:
                continue
            covered += 1
            if reference not in references:
                references.append(reference)

        if not references:
            continue

        resolved_order = ";".join(references)
        df.at[index, "shipment_order_number"] = resolved_order
        df.at[index, "extracted_order_reference"] = resolved_order
        df.at[index, "order_reference_source"] = ORDER_REFERENCE_SOURCE_TIL_EVENTS
        # The event payload names one specific shipment, so only that block of
        # the GET_FULL_ORDER payload is read -- the same rule every other
        # source applies to a shipment-shaped reference.
        df.at[index, "shipment_selection_mode"] = (
            SHIPMENT_SELECTION_EXACT
            if all(
                is_shipment_shaped_order_reference(reference)
                for reference in references
            )
            else SHIPMENT_SELECTION_BY_TRACKING
        )
        # Complete only when every tracking number of the row found an order;
        # a partially resolved row must still go to a human, as before.
        df.at[index, "order_resolution_complete"] = covered == len(
            _row_tracking_numbers(row.get("extracted_tracking_number"))
        )
        df.at[index, "order_processing_blocked"] = contains_blocked_order_reference(
            resolved_order
        )
        resolved_count += 1

    print(f"  Resolved {resolved_count} request(s) from prd_tilEvents.")

    return df


# ============================================================================
# ORDER-REFERENCE FALLBACK
# ============================================================================


def ticket_pdf_attachments(
    ticket_id: int,
    *,
    auth,
    base_url: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Return every PDF attached to any comment of the ticket, de-duplicated."""

    attachments: list[dict[str, Any]] = []
    seen: set[Any] = set()
    url = f"{base_url}/tickets/{ticket_id}/comments.json?page[size]=100"

    while url:
        payload = zendesk_get(url, auth=auth, session=session, timeout=60).json() or {}
        for comment in payload.get("comments") or []:
            for attachment in comment.get("attachments") or []:
                attachment_id = attachment.get("id")
                if is_pdf_attachment(attachment) and attachment_id not in seen:
                    seen.add(attachment_id)
                    attachments.append(attachment)
        meta = payload.get("meta") or {}
        links = payload.get("links") or {}
        next_url = links.get("next") if _zendesk_meta_has_more(meta) else payload.get("next_page")
        url = _absolute_zendesk_url(base_url, next_url) if next_url else None

    return attachments


def download_zendesk_attachment(
    attachment: Mapping[str, Any],
    *,
    auth,
    session: requests.Session | None = None,
) -> bytes:
    """Fetch an attachment. Content URLs are usually public; fall back to auth."""

    content_url = attachment.get("content_url")
    if not content_url:
        raise RuntimeError("attachment has no content_url")

    # (connect, read): a dead host fails in 5s instead of 120, and a stalled
    # transfer gives up after 30s of silence. The old flat 120 applied to BOTH
    # attempts, so one bad attachment could hold a worker for four minutes --
    # and a ticket with several of them for much longer. With 8 workers, a
    # handful of these is enough to make the whole stage look hung.
    attachment_timeout = (5, 30)
    requester = session or requests
    response = requester.get(content_url, timeout=attachment_timeout)
    if response.status_code in (401, 403):
        response = requester.get(content_url, auth=auth, timeout=attachment_timeout)
    if not response.ok:
        raise RuntimeError(
            f"{response.status_code} downloading {attachment.get('file_name')}"
        )
    return response.content


def order_references_from_ticket_pdfs(
    ticket_id: int,
    *,
    auth,
    base_url: str,
    session: requests.Session | None = None,
    log: Callable[[str], None] = print,
) -> tuple[list[str], list[str]]:
    """Open the ticket's PDF attachments; return (order references, trackings).

    Both are read in one pass because the text extraction may OCR every page,
    and the tracking numbers matter: on a UPS overgoods invoice the waybill
    number is the overgoods leg, which no TLG table knows, while the original
    export tracking sits in the "Invoice No." field and is the one that
    resolves to an order.

    Mirrors ticket_order_numbers.py: embedded text layer first, local Tesseract
    OCR as a fallback for scanned invoices.  Any failure (download, unreadable
    PDF, OCR not installed) is reported and treated as "no reference found" so
    one bad attachment can never abort the run.

    ``log`` defaults to ``print``; concurrent callers pass a collector so the
    lines for one ticket stay together instead of interleaving across threads.
    """

    try:
        attachments = ticket_pdf_attachments(
            ticket_id,
            auth=auth,
            base_url=base_url,
            session=session,
        )
    except Exception as exc:  # noqa: BLE001 - never abort the fetch over one ticket
        log(f"  Could not list attachments for ticket {ticket_id}: {exc}")
        return [], []

    if not attachments:
        log(f"      ticket {ticket_id} has no PDF attachment to scan.")
        return [], []

    blobs: list[tuple[str, bytes]] = []
    for attachment in attachments:
        file_name = str(attachment.get("file_name") or "attachment.pdf")
        try:
            blobs.append(
                (
                    file_name,
                    download_zendesk_attachment(attachment, auth=auth, session=session),
                )
            )
        except Exception as exc:  # noqa: BLE001
            log(f"      download failed for {file_name}: {exc}")

    if not blobs:
        return [], []

    return find_identifiers_in_pdfs(blobs, log=log)


def request_needs_no_reply(row: Mapping[str, Any]) -> bool:
    """True when regex_engine.py will exclude this request without answering.

    Only the two text-only exclusions are replicated here, both from
    customs_rules.py so there is a single definition of each:

    * a noreply*/autoreply* sender on request number 1, submitted as solved
      without analysis;
    * a carrier status notification carrying no customer request.

    is_acknowledgement_only() is deliberately NOT used: regex_engine applies it
    only when no real request type also matched, and a message that both thanks
    and asks is answered normally. Replicating that gate would mean duplicating
    the whole classifier here, and the two copies would drift.
    """

    if is_noreply_autoreply_first_request(
        row.get("requester_email"), row.get("request_number", 1)
    ):
        return True
    return bool(is_no_action_carrier_notification(row.get("request_body")))


def _order_reference_pdf_scan_workers(ticket_count: int) -> int:
    if ticket_count <= 0:
        return 0
    configured = _env_int(
        ORDER_REFERENCE_PDF_SCAN_WORKERS_ENV,
        DEFAULT_ORDER_REFERENCE_PDF_SCAN_WORKERS,
        minimum=1,
        maximum=32,
    )
    return min(configured, ticket_count)


def scan_ticket_pdfs_for_order_references(
    ticket_id: int,
    *,
    auth,
    base_url: str,
) -> dict[str, Any]:
    """Scan one ticket's PDFs; safe for ThreadPoolExecutor workers.

    Each worker owns its requests.Session -- a Session is not thread-safe and
    must never be shared across workers.  Log lines are collected rather than
    printed so the caller can replay them in ticket order.
    """

    messages: list[str] = [
        f"  No tracking/order reference in ticket {ticket_id}; "
        "scanning PDF attachments."
    ]
    try:
        with requests.Session() as session:
            references, trackings = order_references_from_ticket_pdfs(
                ticket_id,
                auth=auth,
                base_url=base_url,
                session=session,
                log=messages.append,
            )
        return {
            "ticket_id": ticket_id,
            "references": references,
            "trackings": trackings,
            "messages": messages,
        }
    except Exception as exc:  # noqa: BLE001 - one bad ticket must not stop the rest
        messages.append(f"      PDF scan failed for ticket {ticket_id}: {exc}")
        return {
            "ticket_id": ticket_id,
            "references": [],
            "trackings": [],
            "messages": messages,
        }


def _prefetch_pdf_order_references(
    ticket_ids: list[int],
    *,
    auth,
    base_url: str,
) -> dict[int, dict[str, list[str]]]:
    """Resolve the PDF identifiers for many tickets concurrently.

    Each entry is ``{"references": [...], "trackings": [...]}``: the order
    references found in the attachments, and every tracking-shaped value,
    both from the same single text/OCR pass.

    The per-ticket work (list attachments, download, OCR) is independent, so it
    parallelizes cleanly.  Results are returned as a cache the sequential
    row loop then reads, leaving its resolution logic untouched.
    """

    cache: dict[int, dict[str, list[str]]] = {}
    if not ticket_ids:
        return cache

    workers = _order_reference_pdf_scan_workers(len(ticket_ids))
    print(
        f"Scanning PDF attachments for {len(ticket_ids)} ticket(s) "
        f"with {workers} worker(s)."
    )

    if workers <= 1:
        results = [
            scan_ticket_pdfs_for_order_references(
                ticket_id,
                auth=auth,
                base_url=base_url,
            )
            for ticket_id in ticket_ids
        ]
    else:
        results = []
        # Progress is printed HERE, as workers finish -- not in the replay
        # loop below. The replay only runs once every future has returned, so
        # a counter there leaves this stage completely silent for its whole
        # duration, which is precisely when you need to know it is moving.
        total_to_scan = len(ticket_ids)
        progress_step = max(1, total_to_scan // 10)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    scan_ticket_pdfs_for_order_references,
                    ticket_id,
                    auth=auth,
                    base_url=base_url,
                )
                for ticket_id in ticket_ids
            ]
            for future in as_completed(futures):
                results.append(future.result())
                done = len(results)
                if done % progress_step == 0 or done == total_to_scan:
                    print(f"  ...scanned {done}/{total_to_scan} ticket(s).")

    # Replay in submission order: as_completed yields whichever worker
    # finished first, which would otherwise scramble the run log.
    by_ticket = {result["ticket_id"]: result for result in results}
    for position, ticket_id in enumerate(ticket_ids, start=1):
        result = by_ticket.get(ticket_id)
        if result is None:
            continue
        for message in result["messages"]:
            print(message)
        cache[ticket_id] = {
            "references": result["references"],
            "trackings": result.get("trackings") or [],
        }

    return cache


SHIPMENTS_BY_TRACKING_QUERY = """
SELECT DISTINCT
    tracking_number,
    return_tracking_number,
    shipment_order_number

FROM `tlg-business-intelligence-prd.bi.shipping_platform_shipments`

WHERE is_deleted = false
  AND shipment_order_number IS NOT NULL
  AND TRIM(shipment_order_number) != ''
  AND (
        tracking_number IN UNNEST(@tracking_numbers)
     OR return_tracking_number IN UNNEST(@tracking_numbers)
  )
"""


def shipments_orders_for_trackings(
    tracking_numbers: list[str],
    client,
    bigquery,
) -> dict[str, str]:
    """Map tracking number -> order, straight from bi.shipping_platform_shipments.

    The main fetch already joins this table, but only for the tracking number
    the ticket text carried. This is the same question asked again for the
    tracking numbers found inside a PDF attachment, so it is a small standalone
    query rather than a refactor of that join.

    Both the forward and the return tracking column are matched, and a tracking
    number naming more than one order is skipped rather than resolved to an
    arbitrary one -- the same rule the event-table lookups follow.
    """

    if not tracking_numbers or client is None:
        return {}

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "tracking_numbers", "STRING", tracking_numbers
            )
        ]
    )

    wanted = {value.strip() for value in tracking_numbers if value and value.strip()}
    orders_by_tracking: dict[str, set] = {}

    for row in client.query(SHIPMENTS_BY_TRACKING_QUERY, job_config=job_config).result():
        reference = _to_str(row.get("shipment_order_number")).strip()
        if not reference or not split_order_references(reference):
            continue
        for column in ("tracking_number", "return_tracking_number"):
            tracking = _to_str(row.get(column)).strip()
            if tracking and tracking in wanted:
                orders_by_tracking.setdefault(tracking, set()).add(reference)

    resolved: dict[str, str] = {}
    for tracking, references in orders_by_tracking.items():
        if len(references) != 1:
            print(
                f"  Tracking {tracking} names {len(references)} different orders "
                f"in shipping_platform_shipments ({', '.join(sorted(references))}); "
                "not resolved."
            )
            continue
        resolved[tracking] = next(iter(references))

    return resolved


def apply_pdf_attachment_order_lookup(
    df: pd.DataFrame,
    auth,
    base_url: str,
    client=None,
    til_client=None,
    bigquery=None,
) -> pd.DataFrame:
    """Last resolution step: the ticket's PDF attachments.

    Runs after the tracking lookup, prd_shipmentStatus and prd_tilEvents, so
    an attachment is only ever downloaded and OCR'd for a request no cheaper
    source could resolve.

    Two things are read out of the same text/OCR pass:

    * an **order reference**, which resolves the row outright;
    * every **tracking-shaped value**, used only when no order reference was
      found. Those tracking numbers then go through the same three tables the
      description's tracking number went through (shipping_platform_shipments
      -> prd_shipmentStatus -> prd_tilEvents).

    The second half exists because the tracking number quoted in a ticket's
    description is often the wrong one. On a UPS overgoods invoice the
    "Waybill Number" is the overgoods leg, which no TLG table ever received,
    while the ORIGINAL export tracking number sits in the "Invoice No." field
    (1ZX662916646304172 against 1Z7E38280412626229 on the examples that
    prompted this). Only the second resolves to an order.
    """

    if df.empty:
        return df

    # Downloading and OCR-ing attachments to find an order for a request that
    # will be excluded without a reply is wasted work. NOTE: a blocked order
    # prefix inside those attachments is therefore not detected, so such a
    # ticket is solved rather than left untouched.
    skip_no_reply = _env_bool(
        SKIP_PDF_SCAN_FOR_NO_REPLY_ENV,
        DEFAULT_SKIP_PDF_SCAN_FOR_NO_REPLY,
    )

    def _needs_lookup(row: Mapping[str, Any]) -> bool:
        if split_order_references(row.get("shipment_order_number")):
            return False
        if skip_no_reply and request_needs_no_reply(row):
            return False
        return _to_int(row.get("zendesk_ticket_id")) is not None

    pending_ticket_ids = []
    for _, row in df.iterrows():
        if not _needs_lookup(row):
            continue
        ticket_id = _to_int(row.get("zendesk_ticket_id"))
        if ticket_id is not None and ticket_id not in pending_ticket_ids:
            pending_ticket_ids.append(ticket_id)

    if not pending_ticket_ids:
        return df

    df = df.copy()

    print(
        f"PDF attachment scan: {len(pending_ticket_ids)} ticket(s) still "
        "without an order."
    )

    pdf_cache = _prefetch_pdf_order_references(
        pending_ticket_ids,
        auth=auth,
        base_url=base_url,
    )

    # --- pass A: an order reference in the PDF resolves the row outright ----
    resolved_from_reference = 0

    for index, row in df.iterrows():
        if not _needs_lookup(row):
            continue

        ticket_id = _to_int(row.get("zendesk_ticket_id"))
        references = list((pdf_cache.get(ticket_id) or {}).get("references") or [])
        if not references:
            continue

        _apply_resolved_order(
            df,
            index,
            references,
            ORDER_REFERENCE_SOURCE_PDF,
            has_tracking=bool(_row_tracking_numbers(row.get("extracted_tracking_number"))),
        )
        resolved_from_reference += 1

    print(
        f"  Resolved {resolved_from_reference} request(s) from an order "
        "reference in a PDF attachment."
    )

    # --- pass B: the PDF's tracking numbers go through the order tables -----
    still_pending = [
        (index, row) for index, row in df.iterrows() if _needs_lookup(row)
    ]
    if not still_pending:
        return df

    candidates: list[str] = []
    for index, row in still_pending:
        ticket_id = _to_int(row.get("zendesk_ticket_id"))
        for tracking in (pdf_cache.get(ticket_id) or {}).get("trackings") or []:
            if tracking not in candidates:
                candidates.append(tracking)

    if not candidates:
        return df

    print(
        f"  {len(still_pending)} request(s) still without an order; trying "
        f"{len(candidates)} tracking-shaped value(s) found in their PDFs."
    )

    # Same three sources, in the same order, as for the description's tracking
    # number. Each only sees what the previous one could not resolve.
    resolved_map: dict[str, str] = {}
    for label, lookup, lookup_client in (
        ("shipping_platform_shipments", shipments_orders_for_trackings, client),
        ("prd_shipmentStatus", shipment_status_orders_for_trackings, til_client),
        ("prd_tilEvents", til_events_orders_for_trackings, til_client),
    ):
        remaining = [value for value in candidates if value not in resolved_map]
        if not remaining or lookup_client is None or bigquery is None:
            continue
        try:
            found = lookup(remaining, lookup_client, bigquery)
        except Exception as error:  # noqa: BLE001 - a failed fallback must not
            # break the run: these rows were already heading for human review.
            print(f"    {label} lookup failed for PDF trackings: {error}")
            continue
        if found:
            print(f"    {label} resolved {len(found)} PDF tracking number(s).")
        resolved_map.update(found)

    if not resolved_map:
        print("    No order found for any tracking number read from a PDF.")
        return df

    resolved_from_tracking = 0

    for index, row in still_pending:
        ticket_id = _to_int(row.get("zendesk_ticket_id"))
        references: list[str] = []
        matched_trackings: list[str] = []
        for tracking in (pdf_cache.get(ticket_id) or {}).get("trackings") or []:
            reference = resolved_map.get(tracking)
            if not reference:
                continue
            if tracking not in matched_trackings:
                matched_trackings.append(tracking)
            if reference not in references:
                references.append(reference)

        if not references:
            continue

        # The PDF's tracking number is the one that actually identifies the
        # shipment, so it joins the row's tracking numbers: downstream
        # shipment selection and the export-tracking lookups key off this
        # column, and the description's number is known not to resolve.
        existing = _row_tracking_numbers(row.get("extracted_tracking_number"))
        merged_trackings = existing + [
            tracking for tracking in matched_trackings if tracking not in existing
        ]
        df.at[index, "extracted_tracking_number"] = ";".join(merged_trackings)

        _apply_resolved_order(
            df,
            index,
            references,
            ORDER_REFERENCE_SOURCE_PDF_TRACKING,
            has_tracking=True,
        )
        resolved_from_tracking += 1

    print(
        f"  Resolved {resolved_from_tracking} request(s) from a tracking "
        "number read out of a PDF attachment."
    )

    return df


def _apply_resolved_order(
    df: pd.DataFrame,
    index,
    references: list[str],
    source: str,
    *,
    has_tracking: bool,
) -> None:
    """Write one resolved order onto a row, with the usual selection rules."""

    resolved_order = ";".join(references)

    df.at[index, "shipment_order_number"] = resolved_order
    df.at[index, "extracted_order_reference"] = resolved_order
    df.at[index, "order_reference_source"] = source
    if all(is_shipment_shaped_order_reference(reference) for reference in references):
        df.at[index, "shipment_selection_mode"] = SHIPMENT_SELECTION_EXACT
    elif has_tracking:
        df.at[index, "shipment_selection_mode"] = SHIPMENT_SELECTION_BY_TRACKING
    else:
        df.at[index, "shipment_selection_mode"] = SHIPMENT_SELECTION_ALL
    df.at[index, "order_resolution_complete"] = True
    df.at[index, "order_processing_blocked"] = contains_blocked_order_reference(
        resolved_order
    )


def apply_cross_request_order_propagation(df: pd.DataFrame) -> pd.DataFrame:
    """Share an order already resolved in this run with rows that still lack one.

    Carriers open several tickets about the same shipment, and they do not all
    quote the same tracking numbers. A ticket whose body carries only the
    overgoods waybill resolves to nothing, while a sibling ticket that happens
    to name BOTH the waybill and the original export number resolves fine --
    and in doing so proves which order the waybill belongs to. That mapping
    was then thrown away at the end of the row loop.

    Observed on run 7dc4149f: 5009740_1 quoted
    "1ZX662916663848348;1Z7E37218619724391" and resolved to WL-EUA00589535
    through the shipments table, while four other tickets quoting only
    1ZX662916663848348 went to human review as "no order reference found" in
    the same run.

    Runs last, after every table and the PDF scan: it can only use what those
    produced, and it costs no API call. A tracking number that this run saw
    against two different orders is skipped rather than guessed.
    """

    if df.empty or "extracted_tracking_number" not in df.columns:
        return df

    orders_by_tracking: dict[str, set] = {}
    for _, row in df.iterrows():
        references = split_order_references(row.get("shipment_order_number"))
        if not references:
            continue
        resolved_order = ";".join(references)
        for tracking in _row_tracking_numbers(row.get("extracted_tracking_number")):
            orders_by_tracking.setdefault(tracking, set()).add(resolved_order)

    learned = {
        tracking: next(iter(orders))
        for tracking, orders in orders_by_tracking.items()
        if len(orders) == 1
    }
    if not learned:
        return df

    df = df.copy()
    propagated = 0

    for index, row in df.iterrows():
        if split_order_references(row.get("shipment_order_number")):
            continue

        for tracking in _row_tracking_numbers(row.get("extracted_tracking_number")):
            resolved_order = learned.get(tracking)
            if not resolved_order:
                continue

            references = split_order_references(resolved_order)
            if not references:
                continue

            print(
                f"  Request {row.get('request_id')}: tracking {tracking} was "
                f"resolved to {resolved_order} by another request in this run."
            )
            _apply_resolved_order(
                df,
                index,
                references,
                ORDER_REFERENCE_SOURCE_CROSS_REQUEST,
                has_tracking=True,
            )
            propagated += 1
            break

    if propagated:
        print(
            f"  Resolved {propagated} request(s) from an order another request "
            "in this run had already found."
        )

    return df


def apply_order_reference_fallback(
    df: pd.DataFrame,
    *,
    auth=None,
    base_url: str | None = None,
) -> pd.DataFrame:
    """Resolve an order for rows the tracking-number lookup could not resolve.

    Order of resolution, per the workflow specification:

    1. tracking number -> bi.shipping_platform_shipments (already done);
    2. order reference in the subject / description / request body;
    3. order reference inside a PDF attached to the ticket.

    The resolved reference is written into ``shipment_order_number`` so the
    existing GET_FULL_ORDER URL builder keeps working unchanged (it already
    turns both ``XX-YYA123...`` and ``XX-YY-123...`` into the order URL).
    ``shipment_selection_mode`` then tells response_data_extractor.py which
    shipment block(s) of the GET_FULL_ORDER payload to read.
    """

    df = df.copy()

    if "extracted_order_reference" not in df.columns:
        df["extracted_order_reference"] = ""

    session = requests.Session() if (auth and base_url) else None

    resolved_orders: list[Any] = []
    references_out: list[str] = []
    sources: list[str] = []
    modes: list[str] = []
    blocked_flags: list[bool] = []
    complete_flags: list[bool] = []

    resolved_from_text = 0
    resolved_from_pdf = 0
    unresolved = 0
    discarded_stored_orders = 0

    try:
        for _, row in df.iterrows():
            # NOTE: read through _to_str(). A missing value in a pandas column
            # is NaN, not None, and str(NaN) is the *truthy* string "nan" --
            # which would make every unresolved row look already resolved.
            existing_order = (_to_str(row.get("shipment_order_number")) or "").strip()
            # A run made before split_order_references() validated its input
            # could persist a literal "NAN" here, and a non-empty value in this
            # column otherwise means "already resolved" and marks the request
            # complete. Anything that is not a real order reference is dropped
            # so the row goes through resolution and, failing that, to a human.
            if existing_order and not split_order_references(existing_order):
                print(
                    f"  Ignoring unusable stored order {existing_order!r} on ticket "
                    f"{_to_str(row.get('zendesk_ticket_id'))}; re-resolving."
                )
                discarded_stored_orders += 1
                existing_order = ""

            tracking_value = (_to_str(row.get("extracted_tracking_number")) or "").strip()
            has_tracking = bool(
                tracking_value and tracking_value.upper() != "N/A"
            )

            references = split_order_references(row.get("extracted_order_reference"))
            source = ""
            mode = SHIPMENT_SELECTION_EXACT
            # True when every shipment the ticket talks about is covered by a
            # resolved order. False means at least one tracking number is still
            # unaccounted for, which must keep the request in human review.
            resolution_complete = True
            tracking_not_found = bool(
                _to_bool(row.get("tracking_not_found_in_shipping_platform_shipments"))
            )

            # Resolution precedence:
            #   subject_or_description -> tracking_lookup
            #     -> shipment_status_events -> til_events -> pdf_attachment
            # The first two are decided here. The last three are post-passes
            # over whatever is still unresolved when this loop ends, in that
            # order, so a PDF is only ever opened once every cheaper source
            # has been exhausted.
            if references:
                # An order reference written in the subject or the body is the
                # sender naming the order directly, so it outranks anything
                # derived from the tracking number. When the tracking lookup
                # also produced an order, both are kept: the ticket may be
                # about several shipments and dropping either one would lose
                # a shipment from the reply.
                source = ORDER_REFERENCE_SOURCE_TEXT
                merged = [
                    reference
                    for reference in split_order_references(existing_order)
                    if reference.upper() not in {
                        value.upper() for value in references
                    }
                    # Keep only tracking-resolved references that name a
                    # DIFFERENT order. Replaying the history shows that when
                    # the two disagree they are almost always the same order
                    # written at a different granularity -- stored
                    # "DG-EUD01670341" (one shipment) against a body saying
                    # "DG-EU-01670341" (the whole order), or a stored
                    # multi-shipment list against the body naming one of them.
                    # Merging those would mix shipment- and order-shaped
                    # references, which flips shipment_selection_mode and
                    # silently widens the reply to shipments the sender never
                    # asked about.
                    and order_number_from_order_reference(reference)
                    not in {
                        order_number_from_order_reference(value)
                        for value in references
                    }
                ]
                if merged:
                    references = [*references, *merged]
                    source = (
                        f"{ORDER_REFERENCE_SOURCE_TEXT}+"
                        f"{ORDER_REFERENCE_SOURCE_TRACKING}"
                    )

                resolved_order = ";".join(references)
                resolved_from_text += 1

                # The shape of the reference decides what is read:
                #
                # * shipment number (a letter after the prefix, e.g.
                #   DG-EUA01663254) identifies one specific shipment, so
                #   only that shipment block is read;
                # * order number (a dash after the prefix, e.g.
                #   DG-EU-01663254) covers the whole order, so every
                #   shipment block is read -- narrowed to the block
                #   carrying the ticket's tracking number when the ticket
                #   had one that the shipments table did not know.
                #
                # A mixed list keeps the row-level mode here; the extractor
                # then narrows each shipment-shaped reference on its own.
                if all(
                    is_shipment_shaped_order_reference(reference)
                    for reference in references
                ):
                    mode = SHIPMENT_SELECTION_EXACT
                elif has_tracking:
                    mode = SHIPMENT_SELECTION_BY_TRACKING
                else:
                    mode = SHIPMENT_SELECTION_ALL

            elif existing_order:
                # The tracking number resolved to a shipment in
                # bi.shipping_platform_shipments.
                source = ORDER_REFERENCE_SOURCE_TRACKING
                resolved_order = existing_order

                if tracking_not_found:
                    # Partial resolution: the ticket carries several tracking
                    # numbers and only some are in the shipments table, and it
                    # carries no reference of its own to fill the gap. The
                    # request stays incomplete and goes to human review unless
                    # one of the post-passes below resolves the rest.
                    resolution_complete = False

            else:
                resolved_order = ""
                unresolved += 1
                resolution_complete = False

            blocked = contains_blocked_order_reference(resolved_order, *references)

            resolved_orders.append(resolved_order or None)
            references_out.append(";".join(references))
            sources.append(source)
            modes.append(mode)
            blocked_flags.append(blocked)
            complete_flags.append(resolution_complete)
    finally:
        if session is not None:
            session.close()

    df["shipment_order_number"] = resolved_orders
    df["extracted_order_reference"] = references_out
    df["order_reference_source"] = sources
    df["shipment_selection_mode"] = modes
    df["order_processing_blocked"] = blocked_flags
    df["order_resolution_complete"] = complete_flags

    blocked_count = sum(1 for flag in blocked_flags if flag)
    incomplete_count = sum(1 for flag in complete_flags if not flag)
    if discarded_stored_orders:
        print(
            f"Discarded {discarded_stored_orders} unusable stored order(s) "
            "written by an earlier run; those rows were re-resolved."
        )
    print(
        "Order-reference fallback: "
        f"{resolved_from_text} resolved from subject/description, "
        f"{resolved_from_pdf} resolved from PDF attachments, "
        f"{unresolved} still without an order. "
        f"{incomplete_count} request(s) are incomplete and go to human review. "
        f"{blocked_count} request(s) are on a blocked prefix "
        f"({', '.join(BLOCKED_ORDER_REFERENCE_PREFIXES)}) and will be left untouched."
    )

    return df


def _max_tickets_per_run() -> int:
    return _env_int(
        MAX_TICKETS_PER_RUN_ENV,
        DEFAULT_MAX_TICKETS_PER_RUN,
        minimum=1,
        maximum=100_000,
    )


def limit_to_max_tickets(df: pd.DataFrame) -> pd.DataFrame:
    """Keep at most ``MAX_TICKETS_PER_RUN`` tickets, oldest first.

    The cap counts *tickets*, not rows: one ticket can carry several requests
    and they are kept together, because answering half a ticket's requests in
    this run and half in the next would produce a partial reply.

    Oldest first (by ticket submission, ties broken by ticket id) so the
    backlog drains in order and no ticket can starve across runs. Whatever is
    left over is simply picked up next run: the history de-duplication above
    means a completed request is never fetched again.
    """

    limit = _max_tickets_per_run()
    if df.empty or "zendesk_ticket_id" not in df.columns:
        return df

    ticket_ids = df["zendesk_ticket_id"].dropna().unique()
    if len(ticket_ids) <= limit:
        return df

    ordering = pd.DataFrame({"zendesk_ticket_id": df["zendesk_ticket_id"]})
    ordering["_submitted_at"] = pd.to_datetime(
        df.get("ticket_submission_timestamp"), utc=True, errors="coerce"
    )
    oldest_first = (
        ordering.dropna(subset=["zendesk_ticket_id"])
        .groupby("zendesk_ticket_id", dropna=True)["_submitted_at"]
        .min()
        .reset_index()
        # A ticket with no usable timestamp goes last rather than first, so a
        # bad timestamp can never crowd out a ticket we know to be older.
        .sort_values(
            by=["_submitted_at", "zendesk_ticket_id"], na_position="last"
        )
    )
    keep = set(oldest_first["zendesk_ticket_id"].head(limit))

    # Rows without a ticket id are kept rather than silently dropped; they are
    # not countable against a ticket cap.
    limited = df[
        df["zendesk_ticket_id"].isin(keep) | df["zendesk_ticket_id"].isna()
    ].copy()

    print(
        f"Limiting this run to {limit} ticket(s) of {len(ticket_ids)} "
        f"({len(ticket_ids) - limit} left for the next run): "
        f"{len(limited)} of {len(df)} request(s) kept. "
        f"Set {MAX_TICKETS_PER_RUN_ENV} to change the cap."
    )
    return limited


def fetch_tickets_to_active_table() -> int:
    client, bigquery = bigquery_client()
    email_to_category = load_requester_configuration(
        client,
        bigquery,
    )

    rows = fetch_ticket_rows(
        client=client,
        bigquery=bigquery,
        auth=zendesk_auth(),
        base_url=zendesk_base_url(),
        email_to_category=email_to_category,
    )

    if not rows:
        print("No records found. Clearing active ticket table.")
        replace_active_tickets(
            pd.DataFrame(columns=ACTIVE_TICKET_COLUMNS),
            client,
            bigquery,
        )
        return 0

    df = pd.DataFrame(rows)

    df["request_id"] = df.apply(
        lambda row: row.get("request_id")
        or build_request_id(
            row.get("zendesk_ticket_id"),
            row.get("request_number"),
        ),
        axis=1,
    )

    df.drop_duplicates(subset=["request_id"], inplace=True)

    # Skip only requests that are actually finished (replied, solved without
    # reply, handed to a human, excluded). Requests that were logged but never
    # answered are re-processed so the reply is finally posted.
    existing_ids = completed_history_request_ids(
        df["request_id"].dropna().unique().tolist(),
        client,
        bigquery,
    )

    if existing_ids:
        before_count = len(df)
        df = df[~df["request_id"].isin(existing_ids)].copy()
        print(
            f"Skipped {before_count - len(df)} requests already completed in "
            f"{HISTORY_TABLE}."
        )

    if df.empty:
        print("No new requests to process after history de-duplication.")
        replace_active_tickets(
            pd.DataFrame(columns=ACTIVE_TICKET_COLUMNS),
            client,
            bigquery,
        )
        return 0

    # Capped here, after the completed-request skip and before the expensive
    # enrichment: capping earlier would spend the budget on requests that are
    # already done, so a run could make no progress at all.
    df = limit_to_max_tickets(df)

    df = enrich_with_shipment_numbers(
        df,
        client,
        bigquery,
        auth=zendesk_auth(),
        base_url=zendesk_base_url(),
        # Separate project, separate service account. None when the secret is
        # missing, which only skips the last-resort lookup.
        til_client=til_bigquery_client(),
    )

    print(f"Replacing active ticket table with {len(df)} new requests...")
    replace_active_tickets(df, client, bigquery)
    print("Process completed.")

    return len(df)


# ============================================================================
# CLI
# ============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch active Zendesk requests into BigQuery or append final "
            "request results to the BigQuery history table."
        )
    )
    parser.add_argument(
        "--append-history",
        action="store_true",
        help=(
            "Append output/request_intent_results.jsonl.gz to the BigQuery "
            "history table instead of fetching Zendesk tickets."
        ),
    )
    parser.add_argument(
        "--history-path",
        default=str(REQUEST_INTENT_RESULTS_PATH),
        help="Path to the request_intent_results JSONL.GZ handoff file.",
    )

    args = parser.parse_args()

    if args.append_history:
        logged_rows = append_history_from_file(args.history_path)
        print(f"Logged {logged_rows} new rows from {args.history_path}.")
        return

    fetch_tickets_to_active_table()


if __name__ == "__main__":
    main()
