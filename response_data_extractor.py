"""
response_data_extractor.py

GET_FULL_ORDER API enrichment and PDF document generation for draft responses.

This module is intentionally the single place where response_generator.py gets
API-backed response data and generated customs documents:

- GET_FULL_ORDER client authentication and payload fetching;
- shipment-specific extraction from GET_FULL_ORDER responses;
- LOA / authorization-letter PDF generation;
- POA / power-of-attorney PDF generation.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import unescape as html_unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

try:  # PyMuPDF >= 1.24.3 exposes the `pymupdf` module; `fitz` is deprecated
    import pymupdf
except ImportError:  # pragma: no cover - older PyMuPDF releases only ship `fitz`
    import fitz as pymupdf
import requests
from requests.auth import HTTPBasicAuth

from customs_rules import (
    PLACEHOLDER,
    UPS_BROKERAGE_EMAIL,
    align_invoice_requested_data_with_tracking_context,
    extract_ups_code,
    collapse_document_embedded_requested_data,
    expand_first_returns_customs_clearance_bundle,
    filter_libera_esportazione_response_data,
    first_available_value,
    is_atr_certificate_mandate_request,
    is_no_action_carrier_notification,
    is_noreply_requester_email,
    is_noreply_autoreply_first_request,
    is_ups_uk_import_clearance_instructions_request,
    normalize_email,
    normalize_requested_data,
    only_libera_esportazione_regex_type,
    only_libera_esportazione_requested_data,
    strip_libera_esportazione_requested_data,
)
from order_reference_resolver import (
    ORDER_REFERENCE_PREFIX_SET,
    PREFIX_LENGTH as ORDER_REFERENCE_PREFIX_LENGTH,
    SHIPMENT_SELECTION_ALL,
    SHIPMENT_SELECTION_BY_TRACKING,
    SHIPMENT_SELECTION_EXACT,
    contains_blocked_order_reference,
    is_missing_value,
    is_shipment_shaped_order_reference,
)
from pipeline_io import REQUEST_INTENT_RESULTS_PATH, read_dataframe, write_dataframe

DEFAULT_BASE_URL = "https://zelda.thelevelgroup.com/return/api/v1"
CREDENTIALS_ENV = "GET_FULL_ORDER_API_CREDENTIALS"
BASE_URL_ENV = "GET_FULL_ORDER_API_BASE_URL"
AUTH_MODE_ENV = "GET_FULL_ORDER_AUTH_MODE"
TOKEN_URL_ENV = "GET_FULL_ORDER_API_TOKEN_URL"
TIMEOUT_ENV = "GET_FULL_ORDER_API_TIMEOUT_SECONDS"
INVOICE_DOWNLOAD_TIMEOUT_ENV = "INVOICE_PDF_DOWNLOAD_TIMEOUT_SECONDS"
INVOICE_DOWNLOAD_USER_AGENT_ENV = "INVOICE_PDF_DOWNLOAD_USER_AGENT"
INVOICE_DOWNLOAD_MAX_ATTEMPTS_ENV = "INVOICE_PDF_DOWNLOAD_MAX_ATTEMPTS"

FULL_ORDER_RESPONSE_COLUMNS = {
    "return_proforma_invoice": "full_order_return_proforma_invoice",
    "commercial_invoice": "full_order_commercial_invoice",
    "returned_items_confirmation": "full_order_returned_items",
    "customer_email": "full_order_customer_email",
    "customer_phone": "full_order_customer_phone",
    "customer_name": "full_order_customer_name",
    "shipping_address": "full_order_shipping_address",
    "shipped_at": "full_order_shipped_at",
    "country_code": "full_order_country_code",
    # Export (outbound) and return tracking numbers read straight out of the
    # GET_FULL_ORDER shipment block(s). These are the fallback used whenever
    # bi.shipping_platform_shipments could not resolve the ticket's tracking
    # number and the order was identified from an order reference found in the
    # subject/description or in a PDF attachment instead.
    "export_tracking_number": "full_order_export_tracking_number",
    "return_tracking_number": "full_order_return_tracking_number",
    # Carrier codes of the same two shippingLabels[] entries, so the
    # same-carrier / different-carrier reply can still be chosen correctly.
    "export_carrier_code": "full_order_export_carrier_code",
    "return_carrier_code": "full_order_return_carrier_code",
    # Which rule produced full_order_export_tracking_number. Internal/audit
    # only; never shown to the customer.
    "export_tracking_source": "full_order_export_tracking_source",
    "api_error": "full_order_api_error",
    "missing_shipment_orders_by_key": "full_order_missing_shipment_orders_by_key",
}

FULL_ORDER_DATA_KEYS = {
    "return_proforma_invoice",
    "commercial_invoice",
    "returned_items_confirmation",
    "customer_email",
    "customer_phone",
    "customer_name",
    "shipping_address",
}
# export_tracking_number is deliberately NOT in FULL_ORDER_DATA_KEYS (it is not
# rendered through full_order_data_value, and a missing value must not be
# treated like a missing invoice), but it does require the GET_FULL_ORDER call:
# when the tracking lookup failed, the shipment block is the only place the
# export tracking number can come from.
FULL_ORDER_LOOKUP_KEYS = FULL_ORDER_DATA_KEYS | {
    "authorization_letter",
    "export_tracking_number",
}
DOCUMENT_DATA_KEYS = {"authorization_letter", "power_of_attorney", "shipping_label"}
INVOICE_DOCUMENT_DATA_KEYS = {"return_proforma_invoice", "commercial_invoice"}

DOCUMENT_RESPONSE_COLUMNS = {
    "authorization_letter": "generated_authorization_letter_path",
    "power_of_attorney": "generated_power_of_attorney_path",
    "return_proforma_invoice": "generated_return_proforma_invoice_path",
    "commercial_invoice": "generated_commercial_invoice_path",
    "shipping_label": "generated_shipping_label_path",
    "document_error": "generated_document_error",
}

REQUESTED_DATA_ALIASES = {
    "ups_account": "ups_account_number",
    "ups_account_code": "ups_account_number",
    "ups_code": "ups_account_number",
    "tracking_number": "export_tracking_number",
    "export_tracking": "export_tracking_number",
    "returned_items": "returned_items_confirmation",
    "rpi": "return_proforma_invoice",
    "pri": "return_proforma_invoice",
    "invoice": "commercial_invoice",
    "commercial_invoice_required": "commercial_invoice",
    "invoice_correction": "corrected_invoice",
    "declaration_of_intent": "dichiarazione_di_libera_esportazione",
}

REQUESTED_DATA_SOURCE_COLUMNS = (
    "requested_data",
    "regex_requested_data",
    "regex_request_types",
    "request_types",
    "standard_reply_requested_data",
)

TEMPLATE_DIR = Path(os.getenv("PDF_TEMPLATE_DIR", "templates/pdf"))
LOA_TEMPLATE_PATH = Path(
    os.getenv(
        "LOA_TEMPLATE_PATH",
        str(TEMPLATE_DIR / "authorization_letter_template.pdf"),
    )
)
POA_TEMPLATE_PATH = Path(
    os.getenv(
        "POA_TEMPLATE_PATH",
        str(TEMPLATE_DIR / "power_of_attorney_template.pdf"),
    )
)
GENERATED_DOCUMENTS_DIR = Path(os.getenv("GENERATED_DOCUMENTS_DIR", "generated_documents"))
DOCUMENT_RESPONSE_STYLE = os.getenv("DOCUMENT_RESPONSE_STYLE", "markdown").strip().lower()
DOCUMENT_PUBLIC_BASE_URL = os.getenv("DOCUMENT_PUBLIC_BASE_URL", "").strip().rstrip("/")

# --- Shipping label (ZPL -> PDF) configuration -----------------------------
#
# The labels endpoint is a Logic App request trigger (see
# Labels_triggering_for_DGEUTST0001.py for a batch-trigger example against the
# same workflow). Given a shipment_order_number it returns a JSON payload with
# one "shipment" block per label (outbound + return); each block carries an
# isReturn flag and a base64-or-raw ZPL string in labelData.
SHIPPING_LABELS_API_URL = os.getenv("SHIPPING_LABELS_API_URL") or (
    "https://prod-09.northeurope.logic.azure.com/workflows/"
    "8fca928224de4819aa279973e08f9008/"
    "triggers/request/paths/invoke"
)
SHIPPING_LABELS_API_PARAMS = {
    "api-version": os.getenv("SHIPPING_LABELS_API_VERSION", "2016-10-01"),
    "sp": os.getenv("SHIPPING_LABELS_API_SP", "/triggers/request/run"),
    "sv": os.getenv("SHIPPING_LABELS_API_SV", "1.0"),
    "sig": os.getenv("SHIPPING_LABELS_API_SIG")
    or "5Ay_LV4SS9h3gw9zCfftJ0LabeCnfuig7BgkH9fcAU4",
}
SHIPPING_LABELS_API_TIMEOUT_SECONDS = int(
    os.getenv("SHIPPING_LABELS_API_TIMEOUT_SECONDS", "30")
)
# Labelary renders ZPL to a raster image at this printer density / label size.
ZPL_LABEL_RENDERER_URL = os.getenv(
    "ZPL_LABEL_RENDERER_URL",
    "https://api.labelary.com/v1/printers/8dpmm/labels/4x6/0/",
)


def _join_public_base_url(base_url: str, relative_path: str) -> str:
    if not base_url:
        return relative_path
    return f"{base_url}/{quote(relative_path, safe='/')}"


class DocumentGenerationError(RuntimeError):
    """Raised when a requested PDF cannot be generated safely."""


@dataclass(frozen=True)
class FullOrderCredentials:
    client_id: str
    client_secret: str

    @classmethod
    def from_environment(cls) -> "FullOrderCredentials | None":
        raw_credentials = os.getenv(CREDENTIALS_ENV, "").strip()
        if not raw_credentials:
            return None

        try:
            payload = json.loads(raw_credentials)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{CREDENTIALS_ENV} must be a JSON object with client_id and client_secret"
            ) from exc

        client_id = str(payload.get("client_id", "") or "").strip()
        client_secret = str(payload.get("client_secret", "") or "").strip()
        if not client_id or not client_secret:
            raise ValueError(
                f"{CREDENTIALS_ENV} must contain non-empty client_id and client_secret"
            )

        return cls(client_id=client_id, client_secret=client_secret)


class GetFullOrderClient:
    """Small requests-based client for Zelda's GET_FULL_ORDER endpoint."""

    def __init__(
        self,
        *,
        credentials: FullOrderCredentials | None = None,
        base_url: str | None = None,
        auth_mode: str | None = None,
        token_url: str | None = None,
        timeout: float | None = None,
        session: requests.Session | None = None,
    ):
        self.credentials = credentials if credentials is not None else FullOrderCredentials.from_environment()
        self.base_url = (base_url or os.getenv(BASE_URL_ENV) or DEFAULT_BASE_URL).rstrip("/")
        self.auth_mode = (auth_mode or os.getenv(AUTH_MODE_ENV) or "auto").strip().lower()
        self.token_url = (token_url or os.getenv(TOKEN_URL_ENV) or "").strip()
        self.timeout = timeout if timeout is not None else float(os.getenv(TIMEOUT_ENV, "60"))
        # An explicitly supplied session is used as-is (tests, single-threaded
        # callers). Otherwise each thread gets its own: requests.Session is not
        # thread-safe, and the parallel prefetch below drives several at once.
        self._shared_session = session
        self._thread_local = threading.local()
        self._access_token: str | None = None
        # The OAuth2 token is fetched lazily; without this lock every worker
        # would race to mint its own on the first call.
        self._token_lock = threading.Lock()

    @property
    def session(self) -> requests.Session:
        if self._shared_session is not None:
            return self._shared_session
        existing = getattr(self._thread_local, "session", None)
        if existing is None:
            existing = requests.Session()
            self._thread_local.session = existing
        return existing

    @property
    def is_configured(self) -> bool:
        return self.credentials is not None

    def order_url(self, shipment_order_number: object) -> str:
        """Return the first GET_FULL_ORDER URL attempted for a shipment order."""
        return self.order_urls(shipment_order_number)[0]

    def order_urls(self, shipment_order_number: object) -> list[str]:
        """Return GET_FULL_ORDER URL candidates in the order they should be tried."""
        brand = brand_from_shipment_order_number(shipment_order_number)
        order_numbers = order_number_candidates_from_shipment_order_number(
            shipment_order_number
        )
        if not brand or not order_numbers:
            raise ValueError(f"Invalid shipment_order_number: {shipment_order_number!r}")

        return [
            f"{self.base_url}/brands/{quote(brand, safe='')}/orders/"
            f"{quote(order_number, safe='')}"
            for order_number in order_numbers
        ]

    def get_order_payload(self, shipment_order_number: object) -> dict[str, Any]:
        if not self.credentials:
            raise RuntimeError(f"Missing {CREDENTIALS_ENV}")

        urls = self.order_urls(shipment_order_number)
        last_not_found_error: requests.HTTPError | None = None

        for position, url in enumerate(urls):
            try:
                return self._get_order_payload_from_url(url)
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                has_fallback_url = position < len(urls) - 1
                if status_code == 404 and has_fallback_url:
                    last_not_found_error = exc
                    continue
                raise

        if last_not_found_error is not None:
            raise last_not_found_error

        raise RuntimeError("No usable GET_FULL_ORDER URL was available")

    def _get_order_payload_from_url(self, url: str) -> dict[str, Any]:
        last_auth_error: requests.HTTPError | None = None

        for request_kwargs in self._auth_request_kwargs():
            response = self.session.get(url, timeout=self.timeout, **request_kwargs)

            if response.status_code in {401, 403} and self.auth_mode == "auto":
                last_auth_error = _http_error(response)
                continue

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("GET_FULL_ORDER API response was not a JSON object")
            return payload

        if last_auth_error is not None:
            raise last_auth_error

        raise RuntimeError("No usable GET_FULL_ORDER authentication strategy was configured")

    def _auth_request_kwargs(self) -> list[dict[str, Any]]:
        if not self.credentials:
            return [{}]

        mode = self.auth_mode
        if mode == "auto":
            strategies = []
            if self.token_url:
                strategies.append("oauth2")
            strategies.extend(["headers", "basic"])
        else:
            strategies = [mode]

        return [self._strategy_kwargs(strategy) for strategy in strategies]

    def _strategy_kwargs(self, strategy: str) -> dict[str, Any]:
        assert self.credentials is not None

        if strategy == "headers":
            return {
                "headers": {
                    "Accept": "application/json",
                    "client_id": self.credentials.client_id,
                    "client_secret": self.credentials.client_secret,
                    "X-Client-Id": self.credentials.client_id,
                    "X-Client-Secret": self.credentials.client_secret,
                }
            }

        if strategy == "basic":
            return {
                "headers": {"Accept": "application/json"},
                "auth": HTTPBasicAuth(
                    self.credentials.client_id,
                    self.credentials.client_secret,
                ),
            }

        if strategy == "oauth2":
            return {
                "headers": {
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._get_access_token()}",
                }
            }

        if strategy in {"none", "noauth"}:
            return {"headers": {"Accept": "application/json"}}

        raise ValueError(
            f"Unsupported {AUTH_MODE_ENV}={strategy!r}; use auto, headers, basic, oauth2, or none"
        )

    def _get_access_token(self) -> str:
        if self._access_token:
            return self._access_token
        with self._token_lock:
            if self._access_token:
                return self._access_token
            return self._fetch_access_token()

    def _fetch_access_token(self) -> str:
        if not self.token_url:
            raise ValueError(f"{TOKEN_URL_ENV} is required when {AUTH_MODE_ENV}=oauth2")
        if not self.credentials:
            raise RuntimeError(f"Missing {CREDENTIALS_ENV}")

        response = self.session.post(
            self.token_url,
            data={"grant_type": "client_credentials"},
            auth=HTTPBasicAuth(
                self.credentials.client_id,
                self.credentials.client_secret,
            ),
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        token = str(payload.get("access_token", "") or "").strip()
        if not token:
            raise ValueError("OAuth2 token response did not contain access_token")
        self._access_token = token
        return token


def _http_error(response: requests.Response) -> requests.HTTPError:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        return exc
    return requests.HTTPError(f"HTTP {response.status_code}", response=response)


def is_blank(value: object) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text.lower() in {"", "nan", "none", "null", "n/a", "na", PLACEHOLDER.lower()}


def safe_filename(value: object) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
    text = text.strip("._-")
    return text or "document"


def normalize_data_key(value: object) -> str:
    key = str(value or "").strip()
    return REQUESTED_DATA_ALIASES.get(key, key)


def normalize_requested_data_with_aliases(value: object) -> list[str]:
    cleaned: list[str] = []
    for item in normalize_requested_data(value):
        key = normalize_data_key(item)
        if key and key not in cleaned:
            cleaned.append(key)
    return cleaned


def _row_request_text(row: Mapping[str, Any]) -> str:
    return "\n".join(
        str(value or "")
        for value in (
            row.get("subject", ""),
            row.get("cleaned_request_body", ""),
            row.get("request_body", ""),
        )
        if not is_blank(value)
    )


def _request_type_sources_for_tracking_alignment(row: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for column in ("regex_request_types", "regex_requested_data", "request_types"):
        for key in normalize_requested_data_with_aliases(row.get(column)):
            if key and key not in values:
                values.append(key)
    return values


def _apply_response_requested_data_overrides(
    row: Mapping[str, Any],
    requested_data: object,
) -> list[str]:
    values = filter_libera_esportazione_response_data(
        requested_data,
        row.get("regex_request_types"),
    )
    values = align_invoice_requested_data_with_tracking_context(
        values,
        _request_type_sources_for_tracking_alignment(row),
        extracted_tracking_number=row.get("extracted_tracking_number"),
        shipment_tracking_number=row.get("shipment_tracking_number"),
        return_tracking_number=row.get("return_tracking_number"),
    )
    if is_atr_certificate_mandate_request(_row_request_text(row)):
        values = [value for value in values if value != "power_of_attorney"]
    return values


def requested_data_keys_from_row(row: Mapping[str, Any]) -> list[str]:
    """Return the effective requested_data keys for API/doc retrieval.

    The canonical ``requested_data`` column is the source of truth for what the
    response should provide.  Regex and standard-reply columns are only fallbacks
    for older/incomplete rows where the canonical field is empty.

    This prevents downloading both the commercial invoice and the return
    proforma invoice when one of the non-canonical helper columns still contains
    the other document type.
    """

    def _collapsed_unique_keys(value: Any) -> list[str]:
        keys: list[str] = []
        collapsed_keys = collapse_document_embedded_requested_data(
            value,
            ticket_category=row.get("ticket_category", ""),
            request_number=row.get("request_number", 1),
            requester_email=row.get("requester_email", ""),
            request_text=_row_request_text(row),
        )
        for key in collapsed_keys:
            key = normalize_data_key(key)
            if key and key not in keys:
                keys.append(key)
        return keys

    if only_libera_esportazione_regex_type(row.get("regex_request_types")):
        return []

    regex_trigger_keys: list[str] = []
    for column in ("regex_requested_data", "regex_request_types"):
        for key in _collapsed_unique_keys(row.get(column)):
            if key and key not in regex_trigger_keys:
                regex_trigger_keys.append(key)
    regex_trigger_keys = _apply_response_requested_data_overrides(row, regex_trigger_keys)

    def _expanded_keys(keys: list[str]) -> list[str]:
        expanded_keys = expand_first_returns_customs_clearance_bundle(
            keys,
            ticket_category=row.get("ticket_category", ""),
            request_number=row.get("request_number", 1),
            requester_email=row.get("requester_email", ""),
            trigger_requested_data=regex_trigger_keys or keys,
        )
        return _apply_response_requested_data_overrides(row, expanded_keys)

    raw_canonical_keys = normalize_requested_data_with_aliases(row.get("requested_data"))
    if only_libera_esportazione_requested_data(raw_canonical_keys):
        return []
    canonical_keys = _expanded_keys(
        _collapsed_unique_keys(strip_libera_esportazione_requested_data(raw_canonical_keys))
    )
    if canonical_keys:
        return canonical_keys

    fallback_keys: list[str] = []
    for column in REQUESTED_DATA_SOURCE_COLUMNS:
        if column == "requested_data":
            continue
        for key in _collapsed_unique_keys(row.get(column)):
            if key and key not in fallback_keys:
                fallback_keys.append(key)
    return _expanded_keys(fallback_keys)


def _email_domain(email: object) -> str:
    normalized = normalize_email(email)
    if "@" not in normalized:
        return ""
    return normalized.rsplit("@", 1)[-1]


def is_ups_requester_email(email: object) -> bool:
    normalized = normalize_email(email)
    domain = _email_domain(email)
    return normalized == UPS_BROKERAGE_EMAIL or domain == "ups.com" or domain.endswith(".ups.com")


def row_order_processing_blocked(row: Mapping[str, Any]) -> bool:
    """True for RC-US / RC-EU / LN-EU / LN-US orders, which are never processed."""

    flag = row.get("order_processing_blocked")
    if isinstance(flag, bool):
        if flag:
            return True
    elif not is_blank(flag) and str(flag).strip().lower() in {"true", "1", "yes", "y"}:
        return True

    return contains_blocked_order_reference(
        row.get("shipment_order_number"),
        row.get("extracted_order_reference"),
    )


def row_needs_full_order_lookup(
    row: Mapping[str, Any],
    requested_data: list[str] | None = None,
) -> bool:
    if row_order_processing_blocked(row):
        return False
    if is_noreply_requester_email(row.get("requester_email")):
        return False
    if is_noreply_autoreply_first_request(row.get("requester_email"), row.get("request_number", 1)):
        return False

    request_text = _row_request_text(row)
    if is_no_action_carrier_notification(request_text):
        return False
    if is_ups_uk_import_clearance_instructions_request(request_text):
        return False

    requested_data = requested_data if requested_data is not None else requested_data_keys_from_row(row)
    requested_set = set(requested_data)

    if requested_set & FULL_ORDER_LOOKUP_KEYS:
        return True

    # The standard UPS-account draft includes an LOA.  The LOA export date comes
    # from GET_FULL_ORDER.shipments[].shippedAt, so the lookup is still needed.
    return requested_data == ["ups_account_number"] and is_ups_requester_email(row.get("requester_email"))


def row_needs_document_generation(
    row: Mapping[str, Any],
    requested_data: list[str] | None = None,
) -> bool:
    if row_order_processing_blocked(row):
        return False
    if is_noreply_requester_email(row.get("requester_email")):
        return False
    if is_noreply_autoreply_first_request(row.get("requester_email"), row.get("request_number", 1)):
        return False

    request_text = _row_request_text(row)
    if is_no_action_carrier_notification(request_text):
        return False
    if is_ups_uk_import_clearance_instructions_request(request_text):
        return False

    requested_data = requested_data if requested_data is not None else requested_data_keys_from_row(row)
    requested_set = set(requested_data)

    if requested_set & (DOCUMENT_DATA_KEYS | INVOICE_DOCUMENT_DATA_KEYS):
        return True

    # The standard UPS-account reply includes the generated LOA even when
    # authorization_letter was not explicitly requested as standalone data.
    return requested_data == ["ups_account_number"] and is_ups_requester_email(row.get("requester_email"))


def _initialize_response_data_columns(df):
    for column in FULL_ORDER_RESPONSE_COLUMNS.values():
        if column not in df.columns:
            df[column] = None
    for column in DOCUMENT_RESPONSE_COLUMNS.values():
        if column not in df.columns:
            df[column] = None
    return df


def row_has_full_order_attempt(row: Mapping[str, Any]) -> bool:
    return any(not is_blank(row.get(column)) for column in FULL_ORDER_RESPONSE_COLUMNS.values())


# Values a NULL/absent order column can stringify into. None of these is a
# real order number, and each one produced a /brands/NA/orders/<junk> call.
_NULL_ORDER_SENTINELS = {"NAN", "NONE", "NULL", "NAT", "N/A", "NA"}
FULL_ORDER_LOOKUP_WORKERS_ENV = "FULL_ORDER_LOOKUP_WORKERS"
DEFAULT_FULL_ORDER_LOOKUP_WORKERS = 8


def _full_order_lookup_workers(call_count: int) -> int:
    if call_count <= 0:
        return 0
    try:
        configured = int(
            os.getenv(FULL_ORDER_LOOKUP_WORKERS_ENV, "")
            or DEFAULT_FULL_ORDER_LOOKUP_WORKERS
        )
    except ValueError:
        configured = DEFAULT_FULL_ORDER_LOOKUP_WORKERS
    return max(1, min(configured, 32, call_count))


def _full_order_lookup_plan(df, index) -> tuple[list[str], str, tuple[str, ...]]:
    """Return (shipment_orders, selection_mode, tracking_numbers) for one row.

    Single definition, used by both the parallel prefetch and the main loop, so
    the two can never disagree about which GET_FULL_ORDER calls a row needs.
    """

    selection_mode = str(
        df.at[index, "shipment_selection_mode"]
        if "shipment_selection_mode" in df.columns
        else ""
    ).strip().lower() or SHIPMENT_SELECTION_EXACT
    row_tracking_numbers = tuple(
        split_tracking_numbers(
            df.at[index, "extracted_tracking_number"]
            if "extracted_tracking_number" in df.columns
            else ""
        )
    )
    raw_order = (
        df.at[index, "shipment_order_number"]
        if "shipment_order_number" in df.columns
        else ""
    )
    # A NULL column arrives from BigQuery as a float NaN, and NaN is truthy:
    # str(nan) is "nan". Left unguarded that became the order "NAN", which
    # brand_from_shipment_order_number() turns into brand "NA" -- producing
    # GET_FULL_ORDER calls to /brands/NA/orders/NAN that 500 and then report
    # the row's real data as missing.
    if is_missing_value(raw_order):
        return [], selection_mode, row_tracking_numbers

    shipment_orders = [
        x.strip()
        for x in str(raw_order).split(";")
        if x.strip() and x.strip().upper() not in _NULL_ORDER_SENTINELS
    ]
    return shipment_orders, selection_mode, row_tracking_numbers


def _full_order_effective_mode(shipment_order_number: str, selection_mode: str) -> str:
    # A shipment-shaped reference (a letter after the prefix, e.g.
    # DG-EUA01663254) names one specific shipment, so it is always read on its
    # own even when the row's mode is "all"/"by_tracking" because of another
    # reference on the same ticket. An order-shaped reference (DG-EU-01663254)
    # keeps the row's mode.
    return (
        SHIPMENT_SELECTION_EXACT
        if is_shipment_shaped_order_reference(shipment_order_number)
        else selection_mode
    )


def _full_order_cache_key(
    shipment_order_number: str,
    effective_mode: str,
    row_tracking_numbers: tuple[str, ...],
) -> tuple[str, str, tuple[str, ...]]:
    return (
        shipment_order_number.upper(),
        effective_mode,
        row_tracking_numbers if effective_mode == SHIPMENT_SELECTION_BY_TRACKING else (),
    )


def _prefetch_full_order_payloads(
    df,
    lookup_indices: list,
    *,
    api_client: GetFullOrderClient,
) -> dict[tuple[str, str, tuple[str, ...]], dict[str, Any]]:
    """Fill the GET_FULL_ORDER cache for every call the main loop will make.

    Each distinct (order, mode, tracking) key is one independent HTTP call, so
    they run concurrently. The main loop then finds every key already present
    and performs no I/O, leaving its logic and its write order untouched.
    """

    pending: dict[tuple[str, str, tuple[str, ...]], tuple[str, str, tuple[str, ...]]] = {}
    for index in lookup_indices:
        shipment_orders, selection_mode, row_tracking_numbers = _full_order_lookup_plan(
            df, index
        )
        for shipment_order_number in shipment_orders:
            effective_mode = _full_order_effective_mode(
                shipment_order_number, selection_mode
            )
            cache_key = _full_order_cache_key(
                shipment_order_number, effective_mode, row_tracking_numbers
            )
            pending.setdefault(
                cache_key,
                (shipment_order_number, effective_mode, row_tracking_numbers),
            )

    cache: dict[tuple[str, str, tuple[str, ...]], dict[str, Any]] = {}
    if not pending:
        return cache

    ordered_keys = list(pending)
    workers = _full_order_lookup_workers(len(ordered_keys))
    print(
        f"Fetching {len(ordered_keys)} GET_FULL_ORDER payload(s) "
        f"with {workers} worker(s)."
    )

    def fetch(cache_key):
        shipment_order_number, effective_mode, row_tracking_numbers = pending[cache_key]
        try:
            return cache_key, fetch_shipment_order_data(
                shipment_order_number,
                client=api_client,
                selection_mode=effective_mode,
                tracking_numbers=row_tracking_numbers,
            ), None
        except Exception as exc:  # noqa: BLE001 - one bad order must not stop the rest
            return cache_key, {
                FULL_ORDER_RESPONSE_COLUMNS["api_error"]: str(exc)
            }, (
                "WARNING: GET_FULL_ORDER lookup failed for "
                f"shipment_order_number={shipment_order_number}: {exc}"
            )

    messages: dict[Any, str] = {}
    progress_step = max(1, len(ordered_keys) // 10)
    if workers <= 1:
        results = [fetch(key) for key in ordered_keys]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch, key) for key in ordered_keys]
            for future in as_completed(futures):
                results.append(future.result())
                if len(results) % progress_step == 0 or len(results) == len(ordered_keys):
                    print(
                        f"  ...fetched {len(results)}/{len(ordered_keys)} "
                        "GET_FULL_ORDER payload(s)."
                    )

    for cache_key, value, message in results:
        cache[cache_key] = value
        if message:
            messages[cache_key] = message

    # Replay warnings in call order, not completion order, so the log stays
    # deterministic between runs.
    for cache_key in ordered_keys:
        if cache_key in messages:
            print(messages[cache_key])

    return cache


def enrich_dataframe_with_full_order_data(
    df,
    *,
    client: GetFullOrderClient | None = None,
    fetch_all: bool = False,
    skip_existing: bool = True,
):
    """Enrich request rows with GET_FULL_ORDER values and return the DataFrame.

    This function is used by the standalone workflow step.  It writes API data
    back to the local handoff file before response_generator.py builds the final
    draft, making response_data_extractor.py an explicit part of the pipeline.
    """

    if df.empty:
        return df

    df = _initialize_response_data_columns(df.copy())
    requested_data_by_index = {
        index: requested_data_keys_from_row(row)
        for index, row in df.iterrows()
    }

    lookup_indices = []
    for index, row in df.iterrows():
        if skip_existing and row_has_full_order_attempt(row):
            continue
        if row_order_processing_blocked(row):
            # Blocked order prefix: the ticket is left exactly as it is, so no
            # GET_FULL_ORDER call is made even with --fetch-all.
            continue
        if fetch_all:
            needs_lookup = not is_blank(row.get("shipment_order_number"))
        else:
            needs_lookup = row_needs_full_order_lookup(row, requested_data_by_index[index])
        if needs_lookup:
            lookup_indices.append(index)

    if not lookup_indices:
        print("No rows need GET_FULL_ORDER enrichment.")
        return df

    try:
        api_client = client or GetFullOrderClient()
    except Exception as exc:
        print(f"WARNING: GET_FULL_ORDER client could not be configured: {exc}")
        for index in lookup_indices:
            df.at[index, FULL_ORDER_RESPONSE_COLUMNS["api_error"]] = str(exc)
        return df

    if not api_client.is_configured:
        message = "Missing GET_FULL_ORDER_API_CREDENTIALS"
        print(f"WARNING: {message}; API-backed response data will be unavailable.")
        for index in lookup_indices:
            df.at[index, FULL_ORDER_RESPONSE_COLUMNS["api_error"]] = message
        return df

    # NOTE: this block previously lived (unreachable) inside the
    # `if not api_client.is_configured:` branch above, after its `return df`.
    # That meant GET_FULL_ORDER data was never fetched once the API client
    # *was* configured -- the root cause of tickets with multiple
    # shipment_order_number values only ever getting one invoice attached.
    # Every GET_FULL_ORDER call the loop below needs, issued concurrently.
    # The loop then only reads the cache, so its order of writes into df and
    # its per-row logic are exactly as before.
    cache = _prefetch_full_order_payloads(df, lookup_indices, api_client=api_client)

    for index in lookup_indices:

        # How to pick shipment blocks out of the GET_FULL_ORDER payload. Rows
        # resolved from the tracking number keep the historic "exact" mode;
        # rows resolved from an order reference read the tracking-matched block
        # or every shipment. See order_reference_resolver for the modes.
        shipment_orders, selection_mode, row_tracking_numbers = _full_order_lookup_plan(
            df, index
        )

        if not shipment_orders:
            df.at[index, FULL_ORDER_RESPONSE_COLUMNS["api_error"]] = (
                "Missing shipment_order_number for GET_FULL_ORDER lookup"
            )
            continue

        combined = {}
        missing_by_key: dict[str, list[str]] = {}

        for shipment_order_number in shipment_orders:

            # A shipment-shaped reference (a letter after the prefix, e.g.
            # DG-EUA01663254) names one specific shipment, so it is always read
            # on its own even when the row's mode is "all"/"by_tracking"
            # because of another reference on the same ticket. An order-shaped
            # reference (DG-EU-01663254) keeps the row's mode.
            effective_mode = _full_order_effective_mode(
                shipment_order_number, selection_mode
            )

            cache_key = _full_order_cache_key(
                shipment_order_number, effective_mode, row_tracking_numbers
            )

            # The prefetch fills every key this loop asks for, so this branch
            # is a safety net rather than the normal path.
            if cache_key not in cache:
                try:
                    cache[cache_key] = fetch_shipment_order_data(
                        shipment_order_number,
                        client=api_client,
                        selection_mode=effective_mode,
                        tracking_numbers=row_tracking_numbers,
                    )
                except Exception as exc:
                    print(
                        "WARNING: GET_FULL_ORDER lookup failed for "
                        f"shipment_order_number={shipment_order_number}: {exc}"
                    )
                    cache[cache_key] = {
                        FULL_ORDER_RESPONSE_COLUMNS["api_error"]: str(exc)
                    }

            result = cache[cache_key]

            # Record, per invoice/RPI data key, exactly which shipment(s) had
            # no value for that key in this lookup -- whether because the
            # whole GET_FULL_ORDER call failed or because the payload simply
            # didn't contain that field. This lets the response builder name
            # the exact missing shipment(s) instead of guessing.
            for data_key in INVOICE_DOCUMENT_DATA_KEYS:
                column = FULL_ORDER_RESPONSE_COLUMNS[data_key]
                if is_blank(result.get(column)):
                    missing_by_key.setdefault(data_key, []).append(shipment_order_number)

            for column, value in result.items():

                if column not in df.columns:
                    df[column] = None

                if is_blank(value):
                    continue

                existing = combined.get(column)

                if is_blank(existing):
                    combined[column] = value
                else:
                    values = [
                        x.strip()
                        for x in str(existing).split(";")
                        if x.strip()
                    ]

                    if str(value) not in values:
                        values.append(str(value))

                    combined[column] = ";".join(values)

        for column, value in combined.items():
            df.at[index, column] = value

        missing_column = FULL_ORDER_RESPONSE_COLUMNS["missing_shipment_orders_by_key"]
        if missing_column not in df.columns:
            df[missing_column] = None
        # Only keep entries where at least one (but not all) shipments are
        # missing the key -- if every shipment is missing it, the existing
        # "column is entirely blank" check already blocks the reply and a
        # partial-missing note would be redundant/misleading.
        partial_missing_by_key = {
            data_key: order_numbers
            for data_key, order_numbers in missing_by_key.items()
            if order_numbers and len(order_numbers) < len(shipment_orders)
        }
        df.at[index, missing_column] = (
            json.dumps(partial_missing_by_key) if partial_missing_by_key else None
        )

    print(f"GET_FULL_ORDER enrichment attempted for {len(lookup_indices)} row(s).")
    return df


def document_generation_already_failed(row, data_key: str) -> bool:
    """True when this row already recorded a failure for this document type.

    generate_documents_for_dataframe() runs once in response_data_extractor
    and again in response_generator, and the reply builder falls back to
    generating a missing document a third time. A *successful* document short-
    circuits all of that, because its path is stored in the row. A failure
    stored nothing, so every later pass retried it -- three calls to the
    labels API per request, three identical warnings, and three times the
    latency, all to reach the same answer.

    The recorded error is that missing memory. It is per run: the next run
    starts from a fresh row and will try again, which is what you want for a
    transient API outage.
    """

    error_value = row.get(DOCUMENT_RESPONSE_COLUMNS["document_error"])
    if is_blank(error_value):
        return False
    return f"{data_key}:" in str(error_value)


def _append_document_error(df, index: object, message: str) -> None:
    error_column = DOCUMENT_RESPONSE_COLUMNS["document_error"]
    existing_error = df.at[index, error_column] if error_column in df.columns else None
    if not is_blank(existing_error):
        message = f"{existing_error}; {message}"
    df.at[index, error_column] = message


def _backfill_shipped_at_from_events(df, index: object) -> None:
    """Fill a blank full_order_shipped_at from the PACKED/SHIPPED events.

    GET_FULL_ORDER leaves the export date missing in two different ways: the
    order 404s (and the 404 fallback has already supplied the date), or the
    call succeeds but the shipment block carries no shippedAt at all. Only the
    first case is covered by fetch_shipment_order_data, yet the LOA fails
    identically in both -- "Cannot generate LOA without full_order_shipped_at".

    The carrier events know when the parcel left either way, so they are read
    here as well. The lookup is cached per order, so a row whose date already
    came from the 404 fallback costs nothing, and a row with a usable
    shippedAt never queries at all.
    """

    column = FULL_ORDER_RESPONSE_COLUMNS["shipped_at"]
    if column in df.columns and not is_blank(df.at[index, column]):
        return

    raw_orders = (
        df.at[index, "shipment_order_number"]
        if "shipment_order_number" in df.columns
        else None
    )
    shipment_orders = [part.strip() for part in str(raw_orders or "").split(";") if part.strip()]

    for shipment_order_number in shipment_orders:
        try:
            events = tracking_numbers_from_shipment_status(shipment_order_number)
        except Exception as exc:  # noqa: BLE001 - the LOA simply stays blocked
            print(
                "  prd_shipmentStatus export-date lookup failed for "
                f"{shipment_order_number}: {exc}"
            )
            return

        value = events.get(column)
        if is_blank(value):
            continue

        if column not in df.columns:
            df[column] = None
        df.at[index, column] = value
        print(
            f"  Export date for row {index} taken from the prd_shipmentStatus "
            f"events ({shipment_order_number}): {value}."
        )
        return


def generate_documents_for_dataframe(df, *, force: bool = False):
    if df.empty:
        return df

    df = _initialize_response_data_columns(df.copy())
    generated = 0

    for index, row in df.iterrows():
        requested_data = requested_data_keys_from_row(row)
        if not row_needs_document_generation(row, requested_data):
            continue

        needs_loa = "authorization_letter" in requested_data or (
            requested_data == ["ups_account_number"] and is_ups_requester_email(row.get("requester_email"))
        )
        needs_poa = "power_of_attorney" in requested_data
        needs_shipping_label = "export_tracking_number" in requested_data

        for invoice_key in sorted(set(requested_data) & INVOICE_DOCUMENT_DATA_KEYS):
            column = DOCUMENT_RESPONSE_COLUMNS[invoice_key]
            existing_path = row.get(column)
            if not force and not is_blank(existing_path) and document_path_exists(existing_path):
                continue

            source_column = FULL_ORDER_RESPONSE_COLUMNS[invoice_key]
            source_link = row.get(source_column)
            if is_blank(source_link):
                # The full-order missing-value check will block the automatic reply.
                continue

            try:
                row_data = df.loc[index].to_dict()
                links = [
                    x.strip()
                    for x in str(source_link).split(";")
                    if x.strip()
                ]

                generated_files = []

                for link in links:
                    generated_files.append(
                        download_invoice_pdf(
                            link,
                            invoice_key,
                            row=row_data,
                        )
                    )

                df.at[index, column] = ";".join(generated_files)
                generated += 1
            except Exception as exc:
                message = f"{invoice_key}: {exc}"
                print(f"WARNING: Could not download {invoice_key} PDF for row {index}: {exc}")
                _append_document_error(df, index, message)

        if needs_loa:
            column = DOCUMENT_RESPONSE_COLUMNS["authorization_letter"]
            existing_path = row.get(column)
            if force or is_blank(existing_path) or not document_path_exists(existing_path):
                # The export date is the one LOA field GET_FULL_ORDER often
                # cannot supply -- either because the order 404'd or because
                # the shipment block simply carries no shippedAt -- and
                # without it the letter cannot be generated at all. The
                # carrier events know when the parcel left, so fill the column
                # from them before giving up. Written back into df so the
                # response builder's authorization_letter_export_date check
                # sees it too.
                _backfill_shipped_at_from_events(df, index)
                try:
                    row_data = df.loc[index].to_dict()
                    df.at[index, column] = generate_authorization_letter(row_data)
                    generated += 1
                except Exception as exc:
                    message = f"authorization_letter: {exc}"
                    print(f"WARNING: Could not generate authorization_letter for row {index}: {exc}")
                    _append_document_error(df, index, message)

        if needs_poa and not (
            not force and document_generation_already_failed(row, "power_of_attorney")
        ):
            column = DOCUMENT_RESPONSE_COLUMNS["power_of_attorney"]
            existing_path = row.get(column)
            if force or is_blank(existing_path) or not document_path_exists(existing_path):
                try:
                    row_data = df.loc[index].to_dict()
                    df.at[index, column] = generate_power_of_attorney(row_data)
                    generated += 1
                except Exception as exc:
                    message = f"power_of_attorney: {exc}"
                    print(f"WARNING: Could not generate power_of_attorney for row {index}: {exc}")
                    _append_document_error(df, index, message)

        if needs_shipping_label and not (
            not force and document_generation_already_failed(row, "shipping_label")
        ):
            column = DOCUMENT_RESPONSE_COLUMNS["shipping_label"]
            existing_path = row.get(column)
            if force or is_blank(existing_path) or not document_path_exists(str(existing_path).split(";")[0]):
                try:
                    row_data = df.loc[index].to_dict()
                    df.at[index, column] = generate_shipping_label_document(row_data)
                    generated += 1
                except Exception as exc:
                    message = f"shipping_label: {exc}"
                    print(f"WARNING: Could not generate shipping_label PDF for row {index}: {exc}")
                    _append_document_error(df, index, message)

    print(f"Generated or downloaded {generated} PDF document(s).")
    return df


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich request-intent rows with GET_FULL_ORDER data and generated PDF documents."
    )
    parser.add_argument(
        "--input",
        default=str(REQUEST_INTENT_RESULTS_PATH),
        help="Compressed JSONL handoff to read. Defaults to output/request_intent_results.jsonl.gz.",
    )
    parser.add_argument(
        "--output",
        default=str(REQUEST_INTENT_RESULTS_PATH),
        help="Compressed JSONL handoff to write. Defaults to the same request-intent file.",
    )
    parser.add_argument(
        "--fetch-all",
        action="store_true",
        help="Call GET_FULL_ORDER for every row with a shipment_order_number, not only rows whose requested_data needs it.",
    )
    parser.add_argument(
        "--skip-documents",
        action="store_true",
        help="Skip LOA/POA PDF generation.",
    )
    parser.add_argument(
        "--force-documents",
        action="store_true",
        help="Regenerate LOA/POA PDFs even when generated paths already exist in the handoff.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    df = read_dataframe(args.input)

    if df.empty:
        print("No request-intent rows found. Nothing to enrich.")
        write_dataframe(df, args.output)
        return

    df = enrich_dataframe_with_full_order_data(df, fetch_all=args.fetch_all)
    if not args.skip_documents:
        df = generate_documents_for_dataframe(df, force=args.force_documents)

    write_dataframe(df, args.output)
    print(f"Response data extraction completed for {len(df)} row(s). Wrote {args.output}.")


def _normalize_shipment_order_number(shipment_order_number: object) -> str:
    return str(shipment_order_number or "").strip().upper()


def brand_from_shipment_order_number(shipment_order_number: object) -> str:
    value = _normalize_shipment_order_number(shipment_order_number)
    return value[:2] if len(value) >= 2 else ""


def _looks_like_brand_prefixed_shipment_order(value: str) -> bool:
    if re.match(r"^[A-Z0-9]{2}-.{3,}$", value):
        return True
    # A few known order prefixes have no internal dash (e.g. CVXEU), so the
    # generic "XX-" shape above does not recognize them. They still follow the
    # same rule: 5-character prefix, then a letter (shipment) or "-" (order).
    return (
        len(value) > ORDER_REFERENCE_PREFIX_LENGTH
        and value[:ORDER_REFERENCE_PREFIX_LENGTH] in ORDER_REFERENCE_PREFIX_SET
    )


def _has_euf_or_usf_market_code(value: str) -> bool:
    """Return True only when EUF/USF is the market code after the brand prefix."""
    return bool(re.match(r"^[A-Z0-9]{2}-(?:EUF|USF).+$", value))


def _replace_sixth_character_with_dash(value: str) -> str:
    """Return the GET_FULL_ORDER URL shape by replacing character 6 with '-'."""
    if not _looks_like_brand_prefixed_shipment_order(value):
        return value
    # The string is 1-indexed in the business rule: XX-US?..., so character 6
    # is index 5 in Python. Existing XX-US-... / XX-EU-... values stay the same.
    return f"{value[:5]}-{value[6:]}" if len(value) >= 6 else value


def order_number_candidates_from_shipment_order_number(
    shipment_order_number: object,
) -> list[str]:
    """
    Convert a shipment_order_number into GET_FULL_ORDER URL candidates.

    The API order URL must use the shipment order number with the 6th
    character replaced by '-'. The only exception is EUF/USF market-code values:
    try those exact values first, then retry with EU-/US- if the exact URL is not
    found.

    Examples:
        DG-EUA01663254 -> [DG-EU-01663254]
        DG-EUB01614772 -> [DG-EU-01614772]
        DG-EUC01669099 -> [DG-EU-01669099]
        DG-USA11590412 -> [DG-US-11590412]
        DG-USB11591156 -> [DG-US-11591156]
        DG-USC11589641 -> [DG-US-11589641]
        DG-EU-01663254 -> [DG-EU-01663254]
        DG-US-11590412 -> [DG-US-11590412]
        DG-EUF01663254 -> [DG-EUF01663254, DG-EU-01663254]
        DG-USF11590412 -> [DG-USF11590412, DG-US-11590412]
    """

    value = _normalize_shipment_order_number(shipment_order_number)
    if not value:
        return []

    normalized_for_url = _replace_sixth_character_with_dash(value)
    if _has_euf_or_usf_market_code(value):
        return list(dict.fromkeys([value, normalized_for_url]))
    return [normalized_for_url]


def order_number_from_shipment_order_number(shipment_order_number: object) -> str:
    """Return the first GET_FULL_ORDER order-number candidate."""
    candidates = order_number_candidates_from_shipment_order_number(
        shipment_order_number
    )
    return candidates[0] if candidates else ""


def shipment_order_number_for_shipment_block_lookup(shipment_order_number: object) -> str:
    """Return the shipmentOrderNumber shape used inside GET_FULL_ORDER payloads."""
    value = _normalize_shipment_order_number(shipment_order_number)
    if "-EU-" in value:
        return value.replace("-EU-", "-EUA", 1)
    if "-US-" in value:
        return value.replace("-US-", "-USA", 1)
    return value


def shipment_order_number_lookup_candidates(shipment_order_number: object) -> list[str]:
    """Return possible shipment-block identifiers for the current payload."""
    value = _normalize_shipment_order_number(shipment_order_number)
    if not value:
        return []

    candidates = [shipment_order_number_for_shipment_block_lookup(value)]
    if _has_euf_or_usf_market_code(value):
        fallback_order_number = _replace_sixth_character_with_dash(value)
        candidates.append(shipment_order_number_for_shipment_block_lookup(fallback_order_number))
        candidates.append(fallback_order_number)

    return [candidate for candidate in dict.fromkeys(candidates) if candidate]


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _get_any(mapping: Mapping[str, Any] | None, names: Iterable[str]) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    for name in names:
        if name in mapping:
            return mapping.get(name)
    return None


def _find_first_key(value: Any, target_key: str) -> Any:
    if isinstance(value, Mapping):
        if target_key in value:
            return value[target_key]
        for child in value.values():
            found = _find_first_key(child, target_key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_first_key(child, target_key)
            if found is not None:
                return found
    return None


def _nested_get(mapping: Mapping[str, Any] | None, *path: str) -> Any:
    current: Any = mapping
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _shipment_order_value(shipment: Mapping[str, Any]) -> str:
    value = _get_any(
        shipment,
        (
            "shipmentOrderNumber",
            "shipment_order_number",
            "shipmentOrderNo",
            "shipment_order_no",
        ),
    )
    return str(value or "").strip().upper()


def _shipments_from_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    shipments = payload.get("shipments")
    if shipments is None and isinstance(payload.get("order"), Mapping):
        shipments = payload["order"].get("shipments")
    if shipments is None:
        shipments = _find_first_key(payload, "shipments")

    if not isinstance(shipments, list):
        return []

    return [shipment for shipment in shipments if isinstance(shipment, Mapping)]


def shipment_block_from_order_payload(
    payload: Mapping[str, Any],
    shipment_order_number: object,
) -> Mapping[str, Any] | None:
    wanted_candidates = set(
        shipment_order_number_lookup_candidates(shipment_order_number)
    )
    if not wanted_candidates:
        return None

    for shipment in _shipments_from_payload(payload):
        current_candidates = set(
            shipment_order_number_lookup_candidates(_shipment_order_value(shipment))
        )
        if current_candidates & wanted_candidates:
            return shipment

    return None


def _normalize_tracking_value(value: object) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def split_tracking_numbers(value: object) -> list[str]:
    """Split the stored ``a;b`` tracking column into normalized values."""

    numbers: list[str] = []
    for part in re.split(r"[;,]", str(value or "")):
        normalized = _normalize_tracking_value(part)
        if normalized and normalized != "NA" and normalized not in numbers:
            numbers.append(normalized)
    return numbers


_TRACKING_KEY_HINTS = ("tracking", "awb", "waybill", "labelnumber", "parcelnumber")


def _collect_tracking_entries(
    value: Any,
    found: list[tuple[str, str, str]],
    path: tuple[str, ...] = (),
) -> None:
    """Deep-walk a shipment block collecting (path, raw value, match key).

    The path is kept because the *name* of the field (and of its parents) is
    what distinguishes the export/outbound tracking number from the return
    one, e.g. ``returnLabel.trackingNumber`` vs ``trackingNumber``. The raw
    value is kept because that is what the customer is shown; the normalized
    key is used only for matching.
    """

    if isinstance(value, Mapping):
        for key, child in value.items():
            key_name = str(key).lower().replace("_", "")
            child_path = path + (key_name,)
            if any(hint in key_name for hint in _TRACKING_KEY_HINTS) and isinstance(
                child, (str, int)
            ):
                normalized = _normalize_tracking_value(child)
                if normalized:
                    found.append(
                        ("/".join(child_path), str(child).strip(), normalized)
                    )
            else:
                _collect_tracking_entries(child, found, child_path)
    elif isinstance(value, list):
        for child in value:
            _collect_tracking_entries(child, found, path)


def _collect_tracking_values(value: Any, found: set[str]) -> None:
    entries: list[tuple[str, str, str]] = []
    _collect_tracking_entries(value, entries)
    found.update(normalized for _, _, normalized in entries)


def tracking_numbers_in_shipment(shipment: Mapping[str, Any] | None) -> set[str]:
    """Every tracking number carried by a shipment block.

    The search is a deep walk over the whole block, so it covers both the
    shipping-label and the return-label sub-objects regardless of how the
    GET_FULL_ORDER payload nests them.
    """

    found: set[str] = set()
    _collect_tracking_values(shipment, found)
    return found


# GET_FULL_ORDER shipment blocks carry an explicit per-label list:
#
#     "shippingLabels": [
#         {"carrierCode": "UPS", "labelType": "SHPLBL", "trackingNumber": "1ZV8..."},
#         {"carrierCode": "UPS", "labelType": "RETLBL", "trackingNumber": "1ZY2..."}
#     ]
#
# SHPLBL is the outbound/export leg, RETLBL is the return leg. That label type
# is authoritative: it is read before any field-name heuristic.
SHIPPING_LABELS_KEYS = ("shippingLabels", "shipping_labels", "labels")
EXPORT_LABEL_TYPES = {"SHPLBL", "SHIPPINGLABEL", "SHIPLABEL", "OUTBOUND", "EXPORT"}
RETURN_LABEL_TYPES = {"RETLBL", "RETURNLABEL", "RETLABEL", "RETURN"}

# Fallback only, for payload shapes with no shippingLabels[] list: field/parent
# names that mark a tracking number as belonging to the RETURN leg, and names
# that mark it as belonging to the EXPORT (outbound) leg. Return hints are
# checked first, so "returnShippingLabel" is a return field even though it also
# contains "shipping".
_RETURN_TRACKING_PATH_HINTS = ("return", "reso", "rma", "reverse", "rtn", "retlbl")
_EXPORT_TRACKING_PATH_HINTS = ("export", "outbound", "forward", "shipping", "delivery", "shplbl")


def _shipping_label_blocks(shipment: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    """The shipment's per-label blocks, if the payload provides them."""

    labels = _get_any(shipment, SHIPPING_LABELS_KEYS)
    if not isinstance(labels, list):
        return []
    return [label for label in labels if isinstance(label, Mapping)]


def _normalize_label_type(value: object) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _label_type_bucket(label: Mapping[str, Any]) -> str:
    """``export`` / ``return`` / ``unlabeled`` for one shippingLabels[] entry."""

    label_type = _normalize_label_type(
        _get_any(label, ("labelType", "label_type", "type"))
    )
    if label_type in EXPORT_LABEL_TYPES:
        return "export"
    if label_type in RETURN_LABEL_TYPES:
        return "return"
    return "unlabeled"


def _label_tracking_number(label: Mapping[str, Any]) -> str:
    value = _get_any(
        label,
        ("trackingNumber", "tracking_number", "trackingNo", "awb", "waybill"),
    )
    return str(value or "").strip()


def _classify_tracking_path(path: str) -> str:
    """``return`` / ``export`` / ``unlabeled`` for one tracking field path."""

    lowered = path.lower()
    if any(hint in lowered for hint in _RETURN_TRACKING_PATH_HINTS):
        return "return"
    if any(hint in lowered for hint in _EXPORT_TRACKING_PATH_HINTS):
        return "export"
    return "unlabeled"


def _classified_tracking_numbers(
    shipment: Mapping[str, Any] | None,
) -> dict[str, list[str]]:
    """Split a shipment block's tracking numbers into export / return / other.

    The shippingLabels[] list and its labelType decide this whenever the
    payload has one. Only a payload without that list falls back to the
    field-name heuristic, which is also why an ``isReturn`` flag is never
    consulted: its polarity differs between GET_FULL_ORDER and the labels
    Logic App (see extract_return_label_data).
    """

    buckets: dict[str, list[str]] = {"export": [], "return": [], "unlabeled": []}

    labels = _shipping_label_blocks(shipment)
    if labels:
        for label in labels:
            tracking_number = _label_tracking_number(label)
            if not tracking_number:
                continue
            bucket = buckets[_label_type_bucket(label)]
            if tracking_number not in bucket:
                bucket.append(tracking_number)
        if buckets["export"] or buckets["return"] or buckets["unlabeled"]:
            return buckets

    entries: list[tuple[str, str, str]] = []
    _collect_tracking_entries(shipment, entries)

    for path, raw_value, _ in entries:
        bucket = buckets[_classify_tracking_path(path)]
        if raw_value not in bucket:
            bucket.append(raw_value)

    return buckets


def carrier_codes_in_shipment(shipment: Mapping[str, Any] | None) -> dict[str, str]:
    """The export/return carrier codes of one shipment block, when stated.

    Read from the same shippingLabels[] entries as the tracking numbers, so a
    request resolved without bi.shipping_platform_shipments can still tell
    whether the return travelled with the export carrier.
    """

    carriers: dict[str, str] = {}
    for label in _shipping_label_blocks(shipment):
        bucket = _label_type_bucket(label)
        if bucket == "unlabeled":
            continue
        carrier = str(
            _get_any(label, ("carrierCode", "carrier_code", "carrier")) or ""
        ).strip()
        if carrier and bucket not in carriers:
            carriers[bucket] = carrier
    return carriers


def export_tracking_numbers_in_shipment(
    shipment: Mapping[str, Any] | None,
) -> list[str]:
    """The export (outbound) tracking number(s) of one shipment block.

    A label explicitly typed SHPLBL wins. When the payload types no label as
    the outbound one, any tracking number that is not a return tracking number
    is treated as the export one -- a shipment block whose only tracking field
    is a plain ``trackingNumber`` is the outbound leg.
    """

    buckets = _classified_tracking_numbers(shipment)
    if buckets["export"]:
        return list(buckets["export"])

    return_numbers = {_normalize_tracking_value(number) for number in buckets["return"]}
    return [
        number
        for number in buckets["unlabeled"]
        if _normalize_tracking_value(number) not in return_numbers
    ]


def return_tracking_numbers_in_shipment(
    shipment: Mapping[str, Any] | None,
) -> list[str]:
    """The return-leg (RETLBL) tracking number(s) of one shipment block."""

    return list(_classified_tracking_numbers(shipment)["return"])


# Only these shipment statuses carry data that may be used in a reply. A
# shipment that was never dispatched (cancelled, on hold, still being picked)
# has no usable export tracking number, invoice or returned-items list, and
# reporting one would answer the carrier about a shipment that does not exist.
ELIGIBLE_SHIPMENT_STATUSES = {"READY", "FULFILLED"}


def _shipment_status_value(shipment: Mapping[str, Any] | None) -> str:
    value = _get_any(
        shipment,
        ("status", "shipmentStatus", "shipment_status", "fulfillmentStatus"),
    )
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def shipment_is_eligible(shipment: Mapping[str, Any] | None) -> bool:
    """True when a shipment block may be read.

    A block with no status at all is treated as eligible: older payloads and
    test fixtures omit the field, and excluding them would silently drop data
    the pipeline used to return.
    """

    status = _shipment_status_value(shipment)
    return not status or status in ELIGIBLE_SHIPMENT_STATUSES


def eligible_shipments(shipments: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [shipment for shipment in shipments if shipment_is_eligible(shipment)]


def shipment_blocks_for_selection(
    payload: Mapping[str, Any],
    shipment_order_number: object,
    *,
    selection_mode: str = SHIPMENT_SELECTION_EXACT,
    tracking_numbers: Iterable[str] = (),
) -> list[Mapping[str, Any]]:
    """Return the shipment block(s) to read out of a GET_FULL_ORDER payload.

    * ``exact``       - the order came from the tracking number via
      bi.shipping_platform_shipments, so the matching shipment block is known.
    * ``by_tracking`` - the order came from an order reference while a tracking
      number was present in the ticket but unknown to the shipments table. The
      block whose shipping/return label carries that tracking number is used;
      when no block matches, every shipment is read.
    * ``all``         - no usable tracking number, so every shipment of the
      order is read.

    Whenever the selection is "every shipment of the order" rather than one
    named block, only shipments whose status is READY or FULFILLED are read
    (see shipment_is_eligible). A shipment the order never dispatched would
    otherwise contribute a tracking number or invoice for something that was
    never shipped. A block named explicitly -- by the shipment-shaped
    reference in ``exact`` mode, or by carrying the ticket's own tracking
    number in ``by_tracking`` mode -- is read whatever its status, because
    that is the shipment the ticket is about.
    """

    mode = str(selection_mode or SHIPMENT_SELECTION_EXACT).strip().lower()
    shipments = _shipments_from_payload(payload)

    if mode == SHIPMENT_SELECTION_ALL:
        return eligible_shipments(shipments)

    if mode == SHIPMENT_SELECTION_BY_TRACKING:
        wanted = {
            _normalize_tracking_value(number)
            for number in tracking_numbers
            if _normalize_tracking_value(number)
        }
        if wanted:
            matched = [
                shipment
                for shipment in shipments
                if tracking_numbers_in_shipment(shipment) & wanted
            ]
            if matched:
                return matched
        # The ticket's tracking number is in none of the shipment blocks:
        # fall back to extracting the data from all the usable ones.
        return eligible_shipments(shipments)

    block = shipment_block_from_order_payload(payload, shipment_order_number)
    return [block] if block is not None else []


def _invoice_documents_from_shipment(shipment: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    invoice_documents = _nested_get(shipment, "erpDocuments", "invoiceDocuments")
    if not isinstance(invoice_documents, list):
        return []
    return [document for document in invoice_documents if isinstance(document, Mapping)]


def _items_from_payload(
    payload: Mapping[str, Any],
    shipment: Mapping[str, Any] | None = None,
) -> list[Mapping[str, Any]]:
    # GET_FULL_ORDER orders can contain several shipments (e.g. the original
    # export shipment plus one or more return shipments). Returned-item data
    # belongs to the specific shipment being responded to, so look there
    # first under the field-name variants the API has used, including a
    # recursive search scoped to just that shipment. Only fall back to
    # order/payload-level "items" (which may belong to a different shipment
    # entirely) when nothing shipment-specific is found.
    if shipment is not None:
        shipment_items = _get_any(
            shipment,
            (
                "items",
                "returnedItems",
                "returned_items",
                "returnItems",
                "return_items",
                "lineItems",
                "line_items",
                "orderItems",
                "order_items",
            ),
        )
        if not isinstance(shipment_items, list):
            shipment_items = _find_first_key(shipment, "items")

        if isinstance(shipment_items, list):
            cleaned_shipment_items = [
                item for item in shipment_items if isinstance(item, Mapping)
            ]
            if cleaned_shipment_items:
                return cleaned_shipment_items

    items = payload.get("items")
    if items is None and isinstance(payload.get("order"), Mapping):
        items = payload["order"].get("items")
    if items is None:
        items = _find_first_key(payload, "items")

    if not isinstance(items, list):
        return []

    return [item for item in items if isinstance(item, Mapping)]


def _clean_item_field(value: object) -> str:
    if is_blank(value):
        return ""
    return str(value).strip()


def extract_returned_items(
    payload: Mapping[str, Any],
    shipment: Mapping[str, Any] | None = None,
) -> list[dict[str, str]]:
    returned_items: list[dict[str, str]] = []

    for item in _items_from_payload(payload, shipment):
        returned_item = {
            "sku": _clean_item_field(_get_any(item, ("sku", "SKU"))),
            "productName": _clean_item_field(
                _get_any(item, ("productName", "product_name", "name", "productTitle"))
            ),
            "imageUrl": _clean_item_field(
                _get_any(item, ("imageUrl", "image_url", "imageURL", "image", "imageLink"))
            ),
        }
        if any(returned_item.values()):
            returned_items.append(returned_item)

    return returned_items


def format_returned_items(returned_items: Iterable[Mapping[str, str]]) -> str | None:
    lines: list[str] = []

    for item in returned_items:
        parts = []
        sku = _clean_item_field(item.get("sku"))
        product_name = _clean_item_field(item.get("productName"))
        image_url = _clean_item_field(item.get("imageUrl"))

        if sku:
            parts.append(f"SKU: {sku}")
        if product_name:
            parts.append(f"Product: {product_name}")
        if image_url:
            parts.append(f"Image: {image_url}")

        if parts:
            lines.append("  - " + "; ".join(parts))

    return "\n".join(lines) if lines else None


def select_invoice_document_link(
    invoice_documents: Iterable[Mapping[str, Any]],
    document_type: str,
) -> str | None:
    wanted_document_type = str(document_type or "").strip().upper()
    matches = [
        document
        for document in invoice_documents
        if str(document.get("documentType", "") or "").strip().upper() == wanted_document_type
        and not is_blank(document.get("documentLink"))
    ]

    if not matches:
        return None

    intercompany_matches = [
        document
        for document in matches
        if _truthy(document.get("intercompanyDocument"))
    ]

    selected = intercompany_matches[0] if intercompany_matches else matches[0]
    return str(selected.get("documentLink") or "").strip() or None


def _customer_from_payload_or_shipment(
    payload: Mapping[str, Any],
    shipment: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    customer = _get_any(shipment, ("customer",))
    if isinstance(customer, Mapping):
        return customer

    customer = payload.get("customer")
    if isinstance(customer, Mapping):
        return customer

    order = payload.get("order")
    if isinstance(order, Mapping) and isinstance(order.get("customer"), Mapping):
        return order["customer"]

    customer = _find_first_key(payload, "customer")
    if isinstance(customer, Mapping):
        return customer

    return {}


def _shipping_address_from_payload_or_shipment(
    payload: Mapping[str, Any],
    shipment: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    shipping_address = _get_any(shipment, ("shippingAddress", "shipping_address"))
    if isinstance(shipping_address, Mapping):
        return shipping_address

    shipping_address = _get_any(payload, ("shippingAddress", "shipping_address"))
    if isinstance(shipping_address, Mapping):
        return shipping_address

    order = payload.get("order")
    if isinstance(order, Mapping):
        shipping_address = _get_any(order, ("shippingAddress", "shipping_address"))
        if isinstance(shipping_address, Mapping):
            return shipping_address

    shipping_address = _find_first_key(payload, "shippingAddress")
    if isinstance(shipping_address, Mapping):
        return shipping_address

    shipping_address = _find_first_key(payload, "shipping_address")
    if isinstance(shipping_address, Mapping):
        return shipping_address

    return {}


def _format_customer_name(shipping_address: Mapping[str, Any]) -> str | None:
    parts = [
        _clean_item_field(_get_any(shipping_address, ("name", "firstName", "first_name"))),
        _clean_item_field(_get_any(shipping_address, ("surName", "surname", "lastName", "last_name"))),
    ]
    value = " ".join(part for part in parts if part).strip()
    return value or None


def _format_country_code(shipping_address: Mapping[str, Any]) -> str | None:
    """Return the ISO 3166-1 alpha-2 shipping country code, upper-cased.

    Needed to populate Zendesk's per-brand "Country <BRAND>" field, which
    some brands require to be set before a ticket can be solved (see
    country_ticket_field_id_for_brand in customs_rules.py).
    """
    value = _clean_item_field(
        _get_any(shipping_address, ("country", "countryCode", "country_code"))
    )
    return value.strip().upper() if value else None


def _needs_translation(text: str) -> bool:
    """Return True if *text* contains Chinese, Japanese, Korean, Greek, or Arabic characters."""
    for char in text:
        cp = ord(char)
        if (
            # CJK Unified Ideographs (Chinese, Japanese Kanji, Korean Hanja)
            0x4E00 <= cp <= 0x9FFF
            # CJK Extension A
            or 0x3400 <= cp <= 0x4DBF
            # CJK Compatibility Ideographs
            or 0xF900 <= cp <= 0xFAFF
            # Hiragana
            or 0x3040 <= cp <= 0x309F
            # Katakana
            or 0x30A0 <= cp <= 0x30FF
            # Hangul Syllables (Korean)
            or 0xAC00 <= cp <= 0xD7A3
            # Greek and Coptic
            or 0x0370 <= cp <= 0x03FF
            # Arabic
            or 0x0600 <= cp <= 0x06FF
            # Arabic Supplement / Presentation Forms
            or 0xFB50 <= cp <= 0xFDFF
            or 0xFE70 <= cp <= 0xFEFF
        ):
            return True
    return False


def _translate_address_to_english(address: str) -> str:
    """Translate *address* to English using the Anthropic API.

    Falls back to the original text if the API call fails, so that a
    translation outage never blocks the rest of the pipeline.
    """
    raw_secret = os.environ.get("CLAUDE_API_KEY", "")
    if not raw_secret:
        print("WARNING: CLAUDE_API_KEY not set; skipping address translation.")
        return address
    try:
        api_key = json.loads(raw_secret).get("claude_api_key", "")
    except (json.JSONDecodeError, AttributeError):
        api_key = raw_secret.strip()
    if not api_key:
        print("WARNING: CLAUDE_API_KEY secret is set but claude_api_key value is empty; skipping address translation.")
        return address

    prompt = (
        "Translate the following shipping address to English. "
        "Return only the translated address, preserving line structure where possible, "
        "with no additional commentary or explanation.\n\n"
        f"{address}"
    )
    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 256,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        translated = data["content"][0]["text"].strip()
        if translated:
            return translated
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: Address translation failed: {exc}")
    return address


def _format_shipping_address(shipping_address: Mapping[str, Any]) -> str | None:
    parts = [
        _clean_item_field(_get_any(shipping_address, ("addressLine1", "address_line_1", "address1"))),
        _clean_item_field(_get_any(shipping_address, ("zip", "postalCode", "postal_code"))),
        _clean_item_field(_get_any(shipping_address, ("stateOrProvince", "state_or_province", "state", "province"))),
        _clean_item_field(_get_any(shipping_address, ("country", "countryCode", "country_code"))),
        _clean_item_field(_get_any(shipping_address, ("cityOrTown", "city_or_town", "city", "town"))),
    ]
    value = ", ".join(part for part in parts if part).strip()
    if not value:
        return None
    if _needs_translation(value):
        value = _translate_address_to_english(value)
    return value


def _shipped_at_from_shipment(shipment: Mapping[str, Any] | None) -> str | None:
    # The current GET_FULL_ORDER payload uses camelCase shippedAt.  Keep the
    # snake_case and legacy alternatives to avoid breaking older fixtures.
    value = _get_any(shipment, ("shippedAt", "shipped_at", "shippedDate", "shipmentDate"))
    if is_blank(value):
        return None
    return str(value).strip()


# Columns whose values from several shipments are joined instead of kept
# single-valued. Document links are semicolon-joined because the downloader
# already splits on ";"; the returned-items block is newline-joined so it stays
# readable. Everything else (customer, address, country, shipped date) belongs
# to the order rather than the shipment, so the first non-blank value is kept.
_SEMICOLON_JOINED_COLUMNS = {
    FULL_ORDER_RESPONSE_COLUMNS["return_proforma_invoice"],
    FULL_ORDER_RESPONSE_COLUMNS["commercial_invoice"],
}
_NEWLINE_JOINED_COLUMNS = {
    FULL_ORDER_RESPONSE_COLUMNS["returned_items_confirmation"],
}
# When several shipments are read (an order-shaped reference, so the exact
# shipment is unknown), every shipment's export/return tracking number is
# reported rather than only the first one.
_SEMICOLON_JOINED_COLUMNS |= {
    FULL_ORDER_RESPONSE_COLUMNS["export_tracking_number"],
    FULL_ORDER_RESPONSE_COLUMNS["return_tracking_number"],
}
# Carrier codes are deliberately NOT joined: response_generator only compares
# them when a single unambiguous value exists on each leg, exactly as
# enrich_with_shipment_numbers() does for the shipments-table columns.


def _joined_tracking_numbers(numbers: Iterable[str]) -> str | None:
    values: list[str] = []
    for number in numbers:
        text = str(number or "").strip()
        if text and text not in values:
            values.append(text)
    return ";".join(values) if values else None


def _combine_shipment_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge per-shipment extraction results into one row-shaped dict."""

    if not results:
        return {}
    if len(results) == 1:
        return results[0]

    combined: dict[str, Any] = {}

    for result in results:
        for column, value in result.items():
            if is_blank(value):
                combined.setdefault(column, None)
                continue

            if column not in _SEMICOLON_JOINED_COLUMNS | _NEWLINE_JOINED_COLUMNS:
                if is_blank(combined.get(column)):
                    combined[column] = value
                continue

            separator = ";" if column in _SEMICOLON_JOINED_COLUMNS else "\n"
            existing = combined.get(column)
            if is_blank(existing):
                combined[column] = value
                continue

            values = [
                part.strip()
                for part in str(existing).split(separator)
                if part.strip()
            ]
            if str(value).strip() not in values:
                values.append(str(value).strip())
            combined[column] = separator.join(values)

    # api_error is informational only: never let one shipment's error mask the
    # data successfully extracted from the others.
    error_column = FULL_ORDER_RESPONSE_COLUMNS["api_error"]
    if any(not is_blank(result.get(error_column)) for result in results) and any(
        not is_blank(value)
        for column, value in combined.items()
        if column != error_column
    ):
        combined[error_column] = None

    return combined


def _extract_single_shipment_data(
    payload: Mapping[str, Any],
    shipment: Mapping[str, Any],
) -> dict[str, Any]:
    """Extract one shipment block's response fields from a full order."""

    invoice_documents = _invoice_documents_from_shipment(shipment)
    customer = _customer_from_payload_or_shipment(payload, shipment)
    shipping_address = _shipping_address_from_payload_or_shipment(payload, shipment)

    return {
        FULL_ORDER_RESPONSE_COLUMNS["return_proforma_invoice"]: select_invoice_document_link(
            invoice_documents,
            "RPI",
        ),
        FULL_ORDER_RESPONSE_COLUMNS["commercial_invoice"]: select_invoice_document_link(
            invoice_documents,
            "INV",
        ),
        FULL_ORDER_RESPONSE_COLUMNS["returned_items_confirmation"]: format_returned_items(
            extract_returned_items(payload, shipment)
        ),
        FULL_ORDER_RESPONSE_COLUMNS["customer_email"]: (
            None if is_blank(customer.get("email")) else str(customer.get("email")).strip()
        ),
        FULL_ORDER_RESPONSE_COLUMNS["customer_phone"]: (
            None
            if is_blank(_nested_get(shipping_address, "phoneNumbers", "home"))
            else str(_nested_get(shipping_address, "phoneNumbers", "home")).strip()
        ),
        FULL_ORDER_RESPONSE_COLUMNS["customer_name"]: _format_customer_name(shipping_address),
        FULL_ORDER_RESPONSE_COLUMNS["shipping_address"]: _format_shipping_address(shipping_address),
        FULL_ORDER_RESPONSE_COLUMNS["shipped_at"]: _shipped_at_from_shipment(shipment),
        FULL_ORDER_RESPONSE_COLUMNS["country_code"]: _format_country_code(shipping_address),
        FULL_ORDER_RESPONSE_COLUMNS["export_tracking_number"]: _joined_tracking_numbers(
            export_tracking_numbers_in_shipment(shipment)
        ),
        FULL_ORDER_RESPONSE_COLUMNS["return_tracking_number"]: _joined_tracking_numbers(
            return_tracking_numbers_in_shipment(shipment)
        ),
        FULL_ORDER_RESPONSE_COLUMNS["export_carrier_code"]: (
            carrier_codes_in_shipment(shipment).get("export") or None
        ),
        FULL_ORDER_RESPONSE_COLUMNS["return_carrier_code"]: (
            carrier_codes_in_shipment(shipment).get("return") or None
        ),
        FULL_ORDER_RESPONSE_COLUMNS["api_error"]: None,
    }


EXPORT_TRACKING_SOURCE_TICKET_TRACKING = "ticket_tracking_shipment_block"
EXPORT_TRACKING_SOURCE_SELECTED_SHIPMENT = "selected_shipment_block"
EXPORT_TRACKING_SOURCE_ALL_SHIPMENTS = "all_shipment_blocks"


def _resolve_export_tracking(
    payload: Mapping[str, Any],
    selected_shipments: list[Mapping[str, Any]],
    *,
    tracking_numbers: Iterable[str] = (),
) -> dict[str, Any]:
    """Export/return tracking numbers, read from the right shipment block(s).

    This is what makes a request answerable when the ticket's tracking number
    was not found in ``bi.shipping_platform_shipments`` and the order was
    identified from an order reference in the subject/description or in a PDF
    attachment instead. Three rules, in order:

    1. **The ticket carried a tracking number.** Whichever shipment block of
       the order carries that number -- on its shipping label or on its return
       label -- is the ticket's shipment, so the export tracking number is read
       from that same block. This holds regardless of the shape of the order
       reference, because the tracking number identifies the shipment more
       precisely than an order-level reference does.
    2. **No tracking number, shipment-shaped reference** (e.g.
       ``DG-EUA01663254``). The reference already names one shipment, so the
       export tracking number is read from that block only. This is the
       ``exact`` selection the caller has already applied.
    3. **No tracking number, order-shaped reference** (e.g.
       ``DG-EU-01663254``). The exact shipment is unknown, so every shipment
       block of the order is read and the export tracking numbers are
       semicolon-joined.

    Rules 2 and 3 are simply "read the blocks the caller selected"; rule 1
    re-selects the block by tracking number first.
    """

    wanted = {
        _normalize_tracking_value(number)
        for number in tracking_numbers
        if _normalize_tracking_value(number)
    }

    all_shipments = _shipments_from_payload(payload)
    blocks = list(selected_shipments)
    source = (
        EXPORT_TRACKING_SOURCE_ALL_SHIPMENTS
        if len(blocks) > 1
        else EXPORT_TRACKING_SOURCE_SELECTED_SHIPMENT
    )

    if wanted:
        # No status filter here: a block carrying the ticket's own tracking
        # number IS the shipment the ticket is about, whatever its status.
        matched = [
            shipment
            for shipment in all_shipments
            if tracking_numbers_in_shipment(shipment) & wanted
        ]
        # Only follow the tracking match when that block actually carries an
        # export tracking number; otherwise keep the caller's selection.
        if matched and any(export_tracking_numbers_in_shipment(b) for b in matched):
            blocks = matched
            source = EXPORT_TRACKING_SOURCE_TICKET_TRACKING

    export_numbers: list[str] = []
    return_numbers: list[str] = []
    export_carriers: list[str] = []
    return_carriers: list[str] = []
    for shipment in blocks:
        for number in export_tracking_numbers_in_shipment(shipment):
            if number not in export_numbers:
                export_numbers.append(number)
        for number in return_tracking_numbers_in_shipment(shipment):
            if number not in return_numbers:
                return_numbers.append(number)
        carriers = carrier_codes_in_shipment(shipment)
        for bucket, collected in (("export", export_carriers), ("return", return_carriers)):
            carrier = carriers.get(bucket)
            if carrier and carrier not in collected:
                collected.append(carrier)

    def _single_carrier(values: list[str]) -> str | None:
        # One value only. Shipments that disagree have no single answer, and
        # leaving it unset makes response_generator treat the legs as being on
        # different carriers -- the safe reading, matching what
        # enrich_with_shipment_numbers() does for the shipments-table columns.
        return values[0] if len(values) == 1 else None

    return {
        FULL_ORDER_RESPONSE_COLUMNS["export_tracking_number"]: _joined_tracking_numbers(
            export_numbers
        ),
        FULL_ORDER_RESPONSE_COLUMNS["return_tracking_number"]: _joined_tracking_numbers(
            return_numbers
        ),
        FULL_ORDER_RESPONSE_COLUMNS["export_carrier_code"]: _single_carrier(export_carriers),
        FULL_ORDER_RESPONSE_COLUMNS["return_carrier_code"]: _single_carrier(return_carriers),
        FULL_ORDER_RESPONSE_COLUMNS["export_tracking_source"]: (
            source if export_numbers else None
        ),
    }


def extract_shipment_order_data(
    payload: Mapping[str, Any],
    shipment_order_number: object,
    *,
    selection_mode: str = SHIPMENT_SELECTION_EXACT,
    tracking_numbers: Iterable[str] = (),
) -> dict[str, Any]:
    """Extract the response fields for the selected shipment(s) of an order.

    With the default ``exact`` mode this behaves exactly as before: only the
    shipment block matching ``shipment_order_number`` is read. The other modes
    exist for orders resolved from an order reference rather than from the
    tracking number -- see shipment_blocks_for_selection().
    """

    shipments = shipment_blocks_for_selection(
        payload,
        shipment_order_number,
        selection_mode=selection_mode,
        tracking_numbers=tracking_numbers,
    )

    if not shipments:
        # Distinguish "the order has no such shipment" from "every shipment of
        # the order was filtered out by status", because the second is not a
        # lookup failure and a reviewer needs to see which statuses were there.
        all_shipments = _shipments_from_payload(payload)
        if all_shipments and not eligible_shipments(all_shipments):
            statuses = sorted(
                {
                    _shipment_status_value(shipment) or "(no status)"
                    for shipment in all_shipments
                }
            )
            return {
                FULL_ORDER_RESPONSE_COLUMNS["api_error"]: (
                    "No shipment of this order is READY or FULFILLED "
                    f"(statuses found: {', '.join(statuses)})"
                )
            }
        return {
            FULL_ORDER_RESPONSE_COLUMNS["api_error"]: (
                "Shipment block not found in GET_FULL_ORDER response"
            )
        }

    combined = _combine_shipment_results(
        [_extract_single_shipment_data(payload, shipment) for shipment in shipments]
    )
    combined.update(
        _resolve_export_tracking(
            payload,
            shipments,
            tracking_numbers=tracking_numbers,
        )
    )
    return combined


# ============================================================================
# GET_FULL_ORDER 404 FALLBACK -- the `tlg-wlfs-prd`.til event tables
# ============================================================================
#
# GET_FULL_ORDER answers 404 for orders it does not carry (archived, or never
# registered on the return platform), and the ticket then goes to human review
# as "no invoice found" / "no tracking number" even though both exist.
#
# They exist in the TIL event tables, written by NAV as the order moved:
#
# 1. **Invoice / RPI -- til.prd_invoiceUpdate, eventType FATV_RESULT.** The
#    requestPayload carries the very same DocOpen.aspx link GET_FULL_ORDER
#    would have exposed under erpDocuments.invoiceDocuments:
#
#      {"eventType": "FATV_RESULT",
#       "data": {"fatvResult": {"invoiceNumber": "INV-26115916",
#                               "shipmentOrderNumber": "HB-EU-00117943",
#                               "invoiceLink": "http://azureftp...DocOpen.aspx?link=..."}}}
#
# 2. **Tracking -- til.prd_shipmentStatus, eventType PACKED / SHIPPED.** Each
#    data.shippingData[] entry carries a trackingNumber, a carrierCode and a
#    shipType: SHIPPING is the outbound (export) leg, RETURN is the return leg.
#
# Both halves only have to produce values for the existing full_order_*
# columns; everything downstream is untouched. The invoice link is resolved and
# downloaded by the same download_invoice_pdf() path as a GET_FULL_ORDER link,
# and the tracking numbers feed the same rules (UPS account code, LOA/POA,
# same-carrier vs different-carrier reply) as the ones read out of a shipment
# block.
#
# These tables live in the TIL GCP project, which the BI service account cannot
# read -- hence the separate TIL_BIGQUERY_CREDS secret, the same one
# ticket_fetcher.til_bigquery_client() uses for its prd_shipmentStatus order
# lookup. A missing or unusable secret only disables the fallback: the 404 is
# then re-raised and the row behaves exactly as it does today.

TIL_PROJECT_ID = os.getenv("TIL_PROJECT_ID", "tlg-wlfs-prd")
TIL_BIGQUERY_CREDS_ENV = "TIL_BIGQUERY_CREDS"
INVOICE_UPDATE_TABLE = os.getenv(
    "TIL_INVOICE_UPDATE_TABLE",
    "`tlg-wlfs-prd`.til.prd_invoiceUpdate",
)
INVOICE_UPDATE_EVENT_TYPE = "FATV_RESULT"

# Internal/audit column: which source produced the invoice link on this row.
# Never shown to the customer; absent on rows served by GET_FULL_ORDER.
INVOICE_LINK_SOURCE_COLUMN = "full_order_invoice_link_source"
INVOICE_LINK_SOURCE_INVOICE_UPDATE = "prd_invoiceUpdate.FATV_RESULT"

SHIPMENT_STATUS_TABLE = os.getenv(
    "TIL_SHIPMENT_STATUS_TABLE",
    "`tlg-wlfs-prd`.til.prd_shipmentStatus",
)
# The only two event types that carry shippingData, i.e. tracking numbers.
SHIPMENT_STATUS_EVENT_TYPES = ("PACKED", "SHIPPED")
EXPORT_TRACKING_SOURCE_SHIPMENT_STATUS = "shipment_status_events"

# Only the FATV_RESULT events of the wanted order, newest first. The envelope's
# top-level "success" is deliberately not filtered on: it reports the state of
# the whole NAV event, and a payload carrying a usable invoiceLink is routinely
# flagged false. SAFE_CAST keeps one malformed eventTimestamp from failing the
# whole query -- such a row simply sorts last. The shipmentOrderNumber is
# matched by regex rather than equality so that an order-shaped reference picks
# up every shipment of the order; see til_event_search_patterns().
INVOICE_UPDATE_QUERY = f"""
SELECT
    UPPER(TRIM(IFNULL(i.shipmentOrderNumber, '')))                  AS shipment_order_number,
    JSON_VALUE(i.requestPayload, '$.data.fatvResult.invoiceNumber') AS invoice_number,
    JSON_VALUE(i.requestPayload, '$.data.fatvResult.invoiceLink')   AS invoice_link,
    JSON_VALUE(i.requestPayload, '$.data.fatvResult.fileName')      AS file_name,
    SAFE_CAST(
        JSON_VALUE(i.requestPayload, '$.eventTimestamp') AS TIMESTAMP
    ) AS event_timestamp

FROM {INVOICE_UPDATE_TABLE} AS i

WHERE i.eventType IN UNNEST(@event_types)
  AND EXISTS (
      SELECT 1
      FROM UNNEST(@shipment_order_patterns) AS pattern
      WHERE REGEXP_CONTAINS(UPPER(TRIM(IFNULL(i.shipmentOrderNumber, ''))), pattern)
  )
  AND TRIM(
      IFNULL(JSON_VALUE(i.requestPayload, '$.data.fatvResult.invoiceLink'), '')
  ) != ''

ORDER BY event_timestamp DESC
"""

# One row per shippingData entry of the order's PACKED/SHIPPED events, newest
# first. shipType is what separates the two legs: SHIPPING is the outbound
# (export) label, RETURN is the return label.
#
# The UNNEST is a LEFT JOIN, and empty tracking numbers are filtered in Python
# rather than here, so that an event still contributes its products[] list even
# when it carries no usable shippingData entry at all -- the returned-items
# fallback must not depend on the tracking fallback having found something.
# products[] sits at the top level of the payload, beside data{}, and is
# repeated on every row of the same event; only its productId values are read.
SHIPMENT_STATUS_TRACKING_QUERY = f"""
SELECT
    UPPER(TRIM(IFNULL(
        JSON_VALUE(s.requestPayload, '$.data.shipmentOrderNumber'), ''
    )))                                                         AS shipment_order_number,
    UPPER(TRIM(IFNULL(JSON_VALUE(ship, '$.shipType'), '')))     AS ship_type,
    JSON_VALUE(ship, '$.trackingNumber')                        AS tracking_number,
    JSON_VALUE(ship, '$.carrierCode')                           AS carrier_code,
    JSON_QUERY(s.requestPayload, '$.products')                  AS products_json,
    UPPER(TRIM(IFNULL(s.eventType, '')))                        AS event_type,
    JSON_VALUE(s.requestPayload, '$.eventTimestamp')            AS event_timestamp_raw,
    SAFE_CAST(
        JSON_VALUE(s.requestPayload, '$.eventTimestamp') AS TIMESTAMP
    ) AS event_timestamp

FROM {SHIPMENT_STATUS_TABLE} AS s
LEFT JOIN UNNEST(
    JSON_EXTRACT_ARRAY(s.requestPayload, '$.data.shippingData')
) AS ship

WHERE s.eventType IN UNNEST(@event_types)
  AND EXISTS (
      SELECT 1
      FROM UNNEST(@shipment_order_patterns) AS pattern
      WHERE REGEXP_CONTAINS(
          UPPER(TRIM(IFNULL(
              JSON_VALUE(s.requestPayload, '$.data.shipmentOrderNumber'), ''
          ))),
          pattern
      )
  )

ORDER BY event_timestamp DESC
"""

_til_client_lock = threading.Lock()
_til_client_state: dict[str, Any] = {}
_invoice_update_lock = threading.Lock()
_invoice_update_cache: dict[str, dict[str, str]] = {}
_shipment_status_lock = threading.Lock()
_shipment_status_cache: dict[str, dict[str, Any]] = {}


def _til_bigquery_client():
    """The tlg-wlfs-prd BigQuery client, built once per process.

    Returns ``(None, None)`` when TIL_BIGQUERY_CREDS is absent or unusable
    instead of raising: this is a fallback for rows that are already failing,
    so it must never turn a 404 into a crashed run. The failure is reported
    once, not once per order, because the prefetch calls this from several
    threads at a time.
    """

    with _til_client_lock:
        if "client" in _til_client_state:
            return _til_client_state["client"], _til_client_state["bigquery"]

        client = None
        bigquery_module = None
        raw = (os.getenv(TIL_BIGQUERY_CREDS_ENV) or "").strip()

        if not raw:
            print(
                f"{TIL_BIGQUERY_CREDS_ENV} is not set; the TIL event "
                "fallback for GET_FULL_ORDER 404s will be skipped."
            )
        else:
            try:
                from google.cloud import bigquery as bigquery_module
                from google.oauth2 import service_account

                til_credentials = json.loads(raw)
                credentials = service_account.Credentials.from_service_account_info(
                    til_credentials
                )
                # project_id comes from the key itself, so a key issued for a
                # different project keeps working without a code change.
                client = bigquery_module.Client(
                    project=til_credentials.get("project_id") or TIL_PROJECT_ID,
                    credentials=credentials,
                )
            except Exception as error:  # noqa: BLE001 - see the docstring
                print(
                    f"Could not build the TIL BigQuery client from "
                    f"{TIL_BIGQUERY_CREDS_ENV}: {error}. The TIL event "
                    "fallback for GET_FULL_ORDER 404s will be skipped."
                )
                client = None
                bigquery_module = None

        _til_client_state["client"] = client
        _til_client_state["bigquery"] = bigquery_module
        return client, bigquery_module


def til_event_search_patterns(shipment_order_number: object) -> list[str]:
    """Anchored BigQuery regexes for the shipmentOrderNumber of the TIL events.

    Two cases, because what the workflow knows about the ticket differs:

    - **A shipment is named** (``HB-EUA00117943``). Only that shipment's events
      are wanted. Both shapes are matched exactly, because NAV files the event
      under the order-shaped reference (``HB-EU-00117943``) even though the
      field is called shipmentOrderNumber, while the ticket may carry the
      shipment-shaped one.
    - **Only the order is known** (``HB-EU-00117943``, the order-shaped
      reference the GET_FULL_ORDER URL uses). The exact shipment is unknown, so
      *every* shipment of that order is wanted: the 6th character -- the one
      that distinguishes ``HB-EUA…``, ``HB-EUB…``, ``HB-EU-…`` -- is matched by
      a single ``.``. This is the same "read all the blocks" rule
      ``SHIPMENT_SELECTION_ALL`` applies to a GET_FULL_ORDER payload, which the
      404 denied us.
    """

    value = _normalize_shipment_order_number(shipment_order_number)
    if not value:
        return []

    if is_shipment_shaped_order_reference(value):
        keys: list[str] = []
        for candidate in (
            value,
            shipment_order_number_for_shipment_block_lookup(value),
            *order_number_candidates_from_shipment_order_number(value),
        ):
            key = str(candidate or "").strip().upper()
            if key and key not in keys:
                keys.append(key)
        return [f"^{re.escape(key)}$" for key in keys]

    order_number = order_number_from_shipment_order_number(value) or value
    if _looks_like_brand_prefixed_shipment_order(order_number) and len(order_number) >= 6:
        # XX-EU-01234567 -> ^XX\-EU.01234567$, which also matches XX-EUA…,
        # XX-EUB…, XX-EUF… -- i.e. every shipment of the order.
        return [f"^{re.escape(order_number[:5])}.{re.escape(order_number[6:])}$"]

    return [f"^{re.escape(order_number)}$"]


def _til_pattern_parameters(bigquery, patterns: list[str], event_types: list[str]):
    return [
        bigquery.ArrayQueryParameter("event_types", "STRING", event_types),
        bigquery.ArrayQueryParameter("shipment_order_patterns", "STRING", patterns),
    ]


def _invoice_data_key_from_fatv_result(
    invoice_number: object,
    file_name: object,
    invoice_link: object,
) -> str:
    """Which document the FATV result holds: an RPI or a commercial invoice.

    NAV names both the document and its file after the type -- RPI-26000123 /
    RPI_HB-EU-00117943_... against INV-26115916 / INV_HB-EU-00117943_... -- so
    the prefix is what distinguishes them, exactly as documentType does inside
    a GET_FULL_ORDER invoiceDocuments entry. Anything unrecognized is treated
    as a commercial invoice, which is what NAV issues by default.
    """

    for value in (invoice_number, file_name, invoice_link):
        text = str(value or "").strip().upper()
        if not text:
            continue
        if re.search(r"\b(?:RPI|PRI)[-_]", text):
            return "return_proforma_invoice"
        if re.search(r"\bINV[-_]", text):
            return "commercial_invoice"

    return "commercial_invoice"


def _product_ids_from_payload_products(products_json: object) -> list[str]:
    """The productId values of a prd_shipmentStatus payload's products[] list.

    The event carries no SKU, product name or image -- only identifiers:

        {"products": [{"productId": "408052381130892", "quantity": 1,
                       "data": {"cartonId": "...", "productLineId": "..."}}, ...]}

    so the product ids are all the returned-items confirmation can be built
    from when GET_FULL_ORDER is unavailable. Duplicates are collapsed and the
    payload order is kept.
    """

    raw = products_json
    if isinstance(raw, (str, bytes, bytearray)):
        text = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
        text = text.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except (TypeError, ValueError):
            return []

    if not isinstance(raw, list):
        return []

    product_ids: list[str] = []
    for product in raw:
        if not isinstance(product, Mapping):
            continue
        product_id = str(product.get("productId") or "").strip()
        if product_id and product_id not in product_ids:
            product_ids.append(product_id)
    return product_ids


def format_product_ids(product_ids: Iterable[str]) -> str | None:
    """Render product ids in the shape format_returned_items() produces.

    They are labelled "Product ID" rather than "SKU": the event gives an
    internal identifier, and calling it a SKU in a customer-facing reply would
    be wrong.
    """

    lines = [
        f"  - Product ID: {str(product_id).strip()}"
        for product_id in product_ids
        if str(product_id or "").strip()
    ]
    return "\n".join(lines) if lines else None


def _is_downloadable_invoice_link(value: object) -> bool:
    """Whether a FATV invoiceLink is something the downloader can fetch.

    An order routinely has several FATV_RESULT rows -- retries, reissues, one
    per shipment -- and only the ones carrying a real URL are usable; anything
    else (a bare file name, a UNC share path, an empty string) is skipped
    rather than written into the response as a broken link.
    """

    return str(value or "").strip().lower().startswith(("http://", "https://"))


def invoice_links_from_invoice_update(
    shipment_order_number: object,
) -> dict[str, str]:
    """Map invoice data key -> ";"-joined link(s), from the FATV_RESULT events.

    Returns ``{}`` whenever the fallback cannot answer -- no credentials, no
    matching event, no usable link, or a failed query -- so the caller can
    re-raise the original 404.

    Several rows are the normal case. Within one shipment the newest usable
    row per document type wins, so a reissued invoice answers with the current
    document. Across shipments (the order-only case) every distinct shipment
    contributes its own invoice and the links are semicolon-joined, the same
    separator ``_combine_shipment_results`` and the downloader already use.
    """

    patterns = til_event_search_patterns(shipment_order_number)
    if not patterns:
        return {}

    cache_key = "|".join(patterns)
    with _invoice_update_lock:
        if cache_key in _invoice_update_cache:
            return dict(_invoice_update_cache[cache_key])

    client, bigquery = _til_bigquery_client()
    if client is None or bigquery is None:
        return {}

    try:
        rows = client.query(
            INVOICE_UPDATE_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=_til_pattern_parameters(
                    bigquery, patterns, [INVOICE_UPDATE_EVENT_TYPE]
                )
            ),
        ).result()
    except Exception as error:  # noqa: BLE001 - a failed fallback must leave
        # the original 404 in place rather than break the run.
        print(f"  prd_invoiceUpdate lookup failed for {shipment_order_number}: {error}")
        return {}

    # (shipment, document type) -> link. Rows arrive newest first, so the
    # first usable link seen for a pair is the current one.
    per_shipment: dict[tuple[str, str], str] = {}
    for row in rows:
        link = str(row.get("invoice_link") or "").strip()
        if not _is_downloadable_invoice_link(link):
            continue
        data_key = _invoice_data_key_from_fatv_result(
            row.get("invoice_number"),
            row.get("file_name"),
            link,
        )
        shipment = str(row.get("shipment_order_number") or "").strip().upper()
        per_shipment.setdefault((shipment, data_key), link)

    links: dict[str, list[str]] = {}
    for (_shipment, data_key), link in per_shipment.items():
        collected = links.setdefault(data_key, [])
        if link not in collected:
            collected.append(link)

    joined = {data_key: ";".join(values) for data_key, values in links.items()}

    with _invoice_update_lock:
        _invoice_update_cache[cache_key] = dict(joined)

    return joined


def tracking_numbers_from_shipment_status(
    shipment_order_number: object,
) -> dict[str, Any]:
    """Export/return tracking, carriers and returned items, from the events.

    The PACKED and SHIPPED events are the only ones carrying ``shippingData``,
    and each entry there is labelled with a ``shipType``: ``SHIPPING`` is the
    outbound (export) leg, ``RETURN`` is the return leg. That is the same
    distinction ``_resolve_export_tracking`` reads out of a GET_FULL_ORDER
    shipment block, so the values land in the same columns and every
    downstream rule -- the UPS account code, LOA/POA generation, the
    same-carrier vs different-carrier reply -- keeps working unchanged. An
    entry whose trackingNumber is empty (the RETURN leg routinely is) is
    simply skipped.

    The same events also carry the shipment's contents under a top-level
    ``products[]``, from which only the ``productId`` values are read: that is
    the returned-items confirmation when GET_FULL_ORDER cannot supply one, and
    without it a ticket asking for both tracking and returned items would
    still be held for a human.

    The event's own ``eventTimestamp`` stands in for the shipment's
    ``shippedAt``, which is the export date the LOA prints: the latest SHIPPED
    event, or the latest PACKED one for an order that never produced a
    SHIPPED. Without it, authorization_letter_export_date would block every
    LOA on a 404'd order.

    Returns ``{}`` when nothing usable is found.
    """

    patterns = til_event_search_patterns(shipment_order_number)
    if not patterns:
        return {}

    cache_key = "|".join(patterns)
    with _shipment_status_lock:
        if cache_key in _shipment_status_cache:
            return dict(_shipment_status_cache[cache_key])

    client, bigquery = _til_bigquery_client()
    if client is None or bigquery is None:
        return {}

    try:
        rows = client.query(
            SHIPMENT_STATUS_TRACKING_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=_til_pattern_parameters(
                    bigquery, patterns, list(SHIPMENT_STATUS_EVENT_TYPES)
                )
            ),
        ).result()
    except Exception as error:  # noqa: BLE001 - see invoice fallback above.
        print(
            f"  prd_shipmentStatus tracking lookup failed for "
            f"{shipment_order_number}: {error}"
        )
        return {}

    export_numbers: list[str] = []
    return_numbers: list[str] = []
    export_carriers: list[str] = []
    return_carriers: list[str] = []
    # Products per shipment, newest event first: an event that re-states the
    # shipment's contents supersedes the older one rather than adding to it.
    products_by_shipment: dict[str, list[str]] = {}
    # The export date the LOA prints. GET_FULL_ORDER would have given
    # shipment.shippedAt; here the event's own eventTimestamp is that date.
    # SHIPPED is the real one, PACKED only the stand-in for an order that
    # never produced a SHIPPED event.
    shipped_at_by_event_type: dict[str, str] = {}

    for row in rows:
        event_timestamp = str(row.get("event_timestamp_raw") or "").strip()
        event_type = str(row.get("event_type") or "").strip().upper()
        # Rows arrive newest first, so the first timestamp seen per event type
        # is the latest one.
        if event_timestamp and event_type not in shipped_at_by_event_type:
            shipped_at_by_event_type[event_type] = event_timestamp

        shipment = str(row.get("shipment_order_number") or "").strip().upper()
        if shipment not in products_by_shipment:
            product_ids = _product_ids_from_payload_products(row.get("products_json"))
            if product_ids:
                products_by_shipment[shipment] = product_ids

        tracking = str(row.get("tracking_number") or "").strip()
        if not tracking:
            continue
        ship_type = str(row.get("ship_type") or "").strip().upper()
        carrier = str(row.get("carrier_code") or "").strip()

        if ship_type == "SHIPPING":
            numbers, carriers = export_numbers, export_carriers
        elif ship_type == "RETURN":
            numbers, carriers = return_numbers, return_carriers
        else:
            # An unlabelled or unknown shipType is not guessed at: reporting a
            # return label as the export tracking number would be worse than
            # reporting nothing.
            continue

        if tracking not in numbers:
            numbers.append(tracking)
        if carrier and carrier not in carriers:
            carriers.append(carrier)

    all_product_ids: list[str] = []
    for product_ids in products_by_shipment.values():
        for product_id in product_ids:
            if product_id not in all_product_ids:
                all_product_ids.append(product_id)

    shipped_at = (
        shipped_at_by_event_type.get("SHIPPED")
        or shipped_at_by_event_type.get("PACKED")
        or None
    )

    if not export_numbers and not return_numbers and not all_product_ids and not shipped_at:
        result: dict[str, Any] = {}
    else:
        result = {
            FULL_ORDER_RESPONSE_COLUMNS["export_tracking_number"]: _joined_tracking_numbers(
                export_numbers
            ),
            FULL_ORDER_RESPONSE_COLUMNS["return_tracking_number"]: _joined_tracking_numbers(
                return_numbers
            ),
            # One value only, exactly as _resolve_export_tracking does:
            # shipments that disagree leave it unset, which keeps the safe
            # "assume different carriers" reading.
            FULL_ORDER_RESPONSE_COLUMNS["export_carrier_code"]: (
                export_carriers[0] if len(export_carriers) == 1 else None
            ),
            FULL_ORDER_RESPONSE_COLUMNS["return_carrier_code"]: (
                return_carriers[0] if len(return_carriers) == 1 else None
            ),
            FULL_ORDER_RESPONSE_COLUMNS["export_tracking_source"]: (
                EXPORT_TRACKING_SOURCE_SHIPMENT_STATUS if export_numbers else None
            ),
            FULL_ORDER_RESPONSE_COLUMNS["returned_items_confirmation"]: (
                format_product_ids(all_product_ids)
            ),
            FULL_ORDER_RESPONSE_COLUMNS["shipped_at"]: shipped_at,
        }

    with _shipment_status_lock:
        _shipment_status_cache[cache_key] = dict(result)

    return result


def til_events_fallback_data(shipment_order_number: object) -> dict[str, Any]:
    """Row-shaped GET_FULL_ORDER substitute built from the TIL event tables.

    Invoice/RPI links from prd_invoiceUpdate; tracking numbers, carriers and
    the returned-items confirmation from prd_shipmentStatus. Empty when
    neither table has anything, so the caller re-raises the 404.

    The remaining full_order_* fields stay blank on purpose: these events know
    nothing about the customer or the address, and inventing them would be
    worse than leaving the response builder to report them as missing.
    """

    result: dict[str, Any] = {}

    links = invoice_links_from_invoice_update(shipment_order_number)
    for data_key, link in links.items():
        result[FULL_ORDER_RESPONSE_COLUMNS[data_key]] = link
    if links:
        result[INVOICE_LINK_SOURCE_COLUMN] = INVOICE_LINK_SOURCE_INVOICE_UPDATE

    tracking = tracking_numbers_from_shipment_status(shipment_order_number)
    for column, value in tracking.items():
        if not is_blank(value):
            result[column] = value

    return result


# Kept for callers (and tests) that only want the invoice half.
def invoice_update_fallback_data(shipment_order_number: object) -> dict[str, Any]:
    links = invoice_links_from_invoice_update(shipment_order_number)
    if not links:
        return {}

    result: dict[str, Any] = {
        FULL_ORDER_RESPONSE_COLUMNS[data_key]: link
        for data_key, link in links.items()
    }
    result[INVOICE_LINK_SOURCE_COLUMN] = INVOICE_LINK_SOURCE_INVOICE_UPDATE
    return result


def _describe_til_fallback(fallback: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for data_key in ("commercial_invoice", "return_proforma_invoice"):
        value = fallback.get(FULL_ORDER_RESPONSE_COLUMNS[data_key])
        if not is_blank(value):
            count = len([item for item in str(value).split(";") if item.strip()])
            parts.append(f"{count} {data_key} link(s)")
    for data_key in ("export_tracking_number", "return_tracking_number"):
        value = fallback.get(FULL_ORDER_RESPONSE_COLUMNS[data_key])
        if not is_blank(value):
            parts.append(f"{data_key}={value}")
    returned_items = fallback.get(
        FULL_ORDER_RESPONSE_COLUMNS["returned_items_confirmation"]
    )
    if not is_blank(returned_items):
        count = len([line for line in str(returned_items).split("\n") if line.strip()])
        parts.append(f"{count} product id(s)")
    shipped_at = fallback.get(FULL_ORDER_RESPONSE_COLUMNS["shipped_at"])
    if not is_blank(shipped_at):
        parts.append(f"shipped_at={shipped_at}")
    return ", ".join(parts) or "nothing"


def fetch_shipment_order_data(
    shipment_order_number: object,
    *,
    client: GetFullOrderClient | None = None,
    selection_mode: str = SHIPMENT_SELECTION_EXACT,
    tracking_numbers: Iterable[str] = (),
) -> dict[str, Any]:
    client = client or GetFullOrderClient()

    try:
        payload = client.get_order_payload(shipment_order_number)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code != 404:
            raise

        # The order is unknown to GET_FULL_ORDER; the invoice and the tracking
        # numbers may still exist in the TIL event tables. See the section
        # header above.
        fallback = til_events_fallback_data(shipment_order_number)
        if not fallback:
            raise

        print(
            f"  GET_FULL_ORDER returned 404 for {shipment_order_number}; "
            f"recovered from the TIL events: {_describe_til_fallback(fallback)}."
        )
        return fallback

    return extract_shipment_order_data(
        payload,
        shipment_order_number,
        selection_mode=selection_mode,
        tracking_numbers=tracking_numbers,
    )


def _document_tracking_candidates(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Tracking values usable for LOA/POA generation, best first.

    The two full_order_* values are the fallback for requests whose tracking
    number was not in bi.shipping_platform_shipments: they are read from the
    GET_FULL_ORDER shipment block the order reference resolved to.
    """

    return (
        row.get("shipment_tracking_number"),
        row.get(FULL_ORDER_RESPONSE_COLUMNS["export_tracking_number"]),
        row.get("extracted_tracking_number"),
        row.get("return_tracking_number"),
        row.get(FULL_ORDER_RESPONSE_COLUMNS["return_tracking_number"]),
    )


def tracking_number_for_documents(row: Mapping[str, Any]) -> str:
    return first_available_value(
        row.get("extracted_tracking_number"),
        row.get("shipment_tracking_number"),
        row.get(FULL_ORDER_RESPONSE_COLUMNS["export_tracking_number"]),
        default="",
    )


def ups_account_number_for_documents(row: Mapping[str, Any]) -> str:
    ups_code = extract_ups_code(*_document_tracking_candidates(row))
    if ups_code:
        return ups_code

    # Some carrier emails or file names include an extra trailing check digit or
    # punctuation around an otherwise valid UPS tracking number. The account code
    # is still the 6 characters immediately after the leading 1Z. This fallback is
    # limited to LOA generation and does not alter the existing response logic for
    # ups_account_number.
    for value in _document_tracking_candidates(row):
        text = str(value or "").strip().upper()
        match = re.search(r"1Z([0-9A-Z]{6})", text)
        if match:
            return match.group(1)

    return ""


def _today() -> date:
    override = os.getenv("AGENT_TODAY", "").strip()
    if override:
        parsed = datetime.fromisoformat(override.replace("Z", "+00:00"))
        return parsed.date()
    return datetime.now(timezone.utc).date()


def today_ddmmyyyy() -> str:
    return _today().strftime("%d/%m/%Y")


def today_mmddyyyy() -> str:
    return _today().strftime("%m/%d/%Y")


def format_api_datetime(value: object, output_format: str) -> str:
    if is_blank(value):
        return ""

    raw = str(value).strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DocumentGenerationError(f"Cannot parse API shippedAt value: {raw!r}") from exc

    return parsed.strftime(output_format)


def document_local_path(path: str | Path | None) -> Path | None:
    if not path:
        return None

    text = str(path).strip()
    if not text or re.match(r"^[a-z][a-z0-9+.-]*://", text, flags=re.IGNORECASE):
        return None

    return Path(text)


def document_path_exists(path: str | Path | None) -> bool:
    local_path = document_local_path(path)
    return bool(local_path and local_path.exists() and local_path.is_file())


def is_url(value: object) -> bool:
    return bool(re.match(r"^[a-z][a-z0-9+.-]*://", str(value or "").strip(), flags=re.IGNORECASE))


def _invoice_filename_from_link(source_link: str, data_key: str, row: Mapping[str, Any] | None = None) -> str:
    row = row or {}
    parsed = urlparse(source_link)
    query_values = parse_qs(parsed.query)

    candidate = ""
    for query_key in ("link", "filename", "file", "name"):
        values = query_values.get(query_key)
        if values:
            candidate = values[0]
            break

    if not candidate:
        candidate = Path(unquote(parsed.path)).name

    candidate = unquote(str(candidate or "")).replace("\\", "/").rsplit("/", 1)[-1]
    if not candidate or candidate.lower().endswith((".aspx", ".ashx", ".php", ".html", ".htm")):
        fallback_parts = [
            data_key,
            row.get("shipment_order_number"),
            row.get("extracted_tracking_number"),
            row.get("shipment_tracking_number"),
            row.get("request_id"),
        ]
        candidate = "_".join(str(part).strip() for part in fallback_parts if not is_blank(part))

    filename = safe_filename(candidate)
    if not filename.lower().endswith(".pdf"):
        filename = f"{filename}.pdf"
    return filename


def _invoice_output_path(source_link: str, data_key: str, row: Mapping[str, Any] | None = None) -> Path:
    output_dir = GENERATED_DOCUMENTS_DIR / "invoice"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / _invoice_filename_from_link(source_link, data_key, row)


def _download_timeout() -> float:
    return float(os.getenv(INVOICE_DOWNLOAD_TIMEOUT_ENV, os.getenv(TIMEOUT_ENV, "60")))


def _download_max_attempts() -> int:
    try:
        return max(1, int(os.getenv(INVOICE_DOWNLOAD_MAX_ATTEMPTS_ENV, "12")))
    except ValueError:
        return 12


def _invoice_download_headers(referer: str | None = None) -> dict[str, str]:
    headers = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "application/pdf,application/octet-stream,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": os.getenv(
            INVOICE_DOWNLOAD_USER_AGENT_ENV,
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ),
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _looks_like_pdf(sample: bytes, content_type: object) -> bool:
    return b"%PDF" in sample or "pdf" in str(content_type or "").lower()


def _decode_response_bytes(content: bytes, response: requests.Response | None = None) -> str:
    """Decode already-downloaded response bytes without touching response.content.

    The invoice downloader streams responses to a temp file first. After a streamed
    response has been consumed, requests.Response.apparent_encoding tries to read
    response.content again and raises "The content for this response was already
    consumed". Keep decoding based only on headers plus safe fallbacks.
    """

    encodings: list[str] = []
    if response is not None:
        if response.encoding:
            encodings.append(response.encoding)

        content_type = str(response.headers.get("Content-Type", "") or "")
        charset_match = re.search(r"charset=([A-Za-z0-9_.:-]+)", content_type, flags=re.IGNORECASE)
        if charset_match:
            encodings.append(charset_match.group(1))

    encodings.extend(["utf-8", "utf-8-sig", "latin-1"])

    tried: set[str] = set()
    for encoding in encodings:
        normalized = str(encoding or "").strip().lower()
        if not normalized or normalized in tried:
            continue
        tried.add(normalized)
        try:
            return content.decode(encoding, errors="replace")
        except LookupError:
            continue
    return content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class InvoiceDownloadAttempt:
    """A concrete HTTP request to try while resolving an invoice PDF."""

    url: str
    method: str = "GET"
    data: tuple[tuple[str, str], ...] = ()
    referer: str | None = None

    def key(self) -> tuple[str, str, tuple[tuple[str, str], ...]]:
        normalized_url = self.url.split("#", 1)[0]
        return (self.method.upper(), normalized_url, self.data)

    def summary(self) -> str:
        method = self.method.upper()
        return self.url if method == "GET" else f"{method} {self.url}"


class _InvoiceHtmlParser(HTMLParser):
    """Extract links and ASP.NET form fields from a DocOpen HTML wrapper."""

    DOWNLOAD_ATTRIBUTES = {"href", "src", "data", "action", "formaction"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.forms: list[dict[str, Any]] = []
        self._current_form: dict[str, Any] | None = None

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {str(name).lower(): "" if value is None else str(value) for name, value in attrs}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_name = tag.lower()
        attr_map = self._attrs_to_dict(attrs)

        if tag_name == "form":
            self._current_form = {
                "action": attr_map.get("action", ""),
                "method": attr_map.get("method", "post"),
                "fields": [],
            }
            self.forms.append(self._current_form)

        for attr_name in self.DOWNLOAD_ATTRIBUTES:
            value = attr_map.get(attr_name)
            if value:
                self.links.append(value)

        if tag_name == "meta" and attr_map.get("http-equiv", "").lower() == "refresh":
            content = attr_map.get("content", "")
            match = re.search(r"url\s*=\s*([^;]+)$", content, flags=re.IGNORECASE)
            if match:
                self.links.append(match.group(1).strip())

        if tag_name == "input" and self._current_form is not None:
            name = attr_map.get("name", "")
            if name:
                fields = self._current_form.setdefault("fields", [])
                fields.append((name, attr_map.get("value", "")))

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "form":
            self._current_form = None


def _clean_invoice_candidate(candidate: str) -> str:
    cleaned = html_unescape(str(candidate or "")).strip()
    # JavaScript-generated attributes can arrive escaped as \"...\". Strip only
    # wrapping quote/backslash noise, not meaningful URL characters inside the value.
    for _ in range(3):
        stripped = cleaned.strip().strip('"\'')
        stripped = stripped.lstrip("\\").strip().strip('"\'')
        if stripped == cleaned:
            break
        cleaned = stripped
    cleaned = cleaned.replace("&amp;", "&").strip()
    return cleaned


def _append_invoice_candidate(
    candidates: list[InvoiceDownloadAttempt],
    seen: set[tuple[str, str, tuple[tuple[str, str], ...]]],
    candidate: str,
    *,
    base_url: str,
    method: str = "GET",
    data: Iterable[tuple[str, str]] | Mapping[str, Any] | None = None,
    referer: str | None = None,
) -> None:
    cleaned = _clean_invoice_candidate(candidate)
    if not cleaned:
        return

    lower_cleaned = cleaned.lower()
    if lower_cleaned.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
        return

    # Avoid malformed values produced by regex scans over quoted JavaScript such as
    # '"javascript:__doPostBack(...)'. Those are browser actions, not URLs.
    if "javascript:" in lower_cleaned[:40]:
        return

    absolute_url = urljoin(base_url, cleaned).split("#", 1)[0]
    if not is_url(absolute_url):
        return

    if isinstance(data, Mapping):
        normalized_data = tuple((str(key), "" if value is None else str(value)) for key, value in data.items())
    else:
        normalized_data = tuple((str(key), "" if value is None else str(value)) for key, value in (data or ()))

    attempt = InvoiceDownloadAttempt(
        url=absolute_url,
        method=method.upper(),
        data=normalized_data,
        referer=referer,
    )
    key = attempt.key()
    if key not in seen:
        seen.add(key)
        candidates.append(attempt)


def _parse_invoice_html(html: str) -> tuple[str, _InvoiceHtmlParser]:
    text = html_unescape(html or "").replace("\\/", "/")
    parser = _InvoiceHtmlParser()
    try:
        parser.feed(text)
    except Exception:
        # HTMLParser is best-effort; keep regex fallbacks available even for bad HTML.
        pass
    return text, parser


def _aspnet_postback_attempts_from_html(
    text: str,
    parser: _InvoiceHtmlParser,
    base_url: str,
) -> list[InvoiceDownloadAttempt]:
    """Build POST attempts for ASP.NET WebForms __doPostBack download links."""

    postbacks: list[InvoiceDownloadAttempt] = []
    seen: set[tuple[str, str, tuple[tuple[str, str], ...]]] = set()
    forms = parser.forms or [{"action": "", "method": "post", "fields": []}]

    for match in re.finditer(
        r"__doPostBack\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]*)['\"]\s*\)",
        text,
        flags=re.IGNORECASE,
    ):
        event_target = html_unescape(match.group(1)).strip()
        event_argument = html_unescape(match.group(2)).strip()
        if not event_target:
            continue

        for form in forms:
            fields = [
                (str(name), "" if value is None else str(value))
                for name, value in form.get("fields", [])
                if str(name) not in {"__EVENTTARGET", "__EVENTARGUMENT"}
            ]
            fields.extend([
                ("__EVENTTARGET", event_target),
                ("__EVENTARGUMENT", event_argument),
            ])
            action = str(form.get("action") or "").strip() or base_url
            _append_invoice_candidate(
                postbacks,
                seen,
                action,
                base_url=base_url,
                method="POST",
                data=fields,
                referer=base_url,
            )

    return postbacks


def _invoice_candidates_from_html(html: str, base_url: str) -> list[InvoiceDownloadAttempt]:
    """Return browser-download targets embedded in a DocOpen.aspx HTML wrapper."""

    if not html:
        return []

    text, parser = _parse_invoice_html(html)
    candidates: list[InvoiceDownloadAttempt] = []
    seen: set[tuple[str, str, tuple[tuple[str, str], ...]]] = set()

    # First emulate ASP.NET WebForms download links. DocOpen.aspx pages commonly
    # render a LinkButton whose href is javascript:__doPostBack(...); Chrome then
    # posts the hidden __VIEWSTATE/__EVENTVALIDATION fields back to DocOpen.aspx.
    for postback_attempt in _aspnet_postback_attempts_from_html(text, parser, base_url):
        key = postback_attempt.key()
        if key not in seen:
            seen.add(key)
            candidates.append(postback_attempt)

    # Then follow real URLs exposed in normal HTML attributes.
    for raw_link in parser.links:
        _append_invoice_candidate(
            candidates,
            seen,
            raw_link,
            base_url=base_url,
            referer=base_url,
        )

    # JavaScript redirects that Chrome follows but requests does not execute.
    redirect_patterns = [
        r"(?:window\.)?(?:location(?:\.href)?|document\.location)\s*=\s*['\"]([^'\"]+)['\"]",
        r"(?:window\.open|location\.assign|location\.replace)\s*\(\s*['\"]([^'\"]+)['\"]",
        r"http-equiv\s*=\s*['\"]?refresh['\"]?[^>]+content\s*=\s*['\"][^'\"]*?url=([^'\"]+)",
        r"content\s*=\s*['\"][^'\"]*?url=([^'\"]+)",
        r"(https?://[^\s\"'<>]+?\.pdf(?:\?[^\s\"'<>]*)?)",
        # Relative PDF URLs with an explicit path marker. This intentionally does
        # not match a bare invoice filename inside a DocOpen.aspx?link=... query,
        # because the valid entry point is DocOpen.aspx, not /<filename>.pdf.
        r"((?:/|\./|\.\./)[A-Za-z0-9_./%=&?+-]+?\.pdf(?:\?[^\s\"'<>]*)?)",
    ]
    for pattern in redirect_patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            raw_candidate = match.group(1).strip().rstrip(";,)>\"'")
            _append_invoice_candidate(
                candidates,
                seen,
                raw_candidate,
                base_url=base_url,
                referer=base_url,
            )

    parsed = urlparse(base_url)
    query_values = parse_qs(parsed.query)
    link_values = query_values.get("link") or query_values.get("filename") or query_values.get("file")
    if link_values:
        filename = str(link_values[0] or "").strip()
        if filename:
            quoted_filename = quote(filename, safe="/._-%")
            current_name = Path(parsed.path).name.lower()
            for replacement in (
                "DocDownload.aspx",
                "DocFile.aspx",
                "FileDownload.aspx",
                "Download.aspx",
                "DownloadFile.aspx",
                "GetFile.aspx",
            ):
                if current_name == replacement.lower():
                    continue
                candidate_url = parsed._replace(
                    path=str(Path(parsed.path).with_name(replacement)),
                    query=f"link={quoted_filename}",
                    fragment="",
                ).geturl()
                _append_invoice_candidate(
                    candidates,
                    seen,
                    candidate_url,
                    base_url=base_url,
                    referer=base_url,
                )

    return candidates


def _download_invoice_candidate(
    requester: requests.Session,
    attempt: InvoiceDownloadAttempt,
    output_path: Path,
) -> tuple[bool, str, str, list[InvoiceDownloadAttempt]]:
    """Try one HTTP request. Return (saved_pdf, content_type, final_url, html_candidates)."""

    response: requests.Response | None = None
    temp_path = output_path.with_name(f".{output_path.name}.tmp")
    sample = b""
    bytes_written = 0

    try:
        response = requester.request(
            attempt.method.upper(),
            attempt.url,
            data=list(attempt.data) if attempt.data else None,
            stream=True,
            timeout=_download_timeout(),
            allow_redirects=True,
            headers=_invoice_download_headers(attempt.referer),
        )
        response.raise_for_status()

        with temp_path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=128 * 1024):
                if not chunk:
                    continue
                if len(sample) < 4096:
                    sample = (sample + chunk)[:4096]
                bytes_written += len(chunk)
                file.write(chunk)

        if bytes_written == 0:
            raise DocumentGenerationError(f"Downloaded invoice PDF was empty: {attempt.url}")

        content_type = str(response.headers.get("Content-Type", "") or "").lower()
        final_url = str(response.url or attempt.url)
        if _looks_like_pdf(sample, content_type):
            temp_path.replace(output_path)
            return True, content_type, final_url, []

        content = temp_path.read_bytes()
        html_candidates: list[InvoiceDownloadAttempt] = []
        if "html" in content_type or b"<html" in sample.lower() or b"<script" in sample.lower():
            html = _decode_response_bytes(content, response)
            html_candidates = _invoice_candidates_from_html(html, final_url)

        if temp_path.exists():
            temp_path.unlink()
        return False, content_type, final_url, html_candidates
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise
    finally:
        if response is not None:
            response.close()


def download_invoice_pdf(
    source_link: object,
    data_key: str,
    *,
    row: Mapping[str, Any] | None = None,
    session: requests.Session | None = None,
    output_path_override: Path | None = None,
) -> str:
    """Download a PDF link (invoice, RPI, or hosted shipping label) into
    generated_documents/invoice by default.

    Some DocOpen.aspx links return an HTML wrapper that Chrome executes before
    downloading the real PDF. This function follows regular redirects, emulates
    ASP.NET __doPostBack links, and follows PDF/redirect URLs found in that wrapper.

    Pass output_path_override to save the downloaded PDF at a specific path
    (e.g. generated_documents/shipping_labels/<order>.pdf) instead of the
    default invoice output folder/filename derivation.
    """

    source = str(source_link or "").strip()
    if is_blank(source):
        raise DocumentGenerationError(f"Missing source link for {data_key}")

    if not is_url(source):
        if document_path_exists(source):
            return str(source)
        raise DocumentGenerationError(f"Invoice source is not a downloadable URL: {source}")

    if output_path_override is not None:
        output_path = output_path_override
        output_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        output_path = _invoice_output_path(source, data_key, row)
    if output_path.exists() and output_path.is_file() and output_path.stat().st_size > 0:
        return output_path.as_posix()

    requester = session or requests.Session()
    initial_attempt = InvoiceDownloadAttempt(url=source, method="GET")
    candidates: list[InvoiceDownloadAttempt] = [initial_attempt]
    seen = {initial_attempt.key()}
    last_content_type = "unknown"
    last_url = source
    last_error = ""
    attempted: list[str] = []
    max_attempts = _download_max_attempts()

    try:
        while candidates and len(attempted) < max_attempts:
            attempt = candidates.pop(0)
            attempted.append(attempt.summary())
            try:
                saved, content_type, final_url, html_candidates = _download_invoice_candidate(
                    requester,
                    attempt,
                    output_path,
                )
            except Exception as exc:
                last_error = str(exc)
                last_url = attempt.url
                continue

            last_content_type = content_type or "unknown"
            last_url = final_url or attempt.url
            if saved:
                return output_path.as_posix()

            for html_candidate in html_candidates:
                key = html_candidate.key()
                if key not in seen:
                    seen.add(key)
                    candidates.append(html_candidate)

        attempted_summary = ", ".join(attempted[:4])
        if len(attempted) > 4:
            attempted_summary += f", ... ({len(attempted)} total)"
        error_detail = f"; last error: {last_error}" if last_error else ""
        raise DocumentGenerationError(
            "Downloaded invoice content did not resolve to a PDF "
            f"(last Content-Type: {last_content_type}; last URL: {last_url}; "
            f"attempted: {attempted_summary}; max attempts: {max_attempts}{error_detail})"
        )
    finally:
        if session is None and isinstance(requester, requests.Session):
            requester.close()

def document_reference(path: str | Path | None) -> str:
    if not path:
        return PLACEHOLDER

    text = str(path).strip()
    if not text:
        return PLACEHOLDER

    if re.match(r"^[a-z][a-z0-9+.-]*://", text, flags=re.IGNORECASE):
        href = text
        parsed = urlparse(text)
        query_values = parse_qs(parsed.query)
        display_name = ""
        for query_key in ("link", "filename", "file", "name"):
            values = query_values.get(query_key)
            if values:
                display_name = values[0]
                break
        if not display_name:
            display_name = Path(unquote(parsed.path)).name or text
        display_name = unquote(str(display_name or "")).replace("\\", "/").rsplit("/", 1)[-1]
        display_path = Path(display_name or text)
    else:
        path_obj = Path(text)
        if path_obj.is_absolute():
            try:
                relative_path = path_obj.relative_to(Path.cwd()).as_posix()
            except ValueError:
                relative_path = path_obj.as_posix()
                href = relative_path
            else:
                href = _join_public_base_url(DOCUMENT_PUBLIC_BASE_URL, relative_path)
        else:
            relative_path = path_obj.as_posix()
            href = _join_public_base_url(DOCUMENT_PUBLIC_BASE_URL, relative_path)
        display_path = Path(relative_path)

    if DOCUMENT_RESPONSE_STYLE == "plain":
        return href

    return f"[{display_path.name}]({href})"


def _require_template(path: Path, label: str) -> None:
    if not path.exists():
        raise DocumentGenerationError(f"{label} template not found: {path}")


def _output_path(document_type: str, tracking_number: str) -> Path:
    output_dir = GENERATED_DOCUMENTS_DIR / document_type
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"{safe_filename(tracking_number)}.pdf"


def _first_rect(page: pymupdf.Page, text: str, *, occurrence: int = 0) -> pymupdf.Rect:
    rects = page.search_for(text)
    if not rects:
        raise DocumentGenerationError(f"Could not locate LOA template label: {text}")
    return rects[occurrence]


def _replace_value_after_label(
    page: pymupdf.Page,
    label: str,
    value: str,
    *,
    occurrence: int = 0,
    cover_width: float = 170,
    font_size: float = 12,
    x_padding: float = 4,
) -> None:
    label_rect = _first_rect(page, label, occurrence=occurrence)
    x0 = label_rect.x1 + x_padding
    y0 = label_rect.y0 - 1
    y1 = label_rect.y1 + 2
    value_width = pymupdf.get_text_length(value, fontname="helv", fontsize=font_size) + 10
    x1 = min(page.rect.width - 36, x0 + max(cover_width, value_width))
    cover_rect = pymupdf.Rect(x0, y0, x1, y1)
    page.add_redact_annot(cover_rect, fill=(1, 1, 1))
    page.apply_redactions()
    page.insert_text(
        pymupdf.Point(x0, label_rect.y1 - 1.5),
        value,
        fontname="helv",
        fontsize=font_size,
        color=(0, 0, 0),
        overlay=True,
    )


def generate_authorization_letter(row: Mapping[str, Any]) -> str:
    """
    Generate a filled LOA / authorization letter PDF and return its relative path.

    The generated file name is the extracted tracking number, stored under the
    authorization_letter subfolder to avoid collisions with other document types.
    """

    _require_template(LOA_TEMPLATE_PATH, "LOA")

    tracking_number = tracking_number_for_documents(row)
    ups_account = ups_account_number_for_documents(row)
    shipped_at = row.get("full_order_shipped_at") or row.get("shippedAt") or row.get("shipped_at")

    if is_blank(tracking_number):
        raise DocumentGenerationError("Cannot generate LOA without extracted tracking number")
    if is_blank(ups_account):
        raise DocumentGenerationError("Cannot generate LOA without UPS account number")
    if is_blank(shipped_at):
        raise DocumentGenerationError("Cannot generate LOA without full_order_shipped_at")

    export_date = format_api_datetime(shipped_at, "%d/%m/%Y")
    today = today_ddmmyyyy()
    output_path = _output_path("authorization_letter", tracking_number)

    doc = pymupdf.open(LOA_TEMPLATE_PATH)
    page = doc[0]

    _replace_value_after_label(
        page,
        "UPS Account number:",
        ups_account,
        cover_width=120,
    )
    _replace_value_after_label(
        page,
        "Tracking number(s):",
        tracking_number,
        cover_width=210,
    )
    _replace_value_after_label(
        page,
        "Export date:",
        export_date,
        cover_width=110,
    )
    _replace_value_after_label(
        page,
        "to our UPS account:",
        ups_account,
        cover_width=95,
    )
    _replace_value_after_label(
        page,
        "Date:",
        today,
        occurrence=-1,
        cover_width=110,
    )

    doc.save(output_path, deflate=True, garbage=4)
    doc.close()
    return output_path.as_posix()


def generate_power_of_attorney(row: Mapping[str, Any]) -> str:
    """Generate a filled POA PDF and return its relative path."""

    _require_template(POA_TEMPLATE_PATH, "POA")

    tracking_number = tracking_number_for_documents(row)
    if is_blank(tracking_number):
        raise DocumentGenerationError("Cannot generate POA without extracted tracking number")

    output_path = _output_path("power_of_attorney", tracking_number)
    today = today_mmddyyyy()

    doc = pymupdf.open(POA_TEMPLATE_PATH)
    updated_fields = set()

    for page in doc:
        widgets = page.widgets() or []
        for widget in widgets:
            if widget.field_name == "Tracking Number":
                widget.field_value = tracking_number
                widget.update()
                updated_fields.add(widget.field_name)
            elif widget.field_name == "Date":
                widget.field_value = today
                widget.update()
                updated_fields.add(widget.field_name)

    missing_fields = {"Tracking Number", "Date"} - updated_fields
    if missing_fields:
        doc.close()
        raise DocumentGenerationError(
            "POA template is missing expected form fields: " + ", ".join(sorted(missing_fields))
        )

    doc.save(output_path, deflate=True, garbage=4)
    doc.close()
    return output_path.as_posix()


# ---------------------------------------------------------------------------
# Shipping label (ZPL -> PDF)
# ---------------------------------------------------------------------------


def fetch_shipment_labels(
    shipment_order_number: str,
    *,
    weight: float | None = None,
    packages_number: int = 1,
    timeout: int | None = None,
) -> dict:
    """POST to the labels Logic App and return the parsed JSON response.

    Mirrors the request shape used in Labels_triggering_for_DGEUTST0001.py
    (same workflow, orderNumber/weight/packagesNumber body).
    """

    order_number = str(shipment_order_number or "").strip()
    if not order_number:
        raise DocumentGenerationError("Missing shipment_order_number for label lookup")

    # The labels API expects the shipmentOrderNumber shape used inside
    # GET_FULL_ORDER payloads (e.g. DG-EUA01667336), not the GET_FULL_ORDER
    # *order number* URL shape (e.g. DG-EU-01667336). Passing the order-number
    # shape here caused the labels API to return a payload with no matching
    # shipment block, so no labelData could ever be found.
    shipment_order_number_for_labels_api = shipment_order_number_for_shipment_block_lookup(
        order_number
    )

    body = {
        "orderNumber": shipment_order_number_for_labels_api,
        "weight": weight,
        "packagesNumber": packages_number,
    }

    response = requests.post(
        SHIPPING_LABELS_API_URL,
        params=SHIPPING_LABELS_API_PARAMS,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=body,
        timeout=timeout or SHIPPING_LABELS_API_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _label_shipment_blocks(payload: dict) -> list[dict]:
    """Return the list of shipment label blocks from a labels API payload.

    The real response shape is:
        {"success": true, "data": [{"shipment": {...}, ...}, ...], "error": null}

    i.e. a top-level "data" list where each item wraps its label block under
    "shipment" (one item per parcel/leg: outbound + return). The previous
    implementation looked for a top-level "shipment"/"shipments" key, which
    doesn't exist in this shape, so no block was ever found.
    """

    if not isinstance(payload, dict):
        return []

    items = payload.get("data")
    if isinstance(items, list):
        blocks = [
            item.get("shipment")
            for item in items
            if isinstance(item, dict) and isinstance(item.get("shipment"), dict)
        ]
        if blocks:
            return blocks

    # Fall back to the older/alternate shapes this function originally
    # supported, in case some environments still return them.
    shipments = payload.get("shipment")
    if shipments is None:
        shipments = payload.get("shipments")

    if isinstance(shipments, dict):
        return [shipments]
    if isinstance(shipments, list):
        return [block for block in shipments if isinstance(block, dict)]
    return []


def extract_return_label_data(payload: dict) -> str:
    """Return the raw labelData for the return label block (shipment.isReturn == False).

    The labels API usually returns two blocks: one for the outbound shipment
    and one for the return. The return label is the block where
    isReturn == False (the outbound leg carries isReturn == True since it is
    part of a returns flow); labelData holds either a (often base64-encoded)
    ZPL string, or a URL pointing to a hosted PDF label (e.g. an Azure blob
    storage link such as
    "https://tlgspstorage.blob.core.windows.net/pdflabels/.../LABEL.pdf").
    Callers should inspect the returned value (see _label_data_is_pdf_link)
    to decide whether to decode it as ZPL or download it as a PDF.
    """

    for block in _label_shipment_blocks(payload):
        if block.get("isReturn") is False:
            label_data = block.get("labelData")
            if label_data:
                return str(label_data)

    raise DocumentGenerationError(
        "No return label block (shipment.isReturn == false) with labelData "
        "found in the labels API response"
    )


# Backwards-compatible alias: existing callers/tests may still import the
# original name. The returned value can now be either ZPL or a PDF URL.
extract_return_label_zpl = extract_return_label_data


def _label_data_is_pdf_link(label_data: str) -> bool:
    """Return True if labelData looks like a URL to a hosted PDF label.

    Some carriers/label providers return a direct link to a pre-rendered PDF
    (e.g. Azure blob storage URLs like
    ".../pdflabels/2026/06/12/LN-EUA01675973_....pdf") instead of a ZPL
    string. In that case the label should simply be downloaded, the same way
    invoices/RPIs are downloaded from GETFULLORDER links, instead of being
    decoded/rendered as ZPL.
    """

    text = str(label_data or "").strip()
    if not text:
        return False
    if not is_url(text):
        return False

    parsed = urlparse(text)
    path = unquote(parsed.path or "")
    return path.lower().endswith(".pdf")


def _decode_zpl(encoded_zpl: str) -> str:
    encoded_zpl = str(encoded_zpl or "").strip()
    try:
        zpl = base64.b64decode(encoded_zpl, validate=True).decode("utf-8", errors="ignore")
    except (binascii.Error, ValueError):
        zpl = encoded_zpl

    if "^XA" not in zpl or "^XZ" not in zpl:
        raise DocumentGenerationError(
            "Decoded label data does not contain a complete ZPL label (^XA ... ^XZ)"
        )
    return zpl


def render_zpl_label_pdf(encoded_zpl: str, output_path: Path) -> str:
    """Decode a (base64) ZPL label and render it to a PDF at output_path."""

    from PIL import Image
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas

    zpl = _decode_zpl(encoded_zpl)

    response = requests.post(
        ZPL_LABEL_RENDERER_URL,
        headers={"Accept": "image/png"},
        data=zpl.encode("utf-8"),
        timeout=SHIPPING_LABELS_API_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise DocumentGenerationError(
            f"ZPL label rendering failed (status {response.status_code}): {response.text[:300]}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    png_path = output_path.with_suffix(".png")
    png_path.write_bytes(response.content)

    try:
        image = Image.open(png_path)
        pdf = canvas.Canvas(str(output_path))
        pdf.setPageSize(image.size)
        pdf.drawImage(ImageReader(image), 0, 0, width=image.width, height=image.height)
        pdf.save()
    finally:
        if png_path.exists():
            png_path.unlink()

    return output_path.as_posix()


def generate_shipping_label_document(row: Mapping[str, Any]) -> str:
    """Fetch, decode, and render the return shipping label PDF for a row.

    Saves into generated_documents/shipping_labels and returns the local path
    (or ";"-joined paths when the ticket covers multiple shipments).
    """

    shipment_orders = [
        item.strip()
        for item in str(row.get("shipment_order_number") or "").split(";")
        if item.strip()
    ]
    if not shipment_orders:
        raise DocumentGenerationError("Missing shipment_order_number for shipping label generation")

    output_dir = GENERATED_DOCUMENTS_DIR / "shipping_labels"
    generated_paths: list[str] = []

    for shipment_order_number in shipment_orders:
        output_path = output_dir / f"{safe_filename(shipment_order_number)}.pdf"
        if output_path.exists() and output_path.is_file() and output_path.stat().st_size > 0:
            generated_paths.append(output_path.as_posix())
            continue

        payload = fetch_shipment_labels(shipment_order_number)
        label_data = extract_return_label_data(payload)

        if _label_data_is_pdf_link(label_data):
            # labelData is already a hosted PDF link (e.g. an Azure blob
            # storage URL) rather than ZPL. Download it directly, the same
            # way invoices/RPIs are downloaded from GETFULLORDER links,
            # instead of trying to decode/render it as ZPL.
            generated_paths.append(
                download_invoice_pdf(
                    label_data,
                    "shipping_label",
                    row=row,
                    output_path_override=output_path,
                )
            )
        else:
            generated_paths.append(render_zpl_label_pdf(label_data, output_path))

    return ";".join(generated_paths)


def document_value_for_response(document_path: str | None) -> str:
    return document_reference(document_path)


if __name__ == "__main__":
    main()
