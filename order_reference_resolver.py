"""
order_reference_resolver.py

Shared order-reference helpers used as a fallback when the tracking number is
not usable.

The pipeline resolves a ticket to a GET_FULL_ORDER order in this order:

1. extracted_tracking_number -> bi.shipping_platform_shipments
   (handled in ticket_fetcher.enrich_with_shipment_numbers);
2. order reference found in the ticket subject / description / request body;
3. order reference found inside a PDF attached to the ticket.

An order reference always begins with one of ORDER_REFERENCE_PREFIXES (five
characters, e.g. ``DG-EU`` or ``CVXEU``).  The character immediately after the
prefix decides what the reference is:

* a letter  -> shipment number, e.g. ``DG-EUA01663254``
* a ``-``   -> order number,    e.g. ``DG-EU-01663254``

GET_FULL_ORDER always wants the *order* number, so a shipment-shaped reference
has its letter replaced by ``-`` before the lookup
(``DG-EUA01663254`` -> ``DG-EU-01663254``).  That is exactly the
``_replace_sixth_character_with_dash`` rule already used by
``response_data_extractor.py``, so both shapes feed the existing URL builder
unchanged.

Orders whose prefix is in BLOCKED_ORDER_REFERENCE_PREFIXES are never processed:
the ticket is left exactly as it is, with no reply, no solve and no group
re-assignment.

PDF reading uses the embedded text layer when there is one and falls back to
local Tesseract OCR, mirroring ticket_order_numbers.py.  Both pymupdf and
pytesseract are imported defensively so a runner without them degrades to "no
order reference found" instead of crashing the pipeline.
"""

from __future__ import annotations

import io
import os
import re
from typing import Any, Callable, Iterable, Mapping

try:  # PyMuPDF >= 1.24.3 exposes `pymupdf`; older releases only ship `fitz`.
    import pymupdf  # type: ignore
except ImportError:  # pragma: no cover - defensive
    try:
        import fitz as pymupdf  # type: ignore
    except ImportError:  # pragma: no cover - defensive
        pymupdf = None  # type: ignore

try:  # OCR is optional: scanned PDFs are simply skipped when it is missing.
    import pytesseract  # type: ignore
    from PIL import Image  # type: ignore
except ImportError:  # pragma: no cover - defensive
    pytesseract = None  # type: ignore
    Image = None  # type: ignore


# ---------------------------------------------------------------------------
# Prefixes
# ---------------------------------------------------------------------------

ORDER_REFERENCE_PREFIXES: tuple[str, ...] = (
    "AL-EU",
    "AT-EU",
    "AT-MT",
    "BB-BS",
    "BB-EU",
    "BB-FF",
    "BB-MT",
    "BB-ZL",
    "BO-EU",
    "BO-MT",
    "CA-EU",
    "CA-FF",
    "CA-MT",
    "CV-AC",
    "CV-EU",
    "CV-FF",
    "CV-MT",
    "CV-US",
    "CVXEU",
    "DG-EU",
    "DG-FF",
    "DG-JP",
    "DG-KR",
    "DG-MX",
    "DG-SS",
    "DG-US",
    "DJ-BL",
    "DJ-EU",
    "DJ-FF",
    "DJ-GL",
    "DJ-MT",
    "DJ-NP",
    "DJ-OM",
    "DJ-SP",
    "DJ-SS",
    "DJ-US",
    "FA-EU",
    "FA-US",
    "FE-EU",
    "FE-US",
    "FL-EU",
    "FO-EU",
    "FU-EU",
    "FU-MT",
    "FU-ZL",
    "HE-EU",
    "HE-FF",
    "HE-JP",
    "HE-US",
    "LA-EU",
    "LA-FF",
    "LA-US",
    "LN-AZ",
    "LN-BU",
    "LN-CT",
    "LN-EU",
    "LN-FF",
    "LN-IM",
    "LN-LX",
    "LN-MT",
    "LN-NA",
    "LN-US",
    "MA-EU",
    "MA-FF",
    "MA-MT",
    "MA-ZL",
    "MO-EU",
    "MO-FF",
    "MO-MT",
    "OW-EU",
    "OW-FF",
    "OW-US",
    "PA-EU",
    "PA-FF",
    "PA-US",
    "PJ-BS",
    "PJ-EU",
    "PJ-FF",
    "PJ-MT",
    "PJ-US",
    "PL-EU",
    "PL-FF",
    "PL-MT",
    "RC-EU",
    "RC-US",
    "SC-BS",
    "SC-EU",
    "SC-FF",
    "SC-LX",
    "SC-MT",
    "SC-NP",
    "SC-PZ",
    "SC-SS",
    "SC-US",
    "SC-ZL",
    "SW-EU",
    "SW-FF",
    "SW-MT",
    "SW-ZL",
    "WF-EU",
    "WF-FF",
    "WF-MK",
    "WF-MT",
    "WF-VP",
    "WF-ZL",
    "WL-BS",
    "WL-BU",
    "WL-CT",
    "WL-EP",
    "WL-EU",
    "WL-FF",
    "WL-MS",
    "ZH-EU",
)

ORDER_REFERENCE_PREFIX_SET = frozenset(ORDER_REFERENCE_PREFIXES)

PREFIX_LENGTH = 5

# Orders on these prefixes are never processed: the ticket is left untouched.
BLOCKED_ORDER_REFERENCE_PREFIXES: tuple[str, ...] = (
    "RC-US",
    "RC-EU",
    "LN-EU",
    "LN-US",
)
BLOCKED_ORDER_REFERENCE_PREFIX_SET = frozenset(BLOCKED_ORDER_REFERENCE_PREFIXES)


# ---------------------------------------------------------------------------
# How response_data_extractor picks shipment blocks out of a GET_FULL_ORDER
# payload.  Stored per request in the `shipment_selection_mode` column.
# ---------------------------------------------------------------------------

# The order came from the tracking number via bi.shipping_platform_shipments,
# so the exact shipment block is known. Historic behavior.
SHIPMENT_SELECTION_EXACT = "exact"
# A tracking number was found in the ticket but not in
# bi.shipping_platform_shipments, and the order was resolved from an order
# reference instead. Use the shipment block whose shipping/return label carries
# that tracking number, and fall back to every shipment when none matches.
SHIPMENT_SELECTION_BY_TRACKING = "by_tracking"
# No usable tracking number at all: take the data from every shipment in the
# GET_FULL_ORDER response.
SHIPMENT_SELECTION_ALL = "all"

SHIPMENT_SELECTION_MODES = (
    SHIPMENT_SELECTION_EXACT,
    SHIPMENT_SELECTION_BY_TRACKING,
    SHIPMENT_SELECTION_ALL,
)

# Where the order reference came from, stored in `order_reference_source`.
ORDER_REFERENCE_SOURCE_TRACKING = "tracking_lookup"
ORDER_REFERENCE_SOURCE_TEXT = "subject_or_description"
ORDER_REFERENCE_SOURCE_PDF = "pdf_attachment"


# ---------------------------------------------------------------------------
# Regex
# ---------------------------------------------------------------------------

# Dash-like characters an email client, a PDF or OCR may produce instead of "-".
_DASHES = "\\-\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
_SEPARATOR = f"[{_DASHES}_\\s]"
_DASH_TRANSLATION = {ord(char): "-" for char in "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"}

# Characters Tesseract commonly mixes up, mapped to their digit equivalent.
# Same table as ticket_order_numbers.py.
_DIGIT_FIXES = str.maketrans(
    {
        "O": "0",
        "o": "0",
        "Q": "0",
        "D": "0",
        "I": "1",
        "l": "1",
        "|": "1",
        "S": "5",
        "s": "5",
        "B": "8",
        "Z": "2",
        "G": "6",
    }
)


def _prefix_pattern(prefix: str) -> str:
    """Return the regex for one prefix, tolerating odd separators."""

    if "-" in prefix:
        head, tail = prefix.split("-", 1)
        return f"{re.escape(head)}{_SEPARATOR}{re.escape(tail)}"
    return re.escape(prefix)


_PREFIX_ALTERNATION = "|".join(
    _prefix_pattern(prefix)
    # Longest first so an unusual prefix can never be shadowed by a shorter one.
    for prefix in sorted(ORDER_REFERENCE_PREFIXES, key=len, reverse=True)
)

ORDER_REFERENCE_RE = re.compile(
    rf"\b(?P<prefix>{_PREFIX_ALTERNATION})"
    rf"(?P<marker>[A-Z]|{_SEPARATOR})"
    r"(?P<tail>[A-Z0-9]{4,20})\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Reference parsing
# ---------------------------------------------------------------------------


def _normalize_dashes(text: object) -> str:
    return str(text or "").translate(_DASH_TRANSLATION)


def _canonical_prefix(raw_prefix: str) -> str:
    """Map a matched prefix back to its canonical 5-character form."""

    compact = re.sub(r"[^A-Z0-9]", "", raw_prefix.upper())
    if len(compact) == PREFIX_LENGTH and compact in ORDER_REFERENCE_PREFIX_SET:
        return compact
    if len(compact) == 4:
        candidate = f"{compact[:2]}-{compact[2:]}"
        if candidate in ORDER_REFERENCE_PREFIX_SET:
            return candidate
    return compact


def _normalize_tail(tail: str) -> str:
    tail = tail.upper()
    digits = sum(character.isdigit() for character in tail)
    # Mostly-numeric tails are OCR-corrected, exactly like ticket_order_numbers.py.
    if digits and digits >= len(tail) - 2:
        tail = tail.translate(_DIGIT_FIXES)
    return tail


def find_order_references(*texts: object) -> list[str]:
    """Return every canonical order reference found in the given texts."""

    found: list[str] = []
    for text in texts:
        normalized = _normalize_dashes(text)
        if not normalized:
            continue
        for match in ORDER_REFERENCE_RE.finditer(normalized):
            prefix = _canonical_prefix(match.group("prefix"))
            if prefix not in ORDER_REFERENCE_PREFIX_SET:
                continue
            marker = match.group("marker")
            marker = marker.upper() if marker.isalpha() else "-"
            reference = f"{prefix}{marker}{_normalize_tail(match.group('tail'))}"
            if reference not in found:
                found.append(reference)
    return found


def normalize_order_reference(value: object) -> str:
    value = _normalize_dashes(value).strip().upper()
    return re.sub(r"\s+", "", value)


def order_reference_prefix(value: object) -> str:
    return normalize_order_reference(value)[:PREFIX_LENGTH]


def is_order_reference(value: object) -> bool:
    normalized = normalize_order_reference(value)
    return (
        len(normalized) > PREFIX_LENGTH
        and order_reference_prefix(normalized) in ORDER_REFERENCE_PREFIX_SET
    )


def is_shipment_shaped_order_reference(value: object) -> bool:
    """True when the character after the prefix is a letter (shipment number)."""

    normalized = normalize_order_reference(value)
    if len(normalized) <= PREFIX_LENGTH:
        return False
    return normalized[PREFIX_LENGTH].isalpha()


def order_number_from_order_reference(value: object) -> str:
    """Return the order-number shape used by GET_FULL_ORDER.

    A shipment-shaped reference has the letter after the prefix replaced by a
    dash; an order-shaped reference is returned unchanged.
    """

    normalized = normalize_order_reference(value)
    if not normalized:
        return ""
    if is_shipment_shaped_order_reference(normalized):
        return f"{normalized[:PREFIX_LENGTH]}-{normalized[PREFIX_LENGTH + 1:]}"
    return normalized


def is_blocked_order_reference(value: object) -> bool:
    """True for RC-US / RC-EU / LN-EU / LN-US orders, which are never processed."""

    return order_reference_prefix(value) in BLOCKED_ORDER_REFERENCE_PREFIX_SET


def is_missing_value(value: object) -> bool:
    """True for None and for a float NaN, without importing pandas here.

    A NULL BigQuery column arrives as a float NaN once pandas has read it, and
    NaN is *truthy*: ``str(nan)`` is ``"nan"``. Anything that stringifies a
    column value has to rule that out first or the string "nan" leaks through
    and is treated as real data.
    """

    if value is None:
        return True
    return value != value  # only NaN is unequal to itself


def split_order_references(value: object) -> list[str]:
    """Split a stored ``a;b`` order-reference column into single references.

    Every token is validated against the known-prefix pattern, so only strings
    that really are order references survive. This is what keeps a NULL column
    (NaN -> "nan" -> "NAN") or any other junk from becoming a phantom order:
    an unresolvable row has to come out of here *empty*, because that is what
    routes it to human review instead of to a confidently wrong answer.
    """

    if is_missing_value(value):
        return []

    references: list[str] = []
    for part in re.split(r"[;,]", str(value)):
        normalized = normalize_order_reference(part)
        if not normalized or normalized.upper() == "N/A":
            continue
        # find_order_references() enforces prefix + marker + tail and returns
        # the canonical form, so the stored-column path and the ticket-text
        # path can never disagree about what counts as a reference.
        for reference in find_order_references(normalized):
            if reference not in references:
                references.append(reference)
    return references


def contains_blocked_order_reference(*values: object) -> bool:
    for value in values:
        for reference in split_order_references(value):
            if is_blocked_order_reference(reference):
                return True
    return False


# ---------------------------------------------------------------------------
# PDF attachments
# ---------------------------------------------------------------------------

PDF_OCR_DPI = int(os.getenv("ORDER_REFERENCE_PDF_OCR_DPI", "300") or "300")
PDF_OCR_RETRY_DPI = int(os.getenv("ORDER_REFERENCE_PDF_OCR_RETRY_DPI", "400") or "400")
PDF_OCR_ENABLED = str(
    os.getenv("ORDER_REFERENCE_PDF_OCR_ENABLED", "true")
).strip().lower() in {"true", "1", "yes", "y", "on"}
_TESSERACT_EXE = os.getenv("TESSERACT_EXE", "").strip()

if _TESSERACT_EXE and pytesseract is not None:  # pragma: no cover - env specific
    pytesseract.pytesseract.tesseract_cmd = _TESSERACT_EXE


def is_pdf_attachment(attachment: Mapping[str, Any]) -> bool:
    name = str(attachment.get("file_name") or "").lower()
    content_type = str(attachment.get("content_type") or "").lower()
    return name.endswith(".pdf") or "pdf" in content_type


def _pdf_text(pdf_bytes: bytes, dpi: int) -> tuple[str, str]:
    """Return (text, method). Uses the text layer when there is one, else OCR."""

    if pymupdf is None:  # pragma: no cover - defensive
        raise RuntimeError("pymupdf is not installed; cannot read PDF attachments")

    document = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        layer = "\n".join(page.get_text() for page in document)
        if len(layer.strip()) > 50:
            return layer, "text-layer"

        if not PDF_OCR_ENABLED or pytesseract is None or Image is None:
            return layer, "text-layer"

        zoom = dpi / 72  # PDF user space is 72 dpi
        matrix = pymupdf.Matrix(zoom, zoom)
        chunks = []
        for page in document:
            pixmap = page.get_pixmap(matrix=matrix, colorspace=pymupdf.csGRAY)
            image = Image.open(io.BytesIO(pixmap.tobytes("png")))
            chunks.append(pytesseract.image_to_string(image))
        return "\n".join(chunks), "ocr"
    finally:
        document.close()


def find_order_references_in_pdf_bytes(pdf_bytes: bytes) -> tuple[list[str], str]:
    """Extract order references from one PDF, retrying OCR at higher resolution."""

    text, method = _pdf_text(pdf_bytes, PDF_OCR_DPI)
    references = find_order_references(text)
    if not references and method == "ocr":
        text, _ = _pdf_text(pdf_bytes, PDF_OCR_RETRY_DPI)
        references = find_order_references(text)
    return references, method


def find_order_references_in_pdfs(
    pdf_blobs: Iterable[tuple[str, bytes]],
    *,
    log: Callable[[str], None] = print,
) -> list[str]:
    """Scan several PDFs and return every order reference found, de-duplicated.

    ``log`` receives one line per PDF. It defaults to ``print`` so existing
    callers are unchanged; concurrent callers pass a collector instead and
    replay the lines on the main thread, keeping the run log readable.
    """

    references: list[str] = []
    for file_name, pdf_bytes in pdf_blobs:
        try:
            found, method = find_order_references_in_pdf_bytes(pdf_bytes)
        except Exception as exc:  # noqa: BLE001 - one bad PDF must not stop the rest
            log(f"      could not read PDF {file_name}: {exc}")
            continue
        if found:
            log(f"      order reference(s) in {file_name}: {found} (via {method})")
        for reference in found:
            if reference not in references:
                references.append(reference)
    return references
