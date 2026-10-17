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
    "HB-EU",
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

# Orders on these prefixes are never processed, whatever the destination:
# the ticket gets no reply and goes to the second-level logistics group.
ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIXES: tuple[str, ...] = (
    "RC-US",
    "LN-US",
)
ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIX_SET = frozenset(
    ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIXES
)

# Orders on these prefixes depend on where the shipment is going. They are
# answered normally unless the GET_FULL_ORDER destination country is one of
# BLOCKED_DESTINATION_COUNTRY_CODES, in which case they are treated exactly
# like the always-blocked prefixes above. The destination is only known after
# the GET_FULL_ORDER lookup, so these rows must be allowed through the earlier
# stages rather than short-circuited there.
DESTINATION_CHECKED_ORDER_REFERENCE_PREFIXES: tuple[str, ...] = (
    "RC-EU",
    "LN-EU",
)
DESTINATION_CHECKED_ORDER_REFERENCE_PREFIX_SET = frozenset(
    DESTINATION_CHECKED_ORDER_REFERENCE_PREFIXES
)

# Destination countries that block a RC-EU / LN-EU order.
BLOCKED_DESTINATION_COUNTRY_CODES = frozenset({"US", "GB"})

# Every prefix that may end up blocked, for messages and back-compatibility.
BLOCKED_ORDER_REFERENCE_PREFIXES: tuple[str, ...] = (
    ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIXES
    + DESTINATION_CHECKED_ORDER_REFERENCE_PREFIXES
)
BLOCKED_ORDER_REFERENCE_PREFIX_SET = frozenset(BLOCKED_ORDER_REFERENCE_PREFIXES)

# Three-state outcome of order_processing_state().
ORDER_PROCESSING_ALLOWED = "allowed"
ORDER_PROCESSING_BLOCKED = "blocked"
ORDER_PROCESSING_DESTINATION_PENDING = "destination_pending"


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
# Last resort: the tracking number was not in bi.shipping_platform_shipments
# and the ticket carried no reference, so the shipment order number came from
# the carrier events in til.prd_shipmentStatus.
ORDER_REFERENCE_SOURCE_SHIPMENT_STATUS = "shipment_status_events"


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

# The boundaries are explicit "not an alphanumeric" assertions rather than \b.
# \b treats "_" as a word character, and the UPS return label prints the
# reference with a trailing underscore -- "Reference No.1: CV-US-00042647_" --
# so \b forced the match to fail on exactly the line that carries the order
# number. (The tail cannot be shortened to find a boundary either, because the
# next character is still a digit, so the whole match was lost rather than
# truncated.) These assertions keep every previous match and add the
# underscore-delimited ones.
ORDER_REFERENCE_RE = re.compile(
    rf"(?<![A-Z0-9])(?P<prefix>{_PREFIX_ALTERNATION})"
    rf"(?P<marker>[A-Z]|{_SEPARATOR})"
    r"(?P<tail>[A-Z0-9]{4,20})(?![A-Z0-9])",
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
            reference = strip_reissued_reference_suffix(
                f"{prefix}{marker}{_normalize_tail(match.group('tail'))}"
            )
            if reference not in found:
                found.append(reference)
    return found


# A reference re-issued after a correction is printed with a "_NEW" suffix
# (e.g. "DG-USB11607007_NEW"). The suffix is bookkeeping, not part of the
# number: GET_FULL_ORDER and bi.shipping_platform_shipments both know the
# order only as "DG-USB11607007", so it is stripped everywhere a reference is
# normalized. Trailing separators left behind by the suffix go with it.
# The separator before NEW is required: stripping a bare trailing "NEW" would
# mangle any genuine reference whose tail happens to end in those letters.
_REISSUED_REFERENCE_SUFFIX_RE = re.compile(
    r"[\-_\s]+NEW[\-_\s]*$",
    re.IGNORECASE,
)


def strip_reissued_reference_suffix(value: str) -> str:
    """Drop a trailing "_NEW" marker from an already-normalized reference."""

    stripped = _REISSUED_REFERENCE_SUFFIX_RE.sub("", value)
    # Never strip the reference away entirely: a token that is nothing but
    # "NEW" is not a reference, and returning "" here would hide that from
    # the caller's own validation.
    return stripped or value


def normalize_order_reference(value: object) -> str:
    value = _normalize_dashes(value).strip().upper()
    value = re.sub(r"\s+", "", value)
    return strip_reissued_reference_suffix(value)


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
    """True for any prefix that may be blocked (RC-US / LN-US / RC-EU / LN-EU).

    Kept for callers that only need "could this order be blocked at all".
    Whether a RC-EU / LN-EU order *is* blocked depends on its destination:
    use order_processing_state() for that decision.
    """

    return order_reference_prefix(value) in BLOCKED_ORDER_REFERENCE_PREFIX_SET


def is_always_blocked_order_reference(value: object) -> bool:
    """True for RC-US / LN-US orders, which are never processed."""

    return order_reference_prefix(value) in ALWAYS_BLOCKED_ORDER_REFERENCE_PREFIX_SET


def is_destination_checked_order_reference(value: object) -> bool:
    """True for RC-EU / LN-EU orders, whose outcome depends on the destination."""

    return (
        order_reference_prefix(value)
        in DESTINATION_CHECKED_ORDER_REFERENCE_PREFIX_SET
    )


def destination_country_codes(value: object) -> list[str]:
    """Every ISO alpha-2 destination in a country column, in order.

    Normally one code, but an order whose shipments go to different places
    arrives joined, e.g. "CH;GB". Reading that as a single string hides the
    GB, so every consumer has to look at the list rather than the raw value.
    """

    if is_missing_value(value):
        return []

    codes: list[str] = []
    for part in re.split(r"[^A-Za-z]+", str(value)):
        code = part.strip().upper()
        if len(code) == 2 and code not in {"NA"} and code not in codes:
            codes.append(code)
    return codes


def normalize_destination_country_code(value: object) -> str:
    """The first ISO alpha-2 destination, upper-cased; "" when not known yet."""

    codes = destination_country_codes(value)
    return codes[0] if codes else ""


def destination_country_blocks_order(value: object) -> bool:
    """True when ANY destination on the order blocks a RC-EU / LN-EU order.

    Any is deliberate: an order with one parcel to Switzerland and one to
    Great Britain still has a GB leg, and the policy is that GB legs are not
    answered automatically.
    """

    return any(
        code in BLOCKED_DESTINATION_COUNTRY_CODES
        for code in destination_country_codes(value)
    )


def order_processing_state(
    *references: object,
    destination_country_code: object = None,
    blocked_flag: object = False,
) -> str:
    """Decide whether an order may be processed, in three states.

    * ``ORDER_PROCESSING_BLOCKED`` - no reply, no solve, assigned to the
      second-level logistics group. RC-US / LN-US always; RC-EU / LN-EU when
      the GET_FULL_ORDER destination country is US or GB.
    * ``ORDER_PROCESSING_DESTINATION_PENDING`` - a RC-EU / LN-EU order whose
      destination is not known yet. The request must be processed normally up
      to and including the GET_FULL_ORDER lookup, and this function asked
      again once ``full_order_country_code`` is populated.
    * ``ORDER_PROCESSING_ALLOWED`` - answer normally.

    ``blocked_flag`` is the upstream ``order_processing_blocked`` column. It
    is produced before any destination is known, so on its own it only blocks
    when no reference is available to classify; a RC-EU / LN-EU reference
    always decides for itself.
    """

    found = []
    for value in references:
        for reference in split_order_references(value):
            if reference not in found:
                found.append(reference)

    if any(is_always_blocked_order_reference(reference) for reference in found):
        return ORDER_PROCESSING_BLOCKED

    destination_checked = [
        reference
        for reference in found
        if is_destination_checked_order_reference(reference)
    ]
    if destination_checked:
        codes = destination_country_codes(destination_country_code)
        if not codes:
            return ORDER_PROCESSING_DESTINATION_PENDING
        if any(code in BLOCKED_DESTINATION_COUNTRY_CODES for code in codes):
            return ORDER_PROCESSING_BLOCKED
        return ORDER_PROCESSING_ALLOWED

    # No blockable reference of our own. Fall back to the upstream flag, which
    # is the only signal left when the reference never made it into a column.
    if not is_missing_value(blocked_flag) and str(blocked_flag).strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
    }:
        return ORDER_PROCESSING_BLOCKED

    return ORDER_PROCESSING_ALLOWED


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


def contains_always_blocked_order_reference(*values: object) -> bool:
    """True when any reference is RC-US / LN-US."""

    for value in values:
        for reference in split_order_references(value):
            if is_always_blocked_order_reference(reference):
                return True
    return False


def contains_blocked_order_reference(*values: object) -> bool:
    for value in values:
        for reference in split_order_references(value):
            if is_blocked_order_reference(reference):
                return True
    return False


# ---------------------------------------------------------------------------
# PDF attachments
# ---------------------------------------------------------------------------

# 300 dpi, one pass per PDF, matching ticket_order_numbers.py.
PDF_OCR_DPI = int(os.getenv("ORDER_REFERENCE_PDF_OCR_DPI", "300") or "300")
# Hard ceiling on the rendered size of a single page, in pixels.
#
# A4 at 300 dpi is ~8.7M pixels, so this allows roughly A2. Carrier scans
# occasionally declare an A0-sized page: at 300 dpi that renders to ~150M
# pixels, which is ~150 MB of grayscale for ONE page, trips Pillow's
# decompression-bomb warning, and gives Tesseract an image it will chew on for
# minutes. Above this budget the page is rendered at a proportionally lower
# zoom instead, which is both faster and no worse for OCR: the text on an
# oversized page is drawn proportionally larger, so the effective resolution
# relative to the glyphs is unchanged.
PDF_OCR_MAX_PIXELS = int(
    os.getenv("ORDER_REFERENCE_PDF_OCR_MAX_PIXELS", "40000000") or "40000000"
)
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

        base_zoom = dpi / 72  # PDF user space is 72 dpi
        chunks = []
        clamped = False
        for page in document:
            zoom = base_zoom
            rect = page.rect
            projected = (rect.width * zoom) * (rect.height * zoom)
            if PDF_OCR_MAX_PIXELS > 0 and projected > PDF_OCR_MAX_PIXELS:
                zoom *= (PDF_OCR_MAX_PIXELS / projected) ** 0.5
                clamped = True
            matrix = pymupdf.Matrix(zoom, zoom)
            pixmap = page.get_pixmap(matrix=matrix, colorspace=pymupdf.csGRAY)
            image = Image.open(io.BytesIO(pixmap.tobytes("png")))
            chunks.append(pytesseract.image_to_string(image))
        return "\n".join(chunks), "ocr-clamped" if clamped else "ocr"
    finally:
        document.close()


def find_order_references_in_pdf_bytes(pdf_bytes: bytes) -> tuple[list[str], str]:
    """Extract order references from one PDF in a single pass.

    Text layer first, else one OCR pass at PDF_OCR_DPI. There is deliberately
    no higher-resolution retry: it re-rendered and re-OCR'd every page of the
    document from scratch, so the attachments that found nothing -- the common
    case -- paid roughly double for it.
    """

    text, method = _pdf_text(pdf_bytes, PDF_OCR_DPI)
    return find_order_references(text), method


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
