"""
Rewrite zero item values in a downloaded invoice / RPI PDF.

Some orders are invoiced at zero: samples, gifts-with-purchase, replacements.
GET_FULL_ORDER reports them as ``items[].price == 0`` and the PDF behind
``erpDocuments.invoiceDocuments[].documentLink`` prints ``0,00`` in the unit
price and price columns, with zero totals. Customs will not clear a shipment
whose declared value is zero, so every zero line is re-declared at a nominal
``1`` per unit and the document totals are increased by the same amount.

What is changed, and nothing else:

* every zero in the **unit price** column of an item row -> the nominal value;
* the **line price** on that row -> quantity x nominal value;
* the document totals listed in ``TOTAL_LABELS`` -> old value + the sum of
  the line increases.

The shipping total is deliberately left alone: it is not an item value, and a
zero there is a real free-shipping promotion rather than a missing value.

The edit is made in place, on the already-downloaded PDF, and recorded in a
sidecar ``<file>.value_correction.json`` marker. A file that carries its
marker is never corrected twice, so a re-run that reuses the cached download
cannot inflate the same invoice again.

PyMuPDF does the reading and writing. It is already a pipeline dependency
(installed by .github/workflows/agent_triggerer.yml for the order-reference
PDF scan). When it is unavailable the correction is skipped with one log
line, exactly like the OCR fallback: a slightly wrong value on the invoice is
better than a run that dies.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:  # pragma: no cover - import guard mirrors the OCR fallback
    import pymupdf as fitz
except ImportError:  # pragma: no cover
    try:
        import fitz  # type: ignore
    except ImportError:
        fitz = None  # type: ignore


MARKER_SUFFIX = ".value_correction.json"

# The nominal unit value a zero-priced item is re-declared at.
DEFAULT_REPLACEMENT_VALUE = 1.0

# Column headers, normalized (lower case, no accents/punctuation runs).
UNIT_PRICE_HEADERS = {
    "prezzo unitario",
    "prezzo unit",
    "prezzo unitario eur",
    "unit price",
    "unitary price",
    "price per unit",
    "valore unitario",
}
LINE_PRICE_HEADERS = {
    "prezzo",
    "price",
    "importo",
    "amount",
    "total",
    "totale",
    "valore",
    "value",
}
QUANTITY_HEADERS = {
    "qta",
    "q ta",
    "qty",
    "quantity",
    "quantita",
    "pcs",
    "pieces",
}

# Document totals that must grow by the amount added to the item lines.
# "Totale spedizione" / "Shipping total" is NOT here on purpose.
TOTAL_LABELS = (
    "totale articoli",
    "importo totale dovuto",
    "merchandise total",
    "invoice total amount",
    "invoice total",
    "total amount due",
    "totale merce",
)

_MONEY_RE = re.compile(
    r"(?P<prefix>[^\d\-]*)"
    r"(?P<number>-?\d{1,3}(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?|-?\d+(?:[.,]\d{1,2})?)"
    r"(?P<suffix>[^\d]*)$"
)

# A row's spans are grouped into one line when their vertical centres are
# within this many points of each other.
_ROW_TOLERANCE = 4.0

_BASE14_FONTS = {
    "helvetica": "helv",
    "helvetica-bold": "hebo",
    "helvetica-oblique": "heit",
    "helvetica-boldoblique": "hebi",
    "arial": "helv",
    "arial-bold": "hebo",
    "arialmt": "helv",
    "arial-boldmt": "hebo",
    "times-roman": "tiro",
    "times-bold": "tibo",
    "timesnewromanpsmt": "tiro",
    "courier": "cour",
    "courier-bold": "cobo",
}


class InvoiceValueCorrectionError(RuntimeError):
    """Raised when a PDF was opened but could not be rewritten safely."""


@dataclass
class _Number:
    """One parsed money span on the page."""

    page: int
    bbox: tuple
    origin: tuple
    text: str
    value: float
    prefix: str
    suffix: str
    decimal_sep: str
    thousands_sep: str
    decimals: int
    font: str
    size: float
    color: int


@dataclass
class _Edit:
    number: _Number
    new_value: float
    kind: str


@dataclass
class CorrectionResult:
    path: str
    delta: float = 0.0
    line_edits: int = 0
    total_edits: int = 0
    details: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "path": self.path,
            "delta": round(self.delta, 2),
            "line_edits": self.line_edits,
            "total_edits": self.total_edits,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# text helpers
# ---------------------------------------------------------------------------


def _normalize_label(value: object) -> str:
    text = str(value or "").lower()
    text = text.replace("'", " ").replace("\u2019", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_money(text: str):
    """Parse a money-looking span into (value, prefix, suffix, separators).

    Returns None when the span is not a single money value. Both European
    (``1.234,56``) and Anglo (``1,234.56``) notations are recognized from the
    span itself, so the rewritten number keeps the document's own notation.
    """

    raw = str(text or "").strip()
    if not raw:
        return None

    match = _MONEY_RE.match(raw)
    if not match:
        return None

    number = match.group("number")
    prefix = match.group("prefix") or ""
    suffix = match.group("suffix") or ""

    if any(char.isdigit() for char in prefix):
        return None

    body = number.replace(" ", "")
    decimal_sep = ""
    thousands_sep = ""
    decimals = 0

    last_comma = body.rfind(",")
    last_dot = body.rfind(".")
    separator_index = max(last_comma, last_dot)

    if separator_index >= 0:
        separator = body[separator_index]
        tail = body[separator_index + 1 :]
        if len(tail) in (1, 2) and tail.isdigit():
            decimal_sep = separator
            decimals = len(tail)
            thousands_sep = "." if separator == "," else ","
            integer_part = body[:separator_index]
        else:
            # Only grouping separators, e.g. "1.234" -> an integer amount.
            thousands_sep = separator
            integer_part = body
    else:
        integer_part = body

    digits = re.sub(r"[^\d-]", "", integer_part)
    if digits in ("", "-"):
        return None

    try:
        value = float(digits)
        if decimals:
            fraction = float(body[separator_index + 1 :]) / (10 ** decimals)
            value = value - fraction if value < 0 else value + fraction
    except ValueError:
        return None

    return value, prefix, suffix, decimal_sep, thousands_sep, decimals


def _format_money(number: _Number, value: float) -> str:
    decimals = number.decimals or 2
    decimal_sep = number.decimal_sep or ","
    text = f"{abs(value):,.{decimals}f}"  # 1,234.56
    integer_part, _, fraction = text.partition(".")

    if number.thousands_sep and number.thousands_sep != ",":
        integer_part = integer_part.replace(",", number.thousands_sep)
    elif not number.thousands_sep:
        integer_part = integer_part.replace(",", "")

    rendered = f"{integer_part}{decimal_sep}{fraction}" if decimals else integer_part
    if value < 0:
        rendered = f"-{rendered}"
    return f"{number.prefix}{rendered}{number.suffix}"


def _font_name(span_font: str) -> str:
    key = str(span_font or "").split("+")[-1].strip().lower()
    if key in _BASE14_FONTS:
        return _BASE14_FONTS[key]
    if "bold" in key and "italic" not in key and "oblique" not in key:
        return "hebo"
    if "italic" in key or "oblique" in key:
        return "heit"
    return "helv"


def _overlaps(span_bbox: Sequence[float], column: Sequence[float]) -> float:
    """Horizontal overlap between a span and a column range, in points."""

    left = max(span_bbox[0], column[0])
    right = min(span_bbox[2], column[1])
    return max(0.0, right - left)


# ---------------------------------------------------------------------------
# page model
# ---------------------------------------------------------------------------


def _page_spans(page) -> list:
    spans = []
    for block in page.get_text("dict").get("blocks", []):
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                text = str(span.get("text") or "")
                if not text.strip():
                    continue
                spans.append(span)
    return spans


def _column_ranges(spans: Iterable[Mapping[str, Any]]):
    """Locate the quantity / unit price / line price columns from the headers."""

    quantity = None
    unit_price = None
    line_price = None
    header_bottom = None

    for span in spans:
        label = _normalize_label(span.get("text"))
        if not label:
            continue
        bbox = span.get("bbox")
        if label in UNIT_PRICE_HEADERS and unit_price is None:
            unit_price = (bbox[0], bbox[2])
            header_bottom = max(header_bottom or 0.0, bbox[3])
        elif label in QUANTITY_HEADERS and quantity is None:
            quantity = (bbox[0], bbox[2])
            header_bottom = max(header_bottom or 0.0, bbox[3])
        elif label in LINE_PRICE_HEADERS and line_price is None:
            line_price = (bbox[0], bbox[2])
            header_bottom = max(header_bottom or 0.0, bbox[3])

    return quantity, unit_price, line_price, header_bottom


def _as_number(span: Mapping[str, Any], page_index: int):
    parsed = _parse_money(span.get("text"))
    if parsed is None:
        return None
    value, prefix, suffix, decimal_sep, thousands_sep, decimals = parsed
    return _Number(
        page=page_index,
        bbox=tuple(span.get("bbox")),
        origin=tuple(span.get("origin") or (span["bbox"][0], span["bbox"][3])),
        text=str(span.get("text")),
        value=value,
        prefix=prefix,
        suffix=suffix,
        decimal_sep=decimal_sep,
        thousands_sep=thousands_sep,
        decimals=decimals,
        font=str(span.get("font") or ""),
        size=float(span.get("size") or 8.0),
        color=int(span.get("color") or 0),
    )


def _row_key(bbox: Sequence[float]) -> float:
    return (bbox[1] + bbox[3]) / 2.0


def _cluster_rows(items: Sequence[tuple]) -> list:
    """Group (centre_y, payload) pairs into table rows.

    Fixed-width buckets are not good enough here: the line price in these
    templates sits one point lower than the unit price on the same row, which
    a rounding bucket happily splits in two. Items are sorted by vertical
    centre and a new row starts only when the gap to the previous one exceeds
    the tolerance.
    """

    rows: list = []
    current: list = []
    last_centre = None

    for centre, payload in sorted(items, key=lambda item: item[0]):
        if last_centre is None or abs(centre - last_centre) <= _ROW_TOLERANCE:
            current.append(payload)
        else:
            rows.append(current)
            current = [payload]
        last_centre = centre

    if current:
        rows.append(current)
    return rows


def _plan_page(page, page_index: int, replacement_value: float):
    """Return (item edits, total number candidates) for one page."""

    spans = _page_spans(page)
    quantity_col, unit_col, price_col, header_bottom = _column_ranges(spans)

    edits: list = []
    delta = 0.0

    if unit_col or price_col:
        cells: list = []
        for span in spans:
            bbox = span.get("bbox")
            if header_bottom is not None and bbox[1] < header_bottom:
                continue
            number = _as_number(span, page_index)
            if number is None:
                continue

            # Each numeric span belongs to the column it overlaps most.
            overlaps = {
                "unit": _overlaps(bbox, unit_col) if unit_col else 0.0,
                "price": _overlaps(bbox, price_col) if price_col else 0.0,
                "quantity": _overlaps(bbox, quantity_col) if quantity_col else 0.0,
            }
            column = max(overlaps, key=lambda key: overlaps[key])
            if overlaps[column] <= 0:
                continue

            cells.append((_row_key(bbox), (column, number)))

        rows = []
        for grouped in _cluster_rows(cells):
            row: dict = {}
            for column, number in grouped:
                row.setdefault(column, number)
            rows.append(row)

        for row in rows:
            unit = row.get("unit")
            price = row.get("price")
            if unit is None and price is None:
                continue
            # A totals line has a price but no unit price and no quantity; it
            # is handled by the label pass below, never here.
            if unit is None:
                continue
            if unit.value != 0:
                continue

            quantity_number = row.get("quantity")
            quantity = quantity_number.value if quantity_number else 1.0
            if quantity <= 0:
                quantity = 1.0

            edits.append(_Edit(number=unit, new_value=replacement_value, kind="unit_price"))

            if price is not None and price.value == 0:
                new_price = round(replacement_value * quantity, 2)
                edits.append(_Edit(number=price, new_value=new_price, kind="line_price"))
                delta += new_price - price.value

    # Totals: the label and its value share a row.
    total_candidates: list = []
    for grouped in _cluster_rows(
        [(_row_key(span["bbox"]), span) for span in spans]
    ):
        wanted = ""
        label_right = 0.0
        for span in grouped:
            label = _normalize_label(span.get("text"))
            if not label:
                continue
            for candidate in TOTAL_LABELS:
                if label == candidate or label.startswith(candidate):
                    wanted = candidate
                    label_right = span["bbox"][2]
                    break
            if wanted:
                break

        if not wanted:
            continue

        for span in grouped:
            if span["bbox"][0] < label_right:
                continue  # the label itself, or something left of it
            number = _as_number(span, page_index)
            if number is None:
                continue
            total_candidates.append((wanted, number))

    return edits, delta, total_candidates


def _apply_edits(page, edits: Sequence[_Edit]) -> None:
    if not edits:
        return

    for edit in edits:
        rect = fitz.Rect(edit.number.bbox)
        rect.x0 -= 1.0
        rect.x1 += 1.0
        page.add_redact_annot(rect)

    # Keep the table rules and any logo: only the text is removed.
    try:
        page.apply_redactions(
            images=getattr(fitz, "PDF_REDACT_IMAGE_NONE", 0),
            graphics=getattr(fitz, "PDF_REDACT_LINE_ART_NONE", 0),
        )
    except TypeError:  # pragma: no cover - older PyMuPDF signature
        page.apply_redactions()

    for edit in edits:
        number = edit.number
        text = _format_money(number, edit.new_value)
        fontname = _font_name(number.font)
        width = fitz.get_text_length(text, fontname=fontname, fontsize=number.size)
        # Numbers in these templates are right-aligned in their column, so the
        # replacement is anchored to the original right edge rather than the
        # left one: "9,00" -> "10,00" must not drift into the next column.
        x = number.bbox[2] - width
        y = number.origin[1]
        page.insert_text(
            (x, y),
            text,
            fontname=fontname,
            fontsize=number.size,
            color=_color_tuple(number.color),
        )


def _color_tuple(color: int):
    try:
        value = int(color)
    except (TypeError, ValueError):
        return (0, 0, 0)
    return (
        ((value >> 16) & 255) / 255.0,
        ((value >> 8) & 255) / 255.0,
        (value & 255) / 255.0,
    )


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def marker_path(pdf_path: str | Path) -> Path:
    return Path(str(pdf_path) + MARKER_SUFFIX)


def already_corrected(pdf_path: str | Path) -> bool:
    return marker_path(pdf_path).exists()


def correct_zero_item_values(
    pdf_path: str | Path,
    *,
    replacement_value: float = DEFAULT_REPLACEMENT_VALUE,
    force: bool = False,
) -> CorrectionResult | None:
    """Re-declare zero item values in one invoice/RPI PDF, in place.

    Returns the CorrectionResult when the file was changed, None when there
    was nothing to change (or the file was already corrected). Raises
    InvoiceValueCorrectionError when the PDF could not be rewritten.
    """

    path = Path(pdf_path)
    if not path.is_file():
        raise InvoiceValueCorrectionError(f"Invoice PDF not found: {path}")

    if not force and already_corrected(path):
        return None

    if fitz is None:
        raise InvoiceValueCorrectionError(
            "PyMuPDF is not installed; zero-value invoice correction skipped"
        )

    result = CorrectionResult(path=path.as_posix())

    try:
        document = fitz.open(path)
    except Exception as exc:  # noqa: BLE001
        raise InvoiceValueCorrectionError(f"Could not open {path}: {exc}") from exc

    try:
        page_edits: dict = {}
        total_candidates: list = []
        delta = 0.0

        for page_index, page in enumerate(document):
            edits, page_delta, totals = _plan_page(page, page_index, replacement_value)
            if edits:
                page_edits[page_index] = edits
            delta += page_delta
            total_candidates.extend(
                (label, number) for label, number in totals
            )

        if not page_edits:
            document.close()
            return None

        if delta:
            for label, number in total_candidates:
                page_edits.setdefault(number.page, []).append(
                    _Edit(
                        number=number,
                        new_value=round(number.value + delta, 2),
                        kind=f"total:{label}",
                    )
                )

        for page_index, edits in page_edits.items():
            _apply_edits(document[page_index], edits)
            for edit in edits:
                if edit.kind.startswith("total:"):
                    result.total_edits += 1
                else:
                    result.line_edits += 1
                result.details.append(
                    {
                        "page": page_index + 1,
                        "kind": edit.kind,
                        "from": edit.number.text.strip(),
                        "to": _format_money(edit.number, edit.new_value),
                    }
                )

        result.delta = delta
        # Written to a sibling temp file and moved into place, so a crash
        # mid-save cannot leave a half-written PDF where the pipeline expects
        # a readable one.
        temp_path = path.with_suffix(path.suffix + ".tmp")
        document.save(temp_path.as_posix(), deflate=True, garbage=3)
        document.close()
        temp_path.replace(path)
    except InvoiceValueCorrectionError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise InvoiceValueCorrectionError(f"Could not rewrite {path}: {exc}") from exc
    finally:
        try:
            document.close()
        except Exception:  # noqa: BLE001 - already closed / never opened
            pass

    marker_path(path).write_text(
        json.dumps(result.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result
