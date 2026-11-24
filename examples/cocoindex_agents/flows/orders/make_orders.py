"""Write the sample purchase orders under ``orders/``; the PDFs are checked in.

    python examples/cocoindex_agents/flows/orders/make_orders.py

Hand-written PDF 1.4 with the two standard Helvetica fonts, uncompressed,
no dependency, a few kilobytes each: a title, a header block, a ruled
line-item table, a totals block, and an Approval section with three drawn
boxes of which one is ticked with two vector strokes. The grid and the tick
are what a text extractor cannot see; a model that reads the page natively
can. Generated content, so there is no licensing question.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

PAGE_W, PAGE_H = 595, 842  # A4, points


@dataclass(frozen=True)
class Order:
    number: str
    date: str
    supplier: str
    supplier_address: str
    ship_to: str
    requested_by: str
    items: tuple[tuple[str, int, float], ...]  # description, quantity, unit price
    tax_rate: float
    approval: str  # approved | pending | rejected
    notes: str


ORDERS = (
    Order(
        "PO-2026-1041",
        "2026-10-03",
        "Harbor Metal Supply",
        "1200 Dock Road, Oakland, CA 94607",
        "Riverside Assembly Plant, 48 Mill Street, Stockton, CA 95203",
        "M. Okafor, Maintenance",
        (
            ("Steel angle bar 50x50x5 mm, 6 m length", 24, 38.50),
            ("Hex bolt M12x60, zinc plated, box of 100", 6, 42.00),
            ("Cutting disc 230 mm, pack of 10", 10, 17.80),
        ),
        0.0875,
        "approved",
        "Deliver to gate B between 07:00 and 15:00.",
    ),
    Order(
        "PO-2026-1042",
        "2026-10-09",
        "Northwind Office Interiors",
        "77 Larch Avenue, Portland, OR 97209",
        "Riverside Assembly Plant, 48 Mill Street, Stockton, CA 95203",
        "J. Lindqvist, Facilities",
        (
            ("Task chair, mesh back, black", 12, 189.00),
            ("Sit-stand desk 160x80 cm, oak top", 12, 412.00),
            ("Monitor arm, dual, clamp mount", 12, 74.50),
            ("Cable tray 120 cm", 12, 22.90),
        ),
        0.0,
        "pending",
        "Hold until the floor plan is signed off.",
    ),
    Order(
        "PO-2026-1043",
        "2026-10-16",
        "Cascade Lab Instruments",
        "9 Glacier Court, Bend, OR 97702",
        "Quality Lab, Building 3, 48 Mill Street, Stockton, CA 95203",
        "R. Mehta, Quality",
        (
            ("Digital caliper 0-300 mm, IP67", 4, 268.00),
            ("Surface roughness tester, portable", 1, 2140.00),
        ),
        0.0875,
        "rejected",
        "Over the quarter's budget; resubmit in January.",
    ),
    Order(
        "PO-2026-1044",
        "2026-10-21",
        "Bluefin Packaging",
        "310 Harbor View, Long Beach, CA 90802",
        "Riverside Assembly Plant, 48 Mill Street, Stockton, CA 95203",
        "M. Okafor, Maintenance",
        (
            ("Corrugated box 60x40x40 cm, double wall", 500, 1.84),
            ("Stretch film 500 mm x 300 m, roll", 36, 9.60),
            ("Pallet strap polyester 16 mm, 1000 m", 8, 58.00),
            ("Edge protector 1200 mm, pack of 50", 10, 31.25),
            ("Shipping label 100x150 mm, roll of 500", 20, 12.40),
        ),
        0.0875,
        "approved",
        "Partial deliveries accepted.",
    ),
)


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class Page:
    """Content-stream operators for one page."""

    def __init__(self) -> None:
        self.ops: list[str] = ["0.8 w"]

    def text(
        self, x: float, y: float, s: str, size: int = 10, bold: bool = False
    ) -> None:
        font = "F2" if bold else "F1"
        self.ops.append(
            f"BT /{font} {size} Tf 1 0 0 1 {x:.1f} {y:.1f} Tm ({_esc(s)}) Tj ET"
        )

    def line(self, x1: float, y1: float, x2: float, y2: float) -> None:
        self.ops.append(f"{x1:.1f} {y1:.1f} m {x2:.1f} {y2:.1f} l S")

    def rect(self, x: float, y: float, w: float, h: float) -> None:
        self.ops.append(f"{x:.1f} {y:.1f} {w:.1f} {h:.1f} re S")

    def tick(self, x: float, y: float) -> None:
        """A check mark inside the 12-point box whose corner is (x, y): two strokes."""
        self.ops.append(
            f"1.6 w {x + 2.5:.1f} {y + 6:.1f} m {x + 5:.1f} {y + 2.5:.1f} l {x + 10:.1f} {y + 10:.1f} l S 0.8 w"
        )

    def stream(self) -> bytes:
        return "\n".join(self.ops).encode("latin-1")


def pdf(pages: list[Page]) -> bytes:
    """A PDF 1.4 file: catalog, page tree, the two fonts, then a page and a content stream per page."""
    page_ids = [5 + 2 * i for i in range(len(pages))]
    objects: dict[int, bytes] = {
        1: b"<< /Type /Catalog /Pages 2 0 R >>",
        2: (
            "<< /Type /Pages /Kids ["
            + " ".join(f"{p} 0 R" for p in page_ids)
            + f"] /Count {len(pages)} >>"
        ).encode(),
        3: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
        4: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>",
    }
    for page_id, page in zip(page_ids, pages):
        content = page.stream()
        objects[page_id] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W} {PAGE_H}] "
            f"/Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents {page_id + 1} 0 R >>"
        ).encode()
        objects[page_id + 1] = (
            b"<< /Length %d >>\nstream\n" % len(content) + content + b"\nendstream"
        )
    out = bytearray(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for number in sorted(objects):
        offsets[number] = len(out)
        out += f"{number} 0 obj\n".encode() + objects[number] + b"\nendobj\n"
    xref_at = len(out)
    size = len(objects) + 1
    out += f"xref\n0 {size}\n".encode() + b"0000000000 65535 f \n"
    for number in range(1, size):
        out += f"{offsets[number]:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {size} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()
    return bytes(out)


def money(value: float) -> str:
    return f"{value:,.2f}"


def render(order: Order) -> bytes:
    page = Page()
    left, right = 50, 545
    page.text(left, 785, "PURCHASE ORDER", 18, bold=True)
    page.text(400, 788, f"No. {order.number}", 12, bold=True)
    page.line(left, 775, right, 775)

    y = 748
    header = (
        ("Order date", order.date, "Requested by", order.requested_by),
        ("Supplier", order.supplier, "Payment terms", "Net 30"),
        ("Address", order.supplier_address, "Currency", "USD"),
    )
    for label_l, value_l, label_r, value_r in header:
        page.text(left, y, label_l, bold=True)
        page.text(left + 70, y, value_l)
        page.text(330, y, label_r, bold=True)
        page.text(330 + 80, y, value_r)
        y -= 16
    page.text(left, y, "Ship to", bold=True)
    page.text(left + 70, y, order.ship_to)

    # Line items: a ruled table. Column edges, then one row per item.
    columns = (left, 335, 385, 465, right)
    top, row_h = 650, 18
    page.text(columns[0] + 4, top - 13, "Description", bold=True)
    page.text(columns[1] + 4, top - 13, "Qty", bold=True)
    page.text(columns[2] + 4, top - 13, "Unit price", bold=True)
    page.text(columns[3] + 4, top - 13, "Amount", bold=True)
    subtotal = 0.0
    for i, (description, quantity, unit_price) in enumerate(order.items):
        row_y = top - (i + 2) * row_h + 5
        amount = quantity * unit_price
        subtotal += amount
        page.text(columns[0] + 4, row_y, description)
        page.text(columns[1] + 4, row_y, str(quantity))
        page.text(columns[2] + 4, row_y, money(unit_price))
        page.text(columns[3] + 4, row_y, money(amount))
    bottom = top - (len(order.items) + 1) * row_h
    for x in columns:
        page.line(x, top, x, bottom)
    for j in range(len(order.items) + 2):
        page.line(left, top - j * row_h, right, top - j * row_h)

    tax = round(subtotal * order.tax_rate, 2)
    total = round(subtotal + tax, 2)
    y = bottom - 22
    for label, value, bold in (
        ("Subtotal", money(subtotal), False),
        (f"Tax ({order.tax_rate * 100:g}%)", money(tax), False),
        ("Total", money(total), True),
    ):
        page.text(columns[2] + 4, y, label, bold=bold)
        page.text(columns[3] + 4, y, value, bold=bold)
        y -= 16
    page.line(columns[2], y + 10, right, y + 10)

    # Approval: three boxes, one ticked. The tick is drawn, not typed.
    y -= 30
    page.text(left, y, "Approval", 11, bold=True)
    y -= 22
    for i, state in enumerate(("approved", "pending", "rejected")):
        x = left + i * 130
        page.rect(x, y - 2, 12, 12)
        page.text(x + 18, y, state.capitalize())
        if state == order.approval:
            page.tick(x, y - 2)
    y -= 36
    page.text(left, y, "Notes", bold=True)
    page.text(left + 70, y, order.notes)
    page.line(left, 60, right, 60)
    page.text(left, 48, f"{order.number} · page 1 of 1", 8)
    return pdf([page])


def main() -> None:
    out = pathlib.Path(__file__).resolve().parent / "orders"
    out.mkdir(exist_ok=True)
    for order in ORDERS:
        path = out / f"{order.number.lower()}.pdf"
        path.write_bytes(render(order))
        print(f"{path.name}: {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
