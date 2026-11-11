Each PDF under ./orders is a purchase order. Extract every order into a JSON
file in ./out named after the source file with `.json` appended (for
`po-1.pdf` write `po-1.pdf.json`) with these keys: order_number, order_date
(an ISO date), supplier, ship_to, line_items (a list of objects with
description, quantity, unit_price and amount), subtotal, tax, total, and
approval: which one of the boxes in the Approval section is ticked, as
"approved", "pending" or "rejected". Answer with one line saying what was
written.
