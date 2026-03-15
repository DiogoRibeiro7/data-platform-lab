-- Order items table — line items within an order.
--
-- Matches: data/sample/order_items.csv
-- Known data issues:
--   ORD-003 has a line_total mismatch (45.00 unit_price, 44.99 line_total)
--   ORD-006 has a duplicate line item (P006 appears twice)

CREATE TABLE IF NOT EXISTS order_items (
    order_id    TEXT,                     -- FK to orders
    product_id  TEXT,                     -- FK to products
    quantity    INTEGER,
    unit_price  REAL,
    line_total  REAL
);
