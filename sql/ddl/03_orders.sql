-- Orders table — one row per customer order.
--
-- Matches: data/sample/orders.csv
-- Known data issues:
--   ORD-008 references C099, which does not exist in customers
--   ORD-009 has a date in YYYY/MM/DD format instead of YYYY-MM-DD

CREATE TABLE IF NOT EXISTS orders (
    order_id          TEXT PRIMARY KEY,
    customer_id       TEXT,              -- FK to customers (not enforced for raw load)
    order_date        TEXT,              -- ISO date string
    status            TEXT,              -- completed, shipped, cancelled, pending
    total             REAL,
    shipping_country  TEXT
);
