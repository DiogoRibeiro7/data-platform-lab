-- Products table — product catalogue.
--
-- Matches: data/sample/products.csv
-- Known data issues in the sample:
--   P009 has a negative price (-15.00)
--   P011 has NULL stock
--   P012 uses USD instead of EUR

CREATE TABLE IF NOT EXISTS products (
    product_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT,
    price       REAL,                    -- may be negative in raw data
    currency    TEXT DEFAULT 'EUR',
    stock       INTEGER,                 -- nullable
    active      TEXT DEFAULT 'true'      -- stored as text boolean
);
