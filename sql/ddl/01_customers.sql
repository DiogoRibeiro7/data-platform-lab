-- Customers table — source of truth for customer records.
--
-- Matches: data/sample/customers.csv
-- Note: The sample data contains a duplicate C003 and inconsistent country
-- casing (e.g. "portugal", "ITALY").  This DDL does not enforce uniqueness
-- at the database level so the raw data can be loaded as-is for cleaning
-- exercises.

CREATE TABLE IF NOT EXISTS customers (
    customer_id  TEXT PRIMARY KEY,
    first_name   TEXT NOT NULL,
    last_name    TEXT NOT NULL,
    email        TEXT,                   -- nullable (C004 has no email)
    city         TEXT,
    country      TEXT,
    created_at   TEXT                    -- ISO date string YYYY-MM-DD
);
