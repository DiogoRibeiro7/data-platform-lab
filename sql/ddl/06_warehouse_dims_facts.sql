-- Warehouse dimension and fact tables — star schema for analytics.
--
-- These tables represent the "gold" layer after data has been cleaned,
-- deduplicated, and transformed from the raw staging tables above.

-- -----------------------------------------------------------------------
-- Dimensions
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT NOT NULL UNIQUE,
    first_name     TEXT NOT NULL,
    last_name      TEXT NOT NULL,
    email          TEXT,
    city           TEXT,
    country        TEXT,                  -- standardised to title case
    created_at     TEXT,
    loaded_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id     TEXT NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    category       TEXT,
    price          REAL,
    currency       TEXT DEFAULT 'EUR',
    active         INTEGER DEFAULT 1,    -- boolean as 0/1
    loaded_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key       TEXT PRIMARY KEY,      -- YYYY-MM-DD
    year           INTEGER,
    month          INTEGER,
    day            INTEGER,
    day_of_week    INTEGER,               -- 0 = Sunday (SQLite strftime %w)
    month_name     TEXT,
    is_weekend     INTEGER DEFAULT 0      -- 1 if Saturday or Sunday
);

-- -----------------------------------------------------------------------
-- Facts
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_order (
    order_key       INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT NOT NULL,
    customer_key    INTEGER REFERENCES dim_customer(customer_key),
    order_date_key  TEXT REFERENCES dim_date(date_key),
    status          TEXT,
    total           REAL,
    shipping_country TEXT,
    loaded_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS fact_order_item (
    order_item_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT,
    product_key     INTEGER REFERENCES dim_product(product_key),
    quantity        INTEGER,
    unit_price      REAL,
    line_total      REAL,
    loaded_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS fact_event (
    event_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL,
    type        TEXT,
    user_id     TEXT,
    page        TEXT,
    product_id  TEXT,
    order_id    TEXT,
    event_date_key TEXT REFERENCES dim_date(date_key),
    event_hour  INTEGER,
    loaded_at   TEXT DEFAULT (datetime('now'))
);
