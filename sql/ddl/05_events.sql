-- Events table — user interaction events (page views, cart actions, checkouts).
--
-- Matches: data/sample/events.json
-- Known data issues:
--   evt-014 has a NULL user_id
--   evt-005 is duplicated (appears twice)

CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT,
    type        TEXT,                     -- page_view, add_to_cart, checkout, remove_from_cart
    user_id     TEXT,                     -- nullable
    page        TEXT,
    product_id  TEXT,
    quantity    INTEGER,
    order_id    TEXT,
    timestamp   TEXT                      -- ISO 8601 datetime
);
