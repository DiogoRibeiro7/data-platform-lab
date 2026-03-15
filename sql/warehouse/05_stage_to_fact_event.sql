-- Load fact_event from the raw events staging table.
--
-- Cleaning steps:
--   1. Deduplicate by event_id (keep first occurrence)
--   2. Extract date and hour from ISO timestamp
--   3. Keep NULL user_id rows (anonymous events are valid)

INSERT OR REPLACE INTO fact_event (event_id, type, user_id, page, product_id, order_id, event_date_key, event_hour)
SELECT
    event_id,
    type,
    user_id,
    page,
    product_id,
    order_id,
    substr(timestamp, 1, 10)                       AS event_date_key,
    CAST(substr(timestamp, 12, 2) AS INTEGER)      AS event_hour
FROM events
GROUP BY event_id;
