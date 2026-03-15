-- Detect duplicates across tables.
--
-- Each query identifies rows that appear more than once on their
-- natural key.  The sample data intentionally contains duplicates
-- for practice.

-- 1. Duplicate customers (same customer_id)
SELECT
    customer_id,
    COUNT(*) AS occurrences
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- 2. Duplicate order line items (same order_id + product_id + unit_price)
SELECT
    order_id,
    product_id,
    unit_price,
    COUNT(*) AS occurrences
FROM order_items
GROUP BY order_id, product_id, unit_price
HAVING COUNT(*) > 1;


-- 3. Duplicate events (same event_id)
SELECT
    event_id,
    COUNT(*) AS occurrences
FROM events
GROUP BY event_id
HAVING COUNT(*) > 1;
