-- Deduplication patterns — remove duplicates from staging tables.
--
-- SQLite does not have QUALIFY or ROW_NUMBER() with DELETE, so these
-- patterns use a temporary table or rowid approach.

-- -----------------------------------------------------------------------
-- Pattern 1: Deduplicate customers using rowid
-- Keep the row with the lowest rowid for each customer_id.
-- -----------------------------------------------------------------------

DELETE FROM customers
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM customers
    GROUP BY customer_id
);


-- -----------------------------------------------------------------------
-- Pattern 2: Deduplicate order_items using a temp table
-- For cases where you want to deduplicate a table without a unique key.
-- -----------------------------------------------------------------------

CREATE TEMPORARY TABLE tmp_order_items AS
SELECT
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total
FROM order_items
GROUP BY order_id, product_id, unit_price;

DELETE FROM order_items;

INSERT INTO order_items
SELECT * FROM tmp_order_items;

DROP TABLE tmp_order_items;


-- -----------------------------------------------------------------------
-- Pattern 3: Deduplicate events using rowid
-- -----------------------------------------------------------------------

DELETE FROM events
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM events
    GROUP BY event_id
);
