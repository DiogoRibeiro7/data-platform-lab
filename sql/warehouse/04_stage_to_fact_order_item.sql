-- Load fact_order_item from the raw order_items staging table.
--
-- Cleaning steps:
--   1. Deduplicate identical line items (same order_id, product_id, unit_price)
--   2. Recalculate line_total as quantity * unit_price to fix arithmetic errors
--   3. Join to dim_product for surrogate key

INSERT INTO fact_order_item (order_id, product_key, quantity, unit_price, line_total)
SELECT
    oi.order_id,
    dp.product_key,
    oi.quantity,
    oi.unit_price,
    ROUND(oi.quantity * oi.unit_price, 2) AS line_total
FROM (
    -- Deduplicate: keep one row per (order_id, product_id, unit_price)
    SELECT
        order_id,
        product_id,
        quantity,
        unit_price
    FROM order_items
    GROUP BY order_id, product_id, unit_price
) oi
JOIN dim_product dp ON dp.product_id = oi.product_id;
