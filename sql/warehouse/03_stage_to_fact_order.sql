-- Load fact_order from the raw orders staging table.
--
-- Cleaning steps:
--   1. Normalise order_date from YYYY/MM/DD to YYYY-MM-DD
--   2. Join to dim_customer to resolve surrogate key
--   3. Skip orders with unresolvable customer_id (orphan FK)

INSERT OR REPLACE INTO fact_order (order_id, customer_key, order_date_key, status, total, shipping_country)
SELECT
    o.order_id,
    dc.customer_key,
    replace(o.order_date, '/', '-') AS order_date_key,
    o.status,
    o.total,
    o.shipping_country
FROM orders o
JOIN dim_customer dc ON dc.customer_id = o.customer_id;
