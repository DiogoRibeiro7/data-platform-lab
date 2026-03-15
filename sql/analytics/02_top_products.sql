-- Top products by total revenue and units sold.
--
-- Joins order_items to products to get product names.
-- Ranks by total revenue descending.

SELECT
    p.product_id,
    p.name            AS product_name,
    p.category,
    SUM(oi.quantity)   AS units_sold,
    SUM(oi.line_total) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS order_count
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue DESC;
