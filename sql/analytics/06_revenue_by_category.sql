-- Revenue breakdown by product category.
--
-- Shows contribution of each category to total revenue using a window
-- function for the percentage calculation.

SELECT
    p.category,
    SUM(oi.line_total)                         AS category_revenue,
    COUNT(DISTINCT oi.order_id)                AS order_count,
    SUM(oi.quantity)                           AS units_sold,
    ROUND(
        100.0 * SUM(oi.line_total) /
        (SELECT SUM(line_total) FROM order_items),
        1
    )                                          AS pct_of_total
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY category_revenue DESC;
