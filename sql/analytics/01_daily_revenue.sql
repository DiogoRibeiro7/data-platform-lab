-- Daily revenue from completed orders.
--
-- Groups orders by date and calculates total revenue, order count, and
-- average order value.  Only includes orders with status = 'completed'.
-- Excludes ORD-009 whose date format is non-standard (YYYY/MM/DD).

SELECT
    order_date,
    COUNT(*)           AS order_count,
    SUM(total)         AS daily_revenue,
    ROUND(AVG(total), 2) AS avg_order_value
FROM orders
WHERE status = 'completed'
  AND order_date LIKE '____-__-__'    -- only valid ISO dates
GROUP BY order_date
ORDER BY order_date;
