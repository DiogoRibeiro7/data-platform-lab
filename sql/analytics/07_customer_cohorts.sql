-- Customer cohort analysis — group customers by signup month and show
-- how many ordered, and their total spend.

SELECT
    strftime('%Y-%m', c.created_at)  AS signup_month,
    COUNT(DISTINCT c.customer_id)    AS customers_signed_up,
    COUNT(DISTINCT o.customer_id)    AS customers_ordered,
    ROUND(
        100.0 * COUNT(DISTINCT o.customer_id) /
        COUNT(DISTINCT c.customer_id),
        1
    )                                AS conversion_pct,
    COALESCE(SUM(o.total), 0)       AS cohort_revenue
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY signup_month
ORDER BY signup_month;
