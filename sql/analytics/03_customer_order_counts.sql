-- Customer order activity — order count and total spend per customer.
--
-- LEFT JOIN to include customers who have never placed an order.

SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.city,
    c.country,
    COUNT(o.order_id)     AS order_count,
    COALESCE(SUM(o.total), 0) AS total_spend,
    MIN(o.order_date)     AS first_order,
    MAX(o.order_date)     AS last_order
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, full_name, c.city, c.country
ORDER BY total_spend DESC;
