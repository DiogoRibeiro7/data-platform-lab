-- Detect referential integrity violations.
--
-- These queries find rows that reference a parent table key that
-- does not exist.  The sample data intentionally has orphan records.

-- 1. Orders referencing non-existent customers
SELECT
    o.order_id,
    o.customer_id  AS missing_customer_id,
    o.order_date,
    o.total
FROM orders o
LEFT JOIN customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;


-- 2. Order items referencing non-existent orders
SELECT
    oi.order_id    AS missing_order_id,
    oi.product_id,
    oi.line_total
FROM order_items oi
LEFT JOIN orders o ON o.order_id = oi.order_id
WHERE o.order_id IS NULL;


-- 3. Order items referencing non-existent products
SELECT
    oi.order_id,
    oi.product_id  AS missing_product_id,
    oi.line_total
FROM order_items oi
LEFT JOIN products p ON p.product_id = oi.product_id
WHERE p.product_id IS NULL;
