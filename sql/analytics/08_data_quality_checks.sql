-- Data quality checks — identify known issues in the sample data.
--
-- Each query returns rows that violate a data quality rule.

-- 1. Products with negative prices
SELECT product_id, name, price
FROM products
WHERE price < 0;


-- 2. Products with NULL stock
SELECT product_id, name, stock
FROM products
WHERE stock IS NULL;


-- 3. Products with non-EUR currency
SELECT product_id, name, price, currency
FROM products
WHERE currency != 'EUR';


-- 4. Customers with inconsistent country casing
SELECT customer_id, country,
    CASE
        WHEN country != upper(substr(country, 1, 1)) || lower(substr(country, 2))
        THEN 'casing issue'
        ELSE 'ok'
    END AS issue
FROM customers
WHERE country != upper(substr(country, 1, 1)) || lower(substr(country, 2));


-- 5. Orders with non-standard date format
SELECT order_id, order_date
FROM orders
WHERE order_date NOT LIKE '____-__-__';


-- 6. Order items where line_total != quantity * unit_price
SELECT
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    ROUND(quantity * unit_price, 2) AS expected_total,
    ROUND(line_total - quantity * unit_price, 2) AS difference
FROM order_items
WHERE ROUND(line_total, 2) != ROUND(quantity * unit_price, 2);


-- 7. Customers with NULL email
SELECT customer_id, first_name, last_name
FROM customers
WHERE email IS NULL OR email = '';
