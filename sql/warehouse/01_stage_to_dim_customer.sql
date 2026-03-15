-- Load dim_customer from the raw customers staging table.
--
-- Cleaning steps:
--   1. Deduplicate by customer_id (keep first occurrence via GROUP BY)
--   2. Standardise country to title case
--   3. Skip rows with NULL customer_id

INSERT OR REPLACE INTO dim_customer (customer_id, first_name, last_name, email, city, country, created_at)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    upper(substr(country, 1, 1)) || lower(substr(country, 2)) AS country,
    created_at
FROM customers
WHERE customer_id IS NOT NULL
GROUP BY customer_id;
