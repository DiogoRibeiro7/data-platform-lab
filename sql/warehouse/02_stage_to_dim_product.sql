-- Load dim_product from the raw products staging table.
--
-- Cleaning steps:
--   1. Filter out products with negative prices (data quality gate)
--   2. Convert 'true'/'false' text to 1/0 integer
--   3. Default currency to EUR if not specified

INSERT OR REPLACE INTO dim_product (product_id, name, category, price, currency, active)
SELECT
    product_id,
    name,
    category,
    price,
    COALESCE(NULLIF(currency, ''), 'EUR') AS currency,
    CASE WHEN lower(active) = 'true' THEN 1 ELSE 0 END AS active
FROM products
WHERE price >= 0;
