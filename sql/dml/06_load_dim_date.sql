-- Populate the date dimension for the range covered by the sample data
-- (2024-01-01 through 2024-12-31).
--
-- SQLite does not have a generate_series function by default, so this uses
-- a recursive CTE to produce all dates in the year.

INSERT OR IGNORE INTO dim_date (date_key, year, month, day, day_of_week, month_name, is_weekend)
WITH RECURSIVE dates(d) AS (
    SELECT '2024-01-01'
    UNION ALL
    SELECT date(d, '+1 day')
    FROM dates
    WHERE d < '2024-12-31'
)
SELECT
    d                                          AS date_key,
    CAST(strftime('%Y', d) AS INTEGER)         AS year,
    CAST(strftime('%m', d) AS INTEGER)         AS month,
    CAST(strftime('%d', d) AS INTEGER)         AS day,
    CAST(strftime('%w', d) AS INTEGER)         AS day_of_week,
    CASE CAST(strftime('%m', d) AS INTEGER)
        WHEN 1  THEN 'January'
        WHEN 2  THEN 'February'
        WHEN 3  THEN 'March'
        WHEN 4  THEN 'April'
        WHEN 5  THEN 'May'
        WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'
        WHEN 8  THEN 'August'
        WHEN 9  THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END                                        AS month_name,
    CASE
        WHEN CAST(strftime('%w', d) AS INTEGER) IN (0, 6) THEN 1
        ELSE 0
    END                                        AS is_weekend
FROM dates;
