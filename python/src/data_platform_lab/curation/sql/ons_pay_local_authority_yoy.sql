-- Gold: nominal area-level changes only for adjacent years with observed pay.
-- Missing/suppressed previous-year pay and gaps do not create artificial growth.
CREATE TEMP TABLE gold_local_authority_yoy AS
WITH lagged AS (
    SELECT
        area_code,
        area_name,
        reference_date,
        pay_year,
        weekly_pay_gbp,
        status_code,
        LAG(pay_year) OVER (PARTITION BY area_code ORDER BY pay_year) AS previous_year,
        LAG(weekly_pay_gbp) OVER (
            PARTITION BY area_code ORDER BY pay_year
        ) AS previous_weekly_pay_gbp,
        source_sha256,
        registry_commit
    FROM silver_pay
    WHERE geography = 'Local Authority District'
)
SELECT
    area_code,
    area_name,
    reference_date,
    pay_year,
    weekly_pay_gbp,
    status_code,
    previous_year,
    previous_weekly_pay_gbp,
    CASE
        WHEN previous_year = pay_year - 1
         AND previous_weekly_pay_gbp > 0
         AND weekly_pay_gbp IS NOT NULL
        THEN 100.0 * (weekly_pay_gbp / previous_weekly_pay_gbp - 1.0)
        ELSE NULL
    END AS yoy_nominal_pct,
    source_sha256,
    registry_commit
FROM lagged
