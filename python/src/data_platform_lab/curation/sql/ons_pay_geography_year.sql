-- Gold: unweighted distribution of area-level pay within *each* geography type.
-- These medians are NOT population-weighted, and NOT ONS national pay estimates.
CREATE TEMP TABLE gold_geography_year AS
SELECT
    geography,
    pay_year,
    COUNT(*) AS areas_total,
    COUNT(weekly_pay_gbp) AS areas_with_pay,
    COUNT(*) - COUNT(weekly_pay_gbp) AS areas_without_pay,
    MIN(weekly_pay_gbp) AS min_area_weekly_pay_gbp,
    MEDIAN(weekly_pay_gbp) AS median_area_weekly_pay_gbp,
    MAX(weekly_pay_gbp) AS max_area_weekly_pay_gbp,
    MIN(source_sha256) AS source_sha256,
    MIN(registry_commit) AS registry_commit
FROM silver_pay
GROUP BY geography, pay_year
