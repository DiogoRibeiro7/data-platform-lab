-- Silver grain: one canonical geography and reference date; unavailable pay is NULL.
CREATE TEMP TABLE silver_pay AS
SELECT
    TRIM(areacd) AS area_code,
    TRIM(areanm) AS area_name,
    TRIM(geography) AS geography,
    TRY_CAST(period AS DATE) AS reference_date,
    CAST(EXTRACT(YEAR FROM TRY_CAST(period AS DATE)) AS INTEGER) AS pay_year,
    TRY_CAST(NULLIF(TRIM(observation), '') AS DOUBLE) AS weekly_pay_gbp,
    TRY_CAST(NULLIF(TRIM(lci_95), '') AS DOUBLE) AS lci_95_gbp,
    TRY_CAST(NULLIF(TRIM(uci_95), '') AS DOUBLE) AS uci_95_gbp,
    NULLIF(TRIM(status), '') AS status_code,
    NULLIF(TRIM(notes), '') AS notes,
    ?::VARCHAR AS source_sha256,
    ?::VARCHAR AS registry_commit
FROM raw_input
