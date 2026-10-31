-- Quality gate: no coercion, omission, or inferred replacement of ONS suppressed pay.
WITH normalized AS (
    SELECT
        NULLIF(TRIM(areacd), '') AS area_code,
        NULLIF(TRIM(areanm), '') AS area_name,
        NULLIF(TRIM(geography), '') AS geography_name,
        NULLIF(TRIM(period), '') AS period_text,
        TRY_CAST(NULLIF(TRIM(period), '') AS DATE) AS pay_date,
        NULLIF(TRIM(observation), '') AS pay_text,
        TRY_CAST(NULLIF(TRIM(observation), '') AS DOUBLE) AS pay,
        NULLIF(TRIM(lci_95), '') AS lower_text,
        TRY_CAST(NULLIF(TRIM(lci_95), '') AS DOUBLE) AS lower_pay,
        NULLIF(TRIM(uci_95), '') AS upper_text,
        TRY_CAST(NULLIF(TRIM(uci_95), '') AS DOUBLE) AS upper_pay,
        NULLIF(TRIM(status), '') AS status_code,
        indicator,
        measure,
        unit
    FROM raw_input
)
SELECT
    COUNT(*) AS source_rows,
    COUNT(pay_text) AS rows_with_pay,
    COUNT(*) - COUNT(pay_text) AS rows_without_pay,
    COUNT(lower_text) AS rows_with_confidence_interval,
    COUNT(*) FILTER (
        WHERE area_code IS NULL OR area_name IS NULL OR geography_name IS NULL
    ) AS invalid_identity,
    COUNT(*) FILTER (
        WHERE period_text IS NULL OR pay_date IS NULL
    ) AS invalid_period,
    COUNT(*) FILTER (
        WHERE indicator IS DISTINCT FROM 'Gross median weekly pay'
           OR measure IS DISTINCT FROM 'Pounds'
           OR unit IS DISTINCT FROM '£'
    ) AS invalid_measure,
    COUNT(*) FILTER (
        WHERE (pay_text IS NOT NULL AND (pay IS NULL OR NOT ISFINITE(pay)))
           OR (lower_text IS NOT NULL AND (lower_pay IS NULL OR NOT ISFINITE(lower_pay)))
           OR (upper_text IS NOT NULL AND (upper_pay IS NULL OR NOT ISFINITE(upper_pay)))
    ) AS invalid_number,
    COUNT(*) FILTER (
        WHERE pay < 0 OR lower_pay < 0 OR upper_pay < 0
    ) AS invalid_negative,
    COUNT(*) FILTER (
        WHERE (pay_text IS NULL AND status_code IS NULL)
           OR (pay_text IS NOT NULL AND status_code IS NOT NULL)
           OR (status_code IS NOT NULL AND status_code NOT IN ('x', 'u'))
    ) AS invalid_suppression,
    COUNT(*) FILTER (
        WHERE (lower_text IS NULL) <> (upper_text IS NULL)
           OR (lower_text IS NOT NULL AND pay_text IS NULL)
           OR (lower_pay IS NOT NULL AND upper_pay IS NOT NULL AND pay IS NOT NULL
               AND NOT (lower_pay <= pay AND pay <= upper_pay))
    ) AS invalid_confidence_interval
FROM normalized
