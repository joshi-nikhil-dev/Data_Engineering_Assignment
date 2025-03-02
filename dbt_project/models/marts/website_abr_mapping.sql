{{ config(materialized='table') }}
SELECT
    ROW_NUMBER() OVER (ORDER BY w.website_id, a.abn) AS mapping_id,
    w.website_id,
    a.abn,
    CASE WHEN w.company_name = a.company_name THEN 1.0 ELSE 0.8 END AS confidence_score
FROM {{ ref('stg_websites') }} w
LEFT JOIN {{ ref('stg_abr') }} a ON w.company_name = a.company_name