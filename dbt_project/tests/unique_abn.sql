
        SELECT abn
        FROM {{ ref('stg_abr') }}
        GROUP BY abn
        HAVING COUNT(*) > 1
        