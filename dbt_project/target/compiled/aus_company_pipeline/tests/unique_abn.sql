SELECT abn
        FROM "aus_company_db"."australian_companies"."stg_abr"
        GROUP BY abn
        HAVING COUNT(*) > 1