select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT abn
        FROM "aus_company_db"."australian_companies"."stg_abr"
        GROUP BY abn
        HAVING COUNT(*) > 1
      
    ) dbt_internal_test