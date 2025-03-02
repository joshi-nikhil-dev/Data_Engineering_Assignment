{{ config(materialized='table') }}

SELECT *
FROM {{ source('raw', 'abr_data') }}