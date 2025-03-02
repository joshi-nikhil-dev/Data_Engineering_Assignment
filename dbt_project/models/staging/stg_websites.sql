{{ config(materialized='table') }}

SELECT *
FROM {{ source('raw', 'websites') }}