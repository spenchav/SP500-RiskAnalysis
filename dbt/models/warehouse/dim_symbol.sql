{{ config(
    materialized='table'
) }}

WITH symbol_data AS (
    SELECT DISTINCT
        symbol,
        security,
        gics_sector,
        gics_sub_industry as gics_industry
    FROM {{ ref('stg_wikipedia_scrape') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY symbol) as symbol_id,
    symbol,
    security,
    gics_sector,
    gics_industry
FROM symbol_data