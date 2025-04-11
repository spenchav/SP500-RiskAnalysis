{{ config(
    materialized='table',
    full_refresh=true
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
    CAST(ROW_NUMBER() OVER (ORDER BY symbol) AS UNSIGNED) as symbol_id,
    CAST(symbol AS CHAR(255)) as symbol,
    CAST(security AS TEXT) as security,
    CAST(gics_sector AS TEXT) as gics_sector,
    CAST(gics_industry AS TEXT) as gics_industry
FROM symbol_data