{{ config(
    materialized='view'
) }}

WITH raw AS (
    SELECT 
        TRIM(UPPER(symbol)) AS symbol,
        security,
        gics_sector,
        gics_sub_industry,
        headquarters_location,
        `date_added`,
        cik,
        founded
    FROM `raw_wikipedia_sp500`
)

SELECT *
FROM raw
