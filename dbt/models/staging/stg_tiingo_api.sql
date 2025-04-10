{{ config(
    materialized='view'
) }}

WITH raw AS (
    SELECT 
        symbol,
        CAST(`date` AS DATE) AS trade_date,
        open,
        high,
        low,
        close,
        volume
    FROM `raw_prices`
    -- Possibly filter out questionable data or duplicates if your raw table has them
)

SELECT *
FROM raw
