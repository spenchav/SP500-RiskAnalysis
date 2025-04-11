{{ config(
    materialized='view',
    full_refresh=true
) }}

WITH raw AS (
    SELECT 
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume
    FROM sql_project.raw_prices
)

SELECT *
FROM raw