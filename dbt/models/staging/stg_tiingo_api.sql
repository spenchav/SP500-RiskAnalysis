{{ config(
    materialized='view',
    schema='sql_project'
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