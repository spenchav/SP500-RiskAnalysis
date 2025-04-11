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
        volume,
        adj_open,
        adj_high,
        adj_low,
        adj_close,
        adj_volume,
        div_cash,
        split_factor
    FROM sql_project.raw_prices
)

SELECT *
FROM raw