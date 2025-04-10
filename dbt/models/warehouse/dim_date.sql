{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    SELECT DISTINCT date as trade_date
    FROM {{ ref('stg_tiingo_api') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY trade_date) as date_id,
    trade_date,
    YEAR(trade_date) as year,
    QUARTER(trade_date) as quarter,
    MONTH(trade_date) as month,
    DAY(trade_date) as day,
    WEEKDAY(trade_date) as weekday
FROM date_spine