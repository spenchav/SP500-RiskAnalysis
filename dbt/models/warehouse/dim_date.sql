{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook="SET FOREIGN_KEY_CHECKS=0;",
    post_hook="SET FOREIGN_KEY_CHECKS=1;"
) }}

WITH date_spine AS (
    SELECT DISTINCT date as trade_date
    FROM {{ ref('stg_tiingo_api') }}
)

SELECT 
    CAST(ROW_NUMBER() OVER (ORDER BY trade_date) AS UNSIGNED) as date_id,
    trade_date,
    CAST(YEAR(trade_date) AS UNSIGNED) as year,
    CAST(QUARTER(trade_date) AS UNSIGNED) as quarter,
    CAST(MONTH(trade_date) AS UNSIGNED) as month,
    CAST(DAY(trade_date) AS UNSIGNED) as day,
    CAST(WEEKDAY(trade_date) AS UNSIGNED) as weekday
FROM date_spine