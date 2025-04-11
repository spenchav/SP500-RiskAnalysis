{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook=[
        "SET FOREIGN_KEY_CHECKS=0",
        "DROP TABLE IF EXISTS {{ this }}"
    ],
    post_hook=[
        "SET FOREIGN_KEY_CHECKS=1"
    ]
) }}

WITH date_spine AS (
    SELECT DISTINCT date as trade_date
    FROM {{ ref('stg_tiingo_api') }}
)

SELECT 
    CAST(ROW_NUMBER() OVER (ORDER BY trade_date) AS UNSIGNED) as date_id,
    trade_date,
    YEAR(trade_date) as year,
    QUARTER(trade_date) as quarter,
    MONTH(trade_date) as month,
    DAY(trade_date) as day,
    WEEKDAY(trade_date) as weekday
FROM date_spine