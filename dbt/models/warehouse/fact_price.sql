{{ config(
    materialized='table'
) }}

WITH price_data AS (
    SELECT 
        p.symbol,
        p.date,
        p.open as open_price,
        p.high as high_price,
        p.low as low_price,
        p.close as close_price,
        p.volume
    FROM {{ ref('stg_tiingo_api') }} p
)

SELECT 
    s.symbol_id,
    d.date_id,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume
FROM price_data p
JOIN {{ ref('dim_symbol') }} s ON p.symbol = s.symbol
JOIN {{ ref('dim_date') }} d ON p.date = d.trade_date