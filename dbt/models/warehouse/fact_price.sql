{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook="SET FOREIGN_KEY_CHECKS=0;",
    post_hook="SET FOREIGN_KEY_CHECKS=1;"
) }}

WITH price_data AS (
    SELECT 
        p.symbol,
        p.date,
        CAST(p.adj_open AS DECIMAL(10,2)) as open_price,
        CAST(p.adj_high AS DECIMAL(10,2)) as high_price,
        CAST(p.adj_low AS DECIMAL(10,2)) as low_price,
        CAST(p.adj_close AS DECIMAL(10,2)) as close_price,
        CAST(p.adj_volume AS UNSIGNED) as volume,
        CAST(p.div_cash AS DECIMAL(10,4)) as dividend,
        CAST(p.split_factor AS DECIMAL(10,4)) as split_factor
    FROM {{ ref('stg_tiingo_api') }} p
),
final AS (
    SELECT 
        CAST(s.symbol_id AS UNSIGNED) as symbol_id,
        CAST(d.date_id AS UNSIGNED) as date_id,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.volume,
        p.dividend,
        p.split_factor
    FROM price_data p
    JOIN {{ ref('dim_symbol') }} s ON p.symbol = s.symbol
    JOIN {{ ref('dim_date') }} d ON p.date = d.trade_date
)

SELECT * FROM final