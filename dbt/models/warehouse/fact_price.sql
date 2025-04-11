{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook=[
        "SET FOREIGN_KEY_CHECKS=0",
        "DROP TABLE IF EXISTS {{ this }}"
    ],
    post_hook=[
        "ALTER TABLE {{ this }} ADD CONSTRAINT fact_price_symbol_fk FOREIGN KEY (symbol_id) REFERENCES {{ ref('dim_symbol') }}(symbol_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fact_price_date_fk FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }}(date_id)",
        "SET FOREIGN_KEY_CHECKS=1"
    ]
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
    CAST(s.symbol_id AS BIGINT) as symbol_id,
    CAST(d.date_id AS BIGINT) as date_id,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume
FROM price_data p
JOIN {{ ref('dim_symbol') }} s ON p.symbol = s.symbol
JOIN {{ ref('dim_date') }} d ON p.date = d.trade_date