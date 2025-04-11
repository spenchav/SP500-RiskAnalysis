{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook="SET FOREIGN_KEY_CHECKS=0;",
    post_hook=[
        "ALTER TABLE {{ this }} ADD CONSTRAINT fact_price_symbol_fk FOREIGN KEY (symbol_id) REFERENCES {{ ref('dim_symbol') }}(symbol_id);",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fact_price_date_fk FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }}(date_id);",
        "SET FOREIGN_KEY_CHECKS=1;"
    ]
) }}

WITH price_data AS (
    SELECT 
        p.symbol,
        p.date,
        CAST(p.open AS DECIMAL(10,2)) as open_price,
        CAST(p.high AS DECIMAL(10,2)) as high_price,
        CAST(p.low AS DECIMAL(10,2)) as low_price,
        CAST(p.close AS DECIMAL(10,2)) as close_price,
        CAST(p.volume AS UNSIGNED) as volume
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
        p.volume
    FROM price_data p
    JOIN {{ ref('dim_symbol') }} s ON p.symbol = s.symbol
    JOIN {{ ref('dim_date') }} d ON p.date = d.trade_date
)

SELECT * FROM final