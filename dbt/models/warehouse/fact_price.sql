{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook=[
        "SET FOREIGN_KEY_CHECKS=0",
        "DROP TABLE IF EXISTS {{ this }}"
    ]
) }}

{% set create_table_sql %}
CREATE TABLE {{ this }} (
    symbol_id BIGINT,
    date_id BIGINT,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT,
    PRIMARY KEY (symbol_id, date_id),
    CONSTRAINT fact_price_symbol_fk FOREIGN KEY (symbol_id) REFERENCES {{ ref('dim_symbol') }}(symbol_id),
    CONSTRAINT fact_price_date_fk FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }}(date_id)
)
{% endset %}

{{ create_table_sql }}
;

WITH price_data AS (
    SELECT 
        p.symbol,
        p.date,
        CAST(p.open AS DECIMAL(10,2)) as open_price,
        CAST(p.high AS DECIMAL(10,2)) as high_price,
        CAST(p.low AS DECIMAL(10,2)) as low_price,
        CAST(p.close AS DECIMAL(10,2)) as close_price,
        p.volume
    FROM {{ ref('stg_tiingo_api') }} p
)

INSERT INTO {{ this }}
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