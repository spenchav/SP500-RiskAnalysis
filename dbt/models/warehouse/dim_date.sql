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
    date_id BIGINT PRIMARY KEY,
    trade_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    weekday INT
)
{% endset %}

{{ create_table_sql }}
;

WITH date_spine AS (
    SELECT DISTINCT date as trade_date
    FROM {{ ref('stg_tiingo_api') }}
)

INSERT INTO {{ this }}
SELECT 
    ROW_NUMBER() OVER (ORDER BY trade_date) as date_id,
    trade_date,
    YEAR(trade_date) as year,
    QUARTER(trade_date) as quarter,
    MONTH(trade_date) as month,
    DAY(trade_date) as day,
    WEEKDAY(trade_date) as weekday
FROM date_spine