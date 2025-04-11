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
    symbol_id BIGINT PRIMARY KEY,
    symbol VARCHAR(255),
    security TEXT,
    gics_sector TEXT,
    gics_industry TEXT
)
{% endset %}

{{ create_table_sql }}
;

WITH symbol_data AS (
    SELECT DISTINCT
        symbol,
        security,
        gics_sector,
        gics_sub_industry as gics_industry
    FROM {{ ref('stg_wikipedia_scrape') }}
)

INSERT INTO {{ this }}
SELECT 
    ROW_NUMBER() OVER (ORDER BY symbol) as symbol_id,
    symbol,
    security,
    gics_sector,
    gics_industry
FROM symbol_data