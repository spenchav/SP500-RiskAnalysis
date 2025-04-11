{{ config(
    materialized='table',
    full_refresh=true,
    pre_hook=[
        "SET FOREIGN_KEY_CHECKS=0",
        "DROP TABLE IF EXISTS {{ this }}"
    ],
    post_hook=[
        "ALTER TABLE sql_project.fact_price ADD CONSTRAINT fact_price_symbol_fk FOREIGN KEY (symbol_id) REFERENCES sql_project.dim_symbol(symbol_id)",
        "SET FOREIGN_KEY_CHECKS=1"
    ]
) }}

WITH symbol_data AS (
    SELECT DISTINCT
        symbol,
        security,
        gics_sector,
        gics_sub_industry as gics_industry
    FROM {{ ref('stg_wikipedia_scrape') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY symbol) as symbol_id,
    symbol,
    security,
    gics_sector,
    gics_industry
FROM symbol_data