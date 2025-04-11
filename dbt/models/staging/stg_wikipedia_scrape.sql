{{ config(
    materialized='view'
) }}

WITH raw AS (
    SELECT 
        TRIM(UPPER(Symbol)) AS symbol,
        Security as security,
        GICSSector as gics_sector,
        `GICS Sub-Industry` as gics_sub_industry,
        `Headquarters Location` as headquarters_location,
        `Date added` as date_added,
        CIK as cik,
        Founded as founded
    FROM sql_project.raw_wikipedia_sp500
)

SELECT *
FROM raw