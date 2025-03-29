{{ config(
    materialized='table',
    alias='country_yoy_changes'
) }}

WITH lagged_data AS (
    SELECT
        Country,
        Year,
        Score,
        GDP_per_capita,
        Avg_inflation,
        LAG(Score) OVER (PARTITION BY Country ORDER BY Year) AS prev_score,
        LAG(GDP_per_capita) OVER (PARTITION BY Country ORDER BY Year) AS prev_gdp,
        LAG(Avg_inflation) OVER (PARTITION BY Country ORDER BY Year) AS prev_inflation
    FROM {{ ref('happiness_index_inflation') }}
)

SELECT
    Country,
    Year,
    Score - prev_score AS Happiness_score_yoy,
    GDP_per_capita - prev_gdp AS GDP_yoy,
    Avg_inflation - prev_inflation AS Inflation_yoy
FROM lagged_data
WHERE prev_score IS NOT NULL