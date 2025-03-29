{{ config(
    materialized='table',
    alias='happiness_inflation_analysis'
) }}

SELECT
    Country,
    Year,
    Score,
    Score / NULLIF(Avg_inflation, 0) AS happiness_per_inflation_ratio,
    Score / NULLIF(GDP_per_capita, 0) AS happiness_per_gdp_ratio,
    GDP_per_capita,
    Real_GDP_per_capita,
    Social_support,
    Healthy_life_expectancy,
    Avg_inflation
FROM {{ ref('happiness_index_inflation') }}
WHERE
    Score IS NOT NULL
    AND GDP_per_capita IS NOT NULL
    AND Avg_inflation IS NOT NULL
