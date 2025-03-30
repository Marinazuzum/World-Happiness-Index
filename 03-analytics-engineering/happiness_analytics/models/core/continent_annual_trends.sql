{{ config(
    materialized='table',
    alias='continent_annual_trends'
) }}

SELECT
    Continent,
    Year,
    AVG(Score) AS Avg_happiness_score,
    AVG(GDP_per_capita) AS Avg_gdp,
    AVG(Avg_inflation) AS Avg_inflation
FROM {{ ref('happiness_index_inflation') }}
GROUP BY 1, 2
ORDER BY 2, 1