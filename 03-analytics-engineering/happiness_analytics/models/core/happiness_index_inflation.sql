{{ config(
    materialized='table',
    alias='happiness_index_inflation'
) }}

WITH inflation_calculations AS (
    SELECT
        *,
        -- Calculate weighted average inflation
        SAFE_DIVIDE(
            COALESCE(Energy_inflation, 0) + 
            COALESCE(Food_inflation, 0) + 
            COALESCE(Headline_inflation, 0) + 
            COALESCE(Core_inflation, 0) + 
            COALESCE(Producer_inflation, 0),
            NULLIF(
                (CASE WHEN Energy_inflation IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN Food_inflation IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN Headline_inflation IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN Core_inflation IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN Producer_inflation IS NOT NULL THEN 1 ELSE 0 END),
                0
            )
        ) AS Avg_inflation
    FROM {{ ref('stg_happiness') }}
)

SELECT
    Country,
    Year,
    Score,
    Avg_inflation,
    -- Calculate real GDP (adjusting for inflation)
    GDP_per_capita / (1 + (GDP_deflator / 100)) AS Real_GDP_per_capita,
    -- Other key metrics
    GDP_per_capita,
    Social_support,
    Healthy_life_expectancy,
    Freedom,
    Generosity,
    Corruption_perception,
    Continent
FROM inflation_calculations