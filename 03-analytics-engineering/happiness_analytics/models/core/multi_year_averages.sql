{{ config(
    materialized='table',
    alias='multi_year_averages'
) }}

WITH aggregated AS (
    SELECT
        Country,
        Continent,
        -- Happiness metrics
        AVG(Score) AS avg_happiness_score,
        
        -- Inflation metrics
        AVG(Avg_inflation) AS avg_inflation,
        MAX(Avg_inflation) AS max_inflation,
        MIN(Avg_inflation) AS min_inflation,
        
        -- GDP metrics
        AVG(GDP_per_capita) AS avg_nominal_gdp,
        AVG(Real_GDP_per_capita) AS avg_real_gdp,
        (AVG(Real_GDP_per_capita) - AVG(GDP_per_capita)) AS avg_inflation_impact,
        
        -- Social metrics
        AVG(Social_support) AS avg_social_support,
        AVG(Healthy_life_expectancy) AS avg_life_expectancy,
        
    FROM {{ ref('happiness_index_inflation') }}
    GROUP BY 1, 2
)

SELECT
    *,
    -- Derived ratios
    avg_happiness_score / NULLIF(avg_inflation, 0) AS happiness_per_inflation_ratio,
    avg_real_gdp / NULLIF(avg_nominal_gdp, 0) AS purchasing_power_ratio,
    
    -- Trend indicators
    CASE
        WHEN avg_inflation > 10 THEN 'High Inflation'
        WHEN avg_inflation > 5 THEN 'Moderate Inflation'
        WHEN avg_inflation > 0 THEN 'Low Inflation'
        ELSE 'Deflationary'
    END AS inflation_category,
    
    -- Happiness classification
    CASE
        WHEN avg_happiness_score > 7 THEN 'Very Happy'
        WHEN avg_happiness_score > 6 THEN 'Happy'
        WHEN avg_happiness_score > 5 THEN 'Neutral'
        ELSE 'Unhappy'
    END AS happiness_category
FROM aggregated