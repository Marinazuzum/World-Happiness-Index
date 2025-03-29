{{
    config(
        materialized='view',
        alias='stg_happiness',
        unique_key='happiness_id'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['Country', 'Year']) }} AS happiness_id,
    Country,
    Year,
    Headline_Consumer_Price_Inflation AS Headline_inflation,
    Energy_Consumer_Price_Inflation AS Energy_inflation,
    Food_Consumer_Price_Inflation AS Food_inflation,
    Official_Core_Consumer_Price_Inflation AS Core_inflation,
    Producer_Price_Inflation AS Producer_inflation,
    GDP_Deflator_Index_Growth_Rate AS GDP_deflator,
    Continent_Region AS Continent,
    Score,
    GDP_per_Capita,
    Social_Support,
    Healthy_Life_Expectancy_at_Birth AS Healthy_life_expectancy,
    Freedom_to_Make_Life_Choices AS Freedom,
    Generosity,
    Perceptions_of_Corruption AS Corruption_perception
FROM {{ source('staging', 'happiness_index') }}