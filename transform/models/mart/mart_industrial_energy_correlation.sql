{{ config(materialized='table') }}

WITH monthly_energy AS (
    SELECT
        DATE_TRUNC('month', CAST(date_day AS TIMESTAMP))::DATE AS observation_month,
        SUM(actual_generation_mw) AS total_generation_mw,
        AVG(price_sek_per_mwh) AS avg_price_sek,
        SUM(estimated_revenue_sek) AS total_revenue_sek
    FROM {{ ref('fct_energy_market_hourly') }}
    GROUP BY 1
),
industrial_production AS (
    SELECT
        period_ipi::DATE AS observation_month,
        nace_code,
        calendar_adjusted_ipi 
    FROM {{ ref('fct_ipi') }}
)
SELECT
    e.observation_month,
    i.nace_code,
    i.calendar_adjusted_ipi AS ipi_index,
    e.total_generation_mw,
    e.avg_price_sek,
    e.total_revenue_sek
FROM monthly_energy e
INNER JOIN industrial_production i 
    ON e.observation_month = i.observation_month