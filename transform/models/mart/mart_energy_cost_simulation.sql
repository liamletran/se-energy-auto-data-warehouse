SELECT
    date_trunc('month', TRY_CAST(date_day AS DATE)) AS month,
    bidding_zone_id,
    ROUND(AVG(price_sek_per_mwh) * 1000, 2) AS avg_cost_per_gwh,
    ROUND(
        PERCENTILE_CONT(0.3) WITHIN GROUP (
            ORDER BY price_sek_per_mwh
        ) * 1000, 2
    ) AS optimized_cost_per_gwh,
    ROUND(
        (AVG(price_sek_per_mwh)
        - PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY price_sek_per_mwh))
        / NULLIF(AVG(price_sek_per_mwh), 0) * 100, 1
    ) AS cost_saving_percentage

FROM {{ ref('fct_energy_market_hourly') }}
WHERE TRY_CAST(date_day AS DATE) >= '2019-01-01'  
  AND price_sek_per_mwh IS NOT NULL
  AND price_sek_per_mwh != 0
GROUP BY 1, 2
HAVING COUNT(*) > 100  
ORDER BY 1, 2