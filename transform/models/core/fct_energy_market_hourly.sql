{{ config(materialized='table') }}

WITH generation AS (
    SELECT * FROM {{ ref('fct_generation')}}
), prices AS (
    SELECT * FROM {{ ref('fct_prices')}}
)SELECT
    generation_datetime_utc,
	strftime(g.generation_datetime_utc::TIMESTAMP, '%Y-%m-%d') AS date_day,
	strftime(g.generation_datetime_utc::TIMESTAMP, '%H:%M') AS hourly_timestamp,
    g.bidding_zone_id,
    g.production_type,
    g.actual_generation_mw,
    p.price_sek_mwh AS price_sek_per_mwh,
	(g.actual_generation_mw * p.price_sek_mwh) as estimated_revenue_sek
FROM generation g
INNER JOIN prices p ON g.generation_datetime_utc = p.prices_datetime_utc AND g.bidding_zone_id = p.bidding_zone_id