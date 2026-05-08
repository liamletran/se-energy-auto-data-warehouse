WITH price_raw AS(
	SELECT * FROM entsoe_prices_raw --{{ source('warehouse', 'prices_raw') }} 
	), 
	prices_final AS(
		SELECT 
			CAST(datetime AS timestamptz) AT TIME ZONE 'UTC' AS prices_datetime_utc,
			COALESCE(price_eur_mwh, 0) AS price_eur_mwh,
			bidding_zone AS bidding_zone_id,
			CAST(ingested_at AS timestamptz) AT TIME ZONE 'UTC' AS  price_ingested_at
		FROM price_raw
		ORDER BY prices_datetime_utc
	)
SELECT * FROM prices_final