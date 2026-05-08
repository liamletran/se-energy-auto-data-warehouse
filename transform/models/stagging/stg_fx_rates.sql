WITH fx_raw AS(
		SELECT * FROM fx_rates_raw
	),
	fx_final AS(
		SELECT 
 			CAST(date AS timestamptz) AS fx_rates_date,
 			base AS base_currency,
 			quote AS target_currency,
 			CAST(rate AS DOUBLE) AS eur_sek_rates, 
 			CAST(ingested_at AS timestamptz) AT TIME ZONE 'UTC' AS  price_ingested_at
		FROM fx_raw
		ORDER BY fx_rates_date 
	) 
SELECT * FROM fx_final
