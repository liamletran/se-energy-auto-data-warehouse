WITH ipi_raw AS (
	SELECT * FROM {{ source('warehouse', 'ipi_raw') }}
), ipi_final AS(
	SELECT 
		nace_code,
		period AS period_code,
		strptime(period,  '%YM%m')::DATE AS period_ipi,
		calendar_adj AS calendar_adjusted_ipi,
		not_adj AS not_calendar_adjusted_ipi,
		season_adj AS seasonally_adjusted_ipi,
		trend AS trend_ipi,
		mom_pct AS monthly_development_percentage,
		yoy_pct AS annual_development_percentage,
		CAST(ingested_at AS timestamptz) AT TIME ZONE 'UTC' AS ipi_ingested_at
	FROM ipi_raw
		
)
SELECT * FROM ipi_final
