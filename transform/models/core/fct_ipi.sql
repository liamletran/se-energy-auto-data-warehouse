WITH ipi_raw AS (
	SELECT * FROM {{ ref('stg_ipi')}}
)
	SELECT nace_code,
		period_code,
		period_ipi,
		calendar_adjusted_ipi,
		not_calendar_adjusted_ipi,
		seasonally_adjusted_ipi,
		trend_ipi,
		monthly_development_percentage,
		annual_development_percentage,
		ipi_ingested_at
	FROM  ipi_raw 