WITH ipi_raw AS (
	SELECT DISTINCT("industrial classification (NACE Rev. 2)") AS all_info
	FROM {{ ref('nace_descriptions') }}
), ipi_final AS(
	SELECT string_split(all_info, ' ')[1] AS nace_code,
		regexp_replace(all_info, '^[^\s]+\s', '') AS nace_description
		
	FROM ipi_raw
)
SELECT * FROM ipi_final