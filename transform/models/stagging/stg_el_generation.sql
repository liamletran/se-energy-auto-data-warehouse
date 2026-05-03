WITH generation_raw AS (
    SELECT * FROM {{ source('warehouse', 'generation_raw') }}
)
SELECT 
    datetime AS generation_at,
    CAST(datetime AS DATE) AS generation_date,
    EXTRACT(HOUR FROM datetime) AS generation_hour,
    production_type,  
    COALESCE(actual_generation_mw, 0) AS actual_generation_mw,
    bidding_zone AS bidding_zone_id,
    ingested_at AS generation_ingested_at
FROM generation_raw