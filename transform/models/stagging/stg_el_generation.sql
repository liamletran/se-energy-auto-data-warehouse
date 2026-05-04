WITH source AS (
    SELECT * FROM {{ source('warehouse', 'generation_raw') }}
), generation_raw AS (
    SELECT 
        CAST(datetime AS timestamptz) AT TIME ZONE 'UTC' AS generation_datetime_utc,
        EXTRACT(HOUR FROM datetime) AS generation_hour,
        production_type,  
        COALESCE(actual_generation_mw, 0) AS actual_generation_mw,
        bidding_zone AS bidding_zone_id,
        ingested_at AS generation_ingested_at
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY generation_datetime_utc, production_type, bidding_zone_id ORDER BY generation_datetime_utc) = 1
)

SELECT * 
FROM generation_raw