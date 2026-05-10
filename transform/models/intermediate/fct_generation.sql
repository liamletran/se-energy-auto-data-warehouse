SELECT 
    g.*,
    z.zone_name,
    p.display_name,
    p.is_renewable
FROM {{ ref('stg_el_generation') }} g   
LEFT JOIN {{ ref('dim_bidding_zones') }} z ON g.bidding_zone_id = z.bidding_zone
LEFT JOIN {{ ref('dim_production_types') }} p ON g.production_type = p.production_type