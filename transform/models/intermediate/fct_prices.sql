WITH energy_prices AS (
    SELECT * FROM stg_el_prices --{{ ref("stg_el_prices")}}
), fx_rates AS (
    SELECT * FROM stg_fx_rates --{{ ref("stg_fx_rates") }}
), final_merge AS (
    SELECT 
        e.*,
        f.eur_sek_rates,
        e.price_eur_mwh * coalesce(f.eur_sek_rates, 10.8) AS price_sek_mwh
    FROM energy_prices e
    LEFT JOIN fx_rates f 
    ON date_trunc('day', e.prices_datetime_utc) = date_trunc('day', f.fx_rates_date)
) SELECT * FROM final_merge
