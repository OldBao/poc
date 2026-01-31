-- Source: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , sum(ads_rev_usd) AS ads_rev_usd
    , sum(ads_gmv_usd) AS ads_gmv_usd
FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
