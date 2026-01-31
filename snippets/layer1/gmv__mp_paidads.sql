-- Source: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , sum(distinct platform_gmv_excl_testorder) AS gmv_usd
FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
