-- Source: mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , sum(net_ads_rev_usd) AS net_ads_rev
    , sum(CASE WHEN seller_type_1p NOT IN ('Local SCS', 'SCS', 'Lovito') THEN net_ads_rev_excl_sip_usd_1d END) AS net_ads_rev_excl_1p
FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
