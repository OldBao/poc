-- Net Ads Revenue (base query)
SELECT
    n1.grass_region
    , substr(cast(n1.grass_date as varchar), 1, 7) AS period
    , avg(net_ads_rev) AS net_ads_rev
    , avg(net_ads_rev_excl_1p) AS net_ads_rev_excl_1p
FROM
    (
        SELECT
            grass_region, grass_date
            , sum(net_ads_rev_usd) AS net_ads_rev
            , sum(CASE WHEN seller_type_1p NOT IN ('Local SCS', 'SCS', 'Lovito') THEN net_ads_rev_excl_sip_usd_1d END) AS net_ads_rev_excl_1p
        FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
        WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
            AND tz_type = 'regional'
{%- if market %}
            AND grass_region = '{{ market }}'
{%- endif %}
        GROUP BY 1, 2
    ) n1
GROUP BY 1, 2
ORDER BY 2 DESC
