-- Net Ads Revenue (with BR SCS credit adjustment)
-- Source: monthly_core_metrics_tracker.sql (m2 subquery, net portion)
SELECT
    n1.grass_region
    , substr(cast(n1.grass_date as varchar), 1, 7) AS period
    , avg(net_ads_rev) AS net_ads_rev
    , avg(net_ads_rev_excl_1p + coalesce(br_scs, 0)) AS net_ads_rev_excl_1p
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
    LEFT JOIN (
        SELECT grass_date, grass_region, sum(free_rev) AS br_scs
        FROM (
            SELECT grass_date, grass_region
                , CASE
                    WHEN credit_topup_type_name LIKE '%free credit%' AND credit_program_name = '' THEN 'free_credit_topup_others'
                    WHEN credit_topup_type_name LIKE '%free credit%' THEN coalesce(credit_program_name, 'free_credit_topup_others')
                    WHEN credit_topup_type_name LIKE '%paid credit%' AND credit_program_name = '' THEN 'paid_credit_topup_others'
                    WHEN credit_topup_type_name LIKE '%paid credit%' THEN coalesce(credit_program_name, 'paid_credit_topup_others')
                    ELSE coalesce(credit_program_name, 'others')
                END AS credit_program_name
                , sum(free_ads_revenue_amt_usd_1d) AS free_rev
            FROM mp_paidads.dws_advertise_net_ads_revenue_1d__reg_s0_live
            WHERE grass_date >= date '{{ date_start }}'
                AND tz_type = 'regional'
                AND grass_region = 'BR'
            GROUP BY 1, 2, 3
        ) nn
        WHERE credit_program_name = '2025_0034_BR_AD_SAS_CREDITS'
        GROUP BY 1, 2
    ) n2 ON n1.grass_date = n2.grass_date AND n1.grass_region = n2.grass_region
GROUP BY 1, 2
ORDER BY 2 DESC
