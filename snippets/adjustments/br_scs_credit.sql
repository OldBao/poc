-- BR SCS Credit: free credit revenue adjustment
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
