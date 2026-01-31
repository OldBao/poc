-- Source: traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , a1
FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'local'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
