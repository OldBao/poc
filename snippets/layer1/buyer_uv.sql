-- Source: traffic_omni_oa.dwd_order_item_atc_journey_di__reg_sensitive_live
-- Grain: grass_region, grass_date
SELECT
    grass_region
    , grass_date
    , count(distinct case when gmv_usd * atc_prorate * first_touchpoint_item > 0 then user_id end) AS buyer_uv
FROM traffic_omni_oa.dwd_order_item_atc_journey_di__reg_sensitive_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'local'
    AND first_touchpoint_item = 1
    AND user_id > 0
    AND user_id is not null
    AND order_item_id is not null
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
