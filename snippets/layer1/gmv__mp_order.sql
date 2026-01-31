-- Source: mp_order.dwd_order_item_all_ent_df__reg_s0_live
-- Grain: grass_region, grass_date
-- Note: placeholder — replace with actual order-table GMV query when available
SELECT
    grass_region
    , grass_date
    , sum(gmv_usd) AS gmv_usd
FROM mp_order.dwd_order_item_all_ent_df__reg_s0_live
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'local'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
