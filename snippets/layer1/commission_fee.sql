-- Source: mp_order.dwd_order_item_all_ent_df__reg_s0_live
-- Grain: grass_region, grass_date (using create_datetime as date)
SELECT
    grass_region
    , cast(create_datetime as date) AS grass_date
    , sum(commission_fee_usd) AS commission_fee
FROM mp_order.dwd_order_item_all_ent_df__reg_s0_live
WHERE cast(create_datetime as date) BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'local'
    AND (grass_date >= cast(create_datetime as date) or grass_date = date '9999-01-01')
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
