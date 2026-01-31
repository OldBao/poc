-- Source: dev_video_bi.sr_okr_table_metric_dws
-- Grain: grass_region, grass_date
-- Note: this table already has feature-level granularity, no inner aggregation needed
SELECT
    grass_region
    , grass_date
    , feature
    , order_cnt_login_user_first_lead
FROM dev_video_bi.sr_okr_table_metric_dws
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
