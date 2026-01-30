-- Order % by Channel (first-lead attribution)
-- Source: monthly_core_metrics_tracker.sql (m4 subquery)
SELECT
    grass_region
    , substr(cast(grass_date as varchar), 1, 7) AS period
    , avg(CASE WHEN feature = 'Platform' THEN order_cnt_login_user_first_lead END) AS platform_order_1d
    , avg(CASE WHEN feature = 'Global Search' THEN order_cnt_login_user_first_lead END) AS global_search_order_1d
    , avg(CASE WHEN feature = 'Daily Discover' THEN order_cnt_login_user_first_lead END) AS dd_order_1d
    , avg(CASE WHEN feature = 'You May Also Like' THEN order_cnt_login_user_first_lead END) AS ymal_order_1d
    , avg(CASE WHEN feature = 'post purchase' THEN order_cnt_login_user_first_lead END) AS post_purchase_order_1d
    , avg(CASE WHEN feature = 'Private Domain Features' THEN order_cnt_login_user_first_lead END) AS private_domain_order_1d
    , avg(CASE WHEN feature = 'Live Streaming' THEN order_cnt_login_user_first_lead END) AS live_order_1d
    , avg(CASE WHEN feature = 'Video' THEN order_cnt_login_user_first_lead END) AS video_order_1d
FROM dev_video_bi.sr_okr_table_metric_dws
WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
    AND tz_type = 'regional'
{%- if market %}
    AND grass_region = '{{ market }}'
{%- endif %}
GROUP BY 1, 2
ORDER BY 2 DESC
