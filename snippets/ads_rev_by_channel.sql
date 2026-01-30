-- Ads Revenue % by Channel (entry point breakdown)
-- Source: monthly_core_metrics_tracker.sql (m2 subquery, detailed)
SELECT
    grass_region
    , substr(cast(grass_date as varchar), 1, 7) AS period
    , avg(ads_rev_usd) AS total_ads_rev
    , avg(search_ads_rev_usd) AS search_ads_rev
    , avg(dd_ads_rev_usd) AS dd_ads_rev
    , avg(rcmd_ads_rev_usd) AS rcmd_ads_rev
    , avg(game_ads_rev_usd) AS game_ads_rev
    , avg(brand_ads_rev_usd) AS brand_ads_rev
    , avg(live_ads_rev_usd) AS live_ads_rev
    , avg(video_ads_rev_usd) AS video_ads_rev
FROM
    (
        SELECT
            grass_region, grass_date
            , sum(ads_rev_usd) AS ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Global Search', 'Image Search') THEN ads_rev_usd END) AS search_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Daily Discover External', 'Daily Discover Mix Feed Internal', 'DD External Video') THEN ads_rev_usd END) AS dd_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Cart Recommendation', 'Me You May Also Like', 'My Purchase Page Recommendation', 'Order Detail Page Recommendation', 'Order Successful Recommendation', 'You May Also Like') THEN ads_rev_usd END) AS rcmd_ads_rev_usd
            , sum(CASE WHEN entry_point = 'Game' THEN ads_rev_usd END) AS game_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Shop', 'Shop Game', 'Display') THEN ads_rev_usd END) AS brand_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Livestream Autolanding', 'Livestream PDP', 'Livestream Discovery', 'Livestream Homepage', 'Livestream Video Feed', 'Livestream For You') THEN ads_rev_usd END) AS live_ads_rev_usd
            , sum(CASE WHEN entry_point IN ('Video Trending Tab', 'HP Internal Video', 'DD Internal Video') THEN ads_rev_usd END) AS video_ads_rev_usd
        FROM mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
        WHERE grass_date BETWEEN date '{{ date_start }}' AND date '{{ date_end }}'
            AND tz_type = 'regional'
{%- if market %}
            AND grass_region = '{{ market }}'
{%- endif %}
        GROUP BY 1, 2
    ) n
GROUP BY 1, 2
ORDER BY 2 DESC
