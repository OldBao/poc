-- S&R&A Monthly Metrics Tracker - Output Query
-- Reads from monthly_core_metrics_tracker and temp tables, computes all metrics for display

select
   m2.grass_date
   ,substr(cast(grass_month as varchar), 1, 7) as grass_month
   ,''
   ,''
   ,dau
   ,''
   ,buyer_uv
   ,buyer_uv_rate
   ,order_1d
   ,order_per_u
   ,gmv_usd_1d
   ,''
   ,ads_rev_usd
   ,take_rate
   ,ads_roi
   ,''
   ,net_ads_rev
   ,net_take_rate
   ,''
   ,net_ads_rev_excl_1p
   ,net_take_rate_excl_1p
   ,''
   ,commission_fee_usd
   ,commission_fee_usd * 1.0000 / gmv_usd_1d as commission_rate
   ,rebate_usd
   ,''
   ,''
   ,platform_order_1d * 1.0000 / platform_order_1d as total_order_ratio
   ,if(global_search_order_1d is null, 0, global_search_order_1d) * 1.0000 / platform_order_1d as global_search_order_ratio
   ,if(dd_order_1d is null, 0, dd_order_1d) * 1.0000 / platform_order_1d as dd_order_ratio
   ,if(ymal_order_1d is null, 0, ymal_order_1d) * 1.0000 / platform_order_1d as ymal_order_ratio
   ,if(post_purchase_order_1d is null, 0, post_purchase_order_1d) * 1.0000 / platform_order_1d as post_purchase_order_ratio
   ,if(private_domain_order_1d is null, 0, private_domain_order_1d) * 1.0000 / platform_order_1d as private_domain_order_ratio
   ,if(live_order_1d is null, 0, live_order_1d) * 1.0000 / platform_order_1d as live_order_ratio
   ,if(video_order_1d is null, 0, video_order_1d) * 1.0000 / platform_order_1d as video_order_ratio
   ,(
       platform_order_1d - if(global_search_order_1d is null, 0, global_search_order_1d) - if(dd_order_1d is null, 0, dd_order_1d) - if(ymal_order_1d is null, 0, ymal_order_1d) - if(post_purchase_order_1d is null, 0, post_purchase_order_1d) - if(private_domain_order_1d is null, 0, private_domain_order_1d) - if(live_order_1d is null, 0, live_order_1d) - if(video_order_1d is null, 0, video_order_1d)
   ) * 1.0000 / platform_order_1d as other_order_ratio
   ,''
   ,''
   ,ads_rev_usd * 1.0000 / ads_rev_usd as total_ads_rev_ratio
   ,if(search_ads_rev_usd is null, 0, search_ads_rev_usd) * 1.0000 / ads_rev_usd as search_ads_rev_ratio
   ,if(dd_ads_rev_usd is null, 0, dd_ads_rev_usd) * 1.0000 / ads_rev_usd as dd_ads_rev_ratio
   ,if(rcmd_ads_rev_usd is null, 0, rcmd_ads_rev_usd) * 1.0000 / ads_rev_usd as rcmd_ads_rev_ratio
   ,if(game_ads_rev_usd is null, 0, game_ads_rev_usd) * 1.0000 / ads_rev_usd as game_ads_rev_ratio
   ,if(brand_ads_rev_usd is null, 0, brand_ads_rev_usd) * 1.0000 / ads_rev_usd as brand_ads_rev_ratio
   ,if(live_ads_rev_usd is null, 0, live_ads_rev_usd) * 1.0000 / ads_rev_usd as live_ads_rev_ratio
   ,if(video_ads_rev_usd is null, 0, video_ads_rev_usd) * 1.0000 / ads_rev_usd as video_ads_rev_ratio
   ,(
       ads_rev_usd - if(search_ads_rev_usd is null, 0, search_ads_rev_usd) - if(dd_ads_rev_usd is null, 0, dd_ads_rev_usd) - if(rcmd_ads_rev_usd is null, 0, rcmd_ads_rev_usd) - if(game_ads_rev_usd is null, 0, game_ads_rev_usd) - if(brand_ads_rev_usd is null, 0, brand_ads_rev_usd) - if(live_ads_rev_usd is null, 0, live_ads_rev_usd) - if(video_ads_rev_usd is null, 0, video_ads_rev_usd)
   ) * 1.000000 / ads_rev_usd as undefined_rev_ratio
from
   (
       select
           dau
           ,ads_rev_usd
           ,take_rate
           ,ads_roi
           ,buyer_uv
           ,buyer_uv_rate
           ,order_1d
           ,order_per_u
           ,gmv_usd_1d
           ,search_ads_rev_usd
           ,search_ads_gmv_usd
           ,dd_ads_rev_usd
           ,dd_ads_gmv_usd
           ,rcmd_ads_rev_usd
           ,rcmd_ads_gmv_usd
           ,game_ads_rev_usd
           ,game_ads_gmv_usd
           ,brand_ads_rev_usd
           ,brand_ads_gmv_usd
           ,live_ads_rev_usd
           ,live_ads_gmv_usd
           ,platform_order_1d
           ,global_search_order_1d
           ,dd_order_1d
           ,ymal_order_1d
           ,post_purchase_order_1d
           ,private_domain_order_1d
           ,live_order_1d
           ,video_order_1d
           ,grass_month
           ,commission_fee_usd
           ,rebate_usd
           ,grass_region
           ,cast(net_ads_rev as varchar) as net_ads_rev
           ,cast(net_ads_rev_excl_1p as varchar) as net_ads_rev_excl_1p
           ,cast(net_ads_rev * 1.0000 / gmv_usd_1d as varchar) as net_take_rate
           ,cast(net_ads_rev_excl_1p * 1.0000 / gmv_usd_1d as varchar) as net_take_rate_excl_1p
           ,video_ads_rev_usd
           ,video_ads_gmv_usd
       from dev_video_bi.monthly_core_metrics_tracker
       where
           grass_date = date('${RUN_DATE_MINUS_1}')
           and grass_region = 'ID'
       union all
       select
           dau
           ,ads_rev_usd
           ,take_rate
           ,ads_roi
           ,buyer_uv
           ,buyer_uv_rate
           ,order_1d
           ,order_per_u
           ,gmv_usd_1d
           ,search_ads_rev_usd
           ,search_ads_gmv_usd
           ,dd_ads_rev_usd
           ,dd_ads_gmv_usd
           ,rcmd_ads_rev_usd
           ,rcmd_ads_gmv_usd
           ,game_ads_rev_usd
           ,game_ads_gmv_usd
           ,brand_ads_rev_usd
           ,brand_ads_gmv_usd
           ,live_ads_rev_usd
           ,live_ads_gmv_usd
           ,platform_order_1d
           ,global_search_order_1d
           ,dd_order_1d
           ,ymal_order_1d
           ,post_purchase_order_1d
           ,private_domain_order_1d
           ,live_order_1d
           ,video_order_1d
           ,grass_month
           ,commission_fee_usd
           ,rebate_usd
           ,grass_region
           ,cast(net_ads_rev as varchar) as net_ads_rev
           ,cast(net_ads_rev_excl_1p as varchar) as net_ads_rev_excl_1p
           ,cast(net_ads_rev * 1.0000 / gmv_usd_1d as varchar) as net_take_rate
           ,cast(net_ads_rev_excl_1p * 1.0000 / gmv_usd_1d as varchar) as net_take_rate_excl_1p
           ,video_ads_rev_usd
           ,video_ads_gmv_usd
       from dev_video_bi.temp_monthly_metrics_202511_vff
       where
           grass_region = 'ID'
       union all
       select
           *
           ,'' as net_ads_rev
           ,'' as net_ads_rev_excl_1p
           ,'' as net_take_rate
           ,'' as net_take_rate_excl_1p
           ,0 as video_ads_rev_usd
           ,0 as video_ads_gmv_usd
       from dev_video_bi.temp_monthly_metrics_202404_vf
       where grass_region = 'ID'
   ) m1
   left join (
       select
           grass_date
       from dev_video_bi.monthly_core_metrics_tracker
       where
           grass_date = date('${RUN_DATE_MINUS_1}')
           and grass_region = 'ID'
       group by 1
   ) m2 on m1.grass_month = substr(cast(m2.grass_date as varchar), 1, 7)
order by grass_month desc
