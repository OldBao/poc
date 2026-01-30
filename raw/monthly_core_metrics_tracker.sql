-- S&R&A Monthly Metrics Tracker - ETL
-- Deletes and re-inserts into dev_video_bi.monthly_core_metrics_tracker
-- Sources: DAU (m1), Ads Rev (m2), Orders/Buyer (m3), Order by Channel (m4), Commission/Rebate (m5), CB Rev (m6)

delete from dev_video_bi.monthly_core_metrics_tracker
where grass_date = date('${RUN_DATE_MINUS_1}')
;


insert into dev_video_bi.monthly_core_metrics_tracker
select
   /*+REPARTITION(1)*/
   dau
   ,m2.ads_rev_usd
   ,m2.ads_rev_usd * 1.0000 / gmv_usd_1d as take_rate
   ,m2.ads_gmv_usd * 1.0000 / ads_rev_usd as ads_roi
   ,m3.buyer_uv
   ,m3.buyer_uv * 1.0000 / dau as buyer_uv_rate
   ,m3.order_1d
   ,m3.order_1d * 1.0000 / dau as order_per_u
   ,m2.gmv_usd_1d
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
   ,m1.grass_month
   ,commission_fee_usd
   ,rebate_usd
   ,if(cb_rev is null, 0, cb_rev) as cb_rev
   ,if(cb_lovito_rev is null, 0, cb_lovito_rev) as cb_lovito_rev
   ,if(cb_scs_rev is null, 0, cb_scs_rev) as cb_scs_rev
   ,if(cb_others_rev is null, 0, cb_others_rev) as cb_others_rev
   ,if(cb_unknown_rev is null, 0, cb_unknown_rev) as cb_unknown_rev
   ,net_ads_rev
   ,net_ads_rev_excl_1p
   ,video_ads_rev_usd
   ,video_ads_gmv_usd
   ,m1.grass_region
   ,date('${RUN_DATE_MINUS_1}') as grass_date
from
   -- m1: DAU (avg daily DAU per month)
   (
       select
           substr(cast(grass_date as varchar), 1, 7) as grass_month
           ,grass_region
           ,avg(dau) as dau
       from
           (
               select
                   grass_date
                   ,grass_region
                   ,a1 as dau
               from traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
               where
                   grass_date between date '2025-11-01' and date('${RUN_DATE_MINUS_1}')
                   and tz_type = 'local'
               order by 1, 2, 3
           ) n
       group by 1, 2
   ) m1
   -- m2: Ads revenue, GMV, net ads rev by entry point
   left join (
       select
           n1.grass_region
           ,substr(cast(n1.grass_date as varchar), 1, 7) as grass_month
           ,avg(ads_rev_usd) as ads_rev_usd
           ,avg(net_ads_rev) as net_ads_rev
           ,avg((net_ads_rev_excl_1p + if(br_scs is null, 0, br_scs))) as net_ads_rev_excl_1p
           ,avg(gmv_usd_1d) as gmv_usd_1d
           ,avg(ads_gmv_usd) as ads_gmv_usd
           ,avg(search_ads_rev_usd) as search_ads_rev_usd
           ,avg(search_ads_gmv_usd) as search_ads_gmv_usd
           ,avg(dd_ads_rev_usd) as dd_ads_rev_usd
           ,avg(dd_ads_gmv_usd) as dd_ads_gmv_usd
           ,avg(rcmd_ads_rev_usd) as rcmd_ads_rev_usd
           ,avg(rcmd_ads_gmv_usd) as rcmd_ads_gmv_usd
           ,avg(game_ads_rev_usd) as game_ads_rev_usd
           ,avg(game_ads_gmv_usd) as game_ads_gmv_usd
           ,avg(brand_ads_rev_usd) as brand_ads_rev_usd
           ,avg(brand_ads_gmv_usd) as brand_ads_gmv_usd
           ,avg(live_ads_rev_usd) as live_ads_rev_usd
           ,avg(live_ads_gmv_usd) as live_ads_gmv_usd
           ,avg(video_ads_rev_usd) as video_ads_rev_usd
           ,avg(video_ads_gmv_usd) as video_ads_gmv_usd
       from
           (
               select
                   grass_region
                   ,grass_date
                   ,sum(ads_rev_usd) as ads_rev_usd
                   ,sum(0) as net_ads_rev
                   ,sum(0) as net_ads_rev_excl_1p
                   ,sum(0) as gmv_usd_1d
                   ,sum(ads_gmv_usd) as ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Search', 'Image Search') then ads_rev_usd
                       end
                   ) as search_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Search', 'Image Search') then ads_gmv_usd
                       end
                   ) as search_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Daily Discover') then ads_rev_usd
                       end
                   ) as dd_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Daily Discover') then ads_gmv_usd
                       end
                   ) as dd_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'You May Also Like'
                               ,'My Purchase Page Recommendation'
                               ,'Order Successful Recommendation'
                               ,'Order Detail Page Recommendation'
                               ,'Cart Recommendation'
                           )
                               then ads_rev_usd
                       end
                   ) as rcmd_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'You May Also Like'
                               ,'My Purchase Page Recommendation'
                               ,'Order Successful Recommendation'
                               ,'Order Detail Page Recommendation'
                               ,'Cart Recommendation'
                           )
                               then ads_gmv_usd
                       end
                   ) as rcmd_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Game') then ads_rev_usd
                       end
                   ) as game_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Game') then ads_gmv_usd
                       end
                   ) as game_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Shop', 'Shop Game', 'Display') then ads_rev_usd
                       end
                   ) as brand_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Shop', 'Shop Game', 'Display') then ads_gmv_usd
                       end
                   ) as brand_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Livestream Streaming Room'
                               ,'Livestream For You'
                               ,'Livestream Discovery'
                               ,'Livestream Others'
                           )
                               then ads_rev_usd
                       end
                   ) as live_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Livestream Streaming Room'
                               ,'Livestream For You'
                               ,'Livestream Discovery'
                               ,'Livestream Others'
                           )
                               then ads_gmv_usd
                       end
                   ) as live_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Video Trending Tab'
                               ,'HP Internal Video'
                               ,'DD Internal Video'
                           )
                               then ads_rev_usd
                       end
                   ) as video_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Video Trending Tab'
                               ,'HP Internal Video'
                               ,'DD Internal Video'
                           )
                               then ads_gmv_usd
                       end
                   ) as video_ads_gmv_usd
               from mkplpaidads_analytics.ads_take_rate_dashboard_v2
               where grass_date between date '2025-11-01' and date '2023-12-31'
               group by 1, 2
               union all
               select
                   grass_region
                   ,grass_date
                   ,sum(ads_rev_usd) as ads_rev_usd
                   ,sum(net_ads_rev_usd) as net_ads_rev
                   ,sum(
                       case
                           when seller_type_1p not in ('Local SCS', 'SCS', 'Lovito') then net_ads_rev_excl_sip_usd_1d
                       end
                   ) as net_ads_rev_excl_1p
                   ,sum(distinct platform_gmv_excl_testorder) as gmv_usd_1d
                   ,sum(ads_gmv_usd) as ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Global Search', 'Image Search') then ads_rev_usd
                       end
                   ) as search_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Global Search', 'Image Search') then ads_gmv_usd
                       end
                   ) as search_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Daily Discover External'
                               ,'Daily Discover Mix Feed Internal'
                               ,'DD External Video'
                           )
                               then ads_rev_usd
                       end
                   ) as dd_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Daily Discover External'
                               ,'Daily Discover Mix Feed Internal'
                               ,'DD External Video'
                           )
                               then ads_gmv_usd
                       end
                   ) as dd_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Cart Recommendation'
                               ,'Me You May Also Like'
                               ,'My Purchase Page Recommendation'
                               ,'Order Detail Page Recommendation'
                               ,'Order Successful Recommendation'
                               ,'You May Also Like'
                           )
                               then ads_rev_usd
                       end
                   ) as rcmd_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Cart Recommendation'
                               ,'Me You May Also Like'
                               ,'My Purchase Page Recommendation'
                               ,'Order Detail Page Recommendation'
                               ,'Order Successful Recommendation'
                               ,'You May Also Like'
                           )
                               then ads_gmv_usd
                       end
                   ) as rcmd_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Game') then ads_rev_usd
                       end
                   ) as game_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Game') then ads_gmv_usd
                       end
                   ) as game_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in ('Shop', 'Shop Game', 'Display') then ads_rev_usd
                       end
                   ) as brand_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in ('Shop', 'Shop Game', 'Display') then ads_gmv_usd
                       end
                   ) as brand_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Livestream Autolanding'
                               ,'Livestream PDP'
                               ,'Livestream Discovery'
                               ,'Livestream Homepage'
                               ,'Livestream Video Feed'
                               ,'Livestream For You'
                           )
                               then ads_rev_usd
                       end
                   ) as live_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Livestream Autolanding'
                               ,'Livestream PDP'
                               ,'Livestream Discovery'
                               ,'Livestream Homepage'
                               ,'Livestream Video Feed'
                               ,'Livestream For You'
                           )
                               then ads_gmv_usd
                       end
                   ) as live_ads_gmv_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Video Trending Tab'
                               ,'HP Internal Video'
                               ,'DD Internal Video'
                           )
                               then ads_rev_usd
                       end
                   ) as video_ads_rev_usd
                   ,sum(
                       case
                           when entry_point in (
                               'Video Trending Tab'
                               ,'HP Internal Video'
                               ,'DD Internal Video'
                           )
                               then ads_gmv_usd
                       end
                   ) as video_ads_gmv_usd
               from mp_paidads.ads_advertise_take_rate_v2_1d__reg_s0_live
               where
                   grass_date between date '2025-11-01' and date('${RUN_DATE_MINUS_1}')
                   and tz_type = 'regional'
               group by 1, 2
           ) n1
           left join (
               select
                   grass_date
                   ,grass_region
                   ,sum(free_rev) as br_scs
               from
                   (
                       select
                           grass_date
                           ,grass_region
                           ---- credit_program_name 为空时候，根据充值的类型，划分为other成本
                           ,case
                               when (
                                   credit_topup_type_name like '%free credit%'
                                       and credit_program_name = ''
                               )
                                   then 'free_credit_topup_others'
                               when credit_topup_type_name like '%free credit%' then coalesce(credit_program_name, 'free_credit_topup_others')
                               when (
                                   credit_topup_type_name like '%paid credit%'
                                       and credit_program_name = ''
                               )
                                   then 'paid_credit_topup_others'
                               when credit_topup_type_name like '%paid credit%' then coalesce(credit_program_name, 'paid_credit_topup_others')
                               else coalesce(credit_program_name, 'others')
                           end as credit_program_name
                           ,sum(free_ads_revenue_amt_usd_1d) as free_rev
                       from mp_paidads.dws_advertise_net_ads_revenue_1d__reg_s0_live
                       where
                           grass_date >= date '2025-11-11'
                           and tz_type = 'regional'
                           and grass_region in ('BR')
                       group by 1, 2, 3
                   ) nn
               where credit_program_name = '2025_0034_BR_AD_SAS_CREDITS'
               group by 1, 2
           ) n2 on n1.grass_date = n2.grass_date
           and n1.grass_region = n2.grass_region
       group by 1, 2
   ) m2 on m1.grass_month = m2.grass_month
   and m1.grass_region = m2.grass_region
   -- m3: Orders and buyer UV
   left join (
       select
           substr(cast(grass_date as varchar), 1, 7) as grass_month
           ,grass_region
           ,avg(order_1d) as order_1d
           -- ,avg(gmv_usd_1d) as gmv_usd_1d
           ,avg(buyer_uv) as buyer_uv
       from
           (
               select
                   grass_region
                   ,grass_date
                   ,sum(order_fraction * atc_prorate * first_touchpoint_item) as order_1d
                   ,sum(gmv_usd * atc_prorate * first_touchpoint_item) as gmv_usd_1d
                   ,count(
                       distinct case
                           when gmv_usd * atc_prorate * first_touchpoint_item > 0 then user_id
                       end
                   ) as buyer_uv
               from traffic_omni_oa.dwd_order_item_atc_journey_di__reg_sensitive_live
               where
                   grass_date between date '2025-11-01' and date('${RUN_DATE_MINUS_1}')
                   and tz_type = 'local'
                   and first_touchpoint_item = 1
                   and user_id > 0
                   and user_id is not null
                   and order_item_id is not null
               group by 1, 2
           ) n
       group by 1, 2
   ) m3 on m1.grass_month = m3.grass_month
   and m1.grass_region = m3.grass_region
   -- m4: Order % by channel/feature
   left join (
       select
           n1.grass_month
           ,n1.grass_region
           ,platform_order_1d
           ,global_search_order_1d
           ,dd_order_1d
           ,ymal_order_1d
           ,post_purchase_order_1d
           ,private_domain_order_1d
           ,case
               when n1.grass_month in ('2023-01', '2023-02', '2023-03', '2023-04') then n2.live_order_1d
               else n1.live_order_1d
           end as live_order_1d
           ,case
               when n1.grass_month in ('2023-01', '2023-02', '2023-03', '2023-04') then n2.video_order_1d
               else n1.video_order_1d
           end as video_order_1d
       from
           (
               select
                   substr(cast(grass_date as varchar), 1, 7) as grass_month
                   ,grass_region
                   ,avg(
                       case
                           when feature = 'Platform' then order_cnt_login_user_first_lead
                       end
                   ) as platform_order_1d
                   ,avg(
                       case
                           when feature = 'Global Search' then order_cnt_login_user_first_lead
                       end
                   ) as global_search_order_1d
                   ,avg(
                       case
                           when feature = 'Daily Discover' then order_cnt_login_user_first_lead
                       end
                   ) as dd_order_1d
                   ,avg(
                       case
                           when feature = 'You May Also Like' then order_cnt_login_user_first_lead
                       end
                   ) as ymal_order_1d
                   ,avg(
                       case
                           when feature = 'post purchase' then order_cnt_login_user_first_lead
                       end
                   ) as post_purchase_order_1d
                   ,avg(
                       case
                           when feature = 'Private Domain Features' then order_cnt_login_user_first_lead
                       end
                   ) as private_domain_order_1d
                   ,avg(
                       case
                           when feature in ('Live Streaming') then order_cnt_login_user_first_lead
                       end
                   ) as live_order_1d
                   ,avg(
                       case
                           when feature in ('Video') then order_cnt_login_user_first_lead
                       end
                   ) as video_order_1d
               from dev_video_bi.sr_okr_table_metric_dws
               where
                   grass_date between date '2025-11-01' and date('${RUN_DATE_MINUS_1}')
                   and tz_type = 'regional'
               group by 1, 2
           ) n1
           left join (
               select
                   grass_region
                   ,grass_month
                   ,coalesce(
                       avg(
                           case
                               when feature in ('Live Streaming') then order_1d
                           end
                       )
                       ,0
                   ) as live_order_1d
                   ,coalesce(
                       avg(
                           case
                               when feature in ('Video') then order_1d
                           end
                       )
                       ,0
                   ) as video_order_1d
               from dev_video_bi.temp_video_ls_202301_202304
               group by 1, 2
           ) n2 on n1.grass_region = n2.grass_region
           and n1.grass_month = n2.grass_month
   ) m4 on m1.grass_month = m4.grass_month
   and m1.grass_region = m4.grass_region
   -- m5: Commission fee and rebate
   left join (
       select
           grass_region
           ,substr(cast(grass_date as varchar), 1, 7) as grass_month
           ,avg(commission_fee_usd) as commission_fee_usd
           ,avg(rebate_usd) as rebate_usd
       from
           (
               select
                   cast(create_datetime as date) as grass_date
                   ,grass_region
                   ,sum(commission_fee_usd) as commission_fee_usd
                   ,sum(sv_coin_earn_by_shopee_amt_usd) + sum(pv_coin_earn_by_shopee_amt_usd) + sum(actual_shipping_rebate_by_shopee_amt_usd) + sum(pv_rebate_by_shopee_amt_usd) + sum(sv_rebate_by_shopee_amt_usd) + sum(item_rebate_by_shopee_amt_usd) + sum(card_rebate_by_shopee_amt_usd) as rebate_usd
               from mp_order.dwd_order_item_all_ent_df__reg_s0_live
               where
                   cast(create_datetime as date) between date '2025-11-01' and date('${RUN_DATE_MINUS_1}')
                   and (
                       grass_date >= cast(create_datetime as date)
                       or grass_date = date '9999-01-01'
                   )
                   and tz_type = 'local'
               group by 1, 2
           ) m
       group by
           1
           ,2
           -- union all
           -- select
           --     grass_region
           --     ,grass_month
           --     ,commission_fee_usd
           --     ,rebate_usd
           -- from dev_video_bi.commission_fee_rebate_202201_202403
   ) m5 on m1.grass_month = m5.grass_month
   and m1.grass_region = m5.grass_region
   -- m6: Cross-border (CB) revenue breakdown
   left join (
       select
           substr(cast(grass_date as varchar), 1, 7) as grass_month
           ,grass_region
           ,avg(cb_rev) as cb_rev
           ,avg(cb_lovito_rev) as cb_lovito_rev
           ,avg(cb_scs_rev) as cb_scs_rev
           ,avg(cb_others_rev) as cb_others_rev
           ,avg(cb_unknown_rev) as cb_unknown_rev
       from
           (
               select
                   grass_region
                   ,grass_date
                   ,sum(ads_rev_lovito) + sum(ads_rev_scs) + sum(ads_rev_others) + sum(ads_rev_unknow) as cb_rev
                   ,sum(ads_rev_lovito) as cb_lovito_rev
                   ,sum(ads_rev_scs) as cb_scs_rev
                   ,sum(ads_rev_others) as cb_others_rev
                   ,sum(ads_rev_unknow) as cb_unknown_rev
               from dev_video_bi.ads_okr_tracker_metrics
               where grass_date between date '2025-11-01' and date('${RUN_DATE_MINUS_1}')
               group by 1, 2
           ) a
       group by 1, 2
   ) m6 on m1.grass_month = m6.grass_month
   and m1.grass_region = m6.grass_region
;
