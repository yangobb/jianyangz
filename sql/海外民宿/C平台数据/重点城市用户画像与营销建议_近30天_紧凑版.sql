-- 重点城市用户画像与营销建议基础数据（紧凑版）
-- 默认口径：15 个海外重点城市；订单近 30 天；曝光价格近 14 天
-- 适用场景：Whale 跑数与分析报告，避免大表多次 UNION 重扫和房屋明细 JOIN

with city_list as (
    select '东京' city_name union all
    select '大阪' union all
    select '京都' union all
    select '吉隆坡' union all
    select '首尔' union all
    select '曼谷' union all
    select '巴厘岛' union all
    select '香港' union all
    select '澳门' union all
    select '新加坡' union all
    select '芭堤雅' union all
    select '普吉岛' union all
    select '清迈' union all
    select '济州市' union all
    select '胡志明市'
)
,tj_flow as (
    select city_name
        ,'途家_空搜' as source
        ,'曝光价格' as segment_type
        ,'近14天' as segment_value
        ,count(1) as lpv
        ,count(distinct uid) as luv
        ,count(case when detail_uid is not null then 1 end) as dpv
        ,count(distinct case when detail_uid is not null then detail_uid end) as duv
        ,cast(null as bigint) as order_cnt
        ,cast(null as bigint) as user_cnt
        ,cast(null as double) as night
        ,cast(null as double) as gmv
        ,round(avg(final_price),2) as avg_exp_price
        ,round(avg(case when detail_uid is not null then final_price end),2) as click_avg_price
        ,cast(null as double) as adr
        ,cast(null as double) as avg_night_per_order
        ,cast(null as double) as avg_gmv_per_order
        ,cast(null as double) as avg_lead_days
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1)
      and is_oversea = 1
      and wrapper_name in ('携程','途家','去哪儿')
      and user_type = '用户'
      and empty_filter = 1
      and city_name in (select city_name from city_list)
      and final_price > 0
    group by 1,2,3,4
)
,ct_flow_base as (
    select t2.cityname as city_name
        ,t1.uid
        ,t1.d
        ,t1.fh_price
        ,t1.is_has_click
        ,t1.m_zone
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day t1
    inner join excel_upload.dim_ctrip_list_qid_city t2
      on t1.m_city = t2.m_city
    where t1.d between date_sub(current_date,14) and date_sub(current_date,1)
      and t2.cityname in (select city_name from city_list)
      and t1.fh_price > 0
)
,ct_flow as (
    select city_name
        ,source
        ,'曝光价格' as segment_type
        ,'近14天' as segment_value
        ,count(1) as lpv
        ,count(distinct concat_ws('|',d,uid)) as luv
        ,count(case when is_has_click = 1 then 1 end) as dpv
        ,count(distinct case when is_has_click = 1 then concat_ws('|',d,uid) end) as duv
        ,cast(null as bigint) as order_cnt
        ,cast(null as bigint) as user_cnt
        ,cast(null as double) as night
        ,cast(null as double) as gmv
        ,round(avg(fh_price),2) as avg_exp_price
        ,round(avg(case when is_has_click = 1 then fh_price end),2) as click_avg_price
        ,cast(null as double) as adr
        ,cast(null as double) as avg_night_per_order
        ,cast(null as double) as avg_gmv_per_order
        ,cast(null as double) as avg_lead_days
    from (
        select '携程L页_空搜近似' as source, *
        from ct_flow_base
        where m_zone is null or m_zone = ''
        union all
        select '携程L页_全量基线' as source, *
        from ct_flow_base
    ) a
    group by 1,2,3,4
)
,tj_order_base as (
    select a.city_name
        ,case when a.terminal_type_name like '%本站%' then 'T'
            when a.terminal_type_name like '%携程%' then 'C'
            when a.terminal_type_name like '%去哪儿%' then 'Q'
            else '其他' end as channel
        ,case when u.first_create_date_outseas < a.create_date then '老客' else '新客' end as user_type
        ,case when datediff(a.checkin_date,a.create_date) = 0 then 'T0'
            when datediff(a.checkin_date,a.create_date) between 1 and 7 then 'T1-7'
            when datediff(a.checkin_date,a.create_date) between 8 and 14 then 'T8-14'
            when datediff(a.checkin_date,a.create_date) between 15 and 30 then 'T15-30'
            else 'T31+' end as lead_bucket
        ,case when datediff(a.checkout_date,a.checkin_date) = 1 then '1晚'
            when datediff(a.checkout_date,a.checkin_date) between 2 and 3 then '2-3晚'
            when datediff(a.checkout_date,a.checkin_date) between 4 and 7 then '4-7晚'
            else '8晚及以上' end as stay_bucket
        ,a.order_no
        ,a.user_id
        ,a.order_room_night_count
        ,a.room_total_amount
        ,datediff(a.checkin_date,a.create_date) as lead_days
    from dws.dws_order a
    left join pdb_analysis_c.ads_user_ltv_detail_d u
      on a.user_id = u.user_id
     and u.dt = date_sub(current_date,1)
    where a.create_date between date_sub(current_date,30) and date_sub(current_date,1)
      and a.is_paysuccess_order = 1
      and a.is_cancel_order = 0
      and a.is_overseas = 1
      and a.city_name in (select city_name from city_list)
      and (a.terminal_type_name in ('本站-APP','携程-APP','去哪儿-APP') or a.terminal_type_name like '%小程序%')
)
,tj_order as (
    select city_name
        ,'途家订单' as source
        ,case when grouping(channel) = 0 then '渠道'
            when grouping(user_type) = 0 then '新老客'
            when grouping(lead_bucket) = 0 then '提前期'
            when grouping(stay_bucket) = 0 then '连住'
            else '整体' end as segment_type
        ,case when grouping(channel) = 0 then channel
            when grouping(user_type) = 0 then user_type
            when grouping(lead_bucket) = 0 then lead_bucket
            when grouping(stay_bucket) = 0 then stay_bucket
            else '近30天' end as segment_value
        ,cast(null as bigint) as lpv
        ,cast(null as bigint) as luv
        ,cast(null as bigint) as dpv
        ,cast(null as bigint) as duv
        ,count(distinct order_no) as order_cnt
        ,count(distinct user_id) as user_cnt
        ,sum(order_room_night_count) as night
        ,sum(room_total_amount) as gmv
        ,cast(null as double) as avg_exp_price
        ,cast(null as double) as click_avg_price
        ,round(sum(room_total_amount) / nullif(sum(order_room_night_count),0),2) as adr
        ,round(sum(order_room_night_count) / nullif(count(distinct order_no),0),2) as avg_night_per_order
        ,round(sum(room_total_amount) / nullif(count(distinct order_no),0),2) as avg_gmv_per_order
        ,round(avg(lead_days),2) as avg_lead_days
    from tj_order_base
    group by grouping sets (
        (city_name),
        (city_name, channel),
        (city_name, user_type),
        (city_name, lead_bucket),
        (city_name, stay_bucket)
    )
)
,ct_order_base as (
    select t2.cityname as city_name
        ,t1.orderid
        ,t1.uid
        ,cast(t1.ciiquantity as double) as room_night
        ,cast(t1.ciireceivable as double) as gmv
        ,datediff(date_sub(to_date(t1.departure),cast(nvl(t1.ciiquantity,0) as int)),to_date(t1.orderdate)) as lead_days
        ,case when datediff(date_sub(to_date(t1.departure),cast(nvl(t1.ciiquantity,0) as int)),to_date(t1.orderdate)) = 0 then 'T0'
            when datediff(date_sub(to_date(t1.departure),cast(nvl(t1.ciiquantity,0) as int)),to_date(t1.orderdate)) between 1 and 7 then 'T1-7'
            when datediff(date_sub(to_date(t1.departure),cast(nvl(t1.ciiquantity,0) as int)),to_date(t1.orderdate)) between 8 and 14 then 'T8-14'
            when datediff(date_sub(to_date(t1.departure),cast(nvl(t1.ciiquantity,0) as int)),to_date(t1.orderdate)) between 15 and 30 then 'T15-30'
            else 'T31+' end as lead_bucket
        ,case when cast(t1.ciiquantity as int) = 1 then '1晚'
            when cast(t1.ciiquantity as int) between 2 and 3 then '2-3晚'
            when cast(t1.ciiquantity as int) between 4 and 7 then '4-7晚'
            else '8晚及以上' end as stay_bucket
    from app_ctrip.edw_htl_order_all_split t1
    inner join excel_upload.dim_ctrip_list_qid_city t2
      on t1.cityid = t2.m_city
    where t1.d = date_sub(current_date,1)
      and to_date(t1.orderdate) between date_sub(current_date,30) and date_sub(current_date,1)
      and t1.orderstatus in ('S','P')
      and t1.ordertype = 2
      and t2.cityname in (select city_name from city_list)
)
,ct_order as (
    select city_name
        ,'携程订单' as source
        ,case when grouping(lead_bucket) = 0 then '提前期'
            when grouping(stay_bucket) = 0 then '连住'
            else '整体' end as segment_type
        ,case when grouping(lead_bucket) = 0 then lead_bucket
            when grouping(stay_bucket) = 0 then stay_bucket
            else '近30天' end as segment_value
        ,cast(null as bigint) as lpv
        ,cast(null as bigint) as luv
        ,cast(null as bigint) as dpv
        ,cast(null as bigint) as duv
        ,count(distinct orderid) as order_cnt
        ,count(distinct uid) as user_cnt
        ,sum(room_night) as night
        ,sum(gmv) as gmv
        ,cast(null as double) as avg_exp_price
        ,cast(null as double) as click_avg_price
        ,round(sum(gmv) / nullif(sum(room_night),0),2) as adr
        ,round(sum(room_night) / nullif(count(distinct orderid),0),2) as avg_night_per_order
        ,round(sum(gmv) / nullif(count(distinct orderid),0),2) as avg_gmv_per_order
        ,round(avg(lead_days),2) as avg_lead_days
    from ct_order_base
    group by grouping sets (
        (city_name),
        (city_name, lead_bucket),
        (city_name, stay_bucket)
    )
)
select *
from tj_flow
union all
select *
from ct_flow
union all
select *
from tj_order
union all
select *
from ct_order
order by city_name, source, segment_type, segment_value
;
