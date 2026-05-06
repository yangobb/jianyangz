-- 新加坡出行习惯 / 浏览偏好 / 易转化&难转化分层
-- 优化版：使用 GROUPING SETS，一次扫描流量表完成多维聚合，避免 union all 重复扫描导致超时。

with params as (
    select date_sub(current_date,14) as start_dt
        ,date_sub(current_date,1) as end_dt
        ,'新加坡' as target_city
)
,base as (
    select a.dt
        ,a.uid
        ,a.detail_uid
        ,a.booking_uid
        ,nvl(a.without_risk_access_order_num,0) as attr_order_cnt
        ,nvl(a.without_risk_access_order_room_night,0) as attr_night
        ,nvl(a.without_risk_access_order_gmv,0) as attr_gmv
        ,a.final_price
        ,case when lower(a.platform) like '%ios%' then 'iOS'
            when lower(a.platform) like '%android%' then 'Android'
            when a.platform is null or a.platform = '' then '未知'
            else a.platform end as platform_type
        ,nvl(a.client_agent,'未知') as client_agent
        ,case when a.empty_filter = 1 then '空搜' else '带条件搜索' end as search_filter_type
        ,nvl(a.rank_scene,'未知') as rank_scene
        ,nvl(a.query_type,'未知') as query_type
        ,nvl(a.sort_type,'未知') as sort_type
        ,case when cast(regexp_extract(nvl(a.guest,'0'),'([0-9]+)',1) as int) <= 0 then 0
            when cast(regexp_extract(nvl(a.guest,'0'),'([0-9]+)',1) as int) >= 9 then 9
            else cast(regexp_extract(nvl(a.guest,'0'),'([0-9]+)',1) as int) end as guest_num
        ,case when cast(regexp_extract(nvl(a.filter_bedroom_count,'0'),'([0-9]+)',1) as int) <= 0 then 0
            when cast(regexp_extract(nvl(a.filter_bedroom_count,'0'),'([0-9]+)',1) as int) >= 3 then 3
            else cast(regexp_extract(nvl(a.filter_bedroom_count,'0'),'([0-9]+)',1) as int) end as filter_bedroom_num
        ,case when datediff(to_date(a.checkout_date),to_date(a.checkin_date)) <= 0 then null
            else datediff(to_date(a.checkout_date),to_date(a.checkin_date)) end as stay_night
        ,case when datediff(to_date(a.checkin_date),to_date(a.dt)) < 0 then null
            else datediff(to_date(a.checkin_date),to_date(a.dt)) end as lead_days
        ,case when a.max_price is null or a.max_price <= 0 then '未筛价格'
            when a.max_price <= 500 then '0-500'
            when a.max_price <= 1000 then '500-1000'
            when a.max_price <= 2000 then '1000-2000'
            when a.max_price <= 4000 then '2000-4000'
            else '4000+' end as search_price_bucket
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.start_dt and p.end_dt
      and a.city_name = p.target_city
      and a.is_oversea = 1
      and a.user_type = '用户'
      and a.wrapper_name in ('携程','去哪儿','途家')
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.is_recommend = 0
      and a.final_price > 0
)
,tagged as (
    select b.*
        ,case when b.stay_night >= 7 then '商务/长住'
            when b.guest_num = 1 and b.final_price <= 600 then '背包客/独行低价'
            when b.guest_num = 2 and b.filter_bedroom_num <= 1 and b.stay_night between 1 and 3 then '情侣/双人短途'
            when b.guest_num >= 3 or b.filter_bedroom_num >= 2 then '亲子/家庭/多人'
            when b.stay_night between 4 and 6 then '中长住休闲'
            else '普通出行' end as travel_persona
        ,case when b.stay_night = 1 then '1晚'
            when b.stay_night between 2 and 3 then '2-3晚'
            when b.stay_night between 4 and 7 then '4-7晚'
            when b.stay_night >= 8 then '8晚及以上'
            else '未知' end as stay_bucket
        ,case when b.lead_days = 0 then 'T0'
            when b.lead_days between 1 and 7 then 'T1-7'
            when b.lead_days between 8 and 14 then 'T8-14'
            when b.lead_days between 15 and 30 then 'T15-30'
            when b.lead_days > 30 then 'T31+'
            else '未知' end as lead_bucket
        ,case when b.guest_num = 1 then '1人'
            when b.guest_num = 2 then '2人'
            when b.guest_num between 3 and 4 then '3-4人'
            when b.guest_num >= 5 then '5人及以上'
            else '未知' end as guest_bucket
        ,case when b.filter_bedroom_num = 1 then '1居'
            when b.filter_bedroom_num = 2 then '2居'
            when b.filter_bedroom_num >= 3 then '3居及以上'
            else '未筛居室' end as filter_bedroom_bucket
    from base b
)
,agg as (
    select case when grouping(travel_persona) = 0 then '出行-人群推断'
            when grouping(guest_bucket) = 0 then '出行-入住人数'
            when grouping(stay_bucket) = 0 then '出行-入住晚数'
            when grouping(lead_bucket) = 0 then '出行-提前期'
            when grouping(search_price_bucket) = 0 then '浏览-价格偏好'
            when grouping(filter_bedroom_bucket) = 0 then '浏览-居室偏好'
            when grouping(search_filter_type) = 0 then '浏览-搜索方式'
            when grouping(sort_type) = 0 then '浏览-排序偏好'
            when grouping(rank_scene) = 0 then '浏览-搜索场景'
            when grouping(query_type) = 0 then '浏览-搜索类型'
            when grouping(platform_type) = 0 then '基础-设备平台'
            when grouping(client_agent) = 0 then '基础-客户端入口'
            else '未知' end as profile_type
        ,case when grouping(travel_persona) = 0 then travel_persona
            when grouping(guest_bucket) = 0 then guest_bucket
            when grouping(stay_bucket) = 0 then stay_bucket
            when grouping(lead_bucket) = 0 then lead_bucket
            when grouping(search_price_bucket) = 0 then search_price_bucket
            when grouping(filter_bedroom_bucket) = 0 then filter_bedroom_bucket
            when grouping(search_filter_type) = 0 then search_filter_type
            when grouping(sort_type) = 0 then sort_type
            when grouping(rank_scene) = 0 then rank_scene
            when grouping(query_type) = 0 then query_type
            when grouping(platform_type) = 0 then platform_type
            when grouping(client_agent) = 0 then client_agent
            else '未知' end as profile_value
        ,count(distinct concat(dt,'|',uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when detail_uid is not null then concat(dt,'|',detail_uid) end) as duv
        ,count(distinct case when booking_uid is not null then concat(dt,'|',booking_uid) end) as buv
        ,sum(attr_order_cnt) as attr_order_cnt
        ,sum(attr_night) as attr_night
        ,sum(attr_gmv) as attr_gmv
        ,round(avg(final_price),2) as avg_exp_price
        ,round(avg(stay_night),2) as avg_search_stay_night
        ,round(avg(lead_days),2) as avg_search_lead_days
    from tagged
    group by grouping sets (
        (travel_persona),
        (guest_bucket),
        (stay_bucket),
        (lead_bucket),
        (search_price_bucket),
        (filter_bedroom_bucket),
        (search_filter_type),
        (sort_type),
        (rank_scene),
        (query_type),
        (platform_type),
        (client_agent)
    )
)
select profile_type
    ,profile_value
    ,luv
    ,lpv
    ,duv
    ,buv
    ,round(duv / nullif(luv,0),4) as l2d_uv
    ,round(buv / nullif(luv,0),4) as l2b_uv
    ,attr_order_cnt
    ,attr_night
    ,round(attr_gmv,2) as attr_gmv
    ,avg_exp_price
    ,avg_search_stay_night
    ,avg_search_lead_days
    ,round(attr_order_cnt / nullif(luv,0),4) as attr_order_per_luv
    ,round(attr_night / nullif(luv,0) * 1000,2) as attr_night_per_1000_luv
    ,round(attr_gmv / nullif(luv,0),2) as attr_gmv_per_luv
    ,case when luv < 100 then '样本小-观察'
        when attr_order_cnt / nullif(luv,0) >= 0.010 or attr_night / nullif(luv,0) >= 0.020 then '易转化'
        when duv / nullif(luv,0) >= 0.120 and nvl(attr_order_cnt,0) = 0 then '高点击低下单-难转化'
        when duv / nullif(luv,0) < 0.080 then '低点击-难转化'
        else '中性观察' end as conversion_class
from agg
order by profile_type, conversion_class, luv desc
;
