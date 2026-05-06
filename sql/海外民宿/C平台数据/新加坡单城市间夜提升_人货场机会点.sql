-- 新加坡单城市间夜提升：人 / 货 / 场 / 供需匹配机会点
-- 口径：
--   需求/场：dws.dws_path_ldbo_d 近14天，海外、用户、APP、前台展示、非推荐流量
--   供给：dws.dws_house_d 最新分区，在线海外房源
--   订单：dws.dws_order 近30天，支付未取消海外订单
--   目标城市：新加坡

with params as (
    select date_sub(current_date,14) as flow_start_dt
        ,date_sub(current_date,1) as end_dt
        ,date_sub(current_date,30) as order_start_dt
        ,'新加坡' as target_city
)
,house_base as (
    select h.house_id
        ,h.hotel_id
        ,h.house_city_name as city_name
        ,nvl(h.dynamic_business,'未知商圈') as dynamic_business
        ,case when h.bedroom_count = 1 then '1居'
            when h.bedroom_count = 2 then '2居'
            when h.bedroom_count >= 3 then '3居及以上'
            else '未知居室' end as bedroom_bucket
        ,case when h.landlord_channel_name = '平台商户' or h.landlord_channel = 1 then '直采'
            else 'C接' end as supply_type
        ,nvl(h.house_class,'未知等级') as house_class
        ,nvl(h.avaliable_count,0) as avaliable_count
        ,nvl(h.picture_count,0) as picture_count
        ,case when h.is_fast_booking = 1 then '闪订' else '非闪订' end as fast_booking_type
    from dws.dws_house_d h
    cross join params p
    where h.dt = p.end_dt
      and h.house_is_online = 1
      and h.house_is_oversea = 1
      and h.house_city_name = p.target_city
)
,flow_house as (
    select a.house_id
        ,count(1) as lpv
        ,count(distinct concat(a.dt,'|',a.uid)) as luv
        ,count(case when a.detail_uid is not null then 1 end) as dpv
        ,count(distinct case when a.detail_uid is not null then concat(a.dt,'|',a.detail_uid) end) as duv
        ,nvl(sum(a.without_risk_access_order_num),0) as attr_order_cnt
        ,nvl(sum(a.without_risk_access_order_room_night),0) as attr_night
        ,nvl(sum(a.without_risk_access_order_gmv),0) as attr_gmv
        ,avg(case when a.final_price > 0 then a.final_price end) as avg_exp_price
        ,avg(case when a.detail_uid is not null and a.final_price > 0 then a.final_price end) as click_avg_price
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.flow_start_dt and p.end_dt
      and a.wrapper_name in ('携程','去哪儿','途家')
      and a.client_name = 'APP'
      and a.user_type = '用户'
      and a.is_oversea = 1
      and a.front_display = 'true'
      and a.is_recommend = 0
      and a.city_name = p.target_city
      and a.house_id is not null
    group by 1
)
,order_house as (
    select o.house_id
        ,count(distinct o.order_no) as paid_order_cnt
        ,sum(o.order_room_night_count) as paid_night
        ,sum(o.room_total_amount) as paid_gmv
    from dws.dws_order o
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name = p.target_city
    group by 1
)
,base as (
    select h.*
        ,nvl(f.lpv,0) as lpv
        ,nvl(f.luv,0) as luv
        ,nvl(f.dpv,0) as dpv
        ,nvl(f.duv,0) as duv
        ,nvl(f.attr_order_cnt,0) as attr_order_cnt
        ,nvl(f.attr_night,0) as attr_night
        ,nvl(f.attr_gmv,0) as attr_gmv
        ,f.avg_exp_price
        ,f.click_avg_price
        ,nvl(o.paid_order_cnt,0) as paid_order_cnt
        ,nvl(o.paid_night,0) as paid_night
        ,nvl(o.paid_gmv,0) as paid_gmv
    from house_base h
    left join flow_house f
      on h.house_id = f.house_id
    left join order_house o
      on h.house_id = o.house_id
)
,order_persona as (
    select '人-提前期' as view_type
        ,case when datediff(o.checkin_date,o.create_date) = 0 then 'T0'
            when datediff(o.checkin_date,o.create_date) between 1 and 7 then 'T1-7'
            when datediff(o.checkin_date,o.create_date) between 8 and 14 then 'T8-14'
            when datediff(o.checkin_date,o.create_date) between 15 and 30 then 'T15-30'
            else 'T31+' end as dim_name
        ,'订单需求' as dim_sub_name
        ,cast(null as bigint) as supply_house_cnt
        ,cast(null as bigint) as exposed_house_cnt
        ,cast(null as bigint) as lpv
        ,cast(null as bigint) as luv
        ,cast(null as bigint) as dpv
        ,cast(null as bigint) as duv
        ,count(distinct o.order_no) as paid_order_cnt
        ,sum(o.order_room_night_count) as paid_night
        ,sum(o.room_total_amount) as paid_gmv
        ,round(sum(o.room_total_amount) / nullif(sum(o.order_room_night_count),0),2) as adr
        ,round(sum(o.order_room_night_count) / nullif(count(distinct o.order_no),0),2) as avg_night_per_order
        ,round(avg(datediff(o.checkin_date,o.create_date)),2) as avg_lead_days
        ,cast(null as double) as avg_exp_price
        ,cast(null as double) as l2d_uv
    from dws.dws_order o
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name = p.target_city
    group by 1,2,3
    union all
    select '人-连住' as view_type
        ,case when datediff(o.checkout_date,o.checkin_date) = 1 then '1晚'
            when datediff(o.checkout_date,o.checkin_date) between 2 and 3 then '2-3晚'
            when datediff(o.checkout_date,o.checkin_date) between 4 and 7 then '4-7晚'
            else '8晚及以上' end as dim_name
        ,'订单需求' as dim_sub_name
        ,cast(null as bigint) as supply_house_cnt
        ,cast(null as bigint) as exposed_house_cnt
        ,cast(null as bigint) as lpv
        ,cast(null as bigint) as luv
        ,cast(null as bigint) as dpv
        ,cast(null as bigint) as duv
        ,count(distinct o.order_no) as paid_order_cnt
        ,sum(o.order_room_night_count) as paid_night
        ,sum(o.room_total_amount) as paid_gmv
        ,round(sum(o.room_total_amount) / nullif(sum(o.order_room_night_count),0),2) as adr
        ,round(sum(o.order_room_night_count) / nullif(count(distinct o.order_no),0),2) as avg_night_per_order
        ,round(avg(datediff(o.checkin_date,o.create_date)),2) as avg_lead_days
        ,cast(null as double) as avg_exp_price
        ,cast(null as double) as l2d_uv
    from dws.dws_order o
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name = p.target_city
    group by 1,2,3
)
,match_summary as (
    select '货-供给类型' as view_type
        ,supply_type as dim_name
        ,'供给结构' as dim_sub_name
        ,count(distinct house_id) as supply_house_cnt
        ,count(distinct case when lpv > 0 then house_id end) as exposed_house_cnt
        ,sum(lpv) as lpv
        ,sum(luv) as luv
        ,sum(dpv) as dpv
        ,sum(duv) as duv
        ,sum(paid_order_cnt) as paid_order_cnt
        ,sum(paid_night) as paid_night
        ,sum(paid_gmv) as paid_gmv
        ,round(sum(paid_gmv) / nullif(sum(paid_night),0),2) as adr
        ,round(sum(paid_night) / nullif(sum(paid_order_cnt),0),2) as avg_night_per_order
        ,cast(null as double) as avg_lead_days
        ,round(avg(avg_exp_price),2) as avg_exp_price
        ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    from base
    group by 1,2,3
    union all
    select '货-居室' as view_type
        ,bedroom_bucket as dim_name
        ,'供需匹配' as dim_sub_name
        ,count(distinct house_id) as supply_house_cnt
        ,count(distinct case when lpv > 0 then house_id end) as exposed_house_cnt
        ,sum(lpv) as lpv
        ,sum(luv) as luv
        ,sum(dpv) as dpv
        ,sum(duv) as duv
        ,sum(paid_order_cnt) as paid_order_cnt
        ,sum(paid_night) as paid_night
        ,sum(paid_gmv) as paid_gmv
        ,round(sum(paid_gmv) / nullif(sum(paid_night),0),2) as adr
        ,round(sum(paid_night) / nullif(sum(paid_order_cnt),0),2) as avg_night_per_order
        ,cast(null as double) as avg_lead_days
        ,round(avg(avg_exp_price),2) as avg_exp_price
        ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    from base
    group by 1,2,3
    union all
    select '场-商圈' as view_type
        ,dynamic_business as dim_name
        ,'商圈承接' as dim_sub_name
        ,count(distinct house_id) as supply_house_cnt
        ,count(distinct case when lpv > 0 then house_id end) as exposed_house_cnt
        ,sum(lpv) as lpv
        ,sum(luv) as luv
        ,sum(dpv) as dpv
        ,sum(duv) as duv
        ,sum(paid_order_cnt) as paid_order_cnt
        ,sum(paid_night) as paid_night
        ,sum(paid_gmv) as paid_gmv
        ,round(sum(paid_gmv) / nullif(sum(paid_night),0),2) as adr
        ,round(sum(paid_night) / nullif(sum(paid_order_cnt),0),2) as avg_night_per_order
        ,cast(null as double) as avg_lead_days
        ,round(avg(avg_exp_price),2) as avg_exp_price
        ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    from base
    group by 1,2,3
)
,house_opportunity as (
    select '匹配-高流低单房源' as view_type
        ,concat('house_id=',cast(house_id as string)) as dim_name
        ,concat(dynamic_business,' / ',bedroom_bucket,' / ',supply_type) as dim_sub_name
        ,cast(1 as bigint) as supply_house_cnt
        ,case when lpv > 0 then cast(1 as bigint) else cast(0 as bigint) end as exposed_house_cnt
        ,lpv
        ,luv
        ,dpv
        ,duv
        ,paid_order_cnt
        ,paid_night
        ,paid_gmv
        ,round(paid_gmv / nullif(paid_night,0),2) as adr
        ,round(paid_night / nullif(paid_order_cnt,0),2) as avg_night_per_order
        ,cast(null as double) as avg_lead_days
        ,round(avg_exp_price,2) as avg_exp_price
        ,round(duv / nullif(luv,0),4) as l2d_uv
    from (
        select b.*
            ,row_number() over(order by paid_order_cnt asc, lpv desc, duv desc) as rn
        from base b
        where b.lpv >= 100
          and b.paid_order_cnt <= 1
    ) x
    where rn <= 50
)
select *
from order_persona
union all
select *
from match_summary
union all
select *
from house_opportunity
order by view_type, paid_night desc, lpv desc
;
