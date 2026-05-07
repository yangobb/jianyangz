-- 新加坡：基于 dws_house_d.house_type 追溯主要曝光房型、主要成单房型及商圈/价格聚集
-- 口径：
--   供给：dws.dws_house_d 最新分区，在线海外房源，house_type 为房屋类型。
--   曝光：dws.dws_path_ldbo_d 近14天，海外、用户、APP、前台展示、非推荐流量，按 house_id 归因到房屋类型。
--   订单：dws.dws_order 近30天，支付未取消海外订单，按 house_id 归因到房屋类型。
--   目标城市：新加坡。

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
        ,nvl(h.house_type,'未知房屋类型') as house_type
        ,case when h.bedroom_count = 1 then '1居'
            when h.bedroom_count = 2 then '2居'
            when h.bedroom_count >= 3 then '3居及以上'
            else '未知居室' end as bedroom_bucket
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
        ,sum(nvl(a.without_risk_access_order_num,0)) as attr_order_cnt
        ,sum(nvl(a.without_risk_access_order_room_night,0)) as attr_night
        ,sum(nvl(a.without_risk_access_order_gmv,0)) as attr_gmv
        ,avg(case when a.final_price > 0 then cast(a.final_price as double) end) as avg_exp_price
        ,case when avg(case when a.final_price > 0 then cast(a.final_price as double) end) <= 500 then '0-500'
            when avg(case when a.final_price > 0 then cast(a.final_price as double) end) <= 800 then '500-800'
            when avg(case when a.final_price > 0 then cast(a.final_price as double) end) <= 1200 then '800-1200'
            when avg(case when a.final_price > 0 then cast(a.final_price as double) end) <= 1800 then '1200-1800'
            when avg(case when a.final_price > 0 then cast(a.final_price as double) end) <= 3000 then '1800-3000'
            else '3000+' end as exp_price_bucket
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
      and cast(a.final_price as double) > 0
    group by 1
)
,order_house as (
    select o.house_id
        ,count(distinct o.order_no) as paid_order_cnt
        ,sum(cast(o.order_room_night_count as double)) as paid_night
        ,sum(cast(o.room_total_amount as double)) as paid_gmv
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
    select h.house_id
        ,h.hotel_id
        ,h.city_name
        ,h.dynamic_business
        ,h.house_type
        ,h.bedroom_bucket
        ,nvl(f.lpv,0) as lpv
        ,nvl(f.luv,0) as luv
        ,nvl(f.dpv,0) as dpv
        ,nvl(f.duv,0) as duv
        ,nvl(f.attr_order_cnt,0) as attr_order_cnt
        ,nvl(f.attr_night,0) as attr_night
        ,nvl(f.attr_gmv,0) as attr_gmv
        ,f.avg_exp_price
        ,nvl(f.exp_price_bucket,'无曝光') as exp_price_bucket
        ,nvl(o.paid_order_cnt,0) as paid_order_cnt
        ,nvl(o.paid_night,0) as paid_night
        ,nvl(o.paid_gmv,0) as paid_gmv
    from house_base h
    left join flow_house f
      on h.house_id = f.house_id
    left join order_house o
      on h.house_id = o.house_id
)
select '房屋类型-整体' as view_type
    ,house_type
    ,'整体' as segment
    ,count(distinct house_id) as supply_house_cnt
    ,count(distinct case when lpv > 0 then house_id end) as exposed_house_cnt
    ,sum(lpv) as lpv
    ,sum(luv) as luv
    ,sum(dpv) as dpv
    ,sum(duv) as duv
    ,sum(paid_order_cnt) as paid_order_cnt
    ,sum(paid_night) as paid_night
    ,round(sum(paid_gmv),2) as paid_gmv
    ,round(sum(paid_gmv) / nullif(sum(paid_night),0),2) as adr
    ,round(sum(paid_night) / nullif(sum(paid_order_cnt),0),2) as avg_night_per_order
    ,round(avg(avg_exp_price),2) as avg_exp_price
    ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    ,round(sum(paid_night) / nullif(sum(luv),0) * 1000,2) as paid_night_per_1000_luv
from base
group by 1,2,3
union all
select '房屋类型-商圈' as view_type
    ,house_type
    ,dynamic_business as segment
    ,count(distinct house_id) as supply_house_cnt
    ,count(distinct case when lpv > 0 then house_id end) as exposed_house_cnt
    ,sum(lpv) as lpv
    ,sum(luv) as luv
    ,sum(dpv) as dpv
    ,sum(duv) as duv
    ,sum(paid_order_cnt) as paid_order_cnt
    ,sum(paid_night) as paid_night
    ,round(sum(paid_gmv),2) as paid_gmv
    ,round(sum(paid_gmv) / nullif(sum(paid_night),0),2) as adr
    ,round(sum(paid_night) / nullif(sum(paid_order_cnt),0),2) as avg_night_per_order
    ,round(avg(avg_exp_price),2) as avg_exp_price
    ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    ,round(sum(paid_night) / nullif(sum(luv),0) * 1000,2) as paid_night_per_1000_luv
from base
where lpv > 0 or paid_order_cnt > 0
group by 1,2,3
union all
select '房屋类型-曝光价带' as view_type
    ,house_type
    ,exp_price_bucket as segment
    ,count(distinct house_id) as supply_house_cnt
    ,count(distinct case when lpv > 0 then house_id end) as exposed_house_cnt
    ,sum(lpv) as lpv
    ,sum(luv) as luv
    ,sum(dpv) as dpv
    ,sum(duv) as duv
    ,sum(paid_order_cnt) as paid_order_cnt
    ,sum(paid_night) as paid_night
    ,round(sum(paid_gmv),2) as paid_gmv
    ,round(sum(paid_gmv) / nullif(sum(paid_night),0),2) as adr
    ,round(sum(paid_night) / nullif(sum(paid_order_cnt),0),2) as avg_night_per_order
    ,round(avg(avg_exp_price),2) as avg_exp_price
    ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    ,round(sum(paid_night) / nullif(sum(luv),0) * 1000,2) as paid_night_per_1000_luv
from base
where lpv > 0 or paid_order_cnt > 0
group by 1,2,3
order by view_type, paid_night desc, luv desc
;
