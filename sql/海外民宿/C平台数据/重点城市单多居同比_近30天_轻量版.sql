-- 重点海外城市：单/多居变化（今年近30天 vs 去年同期）
-- 单居=1居；多居=2居及以上；无法关联 bedroom_count 的订单归为未知居室。

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
,params as (
    select date_sub(current_date,30) as this_start_dt
        ,date_sub(current_date,1) as this_end_dt
        ,add_months(date_sub(current_date,30),-12) as last_start_dt
        ,add_months(date_sub(current_date,1),-12) as last_end_dt
)
,order_small as (
    select case when o.create_date between p.this_start_dt and p.this_end_dt then '今年近30天' else '去年同期30天' end as period
        ,o.city_name
        ,o.house_id
        ,o.order_no
        ,o.user_id
        ,cast(o.order_room_night_count as double) as night
        ,cast(o.room_total_amount as double) as gmv
        ,datediff(o.checkin_date,o.create_date) as lead_days
    from dws.dws_order o
    cross join params p
    where (
        o.create_date between p.this_start_dt and p.this_end_dt
        or o.create_date between p.last_start_dt and p.last_end_dt
      )
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name in (select city_name from city_list)
      and (o.terminal_type_name in ('本站-APP','携程-APP','去哪儿-APP') or o.terminal_type_name like '%小程序%')
)
,house_base as (
    select h.house_id
        ,case when h.bedroom_count = 1 then '单居'
            when h.bedroom_count >= 2 then '多居'
            else '未知居室' end as room_type_bucket
    from dws.dws_house_d h
    cross join params p
    where h.dt = p.this_end_dt
      and h.house_is_oversea = 1
      and h.house_id in (select distinct house_id from order_small)
)
,base as (
    select o.*
        ,nvl(h.room_type_bucket,'未知居室') as room_type_bucket
    from order_small o
    left join house_base h
      on o.house_id = h.house_id
)
,agg as (
    select period
        ,city_name
        ,'整体' as segment_type
        ,'整体' as segment_value
        ,count(distinct order_no) as order_cnt
        ,count(distinct user_id) as user_cnt
        ,sum(night) as night
        ,round(sum(gmv),2) as gmv
        ,round(sum(gmv) / nullif(sum(night),0),2) as adr
        ,round(sum(night) / nullif(count(distinct order_no),0),2) as avg_night_per_order
        ,round(avg(lead_days),2) as avg_lead_days
    from base
    group by 1,2,3,4
    union all
    select period
        ,city_name
        ,'单多居' as segment_type
        ,room_type_bucket as segment_value
        ,count(distinct order_no) as order_cnt
        ,count(distinct user_id) as user_cnt
        ,sum(night) as night
        ,round(sum(gmv),2) as gmv
        ,round(sum(gmv) / nullif(sum(night),0),2) as adr
        ,round(sum(night) / nullif(count(distinct order_no),0),2) as avg_night_per_order
        ,round(avg(lead_days),2) as avg_lead_days
    from base
    group by 1,2,3,4
)
select a.*
    ,round(a.order_cnt / nullif(t.order_cnt,0),4) as order_share
    ,round(a.night / nullif(t.night,0),4) as night_share
    ,round(a.gmv / nullif(t.gmv,0),4) as gmv_share
from agg a
left join agg t
  on a.period = t.period
 and a.city_name = t.city_name
 and t.segment_type = '整体'
 and t.segment_value = '整体'
order by city_name, segment_type, segment_value, period
;
