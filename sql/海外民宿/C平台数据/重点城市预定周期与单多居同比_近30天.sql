-- 重点海外城市：用户预定周期与单/多居变化（今年近30天 vs 去年同期）
-- 口径：
--   1. 途家订单：dws.dws_order 支付未取消海外订单。
--   2. 时间窗：今年近30天 = current_date-30 至 current_date-1；去年同期 = 对应日期减 1 年。
--   3. 预定周期：datediff(checkin_date, create_date)，按 T0/T1-7/T8-14/T15-30/T31+ 分桶。
--   4. 单多居：订单 house_id 关联 dws_house_d 最新分区的 bedroom_count；1居=单居，2居及以上=多居。
--   5. 输出聚合，不输出用户明细。

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
,house_base as (
    select h.house_id
        ,case when h.bedroom_count = 1 then '单居'
            when h.bedroom_count >= 2 then '多居'
            else '未知居室' end as room_type_bucket
    from dws.dws_house_d h
    cross join params p
    where h.dt = p.this_end_dt
      and h.house_is_oversea = 1
)
,order_base as (
    select case when o.create_date between p.this_start_dt and p.this_end_dt then '今年近30天'
            else '去年同期30天' end as period
        ,o.city_name
        ,o.order_no
        ,o.user_id
        ,cast(o.order_room_night_count as double) as night
        ,cast(o.room_total_amount as double) as gmv
        ,datediff(o.checkin_date,o.create_date) as lead_days
        ,case when datediff(o.checkin_date,o.create_date) = 0 then 'T0'
            when datediff(o.checkin_date,o.create_date) between 1 and 7 then 'T1-7'
            when datediff(o.checkin_date,o.create_date) between 8 and 14 then 'T8-14'
            when datediff(o.checkin_date,o.create_date) between 15 and 30 then 'T15-30'
            else 'T31+' end as lead_bucket
        ,nvl(h.room_type_bucket,'未知居室') as room_type_bucket
    from dws.dws_order o
    cross join params p
    left join house_base h
      on o.house_id = h.house_id
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
    from order_base
    group by 1,2,3,4
    union all
    select period
        ,city_name
        ,'预定周期' as segment_type
        ,lead_bucket as segment_value
        ,count(distinct order_no) as order_cnt
        ,count(distinct user_id) as user_cnt
        ,sum(night) as night
        ,round(sum(gmv),2) as gmv
        ,round(sum(gmv) / nullif(sum(night),0),2) as adr
        ,round(sum(night) / nullif(count(distinct order_no),0),2) as avg_night_per_order
        ,round(avg(lead_days),2) as avg_lead_days
    from order_base
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
    from order_base
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
