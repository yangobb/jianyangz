-- 新加坡：半年内途家民宿与携程酒店复购用户画像（轻量版）
-- 复购用户：近180天同一用户在新加坡同一端下单 >=2 次。
-- 输出聚合，不输出用户明细。

with params as (
    select date_sub(current_date,180) as order_start_dt
        ,date_sub(current_date,1) as end_dt
        ,'新加坡' as target_city
)
,house_base as (
    select h.house_id
        ,h.hotel_id
        ,nvl(h.dynamic_business,'未知商圈') as dynamic_business
        ,nvl(h.house_type,'未知房屋类型') as house_type
    from dws.dws_house_d h
    cross join params p
    where h.dt = p.end_dt
      and h.house_is_oversea = 1
      and h.house_city_name = p.target_city
)
,tj_order_raw as (
    select '途家民宿' as platform
        ,cast(o.user_id as string) as tujia_user_id
        ,cast(o.order_no as string) as order_id
        ,cast(o.hotel_id as string) as hotel_key
        ,nvl(h.dynamic_business,'未知商圈') as business_area
        ,nvl(h.house_type,'未知房屋类型') as house_type
        ,case when cast(o.room_total_amount as double) / nullif(cast(o.order_room_night_count as double),0) <= 500 then '0-500'
            when cast(o.room_total_amount as double) / nullif(cast(o.order_room_night_count as double),0) <= 800 then '500-800'
            when cast(o.room_total_amount as double) / nullif(cast(o.order_room_night_count as double),0) <= 1200 then '800-1200'
            when cast(o.room_total_amount as double) / nullif(cast(o.order_room_night_count as double),0) <= 1800 then '1200-1800'
            when cast(o.room_total_amount as double) / nullif(cast(o.order_room_night_count as double),0) <= 3000 then '1800-3000'
            else '3000+' end as adr_bucket
        ,cast(o.order_room_night_count as double) as room_night
        ,cast(o.room_total_amount as double) as gmv
        ,to_date(o.create_date) as order_date
    from dws.dws_order o
    left join house_base h
      on o.house_id = h.house_id
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name = p.target_city
      and nvl(o.user_id,0) <> 0
)
,ct_order_raw as (
    select '携程酒店' as platform
        ,cast(m.member_id as string) as tujia_user_id
        ,cast(o.orderid as string) as order_id
        ,cast(o.masterhotelid as string) as hotel_key
        ,nvl(d.zonename,'未知商圈') as business_area
        ,nvl(d.tagname1,'标准酒店') as house_type
        ,case when cast(o.ciireceivable as double) / nullif(cast(o.ciiquantity as double),0) <= 500 then '0-500'
            when cast(o.ciireceivable as double) / nullif(cast(o.ciiquantity as double),0) <= 800 then '500-800'
            when cast(o.ciireceivable as double) / nullif(cast(o.ciiquantity as double),0) <= 1200 then '800-1200'
            when cast(o.ciireceivable as double) / nullif(cast(o.ciiquantity as double),0) <= 1800 then '1200-1800'
            when cast(o.ciireceivable as double) / nullif(cast(o.ciiquantity as double),0) <= 3000 then '1800-3000'
            else '3000+' end as adr_bucket
        ,cast(o.ciiquantity as double) as room_night
        ,cast(o.ciireceivable as double) as gmv
        ,to_date(o.orderdate) as order_date
    from app_ctrip.edw_htl_order_all_split o
    inner join ods_tujia_member.third_user_mapping m
      on lower(cast(o.uid as string)) = lower(cast(m.third_id as string))
     and m.channel_code = 'CtripId'
    left join app_ctrip.dimmasterhotel d
      on o.masterhotelid = d.masterhotelid
     and d.d = date_sub(current_date,2)
    cross join params p
    where o.d = p.end_dt
      and to_date(o.orderdate) between p.order_start_dt and p.end_dt
      and o.orderstatus in ('S','P')
      and o.ordertype = 2
      and o.cityname = p.target_city
      and o.uid is not null
      and cast(o.uid as string) <> ''
)
,order_raw as (
    select * from tj_order_raw
    union all
    select * from ct_order_raw
)
,repeat_user as (
    select platform
        ,tujia_user_id
        ,count(distinct order_id) as order_cnt
        ,count(distinct hotel_key) as hotel_cnt
        ,count(distinct business_area) as business_cnt
        ,sum(room_night) as room_night
        ,sum(gmv) as gmv
        ,round(sum(gmv) / nullif(sum(room_night),0),2) as adr
        ,round(sum(room_night) / nullif(count(distinct order_id),0),2) as avg_night_per_order
        ,datediff(max(order_date),min(order_date)) as repeat_span_days
        ,case when count(distinct hotel_key) = 1 then '同一家酒店/房源' else '不同酒店/房源' end as same_hotel_flag
        ,case when count(distinct business_area) = 1 then '同一商圈' else '跨商圈' end as same_business_flag
        ,case when round(sum(room_night) / nullif(count(distinct order_id),0),2) >= 7 then '商务/长住'
            when count(distinct hotel_key) = 1 and count(distinct business_area) = 1 then '固定目的地复访'
            when sum(gmv) / nullif(sum(room_night),0) <= 500 then '低价高频短住'
            when sum(gmv) / nullif(sum(room_night),0) >= 1500 then '高端品质复访'
            else '常规多次出行' end as inferred_intent
    from order_raw
    group by 1,2
    having count(distinct order_id) >= 2
)
,repeat_order as (
    select o.*
        ,r.same_hotel_flag
        ,r.same_business_flag
        ,r.inferred_intent
    from order_raw o
    inner join repeat_user r
      on o.platform = r.platform
     and o.tujia_user_id = r.tujia_user_id
)
select '复购用户-意图' as view_type
    ,platform
    ,inferred_intent as segment_1
    ,concat(same_hotel_flag,' / ',same_business_flag) as segment_2
    ,count(distinct tujia_user_id) as user_cnt
    ,sum(order_cnt) as order_cnt
    ,sum(room_night) as room_night
    ,round(sum(gmv),2) as gmv
    ,round(sum(gmv) / nullif(sum(room_night),0),2) as adr
    ,round(avg(order_cnt),2) as avg_order_cnt_per_user
    ,round(avg(avg_night_per_order),2) as avg_night_per_order
    ,round(avg(repeat_span_days),2) as avg_repeat_span_days
from repeat_user
group by 1,2,3,4
union all
select '复购订单-商圈房型价带' as view_type
    ,platform
    ,inferred_intent as segment_1
    ,concat(business_area,' / ',house_type,' / ',adr_bucket) as segment_2
    ,count(distinct tujia_user_id) as user_cnt
    ,count(distinct order_id) as order_cnt
    ,sum(room_night) as room_night
    ,round(sum(gmv),2) as gmv
    ,round(sum(gmv) / nullif(sum(room_night),0),2) as adr
    ,cast(null as double) as avg_order_cnt_per_user
    ,round(sum(room_night) / nullif(count(distinct order_id),0),2) as avg_night_per_order
    ,cast(null as double) as avg_repeat_span_days
from repeat_order
group by 1,2,3,4
having count(distinct tujia_user_id) >= 2 or count(distinct order_id) >= 3
order by view_type, platform, user_cnt desc, room_night desc
;
