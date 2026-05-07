-- 新加坡：半年内复购表现、同店/商圈聚集、用户意图推断及相似未下单用户池
-- 口径：
--   1. 复购用户：近180天同一用户在新加坡同一端下单 >=2 次。
--   2. 途家端：dws.dws_order 支付未取消海外订单，酒店用 hotel_id，商圈/房屋类型补 dws_house_d。
--   3. 携程端：app_ctrip.edw_htl_order_all_split 支付订单，用户经 third_user_mapping(CtripId) 映射到途家 user_id，酒店用 masterhotelid，商圈取 app_ctrip.dimmasterhotel.zonename。
--   4. 相似未下单用户：近14天新加坡曝光用户，浏览过复购用户高频商圈/房型/价带，但近180天未在新加坡下单。
--   5. 本 SQL 输出聚合结果，不输出用户明细。

with params as (
    select date_sub(current_date,180) as order_start_dt
        ,date_sub(current_date,14) as flow_start_dt
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
        ,min(order_date) as first_order_date
        ,max(order_date) as last_order_date
        ,datediff(max(order_date),min(order_date)) as repeat_span_days
        ,case when count(distinct hotel_key) = 1 then '同一家酒店/房源'
            else '不同酒店/房源' end as same_hotel_flag
        ,case when count(distinct business_area) = 1 then '同一商圈'
            else '跨商圈' end as same_business_flag
        ,case when round(sum(room_night) / nullif(count(distinct order_id),0),2) >= 7 then '商务/长住'
            when count(distinct hotel_key) = 1 and count(distinct business_area) = 1 then '固定目的地复访'
            when sum(gmv) / nullif(sum(room_night),0) <= 500 then '低价高频短住'
            when sum(gmv) / nullif(sum(room_night),0) >= 1500 then '高端品质复访'
            else '常规多次出行' end as inferred_intent
    from order_raw
    group by 1,2
    having count(distinct order_id) >= 2
)
,repeat_order_enriched as (
    select o.*
        ,r.same_hotel_flag
        ,r.same_business_flag
        ,r.inferred_intent
    from order_raw o
    inner join repeat_user r
      on o.platform = r.platform
     and o.tujia_user_id = r.tujia_user_id
)
,repeat_segment as (
    select platform
        ,inferred_intent
        ,business_area
        ,house_type
        ,adr_bucket
        ,count(distinct tujia_user_id) as repeat_user_cnt
        ,count(distinct order_id) as repeat_order_cnt
        ,sum(room_night) as repeat_night
        ,sum(gmv) as repeat_gmv
        ,round(sum(gmv) / nullif(sum(room_night),0),2) as repeat_adr
        ,round(sum(room_night) / nullif(count(distinct order_id),0),2) as repeat_avg_night_per_order
    from repeat_order_enriched
    group by 1,2,3,4,5
)
,ordered_user_180d as (
    select distinct tujia_user_id
    from order_raw
)
,repeat_user_summary as (
    select platform
        ,inferred_intent
        ,same_hotel_flag
        ,same_business_flag
        ,count(distinct tujia_user_id) as repeat_user_cnt
        ,sum(order_cnt) as order_cnt
        ,sum(room_night) as room_night
        ,round(sum(gmv),2) as gmv
        ,round(sum(gmv) / nullif(sum(room_night),0),2) as adr
        ,round(avg(order_cnt),2) as avg_order_cnt_per_user
        ,round(avg(avg_night_per_order),2) as avg_night_per_order
        ,round(avg(repeat_span_days),2) as avg_repeat_span_days
    from repeat_user
    group by 1,2,3,4
)
,tj_flow_user_segment as (
    select '途家民宿' as platform
        ,cast(a.user_id as string) as tujia_user_id
        ,nvl(h.dynamic_business,'未知商圈') as business_area
        ,nvl(h.house_type,'未知房屋类型') as house_type
        ,case when cast(a.final_price as double) <= 500 then '0-500'
            when cast(a.final_price as double) <= 800 then '500-800'
            when cast(a.final_price as double) <= 1200 then '800-1200'
            when cast(a.final_price as double) <= 1800 then '1200-1800'
            when cast(a.final_price as double) <= 3000 then '1800-3000'
            else '3000+' end as adr_bucket
        ,count(1) as lpv
        ,count(distinct concat(a.dt,'|',a.uid)) as luv
        ,count(case when a.detail_uid is not null then 1 end) as dpv
        ,count(distinct case when a.detail_uid is not null then concat(a.dt,'|',a.detail_uid) end) as duv
        ,avg(cast(a.final_price as double)) as avg_exp_price
    from dws.dws_path_ldbo_d a
    left join house_base h
      on a.house_id = h.house_id
    cross join params p
    where a.dt between p.flow_start_dt and p.end_dt
      and a.city_name = p.target_city
      and a.is_oversea = 1
      and a.user_type = '用户'
      and a.wrapper_name in ('携程','去哪儿','途家')
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.is_recommend = 0
      and cast(a.final_price as double) > 0
      and nvl(a.user_id,0) <> 0
    group by 1,2,3,4,5
)
,ct_flow_user_segment as (
    select '携程酒店' as platform
        ,cast(m.member_id as string) as tujia_user_id
        ,nvl(d.zonename,'未知商圈') as business_area
        ,nvl(d.tagname1,'标准酒店') as house_type
        ,case when cast(f.fh_price as double) <= 500 then '0-500'
            when cast(f.fh_price as double) <= 800 then '500-800'
            when cast(f.fh_price as double) <= 1200 then '800-1200'
            when cast(f.fh_price as double) <= 1800 then '1200-1800'
            when cast(f.fh_price as double) <= 3000 then '1800-3000'
            else '3000+' end as adr_bucket
        ,count(1) as lpv
        ,count(distinct concat(f.d,'|',f.uid)) as luv
        ,count(case when f.is_has_click = 1 then 1 end) as dpv
        ,count(distinct case when f.is_has_click = 1 then concat(f.d,'|',f.uid) end) as duv
        ,avg(cast(f.fh_price as double)) as avg_exp_price
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day f
    inner join ods_tujia_member.third_user_mapping m
      on lower(cast(f.uid as string)) = lower(cast(m.third_id as string))
     and m.channel_code = 'CtripId'
    left join app_ctrip.dimmasterhotel d
      on f.masterhotelid = d.masterhotelid
     and d.d = date_sub(current_date,2)
    cross join params p
    where f.d between p.flow_start_dt and p.end_dt
      and d.cityname = p.target_city
      and cast(f.fh_price as double) > 0
      and f.uid is not null
      and cast(f.uid as string) <> ''
    group by 1,2,3,4,5
)
,flow_user_segment as (
    select * from tj_flow_user_segment
    union all
    select * from ct_flow_user_segment
)
,similar_unordered as (
    select f.platform
        ,s.inferred_intent
        ,f.business_area
        ,f.house_type
        ,f.adr_bucket
        ,count(distinct f.tujia_user_id) as similar_unordered_user_cnt
        ,sum(f.lpv) as similar_lpv
        ,sum(f.luv) as similar_luv
        ,sum(f.dpv) as similar_dpv
        ,sum(f.duv) as similar_duv
        ,round(avg(f.avg_exp_price),2) as similar_avg_exp_price
    from flow_user_segment f
    inner join (
        select platform
            ,inferred_intent
            ,business_area
            ,house_type
            ,adr_bucket
        from repeat_segment
        where repeat_user_cnt >= 2 or repeat_order_cnt >= 3
        group by 1,2,3,4,5
    ) s
      on f.platform = s.platform
     and f.business_area = s.business_area
     and f.house_type = s.house_type
     and f.adr_bucket = s.adr_bucket
    left join ordered_user_180d o
      on f.tujia_user_id = o.tujia_user_id
    where o.tujia_user_id is null
    group by 1,2,3,4,5
)
select '复购用户-意图' as view_type
    ,platform
    ,inferred_intent as segment_1
    ,concat(same_hotel_flag,' / ',same_business_flag) as segment_2
    ,repeat_user_cnt
    ,order_cnt
    ,room_night
    ,gmv
    ,adr
    ,avg_order_cnt_per_user
    ,avg_night_per_order
    ,avg_repeat_span_days
    ,cast(null as bigint) as similar_unordered_user_cnt
    ,cast(null as bigint) as similar_luv
    ,cast(null as bigint) as similar_duv
    ,cast(null as double) as similar_avg_exp_price
from repeat_user_summary
union all
select '复购订单-商圈房型价带' as view_type
    ,platform
    ,inferred_intent as segment_1
    ,concat(business_area,' / ',house_type,' / ',adr_bucket) as segment_2
    ,repeat_user_cnt
    ,repeat_order_cnt as order_cnt
    ,repeat_night as room_night
    ,round(repeat_gmv,2) as gmv
    ,repeat_adr as adr
    ,cast(null as double) as avg_order_cnt_per_user
    ,repeat_avg_night_per_order as avg_night_per_order
    ,cast(null as double) as avg_repeat_span_days
    ,cast(null as bigint) as similar_unordered_user_cnt
    ,cast(null as bigint) as similar_luv
    ,cast(null as bigint) as similar_duv
    ,cast(null as double) as similar_avg_exp_price
from repeat_segment
where repeat_user_cnt >= 2 or repeat_order_cnt >= 3
union all
select '相似未下单-可触达池' as view_type
    ,platform
    ,inferred_intent as segment_1
    ,concat(business_area,' / ',house_type,' / ',adr_bucket) as segment_2
    ,cast(null as bigint) as repeat_user_cnt
    ,cast(null as bigint) as order_cnt
    ,cast(null as double) as room_night
    ,cast(null as double) as gmv
    ,cast(null as double) as adr
    ,cast(null as double) as avg_order_cnt_per_user
    ,cast(null as double) as avg_night_per_order
    ,cast(null as double) as avg_repeat_span_days
    ,similar_unordered_user_cnt
    ,similar_luv
    ,similar_duv
    ,similar_avg_exp_price
from similar_unordered
order by view_type, platform, repeat_user_cnt desc, similar_unordered_user_cnt desc
;
