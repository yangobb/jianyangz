-- 新加坡：携程酒店宫格与途家民宿均曝光用户的曝光价与下单 ADR 对比
-- 口径：
--   1. 携程酒店曝光：app_ctrip.cdm_traf_ht_ctrip_list_qid_day，近14天，新加坡，fh_price > 0。
--   2. 途家民宿曝光：dws.dws_path_ldbo_d，近14天，新加坡，携程 APP 入口、前台展示、非推荐、海外用户，final_price > 0。
--   3. 用户交集：携程酒店 uid 经 ods_tujia_member.third_user_mapping(channel_code='CtripId') 映射到途家 user_id 后，与途家民宿 user_id 取交集。
--   4. 订单对比：近30天支付未取消。携程酒店订单取 app_ctrip.edw_htl_order_all_split；途家民宿订单取 dws.dws_order。
--   5. 本 SQL 输出聚合结果，不输出用户明细。

with params as (
    select date_sub(current_date,14) as flow_start_dt
        ,date_sub(current_date,1) as end_dt
        ,date_sub(current_date,30) as order_start_dt
        ,'新加坡' as target_city
)
,tj_homestay_exp_user as (
    select lower(cast(a.user_id as string)) as tujia_user_id
        ,count(1) as tj_homestay_lpv
        ,count(distinct concat_ws('|',cast(a.dt as string),cast(a.uid as string))) as tj_homestay_luv
        ,count(case when a.detail_uid is not null then 1 end) as tj_homestay_dpv
        ,count(distinct case when a.detail_uid is not null then concat_ws('|',cast(a.dt as string),cast(a.detail_uid as string)) end) as tj_homestay_duv
        ,sum(nvl(a.without_risk_access_order_num,0)) as tj_attr_order_cnt
        ,sum(nvl(a.without_risk_access_order_room_night,0)) as tj_attr_night
        ,sum(nvl(a.without_risk_access_order_gmv,0)) as tj_attr_gmv
        ,avg(cast(a.final_price as double)) as tj_homestay_avg_exp_price
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.flow_start_dt and p.end_dt
      and a.city_name = p.target_city
      and a.is_oversea = 1
      and a.user_type = '用户'
      and a.wrapper_name = '携程'
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.is_recommend = 0
      and cast(a.final_price as double) > 0
      and nvl(a.user_id,0) <> 0
    group by 1
)
,mapped_tj_user as (
    select t.tujia_user_id
        ,lower(cast(m.third_id as string)) as ctrip_user_id
        ,t.tj_homestay_lpv
        ,t.tj_homestay_luv
        ,t.tj_homestay_dpv
        ,t.tj_homestay_duv
        ,t.tj_attr_order_cnt
        ,t.tj_attr_night
        ,t.tj_attr_gmv
        ,t.tj_homestay_avg_exp_price
    from tj_homestay_exp_user t
    inner join ods_tujia_member.third_user_mapping m
      on t.tujia_user_id = lower(cast(m.member_id as string))
     and m.channel_code = 'CtripId'
    where m.third_id is not null
      and cast(m.third_id as string) <> ''
)
,ct_hotel_exp_user as (
    select m.tujia_user_id
        ,m.ctrip_user_id
        ,m.tj_homestay_lpv
        ,m.tj_homestay_luv
        ,m.tj_homestay_dpv
        ,m.tj_homestay_duv
        ,m.tj_attr_order_cnt
        ,m.tj_attr_night
        ,m.tj_attr_gmv
        ,m.tj_homestay_avg_exp_price
        ,count(1) as ct_hotel_lpv
        ,count(distinct concat_ws('|',cast(t1.d as string),cast(t1.uid as string))) as ct_hotel_luv
        ,count(case when t1.is_has_click = 1 then 1 end) as ct_hotel_dpv
        ,count(distinct case when t1.is_has_click = 1 then concat_ws('|',cast(t1.d as string),cast(t1.uid as string)) end) as ct_hotel_duv
        ,avg(cast(t1.fh_price as double)) as ct_hotel_avg_exp_price
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day t1
    inner join excel_upload.dim_ctrip_list_qid_city t2
      on t1.m_city = t2.m_city
    inner join mapped_tj_user m
      on lower(cast(t1.uid as string)) = m.ctrip_user_id
    cross join params p
    where t1.d between p.flow_start_dt and p.end_dt
      and t2.cityname = p.target_city
      and cast(t1.fh_price as double) > 0
      and t1.uid is not null
      and cast(t1.uid as string) <> ''
    group by 1,2,3,4,5,6,7,8,9,10
)
,overlap_user as (
    select c.tujia_user_id
        ,c.ctrip_user_id
        ,c.ct_hotel_lpv
        ,c.ct_hotel_luv
        ,c.ct_hotel_dpv
        ,c.ct_hotel_duv
        ,c.ct_hotel_avg_exp_price
        ,c.tj_homestay_lpv
        ,c.tj_homestay_luv
        ,c.tj_homestay_dpv
        ,c.tj_homestay_duv
        ,c.tj_attr_order_cnt
        ,c.tj_attr_night
        ,c.tj_attr_gmv
        ,c.tj_homestay_avg_exp_price
        ,case
            when c.tj_homestay_avg_exp_price <= c.ct_hotel_avg_exp_price * 0.70 then '途家曝光价低30%+'
            when c.tj_homestay_avg_exp_price <= c.ct_hotel_avg_exp_price * 0.90 then '途家曝光价低10%-30%'
            when c.tj_homestay_avg_exp_price <= c.ct_hotel_avg_exp_price * 1.10 then '两端曝光价接近'
            when c.tj_homestay_avg_exp_price <= c.ct_hotel_avg_exp_price * 1.30 then '途家曝光价高10%-30%'
            else '途家曝光价高30%+'
          end as exp_price_relation
    from ct_hotel_exp_user c
)
,ct_hotel_order_user as (
    select u.tujia_user_id
        ,count(distinct o.orderid) as ct_hotel_order_cnt
        ,sum(cast(o.ciiquantity as double)) as ct_hotel_night
        ,sum(cast(o.ciireceivable as double)) as ct_hotel_gmv
    from app_ctrip.edw_htl_order_all_split o
    inner join excel_upload.dim_ctrip_list_qid_city city
      on o.cityid = city.m_city
    inner join overlap_user u
      on lower(cast(o.uid as string)) = u.ctrip_user_id
    cross join params p
    where o.d = p.end_dt
      and to_date(o.orderdate) between p.order_start_dt and p.end_dt
      and o.orderstatus in ('S','P')
      and o.ordertype = 2
      and city.cityname = p.target_city
      and o.uid is not null
      and cast(o.uid as string) <> ''
    group by 1
)
,tj_homestay_order_user as (
    select lower(cast(o.user_id as string)) as tujia_user_id
        ,count(distinct o.order_no) as tj_homestay_order_cnt
        ,sum(cast(o.order_room_night_count as double)) as tj_homestay_night
        ,sum(cast(o.room_total_amount as double)) as tj_homestay_gmv
    from dws.dws_order o
    inner join overlap_user u
      on lower(cast(o.user_id as string)) = u.tujia_user_id
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name = p.target_city
      and nvl(o.user_id,0) <> 0
    group by 1
)
,user_base as (
    select u.*
        ,nvl(co.ct_hotel_order_cnt,0) as ct_hotel_order_cnt
        ,nvl(co.ct_hotel_night,0) as ct_hotel_night
        ,nvl(co.ct_hotel_gmv,0) as ct_hotel_gmv
        ,nvl(toh.tj_homestay_order_cnt,0) as tj_homestay_order_cnt
        ,nvl(toh.tj_homestay_night,0) as tj_homestay_night
        ,nvl(toh.tj_homestay_gmv,0) as tj_homestay_gmv
        ,case
            when nvl(co.ct_hotel_order_cnt,0) > 0 and nvl(toh.tj_homestay_order_cnt,0) > 0 then '两端都下单'
            when nvl(co.ct_hotel_order_cnt,0) > 0 then '只下携程酒店'
            when nvl(toh.tj_homestay_order_cnt,0) > 0 then '只下途家民宿'
            else '两端均未下单'
          end as order_result
    from overlap_user u
    left join ct_hotel_order_user co
      on u.tujia_user_id = co.tujia_user_id
    left join tj_homestay_order_user toh
      on u.tujia_user_id = toh.tujia_user_id
)
select '整体' as view_type
    ,'交叉曝光用户' as segment
    ,count(distinct tujia_user_id) as overlap_user_cnt
    ,sum(ct_hotel_lpv) as ct_hotel_lpv
    ,sum(tj_homestay_lpv) as tj_homestay_lpv
    ,round(avg(ct_hotel_avg_exp_price),2) as ct_hotel_avg_exp_price
    ,round(percentile(cast(ct_hotel_avg_exp_price as bigint),0.50),2) as ct_hotel_p50_user_exp_price
    ,round(avg(tj_homestay_avg_exp_price),2) as tj_homestay_avg_exp_price
    ,round(percentile(cast(tj_homestay_avg_exp_price as bigint),0.50),2) as tj_homestay_p50_user_exp_price
    ,round((avg(tj_homestay_avg_exp_price) - avg(ct_hotel_avg_exp_price)) / nullif(avg(ct_hotel_avg_exp_price),0),4) as tj_vs_ct_exp_price_diff_rate
    ,count(distinct case when ct_hotel_order_cnt > 0 then tujia_user_id end) as ct_hotel_order_user_cnt
    ,count(distinct case when tj_homestay_order_cnt > 0 then tujia_user_id end) as tj_homestay_order_user_cnt
    ,sum(ct_hotel_order_cnt) as ct_hotel_order_cnt
    ,sum(tj_homestay_order_cnt) as tj_homestay_order_cnt
    ,sum(ct_hotel_night) as ct_hotel_night
    ,sum(tj_homestay_night) as tj_homestay_night
    ,round(sum(ct_hotel_gmv),2) as ct_hotel_gmv
    ,round(sum(tj_homestay_gmv),2) as tj_homestay_gmv
    ,round(sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0),2) as ct_hotel_order_adr
    ,round(sum(tj_homestay_gmv) / nullif(sum(tj_homestay_night),0),2) as tj_homestay_order_adr
    ,round((sum(tj_homestay_gmv) / nullif(sum(tj_homestay_night),0) - sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0)) / nullif(sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0),0),4) as tj_vs_ct_order_adr_diff_rate
from user_base
union all
select '曝光价关系' as view_type
    ,exp_price_relation as segment
    ,count(distinct tujia_user_id) as overlap_user_cnt
    ,sum(ct_hotel_lpv) as ct_hotel_lpv
    ,sum(tj_homestay_lpv) as tj_homestay_lpv
    ,round(avg(ct_hotel_avg_exp_price),2) as ct_hotel_avg_exp_price
    ,round(percentile(cast(ct_hotel_avg_exp_price as bigint),0.50),2) as ct_hotel_p50_user_exp_price
    ,round(avg(tj_homestay_avg_exp_price),2) as tj_homestay_avg_exp_price
    ,round(percentile(cast(tj_homestay_avg_exp_price as bigint),0.50),2) as tj_homestay_p50_user_exp_price
    ,round((avg(tj_homestay_avg_exp_price) - avg(ct_hotel_avg_exp_price)) / nullif(avg(ct_hotel_avg_exp_price),0),4) as tj_vs_ct_exp_price_diff_rate
    ,count(distinct case when ct_hotel_order_cnt > 0 then tujia_user_id end) as ct_hotel_order_user_cnt
    ,count(distinct case when tj_homestay_order_cnt > 0 then tujia_user_id end) as tj_homestay_order_user_cnt
    ,sum(ct_hotel_order_cnt) as ct_hotel_order_cnt
    ,sum(tj_homestay_order_cnt) as tj_homestay_order_cnt
    ,sum(ct_hotel_night) as ct_hotel_night
    ,sum(tj_homestay_night) as tj_homestay_night
    ,round(sum(ct_hotel_gmv),2) as ct_hotel_gmv
    ,round(sum(tj_homestay_gmv),2) as tj_homestay_gmv
    ,round(sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0),2) as ct_hotel_order_adr
    ,round(sum(tj_homestay_gmv) / nullif(sum(tj_homestay_night),0),2) as tj_homestay_order_adr
    ,round((sum(tj_homestay_gmv) / nullif(sum(tj_homestay_night),0) - sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0)) / nullif(sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0),0),4) as tj_vs_ct_order_adr_diff_rate
from user_base
group by 1,2
union all
select '下单结果' as view_type
    ,order_result as segment
    ,count(distinct tujia_user_id) as overlap_user_cnt
    ,sum(ct_hotel_lpv) as ct_hotel_lpv
    ,sum(tj_homestay_lpv) as tj_homestay_lpv
    ,round(avg(ct_hotel_avg_exp_price),2) as ct_hotel_avg_exp_price
    ,round(percentile(cast(ct_hotel_avg_exp_price as bigint),0.50),2) as ct_hotel_p50_user_exp_price
    ,round(avg(tj_homestay_avg_exp_price),2) as tj_homestay_avg_exp_price
    ,round(percentile(cast(tj_homestay_avg_exp_price as bigint),0.50),2) as tj_homestay_p50_user_exp_price
    ,round((avg(tj_homestay_avg_exp_price) - avg(ct_hotel_avg_exp_price)) / nullif(avg(ct_hotel_avg_exp_price),0),4) as tj_vs_ct_exp_price_diff_rate
    ,count(distinct case when ct_hotel_order_cnt > 0 then tujia_user_id end) as ct_hotel_order_user_cnt
    ,count(distinct case when tj_homestay_order_cnt > 0 then tujia_user_id end) as tj_homestay_order_user_cnt
    ,sum(ct_hotel_order_cnt) as ct_hotel_order_cnt
    ,sum(tj_homestay_order_cnt) as tj_homestay_order_cnt
    ,sum(ct_hotel_night) as ct_hotel_night
    ,sum(tj_homestay_night) as tj_homestay_night
    ,round(sum(ct_hotel_gmv),2) as ct_hotel_gmv
    ,round(sum(tj_homestay_gmv),2) as tj_homestay_gmv
    ,round(sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0),2) as ct_hotel_order_adr
    ,round(sum(tj_homestay_gmv) / nullif(sum(tj_homestay_night),0),2) as tj_homestay_order_adr
    ,round((sum(tj_homestay_gmv) / nullif(sum(tj_homestay_night),0) - sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0)) / nullif(sum(ct_hotel_gmv) / nullif(sum(ct_hotel_night),0),0),4) as tj_vs_ct_order_adr_diff_rate
from user_base
group by 1,2
order by view_type, overlap_user_cnt desc
;
