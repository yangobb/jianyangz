-- 新加坡：复购相似但近180天未下单用户池（轻量版）
-- 基于已跑出的复购高频模式，固定几类可运营模式做近14天曝光用户挖掘。
-- 输出聚合，不输出用户明细。

with params as (
    select date_sub(current_date,180) as order_start_dt
        ,date_sub(current_date,14) as flow_start_dt
        ,date_sub(current_date,1) as end_dt
        ,'新加坡' as target_city
)
,house_base as (
    select h.house_id
        ,nvl(h.dynamic_business,'未知商圈') as dynamic_business
        ,nvl(h.house_type,'未知房屋类型') as house_type
    from dws.dws_house_d h
    cross join params p
    where h.dt = p.end_dt
      and h.house_is_oversea = 1
      and h.house_city_name = p.target_city
)
,ordered_user as (
    select distinct cast(o.user_id as string) as tujia_user_id
    from dws.dws_order o
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
      and o.city_name = p.target_city
      and nvl(o.user_id,0) <> 0
    union
    select distinct cast(m.member_id as string) as tujia_user_id
    from app_ctrip.edw_htl_order_all_split o
    inner join ods_tujia_member.third_user_mapping m
      on lower(cast(o.uid as string)) = lower(cast(m.third_id as string))
     and m.channel_code = 'CtripId'
    cross join params p
    where o.d = p.end_dt
      and to_date(o.orderdate) between p.order_start_dt and p.end_dt
      and o.orderstatus in ('S','P')
      and o.ordertype = 2
      and o.cityname = p.target_city
      and o.uid is not null
      and cast(o.uid as string) <> ''
)
,tj_flow as (
    select '途家民宿' as platform
        ,cast(a.user_id as string) as tujia_user_id
        ,case
            when nvl(h.dynamic_business,'未知商圈') = '牛车水'
             and nvl(h.house_type,'未知房屋类型') = '其他类型'
             and cast(a.final_price as double) <= 500 then '低价固定目的地-牛车水其他类型'
            when nvl(h.dynamic_business,'未知商圈') = '加冷'
             and nvl(h.house_type,'未知房屋类型') in ('其他类型','青旅')
             and cast(a.final_price as double) <= 500 then '低价高频短住-加冷'
            when nvl(h.dynamic_business,'未知商圈') = '武吉士'
             and nvl(h.house_type,'未知房屋类型') in ('青旅','其他类型')
             and cast(a.final_price as double) <= 500 then '低价高频短住-武吉士'
            when nvl(h.dynamic_business,'未知商圈') = '河滨区'
             and nvl(h.house_type,'未知房屋类型') = '其他类型'
             and cast(a.final_price as double) <= 500 then '低价固定目的地-河滨区'
            when nvl(h.dynamic_business,'未知商圈') = '芽笼'
             and nvl(h.house_type,'未知房屋类型') = '标准酒店'
             and cast(a.final_price as double) <= 800 then '低价标准酒店-芽笼'
            else null end as similar_pattern
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
    group by 1,2,3
)
,ct_flow as (
    select '携程酒店' as platform
        ,cast(m.member_id as string) as tujia_user_id
        ,case
            when nvl(d.zonename,'未知商圈') = '乌节路'
             and cast(f.fh_price as double) between 1800 and 3000 then '高端品质复访-乌节路1800-3000'
            when nvl(d.zonename,'未知商圈') = '圣淘沙岛'
             and cast(f.fh_price as double) >= 1800 then '高端度假复访-圣淘沙1800+'
            when nvl(d.zonename,'未知商圈') = '滨海湾'
             and cast(f.fh_price as double) >= 1800 then '高端品质复访-滨海湾1800+'
            when nvl(d.zonename,'未知商圈') = '武吉士'
             and cast(f.fh_price as double) between 500 and 1200 then '常规多次出行-武吉士500-1200'
            when nvl(d.zonename,'未知商圈') = '乌节路'
             and cast(f.fh_price as double) between 800 and 1800 then '常规多次出行-乌节路800-1800'
            else null end as similar_pattern
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
    group by 1,2,3
)
,flow_all as (
    select * from tj_flow where similar_pattern is not null
    union all
    select * from ct_flow where similar_pattern is not null
)
select f.platform
    ,f.similar_pattern
    ,count(distinct f.tujia_user_id) as similar_unordered_user_cnt
    ,sum(f.lpv) as lpv
    ,sum(f.luv) as luv
    ,sum(f.dpv) as dpv
    ,sum(f.duv) as duv
    ,round(sum(f.duv) / nullif(sum(f.luv),0),4) as l2d_uv
    ,round(avg(f.avg_exp_price),2) as avg_exp_price
from flow_all f
left join ordered_user o
  on f.tujia_user_id = o.tujia_user_id
where o.tujia_user_id is null
group by 1,2
order by platform, similar_unordered_user_cnt desc
;
