-- 新加坡：途家民宿复购相似但近180天未下单用户池（轻量版）

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
)
,flow_user as (
    select cast(a.user_id as string) as tujia_user_id
        ,case
            when h.dynamic_business = '牛车水' and h.house_type = '其他类型' and cast(a.final_price as double) <= 500 then '低价固定目的地-牛车水其他类型'
            when h.dynamic_business = '加冷' and h.house_type in ('其他类型','青旅') and cast(a.final_price as double) <= 500 then '低价高频短住-加冷'
            when h.dynamic_business = '武吉士' and h.house_type in ('青旅','其他类型') and cast(a.final_price as double) <= 500 then '低价高频短住-武吉士'
            when h.dynamic_business = '河滨区' and h.house_type = '其他类型' and cast(a.final_price as double) <= 500 then '低价固定目的地-河滨区'
            when h.dynamic_business = '芽笼' and h.house_type = '标准酒店' and cast(a.final_price as double) <= 800 then '低价标准酒店-芽笼'
            else null end as similar_pattern
        ,count(1) as lpv
        ,count(distinct concat(a.dt,'|',a.uid)) as luv
        ,count(case when a.detail_uid is not null then 1 end) as dpv
        ,count(distinct case when a.detail_uid is not null then concat(a.dt,'|',a.detail_uid) end) as duv
        ,avg(cast(a.final_price as double)) as avg_exp_price
    from dws.dws_path_ldbo_d a
    inner join house_base h
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
    group by 1,2
)
select '途家民宿' as platform
    ,f.similar_pattern
    ,count(distinct f.tujia_user_id) as similar_unordered_user_cnt
    ,sum(f.lpv) as lpv
    ,sum(f.luv) as luv
    ,sum(f.dpv) as dpv
    ,sum(f.duv) as duv
    ,round(sum(f.duv) / nullif(sum(f.luv),0),4) as l2d_uv
    ,round(avg(f.avg_exp_price),2) as avg_exp_price
from flow_user f
left join ordered_user o
  on f.tujia_user_id = o.tujia_user_id
where f.similar_pattern is not null
  and o.tujia_user_id is null
group by 1,2
order by similar_unordered_user_cnt desc
;
