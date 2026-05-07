-- 新加坡：携程酒店复购相似但近180天未下单用户池（轻量版）
-- 为避免大维表超时，商圈使用曝光表 m_zone，经城市维表限定新加坡。

with params as (
    select date_sub(current_date,180) as order_start_dt
        ,date_sub(current_date,14) as flow_start_dt
        ,date_sub(current_date,1) as end_dt
        ,'新加坡' as target_city
)
,ordered_user as (
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
,flow_user as (
    select cast(m.member_id as string) as tujia_user_id
        ,case
            when nvl(cast(f.m_zone as string),'') = '乌节路' and cast(f.fh_price as double) between 1800 and 3000 then '高端品质复访-乌节路1800-3000'
            when nvl(cast(f.m_zone as string),'') = '圣淘沙岛' and cast(f.fh_price as double) >= 1800 then '高端度假复访-圣淘沙1800+'
            when nvl(cast(f.m_zone as string),'') = '滨海湾' and cast(f.fh_price as double) >= 1800 then '高端品质复访-滨海湾1800+'
            when nvl(cast(f.m_zone as string),'') = '武吉士' and cast(f.fh_price as double) between 500 and 1200 then '常规多次出行-武吉士500-1200'
            when nvl(cast(f.m_zone as string),'') = '乌节路' and cast(f.fh_price as double) between 800 and 1800 then '常规多次出行-乌节路800-1800'
            else null end as similar_pattern
        ,count(1) as lpv
        ,count(distinct concat(f.d,'|',f.uid)) as luv
        ,count(case when f.is_has_click = 1 then 1 end) as dpv
        ,count(distinct case when f.is_has_click = 1 then concat(f.d,'|',f.uid) end) as duv
        ,avg(cast(f.fh_price as double)) as avg_exp_price
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day f
    inner join excel_upload.dim_ctrip_list_qid_city city
      on f.m_city = city.m_city
    inner join ods_tujia_member.third_user_mapping m
      on lower(cast(f.uid as string)) = lower(cast(m.third_id as string))
     and m.channel_code = 'CtripId'
    cross join params p
    where f.d between p.flow_start_dt and p.end_dt
      and city.cityname = p.target_city
      and cast(f.fh_price as double) > 0
      and f.uid is not null
      and cast(f.uid as string) <> ''
    group by 1,2
)
select '携程酒店' as platform
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
