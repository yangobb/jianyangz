-- 新加坡用户年龄画像（轻量版）
-- 先收敛新加坡近14天相关 user_id，再映射携程/去哪儿年龄标签，避免全量多维 union 超时。

with params as (
    select date_sub(current_date,14) as start_dt
        ,date_sub(current_date,1) as end_dt
        ,'新加坡' as target_city
)
,sg_users as (
    select a.wrapper_name
        ,cast(a.user_id as string) as tujia_user_id
        ,count(distinct concat(a.dt,'|',a.uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when a.detail_uid is not null then concat(a.dt,'|',a.detail_uid) end) as duv
        ,count(distinct case when a.booking_uid is not null then concat(a.dt,'|',a.booking_uid) end) as buv
        ,sum(nvl(a.without_risk_access_order_num,0)) as attr_order_cnt
        ,sum(nvl(a.without_risk_access_order_room_night,0)) as attr_night
        ,sum(nvl(a.without_risk_access_order_gmv,0)) as attr_gmv
        ,avg(a.final_price) as avg_exp_price
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.start_dt and p.end_dt
      and a.city_name = p.target_city
      and a.is_oversea = 1
      and a.user_type = '用户'
      and a.wrapper_name in ('携程','去哪儿')
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.is_recommend = 0
      and a.final_price > 0
      and nvl(a.user_id,0) <> 0
    group by 1,2
)
,ctrip_age as (
    select distinct cast(m.member_id as string) as tujia_user_id
        ,case when t.age < 23 then '0-22'
            when t.age < 29 then '23-28'
            when t.age < 36 then '29-35'
            when t.age < 50 then '36-49'
            when t.age >= 50 then '50+'
            else '未知' end as age_bucket
    from sg_users u
    inner join ods_tujia_member.third_user_mapping m
      on u.wrapper_name = '携程'
     and lower(u.tujia_user_id) = lower(cast(m.member_id as string))
     and m.channel_code = 'CtripId'
    left join (
        select uid
            ,cast(ltrim(regexp_replace(split(regexp_extract(regexp_extract(label, '(1175[^}]+)', 1),'("label_value_text":[^,]+)',1),':')[1],'"','')) as int) as age
        from app_ctrip.edw_bnb_dna_user_label_all
        where d in (select max(d) from app_ctrip.edw_bnb_dna_user_label_all where d >= date_sub(current_date,14))
    ) t
      on lower(m.third_id) = lower(t.uid)
    where t.age is not null
)
,qunar_age as (
    select distinct cast(m.member_id as string) as tujia_user_id
        ,case when t.account_age < 23 then '0-22'
            when t.account_age < 29 then '23-28'
            when t.account_age < 36 then '29-35'
            when t.account_age < 50 then '36-49'
            when t.account_age >= 50 then '50+'
            else '未知' end as age_bucket
    from sg_users u
    inner join ods_tujia_member.third_user_mapping m
      on u.wrapper_name = '去哪儿'
     and lower(u.tujia_user_id) = lower(cast(m.member_id as string))
     and m.channel_code = 'QunarId'
    left join tujia_share.dw_alita_user_main_tujia t
      on lower(m.third_id) = lower(t.user_id)
    where t.account_age > 0
      and t.account_age < 100
)
,age_user as (
    select '携程' as wrapper_name, tujia_user_id, age_bucket from ctrip_age
    union all
    select '去哪儿' as wrapper_name, tujia_user_id, age_bucket from qunar_age
)
select '基础-年龄' as profile_type
    ,nvl(a.age_bucket,'未知') as profile_value
    ,sum(u.luv) as luv
    ,sum(u.lpv) as lpv
    ,sum(u.duv) as duv
    ,sum(u.buv) as buv
    ,round(sum(u.duv) / nullif(sum(u.luv),0),4) as l2d_uv
    ,round(sum(u.buv) / nullif(sum(u.luv),0),4) as l2b_uv
    ,sum(u.attr_order_cnt) as attr_order_cnt
    ,sum(u.attr_night) as attr_night
    ,round(sum(u.attr_gmv),2) as attr_gmv
    ,round(avg(u.avg_exp_price),2) as avg_exp_price
    ,round(sum(u.attr_order_cnt) / nullif(sum(u.luv),0),4) as attr_order_per_luv
    ,round(sum(u.attr_night) / nullif(sum(u.luv),0) * 1000,2) as attr_night_per_1000_luv
    ,round(sum(u.attr_gmv) / nullif(sum(u.luv),0),2) as attr_gmv_per_luv
from sg_users u
left join age_user a
  on u.wrapper_name = a.wrapper_name
 and lower(u.tujia_user_id) = lower(a.tujia_user_id)
group by 1,2
order by luv desc
;
