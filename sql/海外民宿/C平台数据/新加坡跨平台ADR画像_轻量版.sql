-- 新加坡跨平台ADR画像（轻量版）
-- 先收敛新加坡携程侧用户，再查这些用户近一年携程酒店订单 ADR。

with params as (
    select date_sub(current_date,14) as start_dt
        ,date_sub(current_date,1) as end_dt
        ,'新加坡' as target_city
)
,sg_ctrip_users as (
    select cast(a.user_id as string) as tujia_user_id
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
      and a.wrapper_name = '携程'
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.is_recommend = 0
      and a.final_price > 0
      and nvl(a.user_id,0) <> 0
    group by 1
)
,mapped as (
    select u.*
        ,m.third_id as ctrip_user_id
    from sg_ctrip_users u
    inner join ods_tujia_member.third_user_mapping m
      on lower(u.tujia_user_id) = lower(cast(m.member_id as string))
     and m.channel_code = 'CtripId'
)
,adr_user as (
    select m.tujia_user_id
        ,m.luv
        ,m.lpv
        ,m.duv
        ,m.buv
        ,m.attr_order_cnt
        ,m.attr_night
        ,m.attr_gmv
        ,m.avg_exp_price
        ,sum(o.ciireceivable) / nullif(sum(o.ciiquantity),0) as cross_platform_adr
    from mapped m
    left join app_ctrip.edw_htl_order_all_split o
      on lower(m.ctrip_user_id) = lower(o.uid)
     and o.d = date_sub(current_date,14)
     and o.submitfrom = 'client'
     and to_date(o.orderdate) between date_sub(date_sub(current_date,14),365) and date_sub(date_sub(current_date,14),1)
     and o.orderstatus in ('S','P')
     and o.country = 1
     and o.ordertype = 2
     and o.clientid <> ''
     and o.clientid is not null
    group by 1,2,3,4,5,6,7,8,9
)
select '基础-跨平台ADR档' as profile_type
    ,case when cross_platform_adr is null then '未知'
        when cross_platform_adr <= 250 then '0-250'
        when cross_platform_adr <= 400 then '250-400'
        when cross_platform_adr <= 800 then '400-800'
        when cross_platform_adr <= 1500 then '800-1500'
        else '1500+' end as profile_value
    ,sum(luv) as luv
    ,sum(lpv) as lpv
    ,sum(duv) as duv
    ,sum(buv) as buv
    ,round(sum(duv) / nullif(sum(luv),0),4) as l2d_uv
    ,round(sum(buv) / nullif(sum(luv),0),4) as l2b_uv
    ,sum(attr_order_cnt) as attr_order_cnt
    ,sum(attr_night) as attr_night
    ,round(sum(attr_gmv),2) as attr_gmv
    ,round(avg(avg_exp_price),2) as avg_exp_price
    ,round(sum(attr_order_cnt) / nullif(sum(luv),0),4) as attr_order_per_luv
    ,round(sum(attr_night) / nullif(sum(luv),0) * 1000,2) as attr_night_per_1000_luv
    ,round(sum(attr_gmv) / nullif(sum(luv),0),2) as attr_gmv_per_luv
from adr_user
group by 1,2
order by luv desc
;
