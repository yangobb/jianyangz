-- 新加坡用户基础画像：年龄 / 跨平台ADR / 设备平台 / 客户端入口 / 消费结构
-- 说明：
--   1. 年龄来源沿用海外大盘流量监控口径：携程 DNA 标签 + Qunar 用户主表，经第三方 ID 映射到途家 user_id。
--   2. 跨平台 ADR 来源：近一年携程酒店订单，按 third_user_mapping 映射到途家 user_id。
--   3. 设备使用：dws_path_ldbo_d 显式字段只有 platform / client_agent；未直接产出手机型号。
--   4. 本 SQL 不输出用户明细，只输出聚合分布。

with params as (
    select date_sub(current_date,14) as flow_start_dt
        ,date_sub(current_date,1) as end_dt
        ,date_sub(current_date,30) as order_start_dt
        ,'新加坡' as target_city
)
,mapp_ctrip as (
    select member_id as tujia_user_id
        ,third_id as ctrip_user_id
    from ods_tujia_member.third_user_mapping
    where channel_code = 'CtripId'
)
,mapp_qunar as (
    select member_id as tujia_user_id
        ,third_id as qunar_user_id
    from ods_tujia_member.third_user_mapping
    where channel_code = 'QunarId'
)
,age_info as (
    select distinct '携程' as wrapper_name
        ,m.tujia_user_id
        ,case when t.age < 23 then '0-22'
            when t.age < 29 then '23-28'
            when t.age < 36 then '29-35'
            when t.age < 50 then '36-49'
            when t.age >= 50 then '50+'
            else '未知' end as age_bucket
    from mapp_ctrip m
    left join (
        select uid
            ,cast(ltrim(regexp_replace(split(regexp_extract(regexp_extract(label, '(1175[^}]+)', 1),'("label_value_text":[^,]+)',1),':')[1],'"','')) as int) as age
        from app_ctrip.edw_bnb_dna_user_label_all
        where d in (select max(d) from app_ctrip.edw_bnb_dna_user_label_all where d >= date_sub(current_date,14))
    ) t
      on lower(m.ctrip_user_id) = lower(t.uid)
    where t.age is not null
    union all
    select distinct '去哪儿' as wrapper_name
        ,m.tujia_user_id
        ,case when t.account_age < 23 then '0-22'
            when t.account_age < 29 then '23-28'
            when t.account_age < 36 then '29-35'
            when t.account_age < 50 then '36-49'
            when t.account_age >= 50 then '50+'
            else '未知' end as age_bucket
    from mapp_qunar m
    left join (
        select user_id
            ,account_age
        from tujia_share.dw_alita_user_main_tujia
        where account_age > 0
          and account_age < 100
    ) t
      on lower(m.qunar_user_id) = lower(t.user_id)
    where t.account_age is not null
)
,ctrip_hotel_adr as (
    select m.tujia_user_id
        ,avg(ord.gmv / nullif(ord.nights,0)) as cross_platform_adr
    from (
        select uid
            ,ciireceivable as gmv
            ,ciiquantity as nights
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,14)
          and submitfrom = 'client'
          and to_date(orderdate) between date_sub(date_sub(current_date,14),365) and date_sub(date_sub(current_date,14),1)
          and orderstatus in ('S','P')
          and country = 1
          and ordertype = 2
          and clientid <> ''
          and clientid is not null
    ) ord
    inner join mapp_ctrip m
      on lower(ord.uid) = lower(m.ctrip_user_id)
    group by 1
)
,flow_base as (
    select a.dt
        ,a.wrapper_name
        ,a.uid
        ,cast(a.user_id as string) as user_id
        ,a.detail_uid
        ,a.order_uid
        ,a.without_risk_access_order_num as attr_order_cnt
        ,a.without_risk_access_order_room_night as attr_night
        ,a.without_risk_access_order_gmv as attr_gmv
        ,a.final_price
        ,case when lower(a.platform) like '%ios%' then 'iOS'
            when lower(a.platform) like '%android%' then 'Android'
            when a.platform is null or a.platform = '' then '未知'
            else a.platform end as platform_type
        ,nvl(a.client_agent,'未知') as client_agent
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.flow_start_dt and p.end_dt
      and a.city_name = p.target_city
      and a.is_oversea = 1
      and a.user_type = '用户'
      and a.wrapper_name in ('携程','去哪儿','途家')
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.final_price > 0
)
,order_base as (
    select o.order_no
        ,o.uid
        ,cast(o.user_id as string) as user_id
        ,case when o.terminal_type_name like '%携程%' then '携程'
            when o.terminal_type_name like '%去哪儿%' then '去哪儿'
            when o.terminal_type_name like '%途家%' then '途家'
            else '其他' end as wrapper_name
        ,o.order_room_night_count as night
        ,o.room_total_amount as gmv
        ,datediff(o.checkout_date,o.checkin_date) as stay_night
        ,datediff(o.checkin_date,o.create_date) as lead_days
    from dws.dws_order o
    cross join params p
    where o.create_date between p.order_start_dt and p.end_dt
      and o.city_name = p.target_city
      and o.is_paysuccess_order = 1
      and o.is_cancel_order = 0
      and o.is_overseas = 1
)
,profile_flow as (
    select '年龄' as profile_type
        ,nvl(ai.age_bucket,'未知') as profile_value
        ,count(distinct concat(f.dt,'|',f.uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when f.detail_uid is not null then concat(f.dt,'|',f.detail_uid) end) as duv
        ,sum(f.attr_order_cnt) as attr_order_cnt
        ,sum(f.attr_night) as attr_night
        ,sum(f.attr_gmv) as attr_gmv
        ,round(avg(f.final_price),2) as avg_exp_price
    from flow_base f
    left join age_info ai
      on lower(f.user_id) = lower(cast(ai.tujia_user_id as string))
     and f.wrapper_name = ai.wrapper_name
    group by 1,2
    union all
    select '跨平台ADR档' as profile_type
        ,case when ca.cross_platform_adr is null then '未知'
            when ca.cross_platform_adr <= 250 then '0-250'
            when ca.cross_platform_adr <= 400 then '250-400'
            when ca.cross_platform_adr <= 800 then '400-800'
            when ca.cross_platform_adr <= 1500 then '800-1500'
            else '1500+' end as profile_value
        ,count(distinct concat(f.dt,'|',f.uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when f.detail_uid is not null then concat(f.dt,'|',f.detail_uid) end) as duv
        ,sum(f.attr_order_cnt) as attr_order_cnt
        ,sum(f.attr_night) as attr_night
        ,sum(f.attr_gmv) as attr_gmv
        ,round(avg(f.final_price),2) as avg_exp_price
    from flow_base f
    left join ctrip_hotel_adr ca
      on lower(f.user_id) = lower(cast(ca.tujia_user_id as string))
    group by 1,2
    union all
    select '设备平台' as profile_type
        ,platform_type as profile_value
        ,count(distinct concat(dt,'|',uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when detail_uid is not null then concat(dt,'|',detail_uid) end) as duv
        ,sum(attr_order_cnt) as attr_order_cnt
        ,sum(attr_night) as attr_night
        ,sum(attr_gmv) as attr_gmv
        ,round(avg(final_price),2) as avg_exp_price
    from flow_base
    group by 1,2
    union all
    select '客户端入口' as profile_type
        ,client_agent as profile_value
        ,count(distinct concat(dt,'|',uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when detail_uid is not null then concat(dt,'|',detail_uid) end) as duv
        ,sum(attr_order_cnt) as attr_order_cnt
        ,sum(attr_night) as attr_night
        ,sum(attr_gmv) as attr_gmv
        ,round(avg(final_price),2) as avg_exp_price
    from flow_base
    group by 1,2
)
,profile_order as (
    select '年龄' as profile_type
        ,nvl(ai.age_bucket,'未知') as profile_value
        ,count(distinct o.order_no) as paid_order_cnt
        ,count(distinct o.user_id) as paid_user_cnt
        ,sum(o.night) as paid_night
        ,sum(o.gmv) as paid_gmv
        ,round(sum(o.gmv) / nullif(sum(o.night),0),2) as adr
        ,round(sum(o.night) / nullif(count(distinct o.order_no),0),2) as avg_night_per_order
        ,round(avg(o.lead_days),2) as avg_lead_days
    from order_base o
    left join age_info ai
      on lower(o.user_id) = lower(cast(ai.tujia_user_id as string))
     and o.wrapper_name = ai.wrapper_name
    group by 1,2
    union all
    select '跨平台ADR档' as profile_type
        ,case when ca.cross_platform_adr is null then '未知'
            when ca.cross_platform_adr <= 250 then '0-250'
            when ca.cross_platform_adr <= 400 then '250-400'
            when ca.cross_platform_adr <= 800 then '400-800'
            when ca.cross_platform_adr <= 1500 then '800-1500'
            else '1500+' end as profile_value
        ,count(distinct o.order_no) as paid_order_cnt
        ,count(distinct o.user_id) as paid_user_cnt
        ,sum(o.night) as paid_night
        ,sum(o.gmv) as paid_gmv
        ,round(sum(o.gmv) / nullif(sum(o.night),0),2) as adr
        ,round(sum(o.night) / nullif(count(distinct o.order_no),0),2) as avg_night_per_order
        ,round(avg(o.lead_days),2) as avg_lead_days
    from order_base o
    left join ctrip_hotel_adr ca
      on lower(o.user_id) = lower(cast(ca.tujia_user_id as string))
    group by 1,2
)
select f.profile_type
    ,f.profile_value
    ,f.luv
    ,f.lpv
    ,f.duv
    ,round(f.duv / nullif(f.luv,0),4) as l2d_uv
    ,f.attr_order_cnt
    ,f.attr_night
    ,round(f.attr_gmv,2) as attr_gmv
    ,f.avg_exp_price
    ,nvl(o.paid_order_cnt,0) as paid_order_cnt
    ,nvl(o.paid_user_cnt,0) as paid_user_cnt
    ,nvl(o.paid_night,0) as paid_night
    ,round(nvl(o.paid_gmv,0),2) as paid_gmv
    ,o.adr
    ,o.avg_night_per_order
    ,o.avg_lead_days
from profile_flow f
left join profile_order o
  on f.profile_type = o.profile_type
 and f.profile_value = o.profile_value
order by f.profile_type, f.luv desc
;
