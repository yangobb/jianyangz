-- 新加坡用户画像：基础属性 / 出行习惯 / 浏览偏好 / 易转化&难转化分层
-- 口径：
--   1. 流量：近14天 dws.dws_path_ldbo_d，APP、海外、用户、前台展示、非推荐流量，城市=新加坡。
--   2. 订单：近30天 dws.dws_order 支付未取消海外订单，城市=新加坡。
--   3. 年龄：沿用海外大盘监控口径，携程 DNA 标签 + Qunar 用户主表，经 third_user_mapping 映射到途家 user_id。
--   4. 跨平台ADR：近一年携程酒店订单，经 third_user_mapping 映射到途家 user_id。
--   5. 出行画像为“可观测推断”：基于入住人数、筛选居室、入住晚数、价格偏好推断情侣/亲子家庭/背包客/商务长住等，不等同用户自填标签。
--   6. 手机型号：当前 dws_path_ldbo_d 显式字段只有 platform/client_agent，无法稳定拆具体机型，本 SQL 输出设备平台和入口。

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
        ,cast(m.tujia_user_id as string) as tujia_user_id
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
        ,cast(m.tujia_user_id as string) as tujia_user_id
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
    select cast(m.tujia_user_id as string) as tujia_user_id
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
        ,a.booking_uid
        ,a.without_risk_access_order_num as attr_order_cnt
        ,a.without_risk_access_order_room_night as attr_night
        ,a.without_risk_access_order_gmv as attr_gmv
        ,a.final_price
        ,case when lower(a.platform) like '%ios%' then 'iOS'
            when lower(a.platform) like '%android%' then 'Android'
            when a.platform is null or a.platform = '' then '未知'
            else a.platform end as platform_type
        ,nvl(a.client_agent,'未知') as client_agent
        ,case when a.empty_filter = 1 then '空搜'
            else '带条件搜索' end as search_filter_type
        ,nvl(a.rank_scene,'未知') as rank_scene
        ,nvl(a.query_type,'未知') as query_type
        ,nvl(a.sort_type,'未知') as sort_type
        ,case when cast(replace(nvl(a.guest,'0'),'人','') as int) <= 0 then 0
            when cast(replace(nvl(a.guest,'0'),'人','') as int) >= 9 then 9
            else cast(replace(nvl(a.guest,'0'),'人','') as int) end as guest_num
        ,case when cast(nvl(a.filter_bedroom_count,'0') as int) <= 0 then 0
            when cast(nvl(a.filter_bedroom_count,'0') as int) >= 3 then 3
            else cast(nvl(a.filter_bedroom_count,'0') as int) end as filter_bedroom_num
        ,case when datediff(to_date(a.checkout_date),to_date(a.checkin_date)) <= 0 then null
            else datediff(to_date(a.checkout_date),to_date(a.checkin_date)) end as stay_night
        ,case when datediff(to_date(a.checkin_date),to_date(a.dt)) < 0 then null
            else datediff(to_date(a.checkin_date),to_date(a.dt)) end as lead_days
        ,case when a.max_price is null or a.max_price <= 0 then '未筛价格'
            when a.max_price <= 500 then '0-500'
            when a.max_price <= 1000 then '500-1000'
            when a.max_price <= 2000 then '1000-2000'
            when a.max_price <= 4000 then '2000-4000'
            else '4000+' end as search_price_bucket
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.flow_start_dt and p.end_dt
      and a.city_name = p.target_city
      and a.is_oversea = 1
      and a.user_type = '用户'
      and a.wrapper_name in ('携程','去哪儿','途家')
      and a.client_name = 'APP'
      and a.front_display = 'true'
      and a.is_recommend = 0
      and a.final_price > 0
)
,flow_tagged as (
    select f.*
        ,nvl(ai.age_bucket,'未知') as age_bucket
        ,case when ca.cross_platform_adr is null then '未知'
            when ca.cross_platform_adr <= 250 then '0-250'
            when ca.cross_platform_adr <= 400 then '250-400'
            when ca.cross_platform_adr <= 800 then '400-800'
            when ca.cross_platform_adr <= 1500 then '800-1500'
            else '1500+' end as cross_adr_bucket
        ,case when f.stay_night >= 7 then '商务/长住'
            when f.guest_num = 1 and f.final_price <= 600 then '背包客/独行低价'
            when f.guest_num = 2 and f.filter_bedroom_num <= 1 and f.stay_night between 1 and 3 then '情侣/双人短途'
            when f.guest_num >= 3 or f.filter_bedroom_num >= 2 then '亲子/家庭/多人'
            when f.stay_night between 4 and 6 then '中长住休闲'
            else '普通出行' end as travel_persona
        ,case when f.stay_night = 1 then '1晚'
            when f.stay_night between 2 and 3 then '2-3晚'
            when f.stay_night between 4 and 7 then '4-7晚'
            when f.stay_night >= 8 then '8晚及以上'
            else '未知' end as stay_bucket
        ,case when f.lead_days = 0 then 'T0'
            when f.lead_days between 1 and 7 then 'T1-7'
            when f.lead_days between 8 and 14 then 'T8-14'
            when f.lead_days between 15 and 30 then 'T15-30'
            when f.lead_days > 30 then 'T31+'
            else '未知' end as lead_bucket
        ,case when f.guest_num = 1 then '1人'
            when f.guest_num = 2 then '2人'
            when f.guest_num between 3 and 4 then '3-4人'
            when f.guest_num >= 5 then '5人及以上'
            else '未知' end as guest_bucket
        ,case when f.filter_bedroom_num = 1 then '1居'
            when f.filter_bedroom_num = 2 then '2居'
            when f.filter_bedroom_num >= 3 then '3居及以上'
            else '未筛居室' end as filter_bedroom_bucket
    from flow_base f
    left join age_info ai
      on lower(f.user_id) = lower(ai.tujia_user_id)
     and f.wrapper_name = ai.wrapper_name
    left join ctrip_hotel_adr ca
      on lower(f.user_id) = lower(ca.tujia_user_id)
)
,profile_union as (
    select '基础-年龄' as profile_type, age_bucket as profile_value, * from flow_tagged
    union all select '基础-跨平台ADR档', cross_adr_bucket, * from flow_tagged
    union all select '基础-设备平台', platform_type, * from flow_tagged
    union all select '基础-客户端入口', client_agent, * from flow_tagged
    union all select '出行-人群推断', travel_persona, * from flow_tagged
    union all select '出行-入住人数', guest_bucket, * from flow_tagged
    union all select '出行-入住晚数', stay_bucket, * from flow_tagged
    union all select '出行-提前期', lead_bucket, * from flow_tagged
    union all select '浏览-价格偏好', search_price_bucket, * from flow_tagged
    union all select '浏览-居室偏好', filter_bedroom_bucket, * from flow_tagged
    union all select '浏览-搜索方式', search_filter_type, * from flow_tagged
    union all select '浏览-排序偏好', sort_type, * from flow_tagged
    union all select '浏览-搜索场景', rank_scene, * from flow_tagged
    union all select '浏览-搜索类型', query_type, * from flow_tagged
)
,agg as (
    select profile_type
        ,profile_value
        ,count(distinct concat(dt,'|',uid)) as luv
        ,count(1) as lpv
        ,count(distinct case when detail_uid is not null then concat(dt,'|',detail_uid) end) as duv
        ,count(distinct case when booking_uid is not null then concat(dt,'|',booking_uid) end) as buv
        ,sum(attr_order_cnt) as attr_order_cnt
        ,sum(attr_night) as attr_night
        ,sum(attr_gmv) as attr_gmv
        ,round(avg(final_price),2) as avg_exp_price
        ,round(avg(stay_night),2) as avg_search_stay_night
        ,round(avg(lead_days),2) as avg_search_lead_days
    from profile_union
    group by 1,2
)
,scored as (
    select a.*
        ,round(duv / nullif(luv,0),4) as l2d_uv
        ,round(buv / nullif(luv,0),4) as l2b_uv
        ,round(attr_order_cnt / nullif(luv,0),4) as attr_order_per_luv
        ,round(attr_night / nullif(luv,0) * 1000,2) as attr_night_per_1000_luv
        ,round(attr_gmv / nullif(luv,0),2) as attr_gmv_per_luv
        ,case when luv < 100 then '样本小-观察'
            when attr_order_cnt / nullif(luv,0) >= 0.010 or attr_night / nullif(luv,0) >= 0.020 then '易转化'
            when duv / nullif(luv,0) >= 0.120 and nvl(attr_order_cnt,0) = 0 then '高点击低下单-难转化'
            when duv / nullif(luv,0) < 0.080 then '低点击-难转化'
            else '中性观察' end as conversion_class
    from agg a
)
select *
from scored
order by profile_type, conversion_class, luv desc
;
