-- CDE : 金特盘+2 
-- IJK : 宝藏+1.5 
-- FGH : 不变


with ab_test as (
select dt
    ,lower(uid) uid 
    ,bucket_type
from ads.ads_abtest_user_key_uid 
where dt >= '2025-07-09' 
and key in ('waptujia001_rank_ecosystem','waptujia016_rank_ecosystem','wapctripbnb_rank_ecosystem')
and bucket_type in ('B','C','D','E','F','G','H','I','J','K')
group by 1,2,3 
)
,h as (
select
    country_name,
    house_city_name,
    hotel_id,
    house_id,
    case when bedroom_count = 1 then 1 else 0 end is_1ju,
    case when landlord_channel = 303 then '携程接入'
        when landlord_channel = 1 then '直采'
    else '其他' end as source_type,
    case when house_type = '标准酒店' then 1 else 0 end is_standard
from dws.dws_house_d
where dt = date_sub(current_date,1)
and house_is_oversea = 1
and house_is_online = 1
)
,is_gold as (
select *
from pdb_analysis_c.dwd_house_rank_tag_76_d
where dt = date_sub(current_date(), 1)
) 
,list as (
select a.*
    ,case
        when house_city_name = '大阪' and t2.bucket_type = 'C' then '金特牌宝藏实验' 
        when house_city_name in ('大阪','东京') and t2.bucket_type in ('C','J','K') then '宝藏实验' 
        when city_name in ('大阪','京都','曼谷','普吉岛','吉隆坡','芭堤雅','济州市') and t2.bucket_type in ('C','D','E') then '金特牌实验'
        when t2.bucket_type in ('F','G','H','I','J','K') then '无变化'
        else 'B组'
        end as bucket
    ,check_type 
    ,t2.bucket_type
    ,source_type
    ,is_standard
    ,case when is_gold.house_id is not null then '七大类金特盘' when source_type = '直采' then '直采' else '非七大类金特盘' end is_gold
    ,country_name
    ,house_city_name
from (
    select  
        dt
        ,wrapper_name
        ,trace_id
        ,city_id
        ,case when city_name = '陵水' then '陵水(三亚)' else city_name end as city_name 
        ,city_level
        ,house_id
        ,lower(uid) uid
        ,geo_position_id
        ,location_filter as geo_name
        ,location_type as geo_type
        ,final_price
        ,detail_uid
        ,distance
        ,position
        ,rank_scene_empty_filter
        ,dynamic_business
        ,dynamic_business_id
        ,search_id
        ,checkin_date
        ,checkout_date
        ,case when datediff(checkin_date,dt) = 0 then 'T0'
            when datediff(checkin_date,dt) <=30 then 'T30'
              else 'TN' 
        end as check_type
        ,case when pmod(datediff(checkin_date,'1900-01-08'),7)+1 in (5,6) then '周末'
              when pmod(datediff(checkin_date,'1900-01-08'),7)+1 not in (5,6) then '周中'
        end as week_type
        ,case when bedroom_count=1 then '一居'
              when bedroom_count=2 then '二居'
              when bedroom_count>=3 then '三居+'
        end as room_type
        ,case when logic_bit & 2048 = 2048 then 1 
              else 0 end as position_type
        ,without_risk_order_num
        ,without_risk_order_room_night
        ,without_risk_order_gmv
        ,without_risk_access_order_gmv
        ,without_risk_access_order_num
        ,without_risk_access_order_room_night
        ,get_json_object(server_log,'$.hasUserClickBehavior') as if_click
    from dws.dws_path_ldbo_d
    where dt between '2025-07-09' and date_sub(current_date,1)
    and is_oversea = 1 
    and wrapper_name in ('途家','携程','去哪儿') 
    and source = '102' 
    and user_type = '用户'
) as a  
left join ab_test as t2
on a.dt=t2.dt
and a.uid=t2.uid 
join h
on a.house_id = h.house_id
left join is_gold 
on a.house_id = is_gold.house_id
where house_city_name in ('大阪','东京','京都','曼谷','普吉岛','芭堤雅','吉隆坡','济州市')
-- and house_city_name = '芭堤雅'
and house_city_name = '大阪' and t2.bucket_type = 'C'
)


select *
from list 
where without_risk_order_gmv > 0
-- group by 1,2  
-- select house_city_name
--     ,bucket
--     ,source_type
--     ,is_gold
--     ,check_type
--     ,count(uid) lpv 
--     ,count(distinct dt,uid) luv 
--     ,sum(without_risk_order_num) order_num
--     ,sum(without_risk_order_room_night) night 
--     ,sum(without_risk_access_order_gmv) gmv 
-- from list 
-- group by 1,2,3,4,5