with ab_test as (
select dt
    ,lower(uid) uid 
    ,bucket_type
from ads.ads_abtest_user_key_uid 
where dt >= '2025-10-10'
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
and house_city_name = '首尔'
)
,list as (
select a.*
    ,case when t2.bucket_type in ('C','D','E','F','G','H','I') then '实验组'
        when t2.bucket_type in ('J','K') then '对照组'
        when t2.bucket_type in ('B') then '模型组'
        else t2.bucket_type
        end as bucket
    ,case when h2.house_id is not null then '二批'
        when h1.house_id is not null then '一批'
        else '其他' end is_chose
    ,check_type 
    ,t2.bucket_type
    ,source_type
    ,country_name
    ,house_city_name
from (
    select  
        dt
        ,wrapper_name
        ,trace_id 
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
    where dt >= '2025-09-25'
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
left join (
    select house_id
    from pdb_analysis_c.dwd_house_rank_tag_78_shouer_d 
    where dt = date_sub(current_date,1)
) h1 
on a.house_id = h1.house_id
left join (
    select a.house_id
    from (
        select house_id 
        from pdb_analysis_c.dwd_house_rank_tag_78_shouer_d
        where dt >= '2025-10-10'
        group by 1 
    ) a 
    left join (
        select house_id 
        from pdb_analysis_c.dwd_house_rank_tag_78_shouer_d
        where dt = '2025-10-09'
    ) b 
    on a.house_id = b.house_id 
    where b.house_id is null 
) h2
on a.house_id = h2.house_id
where house_city_name = '首尔'
)

select 
    bucket
    ,is_chose
    ,count(uid) lpv 
    ,count(distinct dt,uid) luv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_access_order_gmv) gmv 
from list 
group by 1,2