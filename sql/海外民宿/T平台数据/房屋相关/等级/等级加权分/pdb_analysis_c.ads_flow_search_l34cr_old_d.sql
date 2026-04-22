with house_info as 
(
select t1.*
from (
    select distinct
        t1.dt,
        t1.house_id,
        house_city_id as city_id,
        case when house_city_name='陵水' then '陵水(三亚)' else house_city_name end as city_name,
        dynamic_business_id,
        dynamic_business,
        t1.level2_area_id+10000 as level2_area_id,
        t1.level2_area_name,
        t1.house_class,
        scenic_area_id,
        scenic_area_name,
        case when is_prefer_pro=1 then '严选' else '非严选' end as is_prefer_pro,
        case when landlord_channel=303 then '携程接入' 
        when landlord_channel=1 then '直采'
        else '其他接入' end as hs_type
    from dws.dws_house_d as t1
    where t1.dt between date_sub(current_date,14) and date_sub(current_date,1)
    and house_is_online=1
    and house_is_oversea='0'
) as t1
left join (
   select 
  house_id
  ,house_class
from pdb_analysis_c.dwd_house_abtest_d
where dt=date_sub(current_date,1)
) as t2
on t1.house_id=t2.house_id
)
,list as
(select a.* 
    ,b.house_class
     ,case when day_type='节假日' or day_type='暑假' then '节假日' else '非节假日' end as is_holiday
      from 
      (select  
        dt
        ,wrapper_name
        ,trace_id
        ,city_id
        ,case when checkout_date >= '2023-04-29' and checkout_date <= '2023-05-03' then '五一' else '其他' end as is_may
        ,case when city_name = '陵水' then '陵水(三亚)' else city_name end as city_name 
        ,search_city_id
        ,search_city_name
        ,city_level
        ,house_id
        ,uid
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
        ,concat(wrapper_name,search_id,trace_id) as trace_only
        ,checkin_date
        ,checkout_date
        ,case when get_json_object(bucket,'$.bucketClassWeight') in ('C','D') then 'CD'
            when get_json_object(bucket,'$.bucketClassWeight') in ('E') then 'E'
            when get_json_object(bucket,'$.bucketClassWeight') in ('F') then 'F'
            end as bucket
        ,case when datediff(checkin_date,dt)=0 or datediff(checkin_date,dt)=-1 then 'T0'
            else 'TN' 
        end as check_type
        ,case when pmod(datediff(checkin_date,'1900-01-08'),7)+1 in (5,6) then '周末'
            when pmod(datediff(checkin_date,'1900-01-08'),7)+1 not in (5,6) then '周中'
        end as week_type
        ,get_json_object(server_log,'$.usePeekClassScore') as peek
        ,get_json_object(server_log,'$.houseClassScene') as peek_house_type
        ,get_json_object(server_log,'$.releaseCpcDistance') as  peek_distance_type
        ,case 
        when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
        when get_json_object(server_log,'$.searchScene') = 2 then '空搜'
        when get_json_object(server_log,'$.searchScene') = 3 then '景区地区'
        when get_json_object(server_log,'$.searchScene') in(4,8) then '行政区'
        when get_json_object(server_log,'$.searchScene') = 5 then '地标'
        when get_json_object(server_log,'$.searchScene') = 6 then '定位' 
        when get_json_object(server_log,'$.searchScene') = 0 then '无' 
        end as search_type

        ,case when bedroom_count=1 then '一居'
            when bedroom_count=2 then '二居'
            when bedroom_count>=3 then '三居+'
        end as room_type
        ,case when logic_bit & 2048 = 2048 then 1 
            else 0 end as position_type
         ,without_risk_access_order_gmv AS without_risk_order_gmv
        ,without_risk_access_order_num as without_risk_order_num
        ,without_risk_access_order_room_night as without_risk_order_room_night
      from dws.dws_path_ldbo_d
      where dt between date_sub(current_date,14) and date_sub(current_date,1)
        and ((wrapper_name in ('途家','携程','去哪儿') and source = '102')) 
        and user_type = '用户'
      ) as a 
      join 
    house_info as b 
    on 
    a.dt = b.dt and 
    a.house_id = b.house_id
    left join
(select
    day_date
    ,festival_name
    ,day_type
    ,is_weekend
  from pdb_analysis_c.ads_flow_dim_date_info_d
) as t1
on a.checkout_date=t1.day_date
)
,ord as 
(select
    a.*
    ,b.dynamic_business_id
    ,b.house_class 
     ,case when day_type='节假日' or day_type='暑假' then '节假日' else '非节假日' end as is_holiday
    from 
      (select 
        case   
        when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
        when terminal_type_name = '本站-APP' then '途家'
        when terminal_type_name = '携程-APP' then '携程'
        when terminal_type_name = '去哪儿-APP' then '去哪儿'
        end as wrapper_name
        ,city_id
        ,case when city_name = '陵水' then '陵水(三亚)' else city_name end as city_name 
        ,create_date as dt
        ,house_id
        ,uid
        ,order_no
        ,room_total_amount
        ,order_room_night_count
        ,dynamic_business
        ,checkout_date
        ,case when datediff(checkin_date,create_date)=0 or datediff(checkin_date,create_date)=-1 then 'T0'
            else 'TN' 
        end as check_type
      from dws.dws_order 
      where create_date between date_sub(current_date,14) and date_sub(current_date,1)
        and is_paysuccess_order = 1 
        and terminal_type_name in ('携程-APP','去哪儿-APP','本站-APP') 
        and is_risk_order = 0 
        and is_overseas = 0 
      ) as a 
     join  house_info as b    
     on 
     a.dt = b.dt and 
     a.house_id = b.house_id
     left join
(select
    day_date
    ,festival_name
    ,day_type
    ,is_weekend
  from pdb_analysis_c.ads_flow_dim_date_info_d
) as t1
on a.checkout_date=t1.day_date
)
select 
    7d_week.city_id
    ,7d_week.city_name
    ,7d_week.dynamic_business_id
    ,t2.dynamic_business
    ,search_type

    ,(7d_week.l4_cr*0.5+7d_week.l4_gmv_cr*0.5) as l4_jiaquancr
    ,(7d_week.l3_cr*0.5+7d_week.l3_gmv_cr*0.5) as l3_jiaquancr
    ,(7d_week.l34_cr*0.5+7d_week.l34_gmv_cr*0.5) as l34_jiaquancr
    ,(7d_week.l25_cr*0.5+7d_week.l25_gmv_cr*0.5) as l25_jiaquancr
    ,(7d_week.l21_cr*0.5+7d_week.l21_gmv_cr*0.5) as l21_jiaquancr
    ,(7d_week.L25L34_cr*0.5+7d_week.L25L34_gmv_cr*0.5) as L25L34_jiaquancr
    ,(7d_week.l3_cr*0.5+7d_week.l3_gmv_cr*0.5)-(7d_week.l4_cr*0.5+7d_week.l4_gmv_cr*0.5) as l3_4_jiaquancr_diff
    ,(7d_week.l25_cr*0.5+7d_week.l25_gmv_cr*0.5)-(7d_week.l34_cr*0.5+7d_week.l34_gmv_cr*0.5) as l25_34_jiaquancr_diff
    ,(7d_week.l21_cr*0.5+7d_week.l21_gmv_cr*0.5)-(7d_week.L25L34_cr*0.5+7d_week.L25L34_gmv_cr*0.5) as l21_2534_jiaquancr_diff
    ,l34ord_z
    ,l4ord_z
    ,l3ord_z
    ,l25ord_z
    ,L25L34ord_z
    ,l21ord_z
from 

(
    select bb.city_name
    ,bb.city_id
    ,bb.dynamic_business_id
    ,search_type
    ,(l34ord_z/ord_z)/(l34pv/pv) as l34_cr
    ,(l34gmv_z/gmv_z)/(l34pv/pv) as l34_gmv_cr
    ,(l4ord_z/ord_z)/(l4pv/pv) as l4_cr
    ,(l4gmv_z/gmv_z)/(l4pv/pv) as l4_gmv_cr
    ,(l3ord_z/ord_z)/(l3pv/pv) as l3_cr
    ,(l3gmv_z/gmv_z)/(l3pv/pv) as l3_gmv_cr
    ,(l25ord_z/ord_z)/(l25pv/pv) as l25_cr
    ,(l25gmv_z/gmv_z)/(l25pv/pv) as l25_gmv_cr
    ,(L25L34ord_z/ord_z)/(L25L34pv/pv) as L25L34_cr
    ,(L25L34gmv_z/gmv_z)/(L25L34pv/pv) as L25L34_gmv_cr
    ,(l21ord_z/ord_z)/(l21pv/pv) as l21_cr
    ,(l21gmv_z/gmv_z)/(l21pv/pv) as l21_gmv_cr

    ,l34ord_z
    ,l4ord_z
    ,l3ord_z
    ,l25ord_z
    ,L25L34ord_z
    ,l21ord_z

from 
(
    select 
    city_id
    ,aa.city_name
    ,dynamic_business_id
    ,search_type
    ,count(distinct concat(dt,uid)) as uv
    ,count(uid) as pv
    ,count(if(house_class in ('L3'),uid,null)) as l3pv
    ,count(if(house_class in ('L4'),uid,null)) as l4pv
    ,count(if(house_class in ('L25'),uid,null)) as l25pv
    ,count(if(house_class in ('L21'),uid,null)) as l21pv
    ,count(if(house_class in ('L3','L4'),uid,null)) as l34pv
    ,count(if(house_class in ('L3','L4','L25'),uid,null)) as L25L34pv
    ,sum(without_risk_order_num) as ord_z 
    ,sum(if(house_class in ('L3'),without_risk_order_num,null)) as l3ord_z 
    ,sum(if(house_class in ('L4'),without_risk_order_num,null)) as l4ord_z 
    ,sum(if(house_class in ('L25'),without_risk_order_num,null)) as l25ord_z 
    ,sum(if(house_class in ('L21'),without_risk_order_num,null)) as l21ord_z
    ,sum(if(house_class in ('L3','L4'),without_risk_order_num,null)) as l34ord_z
    ,sum(if(house_class in ('L3','L4','L25'),without_risk_order_num,null)) as L25L34ord_z
    ,sum(without_risk_order_gmv) as gmv_z 
    ,sum(if(house_class in ('L3'),without_risk_order_gmv,null)) as l3gmv_z 
    ,sum(if(house_class in ('L4'),without_risk_order_gmv,null)) as l4gmv_z 
    ,sum(if(house_class in ('L25'),without_risk_order_gmv,null)) as l25gmv_z 
    ,sum(if(house_class in ('L21'),without_risk_order_gmv,null)) as l21gmv_z
    ,sum(if(house_class in ('L3','L4'),without_risk_order_gmv,null)) as l34gmv_z
    ,sum(if(house_class in ('L3','L4','L25'),without_risk_order_gmv,null)) as L25L34gmv_z
    from 
    (
      select *
      from list
      where search_type in ('空搜','地标','行政区','景区地区')
    ) as aa 
    group by 1,2,3
    ,4
    -- ,5
) as bb 
) as 7d_week 
left join
(
  select city_name,city_id,city_type 
  from excel_upload.40city_list
)cc 
on 7d_week.city_id = cc.city_id
left join(select
distinct 
    dynamic_business,
    dynamic_business_id
from
    dws.dws_house_d
where dt=date_sub(current_date,1)
)t2
on 7d_week.dynamic_business_id = t2.dynamic_business_id
---where 7d_week.dynamic_business_id>0
