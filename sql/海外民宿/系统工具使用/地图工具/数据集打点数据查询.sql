
-- 底表分类
select case when geo_position_type = 0 then '未知类型'
        when geo_position_type = 1 then '地铁站'
        when geo_position_type = 2 then '机场'
        when geo_position_type = 3 then '高校'
        when geo_position_type = 4 then '火车站'
        when geo_position_type = 5 then '观光景点'
        when geo_position_type = 6 then '汽车站'
        when geo_position_type = 7 then '地铁线路'
        when geo_position_type = 8 then '商圈'
        when geo_position_type = 9 then '郊游景点'
        when geo_position_type = 10 then '医院'
        when geo_position_type = 12 then '行政区'
        when geo_position_type = 13 then '地标'
        when geo_position_type = 14 then '地图'
        when geo_position_type = 15 then '大地标'
        when geo_position_type = 16 then '道路'
        when geo_position_type = 17 then '购物'
        when geo_position_type = 18 then '机构'
        when geo_position_type = 19 then '码头'
        when geo_position_type = 20 then '小区'
        when geo_position_type = 21 then '学校'
        when geo_position_type = 22 then '娱乐'
        when geo_position_type = 23 then '携程地标'
        when geo_position_type = 24 then '携程景区'
        when geo_position_type = 99 then '我的附近'
        when geo_position_type = 100 then '携程商圈'
        end geo_position_type_name 
    ,*
from ods_geo_landmark.geo_position
where city_name = '首尔'
and geo_position_type = 1 

-- 地标与房屋匹配
select b.geo_position_id
    ,c.house_id
    ,sum(room_total_amount) gmv 
    ,sum(order_room_night_count) nights
    ,count(distinct order_no) order_num 
from (
    select geo_position_id
    from ods_geo_landmark.geo_position
    where city_name = '首尔'
    and geo_position_type = 1 
) a 
join (
    select *
    from dws.dws_house_distance_recall 
    where in_gooddis = 1
) b
on a.geo_position_id = b.geo_position_id 
left join (
    select *
    from dws.dws_order 
    where create_date between '2025-06-21' and '2025-09-21'
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and city_name = '首尔'
) c
on b.house_id = c.house_id
group by 1,2 

 
-- 地标类型码表
select *
from tujia_dim.dim_geo_position_type
limit 100 



-- 地图工具打点模板表1
select 
        '商户运营-海外打点' category
        ,a.house_id 
        ,house_name 
        ,longitude lng 
        ,latitude lat
        ,house_class description
        ,a.name 
        
from (
    select *
    from dws.dws_house_distance_recall 
    where in_gooddis = 1
) a 
join (
    select geo_position_id
    from ods_geo_landmark.geo_position
    where city_name = '首尔'
    and geo_position_type = 1 
) b
on a.geo_position_id = b.geo_position_id 
join (
    select *
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_city_name = '首尔'
) c 
on a.house_id = c.house_id 


-- 地图工具打点模板表2
select  '商户运营-海外打点' category
    ,house_id 
    ,house_name 
    ,longitude lng 
    ,latitude lat
    ,house_class description
from dws.dws_house_d 
where dt = date_sub(current_date,1)
and house_is_online = 1 
and house_city_name = '首尔'
-- and near_subway = 1 



-- 近地铁标签
select near_subway 
    ,count(distinct house_id) a 
from dws.dws_house_d 
where dt = date_sub(current_date,1)
and house_is_online = 1 
and house_city_name = '首尔'
group by 1 


-- 地铁站打标
select category
    ,a.house_id 
    ,house_name 
    ,lng 
    ,lat
    ,description
    ,get_json_object(subway_station_info,'$.name') station_name
from (
    select '商户运营-海外打点' category
        ,house_id 
        ,house_name 
        ,longitude lng 
        ,latitude lat
        ,house_class description
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_city_name = '首尔' 
) a 
inner join (
    select *
    from ods_report.housing_indicator
) v 
on a.house_id = v.house_id
