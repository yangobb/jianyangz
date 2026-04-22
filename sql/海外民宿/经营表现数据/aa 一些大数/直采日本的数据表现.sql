

-- 在线房屋数
select concat(substr(dt,1,4),'-W',weekofyear(dt)) week1 
    ,bedroom_count
    ,count(distinct house_id) house_cnt 
from dws.dws_house_d 
where dt between '2023-03-01' and date_sub(current_date,1)
and country_name = '日本'
and house_is_online = 1 
and landlord_channel = 1 
group by 1,2
union all 
select concat(substr(dt,1,4),'-W',weekofyear(dt)) week1 
    ,'一户建' bedroom_count
    ,count(distinct house_id) house_cnt 
from dws.dws_house_d 
where dt between '2023-03-01' and date_sub(current_date,1)
and country_name = '日本'
and house_is_online = 1 
and landlord_channel = 1 
and house_type = '一户建'
group by 1,2

-- 离店订单数据
-- select concat(substr(checkout_date,1,4),'-W',weekofyear(checkout_date)) week1 
--     ,'一户建' house_type
--     ,count(distinct b.house_id) house_cnt_sale 
--     ,count(distinct order_no) order_cnt 
--     ,sum(room_total_amount) gmv 
--     ,sum(order_room_night_count) night   
-- from (
--     select bedroom_count
--         ,house_id
--     from dws.dws_house_d 
--     where dt = date_sub(current_date,1)
--     and country_name = '日本'
--     and house_is_online = 1 
--     and landlord_channel = 1 
--     and house_type = '一户建'
-- )  a 
-- left join (
--     select house_id
--         ,checkout_date
--         ,order_no 
--         ,room_total_amount 
--         ,order_room_night_count  
--     from dws.dws_order 
--     where checkout_date between '2023-03-01' and date_sub(current_date,1)
--     and is_paysuccess_order = 1 
--     and is_cancel_order = 0 
--     and is_done = 1 
-- ) b 
-- on a.house_id = b.house_id
-- group by 1,2 
-- union all 
-- select concat(substr(checkout_date,1,4),'-W',weekofyear(checkout_date)) week1 
--     ,bedroom_count house_type
--     ,count(distinct b.house_id) house_cnt_sale 
--     ,count(distinct order_no) order_cnt 
--     ,sum(room_total_amount) gmv 
--     ,sum(order_room_night_count) night   
-- from (
--     select bedroom_count
--         ,house_id
--     from dws.dws_house_d 
--     where dt = date_sub(current_date,1)
--     and country_name = '日本'
--     and house_is_online = 1 
--     and landlord_channel = 1  
-- )  a 
-- left join (
--     select house_id
--         ,checkout_date
--         ,order_no 
--         ,room_total_amount 
--         ,order_room_night_count  
--     from dws.dws_order 
--     where checkout_date between '2023-03-01' and date_sub(current_date,1)
--     and is_paysuccess_order = 1 
--     and is_cancel_order = 0 
--     and is_done = 1 
-- ) b 
-- on a.house_id = b.house_id
-- group by 1,2 

-- 流量数据
-- select concat(substr(dt,1,4),'-W',weekofyear(dt)) week1
--     ,bedroom_count
--     ,count(distinct uid) luv 
--     ,count(distinct detail_uid) duv 
--     ,count(distinct order_uid) oid 
    
--     ,sum(without_risk_order_num) order_num 
--     ,sum(without_risk_order_room_night) night 
--     ,sum(without_risk_order_gmv) gmv 
-- from (
--     select bedroom_count
--         ,house_id
--     from dws.dws_house_d 
--     where dt = date_sub(current_date,1)
--     and country_name = '日本'
--     and house_is_online = 1 
--     and landlord_channel = 1  
-- )  a 
-- left join (
--     select house_id
--         ,dt
--         ,uid 
--         ,detail_uid 
--         ,order_uid
--         ,without_risk_order_num
--         ,without_risk_order_room_night
--         ,without_risk_order_gmv
--     from dws.dws_path_ldbo_d
--     where dt between '2024-03-01' and date_sub(current_date,1)
--     and is_oversea = 1 
--     AND source = 102
--     AND user_type = '用户'  
--     and nvl(user_id,0) != 0
--     and house_id is not null 
-- ) c 
-- on a.house_id = c.house_id
-- group by 1,2 
-- union all 
-- select concat(substr(dt,1,4),'-W',weekofyear(dt)) week1
--     ,'一户建' bedroom_count
--     ,count(distinct uid) luv 
--     ,count(distinct detail_uid) duv 
--     ,count(distinct order_uid) oid 
    
--     ,sum(without_risk_order_num) order_num 
--     ,sum(without_risk_order_room_night) night 
--     ,sum(without_risk_order_gmv) gmv 
-- from (
--     select bedroom_count
--         ,house_id
--     from dws.dws_house_d 
--     where dt = date_sub(current_date,1)
--     and country_name = '日本'
--     and house_is_online = 1 
--     and landlord_channel = 1  
--     and house_type = '一户建'
-- )  a 
-- left join (
--     select house_id
--         ,dt
--         ,uid 
--         ,detail_uid 
--         ,order_uid
--         ,without_risk_order_num
--         ,without_risk_order_room_night
--         ,without_risk_order_gmv
--     from dws.dws_path_ldbo_d
--     where dt between '2024-03-01' and date_sub(current_date,1)
--     and is_oversea = 1 
--     AND source = 102
--     AND user_type = '用户'  
--     and nvl(user_id,0) != 0
--     and house_id is not null 
-- ) c 
-- on a.house_id = c.house_id
-- group by 1,2 

