-- 主要是基于枫叶季的一些比较分析和预测，
-- 1. 11.15-12.15之间的流量分布和间夜分布（去年的和今年预订的）；
-- 2. 枫叶季流量的提前预定周期和时间（时间对比）； 
-- 3. 东京大阪京都的商圈表现情况，单多居的表现，一户建和公寓的表现（去年即可）；ADR表现（去年表现）


-- 流量表现
select '今年' time1
    ,'途家' bu_type  
    ,wrapper_name 
    ,concat('W',weekofyear(dt)) week1
    ,case when city_name in ('东京','大阪','京都') then city_name else '其他' end city_name
    ,count(1) lpv 
    ,count(distinct uid) luv 
    ,count(distinct case when detail_uid is not null then uid end) duv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_order_gmv) gmv 
from dws.dws_path_ldbo_d 
where dt between '2025-05-19' and '2025-12-15' 
and checkout_date between '2025-11-15' and '2025-12-15' 
and is_oversea = 1
and wrapper_name in ('携程','途家','去哪儿') 
and user_type = '用户' 
and city_name in ('东京','大阪','京都')
group by 1,2,3,4,5
union all 
select '去年' time1
    ,'途家' bu_type  
    ,wrapper_name 
    ,concat('W',weekofyear(dt)) week1
    ,case when city_name in ('东京','大阪','京都') then city_name else '其他' end city_name
    ,count(1) lpv 
    ,count(distinct uid) luv 
    ,count(distinct case when detail_uid is not null then uid end) duv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_order_gmv) gmv 
from dws.dws_path_ldbo_d 
where dt between '2024-05-19' and '2024-12-15' 
and checkout_date between '2024-11-15' and '2024-12-15' 
and is_oversea = 1
and wrapper_name in ('携程','途家','去哪儿') 
and user_type = '用户' 
and city_name in ('东京','大阪','京都')
group by 1,2,3,4,5

-- 订单表现
select '今年' time1
    ,'途家' bu_type  
    ,'离店' od_type
    ,concat('W',weekofyear(create_date)) week1
    ,case when city_name in ('东京','大阪','京都') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv
    ,sum(order_room_night_count) night
from dws.dws_order 
where checkout_date between '2025-11-15' and '2025-12-15' 
and city_name in ('东京','大阪','京都')
and is_overseas = 1
and is_paysuccess_order = 1 
and is_cancel_order = 0 
group by 1,2,3,4,5
union all 
select '去年' time1
    ,'途家' bu_type  
    ,'离店' od_type
    ,concat('W',weekofyear(create_date)) week1
    ,case when city_name in ('东京','大阪','京都') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv
    ,sum(order_room_night_count) night
from dws.dws_order 
where checkout_date between '2024-11-15' and '2024-12-15' 
and city_name in ('东京','大阪','京都')
and is_overseas = 1
and is_paysuccess_order = 1 
and is_cancel_order = 0 
group by 1,2,3,4,5