-- 三十天落于月份区间的逻辑计算，
-- 计算间夜占比，流量占比，间夜CR，动销率，
-- 过去180天的老房和新房（上线30天内）的表现
-- select substr(add_months(current_date,-1),1,7)
-- select concat(substr(add_months(current_date,-1),1,7),'-01')
-- select date_sub(concat(substr(add_months(current_date,-1),1,7),'-01'),29)
-- select last_day(add_months(current_date,-1))

-- 新客
-- 老客
    -- 180天内
    -- 180天外
with h as (
select dt 
    ,house_id
    ,house_first_active_time
    ,case when datediff(dt,house_first_active_time) = 29  then '满30天新房'
        when datediff(dt,house_first_active_time) < 29 then '不满30天新房'
        when datediff(dt,house_first_active_time) < 179 then '180天内老房'
        when datediff(dt,house_first_active_time) >= 179 then '180天外老房'
        else '其他' end house_type 
from dws.dws_house_d 
where substr(dt,1,7) = substr(add_months(current_date,-1),1,7)
-- and dt = '2025-07-31'
and house_is_online = 1 
and house_is_oversea = 1 
and landlord_channel = 1  
and datediff(dt,house_first_active_time) = 29
)
,create_od as (
-- 预定订单
select h.dt
    ,h.house_id 
    ,count(distinct dt) dt_cnt 
    ,count(distinct order_no) `预定订单数`
    ,sum(room_total_amount) `预定GMV`
    ,sum(order_room_night_count) `预定间夜`
from h  
left join (
    select create_date
        ,house_id
        ,order_no
        ,room_total_amount
        ,order_room_night_count
    from dws.dws_order 
    where create_date >= date_sub(concat(substr(add_months(current_date,-1),1,7),'-01'),29)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
) b 
on h.house_id = b.house_id 
and datediff(dt,create_date) <= 29 
group by 1,2
)
,checkout_od as (
-- 离店订单 
select h.dt 
    ,h.house_id
    ,count(distinct order_no) `离店订单数`
    ,sum(room_total_amount) `离店GMV`
    ,sum(order_room_night_count) `离店间夜`
from h
left join (
    select checkout_date
        ,house_id
        ,order_no
        ,room_total_amount
        ,order_room_night_count
    from dws.dws_order 
    where checkout_date >= date_sub(concat(substr(add_months(current_date,-1),1,7),'-01'),29)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_success_order = 1 
    and is_done = 1
) b 
on h.house_id = b.house_id
and datediff(dt,checkout_date) <= 29 
group by 1,2
)
,list_1 as (
-- 预定流量
select h.dt 
    ,h.house_id
    ,count(uid) `预定lpv` 
    ,count(distinct uid,b.dt) `预定luv`
from h 
left join (
    select dt 
        ,house_id
        ,uid 
    from dws.dws_path_ldbo_d 
    where dt >= date_sub(concat(substr(add_months(current_date,-1),1,7),'-01'),29)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户'  
) b 
on h.house_id = b.house_id 
and datediff(h.dt,b.dt) <= 29
group by 1,2
)
,list_2 as (
-- 离店流量
select h.dt 
    ,h.house_id
    ,count(uid) `离店lpv`
    ,count(distinct uid,b.dt) `离店luv`
from h 
left join (
    select dt 
        ,checkout_date
        ,house_id
        ,uid 
    from dws.dws_path_ldbo_d 
    where dt >= date_sub(concat(substr(add_months(current_date,-1),1,7),'-01'),29)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) b 
on h.house_id = b.house_id 
and datediff(h.dt,b.dt) <= 29
and datediff(b.checkout_date,h.house_first_active_time) <= 29
group by 1,2 
)


select 
    -- h.house_id
    -- ,h.house_first_active_time,
    h.house_type
    ,count(distinct h.house_id) `房屋数`
    ,sum(`预定订单数`) `预定订单数`
    ,sum(`预定GMV`) `预定GMV`
    ,sum(`预定间夜`) `预定间夜`
    ,sum(`离店订单数`) `离店订单数`
    ,sum(`离店GMV`) `离店GMV`
    ,sum(`离店间夜`) `离店间夜`
    ,sum(`预定lpv`) `预定lpv`
    ,sum(`预定luv`) `预定luv`
    ,sum(`离店lpv`) `离店lpv`
    ,sum(`离店luv`) `离店luv`
from (
    select * from h 
) h 
left join create_od
on h.house_id = create_od.house_id
and h.dt = create_od.dt
left join checkout_od
on h.house_id = checkout_od.house_id
and h.dt = checkout_od.dt
left join list_1
on h.house_id = list_1.house_id
and h.dt = list_1.dt
left join list_2 
on h.house_id = list_2.house_id
and h.dt = list_2.dt
where `预定lpv` != 0 
group by 1

union all

select house_type
    ,count(distinct a.house_id) `房屋数`
    ,sum(`预定订单数`) `预定订单数`
    ,sum(`预定GMV`) `预定GMV`
    ,sum(`预定间夜`) `预定间夜`
    ,sum(`离店订单数`) `离店订单数`
    ,sum(`离店GMV`) `离店GMV`
    ,sum(`离店间夜`) `离店间夜`
    ,sum(`预定lpv`) `预定lpv`
    ,sum(`预定luv`) `预定luv`
    ,sum(`离店lpv`) `离店lpv`
    ,sum(`离店luv`) `离店luv`
from (
    select house_id
        ,house_first_active_time
        ,case when datediff(dt,house_first_active_time) < 179 then '180天内老房'
            when datediff(dt,house_first_active_time) >= 179 then '180天外老房'
            else '其他' end house_type 
    from dws.dws_house_d 
    where dt = concat(substr(add_months(current_date,-1),1,7),'-01')
    -- and dt = '2025-07-31'
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 1  
    and datediff(dt,house_first_active_time) >= 30 
) a 
left join (
    select house_id
        ,count(uid) `预定lpv`
        ,count(distinct uid,dt) `预定luv`
        ,count(case when substr(checkout_date,1,7) = substr(add_months(current_date,-1),1,7) then uid end) `离店lpv`
        ,count(case when substr(checkout_date,1,7) = substr(add_months(current_date,-1),1,7) then concat(uid,dt) end) `离店luv` 
    from dws.dws_path_ldbo_d 
    where substr(dt,1,7) = substr(add_months(current_date,-1),1,7)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
    and city_name = '大阪'
    -- and landlord_channel = 1 
    group by 1 
) b
on a.house_id = b.house_id 
left join (
    select house_id
        ,count(distinct case when substr(checkout_date,1,7) = substr(add_months(current_date,-1),1,7) and is_done = 1 then order_no end) `离店订单数`
        ,sum(case when substr(checkout_date,1,7) = substr(add_months(current_date,-1),1,7) and is_done = 1 then room_total_amount end) `离店GMV`
        ,sum(case when substr(checkout_date,1,7) = substr(add_months(current_date,-1),1,7) and is_done = 1 then order_room_night_count end) `离店间夜`
        
        ,count(distinct case when substr(create_date,1,7) = substr(add_months(current_date,-1),1,7) then order_no end) `预定订单数`
        ,sum(case when substr(create_date,1,7) = substr(add_months(current_date,-1),1,7) then room_total_amount end) `预定GMV`
        ,sum(case when substr(create_date,1,7) = substr(add_months(current_date,-1),1,7) then order_room_night_count end) `预定间夜`
    from dws.dws_order 
    where (
        substr(checkout_date,1,7) = substr(add_months(current_date,-1),1,7) or 
        substr(create_date,1,7) = substr(add_months(current_date,-1),1,7)
    )
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_overseas = 1 
    and city_name = '大阪'
    and landlord_channel = 1 
    group by 1 
) c 
on a.house_id = c.house_id
group by 1