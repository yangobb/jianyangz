-- 离店订单
select case when checkin_date between '2025-04-30' and '2025-05-05' then '五一'
        when checkin_date between '2025-05-31' and '2025-06-02' then '端午'
        when checkin_date between '2025-05-06' and '2025-05-30' and dayofweek(checkin_date) in (1,7) then '普通周末'
        when checkin_date between '2025-05-06' and '2025-05-30' and dayofweek(checkin_date) not in (1,7) then '普通周中' 
    end time_type 
    ,city_name
    -- ,substr(create_date,1,7) 
    ,ceil((datediff(checkin_date,create_date)+1)/30)*30 create_month
    ,count(distinct order_no) od_num_k 
    ,sum(order_room_night_count) nights_k
    ,sum(room_total_amount) gmv_k
from dws.dws_order 
-- where checkin_date between date_sub(current_date,30) and date_sub(current_date,30)
where checkin_date between '2025-04-30' and '2025-06-02'
and is_paysuccess_order = 1 
and is_cancel_order = 0 
and is_done = 1
and city_name = '大阪'
group by 1,2,3 


-- 离店流量
select case when checkin_date between '2025-04-30' and '2025-05-05' then '五一'
        when checkin_date between '2025-05-31' and '2025-06-02' then '端午'
        when checkin_date between '2025-05-06' and '2025-05-30' and dayofweek(checkin_date) in (1,7) then '普通周末'
        when checkin_date between '2025-05-06' and '2025-05-30' and dayofweek(checkin_date) not in (1,7) then '普通周中' 
        end time_type 
    ,ceil((datediff(checkin_date,dt)+1)/30)*30 create_month
    ,count(uid) lpv
    ,count(distinct uid,dt) luv 
FROM dws.dws_path_ldbo_d
WHERE dt BETWEEN date_sub('2025-06-02', 90) AND '2025-06-02'
AND source = 102
AND user_type = '用户'       
and is_oversea = 1 
and city_name = '大阪'
group by 1,2 