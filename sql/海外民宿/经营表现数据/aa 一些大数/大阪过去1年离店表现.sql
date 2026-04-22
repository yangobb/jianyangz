-- 大阪过去1年离店订单表现（按月 + 合计）
-- 口径：checkout_date 滚动过去12个月，海外离店成单（不限渠道）

select substr(checkout_date,1,7) month1
    ,count(distinct order_no) order_cnt
    ,sum(order_room_night_count) night
    ,sum(room_total_amount) gmv
from dws.dws_order
where checkout_date between date_sub(current_date,365) and date_sub(current_date,1)
and city_name = '大阪'
and is_cancel_order = 0
and is_paysuccess_order = 1
and is_done = 1
and is_overseas = 1
group by 1
union all
select '合计' month1
    ,count(distinct order_no) order_cnt
    ,sum(order_room_night_count) night
    ,sum(room_total_amount) gmv
from dws.dws_order
where checkout_date between date_sub(current_date,365) and date_sub(current_date,1)
and city_name = '大阪'
and is_cancel_order = 0
and is_paysuccess_order = 1
and is_done = 1
and is_overseas = 1
order by 1
