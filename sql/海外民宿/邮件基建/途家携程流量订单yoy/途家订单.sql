
select '今年' time1
    ,'途家' bu_type 
    ,'离店' od_type 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
from dws.dws_order 
where checkout_date between date_sub(current_date,14) and date_sub(current_date,1)
and is_cancel_order	= 0
and is_done = 1
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2,3,4
union all 

select '今年' time1
    ,'途家' bu_type 
    ,'预定' od_type 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
from dws.dws_order 
where create_date between date_sub(current_date,14) and date_sub(current_date,1)
and is_cancel_order	= 0 
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2,3,4
union all 

select '去年' time1
    ,'途家' bu_type 
    ,'离店' od_type 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
from dws.dws_order 
where checkout_date between date_sub(add_months(current_date,-12),14) and date_sub(add_months(current_date,-12),1)
and is_cancel_order	= 0
and is_done = 1
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2,3,4
union all 

select '去年' time1
    ,'途家' bu_type 
    ,'预定' od_type 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
from dws.dws_order 
where create_date between date_sub(add_months(current_date,-12),14) and date_sub(add_months(current_date,-12),1)
and is_cancel_order	= 0 
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2,3,4





