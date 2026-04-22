select '今年' time1
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    
    ,percentile(datediff(checkin_date,create_date),0.5) date_gap_median
    ,avg(datediff(checkin_date,create_date)) date_gap_avg  
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv
    ,sum(order_room_night_count) night
from dws.dws_order 
where checkout_date between '2025-10-01' and '2025-10-08'
and is_cancel_order	= 0
and is_paysuccess_order = 1 
and is_done = 1 
and is_overseas = 1 
group by 1,2
union all 
select '去年' time1
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    
    ,percentile(datediff(checkin_date,create_date),0.5) date_gap_median
    ,avg(datediff(checkin_date,create_date)) date_gap_avg  
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv
    ,sum(order_room_night_count) night
from dws.dws_order 
where checkout_date between '2024-10-01' and '2024-10-08' 
and is_cancel_order	= 0
and is_paysuccess_order = 1 
and is_done = 1 
and is_overseas = 1 
group by 1,2