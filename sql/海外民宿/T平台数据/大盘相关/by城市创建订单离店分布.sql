select '今年' time1
    ,weekofyear(create_date) week_checkout
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,case when datediff(checkin_date,create_date) = 0 then 'T0'
        when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
        when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
        when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
        when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
        when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
        else 'T61' end date_gap
    -- ,percentile(datediff(checkin_date,create_date),0.5) date_gap_median
    -- ,avg(datediff(checkin_date,create_date)) date_gap_avg  
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv
    ,sum(order_room_night_count) night
from dws.dws_order 
where create_date between '2025-11-03' and '2025-12-21'
and is_cancel_order	= 0
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2,3,4
union all 
select '去年' time1
    ,weekofyear(create_date) week_checkout
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,case when datediff(checkin_date,create_date) = 0 then 'T0'
        when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
        when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
        when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
        when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
        when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
        else 'T61' end date_gap
    -- ,percentile(datediff(checkin_date,create_date),0.5) date_gap_median
    -- ,avg(datediff(checkin_date,create_date)) date_gap_avg  
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv
    ,sum(order_room_night_count) night
from dws.dws_order 
where create_date between '2024-11-03' and '2024-12-22'
and is_cancel_order	= 0
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2,3,4

