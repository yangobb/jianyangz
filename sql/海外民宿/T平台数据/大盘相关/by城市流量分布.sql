select '今年' time1 
    ,weekofyear(dt) week_create
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,case when datediff(checkin_date,dt) = 0 then 'T0'
        when datediff(checkin_date,dt) between 1 and 7 then 'T7'
        when datediff(checkin_date,dt) between 8 and 14 then 'T14'
        when datediff(checkin_date,dt) between 15 and 21 then 'T21'
        when datediff(checkin_date,dt) between 22 and 28 then 'T28'
        when datediff(checkin_date,dt) between 29 and 60 then 'T60'
        else 'T61' end date_gap
    
    ,count(1) lpv 
    ,count(distinct dt,uid) luv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night
    ,sum(without_risk_order_gmv) gmv
from dws.dws_path_ldbo_d
where dt between '2025-11-03' and '2025-12-21'
and is_oversea = 1 
and wrapper_name in ('途家','携程','去哪儿') 
and source = '102' 
and user_type = '用户' 
group by 1,2,3,4
union all 

select '去年' time1 
    ,weekofyear(dt) week_create
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,case when datediff(checkin_date,dt) = 0 then 'T0'
        when datediff(checkin_date,dt) between 1 and 7 then 'T7'
        when datediff(checkin_date,dt) between 8 and 14 then 'T14'
        when datediff(checkin_date,dt) between 15 and 21 then 'T21'
        when datediff(checkin_date,dt) between 22 and 28 then 'T28'
        when datediff(checkin_date,dt) between 29 and 60 then 'T60'
        else 'T61' end date_gap
    
    ,count(1) lpv 
    ,count(distinct dt,uid) luv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night
    ,sum(without_risk_order_gmv) gmv
from dws.dws_path_ldbo_d
where dt between '2025-11-04' and '2025-12-22'
and is_oversea = 1 
and wrapper_name in ('途家','携程','去哪儿') 
and source = '102' 
and user_type = '用户' 
group by 1,2,3,4 