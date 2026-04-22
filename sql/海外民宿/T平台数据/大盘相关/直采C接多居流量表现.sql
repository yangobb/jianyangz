select 
    case when city_name in ('曼谷','普吉岛','清迈','芭堤雅') then city_name else '其他' end city_name
    ,week1
    ,B.landlord_channel
    ,B.bedroom_count
    ,count(1) lpv 
    ,count(distinct dt,uid) luv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night
    ,sum(without_risk_order_gmv) gmv
from (
    select *
        ,weekofyear(dt) week1
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date, dayofweek(current_date) + 33) and date_sub(current_date, dayofweek(current_date) - 1)
    and is_oversea = 1 
    and wrapper_name in ('途家','携程','去哪儿') 
    and source = '102' 
    and user_type = '用户' 
) a 
inner join (
    select house_id 
        ,case when landlord_channel = 1 then '直采' else 'C接' end landlord_channel
    ,case when bedroom_count = 1 then '一居' else '多居' end bedroom_count
    from dws.dws_house_d 
    where dt = date_sub(current_date,1) 
    and house_is_online = 1 
    and house_is_oversea = 1 
    and country_name = '泰国'
) b 
on a.house_id = b.house_id
group by 1,2,3,4