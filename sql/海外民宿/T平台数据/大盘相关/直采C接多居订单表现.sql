select 
    case when city_name in ('曼谷','普吉岛','清迈','芭堤雅') then city_name else '其他' end city_name 
    ,month1
    ,b.landlord_channel
    ,b.bedroom_count
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) gmv 
    ,sum(order_room_night_count) night 
from (
    select *
        ,substr(create_date,1,7) month1
    from dws.dws_order 
    where create_date between concat(substr(add_months(current_date,-1),1,8),'01') and date_sub(current_date,1)
    and substr(create_date,9,2) between '01' and substr(date_sub(current_date,1),9,2)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_overseas = 1 
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