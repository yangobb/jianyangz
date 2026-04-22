
 
select case when create_date between date_sub(current_date,56) and date_sub(current_date,50) then 'W8'
        when create_date between date_sub(current_date,49) and date_sub(current_date,43) then 'W7'
        when create_date between date_sub(current_date,42) and date_sub(current_date,36) then 'W6'
        when create_date between date_sub(current_date,35) and date_sub(current_date,29) then 'W5'
        when create_date between date_sub(current_date,28) and date_sub(current_date,22) then 'W4'
        when create_date between date_sub(current_date,21) and date_sub(current_date,15) then 'W3'
        when create_date between date_sub(current_date,14) and date_sub(current_date,8) then 'W2'
        when create_date between date_sub(current_date,7) and date_sub(current_date,1) then 'W1'
        end week1 
    ,h.country_name
    ,h.house_city_name
    ,h.dynamic_business
    ,h.bedroom_count
    -- ,case when datediff(checkin_date,create_date) <= 6 then '0~7' 
    --     when datediff(checkin_date,create_date) between 7 and 29 then '7~30'
    --     else '30+' end `预定类型`
    ,datediff(checkin_date,create_date) `预定天数`
    ,count(distinct od.house_id) `房屋数`
    ,count(order_no) `订单数`
    ,count(distinct uid) `用户`
    ,sum(room_total_amount) `销售额`
    ,sum(order_room_night_count) `间夜`

from dws.dws_order od 
inner join (
    select dt
        ,house_id 
        ,hotel_id
        ,hotel_name
        ,case when country_name in ('中国大陆','日本','泰国','马来西亚','韩国','新加坡') then country_name else '其他' end country_name
        ,house_city_name
        ,dynamic_business
        ,dynamic_business_distance
        ,bedroom_count
        ,gross_area
        ,house_type
        ,house_class	
        ,is_fast_booking
        ,house_first_active_time
        ,picture_count
        ,cover_picture_url	
    from dws.dws_house_d
    where dt = date_sub(current_date, 1)
    -- AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1
)h 
on od.house_id = h.house_id
where create_date between date_sub(current_date,56) and date_sub(current_date,1)
and is_paysuccess_order = 1 
and is_overseas = 1 
and is_cancel_order  = 0 
group by 1,2,3,4,5,6  


