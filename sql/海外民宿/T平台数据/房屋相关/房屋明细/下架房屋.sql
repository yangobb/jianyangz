 

select b.*
    -- ,a.house_id
    ,a.house_first_active_time `首次上架时间`
    ,a.dt `首次下架时间`
    ,datediff(a.dt,a.house_first_active_time) `上架天数`
from (
    select a.house_id
        ,house_first_active_time
        ,min(dt) dt 
    from dws.dws_house_d a 
    inner join (
        select house_id
        from dws.dws_house_d 
        where dt = date_sub(current_date,1)
        and landlord_channel_name = '平台商户' 
        and house_is_oversea = 1 
        and hotel_is_oversea = 1 
        and house_is_online = 0    
    ) b 
    on a.house_id = b.house_id
    where dt >= '2025-01-01'
    and house_is_oversea = 1 
    and hotel_is_oversea = 1 
    and house_is_online = 0 
    and landlord_channel_name = '平台商户'
    group by 1,2 
    having min(dt) != '2025-01-01'
    and min(dt) between date_sub(current_date,7) and date_sub(current_date,1)
) a 
join (
    -- 房屋
    select dt `取数日期`
        ,house_id 
        ,hotel_id
        ,hotel_name
        ,country_name `国家`
        ,house_city_name `城市`
        ,dynamic_business `商圈`
        ,dynamic_business_distance `商圈距离`
        ,bedroom_count `居室`

        ,house_type `房屋类型`
        ,house_class `房屋等级`
        ,is_fast_booking `闪订`
        ,house_first_active_time `房屋上架时间`
        ,picture_count `图片数`
        ,cover_picture_url `头图`
        ,share_type `共享类型`
        ,gross_area `面积`
        ,hotel_first_active_time `门店上架时间`
    from dws.dws_house_d
    where dt = date_sub(current_date, 1)
    AND landlord_channel_name = '平台商户'
) b 
on a.house_id = b.house_id 
