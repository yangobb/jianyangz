select * from (
select 
    a.country_name `国家`
    ,'国家汇总' `城市`
    ,a.landlord_type `房东类型`
    ,nvl(`离店订单数`,0) `离店订单数`
    ,nvl(`离店销售额`,0) `离店销售额`
    ,nvl(`离店间夜数`,0) `离店间夜数`
    ,nvl(`上线房屋数`,0) `上线房屋数`
from (
    select
        case when country_name in ('日本','泰国') then country_name else '其他' end country_name
        ,landlord_type 
        ,count(distinct order_no) `离店订单数`
        ,sum(gmv) `离店销售额`
        ,sum(night) `离店间夜数`
    from (
        select
            country_name
            ,house_city_name
            ,case when hotel_first_active_time >= date_sub(to_date(date_trunc('MM', date_sub(current_date, 1))),59) and hotel_id not in (30263128,30281782,30447850) then '新房东' else '老房东' end landlord_type
            ,to_date(hotel_first_active_time) hotel_first_active_time
            ,hotel_id
            ,hotel_name
            ,house_id
            ,house_name
        from dws.dws_house_d
        where dt = last_day(add_months(current_date,-1))
        AND landlord_channel_name = '平台商户'
        and house_is_online = 1 
        AND house_is_oversea = 1 
        and country_name in ('泰国','日本')
    ) h 
    left join (
        select house_id 
            ,order_no
            ,room_total_amount gmv 
            ,order_room_night_count night 
        from dws.dws_order 
        where checkout_date between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1)
        and is_paysuccess_order = 1 
        and is_cancel_order = 0 
        and is_done = 1 
    ) o 
    on h.house_id = o.house_id
    group by 1,2
) a 
left join (
    select 
        case when country_name in ('日本','泰国') then country_name else '其他' end country_name
        ,landlord_type 
        ,count(distinct h.house_id) `上线房屋数`
    from (
        select
            country_name
            ,house_city_name
            ,case when hotel_first_active_time >= date_sub(trunc(add_months(current_date, -1), 'MM'),59) then '新房东' else '老房东' end landlord_type
            ,to_date(hotel_first_active_time) hotel_first_active_time
            ,hotel_id
            ,hotel_name
            ,house_id
            ,house_name
            ,avaliable_count
        from dws.dws_house_d
        where dt = last_day(add_months(current_date,-1))
        AND landlord_channel_name = '平台商户'
        and house_is_online = 1 
        AND house_is_oversea = 1 
        and house_first_active_time between trunc(add_months(current_date, -1), 'MM') and last_day(add_months(current_date,-1))
    ) h 
    left join (
        select
            prd.unit_id house_id
            ,stay_night
            ,stay_night_special_rule_range
            ,stay_night_special_rule_day
        from ods_tns_product.product_shard prd
        join ods_tns_baseinfo.house_search hs 
        on hs.house_id = prd.unit_id
    ) p 
    on h.house_id = p.house_id
    group by 1,2
) b 
on a.country_name = b.country_name 
and a.landlord_type = b.landlord_type 
order by 
    case when `国家` = '泰国' then 1 
         when `国家` = '日本' then 2
         end 
    ,`房东类型`  
) a 
union all 
select * from (
select 
    a.country_name `国家`
    ,a.city_name `城市`
    ,a.landlord_type `房东类型`
    ,nvl(`离店订单数`,0) `离店订单数`
    ,nvl(`离店销售额`,0) `离店销售额`
    ,nvl(`离店间夜数`,0) `离店间夜数`
    ,nvl(`上线房屋数`,0) `上线房屋数`
from (
    select
        case when country_name in ('日本','泰国') then country_name else '其他' end country_name
        ,case when house_city_name in ('大阪','东京','京都','曼谷','芭堤雅','清迈','普吉岛') then house_city_name else '其他' end city_name
        ,landlord_type 
        ,count(distinct order_no) `离店订单数`
        ,sum(gmv) `离店销售额`
        ,sum(night) `离店间夜数`
    from (
        select
            country_name
            ,house_city_name
            ,case when hotel_first_active_time >= date_sub(to_date(date_trunc('MM', date_sub(current_date, 1))),59) and hotel_id not in (30263128,30281782,30447850) then '新房东' else '老房东' end landlord_type
            ,to_date(hotel_first_active_time) hotel_first_active_time
            ,hotel_id
            ,hotel_name
            ,house_id
            ,house_name
        from dws.dws_house_d
        where dt = last_day(add_months(current_date,-1))
        AND landlord_channel_name = '平台商户'
        and house_is_online = 1 
        AND house_is_oversea = 1 
    ) h 
    left join (
        select house_id 
            ,order_no
            ,room_total_amount gmv 
            ,order_room_night_count night 
        from dws.dws_order 
        where checkout_date between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1)
        and is_paysuccess_order = 1 
        and is_cancel_order = 0 
        and is_done = 1 
    ) o 
    on h.house_id = o.house_id
    group by 1,2,3
) a 
left join (
    select 
        case when country_name in ('日本','泰国') then country_name else '其他' end country_name
        ,case when house_city_name in ('大阪','东京','京都','曼谷','芭堤雅','清迈','普吉岛') then house_city_name else '其他' end city_name
        ,landlord_type 
        ,count(distinct h.house_id) `上线房屋数`
    from (
        select
            country_name
            ,house_city_name
            ,case when hotel_first_active_time >= date_sub(trunc(add_months(current_date, -1), 'MM'),59) then '新房东' else '老房东' end landlord_type
            ,to_date(hotel_first_active_time) hotel_first_active_time
            ,hotel_id
            ,hotel_name
            ,house_id
            ,house_name
            ,avaliable_count
        from dws.dws_house_d
        where dt = last_day(add_months(current_date,-1))
        AND landlord_channel_name = '平台商户'
        and house_is_online = 1 
        AND house_is_oversea = 1 
        and house_first_active_time between trunc(add_months(current_date, -1), 'MM') and last_day(add_months(current_date,-1))
    ) h 
    left join (
        select
            prd.unit_id house_id
            ,stay_night
            ,stay_night_special_rule_range
            ,stay_night_special_rule_day
        from ods_tns_product.product_shard prd
        join ods_tns_baseinfo.house_search hs 
        on hs.house_id = prd.unit_id
    ) p 
    on h.house_id = p.house_id
    group by 1,2,3 
) b 
on a.country_name = b.country_name 
and a.city_name = b.city_name 
and a.landlord_type = b.landlord_type 
order by 
    case when `国家` = '泰国' then 1 
         when `国家` = '日本' then 2
         when `国家` = '其他' then 3 
         end
    ,case when `城市` = '曼谷' then 1
          when `城市` = '芭堤雅' then 2
          when `城市` = '普吉岛' then 3
          when `城市` = '清迈' then 4
          when `城市` = '大阪' then 5
          when `城市` = '东京' then 6
          when `城市` = '京都' then 7
          when `城市` = '其他' then 8
          end
    ,`房东类型`  
) a 