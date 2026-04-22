with od_create as (
select a.*
    ,case when weekofyear(create_date) = 1 and month(create_date) = 12 then concat(year(create_date) + 1,'-W',weekofyear(create_date))
            when weekofyear(create_date) >= 52 and month(create_date) = 1 then concat(year(create_date) - 1,'-W',weekofyear(create_date))
            else concat(year(create_date),'-W',weekofyear(create_date))
          END year_week
from (
    select 
        case when terminal_type_name like '%携程%' then 'C'
            when terminal_type_name like '%去哪儿%' then 'Q'
            when terminal_type_name like '%本站%' then 'T'
            else '非cqt'
            end terminal_type_name 
        ,create_date
        ,case when is_overseas = 0 then '国内' when country_name in ('日本','泰国') then country_name else '其他国家' end country_name
        ,case when city_name in ('上海','广州','深圳','成都','重庆','北京','大理州','杭州','丽江','珠海','东京','大阪','京都','曼谷','普吉岛','芭堤雅','清迈') then city_name else '其他城市' end city_name
        ,order_no 
        ,room_total_amount gmv 
        ,order_room_night_count night 
    from dws.dws_order
    where create_date >= '2025-12-26' 
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
) a 
inner join (
    SELECT 
        order_no 
        ,get_json_object(order_guests, '$[0].identityCardType') AS identityCardType 
        ,country_Code
    FROM dwd.dwd_order_d  
    where dt = date_sub(current_date,1)
    and create_date >= '2025-12-26' 
    and country_Code != 86
    and get_json_object(order_guests, '$[0].identityCardType') in (2,3,4)
) b 
on a.order_no = b.order_no
)
,od_checkout as (
select a.*
    ,case when weekofyear(checkout_date) = 1 and month(checkout_date) = 12 then concat(year(checkout_date) + 1,'-W',weekofyear(checkout_date))
            when weekofyear(checkout_date) >= 52 and month(checkout_date) = 1 then concat(year(checkout_date) - 1,'-W',weekofyear(checkout_date))
            else concat(year(checkout_date),'-W',weekofyear(checkout_date))
          END year_week
from (
    select 
        case when terminal_type_name like '%携程%' then 'C'
            when terminal_type_name like '%去哪儿%' then 'Q'
            when terminal_type_name like '%本站%' then 'T'
            else '非cqt'
            end terminal_type_name
        ,checkout_date
        ,case when is_overseas = 0 then '国内' when country_name in ('日本','泰国') then country_name else '其他国家' end country_name
        ,case when city_name in ('上海','广州','深圳','成都','重庆','北京','大理州','杭州','丽江','珠海','东京','大阪','京都','曼谷','普吉岛','芭堤雅','清迈') then city_name else '其他' end city_name
        ,house_id
        ,order_no 
        ,room_total_amount gmv 
        ,order_room_night_count night 
    from dws.dws_order
    where checkout_date >= '2025-12-26' 
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_success_order = 1 
    and is_done = 1
) a 
inner join (
    SELECT 
        order_no 
        ,get_json_object(order_guests, '$[0].identityCardType') AS identityCardType 
        ,country_Code
    FROM dwd.dwd_order_d  
    where dt = date_sub(current_date,1)
    and checkout_date >= '2025-12-26' 
    and country_Code != 86
    and get_json_object(order_guests, '$[0].identityCardType') in (2,3,4)
) b 
on a.order_no = b.order_no
left join (
    select house_id 
        ,bedroom_count
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    -- and house_is_online = 1 
) c 
on a.house_id = c.house_id 
)


select year_week
    ,'总计' area_name 
    ,'预定' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_create
group by 1,2,3 

union all 
select year_week
    ,nvl(country_name,'其他国家') area_name 
    ,'预定' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_create
group by 1,2,3 

union all 
select year_week
    ,nvl(city_name,'其他城市') area_name 
    ,'预定' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_create
group by 1,2,3  

union all 
select year_week
    ,nvl(terminal_type_name,'非cqt') area_name 
    ,'预定' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_create
group by 1,2,3  


union all 
select year_week
    ,'总计' area_name 
    ,'离店' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_checkout
group by 1,2,3 

union all 
select year_week
    ,nvl(country_name,'其他国家') area_name 
    ,'离店' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_checkout
group by 1,2,3 

union all 
select year_week
    ,nvl(city_name,'其他城市') area_name 
    ,'离店' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_checkout
group by 1,2,3  

union all 
select year_week
    ,nvl(terminal_type_name,'非cqt') area_name 
    ,'离店' `订单类型`
    ,count(distinct order_no) `订单数`
    ,sum(night) `间夜数`
    ,sum(gmv) `gmv` 
from od_checkout
group by 1,2,3