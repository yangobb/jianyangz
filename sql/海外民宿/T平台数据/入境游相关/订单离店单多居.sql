with od_checkout as (
select a.*
    ,country_Code
    ,case when bedroom_count = 1 then '1' when bedroom_count > 1 then '2' else '0' end bedroom_count
    ,case when weekofyear(checkout_date) = 1 and month(checkout_date) = 12 then concat(year(checkout_date) + 1,'-W',weekofyear(checkout_date))
            when weekofyear(checkout_date) >= 52 and month(checkout_date) = 1 then concat(year(checkout_date) - 1,'-W',weekofyear(checkout_date))
            else concat(year(checkout_date),'-W',weekofyear(checkout_date))
          END year_week
from (
    select 
         case when terminal_type_name like '%携程%' then 'C'
            when terminal_type_name like '%去哪儿%' then 'Q'
            when terminal_type_name like '%去哪儿%' then 'T'
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
    ,country_Code
    ,sum(night) `总计间夜`
    ,sum(case when bedroom_count = 1 then night end) `单居间夜`
    ,sum(case when bedroom_count = 1 then gmv end) `单居gmv`
    ,sum(case when bedroom_count = 1 then gmv end) / sum(case when bedroom_count = 1 then night end) `单居adr`
    
    ,sum(case when bedroom_count = 2 then night end) `多居间夜`
    ,sum(case when bedroom_count = 2 then gmv end) `多居gmv`
    ,sum(case when bedroom_count = 2 then gmv end) / sum(case when bedroom_count = 2 then night end) `多居adr`
from od_checkout 
group by 1,2 
order by `总计间夜` desc 
