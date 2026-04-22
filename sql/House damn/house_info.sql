with h as (
select distinct house_id
    ,house_name
    ,hotel_id
    ,house_city_id
    ,house_city_name
    ,house_is_online
    ,recommended_guest
    ,dynamic_business_id
    ,dynamic_business
    ,great_tag
    ,house_class
    ,house_first_active_time
    ,case when house_class in ('L21','L1') then '低'
        when house_class in ('L25') then '中'
        when house_class in ('L3','L4') then '高'
        end as `房屋等级`
    ,case when year(house_first_active_time) >= 2023 then '新'
        when year(house_first_active_time) < 2023 then '旧'
        end as `上房时间`
from dws.dws_house_d
where dt = date_sub(current_date,1)
AND house_is_oversea = 0 --国内
and hotel_is_oversea = 0
and great_tag = 1 
and landlord_channel = 1 
),
