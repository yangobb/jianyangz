-- P1 - 上房数量统计
-- 规则：
-- 1. 新老房东的区分规则同上，即按照 hotel_first_active_time 计算，新房东上房和老房东上房需要分别统计；
-- 2.house_first_active_time as`发布时间，仅在当月的发布，且 house_is_online =1 字段的在线房屋为有效房屋

-- 可变规则：
-- 1.由于每月的重点不同，会基于不同地区房源给予不同权重，除house-id, name，hotel-id，name 
--      房屋的所在城市, house_city_name as`房屋城市名称`，house_type 房屋类型； avaliable_count 有效库存
-- 2.还需要计算房屋的可订天数，stay_night `最小预定天数` , stay_night_special_rule_day `X天起订`，并展示两字段

-- 城市明细
select 
    country_name `国家`
    ,city_name `城市`
    ,landlord_type `房东类型`
    ,hotel_id
    ,hotel_name
    ,hotel_first_active_time `门店首次上线时间`
    ,h.house_id
    ,house_name
    ,house_type `房屋类型`
    ,house_first_active_time `房屋首次上线时间`
    ,avaliable_count `有效库存`
    ,stay_night `最小预定天数`
    ,stay_night_special_rule_range `特殊预定天数范围`
    ,stay_night_special_rule_day `X天起订`
from (
    select
        country_name
        ,house_city_name city_name
        ,case when hotel_first_active_time >= date_sub(date_trunc('MM', date_sub(current_date, 1)),59) and hotel_id not in (30263128,30281782,30447850) then '新房东' else '老房东' end landlord_type
        ,to_date(hotel_first_active_time) hotel_first_active_time
        ,hotel_id
        ,hotel_name
        ,to_date(house_first_active_time) house_first_active_time
        ,house_id
        ,house_name
        ,house_type 
        ,avaliable_count
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1 
    and house_first_active_time between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1)
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
order by 
    case when country_name = '泰国' then 1 
        when country_name = '日本' then 2
        else 3
        end
    ,case when city_name = '曼谷' then 1
        when city_name = '芭堤雅' then 2
        when city_name = '普吉岛' then 3
        when city_name = '清迈' then 4
        when city_name = '大阪' then 5
        when city_name = '东京' then 6
        when city_name = '京都' then 7
        when city_name = '其他' then 9
        else 8
        end,
    landlord_type  
