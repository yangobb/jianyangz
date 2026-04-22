-- 收件人：包含我，刘双，金池，左峰，博文，和建阳

-- P0 - 绩效核心 - 日常GMV统计规则：
-- 1.按照所属月份的该区域离店订单，间夜和GMV进行统计，区分新老房东，
--      新老房东定义：按照当月绩效开始日期前60天内的房东（按照门店hotel_first_active_time计算）为新房东，
--      如果超出60天之上的房东，计算为老房东。
-- 2.所需字段，house-id，house name，hotel id，hotel name，
-- 举例说明，3月份绩效，则按照3月1号往前倒数60天的时间，即1月1号，作为账号的分水岭，1月1号之前计算为老房东，1月1号计算为新房东；
-- 特例说明，在覆盖动作执行时，由于分工原因会有部分房东进行调整，比如 30281782 和 30263128 按照沟通确认的划分方式，由新房东调整为老房东。
select
    country_name `国家`
    ,city_name `城市`
    ,landlord_type `房东类型`
    ,hotel_id
    ,hotel_name
    ,hotel_first_active_time `门店首次上线时间`
    ,h.house_id
    ,house_name
    ,house_first_active_time `房屋首次上线时间`
    ,house_type `房屋类型`
    ,`离店订单数`
    ,`离店销售额`
    ,`离店间夜数`
from (
    select
        country_name
        ,house_city_name city_name
        ,case when hotel_first_active_time >= date_sub(trunc(add_months(current_date, -1), 'MM'),59) and hotel_id not in (30263128,30281782,30447850) then '新房东' else '老房东' end landlord_type
        ,to_date(hotel_first_active_time) hotel_first_active_time
        ,to_date(house_first_active_time) house_first_active_time
        ,hotel_id
        ,hotel_name
        ,house_id
        ,house_name
        ,house_type
        ,avaliable_count
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1 
) h 
join (
    select house_id 
        ,count(distinct order_no) `离店订单数`
        ,sum(room_total_amount) `离店销售额` 
        ,sum(order_room_night_count) `离店间夜数` 
    from dws.dws_order 
    where checkout_date between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_done = 1
    and is_overseas = 1 
    group by 1 
) o 
on h.house_id = o.house_id

order by 
    case when country_name = '泰国' then 1 
        when country_name = '日本' then 2
        else 8 
        end
    ,case when city_name = '曼谷' then 1
        when city_name = '芭堤雅' then 2
        when city_name = '普吉岛' then 3
        when city_name = '清迈' then 4
        when city_name = '大阪' then 5
        when city_name = '东京' then 6
        when city_name = '京都' then 7
        else 8
        end,
    landlord_type  
