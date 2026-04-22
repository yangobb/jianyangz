-- 定义入境游流量		
-- 狭义口径	注册手机非中国大陆，且浏览IP地址在境外					
-- 广义口径	浏览IP地址在境外，且浏览非T+0入离订单的用户					
with xia as (
-- 狭义口径	注册手机非中国大陆，且浏览IP地址在境外
select case when weekofyear(dt) = 1 and month(dt) = 12 then concat(year(dt) + 1,'-W',weekofyear(dt))
            when weekofyear(dt) >= 52 and month(dt) = 1 then concat(year(dt) - 1,'-W',weekofyear(dt))
            else concat(year(dt),'-W',weekofyear(dt))
          END year_week
    ,dt
    ,case when country_name in ('日本','泰国') then country_name else '其他国家' end country_name
    ,case when city_name in ('上海','广州','深圳','成都','重庆','北京','大理州','杭州','丽江','珠海','东京','大阪','京都','曼谷','普吉岛','芭堤雅','清迈') then city_name else '其他城市' end city_name
    ,uid 
    ,without_risk_order_num order_num 
    ,without_risk_order_gmv gmv 
    ,without_risk_order_room_night night 
 
from (
    select *
    from dws.dws_path_ldbo_d
    where dt >= '2025-12-26' 
    and source = 102 
    and user_type = '用户'
    -- and is_oversea = 1 
) l
left join (
    select 
        country_name
        ,house_city_name
        ,house_is_oversea 
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    group by 1,2,3 
) c 
on l.locate_city_name = c.house_city_name
inner join (
    -- 非国内手机号
    select id
    from tujia_tmp.member_d
    where dt = date_sub(current_date,1)
    and country_code != '86'
    group by 1 
) m 
on l.user_id = m.id
-- 浏览IP地址在境外
where c.house_is_oversea = 1 
) 
,guang as (
-- 广义口径	浏览IP地址在境外，且浏览非T+0入离订单的用户	
select case when weekofyear(dt) = 1 and month(dt) = 12 then concat(year(dt) + 1,'-W',weekofyear(dt))
            when weekofyear(dt) >= 52 and month(dt) = 1 then concat(year(dt) - 1,'-W',weekofyear(dt))
            else concat(year(dt),'-W',weekofyear(dt))
          END year_week
    ,dt
    ,case when country_name in ('日本','泰国') then country_name else '其他国家' end country_name
    ,case when city_name in ('上海','广州','深圳','成都','重庆','北京','大理州','杭州','丽江','珠海','东京','大阪','京都','曼谷','普吉岛','芭堤雅','清迈') then city_name else '其他城市' end city_name
    ,uid 
    ,without_risk_order_num order_num 
    ,without_risk_order_gmv gmv 
    ,without_risk_order_room_night night 
 
from (
    select *
    from dws.dws_path_ldbo_d
    where dt >= '2025-12-26' 
    and source = 102 
    and user_type = '用户'
    -- and is_oversea = 1 
    -- 非T+0入住订单
    and checkin_date != dt
) l
left join (
    select 
        country_name
        ,house_city_name
        ,house_is_oversea 
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    group by 1,2,3 
) c 
on l.locate_city_name = c.house_city_name 
-- 浏览IP地址在境外
where c.house_is_oversea = 1 
)

select year_week
    ,'总计' area_name 
    ,'狭义' user_type
    ,count(distinct uid,dt) luv 
    ,count(uid) lpv 
    ,sum(order_num) order_num 
    ,sum(gmv) gmv 
    ,sum(night) night 
    ,sum(order_num) / count(distinct uid,dt) l2o 
    ,sum(gmv) / count(distinct uid,dt) gmv_uv 
from xia 
group by 1,2,3

union all 
select year_week
    ,country_name area_name 
    ,'狭义' user_type
    ,count(distinct uid,dt) luv 
    ,count(uid) lpv 
    ,sum(order_num) order_num 
    ,sum(gmv) gmv 
    ,sum(night) night 
    ,sum(order_num) / count(distinct uid,dt) l2o 
    ,sum(gmv) / count(distinct uid,dt) gmv_uv 
from xia 
where country_name in ('日本','泰国')
group by 1,2,3

union all 
select year_week
    ,city_name area_name 
    ,'狭义' user_type
    ,count(distinct uid,dt) luv 
    ,count(uid) lpv 
    ,sum(order_num) order_num 
    ,sum(gmv) gmv 
    ,sum(night) night 
    ,sum(order_num) / count(distinct uid,dt) l2o 
    ,sum(gmv) / count(distinct uid,dt) gmv_uv 
from xia 
where city_name != '其他城市'
group by 1,2,3 

union all 
select year_week
    ,'总计' area_name 
    ,'广义' user_type
    ,count(distinct uid,dt) luv 
    ,count(uid) lpv 
    ,sum(order_num) order_num 
    ,sum(gmv) gmv 
    ,sum(night) night 
    ,sum(order_num) / count(distinct uid,dt) l2o 
    ,sum(gmv) / count(distinct uid,dt) gmv_uv 
from guang
group by 1,2,3

union all 
select year_week
    ,country_name area_name 
    ,'广义' user_type
    ,count(distinct uid,dt) luv 
    ,count(uid) lpv 
    ,sum(order_num) order_num 
    ,sum(gmv) gmv 
    ,sum(night) night 
    ,sum(order_num) / count(distinct uid,dt) l2o 
    ,sum(gmv) / count(distinct uid,dt) gmv_uv 
from guang 
where country_name in ('日本','泰国')
group by 1,2,3

union all 
select year_week
    ,city_name area_name 
    ,'广义' user_type
    ,count(distinct uid,dt) luv 
    ,count(uid) lpv 
    ,sum(order_num) order_num 
    ,sum(gmv) gmv 
    ,sum(night) night 
    ,sum(order_num) / count(distinct uid,dt) l2o 
    ,sum(gmv) / count(distinct uid,dt) gmv_uv 
from guang 
where city_name != '其他城市'
group by 1,2,3