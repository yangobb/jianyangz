--  直采
select '直采' type
    ,'大盘' area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 1 
) b 
on a.house_id = b.house_id
group by 1,2,3
union all 
select '直采' type
    ,country_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 1 
    and country_name in ('日本','泰国')
) b 
on a.house_id = b.house_id
group by 1,2,3
union all 
select '直采' type
    ,city_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select house_id 
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 1 
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
) b 
on a.house_id = b.house_id
group by 1,2,3 
 
union all
--  携程接入
select '携程接入' type
    ,'大盘' area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 334 
) b 
on a.house_id = b.house_id
group by 1,2,3
union all 
select '携程接入' type
    ,country_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 334
    and country_name in ('日本','泰国')
) b 
on a.house_id = b.house_id
group by 1,2,3 
union all 
select '携程接入' type
    ,city_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 334
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
) b 
on a.house_id = b.house_id
group by 1,2,3

union all 

--  优选
select '优选' type
    ,'大盘' area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
) b 
on a.house_id = b.house_id
inner join (
    select distinct house_id
    from pdb_analysis_b.dwd_house_label_1000487_d
    where dt = date_sub(current_date,1)
) c 
on a.house_id = c.house_id
group by 1,2,3
union all 
select '优选' type
    ,country_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and country_name in ('日本','泰国')
) b 
on a.house_id = b.house_id
inner join (
    select distinct house_id
    from pdb_analysis_b.dwd_house_label_1000487_d
    where dt = date_sub(current_date,1)
) c 
on a.house_id = c.house_id
group by 1,2,3 
union all 
select '优选' type
    ,city_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
) b 
on a.house_id = b.house_id
inner join (
    select distinct house_id
    from pdb_analysis_b.dwd_house_label_1000487_d
    where dt = date_sub(current_date,1)
) c 
on a.house_id = c.house_id
group by 1,2,3
union all 

--  宝藏
select '宝藏' type
    ,'大盘' area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
) b 
on a.house_id = b.house_id
inner join (
    select distinct house_id
    from pdb_analysis_b.dwd_house_label_1000488_d
    where dt = date_sub(current_date,1)
) c 
on a.house_id = c.house_id
group by 1,2,3
union all 
select '宝藏' type
    ,country_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and country_name in ('日本','泰国')
) b 
on a.house_id = b.house_id
inner join (
    select distinct house_id
    from pdb_analysis_b.dwd_house_label_1000488_d
    where dt = date_sub(current_date,1)
) c 
on a.house_id = c.house_id
group by 1,2,3 
union all 
select '宝藏' type
    ,city_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
) b 
on a.house_id = b.house_id
inner join (
    select distinct house_id
    from pdb_analysis_b.dwd_house_label_1000488_d
    where dt = date_sub(current_date,1)
) c 
on a.house_id = c.house_id
group by 1,2,3

union all 

--  L34
select 'L34' type
    ,'大盘' area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1
    and house_class in ('L3','L4')
) b 
on a.house_id = b.house_id
group by 1,2,3
union all 
select 'L34' type
    ,country_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and country_name in ('日本','泰国')
    and house_class in ('L3','L4')
) b 
on a.house_id = b.house_id
group by 1,2,3 
union all 
select 'L34' type
    ,city_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
    and house_class in ('L3','L4')
) b 
on a.house_id = b.house_id
group by 1,2,3
union all 
--  L25
select 'L25' type
    ,'大盘' area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1
    and house_class in ('L25')
) b 
on a.house_id = b.house_id
group by 1,2,3
union all 
select 'L25' type
    ,country_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select country_name
        ,house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and country_name in ('日本','泰国')
    and house_class in ('L25')
) b 
on a.house_id = b.house_id
group by 1,2,3 
union all 
select 'L25' type
    ,city_name area_name
    ,concat('W',weekofyear(dt)) week_type
    ,count(uid) lpv 
    ,count(distinct concat(uid,dt)) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) nights 
    ,sum(without_risk_order_gmv) gmv 
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between date_sub(date_sub(current_date,1),14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
join (
    select house_id
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1  
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
    and house_class in ('L25')
) b 
on a.house_id = b.house_id
group by 1,2,3
