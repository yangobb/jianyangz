

-- 2.3 各用户渠道表现， CQT三端和各个入口的流量变化情况；


select '今年' time
    ,wrapper_name
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct concat(uid,dt)) luv 
    ,count(uid) lpv 
from dws.dws_path_ldbo_d 
where dt between '2025-03-01' and '2025-04-30'
and checkout_date between '2025-04-30' and '2025-05-05'
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户' 
group by 1,2,3
union all 
select '去年' time
    ,wrapper_name
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct concat(uid,dt)) luv 
    ,count(uid) lpv 
from dws.dws_path_ldbo_d 
where dt between '2024-03-01' and '2024-04-30'
and checkout_date between '2024-04-30' and '2024-05-05'
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户' 
group by 1,2,3 