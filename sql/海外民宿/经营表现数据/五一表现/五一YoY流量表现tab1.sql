
-- 2. 流量变化，
-- 2.1 总体流量+各城市流量变化，对比C酒店流量（同上），C七大类流量（同上），全球流量涨跌幅（对比24） top 10涨幅+top 10跌幅；


select '去年' `type`
    ,house_city_name
    ,sum(ms_duv) ms_duv
    ,sum(hotel_duv) hotel_duv	
    ,sum(hotel7_duv) hotel7_duv
from app_ctrip.adm_oversea_detail_uv_d
where st_dt	between '2024-04-30' and '2024-05-05'
and case when house_city_name in ('396','1275540','1273716','397','1001478','1168785','1002013','新加坡','1274982','1006811','1001506','1274938','1275435','1001716','1001509','1273263','1168667') then 1  
        when house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then 1 end = 1 
group by 1,2
union all 
select '今年' `type`
    ,case when house_city_name = 396 then '香港'
         when house_city_name = 1275540 then '东京'
         when house_city_name = 1273716 then '名古屋'
         when house_city_name = 397 then '澳门'
         when house_city_name = 1001478 then '吉隆坡'
         when house_city_name = 1168785 then '首尔'
         when house_city_name = 1002013 then '普吉岛'
         when house_city_name = 1001506 then '新加坡'
         when house_city_name = 1274982 then '大阪'
         when house_city_name = 1006811 then '曼谷'
         when house_city_name = 1001506 then '新加坡'
         when house_city_name = 1274938 then '京都'
         when house_city_name = 1275435 then '福冈'
         when house_city_name = 1001716 then '清迈'
         when house_city_name = 1001509 then '芭堤雅'
         when house_city_name = 1273263 then '札幌'
         when house_city_name = 1168667 then '济州市'
        else house_city_name end house_city_name
    ,sum(ms_duv) ms_duv
    ,sum(hotel_duv) hotel_duv	
    ,sum(hotel7_duv) hotel7_duv
from app_ctrip.adm_oversea_detail_uv_d
where st_dt between '2025-04-30' and '2025-05-05'
and case when house_city_name in ('396','1275540','1273716','397','1001478','1168785','1002013','新加坡','1274982','1006811','1001506','1274938','1275435','1001716','1001509','1273263','1168667') then 1  
        when house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then 1 end = 1 
group by 1,2 
union all 
select '去年' `type`
    ,'其他' house_city_name
    ,sum(ms_duv) ms_duv
    ,sum(hotel_duv) hotel_duv	
    ,sum(hotel7_duv) hotel7_duv
from app_ctrip.adm_oversea_detail_uv_d
where st_dt	between '2024-04-30' and '2024-05-05'
and case when house_city_name in ('396','1275540','1273716','397','1001478','1168785','1002013','新加坡','1274982','1006811','1001506','1274938','1275435','1001716','1001509','1273263','1168667') then 1  
        when house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then 1 else 0 end = 0 
group by 1,2
union all 
select '今年' `type`
    ,'其他' house_city_name
    ,sum(ms_duv) ms_duv
    ,sum(hotel_duv) hotel_duv	
    ,sum(hotel7_duv) hotel7_duv
from app_ctrip.adm_oversea_detail_uv_d
where st_dt between '2025-04-30' and '2025-05-05'
and case when house_city_name in ('396','1275540','1273716','397','1001478','1168785','1002013','新加坡','1274982','1006811','1001506','1274938','1275435','1001716','1001509','1273263','1168667') then 1  
        when house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then 1 else 0 end = 0
group by 1,2 
union all 
select '去年' `type`
    ,'大盘' house_city_name
    ,sum(ms_duv) ms_duv
    ,sum(hotel_duv) hotel_duv	
    ,sum(hotel7_duv) hotel7_duv
from app_ctrip.adm_oversea_detail_uv_d
where st_dt	between '2024-04-30' and '2024-05-05'
group by 1,2
union all 
select '今年' `type`
    ,'大盘' house_city_name
    ,sum(ms_duv) ms_duv
    ,sum(hotel_duv) hotel_duv	
    ,sum(hotel7_duv) hotel7_duv
from app_ctrip.adm_oversea_detail_uv_d
where st_dt between '2025-04-30' and '2025-05-05'
group by 1,2 

