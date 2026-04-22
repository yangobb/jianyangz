
select '今年' time 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end area_name
    ,count(distinct concat(uid,dt)) luv 
    ,count(uid) lpv 
    ,count(distinct case when detail_uid is not null then uid end) duv
from dws.dws_path_ldbo_d 
where dt between date_sub(current_date,14) and date_sub(current_date,1)
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户' 
group by 1,2
union all 
select '去年' time 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end area_name
    ,count(distinct concat(uid,dt)) luv 
    ,count(uid) lpv 
    ,count(distinct case when detail_uid is not null then uid end) duv
from dws.dws_path_ldbo_d 
where dt between date_sub(add_months(current_date,-12),14) and date_sub(add_months(current_date,-12),1)
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户' 
group by 1,2
