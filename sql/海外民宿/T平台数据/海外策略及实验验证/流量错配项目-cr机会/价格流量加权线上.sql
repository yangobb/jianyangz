with a as (
SELECT
    t1.house_city_name
    ,t1.dynamic_business
    ,case when t1.bedroom_count >= 4 then 4 else t1.bedroom_count end bedroom_count
    ,case when t1.bedcount >= 8 then 8 else t1.bedcount end bedcount
    -- ,ceil(percentile(final_price,0.5)/100)*100 as medprice_100
    ,case when ceil(final_price/100)*100 >= 2000 then 2000 else ceil(final_price/100)*100 end as medprice_100
    
    ,count(distinct house_id) house_cnt 
    ,count(uid) lpv
    ,count(distinct uid) luv
    
    ,count(case when detail_uid is not null then uid end) dpv
    ,count(distinct case when detail_uid is not null then uid end) duv 
    
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) order_room_night
    ,sum(without_risk_order_gmv) order_gmv
FROM (
    SELECT a.*
        ,b.house_city_name 
        ,b.dynamic_business
        ,b.bedroom_count
        ,b.bedcount
    FROM (
        SELECT   
            house_id,
            uid,
            detail_uid,
            user_id,
            final_price, 
            without_risk_order_num,
            without_risk_order_room_night,
            without_risk_order_gmv
        FROM dws.dws_path_ldbo_d
        WHERE dt BETWEEN date_sub(current_date, 30) AND date_sub(current_date, 1)
        and dayofweek(dt) in (1,7)
        AND source = 102
        and is_oversea = 1 
        AND user_type = '用户'
        and is_oversea = 1 
        and city_name in ('东京','大阪','京都')
        and city_name = '东京'
    ) a 
    join (
        select house_id 
            ,case when country_name in ('日本','泰国','马来西亚','韩国','新加坡') then country_name when country_name = '中国大陆' then '港澳（中国）' else '其他' end country_name
            ,house_city_name    
            ,dynamic_business 
            ,bedroom_count
            ,bedcount 
            ,case when landlord_channel_name = '平台商户' then '直采' else 'c接' end landlord_channel_name
            ,case when house_type = '标准酒店' then 1 else 0 end house_type
        from dws.dws_house_d
        where dt = date_sub(current_date,1)
        and house_is_online = 1 
        AND house_is_oversea = 1
        and house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门')
        and house_city_name = '东京'
        and dynamic_business = '上野/浅草'
        and bedroom_count = 1 
        and bedcount = 2 
    ) b 
    on a.house_id = b.house_id

) as t1
GROUP BY 1,2,3,4,5
) 


select house_city_name
    ,dynamic_business
    ,bedroom_count
    ,bedcount
    ,medprice_100
    ,od_cr
    ,lpv_pp
    ,lpv_pp * cr_coel / sum(lpv_pp * cr_coel) 
    over(partition by house_city_name,dynamic_business,bedroom_count,bedcount) 
    lpv_pp_new 
 
    ,cr_coel_v1
    ,case when cr_coel = 1.2 then 2 
	    when cr_coel = 1.1 then 1 
	    when cr_coel = 1 then 0
	    when cr_coel = 0.9 then -0.5
	    when cr_coel = 0.8 then -1 else 1 
	    end cr_coel_new_v2

from (
select house_city_name
    ,dynamic_business
    ,bedroom_count
    ,bedcount
    ,medprice_100
    ,od_cr
    ,lpv_pp
    ,case when od_cr >= 1.2 then 1.2 
        when od_cr >= 1.1 then 1.1 
        when od_cr <= 0.8 then 0.8 
        when od_cr <= 0.9 then 0.9 
        else 1 end cr_coel_v1
    ,case when od_cr >= 1.2 then 1.2 
        when od_cr >= 1.1 then 1.1 
        when od_cr <= 0.8 then 0.8 
        when od_cr <= 0.9 then 0.9 
        else 1 end cr_coel 
from (
select a.house_city_name
    ,a.dynamic_business
    ,a.bedroom_count
    ,a.bedcount
    ,b.medprice_100
    ,nvl(b.lpv / a.lpv,0) `lpv_pp`
    ,nvl(b.order_num / a.order_num,0) `order_num_pp`
    ,nvl(b.order_gmv / a.order_gmv,0) `order_gmv_pp`
    ,nvl((nvl(b.order_num / a.order_num,0) / nvl(b.lpv / a.lpv,0) * 0.5 + nvl(b.order_gmv / a.order_gmv,0) / nvl(b.lpv / a.lpv,0) * 0.5),0) od_cr

from (
    select house_city_name
        ,dynamic_business
        ,bedroom_count
        ,bedcount
        ,sum(lpv) lpv
        ,sum(order_num) order_num
        ,sum(order_room_night) order_room_night
        ,sum(order_gmv) order_gmv
    from a 
    group by 1,2,3,4 
) a 
left join (
    select house_city_name
        ,dynamic_business
        ,bedroom_count
        ,bedcount
        ,medprice_100
        ,house_cnt 
        ,lpv
        ,luv
        ,dpv
        ,duv 
        ,order_num
        ,order_room_night
        ,order_gmv
    from a 
) b 
on a.house_city_name = b.house_city_name
and a.dynamic_business = b.dynamic_business
and a.bedroom_count = b.bedroom_count
and a.bedcount = b.bedcount
) a 
) a