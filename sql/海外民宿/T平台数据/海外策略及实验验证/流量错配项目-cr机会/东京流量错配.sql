SELECT
    t1.house_city_name
    ,t1.dynamic_business
    ,t1.bedroom_count
    ,t1.bedcount
    -- ,ceil(percentile(final_price,0.5)/100)*100 as medprice_100
    ,ceil(final_price/100)*100 as medprice_100
    
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
            dt,
            wrapper_name,
            house_id,
            uid,
            detail_uid,
            user_id,
            final_price,
            position,
            without_risk_order_num,
            without_risk_order_room_night,
            without_risk_order_gmv
        FROM dws.dws_path_ldbo_d
        WHERE dt BETWEEN date_sub(current_date, 14) AND date_sub(current_date, 1)
        -- AND wrapper_name IN ('携程')
        AND source = 102
        AND user_type = '用户'       
        and is_oversea = 1 
        and city_name in ('东京','大阪','京都')
        and city_name = '东京'
    ) a 
    join (
        select house_id 
            ,hotel_id
            ,hotel_name
            ,case when country_name in ('日本','泰国','马来西亚','韩国','新加坡') then country_name when country_name = '中国大陆' then '港澳（中国）' else '其他' end country_name
            ,house_city_name    
            ,dynamic_business
            ,dynamic_business_distance
            ,bedroom_count
            ,bedcount
            ,gross_area
            -- ,house_type
            ,house_class	
            ,is_fast_booking
            ,house_first_active_time
            ,picture_count
            ,cover_picture_url	
            ,case when landlord_channel_name = '平台商户' then '直采' else 'c接' end landlord_channel_name
            ,case when house_type = '标准酒店' then 1 else 0 end house_type
        from dws.dws_house_d
        where dt = date_sub(current_date,1)
        and house_is_online = 1 
        AND house_is_oversea = 1
        and house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门')
    ) b 
    on a.house_id = b.house_id

) as t1
GROUP BY 1,2,3,4,5