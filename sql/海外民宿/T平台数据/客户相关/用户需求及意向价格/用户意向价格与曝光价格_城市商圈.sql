
select a.country_name
    ,a.house_city_name    
    ,a.dynamic_business
    -- ,a.landlord_channel_name
    -- ,case when coalesce(user_pagel_price5,city_price5,0) = 0 then '9999' when ceil(cast(user_pagel_price5 / city_price5 * 100 as int) / 10) * 10 >= 200 then 200 else ceil(cast(user_pagel_price5 / city_price5 * 100 as int) / 10) * 10 end `曝光`
    ,case when coalesce(user_paged_price5,city_price5,0) = 0 then '9999' 
        when ceil(cast(user_paged_price5 / city_price5 * 100 as int) / 10) * 10 >= 200 then 200 
        else ceil(cast(user_paged_price5 / city_price5 * 100 as int) / 10) * 10 end `点击`
    ,count(distinct uid) u_cnt 
    ,sum(user_lpv) user_lpv
    ,sum(user_dpv) user_dpv

    ,sum(order_num) order_num 
    ,sum(order_gmv) order_gmv
    ,sum(room_night) room_night 
from (
    select country_name
        ,house_city_name    
        ,dynamic_business 
        ,percentile(final_price,0.5) city_price5
        ,sum(lpv) city_lpv 
        ,sum(dpv) city_dpv
    from (
        select h.country_name
            ,h.house_city_name    
            ,h.dynamic_business
            -- ,h.bedroom_count
            ,a.house_id
            ,count(uid) lpv
            -- ,count(distinct uid,dt) luv
            ,count(case when detail_uid is not null then uid end) dpv
            ,count(distinct case when detail_uid is not null then uid end) duv
            ,count(case when order_uid is not null then uid end) opv
            -- ,count(distinct case when order_uid is not null then uid,dt end) ouv
            ,sum(without_risk_order_num) order_num 
            ,sum(without_risk_order_gmv) order_gmv
            ,sum(without_risk_order_room_night) room_night 
            ,max(case when cast(replace(nvl(guest,0),'人','') as int) <= 9 then cast(replace(nvl(guest,0),'人','') as int) else 9 end) guest
            ,max(final_price) final_price
        from (
            select *
            from dws.dws_path_ldbo_d 
            where dt between date_sub(current_date,14) and date_sub(current_date,1)
            and wrapper_name in ('携程','去哪儿','途家')
            and client_name = 'APP' 
            and user_type = '用户'
            and nvl(house_id,0) != 0  
            and is_oversea = 1 
            and front_display='true'  
            and is_recommend = 0
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
            where 
            -- dt between date_sub(current_date,14) and date_sub(current_date,1) 
            dt = date_sub(current_date,1)
            and house_is_online = 1 
            AND house_is_oversea = 1
            and house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门')
        ) h 
        on a.house_id = h.house_id  
        group by 1,2,3,4
    ) tmp 
    group by 1,2,3
) a  
left join (
    select uid
        ,country_name
        ,house_city_name    
        ,dynamic_business
        -- ,bedroom_count
        ,percentile(final_price,0.5) user_pagel_price5
        ,sum(lpv) user_lpv 
        ,percentile(case when nvl(dpv,0) != 0 then null else final_price end,0.5) user_paged_price5
        ,sum(dpv) user_dpv
        ,sum(order_num) order_num 
        ,sum(order_gmv) order_gmv
        ,sum(room_night) room_night 
    from (
        select h.country_name
            ,h.house_city_name    
            ,h.dynamic_business
            ,a.house_id
            -- ,h.bedroom_count
            ,uid
            ,count(uid) lpv
            ,count(distinct uid) luv
            ,count(case when detail_uid is not null then uid end) dpv
            ,count(distinct case when detail_uid is not null then uid end) duv
            ,count(case when order_uid is not null then uid end) opv
            ,count(distinct case when order_uid is not null then uid end) ouv
            ,sum(without_risk_order_num) order_num 
            ,sum(without_risk_order_gmv) order_gmv
            ,sum(without_risk_order_room_night) room_night 
            ,max(case when cast(replace(nvl(guest,0),'人','') as int) <= 9 then cast(replace(nvl(guest,0),'人','') as int) else 9 end) guest
            ,max(final_price) final_price
        from (
            select *
            from dws.dws_path_ldbo_d 
            where dt between date_sub(current_date,14) and date_sub(current_date,1)
            and wrapper_name in ('携程','去哪儿','途家')
            and client_name = 'APP' 
            and user_type = '用户'
            and nvl(house_id,0) != 0  
            and is_oversea = 1 
            and front_display='true'  
            and is_recommend = 0
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
            where 
            -- dt between date_sub(current_date,14) and date_sub(current_date,1) 
            dt = date_sub(current_date,1)
            and house_is_online = 1 
            AND house_is_oversea = 1
            and house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门')
        ) h 
        on a.house_id = h.house_id  
        group by 1,2,3,4,5
    ) tmp 
    group by 1,2,3,4
) b 
on a.country_name = b.country_name
and a.house_city_name = b.house_city_name
and a.dynamic_business = b.dynamic_business 
-- and a.landlord_channel_name = b.landlord_channel_name
group by  1,2,3,4
