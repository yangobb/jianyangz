select case when house_type = 1 then '标准酒店' 
            when t2.house_id is not null then '宝藏' 
            when t1.house_id is not null then '优选' 
            else '普通' end sanxuan
    ,landlord_channel_name
    ,country_name
    ,dynamic_business
    ,count(distinct h.house_id) house_cnt
    
    ,sum(lpv) lpv
    ,sum(luv) luv
    ,sum(dpv) dpv
    ,sum(dpv) dpv
    ,sum(opv) opv
    ,sum(opv) opv
    
    ,sum(order_num) order_num
    ,sum(order_gmv) order_gmv
    ,sum(room_night) room_night
    
    ,sum(order_cnt) order_cnt
    ,sum(room_total_amount) room_total_amount
    
    ,price3
    ,price5
    ,price7
    ,price_avg
from (
    select dt
        ,house_id 
        ,hotel_id
        ,hotel_name
        ,case when country_name in ('日本','泰国','马来西亚','韩国','新加坡') then country_name when country_name = '中国大陆' then '港澳（中国）' else '其他' end country_name
        ,house_city_name
        ,dynamic_business
        ,dynamic_business_distance
        ,bedroom_count
        ,gross_area
        -- ,house_type
        ,house_class	
        ,is_fast_booking
        ,house_first_active_time
        ,picture_count
        ,cover_picture_url	
        ,landlord_channel_name
        ,case when house_type = '标准酒店' then 1 else 0 end house_type
    from dws.dws_house_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1) 
    and dt = date_sub(current_date,1)
    and house_is_online = 1 
    AND house_is_oversea = 1
    and house_city_name in ('东京','大阪','京都')
) h 
left join (
    select 
        dt
        ,house_id
        ,weighted_price
        
        ,percentile(weighted_price,0.3) price3
        ,percentile(weighted_price,0.5) price5
        ,percentile(weighted_price,0.7) price7
        ,avg(weighted_price) price_avg
    from pdb_analysis_b.ads_house_daily_prices_base_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1) 
) p
on h.house_id = p.house_id
and h.dt = p.dt 
left join (
    select dt
        ,house_id
        ,percentile(final_price,0.5) final_price5
        ,count(uid) lpv
        ,count(distinct uid) luv
        ,count(case when detail_uid is not null then uid end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when order_uid is not null then uid end) opv
        ,count(distinct case when order_uid is not null then uid end) ouv
        ,sum(without_risk_order_num) order_num 
        ,sum(without_risk_order_gmv) order_gmv
        ,sum(without_risk_order_room_night) room_night 
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1)
    and wrapper_name in ('携程','去哪儿','途家')
    and client_name = 'APP' 
    and user_type = '用户'
    and house_id is not null 
    and is_oversea = 1 
    and front_display='true'  
    and is_recommend = 0
    group by 1,2 
) l 
on h.house_id = l.house_id
and h.dt = l.dt 
left join (
    select create_date dt 
        ,house_id
        ,count(distinct order_no) order_cnt 
        ,sum(room_total_amount) room_total_amount
    from dws.dws_order 
    where create_date between date_sub(current_date,14) and date_sub(current_date,1)
    and is_paysuccess_order = 1 
    and is_overseas = 1 
    and is_cancel_order  = 0 
    group by 1,2 
) o 
on h.house_id = o.house_id
and h.dt = o.dt 
LEFT JOIN   (
    -- 优选
    SELECT dt
        ,house_id
    FROM pdb_analysis_b.dwd_house_label_1000487_d
    WHERE dt between date_sub(current_date,14) and date_sub(current_date,1)
    group by 1,2
) t1
ON h.house_id = t1.house_id 
and h.dt = t1.dt
LEFT JOIN   (
    -- 宝藏
    SELECT dt
        ,house_id
    FROM pdb_analysis_b.dwd_house_label_1000488_d
    WHERE dt between date_sub(current_date,14) and date_sub(current_date,1)
    group by 1,2
) t2
ON h.house_id = t2.house_id
and h.dt = t2.dt
group by 1,2,3,4