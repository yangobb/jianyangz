with h as (
select dt
    ,h.house_id
    ,case when house_type = '标准酒店' then '标准酒店' 
        when t1.house_id is not null then '优选'
        when t2.house_id is not null then '宝藏' 
        else '普通' end house_level
    ,hotel_id
    ,hotel_name
    ,country_name
    ,house_city_name
    ,dynamic_business
    ,dynamic_business_distance
    ,bedroom_count
    ,bedcount
    ,gross_area
    ,house_type
    ,house_class	
    ,is_fast_booking
    ,house_first_active_time
    ,picture_count
    ,cover_picture_url	
from dws.dws_house_d h 
LEFT JOIN   (
    -- 优选
    SELECT DISTINCT house_id
    FROM pdb_analysis_b.dwd_house_label_1000487_d
    WHERE dt = date_sub(CURRENT_DATE(),1)
) t1
ON h.house_id = t1.house_id 
LEFT JOIN   (
    -- 宝藏
    SELECT DISTINCT house_id
    FROM pdb_analysis_b.dwd_house_label_1000488_d
    WHERE dt = date_sub(CURRENT_DATE(),1)
) t2
ON h.house_id = t2.house_id
where dt = date_sub(current_date, 1)
-- AND landlord_channel_name = '平台商户'
and house_is_online = 1 
AND house_is_oversea = 1
and country_name in ('日本','泰国')
and house_city_name in ('东京','京都','大阪')
)
,ldbo as (
select distinct 
    user_id
    ,uid 
    ,dt
    ,house_id
    ,detail_uid
    ,avg(final_price)
    ,count(1) pv
    ,sum(without_risk_order_num) without_risk_order_num
    ,sum(without_risk_order_gmv) without_risk_order_gmv
    ,sum(without_risk_order_room_night) without_risk_order_room_night
    from dws.dws_path_ldbo_d
where dt between date_sub(current_date,14) and date_sub(current_date,1)
and is_oversea = 1 
and city_name in ('东京','大阪','京都')
and conditions is not null
and nvl(user_id,0) != 0
group by 1,2,3,4,5
)


select house_city_name
    ,dynamic_business
    ,bedroom_count
    ,bedcount
    ,percentile(final_price,0.3) price3
    ,percentile(final_price,0.5) price5
    ,percentile(final_price,0.7) price7
from ldbo 
inner join h 
on ldbo.house_id = h.house_id
group by 1,2,3,4 




