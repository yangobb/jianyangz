
select house_city_name
    ,a.*
from (
    select
        dt,
        t_check_in,
        t_check_out,
        case
          c_user_level
          when 'R1' then '0-普通身份'
          when 'R4' then '1-钻石身份'
        end as c_user_level,
        c_hotel_id,
        c_hotel_name,
        c_base_room_id,
        c_base_room_name,
        c_product_room_id,
        c_original_price,
        c_discount_price,
        c_identity_business_discount_price,
        c_identity_business_discount_after_price,
        c_business_discount_price,
        c_business_discount_after_price,
        c_red_bag_discount_price,
        c_red_bag_discount_after_price,
        c_platform_discount_price,
        c_platform_discount_after_price,
        c_platform_red_bag_discount_price,
        c_platform_red_bag_discount_after_price,
        c_discount_list,
        t_house_id,
        competitor_product_id as t_competitor_product_id,
        t_competitor_pre_price,
        t_competitor_sale_price,
        t_competitor_price, 
        c_activity_price,
        t_competitor_promotion,
        c_result
    from dwd.dwd_house_crm_ctrip_api_data_haiwai_flink_daily_info_d
    where  dt between date_sub(current_date, 7) and date_sub(current_date, 1)
    -- and dt >= '2025-07-27' 
    and c_room_proxy = 'false'
    -- and c_online = 'true'
    -- and c_is_bookable = 'true'
    and c_stock = 'lose' 
) a 
inner join (
    select house_city_name
        ,house_id 
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    -- and house_city_name = '大阪'
    and house_is_online = 1 
    and house_is_oversea = 1 
) b 
on a.t_house_id = b.house_id 