with house_qx as (
select  
    tmp.house_id
from (
    -- 标准酒店
    SELECT house_id 
    FROM (
        select *
        from ods_houseimport_config.api_hotel 
        where merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
    ) h
    inner join (
        select hotel_id
            ,country_name 
            ,house_id
            ,house_name
        from dws.dws_house_d
        where dt = date_sub(current_date,1)
        and house_is_online = 1 
        and house_is_oversea = 1 
        and house_type = '标准酒店'
    ) h1 
    on h.partner_hotel_id = h1.hotel_id
    -- 关联酒店门店房屋数
    inner join (
        SELECT *
        FROM (
            SELECT
                t1.hotel_id,
                get_json_object(t1.hotel_static_info_content, '$.hotelStaticInfo.roomQuantity') AS roomQuantity,
                row_number() OVER (PARTITION BY t1.hotel_id ORDER BY t1.update_time DESC) AS rn
            FROM dwd.dwd_house_gctrip_origin_hotel_info_d t1
        ) t
        WHERE rn = 1
    ) t3 
    ON t3.hotel_id = h.partner_hotel_id 
    -- 关联大型连锁，
    left join (
        select type 
            ,concat_ws('|',collect_set(brand)) check_info 
        from excel_upload.houses_level_info0312v1	
        group by 1
    ) h2
    on 1 = 1
    where regexp_like(house_name,check_info) = 0
    -- 日本50以下，其他国家70以下
    and case when h1.country_name = '日本' and t3.roomQuantity <= 50 then 1 when t3.roomQuantity <= 70 then 1 end = 1 
    
    union all 
    -- 白名单
    select house_id
    from excel_upload.oversea_yx_white_list
    
    union all 
    -- 非标准酒店 
    select house_id  
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and house_type != '标准酒店'
    group by 1
) tmp
left join (
    select *
    from excel_upload.oversea_yx_black_list 
) black 
on tmp.house_id = black.house_id
where black.house_id is null 
group by 1 
)
select a.country_name
    ,a.house_city_name
    ,a.house_id 
    ,a.house_name 
from (
    select *
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_oversea = 1 
    and house_is_online = 1 
    and house_class != 'L0' -- 非L0
    -- and is_shared_dormitory_room = 0 -- 床位房
    and house_type not IN ('青旅','其他类型') 
    and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0
    and case when comment_num >= 1 and nvl(comment_score,0) >= 4.0 then 1 when nvl(comment_num,0) = 0 then 1 end = 1  --满足条件： 1、点评条数≥1且点评分≥4.0 2、无点评
    and case when comment_num >= 1 and nvl(hygiene_comment_score,0) >= 4.0 then 1 when nvl(comment_num,0) = 0 then 1 end = 1 -- 1、点评条数≥1且卫生点评分≥4.0 2、无点评
    and independentbathroom = 1 -- 独立卫浴
    and case when nvl(gross_area,0) >= 15 then 1 when nvl(gross_area,0) = 0 then 1 end = 1  -- 面积
    and case when CAST(enum_house_facilities_name AS STRING) LIKE '%可洗热水澡%' THEN 1 ELSE 0 END = 1 -- 热水
) a 
-- 处罚分
join (
    select house_id
        ,credit_score -- 处罚分
    from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d 
    where dt = date_sub(current_date,1)
    and credit_score >= -2.0
) c 
on a.house_id = c.house_id
join house_qx d 
on a.house_id = d.house_id
-- 商圈10分位adr
left join (
    select city_name
        ,dynamic_business
        ,percentile(adr,0.1) adr_90 
    from (
        select city_name
            ,dynamic_business
            ,house_id
            ,sum(room_total_amount) / sum(order_room_night_count) adr 
        from dws.dws_order 
        where checkout_date between date_sub(current_date,90) and date_sub(current_date,1)
        and is_paysuccess_order = 1 
        and is_cancel_order = 0 
        and is_done = 1 
        and is_overseas = 1 
        group by 1,2,3 
    ) a 
    group by 1,2 
) f 
on a.house_city_name = f.city_name
and a.dynamic_business = f.dynamic_business
-- 房屋adr
left join (
    select house_id
        ,sum(room_total_amount) / sum(order_room_night_count) adr 
    from dws.dws_order 
    where checkout_date between date_sub(current_date,90) and date_sub(current_date,1)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_done = 1 
    and is_overseas = 1 
    group by 1
) f1
on a.house_id = f1.house_id
and f1.adr >= f.adr_90
