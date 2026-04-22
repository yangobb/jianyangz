with 
-- 圈选房屋
house_qx as (
select  
    tmp.house_id
    ,roomQuantity
from (
    -- 标准酒店
    SELECT house_id 
        ,roomQuantity
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
        where dt = date_sub("${date}",1)
        and house_is_online = 1 
        and house_is_oversea = 1 
        and house_type in ('标准酒店','度假村')
    ) h1 
    on h.partner_hotel_id = h1.hotel_id
    inner join (
        -- 关联酒店门店房屋数
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
    left join (
        -- 关联大型连锁
        select type 
            ,concat_ws('|',collect_set(brand)) check_info 
        from excel_upload.houses_level_info0312v1	
        group by 1
    ) h2
    on 1 = 1
    where regexp_like(house_name,check_info) = 0
    -- 包含null和0，且日本房间数50以下，其他国家房间数70以下
    and case when nvl(t3.roomQuantity,0) = 0 then 1
             when h1.country_name = '日本' and t3.roomQuantity <= 50 then 1 
             when t3.roomQuantity <= 70 then 1 end = 1 
    
    union all 
    -- 白名单
    select house_id
        ,null roomQuantity 
    from excel_upload.oversea_yx_white_list
    union all 
    -- 非标准酒店 
    select house_id  
        ,null roomQuantity 
    from dws.dws_house_d
    where dt = date_sub("${date}",1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and house_type not in ('标准酒店','度假村')
    group by 1,2 
) tmp
-- 黑名单用户
left join (
    select *
    from excel_upload.oversea_yx_black_list 
) black 
on tmp.house_id = black.house_id
where black.house_id is null 
group by 1,2 
)
,final_house as (
select a.*
    ,d.roomQuantity
from (
    select distinct 
        s.dt,
        s.house_id,
        s.landlord_channel_name,
        s.bedroom_count,
        s.house_type,
        s.house_name,
        s.hotel_id,
        s.hotel_name,
        s.house_city_name,
        s.style_score_rule,
        s.house_quality_score,--质量分
        s.house_is_online,
        s.house_level,
        s.credit_score,-- 处罚分
        h.picture_count,
        h.gross_area,
        h.comment_score,
        h.enum_house_facilities_name,
        h.country_name,
        h.base_facilities_num,
        h.base_facilities_num_bedwin,
        h.safety_facilities_num,
        h.entertainment_facilities_num,
        h.else_facilities_num,
        h.kitchen_facilities_num,
        h.washing_facilities_num,
        h.dynamic_business
    from (
        select *
            -- 周边有饭店
            ,case when CAST(enum_house_facilities_name AS STRING) LIKE '%餐厅%' then 1 else 0 end is_res 
            -- 基础设施(3)
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%空调%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%窗%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%独立卫浴%' THEN 1 ELSE 0 END
            AS INT) AS base_facilities_num
            -- 基础设施(3)
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%空调%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%窗%' THEN 1 ELSE 0 END 
            AS INT) AS base_facilities_num_bedwin
            -- 安全设施(2)
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%灭火器%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%烟雾报警器%' THEN 1 ELSE 0 END
            AS INT) AS safety_facilities_num
            -- 娱乐设施(2)
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电视%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%无线网络%' THEN 1 ELSE 0 END
            AS INT) AS entertainment_facilities_num
            -- 洗漱设施(4)
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电吹风%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%拖鞋%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%洗发水/沐浴露%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%牙具%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%洗衣机%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%可洗热水澡%' THEN 1 ELSE 0 END
            AS INT) AS else_facilities_num
            -- 厨房设施(5)
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%餐桌%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%冰箱%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%微波炉%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电磁炉%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%烹饪锅具%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%餐具%' THEN 1 ELSE 0 END
            AS INT) AS kitchen_facilities_num
            -- 洗漱单独一个字段
            ,CAST(
                CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电吹风%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%拖鞋%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%洗发水/沐浴露%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%牙具%' THEN 1 ELSE 0 END
                + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%毛巾%' THEN 1 ELSE 0 END
            AS INT) AS washing_facilities_num
        from dws.dws_house_d 
        where dt = date_sub("${date}",1)
        and house_is_oversea = 1 
        and house_is_online = 1 
        and house_class != 'L0' -- 非L0
        and case when comment_num >= 1 and nvl(comment_score,0) >= 4.0 then 1 when nvl(comment_num,0) = 0 then 1 end = 1  --满足条件： 1、点评条数≥1且点评分≥4.0 2、无点评
        and case when comment_num >= 1 and nvl(hygiene_comment_score,0) >= 4.0 then 1 when nvl(comment_num,0) = 0 then 1 end = 1 -- 1、点评条数≥1且卫生点评分≥4.0 2、无点评
        and case when nvl(gross_area,0) >= 15 then 1 when nvl(gross_area,0) = 0 then 1 end = 1  -- 面积≥15 / is null / 0
        and picture_count >= 14     
        and (CAST(
            CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%空调%' THEN 1 ELSE 0 END
            + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%窗%' THEN 1 ELSE 0 END
            + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%独立卫浴%' THEN 1 ELSE 0 END
            AS INT)) >= 3 
    ) h
    join (
        select distinct 
            dt,
            house_id,
            landlord_channel_name,
            bedroom_count,
            house_type,
            house_name,
            hotel_id,
            hotel_name,
            house_city_name,
            style_score_rule,
            house_quality_score,--质量分
            house_is_online,
            house_level,
            credit_score -- 处罚分
        from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d 
        where dt = date_sub("${date}",1)
        and credit_score >= -2.0
    ) s 
    on h.house_id = s.house_id
    where case when country_name = '日本' and washing_facilities_num >= 1 and kitchen_facilities_num >= 2 and house_quality_score >= 1.8 then 1 
            when country_name != '日本' and washing_facilities_num >= 1 and (kitchen_facilities_num >= 1 or is_res = 1) then 1 end = 1 
    -- and house_type not IN ('青旅','其他类型') 
    -- and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0 -- 非青旅
    -- and is_shared_dormitory_room = 0 -- 床位房
    -- and independentbathroom = 1 -- 独立卫浴
    -- and case when CAST(enum_house_facilities_name AS STRING) LIKE '%可洗热水澡%' THEN 1 ELSE 0 END = 1 -- 热水
) a 
inner join excel_upload.oversea_city_level city_level 
on a.house_city_name = city_level.city_name
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
        where checkout_date between date_sub("${date}",90) and date_sub("${date}",1)
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
    where checkout_date between date_sub("${date}",90) and date_sub("${date}",1)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and is_done = 1 
    and is_overseas = 1 
    group by 1
) f1
on a.house_id = f1.house_id
and f1.adr >= f.adr_90
)
,list AS (
    SELECT
        t1.dt,
        t1.house_id,
        COUNT(uid) AS lpv
    FROM (
        select *
        from dws.dws_path_ldbo_d
        where dt = date_sub("${date}", 1)
        AND wrapper_name IN ('途家','携程','去哪儿') 
        AND source = '102'
        AND user_type = '用户'
        and is_oversea = 1 
    ) t1
    INNER JOIN final_house t2 
    ON t1.house_id = t2.house_id
    GROUP BY t1.dt, t1.house_id
)
,order_pay AS (
    SELECT DISTINCT
        t1.create_date AS dt,
        t1.house_id,
        COUNT(DISTINCT t1.order_no) AS ord_cnt,
        SUM(t1.order_room_night_count) AS jianye,
        SUM(t1.room_total_amount) AS gmv
    FROM (
        select *
        from dws.dws_order
        WHERE is_paysuccess_order = '1'
        AND (terminal_type_name IN ('携程-APP','去哪儿-APP','本站-APP') OR sell_channel_type = '10')
        AND create_date = date_sub("${date}", 1)
        AND is_overseas = 1
    ) t1
    INNER JOIN final_house t2 
    ON t1.house_id = t2.house_id
    GROUP BY 1, 2
)
,order_done AS (
    SELECT DISTINCT
        t1.checkout_date AS dt,
        t1.house_id,
        COUNT(DISTINCT t1.order_no) AS ord_cnt_done,
        SUM(t1.order_room_night_count) AS jianye_done,
        SUM(t1.room_total_amount) AS gmv_done
    FROM (
        select *
        from dws.dws_order 
        WHERE is_done = '1'
        AND (terminal_type_name IN ('携程-APP','去哪儿-APP','本站-APP') OR sell_channel_type = '10')
        AND checkout_date = date_sub("${date}", 1)
        AND is_overseas = 1
    ) t1
    INNER JOIN final_house t2 
    ON t1.house_id = t2.house_id
    GROUP BY 1, 2
)

select
    a.house_id,
    a.house_is_online,
    a.house_name,
    a.house_level,
    a.house_city_name,
    a.hotel_id,
    a.hotel_name,
    a.landlord_channel_name,
    a.house_type,
    a.bedroom_count,
    a.gross_area,
    a.house_quality_score,
    a.credit_score,
    a.picture_count,
    a.style_score_rule,
    a.comment_score,
    a.enum_house_facilities_name,
    a.base_facilities_num,
    a.else_facilities_num,
    NULL AS core_room_num,
    a.roomQuantity,
    c.lpv,
    d.ord_cnt,
    d.jianye,
    d.gmv,
    e.ord_cnt_done,
    e.jianye_done,
    e.gmv_done,
    a.country_name,
    a.kitchen_facilities_num,
    a.washing_facilities_num,
    date_sub("${date}", 1) AS dt
from final_house a

LEFT JOIN list c ON a.house_id = c.house_id AND a.dt = c.dt
LEFT JOIN order_pay d ON a.house_id = d.house_id AND a.dt = d.dt
LEFT JOIN order_done e ON a.house_id = e.house_id AND a.dt = e.dt
