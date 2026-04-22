WITH hos_oversea AS (
SELECT DISTINCT
    a.dt,
    a.house_id,
    a.landlord_channel_name,
    a.bedroom_count,
    a.house_type,
    a.house_name,
    a.hotel_id,
    a.hotel_name,
    a.house_city_name,
    a.style_score_rule,
    a.house_quality_score,
    a.house_is_online,
    a.house_level,
    a.credit_score,
    b.picture_count,
    b.gross_area,
    b.comment_score,
    b.enum_house_facilities_name,
    b.country_name,
    b.hygiene_comment_score,
    b.comment_num,
    -- 基础设施(3)
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%空调%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%窗%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%独立卫浴%' THEN 1 ELSE 0 END
    AS INT) AS base_facilities_num,
    -- 基础设施(3)
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%空调%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%窗%' THEN 1 ELSE 0 END 
    AS INT) AS base_facilities_num_bedwin,
    -- 安全设施(2)
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%灭火器%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%烟雾报警器%' THEN 1 ELSE 0 END
    AS INT) AS safety_facilities_num,
    -- 娱乐设施(2)
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电视%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%无线网络%' THEN 1 ELSE 0 END
    AS INT) AS entertainment_facilities_num,
    -- 洗漱设施(4)
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电吹风%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%拖鞋%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%洗发水/沐浴露%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%牙具%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%洗衣机%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%可洗热水澡%' THEN 1 ELSE 0 END
    AS INT) AS else_facilities_num,
    -- 厨房设施(5)
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%餐桌%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%冰箱%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%微波炉%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电磁炉%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%烹饪锅具%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%餐具%' THEN 1 ELSE 0 END
    AS INT) AS kitchen_facilities_num,
    -- 洗漱单独一个字段
    CAST(
        CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%电吹风%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%拖鞋%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%洗发水/沐浴露%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%牙具%' THEN 1 ELSE 0 END
        + CASE WHEN CAST(enum_house_facilities_name AS STRING) LIKE '%毛巾%' THEN 1 ELSE 0 END
    AS INT) AS washing_facilities_num
FROM (
    select * 
    from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d 
    where dt = date_sub("${date}", 1) 
    AND house_city_name IN ("东京", "曼谷", "首尔", "大阪", "普吉岛", "京都", "济州市", "吉隆坡", "清迈", "芭堤雅")
) a
JOIN (
    select *
    from dws.dws_house_d
    where dt = date_sub("${date}", 1)
    and house_is_online = 1 -- 上架
    and house_level != "L0" -- 非L0
) b 
ON a.house_id = b.house_id 
),
hotel AS (
    SELECT DISTINCT
        h.hotel_id,
        t3.roomQuantity
    FROM (
        select *
        from ods_houseimport_config.api_hotel 
        where merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
    ) h
    INNER JOIN (
        SELECT *
        FROM (
            SELECT
                t1.hotel_id,
                get_json_object(t1.hotel_static_info_content, '$.hotelStaticInfo.roomQuantity') AS roomQuantity,
                row_number() OVER (PARTITION BY t1.hotel_id ORDER BY t1.update_time DESC) AS rn
            FROM dwd.dwd_house_gctrip_origin_hotel_info_d t1
        ) t
        WHERE rn = 1
    ) t3 ON t3.hotel_id = h.partner_hotel_id 
),

list AS (
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
    INNER JOIN hos_oversea t2 
    ON t1.house_id = t2.house_id
    GROUP BY t1.dt, t1.house_id
),

order_pay AS (
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
    INNER JOIN hos_oversea t2 
    ON t1.house_id = t2.house_id
    GROUP BY 1, 2
),

order_done AS (
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
    INNER JOIN hos_oversea t2 
    ON t1.house_id = t2.house_id
    GROUP BY 1, 2
)

SELECT
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
    b.roomQuantity,
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
FROM (
    select *
    from hos_oversea 
    where nvl(picture_count, 0) >= 14 -- 图片数 14
    AND (comment_score >= 4 OR comment_score IS NULL) -- 点评分 4ornull
    AND nvl(credit_score, 0) > -2 -- 处罚分 -2
) a
LEFT JOIN hotel b ON a.hotel_id = b.hotel_id
LEFT JOIN list c ON a.house_id = c.house_id AND a.dt = c.dt
LEFT JOIN order_pay d ON a.house_id = d.house_id AND a.dt = d.dt
LEFT JOIN order_done e ON a.house_id = e.house_id AND a.dt = e.dt
left join (
select type 
    ,concat_ws('|',collect_set(brand)) check_info 
from excel_upload.houses_level_info0312v1	
group by 1
) f 
on 1 = 1
WHERE 
    case 
        when house_city_name = '首尔' 
            and regexp_like(house_name,check_info) = 1
            then 0
        when house_city_name = '首尔'
            AND house_type in ('标准酒店','度假村') 
            AND nvl(gross_area, 0) between 1 and 14 
            then 0        
        when house_city_name = '首尔'
            AND house_type in ('标准酒店','度假村')
            AND roomQuantity > 70 
            then 0 
        when house_city_name IN ('东京','大阪','京都') 
            AND nvl(base_facilities_num, 0) >= 3 -- 基础设施 3
            AND house_type in ('标准酒店','度假村')
            and (house_type not IN ('青旅','其他类型') and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0)
            AND roomQuantity <= 50 
            and house_quality_score >= 1.8
            and kitchen_facilities_num >= 2 
            and washing_facilities_num >= 1 
            and (nvl(gross_area, 0) >= 15 or nvl(gross_area, 0) = 0)
            THEN 1
        when house_city_name IN ('东京','大阪','京都') 
            AND nvl(base_facilities_num, 0) >= 3 -- 基础设施 3
            AND house_type not in ('标准酒店','度假村')
            and (house_type not IN ('青旅','其他类型') and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0)
            and house_quality_score >= 1.8
            and kitchen_facilities_num >= 2 
            and washing_facilities_num >= 1 
            and (nvl(gross_area, 0) >= 15 or nvl(gross_area, 0) = 0)
            THEN 1
        when house_city_name IN ('曼谷','普吉岛','清迈','芭堤雅','济州市','吉隆坡') 
            AND nvl(base_facilities_num, 0) >= 3 -- 基础设施 3
            AND house_type in ('标准酒店','度假村')
            and (house_type not IN ('青旅','其他类型') and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0) 
            AND roomQuantity <= 70
            and (kitchen_facilities_num >= 1 or (kitchen_facilities_num = 0 AND CAST(enum_house_facilities_name AS STRING) LIKE '%餐厅%'))
            and washing_facilities_num >= 1 
            and (nvl(gross_area, 0) >= 15 or nvl(gross_area, 0) = 0)
            then 1
        when house_city_name IN ('曼谷','普吉岛','清迈','芭堤雅','济州市','吉隆坡') 
            AND nvl(base_facilities_num, 0) >= 3 -- 基础设施 3
            AND house_type not in ('标准酒店','度假村')
            and (house_type not IN ('青旅','其他类型') and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0) 
            and (kitchen_facilities_num >= 1 or (kitchen_facilities_num = 0 AND CAST(enum_house_facilities_name AS STRING) LIKE '%餐厅%'))
            and washing_facilities_num >= 1
            and (nvl(gross_area, 0) >= 15 or nvl(gross_area, 0) = 0)
            then 1 
        when house_city_name = '首尔' 
            AND nvl(base_facilities_num_bedwin, 0) >= 2
            and (house_type IN ('青旅','其他类型') or regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 1) 
            and washing_facilities_num >= 1 
            then 1
        when house_city_name = '首尔' 
            AND nvl(base_facilities_num, 0) >= 3 -- 基础设施 3
            and (house_type not IN ('青旅','其他类型') and regexp_like(house_name, '青旅|其他类型|睡眠报告|睡眠舱|胶囊|男性|女性|男女|混合|床位|青年|宿舍|背包客') = 0) 
            and washing_facilities_num >= 1 
            and (nvl(gross_area, 0) >= 10 or nvl(gross_area, 0) = 0)
            then 1 
        else 0
    end = 1

