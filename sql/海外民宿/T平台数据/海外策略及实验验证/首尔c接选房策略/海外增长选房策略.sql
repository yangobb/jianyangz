-- 结果目标：   
-- 转化导向： l2o
-- 房屋质量: 近90天好评率
    -- 备选:
    -- 热度导向： 未来14天贡献订单数;未来14天贡献间夜数

-- 过程指标
    -- 房屋基础属性
        -- 设备完成度: 16个设备以上
        -- 图片张数
        -- 户型: 一居两居三居
        -- 地理位置:是否景观
    -- 供给稳定性
        -- 接单响应时长
        -- 空房率
    -- 用户口碑维度
        -- 综合评分
        -- 差评率
    -- 价格竞争力
        -- 对比三星酒店价格
        -- 是否参与活动

-- house_d 
with h as (
select 
    house_id,
    landlord_channel,
    house_is_online,
    house_name,
    is_cooking,
    is_bring_pet,
    picture_count,
    bedroom_count,
    case when bedroom_count=1 then '1'
        when bedroom_count=2 then '2'
        when bedroom_count>=3 then '3'
        end as `户型`,
    bathroom_count,
    kitchen_count,
    livingroom_count,
    share_type,
    enum_house_facilities_name,
    livingroom_picture_count,
    bedroom_picture_count,
    kitchen_picture_count,
    bathroom_picture_count,
    case when is_mountain_view=1 or is_sea_view=1 or is_garden_view=1 or is_lake_view=1 or  is_river_view=1 or is_great_river_view=1 or is_city_view=1 then 1 else 0 end as `景区` 
    
from dws.dws_house_d  
where dt =date_sub(current_date(),1)
and house_is_oversea=1
and hotel_is_oversea=1
and house_is_online = 1 
and house_city_name = '首尔'
)

-- 结果
,ldbo as (
select house_id
    ,count(1) lpv 
    ,count(distinct uid,dt) luv 
    ,sum(without_risk_order_num) order_num
    ,sum(without_risk_order_room_night) night
    ,sum(without_risk_order_gmv) gmv
    ,sum(without_risk_order_num) / count(distinct uid,dt) l2o 
from dws.dws_path_ldbo_d
where dt between date_sub(current_date,30) and date_sub(current_date,1)
and source = 102
and user_type = '用户'
and is_oversea = 1
and city_name = '首尔'
group by 1 
)


-- --------------过程影响指标--------------
-- 评论
,comment as (
select a.*
from (
    select house_id
        ,comment_score
        ,service_comment_score
        ,hygiene_comment_score
    from dws.dws_comment_d
    where dt = date_sub(current_date,1)
) a 
inner join h on a.house_id = h.house_id
)
-- 未来库存表现
,inventory as (
-- select unitid as house_id
--     ,sum(instancecount) `物理库存`
--     ,sum(avaliablecount) `可售库存`
--     ,sum(instancecount - avaliablecount) `全平台已售库存`
--     ,sum(unavaliablecount) `途家平台已售` 
-- from (
--     select * 
--     from dim_tujiaproduct.unit_inventory_log a
--     where createdate = current_date
--     and substr(a.gettime,9,2) = '00' 
--     and inventorydate between date_add(current_date,1) and date_add(current_date,14)
-- ) a 
-- inner join h on a.unitid = h.house_id
-- group by 1
select a.house_id
    ,max(14) `物理库存`
    ,sum(can_booking) `可售库存` 
from (
    select house_id 
        ,can_booking
        ,checkin_date dt 
    from dwd.dwd_house_daily_price_d 
    where dt = date_sub(current_date,1)
    and checkin_date between date_add(current_date,1) and date_add(current_date,14)
) a 
inner join h on a.house_id = h.house_id
group by 1
)
-- -- 订单
-- ,od as (
-- select a.house_id 
-- from (
--     select *
--     from dws.dws_order 
--     where create_date between date_sub(current_date,30) and date_sub(current_date,1)
--     and is_paysuccess_order = 1 
--     and is_cancel_order = 0 
-- ) a 
-- inner join h on a.house_id = h.house_id
-- group by 1 
-- ) 
-- 设施
,facilities as (
select a.house_id  
    ,count(1) facilities_num
from (
    select explode(enum_house_facilities_name) num1 
        ,house_id 
        ,house_city_name
    from dws.dws_house_d  a
    where dt =date_sub(current_date(),1)
    and house_is_oversea=1
    and hotel_is_oversea=1
    and house_is_online = 1  
) a 
inner join h on a.house_id = h.house_id
group by 1 
) 
-- 携程漏斗
,l2o_ctrip as (
select a.masterhotelid
    ,house_id
    ,fh_price_ctrip
    ,lpv_ctrip
    ,luv_ctrip
    ,dpv_ctrip
    ,duv_ctrip
    ,gmv_ctrip
    ,night_ctrip
    ,order_num_ctrip
from (
    select a.masterhotelid
        ,percentile(fh_price,0.5) fh_price_ctrip
        ,count(1) lpv_ctrip
        ,count(distinct d,cid) luv_ctrip
        ,count(case when is_has_click = 1 then concat(d,cid) end) dpv_ctrip
        ,count(distinct case when is_has_click = 1 then concat(d,cid) end) duv_ctrip
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(current_date,30) AND date_sub(current_date, 1)
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门')) 
        and cityname = '首尔'
        and masterhotelid > 0 
        and is_standard = 0
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) a 
left join (
    select masterhotelid  
        ,sum(ciireceivable) gmv_ctrip 
        ,sum(ciiquantity) night_ctrip
        ,count(distinct orderid) order_num_ctrip 
    FROM app_ctrip.edw_htl_order_all_split
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between date_sub(current_date,30) and date_sub(current_date,1)
    AND orderstatus IN ('P','S')
    -- AND cityname in ("东京","香港","曼谷","首尔","大阪","吉隆坡","京都","芭堤雅","普吉岛","新加坡","澳门","清迈","济州市")
    and cityname = '首尔'
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
    group by 1 
) b 
on a.masterhotelid = b.masterhotelid
inner join (
    select partner_hotel_id
        ,partner_unit_id
        ,hotel_id
        ,unit_id house_id 
    from ods_houseimport_config.api_unit 
    where unit_id > 0 
    and merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
    group by 1,2,3,4
) c 
on a.masterhotelid = c.partner_hotel_id
)
,active as (
select a.house_id
from (
    SELECT act_unit_id as house_id
    FROM dwd.dwd_tns_salespromotion_activity_detail_d
    WHERE dt = date_sub(current_date(),1) 
    AND audit_status = '2'
) a 
inner join h on a.house_id = h.house_id
group by 1
)
,jiudian as (
select percentile(fh_price_ctrip,0.5) jiudian_price_50
from (
    select a.masterhotelid
        ,percentile(fh_price,0.5) fh_price_ctrip 
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(current_date,30) AND date_sub(current_date, 1)
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门')) 
        and cityname = '首尔'
        and masterhotelid > 0 
        and is_standard = 1
        and star = 3 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1
) a 
)
    
    
select h.house_id
    ,lpv / nvl(order_num,1) `pv订单数`
    ,lpv
    ,order_num
    ,lpv_50_tujia `pv订单数_50分位`
    ,lpv_ctrip / nvl(order_num_ctrip,1) `pv订单数携程`
    ,lpv_ctrip
    ,order_num_ctrip
    ,lpv_50_ctrip `pv订单数_50分位携程`

    ,case when (lpv / order_num) > (lpv_50_tujia / order_num) then 1 else 0 end `pv订单数_50分位_对比`
    ,case when (lpv_ctrip / order_num_ctrip) > (lpv_50_ctrip / order_num_ctrip) then 1 else 0 end `pv订单数_50分位_对比携程`



    ,h.`户型` bedroom_count
    ,h.`景区` good_view 
    ,comment_score 
    ,picture_count
    ,`可售库存` / `物理库存` empty_rate
    ,facilities_num 
    ,fh_price_ctrip / jiudian_price_50  `vs_jiudian_price`
    ,case when active.house_id is not null then 1 else 0 end `is_active`

from h 
left join ldbo 
on h.house_id = ldbo.house_id
left join comment
on h.house_id = comment.house_id
left join facilities
on h.house_id = facilities.house_id
left join l2o_ctrip
on h.house_id = l2o_ctrip.house_id
left join active
on h.house_id = active.house_id
left join inventory
on h.house_id = inventory.house_id
left join (
    select percentile(lpv / order_num,0.5) lpv_50_tujia
    from ldbo
) pp_tujia
on 1 = 1 
left join (
    select percentile(lpv_ctrip / order_num_ctrip,0.5) lpv_50_ctrip
    from l2o_ctrip 
) pp_ctrip
on 1 = 1
left join jiudian 
on 1 = 1
