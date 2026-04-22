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

-- 房屋明细
with h as (
select hotel_id
    ,house_id
    ,landlord_channel
    ,house_is_online
    ,house_name
    ,is_cooking
    ,is_bring_pet
    ,picture_count
    ,case when bedroom_count=1 then '1'
        when bedroom_count=2 then '2'
        when bedroom_count>=3 then '3'
        end as bedroom_count
    ,share_type
    ,enum_house_facilities_name
    ,case when is_mountain_view=1 or is_sea_view=1 or is_garden_view=1 or is_lake_view=1 or  is_river_view=1 or is_great_river_view=1 or is_city_view=1 then 1 else 0 end as good_view
    ,case when is_fast_booking = 1 then 1 else 0 end is_fast_booking
from dws.dws_house_d  
where dt =date_sub(current_date(),1)
and house_is_oversea=1
and hotel_is_oversea=1
and house_is_online = 1 
and house_city_name = '大阪'
)

-- 结果
-- -- 途家
,ldbo_tj as (
select a.house_id
    ,lpv 
    ,luv 
    ,order_num
    ,night
    ,gmv
    ,final_price
    ,order_num / lpv l2o
from (
    select house_id
        ,count(1) lpv 
        ,count(distinct uid,dt) luv 
        ,percentile(final_price,0.5) final_price
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date,30) and date_sub(current_date,1)
    and source = 102
    and user_type = '用户'
    and is_oversea = 1
    and city_name = '大阪'
    group by 1 
) a 
join (
    select house_id
        ,count(distinct order_no) order_num
        ,sum(order_room_night_count) night
        ,sum(room_total_amount) gmv 
    from dws.dws_order 
    where create_date between date_sub(current_date,30) and date_sub(current_date,1)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    and city_name = '大阪'
    group by 1 
) b 
on a.house_id = b.house_id 
)
-- -- 携程
,ldbo_ctrip as (
select house_id
    ,lpv
    ,luv
    ,gmv
    ,night
    ,order_num
    ,final_price
from (
    select a.masterhotelid
        ,percentile(fh_price,0.5) final_price
        ,count(1) lpv
        ,count(distinct d,cid) luv
        ,count(case when is_has_click = 1 then concat(d,cid) end) dpv
        ,count(distinct case when is_has_click = 1 then concat(d,cid) end) duv
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
        and cityname = '大阪'
        and masterhotelid > 0 
        and is_standard = 0
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) a 
join (
    select masterhotelid  
        ,sum(ciireceivable) gmv
        ,sum(ciiquantity) night
        ,count(distinct orderid) order_num
    FROM app_ctrip.edw_htl_order_all_split
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between date_sub(current_date,30) and date_sub(current_date,1)
    AND orderstatus IN ('P','S')
    and cityname = '大阪'
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
    group by 1 
) b 
on a.masterhotelid = b.masterhotelid
inner join (
    select partner_hotel_id
        ,unit_id house_id 
    from ods_houseimport_config.api_unit 
    where unit_id > 0 
    and merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
    group by 1,2
) c 
on a.masterhotelid = c.partner_hotel_id
)
,final_ldbo as (
select h.house_id
    ,picture_count
    ,nvl(a.lpv,b.lpv) lpv
    ,nvl(a.luv,b.luv) luv
    ,nvl(a.order_num,b.order_num) order_num
    ,nvl(a.night,b.night) night
    ,nvl(a.gmv,b.gmv) gmv
    ,nvl(a.final_price,b.final_price) final_price
from h 
left join ldbo_tj a
on h.house_id = a.house_id
left join ldbo_ctrip b
on a.house_id = b.house_id
where nvl(a.order_num,b.order_num) is not null 
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
select unitid as house_id
    ,sum(instancecount) `物理库存`
    ,sum(avaliablecount) `可售库存` 
    ,sum(avaliablecount) / sum(instancecount) empty_rate
from (
    select * 
    from dim_tujiaproduct.unit_inventory_log a
    where createdate = current_date
    and substr(a.gettime,9,2) = '00' 
    and inventorydate between date_add(current_date,1) and date_add(current_date,14)
) a 
inner join (select house_id from h where landlord_channel = 1) h on a.unitid = h.house_id
group by 1
union all 
select a.house_id
    ,max(14) `物理库存`
    ,sum(can_booking) `可售库存` 
    ,sum(can_booking) / max(14) empty_rate
from (
    select house_id 
        ,can_booking
        ,checkin_date dt 
    from dwd.dwd_house_daily_price_d 
    where dt = date_sub(current_date,1)
    and checkin_date between date_add(current_date,1) and date_add(current_date,14)
) a 
inner join (select house_id from h where landlord_channel != 1) h on a.house_id = h.house_id
group by 1
)
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
-- 活动
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
-- im回复时间
,im as (
select a.hotel_id
    ,ceil(avg(round_time_length) / 60) round_time_length
from (
    select to_date(round_create_time) AS create_date
        ,hotel_id
        ,round_time_length
    from dwd.dwd_comment_im_conversation_d 
    where  to_date(round_create_time) >= date_sub(current_Date(),56)
    and hour(round_create_time) >= 8 AND hour(round_create_time) <= 21
    and round_initiator = '房客'
    and round_time_length > 0
) a 
inner join (
    select hotel_id
    from dws.dws_house_d 
    where dt = date_sub(current_Date,1)
    and house_city_name = '大阪'
    and house_is_online = 1 
    and house_is_oversea = 1 
    group by 1 
) b 
on a.hotel_id = b.hotel_id
group by 1  
)
-- 酒店价格
,jiudian as (
select percentile(fh_price,0.5) jiudian_price_50
from (
    select a.masterhotelid
        ,percentile(fh_price,0.5) fh_price
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
        and cityname = '大阪'
        and masterhotelid > 0 
        and is_standard = 1
        and star = 3 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1
) a 
)
    
    
select h.house_id
    ,lpv
    ,order_num  
    
    -- 结果指标
    ,lpv / nvl(order_num,1) lpv_order_num
    ,lpv_order_num_50
    
    -- 特征指标
    ,h.bedroom_count
    ,h.good_view 
    ,comment_score 
    ,h.picture_count
    ,empty_rate
    ,final_price
    ,facilities_num  
    ,is_fast_booking
    ,round_time_length
    
    ,case when active.house_id is not null then 1 else 0 end `is_active`

    ,vs_jiudian_price_50
    ,facilities_num_50
    ,jiudian_price_50
    ,comment_score_50
    ,empty_rate_50
    ,round_time_length_50
    ,picture_count_50
from h
join final_ldbo fl
on h.house_id = fl.house_id
left join comment
on h.house_id = comment.house_id
left join facilities
on h.house_id = facilities.house_id
left join active
on h.house_id = active.house_id
left join inventory
on h.house_id = inventory.house_id
left join im 
on h.hotel_id = im.hotel_id
left join (
    select percentile(lpv / order_num,0.5) lpv_order_num_50
        ,percentile(final_price / jiudian_price_50,0.5) vs_jiudian_price_50 
        ,max(jiudian_price_50) jiudian_price_50
        ,percentile(picture_count,0.5) picture_count_50
    from final_ldbo 
    left join jiudian 
    on 1 = 1
) fl_50
on 1 = 1
left join (
    select percentile(facilities_num,0.5) facilities_num_50
    from facilities
) facilities_50 
on 1 = 1 
left join (
    select percentile(comment_score,0.5) comment_score_50
    from comment 
) comment_score_50 
on 1 = 1 
left join (
    select percentile(empty_rate,0.5) empty_rate_50
    from inventory 
) empty_rate_50
on 1 = 1 
left join (
    select percentile(round_time_length,0.5) round_time_length_50
    from im 
) im_50 
on 1 = 1