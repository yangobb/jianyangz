
select cityname
    ,a.masterhotelid
    ,a.hotel_id
    ,a.partner_unit_id
    ,a.room_type_id
    ,a.house_id
    ,c.dynamic_business
    ,c.is_jiudian
    ,c.is_tujia_sell    
    ,b.`酒店名称`
    ,b.`房屋类型一级`
    ,b.`房屋类型二级`
    ,b.`酒店星级`
    ,b.`大酒店城市`
    ,b.`行政区`
    ,b.`商圈`
    ,b.`开业/装修年份`
    ,b.`开业/装修时间`
    ,b.`距离火车站距离`
    ,b.`距离机场距离`
    ,b.`酒店谷歌经度`
    ,b.`酒店谷歌纬度` 
    ,b.`点评人数`
    ,b.`酒店评分`
    ,b.`酒店位置评分`
    ,b.`酒店卫生评分`
    ,b.`酒店服务评分`
    ,b.`酒店设施评分`
    ,b.`用户推荐比例`
    ,b.`是否华人礼遇`
    ,b.`是否民宿`
    ,b.`是否Top5集团`
    ,b.`是否Top8集团`
    ,b.`有效城市名称`
    ,b.`酒店dril等级`
    ,b.`是否标杆酒店`
    ,b.`是否开通im`
    ,b.`城市等级`
    ,b.`酒店房屋数`
    ,b.`标非标` 
    ,`是否金特` 
    ,`携程曝光价`
    ,`携程lpv`
    ,`携程luv`
    ,`携程dpv` 
    ,`携程duv`
    ,`携程订单数` 
    ,`携程gmv`
    ,`携程间夜`
    ,`民宿曝光价`
    ,`民宿lpv`
    ,`民宿luv`
    ,`民宿订单数`
    ,`民宿gmv`
    ,`民宿间夜`
from (
    select cityname 
        ,masterhotelid
        ,b.hotel_id 
        ,c.partner_unit_id
        ,room_type_id 
        ,house_id
        ,sum(gmv) `携程gmv`
        ,sum(night) `携程间夜`
        ,count(distinct orderid) `携程订单数`
    from (
        select masterhotelid
            ,hotel        
            ,cityname
            ,room -- 售卖房型
            ,ciireceivable gmv 
            ,ciiquantity night
            ,orderid 
        FROM app_ctrip.edw_htl_order_all_split
        WHERE submitfrom = 'client'
        AND TO_DATE(orderdate) between date_sub(current_date,30) and date_sub(current_date,1)
        AND orderstatus IN ('P','S') 
        and cityname = '曼谷'
        AND ordertype = 2 -- 酒店订单
        and d = current_Date()
    ) a 
    left join (
        select partner_hotel_id --  merchant_guid	
            ,hotel_id 	
        from ods_houseimport_config.api_hotel
        where status = 1
        group by 1,2
    ) b
    on a.masterhotelid = b.partner_hotel_id
    left join (
        select u.partner_hotel_id
            ,u.partner_unit_id
            ,u.hotel_id
            ,u.house_id   
            ,r.room_type_id
            ,r.room_id
        from (
            -- api接入房屋快照表
            select partner_hotel_id
                ,partner_unit_id
                ,hotel_id
                ,unit_id house_id
            from ods_houseimport_config.api_unit
            where status = 1
            and merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
            and unit_id > 0
            group by 1,2,3,4
        ) u
        inner join (
            -- 售卖房型id与房型id 
            select room_type_id
                ,room_id
            from ods_distributionmanager.ctrip_pre_analyze_room
            group by 1,2
        ) r 
        on r.room_type_id = u.partner_unit_id 
        group by 1,2,3,4,5,6 
    ) c
    on b.partner_hotel_id = c.partner_hotel_id
    and b.hotel_id = c.hotel_id 
    and a.room = c.room_id
    group by 1,2,3,4,5,6
) a
left join (
    select
        masterhotelid
        ,tagname1 `房屋类型一级`
        ,tagname2 `房屋类型二级`
        ,hotelname `酒店名称`
        ,star `酒店星级`
        ,bigcityname `大酒店城市`
        ,locationname `行政区`
        ,zonename `商圈` 
        ,greatest(openyear,fitmentyear) `开业/装修年份`
        ,greatest(opendate,fitmentdate) `开业/装修时间`
        ,fromrailway `距离火车站距离`
        ,fromairport `距离机场距离`
        ,glon `酒店谷歌经度`
        ,glat `酒店谷歌纬度`
        ,lat `酒店纬度`
        ,lon `酒店经度`
        ,hotelnameabbreviation `酒店名简称`
        ,novoters `点评人数`
        ,hotelrating `酒店评分`
        ,ratingposit `酒店位置评分`
        ,ratingroom `酒店卫生评分`
        ,ratingservice `酒店服务评分`
        ,ratingcostbenefit `酒店设施评分`
        ,recommend `用户推荐比例`
        ,is_chinahotel `是否华人礼遇`
        ,isfamilystay `是否民宿`
        ,is_top5group `是否Top5集团`
        ,is_top8group `是否Top8集团`
        ,last_valid_cityname `有效城市名称`
        ,case when drilllevel = 1 then '金钻' when drilllevel = 2 then '铂钻' end `酒店dril等级`
        ,ifbenchmark `是否标杆酒店`
        ,ifim `是否开通im`
        ,citylevel `城市等级`
        ,case when is_standard = 0 then '七大类' else '标准酒店' end `标非标`
        ,case when goldstar_ori in ('6','5') then '金特牌' else '其他' end `是否金特`
        ,roomquantity `酒店房屋数`
    from app_ctrip.dimmasterhotel
    where d = date_sub(current_date,2)
    and cityname = '曼谷'
    and masterhotelid > 0
) b
on a.masterhotelid = b.masterhotelid
-- 房屋信息
left join (
    select hotel_id
        ,hotel_name
        ,house_id
        ,house_city_name
        ,dynamic_business
        ,case when house_type = '标准酒店' then 1 else 0 end is_jiudian
        ,is_tujia_sell
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    and house_is_online = 1
    AND house_is_oversea = 1
    and house_city_name = '曼谷'
) c
on a.hotel_id = c.hotel_id
and a.house_id = c.house_id
-- 离店订单
left join (
    select house_id
        ,sum(real_pay_amount) `民宿gmv`
        ,sum(order_room_night_count) `民宿间夜`
        ,count(distinct order_no) `民宿订单数`
    from dws.dws_order
    where create_date between date_sub(current_date,30) and date_sub(current_date,1)
    and is_paysuccess_order = 1
    and is_cancel_order = 0
    and city_name = '曼谷'
    and is_overseas = 1
    group by 1
) d
on a.house_id = d.house_id
-- 曝光
left join (
    select house_id
        ,count(uid) `民宿lpv`
        ,count(distinct uid,dt) `民宿luv`
        ,percentile(final_price,0.5) `民宿曝光价`
    FROM dws.dws_path_ldbo_d
    WHERE dt BETWEEN date_sub(current_date,30) and date_sub(current_date,1)
    AND source = 102
    AND user_type = '用户'
    and is_oversea = 1
    and city_name = '曼谷'
    group by 1
) e
on a.house_id = e.house_id
left join (
    select
        a.masterhotelid
        ,percentile(fh_price,0.5) `携程曝光价`
        ,count(1) `携程lpv`
        ,count(distinct d,cid) `携程luv`
        ,count(case when is_has_click = 1 then concat(d,cid) end) `携程dpv`
        ,count(distinct case when is_has_click = 1 then concat(d,cid) end) `携程duv`
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(current_date,30) and date_sub(current_date,1)
        and fh_price > 0
    ) a
    inner join (
        select masterhotelid
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and cityname = '曼谷'
        and masterhotelid > 0
    ) b
    on a.masterhotelid = b.masterhotelid
    group by 1
) f
on a.masterhotelid = f.masterhotelid

