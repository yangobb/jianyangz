select cityname
    ,a.masterhotelid
    ,a.hotel_id 
    ,a.partner_unit_id
    ,a.room_type_id 
    ,a.house_id
    ,b.mgrgroupname
    ,b.brandname
    ,b.openyear
    ,b.fitmentyear
    ,b.ispkghotel
    ,b.star 
    ,b.zonename 
    ,b.is_standard 
    ,b.is_gold
    ,fh_price
    ,lpv_c
    ,luv_c
    ,dpv_c 
    ,duv_c 
    ,tagname1
    ,tagname2
    ,fromrailway
    ,fromairport
    ,fromcitycenter
    ,hotelrating
    ,ratingposit
    ,ratingroom
    ,ratingservice
    ,ratingcostbenefit
    ,c.country_name
    ,c.house_city_name
    ,c.hotel_name     
    ,c.dynamic_business
    ,c.is_jiudian
    ,c.is_tujia_sell
    ,gmv
    ,night
    ,od_cnt
    ,ms_gmv 
    ,ms_nights 
    ,ms_od_cnt 
    ,final_price
    ,lpv
    ,luv

from (
    select cityname
        ,masterhotelid
        ,b.hotel_id 
        ,c.partner_unit_id
        ,room_type_id 
        ,house_id
        ,sum(gmv) gmv
        ,sum(night) night
        ,count(distinct orderid) od_cnt
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
        AND TO_DATE(orderdate) between '2025-04-30' and '2025-05-06'
        AND orderstatus IN ('P','S')
        AND cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港')
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
            ,u.unit_id house_id  
            ,r.room_type_id
            ,r.room_id
        from (
            select partner_hotel_id
            ,partner_unit_id
            ,hotel_id
            ,unit_id 
            from ods_houseimport_config.api_unit 
            where unit_id > 0 
            and merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f' 
            group by 1,2,3,4
        ) u
        inner join (
            select house_id 
            from ods_tns_baseinfo.house_search 
            where can_sale = 1 
            and active = 1 
            and oversea = 1 
            group by 1
        ) s 
        on s.house_id = u.unit_id
        inner join (
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
        ,mgrgroupname
        ,brandname
        ,openyear
        ,fitmentyear
        ,ispkghotel
        ,star 
        ,zonename 
        ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        ,case when goldstar_ori in ('6','5') then '金特牌' else '其他' end is_gold
        ,tagname1
        ,tagname2
        ,fromrailway
        ,fromairport
        ,fromcitycenter
        ,hotelrating
        ,ratingposit
        ,ratingroom
        ,ratingservice
        ,ratingcostbenefit
    from app_ctrip.dimmasterhotel
    where d = date_sub(current_date,2)
    -- and (countryname != '中国'   or cityname in ('香港','澳门'))
    and cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港')
    and masterhotelid > 0 
) b 
on a.masterhotelid = b.masterhotelid
-- 房屋信息
left join (
    select hotel_id
        ,hotel_name
        ,house_id 
        ,case when country_name in ('日本','泰国','马来西亚','韩国','新加坡') then country_name when country_name = '中国大陆' then '港澳（中国）' else '其他' end country_name
        ,house_city_name    
        ,dynamic_business
        ,case when house_type = '标准酒店' then 1 else 0 end is_jiudian
        ,is_tujia_sell
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    AND house_is_oversea = 1
    and house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港')
    -- and house_city_name = '东京'
) c
on a.hotel_id = c.hotel_id
and a.house_id = c.house_id
-- 离店订单
left join (
    select house_id
        ,sum(real_pay_amount) ms_gmv 
        ,sum(order_room_night_count) ms_nights 
        ,count(distinct order_no) ms_od_cnt 
    from dws.dws_order
    where create_date between '2025-04-30' and '2025-05-06'
    and is_paysuccess_order = 1 --支付成功
    and is_cancel_order = 0 
    and city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港')
    and is_overseas = 1 
    group by 1 
) d
on a.house_id = d.house_id
-- 曝光
left join (
    select house_id 
        ,count(uid) lpv
        ,count(distinct uid,dt) luv 
        ,percentile(final_price,0.5) final_price
    FROM dws.dws_path_ldbo_d
    WHERE dt BETWEEN '2025-04-30' and '2025-05-06'
    AND source = 102
    AND user_type = '用户'       
    and is_oversea = 1 
    and city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港')
    -- and city_name = '东京'
    group by 1
) e
on a.house_id = e.house_id
left join (
    select 
        a.masterhotelid
        ,percentile(fh_price,0.5) fh_price
        ,count(1) lpv_c
        ,count(distinct d,cid) luv_c
        ,count(case when is_has_click = 1 then concat(d,cid) end) dpv_c 
        ,count(distinct case when is_has_click = 1 then concat(d,cid) end) duv_c 
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between '2025-04-30' and '2025-05-06'
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        -- and (countryname != '中国'   or cityname in ('香港','澳门'))
        and cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港')
        and masterhotelid > 0 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) f
on a.masterhotelid = f.masterhotelid


