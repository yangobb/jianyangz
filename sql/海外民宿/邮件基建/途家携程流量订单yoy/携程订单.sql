

SELECT '今年' time1 
    ,'携程' bu_type
    ,'预定' od_type 
    ,case when t11.ord_id is not null then '七大类' when t11.ord_id is null then '标准酒店' else '其他' end `是否七大类`
    ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM (
    select *
    from app_ctrip.edw_htl_order_all_split 
    WHERE submitfrom = 'client'
    AND TO_DATE(orderdate) between date_sub(current_date,14) and date_sub(current_date,1)
    AND orderstatus IN ('P','S')
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
) a 
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and bk_date between date_sub(current_date,14) and date_sub(current_date,1)
    and ord_status in ('P','S')
    and is_tcom = 0
) t11
on a.orderid = t11.ord_id 
group by 1,2,3,4,5

union all 

SELECT '去年' time1 
    ,'携程' bu_type
    ,'预定' od_type 
    ,case when t11.ord_id is not null then '七大类' when t11.ord_id is null then '标准酒店' else '其他' end `是否七大类`
    ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM (
    select *
    from app_ctrip.edw_htl_order_all_split 
    WHERE submitfrom = 'client'
    AND TO_DATE(orderdate) between add_months(date_sub(current_date,14),-12) and add_months(date_sub(current_date,1),-12)
    AND orderstatus IN ('P','S')
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
) a 
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and bk_date between add_months(date_sub(current_date,14),-12) and add_months(date_sub(current_date,1),-12)
    and ord_status in ('P','S')
    and is_tcom = 0
) t11
on a.orderid = t11.ord_id 
group by 1,2,3,4,5

union all 

SELECT '今年' time1 
    ,'携程' bu_type
    ,'离店' od_type 
    ,case when t11.ord_id is not null then '七大类' when t11.ord_id is null then '标准酒店' else '其他' end `是否七大类`
    ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM (
    select *
    from app_ctrip.edw_htl_order_all_split 
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between date_sub(current_date,14) and date_sub(current_date,1)
    AND orderstatus IN ('P','S')
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
) a 
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and checkout_date between date_sub(current_date,14) and date_sub(current_date,1)
    and ord_status in ('P','S')
    and is_tcom = 0
) t11
on a.orderid = t11.ord_id 
group by 1,2,3,4,5

union all 

SELECT '去年' time1 
    ,'携程' bu_type
    ,'离店' od_type 
    ,case when t11.ord_id is not null then '七大类' when t11.ord_id is null then '标准酒店' else '其他' end `是否七大类`
    ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM (
    select *
    from app_ctrip.edw_htl_order_all_split 
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between add_months(date_sub(current_date,14),-12) and add_months(date_sub(current_date,1),-12)
    AND orderstatus IN ('P','S')
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
) a 
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and checkout_date between add_months(date_sub(current_date,14),-12) and add_months(date_sub(current_date,1),-12)
    and ord_status in ('P','S')
    and is_tcom = 0
) t11
on a.orderid = t11.ord_id 
group by 1,2,3,4,5
