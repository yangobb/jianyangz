SELECT case when weekofyear(create_date) = 1 and month(create_date) = 12 then year(create_date) + 1
        when weekofyear(create_date) >= 52 and month(create_date) = 1 then year(create_date) - 1
        else year(create_date)
      END  time1
  ,'携程' bu_type
  ,'预定' od_type  
  ,weekofyear(create_date) year_week
  ,case when t11.ord_id is not null then '七大类' when t11.ord_id is null then '标准酒店' else '其他' end `是否七大类`
  ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end city_name
  ,count(distinct orderid) order_cnt
  ,sum(ciireceivable) gmv
  ,sum(ciiquantity) night
  ,count(distinct case when orderstatus IN ('P','S') then orderid end) done_order_cnt
  ,sum(case when orderstatus IN ('P','S') then ciireceivable end) done_gmv
  ,sum(case when orderstatus IN ('P','S') then ciiquantity end) done_night
FROM (
    select *
        ,to_date(orderdate) create_date
    from app_ctrip.edw_htl_order_all_split
    WHERE d = current_Date()
    AND TO_DATE(orderdate) between '2024-01-01' and date_sub(next_day(current_date, 'MO'), 8)
    AND orderstatus IN ('P','S','C')
    AND ordertype = 2 -- 酒店订单
    and (country != 1 or (country = 1 and cityid in (58,59)))
) a
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and bk_date between '2024-01-01' and date_sub(next_day(current_date, 'MO'), 8)
    and ord_status in ('P','S','C')
    and is_tcom = 0
    and chl != 'API分销'
) t11
on a.orderid = t11.ord_id
group by 1,2,3,4,5,6
union all 
SELECT case when weekofyear(create_date) = 1 and month(create_date) = 12 then year(create_date) + 1
        when weekofyear(create_date) >= 52 and month(create_date) = 1 then year(create_date) - 1
        else year(create_date)
      END  time1
  ,'携程' bu_type
  ,'离店' od_type
  ,weekofyear(create_date) year_week
  ,case when t11.ord_id is not null then '七大类' when t11.ord_id is null then '标准酒店' else '其他' end `是否七大类`
  ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end city_name
  ,count(distinct orderid) order_cnt
  ,sum(ciireceivable) gmv
  ,sum(ciiquantity) night
  ,count(distinct case when orderstatus IN ('P','S') then orderid end) done_order_cnt
  ,sum(case when orderstatus IN ('P','S') then ciireceivable end) done_gmv
  ,sum(case when orderstatus IN ('P','S') then ciiquantity end) done_night
FROM (
    select *
        ,to_date(departure) checkout_date
    from app_ctrip.edw_htl_order_all_split
    WHERE d = current_Date()
    AND TO_DATE(departure) between '2024-01-01' and date_sub(next_day(current_date, 'MO'), 8)
    AND orderstatus IN ('P','S','C')
    AND ordertype = 2 -- 酒店订单
    and (country != 1 or (country = 1 and cityid in (58,59)))
) a
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and checkout_date between '2024-01-01' and date_sub(next_day(current_date, 'MO'), 8)
    and ord_status in ('P','S','C')
    and is_tcom = 0
    and chl != 'API分销'
) t11
on a.orderid = t11.ord_id
group by 1,2,3,4,5,6
union all 
select case when weekofyear(create_date) = 1 and month(create_date) = 12 then year(create_date) + 1
        when weekofyear(create_date) >= 52 and month(create_date) = 1 then year(create_date) - 1
        else year(create_date)
      END  time1
  ,'途家' bu_type
  ,'预定' od_type
  ,weekofyear(create_date) year_week
  ,'民宿' is_standard
  ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
  ,count(distinct order_no) order_cnt
  ,sum(room_total_amount) gmv
  ,sum(order_room_night_count) night
  ,count(distinct case when is_cancel_order	= 0 then order_no end) done_order_cnt
  ,sum(case when is_cancel_order = 0 then room_total_amount end) done_gmv
  ,sum(case when is_cancel_order = 0 then order_room_night_count end) done_night
from dws.dws_order
where create_date between '2024-01-01' and date_sub(next_day(current_date, 'MO'), 8)
and is_paysuccess_order = 1
and is_overseas = 1
group by 1,2,3,4,5,6
union all
select case when weekofyear(create_date) = 1 and month(create_date) = 12 then year(create_date) + 1
        when weekofyear(create_date) >= 52 and month(create_date) = 1 then year(create_date) - 1
        else year(create_date)
      END  time1
  ,'途家' bu_type
  ,'离店' od_type
  ,weekofyear(create_date) year_week
  ,'民宿' is_standard
  ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
  ,count(distinct order_no) order_cnt
  ,sum(room_total_amount) gmv
  ,sum(order_room_night_count) night
  ,count(distinct case when is_cancel_order	= 0 and is_done = 1 then order_no end) done_order_cnt
  ,sum(case when is_cancel_order = 0 and is_done = 1 then room_total_amount end) done_gmv
  ,sum(case when is_cancel_order = 0 and is_done = 1 then order_room_night_count end) done_night
from dws.dws_order
where checkout_date between '2024-01-01' and date_sub(next_day(current_date, 'MO'), 8)
and is_paysuccess_order = 1
and is_overseas = 1
group by 1,2,3,4,5,6

