with cross_uid as (
select a.dt  
    ,a.uid
from (
    select 
        dt 
        ,lower(uid) uid
    from dws.dws_path_ldbo_d t1 
    where dt between date_sub(current_date,30) and date_sub(current_date,1)
    and wrapper_name = '携程' 
    and source = 102
    and user_type = '用户'
    and is_oversea = 1 
    group by 1,2
) a  
inner join (
    select d dt  
        ,lower(cid) uid  
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(current_date,30) and date_sub(current_date,1)
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
            ,cityname
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门')) 
        and masterhotelid > 0
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1,2
) b 
on a.dt = b.dt 
and a.uid = b.uid
)
,list as (
select a.dt
    ,count(a.uid) `交叉uv`
    ,count(b.uid) `携程下单uv`
    ,count(c.uid) `途家下单uv`
    ,sum(od_cnt) `携程订单量`
    ,sum(hotel_gmv) `酒店GMV`
    ,sum(hotel_nights) `酒店间夜`
    ,avg(ms_od_cnt) `途家订单数`
    ,sum(ms_nights)	`途家间夜`
    ,sum(ms_gmv) `途家GMV`
from cross_uid a 
left join (
    select to_date(orderdate) dt 
        ,lower(clientid) uid 
        ,count(distinct orderid) od_cnt 
        ,sum(ciireceivable) hotel_gmv
        ,sum(ciiquantity) hotel_nights
        ,sum(ciireceivable) / sum(ciiquantity) hotel_adr
    from (
        select *
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,0)
        and to_date(orderdate) between date_sub(current_date,30) and date_sub(current_date,1)
        and distributer = 'ctrip' 
        and submitfrom='client'
        and orderstatus in ('S','P')
        and ordertype = 2 -- 酒店订单
        and clientid <> ''
        and clientid is not null
    ) a 
    inner join (
        select masterhotelid
            ,cityname
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门'))
        and masterhotelid > 0
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1,2
) b
ON a.dt = b.dt 
and a.uid = b.uid
left join (
    select to_date(create_date) dt 
        ,lower(uid) uid 
        ,count(distinct order_no) ms_od_cnt 
        ,sum(order_room_night_count) ms_nights	
        ,sum(real_pay_amount) ms_gmv 
        ,sum(real_pay_amount) / sum(order_room_night_count) ms_adr 
    from dws.dws_order a 
    where create_date between date_sub(current_date,30) and date_sub(current_date,1)
    and is_paysuccess_order = 1 --支付成功
    and is_risk_order = 0
    and is_overseas = 1 
    group by 1,2
) c
ON a.dt = c.dt 
and a.uid = c.uid
group by 1
)
select * from list