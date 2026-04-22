with cross_uid as (
select a.dt  
    ,a.T0Tn
    ,a.uid
from (
    select 
        dt 
        ,case when datediff(checkin_date,dt) = 0 then 'T0'
            when datediff(checkin_date,dt) between 1 and 7 then 'T7'
            when datediff(checkin_date,dt) between 8 and 14 then 'T14'
            when datediff(checkin_date,dt) between 15 and 21 then 'T21'
            when datediff(checkin_date,dt) between 22 and 28 then 'T28'
            when datediff(checkin_date,dt) between 29 and 60 then 'T60'
            else 'T61' end T0Tn 
        ,lower(uid) uid
    from dws.dws_path_ldbo_d t1 
    where dt between date_sub(next_day(current_date,'MO'),49) and date_sub(next_day(current_date,'MO'),8) 
    and wrapper_name = '携程' 
    and source = 102
    and user_type = '用户'
    and is_oversea = 1 
    group by 1,2,3
) a  
inner join (
    select d dt  
        ,case when datediff(checkin_date,create_date) = 0 then 'T0'
            when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
            when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
            when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
            when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
            when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
            else 'T61' end T0Tn 
        ,lower(cid) uid  
    from (
        select *
            ,checkin checkin_date
            ,checkout checkout_date 
            ,d create_date
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(next_day(current_date,'MO'),49) and date_sub(next_day(current_date,'MO'),8) 
        and fh_price > 0 
    ) a 
    inner join excel_upload.dim_ctrip_list_qid_city b 
    on a.m_city = b.m_city
    group by 1,2,3
) b 
on a.dt = b.dt 
and a.uid = b.uid
and a.T0Tn = b.T0Tn
)
,list as (
select a.dt
    ,a.T0Tn
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
        ,case when datediff(checkin_date,create_date) = 0 then 'T0'
            when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
            when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
            when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
            when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
            when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
            else 'T61' end T0Tn 
        ,count(distinct orderid) od_cnt 
        ,sum(ciireceivable) hotel_gmv
        ,sum(ciiquantity) hotel_nights
        ,sum(ciireceivable) / sum(ciiquantity) hotel_adr
    from (
        select *
            ,to_date(orderdate) create_date
            ,date_sub(to_date(departure),cast(nvl(ciiquantity,0) as int)) checkin_date 
            ,to_date(departure) checkout_date
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,0)
        and to_date(orderdate) between date_sub(next_day(current_date,'MO'),49) and date_sub(next_day(current_date,'MO'),8) 
        and orderstatus in ('S','P')
        and ordertype = 2 -- 酒店订单
    ) a 
    inner join excel_upload.dim_ctrip_list_qid_city b 
    on a.cityid = b.m_city
    group by 1,2,3
) b
ON a.dt = b.dt 
and a.uid = b.uid
and a.T0Tn = b.T0Tn
left join (
    select to_date(create_date) dt 
        ,lower(uid) uid 
        ,case when datediff(checkin_date,create_date) = 0 then 'T0'
            when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
            when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
            when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
            when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
            when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
            else 'T61' end T0Tn 
        ,count(distinct order_no) ms_od_cnt 
        ,sum(order_room_night_count) ms_nights	
        ,sum(real_pay_amount) ms_gmv 
        ,sum(real_pay_amount) / sum(order_room_night_count) ms_adr 
    from dws.dws_order a 
    where create_date between date_sub(next_day(current_date,'MO'),49) and date_sub(next_day(current_date,'MO'),8) 
    and is_paysuccess_order = 1 --支付成功
    and is_risk_order = 0
    and is_overseas = 1 
    group by 1,2,3
) c
ON a.dt = c.dt 
and a.uid = c.uid
and a.T0Tn = c.T0Tn
group by 1,2 
)
select * from list