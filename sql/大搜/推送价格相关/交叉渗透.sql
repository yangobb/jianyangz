with cross_uid as(
select a.dt 
    ,a.uid
from (
    select 
        dt 
        ,uid
    from dws.dws_path_ldbo_d t1 
    where dt in ('2025-04-17','2025-05-15')
    and wrapper_name = '携程' 
    and source = 102
    and user_type = '用户'
    and is_oversea = 0
    group by 1,2 
) a 
inner join (
    select d.d dt 
        ,d.cid uid  
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day d 
    where d.d in ('2025-04-17','2025-05-15')
    and fh_price > 0 
    group by 1,2 
) b 
on a.dt = b.dt 
and lower(a.uid) = lower(b.uid)
)
,hotel_ord_info as(
select
    to_date(orderdate) as dt
    ,clientid as uid 
    ,count(distinct orderid) hotel_od_cnt
    ,sum(ciireceivable) hotel_gmv
    ,sum(ciiquantity) hotel_nights
    ,sum(ciireceivable) / sum(ciiquantity) hotel_adr 
from (
    select *
    from app_ctrip.edw_htl_order_all_split
    where d = date_sub(current_date,0)
    and to_date(orderdate) in ('2025-04-17','2025-05-15')
    and submitfrom='client'
    and orderstatus in ('S','P')
    and country = 1
    and ordertype = 2 -- 酒店订单
    and clientid <> ''
    and clientid is not null
) t1
LEFT JOIN (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select masterhotelid 
        ,max(case when goldstar_ori in ('5','6') then 1 else 0 end) is_gold
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date,2)                             
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 0  --是否标准酒店 1：是、0：否 
    group by 1 
) t2
ON t1.masterhotelid = t2.masterhotelid 
group by 1,2
)
,od as (
select to_date(create_date) dt 
    ,uid
    ,count(distinct order_no) ms_od_cnt 
    ,sum(order_room_night_count) ms_od_night	
    ,sum(real_pay_amount) ms_od_gmv 
    ,sum(real_pay_amount) / sum(order_room_night_count) ms_adr 
from dws.dws_order a 
where create_date in ('2025-04-17','2025-05-15')
and is_paysuccess_order = 1 --支付成功
and is_success_order = 1 
and is_overseas = 0 
group by 1,2
)

,cross_info as (
select 
    cross_uid.uid
    ,cross_uid.dt
    ,nvl(hotel_od_cnt,0) hotel_od_cnt
    ,nvl(hotel_gmv,0) hotel_gmv
    ,nvl(hotel_nights,0) hotel_nights
    ,nvl(ms_od_cnt,0) ms_od_cnt 
    ,nvl(ms_od_night,0) ms_od_night	
    ,nvl(ms_od_gmv,0) ms_od_gmv
    ,hotel_adr
    ,ms_adr
    
from cross_uid
left join hotel_ord_info
on cross_uid.dt = hotel_ord_info.dt 
and lower(cross_uid.uid) = lower(hotel_ord_info.uid)
left join od 
on cross_uid.dt = od.dt 
and lower(cross_uid.uid) = lower(od.uid)
)


select  a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
        
        ,hotel_od_cnt
        ,hotel_gmv
        ,hotel_nights
        ,ms_od_cnt
        ,ms_od_night
        ,ms_od_gmv
        
        ,hotel_adr
        ,ms_adr
        
        ,ms_od_night / hotel_nights `交叉抢夺率`
from (
    select a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
        
        ,sum(hotel_od_cnt) hotel_od_cnt
        ,sum(hotel_gmv) hotel_gmv
        ,sum(hotel_nights) hotel_nights
        ,sum(ms_od_cnt) ms_od_cnt 
        ,sum(ms_od_night) ms_od_night	
        ,sum(ms_od_gmv) ms_od_gmv
        ,percentile(hotel_adr,0.5) hotel_adr
        ,percentile(ms_adr,0.5) ms_adr 
        
    from (
          select 
                dt
                ,bucket
                ,fromforlog
                ,search_type
                ,is_new
                ,if_click
                ,uid
            from tujia_tmp.list_rank_jianyang_0611_v1
            group by 1,2,3,4,5,6,7 
    ) a 
    join cross_info b 
    on a.dt = b.dt 
    and lower(a.uid) = lower(b.uid)
    group by a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
) a 
left join (
    select 
        a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
        ,percentile(final_price,0.5) ms_price_1 
    from (
          select 
                dt
                ,bucket
                ,fromforlog
                ,search_type
                ,is_new
                ,if_click
                ,uid
            from tujia_tmp.list_rank_jianyang_0611_v1
            group by 1,2,3,4,5,6,7 
    ) a 
    left join (
        select dt 
            ,uid 
            ,final_price
        from dws.dws_path_ldbo_d 
        where dt in ('2025-04-17','2025-05-15')
        and wrapper_name = '携程' 
        and source = 102
        and user_type = '用户'
        and is_oversea = 0
    ) b 
    on a.dt = b.dt 
    and lower(a.uid) = lower(b.uid)
    group by 
        a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
) b
on a.dt = b.dt
and a.bucket = b.bucket
and a.fromforlog = b.fromforlog
and a.search_type = b.search_type
and a.is_new = b.is_new
and a.if_click = b.if_click

left join (
    select 
        a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
        ,percentile(fh_price,0.5) jd_price_1 
    from (
          select 
                dt
                ,bucket
                ,fromforlog
                ,search_type
                ,is_new
                ,if_click
                ,uid
            from tujia_tmp.list_rank_jianyang_0611_v1
            group by 1,2,3,4,5,6,7 
    ) a 
    left join (
        select d dt 
            ,cid uid
            ,fh_price
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day d 
        where d.d in ('2025-04-17','2025-05-15')
        and fh_price > 0 
    ) b 
    on a.dt = b.dt 
    and lower(a.uid) = lower(b.uid)
    group by 
        a.dt
        ,a.bucket
        ,a.fromforlog
        ,a.search_type
        ,a.is_new
        ,a.if_click
) c 

on a.dt = c.dt
and a.bucket = c.bucket
and a.fromforlog = c.fromforlog
and a.search_type = c.search_type
and a.is_new = c.is_new
and a.if_click = c.if_click