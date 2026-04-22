select a.countryname
    ,a.cityname
    ,lpv
    ,luv 
    ,order_num
    ,gmv
    ,night
from (
    select countryname
        ,cityname
        ,count(1) lpv 
        ,count(distinct t1.cid,d) luv 
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between '2025-11-01' and '2025-11-30'
        and fh_price > 0 
    ) t1 
    JOIN excel_upload.dim_ctrip_list_qid_city t2
    ON t1.m_city = t2.m_city 
    group by 1,2
) a 
join (
    select t2.countryname
        ,t2.cityname
        ,count(distinct orderid) order_num
        ,sum(ciireceivable) gmv
        ,sum(ciiquantity) night
    from (
        select *
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,1)
        and to_date(orderdate) between '2025-11-01' and '2025-11-30'
        -- and submitfrom='client'
        and orderstatus in ('S','P')
        and ordertype = 2 -- 酒店订单
        -- and clientid <> ''
        -- and clientid is not null
    ) t1 JOIN excel_upload.dim_ctrip_list_qid_city t2
    ON t1.cityid = t2.m_city 
    group by 1,2
) b 
on a.countryname = b.countryname
and a.cityname = b.cityname


