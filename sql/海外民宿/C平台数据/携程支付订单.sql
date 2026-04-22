
select 
    '今年' time1 
    ,weekofyear(create_date) week1  
    ,case when t2.cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then t2.cityname else '其他' end city_name
    ,case when datediff(checkin_date,create_date) = 0 then 'T0'
        when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
        when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
        when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
        when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
        when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
        else 'T61' end date_gap
    ,count(distinct orderid) order_num
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night
from (
    select *
        ,to_date(orderdate) create_date
        ,date_sub(to_date(departure),cast(nvl(ciiquantity,0) as int)) checkin_date 
        ,to_date(departure) checkout_date
    from app_ctrip.edw_htl_order_all_split
    where d = date_sub(current_date,1)
    and to_date(departure) between '2025-11-03' and '2025-12-21' 
    and orderstatus in ('S','P')
    and ordertype = 2 -- 酒店订单 
) t1 JOIN excel_upload.dim_ctrip_list_qid_city t2
ON t1.cityid = t2.m_city 
group by 1,2,3,4 
union all 
select 
    '去年' time1 
    ,weekofyear(create_date) week1 
    ,case when t2.cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then t2.cityname else '其他' end city_name
    ,case when datediff(checkin_date,create_date) = 0 then 'T0'
        when datediff(checkin_date,create_date) between 1 and 7 then 'T7'
        when datediff(checkin_date,create_date) between 8 and 14 then 'T14'
        when datediff(checkin_date,create_date) between 15 and 21 then 'T21'
        when datediff(checkin_date,create_date) between 22 and 28 then 'T28'
        when datediff(checkin_date,create_date) between 29 and 60 then 'T60'
        else 'T61' end date_gap
    ,count(distinct orderid) order_num
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night
from (
    select *
        ,to_date(orderdate) create_date
        ,date_sub(to_date(departure),cast(nvl(ciiquantity,0) as int)) checkin_date 
        ,to_date(departure) checkout_date
    from app_ctrip.edw_htl_order_all_split
    where d = date_sub(current_date,1)
    and to_date(departure) between '2024-11-04' and '2024-12-22' 
    and orderstatus in ('S','P')
    and ordertype = 2 -- 酒店订单 
) t1 JOIN excel_upload.dim_ctrip_list_qid_city t2
ON t1.cityid = t2.m_city 
group by 1,2,3,4 


