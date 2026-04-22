select countryname
    ,is_standard
    ,case when datediff(checkout_date,dt) >= 179 then '180' 
        when datediff(checkout_date,dt) >= 149 then '150' 
        when datediff(checkout_date,dt) >= 119 then '120' 
        when datediff(checkout_date,dt) >= 89 then '90' 
        when datediff(checkout_date,dt) >= 59 then '60' 
        when datediff(checkout_date,dt) >= 29 then '30'
        when datediff(checkout_date,dt) >= 0 then '29'  
        else '其他' end od_type 
    ,count(distinct case when is_cancel_order = 1 then orderid end) cancel_order_num
    ,sum(case when is_cancel_order = 1 then night end) cancel_night
    ,sum(case when is_cancel_order = 1 then gmv end) cancel_gmv
    ,count(distinct orderid) order_num
    ,sum(night) night
    ,sum(gmv) gmv
from (
    select to_date(orderdate) as dt
        ,to_date(departure) checkout_date
        ,cityname 
        ,room
        ,masterhotelid
        ,clientid as uid
        ,case when orderstatus = 'C' then 1 else 0 end is_cancel_order
        ,orderid
        ,ciiquantity  night
        ,ciireceivable gmv
    from app_ctrip.edw_htl_order_all_split
    where d =  current_date()
    and to_date(departure) between '2025-10-01' and '2025-10-31'
    and submitfrom='client'  --携程app酒店
    and orderstatus in ('P','S','C') -- 离店口径
    and ordertype = 2 -- 酒店订单
    and (country != 1 or cityname in ('香港','澳门'))
) a 
inner join (
    select is_standard
        ,masterhotelid
        ,case when countryname in ('泰国','日本','韩国','中国','越南','马来西亚') then countryname else '其他' end countryname
    from app_ctrip.dimmasterhotel   --C酒店基础信息表
    where d = date_sub(current_date(),1)
    and masterhotelid > 0 -- 母酒店ID有值
    and is_standard != '-1'
) b 
on a.masterhotelid = b.masterhotelid
group by 1,2,3 