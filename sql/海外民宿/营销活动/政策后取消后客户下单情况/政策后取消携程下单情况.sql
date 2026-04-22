select a.user_id    
    ,a.third_id
    ,a.checkin_date checkin_date_before
    ,a.checkout_date checkout_date_before 
    ,a.city_name cityname_before
    
    ,d.checkin_date checkin_date_after
    ,d.checkout_date checkout_date_after
    ,d.countryname countryname_after
    ,d.cityname cityname_after
    ,d.masterhotelid masterhotelid_after
from (
    select user_id
        ,third_id 
        ,checkin_date 
        ,checkout_date
        ,city_name
    from (
        select user_id
            ,checkin_date
            ,checkout_date
            ,order_id
            ,city_name
        from dws.dws_order 
        where checkout_date >= '2025-11-15' 
        and is_overseas = 1
        and country_name = '日本' 
        and to_date(cancel_time) >= '2025-11-15' 
    ) a 
    inner join excel_upload.policy_cancel_japan_1201 b 
    on a.order_id = b.order_id  
    join (
        select lower(member_id) member_id 
            ,lower(third_id) third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId'  
        group by 1,2
    ) c 
    on a.user_id = c.member_id
    group by 1,2,3,4,5 
) a 
join (
    select a.uid
        ,a.clientid
        ,a.checkin_date
        ,a.checkout_date
        ,b.countryname	
        ,b.cityname
        ,a.masterhotelid
    from (
        select uid
            ,clientid
            ,masterhotelid
            ,date_sub(departure,cast(ciiquantity as int)) checkin_date
            ,to_date(departure) checkout_date
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,0)
        and distributer = 'ctrip' 
        and submitfrom='client'
        and orderstatus in ('S','P')
        and ordertype = 2 -- 酒店订单
        and clientid <> ''
        and clientid is not null
        and to_date(departure) >= '2025-11-15'
    ) a 
    inner join (
        select masterhotelid
            ,countryname
            ,cityname
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and masterhotelid > 0
        -- and (countryname != '中国' or cityname in ('香港','澳门'))
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1,2,3,4,5,6,7
) d 
on a.third_id = d.uid 
where a.checkin_date <= d.checkout_date
and a.checkout_date >= d.checkin_date



