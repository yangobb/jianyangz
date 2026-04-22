select a.user_id    
    ,a.uid
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
        ,uid 
        ,checkin_date 
        ,checkout_date
        ,city_name
    from (
        select user_id
            ,uid
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
    left join (
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
    select uid
        ,user_id 
        ,checkin_date
        ,checkout_date
        ,country_name countryname	
        ,city_name cityname
        ,house_id masterhotelid
    from dws.dws_order 
    where checkout_date >= '2025-11-15' 
    -- and is_overseas = 1 
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
    group by 1,2,3,4,5,6,7
) d 
on a.uid = d.uid 
where a.checkin_date <= d.checkout_date
and a.checkout_date >= d.checkin_date



