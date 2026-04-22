
-- 早鸟特惠
select a.house_id
    ,house_city_name
    ,'早鸟特惠' activiy_name
    ,a.activity_create_date `活动最后一次调整时间`
    ,nvl(a1.activity_create_date,last_day(add_months(current_date,-1))) `统计截至时间`
    ,cast(regexp_extract_all(a.operate_content,'报名折扣:(\\d+)',1) as string) `报名折扣`
    ,array_max(regexp_extract_all(a.operate_content,'报名折扣:(\\d+)',1)) `最低折扣`
    ,cast(regexp_extract_all(a.operate_content,'提前预定天数:(\\d+)',1) as string) `提前预定天数`
    ,array_min(regexp_extract_all(a.operate_content,'提前预定天数:(\\d+)',1)) `最低预定天数`

    ,cast(regexp_extract_all(a.operate_content,'连住天数:(\\d+)',1) as string) `提前预定天数`
    ,array_min(regexp_extract_all(a.operate_content,'连住天数:(\\d+)',1)) `最低预定天数`
from (
    select * from (
        select *
            ,to_date(create_time) activity_create_date
            ,row_number() over(partition by house_id order by create_time desc) rn 
        from ods_tns_baseinfo.house_log
        where substr(to_date(create_time),1,7) = substr(last_day(add_months(current_date,-1)),1,7)
        and operate_platform = '营销系统'
        and operate_type in ('商务端异步报名','修改报名信息')
        -- and operate_type = '放弃报名'
        and operate_content like '%早鸟特惠%'
    ) tmp 
    where rn = 1 
) a 
left join (
    select * from (
        select *
            ,to_date(create_time) activity_create_date
            ,row_number() over(partition by house_id order by create_time desc) rn 
        from ods_tns_baseinfo.house_log
        where substr(to_date(create_time),1,7) = substr(last_day(add_months(current_date,-1)),1,7)
        and operate_platform = '营销系统'
        -- and operate_type in ('商务端异步报名','修改报名信息')
        and operate_type = '放弃报名'
        and operate_content like '%早鸟特惠%'
    ) tmp 
    where rn = 1 
) a1 
on a.house_id = a1.house_id 
and a.activity_create_date <= a1.activity_create_date
inner join (
    select house_id
        ,house_city_name
    from dws.dws_house_d 
    where dt = last_day(add_months(current_date,-1))
    and house_is_online = 1 
    and country_name in ('日本','泰国')
    and house_is_oversea = 1 
    and landlord_channel = 1 
) b 
on a.house_id = b.house_id 
left join (
    select a.house_id
        ,op_date
    from (
        select connect_id house_id
            ,to_date(op_time) op_date
        from ods_tns_baseinfo_log.product_op_log
        where substr(to_date(op_time),1,7) = substr(last_day(add_months(current_date,-1)),1,7)
        and type = '2'
        and domain = '3'
        and operator != '智能调价系统'
    ) a 
    inner join (
        select house_id
            ,house_city_name
        from dws.dws_house_d 
        where dt = last_day(add_months(current_date,-1))
        and house_is_online = 1 
        and country_name in ('日本','泰国')
        and house_is_oversea = 1 
        and landlord_channel = 1 
    ) b 
    on a.house_id = b.house_id
    group by 1,2
) c 
on a.house_id = c.house_id
and a.activity_create_date = c.op_date
union all 
-- 连住优惠
select a.house_id
    ,house_city_name
    ,'连住优惠' activiy_name
    ,a.activity_create_date `活动最后一次调整时间`
    ,nvl(a1.activity_create_date,last_day(add_months(current_date,-1))) `统计截至时间`
    ,cast(regexp_extract_all(a.operate_content,'报名折扣:(\\d+)',1) as string) `报名折扣`
    ,array_max(regexp_extract_all(a.operate_content,'报名折扣:(\\d+)',1)) `最低折扣`
    ,cast(regexp_extract_all(a.operate_content,'提前预定天数:(\\d+)',1) as string) `提前预定天数`
    ,array_min(regexp_extract_all(a.operate_content,'提前预定天数:(\\d+)',1)) `最低预定天数`
    ,cast(regexp_extract_all(a.operate_content,'连住天数:(\\d+)',1) as string) `提前预定天数`
    ,array_min(regexp_extract_all(a.operate_content,'连住天数:(\\d+)',1)) `最低预定天数`
 
from (
    select * from (
        select *
            ,to_date(create_time) activity_create_date
            ,row_number() over(partition by house_id order by create_time desc) rn 
        from ods_tns_baseinfo.house_log
        where substr(to_date(create_time),1,7) = substr(last_day(add_months(current_date,-1)),1,7)
        and operate_platform = '营销系统'
        and operate_type in ('商务端异步报名','修改报名信息')
        -- and operate_type = '放弃报名'
        and operate_content like '%连住优惠%'
    ) tmp 
    where rn = 1 
) a 
left join (
    select * from (
        select *
            ,to_date(create_time) activity_create_date
            ,row_number() over(partition by house_id order by create_time desc) rn 
        from ods_tns_baseinfo.house_log
        where substr(to_date(create_time),1,7) = substr(last_day(add_months(current_date,-1)),1,7)
        and operate_platform = '营销系统'
        -- and operate_type in ('商务端异步报名','修改报名信息')
        and operate_type = '放弃报名'
        and operate_content like '%连住优惠%'
    ) tmp 
    where rn = 1 
) a1 
on a.house_id = a1.house_id 
and a.activity_create_date <= a1.activity_create_date
inner join (
    select house_id
        ,house_city_name
    from dws.dws_house_d 
    where dt = last_day(add_months(current_date,-1))
    and house_is_online = 1 
    and country_name in ('日本','泰国')
    and house_is_oversea = 1 
    and landlord_channel = 1 
) b 
on a.house_id = b.house_id 
left join (
    select a.house_id
        ,op_date
    from (
        select connect_id house_id
            ,to_date(op_time) op_date
        from ods_tns_baseinfo_log.product_op_log
        where substr(to_date(op_time),1,7) = substr(last_day(add_months(current_date,-1)),1,7)
        and type = '2'
        and domain = '3'
        and operator != '智能调价系统'
    ) a 
    inner join (
        select house_id
            ,house_city_name
        from dws.dws_house_d 
        where dt = last_day(add_months(current_date,-1))
        and house_is_online = 1 
        and country_name in ('日本','泰国')
        and house_is_oversea = 1 
        and landlord_channel = 1 
    ) b 
    on a.house_id = b.house_id
    group by 1,2
) c 
on a.house_id = c.house_id
and a.activity_create_date = c.op_date