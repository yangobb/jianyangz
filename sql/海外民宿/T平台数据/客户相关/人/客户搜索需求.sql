




select city_name
    ,dynamic_business
    ,filter_bedroom_count
    ,guest
    ,ceil(price_max/100)*100 price_max
    ,count(distinct uid) u_cnt 
    ,sum(pv) pv
    ,sum(order_num) order_num
    ,sum(order_gmv) order_gmv
    ,sum(order_night) order_night

from (
    select dt
        ,city_name
        ,dynamic_business
        ,uid
        ,filter_bedroom_count
        ,case when cast(replace(nvl(guest,0),'人','') as int) <= 9 then cast(replace(nvl(guest,0),'人','') as int) else 9 end guest
        -- ,house_id
        -- ,room_bed_type
        ,count(1) pv 
        ,sum(without_risk_order_num) order_num
        ,max(case when get_json_object(json_info,'$.type') = '7'  then split(get_json_object(json_info,'$.value'),',')[1] end) price_max
        ,sum(without_risk_order_gmv) order_gmv 
        ,sum(without_risk_order_room_night) order_night 
    from (
        select distinct 
            trace_id
            ,user_id
            ,uid 
            ,dt
            ,fromforlog 
            ,city_name
            ,house_id
            ,dynamic_business
            ,filter_bedroom_count
            ,room_bed_type
            ,guest
            ,conditions
            ,final_price
            ,detail_uid
            ,without_risk_order_num
            ,without_risk_order_gmv
            ,without_risk_order_room_night
            ,from_unixtime(UNIX_TIMESTAMP(substr(act_time,0,14),'yyyyMMddHHmmss')) as act_time
        from dws.dws_path_ldbo_d
        where dt between date_sub(current_date,14) and date_sub(current_date,1)
        and is_oversea = 1 
        and city_name in ('东京','大阪','京都')
        and conditions is not null
        and nvl(user_id,0) != 0
    ) t1 
    lateral view explode(udf.json_split_new(conditions)) r as json_info  
    group by 1,2,3,4,5,6
) a 
group by 1,2,3,4,5 





