with rec as (
-- query流量曝光
select query
    ,dt
    ,user_id
    ,uid
    ,channel
    ,search_time
    ,unix_timestamp(search_time) search_time_unix 
from ads.ads_flow_tujia_redbook_shuangliu_recommend_d 
where dt >= '2026-03-01'
and uid != 'visitor000000' 
)
,od as (
-- 订单表s、所有关联订单
select *
    ,unix_timestamp(create_time) create_time_unix
from dws.dws_order 
where create_date >= '2026-03-01'
and create_date < current_date
and is_paysuccess_order = 1 
and is_success_order = 1 
) 
select order_no,
    order_time, 
    order_status,
    real_pay_amount, 
    order_room_night_count,
    checkout_date,
    is_paysuccess_order,
    is_done,
    uid,
    user_id,
    qunar_user_name,
    is_new,
    first_order_time,
    query,
    last_search_time,
    dt
from (
    select order_no,
        order_time, 
        order_status,
        real_pay_amount, 
        order_room_night_count,
        checkout_date,
        is_paysuccess_order,
        is_done,
        uid,
        user_id,
        qunar_user_name,
        is_new,
        first_order_time,
        query,
        search_time as last_search_time,
        dt
    from (
        select *
            ,row_number() over(partition by order_no order by search_time_unix desc) rn 
        from (
            select query
                ,create_date dt 
                ,create_time order_time
                ,nvl(rec.user_id,od.user_id) user_id
                ,nvl(rec.uid,od.uid) uid
                ,rec.dt flow_dt 
                ,channel
                ,order_no
                ,create_time
                ,search_time_unix
                ,order_status
                ,real_pay_amount
                ,order_room_night_count
                ,checkout_date
                ,is_paysuccess_order
                ,is_done
                ,user_name qunar_user_name
                ,channel is_new
                ,'' first_order_time
                ,search_time
            from rec 
            left join  od
            on rec.uid = od.uid
            and rec.search_time_unix < od.create_time_unix 
            union all 
            select query
                ,create_date dt 
                ,create_time order_time
                ,nvl(rec.user_id,od.user_id) user_id
                ,nvl(rec.uid,od.uid) user_id
                ,rec.dt flow_dt 
                ,channel
                ,order_no
                ,create_time
                ,search_time_unix
                ,order_status
                ,real_pay_amount
                ,order_room_night_count
                ,checkout_date
                ,is_paysuccess_order
                ,is_done
                ,user_name
                ,channel is_new
                ,'' first_order_time
                ,search_time
            from rec
            left join od
            on rec.user_id = od.user_id
            and rec.search_time_unix < od.create_time_unix 
        ) tmp
    ) a 
    where rn = 1 
    union all 
    
    select order_no,
        order_time, 
        order_status,
        real_pay_amount, 
        order_room_night_count,
        checkout_date,
        is_paysuccess_order,
        is_done,
        uid,
        user_id,
        qunar_user_name,
        is_new,
        first_order_time,
        query,
        search_time as last_search_time,
        nvl(dt,to_date(search_time)) dt
    from (
        select * 
        from (
            select query
                ,create_date dt 
                ,create_time order_time
                ,nvl(rec.user_id,od.user_id) user_id
                ,nvl(rec.uid,od.uid) uid
                ,rec.dt flow_dt 
                ,channel
                ,order_no
                ,create_time
                ,search_time_unix
                ,order_status
                ,real_pay_amount
                ,order_room_night_count
                ,checkout_date
                ,is_paysuccess_order
                ,is_done
                ,user_name qunar_user_name
                ,channel is_new
                ,'' first_order_time
                ,search_time
            from rec 
            left join  od
            on rec.uid = od.uid
            and rec.search_time_unix < od.create_time_unix 
            union all 
            select query
                ,create_date dt 
                ,create_time order_time
                ,nvl(rec.user_id,od.user_id) user_id
                ,nvl(rec.uid,od.uid) user_id
                ,rec.dt flow_dt 
                ,channel
                ,order_no
                ,create_time
                ,search_time_unix
                ,order_status
                ,real_pay_amount
                ,order_room_night_count
                ,checkout_date
                ,is_paysuccess_order
                ,is_done
                ,user_name
                ,channel is_new
                ,'' first_order_time
                ,search_time
            from rec
            left join od
            on rec.user_id = od.user_id
            and rec.search_time_unix < od.create_time_unix 
        ) tmp
    ) a 
    where order_no is null 
) a 