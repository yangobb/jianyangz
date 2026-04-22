with tiaojia as (
select a.country_name
    ,a.house_city_name
    ,a.hotel_id
    ,a.house_id
    ,count(distinct a.op_dt) `调价次数`
    ,sum(case when b1.price <= b.price then '1' else 0 end) `降价次数` 
from (
    select country_name
        ,house_city_name
        ,house_id
        ,b.hotel_id
        ,op_dt
    from (
        select *
            ,to_date(op_time) op_dt 
        from ods_tns_baseinfo_log.product_op_log
        where domain = 3 
        and type = 2
        and substr(to_date(op_time),1,7) = '2025-06'
        and substr(to_date(biz_date_begin),1,7) = '2025-06'
        and substr(to_date(biz_date_end),1,7) = '2025-06'
    ) a 
    inner join (
        select country_name
            ,house_city_name 
            ,house_id
            ,hotel_id 
        from dws.dws_house_d 
        where dt = date_sub(current_date,1)
        and country_name = '日本'
        and house_is_online = 1 
        and landlord_channel = 1 
    ) b 
    on a.connect_id = b.house_id 
    group by 1,2,3,4,5 
) a
left join (
    select dt
        ,house_id 
        ,avg(price) price
    from dwd.dwd_house_daily_price_d 
    where substr(dt,1,7) = '2025-06'
    and substr(checkin_date,1,7) = '2025-06'
    and substr(checkout_date,1,7) = '2025-06'
    group by 1,2 
) b1 
on a.house_id = b1.house_id
and a.op_dt = b1.dt 
left join (
    select house_id 
        ,avg(price) price
    from dwd.dwd_house_daily_price_d 
    where substr(dt,1,7) = '2025-06'
    and substr(checkin_date,1,7) = '2025-06'
    and substr(checkout_date,1,7) = '2025-06'
    group by 1
) b 
on a.house_id = b.house_id
group by 1 ,2,3,4 
) 


select  a.country_name
    ,a.house_city_name
    ,a.hotel_id
    ,a.house_id
    ,`调价次数`
    ,`降价次数` 
    ,`修改活动次数`
    ,`参与调价活动`
    ,c.gmv `5月GMV`
    ,c.night `5月间夜`
    ,c.order_cnt `5月订单数`
    ,dt_5
    ,c1.gmv `4月gmv`
    ,c1.night `4月间夜`
    ,c1.order_cnt `4月订单数`
    ,dt_4
    ,(c.gmv / c1.gmv) - 1 `GMV环比`
    
from tiaojia a 
left join (
    select house_id
        ,sum(room_total_amount) gmv 
        ,sum(order_room_night_count) night 
        ,count(distinct order_no) order_cnt 
        ,count(distinct checkin_date) dt_5
    from dws.dws_order a 
    where substr(checkin_date,1,7) = '2025-06'
    and is_paysuccess_order = 1
    and is_done = 1
    and country_name = '日本'
    group by 1 
) c 
on a.house_id = c.house_id 
left join (
    select house_id
        ,sum(room_total_amount) gmv 
        ,sum(order_room_night_count) night 
        ,count(distinct order_no) order_cnt 
        ,count(distinct checkin_date) dt_4
    from dws.dws_order a 
    where substr(checkin_date,1,7) = '2025-05'
    and is_paysuccess_order = 1
    and is_done = 1
    and country_name = '日本'
    group by 1 
) c1
on a.house_id = c1.house_id
left join (
    select a.house_id
        ,count(distinct dt) `修改活动次数`
        ,count(distinct case when is_down = 1 then dt end) `参与调价活动`
    from (
        select house_id
            ,to_date(create_time) dt 
            ,case when operate_type in ('恢复参加','商务端异步报名','数据迁移，新活动报名','修改报名信息','同意升级') then 1 else 0 end is_down
        from ods_tns_baseinfo.house_log
        where substr(to_date(create_time),1,7) = '2025-06'
        and operate_platform = '营销系统'
        and operate_type in ('恢复参加','商务端异步报名','数据迁移，新活动报名','修改报名信息','同意升级')
        group by 1,2,3
    ) a 
    inner join (
        select house_id
            ,house_city_name
        from dws.dws_house_d 
        where dt = date_sub(current_date,1)
        and country_name = '日本'
        and house_is_online = 1 
    ) b 
    on a.house_id = b.house_id
    group by 1 
) d
on a.house_id = d.house_id