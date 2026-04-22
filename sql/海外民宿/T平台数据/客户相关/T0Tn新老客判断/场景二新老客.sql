select dt 
    ,'海外' type 
    ,case when dt = first_create_date_outseas then '新客' else '老客' end cus_type 
    ,count(distinct a.user_id) user_cnt 
    ,sum(od_cnt) od_cnt  
    ,sum(nights) nights  
    ,sum(gmv) gmv 
from (
    select 
        create_date dt
        ,user_id 
        ,count(distinct order_no) od_cnt 
        ,sum(order_room_night_count) nights
        ,sum(room_total_amount) gmv
    from dws.dws_order 
    where create_date between date_sub(current_date,14) and date_sub(current_date,1)
    and create_date = date_sub(current_date,1)
    and is_paysuccess_order = 1
    and is_cancel_order = 0 
    and is_overseas = 1 
    group by 1,2
) a 
inner join (
    select user_id
        ,first_create_date_inseas
        ,first_create_date_outseas
    from pdb_analysis_c.ads_user_ltv_detail_d
    where dt = date_sub(current_date,1)
) b 
on a.user_id = b.user_id
group by 1,2,3 

union all 

select dt 
    ,'国内' type 
    ,case when dt = first_create_date_inseas then '新客' else '老客' end cus_type 
    ,count(distinct a.user_id) user_cnt 
    ,sum(od_cnt) od_cnt  
    ,sum(nights) nights  
    ,sum(gmv) gmv 
from (
    select 
        create_date dt
        ,user_id 
        ,count(distinct order_no) od_cnt 
        ,sum(order_room_night_count) nights
        ,sum(room_total_amount) gmv
    from dws.dws_order 
    where create_date between date_sub(current_date,14) and date_sub(current_date,1)
    and is_paysuccess_order = 1
    and is_cancel_order = 0
    and is_overseas = 0
    group by 1,2
) a 
inner join (
    select user_id
        ,first_create_date_inseas
        ,first_create_date_outseas
    from pdb_analysis_c.ads_user_ltv_detail_d
    where dt = date_sub(current_date,1)
) b 
on a.user_id = b.user_id
group by 1,2,3