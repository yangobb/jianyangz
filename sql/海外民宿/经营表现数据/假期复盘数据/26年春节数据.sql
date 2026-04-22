-- 日期
-- 2024-12-06 select date_sub('2025-02-04',60)
-- 2025-12-25 select date_sub('2026-02-23',60)
-- 2025-01-05 select date_sub('2025-02-04',30)
-- 2026-01-24 select date_sub('2026-02-23',30)
-- 初七
-- '2026-02-23' 
-- '2025-02-04'

select count(1)
from dws.dws_path_ldbo_d
where dt between '2025-12-25' and '2026-02-23' 
and is_oversea = 1 
and source =  102 
and user_type = '用户'
union all
select count(1)
from dws.dws_path_ldbo_d
where dt between '2024-12-06' and '2025-02-04'
and is_oversea = 1
and source =  102 
and user_type = '用户'


-- -- 流量表现
select 
    '今年' year1 
    ,nvl(b.city_name,'其他') city_name
    ,case when first_create_date_outseas = dt then '新客'
        when first_create_date_outseas < dt then '老客'
        when first_create_date_outseas is null then '未下单客户' 
        else '其他'
        end cus_type 
    ,count(dt,uid) lpv 
    ,count(distinct dt,uid) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_order_gmv) gmv
from (
    select *
    from dws.dws_path_ldbo_d
    where dt between '2026-01-24' and '2026-02-23' 
    and checkout_date between '2026-02-15' and '2026-02-23' 
    and is_oversea = 1 
    and source =  102 
    and user_type = '用户'
) a 
left join (
    select *
    from excel_upload.oversea_city_level
    where city_level != 'C'
) b 
on a.city_name = b.city_name
left join (
    select user_id  
        ,first_create_date_outseas
    from pdb_analysis_c.ads_user_ltv_detail_d
    where dt = date_sub(current_date,1)
    and first_create_date_outseas >= '2025-01-01'
) c 
on a.user_id = c.user_id
group by 1,2,3
union all
select 
    '去年' year1 
    ,nvl(b.city_name,'其他') city_name
    ,case when first_create_date_outseas = dt then '新客'
        when first_create_date_outseas < dt then '老客'
        when first_create_date_outseas is null then '未下单客户' 
        else '其他'
        end cus_type 
    ,count(dt,uid) lpv 
    ,count(distinct dt,uid) luv 
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_order_gmv) gmv
from (
    select *
    from dws.dws_path_ldbo_d
    where dt between '2025-01-05' and '2025-02-04'
    and checkout_date between '2025-01-27' and '2025-02-04' 
    and is_oversea = 1
    and source =  102 
    and user_type = '用户'
) a 
left join (
    select *
    from excel_upload.oversea_city_level
    where city_level != 'C'
) b 
on a.city_name = b.city_name
left join (
    select user_id  
        ,first_create_date_outseas
    from pdb_analysis_c.ads_user_ltv_detail_d
    where dt = date_sub(current_date,1)
    and first_create_date_outseas >= '2025-01-01'
) c 
on a.user_id = c.user_id
group by 1,2,3




-- 订单表现
select 
    '今年' year1  
    ,city_level
    ,nvl(b.city_name,'其他') city_name
    ,count(distinct order_no) order_num
    ,sum(room_total_amount) gmv 
    ,sum(order_room_night_count) night 
    
    ,count(distinct case when is_cancel_order = 0 and is_done = 1 then order_no end) order_num_done
    ,sum(case when is_cancel_order = 0 and is_done = 1 then room_total_amount end) gmv_done 
    ,sum(case when is_cancel_order = 0 and is_done = 1 then order_room_night_count end) night_done 
from (
    select *
    from dws.dws_order 
    where checkout_date between '2026-02-15' and '2026-02-23'
    -- and create_date between '2026-01-24' and '2026-02-23' 
    and is_paysuccess_order = 1 
    -- and is_cancel_order = 0 
    -- and is_done = 1 
    and is_overseas = 1 
) a 
left join (
    select *
    from excel_upload.oversea_city_level
    where city_level != 'C'
) b 
on a.city_name = b.city_name
group by 1,2,3
union all
select 
    '去年' year1    
    ,city_level
    ,nvl(b.city_name,'其他') city_name
    ,count(distinct order_no) order_num
    ,sum(room_total_amount) gmv 
    ,sum(order_room_night_count) night 
    
    ,count(distinct case when is_cancel_order = 0 and is_done = 1 then order_no end) order_num_done
    ,sum(case when is_cancel_order = 0 and is_done = 1 then room_total_amount end) gmv_done 
    ,sum(case when is_cancel_order = 0 and is_done = 1 then order_room_night_count end) night_done 
from (
    select *
    from dws.dws_order 
    where checkout_date between '2025-01-27' and '2025-02-04' 
    -- and create_date between '2025-01-05' and '2025-02-04'
    and is_paysuccess_order = 1 
    -- and is_cancel_order = 0 
    -- and is_done = 1 
    and is_overseas = 1 
) a 
left join (
    select *
    from excel_upload.oversea_city_level
    where city_level != 'C'
) b 
on a.city_name = b.city_name
group by 1,2,3



-- C平台订单
select 
    '今年' time1 
    ,case when t11.ord_id is not null then 1 else 0 end is_7jd
    ,city_level
    ,nvl(t3.city_name,'其他') city_name
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
    and to_date(departure) between '2026-02-15' and '2026-02-23'
    and orderstatus in ('S','P')
    and ordertype = 2 -- 酒店订单 
) t1 
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and checkout_date between '2026-02-15' and '2026-02-23'
    and ord_status in ('P','S')
    and is_tcom = 0
) t11
on t1.orderid = t11.ord_id
JOIN excel_upload.dim_ctrip_list_qid_city t2
ON t1.cityid = t2.m_city 
left join (
    select *
    from excel_upload.oversea_city_level
    where city_level != 'C'
) t3
on t2.cityname = t3.city_name
group by 1,2,3,4
union all 
select 
    '去年' time1 
    ,case when t11.ord_id is not null then 1 else 0 end is_7jd
    ,city_level
    ,nvl(t3.city_name,'其他') city_name 
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
    and to_date(departure) between '2025-01-27' and '2025-02-04' 
    and orderstatus in ('S','P')
    and ordertype = 2 -- 酒店订单 
) t1 
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and checkout_date between '2025-01-27' and '2025-02-04' 
    and ord_status in ('P','S')
    and is_tcom = 0
) t11
on t1.orderid = t11.ord_id
JOIN excel_upload.dim_ctrip_list_qid_city t2
ON t1.cityid = t2.m_city 
left join (
    select *
    from excel_upload.oversea_city_level
    where city_level != 'C'
) t3
on t2.cityname = t3.city_name
group by 1,2,3,4
