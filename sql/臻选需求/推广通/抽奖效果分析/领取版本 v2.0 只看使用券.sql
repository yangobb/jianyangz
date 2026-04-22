select 
    a.*
    ,lpv
    ,dpv
    
    ,`总间夜`
    ,`总gmv`
    
    ,`曝光量` / lpv `推广通pv`
    ,tag
from (
    select a.unit_id house_id
        ,hotel_id
        ,sum(`曝光量`) `曝光量`
        ,sum(`点击量`) `点击量`
        ,sum(`订单量`) `订单量`
        ,sum(`gmv_value`) gmv_value
        ,sum(`间夜`) `间夜`
        ,sum(`花费`) `花费`
    from (
        select unit_id 
                ,plan_id 
        from (
            select coupon_code
                ,unit_id 
                ,plan_id 
            from ods_tns_salespromotion.coupon_plan_record 
            where status != 2 -- 0 已使用 1 已核销 2 使用失败
        ) a 
        inner join (
            select coupon_code
                ,strategy_id
            from ods_tujiaonlinepromo.merchant_coupon
            where strategy_id in ('3747810','3747825','3747828','3747861','3747873')
            group by 1,2
        ) b
        on a.coupon_code = b.coupon_code
        group by 1,2
    ) a 
    join (
        select hotel_id
            ,unit_id
            ,plan_id
            ,sum(exposure_count) as `曝光量`
            ,sum(click_count) as `点击量`
            ,sum(order_count) as `订单量`
            ,sum(gmv_value) as `gmv_value`
            ,sum(nights) as `间夜` 
            ,(sum(physical_cost)/100)+(sum(if(real_cost = 0,pre_cost,virtual_cost))/100) as `花费`
        from dwd.dwd_flow_cpc_poi_cost_d
        where dt = date_sub(current_date,1)
        and concat(substr(date_date,1,4),'-',substr(date_date,5,2),'-',substr(date_date,7,2)) >= '2025-03-12'
        group by 1,2,3
    ) c 
    on a.unit_id = c.unit_id
    and a.plan_id = c.plan_id
    group by 1,2
) a  
left join (
	select house_id
	    ,count(uid) lpv
	    ,count(detail_uid) dpv
	from dws.dws_path_ldbo_d
	where dt >= '2025-03-12'
	and wrapper_name in  ('携程','去哪儿','途家')
	group by 1
) list  
on a.house_id = list.house_id 
left join (
    select house_id
        ,sum(order_room_night_count) as `总间夜`
        ,sum(room_total_amount) as `总gmv`
    from dws.dws_order
    where create_date >= '2025-03-12'
    and terminal_type_name in ('携程-APP','本站-APP','去哪儿-APP','携程-小程序','本站-小程序','去哪儿-小程序')
    and is_paysuccess_order = '1'
    and is_cancel_order = 0 --非取消
    --and is_done = 1
    and is_overseas = 0 --国内
    group by 1			
) ord_k 
on a.house_id = ord_k.house_id 
left join (
    select a.house_id
        ,b.tag
    from (
        select house_id 
            ,city_name 
        from dwd.dwd_house_d 
        where dt = date_sub(current_date,1)
        group by 1,2
    ) a 
    right join (
        select city_name
            ,tag
        from excel_upload.city_list_2024 
        group by 1,2
    ) b
    on a.city_name = b.city_name
) a1
on a.house_id = a1.house_id 