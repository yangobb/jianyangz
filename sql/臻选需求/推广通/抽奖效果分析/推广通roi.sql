

select b.unit_id
    ,b.hotel_id
    ,hotel_order_count
    ,exposure_count
    ,gmv_value
    ,nights
    ,hotel_nights
    ,hotel_gmv_value
    ,house_gmv_value
    ,house_nights
    ,pre_cost
    ,real_cost
from (
    select unit_id
        ,plan_id
    from (
        select coupon_code
            ,unit_id 
            ,plan_id 
        from ods_tns_salespromotion.coupon_plan_record 
        where status != 2 
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
    select unit_id
        ,hotel_id 
        ,plan_id
        ,sum(hotel_order_count) hotel_order_count
        ,sum(exposure_count) exposure_count
        ,sum(gmv_value) gmv_value
        ,sum(nights) nights
        ,sum(hotel_nights) hotel_nights
        ,sum(hotel_gmv_value) hotel_gmv_value
        ,sum(house_gmv_value) house_gmv_value
        ,sum(house_nights) house_nights
    from dwd.dwd_flow_cpc_poi_click_d
    where dt >= '2025-03-12'
    group by 1,2,3
) b 
on a.unit_id = b.unit_id
and a.plan_id = b.plan_id
join (
    select unit_id
        ,hotel_id 
        ,plan_id
        ,sum(pre_cost) pre_cost
        ,sum(real_cost) real_cost
    from dwd.dwd_flow_cpc_poi_cost_d
    where dt >= '2025-03-12'
    group by 1,2,3
) c 
on a.unit_id = c.unit_id
and a.plan_id = c.plan_id