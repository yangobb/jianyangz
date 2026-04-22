with h as (
select house_id
    ,case when house_id in (69180030,69180033,75982954,66627193,74306953,74306947,78808963,69253131,69253128,68774295,68774040,68774487,74375851,74375989,68774256,68774358,69252594,69253227,75517669,76886218,76886179,77597980,78115105,80934097,80934073,80934064,80934100,80934076,80934085) then 1 else 0 end is_test 
    ,case when landlord_channel = 1 then '直采' when landlord_channel = 334 then '携程接入' else '其他' end landlord_channel
from dws.dws_house_d 
where dt = date_sub(current_date,1)
and house_is_online = 1 
and house_is_oversea = 1 
)

,cpc as (
select '实验周期' time_type
    ,ceil(sum(pre_cost/100),1) `应收`
    ,sum(real_cost) `实际花费` 
    ,sum(exposure_count) `cpc总曝光量`
    ,sum(gmv_value) `总GMV`
    ,sum(nights) `总间夜量`
    ,sum(house_gmv_value) `房屋归因gmv`
    ,sum(house_nights) `房屋归因间夜`
from (select * from h where is_test = 1) h
join (
    select unit_id
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
    where dt >= '2025-07-17'
    group by 1,2
) b 
on h.house_id = b.unit_id 
join (
    select unit_id  
        ,plan_id
        ,sum(pre_cost) pre_cost
        ,sum(real_cost) real_cost
    from dwd.dwd_flow_cpc_poi_cost_d
    where dt >= '2025-07-17'
    group by 1,2 
) c 
on b.unit_id = c.unit_id
and b.plan_id = c.plan_id
group by 1 
) 

,ldbo as (
select 
    case when dt between '2025-07-17' and date_sub(current_date,1) then '实验周期'
        when dt between date_sub('2025-07-17',datediff(current_date,'2025-07-17')) and '2025-07-16' then '对照周期' end time_type 
    ,sum(lpv) `大盘lpv` 
    ,sum(luv) `大盘luv`
    ,sum(order_cnt) `大盘订单数`
    ,sum(gmv) `大盘GMV`
    ,sum(nights) `大盘间夜`
    
    ,sum(case when landlord_channel = '直采' then lpv end) `直采lpv` 
    ,sum(case when landlord_channel = '直采' then luv end) `直采luv`
    ,sum(case when landlord_channel = '直采' then order_cnt end) `直采订单数`
    ,sum(case when landlord_channel = '直采' then gmv end) `直采gmv`
    ,sum(case when landlord_channel = '直采' then nights end) `直采间夜`
from (
    select dt 
        ,house_id
        ,count(1) lpv 
        ,count(distinct uid,dt) luv
    from dws.dws_path_ldbo_d
    where dt between date_sub('2025-07-17',datediff(current_date,'2025-07-17')) and date_sub(current_date,1)
    and is_oversea = 1 
    and wrapper_name in ('途家','携程','去哪儿') 
    and source = '102' 
    and user_type = '用户'
    and city_name = '大阪'
    group by 1,2 
    
) a 
inner join h 
on a.house_id = h.house_id
left join (
    select create_date
        ,house_id 
        ,count(distinct order_no) order_cnt 
        ,sum(room_total_amount) gmv 
        ,sum(order_room_night_count) nights
    from dws.dws_order
    where is_overseas=1
    and create_date between date_sub('2025-07-17',datediff(current_date,'2025-07-17')) and date_sub(current_date,1)
    and is_paysuccess_order = 1 
    and is_cancel_order = 0
    group by 1,2 
) od 
on a.house_id = od.house_id
and a.dt = od.create_date
group by 1 
)
 

select  ldbo.time_type
    ,`大盘lpv` 
    ,`大盘luv`
    ,`大盘订单数`
    ,`大盘GMV`
    ,`大盘间夜`

    ,`直采lpv` 
    ,`直采luv`
    ,`直采订单数`
    ,`直采gmv`
    ,`直采间夜`
    ,`应收`
    ,`实际花费` 
    ,`cpc总曝光量`
    ,`房屋归因gmv`
    ,`房屋归因间夜`
from ldbo 
left join cpc 
on ldbo.time_type = cpc.time_type