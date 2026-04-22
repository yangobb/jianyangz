with final as (
select *
    ,row_number() over(partition by city_name order by luv2o desc) lpv2o_rn 
    ,row_number() over(partition by city_name order by lpv2o desc) luv2o_rn 
    ,row_number() over(partition by city_name order by pv_value desc) pv_value_rn 
    ,row_number() over(partition by city_name order by uv_value desc) uv_value_rn  
    ,percentile(case when luv2o != 0 then luv2o end,0.5) over(partition by city_name) luv2o_5
    ,percentile(case when gmv > 0 then lpv end,0.5) over(partition by city_name) lpv_5
from (
    select city_name
        ,dynamic_business
        ,house_id
        ,lpv
        ,luv
        ,order_num
        ,night
        ,gmv
        ,order_num / lpv lpv2o
        ,order_num / luv luv2o
        ,gmv / lpv pv_value 
        ,gmv / luv uv_value 
    from (
        select 
            city_name
            ,dynamic_business
            ,a.house_id
            ,count(1) lpv 
            ,count(distinct dt,uid) luv 
            ,nvl(sum(without_risk_order_num),0) order_num
            ,nvl(sum(without_risk_order_room_night),0) night
            ,nvl(sum(without_risk_order_gmv),0) gmv
        from (
            select *
            from dws.dws_path_ldbo_d
            where dt between date_sub(current_date,60) and date_sub(current_date,1)
            and is_oversea = 1 
            and wrapper_name in ('途家','携程','去哪儿') 
            and source = '102' 
            and user_type = '用户'
            and city_name in ('东京','大阪','京都')
        ) a 
        group by 1,2,3
    ) a 
) a 
)
 

select a.city_name
    ,a.dynamic_business
    ,a.house_id
    ,b.house_name
    ,b.hotel_id 
    ,b.hotel_name 
    ,a.lpv
    ,a.luv
    ,a.order_num
    ,a.night
    ,a.gmv
    ,a.lpv2o
    ,a.luv2o
    ,a.pv_value
    ,a.uv_value
    ,a.luv2o_5
    ,house_class
    ,comment_score
from (
    select *
    from final 
    where case when gmv > 0 and luv2o >= luv2o_5 then 1
        when gmv = 0 and lpv < lpv_5 then 1 end = 1 
) a 
inner join (
    select house_id
        ,house_name
        ,hotel_id 
        ,hotel_name 
        ,house_class
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_city_name in ('东京','大阪','京都')
    and house_is_online = 1 
    and landlord_channel = 1 
    and house_class not in ('L0','L1')
) b 
on a.house_id = b.house_id 
join (
    select house_id
        ,comment_score
    from dws.dws_comment_d
    where dt = date_sub(current_date,1)
    and comment_score >= 4 
) c 
on a.house_id = c.house_id 
