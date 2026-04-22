
select '今年' time1 
    ,case when a.dt between '2025-10-01' and '2025-10-08' then '国庆'
        when a.dt between '2025-09-24' and '2025-09-30' then '节前第一周'
        when a.dt between '2025-09-17' and '2025-09-23' then '节前第二周'
        end time_type 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,case when first_create_date_outseas < a.dt then '老客'
        else '新客'
        end xinlaoke
    ,count(distinct concat(a.uid,a.dt)) luv 
    ,count(uid) lpv 
    ,count(distinct case when detail_uid is not null then concat(a.dt,a.uid) end) duv
    ,count(case when detail_uid is not null then concat(a.dt,a.uid) end) dpv
    ,count(distinct house_id) house_cnt
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_order_gmv) gmv
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between '2025-09-17' and '2025-10-08'
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
left join (
    select dt 
        ,user_id
        ,first_create_date_outseas
    from pdb_analysis_c.ads_user_ltv_detail_d
    where dt between '2025-09-17' and '2025-10-08'
) b 
on a.dt = b.dt 
and a.user_id = b.user_id 
group by 1,2,3,4

union all  

select '去年' time1 
    ,case when a.dt between '2024-10-01' and '2024-10-08' then '国庆'
        when a.dt between '2024-09-24' and '2024-09-30' then '节前第一周'
        when a.dt between '2024-09-17' and '2024-09-23' then '节前第二周'
        end time_type 
    ,case when city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then city_name else '其他' end city_name
    ,case when first_create_date_outseas < a.dt then '老客'
        else '新客'
        end xinlaoke
    ,count(distinct concat(a.uid,a.dt)) luv 
    ,count(uid) lpv 
    ,count(distinct case when detail_uid is not null then concat(a.dt,a.uid) end) duv
    ,count(case when detail_uid is not null then concat(a.dt,a.uid) end) dpv
    ,count(distinct house_id) house_cnt
    ,sum(without_risk_order_num) order_num 
    ,sum(without_risk_order_room_night) night 
    ,sum(without_risk_order_gmv) gmv
from (
    select *
    from dws.dws_path_ldbo_d 
    where dt between '2024-09-17' and '2024-10-08'
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
left join (
    select dt 
        ,user_id
        ,first_create_date_outseas
    from pdb_analysis_c.ads_user_ltv_detail_d
    where dt between '2024-09-17' and '2024-10-08'
) b 
on a.dt = b.dt 
and a.user_id = b.user_id 
group by 1,2,3,4
