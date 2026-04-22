
select 
    substr(dt,1,7) month 
    ,country_name
    ,province_name
    ,city_name
    ,count(distinct concat(dt,orig_device_id)) luv 
    ,count(distinct case when home_pv > 0 then concat(dt,orig_device_id) end) huv
    ,count(distinct case when detail_pv > 0 then concat(dt,orig_device_id) end) duv
    ,count(distinct case when booking_pv > 0 then concat(dt,orig_device_id) end) buv
    ,count(distinct case when order_pv > 0 then concat(dt,orig_device_id) end) ouv
from ihotel_default.mdw_user_app_log_sdbo_di_v1
where (dt between '2025-07-01' and '2025-08-31' or dt between '2024-07-01' and '2024-08-31')
and user_id is not null 
and user_id <> ''
group by 1,2,3,4
