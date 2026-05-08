select 
    distinct 
    dt,
    substr(dt,1,4) yr,
    substr(dt,6,2) mon,
    CONCAT(substr(dt,1,4),'-','wk', weekofyear(dt)) wk,
    orig_device_id as device_id
from hotel.dwd_flow_app_searchlist_di
where 
((dt between '2025-11-03' and '2025-11-16') or (dt between '2024-11-04' and '2024-11-17'))
and user_id is not null and user_id <> ''
and is_international=0---非国际城市
and country_type=1---非港澳台城市