
select 
from (
    select 
        dt
        ,hotel_seq
        ,orig_device_id as device_id
    from hotel.dwd_flow_app_searchlist_di
    where (
        dt between date_sub('2026-05-01',60) and '2026-05-05' and checkout_date between '2026-05-01' and '2026-05-05'
        or dt between date_sub('2025-05-01',60) and '2025-05-05' and checkout_date between '2025-05-01' and '2025-05-05'
        )
    and user_id is not null and user_id <> ''
    and is_international = 1---非国际城市
) a 
inner join (
    select b.hotel_seq
        ,case when is_standard = 1 then 'Q酒店' when is_standard = 0 then 'Q非标' end is_standard
    from (
        select masterhotelid
            ,is_standard
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,1)
        and is_standard in (0,1)
    ) a 
    join (
        select 
            hotel_seq
            ,partner_hotel_id --为了映射C酒店ID
        from default.dim_hotel_mapping_v3
        where dt = date_sub(current_date,1)
        and partner = 'ctrip' 
        group by 1,2 
    ) b 
    on a.masterhotelid = b.partner_hotel_id
) b 
on a.hotel_seq = b.hotel_seq