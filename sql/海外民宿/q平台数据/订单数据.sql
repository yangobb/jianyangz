select  
    case when checkout_date between '2026-05-01' and '2026-05-05' then '2026五一'
         when checkout_date between '2025-05-01' and '2025-05-05' then '2025五一'
         end as period_name
    --,city_name_info
    ,is_standard
    ,count(distinct order_no) order_num
    ,sum(room_night) night
    ,sum(final_gmv) gmv
from (
    select *
        ,case when city_name in ('吉隆坡','东京','大阪','曼谷','京都','普吉岛','清迈','巴厘岛','巴黎','悉尼',
            '胡志明市','伦敦','福冈','芭堤雅','新山','札幌','济州市','河内','墨尔本','哥打京那巴鲁',
            '首尔','乔治市','罗马','釜山','香港','迪拜','那霸','新加坡','八打灵再也','雅加达',
            '莫斯科','巴塞罗那') then city_name else '其他' end city_name_info
    from hotel.dwd_ord_wide_order_detail_di
    where (
        checkout_date between '2026-05-01' and '2026-05-05'
        or checkout_date between '2025-05-01' and '2025-05-05'
        )
    and is_mainland_china = 0 
    and is_valid = '1'
    and order_status not in ('CANCELLED','REJECTED') --剔除取消和拒单、
    and (distributor_package ='非打包' or distributor_package is null) --非打包
    and buyout_type not in('免费房','广告免房','试睡免房') --非免房 
) a  
left join (
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
group by 1,2