select 'E' channel,
count(distinct T1.house_id) `已上货房屋`
from (
    select --取途家在线房源数
        house_id
    from dws.dws_house_d
    where landlord_channel = 1 --平台商户
    and house_is_oversea = 1 --国内房源
    and dt = date_sub(current_date(),1) --1天前
    and house_is_online = 1 --房屋在线
    and hotel_is_online = 1
) T1
left join (
    select --取支持携程分销的房屋表
        unit_id --房屋id
    from ods_tns_hds.distribution_product_snapshot
    where channel&281474976710656 = 281474976710656 --新艺龙酒店
    and data_entity_status=0
) T2
on T1.house_id = T2.unit_id
left join (
    select
        room_id,
        m_hotel_id,
        m_room_type_id --艺龙m酒店id,艺龙m房型id
    from tujia_ods.ods_hds_norm_tu_elong_room_mapping
    where data_entity_status = 0
    -- and date(update_time)= date_sub(current_date(), 1)
) T3 on T1.house_id = T3.room_id
left join (
    select
        room_id,
        elong_hotel_id,
        elong_room_id,
        elong_product_id --Elong酒店id,Elong房型id,Elong产品id
    from tujia_ods.ods_hds_norm_tu_elong_product_mapping
    where data_entity_status = 0
    -- and date(update_time) = date_sub(current_date(), 1)
) T4 
on T1.house_id = T4.room_id

where T2.unit_id is not null
and T3.m_hotel_id is not null
and T3.m_room_type_id is not null
and T4.elong_hotel_id is not null
and T4.elong_room_id is not null
and T4.elong_product_id is not null
group by 1