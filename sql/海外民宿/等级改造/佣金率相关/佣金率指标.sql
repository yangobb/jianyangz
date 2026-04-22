-- 默认佣金率
select h.house_id
    ,h.hotel_id
    ,h.landlord_id
    ,landlord_channel
    ,country_name
    ,case when ps.commission_rate = 0 then l.commission_rate else ps.commission_rate end commission_rate
from (
    select house_id
        ,hotel_id
        ,landlord_id
        ,house_class 
        ,house_type
        ,case when landlord_channel = 1 then '直采' else 'C接' end landlord_channel
        ,case when country_name in ('日本','泰国','韩国','中国大陆') then country_name else '其他' end country_name
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    and house_is_online = 1
    and house_is_oversea = 1 
) h 
left join (
    select *
    from (
        select unit_id
            ,commission_rate 
            ,row_number() over(partition by unit_id order by update_time desc) rn 
        from ods_tns_product.product_shard
        where deleted = 0 
    ) a 
    where rn = 1 
) ps
on h.house_id = ps.unit_id
left join (
    select 
        landlord_id 
        ,commition_rate commission_rate
    from ods_tns_baseinfo.landlord
) l 
on h.landlord_id = l.landlord_id
-- 报价组提供
select house_id
    ,checkin_date
    ,checkout_date
    ,final_price
    ,commission_rate_t
    ,commission_price commission_price_t
    ,commission_rate_c
    ,commission_price_c
from dws.dws_house_daily_price_member_commission_rate_d
where dt = date_sub(current_date,1)
-- 曝光房屋展示佣金
select get_json_object(extend,'$.finalScoreTrace.earningRate') as `佣金率`
    ,* 
from pdb_analysis_c.ads_flow_list_price_day_d 
where dt = date_sub(current_date,1)