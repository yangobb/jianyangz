select date_sub(current_date,1) dt 
    ,count(distinct house_id) `监控房屋数`
    ,count(distinct case when `总计售卖天数` between 160 and 179 then house_id end) `售卖天数160至180天`
    ,count(distinct case when `总计售卖天数` >= 180 then house_id end) `售卖天数大于等于180天`
    ,'' `仅CQ售卖房屋数`
    ,count(distinct case when `总计售卖天数` >= 180 and `房屋是否在线` = 1 then house_id end) `需下线房屋数`
    ,count(distinct case when `总计售卖天数` >= 180 and `房屋是否在线` = 0 then house_id end) `已下线房屋数`
    ,count(distinct case when `总计售卖天数` >= 180 and `房屋是否在线` = 1 then house_id end) `高危房屋`
from (
select a.house_city_name `城市`
    ,houseQualificationNumber `系统编码`
    ,notification_number `通知书编号`
    ,a.hotel_id 
    ,a.hotel_name 
    ,a.house_id
    ,a.house_is_online `房屋是否在线`
    ,'' `是否仅CQ售卖`
    ,days `日本观光厅统计已售天数`
    ,unavaliablecount `途家远期已售天数`
    ,nvl(days,0) + nvl(unavaliablecount,0) `总计售卖天数`
from (
    select *
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_oversea = 1 
    and country_name = '日本'
) a 

join (
    select house_guid
        ,get_json_object(property_data, '$.houseQualificationNumber') AS houseQualificationNumber
    from ods_crm.house_credential_info
    where get_json_object(property_data, '$.houseQualificationNumber') is not null 
    and get_json_object(property_data, '$.houseQualificationType') = '21'
    -- and house_guid = '21745a83-2f5e-4bd1-b34b-9a4e96288a08'
) b 
on a.house_guid = b.house_guid

left join excel_upload.oversea_results_japan c 
on b.houseQualificationNumber = c.notification_number

left join (
    select house_id  
        ,sum(unavaliablecount) unavaliablecount
    from (
        select * from dim_tujiaproduct.unit_inventory_log
        where createdate = current_date
        and substr(gettime,9,2) = '00'
        and inventorydate between current_date and '2026-03-31'
    ) a
    inner join (
        select house_id
        from dws.dws_house_d 
        where dt = date_sub(current_date,1)
        and house_is_oversea = 1 
        and country_name = '日本'
    ) b 
    on a.unitid = b.house_id
    group by 1 
) d 
on a.house_id = d.house_id 
) A 
group by 1