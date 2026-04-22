SELECT b.landlord_name,
    b.house_id,
    b.hotel_name,
    b.house_city_name,
    b.house_first_active_time,
    b.hotel_first_active_time,
    
    a.houseQualificationNumber,
    a.houseQualificationType,
    a.extracted_numbers,
    
    c.info,
    
    concat('https://m.tujia.com/detail/',house_id,'.htm') url,
    operation_email
    
FROM (
    select *
    from dws.dws_house_d
    WHERE landlord_channel_name = '平台商户'
    AND country_name = '日本'
    AND dt = date_sub(current_date,1)
) b
LEFT JOIN (
    select house_guid,
        get_json_object(property_data,'$.houseQualificationNumber') AS houseQualificationNumber,
        get_json_object(property_data,'$.houseQualificationType') AS houseQualificationType,
        REGEXP_EXTRACT(get_json_object(property_data, '$.houseQualificationNumber'), '\\d+', 0) extracted_numbers
    from ods_crm.house_credential_info 
)a 
ON b.house_guid = a.house_guid
left join (
    select REGEXP_EXTRACT(notification_number, '\\d+', 0) AS extracted_numbers
       ,CONCAT('M',REGEXP_EXTRACT(notification_number, '\\d+', 0), ',', REGEXP_REPLACE(date_sub(current_date, 1), '-', '/')) info 
    from excel_upload.oversea_minbo_japan  
) c
ON a.extracted_numbers = c.extracted_numbers
left join (
    select hotel_id 
        ,contact_phone
        ,operation_email
    from ods_tns_baseinfo.hotel 
    where enum_data_entity_status = 0 
) d 
on b.hotel_id = d.hotel_id