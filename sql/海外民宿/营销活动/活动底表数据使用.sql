
select house_id
    ,case when month(max(create_time)) = month(date_sub(current_date,1)) then 1 else 0 end is_create_thismonth 
    ,min(merchantRate) merchantRate
    -- ,concat_ws('|',collect_set(concat('   ',activity_name,'-',merchantRate,'%','-',to_date(create_time),'   '))) `活动信息`
from (
    SELECT act_unit_id house_id
        ,ladder_level_rule
        ,activity_id
        ,create_time
        ,get_json_object(d.json_string,'$.merchantRate') merchantRate
        ,get_json_object(d.json_string,'$.roomNights') roomnight
    FROM dwd.dwd_tns_salespromotion_activity_detail_d d
    lateral view explode(udf.json_split(ladder_level_rule)) r as d
    WHERE audit_status = 2  
    AND d.dt = date_sub(current_date,1)
    and check_out_date >= d.dt
) a 
left join (
    select categorie
        ,activity_id
        ,activity_name
    from ads.ads_house_activity_categories_mapping
    -- where activity_name in ('优享家','连住优惠','早鸟特惠','天天特惠','出行特惠','学生特惠')
    group by 1,2,3 
) b
on a.activity_id = b.activity_id 
group by 1 