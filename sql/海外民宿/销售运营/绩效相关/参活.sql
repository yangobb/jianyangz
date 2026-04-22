-- P1 - 房屋参活统计
-- 1.区分新老房东同上
-- 2.计算房屋维度的参活，to_date(create_time) activity_create_date，参活生效日期，max(activity_name) activity_name,min(discount) discount
-- 3.仅计算仍在线上的参活，且折扣力度大于9折的为有效参活
-- 4.一个房屋如果存在多个参与的活动，仅以最大力度的活动为有效活动，避免重复计算；
 

select
    `房屋上房时间`
    ,country_name `国家`
    ,city_name `城市`
    ,`房东类型`
    ,h.hotel_id
    ,hotel_name
    ,`门店首次上线时间`
    ,h.house_id
    ,house_name
    ,`房屋首次上线时间`
    ,`有效库存`
    ,case when `参与活动数量` > 0 then 1 else 0 end `是否参活` 
    ,nvl(`参与活动数量`,0) `本月生效参与活动数量` 
    ,nvl(`是否阶梯定价`,0) `本月生效是否阶梯定价`  
    ,nvl(`折扣力度`,0) `本月生效最低折扣` 
    ,case when b.house_id is null then 0 else 1 end `是否本月创建参活` 
    ,nvl(`本月创建活动数`,0) `本月创建活动数` 
    ,nvl(`本月创建最低折扣`,0) `本月创建最低折扣` 
from (
    select
        country_name
        ,house_city_name city_name
        ,case when hotel_first_active_time >= date_sub(to_date(date_trunc('MM', date_sub(current_date, 1))),59) and hotel_id not in (30263128,30281782,30447850) then '新房东' else '老房东' end `房东类型`
        ,case when house_first_active_time between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1) then '本月上房' else '之前上房' end `房屋上房时间`
        ,hotel_id
        ,hotel_name
        ,house_id
        ,house_name 
        ,to_date(hotel_first_active_time) `门店首次上线时间`
        ,to_date(house_first_active_time) `房屋首次上线时间`
        ,avaliable_count `有效库存`
        ,bedroom_picture_count
        ,bathroom_picture_count
        ,(picture_count - bedroom_picture_count - bathroom_picture_count) other_picture_count
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1 
    -- and house_first_active_time between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1)
) h 
left join (
    select house_id
        ,case when substr(max(create_time),1,7) = substr(date_sub(current_date,1),1,7) then 1 else 0 end is_create_thismonth 
        ,count(distinct a.activity_id) `参与活动数量`
        ,min(merchantRate) `折扣力度`
        ,case when max(roomnight) != '-1' then 1 else 0 end `是否阶梯定价` 
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
    group by 1 
) a 
on h.house_id = a.house_id  
left join (
    select 
        b.house_id
        ,count(distinct a.activity_name) `本月创建活动数`
        ,min(discount) `本月创建最低折扣` 
    from (
        SELECT
            house_id,
            create_time,
            operate_content,
            regexp_extract(operate_content, '商务端异步报名: ([^($]+)', 1) AS activity_name,
            discount
        FROM ods_tns_baseinfo.house_log
        LATERAL VIEW explode(regexp_extract_all(operate_content, '报名折扣:(\\d+)', 1)) t AS discount
        where substr(to_date(create_time),1,7) = substr(date_sub(current_date,1),1,7)
        and operate_platform = '营销系统'
        and operate_type in ('商务端异步报名','修改报名信息')
    ) a 
    inner join (
        select house_id
            ,house_city_name
        from dws.dws_house_d 
        where dt = date_sub(current_date,1)
        and house_is_online = 1
        and house_is_oversea = 1 
        and landlord_channel = 1 
    ) b 
    on a.house_id = b.house_id
    left join (
        select activity_id
            ,activity_name
        from ads.ads_house_activity_categories_mapping
        group by 1,2 
    ) n
    on a.activity_name = n.activity_name
    inner join (
        SELECT act_unit_id house_id 
            ,activity_id
        FROM dwd.dwd_tns_salespromotion_activity_detail_d d
        lateral view explode(udf.json_split(ladder_level_rule)) r as d
        WHERE audit_status = 2  
        AND d.dt = date_sub(current_date,1)
        and check_out_date >= d.dt
        group by 1,2
    ) c 
    on a.house_id = c.house_id
    and n.activity_id = c.activity_id
    group by 1
) b 
on h.house_id = b.house_id  
order by 
    case when `国家` = '泰国' then 1 
         when `国家` = '日本' then 2
        else 8 
        end
    ,case when `城市` = '曼谷' then 1
          when `城市` = '芭堤雅' then 2
          when `城市` = '普吉岛' then 3
          when `城市` = '清迈' then 4
          when `城市` = '大阪' then 5
          when `城市` = '东京' then 6
          when `城市` = '京都' then 7
        else 8
        end
    ,`房东类型`
