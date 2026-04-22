
-- P1 - 房屋参活统计
-- 1.区分新老房东同上
-- 2.计算房屋维度的参活，to_date(create_time) activity_create_date，参活生效日期，max(activity_name) activity_name,min(discount) discount
-- 3.仅计算仍在线上的参活，且折扣力度大于9折的为有效参活
-- 4.一个房屋如果存在多个参与的活动，仅以最大力度的活动为有效活动，避免重复计算；

-- 4.P2 - 房屋视频
-- 1.房屋ID，房屋名称， 门店ID， 门店名称，记录直采房屋的视频补充情况，最新一期是否有视频，房屋是否在线；

-- 5.P2 - 房屋图片
-- 1.房屋ID，房屋名称， 门店ID， 门店名称， 房屋图片数量，最新房屋更新时间，各图片类别的房屋数量(卧室、卫生间、其他)

-- 6.P2 - 房东档案
-- 1.门店ID， 门店名称，房东档案是否填写，是否为有效填写，房东档案首次创建时间； 


select
    `房屋上房时间`
    ,country_name `国家`
    ,city_name `城市`
    ,landlord_type `房东类型`
    ,h.hotel_id
    ,hotel_name
    ,hotel_first_active_time `门店首次上线时间`
    ,h.house_id
    ,house_name
    ,house_first_active_time `房屋首次上线时间`
    ,avaliable_count `有效库存`

    ,case when v.house_id is not null then 1 else 0 end `是否有视频`

    ,bedroom_picture_count `卧室图片数`
    ,bathroom_picture_count `卫生间图片数`
    ,other_picture_count `其他图片数`

    ,case when ar.hotel_id is not null then 1 else 0 end `档案是否填写`
    ,nvl(`档案是否有效`,0) `档案是否有效` 
    ,nvl(`档案首次创建时间`,'-') `档案首次创建时间`
from (
    select
        country_name
        ,house_city_name city_name
        ,case when hotel_first_active_time >= date_sub(to_date(date_trunc('MM', date_sub(current_date, 1))),59) and hotel_id not in (30263128,30281782,30447850) then '新房东' else '老房东' end landlord_type
        ,to_date(hotel_first_active_time) hotel_first_active_time
        ,case when house_first_active_time between to_date(date_trunc('MM', date_sub(current_date, 1))) and date_sub(current_date,1) then '本月上房' else '之前上房' end `房屋上房时间`
        ,hotel_id
        ,hotel_name
        ,house_id
        ,house_name 
        ,to_date(house_first_active_time) house_first_active_time
        ,avaliable_count
        ,bedroom_picture_count
        ,bathroom_picture_count
        ,(picture_count - bedroom_picture_count - bathroom_picture_count) other_picture_count
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
    AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1  
) h
left join (
    select house_id
    from dws.dws_house_video_info_d
    where dt = date_sub(current_date,1)
    and source = 1 
    group by 1 
) v 
on h.house_id = v.house_id
left join (
    select hotel_id 
        ,arch_pic_info 
        ,case when get_json_object(arch_pic_info,'$.landlordCardBigUrl') is not null then 1 else 0 end `档案是否有效` 
        ,create_time `档案首次创建时间`
    from tujia_ods.ods_tns_cms_landlord_archival_record 
    where oversea = 0 
) ar 
on h.hotel_id = ar.hotel_id

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
