--分等级新版房屋信息分平均分

with bs as(--房屋名称和特色
select  distinct
a.house_id,
a.landlord_channel,
a.house_is_online,
--房屋描述
case when length(a.house_name) <11  and a.house_name LIKE '%近医院温馨私享%' then 0.00 else 2.00 end as `房屋名称`,

--20231130结束
-- case when b.house_property LIKE '%{"houseFeatures":[{"featureTitle"%' then 8.00  --新特色
--     when get_json_object(house_property,'$.houseQualityDesc') is not null  then 5.00 --旧特色
--     when get_json_object(house_property,'$.houseQualityPictures') is not null 
--         and  get_json_object(house_property,'$.houseQualityPictures') !='[]' then 5.00  --旧特色
--     when b.house_property  is  null 
--         and get_json_object(c.checkin_instructions, '$.houseDesc') is not null  then 5.00 --旧特色
--      else 0.00 end as `房屋特色`,

--20231201开始：修复集团数据存储格式bug
case when b.house_property LIKE '%houseFeatures":[{"featureTitle%' and  b.house_property not LIKE '%houseFeatures":[{"featureTitle":""%' then 8.00  --新特色
    when b.house_property LIKE '%houseFeatures":[{"featureTitle%' and  b.house_property not LIKE '%houseFeatures":[{"featureTitle":"","featureDesc":""%' then 5.00  --旧特色 
    when b.house_property LIKE '%houseFeatures":[{"featureTitle%' and  b.house_property not LIKE '%houseFeatures":[{"featureTitle":"","featureDesc":"","featurePictures":[]%' then 5.00  --旧特色
    when get_json_object(house_property,'$.houseQualityDesc') is not null and get_json_object(house_property,'$.houseQualityDesc') !='' then 5.00 --旧特色
    when get_json_object(house_property,'$.houseQualityPictures') is not null and  get_json_object(house_property,'$.houseQualityPictures') !='[]' and get_json_object(house_property,'$.houseQualityPictures') !='' then 5.00  --旧特色
    when get_json_object(c.checkin_instructions, '$.houseDesc') is not null and  get_json_object(c.checkin_instructions, '$.houseDesc') !='' then 5.00 --旧特色
     else 0.00 end as `房屋特色`,


--对客要求
case when a.is_cooking =1 then 1.00 else 0.00 end as `允许做饭`,
case when a.is_bring_pet =1 then 1.00 else 0.00 end as `允许带宠物`,

--图片
--20231130结束
-- case when a.picture_count>=35 then 25
-- when a.picture_count<35 and share_type='整租' and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count)=1 then 0 
-- when  a.picture_count<35 and share_type='整租' and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >1 and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <=3 and a.picture_count>6 then 5
-- when  a.picture_count<35 and share_type='整租' and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >3 and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <=5 then 15
-- when  a.picture_count<35 and share_type='整租' and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >5 and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <=7 then 20
-- when  a.picture_count<35 and share_type='整租' and a.picture_count/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >7 then 25
-- when  a.picture_count<35 and share_type='单间' and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) =1 then 0 
-- when  a.picture_count<35 and share_type='单间' and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) >1 and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) <=3 and a.picture_count>6 then 5
-- when  a.picture_count<35 and share_type='单间' and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) >3 and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) <=5 then 15
-- when  a.picture_count<35 and share_type='单间' and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) >5 and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) <=7 then 20
-- when a.picture_count<35 and share_type='单间' and a.picture_count/(1+1+a.kitchen_count+a.livingroom_count) >7 then 25
-- else 0
-- end as `图片张数`,

--20231201开始：图片张数（核心空间图片数达到35张给满分25分；未达到35张的，按照图片倍数，1-2倍5分；2-3倍10分，3-4倍15分，4-5倍20分，5倍及以上25分）
case when (livingroom_picture_count+bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)>=35 then 25
when (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='整租' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >=1.0 and (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <2.0  then 5
when (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='整租' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >=2.0 and (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <3.0 then 10
when  (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='整租' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >=3.0 and (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <4.0 then 15
when  (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='整租' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >=4.0 and (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) <5.0 then 20
when  (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='整租' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(a.bedroom_count+a.bathroom_count+a.kitchen_count+a.livingroom_count) >=5.0 then 25
when  (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='单间' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) >=1.0 and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) <2.0 and a.picture_count>6 then 5
when (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='单间' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) >=2.0 and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) <3.0 then 10
when  (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='单间' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) >=3.0 and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) <4.0 then 15
when  (livingroom_picture_count+  bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='单间' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) >=4.0 and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) <5.0 then 20
when (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)<35 and share_type='单间' and (livingroom_picture_count+ bedroom_picture_count+kitchen_picture_count+bathroom_picture_count)/(1+1+a.kitchen_count+a.livingroom_count) >=5.0 then 25
else 0
end as `图片张数`,


nvl(round((f.`清晰图片占比` *15),2) ,0.00) as `图片质量`,

--退订规则
case when d.`退订`='灵活' then 10.00
     when d.`退订`='宽松' then 7.00
     when d.`退订`='中等' then 5.00
     else 1.00
end as `退订规则`,

--视频
case when c.base_info LIKE '%auditPassVideo%'  
  or c.base_info LIKE '%houseSonVideo%' then 10.00
     else 0.00
    end as `视频`,

--核心设施
nvl(round((`窗户`+`无线网络`+`可洗热水澡`+`电视`+`空调`)*0.2,2),0.00) as `核心设施`,

--周边
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%餐厅%' or concat_ws('',a.enum_house_facilities_name) LIKE '%餐馆%'then 0.20 else 0.00 end as `周边有餐厅`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%海滩%' then  0.20 else 0.00 end as `周边有海滩`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%超市%' then  0.20 else 0.00 end as `周边有超市`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%公园%' then  0.20 else 0.00 end as `周边有公园`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%菜市场%' then  0.20 else 0.00 end as `周边有菜市场`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%便利店%' then  0.20 else 0.00 end as `周边有便利店`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%提款机%' then  0.20 else 0.00 end as `周边有提款机`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%药店%' then  0.20 else 0.00 end as `周边有药店`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%花园%' then  0.20 else 0.00 end as `周边有花园`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%儿童乐园%' then  0.20 else 0.00 end as `周边有儿童乐园`,

--居家  落地窗和露台
case when  concat_ws('',a.enum_house_facilities_name) regexp  '落地窗|观景露台' then 1.00 else 0.00 end as `居家`,

--休闲
nvl(round((`投影`+`泳池`+`卡拉`+`麻将机`),2),0.00) as `休闲`,

--配套设施
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%免费停车%' then 1.00 else 0.00 end as `免费停车位`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%充电桩%' then 1.00 else 0.00 end as `充电桩`,
case when  concat_ws('',a.enum_house_facilities_name) LIKE '%付费停车%' then 1.00 else 0.00 end as `付费停车位`,

--景观
`景观`,

--儿童设施
case when  concat_ws('',a.enum_house_facilities_name) regexp '儿童玩具|儿童餐具|儿童餐椅' then 1.00 else 0.00 end as `儿童设施`,

--卫浴
nvl(round((`洗衣机`+`洗发水`+`电吹风`),2),0.00)  as `卫浴`,

--餐厨
nvl(round((`冰箱`+`热水壶`+ `电磁炉`+`微波炉`+`燃气灶`+ `锅具`+`电饭煲`+ `餐具`+ `烧烤器具`+`刀具菜板`)*0.5,2),0.00)  as `餐厨`,

--房东服务
nvl(round((`接机` +`行李寄存`+`提供早餐`),2),0.00)  as `房东服务`
from  (
    select 
        house_id,
        case when is_mountain_view=1 or   is_sea_view=1 or is_garden_view=1 
                or is_lake_view=1 or  is_river_view=1 or is_great_river_view=1 or is_city_view=1 then 5.00 else 0.00 end as `景观`,
        landlord_channel,
        house_is_online,
        house_name,
        is_cooking,
        is_bring_pet,
        picture_count,
        bedroom_count,
        bathroom_count,
        kitchen_count,
        livingroom_count,
        share_type,
        enum_house_facilities_name,
        livingroom_picture_count,
        bedroom_picture_count,
        kitchen_picture_count,
        bathroom_picture_count,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%洗衣机%' then 1 else 0 end as `洗衣机`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%浴缸%' then 1 else 0 end as `浴缸`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%洗发水%' then 1 else 0 end as `洗发水`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%电吹风%' then 1 else 0 end as `电吹风`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%晾衣架%' then 1 else 0 end as `晾衣架`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%冰箱%' then 1 else 0 end as `冰箱`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%热水壶%' then 1 else 0 end as `热水壶`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%电磁炉%' then 1 else 0 end as `电磁炉`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%燃气灶%' then 1 else 0 end as `燃气灶`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%微波炉%' then 1 else 0 end as `微波炉`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%锅具%' then 1 else 0 end as `锅具`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%电饭煲%' then 1 else 0 end as `电饭煲`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%餐具%' then 1 else 0 end as `餐具`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%烧烤%' then 1 else 0 end as `烧烤器具`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%刀具%' then 1 else 0 end as `刀具菜板`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%瓶装水%' then 1 else 0 end as `免费瓶装水`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%管家式服务%' then 1 else 0 end as `管家式服务`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%免费接机%' 
          or  concat_ws('',enum_house_facilities_name) LIKE '%付费接机%' then 1 else 0 end as `接机`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%提供早餐%' then 1 else 0 end as `提供早餐`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%行李寄存%' then 1 else 0 end as `行李寄存`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%窗户%' then 1 else 0 end as `窗户`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%热水澡%' then 1 else 0 end as `可洗热水澡`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%电视%' then 1 else 0 end as `电视`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%无线网络%' then 1 else 0 end as `无线网络`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%空调%' then 1 else 0 end as `空调`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%投影%' then 1 else 0 end as `投影`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%泳池%' then 1 else 0 end as `泳池`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%卡拉%' then 1 else 0 end as `卡拉`,
        case when  concat_ws('',enum_house_facilities_name) LIKE '%麻将机%' then 1 else 0 end as `麻将机`
    from dws.dws_house_d  
    where dt =date_sub(current_date(),1)
    and landlord_channel!=334 
    and house_is_oversea=1
    and hotel_is_oversea=1
    )a
left join ods_tns_baseinfo.house_properties b on a.house_id=b.house_id
left join ods_tns_baseinfo.house_info c on c.house_id = a.house_id 
left join 
(
select distinct
    house_id,
    case when cancelable != 'false'and preDay = 0 then '灵活'
        when cancelable != 'false' and preDay = 1 then '宽松'
        when cancelable != 'false' and preDay between 2 and 5 then '中等'
        when cancelable != 'false' and preDay > 5 then '严格'
        when cancelable = 'false' then '不可取消'
    end as `退订`
      from
        (
          select distinct 
            unit_id house_id,
            rate_plan_id,
            udf.get_json_object(cancel_rule, '$.cancelable') cancelable,
            udf.get_json_object(cancel_rule, '$.preDay') preDay,
            udf.get_json_object(cancel_rule, '$.fineAmount') fineAmount
          from
            ods_tns_product.rate_plan_shard
          where
            1 = 1
            and enum_rate_plan_type = 1 --基础价规则
            and  deleted = '0'
            )t1 
        join (
    select * from ods_tns_product.product_shard  --这个表中还存在5条房屋并发数据，导致出现了10条数据，所以结果中必须用distinct，否则匹配两次，会出现两行数据
      where  active=1 --一个 product_shard  对应一条rate_plan_shard ，如果product被删除了，那rateplan也没用,需要剔除,不在线房屋状态为0，active取的是房屋是否在架
      and deleted=0    --记录是否被删除，两条必须同时满足
       and enum_product_type=1 --基础价规则
      ) t2 on t1.rate_plan_id=t2.rate_plan_id
)d on a.house_id=d.house_id
left join 
(
  select a.house_id,
  nvl(
    round(
      count(distinct(
        if(nima_score>4.5,a.picture_guid,null))) 
      /count(distinct a.picture_guid)
      ,2) ,0) as `清晰图片占比`
  from dw_algorithm.house_picture_clarity a
  --20231201开始：图片清晰度分数过滤被删除图片
  join ods_tns_house.house_picture b on a.picture_guid=b.picture_guid
  group by 1
  )f on a.house_id=f.house_id
    
)




select 
house_id,
nvl(`图片`,0.00) picture_score,
nvl(`设施`,0.00) facility_score,
nvl(`退订规则`,0.00) unsubscribe_rule_score, 
nvl(`房屋描述`,0.00) house_desc_score,
nvl(`视频`,0.00) video_score,
nvl(`对客要求`,0.00) guest_request_score,
nvl(round(`图片`+ `设施`+`退订规则`+ `房屋描述`+`视频`+`对客要求`,2),0.00)as total_score,
nvl(`图片张数`,0.00) picNum,
nvl(`图片质量`,0.00) picQual,
nvl(`餐厨`,0.00) kitFacility,
nvl(`儿童设施`,0.00) childFacility,
nvl(`房东服务`,0.00) landlordServe,
nvl(`核心设施`,0.00) coreFacility,
nvl(`景观`,0.00) scenery,
nvl(`居家`,0.00) livingView,
nvl(`配套设施`,0.00) supportFacility,
nvl(`卫浴`,0.00) sanitaryWare,
nvl(`休闲`,0.00) relaxFacility,
nvl(`周边`,0.00) surroundings,
nvl(`房屋特色`,0.00) houseChar,
nvl(`房屋名称`,0.00) houseName,
landlord_channel,
house_is_online
from(
select 
  house_id,
  landlord_channel,
  house_is_online,
  nvl(`图片质量`,0.00) `图片质量`,
  nvl(`图片张数`,0.00) `图片张数`,
  nvl(`餐厨`,0.00)as `餐厨`,
  nvl(`儿童设施`,0.00)as `儿童设施`,
  nvl(`房东服务`,0.00)as `房东服务`,
  nvl(`核心设施`,0.00)as `核心设施`,
  nvl(`景观`,0.00)as `景观`,
  nvl(`居家`,0.00)as `居家`,
  nvl(`卫浴`,0.00)as `卫浴`,
  nvl(`休闲`,0.00)as `休闲`,
  nvl(`退订规则`,0.00) `退订规则`,
  nvl(`房屋特色`,0.00)as `房屋特色`,
  nvl(`房屋名称`,0.00)as `房屋名称`,
  nvl(`视频`,0.00) `视频`,
  nvl(`图片张数`+`图片质量`,0.00) as`图片`,
  nvl(`免费停车位`+`充电桩`+`付费停车位`,0.00)as `配套设施`,
  nvl(`周边有餐厅`+`周边有海滩`+ `周边有超市`+`周边有公园`+`周边有菜市场`+`周边有便利店`+`周边有提款机`+`周边有药店`+`周边有花园`+`周边有儿童乐园`,0.00)as `周边`,
  nvl(`核心设施`+`免费停车位`+`充电桩`+`付费停车位`+`餐厨`+`卫浴`+`房东服务`+
  `周边有餐厅`+`周边有海滩`+ `周边有超市`+`周边有公园`+`周边有菜市场`+`周边有便利店`+`周边有提款机`+`周边有药店`+`周边有花园`+`周边有儿童乐园`+
  `休闲`+`景观`+`儿童设施`+`居家`,0.00)as `设施`,
  nvl(`房屋名称`+`房屋特色`,0.00) as `房屋描述`,
  nvl(`允许做饭`+`允许带宠物`,0.00) as `对客要求`
--percentile((`房屋名称`+`房屋特色`+`允许做饭`+`允许带宠物`+`图片张数`+`图片质量`+`退订规则`+`视频`+`核心设施`+`免费停车位`+`充电桩`+`付费停车位`+`餐厨`+`卫浴`+`房东服务`+`周边有餐厅`+`周边有海滩`+ `周边有超市`+`周边有公园`+`周边有菜市场`+`周边有便利店`+`周边有提款机`+`周边有药店`+`周边有花园`+`周边有儿童乐园`+`休闲`+`景观`+`儿童设施`+`居家`),0.8)as `总分`
from bs a
) t1

