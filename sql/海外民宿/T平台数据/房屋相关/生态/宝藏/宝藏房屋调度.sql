
--1.1版本
--其中设计风格，特色设施，人工打标分三个为核心维度。占比各为 33%
--特色风景，特色建筑为附加得分维度。特色风景的 SAB标签分别加分为。3 分。1.5 分。1分。特色建筑的 SAB 标签分别加分为 1.5 分，1 分，0.5 分。得分可以进行试算调整
--每个维度的得分上限为 10 分
--新增特色品牌列与特色品牌附加分，特色品牌附加分梯度为 1分，0.5分。0.25 分
with t1 as (
select
  t.*
  ,((LENGTH(design_style_level) - LENGTH(REPLACE(design_style_level, 'S', ''))) * 10) +
   ((LENGTH(design_style_level) - LENGTH(REPLACE(design_style_level, 'A', ''))) * 6) +
   ((LENGTH(design_style_level) - LENGTH(REPLACE(design_style_level, 'B', ''))) * 3)  AS design_style_level_score -- 设计风格分

  ,((LENGTH(featured_facilities_level) - LENGTH(REPLACE(featured_facilities_level, 'S', ''))) * 10) +
   ((LENGTH(featured_facilities_level) - LENGTH(REPLACE(featured_facilities_level, 'A', ''))) * 6) +
   ((LENGTH(featured_facilities_level) - LENGTH(REPLACE(featured_facilities_level, 'B', ''))) * 3)  AS featured_facilities_level_score -- 特色设施分

  ,round(((COALESCE(image_quality_score, 0) + COALESCE(decoration_quality_score, 0) + COALESCE(sanitation_facilities_division, 0)) / 3 )*2,1) AS basic_quality_score -- 基础品质分

  ,((LENGTH(characteristic_buildings_level) - LENGTH(REPLACE(characteristic_buildings_level, 'S', ''))) * 1.5) +
   ((LENGTH(characteristic_buildings_level) - LENGTH(REPLACE(characteristic_buildings_level, 'A', ''))) * 1) +
   ((LENGTH(characteristic_buildings_level) - LENGTH(REPLACE(characteristic_buildings_level, 'B', ''))) * 0.5)  AS characteristic_buildings_level_score -- 特色建筑分

  ,((LENGTH(featured_scenery_level) - LENGTH(REPLACE(featured_scenery_level, 'S', ''))) * 3) +
   ((LENGTH(featured_scenery_level) - LENGTH(REPLACE(featured_scenery_level, 'A', ''))) * 1.5) +
   ((LENGTH(featured_scenery_level) - LENGTH(REPLACE(featured_scenery_level, 'B', ''))) * 0.5)  AS featured_scenery_level_score -- 特色风景分

  ,((LENGTH(Featuredbrand_level) - LENGTH(REPLACE(Featuredbrand_level, 'S', ''))) * 1) +
   ((LENGTH(Featuredbrand_level) - LENGTH(REPLACE(Featuredbrand_level, 'A', ''))) * 0.5) +
   ((LENGTH(Featuredbrand_level) - LENGTH(REPLACE(Featuredbrand_level, 'B', ''))) * 0.25)  AS Featuredbrand_level_score -- 特色品牌分

from pdb_analysis_c.ads_house_baozang_overseas_d t
where dt = DATE_ADD(CURRENT_DATE(), -1)
)
,t2 as (
select t1.*
    ,case when design_style_level_score >=10 then 10 else design_style_level_score end as design_style_level_score_10             -- 转化为每项得分10分制
    ,case when featured_scenery_level_score >=10 then 10 else featured_scenery_level_score end as featured_scenery_level_score_10
    ,case when featured_facilities_level_score >=10 then 10 else featured_facilities_level_score end as featured_facilities_level_score_10
    ,case when characteristic_buildings_level_score >=10 then 10 else characteristic_buildings_level_score end as characteristic_buildings_level_score_10
    ,case when basic_quality_score >= 10 then 10 else basic_quality_score end as basic_quality_score_10

    ,round((design_style_level_score_10*0.33+featured_facilities_level_score_10*0.33+basic_quality_score_10*0.33+characteristic_buildings_level_score_10+featured_scenery_level_score_10+Featuredbrand_level_score),2) as TH_score  -- 10分制得分
    ,round((design_style_level_score_10*0.33+featured_facilities_level_score_10*0.33+basic_quality_score_10*0.33+characteristic_buildings_level_score_10+featured_scenery_level_score_10+Featuredbrand_level_score),0) as TH_score_10  -- 10分制得分

from t1
)
,t4 as (
select a.house_id
    ,min(dt) first_baozang
    ,nvl(datediff(current_date,min(dt)),0) baozang_date_gap
from (
    select house_id
    FROM dws.dws_house_d
    WHERE dt = date_sub(current_date,1)
    AND house_is_oversea = 1
    AND house_is_online = 1
    AND house_city_name in ('清迈','普吉岛') 
    AND landlord_channel_name in ('平台商户')
) a 
inner join (
    select house_id
        ,dt 
    from pdb_analysis_b.dwd_house_label_1000488_d
    where dt >= date_sub(current_date,89)
) b 
on a.house_id = b.house_id
group by 1 
)

select distinct t2.house_id,'1000488' as label_id,'宝藏民宿' as label_name
from (
    select * from t2
) t2 
inner join (
    select distinct bb.house_id
        ,bb.house_city_name
        ,1 is_video
    from (
        -- select distinct unitnumber
        --     ,auditvideo
        --     ,auditpassvideo
        --     ,case when auditvideo is not null and auditvideo != '{ }' and auditpassvideo is not null  and auditpassvideo != '{ }' and auditpassvideo != '{}' then 1 else 0 end is_video
        -- from ods_merchantcrm.houseunitedit
        select house_id
        from ods_tns_baseinfo.house_info
        where enum_data_entity_status = 0
        and get_json_object(get_json_object(base_info,'$.auditPassVideo'),'$.videoUrl') is not null  --C接中存在videoGuid为"",但有videoUrl的情况
        and get_json_object(get_json_object(base_info,'$.auditPassVideo'),'$.videoUrl')<>''
    ) aa 
    join (
        select distinct house_number,house_id, house_name, hotel_name,country_name,house_class,house_city_name,landlord_channel_name 
        FROM    dws.dws_house_d
        WHERE   dt = date_sub(current_date,1)
        AND     house_is_oversea = 1
        AND     house_is_online = 1
        AND     house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') 
        AND     landlord_channel_name in ('平台商户')
    ) bb
    on aa.unitnumber=bb.house_number
) t3 
on t2.house_id = t3.house_id
left join t4 
on t2.house_id = t4.house_id
where TH_score_10 >=6 -- 模型分6
and case when house_city_name in ('新加坡','香港','首尔','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') and is_video = 1 then 1 
           when house_city_name in ('清迈','普吉岛') and baozang_date_gap <= 89 then 1 end = 1


