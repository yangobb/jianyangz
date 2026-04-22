with house_quality_score as (
select 
    house_id
    ,a.soft_hard
    ,a.house_layout
    ,a.sanitation_facilities
    ,a.picture_quality
    ,nvl(if(sum(b.score*0.60 + c.score*0.133 + d.score*0.133 + e.score*0.133)>=10,10,sum(b.score*0.60 + c.score*0.133 + d.score*0.133 + e.score*0.133)),0) as house_quality_score
from excel_upload.overseasrm a
left join excel_upload.overseas_house_quality13 b on a.soft_hard = b.type and b.house_quality = '房屋软硬装'
left join excel_upload.overseas_house_quality13 c on a.house_layout = c.type and c.house_quality = '空间布局'
left join excel_upload.overseas_house_quality13 d on a.sanitation_facilities = d.type and d.house_quality = '设施与卫生'
left join excel_upload.overseas_house_quality13 e on a.picture_quality = e.type and e.house_quality = '图片质量'
group by 
    house_id
    ,a.soft_hard
    ,a.house_layout
    ,a.sanitation_facilities
    ,a.picture_quality
)
,style_score as (
select 
    house_id
    ,case when count(house_label)>=4 then 5 
        when count(house_label)>=3 then 4
        when count(house_label)>=2 then 3
        when count(house_label)>=1 then 2
    else 1 end as style_score 
from (
    select 
        a.house_id
        ,b.house_label
    from excel_upload.overseasrm a
    LATERAL VIEW explode(split(concat(design_style,',',landscape,',',special_device,',',characteristic_buildings,',',featured_scenes,',',special_services,',',special_experiences,',',special_location,',',landmark,',',infrastructure),",")) b as house_label
    where house_label != '' --合并时会有空字符串
    group by 
        a.house_id
        ,b.house_label
) t 
group by house_id
)


