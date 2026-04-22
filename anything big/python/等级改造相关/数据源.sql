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



select
     h.house_city_name
    ,h.dynamic_business
    ,h.house_id 
    ,case when h.house_class = 'L4' then 7
          when h.house_class = 'L3' then 6
          when h.house_class = 'L25' then 5
          when h.house_class = 'L24' then 4
          when h.house_class = 'L21' then 3
          when h.house_class = 'L1' then 2
          else 1 end house_class
    ,h.landlord_channel
    ,h.bedroom_count
    ,h.bedcount
    ,h.picture_count 
    ,h.comment_score
    ,h.service_comment_score
    ,h.hygiene_comment_score
    ,case when t1.house_id is not null then 1 else 0 end is_yx
    ,case when t2.house_id is not null then 1 else 0 end is_bz
    ,luv
    ,price_5
    ,house_quality_score
    ,style_score
    ,nvl(lpv/order_num,0) conversion_1000uv 
from (
    select house_id 
        ,house_city_name
        ,dynamic_business
        ,house_class
        ,landlord_channel
        ,bedroom_count
        ,bedcount
        ,picture_count 
        ,comment_score
        ,service_comment_score
        ,hygiene_comment_score
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
    and house_is_oversea = 1 
    and landlord_channel = 1 
) h 
inner join (
    select *
    from excel_upload.oversea_city_level
    where city_level in ('S','A','B')
    and city_name in ('曼谷','首尔','大阪')
) cl 
on h.house_city_name = cl.city_name
left join (
    -- 优选
    select house_id
    from pdb_analysis_b.dwd_house_label_1000487_d
    where dt = date_sub(current_date,1)
) t1
on h.house_id = t1.house_id 
left join (
    -- 宝藏
    select house_id
    from pdb_analysis_b.dwd_house_label_1000488_d
    where dt = date_sub(current_date,1)
) t2
on h.house_id = t2.house_id
join (
    select house_id 
        ,count(1) lpv 
        ,count(distinct dt,uid) luv 
        ,percentile(final_price,0.5) price_5
        ,sum(without_risk_order_num) order_num
        ,sum(without_risk_order_gmv) gmv
        ,sum(without_risk_order_room_night) night
    from (
        select *
        from dws.dws_path_ldbo_d
        where dt >= date_sub(current_date,30)
        and source = 102
        and user_type = '用户'
        and is_oversea = 1 
    ) l
    group by 1 
    having count(distinct dt,uid) >= 20
) l 
on h.house_id = l.house_id
left join house_quality_score a 
on h.house_id = a.house_id
left join style_score b 
on h.house_id = b.house_id