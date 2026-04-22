
-- -- 直采
-- t_rk_dynamic_gmv_score * 0.35
-- c_rk_dynamic_gmv_score * 0.15
-- comment_score * 0.1 
-- infor_score * 0.1 -- （55=5、50=4、40=3、25=2、null=1）
-- house_quality_score * 0.15 --（上传表：空间布局、设施与卫生、图片质量）
-- style_score *0.15 -- （人工打标：4=5、3=4、2=3、1=2、null=1）

-- -- C接
-- t_rk_dynamic_gmv_score * 0.25
-- c_rk_dynamic_gmv_score * 0.25
-- comment_score * 0.1 --（4.9=5、4.7=4、4.5=3、3.5=2、null=1）
-- infor_score * 0.1 -- （携程psi）
-- house_quality_score * 0.15 --（上传表：空间布局、设施与卫生、图片质量）
-- style_score *0.15 -- （人工打标：4=5、3=4、2=3、1=2、null=1）

 
 
 
 with final as (
select a.*
    ,case when o.house_id is not null then 1 else 0 end is_order 
    ,case when f.house_id is not null then 1 else 0 end is_flow
from (
    select 
        house_city_name
        ,city_level_yunying
        ,landlord_channel_name
        ,house_id
        ,nvl(house_score,0) house_score
        ,nvl(t_rk_dynamic_gmv_score,0) t_rk_dynamic_gmv_score
        ,nvl(c_rk_dynamic_gmv_score,0) c_rk_dynamic_gmv_score
        ,nvl(comment_score,0) comment_score
        ,nvl(infor_score,0) infor_score
        ,nvl(house_quality_score,0) house_quality_score
        ,nvl(style_score,0) style_score
    from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d
    where dt = date_sub(current_date,1)
    and house_is_online = 1 
) a   
left join (
    select house_id
    from dws.dws_order 
    where create_date between date_sub(current_date,30) and current_date
    and is_paysuccess_order = 1 
    group by 1 
) o 
on a.house_id = o.house_id
left join (
    select house_id
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date,30) and current_date
    and source = 102 
    and user_type = '用户'
    and is_oversea = 1 
    group by 1 
) f
on a.house_id = f.house_id
where city_level_yunying in ('S','A','B')
)
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'房屋总分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(house_score),2) score_avg
    ,round(percentile(house_score,0.1),2) score_1
    ,round(percentile(house_score,0.3),2) score_2
    ,round(percentile(house_score,0.3),2) score_3
    ,round(percentile(house_score,0.3),2) score_4
    ,round(percentile(house_score,0.3),2) score_5
    ,round(percentile(house_score,0.3),2) score_6
    ,round(percentile(house_score,0.5),2) score_7
    ,round(percentile(house_score,0.7),2) score_8
    ,round(percentile(house_score,0.9),2) score_9 
    ,round(percentile(house_score,1),2) score_max

from final
group by 1,2,3
union all 
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'途家经营分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(t_rk_dynamic_gmv_score),2) score_avg
    ,round(percentile(t_rk_dynamic_gmv_score,0.1),2) score_1
    ,round(percentile(t_rk_dynamic_gmv_score,0.3),2) score_2
    ,round(percentile(t_rk_dynamic_gmv_score,0.3),2) score_3
    ,round(percentile(t_rk_dynamic_gmv_score,0.3),2) score_4
    ,round(percentile(t_rk_dynamic_gmv_score,0.3),2) score_5
    ,round(percentile(t_rk_dynamic_gmv_score,0.3),2) score_6
    ,round(percentile(t_rk_dynamic_gmv_score,0.5),2) score_7
    ,round(percentile(t_rk_dynamic_gmv_score,0.7),2) score_8
    ,round(percentile(t_rk_dynamic_gmv_score,0.9),2) score_9 
    ,round(percentile(t_rk_dynamic_gmv_score,1),2) score_max

from final
group by 1,2,3
union all 
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'携程经营分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(c_rk_dynamic_gmv_score),2) score_avg
    ,round(percentile(c_rk_dynamic_gmv_score,0.1),2) score_1
    ,round(percentile(c_rk_dynamic_gmv_score,0.3),2) score_2
    ,round(percentile(c_rk_dynamic_gmv_score,0.3),2) score_3
    ,round(percentile(c_rk_dynamic_gmv_score,0.3),2) score_4
    ,round(percentile(c_rk_dynamic_gmv_score,0.3),2) score_5
    ,round(percentile(c_rk_dynamic_gmv_score,0.3),2) score_6
    ,round(percentile(c_rk_dynamic_gmv_score,0.5),2) score_7
    ,round(percentile(c_rk_dynamic_gmv_score,0.7),2) score_8
    ,round(percentile(c_rk_dynamic_gmv_score,0.9),2) score_9 
    ,round(percentile(c_rk_dynamic_gmv_score,1),2) score_max

from final
group by 1,2,3
union all 
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'评论分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(comment_score),2) score_avg
    ,round(percentile(comment_score,0.1),2) score_1
    ,round(percentile(comment_score,0.3),2) score_2
    ,round(percentile(comment_score,0.3),2) score_3
    ,round(percentile(comment_score,0.3),2) score_4
    ,round(percentile(comment_score,0.3),2) score_5
    ,round(percentile(comment_score,0.3),2) score_6
    ,round(percentile(comment_score,0.5),2) score_7
    ,round(percentile(comment_score,0.7),2) score_8
    ,round(percentile(comment_score,0.9),2) score_9 
    ,round(percentile(comment_score,1),2) score_max

from final
group by 1,2,3
union all 
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'信息分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(infor_score),2) score_avg
    ,round(percentile(infor_score,0.1),2) score_1
    ,round(percentile(infor_score,0.3),2) score_2
    ,round(percentile(infor_score,0.3),2) score_3
    ,round(percentile(infor_score,0.3),2) score_4
    ,round(percentile(infor_score,0.3),2) score_5
    ,round(percentile(infor_score,0.3),2) score_6
    ,round(percentile(infor_score,0.5),2) score_7
    ,round(percentile(infor_score,0.7),2) score_8
    ,round(percentile(infor_score,0.9),2) score_9 
    ,round(percentile(infor_score,1),2) score_max

from final
group by 1,2,3
union all 
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'质量分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(house_quality_score),2) score_avg
    ,round(percentile(house_quality_score,0.1),2) score_1
    ,round(percentile(house_quality_score,0.3),2) score_2
    ,round(percentile(house_quality_score,0.3),2) score_3
    ,round(percentile(house_quality_score,0.3),2) score_4
    ,round(percentile(house_quality_score,0.3),2) score_5
    ,round(percentile(house_quality_score,0.3),2) score_6
    ,round(percentile(house_quality_score,0.5),2) score_7
    ,round(percentile(house_quality_score,0.7),2) score_8
    ,round(percentile(house_quality_score,0.9),2) score_9 
    ,round(percentile(house_quality_score,1),2) score_max

from final
group by 1,2,3
union all 
select 
    house_city_name
    ,city_level_yunying
    ,landlord_channel_name
    ,'特色分' score_type
    ,count(distinct house_id) house_cnt
    ,round(avg(style_score),2) score_avg
    ,round(percentile(style_score,0.1),2) score_1
    ,round(percentile(style_score,0.3),2) score_2
    ,round(percentile(style_score,0.3),2) score_3
    ,round(percentile(style_score,0.3),2) score_4
    ,round(percentile(style_score,0.3),2) score_5
    ,round(percentile(style_score,0.3),2) score_6
    ,round(percentile(style_score,0.5),2) score_7
    ,round(percentile(style_score,0.7),2) score_8
    ,round(percentile(style_score,0.9),2) score_9 
    ,round(percentile(style_score,1),2) score_max

from final
group by 1,2,3