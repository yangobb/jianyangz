 
select h.dt `日期`
    ,h.house_id `房屋id`
    ,h.hotel_id `门店id`
    ,h.hotel_name `门店名称`
    ,h.country_name `国家`
    ,h.house_city_name `城市`
    ,h.dynamic_business `商圈`
    ,h.dynamic_business_distance `距商圈距离`
    ,h.bedroom_count `居室数`
    ,h.gross_area `面积`
    ,h.house_type `房屋类型`
    ,h.house_class `房屋等级`
    ,h.house_first_active_time `上房时间`
    ,t3.arch_pic_info `房东档案`


    -- 转化信息
    ,lpv 
    ,luv 
    ,dpv 
    ,l2d_pv
    ,l2o_pv
    ,l2o_pv_wow `l2o_pv环比`
    ,l2o_uv
    ,l2o_uv_wow `l2o_uv环比`
    ,d2o_uv
    ,d2o_uv_wow `d2o_uv环比`
    ,without_risk_access_order_num `近7日订单`
    ,without_risk_order_gmv `近7日gmv`
    ,t_90d_gmv `近90日gmv`
    
    -- 信息
    ,case when t12.house_id is not null then 1 else 0 end `是否头图视频`
    ,h.cover_picture_url `头图`
    ,case when h.cover_picture_url = h1.cover_picture_url then 1 else 0 end  `头图是否与月初一致`
    ,h.picture_count `图片数量`
    ,case when h.picture_count = h1.picture_count then 1 else 0 end `图片数量是否与月初一致`
    ,case when t1.house_id is not null then '优选' when t2.house_id is not null then '宝藏' end `优选/宝藏`

    -- 价格
    ,t6.final_price `曝光价格`
    ,t8.final_price_per `商圈中位数价格`

    -- 评价
    ,totalscore `评论数`

    -- 服务
    ,h.is_fast_booking `闪订`
    ,t4.is_maintain_checkin_guide `入住指引`
    ,t9.cancel_pp `近7日取消率`

    -- 库存
    ,t5.avaliablecount_7 `未来7日可售库存`
    ,t5.avaliablecount_30 `未来30日可售库存`
    ,t5.avaliablecount_90 `未来90日可售库存`
    ,t5.unavaliablecount_30 `未来30天途家已售`
    ,t5.full_pp_30 `未来30天满房率`
    ,t7.tu_pp_30 `近30日途占比`

    -- 等级分数
    ,t11.t_rk_dynamic_gmv_score `销售分`
    ,t11.comment_score `评论分`
    ,t11.infor_score `信息分`
    ,t11.house_quality_score `品质分`
    ,t11.style_score `标签分`
    ,t11.base_score `基础分`
    ,t11.credit_score `诚信分`
    ,t11.house_score `房屋分`

    ,case when t13.house_id is not null then 1 else 0 end `是否违规`
    ,case when t16.house_id is not null then 1 else 0 end `是否参活`
    ,case when t17.house_id is not null then 1 else 0 end `是否暑期冲高`
FROM (
    -- 房屋
    select dt
        ,house_id 
        ,hotel_id
        ,hotel_name
        ,country_name
        ,house_city_name
        ,dynamic_business
        ,dynamic_business_distance
        ,bedroom_count
        ,gross_area
        ,house_type
        ,house_class	
        ,is_fast_booking
        ,house_first_active_time
        ,picture_count
        ,cover_picture_url	
    from dws.dws_house_d
    where dt = date_sub(current_date, 1)
    AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1
    -- AND country_name IN ('日本', '泰国')
) h 
left join (
    -- 历史表现
    select house_id
        ,picture_count
        ,cover_picture_url	
    from dws.dws_house_d
    where dt = concat(substr(current_date,1,7),'-','01')
    AND landlord_channel_name = '平台商户'
    and house_is_online = 1 
    AND house_is_oversea = 1
) h1 
ON h.house_id = h1.house_id 
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
left join (
    -- 房屋信息
    select hotel_id 
        ,arch_pic_info 
    from tujia_ods.ods_tns_cms_landlord_archival_record 
    where oversea = 0 
) t3 
on h.hotel_id = t3.hotel_id
LEFT JOIN (
    -- 入住指引
    select house_id
        ,max(is_maintain_checkin_guide) is_maintain_checkin_guide
    from dwd.dwd_house_checkin_guide_d 
    where dt = date_sub(current_date,1)
    group by 1 
)t4 
ON h.house_id = t4.house_id
left join (
    -- 库存
    select
        unitid as house_id,
        sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),7) then avaliablecount end) as avaliablecount_7,
        sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then avaliablecount end) as avaliablecount_30,
        sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),90) then avaliablecount end) as avaliablecount_90,
        sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then unavaliablecount end) as unavaliablecount_30,
        -- sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then unavaliablecount end) /
        -- sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then inventorycount end) tu_pp_30,
        sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then inventorycount - avaliablecount end) /
        sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then inventorycount end) full_pp_30
    from dim_tujiaproduct.unit_inventory_log
    where createdate = date_sub(current_date(),1)
    and inventorydate between date_add(current_date(),1) and date_add(current_date(),90)
    and substr(gettime, 9, 2) = '22'
    and inventorycount is not null
    and avaliablecount is not null
    and unavaliablecount is not null
    group by 1
) t5 
on h.house_id = t5.house_id
left join (
    -- 周度流量转化
    select week1
        ,house_id
        ,lpv 
        ,luv 
        ,dpv 
        ,l2d_pv
        ,l2o_pv
        ,l2o_pv - l2o_pv_lw l2o_pv_wow
        ,l2o_uv
        ,l2o_uv - l2o_uv_lw l2o_uv_wow
        ,d2o_uv
        ,d2o_uv - d2o_uv_lw d2o_uv_wow
        ,without_risk_access_order_num
        ,without_risk_order_gmv
        ,final_price
    from (
        select week1
            ,house_id
            ,lpv 
            ,luv 
            ,dpv 
            ,l2d_pv
            ,l2o_pv
            ,l2o_uv
            ,d2o_uv
            ,without_risk_access_order_num
            ,without_risk_order_gmv
            ,lead(l2o_pv) over(partition by house_id order by week1 ) l2o_pv_lw
            ,lead(l2o_uv) over(partition by house_id order by week1 ) l2o_uv_lw
            ,lead(d2o_uv) over(partition by house_id order by week1 ) d2o_uv_lw
            ,final_price
        from (
            select
                case when dt between date_sub(current_date,7) and date_sub(current_date,1) then 'W1'
                    when dt between date_sub(current_date,14) and date_sub(current_date,8) then 'W2' 
                    end week1 
                ,house_id
                ,count(uid) lpv
                ,count(distinct case when detail_uid is not null then uid end) dpv 
                ,count(distinct concat(uid,dt)) luv

                ,nvl(count(distinct case when detail_uid is not null then uid end) / count(uid),0) l2d_pv
                ,nvl(sum(without_risk_access_order_num) / count(uid),0) l2o_pv
                ,nvl(sum(without_risk_access_order_num) / count(distinct concat(uid,dt)),0) l2o_uv    
                ,nvl(sum(without_risk_access_order_num) / count(distinct case when detail_uid is not null then concat(uid,dt) end),0) d2o_uv
                ,nvl(sum(without_risk_access_order_num),0) without_risk_access_order_num 
                ,nvl(sum(without_risk_order_gmv),0) without_risk_order_gmv
                ,avg(final_price) final_price
            from dws.dws_path_ldbo_d
            where dt between date_sub(current_date,14) and date_sub(current_date,1)
            and wrapper_name in ('携程','途家','去哪儿') 
            and is_oversea = 1 
            and user_type = '用户' 
            -- and house_id = 77535505
            group by 1,2 
        ) a 
    ) a 
    where week1 = 'W1'
) t6 
on h.house_id = t6.house_id
left join (
    -- 途占比
    select house_id
        ,(sum(instancecount)-sum(avaliablecount))/sum(instancecount) `满房率`
        ,sum(unavaliablecount)/(sum(instancecount)-sum(avaliablecount)) tu_pp_30
    from (
        select distinct unitid as house_id
            ,instancecount                           --物理库存
            ,avaliablecount                          --可售库存
            ,unavaliablecount                        --已售库存
            ,createdate
            ,inventorydate
        from  dim_tujiaproduct.unit_inventory_log a
        where a.createdate between date_sub(current_date,30) and date_sub(current_date,1)
        and a.createdate = a.inventorydate
        and substr(a.gettime,9,2) = 22
    ) a
    group by 1 
) t7 
on h.house_id = t7.house_id
left join (
    -- 商圈价格中位数
    select house_city_name
        ,dynamic_business
        ,PERCENTILE(final_price, 0.5) final_price_per
    from dws.dws_path_ldbo_d ldbo
    left join (
        select distinct house_id
                ,house_city_id
                ,house_city_name 
                ,great_tag
                ,case when bedroom_count = 1 then  '一居'
                    when bedroom_count = 2 then  '二居'
                    when bedroom_count = 3 then  '三居'
                    when bedroom_count >= 4 then  '四居+'
                    end as bedroom_count_type
        from dws.dws_house_d
        where dt = date_sub(current_date,1)
        AND landlord_channel_name = '平台商户'
        and house_is_online = 1 
        AND house_is_oversea = 1
    ) house
    on ldbo.house_id = house.house_id
    where dt between DATE_SUB(current_date,7) and DATE_SUB(current_date,1)
    and user_type = '用户'
    and wrapper_name in  ('携程','去哪儿','途家')
    and source = '102' 
    group by 1,2 
) t8 
on h.house_city_name = t8.house_city_name
and h.dynamic_business = t8.dynamic_business
left join (
    -- 7日取消率
    select house_id
        ,count(distinct case when is_cancel_order = 1 then order_no end) / count(distinct order_no) cancel_pp
    from dws.dws_order 
    where create_date between DATE_SUB(current_date,7) and DATE_SUB(current_date,1)
    and is_paysuccess_order = 1 
    and is_overseas = 1 
    group by 1 
) t9 
on h.house_id = t9.house_id
left join (
    -- 评分
    select unitid house_id 
        ,count(totalscore) totalscore	
    from ods_tujiacustomer.comment 
    where createtime between DATE_SUB(current_date,7) and DATE_SUB(current_date,1)
    group by 1 
) t10 
on h.house_id = t10.house_id
left join (
    select house_id
        ,t_90d_gmv
        ,t_rk_dynamic_gmv_score
        ,c_rk_dynamic_gmv_score
        ,comment_score
        ,infor_score
        ,house_quality_score
        ,style_score
        ,base_score
        ,reward_score
        ,credit_score
        ,penalty_score
        ,house_score 
    from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d
    where dt = date_sub(current_date,1)
) t11 
on h.house_id = t11.house_id
left join (
    select house_id
    from dws.dws_house_video_info_d
    where dt = date_sub(current_date,1)
    and source = 1 
    group by 1 
) t12 
on h.house_id = t12.house_id
left join (
    SELECT house_id
    FROM dws.dws_order_defect_d
    where dt=DATE_SUB(CURRENT_DATE,1)
    and is_overseas = 1
    and landlord_channel_name in ('平台商户')
    and checkout_date BETWEEN DATE_SUB(current_date,14) and DATE_SUB(current_date,1)
    and defect_user is not null 
    group by 1  
) t13 
on h.house_id = t13.house_id

left join (
    select house_id
    from (
        SELECT act_unit_id house_id,
            ladder_level_rule,
            activity_id,
            get_json_object(d.json_string,'$.merchantRate') merchantRate,
            get_json_object(d.json_string,'$.roomNights') roomnight
        FROM 
        dwd.dwd_tns_salespromotion_activity_detail_d d
        lateral view explode(udf.json_split(ladder_level_rule)) r as d
        WHERE audit_status = 2 -- 在 线
        AND d.dt = date_sub(current_date,1)
        and check_out_date >= d.dt
    ) a 
    where merchantRate < 95 
    group by 1 
) t16 
on h.house_id = t16.house_id
left join (
    select *
    from pdb_analysis_c.dwd_house_rank_tag_77_d 
    where dt = date_sub(current_date(),1)
) t17 
on h.house_id = t17.house_id
