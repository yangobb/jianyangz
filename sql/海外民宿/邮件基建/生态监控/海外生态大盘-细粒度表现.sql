
with house_info as
(
        SELECT
            t1.dt,
            hotel_id,
            house_create_time,
            house_city_id as city_id,
            city_level,
            case when house_city_name = '陵水' then '陵水(三亚)' else house_city_name end as house_city_name,
            t1.house_id,
            house_name,
            dynamic_business,
            dynamic_business_id,
            house_class,
            holiday_class,
            house_type,
            case when is_gold_medal = 1 or is_special_brand_homestay = 1  then '金特牌'
            else '非金特牌' end as is_gold,
            case when is_prefer_pro=1 then is_prefer_pro else landlord_shoot_tag end as is_prefer_pro,
            main_label,
            -- landlord_channel,
            CASE 
                WHEN bedroom_count = 1 OR share_type = '单间' THEN 1
                WHEN bedroom_count = 2 AND share_type = '整租' THEN 2
                WHEN bedroom_count = 3 AND share_type = '整租' THEN 3
                WHEN bedroom_count = 4 AND share_type = '整租' THEN 4
                WHEN bedroom_count = 5 AND share_type = '整租' then 5
                when bedroom_count = 6 AND share_type = '整租' then 6
                when bedroom_count = 7 AND share_type = '整租' then 7
                when bedroom_count = 8 AND share_type = '整租' then 8
                when bedroom_count = 9 AND share_type = '整租' then 9
                when bedroom_count = 10 AND share_type = '整租' then 10
                when bedroom_count > 10 AND share_type = '整租' then '10+'
                ELSE '其他' end as `具体居室`,
            CASE 
                WHEN bedroom_count = 1 OR share_type = '单间' THEN 1
                WHEN bedroom_count = 2 AND share_type = '整租' THEN 2
                WHEN bedroom_count >= 3 AND share_type = '整租' THEN '3+'
                -- when bedroom_count >= 4 AND share_type = '整租' then '4+'
                ELSE '其他' end as `居室`,
            bedroom_count,
            share_type,
            bedcount as `床数`,
            recommended_guest as `人数`,
            comment_score,
            valid_comment_num,
            case when landlord_channel_name = '平台商户' then '直采' else '接入' end as landlord_channel,
            -- case when landlord_channel=303 then '接入' --携程接入
            --     when landlord_channel=1 then '直采'
            --     else '其他接入' end as landlord_channel,
            instance_count --物理库存(房屋实例数)
            ,CASE WHEN is_prefer=1   THEN '优选'
            ELSE '其他' END AS `是否优选`
            ,CASE WHEN is_prefer_pro=1   THEN '严选' 
            ELSE '其他' END AS `是否严选`
            ,CASE WHEN great_tag=1 THEN '臻选'
            ELSE '其他' END AS `是否臻选`
            ,house_is_online
        FROM dws.dws_house_d t1
        WHERE t1.dt between date_sub('${partition}',7) and date_sub('${partition}',1)
        --   AND house_is_online = 1
        AND house_is_oversea = '1' -- 海外
)
,hotel_type_info as(
    select distinct
        dt
        ,hotelid
        ,leveldesc
    from pdb_analysis_c.dwd_landlord_subindex_score_d
    where dt between date_sub('${partition}',7) and date_sub('${partition}',1)
)
,baozang_info as(
    select distinct 
        dt,house_id 
    from pdb_analysis_b.dwd_house_label_1000488_d 
    where dt between date_sub('${partition}',7) and date_sub('${partition}',1)
)
,real_video_info as(
    select distinct 
        bb.dt
        ,bb.house_id
    from 
        (select distinct unitnumber, auditvideo,auditpassvideo
        from ods_merchantcrm.houseunitedit
        where auditvideo is not null and auditvideo != '{ }' and auditpassvideo is not null and auditpassvideo != '{ }'
        ) aa
    join 
    (select distinct dt,house_number,house_id
    FROM    dws.dws_house_d
            WHERE   dt between date_sub('${partition}',7) and date_sub('${partition}',1)
            AND     house_is_oversea = 1
            AND     house_is_online = 1
            -- AND     house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') 
            AND     landlord_channel_name in ('平台商户'))bb
    on aa.unitnumber=bb.house_number
)
,youxuan_info as(
    select distinct 
        dt,house_id
    from pdb_analysis_b.dwd_house_label_1000487_d 
    where dt between date_sub('${partition}',7) and date_sub('${partition}',1)
)
,tese_info as(
    select distinct 
        dt,house_id
    from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d 
    where dt between date_sub('${partition}',7) and date_sub('${partition}',1)
    and house_is_online =1 
    and style_score_rule is not null 
)
,geo_info as(
    select 
        t1.geo_position_id
        ,t1.name as geo_name
        ,case
        when t1.geo_position_type='1' then '地铁站'
        when t1.geo_position_type='2' then '机场'
        when t1.geo_position_type='3' then '高校'
        when t1.geo_position_type='4' then '火车站'
        when t1.geo_position_type='5' then '观光景点'
        when t1.geo_position_type='6' then '汽车站'
        when t1.geo_position_type='7' then '地铁线路'
        when t1.geo_position_type='8' then '商圈'
        when t1.geo_position_type='10' then '医院'
        when t1.geo_position_type='16' then '道路'
        when t1.geo_position_type='17' then '购物'
        when t1.geo_position_type='18' then '机构'
        when t1.geo_position_type='19' then '码头'
        when t1.geo_position_type='20' then '小区'
        when t1.geo_position_type='21' then '学校'
        when t1.geo_position_type='22' then '娱乐'
        when t1.geo_position_type='23' then '携程地标'
        when t1.geo_position_type='24' then '景区'
        else '其他'
        end as geo_type
        ,destination_id as city_id
    from 
        ods_geo_landmark.geo_position as t1
    where valid = '1' and front_show = '1'
)
,house_weight as 
    (
        select
            city_id
            ,geo_position_id
            ,weight_dis
            -- ,case when weight_dis is null then 2000 else weight_dis end as weight_dis
        from
            (
                select distinct 
                    city_id
                    -- ,city_name
                    ,geo_position_id
                    ,get_json_object(regexp_extract(get_json_object(adjust_distance,'$.adjustDistance'),'^\\[(.+)\\]$',1),'$.distanceThreshold')*1000 as weight_dis --降权距离（单位：m）
                    from tujia_ods.rank_data_rank_rank_geo_config 
                    where geo_type not in ('8','24') --只看poi点
                --and get_json_object(regexp_extract(get_json_object(adjust_distance,'$.adjustDistance'),'^\\[(.+)\\]$',1),'$.distanceThreshold') > 0
            )a
    )
,recall_distance_info as (
    select distinct 
        a.geo_position_id
        ,a.house_id
        ,a.if_in_business_district
    from
        dws.dws_house_distance_recall a
)
,hotel_ord_info as(
    select
        to_date(orderdate) as dt
        ,uid
        ,clientid
        ,orderid
        ,ciireceivable as gmv
        ,ciiquantity as nights
        ,ciireceivable/ciiquantity as adr
    from app_ctrip.edw_htl_order_all_split
    where 
        d = date_sub('${partition}',1)
        and submitfrom='client'
        and to_date(orderdate) between date_sub('${partition}',7) and date_sub('${partition}',1)
        and orderstatus in ('S','P')
        and (cityname in ('香港','澳门') or (country<>1))
        and ordertype = 2 -- 酒店订单
        and uid not in ('_A20190122115701366','_A20151130164107749','_A20190725013107744','E275301478','_A20200710175238972','_A20200211153419761','_A20200921154622724','_A20180814102302643','_A20150928110743155','_A20210226104734937')
        and clientid <> ''
        and clientid is not null
)
,hotel_adr_info as(
    select
        case when a.dt between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time
        ,percentile_approx(adr,0.5) as `酒店成单中位价`
        ,percentile_approx(adr,0.2) as `酒店成单20分位价`
    from hotel_ord_info a
    group by 1
)
,list as(
    select
        dt
        ,city_id
        ,city_name
        ,uid
        ,detail_uid
        ,house_id
        ,hotel_id
        ,search_id
        ,rank_trace_id
        ,geo_position_id
        ,final_price
        ,position
        ,wrapper_name
        ,without_risk_access_order_room_night
        ,without_risk_access_order_gmv
        ,without_risk_access_order_num
        ,distance
        ,case 
            when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
            when get_json_object(server_log,'$.searchScene') = 2 then '城市空搜'
            when get_json_object(server_log,'$.searchScene') = 3 then '景区地区'
            when get_json_object(server_log,'$.searchScene') = 8 then '县级市'
            when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
            when get_json_object(server_log,'$.searchScene') = 5 then '地标'
            when get_json_object(server_log,'$.searchScene') = 6 then '定位'
            when get_json_object(server_log,'$.searchScene') = 7 then '房屋搜索'
            when get_json_object(server_log,'$.searchScene') = 9 then '三方地标' 
            when get_json_object(server_log,'$.searchScene') = 0 then '无' 
            else '其他'
            end as search_type
        ,case when dt between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time
        ,get_json_object(server_log,'$.canSalePercentOfL25L34') as canSalePercentOfL25L34
        ,get_json_object(server_log,'$.canSalePercentOfL34') as canSalePercentOfL34
        ,case when datediff(checkout_date,checkin_date)<=1 then 1
              when datediff(checkout_date,checkin_date)>1 and datediff(checkout_date,checkin_date)<=3 then '2_3'
              when datediff(checkout_date,checkin_date)>3 and datediff(checkout_date,checkin_date)<=5 then '4_5'
              when datediff(checkout_date,checkin_date)>5 and datediff(checkout_date,checkin_date)<=7 then '6_7'
              when datediff(checkout_date,checkin_date)>7 and datediff(checkout_date,checkin_date)<=10 then '8_10'
              when datediff(checkout_date,checkin_date)>10 then '10+'
              end as `连住天数`
    from
        dws.dws_path_ldbo_d
    where  dt between date_sub('${partition}',7) and date_sub('${partition}',1)
        and user_type = '用户'
        and ((wrapper_name in ('携程','途家','去哪儿') and source = '102'))
        and (sort_type = '推荐排序' or sort_type is null)
        and is_oversea = 1 -- 海外
)
,ord as(
    select 
        case   
        when terminal_type_name = '艺龙-小程序' then '艺龙'
        when terminal_type_name = '本站-APP' then '途家'
        when terminal_type_name = '携程-APP' then '携程'
        when terminal_type_name = '去哪儿-APP' then '去哪儿'
        end as wrapper_name
        ,create_date as dt
        ,house_id
        ,hotel_id
        ,uid
        ,order_no
        ,room_total_amount
        ,order_room_night_count
        ,room_total_amount/order_room_night_count as adr
        ,checkin_date
        ,checkout_date
        ,city_name
        ,case when create_date between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time
        ,case when datediff(checkout_date,checkin_date)<=1 then 1
              when datediff(checkout_date,checkin_date)>1 and datediff(checkout_date,checkin_date)<=3 then '2_3'
              when datediff(checkout_date,checkin_date)>3 and datediff(checkout_date,checkin_date)<=5 then '4_5'
              when datediff(checkout_date,checkin_date)>5 and datediff(checkout_date,checkin_date)<=7 then '6_7'
              when datediff(checkout_date,checkin_date)>7 and datediff(checkout_date,checkin_date)<=10 then '8_10'
              when datediff(checkout_date,checkin_date)>10 then '10+'
              end as `连住天数`
    from dws.dws_order 
    where create_date between date_sub('${partition}',7) and date_sub('${partition}',1)
    and is_paysuccess_order = 1 
    and terminal_type_name in ('携程-APP','去哪儿-APP','本站-APP')
    and is_risk_order = 0
    and is_overseas = 1 -- 海外
)
,baojia_info as(
    select
        dt
        ,house_id
        ,percentile_approx(base_price,0.5) as `报价中位`
        ,avg(base_price) as `报价均值`
    from
        (
            -- select distinct 
            --     house_id
            --     ,checkin_date
            --     ,dt
            --     ,base_price
            -- from dwd.dwd_house_daily_price_member_d
            -- where dt between date_sub('2024-10-16',28) and date_sub('2024-10-16', 1)
            -- and inventory>0
            -- and can_booking=1
            -- and base_price>0
            select
                dt
                ,house_id
                ,checkin_date
                ,base_price
                -- ,min(base_price) as base_price --基础价
                -- ,min(price) as act_price --活动价
            from dwd.dwd_house_daily_price_d
            where dt between date_sub('${partition}',7) and date_sub('${partition}',1)
            -- and checkin_date=dt
            and inventory>0
            and can_booking=1
        ) as t1
    group by 1,2
)
,house_cnt_info as(
    select
        case when a.dt between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time
    -----库存
        ,count(distinct if(instance_count=1,a.house_id,null)) as dankucun_hs_cnt
        ,count(distinct if(instance_count>10,a.house_id,null)) as 10kucun_hs_cnt
    -----生态
        ,count(distinct if(house_class='L4',a.house_id,null)) as L4_hs_cnt
        ,count(distinct if(house_class='L3',a.house_id,null)) as L3_hs_cnt
        ,count(distinct if(house_class in ('L25','L3','L4'),a.house_id,null)) as L25plus_hs_cnt
        ,count(distinct if(house_class in ('L21','L25','L3','L4'),a.house_id,null)) as L21up_hs_cnt
        ,count(distinct if(d.house_id is not null,a.house_id,null)) as baozang_hs_cnt
        ,count(distinct if(e.house_id is not null,a.house_id,null)) as real_video_hs_cnt
        ,count(distinct if(f.house_id is not null,a.house_id,null)) as youxuan_hs_cnt
        ,count(distinct if(g.house_id is not null,a.house_id,null)) as tese_hs_cnt
        ,count(distinct if(landlord_channel='直采',a.house_id,null)) as zhicai_hs_cnt
        ,count(distinct if(landlord_channel='接入',a.house_id,null)) as jieru_hs_cnt
    ------评论
        ,count(distinct if(comment_score<4,a.house_id,null)) as commentscore_low_4_hs_cnt
        ,count(distinct if(comment_score<4.5,a.house_id,null)) as commentscore_low_45_hs_cnt
        ,count(distinct if(valid_comment_num=0 or valid_comment_num is null,a.house_id,null)) as no_comment_hs_cnt
        -- ,count(distinct if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),house_id,null)) as top10_nocomment_or_commentscore_low4_hs_cnt
        -- ,count(distinct if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),house_id,null)) as top3_nocomment_or_commentscore_low4_hs_cnt
    ------价格
        ,count(distinct if(`报价中位`<100,a.house_id,null)) as low_100_hs_cnt
        ,count(distinct if(`报价中位`<200,a.house_id,null)) as low_200_hs_cnt
        ,count(distinct if(`报价中位`>3000,a.house_id,null)) as high_3000_hs_cnt
    ------居室
        ,count(distinct if(`居室`=1,a.house_id,null)) as 1_bed_hs_cnt
        ,count(distinct if(`居室`=2,a.house_id,null)) as 2_bed_hs_cnt
        ,count(distinct if(`居室`='3+',a.house_id,null)) as 3_bed_hs_cnt
    from house_info a
    left join hotel_type_info c
    on a.dt = c.dt
    and a.hotel_id = c.hotelid
    left join baozang_info d
    on a.dt = d.dt
    and a.house_id = d.house_id
    left join real_video_info e
    on a.dt = e.dt
    and a.house_id = e.house_id
    left join youxuan_info f
    on a.dt = f.dt
    and a.house_id = f.house_id
    left join tese_info g
    on a.dt = g.dt
    and a.house_id = g.house_id
    left join baojia_info h
    on a.dt = h.dt
    and a.house_id = h.house_id
    where a.house_is_online = 1 -- 在线房源
    group by 1
)
,pv_info as(
    select
        case when a.dt between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time

        ,percentile_approx(canSalePercentOfL34,0.5) as canSalePercentOfL34
        ,percentile_approx(canSalePercentOfL25L34,0.5) as canSalePercentOfL2534
        ,count(1) as list_pv
        ,sum(without_risk_access_order_room_night) as total_nights_z
        ,sum(without_risk_access_order_gmv) as total_gmv_z
    -----库存
        ,count(if(instance_count=1,1,null)) as dankucun_pv
        ,count(if(instance_count>10,1,null)) as 10kucun_pv
    -----生态
        ,count(if(house_class='L4',1,null)) as L4_pv
        ,count(if(house_class='L3',1,null)) as L3_pv
        ,count(if(house_class in ('L25','L3','L4'),1,null)) as L25plus_pv
        ,count(if(house_class in ('L21','L25','L3','L4'),1,null)) as L21up_pv
        ,count(if(d.house_id is not null,1,null)) as baozang_pv
        ,count(if(e.house_id is not null,1,null)) as real_video_pv
        ,count(if(f.house_id is not null,1,null)) as youxuan_pv
        ,count(if(g.house_id is not null,1,null)) as tese_pv
        ,count(if(landlord_channel='直采',1,null)) as zhicai_pv
        ,count(if(landlord_channel='接入',1,null)) as jieru_pv
    ------评论
        ,count(if(comment_score<4,1,null)) as commentscore_low_4_pv
        ,count(if(comment_score<4.5,1,null)) as commentscore_low_45_pv
        ,count(if(valid_comment_num=0 or valid_comment_num is null,1,null)) as no_comment_pv
        ,count(if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),1,null)) as top10_nocomment_or_commentscore_low4_pv
        ,count(if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),1,null)) as top3_nocomment_or_commentscore_low4_pv

        ,sum(if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),without_risk_access_order_room_night,null)) as top10_nocomment_or_commentscore_low4_nights
        ,sum(if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),without_risk_access_order_room_night,null)) as top3_nocomment_or_commentscore_low4_nights

        ,sum(if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),without_risk_access_order_gmv,null)) as top10_nocomment_or_commentscore_low4_gmv
        ,sum(if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),without_risk_access_order_gmv,null)) as top3_nocomment_or_commentscore_low4_gmv
    ------价格
        ,count(if(final_price<100,1,null)) as low_100_pv
        ,count(if(final_price<200,1,null)) as low_200_pv
        ,count(if(final_price>3000,1,null)) as high_3000_pv
        ,avg(final_price) as avg_price
        ,percentile_approx(if(`居室`=1,final_price,null),0.5) as 1_bed_mid_price
        ,percentile_approx(if(`居室`=2,final_price,null),0.5) as 2_bed_mid_price
        ,percentile_approx(if(`居室`='3+',final_price,null),0.5) as 3_bed_mid_price

        ,count(if(final_price<`酒店成单20分位价`,1,null)) as `酒店同城成单价格20分位以下pv`
        ,count(if(final_price<`酒店成单中位价`,1,null)) as `酒店同城成单价格50分位以下pv`
    ------居室
        ,count(if(`居室`=1,1,null)) as 1_bed_pv
        ,count(if(`居室`=2,1,null)) as 2_bed_pv
        ,count(if(`居室`='3+',1,null)) as 3_bed_pv
    ------距离
        ,count(if(search_type='定位',1,null)) as locating_pv
        ,count(if(search_type='地标',1,null)) as land_pv

        ,count(if(search_type='定位' and distance<1000,1,null)) as locating_low_1000m_pv
        ,count(if(search_type='定位' and distance<2000,1,null)) as locating_low_2000m_pv
        ,count(if(search_type='定位' and distance>3000,1,null)) as locating_high_3000m_pv
        ,count(if(search_type='地标' and distance>3000,1,null)) as land_high_3000m_pv
        ,count(if((search_type='地标' and j.geo_type <> '商圈' and distance <= coalesce(weight_dis,2000)) or (j.geo_type='商圈' and if_in_business_district=1),1,null)) as jiangquanjuli_pv

        ,sum(if(search_type='定位',without_risk_access_order_room_night,null)) as locating_nights
        ,sum(if(search_type='地标',without_risk_access_order_room_night,null)) as land_nights

        ,sum(if(search_type='定位' and distance<1000,without_risk_access_order_room_night,null)) as locating_low_1000m_nights
        ,sum(if(search_type='定位' and distance<2000,without_risk_access_order_room_night,null)) as locating_low_2000m_nights
        ,sum(if(search_type='定位' and distance>3000,without_risk_access_order_room_night,null)) as locating_high_3000m_nights
        ,sum(if(search_type='地标' and distance>3000,without_risk_access_order_room_night,null)) as land_high_3000m_nights
        ,sum(if((search_type='地标' and j.geo_type <> '商圈' and distance <= coalesce(weight_dis,2000)) or (j.geo_type='商圈' and if_in_business_district=1),without_risk_access_order_room_night,null)) as jiangquanjuli_nights

        ,sum(if(search_type='定位',without_risk_access_order_gmv,null)) as locating_gmv
        ,sum(if(search_type='地标',without_risk_access_order_gmv,null)) as land_gmv

        ,sum(if(search_type='定位' and distance<1000,without_risk_access_order_gmv,null)) as locating_low_1000m_gmv
        ,sum(if(search_type='定位' and distance<2000,without_risk_access_order_gmv,null)) as locating_low_2000m_gmv
        ,sum(if(search_type='定位' and distance>3000,without_risk_access_order_gmv,null)) as locating_high_3000m_gmv
        ,sum(if(search_type='地标' and distance>3000,without_risk_access_order_gmv,null)) as land_high_3000m_gmv
        ,sum(if((search_type='地标' and j.geo_type <> '商圈' and distance <= coalesce(weight_dis,2000)) or (j.geo_type='商圈' and if_in_business_district=1),without_risk_access_order_gmv,null)) as jiangquanjuli_gmv
    --------连住天数
        ,count(if(`连住天数`=1,1,null)) as continue_1_pv
        ,count(if(`连住天数`='2_3',1,null)) as continue_3_pv
        ,count(if(`连住天数`='4_5',1,null)) as continue_5_pv
        ,count(if(`连住天数`='6_7',1,null)) as continue_7_pv
        ,count(if(`连住天数`='8_10',1,null)) as continue_10_pv
        ,count(if(`连住天数`='10+',1,null)) as continue_10plus_pv
    from list a
    left join house_info b
    on a.dt = b.dt
    and a.house_id = b.house_id
    left join hotel_type_info c
    on a.dt = c.dt
    and a.hotel_id = c.hotelid
    left join baozang_info d
    on a.dt = d.dt
    and a.house_id = d.house_id
    left join real_video_info e
    on a.dt = e.dt
    and a.house_id = e.house_id
    left join youxuan_info f
    on a.dt = f.dt
    and a.house_id = f.house_id
    left join tese_info g
    on a.dt = g.dt
    and a.house_id = g.house_id
    left join house_weight h
    on a.geo_position_id = h.geo_position_id
    and a.city_id = h.city_id
    left join recall_distance_info i
    on a.geo_position_id = i.geo_position_id
    and a.house_id = i.house_id
    left join geo_info j
    on a.geo_position_id = j.geo_position_id
    and a.city_id = j.city_id
    left join hotel_adr_info k
    on a.week_time = k.week_time
    group by 1
)
,ord_info as(
    select
        case when a.dt between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time
-------间夜
        ,sum(order_room_night_count) as total_nights_k
    -----库存
        ,sum(if(instance_count=1,order_room_night_count,null)) as dankucun_nights
        ,sum(if(instance_count>10,order_room_night_count,null)) as 10kucun_nights
    -----生态
        ,sum(if(house_class='L4',order_room_night_count,null)) as L4_nights
        ,sum(if(house_class='L3',order_room_night_count,null)) as L3_nights
        ,sum(if(house_class in ('L25','L3','L4'),order_room_night_count,null)) as L25plus_nights
        ,sum(if(house_class in ('L21','L25','L3','L4'),order_room_night_count,null)) as L21up_nights
        ,sum(if(d.house_id is not null,order_room_night_count,null)) as baozang_nights
        ,sum(if(e.house_id is not null,order_room_night_count,null)) as real_video_nights
        ,sum(if(f.house_id is not null,order_room_night_count,null)) as youxuan_nights
        ,sum(if(g.house_id is not null,order_room_night_count,null)) as tese_nights
        ,sum(if(landlord_channel='直采',order_room_night_count,null)) as zhicai_nights
        ,sum(if(landlord_channel='接入',order_room_night_count,null)) as jieru_nights
    ------评论
        ,sum(if(comment_score<4,order_room_night_count,null)) as commentscore_low_4_nights
        ,sum(if(comment_score<4.5,order_room_night_count,null)) as commentscore_low_45_nights
        ,sum(if(valid_comment_num=0 or valid_comment_num is null,order_room_night_count,null)) as no_comment_nights
        -- ,sum(if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),order_room_night_count,null)) as top10_nocomment_or_commentscore_low4_nights
        -- ,sum(if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),order_room_night_count,null)) as top3_nocomment_or_commentscore_low4_nights
    ------价格
        ,sum(if(adr<100,order_room_night_count,null)) as low_100_nights
        ,sum(if(adr<200,order_room_night_count,null)) as low_200_nights
        ,sum(if(adr>3000,order_room_night_count,null)) as high_3000_nights
        ,sum(room_total_amount)/sum(order_room_night_count) as avg_adr
        ,percentile_approx(if(`居室`=1,adr,null),0.5) as 1_bed_mid_adr
        ,percentile_approx(if(`居室`=2,adr,null),0.5) as 2_bed_mid_adr
        ,percentile_approx(if(`居室`='3+',adr,null),0.5) as 3_bed_mid_adr

        ,sum(if(adr<`酒店成单20分位价`,order_room_night_count,null)) as `酒店同城成单价格20分位以下间夜`
        ,sum(if(adr<`酒店成单中位价`,order_room_night_count,null)) as `酒店同城成单价格50分位以下间夜`
    ------居室
        ,sum(if(`居室`=1,order_room_night_count,null)) as 1_bed_nights
        ,sum(if(`居室`=2,order_room_night_count,null)) as 2_bed_nights
        ,sum(if(`居室`='3+',order_room_night_count,null)) as 3_bed_nights
------gmv
    ,sum(room_total_amount) as total_gmv_k
-----库存
    ,sum(if(instance_count=1,room_total_amount,null)) as dankucun_gmv
    ,sum(if(instance_count>10,room_total_amount,null)) as 10kucun_gmv
-----生态
    ,sum(if(house_class='L4',room_total_amount,null)) as L4_gmv
    ,sum(if(house_class='L3',room_total_amount,null)) as L3_gmv
    ,sum(if(house_class in ('L25','L3','L4'),room_total_amount,null)) as L25plus_gmv
    ,sum(if(house_class in ('L21','L25','L3','L4'),room_total_amount,null)) as L21up_gmv
    ,sum(if(d.house_id is not null,room_total_amount,null)) as baozang_gmv
    ,sum(if(e.house_id is not null,room_total_amount,null)) as real_video_gmv
    ,sum(if(f.house_id is not null,room_total_amount,null)) as youxuan_gmv
    ,sum(if(g.house_id is not null,room_total_amount,null)) as tese_gmv
    ,sum(if(landlord_channel='直采',room_total_amount,null)) as zhicai_gmv
    ,sum(if(landlord_channel='接入',room_total_amount,null)) as jieru_gmv
------评论
    ,sum(if(comment_score<4,room_total_amount,null)) as commentscore_low_4_gmv
    ,sum(if(comment_score<4.5,room_total_amount,null)) as commentscore_low_45_gmv
    ,sum(if(valid_comment_num=0 or valid_comment_num is null,room_total_amount,null)) as no_comment_gmv
    -- ,sum(if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),room_total_amount,null)) as top10_nocomment_or_commentscore_low4_gmv
    -- ,sum(if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),room_total_amount,null)) as top3_nocomment_or_commentscore_low4_gmv
------价格
    ,sum(if(adr<100,room_total_amount,null)) as low_100_gmv
    ,sum(if(adr<200,room_total_amount,null)) as low_200_gmv
    ,sum(if(adr>3000,room_total_amount,null)) as high_3000_gmv

    ,sum(if(adr<`酒店成单20分位价`,room_total_amount,null)) as `酒店同城成单价格20分位以下gmv`
    ,sum(if(adr<`酒店成单中位价`,room_total_amount,null)) as `酒店同城成单价格50分位以下gmv`
------居室
    ,sum(if(`居室`=1,room_total_amount,null)) as 1_bed_gmv
    ,sum(if(`居室`=2,room_total_amount,null)) as 2_bed_gmv
    ,sum(if(`居室`='3+',room_total_amount,null)) as 3_bed_gmv
--------连住天数
        ,sum(if(`连住天数`=1,order_room_night_count,null))      as continue_1_nights     
        ,sum(if(`连住天数`='2_3',order_room_night_count,null))  as continue_3_nights     
        ,sum(if(`连住天数`='4_5',order_room_night_count,null))  as continue_5_nights     
        ,sum(if(`连住天数`='6_7',order_room_night_count,null))  as continue_7_nights     
        ,sum(if(`连住天数`='8_10',order_room_night_count,null)) as continue_10_nights    
        ,sum(if(`连住天数`='10+',order_room_night_count,null))  as continue_10plus_nights

        ,sum(if(`连住天数`=1,room_total_amount,null)) as continue_1_gmv
        ,sum(if(`连住天数`='2_3',room_total_amount,null)) as continue_3_gmv
        ,sum(if(`连住天数`='4_5',room_total_amount,null)) as continue_5_gmv
        ,sum(if(`连住天数`='6_7',room_total_amount,null)) as continue_7_gmv
        ,sum(if(`连住天数`='8_10',room_total_amount,null)) as continue_10_gmv
        ,sum(if(`连住天数`='10+',room_total_amount,null)) as continue_10plus_gmv
    from ord a
    left join house_info b
    on a.dt = b.dt
    and a.house_id = b.house_id
    left join hotel_type_info c
    on a.dt = c.dt
    and a.hotel_id = c.hotelid
    left join baozang_info d
    on a.dt = d.dt
    and a.house_id = d.house_id
    left join real_video_info e
    on a.dt = e.dt
    and a.house_id = e.house_id
    left join youxuan_info f
    on a.dt = f.dt
    and a.house_id = f.house_id
    left join tese_info g
    on a.dt = g.dt
    and a.house_id = g.house_id
    left join hotel_adr_info k
    on a.week_time = k.week_time
    group by 1
)
,inventory_info as(
select
    case when dt between date_sub('${partition}',7) and date_sub('${partition}',1) then concat(date_sub('${partition}',7),'_',date_sub('${partition}',1))
            end as week_time
    ,avg(dankucun_keshoulv) as dankucun_keshoulv
    ,avg(10kucun_keshoulv)  as 10kucun_keshoulv
    ,avg(L4_keshoulv)       as L4_keshoulv
    ,avg(L3_keshoulv)       as L3_keshoulv
    ,avg(L25plus_keshoulv)  as L25plus_keshoulv
    ,avg(L21up_keshoulv) as L21up_keshoulv
    ,avg(baozang_keshoulv)  as baozang_keshoulv
    ,avg(real_video_keshoulv) as real_video_keshoulv
    ,avg(youxuan_keshoulv) as youxuan_keshoulv
    ,avg(tese_keshoulv) as tese_keshoulv
    ,avg(zhicai_keshoulv) as zhicai_keshoulv
    ,avg(jieru_keshoulv) as jieru_keshoulv
    ,avg(commentscore_low_4_keshoulv) as commentscore_low_4_keshoulv
    ,avg(commentscore_low_45_keshoulv) as commentscore_low_45_keshoulv
    ,avg(no_comment_keshoulv) as no_comment_keshoulv
    ,avg(low_100_keshoulv) as low_100_keshoulv
    ,avg(low_200_keshoulv) as low_200_keshoulv
    ,avg(high_3000_keshoulv) as high_3000_keshoulv
    ,avg(1_bed_keshoulv) as 1_bed_keshoulv
    ,avg(2_bed_keshoulv) as 2_bed_keshoulv
    ,avg(3_bed_keshoulv) as 3_bed_keshoulv
from
(
    select
        dt
        ,dankucun_hs_cnt_keshou/dankucun_hs_cnt as dankucun_keshoulv
        ,10kucun_hs_cnt_keshou/10kucun_hs_cnt as 10kucun_keshoulv
        ,L4_hs_cnt_keshou/L4_hs_cnt as L4_keshoulv
        ,L3_hs_cnt_keshou/L3_hs_cnt as L3_keshoulv
        ,L25plus_hs_cnt_keshou/L25plus_hs_cnt as L25plus_keshoulv
        ,L21up_hs_cnt_keshou/L21up_hs_cnt as L21up_keshoulv
        ,baozang_hs_cnt_keshou/baozang_hs_cnt as baozang_keshoulv
        ,real_video_hs_cnt_keshou/real_video_hs_cnt as real_video_keshoulv
        ,youxuan_hs_cnt_keshou/youxuan_hs_cnt as youxuan_keshoulv
        ,tese_hs_cnt_keshou/tese_hs_cnt as tese_keshoulv
        ,zhicai_hs_cnt_keshou/zhicai_hs_cnt as zhicai_keshoulv
        ,jieru_hs_cnt_keshou/jieru_hs_cnt as jieru_keshoulv
        ,commentscore_low_4_hs_cnt_keshou/commentscore_low_4_hs_cnt as commentscore_low_4_keshoulv
        ,commentscore_low_45_hs_cnt_keshou/commentscore_low_45_hs_cnt as commentscore_low_45_keshoulv
        ,no_comment_hs_cnt_keshou/no_comment_hs_cnt as no_comment_keshoulv
        ,low_100_hs_cnt_keshou/low_100_hs_cnt as low_100_keshoulv
        ,low_200_hs_cnt_keshou/low_200_hs_cnt as low_200_keshoulv
        ,high_3000_hs_cnt_keshou/high_3000_hs_cnt as high_3000_keshoulv
        ,1_bed_hs_cnt_keshou/1_bed_hs_cnt as 1_bed_keshoulv
        ,2_bed_hs_cnt_keshou/2_bed_hs_cnt as 2_bed_keshoulv
        ,3_bed_hs_cnt_keshou/3_bed_hs_cnt as 3_bed_keshoulv
    FROM
    (
        select 
            a.dt
        ------------- 总数
        -----库存
            ,count(if(instance_count=1,a.house_id,null)) as dankucun_hs_cnt
            ,count(if(instance_count>10,a.house_id,null)) as 10kucun_hs_cnt
        -----生态
            ,count(if(house_class='L4',a.house_id,null)) as L4_hs_cnt
            ,count(if(house_class='L3',a.house_id,null)) as L3_hs_cnt
            ,count(if(house_class in ('L25','L3','L4'),a.house_id,null)) as L25plus_hs_cnt
            ,count(if(house_class in ('L21','L25','L3','L4'),a.house_id,null)) as L21up_hs_cnt
            ,count(if(d.house_id is not null,a.house_id,null)) as baozang_hs_cnt
            ,count(if(e.house_id is not null,a.house_id,null)) as real_video_hs_cnt
            ,count(if(f.house_id is not null,a.house_id,null)) as youxuan_hs_cnt
            ,count(if(g.house_id is not null,a.house_id,null)) as tese_hs_cnt
            ,count(if(landlord_channel='直采',a.house_id,null)) as zhicai_hs_cnt
            ,count(if(landlord_channel='接入',a.house_id,null)) as jieru_hs_cnt
        ------评论
            ,count(if(comment_score<4,a.house_id,null)) as commentscore_low_4_hs_cnt
            ,count(if(comment_score<4.5,a.house_id,null)) as commentscore_low_45_hs_cnt
            ,count(if(valid_comment_num=0 or valid_comment_num is null,a.house_id,null)) as no_comment_hs_cnt
            -- ,count(distinct if(position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),house_id,null)) as top10_nocomment_or_commentscore_low4_hs_cnt
            -- ,count(distinct if(position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),house_id,null)) as top3_nocomment_or_commentscore_low4_hs_cnt
        ------价格
            ,count(if(`报价中位`<100,a.house_id,null)) as low_100_hs_cnt
            ,count(if(`报价中位`<200,a.house_id,null)) as low_200_hs_cnt
            ,count(if(`报价中位`>3000,a.house_id,null)) as high_3000_hs_cnt
        ------居室
            ,count(if(`居室`=1,a.house_id,null)) as 1_bed_hs_cnt
            ,count(if(`居室`=2,a.house_id,null)) as 2_bed_hs_cnt
            ,count(if(`居室`='3+',a.house_id,null)) as 3_bed_hs_cnt

        ------------- 可售数
        -----库存
            ,count(if(avaliablecount>0 and instance_count=1,a.house_id,null)) as dankucun_hs_cnt_keshou
            ,count(if(avaliablecount>0 and instance_count>10,a.house_id,null)) as 10kucun_hs_cnt_keshou
        -----生态
            ,count(if(avaliablecount>0 and house_class='L4',a.house_id,null)) as L4_hs_cnt_keshou
            ,count(if(avaliablecount>0 and house_class='L3',a.house_id,null)) as L3_hs_cnt_keshou
            ,count(if(avaliablecount>0 and house_class in ('L25','L3','L4'),a.house_id,null)) as L25plus_hs_cnt_keshou
            ,count(if(avaliablecount>0 and house_class in ('L21','L25','L3','L4'),a.house_id,null)) as L21up_hs_cnt_keshou
            ,count(if(avaliablecount>0 and d.house_id is not null,a.house_id,null)) as baozang_hs_cnt_keshou
            ,count(if(avaliablecount>0 and e.house_id is not null,a.house_id,null)) as real_video_hs_cnt_keshou
            ,count(if(avaliablecount>0 and f.house_id is not null,a.house_id,null)) as youxuan_hs_cnt_keshou
            ,count(if(avaliablecount>0 and g.house_id is not null,a.house_id,null)) as tese_hs_cnt_keshou
            ,count(if(avaliablecount>0 and landlord_channel='直采',a.house_id,null)) as zhicai_hs_cnt_keshou
            ,count(if(avaliablecount>0 and landlord_channel='接入',a.house_id,null)) as jieru_hs_cnt_keshou
        ------评论
            ,count(if(avaliablecount>0 and comment_score<4,a.house_id,null)) as commentscore_low_4_hs_cnt_keshou
            ,count(if(avaliablecount>0 and comment_score<4.5,a.house_id,null)) as commentscore_low_45_hs_cnt_keshou
            ,count(if(avaliablecount>0 and valid_comment_num=0 or valid_comment_num is null,a.house_id,null)) as no_comment_hs_cnt_keshou
            -- ,count(distinct if(avaliablecount>0 and position<=10 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),house_id,null)) as top10_nocomment_or_commentscore_low4_hs_cnt
            -- ,count(distinct if(avaliablecount>0 and position<=3 and ((valid_comment_num=0 or valid_comment_num is null) or (comment_score<4)),house_id,null)) as top3_nocomment_or_commentscore_low4_hs_cnt
        ------价格
            ,count(if(avaliablecount>0 and `报价中位`<100,a.house_id,null)) as low_100_hs_cnt_keshou
            ,count(if(avaliablecount>0 and `报价中位`<200,a.house_id,null)) as low_200_hs_cnt_keshou
            ,count(if(avaliablecount>0 and `报价中位`>3000,a.house_id,null)) as high_3000_hs_cnt_keshou
        ------居室
            ,count(if(avaliablecount>0 and `居室`=1,a.house_id,null)) as 1_bed_hs_cnt_keshou
            ,count(if(avaliablecount>0 and `居室`=2,a.house_id,null)) as 2_bed_hs_cnt_keshou
            ,count(if(avaliablecount>0 and `居室`='3+',a.house_id,null)) as 3_bed_hs_cnt_keshou
        from
        (
            select
                a.house_id
                ,hotel_id
                ,a.dt
                ,case when landlord_channel = '直采' then a.avaliablecount 
                      when landlord_channel = '接入' and c.house_id is not null then c.inventory 
                      when landlord_channel = '接入' and c.house_id is null then 0
                      end as avaliablecount
            from
                (
                    select
                        unitid as house_id
                        ,createdate as dt
                        ,coalesce(sum(instancecount),0) as instancecount  --物理库存量
                        ,coalesce(sum(inventorycount),0) as inventorycount  --给T库存量
                        ,coalesce(sum(avaliablecount),0) as avaliablecount  --可售库存量
                        ,coalesce(sum(unavaliablecount),0) as unavaliablecount --不可售库存量
                    from dim_tujiaproduct.unit_inventory_log
                    where 
                        createdate between date_sub('${partition}',7) and date_sub('${partition}',1)
                    and datediff(inventorydate,createdate)=0 -- T0
                    and substr(gettime,9,2) in ('10')--每天10点
                    and inventorycount is not null
                    and avaliablecount is not null
                    and unavaliablecount is not null
                    group by 1,2
                )a
            left join 
                (
                    select distinct 
                        house_id
                        ,dt
                        ,coalesce(sum(inventory),0) as inventory
                    from dwd.dwd_house_daily_price_d
                    where dt between date_sub('${partition}',7) and date_sub('${partition}',1)
                    and dt = checkin_date
                    group by 1,2
                )c on a.dt = c.dt and a.house_id = c.house_id
            join 
                (
                    select distinct 
                        house_id
                        ,hotel_id
                        ,landlord_channel
                    from house_info
                    where dt = date_sub('${partition}',1)
                )b on a.house_id = b.house_id
        )a
        join house_info b
        on a.dt = b.dt 
        and a.house_id = b.house_id
        left join hotel_type_info c
        on a.dt = c.dt
        and a.hotel_id = c.hotelid
        left join baozang_info d
        on a.dt = d.dt
        and a.house_id = d.house_id
        left join real_video_info e
        on a.dt = e.dt
        and a.house_id = e.house_id
        left join youxuan_info f
        on a.dt = f.dt
        and a.house_id = f.house_id
        left join tese_info g
        on a.dt = g.dt
        and a.house_id = g.house_id
        left join baojia_info h
        on a.dt = h.dt
        and a.house_id = h.house_id
        group by 1
    )a
)b
group by 1
)


select
    a.week_time
    ,a.list_pv
    ,a.canSalePercentOfL34
    ,a.canSalePercentOfL2534
    ,c.L4_hs_cnt -- as `L4在线房源数`
    ,L4_keshoulv    --          as `L4可售率`
    ,a.L4_pv/a.list_pv            as L4_pv_rate -- as `L4pv占比`
    ,b.L4_nights/b.total_nights_k as L4_nights_rate -- as `L4间夜占比`
    ,b.L4_gmv/b.total_gmv_k       as L4_gmv_rate-- as `L4gmv占比`
    ,c.L3_hs_cnt -- as `L3在线房源数`
    ,L3_keshoulv              -- as `L3可售率`
    ,a.L3_pv/a.list_pv            as L3_pv_rate -- as `L3pv占比`
    ,b.L3_nights/b.total_nights_k as L3_nights_rate  -- as `L3间夜占比`
    ,b.L3_gmv/b.total_gmv_k       as L3_gmv_rate  -- as `L3gmv占比`
    ,c.L25plus_hs_cnt -- as `L25+在线房源数`
    ,L25plus_keshoulv         -- as `L25+可售率`
    ,a.L25plus_pv/a.list_pv            as L25plus_pv_rate -- as `L25+pv占比`
    ,b.L25plus_nights/b.total_nights_k as L25plus_nights_rate -- as `L25+间夜占比`
    ,b.L25plus_gmv/b.total_gmv_k       as L25plus_gmv_rate -- as `L25+gmv占比`
    ,c.L21up_hs_cnt -- as `L21+在线房源数`
    ,L21up_keshoulv -- as `L21+可售率`
    ,a.L21up_pv/a.list_pv            as L21up_pv_rate-- as `L21+pv占比`
    ,b.L21up_nights/b.total_nights_k as L21up_nights_rate -- as `L21+间夜占比`
    ,b.L21up_gmv/b.total_gmv_k       as L21up_gmv_rate -- as `L21+gmv占比`
    ,c.baozang_hs_cnt -- as `宝藏在线房源数`
    ,baozang_keshoulv         -- as `宝藏可售率`
    ,a.baozang_pv/a.list_pv            as baozang_pv_rate-- as `宝藏pv占比`
    ,b.baozang_nights/b.total_nights_k as baozang_nights_rate -- as `宝藏间夜占比`
    ,b.baozang_gmv/b.total_gmv_k       as baozang_gmv_rate -- as `宝藏gmv占比`
    ,c.real_video_hs_cnt -- as `真视频在线房源数`
    ,real_video_keshoulv      -- as `真视频可售率`
    ,a.real_video_pv/a.list_pv            as real_video_pv_rate -- as `真视频pv占比`
    ,b.real_video_nights/b.total_nights_k as real_video_nights_rate -- as `真视频间夜占比`
    ,b.real_video_gmv/b.total_gmv_k       as real_video_gmv_rate -- as `真视频gmv占比`
    ,c.youxuan_hs_cnt -- as `优选在线房源数`
    ,youxuan_keshoulv -- as `优选可售率`
    ,a.youxuan_pv/a.list_pv            as youxuan_pv_rate -- as `优选pv占比`
    ,b.youxuan_nights/b.total_nights_k as youxuan_nights_rate -- as `优选间夜占比`
    ,b.youxuan_gmv/b.total_gmv_k       as youxuan_gmv_rate -- as `优选gmv占比`
    ,c.tese_hs_cnt -- as `特色在线房源数`
    ,tese_keshoulv -- as `特色可售率`
    ,a.tese_pv/a.list_pv            as tese_pv_rate -- as `特色pv占比`
    ,b.tese_nights/b.total_nights_k as tese_nights_rate -- as `特色间夜占比`
    ,b.tese_gmv/b.total_gmv_k       as tese_gmv_rate  -- as `特色gmv占比`
    ,c.zhicai_hs_cnt -- as `直采在线房源数`
    ,zhicai_keshoulv -- as `直采可售率`
    ,a.zhicai_pv/a.list_pv            as zhicai_pv_rate -- as `直采pv占比`
    ,b.zhicai_nights/b.total_nights_k as zhicai_nights_rate -- as `直采间夜占比`
    ,b.zhicai_gmv/b.total_gmv_k       as zhicai_gmv_rate -- as `直采gmv占比`
    ,c.jieru_hs_cnt -- as `接入在线房源数`
    ,jieru_keshoulv -- as `接入可售率`
    ,a.jieru_pv/a.list_pv            as jieru_pv_rate  -- as `接入pv占比`
    ,b.jieru_nights/b.total_nights_k as jieru_nights_rate  -- as `接入间夜占比`
    ,b.jieru_gmv/b.total_gmv_k       as jieru_gmv_rate -- as `接入gmv占比`
    ,c.dankucun_hs_cnt -- as `单库存在线房源数`
    ,dankucun_keshoulv        -- as `单库存可售率`
    ,a.dankucun_pv/a.list_pv            as dankucun_pv_rate  -- as `单库存pv占比`
    ,b.dankucun_nights/b.total_nights_k as dankucun_nights_rate -- as `单库存间夜占比`
    ,b.dankucun_gmv/b.total_gmv_k       as dankucun_gmv_rate  -- as `单库存gmv占比`
    ,c.10kucun_hs_cnt -- as `大库存在线房源数`
    ,10kucun_keshoulv         -- as `大库存可售率`
    ,a.10kucun_pv/a.list_pv             as 10kucun_pv_rate -- as `大库存pv占比`
    ,b.10kucun_nights/b.total_nights_k  as 10kucun_nights_rate -- as `大库存间夜占比`
    ,b.10kucun_gmv/b.total_gmv_k        as 10kucun_gmv_rate -- as `大库存gmv占比`
    ,c.commentscore_low_4_hs_cnt -- as `小于4分在线房源数`
    ,commentscore_low_4_keshoulv -- as `小于4分可售率`
    ,a.commentscore_low_4_pv/a.list_pv             as commentscore_low_4_pv_rate -- as `小于4分pv占比`
    ,b.commentscore_low_4_nights/b.total_nights_k  as commentscore_low_4_nights_rate -- as `小于4分间夜占比`
    ,b.commentscore_low_4_gmv/b.total_gmv_k        as commentscore_low_4_gmv_rate   -- as `小于4分gmv占比`
    ,c.commentscore_low_45_hs_cnt -- as `小于4点5分在线房源数`
    ,commentscore_low_45_keshoulv -- as `小于4点5分可售率`
    ,a.commentscore_low_45_pv/a.list_pv             as commentscore_low_45_pv_rate -- as `小于4点5分pv占比`
    ,b.commentscore_low_45_nights/b.total_nights_k  as commentscore_low_45_nights_rate -- as `小于4点5分间夜占比`
    ,b.commentscore_low_45_gmv/b.total_gmv_k        as commentscore_low_45_gmv_rate-- as `小于4点5分gmv占比`
    ,c.no_comment_hs_cnt -- as `无评论在线房源数`
    ,no_comment_keshoulv -- as `无评论可售率`
    ,a.no_comment_pv/a.list_pv            as no_comment_pv_rate -- as `无评论pv占比`
    ,b.no_comment_nights/b.total_nights_k as no_comment_nights_rate -- as `无评论间夜占比`
    ,b.no_comment_gmv/b.total_gmv_k       as no_comment_gmv_rate -- as `无评论gmv占比`
    ,a.top10_nocomment_or_commentscore_low4_pv/a.list_pv            as top10_nocomment_or_commentscore_low4_pv_rate -- as `TOP10无评论或评分小于4分pv占比`
    ,a.top10_nocomment_or_commentscore_low4_nights/a.total_nights_z as top10_nocomment_or_commentscore_low4_nights_rate  -- as `TOP10无评论或评分小于4分间夜占比`
    ,a.top10_nocomment_or_commentscore_low4_gmv/a.total_gmv_z       as top10_nocomment_or_commentscore_low4_gmv_rate -- as `TOP10无评论或评分小于4分gmv占比`
    ,a.top3_nocomment_or_commentscore_low4_pv/a.list_pv             as top3_nocomment_or_commentscore_low4_pv_rate -- as `TOP3无评论或评分小于4分pv占比`
    ,a.top3_nocomment_or_commentscore_low4_nights/a.total_nights_z  as top3_nocomment_or_commentscore_low4_nights_rate -- as `TOP3无评论或评分小于4分间夜占比`
    ,a.top3_nocomment_or_commentscore_low4_gmv/a.total_gmv_z        as top3_nocomment_or_commentscore_low4_gmv_rate -- as `TOP3无评论或评分小于4分gmv占比`
    ,c.low_100_hs_cnt -- as `低于100在线房源数`
    ,low_100_keshoulv -- as `低于100可售率`
    ,a.low_100_pv/a.list_pv              as low_100_pv_rate     -- as `低于100pv占比`
    ,b.low_100_nights/b.total_nights_k   as low_100_nights_rate -- as `低于100间夜占比`
    ,b.low_100_gmv/b.total_gmv_k         as low_100_gmv_rate    -- as `低于100gmv占比`
    ,c.low_200_hs_cnt -- as `低于200在线房源数`
    ,low_200_keshoulv -- as `低于200可售率`
    ,a.low_200_pv/a.list_pv             as low_200_pv_rate     -- as `低于200pv占比`
    ,b.low_200_nights/b.total_nights_k  as low_200_nights_rate -- as `低于200间夜占比`
    ,b.low_200_gmv/b.total_gmv_k        as low_200_gmv_rate    -- as `低于200gmv占比`
    ,c.high_3000_hs_cnt -- as `高于3000在线房源数`
    ,high_3000_keshoulv -- as `高于3000可售率`
    ,a.high_3000_pv/a.list_pv              as high_3000_pv_rate     -- as `高于3000pv占比`
    ,b.high_3000_nights/b.total_nights_k   as high_3000_nights_rate -- as `高于3000间夜占比`
    ,b.high_3000_gmv/b.total_gmv_k         as high_3000_gmv_rate    -- as `高于3000gmv占比`
    ,a.avg_price -- as `曝光均价`
    ,b.avg_adr -- as `成单均价`
    ,a.1_bed_mid_price -- as `一居曝光中位价`
    ,b.1_bed_mid_adr -- as `一居成单中位价`
    ,a.2_bed_mid_price -- as `二居曝光中位价`
    ,b.2_bed_mid_adr -- as `二居成单中位价`
    ,a.3_bed_mid_price -- as `三居以上曝光中位价`
    ,b.3_bed_mid_adr -- as `三居以上成单中位价`
    ,c.1_bed_hs_cnt -- as `一居在线房源数`
    ,1_bed_keshoulv -- as `一居可售率`
    ,a.1_bed_pv/a.list_pv              as 1_bed_pv_rate     -- as `一居pv占比`
    ,b.1_bed_nights/b.total_nights_k   as 1_bed_nights_rate -- as `一居间夜占比`
    ,b.1_bed_gmv/b.total_gmv_k         as 1_bed_gmv_rate    -- as `一居gmv占比`
    ,c.2_bed_hs_cnt -- as `二居在线房源数`
    ,2_bed_keshoulv -- as `二居可售率`
    ,a.2_bed_pv/a.list_pv              as 2_bed_pv_rate     -- as `二居pv占比`
    ,b.2_bed_nights/b.total_nights_k   as 2_bed_nights_rate -- as `二居间夜占比`
    ,b.2_bed_gmv/b.total_gmv_k         as 2_bed_gmv_rate    -- as `二居gmv占比`
    ,c.3_bed_hs_cnt -- as `三居及以上在线房源数`
    ,3_bed_keshoulv -- as `三居及以上可售率`
    ,a.3_bed_pv/a.list_pv              as 3_bed_pv_rate      -- as `三居及以上pv占比`
    ,b.3_bed_nights/b.total_nights_k   as 3_bed_nights_rate  -- as `三居及以上间夜占比`
    ,b.3_bed_gmv/b.total_gmv_k         as 3_bed_gmv_rate     -- as `三居及以上gmv占比`
    ,`酒店成单20分位价`                                 as hotel_adr_20p
    ,`酒店同城成单价格20分位以下pv`/a.list_pv            as hotel_adr_20p_low_pv_rate       -- as `酒店同城成单价格20分位以下pv占比`
    ,`酒店同城成单价格20分位以下间夜`/b.total_nights_k   as hotel_adr_20p_low_nights_rate  -- as `酒店同城成单价格20分位以下间夜占比`
    ,`酒店同城成单价格20分位以下gmv`/b.total_gmv_k       as hotel_adr_20p_low_gmv_rate      -- as `酒店同城成单价格20分位以下gmv占比`
    ,`酒店成单中位价`                                   as hotel_adr_50p
    ,`酒店同城成单价格50分位以下pv`/a.list_pv            as hotel_adr_50p_low_pv_rate     -- as `酒店同城成单价格50分位以下pv占比`
    ,`酒店同城成单价格50分位以下间夜`/b.total_nights_k   as hotel_adr_50p_low_nights_rate  -- as `酒店同城成单价格50分位以下间夜占比`
    ,`酒店同城成单价格50分位以下gmv`/b.total_gmv_k       as hotel_adr_50p_low_gmv_rate    -- as `酒店同城成单价格50分位以下gmv占比`
    ,a.locating_low_1000m_pv/a.locating_pv             as locating_low_1000m_pv_rate     -- as `身边1000米内pv占比`
    ,a.locating_low_1000m_nights/a.locating_nights     as locating_low_1000m_nights_rate -- as `身边1000米内间夜占比`
    ,a.locating_low_1000m_gmv/a.locating_gmv           as locating_low_1000m_gmv_rate    -- as `身边1000米内gmv占比`
    ,a.locating_low_2000m_pv/a.locating_pv             as locating_low_2000m_pv_rate      -- as `身边2000米内pv占比`
    ,a.locating_low_2000m_nights/a.locating_nights     as locating_low_2000m_nights_rate  -- as `身边2000米内间夜占比`
    ,a.locating_low_2000m_gmv/a.locating_gmv           as locating_low_2000m_gmv_rate     -- as `身边2000米内gmv占比`
    ,a.locating_high_3000m_pv/a.locating_pv            as locating_high_3000m_pv_rate      -- as `身边3000米上pv占比`
    ,a.locating_high_3000m_nights/a.locating_nights    as locating_high_3000m_nights_rate  -- as `身边3000米上间夜占比`
    ,a.locating_high_3000m_gmv/a.locating_gmv          as locating_high_3000m_gmv_rate     -- as `身边3000米上gmv占比`
    ,a.land_high_3000m_pv/a.land_pv                    as land_high_3000m_pv_rate        -- as `地标大于3kmpv占比`
    ,a.land_high_3000m_nights/a.land_nights            as land_high_3000m_nights_rate    -- as `地标大于3km间夜占比`
    ,a.land_high_3000m_gmv/a.land_gmv                  as land_high_3000m_gmv_rate       -- as `地标大于3kmgmv占比`
    ,a.jiangquanjuli_pv/a.land_pv                      as jiangquanjuli_pv_rate          -- as `poi降权距离pv占比`
    ,a.jiangquanjuli_nights/a.land_nights              as jiangquanjuli_nights_rate      -- as `poi降权距离间夜占比`
    ,a.jiangquanjuli_gmv/a.land_gmv                    as jiangquanjuli_gmv_rate         -- as `poi降权距离gmv占比`

    ,a.continue_1_pv/a.list_pv       as continue_1_pv_rate
    ,a.continue_3_pv/a.list_pv       as continue_3_pv_rate
    ,a.continue_5_pv/a.list_pv       as continue_5_pv_rate
    ,a.continue_7_pv/a.list_pv       as continue_7_pv_rate
    ,a.continue_10_pv/a.list_pv      as continue_10_pv_rate
    ,a.continue_10plus_pv/a.list_pv  as continue_10plus_pv_rate

    ,b.continue_1_nights/b.total_nights_k       as continue_1_nights_rate
    ,b.continue_3_nights/b.total_nights_k       as continue_3_nights_rate
    ,b.continue_5_nights/b.total_nights_k       as continue_5_nights_rate
    ,b.continue_7_nights/b.total_nights_k       as continue_7_nights_rate
    ,b.continue_10_nights/b.total_nights_k      as continue_10_nights_rate
    ,b.continue_10plus_nights/b.total_nights_k  as continue_10plus_nights_rate

    ,b.continue_1_gmv/b.total_gmv_k       as continue_1_gmv_rate
    ,b.continue_3_gmv/b.total_gmv_k       as continue_3_gmv_rate
    ,b.continue_5_gmv/b.total_gmv_k       as continue_5_gmv_rate
    ,b.continue_7_gmv/b.total_gmv_k       as continue_7_gmv_rate
    ,b.continue_10_gmv/b.total_gmv_k      as continue_10_gmv_rate
    ,b.continue_10plus_gmv/b.total_gmv_k  as continue_10plus_gmv_rate

    ,a.total_nights_z
    ,a.total_gmv_z
    ,a.L4_pv
    ,a.L3_pv
    ,a.L25plus_pv
    ,a.L21up_pv
    ,a.baozang_pv
    ,a.real_video_pv
    ,a.youxuan_pv
    ,a.tese_pv
    ,a.zhicai_pv
    ,a.jieru_pv
    ,a.commentscore_low_4_pv
    ,a.commentscore_low_45_pv
    ,a.no_comment_pv
    ,a.top10_nocomment_or_commentscore_low4_pv
    ,a.top3_nocomment_or_commentscore_low4_pv
    ,a.top10_nocomment_or_commentscore_low4_nights
    ,a.top3_nocomment_or_commentscore_low4_nights
    ,a.top10_nocomment_or_commentscore_low4_gmv
    ,a.top3_nocomment_or_commentscore_low4_gmv
    ,a.low_100_pv
    ,a.low_200_pv
    ,a.high_3000_pv
    ,a.1_bed_pv
    ,a.2_bed_pv
    ,a.3_bed_pv

    ,a.continue_1_pv     
    ,a.continue_3_pv     
    ,a.continue_5_pv     
    ,a.continue_7_pv     
    ,a.continue_10_pv    
    ,a.continue_10plus_pv

    ,a.locating_pv
    ,a.land_pv
    ,a.locating_low_1000m_pv
    ,a.locating_low_2000m_pv
    ,a.locating_high_3000m_pv
    ,a.land_high_3000m_pv
    ,a.jiangquanjuli_pv

    ,a.locating_nights
    ,a.land_nights
    ,a.locating_low_1000m_nights
    ,a.locating_low_2000m_nights
    ,a.locating_high_3000m_nights
    ,a.land_high_3000m_nights
    ,a.jiangquanjuli_nights

    ,a.locating_gmv
    ,a.land_gmv
    ,a.locating_low_1000m_gmv
    ,a.locating_low_2000m_gmv
    ,a.locating_high_3000m_gmv
    ,a.land_high_3000m_gmv
    ,a.jiangquanjuli_gmv

    ,b.total_nights_k
    ,b.L4_nights
    ,b.L3_nights
    ,b.L25plus_nights
    ,b.L21up_nights
    ,b.baozang_nights
    ,b.real_video_nights
    ,b.youxuan_nights
    ,b.tese_nights
    ,b.zhicai_nights
    ,b.jieru_nights
    ,b.commentscore_low_4_nights
    ,b.commentscore_low_45_nights
    ,b.no_comment_nights
    ,b.low_100_nights
    ,b.low_200_nights
    ,b.high_3000_nights
    ,b.1_bed_nights
    ,b.2_bed_nights
    ,b.3_bed_nights

    ,b.continue_1_nights     
    ,b.continue_3_nights     
    ,b.continue_5_nights     
    ,b.continue_7_nights     
    ,b.continue_10_nights    
    ,b.continue_10plus_nights

    ,b.total_gmv_k
    ,b.L4_gmv
    ,b.L3_gmv
    ,b.L25plus_gmv
    ,b.L21up_gmv
    ,b.baozang_gmv
    ,b.real_video_gmv
    ,b.youxuan_gmv
    ,b.tese_gmv
    ,b.zhicai_gmv
    ,b.jieru_gmv
    ,b.commentscore_low_4_gmv
    ,b.commentscore_low_45_gmv
    ,b.no_comment_gmv
    ,b.low_100_gmv
    ,b.low_200_gmv
    ,b.high_3000_gmv
    ,b.1_bed_gmv
    ,b.2_bed_gmv
    ,b.3_bed_gmv
    
    ,b.continue_1_gmv     
    ,b.continue_3_gmv     
    ,b.continue_5_gmv     
    ,b.continue_7_gmv     
    ,b.continue_10_gmv   
    ,b.continue_10plus_gmv
    ,date_sub('${partition}',1) as dt
from
pv_info a
left join ord_info b
on a.week_time = b.week_time
left join house_cnt_info c
on a.week_time = c.week_time
left join inventory_info d
on a.week_time = d.week_time
left join hotel_adr_info e
on a.week_time = e.week_time