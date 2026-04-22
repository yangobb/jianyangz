with
house_info as (
    select
        dt,
        hotel_id,
        house_city_id,
        case when house_city_name = '陵水' then '陵水(三亚)' else house_city_name end as house_city_name,
        house_id,
        dynamic_business,
        dynamic_business_id,
        house_class,
        case when is_prefer_pro=1 then is_prefer_pro else landlord_shoot_tag end as is_prefer_pro,
        level2_area_id+10000 as level2_area_id,
        level2_area_name,
        CASE 
            WHEN bedroom_count = 1
                OR share_type = '单间'
            THEN '一居'
            WHEN bedroom_count = 2
                AND share_type = '整租'
            THEN '二居'
            WHEN bedroom_count >= 3
                AND share_type = '整租'
            THEN '三居+'
            ELSE '其他' end as `居室`,
        case when hotel_level='4' then '超赞房东'
                when hotel_level='3' then '人气房东'
                when hotel_level='2' then '成长房东'
                when hotel_level='1' then '潜力房东'
                else '其他' end as `房东等级`,
        case -- when landlord_channel=303 then 'C接' --携程接入
            when landlord_channel=1 then '直采'
            else '接入' end as landlord_channel,
        instance_count --物理库存(房屋实例数)
        ,CASE WHEN is_prefer=1   THEN '优选'
        ELSE '其他' END AS `是否优选`
        ,CASE WHEN is_prefer_pro=1   THEN '严选' 
        ELSE '其他' END AS `是否严选`
        ,CASE WHEN great_tag=1   THEN '臻选'
        ELSE '其他' END AS `是否臻选`
        ,house_type
    FROM dws.dws_house_d
    WHERE dt between date_sub(current_date,14) and date_sub(current_date,1)
        AND house_is_online = 1
        AND house_is_oversea = '0'
)
,hotel_type_info as(
    select distinct
        dt
        ,hotelid
        ,leveldesc
    from pdb_analysis_c.dwd_landlord_subindex_score_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1)
)
,list as 
(select 
      dt
      ,wrapper_name
      ,search_city_name
      ,case when search_type = '景区地区' then city_id
            when search_type='县级市' then city_id
            else geo_city_id end as city_id
      ,case when search_type = '景区地区' then city_name
            when search_type='县级市' then city_name
            else geo_city_name end as city_name
      ,case when search_type = '景区地区' then area_id
            when search_type='县级市' and search_city_id>0 then search_city_id+10000
            when search_type='县级市' and search_city_id=0 then cast(aa+10000 as int)
            else geo_position_id end as geo_position_id
      ,black_type
      ,search_type
      ,check_type
      ,search_id
      ,trace_id
      ,rank_trace_id
      ,house_id
      ,hotel_id
      ,uid
      ,final_price
      ,detail_uid
      ,distance
      ,position
      ,max_price
      ,min_price
      ,without_risk_order_num
      ,without_risk_order_gmv
      ,without_risk_order_room_night
      ,without_risk_access_order_gmv
      ,without_risk_access_order_num
      ,without_risk_access_order_room_night
      ,rank_scene_empty_filter
from 
        (select 
            dt
            ,wrapper_name
            ,search_id
            ,trace_id
            ,rank_trace_id
            ,city_id
            ,t1.city_name
            ,search_city_id
            ,t2.district_name as search_city_name
            ,geo_city_id
            ,geo_city_name
            ,geo_position_id
            -- ,location_filter
            -- ,location_type
            ,conditions_map['type1']['value'] aa
            ,conditions_map['type1']['label']  bb
            ,get_json_object(server_log,'$.searchScene') as searchScene
            ,get_json_object(server_log,'$.area_id') as area_id
            ,case when get_json_object(extend_map,'$.blacklist') = 4 then '命中远距离减分'
                  end as black_type
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
            ,case when datediff(checkin_date,dt)=0 or datediff(checkin_date,dt)=-1 then 'T0'
                  else 'TN' 
                  end as check_type
            ,house_id
            ,hotel_id
            ,uid
            ,final_price
            ,detail_uid
            ,distance
            ,position
            ,max_price
            ,min_price
            ,without_risk_order_num
            ,without_risk_order_gmv
            ,without_risk_order_room_night
            ,without_risk_access_order_gmv
            ,without_risk_access_order_num
            ,without_risk_access_order_room_night
            ,rank_scene_empty_filter
        from   dws.dws_path_ldbo_d t1
        left join(select distinct 
                city_name,
                district_name 
            from tujia_dim.dim_region) as t2
            on t1.search_city_name=t2.district_name
        where  dt between date_sub(current_date,14) and date_sub(current_date,1)
            and wrapper_name in ('携程','途家','去哪儿') 
            and source = 102
            and user_type = '用户'
            and is_oversea = 0
            and geo_city_id is not null
            and get_json_object(server_log,'$.searchScene') in (5,3,4,8)
        ) as a 
)  
,ord as 
        (select 
            case   
            when terminal_type_name = '艺龙-小程序' then '艺龙'
            when terminal_type_name = '本站-APP' then '途家'
            when terminal_type_name = '携程-APP' then '携程'
            when terminal_type_name = '去哪儿-APP' then '去哪儿'
            end as wrapper_name
            ,city_id,city_name,create_date as dt,house_id,uid,order_no,room_total_amount,order_room_night_count,dynamic_business
        from dws.dws_order 
        where create_date between date_sub(current_date,14) and date_sub(current_date,1)
            and is_paysuccess_order = 1 
            and terminal_type_name in ('携程-APP','本站-APP','去哪儿-APP') 
            and is_risk_order = 0 
            and is_overseas = 0 
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
union 
    select
        a.geo_position_id
        ,a.geo_name
        ,'县级市' as geo_type
        ,city_id
    from
        (
            select distinct 
                level2_area_id as geo_position_id
                ,level2_area_name as geo_name
                ,house_city_id as city_id
            from house_info
            where dt = date_sub(current_date,1)
        )a
    join
        (
            select distinct
                search_city_name
            from list
            where search_type = '县级市'
        )b on a.geo_name = b.search_city_name
union 
    select
        a.geo_position_id
        ,a.geo_name
        ,'行政区' as geo_type
        ,city_id
    from
        (
            select distinct 
                level2_area_id as geo_position_id
                ,level2_area_name as geo_name
                ,house_city_id as city_id
            from house_info
            where dt = date_sub(current_date,1)
        )a
    left join
        (
            select distinct
                search_city_name
            from list
            where search_type = '县级市'
        )b on a.geo_name = b.search_city_name
    where b.search_city_name is null
union 
    select  distinct 
        id as geo_position_id
        ,name as geo_name
        ,'景区地区' as geo_type
        ,city_id
    from(
        select distinct id,name,city_id
        from ods_geo_landmark.area
        lateral view explode(split(city_ids,','))t as city_id
        ) as t1
)
,house_weight as 
    (
        select
            city_id
            ,geo_position_id
            ,weight_dis
        from
            (
                select distinct 
                    city_id
                    -- ,city_name
                    ,geo_position_id
                    ,get_json_object(regexp_extract(get_json_object(adjust_distance,'$.adjustDistance'),'^\\[(.+)\\]$',1),'$.distanceThreshold')*1000 as weight_dis --降权距离（单位：m）
                    from tujia_ods.rank_data_rank_rank_geo_config 
                    where geo_type not in ('8','24') --只看poi点
            )a
    )
,toppoi as (
    select
        a.*
        ,geo_type
    from
    (
        select
            wrapper_name
            ,city_id
            ,city_name
            ,geo_position_id
            ,count(distinct uid,dt) as list_uv
        from list
        where dt between date_sub(current_date,7) and date_sub(current_date,1)
        and search_type in ('地标','行政区','县级市','景区地区')
        group by 1,2,3,4
        having list_uv >= 10
    )a
    left join
    (
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
    )b on a.geo_position_id = b.geo_position_id
)
,recall_distance_info as (
    select distinct 
        a.geo_position_id
        ,a.house_id
        ,a.if_in_business_district
    from
        dws.dws_house_distance_recall a
    join 
        (
            select
                distinct geo_position_id
            from toppoi
        )b
    on a.geo_position_id = b.geo_position_id
)
,list_info as (
    select
        list.dt
        ,list.wrapper_name
        ,list.city_id
        ,list.city_name
        ,list.geo_position_id
        ,list.uid
        ,list.detail_uid
        ,list.without_risk_access_order_num
        ,list.without_risk_access_order_gmv
        ,list.without_risk_access_order_room_night
        ,list.without_risk_access_order_gmv/list.without_risk_access_order_room_night as adr_z
        ,list.check_type
        ,list.house_id
        ,list.distance
        ,list.final_price
        ,list.black_type
        ,list.position
        ,list.max_price
        ,list.min_price
        ,house_info.`是否优选`
        ,house_info.`是否严选`
        ,house_info.`是否臻选`
        ,house_info.`居室`
        ,house_info.landlord_channel
        ,house_info.house_type
        ,leveldesc
        ,toppoi.geo_type
        ,weight_dis
        ,if_in_business_district
        ,case when ((toppoi.geo_type not in ('商圈','景区') and distance <= coalesce(weight_dis,2000)) 
                 or (toppoi.geo_type='商圈' and if_in_business_district=1))
              then '是' else '否' end as `是否降权距离内`
    from    
      (select * from list where dt between date_sub(current_date,14) and date_sub(current_date,1)) as list
    join 
      (select * from house_info where dt between date_sub(current_date,14) and date_sub(current_date,1)) as house_info
        on list.dt = house_info.dt and list.house_id = house_info.house_id
    left join house_weight dis
        on list.geo_position_id = dis.geo_position_id
        and list.city_id = dis.city_id
    left join recall_distance_info cc
        on list.geo_position_id = cc.geo_position_id
        and list.house_id = cc.house_id
    join toppoi
    on list.city_id = toppoi.city_id
    and list.city_name = toppoi.city_name
    and list.geo_position_id = toppoi.geo_position_id
    and list.wrapper_name = toppoi.wrapper_name
    left join hotel_type_info
    on list.dt = hotel_type_info.dt and list.hotel_id = hotel_type_info.hotelid
    where list.search_type in ('地标','行政区','县级市','景区地区')
)
,hs_weight_cnt_info as (
    select 
         a.city_id
        ,a.geo_position_id
        ,count(distinct case when geo_type not in ('行政区','景区地区','商圈','景区') and distance<=coalesce(weight_dis,2000) and d.house_id is not null then b.house_id
                             when geo_type = '商圈' and if_in_business_district=1 and d.house_id is not null then b.house_id
                             end) as `降权距离内优严臻在线房源数`
        ,count(distinct case when geo_type not in ('行政区','景区地区','商圈','景区') and distance<=coalesce(weight_dis,2000) and d.house_id is not null then b.house_id
                             when geo_type = '商圈' and if_in_business_district=1 and e.house_id is not null then b.house_id
                             end) as `降权距离内严臻在线房源数`
    from 
        (
            select distinct
                 toppoi.city_id
                ,toppoi.geo_position_id
                ,geo_info.geo_type
            from toppoi
            join geo_info
            on  toppoi.city_id = geo_info.city_id
            and toppoi.geo_position_id = geo_info.geo_position_id
        )a
    left join
        (
            select distinct
                geo_position_id
                ,house_id
                ,distance
                ,if_in_business_district
            from dws.dws_house_distance_recall
        )b
    on a.geo_position_id = b.geo_position_id
    left join house_weight c
    on a.geo_position_id = c.geo_position_id
    and a.city_id = c.city_id
    left join
        (
            select distinct
                house_id
            from house_info
            where (`是否优选`='优选' or `是否严选`='严选' or `是否臻选`='臻选')
            and dt = date_sub(current_date,1)
        )d
    on b.house_id = d.house_id
    left join
        (
            select distinct
                house_id
            from house_info
            where (`是否严选`='严选' or `是否臻选`='臻选')
            and dt = date_sub(current_date,1)
        )e
    on b.house_id = e.house_id
    group by 1,2
)
,cross_uid as(
    select distinct
        '携程' as wrapper_name
        ,city_id
        ,city_name
        ,geo_position_id
        ,uid
        ,dt
    from app_ctrip.edw_c_bnb_hotel_cross_uid_d_zwm
    where d = date_sub(current_date,1)
    and dt between date_sub(current_date,14) and date_sub(current_date,1)
)
,hotel_ord_info as(
    select
        to_date(orderdate) as dt
        -- ,uid
        ,clientid as uid
        ,orderid
        ,ciireceivable
        ,ciiquantity
        -- ,ciireceivable/ciiquantity as adr_k
    from app_ctrip.edw_htl_order_all_split
    where 
        d = date_sub(current_date,0)
        and submitfrom='client'
        and to_date(orderdate) between date_sub(current_date,14) and date_sub(current_date,1)
        and orderstatus in ('S','P')
        and country = 1
        and ordertype = 2 -- 酒店订单
        and uid not in ('_A20190122115701366','_A20151130164107749','_A20190725013107744','E275301478','_A20200710175238972','_A20200211153419761','_A20200921154622724','_A20180814102302643','_A20150928110743155','_A20210226104734937')
        and clientid <> ''
        and clientid is not null
)
,hotal_cross_now as(
    select 
        '携程' as wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,sum(nights_k) as `交叉uv_酒店间夜`
        ,sum(gmv_k)    as `交叉uv_酒店gmv`
        ,sum(ord_k)    as `交叉uv_酒店订单`
        ,percentile_approx(adr_k,0.5) as `交叉UV酒店下单中位价` 
    from cross_uid a
    join
        (
            select 
                uid
                ,dt
                ,count(distinct orderid) as ord_k 
                ,sum(ciiquantity) as nights_k
                ,sum(ciireceivable) as gmv_k
                ,sum(ciireceivable)/sum(ciiquantity) as adr_k
            from hotel_ord_info
            group by 1,2
        )b
    on lower(a.uid) = lower(b.uid)
    and a.dt = b.dt
    where a.dt between date_sub(current_date,7) and date_sub(current_date,1)
    group by 1,2,3,4
)
,hotal_cross_lw as(
    select 
        '携程' as wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,sum(nights_k) as `交叉uv_酒店间夜_lw`
        ,sum(gmv_k)    as `交叉uv_酒店gmv_lw`
        ,sum(ord_k)    as `交叉uv_酒店订单_lw`
        ,percentile_approx(adr_k,0.5) as `交叉UV酒店下单中位价_lw` 
    from cross_uid a
    join
        (
            select 
                uid
                ,dt
                ,count(distinct orderid) as ord_k 
                ,sum(ciiquantity) as nights_k
                ,sum(ciireceivable) as gmv_k
                ,sum(ciireceivable)/sum(ciiquantity) as adr_k
            from hotel_ord_info
            group by 1,2
        )b
    on lower(a.uid) = lower(b.uid)
    and a.dt = b.dt
    where a.dt between date_sub(current_date,14) and date_sub(current_date,8)
    group by 1,2,3,4
)
,bnb_cross_now as(
    select 
        '携程' as wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,sum(nights_k) as `交叉uv_民宿间夜`
        ,sum(gmv_k)    as `交叉uv_民宿gmv`
        ,sum(ord_k)    as `交叉uv_民宿订单`
    from cross_uid a
    join
        (
            select 
                uid
                ,dt
                ,count(distinct order_no) as ord_k 
                ,sum(order_room_night_count) as nights_k
                ,sum(room_total_amount) as gmv_k
            from ord
            group by 1,2
        )b
    on lower(a.uid) = lower(b.uid)
    and a.dt = b.dt
    where a.dt between date_sub(current_date,7) and date_sub(current_date,1)
    group by 1,2,3,4
)
,bnb_cross_lw as(
    select 
        '携程' as wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,sum(nights_k) as `交叉uv_民宿间夜_lw`
        ,sum(gmv_k)    as `交叉uv_民宿gmv_lw`
        ,sum(ord_k)    as `交叉uv_民宿订单_lw`
    from cross_uid a
    join
        (
            select 
                uid
                ,dt
                ,count(distinct order_no) as ord_k 
                ,sum(order_room_night_count) as nights_k
                ,sum(room_total_amount) as gmv_k
            from ord
            group by 1,2
        )b
    on lower(a.uid) = lower(b.uid)
    and a.dt = b.dt
    where a.dt between date_sub(current_date,14) and date_sub(current_date,8)
    group by 1,2,3,4
)
,pv_info as(
    select
        a.wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        -- ,count(1) as list_pv
        ,percentile_approx(if(c.uid is not null and without_risk_order_num>0,distance,null),0.8) as `交叉下单房源距离8分位`

        ,count(if(final_price<`交叉UV酒店下单中位价`,1,null)) as `交叉UV酒店下单中位价以下曝光`
    from list a
    left join hotal_cross_now b
    on  a.city_id = b.city_id
    and a.city_name = b.city_name
    and a.geo_position_id = b.geo_position_id
    and a.wrapper_name = b.wrapper_name
    left join cross_uid c
    on  a.city_id = c.city_id
    and a.city_name = c.city_name
    and a.geo_position_id = c.geo_position_id
    and a.wrapper_name = c.wrapper_name
    and lower(a.uid) = lower(c.uid)
    and a.dt = c.dt
    where a.dt between date_sub(current_date,7) and date_sub(current_date,1)
    group by 1,2,3,4
)
,k_ord_info as(
    select
        a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,a.uid
        ,a.dt
        ,a.wrapper_name
        ,ord_k
        ,nights_k
        ,gmv_k
        ,gmv_k/nights_k as adr_k
    from
        (
            select distinct
                city_id
                ,city_name
                ,geo_position_id
                ,uid
                ,dt
                ,wrapper_name
            from list
            where search_type in ('地标','行政区','县级市','景区地区')
        )a
    join
        (
            select 
                wrapper_name
                ,uid
                ,dt
                ,count(distinct order_no) as ord_k 
                ,sum(order_room_night_count) as nights_k
                ,sum(room_total_amount) as gmv_k
            from ord
            group by 1,2,3
        )b
    on a.dt = b.dt
    and lower(a.uid) = lower(b.uid)
    and a.wrapper_name = b.wrapper_name
)
,ord_info as(
    select 
        a.wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        -- ,sum(ord_k) as ord_k

        ,sum(if(adr_k<`交叉UV酒店下单中位价`,ord_k,null)) as `交叉UV酒店下单中位价以下订单`
    from k_ord_info a
    left join hotal_cross_now b
    on  a.city_id = b.city_id
    and a.city_name = b.city_name
    and a.geo_position_id = b.geo_position_id
    and a.wrapper_name = b.wrapper_name
    where dt between date_sub(current_date,7) and date_sub(current_date,1)
    group by 1,2,3,4
)
,recall_online_info as(
    select
         a.geo_position_id
    ---召回在线
        -- ,count(distinct a.house_id) as `召回在线房屋数`
        ,count(distinct case when leveldesc='超赞房东' then a.house_id end) as `超赞在线房源数`
        ,count(distinct case when `是否严臻`='是' then a.house_id end) as `严臻在线房源数`
        ,count(distinct case when `是否优选`='优选' then a.house_id end) as `优选在线房源数`
        ,count(distinct case when landlord_channel='接入' then a.house_id end) as `接入在线房源数`
        ,count(distinct case when house_type='标准酒店' then a.house_id end) as `标准酒店在线房源数`
    from
        (
            select distinct 
                geo_position_id
                ,house_id
                ,hotel_id
            from
                dws.dws_house_distance_recall -- poi
            
            union

            select distinct 
                level2_area_id as geo_position_id
                ,house_id
                ,hotel_id
            from house_info
            where dt = date_sub(current_date,1) -- "行政区"
        )a
    left join 
        (
            select distinct
                house_id
                ,case when (`是否严选`='严选' or `是否臻选`='臻选') then '是' else '否' end as `是否严臻`
                ,`是否优选`
                ,landlord_channel
                ,house_type
            from house_info
            where dt = date_sub(current_date,1)
        )b on a.house_id = b.house_id
    left join
        (
            select
                hotelid
                ,leveldesc
            from hotel_type_info
            where dt = date_sub(current_date,1)
        )c on a.hotel_id = c.hotelid
    group by 1
)
,keshou_request as(
    select
        dt
        ,search_id
        ,rank_trace_id
        ,uid
        ,house_id
        ,wrapper_name
        ,get_json_object(get_json_object(get_json_object(extend_field, '$.rankTagIdCanSaleInfoMap'), '$.1'), '$.canSalePercent') 
         as `超赞L21+请求维度可售率`
        ,get_json_object(get_json_object(get_json_object(extend_field, '$.rankTagIdCanSaleInfoMap'), '$.101'), '$.canSalePercent') 
         as `严臻L21+请求维度可售率`
        ,get_json_object(get_json_object(get_json_object(extend_field, '$.rankTagIdCanSaleInfoMap'), '$.102'), '$.canSalePercent') 
         as `直采L21+请求维度可售率`
    from pdb_analysis_c.ads_flow_list_price_day_d
    where 
        dt between date_sub(current_date,7) and date_sub(current_date,1)
)
,keshou as(
    select
        a.wrapper_name
        ,city_id
        ,city_name
        ,geo_position_id
        ,percentile_approx(`超赞L21+请求维度可售率`,0.5) as `超赞L21+可售率`
        ,percentile_approx(`严臻L21+请求维度可售率`,0.5) as `严臻L21+可售率`
    from keshou_request a
    join
        (
            select distinct
                dt
                ,search_id
                ,rank_trace_id
                ,uid
                ,geo_position_id
                ,city_id
                ,city_name
                ,wrapper_name
            from list
            where search_type in ('地标','行政区','县级市','景区地区')
        )b
    on a.dt = b.dt
    and a.search_id = b.search_id
    and a.rank_trace_id = b.rank_trace_id
    and lower(a.uid) = lower(b.uid)
    and a.wrapper_name = b.wrapper_name
    group by 1,2,3,4
)
,mid_price as(
    select
         a.wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,percentile_approx(adr_z,0.5) as `间夜中位价`
        ,percentile_approx(if(without_risk_access_order_num > 0,distance,null),0.8) as 8_distance
    from list_info a
    where a.dt between date_sub(current_date,7) and date_sub(current_date,1)
    group by 1,2,3,4
)




select
    case when a.wrapper_name = '携程' then 'C携程'
         when a.wrapper_name = '去哪儿' then 'Q去哪儿'
         when a.wrapper_name = '途家' then 'T途家'
         end as wrapper_name
    ,a.city_id
    ,a.city_name
    ,a.geo_position_id
    ,geo_info.geo_name
    ,geo_info.geo_type
    ,concat(round(ord_z/list_uv*100,2),'%') as `窄l2o_uv`
    ,concat(round(((ord_z/list_uv)/(ord_z_lw/list_uv_lw)-1)*100,2),'%') as `窄l2o_uv周环比`
    ,concat(round(ord_k/list_uv*100,2),'%') as `宽l2o_uv`
    ,concat(round(((ord_k/list_uv)/(ord_k_lw/list_uv_lw)-1)*100,2),'%') as `宽l2o_uv周环比`
    ,round(gmv_z/list_uv,2) as `窄gmv/uv`
    ,concat(round(((gmv_z/list_uv)/(gmv_z_lw/list_uv_lw)-1)*100,2),'%') as `窄gmv/uv周环比`
    ,round(gmv_k/list_uv,2) as `宽gmv/uv`
    ,concat(round(((gmv_k/list_uv)/(gmv_k_lw/list_uv_lw)-1)*100,2),'%') as `宽gmv/uv周环比`
    ,concat(round(detail_uv/list_uv*100,2),'%') as `L2D_uv`
    ,concat(round(((detail_uv/list_uv)/(detail_uv_lw/list_uv_lw)-1)*100,2),'%') as `L2D_uv周环比`
        
    ,list_uv
    ,concat(round((list_uv/list_uv_lw-1)*100,2),'%') as `list_uv周环比`
    ,list_pv
    ,concat(round((list_pv/list_pv_lw-1)*100,2),'%') as `list_pv周环比`
    ,ord_z as `窄订单`
    ,nights_z as `窄间夜`
    ,gmv_z as `窄gmv`
    ,ord_k as `宽订单`
    ,nights_k as `宽间夜`
    ,round(gmv_k,2) as `宽gmv`
    ,round(adr_k,2) as `成单均价`
    ,round(adr_mid,2) as `成单中位价`
    

    ,concat(round(`交叉uv_民宿间夜`/(`交叉uv_民宿间夜`+`交叉uv_酒店间夜`)*100,2),'%') as `交叉抢夺率`
    ,concat(round(((`交叉uv_民宿间夜`/(`交叉uv_民宿间夜`+`交叉uv_酒店间夜`))/(`交叉uv_民宿间夜_lw`/(`交叉uv_民宿间夜_lw`+`交叉uv_酒店间夜_lw`))-1)*100,2),'%') as `交叉抢夺率周环比`
    
    ,round(`交叉UV酒店下单中位价`,2) as `交叉UV酒店下单中位价`
    ,concat(round(`交叉UV酒店下单中位价以下曝光`/list_pv*100,2),'%') as `交叉UV酒店下单中位价以下曝光占比`
    ,concat(round(`交叉UV酒店下单中位价以下订单`/ord_k*100,2),'%')   as `交叉UV酒店下单中位价以下订单占比`
    ,case when geo_info.geo_type in ('行政区','景区地区') then '无降权距离'
          when geo_info.geo_type not in ('商圈','景区') and weight_dis is not null then weight_dis
          when geo_info.geo_type not in ('商圈','景区') and weight_dis is null then '默认2km'
          when geo_info.geo_type='商圈' then '商圈外降权'
          end as `降权距离`
    ,round(`窄口径下单房源距离8分位`,2) as `窄口径下单房源距离8分位`
    ,round(`交叉下单房源距离8分位`,2) as `交叉下单房源距离8分位`
    
    ,concat(round(`降权距离内曝光`/list_pv*100,2),'%') as `降权距离内曝光占比`
    ,concat(round(`降权距离内订单`/ord_z*100,2),'%') as `降权距离内订单占比`
    ,`降权距离内优严臻在线房源数`
    ,concat(round(`降权距离内优严臻曝光`/list_pv*100,2),'%') as `降权距离内优严臻曝光占比`
    
    ,`降权距离内严臻在线房源数`
    ,concat(round(`降权距离内严臻曝光`/list_pv*100,2),'%') as `降权距离内严臻曝光占比`

    ,concat(round(`命中远距离减分曝光`/list_pv*100,2),'%') as `命中远距离减分曝光占比`

    ,concat(round(T0_list_uv/list_uv*100,2),'%') as `T0UV占比`
    ,concat(round(((T0_list_uv/list_uv)/(T0_list_uv_lw/list_uv_lw)-1)*100,2),'%') as `T0UV占比周环比`
    ,concat(round(T0_ord_z/ord_z*100,2),'%') as `T0订单占比(窄)`
    ,concat(round(((T0_ord_z/ord_z)/(T0_ord_z_lw/ord_z_lw)-1)*100,2),'%') as `T0订单占比周环比(窄)`
    ,round(T0_adr_mid,2) as `T0成单中位价`
    ,round(TN_adr_mid,2) as `TN成单中位价`

    ,concat(round(`一居曝光`/list_pv*100,2),'%') as `一居曝光占比`
    ,concat(round(`一居窄订单`/`窄订单`*100,2),'%') as `一居订单占比`
    ,round(`一居成单中位价(窄)`,2) as `一居成单中位价(窄)`

    ,concat(round(`二居曝光`/list_pv*100,2),'%') as `二居曝光占比`
    ,concat(round(`二居窄订单`/`窄订单`*100,2),'%') as `二居订单占比`
    ,round(`二居成单中位价(窄)`,2) as `二居成单中位价(窄)`

    ,concat(round(`三居+曝光`/list_pv*100,2),'%') as `三居+光占比`
    ,concat(round(`三居+窄订单`/`窄订单`*100,2),'%') as `三居+订单占比`
    ,round(`三居+成单中位价(窄)`,2) as `三居+成单中位价(窄)`

    ,nvl(`超赞在线房源数`,0) as `超赞在线房源数`
    ,concat(round(`超赞L21+可售率`,2),'%') as `超赞L21+可售率`
    ,concat(round(`超赞曝光`/list_pv*100,2),'%') as `超赞曝光占比`
    ,concat(round(`超赞窄订单`/`窄订单`*100,2),'%') as `超赞订单占比`
    ,concat(round(`超赞窄gmv`/`窄gmv`*100,2),'%') as `超赞gmv占比`
    ,round(`超赞成单中位价(窄)`,2) as `超赞成单中位价(窄)`
    
    ,nvl(`严臻在线房源数`,0) as `严臻在线房源数`
    ,concat(round(`严臻L21+可售率`,2),'%') as `严臻L21+可售率`
    ,concat(round(`严臻曝光`/list_pv*100,2),'%') as `严臻曝光占比`
    ,concat(round(`严臻窄订单`/`窄订单`*100,2),'%') as `严臻订单占比`
    ,concat(round(`严臻窄gmv`/`窄gmv`*100,2),'%') as `严臻gmv占比`
    ,round(`严臻成单中位价(窄)`,2) as `严臻成单中位价(窄)`

    ,nvl(`优选在线房源数`,0) as `优选在线房源数`
    ,concat(round(`优选曝光`/list_pv*100,2),'%') as `优选曝光占比`
    ,concat(round(`优选窄订单`/`窄订单`*100,2),'%') as `优选订单占比`
    ,concat(round(`优选窄gmv`/`窄gmv`*100,2),'%') as `优选gmv占比`
    ,round(`优选成单中位价(窄)`,2) as `优选成单中位价(窄)`

    ,nvl(`接入在线房源数`,0) as `接入在线房源数`
    ,concat(round(`接入曝光`/list_pv*100,2),'%') as `接入曝光占比`
    ,concat(round(`接入窄订单`/`窄订单`*100,2),'%') as `接入订单占比`
    ,concat(round(`接入窄gmv`/`窄gmv`*100,2),'%') as `接入gmv占比`
    ,round(`接入成单中位价(窄)`,2) as `接入成单中位价(窄)`


    ,nvl(`标准酒店在线房源数`,0) as `标准酒店在线房源数`
    ,concat(round(`标准酒店曝光`/list_pv*100,2),'%') as `标准酒店曝光占比`
    ,concat(round(`标准酒店窄订单`/`窄订单`*100,2),'%') as `标准酒店订单占比`
    ,concat(round(`标准酒店窄gmv`/`窄gmv`*100,2),'%') as `标准酒店gmv占比`
    ,round(`标准酒店成单中位价(窄)`,2) as `标准酒店成单中位价(窄)`

    ,round(`间夜均价`,0) as `间夜均价`
    ,round(`间夜中位价`,0) as `间夜中位价`
    ,round(`间夜80分位价`,0) as `间夜80分位价`
    ,round(`top10曝光均价_无价格筛选`,0) as `top10曝光均价_无价格筛选`
    ,concat(round(`无价格筛选曝光`/list_pv * 100,2),'%') as `无价格筛选曝光占比`
    ,concat(round(`top10_无价格筛选_间夜中位价以下曝光`/`top10_无价格筛选曝光` * 100,2),'%') as `top10_无价格筛选_间夜中位价以下曝光占比`
    ,concat(round(`top20_无价格筛选_间夜中位价以下曝光`/`top20_无价格筛选曝光` * 100,2),'%') as `top20_无价格筛选_间夜中位价以下曝光占比`
    ,concat(round(`间夜中位价以下曝光`/list_pv * 100,2),'%') as `间夜中位价以下曝光占比`
    ,round(`中位下单距离`,0) as `中位下单距离` 
    ,round(`80分位下单距离`,0) as `80分位下单距离`  
    ,concat(round(`top10_成单距离80分位以下曝光`/top10_list_pv * 100,2),'%') as `top10_成单距离80分位以下曝光占比`
    ,concat(round(`top20_成单距离80分位以下曝光`/top20_list_pv * 100,2),'%') as `top20_成单距离80分位以下曝光占比`
    ,concat(round(`成单距离80分位以下曝光`/list_pv * 100,2),'%') as `成单距离80分位以下曝光占比`

    ,`交叉uv_民宿间夜`
    ,round(`交叉uv_民宿gmv`,2) as `交叉uv_民宿gmv`
    ,`交叉uv_民宿订单`

    ,`交叉uv_酒店间夜`
    ,round(`交叉uv_酒店gmv`,2) as `交叉uv_酒店gmv`
    ,`交叉uv_酒店订单`

    ,concat(round((`交叉uv_民宿间夜_lw`/(`交叉uv_民宿间夜_lw`+`交叉uv_酒店间夜_lw`))*100,2),'%') as `交叉抢夺率_lw`
    ,`交叉uv_民宿间夜_lw`
    ,round(`交叉uv_民宿gmv_lw`,2) as `交叉uv_民宿gmv_lw`
    ,`交叉uv_民宿订单_lw`

    ,`交叉uv_酒店间夜_lw`
    ,round(`交叉uv_酒店gmv_lw`,2) as `交叉uv_酒店gmv_lw`
    ,`交叉uv_酒店订单_lw`
from
    (select
         a.wrapper_name
        ,a.city_id
        ,a.city_name
        ,a.geo_position_id
        ,count(1) as list_pv
        ,count(distinct a.uid,a.dt) as list_uv
        -- ,count(distinct uid) as `7天去重uv`
        ,count(distinct if(detail_uid is not null,concat(a.uid,a.dt),null)) as detail_uv
        ,sum(without_risk_access_order_num) as ord_z
        ,sum(without_risk_access_order_room_night) as nights_z
        ,sum(without_risk_access_order_gmv) as gmv_z
        
        ,count(if(`是否降权距离内`='是',1,null)) as `降权距离内曝光`
        ,sum(if(`是否降权距离内`='是',without_risk_access_order_num,null)) as `降权距离内订单`
        
        ,count(if(`是否降权距离内`='是' and (`是否优选`='优选' or `是否严选`='严选' or `是否臻选`='臻选'),1,null)) as `降权距离内优严臻曝光`
        ,sum(if(`是否降权距离内`='是'and (`是否优选`='优选' or `是否严选`='严选' or `是否臻选`='臻选'),without_risk_access_order_num,null)) as `降权距离内优严臻订单`

        ,count(if(`是否降权距离内`='是' and (`是否严选`='严选' or `是否臻选`='臻选'),1,null)) as `降权距离内严臻曝光`
        ,sum(if(`是否降权距离内`='是'and (`是否严选`='严选' or `是否臻选`='臻选'),without_risk_access_order_num,null)) as `降权距离内严臻订单`

        ,count(if(black_type='命中远距离减分',1,null)) as `命中远距离减分曝光`
        ,count(distinct if(check_type='T0',concat(a.uid,a.dt),null)) as T0_list_uv
        ,sum(if(check_type='T0',without_risk_access_order_num,null)) as T0_ord_z
        ,percentile_approx(if(check_type='T0',adr_z,null),0.5) as T0_adr_mid
        ,percentile_approx(if(check_type='TN',adr_z,null),0.5) as TN_adr_mid

        ,percentile_approx(if(without_risk_access_order_num>0,distance,null),0.8) as `窄口径下单房源距离8分位`

        -----------曝光
        ,count(if(`居室`='一居',1,null)) as `一居曝光`
        ,count(if(`居室`='二居',1,null)) as `二居曝光`
        ,count(if(`居室`='三居+',1,null)) as `三居+曝光`
        
        ,count(if(leveldesc='超赞房东',1,null)) as `超赞曝光`
        ,count(if(`是否严选`='严选' or `是否臻选`='臻选',1,null)) as `严臻曝光`
        ,count(if(`是否优选`='优选',1,null)) as `优选曝光`
        ,count(if(landlord_channel='接入',1,null)) as `接入曝光`
        ,count(if(house_type='标准酒店',1,null)) as `标准酒店曝光`
    ---------订单
        ,sum(if(`居室`='一居',without_risk_access_order_num,null)) as `一居窄订单`
        ,sum(if(`居室`='二居',without_risk_access_order_num,null)) as `二居窄订单`
        ,sum(if(`居室`='三居+',without_risk_access_order_num,null)) as `三居+窄订单`
        
        ,sum(if(leveldesc='超赞房东',without_risk_access_order_num,null)) as `超赞窄订单`
        ,sum(if(`是否严选`='严选' or `是否臻选`='臻选',without_risk_access_order_num,null)) as `严臻窄订单`
        ,sum(if(`是否优选`='优选',without_risk_access_order_num,null)) as `优选窄订单`
        ,sum(if(landlord_channel='接入',without_risk_access_order_num,null)) as `接入窄订单`
        ,sum(if(house_type='标准酒店',without_risk_access_order_num,null)) as `标准酒店窄订单`
    ---------间夜
        ,sum(if(`居室`='一居',without_risk_access_order_room_night,null)) as `一居窄间夜`
        ,sum(if(`居室`='二居',without_risk_access_order_room_night,null)) as `二居窄间夜`
        ,sum(if(`居室`='三居+',without_risk_access_order_room_night,null)) as `三居+窄间夜`
        
        ,sum(if(leveldesc='超赞房东',without_risk_access_order_room_night,null)) as `超赞窄间夜`
        ,sum(if(`是否严选`='严选' or `是否臻选`='臻选',without_risk_access_order_room_night,null)) as `严臻窄间夜`
        ,sum(if(`是否优选`='优选',without_risk_access_order_room_night,null)) as `优选窄间夜`
        ,sum(if(landlord_channel='接入',without_risk_access_order_room_night,null)) as `接入窄间夜`
        ,sum(if(house_type='标准酒店',without_risk_access_order_room_night,null)) as `标准酒店窄间夜`
    ---------gmv
        ,sum(if(`居室`='一居',without_risk_access_order_gmv,null)) as `一居窄gmv`
        ,sum(if(`居室`='二居',without_risk_access_order_gmv,null)) as `二居窄gmv`
        ,sum(if(`居室`='三居+',without_risk_access_order_gmv,null)) as `三居+窄gmv`
        
        ,sum(if(leveldesc='超赞房东',without_risk_access_order_gmv,null)) as `超赞窄gmv`
        ,sum(if(`是否严选`='严选' or `是否臻选`='臻选',without_risk_access_order_gmv,null)) as `严臻窄gmv`
        ,sum(if(`是否优选`='优选',without_risk_access_order_gmv,null)) as `优选窄gmv`
        ,sum(if(landlord_channel='接入',without_risk_access_order_gmv,null)) as `接入窄gmv`
        ,sum(if(house_type='标准酒店',without_risk_access_order_gmv,null)) as `标准酒店窄gmv`
    ---------成单中位价(窄)
        -- ,percentile_approx(adr,0.5) as `窄`
        ,percentile_approx(if(`居室`='一居',adr_z,null),0.5) as `一居成单中位价(窄)`
        ,percentile_approx(if(`居室`='二居',adr_z,null),0.5) as `二居成单中位价(窄)`
        ,percentile_approx(if(`居室`='三居+',adr_z,null),0.5) as `三居+成单中位价(窄)`
        
        ,percentile_approx(if(leveldesc='超赞房东',adr_z,null),0.5) as `超赞成单中位价(窄)`
        ,percentile_approx(if(`是否严选`='严选' or `是否臻选`='臻选',adr_z,null),0.5) as `严臻成单中位价(窄)`
        ,percentile_approx(if(`是否优选`='优选',adr_z,null),0.5) as `优选成单中位价(窄)`
        ,percentile_approx(if(landlord_channel='接入',adr_z,null),0.5) as `接入成单中位价(窄)`
        ,percentile_approx(if(house_type='标准酒店',adr_z,null),0.5) as `标准酒店成单中位价(窄)`

        ,avg(adr_z) as `间夜均价`
        ,percentile_approx(adr_z,0.5) as `间夜中位价`
        ,percentile_approx(adr_z,0.8) as `间夜80分位价`
        ,avg(if(position<=10 and max_price = -1 and min_price =-1,final_price,null)) as `top10曝光均价_无价格筛选`
        ,count(if(max_price = -1 and min_price =-1,1,null)) as `无价格筛选曝光`
        ,count(if(position<=10 and max_price = -1 and min_price =-1,uid,null)) as `top10_无价格筛选曝光`
        ,count(if(position<=20 and max_price = -1 and min_price =-1,uid,null)) as `top20_无价格筛选曝光`
        ,count(if(position<=10 and max_price = -1 and min_price =-1 and final_price <=b.`间夜中位价`,uid,null)) as `top10_无价格筛选_间夜中位价以下曝光`
        ,count(if(position<=20 and max_price = -1 and min_price =-1 and final_price <=b.`间夜中位价`,uid,null)) as `top20_无价格筛选_间夜中位价以下曝光`
        ,count(if(final_price<=b.`间夜中位价`,uid,null)) as `间夜中位价以下曝光`
        ,percentile_approx(if(without_risk_access_order_num > 0,distance,null),0.5) as `中位下单距离`
        ,percentile_approx(if(without_risk_access_order_num > 0,distance,null),0.8) as `80分位下单距离`        
        ,count(if(position <= 10 and distance <= b.8_distance,uid,null)) as `top10_成单距离80分位以下曝光`
        ,count(if(position <= 20 and distance <= b.8_distance,uid,null)) as `top20_成单距离80分位以下曝光`
        ,count(if(distance <= b.8_distance,uid,null)) as `成单距离80分位以下曝光`
        ,count(if(position<=10,uid,null)) as top10_list_pv
        ,count(if(position<=20,uid,null)) as top20_list_pv
    from list_info a
    left join mid_price b
    on  a.wrapper_name = b.wrapper_name
    and a.city_id      = b.city_id
    and a.city_name    = b.city_name
    and a.geo_position_id = b.geo_position_id
    where a.dt between date_sub(current_date,7) and date_sub(current_date,1)
    group by 1,2,3,4
    )a
--获取上周
left join 
    (
        select
            wrapper_name
            ,city_id
            ,city_name
            ,geo_position_id
            ,count(1) as list_pv_lw
            ,count(distinct uid,dt) as list_uv_lw
            -- ,count(distinct uid) as `7天去重uv`
            ,count(distinct if(detail_uid is not null,concat(uid,dt),null)) as detail_uv_lw
            ,sum(without_risk_access_order_num) as ord_z_lw
            ,sum(without_risk_access_order_room_night) as nights_z_lw
            ,sum(without_risk_access_order_gmv) as gmv_z_lw
            
            ,count(if(`是否降权距离内`='是',1,null)) as `降权距离内曝光_lw`
            ,sum(if(`是否降权距离内`='是',without_risk_access_order_num,null)) as `降权距离内订单_lw`
            
            ,count(if(`是否降权距离内`='是' and (`是否优选`='优选' or `是否严选`='严选' or `是否臻选`='臻选'),1,null)) as `降权距离内优严臻曝光_lw`
            ,sum(if(`是否降权距离内`='是'and (`是否优选`='优选' or `是否严选`='严选' or `是否臻选`='臻选'),without_risk_access_order_num,null)) as `降权距离内优严臻订单_lw`
        
            ,count(distinct if(check_type='T0',concat(uid,dt),null)) as T0_list_uv_lw
            ,sum(if(check_type='T0',without_risk_access_order_num,null)) as T0_ord_z_lw

            ,percentile_approx(if(without_risk_access_order_num>0,distance,null),0.8) as `窄口径下单房源距离8分位_lw`

        from list_info
        where dt between date_sub(current_date,14) and date_sub(current_date,8)
        group by 1,2,3,4
    )b
on  a.wrapper_name = b.wrapper_name
and a.city_id = b.city_id
and a.city_name = b.city_name
and a.geo_position_id = b.geo_position_id

--获取宽口径转化
left join 
(select 
     a.wrapper_name
    ,a.city_id
    ,a.city_name
    ,a.geo_position_id
    ,sum(ord_k) as ord_k
    ,sum(nights_k) as nights_k
    ,sum(gmv_k) as gmv_k
    ,sum(gmv_k)/sum(nights_k) as adr_k
    ,percentile_approx(adr_k,0.5) as adr_mid
from k_ord_info a
where dt between date_sub(current_date,7) and date_sub(current_date,1)
group by 1,2,3,4
)c
on  a.wrapper_name = c.wrapper_name
and a.city_id = c.city_id
and a.city_name = c.city_name
and a.geo_position_id = c.geo_position_id
left join 
(select 
    wrapper_name
    ,city_id
    ,city_name
    ,geo_position_id
    ,sum(ord_k) as ord_k_lw
    ,sum(nights_k) as nights_k_lw
    ,sum(gmv_k) as gmv_k_lw
from k_ord_info
where dt between date_sub(current_date,14) and date_sub(current_date,8)
group by 1,2,3,4
)d
on  a.wrapper_name    = d.wrapper_name
and a.city_id         = d.city_id
and a.city_name       = d.city_name
and a.geo_position_id = d.geo_position_id
join geo_info
on  a.geo_position_id = geo_info.geo_position_id
and a.city_id=geo_info.city_id
left join house_weight e
on a.geo_position_id = e.geo_position_id
and a.city_id = e.city_id
left join hs_weight_cnt_info f
on  a.city_id = f.city_id
and a.geo_position_id = f.geo_position_id
left join bnb_cross_now g
on  a.wrapper_name    = g.wrapper_name
and a.city_id         = g.city_id
and a.city_name       = g.city_name
and a.geo_position_id = g.geo_position_id
left join bnb_cross_lw g_1
on  a.wrapper_name    = g_1.wrapper_name
and a.city_id         = g_1.city_id
and a.city_name       = g_1.city_name
and a.geo_position_id = g_1.geo_position_id
left join hotal_cross_now h
on  a.wrapper_name    = h.wrapper_name
and a.city_id         = h.city_id
and a.city_name       = h.city_name
and a.geo_position_id = h.geo_position_id
left join hotal_cross_lw h_1
on  a.wrapper_name    = h_1.wrapper_name
and a.city_id         = h_1.city_id
and a.city_name       = h_1.city_name
and a.geo_position_id = h_1.geo_position_id
left join pv_info i
on  a.wrapper_name    = i.wrapper_name
and a.city_id         = i.city_id
and a.city_name       = i.city_name
and a.geo_position_id = i.geo_position_id
left join ord_info j
on  a.wrapper_name    = j.wrapper_name
and a.city_id         = j.city_id
and a.city_name       = j.city_name
and a.geo_position_id = j.geo_position_id

left join recall_online_info k
on a.geo_position_id = k.geo_position_id
left join keshou l
on a.wrapper_name     = l.wrapper_name
and a.city_id         = l.city_id
and a.city_name       = l.city_name
and a.geo_position_id = l.geo_position_id
where a.city_name <> '澳门'
order by wrapper_name,list_uv desc
