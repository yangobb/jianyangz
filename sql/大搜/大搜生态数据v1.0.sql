
with ldbo as (
select 
    dt
    ,wrapper_name
    ,search_id
    ,trace_id
    ,rank_trace_id
    ,city_id
    ,t1.city_name
    ,search_city_id
    -- ,district_name as search_city_name
    ,geo_city_id
    ,geo_city_name
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
        when get_json_object(server_log,'$.searchScene') = 6 then '定位'
        when get_json_object(server_log,'$.searchScene') = 7 then '房屋搜索'
        when get_json_object(server_log,'$.searchScene') = 9 then '三方地标' 
        when get_json_object(server_log,'$.searchScene') = 0 then '无' 

        -- 重点关注
        when get_json_object(server_log,'$.searchScene') = 3 then '景区地区'
        when get_json_object(server_log,'$.searchScene') = 8 then '县级市'
        when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
        when get_json_object(server_log,'$.searchScene') = 5 then '地标'

        else '其他'
        end as search_type
    ,   
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
where dt >= '2025-05-07'
and wrapper_name in ('携程','途家','去哪儿') 
and source = 102
and user_type = '用户'
and is_oversea = 0
-- 本次需求仅看携程
-- and wrapper_name = '携程'
)
,geo_name as (
select distinct
    case when geo_position_type='1' then '地铁站'
        when geo_position_type='2' then '机场'
        when geo_position_type='3' then '高校'
        when geo_position_type='4' then '火车站'
        when geo_position_type='5' then '观光景点'
        when geo_position_type='6' then '汽车站'
        when geo_position_type='7' then '地铁线路'
        when geo_position_type='8' then '商圈'
        when geo_position_type='9' then '郊游景点'
        when geo_position_type='10' then '医院'
        when geo_position_type='13' then '地标'
        when geo_position_type='15' then '大地标'
        else '其他'
        end as poi_type
    ,name
    ,destination_id
    ,geo_position_id
from ods_geo_landmark.geo_position
where valid = '1' 
and front_show = '1'
)
,ab_test as (
select a.dt
    ,a.uid
    ,a.wrapper_name
    ,a.bucket
from (
select distinct 
    dt
    ,uid
    ,wrapper_name
    ,case when wrapper_name='携程' then get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v')
        when wrapper_name='去哪儿' then get_json_object(ab_test,'$.waptujia016_q_api_cj_pingbi.v')
        when wrapper_name='途家' then get_json_object(ab_test,'$.waptujia001_t_api_cj_pingbi.v')
        else '其他' end as bucket

    from dws.dws_path_ldbo_d 
    where dt >= '2025-05-07'
    and user_type = '用户'
    and wrapper_name in ('途家','携程','去哪儿') 
    and source = '102'
    and uid!='visitor000000'
    and (
        (wrapper_name='携程' and get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') is not null )
        or (wrapper_name='去哪儿' and  get_json_object(ab_test,'$.waptujia016_q_api_cj_pingbi.v') is not null )
        or (wrapper_name='途家' and  get_json_object(ab_test,'$.waptujia001_t_api_cj_pingbi.v') is not null )
    ) 
) a 
join (
    select
        dt
        ,uid
        ,wrapper_name
        ,count(distinct case when wrapper_name='携程' then get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') 
            when wrapper_name='去哪儿' then get_json_object(ab_test,'$.waptujia016_q_api_cj_pingbi.v')
            when wrapper_name='途家' then get_json_object(ab_test,'$.waptujia001_t_api_cj_pingbi.v')
            end) bucket
    from dws.dws_path_ldbo_d
    --where dt between date_sub(current_date,7) and date_sub(current_date,1) --？
    where dt >= '2025-05-07'
    and user_type = '用户'
    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102'))
    and (
        (wrapper_name='携程' and get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') is not null )
        or (wrapper_name='去哪儿' and  get_json_object(ab_test,'$.waptujia016_q_api_cj_pingbi.v') is not null )
        or (wrapper_name='途家' and  get_json_object(ab_test,'$.waptujia001_t_api_cj_pingbi.v') is not null )
    ) 
    group by 1,2,3
    having bucket=1 
) b 
on a.dt=b.dt 
and lower(a.uid)=lower(b.uid) 
and a.wrapper_name=b.wrapper_name
)
,od as (
select to_date(create_date) dt 
    ,uid 
    ,count(distinct order_no) od_cnt 
    ,sum(order_room_night_count) order_room_night_count	
from dws.dws_order a 
where create_date >= '2025-05-07'
and is_paysuccess_order = 1 --支付成功
and is_overseas = 0 
group by 1,2 
)
,zc_house as (
select house_id
from dws.dws_house_d 
where dt = date_sub(current_date,1)
and landlord_channel = 303
group by 1 
)
,final as (
select 
    ldbo.dt   
    ,ldbo.wrapper_name
    ,ldbo.search_id
    ,ldbo.trace_id
    ,ldbo.rank_trace_id
    ,ldbo.city_id
    ,ldbo.city_name
    ,ldbo.search_city_id
    -- ,ldbo.search_city_name
    ,ldbo.geo_city_id
    ,ldbo.geo_city_name
    ,ldbo.aa
    ,ldbo.bb
    ,ldbo.searchScene
    ,ldbo.area_id
    ,ldbo.black_type
    ,ldbo.search_type
    ,ldbo.geo_position_id
    ,ldbo.check_type
    ,ldbo.house_id
    ,ldbo.hotel_id
    ,ldbo.uid
    ,ldbo.final_price
    ,ldbo.detail_uid
    ,ldbo.distance
    ,ldbo.position
    ,ldbo.max_price
    ,ldbo.min_price
    ,ldbo.without_risk_order_num
    ,ldbo.without_risk_order_gmv
    ,ldbo.without_risk_order_room_night
    ,ldbo.without_risk_access_order_gmv
    ,ldbo.without_risk_access_order_num
    ,ldbo.without_risk_access_order_room_night
    ,ldbo.rank_scene_empty_filter
    ,geo_name.poi_type
    ,geo_name.name
    ,geo_name.destination_id
    ,case when zc_house.house_id is not null then 'C接' else '其他' end house_type
    ,CASE WHEN ab_test.bucket IN ('D','E','F') THEN '实验'
        WHEN ab_test.bucket IN ('B','C') THEN '对照'
    END AS bucket 
    ,case when cansale_num_l21plus_zc>500 then 'L21+直采可售数量高于500'
        when cansale_num_l21plus_zc>200 then 'L21+直采可售数量高于200'
        when cansale_num_l21plus_zc>50 then 'L21+直采可售数量高于50'
        when cansale_num_l21plus_zc>20 then 'L21+直采可售数量高于20'
        else 'L21+直采可售数量低于20' end as type
    ,case when cansale_num_l21plus_zc<50 then 'L21+直采可售数量小于50' else 'L21+直采可售数量高于50' end as type1
    ,case when cansale_num_l21plus_zc<20 then 'L21+直采可售数量小于20' else 'L21+直采可售数量高于20' end as type2
from ldbo 
left join geo_name 
on ldbo.geo_position_id = geo_name.geo_position_id
left join zc_house 
on ldbo.house_id = zc_house.house_id
LEFT JOIN ab_test 
ON ldbo.dt = ab_test.dt AND lower(ldbo.uid)=lower(ab_test.uid) AND ldbo.wrapper_name = ab_test.wrapper_name
left join (
    select distinct dt
    ,search_id
    ,uid
    ,rank_trace_id
    ,city_id
    ,GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(extend_field, '$.rankTagIdCanSaleInfoMap'), '$.102'), '$.canSaleNum') as cansale_num_l21plus_zc
    FROM pdb_analysis_c.ads_flow_list_price_day_d  t1
    WHERE dt >= '2025-05-07'
) cansale_num
on ldbo.dt = cansale_num.dt
and ldbo.search_id = cansale_num.search_id
and lower(ldbo.uid) = lower(cansale_num.uid)
and ldbo.rank_trace_id = cansale_num.rank_trace_id
and ldbo.city_id = cansale_num.city_id
left join od 
on ldbo.dt = od.dt 
and lower(ldbo.uid) = lower(od.uid)
) 


select 
    geo_city_name
    ,poi_type
    ,geo_position_id
    ,name
    ,destination_id
    ,count(distinct concat(dt,uid)) uv 
    ,count(distinct concat(search_id,uid)) spv

    ,count(distinct case when type1 = 'L21+直采可售数量小于50' then concat(search_id,dt) end) / count(distinct concat(search_id,dt)) `L21直采可售数量小于50的请求占比`
    ,count(distinct case when type2 = 'L21+直采可售数量小于20' then concat(search_id,dt) end) / count(distinct concat(search_id,dt)) `L21直采可售数量小于20的请求占比`
    
    ,count(case when house_type = 'C接' then uid end) / count(uid) `接入曝光占比`
    ,sum(case when house_type = 'C接' then without_risk_access_order_room_night end) / sum(without_risk_access_order_room_night) `接入间夜占比`

    ,count(case when type2 = 'L21+直采可售数量小于20' and house_type = 'C接' then uid end) / count(case when type2 = 'L21+直采可售数量小于20' then uid end) `L21直采可售数量小于20的接入曝光占比`
    ,count(case when type1 = 'L21+直采可售数量小于50' and house_type = 'C接' then uid end) / count(case when type1 = 'L21+直采可售数量小于50' then uid end) `L21直采可售数量小于50的接入曝光占比`
    ,sum(case when type2 = 'L21+直采可售数量小于20' and house_type = 'C接' then without_risk_access_order_room_night end) / sum(case when type2 = 'L21+直采可售数量小于20' then without_risk_access_order_room_night end) `L21直采可售数量小于20的接入间夜请求占比`
    ,sum(case when type1 = 'L21+直采可售数量小于50' and house_type = 'C接' then without_risk_access_order_room_night end) / sum(case when type1 = 'L21+直采可售数量小于50' then without_risk_access_order_room_night end) `L21直采可售数量小于50的接入间夜请求占比`

    ,sum(case when bucket = '实验' then without_risk_access_order_num end) / count(distinct case when bucket = '实验' then concat(dt,uid) end) `l2o窄_实验`
    ,sum(case when bucket = '对照' then without_risk_access_order_num end) / count(distinct case when bucket = '对照' then concat(dt,uid) end) `l2o窄_对照`
    ,sum(case when bucket = '实验' then without_risk_access_order_gmv end) / count(distinct case when bucket = '实验' then concat(dt,uid) end) `GMV/UV窄_实验`
    ,sum(case when bucket = '对照' then without_risk_access_order_gmv end) / count(distinct case when bucket = '对照' then concat(dt,uid) end) `GMV/UV窄_对照`

    ,count(case when house_type = 'C接' and bucket = '实验' then uid end) / count(case when bucket = '实验' then uid end) `接入曝光占比_实验`
    ,count(case when house_type = 'C接' and bucket = '对照' then uid end) / count(case when bucket = '对照' then uid end) `接入曝光占比_对照`
    ,sum(case when house_type = 'C接' and bucket = '实验' then without_risk_access_order_room_night end) / sum(case when bucket = '实验' then without_risk_access_order_room_night end) `接入间夜占比_实验`
    ,sum(case when house_type = 'C接' and bucket = '对照' then without_risk_access_order_room_night end) / sum(case when bucket = '对照' then without_risk_access_order_room_night end) `接入间夜占比_对照`
    
    -- 实验
    ,count(case when type2 = 'L21+直采可售数量小于20' and house_type = 'C接' and bucket = '实验' then uid end) / count(case when type2 = 'L21+直采可售数量小于20' and bucket = '实验' then uid end) `L21直采可售数量小于20的接入曝光占比_实验`
    ,count(case when type1 = 'L21+直采可售数量小于50' and house_type = 'C接' and bucket = '实验' then uid end) / count(case when type1 = 'L21+直采可售数量小于50' and bucket = '实验' then uid end) `L21直采可售数量小于50的接入曝光占比_实验`
    ,sum(case when type2 = 'L21+直采可售数量小于20' and house_type = 'C接' and bucket = '实验' then without_risk_access_order_room_night end) / sum(case when type2 = 'L21+直采可售数量小于20' and bucket = '实验' then without_risk_access_order_room_night end) `L21直采可售数量小于20的接入间夜请求占比_实验`
    ,sum(case when type1 = 'L21+直采可售数量小于50' and house_type = 'C接' and bucket = '实验' then without_risk_access_order_room_night end) / sum(case when type1 = 'L21+直采可售数量小于50' and bucket = '实验' then without_risk_access_order_room_night end) `L21直采可售数量小于50的接入间夜请求占比_实验`
    -- 对照
    ,count(case when type2 = 'L21+直采可售数量小于20' and house_type = 'C接' and bucket = '对照' then uid end) / count(case when type2 = 'L21+直采可售数量小于20' and bucket = '对照' then uid end) `L21直采可售数量小于20的接入曝光占比_对照`
    ,count(case when type1 = 'L21+直采可售数量小于50' and house_type = 'C接' and bucket = '对照' then uid end) / count(case when type1 = 'L21+直采可售数量小于50' and bucket = '对照' then uid end) `L21直采可售数量小于50的接入曝光占比_对照`
    ,sum(case when type2 = 'L21+直采可售数量小于20' and house_type = 'C接' and bucket = '对照' then without_risk_access_order_room_night end) / sum(case when type2 = 'L21+直采可售数量小于20' and bucket = '对照' then without_risk_access_order_room_night end) `L21直采可售数量小于20的接入间夜请求占比_对照`
    ,sum(case when type1 = 'L21+直采可售数量小于50' and house_type = 'C接' and bucket = '对照' then without_risk_access_order_room_night end) / sum(case when type1 = 'L21+直采可售数量小于50' and bucket = '对照' then without_risk_access_order_room_night end) `L21直采可售数量小于50的接入间夜请求占比_对照`
from final 
group by 1,2,3,4,5 
having count(distinct concat(dt,uid)) >= 200
