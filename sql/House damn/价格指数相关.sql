select a.wrapper_name
,a.trace_id
,a.city_id
,a.city_name 
,a.city_level
,a.house_id
,a.uid
,a.loc_or_other
,a.price_type
,a.geo_position_id
,a.geo_name
,a.geo_type
,a.final_price
,a.detail_uid
,a.distance
,a.position
,a.rank_scene_empty_filter
,a.dynamic_business
,a.dynamic_business_id
,a.search_id
,a.trace_only
,a.checkin_date
,a.checkout_date
,a.bucket
,a.check_type
,a.week_type
,a.peek
,a.houseClassScene
,a.releaseCpcDistance
,a.search_type
,b.room_type
,a.position_type
,a.without_risk_order_num
,a.without_risk_order_room_night
,a.without_risk_order_gmv  
,b.house_class
,b.is_prefer_pro
,b.is_special
,b.leveldesc
,a.bucket_discount
,c.realprice
,c.recommandprice
,c.selfpricerise
,c.basepricerise
,c.pricerise
,c.pricerisescore
,c.class_weight_score
,c.scarcehousescore
,c.longtermdowngradescore
,c.userclickscore
,c.poiscore
,c.userlevelpricescore
,a.search_city_id
,c.lastntimeuserclickpriceavg
,c.extend_field
,c.beforescore
,c.s_score
,a.if_click
,a.ab_test
,a.without_risk_access_order_num
,a.without_risk_access_order_room_night
,a.without_risk_access_order_gmv
,a.without_risk_access_order_no
,a.rank_trace_id
,c.userclicksize
,c.qpriceprefer
,c.extend
,a.user_id
,a.fromforlog
,a.server_log
,case when t3.uid is not null then '高价值' 
when t4.user_id is not null then '低价值'
else '其他' end as user_type
,case when t3.uid is not null then new_type
when t4.user_id is not null then '低价值'
else '其他' end as highvalue_type
,case when get_json_object(extend_field,'$.lastNTimeUserClickPeopleMode')=recommended_guest
and get_json_object(extend_field,'$.lastNTimeUserClickBedNumMode')=bedcount
and get_json_object(extend_field,'$.lastNTimeUserClickHouseModelMode')=bedroom_count
and final_price/get_json_object(extend_field,'$.lastNTimeUserClickPriceAvg')>=0.8
and final_price/get_json_object(extend_field,'$.lastNTimeUserClickPriceAvg')<=1.2 then '命中人床居价个性化' else '其他' 
end as is_personal_house
,b.is_zhenxuan
,case when check_type='TN' and day_type='非节假日' then 'TN-非节假日'
when check_type='TN' and day_type<>'非节假日' then 'TN-节假日'
when check_type= 'T0' then 'T0'
end as day_type
,citytype2
,t9.bucket as bucket_eco
,advert
,advert_type

,a.dt
from 
(
    select t1.dt
    ,t1.wrapper_name
    ,t1.trace_id
    ,city_id
    ,case when city_name = '陵水' then '陵水(三亚)' else city_name end as city_name 
    ,city_level
    ,t1.house_id
    ,t1.uid
    ,t1.search_city_id
    ,case when locate_city_id is not null and city_id is not null and locate_city_id=city_id then '本地'
    when locate_city_id is not null and city_id is not null and locate_city_id<>city_id then '异地'
    else null end as loc_or_other
    ,case 
    when final_price <=  100 then 100
    when final_price <=  150 then 150
    when final_price <=  200 then 200
    when final_price <=  250 then 250
    when final_price <=  300 then 300
    when final_price <=  500 then 500
    else '501' end as price_type
    ,geo_position_id
    ,location_filter as geo_name
    ,location_type as geo_type
    ,final_price
  	,user_id
  	,fromforlog
    ,detail_uid
    ,distance
    ,position
    ,rank_scene_empty_filter
    ,dynamic_business
    ,dynamic_business_id
    ,search_id
    ,concat(t1.wrapper_name,search_id,t1.trace_id) as trace_only
    ,checkin_date
    ,checkout_date
    ,get_json_object(bucket,'$.bucketClassWeight') as bucket
    ,get_json_object(bucket,'$.bucketDiscount') as bucket_discount
    ,case when datediff(checkin_date,t1.dt)=0 or datediff(checkin_date,t1.dt)=-1 then 'T0'
          else 'TN' 
    end as check_type
    ,case when pmod(datediff(checkin_date,'1900-01-08'),7)+1 in (5,6) then '周末'
          when pmod(datediff(checkin_date,'1900-01-08'),7)+1 not in (5,6) then '周中'
    end as week_type
    ,get_json_object(server_log,'$.usePeekClassScore') as peek
    ,get_json_object(server_log,'$.houseClassScene') as houseClassScene
    ,get_json_object(server_log,'$.releaseCpcDistance') as  releaseCpcDistance
    ,case 
    when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
    when get_json_object(server_log,'$.searchScene') = 2 then '空搜'
    when get_json_object(server_log,'$.searchScene') = 3 then '景区'
    when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
    when get_json_object(server_log,'$.searchScene') = 5 then '地标'
    when get_json_object(server_log,'$.searchScene') = 6 then '定位'
    when get_json_object(server_log,'$.searchScene') = 8 then '县级市空搜'
    when get_json_object(server_log,'$.searchScene') = 0 then '无' 
    end as search_type
    ,case when bedroom_count=1 then '一居'
    when bedroom_count=2 then '二居'
    when bedroom_count>=3 then '三居+'
    end as room_type
    ,case when logic_bit & 2048 = 2048 then 1 
    else 0 end as position_type
    ,without_risk_order_num
    ,without_risk_order_room_night
    ,without_risk_order_gmv
    ,without_risk_access_order_num
    ,without_risk_access_order_room_night
    ,without_risk_access_order_gmv
    ,without_risk_access_order_no
    ,rank_trace_id
  	,get_json_object(server_log,'$.hasUserClickBehavior') as if_click
  	,ab_test
  	,server_log
    ,advert
    ,advert_type
    from dws.dws_path_ldbo_d t1
    where t1.dt="${partition}"
    and  (t1.wrapper_name in ('途家','携程','去哪儿') and source = '102') 
    and user_type = '用户'
    --and get_json_object(server_log,'$.rankInvokeType')=1
) as a 
join
(
    select t1.house_id,
    dynamic_business_id,
    dynamic_business,
    house_class,
    instance_count,
    case when hotel_level='4' then '超赞房东'
    when hotel_level='3' then '人气房东'
    when hotel_level='2' then '成长房东'
    when hotel_level='1' then '潜力房东'
    else '其他' end as leveldesc,
    case when is_prefer_pro=1 then '严选' else '其他' end as is_prefer_pro,
    case when is_prefer=1 then '优选' else '其他' end as is_special,
    case when landlord_channel=1 then '直采'
    else  '接入'  end as hs_type,
    cast(recommended_guest as int) as recommended_guest,
    cast(bedroom_count as int) as bedroom_count,
    cast(bedcount as int) as bedcount,
    case when great_tag=1 then '臻选' else '其他' end as is_zhenxuan,
    case when bedroom_count<=1 then '一居'
    when bedroom_count=2 then '二居'
    when bedroom_count>=3 then '三居+'
    end as room_type,
    t1.dt
    from dws.dws_house_d as t1
    where dt="${partition}"
    and house_is_online=1
    and house_is_oversea='0' 
) as b
on a.dt = b.dt 
and a.house_id = b.house_id
join
(
    select *
    ,get_json_object(extend_field,'$.lastNTimeUserClickPriceAvg') as lastntimeuserclickpriceavg
    ,get_json_object(extend_field,'$.userClickSize') as userclicksize
    ,get_json_object(extend_field,'$.qPricePrefer') as qpriceprefer
    from dwd.dwd_flow_rank_weighting_d
    where dt="${partition}"
) as c
on a.dt = c.dt
and a.search_id = c.search_id
and a.rank_trace_id = c.rank_trace_id 
and a.house_id = c.house_id
left join
(
    select *
    from pdb_analysis_c.ads_flow_highlevel_uids_day_d
    where user_type=1
    and dt=date_sub("${partition}",1)
) as t3
on LOWER(a.uid)=LOWER(t3.uid)
left join 
(
    select distinct user_id
    from
    (
        select user_id
        from pdb_analysis_c.ads_flow_tj_uid_adr_d
        where adr_1 <= 150
        union
        select user_id
        from pdb_analysis_c.ads_flow_hotel_user_price_d
        where cast(price_middle as double) <= 150
        and dt="${partition}"
    ) as t4
) t4
on a.user_id = t4.user_id
left join
(
    select *
    from pdb_analysis_c.ads_flow_dim_date_info_d
) as t7
on a.checkout_date=t7.day_date
left join
(
    select city_id
    ,city_name
    ,citytype2
    from excel_upload.citytype2024
)t8
on a.city_id=t8.city_id
left join
(
    select dt
    ,uid
    ,wrapper_name
    ,bucket
    from pdb_analysis_c.ads_flow_ecosystem_uid_ab_d
    where dt="${partition}"
) as t9
on a.dt=t9.dt
and a.uid=t9.uid
and a.wrapper_name=t9.wrapper_name