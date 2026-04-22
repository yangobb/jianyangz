with h as (
select h.house_city_name
    ,h.hotel_id 
    ,h.house_id
    ,h.house_class
    ,h.landlord_channel
    ,h.bedroom_count 
    ,case when t1.superior_tag = 1 then 1 else 0 end is_youxuan
    ,case when t1.yx_verify_tag is not null then 1 else 0 end is_baozang
    ,case when t1.great_tag is not null then 1 else 0 end is_zhenshipin
from (
    select house_id
        ,hotel_id 
        ,house_class
        ,house_city_name
        ,case when landlord_channel = 1 then '直采'
            else 'C接' end landlord_channel
        ,case when bedroom_count = 1 then '一居' 
            when bedroom_count = 2 then '二居'
            when bedroom_count >=3 then '三居以上' end bedroom_count
    from dws.dws_house_d 
    where dt = date_sub(next_day(current_date(), 'SU'), 7)
    and house_is_online = 1 
    and house_is_oversea = 0
) h
left join (
    select house_id 
        ,superior_tag --优选
        ,yx_verify_tag -- 严选
        ,great_tag	-- 臻选
    from pdb_analysis_c.dwd_house_sanxuan_d
    where dt = date_sub(current_date,1)
) t1
ON h.house_id = t1.house_id
)
,od as (
select h.house_city_name
    ,h.hotel_id
    ,h.house_id
    ,h.house_class
    ,h.landlord_channel
    ,h.bedroom_count
    ,h.is_youxuan
    ,h.is_baozang
    ,h.is_zhenshipin
    ,order_no
    ,a.order_room_night_count night 
    ,a.room_total_amount gmv 
    ,create_date dt  
from (
    select *
    from dws.dws_order
    where create_date between date_sub(next_day(current_date(), 'SU'), 35) and date_sub(next_day(current_date(), 'SU'), 7) 
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
) a 
inner join h on a.house_id = h.house_id
)
,ldbo as (
select h.house_city_name
    ,h.hotel_id
    ,h.house_id
    ,h.house_class
    ,h.landlord_channel
    ,h.bedroom_count
    ,h.is_youxuan
    ,h.is_baozang
    ,h.is_zhenshipin
    ,a.T0Tn
    ,a.empty_filter
    ,a.dt
    ,a.geo_position_id
    ,a.final_price
    ,without_risk_order_num order_num
    ,without_risk_order_room_night night
    ,without_risk_order_gmv gmv
    ,without_risk_order_gmv / without_risk_order_room_night adr
    ,uid
    ,detail_uid
    ,booking_uid
    ,case when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
        when get_json_object(server_log,'$.searchScene') = 2 then '空搜'
        when get_json_object(server_log,'$.searchScene') = 3 then '景区'
        when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
        when get_json_object(server_log,'$.searchScene') = 5 then '地标'
        when get_json_object(server_log,'$.searchScene') = 6 then '定位'
        when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
        when get_json_object(server_log,'$.searchScene') = 8 then '行政区'
        when get_json_object(server_log,'$.searchScene') = 0 then '无' 
        end as search_type 
from (
    select  
         case when checkin_date = dt then 'T0'
            when datediff(checkin_date,dt) <= 7 then 'T7'
            when datediff(checkin_date,dt) <= 14 then 'T14'
            else 'T14+' end T0Tn 
        ,*
    from dws.dws_path_ldbo_d 
    where dt between date_sub(next_day(current_date(), 'SU'), 35) and date_sub(next_day(current_date(), 'SU'), 7)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 0
    and user_type = '用户' 
) a 
inner join h
on a.house_id = h.house_id
)
-- 汇总
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,'汇总' type_filer
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo 
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,'汇总' type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer
union all 
-- 城市 + c接直采
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,landlord_channel type_filer
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo  
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,landlord_channel type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od 
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer
union all 
-- 城市 + 等级
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,house_class type_filer
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo  
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,house_class type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od 
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer
union all 
-- 城市 + 居室
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,bedroom_count type_filer
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo  
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,bedroom_count type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od 
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer 
union all
-- 城市 + 优选
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,'优选' type_filer 
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo  
    where is_youxuan = 1 
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,'优选' type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od 
    where is_youxuan = 1 
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer
union all 
-- 城市 + 宝藏
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,'宝藏' type_filer 
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo  
    where is_baozang = 1 
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,'宝藏' type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od 
    where is_baozang = 1 
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer
union all 
-- 城市 + 真视频
select a.week1 
    ,a.area_name
    ,a.type_filer
    ,lpv 
    ,luv 
    ,dpv
    ,duv
    ,bpv
    ,buv
    ,order_num_z
    ,night_z
    ,gmv_z

    ,order_num_k
    ,night_k
    ,gmv_k
from (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name
        ,'真视频' type_filer 
        ,count(1) lpv 
        ,count(distinct uid) luv 
        ,count(case when detail_uid is not null then 1 end) dpv
        ,count(distinct case when detail_uid is not null then uid end) duv
        ,count(case when booking_uid is not null then 1 end) bpv
        ,count(distinct case when booking_uid is not null then uid end) buv
        ,sum(order_num) order_num_z
        ,sum(night) night_z
        ,sum(gmv) gmv_z
    from ldbo  
    where is_zhenshipin = 1 
    group by 1,2,3
) a 
left join (
    select concat("W",weekofyear(dt)) week1 
        ,'汇总' area_name 
        ,'真视频' type_filer 
        ,count(distinct order_no) order_num_k
        ,sum(night) night_k
        ,sum(gmv) gmv_k
    from od 
    where is_zhenshipin = 1 
    group by 1,2,3   
) b
on a.week1 = b.week1 and a.area_name = b.area_name and a.type_filer = b.type_filer