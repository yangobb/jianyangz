
with ab_test as (
select a.*
from (select distinct 
    dt
    ,uid
    ,wrapper_name
    ,case when wrapper_name='携程' then get_json_object(ab_test,'$.wapctripbnb_rank_ecosystem.v')
    when wrapper_name='去哪儿' then get_json_object(ab_test,'$.waptujia016_rank_ecosystem.v')
    when wrapper_name='途家' then get_json_object(ab_test,'$.waptujia001_rank_ecosystem.v')
    end bucket 
    from dws.dws_path_ldbo_d
    where dt between '2025-05-07' and date_sub(current_date,1)
    and user_type = '用户'
    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102'))
    and (
    (wrapper_name='携程' and get_json_object(ab_test,'$.wapctripbnb_rank_ecosystem.v') is not null )
    or (wrapper_name='去哪儿' and  get_json_object(ab_test,'$.waptujia016_rank_ecosystem.v') is not null )
    or (wrapper_name='途家' and  get_json_object(ab_test,'$.waptujia001_rank_ecosystem.v') is not null )
    ) 
    and uid!='visitor000000'
) a 
join 
(
    select
    dt
    ,uid
    ,wrapper_name
    ,count(distinct 
    case when wrapper_name='携程' then get_json_object(ab_test,'$.wapctripbnb_rank_ecosystem.v')
    when wrapper_name='去哪儿' then get_json_object(ab_test,'$.waptujia016_rank_ecosystem.v')
    when wrapper_name='途家' then get_json_object(ab_test,'$.waptujia001_rank_ecosystem.v')
    end) bucket
    from dws.dws_path_ldbo_d
    where dt between '2025-05-07' and date_sub(current_date,1)
    and user_type = '用户'
    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102'))
    and (
    (wrapper_name='携程' and get_json_object(ab_test,'$.wapctripbnb_rank_ecosystem.v') is not null )
    or (wrapper_name='去哪儿' and  get_json_object(ab_test,'$.waptujia016_rank_ecosystem.v') is not null )
    or (wrapper_name='途家' and  get_json_object(ab_test,'$.waptujia001_rank_ecosystem.v') is not null )
    ) 
    group by 1,2,3
    having bucket=1 
) b on a.dt=b.dt and a.uid=b.uid and a.wrapper_name=b.wrapper_name
)
,house_info as 
(
    select t1.*
    ,case when t2.house_id is not null then '海外宝藏' else '其他' end as is_baozang
    ,case when t3.house_id is not null then '海外真视频' else '其他' end as is_shiping
    ,case when t4.house_id is not null then '海外优选' else '其他' end as is_youxuan
    from
    (
        select distinct
        t1.dt,
        house_city_name,
        house_city_id as city_id,
        t1.house_id,
        hotel_id,
        valid_comment_num as comment_count,
        comment_score,
        favoritecount,
        great_tag,
        is_prefer_pro as is_yanxuan,
        hotel_level,
        is_prefer,
        bedroom_count,
        landlord_channel,
        t1.dynamic_business,
        t1.dynamic_business_id,
        case when landlord_channel=303 then '携程接入'
        when landlord_channel=1 then '直采'
        else '其他接入' end as hs_type,
        instance_count,
        share_type,
        house_type,
        recommended_guest,
        house_level,
        house_class
        from dws.dws_house_d t1
        where t1.dt between '2025-05-07' and date_sub(current_date,1)
        and house_is_oversea = '1'
        and house_is_online = 1
        and house_city_name in ('首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','香港','澳门','新加坡')
    ) as t1
    left join
    (
        SELECT distinct house_id
        ,dt
        from pdb_analysis_b.dwd_house_label_1000488_d
        where dt between '2025-05-07' and date_sub(current_date,1)
    ) as t2
    on t1.dt=t2.dt
    and t1.house_id=t2.house_id

    left join
    (
        SELECT distinct house_id
        ,dt
        from pdb_analysis_b.dwd_house_label_1000487_d
        where dt between '2025-05-07' and date_sub(current_date,1)
    ) as t4
    on t1.dt=t4.dt
    and t1.house_id=t4.house_id
    left join
    (
        select distinct bb.house_id
        from 
        (
          select distinct unitnumber
          ,auditvideo,auditpassvideo
          from ods_merchantcrm.houseunitedit
          where auditvideo is not null and auditvideo != '{ }' and auditpassvideo is not null and auditpassvideo != '{ }'
        ) aa
        join 
        (
          select distinct house_number
          ,house_id
          ,house_name
          ,hotel_name
          ,country_name
          ,house_class
          ,house_city_name
          ,landlord_channel_name 
          FROM dws.dws_house_d
          WHERE dt = date_sub(current_date,2)
          AND house_is_oversea = 1
          AND house_is_online = 1
          AND house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') 
          AND landlord_channel_name in ('平台商户')
        )bb
        on aa.unitnumber=bb.house_number
    ) as t3
    on t1.house_id=t3.house_id
)
,list as
(
    select a.*  
    ,b.house_class
    ,b.is_baozang
    ,b.is_shiping
    ,b.hs_type
    ,is_youxuan
    ,case when t2.bucket in ('B','C','D','E','F') then '实验'
    when t2.bucket in ('G','H','I','J','K') then '对照'
    end as bucket
    from 
        (
        select  
        t1.dt
        ,t1.wrapper_name
        ,t1.trace_id
        ,city_id
        ,case when city_name = '陵水' then '陵水(三亚)' else city_name end as city_name 
        ,city_level
        ,t1.house_id
        ,t1.uid
        ,geo_position_id
        ,location_filter as geo_name
        ,location_type as geo_type
        ,final_price
        ,detail_uid
        ,distance
        ,position
        ,rank_scene_empty_filter
        ,dynamic_business
        ,dynamic_business_id
        ,search_id
        ,checkin_date
        ,checkout_date
        ,case when datediff(checkin_date,t1.dt)=0 or datediff(checkin_date,t1.dt)=-1 then 'T0'
              else 'TN' 
        end as check_type
        ,case when pmod(datediff(checkin_date,'1900-01-08'),7)+1 in (5,6) then '周末'
              when pmod(datediff(checkin_date,'1900-01-08'),7)+1 not in (5,6) then '周中'
        end as week_type
        ,case when bedroom_count=1 then '一居'
              when bedroom_count=2 then '二居'
              when bedroom_count>=3 then '三居+'
        end as room_type
        ,case when logic_bit & 2048 = 2048 then 1 
              else 0 end as position_type
        ,without_risk_order_num
        ,without_risk_order_room_night
        ,without_risk_order_gmv
        ,without_risk_access_order_gmv
        ,without_risk_access_order_num
        ,without_risk_access_order_room_night
        ,get_json_object(server_log,'$.hasUserClickBehavior') as if_click
        from dws.dws_path_ldbo_d t1
        where t1.dt between '2025-05-07' and date_sub(current_date,1)
        and  (t1.wrapper_name in ('途家','携程','去哪儿') and source = '102') 
        and user_type = '用户'
        and city_name in ('京都','东京','大阪','清迈','芭堤雅','曼谷','普吉岛')
        ) as a 
        join house_info as b on a.dt = b.dt and a.house_id = b.house_id
        left join ab_test as t2
        on a.dt=t2.dt
        and a.uid=t2.uid
        and a.wrapper_name=t2.wrapper_name
)
,ord as 
(
    select 
    a.*
    ,b.house_class 
    ,b.is_baozang
    from 
    (
        select 
        case   
        when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
        when terminal_type_name = '本站-APP' then '途家'
        when terminal_type_name = '携程-APP' then '携程'
        when terminal_type_name = '去哪儿-APP' then '去哪儿'
        end as wrapper_name
        ,city_id
        ,case when datediff(checkin_date,create_date)=0 or datediff(checkin_date,create_date)=-1 then 'T0'
                else 'TN' 
        end as check_type
        ,case when city_name = '陵水' then '陵水(三亚)' else city_name end as city_name 
        ,create_date as dt
        ,house_id
        ,uid
        ,order_no
        ,room_total_amount
        ,order_room_night_count
        ,dynamic_business
        ,checkout_date
        from dws.dws_order 
        where create_date between '2025-05-07' and date_sub(current_date,1)
        and is_paysuccess_order = 1 
        and terminal_type_name in ('携程-APP','去哪儿-APP','本站-APP') 
        and is_overseas =1 
        and city_name in ('京都','东京','大阪','清迈','芭堤雅','曼谷','普吉岛')
    ) as a 
    join house_info as b on a.dt = b.dt and a.house_id = b.house_id
)


select t1.type,
    concat(round(((ord_k_sy/uv_sy)/(ord_k_dz/uv_dz)-1)*100,2),'%') AS `L2O_UV-AB`,
    concat(round(((gmv_k_sy/uv_sy)/(gmv_k_dz/uv_dz)-1)*100,2),'%') AS `GMV_UV-AB`,
    concat(round(((gmv_k_sy/night_k_sy)/(gmv_k_dz/night_k_dz)-1)*100,2),'%') AS `ADR-AB`,

    concat(round((pv_sy_pro/pv_sy)*100,2),'%') AS `宝藏房屋流量占比-实验`,
    concat(round((pv_dz_pro/pv_dz)*100,2),'%') AS `宝藏房屋流量占比-空白`,
    concat(round(((pv_sy_pro/pv_sy)/(pv_dz_pro/pv_dz)-1)*100,2),'%') AS `宝藏房屋流量占比-AB`,

    concat(round((ord_sy_pro/ord_sy)*100,2),'%') AS `宝藏房屋订单占比-实验`,
    concat(round((ord_dz_pro/ord_dz)*100,2),'%') AS `宝藏房屋订单占比-空白`,
    concat(round(((ord_sy_pro/ord_sy)/(ord_dz_pro/ord_dz)-1)*100,2),'%') AS `宝藏房屋订单占比-AB`,

    concat(round((gmv_sy_pro/gmv_sy)*100,2),'%') AS `宝藏房屋GMV占比-实验`,
    concat(round((gmv_dz_pro/gmv_dz)*100,2),'%')  AS `宝藏房屋GMV占比-空白`,
    concat(round(((gmv_sy_pro/gmv_sy)/(gmv_dz_pro/gmv_dz)-1)*100,2),'%')  AS `宝藏房屋GMV占比-AB`,


    concat(round((pv_sy_youxuan/pv_sy)*100,2),'%') AS `优选房屋流量占比-实验`,
    concat(round((pv_dz_youxuan/pv_dz)*100,2),'%') AS `优选房屋流量占比-空白`,
    concat(round(((pv_sy_youxuan/pv_sy)/(pv_dz_youxuan/pv_dz)-1)*100,2),'%') AS `优选房屋流量占比-AB`,

    concat(round((ord_sy_youxuan/ord_sy)*100,2),'%') AS `优选房屋订单占比-实验`,
    concat(round((ord_dz_youxuan/ord_dz)*100,2),'%') AS `优选房屋订单占比-空白`,
    concat(round(((ord_sy_youxuan/ord_sy)/(ord_dz_youxuan/ord_dz)-1)*100,2),'%') AS `优选房屋订单占比-AB`,

    concat(round((gmv_sy_youxuan/gmv_sy)*100,2),'%') AS `优选房屋GMV占比-实验`,
    concat(round((gmv_dz_youxuan/gmv_dz)*100,2),'%')  AS `优选房屋GMV占比-空白`,
    concat(round(((gmv_sy_youxuan/gmv_sy)/(gmv_dz_youxuan/gmv_dz)-1)*100,2),'%')  AS `优选房屋GMV占比-AB`,


    concat(round((pv_sy_video/pv_sy)*100,2),'%') AS `真视频流量占比-实验`,
    concat(round((pv_dz_video/pv_dz)*100,2),'%') AS `真视频流量占比-空白`,
    concat(round(((pv_sy_video/pv_sy)/(pv_dz_video/pv_dz)-1)*100,2),'%') AS `真视频流量占比-AB`,

    concat(round((ord_sy_video/ord_sy)*100,2),'%') AS `真视频订单占比-实验`,
    concat(round((ord_dz_video/ord_dz)*100,2),'%') AS `真视频订单占比-空白`,
    concat(round(((ord_sy_video/ord_sy)/(ord_dz_video/ord_dz)-1)*100,2),'%') AS `真视频订单占比-AB`,

    concat(round((gmv_sy_video/gmv_sy)*100,2),'%') AS `真视频GMV占比-实验`,
    concat(round((gmv_dz_video/gmv_dz)*100,2),'%')  AS `真视频GMV占比-空白`,
    concat(round(((gmv_sy_video/gmv_sy)/(gmv_dz_video/gmv_dz)-1)*100,2),'%')  AS `真视频GMV占比-AB`,

    concat(round((pv_sy_l34/pv_sy)*100,2),'%') AS `L34房屋流量占比-实验`,
    concat(round((pv_dz_l34/pv_dz)*100,2),'%') AS `L34房屋流量占比-空白`,
    concat(round(((pv_sy_l34/pv_sy)/(pv_dz_l34/pv_dz)-1)*100,2),'%') AS `L34房屋流量占比-AB`,

    concat(round((ord_sy_l34/ord_sy)*100,2),'%') AS `L34房屋订单占比-实验`,
    concat(round((ord_dz_l34/ord_dz)*100,2),'%') AS `L34房屋订单占比-空白`,
    concat(round(((ord_sy_l34/ord_sy)/(ord_dz_l34/ord_dz)-1)*100,2),'%') AS `L34房屋订单占比-AB`,

    concat(round((gmv_sy_l34/gmv_sy)*100,2),'%') AS `L34房屋GMV占比-实验`,
    concat(round((gmv_dz_l34/gmv_dz)*100,2),'%')  AS `L34房屋GMV占比-空白`,
    concat(round(((gmv_sy_l34/gmv_sy)/(gmv_dz_l34/gmv_dz)-1)*100,2),'%')  AS `L34房屋GMV占比-AB`,

    ord_k,
    ord_k_sy


from
(
    select city_name as type
    ,count(distinct concat(dt,uid)) as uv
    ,count(uid) as pv
    ,count(if(bucket='实验',concat(dt,uid),null)) as pv_sy
    ,count(if(bucket='对照',concat(dt,uid),null)) as pv_dz
    ,count(distinct if(bucket='实验',concat(dt,uid),null)) as uv_sy
    ,count(distinct if(bucket='对照',concat(dt,uid),null)) as uv_dz
    ,sum(if(bucket='实验',without_risk_order_num,0)) as ord_sy
    ,sum(if(bucket='对照',without_risk_order_num,0)) as ord_dz
    ,sum(if(bucket='实验',without_risk_order_gmv,0)) as gmv_sy
    ,sum(if(bucket='对照',without_risk_order_gmv,0)) as gmv_dz
    ,sum(if(bucket='实验',without_risk_order_room_night,0)) as night_sy
    ,sum(if(bucket='对照',without_risk_order_room_night,0)) as night_dz

    ,count(if(bucket='实验' and is_baozang='海外宝藏',concat(dt,uid),null)) as pv_sy_pro
    ,count(if(bucket='对照' and is_baozang='海外宝藏',concat(dt,uid),null)) as pv_dz_pro
    ,sum(if(bucket='实验' and is_baozang='海外宝藏',without_risk_order_num,0)) as ord_sy_pro
    ,sum(if(bucket='对照' and is_baozang='海外宝藏',without_risk_order_num,0)) as ord_dz_pro
    ,sum(if(bucket='实验' and is_baozang='海外宝藏',without_risk_order_gmv,0)) as gmv_sy_pro
    ,sum(if(bucket='对照' and is_baozang='海外宝藏',without_risk_order_gmv,0)) as gmv_dz_pro
    ,sum(if(bucket='实验' and is_baozang='海外宝藏',without_risk_order_room_night,0)) as night_sy_pro
    ,sum(if(bucket='对照' and is_baozang='海外宝藏',without_risk_order_room_night,0)) as night_dz_pro

    ,count(if(bucket='实验' and is_shiping='海外真视频',concat(dt,uid),null)) as pv_sy_video
    ,count(if(bucket='对照' and is_shiping='海外真视频',concat(dt,uid),null)) as pv_dz_video
    ,sum(if(bucket='实验' and is_shiping='海外真视频',without_risk_order_num,0)) as ord_sy_video
    ,sum(if(bucket='对照' and is_shiping='海外真视频',without_risk_order_num,0)) as ord_dz_video
    ,sum(if(bucket='实验' and is_shiping='海外真视频',without_risk_order_gmv,0)) as gmv_sy_video
    ,sum(if(bucket='对照' and is_shiping='海外真视频',without_risk_order_gmv,0)) as gmv_dz_video
    ,sum(if(bucket='实验' and is_shiping='海外真视频',without_risk_order_room_night,0)) as night_sy_video
    ,sum(if(bucket='对照' and is_shiping='海外真视频',without_risk_order_room_night,0)) as night_dz_video


    ,count(if(bucket='实验' and house_class in ('L3','L4'),concat(dt,uid),null)) as pv_sy_l34
    ,count(if(bucket='对照' and house_class in ('L3','L4'),concat(dt,uid),null)) as pv_dz_l34
    ,sum(if(bucket='实验' and house_class in ('L3','L4'),without_risk_order_num,0)) as ord_sy_l34
    ,sum(if(bucket='对照' and house_class in ('L3','L4'),without_risk_order_num,0)) as ord_dz_l34
    ,sum(if(bucket='实验' and house_class in ('L3','L4'),without_risk_order_gmv,0)) as gmv_sy_l34
    ,sum(if(bucket='对照' and house_class in ('L3','L4'),without_risk_order_gmv,0)) as gmv_dz_l34
    ,sum(if(bucket='实验' and house_class in ('L3','L4'),without_risk_order_room_night,0)) as night_sy_l34
    ,sum(if(bucket='对照' and house_class in ('L3','L4'),without_risk_order_room_night,0)) as night_dz_l34


    ,count(if(bucket='实验' and is_youxuan='海外优选',concat(dt,uid),null)) as pv_sy_youxuan
    ,count(if(bucket='对照' and is_youxuan='海外优选',concat(dt,uid),null)) as pv_dz_youxuan
    ,sum(if(bucket='实验' and is_youxuan='海外优选',without_risk_order_num,0)) as ord_sy_youxuan
    ,sum(if(bucket='对照' and is_youxuan='海外优选',without_risk_order_num,0)) as ord_dz_youxuan
    ,sum(if(bucket='实验' and is_youxuan='海外优选',without_risk_order_gmv,0)) as gmv_sy_youxuan
    ,sum(if(bucket='对照' and is_youxuan='海外优选',without_risk_order_gmv,0)) as gmv_dz_youxuan
    ,sum(if(bucket='实验' and is_youxuan='海外优选',without_risk_order_room_night,0)) as night_sy_youxuan
    ,sum(if(bucket='对照' and is_youxuan='海外优选',without_risk_order_room_night,0)) as night_dz_youxuan
    from 
    (
        select * from list
    ) as aa 
    group by 1
) as t1 
--获取订单
left join
(
    select
   city_name as type
    ,sum(order_room_night_count) as night_k
    ,sum(room_total_amount) as gmv_k
    ,count(order_no) as ord_k
    ,sum(if(bucket='实验',order_room_night_count,null)) as night_k_sy
    ,sum(if(bucket='实验',room_total_amount,null)) as gmv_k_sy
    ,count(if(bucket='实验',order_no,null)) as ord_k_sy
    ,sum(if(bucket='对照',order_room_night_count,null)) as night_k_dz
    ,sum(if(bucket='对照',room_total_amount,null)) as gmv_k_dz
    ,count(if(bucket='对照',order_no,null)) as ord_k_dz
    from 
    (
        select * 
        from ord 
    ) as cc 
    left join 
    (
        select distinct 
        uid,
        bucket,
        wrapper_name,
        dt
        from list 
        group by 1,2,3,4
    ) as aa
    on LOWER(cc.uid)=LOWER(aa.uid)
    and cc.dt=aa.dt
    and cc.wrapper_name=aa.wrapper_name
    group by 1
) as t2 
on t1.type = t2.type
order by ord_k desc

