select '' id 
	,uid
	,channel
    ,biz_data
    ,dt
    ,'' create_date
from (
  
  select 
	uid 
    ,channel
    ,collect_set(biz_data) biz_data
    ,max(dt) dt 
from (
select 
ctripuid uid 
,'2' channel 
,udf.object_to_string(map('uid', ctripuid,
		'cityId', cityid,
        'cityName', cityname,
        'cityPinyin',city_pinyin,
		'houseId',house_id,
		'scene',scene,
		'conditions','',
        'price','final_price_tn',
        'actTime',add_time
	)) as biz_data
,current_date dt 
, date_format(from_unixtime(unix_timestamp()), 'yyyy-MM-dd HH:mm:ss') 

from (
select a.uid
    ,mp.ctripuid 
    ,a.house_id
    ,a.add_time
    ,b.final_price_tn
    
    ,d.city_id cityid
    ,d.city_name cityname 
    ,d.city_pinyin

    ,scene
    ,rn 
from (
    select *
        ,row_number() over(partition by uid order by add_time desc) rn 
    from (
        seLect user_id uid 
            ,house_id 
            ,'sc1001' scene
            ,max(date(add_time)) as sc_dt
            ,max(add_time) add_time
        from dwd.dwd_favorite_d
        where dt=date_sub(current_date,1)
        and date(add_time) between date_sub(current_date,50) and date_sub(current_date,20)
        group by 1,2,3
        -- union all 
        -- seLect user_id uid 
        --     ,house_id 
        --     ,'sc002' scene
        --     ,max(date(add_time)) as sc_dt
        --     ,max(add_time) add_time
        -- from dwd.dwd_favorite_d
        -- where dt=date_sub(current_date,1)
        -- and date(add_time) between date_sub(current_date,7) and date_sub(current_date,1)
        -- group by 1,2,3
    ) a 
) a 

join (
    select distinct member_id
        ,third_id ctripuid
    from ods_tujia_member.third_user_mapping
    where channel_code ='CtripId' 
) mp 
on a.uid = mp.member_id
left join (
    SELECT house_id
        ,user_id uid 
        ,dt
        ,city_name 
        ,avg(final_price) as final_price_tn
    FROM dws.dws_path_ldbo_d 
    where dt between date_sub(current_date,50) and date_sub(current_date,20)
    and  front_display='true'  
    AND  wrapper_name in ('携程','去哪儿','途家')
    and client_name = 'APP' 
    and user_type = '用户'
    group by 1,2,3,4 
) b 
on a.uid = b.uid 
and a.house_id = b.house_id
and a.sc_dt = b.dt  
left join (
    select user_id uid
    from dws.dws_order
    where checkin_date between date_sub(current_date,1) and date_add(current_date,2)
    and is_paysuccess_order = 1 --支付成功
    and is_overseas = 0 --国内
    and is_risk_order = 0 --非风控
    and is_cancel_order=0 --非取消
    group by 1 
) c
on a.uid = c.uid 
left join (
    select house_id
        ,house_city_name city_name
    from dws.dws_house_d 
    where dt = date_sub(current_date,1)
) a1 
on a.house_id = a1.house_id
left join (
    select province_id
        ,province_name
        ,city_id
        ,city_name	
        ,city_pinyin
    from tujia_dim.dim_region	 
    where is_oversea = 0 
    group by 1,2,3,4,5 
) d 
on a1.city_name = d.city_name 
where c.uid is null
and  rn <= 10
) a 

) a 
group by 1,2
) a 
 