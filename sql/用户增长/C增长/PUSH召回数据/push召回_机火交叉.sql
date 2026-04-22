with fafang as (
select
    create_time,
    user_id,
    promo_code, --卡券包编号 防止被遍历
    activity_type,
    activity_code, --红包活动ID
    ActivityChannelID, --渠道名
    flow_status,--fromStatus->toStatus:0->10发放，10→40 使用，40→10 回退，10→50 过期,10→60 作废 ,10→10 使用失败
    enumprovidertype, -- 承担方类型
    enumpromopartytype --识红包是哪类的,0途家，1去哪儿，2携程，flag值
from ods_tujiaonlinepromo.promo --红包发放明细 
where activity_code in ('rp_CHhil4Pz9Pm','rp_CHvGLl4Bnh2')
and to_date(create_time) between date_sub(current_date,30) and date_sub(current_date,1)
and flow_status = 10
),
promo_use as (
select
*
from (
    select
    *,
    row_number() over(partition by promo_code order by update_time,action desc) rk --同一promo_code可能有多个订单(退回),同一订单的同一红包可能部分退回，取最后的状态
    from(
        select
            promo_code,
            order_num,
            update_time,
            action,
            user_id,
            activity_type,
            case
                when action = 1 then '生成'
                when action = 2 then '使用'
                when action = 3 then '全额退回'
                when action = 4 then '部分退回'
                when action = 5 then '作废'
                else '其他'
                end as use_status,
            get_json_object(promo_use_strategy, '$.usedAmount') usedAmount --实际满减的金额
        from ods_tujiaonlinepromo.promouselog --卡券使用
    ) m
) n
where rk = 1
and action in (2,4) -- 1-'生成'，2-'使用'，3-'全额退回'，4-'部分退回'，5-'作废'
),

activity_isuse as (
select fafang.user_id uid 
from fafang
left join promo_use
on fafang.user_id = promo_use.user_id
and fafang.promo_code = promo_use.promo_code 
where promo_use.user_id is null 
group by 1 
),

travl_t1 as (
select ctripuid
    ,member_id uid 
    ,city.city_id cityid
    ,city.city_name  cityname
    ,city.city_pinyin 
    ,max(checkindate) checkindate 
    ,max(checkoutdate) checkoutdate
from (
    select ctripuid
        ,tocityid cityid
        ,tocityname cityname
        ,arrivaldatetime checkindate
        ,date_add(arrivaldatetime,1) checkoutdate
    from app_ctrip.edw_bnb_trn_ord_order_d 
    where d = date_sub(current_date,1)
    -- and arrivaldatetime = date_sub(current_date,1)
    and returnticketstate = 0 
    union all 
    select uid ctripuid
        ,acity cityid
        ,acityname cityname
        ,takeofftime checkindate
        ,date_add(takeofftime,1) checkoutdate
    from app_ctrip.edw_bnb_fl_ord_d
    where d = date_sub(current_date,1)
    -- and takeofftime = date_sub(current_date,1)
) a 
left join excel_upload.dim_qijin_ctcity_mapping a1 
on a.cityname = a1.city_c
join (
    select distinct member_id
        ,third_id --三方user_id
    from ods_tujia_member.third_user_mapping
    where channel_code ='CtripId' 
    ) mp 
on a.ctripuid = mp.third_id
left join (
    select province_id
        ,province_name
        ,city_id
        ,city_name  
        ,city_pinyin
    from tujia_dim.dim_region   
    where is_oversea = 0 
    group by 1,2,3,4,5 
) city 
on a1.city_t = city.city_name
group by 1,2,3,4,5
),

tujia_od as (
select user_id uid
from dws.dws_order
where checkin_date between date_sub(current_date,1) and date_add(current_date,2)
and is_paysuccess_order = 1 --支付成功
and is_overseas = 0 --国内
and is_risk_order = 0 --非风控
and is_cancel_order=0 --非取消
group by 1 
),


changzhu as (
select mp.member_id uid 
    ,city cityid 
    ,city_name cityname
from (
    select distinct uid 
        ,get_json_object(c.json_string, '$.label_value_text') city
    from (
        select * 
        from app_ctrip.edw_bnb_dna_user_label_all 
        where d = date_sub(current_date,2)
    ) a 
    lateral view outer explode(udf.json_split(label)) b as c
    where get_json_object(c.json_string, '$.labelid') = '1013'
    and get_json_object(c.json_string, '$.label_value_text') is not null
) a 
left join excel_upload.dim_qijin_ctcity_mapping a1 
on a.city = a1.city_c
join (
    select distinct member_id
        ,third_id --三方user_id
    from ods_tujia_member.third_user_mapping
    where channel_code ='CtripId' 
) mp 
on a.uid = mp.third_id
left join (
    select province_id
        ,province_name
        ,city_id
        ,city_name  
        ,city_pinyin
    from tujia_dim.dim_region   
    where is_oversea = 0 
    group by 1,2,3,4,5 
) city 
on a1.city_t = city.city_id
),

final as (
select b.ctripuid uid
    ,b.cityid
    ,b.cityname
    ,b.city_pinyin
    ,b.checkindate 
    ,b.checkoutdate

    
    ,case when  a1.uid is not null then 'cx001'
        when  a2.uid is not null then 'cx002'
        when  a3.uid is not null then 'cx003' 
        else 'cx004' end scene

from travl_t1 b -- t-1出行
left join tujia_od c -- 有民宿订单
on b.uid = c.uid
join activity_isuse d -- 账户有优惠券
on b.uid = d.uid
left join (
    select uid
    from (
        select ctripuid  uid
            ,tocityid cityid
            ,tocityname cityname
            ,arrivaldatetime checkindate
            ,date_add(arrivaldatetime,1) checkoutdate
        from app_ctrip.edw_bnb_trn_ord_order_d 
        where d between date_sub(current_date,30) and date_sub(current_date,1)
        and arrivaldatetime between date_sub(current_date,30) and date_sub(current_date,1)
        and returnticketstate = 0 
        and tocityname in ('北京','上海','成都','广州','深圳','杭州','重庆','南京','苏州','郑州','西安','长沙','天津')
        union all 
        select uid uid
            ,acity cityid
            ,acityname cityname
            ,takeofftime checkindate
            ,date_add(takeofftime,1) checkoutdate
        from app_ctrip.edw_bnb_fl_ord 
        where d between date_sub(current_date,30) and date_sub(current_date,1)
        and takeofftime between date_sub(current_date,30) and date_sub(current_date,1)
        and acityname in ('北京','上海','成都','广州','深圳','杭州','重庆','南京','苏州','郑州','西安','长沙','天津')
    ) a 
    group by 1 
    having count(1) >= 4 
) a1 
on b.ctripuid = a1.uid 
left join (
    select member_id uid 
    from (
        select ctripuid  uid 
            ,tocityname cityname
        from app_ctrip.edw_bnb_trn_ord_order_d
        where d = date_sub(current_date,1)
        and arrivaldatetime = date_sub(current_date,1)
        and returnticketstate = 0 
        and ticketseat in ('商务座','一等座')
        union all 
        select uid 
            ,acityname cityname
        from app_ctrip.edw_bnb_fl_ord_d
        where d = date_sub(current_date,1)
        and takeofftime = date_sub(current_date,1)
        and cast(price_tax as int) >= 2000
    ) a 
    join (        
        select distinct member_id
            ,third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId' 
    ) mp 
    on a.uid = mp.third_id
    left join changzhu b
    on mp.member_id = b.uid 
    where a.cityname != b.cityname
    group by 1
) a2 
on b.uid = a2.uid 
left join (
    select member_id uid 
    from (
        select ctripuid  uid 
            ,tocityname cityname
            ,ticketcount
        from app_ctrip.edw_bnb_trn_ord_order_d
        where d = date_sub(current_date,1)
        and arrivaldatetime = date_sub(current_date,1)
        and returnticketstate = 0 
        union all 
        select uid 
            ,acityname cityname
            ,persons  
        from app_ctrip.edw_bnb_fl_ord_d
        where d = date_sub(current_date,1)
        and takeofftime = date_sub(current_date,1) 
    ) a 
    join (
        select distinct member_id
            ,third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId' 
    ) mp 
    on a.uid = mp.third_id
    left join changzhu b
    on mp.member_id = b.uid 
    where a.cityname != b.cityname
    group by 1
    having sum(ticketcount) >= 3
) a3
on b.uid = a3.uid 

where c.uid is null

) 

select 

'' id 
,uid 
,'2' channel 
,scene

,udf.object_to_string(map('uid', uid,
    'cityId', cityid,
        'cityName', cityname,
        'cityPinyin',city_pinyin,
    'checkinDate',checkindate,
        'checkoutDate',checkoutdate,
    'scene',scene,
    'conditions',''
  )) as biz_data
,current_date dt 
, date_format(from_unixtime(unix_timestamp()), 'yyyy-MM-dd HH:mm:ss') create_time

from final
where cityid is not null 