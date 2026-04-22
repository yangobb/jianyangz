
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
from ods_tujiaonlinepromo.promo
where enumprovidertype = 2 --承担方类型
and to_date(create_time) between date_sub(current_date,30) and date_sub(current_date,1)
and enumpromopartytype in (0,1,2) --途家、去哪儿
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


fafang_mlk as (
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
from ods_tujiaonlinepromo.promo
where to_date(create_time) between date_sub(current_date,30) and date_sub(current_date,1)
-- where activity_code in ('rp_CHhil4Pz9Pm','rp_CHvGLl4Bnh2') -- 抹零卡
),
promo_use_mlk as (
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
activity_isuse_mlk as (
select fafang_mlk.user_id uid 
from fafang_mlk
left join promo_use_mlk
on fafang_mlk.user_id = promo_use_mlk.user_id
and fafang_mlk.promo_code = promo_use_mlk.promo_code 
where promo_use_mlk.user_id is null 
group by 1 
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

server_log as (

select 
    dt
    ,user_id
    ,channel
    ,house_id_new house_id
    ,qinzi_label
    ,ani_label
    ,duoren_label
    ,jushi_label2 
    ,jushi_label3 
    ,jushi_label4 
    ,jushi_label5 
    ,jushi_label6 
    ,jushi_label7 
    ,jushi_label8 
    ,jushi_label9 
    ,jushi_label10
    ,checkindate 
    ,checkoutdate
    ,chongqing
    ,sanya 
    ,shanghai 
    ,hangzhou 
    ,dali 
    ,chengdu
    ,detail_page
    ,act_time
    ,city_name

from (
select
    dt
    ,user_id
    ,channel
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'亲子泳池|亲子精选|亲子爱住|亲子乐园') = 1 then 1 end) qinzi_label
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'宠物友好') = 1 then 1 end) ani_label
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'别墅|独栋|轰趴') = 1 then 1 end) duoren_label
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'2居') = 1 then 1 end) jushi_label2 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'3居') = 1 then 1 end) jushi_label3 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'4居') = 1 then 1 end) jushi_label4 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'5居') = 1 then 1 end) jushi_label5 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'6居') = 1 then 1 end) jushi_label6 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'7居') = 1 then 1 end) jushi_label7 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'8居') = 1 then 1 end) jushi_label8 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'9居') = 1 then 1 end) jushi_label9 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'10居') = 1 then 1 end) jushi_label10
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'入住日期') = 1 then get_json_object(json_info,'$.value') end) checkindate 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'离店日期') = 1 then get_json_object(json_info,'$.value') end) checkoutdate
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'重庆') = 1 then 1 end) chongqing
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'三亚') = 1 then 1 end) sanya 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'上海') = 1 then 1 end) shanghai 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'杭州') = 1 then 1 end) hangzhou 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'大理') = 1 then 1 end) dali 
    ,max(case when regexp_like(get_json_object(json_info,'$.label'),'成都') = 1 then 1 end) chengdu
    
    ,max(case when (curpage = 'product' AND logname = 'unit_detail') then 1 end) detail_page
    ,max(house_id) house_id
    ,max(act_time) act_time
    ,max(city_name) city_name
from (
    select distinct 
        trace_id
        ,user_id
        ,dt
        ,fromforlog
        ,tujia_code 
        ,curpage 
        ,logname  
        ,city_name
        ,house_id
        ,channel
        ,conditions
        ,from_unixtime(UNIX_TIMESTAMP(substr(act_time,0,14),'yyyyMMddHHmmss')) as act_time
        ,row_number() over(partition by user_id order by act_time desc) rn 
    from dwd.dwd_server_log_d_iceberg
    where dt = current_date
    and from_unixtime(UNIX_TIMESTAMP(SUBSTR(act_time,1,14),'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') >= from_unixtime(unix_timestamp() - 3600, 'yyyy-MM-dd HH:mm:ss')
    and channel = 'ctrip'
    and conditions is not null
) t1 
lateral view explode(udf.json_split_new(conditions)) r as json_info 
where rn = 1 
group by 
    dt
    ,user_id
    ,channel
) a 
lateral view explode(split(house_id,',')) r as house_id_new 
)



select 
 '' id
, uid  
 
,case when channel = 'ctrip' then 2 
	when channel = 'qunar' then 1
	when channel = 'tujia' then 3 
    end 
channel 
,udf.object_to_string(map('uid', uid,
		'cityId', cityid,
        'cityName', cityname,
        'cityPinyin',city_pinyin,
		'houseId',house_id,
		'scene',scene,
		'checkinDate',checkindate,
        'checkoutDate',checkoutdate,
		'conditions',conditions
	)) as biz_data
,act_time
,current_date dt 
,'' create_time
from (
select user_id uid 
    ,house_id
    ,city_id cityid 
    ,a.city_name cityname 
    ,city_pinyin
    ,act_time
    ,channel
    ,checkindate
    ,checkoutdate
    ,case when c.uid is not null and (pmod(dayofweek(checkindate) + 5, 7) + 1) in (6,7) then 'bj001' -- 抹零卡 d.uid 取消
        when c.uid is not null and  (pmod(dayofweek(checkindate) + 5, 7) + 1) <= 5 then 'bj002'  
        when detail_page = 1 and d1.uid is not null then 'cg001'
        when qinzi_label = 1 then 'cg002'
        when ani_label = 1 then 'cg003'
        when duoren_label = 1 then 'cg004'
        when coalesce(jushi_label2,jushi_label3,jushi_label4,jushi_label5,jushi_label6,jushi_label7,jushi_label8,jushi_label9,jushi_label10) = 1 then 'cg005'
        when chongqing = 1 then 'cg006'
        when sanya = 1 then 'cg007'
        when shanghai = 1 then 'cg008'
        when hangzhou = 1 then 'cg009'
        when dali = 1 then 'cg010' 
        when chengdu = 1 then 'cg011'
        when datediff(checkoutdate,checkindate) >= 3 then 'cg012'
        when checkindate = current_date then 'cg013'
        else 'yhysj'
        end scene
    ,case when qinzi_label = 1 then '6=4602'
        when ani_label = 1 then '6=2304'
        when duoren_label = 1 then '6=102'

        when jushi_label10 = 1 then '6=10'
        when jushi_label9 = 1 then '6=9'
        when jushi_label8 = 1 then '6=8'
        when jushi_label7 = 1 then '6=7'
        when jushi_label6 = 1 then '6=6'
        when jushi_label5 = 1 then '6=5'
        when jushi_label4 = 1 then '6=4'
        when jushi_label3 = 1 then '6=3'        
        when jushi_label2 = 1 then '6=2'

        end conditions
from server_log a
left join (
    select province_id
        ,province_name
        ,city_id
        ,city_name	
        ,city_pinyin
    from tujia_dim.dim_region	 
    where is_oversea = 0 
    group by 1,2,3,4,5 
) b
on a.city_name = b.city_name 
join (
    select distinct member_id
        ,third_id --三方user_id
    from ods_tujia_member.third_user_mapping
    where channel_code ='CtripId' 
) mp 
on a.user_id = mp.member_id
left join (
    select distinct uid 
    ,case when get_json_object(c.json_string, '$.label_value_text') = 0 then '普通' 
            when get_json_object(c.json_string, '$.label_value_text') = 10 then '金牌' 
            when get_json_object(c.json_string, '$.label_value_text') = 20 then '白金' 
            when get_json_object(c.json_string, '$.label_value_text') = 30 then '钻石'
            else '其他'
            end level 
    from (
        select * 
        from app_ctrip.edw_bnb_dna_user_label_all 
        where d = date_sub(current_date,2)
    ) a 
    lateral view outer explode(udf.json_split(label)) b as c
    where get_json_object(c.json_string, '$.labelid') = '1023'
    and get_json_object(c.json_string, '$.label_value_text') in ('20','30')
) c
on mp.third_id = c.uid 

left join activity_isuse d1
on a.user_id = d1.uid 

left join tujia_od e 
on a.user_id = e.uid 
where e.uid is null 
) a 
where scene != 'yhysj'
and a.cityid is not null 
and nvl(a.cityid,0) != 0 
