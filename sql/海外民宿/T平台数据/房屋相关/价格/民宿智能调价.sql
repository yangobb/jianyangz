--每天更新最近3天的数据，因为涉及到未来3天退参率

with house_d as (
select distinct
    dt,
    hotel_level,
    house_class,
    house_id,
    great_tag as is_zhenxuan,
    is_prefer_pro as is_yanxuan,
    instance_count
from dws.dws_house_d
where dt between date_sub("${partition}",14) and  date_sub("${partition}",1)
and house_is_oversea=0 
and house_is_online=1 
and hotel_is_online=1 
and landlord_channel_name='平台商户'
)
,gj as (
select distinct dt,type,house_id,checkin_date,base_price_now,price
from pdb_analysis_c.dim_house_intelligent_pricing_gjfinal_d
where dt between date_sub("${partition}",14) and  date_sub("${partition}",1)
and base_price_now>price
)
,aoto_yindao as (--和线上差一天
select 
a.*,
round((base_price-base_price_now)/base_price_now,2) fudu
from (select distinct
    dt 
    ,hotel_id
    ,house_id
    ,checkin_date
    ,strage_source
    ,is_algorithm
    ,act_price_now --原活动价
    ,act_price --推荐活动价
    ,base_price_now --原基础价
    ,price_type 
    ,act_price/(act_price_now/base_price_now) base_price --推荐基础价
    ,if(strage_source regexp 'T0无产单|T0已调价无产单|T0有产单|T0已调价有产单',1,0) is_t0_no_order
from pdb_analysis_b.ads_house_intelligent_pricing_d
where dt between date_sub("${partition}",15) and  date_sub("${partition}",2)
and dt>= '2024-08-11'
) a 
)

,auto_price as (
select distinct
    dt 
    ,fail_cause --失败原因
    ,check_in
    ,time
    ,unit_id house_id
    ,target_act_price  --目标活动价
    ,orig_act_price  --原活动价
    ,target_base_price --目标基础价
    ,orig_base_price --原基础价
    ,discount_rate --拿到的折扣
    ,if(hour(time)>=18,1,0) is_t0_no_order
from ads.ads_other_luigi_task_adjust_price_log_d
where dt between date_sub("${partition}",14) and  date_sub("${partition}",1)
and dt>= '2024-08-12'
and fail_cause not in ('未开启智能调价'
,'当日自动跟价房源',
'调价类型(0)与结果不一致',
'调价类型(1)与结果不一致',
'调价类型(2)与结果不一致',
'无可订产品',
'一口价过滤')
)
,ord as (
select 
    create_date dt,
    to_date(a.checkin_date_new) checkin_date,
    b.house_id,
    b.create_time,
    sum(a.real_unit_rate*b.booking_count) as gmv,
    sum(b.booking_count) as nights,
    count(distinct a.order_no) as ord
from(
    select distinct
        order_no, --不唯一，同一个订单有10晚，则会记10次
        nvl(real_unit_rate,0) real_unit_rate,  --每天实付
        concat(get_json_object(real_day,'$.year'),'-',
            lpad(cast(get_json_object(real_day,'$.month') as string),2,'0'),'-',
            lpad(cast(get_json_object(real_day,'$.day') as string),2,'0')
            ) as checkin_date_new
    from dwd.dwd_order_product_d --全量分区表
    where dt= date_sub(current_date,1)
)a
join(--取套数、房屋id、限制订单状态
    select distinct
        order_no, 
        booking_count,
        house_id,
        room_total_amount,
        checkin_date,
        create_time,
        checkout_date,
        create_date
    from dws.dws_order 
    where create_date  between date_sub("${partition}",14) and  date_sub("${partition}",1)
    and create_date>= '2024-08-12'
    and is_success_order = 1 --实住
    and is_cancel_order = 0 -- 非取消单
    and is_overseas = 0 -- 非海外
    and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907') --剔除合伙人订单
)b on a.order_no=b.order_no
group by 1,2,3,4
)

,dp_ord as (--大盘订单
    select create_date dt,
    sum(room_total_amount) gmv,
    nvl(sum(order_room_night_count),0) nights,
    count(distinct order_no) ord 
    from dws.dws_order 
    where create_date  between date_sub("${partition}",14) and  date_sub("${partition}",1)
    and create_date>= '2024-08-12'
    and is_success_order = 1 --实住
    and is_cancel_order = 0 -- 非取消单
    and is_overseas = 0 -- 非海外
    and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907') --剔除合伙人订单
    group by 1 
)
,result as (
select
    date_add(a.dt,1) dt,
    a.house_id, --策略房屋量
    a.strage_source,--策略
    a.is_algorithm,--算法/人工
    a.price_type,
    max(case when b.fail_cause is not null then 1 else 0 end) as is_chuda, --是否触达 
    max(case when b.fail_cause='成功' then 1 else 0 end) as is_accept, --是否改价
    max(case when b.fail_cause='成功' and c.hotel_id is not null then 1 else 0 end) is_out_act, --改价后是否退参
    sum(case when b.fail_cause='成功' then d.nights end) nights, --改价后当日间夜量
    count(*)  as price_num  --报价数
from aoto_yindao a --智能调价底表
join auto_price b on a.dt=date_sub(b.dt,1) and a.house_id=b.house_id and a.checkin_date=b.check_in and a.is_t0_no_order=b.is_t0_no_order--推至线上的表
left join (
---改价后未来3天退参门店,从改价当天算起
select distinct dt,hotel_id
from pdb_analysis_c.dws_other_auto_price_d
where dt  between date_sub("${partition}",14) and  date_sub("${partition}",1)
and dt>= '2024-08-12' 
and protocol_status=0 
) c on a.hotel_id=c.hotel_id and datediff(c.dt,a.dt) between 1 and 3
left join (select d.dt,d.checkin_date,d.house_id,
case when hour(b.second_time)>=18 and d.create_time>b.second_time then 1 else 0 end as is_t0_no_order,
sum(gmv) as gmv,
sum(nights) as nights,
sum(ord) as ord
from ord d 
left join 
(select dt,check_in,house_id,min(time) first_time,max(time) second_time
from auto_price
where fail_cause='成功'
group by 1,2,3) b on b.dt=d.dt and b.house_id=d.house_id and b.check_in=d.checkin_date
group by 1,2,3,4) d on b.dt=d.dt and b.house_id=d.house_id and b.check_in=d.checkin_date and d.is_t0_no_order=b.is_t0_no_order --智能调价看修改的对应的入离的成单
group by 1,2,3,4 ,5
)
, total as (select 
        a.is_algorithm,
        case when a.price_type =2 then '涨价' else '降价' end `调价类型`,
        a.strage_source,
        a.dt_tpye,
        a.`策略覆盖房屋量`,
        concat(round(a.`策略覆盖房屋量`*100/c.`策略覆盖房屋量`,2),'%') `覆盖房屋量占比`,
        a.`触达房屋量`,
        a.`采纳房屋量`,
        a.`退参房屋量`,
        a.`间夜量`,
        concat(round(a.`商户采纳率`*100,2),'%') `商户采纳率`,
        concat(round(a.`间夜量`*100/b.`大盘间夜量`,2),'%') `间夜占比`
    from (
    select 
        case when dt between date_sub("${partition}",7) and  date_sub("${partition}",1) then '本周'
        when dt between date_sub("${partition}",14) and  date_sub("${partition}",8) then '上周' end as dt_tpye,
        is_algorithm,--算法/人工
        strage_source,--策略
        price_type,
        nvl(count(distinct a.house_id),0) `策略覆盖房屋量`,
        nvl(count(distinct case when is_chuda=1 then house_id end),0) `触达房屋量`,
        nvl(count(distinct case when is_accept=1 then house_id end),0) `采纳房屋量`,
        nvl(count(distinct case when is_out_act=1 then house_id end),0) as `退参房屋量`,
        nvl(sum(a.nights),0) `间夜量`,
        `采纳房屋量`/`触达房屋量`-`退参房屋量`/`触达房屋量` `商户采纳率`
    from result  a
    group by 1,2,3 ,4
    ) a left join  (
    select 
    case when dt between date_sub("${partition}",7) and  date_sub("${partition}",1) then '本周'
    when dt between date_sub("${partition}",14) and  date_sub("${partition}",8) then '上周' end as dt_tpye,
    nvl(sum(nights),0) `大盘间夜量`
    from dp_ord
    group by 1
    )b on a.dt_tpye=b.dt_tpye
    left join (
        select dt_tpye,
        sum(`策略覆盖房屋量`) `策略覆盖房屋量`
        from(
        select 
        case when dt between date_sub("${partition}",7) and  date_sub("${partition}",1) then '本周'
        when dt between date_sub("${partition}",14) and  date_sub("${partition}",8) then '上周' end as dt_tpye,
        is_algorithm,--算法/人工
        strage_source,--策略
        nvl(count(distinct house_id),0) `策略覆盖房屋量`
    from result  
    group by 1,2,3) a 
    group by 1
    ) c on a.dt_tpye=c.dt_tpye
)

select 
is_algorithm,
`调价类型` price_type,
strage_source,
dt_tpye,
`策略覆盖房屋量` cover_num,
`覆盖房屋量占比` cover_num_rate,
`触达房屋量` chuda_num,
`采纳房屋量`caina_num,
`退参房屋量` tuican_num,
`间夜量` nights_num,
`商户采纳率` caina_rate,
`间夜占比` nights_rate,
-- b. `策略覆盖房屋量`,
-- b.`覆盖房屋量占比`,
-- b.`触达房屋量`,
-- b. `采纳房屋量`,
-- b. `退参房屋量`,
-- b. `间夜量`,
-- b.`商户采纳率`,
-- b.`间夜占比`
date_sub("${partition}",1) dt 
-- (select 
--     is_algorithm,
--     `调价类型`,
--     strage_source,
--     `策略覆盖房屋量`,
--     `覆盖房屋量占比`,
--     `触达房屋量`,
--     `采纳房屋量`,
--     `退参房屋量`,
--     `间夜量`,
--     `商户采纳率`,
--     `间夜占比`
from total 