-- 需求整体，我会提个PMO, 核心是对比24年YOY的数据，分为业绩和流量两部分：
-- 时间范围4月30日到5月5日；

-- 1. 业绩数值+YOY变化（订单，间夜，GMV，ADR, L2O_uv宽，取消率（支付）)（离店+支付口径） 
-- 1.1 总体表现；
-- 1.2 城市维度表现（top 13城+札幌，福冈，名古屋，非13城汇总）
-- 1.3 对比C酒店表现，C七大类表现（看总体25年表现和24年YoY增速）

-- 2. 流量变化，
-- 2.1 总体流量+各城市流量变化，对比C酒店流量（同上），C七大类流量（同上），全球流量涨跌幅（对比24） top 10涨幅+top 10跌幅；
-- 2.2 用户行为，用户提前预定周期表现（流量分布情况），T0/Tn流量表现，新老客比例；
-- 2.3 各用户渠道表现， CQT三端和各个入口的流量变化情况；


with 

ldbo as (
select '今年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct concat(uid,dt)) luv   
    ,count(distinct case when detail_uid is not null then concat(uid,dt) end) duv 
    ,sum(without_risk_access_order_num) od_num_z 
    ,count(uid) lpv   
    ,count(case when detail_uid is not null then uid end) dpv 
from dws.dws_path_ldbo_d
where dt between '2025-04-30' and '2025-05-05'
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户'  
group by 1,2
union all 
select '去年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct concat(uid,dt)) luv   
    ,count(distinct case when detail_uid is not null then concat(uid,dt) end) duv 
    ,sum(without_risk_access_order_num) od_num_z 
    ,count(uid) lpv   
    ,count(case when detail_uid is not null then uid end) dpv 
from dws.dws_path_ldbo_d
where dt between '2025-04-30' and '2025-05-05'
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户'  
group by 1,2
)
,od as (
select '今年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) cancel_order_cnt
from dws.dws_order 
where create_date between '2025-04-30' and '2025-05-05'
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2
union all
select '去年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) 
from dws.dws_order 
where create_date between '2024-04-30' and '2024-05-05'
and is_paysuccess_order = 1 
and is_overseas = 1 
group by 1,2 
) 
,od_check as (
select '今年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) cancel_order_cnt
from dws.dws_order 
where checkout_date between '2025-04-30' and '2025-05-05'
and is_cancel_order	= 0
and is_done = 1 
and is_overseas = 1 
group by 1,2
union all
select '去年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) 
from dws.dws_order 
where checkout_date between '2024-04-30' and '2024-05-05'
and is_cancel_order	= 0
and is_done = 1 
and is_overseas = 1 
group by 1,2
) 
,jiudian as (
SELECT '去年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split
WHERE submitfrom = 'client'
AND TO_DATE(orderdate) between '2024-04-30' and '2024-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2 
union all 
SELECT '今年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split
WHERE submitfrom = 'client'
AND TO_DATE(orderdate) between '2025-04-30' and '2025-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2
)
,jiudian_check as (
SELECT '去年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split
WHERE submitfrom = 'client'
AND TO_DATE(departure) between '2024-04-30' and '2024-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2 
union all 
SELECT '今年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split
WHERE submitfrom = 'client'
AND TO_DATE(departure) between '2025-04-30' and '2025-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2
)
,jiudian_jiudian as (
SELECT '去年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split a 
inner join (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select distinct masterhotelid                                        
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date(),1)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 1  --是否标准酒店 1：是、0：否
) b 
on a.masterhotelid = b.masterhotelid
WHERE submitfrom = 'client'
AND TO_DATE(orderdate) between '2024-04-30' and '2024-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2 
union all 
SELECT '今年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split a 
inner join (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select distinct masterhotelid                                        
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date(),1)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 1  --是否标准酒店 1：是、0：否
) b 
on a.masterhotelid = b.masterhotelid
WHERE submitfrom = 'client'
AND TO_DATE(orderdate) between '2025-04-30' and '2025-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2
)

,jiudian_check_jiudian as (
SELECT '去年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split a 
inner join (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select distinct masterhotelid                                        
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date(),1)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 1  --是否标准酒店 1：是、0：否
) b 
on a.masterhotelid = b.masterhotelid
WHERE submitfrom = 'client'
AND TO_DATE(departure) between '2024-04-30' and '2024-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2 
union all 
SELECT '今年' time1 
    ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then cityname else '其他' end city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
FROM app_ctrip.edw_htl_order_all_split a 
inner join (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select distinct masterhotelid                                        
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date(),1)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 1  --是否标准酒店 1：是、0：否
) b 
on a.masterhotelid = b.masterhotelid
WHERE submitfrom = 'client'
AND TO_DATE(departure) between '2025-04-30' and '2025-05-05'
AND orderstatus IN ('P','S')
AND (country <> 1 or cityname in ('香港','澳门'))--海外
AND ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2
)
,ldbo1 as (
select '今年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,uid
    ,count(distinct concat(uid,dt)) luv   
    ,count(distinct case when detail_uid is not null then concat(uid,dt) end) duv 
    ,sum(without_risk_access_order_num) od_num_z 
    ,count(uid) lpv   
    ,count(case when detail_uid is not null then uid end) dpv 
from dws.dws_path_ldbo_d
where dt between '2025-03-01' and '2025-04-30'
and checkout_date between '2025-04-30' and '2025-05-05'
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户'  
group by 1,2,3 
union all 
select '去年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then city_name else '其他' end city_name
    ,uid
    ,count(distinct concat(uid,dt)) luv   
    ,count(distinct case when detail_uid is not null then concat(uid,dt) end) duv 
    ,sum(without_risk_access_order_num) od_num_z 
    ,count(uid) lpv   
    ,count(case when detail_uid is not null then uid end) dpv 
from dws.dws_path_ldbo_d
where dt between '2024-03-01' and '2024-04-30'
and checkout_date between '2024-04-30' and '2024-05-05'
and wrapper_name in ('携程','途家','去哪儿') 
and is_oversea = 1 
and user_type = '用户'  
group by 1,2,3  
) 
,od1 as (
select '今年' time1 
    ,uid
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) cancel_order_cnt
from dws.dws_order 
where checkout_date between '2025-04-30' and '2025-05-05'
and is_done = 1 
and is_overseas = 1 
group by 1,2
union all
select '去年' time1 
    ,uid
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) 
from dws.dws_order 
where create_date between '2024-04-30' and '2024-05-05'
and is_done = 1 
and is_overseas = 1 
group by 1,2 
) 
,zhifu as (
select ldbo1.time1
    ,ldbo1.city_name
    ,sum(luv) luv
    ,sum(duv) duv
    ,sum(lpv) lpv
    ,sum(dpv) dpv
    ,sum(order_cnt) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
from ldbo1 
left join od1 
on ldbo1.time1 = od1.time1 
and ldbo1.uid = od1.uid 
group by 1,2 
)

select ldbo.time1 
    ,ldbo.city_name 
    ,ldbo.luv   
    ,ldbo.duv 
    ,ldbo.od_num_z 
    ,ldbo.lpv   
    ,ldbo.dpv 
    ,od.order_cnt `支付订单数` 
    ,od.room_total_amount `支付销售额`
    ,od.order_room_night_count `支付间夜数`
    ,od.cancel_order_cnt `支付取消率`
    ,od_check.order_cnt `离店订单订单数`
    ,od_check.room_total_amount `离店销售额`
    ,od_check.order_room_night_count `离店间夜数`
    ,od_check.cancel_order_cnt `离店取消订单数`
    
    ,jiudian.order_cnt `酒店支付订单数`
    ,jiudian.gmv `酒店支付gmv`
    ,jiudian.night `酒店支付间夜`
    ,jiudian_check.order_cnt `酒店离店订单数`
    ,jiudian_check.gmv `酒店离店gmv`
    ,jiudian_check.night `酒店离店间夜`
    
    
    
    ,jiudian_jiudian.order_cnt `酒店支付订单数(标品)`
    ,jiudian_jiudian.gmv `酒店支付gmv(标品)`
    ,jiudian_jiudian.night `酒店支付间夜(标品)`
    ,jiudian_check_jiudian.order_cnt `酒店离店订单数(标品)`
    ,jiudian_check_jiudian.gmv `酒店离店gmv(标品)`
    ,jiudian_check_jiudian.night `酒店离店间夜(标品)`
    
    ,zhifu.luv `预定支付uv`
    ,zhifu.lpv `预定支付pv`
    ,zhifu.order_cnt `预定支付订单数` 
    ,zhifu.room_total_amount `预定支付gmv`
    ,zhifu.order_room_night_count `预定支付间夜`

from ldbo 
left join od 
on ldbo.time1 = od.time1
and ldbo.city_name = od.city_name
left join od_check
on ldbo.time1 = od_check.time1
and ldbo.city_name = od_check.city_name
left join jiudian 
on ldbo.time1 = jiudian.time1
and ldbo.city_name = jiudian.city_name
left join jiudian_check
on ldbo.time1 = jiudian_check.time1
and ldbo.city_name = jiudian_check.city_name

left join jiudian_jiudian 
on ldbo.time1 = jiudian_jiudian.time1
and ldbo.city_name = jiudian_jiudian.city_name
left join jiudian_check_jiudian
on ldbo.time1 = jiudian_check_jiudian.time1
and ldbo.city_name = jiudian_check_jiudian.city_name


left join zhifu
on ldbo.time1 = zhifu.time1
and ldbo.city_name = zhifu.city_name





