-- ------------------------------------------------------
-- time： 20250320
-- autor： jianyangz
-- need by: huijiexu
-- describe: 风险订单识别
-- ------------------------------------------------------
-- 取消单
with h as (
select distinct house_id
    ,house_name
    ,hotel_id
    ,house_city_id
    ,house_city_name
    ,recommended_guest
    ,dynamic_business_id
    ,dynamic_business
    ,great_tag
    ,house_class
    ,house_first_active_time `上房时间`
    ,landlord_name
    ,landlord_channel_name
    ,bedroom_count
    ,is_chiness_landlord
    ,case when house_class in ('L21','L1') then '低'
        when house_class in ('L25') then '中'
        when house_class in ('L3','L4') then '高'
        end as `房屋等级`
    ,case when year(house_first_active_time) >= 2023 then '新'
        when year(house_first_active_time) < 2023 then '旧'
        end as `上房时间`
from dws.dws_house_d
where dt = date_sub(current_date,1)
AND house_is_oversea = 1 --国外
and house_is_online = 1 --在线
-- and landlord_channel = 1 
),
od_cancal as (
select terminal_type_name 
    ,checkin_date	 
    ,checkout_date
    ,create_date
    ,uid
    ,city_id
    ,city_name
    ,house_id
    ,hotel_id
    ,order_no
    ,order_room_night_count
    ,room_total_amount
    ,case when terminal_type_name = '携程-APP' then '携程app'
        when terminal_type_name = '本站-APP' then '途家app'
        when terminal_type_name = '去哪儿-APP' then '去哪儿app'
        when terminal_type_name = '携程-小程序' then '携程小程序'
        when terminal_type_name = '本站-小程序' then '途家小程序'
        when terminal_type_name = '去哪儿-小程序' then '去哪儿小程序'
        end as channel
    ,case when terminal_type_name = '携程-APP' or terminal_type_name = '携程-小程序' then '携程'
        when terminal_type_name like '本站%' then '途家'
        when terminal_type_name like '去哪儿%' then '去哪儿'
        end as channel_total
from dws.dws_order
where checkout_date between date_add(current_date,1) and date_add(current_date,30)
and is_overseas = 1 
and is_paysuccess_order = 1
and is_cancel_order = 1 
-- and cancel1 = 1 -- 支付前取消
-- and cancel2 = 1 -- 确认前取消
-- and cancel3 = 1 -- 确认后取消 
-- and is_confirm_order -- 确认
-- and is_done = 1 -- 完成
-- and is_cancel_order -- 取消单	
),
od_all as (
select terminal_type_name 
    ,checkin_date	 
    ,checkout_date
    ,create_date
    ,uid
    ,city_id
    ,city_name
    ,house_id
    ,hotel_id
    ,order_no
    ,order_room_night_count
    ,room_total_amount
    ,is_cancel_order
    ,case when terminal_type_name = '携程-APP' then '携程app'
        when terminal_type_name = '本站-APP' then '途家app'
        when terminal_type_name = '去哪儿-APP' then '去哪儿app'
        when terminal_type_name = '携程-小程序' then '携程小程序'
        when terminal_type_name = '本站-小程序' then '途家小程序'
        when terminal_type_name = '去哪儿-小程序' then '去哪儿小程序'
        end as channel
    ,case when terminal_type_name = '携程-APP' or terminal_type_name = '携程-小程序' then '携程'
        when terminal_type_name like '本站%' then '途家'
        when terminal_type_name like '去哪儿%' then '去哪儿'
        end as channel_total
from dws.dws_order
where checkout_date between date_add(current_date,1) and date_add(current_date,30)
and is_overseas = 1 
and is_paysuccess_order = 1
),
close_by_hotel as (
SELECT a.house_id AS house_id,
    b.begindate AS begindate,
    b.enddate AS enddate,
    c.day_date
    
FROM dws.dws_house_d a
JOIN dwd.dwd_bizlog_unitstock_d b 
ON a.house_guid = b.unitguid
left join tujia_dim.dim_date_info c 
on c.day_date between b.begindate and b.enddate
WHERE a.landlord_channel_name = '平台商户'
AND a.house_is_online = 1
AND a.house_is_oversea = 1
AND a.dt = date_sub(CURRENT_DATE, 1)
AND b.createtime BETWEEN date_sub(CURRENT_DATE(), 190) AND date_add(CURRENT_DATE(),190)
AND b.begindate BETWEEN date_sub(CURRENT_DATE(), 190) AND date_add(CURRENT_DATE(),190)
and CASE WHEN b.operatorname LIKE '%pms%' THEN 'pms'
        WHEN b.operatorname LIKE '%iCal-Airbnb%' THEN 'airbnb'
        WHEN b.operatorname LIKE '%OrderDiffQueue%' THEN 'tujia'
        WHEN b.operatorname LIKE '%booking%' THEN 'booking'
        ELSE 'hotel_op'
        END = 'hotel_op'
and CASE WHEN b.remarks LIKE '%打开%' OR b.remarks LIKE '%可售卖库存数为1%' OR b.remarks LIKE '%释放%' THEN 'open'
        WHEN b.remarks LIKE '%关闭%' OR b.remarks LIKE '%占用库存1%' OR b.remarks LIKE '%可售卖数为0%' THEN 'close'
        ELSE 'open'
        END = 'close' 
and datediff(b.enddate,c.day_date) / datediff(b.enddate,b.begindate) >= 0.5 
group by 1,2,3,4 
)



select oc.order_no
    ,oc.uid 
    ,oc.house_id
    ,h.house_name 
    ,oc.checkin_date
    ,oc.checkout_date
    ,max(case when oa.is_cancel_order = 0 then 1 else 0 end) `退改单`
    ,max(case when cyh.house_id is not null then 1 else 0 end) `房东修改库存`
from h 
join od_cancal oc -- 取消订单明细
on h.house_id = oc.house_id 
left join od_all oa -- 全量订单明细
on h.house_id = oa.house_id 
and oa.checkout_date between oc.checkin_date and oc.checkout_date
and oc.order_no != oa.order_no

left join close_by_hotel cyh 
on h.house_id = cyh.house_id
and cyh.day_date between oc.checkin_date and oc.checkout_date
group by 1,2,3,4,5,6