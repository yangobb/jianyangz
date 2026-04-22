with h as (
select distinct house_id,house_city_id,house_city_name,house_is_online,recommended_guest,
dynamic_business_id,dynamic_business,great_tag,house_class,house_first_active_time,
case when house_class in ('L21','L1') then '低'
when house_class in ('L25') then '中'
when house_class in ('L3','L4') then '高'
end as `房屋等级`,
case when year(house_first_active_time) >= 2023 then '新'
when year(house_first_active_time) < 2023 then '旧'
end as `上房时间`
from dws.dws_house_d
where dt = date_sub(current_date,1)
AND house_is_oversea = 0 --国内
and hotel_is_oversea = 0
--and house_is_online = 1 --在线
and landlord_channel = 1 
),
list as (
select distinct house_city_id,dynamic_business_id,lpv,rank() over(order by lpv desc) rk 
from 
(select sum(lpv) lpv,house_city_id,dynamic_business_id
from (
select house_id,count(uid) lpv
from dws.dws_path_ldbo_d
where dt between DATE_SUB(current_date,7) and DATE_SUB(current_date,1)
and checkin_date between date_add(current_date,1) and date_add(current_date,15)
and user_type = '用户'
and wrapper_name in  ('携程','去哪儿','途家')
and source = '102'
group by 1
	) a join h 
on a.house_id = h.house_id
group by 2,3
) a
),
mjd as (--判断是否七大类  --母酒店
select distinct  is_standard,star,masterhotelid,hotelname,city_t,zonename
from (
select distinct  is_standard,star,masterhotelid,hotelname,cityname,zonename 
from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
where d = date_sub(current_date(),1)                                         
and countryname = '中国'                                         
and masterhotelid > 0 -- 母酒店ID有值 
and is_standard = 1 --是否标准酒店 1：是、0：否
and star = 4 --in (3,4,5)
) a 
join excel_upload.dim_qijin_ctcity_mapping m 
on a.cityname = m.city_c  
),
jd_o as (--酒店下单用户
select to_date(orderdate) as dt,
cityname, --下单时城市名称
room,masterhotelid,
orderid,clientid as uid
,ciiquantity  --night
,ciireceivable -- gmv
from  app_ctrip.edw_htl_order_all_split
where d =  current_date()
and to_date(departure) between date_sub(current_date(),30)  and date_sub(current_date(),1)
--and cityname in ( '惠州','博罗','惠东','龙门')
and  submitfrom='client'  --携程app酒店
and orderstatus in ('S') -- 离店口径
and country = 1   --下单时国家id
and ordertype = 2 -- 酒店订单
),
hotel_price as 
(
select city_t,sum(ciireceivable)/sum(ciiquantity) as jd_adr
from (--判断是否七大类  --母酒店
select distinct  is_standard,star,masterhotelid,hotelname,city_t,zonename
from (
select distinct  is_standard,star,masterhotelid,hotelname,cityname,zonename 
from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
where d = date_sub(current_date(),1)                                         
and countryname = '中国'                                         
and masterhotelid > 0 -- 母酒店ID有值 
and is_standard = 1 --是否标准酒店 1：是、0：否
and star = 4 --in (3,4,5)
) a 
join excel_upload.dim_qijin_ctcity_mapping m 
on a.cityname = m.city_c  
) mjd 
join (--酒店下单用户
select to_date(orderdate) as dt,
cityname, --下单时城市名称
room,masterhotelid,
orderid,clientid as uid
,ciiquantity  --night
,ciireceivable -- gmv
from  app_ctrip.edw_htl_order_all_split
where d =  current_date()
and to_date(departure) between date_sub(current_date(),30)  and date_sub(current_date(),1)
--and cityname in ( '惠州','博罗','惠东','龙门')
and  submitfrom='client'  --携程app酒店
and orderstatus in ('S') -- 离店口径
and country = 1   --下单时国家id
and ordertype = 2 -- 酒店订单
) jd_o 
on mjd.masterhotelid = jd_o.masterhotelid 
group by 1
),
price as 
(        
select distinct	weighted_price,house_id
from pdb_analysis_c.dwd_house_price_level_d
where dt = date_sub(current_date,1)
),
info_score as (--信息分
select house_id
	,avg(`基础分-信息分`)`基础分-信息分`
	,avg(`基础分-价格实惠分`) `基础分-价格实惠分`
	,avg(`基础分-可预订率`) `基础分-可预订率`
	,avg(`基础分-取消政策`) `基础分-取消政策`
	,avg(`基础分-及时回复率`) `基础分-及时回复率`
	,avg(`特色分`) `特色分`
	,avg(`奖励分-膨胀神券`) `奖励分-膨胀神券`
	,avg(`奖励分-优享家`)`奖励分-优享家`
	,avg(`奖励分-实拍视频`) `奖励分-实拍视频`
	,avg(`奖励分-图片新鲜度`)`奖励分-图片新鲜度`
	,avg(`奖励分-自动接单`)`奖励分-自动接单`
	,avg(`奖励分-取消扣首晚`)`奖励分-取消扣首晚`
	,avg(`奖励分-智能调价`)`奖励分-智能调价`
from (
select distinct dt,
	house_id,
  house_information_score as `基础分-信息分`,
  discounted_price_score as `基础分-价格实惠分`,
  canbooking_rate_score as `基础分-可预订率`,
  cancel_policy_score as `基础分-取消政策`,
  im_reply_rate_score as `基础分-及时回复率`,
  5 as `特色分`,
  inflation_coupon_score as `奖励分-膨胀神券`,
	joinact_score as `奖励分-优享家`,
   realvideo_score as `奖励分-实拍视频`,
	picfresh_score as `奖励分-图片新鲜度`,
  fastbooking_score as `奖励分-自动接单`,
  cancelfirst_score as `奖励分-取消扣首晚`,
	pursueprice_score as  `奖励分-智能调价`
  
from pdb_analysis_c.ads_house_detail_d t1
where dt between date_sub(current_date,14) and date_sub(current_date,1) 
	) a 
group by 1
),
base as (
select house_id,`途占比`,`满房率`,
case when `途占比` < 0.35 then '<35%'
 when `途占比` >= 0.35 then '>=35%'
end as `途占比分类`,
case when `满房率` >= 0.3 and `满房率` <= 0.6 then '30%-60%'
 when `满房率` >= 0.9 or `满房率` <= 0.2 then '90%以上或20%以下'
 else '中等' end as `满房率分类`
from 
(
select house_id,
(sum(instancecount)-sum(avaliablecount))/sum(instancecount) `满房率`,
sum(unavaliablecount)/(sum(instancecount)-sum(avaliablecount)) `途占比`
from (
select distinct unitid as house_id,
instancecount,                           --物理库存
avaliablecount,                          --可售库存
unavaliablecount,                        --已售库存
createdate
from  dim_tujiaproduct.unit_inventory_log a
where a.createdate between date_sub(current_date,14) and date_sub(current_date,1) 
and a.createdate = inventorydate --当天看当天
and substr(a.gettime,9,2) = 22
) t 
group by 1
) t 
)



select distinct house_id,
house_is_online,
`途占比`,
`满房率`,
house_class,
`信息维护分`,
house_city_name,
dynamic_business,
rk,lpv,
house_first_active_time,
recommended_guest,
weighted_price,
jd_adr,
`人均比价`,

`途占比分类`,
`满房率分类`,
`房屋等级`,
case when `信息维护分` >= 2.8 then '高'
when `信息维护分` >= 2.5 then '中'
else '低' end as `信息维护分分类`,
`商圈搜索流量`,
`上房时间`,
case when `人均比价` <= 1 then '低分位'
when `人均比价` > 1 and `人均比价` <= 1.3 then '中分位'
when `人均比价` > 1.3 then '高分位'
end as `价格对比酒店`
from
(select distinct a.house_id,
house_is_online,
a.house_city_name,
a.dynamic_business,
house_class,
house_first_active_time,
recommended_guest,
weighted_price,
jd_adr,
lpv,rk, 
`满房率`,
`途占比`,
case when rk < 100 then '高'
when rk >= 100 and rk <= 300 then '中'
else '低' end as `商圈搜索流量`,
(weighted_price/recommended_guest)/(jd_adr/2) as `人均比价`,
`满房率分类`,
`途占比分类`,
`上房时间`,
`房屋等级`,
`基础分-信息分`*0.1 
	+ `基础分-价格实惠分` * 0.05
	+ `基础分-可预订率` * 0.05
	+ `基础分-取消政策` * 0.05
	+ `基础分-及时回复率` * 0.05
	+ `特色分` * 0.15
	+ `奖励分-膨胀神券` * 1
	+ `奖励分-优享家` * 1
	+ `奖励分-实拍视频` * 1
	+ `奖励分-图片新鲜度` * 1
	+ `奖励分-自动接单` * 1
	+ `奖励分-取消扣首晚` * 1
	+ `奖励分-智能调价` * 1
as `信息维护分`

from (select distinct * from h where great_tag = 1) a 
left join list l on a.dynamic_business_id = l.dynamic_business_id and a.house_city_id = l.house_city_id
left join hotel_price hp on a.house_city_name = hp.city_t
left join price p on a.house_id = p.house_id
left join info_score i on a.house_id = i.house_id
left join base b on a.house_id = b.house_id
) t1 


 