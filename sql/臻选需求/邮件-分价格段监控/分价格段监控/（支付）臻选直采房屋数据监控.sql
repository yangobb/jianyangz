with o as (
select terminal_type_name,checkout_date as dt,create_date
		,uid,city_id, city_name, house_id, hotel_id, order_no, order_room_night_count,room_total_amount
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
where create_date between date_sub(current_date,14) and date_sub(current_date, 1)    -- 离店日期 近7天
--and city_id in (select city_id from excel_upload.wanghongcityid)
and terminal_type_name in ('携程-APP','本站-APP','去哪儿-APP','携程-小程序','本站-小程序','去哪儿-小程序')
and is_paysuccess_order = '1'
and is_done = 1
and is_overseas = 0 --国内
),
h as (
select distinct a.house_id,hotel_id,house_city_name,nvl(e.tag,'05_电销B') as `城市类型`,great_tag,bedroom_count
	,case when bedroom_count = 1 then  '一居' 
		when bedroom_count = 2 then  '二居' 
		when bedroom_count = 3 then  '三居' 
		when bedroom_count >= 4 then  '四居+' 
		end as bedroom_count_type
from (
select dt,house_id,hotel_id,house_city_id,dynamic_business_id,house_city_name
                ,longitude,latitude
				,bed_info,share_type
                ,great_tag
                ,case when (share_type = '单间' or house_type = '别墅单间')then 1 else bedroom_count end as bedroom_count
				,bathroom_count,livingroom_count
                ,bedcount,recommended_guest
                ,case when (share_type = '单间' or house_type = '别墅单间') and rent_single_room_area is not null then rent_single_room_area else gross_area end as gross_area
				,case when house_type in ('普通公寓','独栋别墅','客栈','Loft复式','酒店式公寓','别墅单间','其他') then house_type else '特殊类型' end as house_type
                ,house_class,enum_house_facilities_name,house_create_time,house_first_active_time,landlord_channel
from dws.dws_house_d
where dt = date_sub(current_date, 1)
and house_is_oversea=0
and hotel_is_oversea=0
and landlord_channel = 1 
	) a 
-- join (	select distinct house_id 
-- 		from  pdb_analysis_c.dws_house_wanghong_tag_d 
-- 		where dt = date_sub(current_date,1)
-- 		)zx 
-- on a.house_id = zx.house_id
left join excel_upload.city_list_2024 e 
on a.house_city_name = e.city_name
),
kucun as (
select distinct unitid as house_id
,instancecount --物理房量
,avaliablecount --可售库存
,unavaliablecount --已售库存
,avaliablecount + unavaliablecount as total_count
from  dim_tujiaproduct.unit_inventory_log a
where a.createdate = date_sub(current_date,1) 
and a.createdate = inventorydate --当天看当天
and substr(a.gettime,9,2) = 22
),
order_base_h_tuzhanbi as ( -- 订单 房屋 点评 表格连接
select distinct `城市类型`,great_tag,bedroom_count,h.house_city_name
,h.house_id,h.hotel_id,bedroom_count_type, dh.dt,
case when great_tag = 1 then '臻选' else '非臻选' end as is_great_tag,
case when weighted_price <= 100 then '100-' 
     when weighted_price > 100 and weighted_price <= 200 then '100-200'
     when weighted_price > 200 and weighted_price <= 300 then '200-300'
     when weighted_price > 300 and weighted_price <= 400 then '300-400'
     when weighted_price > 400 and weighted_price <= 500 then '400-500'
     when weighted_price > 500 and weighted_price <= 600 then '500-600'
     when weighted_price > 600 and weighted_price <= 800 then '600-800'
     when weighted_price > 800 and weighted_price <= 1000 then '800-1000'
     when weighted_price > 1000 and weighted_price <= 1500 then '1000-1500'
     when weighted_price > 1500 then '1500+'
     else 'Null' end as price_range,
yishoukucun_7,wulifangliang_7,keshoukucun_7
from h
left join pdb_analysis_c.dwd_house_tuzhanbi_d dh
on dh.house_id = h.house_id
left join pdb_analysis_c.dwd_house_price_level_d dhpl 
on dhpl.house_id = h.house_id
where (dh.dt = date_sub(current_date, 1) or dh.dt = date_sub(current_date, 8))
and dhpl.dt = date_sub(current_date, 1)
),
order_base_h_kucun as ( -- 订单 房屋 点评 表格连接
select distinct `城市类型`,great_tag,bedroom_count,h.house_city_name,instancecount,avaliablecount,unavaliablecount,total_count
,h.house_id,h.hotel_id,bedroom_count_type,
case when great_tag = 1 then '臻选' else '非臻选' end as is_great_tag,
case when weighted_price <= 100 then '100-' 
     when weighted_price > 100 and weighted_price <= 200 then '100-200'
     when weighted_price > 200 and weighted_price <= 300 then '200-300'
     when weighted_price > 300 and weighted_price <= 400 then '300-400'
     when weighted_price > 400 and weighted_price <= 500 then '400-500'
     when weighted_price > 500 and weighted_price <= 600 then '500-600'
     when weighted_price > 600 and weighted_price <= 800 then '600-800'
     when weighted_price > 800 and weighted_price <= 1000 then '800-1000'
     when weighted_price > 1000 and weighted_price <= 1500 then '1000-1500'
     when weighted_price > 1500 then '1500+'
     else 'Null' end as price_range
from h
left join kucun 
on h.house_id = kucun.house_id
left join pdb_analysis_c.dwd_house_price_level_d dhpl 
on dhpl.house_id = h.house_id
where dhpl.dt = date_sub(current_date, 1)
),
order_base as ( -- 订单 房屋 点评 表格连接
select distinct o.dt,`城市类型`,great_tag,bedroom_count,instancecount,avaliablecount,unavaliablecount,total_count,
channel,channel_total,o.city_name,oh.house_city_name,o.city_id,o.order_no,order_room_night_count,room_total_amount
,oh.house_id,oh.hotel_id,bedroom_count_type,
oh.is_great_tag,
oh.price_range,
((room_total_amount/order_room_night_count)/bedroom_count) as `单居ADR`
from o join order_base_h_kucun oh
on o.house_id = oh.house_id
)
select t1.dim as `维度1`,t1.sub_dim as `维度2`,concat(round(zx_night_rate * 100,2),'%') as `离店间夜占比`
,concat(round(zx_night_rate_wow * 100,2),'%') as `离店间夜占比wow`
,concat(round(`途占比` * 100,2),'%') as `途占比`
,concat(round(wow * 100,2),'%') as `途占比wow`
,concat(round(zx_gmv_rate * 100,2),'%') as `离店GMV占比`
,concat(round(zx_gmv_rate_wow * 100,2),'%') as `离店GMV占比wow`
,concat(round(zx_ord_rate * 100,2),'%') as `离店订单占比`
,round(ADR,2) as `离店ADR`,round(yiju_ADR,2) as `一居ADR`, round(`多居的单居ADR`,2) as `多居的单居ADR`,round(`单订单间夜数`,2) as `单订单间夜数`,
`总房源数`,`总可售库存数`,concat(round(`房屋动销率` * 100,2),'%') as `房屋动销率`,concat(round(`库存动销率` * 100,2),'%') as `库存动销率`,
`一居总房源数`,`一居总可售库存数`,concat(round(`一居房屋动销率` * 100,2),'%') as `一居房屋动销率`,concat(round(`一居库存动销率` * 100,2),'%') as `一居库存动销率`,
`二居总房源数`,`二居总可售库存数`,concat(round(`二居房屋动销率` * 100,2),'%') as `二居房屋动销率`,concat(round(`二居库存动销率` * 100,2),'%') as `二居库存动销率`,
`三居总房源数`,`三居总可售库存数`,concat(round(`三居房屋动销率` * 100,2),'%') as `三居房屋动销率`,concat(round(`三居库存动销率` * 100,2),'%') as `三居库存动销率`,
`四居+总房源数`,`四居+总可售库存数`,concat(round(`四居+房屋动销率` * 100,2),'%') as `四居+房屋动销率`,concat(round(`四居+库存动销率` * 100,2),'%') as `四居+库存动销率`
from 
(
select t1.`汇总` as dim,t2.`汇总` as sub_dim,t1.zx_night_rate,t1.zx_night_rate/t2.zx_night_rate - 1 as zx_night_rate_wow,t1.`途占比`,
t1.`途占比`/t2.`途占比` - 1 as wow,
t1.zx_gmv_rate,t1.zx_gmv_rate/t2.zx_gmv_rate - 1 as zx_gmv_rate_wow,
zx_ord_rate,ADR,yiju_ADR, `多居的单居ADR`,`单订单间夜数`
from 
(
    select '汇总' as `汇总`,nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
           nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
           nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate,
           nvl(a.zx_ord/a.dapan_ord ,0) as zx_ord_rate,
           nvl(a.zx_gmv/a.zx_night ,0) as ADR,
           nvl(a.yiju_gmv/a.yiju_night ,0) as yiju_ADR,
           `多居的单居ADR`,
           `单订单间夜数`
    from 
    (
        select '汇总' as `汇总`,
        count(distinct order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then order_room_night_count end),0) as yiju_night,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then room_total_amount end),0) as yiju_gmv,
        nvl(avg(case when bedroom_count > 1 and is_great_tag = '臻选' then `单居ADR` end),0) as `多居的单居ADR`,
        nvl(avg(case when is_great_tag = '臻选' then order_room_night_count end),0) as `单订单间夜数`
        from order_base
        where dt between date_sub(current_date,7) and date_sub(current_date, 1)
    ) a 
    left join (
    select '汇总' as `汇总`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 1)
    ) b
    on a.`汇总` = b.`汇总`
) t1 
left join 
(
select '汇总' as `汇总`,nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
        nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
        nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate
    from 
    (
        select '汇总' as `汇总`,
        count(distinct order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        from order_base
        where dt between date_sub(current_date,14) and date_sub(current_date, 8)
    ) a
    left join (
    select '汇总' as `汇总`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 8)
    ) b
    on a.`汇总` = b.`汇总`    
) t2
on t1.`汇总` = t2.`汇总`
) t1 
left join 
(
select '汇总' as  dim, '汇总' as sub_dim
	,count(distinct case when is_great_tag = '臻选' then house_id end) `总房源数`
	,sum(case when is_great_tag = '臻选' then total_count end) as `总可售库存数`
	,count(distinct case when is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when is_great_tag = '臻选' then house_id end) as `房屋动销率`
	,sum(case when is_great_tag = '臻选' then unavaliablecount end)/sum(case when is_great_tag = '臻选' then total_count end) as `库存动销率`

	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) `一居总房源数`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居总可售库存数`
	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) as `一居房屋动销率`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居库存动销率`
	
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) `二居总房源数`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居总可售库存数`
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) as `二居房屋动销率`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居库存动销率`
	
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) `三居总房源数`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居总可售库存数`
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) as `三居房屋动销率`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居库存动销率`
	
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) `四居+总房源数`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+总可售库存数`
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) as `四居+房屋动销率`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+库存动销率`
from order_base_h_kucun
group by 1,2
) t2 
on t1.dim = t2.dim 
and t1.sub_dim = t2.sub_dim 

union all 

select *
from 
(
select t1.dim as `维度1`,t1.sub_dim as `维度2`,concat(round(zx_night_rate * 100,2),'%') as `离店间夜占比`
,concat(round(zx_night_rate_wow * 100,2),'%') as `离店间夜占比wow`
,concat(round(`途占比` * 100,2),'%') as `途占比`
,concat(round(wow * 100,2),'%') as `途占比wow`
,concat(round(zx_gmv_rate * 100,2),'%') as `离店GMV占比`
,concat(round(zx_gmv_rate_wow * 100,2),'%') as `离店GMV占比wow`
,concat(round(zx_ord_rate * 100,2),'%') as `离店订单占比`
,round(ADR,2) as `离店ADR`,round(yiju_ADR,2) as `一居ADR`, round(`多居的单居ADR`,2) as `多居的单居ADR`,round(`单订单间夜数`,2) as `单订单间夜数`,
`总房源数`,`总可售库存数`,concat(round(`房屋动销率` * 100,2),'%') as `房屋动销率`,concat(round(`库存动销率` * 100,2),'%') as `库存动销率`,
`一居总房源数`,`一居总可售库存数`,concat(round(`一居房屋动销率` * 100,2),'%') as `一居房屋动销率`,concat(round(`一居库存动销率` * 100,2),'%') as `一居库存动销率`,
`二居总房源数`,`二居总可售库存数`,concat(round(`二居房屋动销率` * 100,2),'%') as `二居房屋动销率`,concat(round(`二居库存动销率` * 100,2),'%') as `二居库存动销率`,
`三居总房源数`,`三居总可售库存数`,concat(round(`三居房屋动销率` * 100,2),'%') as `三居房屋动销率`,concat(round(`三居库存动销率` * 100,2),'%') as `三居库存动销率`,
`四居+总房源数`,`四居+总可售库存数`,concat(round(`四居+房屋动销率` * 100,2),'%') as `四居+房屋动销率`,concat(round(`四居+库存动销率` * 100,2),'%') as `四居+库存动销率`
from 
(
select '城市类型' as dim,t1.`城市类型` as sub_dim,t1.zx_night_rate,t1.zx_night_rate/t2.zx_night_rate - 1 as zx_night_rate_wow,
t1.`途占比`,t1.`途占比`/t2.`途占比` - 1 as wow,
t1.zx_gmv_rate,t1.zx_gmv_rate/t2.zx_gmv_rate - 1 as zx_gmv_rate_wow,
zx_ord_rate,ADR,yiju_ADR, `多居的单居ADR`,`单订单间夜数`
from 
(
    select a.`城市类型`,
           nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
           nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
           nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate,
           nvl(a.zx_ord/a.dapan_ord ,0) as zx_ord_rate,
           nvl(a.zx_gmv/a.zx_night ,0) as ADR,
           nvl(a.yiju_gmv/a.yiju_night ,0) as yiju_ADR,
           `多居的单居ADR`,
           `单订单间夜数`
    from 
    (
        select `城市类型`,
        count(distinct order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then order_room_night_count end),0) as yiju_night,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then room_total_amount end),0) as yiju_gmv,
        nvl(avg(case when bedroom_count > 1 and is_great_tag = '臻选' then `单居ADR` end),0) as `多居的单居ADR`,
        nvl(avg(case when is_great_tag = '臻选' then order_room_night_count end),0) as `单订单间夜数`
        from order_base
        where dt between date_sub(current_date,7) and date_sub(current_date, 1)
        group by 1
    ) a
    left join (
        select `城市类型`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 1)
        group by 1
    ) b 
    on a.`城市类型` = b.`城市类型`
) t1 
left join 
(
select  a.`城市类型`,
        nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
        nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
        nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate
    from 
    (
        select `城市类型`,
        count(order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        from order_base
        where dt between date_sub(current_date,14) and date_sub(current_date, 8)
        group by 1
    ) a
    left join (
    select `城市类型`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 8)
        group by 1
    ) b
    on a.`城市类型` = b.`城市类型`
) t2
on t1.`城市类型` = t2.`城市类型`
) t1
left join
(
select '城市类型' as dim, `城市类型` as sub_dim
    ,count(distinct case when is_great_tag = '臻选' then house_id end) `总房源数`
	,sum(case when is_great_tag = '臻选' then total_count end) as `总可售库存数`
	,count(distinct case when is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when is_great_tag = '臻选' then house_id end) as `房屋动销率`
	,sum(case when is_great_tag = '臻选' then unavaliablecount end)/sum(case when is_great_tag = '臻选' then total_count end) as `库存动销率`

	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) `一居总房源数`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居总可售库存数`
	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) as `一居房屋动销率`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居库存动销率`
	
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) `二居总房源数`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居总可售库存数`
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) as `二居房屋动销率`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居库存动销率`
	
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) `三居总房源数`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居总可售库存数`
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) as `三居房屋动销率`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居库存动销率`
	
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) `四居+总房源数`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+总可售库存数`
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) as `四居+房屋动销率`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+库存动销率`
from order_base_h_kucun
group by 1,2
) t2
on t1.dim = t2.dim 
and t1.sub_dim = t2.sub_dim 
order by FIND_IN_SET (`维度1`,'01_地面S,02_地面A,03_电销S,04_电销A,05_电销B')
) t

union all

select `维度1`,`维度2`,`离店间夜占比`
,`离店间夜占比wow`
,`途占比`
,`途占比wow`
,`离店GMV占比`
,`离店GMV占比wow`
,`离店订单占比`
,`离店ADR`,`一居ADR`,`多居的单居ADR`,`单订单间夜数`,
`总房源数`,`总可售库存数`,`房屋动销率`,`库存动销率`,
`一居总房源数`,`一居总可售库存数`,`一居房屋动销率`,`一居库存动销率`,
`二居总房源数`,`二居总可售库存数`,`二居房屋动销率`,`二居库存动销率`,
`三居总房源数`,`三居总可售库存数`,`三居房屋动销率`,`三居库存动销率`,
`四居+总房源数`,`四居+总可售库存数`,`四居+房屋动销率`,`四居+库存动销率`
from 
(
select *
from 
(
select t1.dim as `维度1`,t1.sub_dim as `维度2`,concat(round(zx_night_rate * 100,2),'%') as `离店间夜占比`
,concat(round(zx_night_rate_wow * 100,2),'%') as `离店间夜占比wow`
,concat(round(`途占比` * 100,2),'%') as `途占比`
,concat(round(wow * 100,2),'%') as `途占比wow`
,concat(round(zx_gmv_rate * 100,2),'%') as `离店GMV占比`
,concat(round(zx_gmv_rate_wow * 100,2),'%') as `离店GMV占比wow`
,concat(round(zx_ord_rate * 100,2),'%') as `离店订单占比`
,round(ADR,2) as `离店ADR`,round(yiju_ADR,2) as `一居ADR`, round(`多居的单居ADR`,2) as `多居的单居ADR`,round(`单订单间夜数`,2) as `单订单间夜数`,
`总房源数`,`总可售库存数`,concat(round(`房屋动销率` * 100,2),'%') as `房屋动销率`,concat(round(`库存动销率` * 100,2),'%') as `库存动销率`,
`一居总房源数`,`一居总可售库存数`,concat(round(`一居房屋动销率` * 100,2),'%') as `一居房屋动销率`,concat(round(`一居库存动销率` * 100,2),'%') as `一居库存动销率`,
`二居总房源数`,`二居总可售库存数`,concat(round(`二居房屋动销率` * 100,2),'%') as `二居房屋动销率`,concat(round(`二居库存动销率` * 100,2),'%') as `二居库存动销率`,
`三居总房源数`,`三居总可售库存数`,concat(round(`三居房屋动销率` * 100,2),'%') as `三居房屋动销率`,concat(round(`三居库存动销率` * 100,2),'%') as `三居库存动销率`,
`四居+总房源数`,`四居+总可售库存数`,concat(round(`四居+房屋动销率` * 100,2),'%') as `四居+房屋动销率`,concat(round(`四居+库存动销率` * 100,2),'%') as `四居+库存动销率`,
case when t1.sub_dim = '1500+' then 1
     when t1.sub_dim = '1000-1500' then 2
     when t1.sub_dim = '800-1000' then 3
     when t1.sub_dim = '600-800' then 4
     when t1.sub_dim = '500-600' then 5
     when t1.sub_dim = '400-500' then 6
     when t1.sub_dim = '300-400' then 7
     when t1.sub_dim = '200-300' then 8
     when t1.sub_dim = '100-200' then 9
     when t1.sub_dim = '100-' then 10
     when t1.sub_dim = 'NUll' then 11
end as range_p
from
(
select '价格段' as dim,t1.`价格段` as sub_dim,t1.zx_night_rate,t1.zx_night_rate/t2.zx_night_rate - 1 as zx_night_rate_wow,
t1.`途占比`,t1.`途占比`/t2.`途占比` - 1 as wow,
t1.zx_gmv_rate,t1.zx_gmv_rate/t2.zx_gmv_rate - 1 as zx_gmv_rate_wow,
zx_ord_rate,ADR,yiju_ADR, `多居的单居ADR`,`单订单间夜数`
from 
(
    select a.`价格段`,
           nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
           nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
           nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate,
           nvl(a.zx_ord/a.dapan_ord ,0) as zx_ord_rate,
           nvl(a.zx_gmv/a.zx_night ,0) as ADR,
           nvl(a.yiju_gmv/a.yiju_night ,0) as yiju_ADR,
           `多居的单居ADR`,
           `单订单间夜数`
    from 
    (
        select price_range as `价格段`,
        count(distinct order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then order_room_night_count end),0) as yiju_night,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then room_total_amount end),0) as yiju_gmv,
        nvl(avg(case when bedroom_count > 1 and is_great_tag = '臻选' then `单居ADR` end),0) as `多居的单居ADR`,
        nvl(avg(case when is_great_tag = '臻选' then order_room_night_count end),0) as `单订单间夜数`
        from order_base
        where dt between date_sub(current_date,7) and date_sub(current_date, 1)
        group by 1
    ) a 
    left join (
        select price_range as `价格段`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 1)
        group by 1
    ) b 
    on a.`价格段` = b.`价格段`
) t1 
left join 
(
select  a.`价格段`,
        nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
        nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
        nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate
    from 
    (
        select price_range as `价格段`,
        count(distinct order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        from order_base
        where dt between date_sub(current_date,14) and date_sub(current_date, 8)
        group by 1
    ) a
    left join (
    select price_range as `价格段`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 8)
        group by 1
    ) b
    on a.`价格段` = b.`价格段`
) t2
on t1.`价格段` = t2.`价格段`
) t1
left join 
(
select '价格段' as dim, price_range as sub_dim
    ,count(distinct case when is_great_tag = '臻选' then house_id end) `总房源数`
	,sum(case when is_great_tag = '臻选' then total_count end) as `总可售库存数`
	,count(distinct case when is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when is_great_tag = '臻选' then house_id end) as `房屋动销率`
	,sum(case when is_great_tag = '臻选' then unavaliablecount end)/sum(case when is_great_tag = '臻选' then total_count end) as `库存动销率`

	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) `一居总房源数`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居总可售库存数`
	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) as `一居房屋动销率`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居库存动销率`
	
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) `二居总房源数`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居总可售库存数`
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) as `二居房屋动销率`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居库存动销率`
	
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) `三居总房源数`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居总可售库存数`
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) as `三居房屋动销率`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居库存动销率`
	
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) `四居+总房源数`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+总可售库存数`
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) as `四居+房屋动销率`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+库存动销率`
from order_base_h_kucun
group by 1,2
) t2 
on t1.dim = t2.dim 
and t1.sub_dim = t2.sub_dim 
order by range_p
) t
) t

union all 

select *
from
(
select t1.dim as `维度1`,t1.sub_dim as `维度2`,concat(round(zx_night_rate * 100,2),'%') as `离店间夜占比`
,concat(round(zx_night_rate_wow * 100,2),'%') as `离店间夜占比wow`
,concat(round(`途占比` * 100,2),'%') as `途占比`
,concat(round(wow * 100,2),'%') as `途占比wow`
,concat(round(zx_gmv_rate * 100,2),'%') as `离店GMV占比`
,concat(round(zx_gmv_rate_wow * 100,2),'%') as `离店GMV占比wow`
,concat(round(zx_ord_rate * 100,2),'%') as `离店订单占比`
,round(ADR,2) as `离店ADR`,round(yiju_ADR,2) as `一居ADR`, round(`多居的单居ADR`,2) as `多居的单居ADR`,round(`单订单间夜数`,2) as `单订单间夜数`,
`总房源数`,`总可售库存数`,concat(round(`房屋动销率` * 100,2),'%') as `房屋动销率`,concat(round(`库存动销率` * 100,2),'%') as `库存动销率`,
`一居总房源数`,`一居总可售库存数`,concat(round(`一居房屋动销率` * 100,2),'%') as `一居房屋动销率`,concat(round(`一居库存动销率` * 100,2),'%') as `一居库存动销率`,
`二居总房源数`,`二居总可售库存数`,concat(round(`二居房屋动销率` * 100,2),'%') as `二居房屋动销率`,concat(round(`二居库存动销率` * 100,2),'%') as `二居库存动销率`,
`三居总房源数`,`三居总可售库存数`,concat(round(`三居房屋动销率` * 100,2),'%') as `三居房屋动销率`,concat(round(`三居库存动销率` * 100,2),'%') as `三居库存动销率`,
`四居+总房源数`,`四居+总可售库存数`,concat(round(`四居+房屋动销率` * 100,2),'%') as `四居+房屋动销率`,concat(round(`四居+库存动销率` * 100,2),'%') as `四居+库存动销率`
from 
(
select t1.`城市类型` as dim,t1.`城市` as sub_dim,t1.zx_night_rate,t1.zx_night_rate/t2.zx_night_rate - 1 as zx_night_rate_wow,
t1.`途占比`,t1.`途占比`/t2.`途占比` - 1 as wow,
t1.zx_gmv_rate,t1.zx_gmv_rate/t2.zx_gmv_rate - 1 as zx_gmv_rate_wow,
zx_ord_rate,ADR,yiju_ADR, `多居的单居ADR`,`单订单间夜数`
from 
(
    select a.`城市类型`,a.`城市`,
           nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
           nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
           nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate,
           nvl(a.zx_ord/a.dapan_ord ,0) as zx_ord_rate,
           nvl(a.zx_gmv/a.zx_night ,0) as ADR,
           nvl(a.yiju_gmv/a.yiju_night ,0) as yiju_ADR,
           `多居的单居ADR`,
           `单订单间夜数`
    from 
    (
        select `城市类型` as `城市类型`,house_city_name as `城市`,
        count(distinct order_no ) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then order_room_night_count end),0) as yiju_night,
        nvl(sum(case when bedroom_count = 1 and is_great_tag = '臻选' then room_total_amount end),0) as yiju_gmv,
        nvl(avg(case when bedroom_count > 1 and is_great_tag = '臻选' then `单居ADR` end),0) as `多居的单居ADR`,
        nvl(avg(case when is_great_tag = '臻选' then order_room_night_count end),0) as `单订单间夜数`
        from order_base
        where dt between date_sub(current_date,7) and date_sub(current_date, 1)
        group by 1,2
    ) a
    left join (select `城市类型` as `城市类型`,house_city_name as `城市`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 1)
        group by 1,2
    ) b
    on a.`城市类型` = b.`城市类型` and a.`城市` = b.`城市`
) t1 
left join 
(
select  a.`城市类型`,a.`城市`,
        nvl(a.zx_night/a.dapan_night ,0) as zx_night_rate,
        nvl(b.yishoukucun/(b.wulifangliang - b.keshoukucun) ,0) as `途占比`,
        nvl(a.zx_gmv/a.dapan_gmv ,0) as zx_gmv_rate
    from 
    (
        select `城市类型` as `城市类型`,house_city_name as `城市`,
        count(distinct order_no) as dapan_ord,
        nvl(sum(order_room_night_count),0) as dapan_night,
        nvl(sum(room_total_amount),0) as dapan_gmv,
        count(distinct case when is_great_tag = '臻选' then order_no end) as zx_ord,
        nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night,
        nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        from order_base
        where dt between date_sub(current_date,14) and date_sub(current_date, 8)
        group by 1,2
    ) a
    left join (
    select `城市类型` as `城市类型`,house_city_name as `城市`,
        nvl(sum(case when is_great_tag = '臻选' then yishoukucun_7 end),0) as yishoukucun,
        nvl(sum(case when is_great_tag = '臻选' then wulifangliang_7 end),0) as wulifangliang,
        nvl(sum(case when is_great_tag = '臻选' then keshoukucun_7 end),0) as keshoukucun 
        from order_base_h_tuzhanbi
        where dt = date_sub(current_date, 8)
        group by 1,2
    ) b
    on a.`城市类型` = b.`城市类型` 
    and a.`城市` = b.`城市`
) t2
on t1.`城市类型` = t2.`城市类型` 
and t1.`城市` = t2.`城市`
) t1 
left join 
(
select `城市类型` as  dim, house_city_name as sub_dim
	,count(distinct case when is_great_tag = '臻选' then house_id end) `总房源数`
	,sum(case when is_great_tag = '臻选' then total_count end) as `总可售库存数`
	,count(distinct case when is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when is_great_tag = '臻选' then house_id end) as `房屋动销率`
	,sum(case when is_great_tag = '臻选' then unavaliablecount end)/sum(case when is_great_tag = '臻选' then total_count end) as `库存动销率`

	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) `一居总房源数`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居总可售库存数`
	,count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '一居' and is_great_tag = '臻选' then house_id end) as `一居房屋动销率`
	,sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '一居' and is_great_tag = '臻选' then total_count end) as `一居库存动销率`
	
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) `二居总房源数`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居总可售库存数`
	,count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '二居' and is_great_tag = '臻选' then house_id end) as `二居房屋动销率`
	,sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '二居' and is_great_tag = '臻选' then total_count end) as `二居库存动销率`
	
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) `三居总房源数`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居总可售库存数`
	,count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '三居' and is_great_tag = '臻选' then house_id end) as `三居房屋动销率`
	,sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '三居' and is_great_tag = '臻选' then total_count end) as `三居库存动销率`
	
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) `四居+总房源数`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+总可售库存数`
	,count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' and unavaliablecount is not null and unavaliablecount <> 0 then house_id end)/count(distinct case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then house_id end) as `四居+房屋动销率`
	,sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then unavaliablecount end)/sum(case when bedroom_count_type = '四居+' and is_great_tag = '臻选' then total_count end) as `四居+库存动销率`
from order_base_h_kucun
group by 1,2
) t2 
on t1.dim = t2.dim 
and t1.sub_dim = t2.sub_dim
order by FIND_IN_SET (`维度1`,'01_地面S,02_地面A,03_电销S,04_电销A,05_电销B')
) t
