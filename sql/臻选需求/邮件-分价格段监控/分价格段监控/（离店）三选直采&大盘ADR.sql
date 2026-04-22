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
where checkout_date between date_sub(current_date,7) and date_sub(current_date, 1)    -- 离店日期 近7天
--and city_id in (select city_id from excel_upload.wanghongcityid)
and terminal_type_name in ('携程-APP','本站-APP','去哪儿-APP','携程-小程序','本站-小程序','去哪儿-小程序')
--and is_paysuccess_order = '1'
and is_done = 1
and is_overseas = 0 --国内
),
h as ( 
select distinct house_id,hotel_id,house_city_name,bedroom_count
	,case when bedroom_count = 1 then  '一居' 
		when bedroom_count = 2 then  '二居' 
		when bedroom_count = 3 then  '三居' 
		when bedroom_count >= 4 then  '四居+' 
		end as bedroom_count_type
    ,case when great_tag = 1 then '臻选' else '非臻选' end as is_great_tag
    ,case when is_prefer_pro = '1' then '严选' else '非严选' end as is_prefer_pro
	,case when is_prefer = '1' then '优选' else '非优选' end as is_prefer
    ,case when landlord_channel_name = '平台商户' then '直采' else '非直采' end as is_zhicai
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
                ,is_prefer_pro,is_prefer,landlord_channel_name
from dws.dws_house_d
where dt = date_sub(current_date, 1)
and house_is_oversea=0
and hotel_is_oversea=0
) a 
-- join (	select distinct house_id 
-- 		from  pdb_analysis_c.dws_house_wanghong_tag_d 
-- 		where dt = date_sub(current_date,1)
-- 		)zx 
-- on a.house_id = zx.house_id
),
order_base as ( -- 订单 房屋 点评 表格连接
select distinct dt,is_great_tag,is_prefer_pro,is_prefer,is_zhicai,bedroom_count_type,
channel,channel_total,o.city_name,o.city_id,o.order_no,order_room_night_count,room_total_amount,h.house_id,h.hotel_id
from o join h
on o.house_id = h.house_id
)
select *
from 
(
    select 
    dim1,dim2,round(nvl(zx_gmv/zx_night,0),0) as `臻选ADR`, round(nvl(yanxuan_gmv/yanxuan_night,0),0) as `严选ADR`
    ,round(nvl(youxuan_gmv/youxuan_night,0),0) as `优选ADR`,round(nvl(dapan_gmv/dapan_night,0),0) as `大盘ADR`
    ,round(nvl(zx_gmv/zx_night,0)/nvl(youxuan_gmv/youxuan_night,0),2) as `臻选/优选ADR`
    from 
    (
        select 
        '直采' as dim1,'汇总' as dim2
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night
        ,nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        ,nvl(sum(case when is_prefer_pro = '严选' then order_room_night_count end),0) as yanxuan_night
        ,nvl(sum(case when is_prefer_pro = '严选' then room_total_amount end),0) as yanxuan_gmv
        ,nvl(sum(case when is_prefer = '优选' then order_room_night_count end),0) as youxuan_night
        ,nvl(sum(case when is_prefer = '优选' then room_total_amount end),0) as youxuan_gmv
        ,nvl(sum(order_room_night_count),0) as dapan_night
        ,nvl(sum(room_total_amount),0) as dapan_gmv
        from order_base
        where is_zhicai = '直采'
        group by 1,2
    ) t

    union all 

    select 
    dim1,dim2,round(nvl(zx_gmv/zx_night,0),0) as `臻选ADR`, round(nvl(yanxuan_gmv/yanxuan_night,0),0) as `严选ADR`
    ,round(nvl(youxuan_gmv/youxuan_night,0),0) as `优选ADR`,round(nvl(dapan_gmv/dapan_night,0),0) as `大盘ADR`
    ,round(nvl(zx_gmv/zx_night,0)/nvl(youxuan_gmv/youxuan_night,0),2) as `臻选/优选ADR`
    from 
    (
        select 
        '直采' as dim1,bedroom_count_type as dim2
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night
        ,nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        ,nvl(sum(case when is_prefer_pro = '严选' then order_room_night_count end),0) as yanxuan_night
        ,nvl(sum(case when is_prefer_pro = '严选' then room_total_amount end),0) as yanxuan_gmv
        ,nvl(sum(case when is_prefer = '优选' then order_room_night_count end),0) as youxuan_night
        ,nvl(sum(case when is_prefer = '优选' then room_total_amount end),0) as youxuan_gmv
        ,nvl(sum(order_room_night_count),0) as dapan_night
        ,nvl(sum(room_total_amount),0) as dapan_gmv
        from order_base
        where is_zhicai = '直采'
        group by 1,2
    ) t

    union all 

    select 
    dim1,dim2,round(nvl(zx_gmv/zx_night,0),0) as `臻选ADR`, round(nvl(yanxuan_gmv/yanxuan_night,0),0) as `严选ADR`
    ,round(nvl(youxuan_gmv/youxuan_night,0),0) as `优选ADR`,round(nvl(dapan_gmv/dapan_night,0),0) as `大盘ADR`
    ,round(nvl(zx_gmv/zx_night,0)/nvl(youxuan_gmv/youxuan_night,0),2) as `臻选/优选ADR`
    from 
    (
        select 
        '大盘' as dim1,'汇总' as dim2
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night
        ,nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        ,nvl(sum(case when is_prefer_pro = '严选' then order_room_night_count end),0) as yanxuan_night
        ,nvl(sum(case when is_prefer_pro = '严选' then room_total_amount end),0) as yanxuan_gmv
        ,nvl(sum(case when is_prefer = '优选' then order_room_night_count end),0) as youxuan_night
        ,nvl(sum(case when is_prefer = '优选' then room_total_amount end),0) as youxuan_gmv
        ,nvl(sum(order_room_night_count),0) as dapan_night
        ,nvl(sum(room_total_amount),0) as dapan_gmv
        from order_base
        group by 1,2
    ) t

    union all 

    select 
    dim1,dim2,round(nvl(zx_gmv/zx_night,0),0) as `臻选ADR`, round(nvl(yanxuan_gmv/yanxuan_night,0),0) as `严选ADR`
    ,round(nvl(youxuan_gmv/youxuan_night,0),0) as `优选ADR`,round(nvl(dapan_gmv/dapan_night,0),0) as `大盘ADR`
    ,round(nvl(zx_gmv/zx_night,0)/nvl(youxuan_gmv/youxuan_night,0),2) as `臻选/优选ADR`
    from 
    (
        select 
        '大盘' as dim1,bedroom_count_type as dim2
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as zx_night
        ,nvl(sum(case when is_great_tag = '臻选' then room_total_amount end),0) as zx_gmv
        ,nvl(sum(case when is_prefer_pro = '严选' then order_room_night_count end),0) as yanxuan_night
        ,nvl(sum(case when is_prefer_pro = '严选' then room_total_amount end),0) as yanxuan_gmv
        ,nvl(sum(case when is_prefer = '优选' then order_room_night_count end),0) as youxuan_night
        ,nvl(sum(case when is_prefer = '优选' then room_total_amount end),0) as youxuan_gmv
        ,nvl(sum(order_room_night_count),0) as dapan_night
        ,nvl(sum(room_total_amount),0) as dapan_gmv
        from order_base
        group by 1,2
    ) t
) t
where dim2 is not null
order by 1,instr('汇总,一居,二居,三居,四居+',dim2)
