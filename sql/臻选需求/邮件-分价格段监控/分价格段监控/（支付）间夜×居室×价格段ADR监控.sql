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
where create_date between date_sub(current_date,7) and date_sub(current_date, 1)    -- 离店日期 近7天
--and city_id in (select city_id from excel_upload.wanghongcityid)
and terminal_type_name in ('携程-APP','本站-APP','去哪儿-APP','携程-小程序','本站-小程序','去哪儿-小程序')
and is_paysuccess_order = '1'
and is_done = 1
and is_overseas = 0 --国内
),
h as (
select distinct a.house_id,a.hotel_id,house_city_name,bedroom_count
    ,case when bedroom_count = 1 then  '一居'
        when bedroom_count = 2 then  '二居'
        when bedroom_count = 3 then  '三居'
        when bedroom_count >= 4 then  '四居+'
        end as bedroom_count_type
    ,case when a.great_tag = 1 then '臻选' else '非臻选' end as is_great_tag
    ,case when is_prefer_pro = '1' then '严选' else '非严选' end as is_prefer_pro
    ,case when is_prefer = '1' then '优选' else '非优选' end as is_prefer
    ,case when landlord_channel_name = '平台商户' then '直采' else '非直采' end as is_zhicai
    ,nvl(b.is_whitebox_pass,0) as is_whitebox_pass --白盒通过
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
-- join (   select distinct house_id
--      from  pdb_analysis_c.dws_house_wanghong_tag_d
--      where dt = date_sub(current_date,1)
--      )zx
-- on a.house_id = zx.house_id
left join ( select *
            from pdb_analysis_c.dwd_house_zhenxuan_bottom_d
            where dt = DATE_SUB(CURRENT_DATE, 1)
            ) b
on a.house_id  =b.house_id
),
base as ( -- 订单 房屋 点评 表格连接
select distinct dt,is_great_tag,is_prefer_pro,is_prefer,is_zhicai,bedroom_count_type,bedroom_count,is_whitebox_pass,
channel,channel_total,o.city_name,o.city_id,o.order_no,order_room_night_count,room_total_amount,h.house_id,h.hotel_id
from o join h
on o.house_id = h.house_id
),
order_base as (
select base.*
    ,case when danju_adr <= 200 then '0-200'
        when danju_adr > 200 and danju_adr <= 300 then '200-300'
        when danju_adr > 300 and danju_adr <= 400 then '300-400'
        when danju_adr > 400 and danju_adr <= 500 then '400-500'
        when danju_adr > 500 then '500+'
        end as danju_adr_range
from base
join (
        select house_id,(sum(room_total_amount)/sum(order_room_night_count))/max(bedroom_count) as danju_adr
        from base
        group by 1
        ) a
on base.house_id = a.house_id
)
 
 
 
select *
from
(
  
        select
        '直采' as dim1,'汇总' as dim2
        ,nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0) as `0_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '0-200' then order_room_night_count end),0) *100,2),'%') as `0_臻选渗透率`
        ,count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end) `0_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '0-200' then house_id end)*100,2),'%') `0_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0) as `2_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '200-300' then order_room_night_count end),0) *100,2),'%') as `2_臻选渗透率`
        ,count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end) `2_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '200-300' then house_id end)*100,2),'%') `2_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0) as `3_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '300-400' then order_room_night_count end),0)*100,2),'%') as `3_臻选渗透率`
        ,count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end) `3_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '300-400' then house_id end)*100,2),'%') `3_臻选房屋占比`
         
        ,nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0) as `4_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '400-500' then order_room_night_count end),0)*100,2),'%') as `4_臻选渗透率`
        ,count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end) `4_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '400-500' then house_id end)*100,2),'%') `4_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0) as `5_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '500+' then order_room_night_count end),0)*100,2),'%') as `5_臻选渗透率`
        ,count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end) `5_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '500+' then house_id end)*100,2),'%') `5_臻选房屋占比`
 
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as `臻选间夜`
        ,concat(round(nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(order_room_night_count),0)*100,2),'%') as `臻选渗透率`
        ,count(distinct case when is_great_tag = '臻选' then house_id end) `臻选房屋`
        ,concat(round(count(distinct case when is_great_tag = '臻选' then house_id end)
                /count(distinct house_id)*100,2),'%') `臻选房屋占比`
 
        from order_base
        where is_zhicai = '直采'
        group by 1,2
 
 
    union all
 
     
        select
        '直采' as dim1,bedroom_count_type as dim2
         ,nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0) as `0_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '0-200' then order_room_night_count end),0) *100,2),'%') as `0_臻选渗透率`
        ,count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end) `0_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '0-200' then house_id end)*100,2),'%') `0_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0) as `2_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '200-300' then order_room_night_count end),0) *100,2),'%') as `2_臻选渗透率`
        ,count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end) `2_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '200-300' then house_id end)*100,2),'%') `2_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0) as `3_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '300-400' then order_room_night_count end),0)*100,2),'%') as `3_臻选渗透率`
        ,count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end) `3_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '300-400' then house_id end)*100,2),'%') `3_臻选房屋占比`
         
        ,nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0) as `4_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '400-500' then order_room_night_count end),0)*100,2),'%') as `4_臻选渗透率`
        ,count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end) `4_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '400-500' then house_id end)*100,2),'%') `4_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0) as `5_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '500+' then order_room_night_count end),0)*100,2),'%') as `5_臻选渗透率`
        ,count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end) `5_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '500+' then house_id end)*100,2),'%') `5_臻选房屋占比`
 
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as `臻选间夜`
        ,concat(round(nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(order_room_night_count),0)*100,2),'%') as `臻选渗透率`
        ,count(distinct case when is_great_tag = '臻选' then house_id end) `臻选房屋`
        ,concat(round(count(distinct case when is_great_tag = '臻选' then house_id end)
                /count(distinct house_id)*100,2),'%') `臻选房屋占比`
 
        from order_base
        where is_zhicai = '直采'
        group by 1,2
 
        union
         
        select
        '直采过白盒' as dim1,'汇总' as dim2
        ,nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0) as `0_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '0-200' and is_whitebox_pass = 1 then order_room_night_count end),0) *100,2),'%') as `0_臻选渗透率`
        ,count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end) `0_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '0-200' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `0_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0) as `2_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '200-300' and is_whitebox_pass = 1 then order_room_night_count end),0) *100,2),'%') as `2_臻选渗透率`
        ,count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end) `2_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '200-300' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `2_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0) as `3_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '300-400' and is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `3_臻选渗透率`
        ,count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end) `3_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '300-400' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `3_臻选房屋占比`
         
        ,nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0) as `4_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '400-500' and is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `4_臻选渗透率`
        ,count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end) `4_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '400-500' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `4_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0) as `5_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '500+' and is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `5_臻选渗透率`
        ,count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end) `5_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '500+' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `5_臻选房屋占比`
 
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as `臻选间夜`
        ,concat(round(nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `臻选渗透率`
        ,count(distinct case when is_great_tag = '臻选' then house_id end) `臻选房屋`
        ,concat(round(count(distinct case when is_great_tag = '臻选' then house_id end)
                /count(distinct case when is_whitebox_pass = 1 then house_id end)*100,2),'%') `臻选房屋占比`
 
        from order_base
        where is_zhicai = '直采'
        group by 1,2
 
 
    union all
 
     
        select
        '直采过白盒' as dim1,bedroom_count_type as dim2
         ,nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0) as `0_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '0-200' and is_whitebox_pass = 1 then order_room_night_count end),0) *100,2),'%') as `0_臻选渗透率`
        ,count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end) `0_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '0-200' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `0_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0) as `2_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '200-300' and is_whitebox_pass = 1 then order_room_night_count end),0) *100,2),'%') as `2_臻选渗透率`
        ,count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end) `2_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '200-300' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `2_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0) as `3_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '300-400' and is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `3_臻选渗透率`
        ,count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end) `3_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '300-400' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `3_臻选房屋占比`
         
        ,nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0) as `4_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '400-500' and is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `4_臻选渗透率`
        ,count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end) `4_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '400-500' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `4_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0) as `5_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '500+' and is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `5_臻选渗透率`
        ,count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end) `5_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '500+' and is_whitebox_pass = 1 then house_id end)*100,2),'%') `5_臻选房屋占比`
 
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as `臻选间夜`
        ,concat(round(nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when is_whitebox_pass = 1 then order_room_night_count end),0)*100,2),'%') as `臻选渗透率`
        ,count(distinct case when is_great_tag = '臻选' then house_id end) `臻选房屋`
        ,concat(round(count(distinct case when is_great_tag = '臻选' then house_id end)
                /count(distinct case when is_whitebox_pass = 1 then house_id end)*100,2),'%') `臻选房屋占比`
 
        from order_base
        where is_zhicai = '直采'
        group by 1,2
     
 
    union all
 
    
        select
        '大盘' as dim1,'汇总' as dim2
        ,nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0) as `0_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '0-200' then order_room_night_count end),0) *100,2),'%') as `0_臻选渗透率`
        ,count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end) `0_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '0-200' then house_id end)*100,2),'%') `0_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0) as `2_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '200-300' then order_room_night_count end),0) *100,2),'%') as `2_臻选渗透率`
        ,count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end) `2_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '200-300' then house_id end)*100,2),'%') `2_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0) as `3_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '300-400' then order_room_night_count end),0)*100,2),'%') as `3_臻选渗透率`
        ,count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end) `3_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '300-400' then house_id end)*100,2),'%') `3_臻选房屋占比`
         
        ,nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0) as `4_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '400-500' then order_room_night_count end),0)*100,2),'%') as `4_臻选渗透率`
        ,count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end) `4_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '400-500' then house_id end)*100,2),'%') `4_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0) as `5_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '500+' then order_room_night_count end),0)*100,2),'%') as `5_臻选渗透率`
        ,count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end) `5_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '500+' then house_id end)*100,2),'%') `5_臻选房屋占比`
 
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as `臻选间夜`
        ,concat(round(nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(order_room_night_count),0)*100,2),'%') as `臻选渗透率`
        ,count(distinct case when is_great_tag = '臻选' then house_id end) `臻选房屋`
        ,concat(round(count(distinct case when is_great_tag = '臻选' then house_id end)
                /count(distinct house_id)*100,2),'%') `臻选房屋占比`
 
        from order_base
        group by 1,2
 
 
    union all
 
    
        select
        '大盘' as dim1,bedroom_count_type as dim2
        ,nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0) as `0_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '0-200' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '0-200' then order_room_night_count end),0) *100,2),'%') as `0_臻选渗透率`
        ,count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end) `0_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '0-200' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '0-200' then house_id end)*100,2),'%') `0_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0) as `2_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '200-300' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '200-300' then order_room_night_count end),0) *100,2),'%') as `2_臻选渗透率`
        ,count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end) `2_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '200-300' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '200-300' then house_id end)*100,2),'%') `2_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0) as `3_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '300-400' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '300-400' then order_room_night_count end),0)*100,2),'%') as `3_臻选渗透率`
        ,count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end) `3_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '300-400' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '300-400' then house_id end)*100,2),'%') `3_臻选房屋占比`
         
        ,nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0) as `4_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '400-500' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '400-500' then order_room_night_count end),0)*100,2),'%') as `4_臻选渗透率`
        ,count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end) `4_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '400-500' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '400-500' then house_id end)*100,2),'%') `4_臻选房屋占比`
 
        ,nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0) as `5_臻选间夜`
        ,concat(round(nvl(sum(case when danju_adr_range = '500+' and is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(case when danju_adr_range = '500+' then order_room_night_count end),0)*100,2),'%') as `5_臻选渗透率`
        ,count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end) `5_臻选房屋`
        ,concat(round(count(distinct case when danju_adr_range = '500+' and is_great_tag = '臻选' then house_id end)
                /count(distinct case when danju_adr_range = '500+' then house_id end)*100,2),'%') `5_臻选房屋占比`
 
        ,nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0) as `臻选间夜`
        ,concat(round(nvl(sum(case when is_great_tag = '臻选' then order_room_night_count end),0)
            /nvl(sum(order_room_night_count),0)*100,2),'%') as `臻选渗透率`
        ,count(distinct case when is_great_tag = '臻选' then house_id end) `臻选房屋`
        ,concat(round(count(distinct case when is_great_tag = '臻选' then house_id end)
                /count(distinct house_id)*100,2),'%') `臻选房屋占比`
 
        from order_base
        group by 1,2
   
) t
where dim2 is not null
order by 1,instr('汇总,一居,二居,三居,四居+',dim2)