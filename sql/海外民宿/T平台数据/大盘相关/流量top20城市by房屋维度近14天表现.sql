with top_city as (
select city_name
    ,lpv
    ,row_number() over(order by lpv desc) city_rank
from (
    select city_name
        ,count(1) lpv
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1)
    and wrapper_name in ('携程','途家','去哪儿')
    and is_oversea = 1
    and user_type = '用户'
    and city_name is not null
    and house_id is not null
    group by 1
) a
)
,flow as (
select a.city_name
    ,a.house_id
    ,count(1) lpv
    ,count(distinct concat(a.dt,a.uid)) luv
    ,count(case when a.detail_uid is not null then 1 end) dpv
    ,count(distinct case when a.detail_uid is not null then concat(a.dt,a.uid) end) duv
    ,count(case when a.booking_uid is not null then 1 end) bpv
    ,count(distinct case when a.booking_uid is not null then concat(a.dt,a.uid) end) buv
    ,nvl(sum(a.without_risk_access_order_num),0) order_num_z
    ,nvl(sum(a.without_risk_access_order_room_night),0) night_z
    ,nvl(sum(a.without_risk_access_order_gmv),0) gmv_z
    ,nvl(avg(a.final_price),0) final_price
from dws.dws_path_ldbo_d a
inner join top_city b
on a.city_name = b.city_name
where a.dt between date_sub(current_date,14) and date_sub(current_date,1)
and a.wrapper_name in ('携程','途家','去哪儿')
and a.is_oversea = 1
and a.user_type = '用户'
and a.house_id is not null
and b.city_rank <= 20
group by 1,2
)
,h as (
select a.house_id
    ,a.hotel_id
    ,a.house_class
    ,case when a.landlord_channel = 1 then '直采'
        when a.landlord_channel = 334 then 'C接'
        else '其他' end landlord_channel
    ,case when a.bedroom_count = 1 then '一居'
        when a.bedroom_count = 2 then '二居'
        when a.bedroom_count >= 3 then '三居以上'
        else '其他' end bedroom_count
from dws.dws_house_d a
where a.dt = date_sub(current_date,1)
and a.house_is_online = 1
and a.house_is_oversea = 1
)
,od as (
select a.house_id
    ,count(distinct a.order_no) order_num_k
    ,sum(a.order_room_night_count) night_k
    ,sum(a.room_total_amount) gmv_k
from dws.dws_order a
where a.create_date between date_sub(current_date,14) and date_sub(current_date,1)
and a.is_paysuccess_order = 1
and a.is_cancel_order = 0
and a.is_overseas = 1
group by 1
)

select b.city_rank `城市流量排名`
    ,a.city_name `城市`
    ,h.hotel_id `门店id`
    ,a.house_id `房屋id`
    ,h.house_class `房屋等级`
    ,h.landlord_channel `房东类型`
    ,h.bedroom_count `居室`
    ,a.lpv `近14天pv`
    ,a.luv `近14天uv`
    ,a.dpv `近14天dpv`
    ,a.duv `近14天duv`
    ,case when a.lpv = 0 then '0.00%' else concat(round(a.dpv / a.lpv * 100,2),'%') end `近14天l2d_pv`
    ,case when a.luv = 0 then '0.00%' else concat(round(a.duv / a.luv * 100,2),'%') end `近14天l2d_uv`
    ,a.bpv `近14天bpv`
    ,a.buv `近14天buv`
    ,a.order_num_z `近14天归因订单数`
    ,a.night_z `近14天归因间夜`
    ,round(a.gmv_z,2) `近14天归因gmv`
    ,nvl(c.order_num_k,0) `近14天支付订单数`
    ,nvl(c.night_k,0) `近14天支付间夜`
    ,round(nvl(c.gmv_k,0),2) `近14天支付gmv`
    ,case when nvl(c.night_k,0) = 0 then 0 else round(nvl(c.gmv_k,0) / c.night_k,2) end `支付adr`
    ,case when a.luv = 0 then 0 else round(nvl(c.gmv_k,0) / a.luv,2) end `支付uv价值`
    ,round(a.final_price,2) `曝光均价`
    ,date_sub(current_date,14) `开始日期`
    ,date_sub(current_date,1) `结束日期`
from flow a
inner join top_city b
on a.city_name = b.city_name
left join h
on a.house_id = h.house_id
left join od c
on a.house_id = c.house_id
where b.city_rank <= 20
order by b.city_rank,a.lpv desc
;
