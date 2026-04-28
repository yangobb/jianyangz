-- 目标：曼谷 / 清迈 / 普吉岛 / 芭堤雅 / 东京 / 大阪 6 个城市
-- 直采房源（landlord_channel = 1 / landlord_channel_name = '平台商户'）近 14 天 pv 高、订单低，输出给销售做工
-- 口径：
--   流量：dws.dws_path_ldbo_d，wrapper_name in ('携程','途家','去哪儿')，is_oversea=1，user_type='用户'
--   订单：dws.dws_order，is_paysuccess_order=1，is_cancel_order=0，is_overseas=1
--   房屋：dws.dws_house_d，house_is_online=1，house_is_oversea=1，landlord_channel=1（直采）
--   默认筛选：近 14 天 pv >= 200，支付订单 <= 1，每个城市最多输出 top100；如销售池过大/过小，可调整 params。

with params as (
    select date_sub(current_date,14) as start_dt
        ,date_sub(current_date,1) as end_dt
        ,200 as min_pv
        ,1 as max_paid_order
        ,100 as city_top_n
)
,target_city as (
    select city_name
    from (
        select '曼谷' city_name union all
        select '清迈' union all
        select '普吉岛' union all
        select '芭堤雅' union all
        select '东京' union all
        select '大阪'
    ) t
)
,flow as (
    select a.city_name
        ,a.house_id
        ,count(1) as lpv
        ,count(distinct concat(a.dt,a.uid)) as luv
        ,count(case when a.detail_uid is not null then 1 end) as dpv
        ,count(distinct case when a.detail_uid is not null then concat(a.dt,a.uid) end) as duv
        ,count(case when a.booking_uid is not null then 1 end) as bpv
        ,count(distinct case when a.booking_uid is not null then concat(a.dt,a.uid) end) as buv
        ,nvl(sum(a.without_risk_access_order_num),0) as order_num_z
        ,nvl(sum(a.without_risk_access_order_room_night),0) as night_z
        ,nvl(sum(a.without_risk_access_order_gmv),0) as gmv_z
        ,nvl(avg(a.final_price),0) as final_price
    from dws.dws_path_ldbo_d a
    inner join target_city b
        on a.city_name = b.city_name
    cross join params p
    where a.dt between p.start_dt and p.end_dt
        and a.wrapper_name in ('携程','途家','去哪儿')
        and a.is_oversea = 1
        and a.user_type = '用户'
        and a.house_id is not null
    group by 1,2
)
,h as (
    select a.house_id
        ,a.hotel_id
        ,a.country_name
        ,a.house_city_name
        ,a.hotel_name
        ,a.house_name
        ,a.house_class
        ,a.landlord_channel
        ,a.landlord_channel_name
        ,to_date(a.hotel_first_active_time) as hotel_first_active_date
        ,to_date(a.house_first_active_time) as house_first_active_date
        ,case when a.hotel_first_active_time >= date_sub(to_date(date_trunc('MM', date_sub(current_date, 1))),59)
            then '新房东'
            else '老房东'
        end as landlord_type
        ,case when a.bedroom_count = 1 then '一居'
            when a.bedroom_count = 2 then '二居'
            when a.bedroom_count >= 3 then '三居以上'
            else '其他' end bedroom_count
        ,a.avaliable_count
        ,a.picture_count
        ,a.bedroom_picture_count
        ,a.bathroom_picture_count
    from dws.dws_house_d a
    where a.dt = date_sub(current_date,1)
        and a.house_is_online = 1
        and a.house_is_oversea = 1
        and a.landlord_channel = 1   -- 直采
)
,video as (
    select house_id
        ,1 as has_video
    from dws.dws_house_video_info_d
    where dt = date_sub(current_date,1)
        and source = 1
    group by 1
)
,od as (
    select a.house_id
        ,count(distinct a.order_no) as order_num_k
        ,sum(a.order_room_night_count) as night_k
        ,sum(a.room_total_amount) as gmv_k
    from dws.dws_order a
    cross join params p
    where a.create_date between p.start_dt and p.end_dt
        and a.is_paysuccess_order = 1
        and a.is_cancel_order = 0
        and a.is_overseas = 1
    group by 1
)
,joined as (
    select a.city_name
        ,h.country_name
        ,h.house_city_name
        ,h.hotel_id
        ,h.hotel_name
        ,a.house_id
        ,h.house_name
        ,h.house_class
        ,h.landlord_channel
        ,h.landlord_channel_name
        ,h.landlord_type
        ,h.hotel_first_active_date
        ,h.house_first_active_date
        ,h.bedroom_count
        ,h.avaliable_count
        ,h.picture_count
        ,h.bedroom_picture_count
        ,h.bathroom_picture_count
        ,nvl(v.has_video,0) as has_video
        ,a.lpv
        ,a.luv
        ,a.dpv
        ,a.duv
        ,a.bpv
        ,a.buv
        ,a.order_num_z
        ,a.night_z
        ,a.gmv_z
        ,a.final_price
        ,nvl(c.order_num_k,0) order_num_k
        ,nvl(c.night_k,0) night_k
        ,nvl(c.gmv_k,0) gmv_k
    from flow a
    inner join h            -- inner join 直接锁定直采在线海外房源
        on a.house_id = h.house_id
    left join od c
        on a.house_id = c.house_id
    left join video v
        on a.house_id = v.house_id
)
,filtered as (
    select a.*
        ,row_number() over(
            partition by a.city_name
            order by a.order_num_k asc,a.lpv desc,a.luv desc,a.duv desc
        ) as city_priority_rank
        ,case when a.order_num_k = 0 then '高pv无支付订单'
            when a.order_num_k = 1 then '高pv低支付订单'
            else '高pv订单偏低'
        end as sales_reason
    from joined a
    cross join params p
    where a.lpv >= p.min_pv
        and a.order_num_k <= p.max_paid_order
)

select city_priority_rank `城市内优先级`
    ,sales_reason `销售作业原因`
    ,city_name `流量城市`
    ,country_name `国家`
    ,house_city_name `房屋城市`
    ,hotel_id `门店id`
    ,hotel_name `门店名称`
    ,house_id `房屋id`
    ,house_name `房屋名称`
    ,house_class `房屋等级`
    ,landlord_channel `房东渠道id`
    ,landlord_channel_name `房东渠道`
    ,landlord_type `房东类型`
    ,hotel_first_active_date `门店首次上线日期`
    ,house_first_active_date `房屋首次上线日期`
    ,bedroom_count `居室`
    ,avaliable_count `有效库存`
    ,picture_count `图片数`
    ,bedroom_picture_count `卧室图片数`
    ,bathroom_picture_count `卫生间图片数`
    ,has_video `是否有视频`
    ,lpv `近14天pv`
    ,luv `近14天uv`
    ,dpv `近14天dpv`
    ,duv `近14天duv`
    ,case when lpv = 0 then '0.00%' else concat(round(dpv / lpv * 100,2),'%') end `近14天l2d_pv`
    ,case when luv = 0 then '0.00%' else concat(round(duv / luv * 100,2),'%') end `近14天l2d_uv`
    ,bpv `近14天bpv`
    ,buv `近14天buv`
    ,order_num_z `近14天归因订单数`
    ,night_z `近14天归因间夜`
    ,round(gmv_z,2) `近14天归因gmv`
    ,order_num_k `近14天支付订单数`
    ,night_k `近14天支付间夜`
    ,round(gmv_k,2) `近14天支付gmv`
    ,case when night_k = 0 then 0 else round(gmv_k / night_k,2) end `支付adr`
    ,case when luv = 0 then 0 else round(gmv_k / luv,2) end `支付uv价值`
    ,round(final_price,2) `曝光均价`
    ,date_sub(current_date,14) `开始日期`
    ,date_sub(current_date,1) `结束日期`
from filtered
cross join params p
where city_priority_rank <= p.city_top_n
order by `流量城市`,`城市内优先级`
;
