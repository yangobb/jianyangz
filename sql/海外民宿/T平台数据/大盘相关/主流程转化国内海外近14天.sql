-- 主流程转化：国内 vs 海外，近14天
-- 口径：
-- 1. list页：dws.dws_path_ldbo_d 每行一次列表曝光，过滤 wrapper_name in ('携程','途家','去哪儿')、user_type='用户'
-- 2. detail页：detail_uid is not null
-- 3. booking页：booking_uid is not null
-- 4. order页：order_uid is not null
-- 5. 下单：dws_path_ldbo_d without_risk_order_num 归因下单；支付订单数补充来自 dws.dws_order
-- 6. 时间：date_sub(current_date,14) ~ date_sub(current_date,1)
-- 7. 国内流量量级较大，UV 使用 approx_count_distinct 近似去重；PV、订单、间夜、GMV 为精确聚合。

with params as (
    select date_sub(current_date,14) as start_dt
        ,date_sub(current_date,1) as end_dt
)
,ldbo_base as (
    select a.dt
        ,case when a.is_oversea = 1 then '海外' else '国内' end as market_type
        ,a.uid
        ,a.detail_uid
        ,a.booking_uid
        ,a.order_uid
        ,nvl(a.without_risk_order_num,0) as order_num
        ,nvl(a.without_risk_order_room_night,0) as room_night
        ,nvl(a.without_risk_order_gmv,0) as gmv
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.start_dt and p.end_dt
    and a.wrapper_name in ('携程','途家','去哪儿')
    and a.user_type = '用户'
    and a.is_oversea in (0,1)
)
,ldbo_daily as (
    select dt
        ,market_type
        ,count(1) as list_pv
        ,approx_count_distinct(uid) as list_uv
        ,count(case when detail_uid is not null then 1 end) as detail_pv
        ,approx_count_distinct(case when detail_uid is not null then uid end) as detail_uv
        ,count(case when booking_uid is not null then 1 end) as booking_pv
        ,approx_count_distinct(case when booking_uid is not null then uid end) as booking_uv
        ,count(case when order_uid is not null then 1 end) as order_page_pv
        ,approx_count_distinct(case when order_uid is not null then uid end) as order_page_uv
        ,sum(order_num) as attributed_order_num
        ,sum(room_night) as attributed_room_night
        ,sum(gmv) as attributed_gmv
    from ldbo_base
    group by 1,2
)
,paid_order_daily as (
    select a.create_date as dt
        ,case when a.is_overseas = 1 then '海外' else '国内' end as market_type
        ,count(distinct a.order_no) as paid_order_num
        ,sum(a.order_room_night_count) as paid_room_night
        ,sum(a.room_total_amount) as paid_gmv
    from dws.dws_order a
    cross join params p
    where a.create_date between p.start_dt and p.end_dt
    and a.is_paysuccess_order = 1
    and a.is_cancel_order = 0
    and a.is_overseas in (0,1)
    group by 1,2
)
,daily as (
    select '日明细' as period_type
        ,a.dt
        ,a.market_type
        ,a.list_pv
        ,a.list_uv
        ,a.detail_pv
        ,a.detail_uv
        ,a.booking_pv
        ,a.booking_uv
        ,a.order_page_pv
        ,a.order_page_uv
        ,a.attributed_order_num
        ,a.attributed_room_night
        ,a.attributed_gmv
        ,nvl(b.paid_order_num,0) as paid_order_num
        ,nvl(b.paid_room_night,0) as paid_room_night
        ,nvl(b.paid_gmv,0) as paid_gmv
    from ldbo_daily a
    left join paid_order_daily b
    on a.dt = b.dt
    and a.market_type = b.market_type
)
,summary as (
    select '近14天汇总' as period_type
        ,'合计' as dt
        ,market_type
        ,sum(list_pv) as list_pv
        ,sum(list_uv) as list_uv
        ,sum(detail_pv) as detail_pv
        ,sum(detail_uv) as detail_uv
        ,sum(booking_pv) as booking_pv
        ,sum(booking_uv) as booking_uv
        ,sum(order_page_pv) as order_page_pv
        ,sum(order_page_uv) as order_page_uv
        ,sum(attributed_order_num) as attributed_order_num
        ,sum(attributed_room_night) as attributed_room_night
        ,sum(attributed_gmv) as attributed_gmv
        ,sum(paid_order_num) as paid_order_num
        ,sum(paid_room_night) as paid_room_night
        ,sum(paid_gmv) as paid_gmv
    from daily
    group by 1,2,3
)
select period_type as `周期类型`
    ,dt as `日期`
    ,market_type as `市场`
    ,list_pv as `list页pv`
    ,list_uv as `list页uv`
    ,detail_pv as `detail页pv`
    ,detail_uv as `detail页uv`
    ,case when list_uv = 0 then 0 else round(detail_uv / list_uv * 100,2) end as `list到detail_uv转化率`
    ,booking_pv as `booking页pv`
    ,booking_uv as `booking页uv`
    ,case when detail_uv = 0 then 0 else round(booking_uv / detail_uv * 100,2) end as `detail到booking_uv转化率`
    ,order_page_pv as `order页pv`
    ,order_page_uv as `order页uv`
    ,case when booking_uv = 0 then 0 else round(order_page_uv / booking_uv * 100,2) end as `booking到order页uv转化率`
    ,attributed_order_num as `归因下单数`
    ,case when order_page_uv = 0 then 0 else round(attributed_order_num / order_page_uv * 100,2) end as `order页到归因下单转化率`
    ,case when list_uv = 0 then 0 else round(attributed_order_num / list_uv * 100,2) end as `list到归因下单转化率`
    ,attributed_room_night as `归因间夜`
    ,round(attributed_gmv,2) as `归因gmv`
    ,paid_order_num as `支付订单数`
    ,paid_room_night as `支付间夜`
    ,round(paid_gmv,2) as `支付gmv`
    ,case when paid_room_night = 0 then 0 else round(paid_gmv / paid_room_night,2) end as `支付adr`
from summary
union all
select period_type as `周期类型`
    ,dt as `日期`
    ,market_type as `市场`
    ,list_pv as `list页pv`
    ,list_uv as `list页uv`
    ,detail_pv as `detail页pv`
    ,detail_uv as `detail页uv`
    ,case when list_uv = 0 then 0 else round(detail_uv / list_uv * 100,2) end as `list到detail_uv转化率`
    ,booking_pv as `booking页pv`
    ,booking_uv as `booking页uv`
    ,case when detail_uv = 0 then 0 else round(booking_uv / detail_uv * 100,2) end as `detail到booking_uv转化率`
    ,order_page_pv as `order页pv`
    ,order_page_uv as `order页uv`
    ,case when booking_uv = 0 then 0 else round(order_page_uv / booking_uv * 100,2) end as `booking到order页uv转化率`
    ,attributed_order_num as `归因下单数`
    ,case when order_page_uv = 0 then 0 else round(attributed_order_num / order_page_uv * 100,2) end as `order页到归因下单转化率`
    ,case when list_uv = 0 then 0 else round(attributed_order_num / list_uv * 100,2) end as `list到归因下单转化率`
    ,attributed_room_night as `归因间夜`
    ,round(attributed_gmv,2) as `归因gmv`
    ,paid_order_num as `支付订单数`
    ,paid_room_night as `支付间夜`
    ,round(paid_gmv,2) as `支付gmv`
    ,case when paid_room_night = 0 then 0 else round(paid_gmv / paid_room_night,2) end as `支付adr`
from daily
order by `周期类型` desc,`日期`,`市场`
;
