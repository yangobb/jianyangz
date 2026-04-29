-- 目标：泰国四城（曼谷/清迈/普吉岛/芭堤雅）+ 日本两城（东京/大阪）
-- 直采房源明细，输出每套房屋的均价（曝光均价 / 支付ADR）及"同商圈+同居室"对标均价
-- 口径：
--   房屋：dws.dws_house_d，house_is_online=1，house_is_oversea=1，landlord_channel=1（直采）
--   流量曝光均价：dws.dws_path_ldbo_d 近 14 天，wrapper_name in ('携程','途家','去哪儿')，is_oversea=1，user_type='用户'
--   支付ADR：dws.dws_order 近 14 天，is_paysuccess_order=1，is_cancel_order=0，is_overseas=1
--   商圈：dws.dws_house_d.dynamic_business（如线上字段名不同请整体替换）
--   同商圈同居室对标 = 同 (city_name, dynamic_business, bedroom_bucket) 内、所有直采房屋的均价（不含自身则改写为 (sum-x)/(cnt-1)，本版按"含自身"算，量大时近似一致）

with params as (
    select date_sub(current_date,14) as start_dt
        ,date_sub(current_date,1)  as end_dt
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
,h as (
    select a.house_id
        ,a.hotel_id
        ,a.country_name
        ,a.house_city_name              -- 房屋城市
        ,a.dynamic_business       -- 商圈
        ,a.hotel_name
        ,a.house_name
        ,a.house_class
        ,a.landlord_channel
        ,a.landlord_channel_name
        ,a.bedroom_count
        ,case when a.bedroom_count = 1 then '一居'
            when a.bedroom_count = 2 then '二居'
            when a.bedroom_count >= 3 then '三居以上'
            else '其他' end as bedroom_bucket
    from dws.dws_house_d a
    inner join target_city tc
        on a.house_city_name = tc.city_name      -- 用房屋城市匹配，避免流量城市与房屋归属差异
    where a.dt = date_sub(current_date,1)
        and a.house_is_online = 1
        and a.house_is_oversea = 1
        and a.landlord_channel = 1               -- 直采
)
,flow_price as (
    -- 房屋自身近14天曝光均价
    select a.house_id
        ,avg(a.final_price)        as house_expose_price
        ,count(1)                  as lpv
        ,count(distinct concat(a.dt,a.uid)) as luv
    from dws.dws_path_ldbo_d a
    cross join params p
    where a.dt between p.start_dt and p.end_dt
        and a.wrapper_name in ('携程','途家','去哪儿')
        and a.is_oversea = 1
        and a.user_type = '用户'
        and a.house_id is not null
        and a.final_price is not null
        and a.final_price > 0
    group by 1
)
,od_price as (
    -- 房屋自身近14天支付ADR
    select a.house_id
        ,sum(a.room_total_amount)        as gmv_k
        ,sum(a.order_room_night_count)   as night_k
        ,count(distinct a.order_no)      as order_num_k
    from dws.dws_order a
    cross join params p
    where a.create_date between p.start_dt and p.end_dt
        and a.is_paysuccess_order = 1
        and a.is_cancel_order = 0
        and a.is_overseas = 1
    group by 1
)
,base as (
    select h.house_id
        ,h.hotel_id
        ,h.country_name
        ,h.house_city_name
        ,h.dynamic_business
        ,h.hotel_name
        ,h.house_name
        ,h.house_class
        ,h.landlord_channel
        ,h.landlord_channel_name
        ,h.bedroom_count
        ,h.bedroom_bucket
        ,fp.house_expose_price
        ,fp.lpv
        ,fp.luv
        ,op.gmv_k
        ,op.night_k
        ,op.order_num_k
        ,case when nvl(op.night_k,0) = 0 then null
            else op.gmv_k / op.night_k end as house_paid_adr
    from h
    left join flow_price fp on h.house_id = fp.house_id
    left join od_price   op on h.house_id = op.house_id
)
,peer as (
    -- 同商圈同居室对标均价（仅在直采池内对标，且仅用有价格的房屋参与平均）
    select house_city_name
        ,dynamic_business
        ,bedroom_bucket
        ,avg(case when house_expose_price is not null then house_expose_price end) as peer_expose_price
        ,avg(case when house_paid_adr     is not null then house_paid_adr     end) as peer_paid_adr
        ,count(distinct case when house_expose_price is not null then house_id end) as peer_expose_house_cnt
        ,count(distinct case when house_paid_adr     is not null then house_id end) as peer_adr_house_cnt
        ,count(distinct house_id) as peer_house_cnt
    from base
    group by 1,2,3
)

select b.house_city_name              `房屋城市`
    ,b.dynamic_business         `商圈`
    ,b.bedroom_bucket                 `居室`
    ,b.country_name                   `国家`
    ,b.hotel_id                       `门店id`
    ,b.hotel_name                     `门店名称`
    ,b.house_id                       `房屋id`
    ,b.house_name                     `房屋名称`
    ,b.house_class                    `房屋等级`
    ,b.landlord_channel               `房东渠道id`
    ,b.landlord_channel_name          `房东渠道`
    ,round(b.house_expose_price,2)    `近14天曝光均价`
    ,round(p.peer_expose_price,2)     `同商圈同居室曝光均价`
    ,case when nvl(p.peer_expose_price,0) = 0 or b.house_expose_price is null then null
        else round((b.house_expose_price - p.peer_expose_price) / p.peer_expose_price * 100,2)
        end                           `曝光均价_VS_对标_百分比`
    ,round(b.house_paid_adr,2)        `近14天支付ADR`
    ,round(p.peer_paid_adr,2)         `同商圈同居室支付ADR`
    ,case when nvl(p.peer_paid_adr,0) = 0 or b.house_paid_adr is null then null
        else round((b.house_paid_adr - p.peer_paid_adr) / p.peer_paid_adr * 100,2)
        end                           `支付ADR_VS_对标_百分比`
    ,nvl(b.lpv,0)                     `近14天pv`
    ,nvl(b.luv,0)                     `近14天uv`
    ,nvl(b.order_num_k,0)             `近14天支付订单数`
    ,nvl(b.night_k,0)                 `近14天支付间夜`
    ,round(nvl(b.gmv_k,0),2)          `近14天支付gmv`
    ,p.peer_house_cnt                 `同商圈同居室房源数`
    ,p.peer_expose_house_cnt          `同商圈同居室有曝光均价房源数`
    ,p.peer_adr_house_cnt             `同商圈同居室有支付ADR房源数`
    ,date_sub(current_date,14)        `开始日期`
    ,date_sub(current_date,1)         `结束日期`
from base b
left join peer p
    on b.house_city_name        = p.house_city_name
    and nvl(b.dynamic_business,'-') = nvl(p.dynamic_business,'-')
    and b.bedroom_bucket        = p.bedroom_bucket
order by b.house_city_name, b.dynamic_business, b.bedroom_bucket, b.house_id
;
