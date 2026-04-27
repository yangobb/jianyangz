-- 海外重点城市 S/A/B · 滚动7天 · 支付口径
-- 占位符 ${end_date} 由 skill 运行时替换为 YYYY-MM-DD
-- 输出：city_name | city_level | period | gmv | room_night | order_cnt

with key_cities as (
    select city_name, city_level
    from excel_upload.oversea_city_level
    where city_level in ('S','A','B')
),
base as (
    select
        o.city_name,
        case
            when o.create_date between date_sub('${end_date}', 6) and '${end_date}'             then '本期'
            when o.create_date between date_sub('${end_date}',13) and date_sub('${end_date}',7) then '上期'
            when o.create_date between date_sub(add_months('${end_date}',-12),6) and add_months('${end_date}',-12)
                                                                                                then '去年同期'
        end as period,
        o.order_no,
        o.room_total_amount,
        o.order_room_night_count
    from dws.dws_order o
    join key_cities k on o.city_name = k.city_name
    where o.is_overseas = 1
      and o.is_paysuccess_order = 1
      and (
            o.create_date between date_sub('${end_date}',13) and '${end_date}'
         or o.create_date between date_sub(add_months('${end_date}',-12),6) and add_months('${end_date}',-12)
          )
)
select
    b.city_name,
    k.city_level,
    b.period,
    round(sum(b.room_total_amount), 0) as gmv,
    sum(b.order_room_night_count)      as room_night,
    count(distinct b.order_no)         as order_cnt
from base b
join key_cities k on b.city_name = k.city_name
where b.period is not null
group by b.city_name, k.city_level, b.period
order by k.city_level, b.city_name, case b.period when '本期' then 1 when '上期' then 2 when '去年同期' then 3 end;
