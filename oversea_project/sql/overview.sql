-- 海外业绩大盘 · 滚动7天 · 支付口径
-- 占位符 ${end_date} 由 skill 运行时替换为 YYYY-MM-DD
-- 输出：period(本期/上期/去年同期) | gmv | room_night | order_cnt

with base as (
    select
        case
            when create_date between date_sub('${end_date}', 6) and '${end_date}'            then '本期'
            when create_date between date_sub('${end_date}',13) and date_sub('${end_date}',7) then '上期'
            when create_date between date_sub(add_months('${end_date}',-12),6) and add_months('${end_date}',-12)
                                                                                              then '去年同期'
        end as period,
        order_no,
        room_total_amount,
        order_room_night_count
    from dws.dws_order
    where is_overseas = 1
      and is_paysuccess_order = 1
      and (
            create_date between date_sub('${end_date}',13) and '${end_date}'
         or create_date between date_sub(add_months('${end_date}',-12),6) and add_months('${end_date}',-12)
          )
)
select
    period,
    round(sum(room_total_amount), 0)           as gmv,
    sum(order_room_night_count)                as room_night,
    count(distinct order_no)                   as order_cnt
from base
where period is not null
group by period
order by case period when '本期' then 1 when '上期' then 2 when '去年同期' then 3 end;
