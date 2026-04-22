--红包领用数据
with red_gain_use as (
select dt,
p.red_name,
p.channel,
red_gain,
all_order,
total_red_order,
total_red_amount,
red_gmv,
gmv,
total_red_room_night,
all_night
from
---红包领取情况
(select to_date(create_time) as dt
,case 
        when activity_code in ('rp_CZqetSIE16s','rp_CNIOkczBN0U','rp_CNINmEIQBK2','rp_CZqd3APDTxe','rp_CNyGeoM3QZc','rp_CNyGo9lSdgs','rp_CNrKMbHau8B','rp_CNrK3FsC8YZ','rp_CNsmWFVbvuy','rp_CNs0BvgKfpe') then '国庆连住券'
        when activity_code in ('rp_COLC6tvCXl4','rp_COLBh5racEj','rp_COLfEm00tvF','rp_CN1gLosJaeA','rp_CN1g2wZgTSY','rp_CNOny96VA8O','rp_CNfYn3LqOIO','rp_CNfMkAIAbec','rp_CZ7fC9OlR5W','rp_CZ7EVa7aD6E','rp_CZ7Go66Bvm6','rp_CZ7POcQLvIi','rp_CZmjnvgMjHW','rp_CZmFnrGbVfa','rp_CZmVd5hcvz4','rp_CZmWLimvgeU','rp_CZqdL88gVu2','rp_CZqaDANxy5S','rp_CZqDNryMGg0','rp_CZqDksd8kC4','rp_CZqD4IsWHMy','rp_CZqAcKeaumc','rp_CZoLA9ONQWU','rp_CZoXmPIlGec','rp_CLvKqHICdij','rp_CLvK2BYUpTi','rp_CL2U7ZRXu4c',"rp_CL2k7X60Ohq","rp_CL2yckXdm5m",'rp_CZf2Tr1ixcA','rp_CZJFz6Lnlc0','rp_CZJFbdMzJ77','rp_CZJc0f02nVe','rp_CZjIYUF4FNm','rp_CZAIWRn2Uuq','rp_CZAivbfV4dq') then '连住专享券'
        when activity_code in ('rp_CNfBjoF5Xd4','rp_CL0yEbHT2wQ','rp_CL0l19XVJ60','rp_COozQ581F6w') then '文本直搜券' 
        when activity_code in ('rp_CNk1EqjcrbX') then '国庆贵宾礼遇' 
        when activity_code in ('rp_CNPAK6Zs4xa','rp_CNPeBqpPHXg','rp_CNPehrlpYsQ','rp_CNPDJIbUlxe','rp_CNYe6k1xxao','rp_CNYDoo4LmWA','rp_CNYgOKWwTRW','rp_CNYhEKnqwzy') then '国庆复购券'
        when activity_code in ('rp_CZPwXbHK6ms','rp_CZPmfeSPRx2') then '810冲高券'
        when activity_code in ('rp_CNcFecu1VM2','rp_CNcjBn9ylsE','rp_CNcXl3DTzzi') then '错峰出游红包'
        when activity_code in ('rp_CNPEPmFjsby','rp_CNPErLaHx3i','rp_CNNjRCHZObO','rp_CNNHlLtYqgY','rp_CNOYnZU7LNO','rp_CNOKoBMJh5y','rp_CNcJuNMhmXk','rp_CNcLIpaDRAs','rp_CNcZjNt7u1y','rp_CNcZh4XMint','rp_CNMRkqtysGq','rp_CNMSsa5qQ6w') then '国庆早订券'
        when activity_code in ('rp_CNfWah387ha','rp_CNfd60qFuHq','rp_CNfDJgeoaVi','rp_CNfDnmIBXB7','rp_CZBb5P5Gyjw','rp_CZBaTmZOHU7','rp_CLot8SxnnQs','rp_CLot6Zwr9Y0','rp_CLoujlDEqX7','rp_CLouGdnL6uB','rp_CZ9YLip1ntY','rp_CZ9Y4wnT8oZ') then '早鸟券'
    else '' end as red_name
    ,case 
        when enumpromopartytype = 0 then '途家'
        when enumpromopartytype = 1 then '去哪儿'
        when enumpromopartytype = 2 then '携程' 
    else '' end as channel
,count(distinct promo_code) as red_gain
from ods_tujiaonlinepromo.promo --红包领取表
where  activity_code in ('rp_CNOny96VA8O',
'rp_CNfYn3LqOIO',
'rp_CNfMkAIAbec',
'rp_CZ7fC9OlR5W',
'rp_CZ7EVa7aD6E',
'rp_CZ7Go66Bvm6',
'rp_CZ7POcQLvIi',
'rp_CZmjnvgMjHW',
'rp_CZmFnrGbVfa',
'rp_CZmVd5hcvz4',
'rp_CZmWLimvgeU',
'rp_CZqdL88gVu2',
'rp_CZqd3APDTxe',
'rp_CZqetSIE16s',
'rp_CZqaDANxy5S',
'rp_CZqDNryMGg0',
'rp_CZqDksd8kC4',
'rp_CZqD4IsWHMy',
'rp_CZqAcKeaumc',
'rp_CZoLA9ONQWU',
'rp_CZoXmPIlGec',
'rp_CLvKqHICdij',
'rp_CLvK2BYUpTi',
'rp_CL2U7ZRXu4c',
"rp_CL2k7X60Ohq",
"rp_CL2yckXdm5m",
'rp_CZf2Tr1ixcA',
'rp_CZJFz6Lnlc0',
'rp_CZJFbdMzJ77',
'rp_CZJc0f02nVe',
'rp_CZjIYUF4FNm',
'rp_CZAIWRn2Uuq',
'rp_CZAivbfV4dq',
'rp_CNfBjoF5Xd4',
'rp_CL0yEbHT2wQ',
'rp_CL0l19XVJ60',
'rp_CZPwXbHK6ms',
'rp_CZPmfeSPRx2',
'rp_CNcFecu1VM2',
'rp_CNcjBn9ylsE',
'rp_CNcXl3DTzzi',
'rp_CNPEPmFjsby',
'rp_CNPErLaHx3i',
'rp_CNNjRCHZObO',
'rp_CNNHlLtYqgY',
'rp_CNOYnZU7LNO',
'rp_CNOKoBMJh5y',
'rp_CNcJuNMhmXk',
'rp_CNcLIpaDRAs',
'rp_CNcZjNt7u1y',
'rp_CNcZh4XMint',
'rp_CNfWah387ha',
'rp_CNfd60qFuHq',
'rp_CNfDJgeoaVi',
'rp_CNfDnmIBXB7',
'rp_CZBb5P5Gyjw',
'rp_CZBaTmZOHU7',
'rp_CLot8SxnnQs',
'rp_CLot6Zwr9Y0',
'rp_CLoujlDEqX7',
'rp_CLouGdnL6uB',
'rp_CZ9YLip1ntY',
'rp_CZ9Y4wnT8oZ',

'rp_CNPAK6Zs4xa',
'rp_CNPeBqpPHXg',
'rp_CNPehrlpYsQ',
'rp_CNPDJIbUlxe',
'rp_CNYe6k1xxao',
'rp_CNYDoo4LmWA',
'rp_CNYgOKWwTRW',
'rp_CNYhEKnqwzy',
'rp_CNMRkqtysGq',
'rp_CNMSsa5qQ6w',

'rp_CNIOkczBN0U',
'rp_CNINmEIQBK2',
'rp_CNyGeoM3QZc',
'rp_CNyGo9lSdgs',
'rp_CNk1EqjcrbX',
'rp_CNrKMbHau8B',
'rp_CNrK3FsC8YZ',
'rp_CNsmWFVbvuy','rp_CNs0BvgKfpe',

'rp_CN1gLosJaeA','rp_CN1g2wZgTSY',
'rp_COLC6tvCXl4','rp_COLBh5racEj','rp_COLfEm00tvF',
'rp_COgkaXCAPCW',
'rp_COozQ581F6w'
)
and to_date(create_time) between date_sub(current_date,7) and date_sub(current_date,1)
group by 1,2,3
) p
left join
(
  select distinct
    o.create_date
    ,q.activity_name
    ,case 
        when terminal_type_name REGEXP '本站' then '途家'
        when terminal_type_name REGEXP '携程' then '携程'
        when terminal_type_name REGEXP '去哪儿' then '去哪儿'
    else '其他' end channel
    ,count(distinct o.order_no) as all_order
    ,count(distinct if(q.order_no is not null,o.order_no,null)) as total_red_order
    ,sum(q.pay_amount) as total_red_amount
    ,sum(if(q.order_no is not null,room_total_amount,null))As red_gmv
    ,sum(room_total_amount)As gmv
    ,sum(if(q.order_no is not null,order_room_night_count,null))as total_red_room_night
    ,sum(order_room_night_count) all_night
  from dws.dws_order o 
  left join (
select 
to_date(create_time) dt,
order_no,
case 
    when activity_name like '%国庆连住券%' then '国庆连住券'
    when activity_name like '%国庆贵宾礼遇%' then '国庆贵宾礼遇'
    when activity_name regexp '暑期连住券|连住专享券' then '连住专享券'
    when activity_name like '%错峰出游红包%' then '错峰出游红包'
    when activity_name like '%国庆复购券%' then '国庆复购券'
    when activity_name like '%国庆早订券%' then '国庆早订券'
    when activity_name REGEXP '惊喜红包|房东感恩红包' then '文本直搜券'
    when activity_name like '%810福利券%' then '810冲高券'
    when activity_name like '%惊喜早鸟特惠%' then '早鸟券'
else '' end activity_name,
pay_amount
from
  dwd.dwd_order_pay_d
where
    dt = date_sub(current_date,1) --取最新即可
    and activity_name regexp '错峰出游红包|国庆早订券|连住专享券|暑期连住券|惊喜红包|810福利券|惊喜早鸟特惠|房东感恩红包|国庆复购券|国庆连住券'
    and payment_provider = 1
) q --使用24年暑期连住券
  on o.order_no = q.order_no  
  where o.is_success_order=1 --支付成功
   and order_status<>'CANCELLED' --非取消
   and o.create_date >= date_sub(current_date,7)  --下单日期
--   and checkin_date between date_sub(current_date,7) and '2025-08-31'
  group by 1,2,3
)pr
on p.dt=pr.create_date and p.red_name = pr.activity_name and p.channel = pr.channel
),
 
red_gain_use_dt as (
select 
channel
,dt as `领券日期`
,red_name as `红包名称`
,sum(red_gain) as `领取量`
,sum(total_red_order) as `使用订单量`
,concat(round(sum(total_red_order)/sum(red_gain)*100,2),'%') as `核销率`
,round(sum(red_gmv)/10000,2) as `用券订单gmv(万)`
,sum(total_red_room_night) as `使用间夜量`
,round(sum(total_red_amount)/10000,2) as `红包使用金额(万)`
,cast(sum(red_gmv)/sum(total_red_room_night) as bigint) as `使用补贴ADR`
,concat(round(sum(total_red_amount)/sum(red_gmv)*100,2),'%') as `使用补贴率`
,round(sum(gmv)/10000,2) as `大盘gmv(万)`
,sum(all_order) as `大盘订单量`
,sum(all_night) as `大盘间夜量`
,cast(sum(gmv)/sum(all_night) as bigint) as `大盘ADR`
,concat(round(sum(total_red_amount)/sum(gmv)*100,2),'%') as `大盘补贴率`
from red_gain_use
group by 1,2,3
),
 
red_gain_use_all as (

select 
channel
,'总计' as `领券日期`
,red_name as `红包名称`

,sum(red_gain) as `领取量`
,sum(total_red_order) as `使用订单量`
,concat(round(sum(total_red_order)/sum(red_gain)*100,2),'%') as `核销率`
,round(sum(red_gmv)/10000,2) as `用券订单gmv(万)`
,sum(total_red_room_night) as `使用间夜量`
,round(sum(total_red_amount)/10000,2) as `红包使用金额(万)`
,cast(sum(red_gmv)/sum(total_red_room_night) as bigint) as `使用补贴ADR`
,concat(round(sum(total_red_amount)/sum(red_gmv)*100,2),'%') as `使用补贴率`
,round(sum(gmv)/10000,2) as `大盘gmv(万)`
,sum(all_order) as `大盘订单量`
,sum(all_night) as `大盘间夜量`
,cast(sum(gmv)/sum(all_night) as bigint) as `大盘ADR`
,concat(round(sum(total_red_amount)/sum(gmv)*100,2),'%') as `大盘补贴率`
from red_gain_use
group by 1,2,3
)
 
select *
from (
select *
from red_gain_use_all

union all 

select 
*
from (
select *
from red_gain_use_dt
order by `领券日期` desc
) a
) mm 

order by channel,`红包名称`,`领券日期` desc
