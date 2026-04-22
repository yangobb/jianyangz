with a as (
select distinct
trade_currency, --交易币种  
biz_order_no  
from ods_pay_core.payment_trade
),
b as (
select  
    -- *
    landlord_id
    ,hotel_id
    ,case when collectionAccountType = 'BANK' then 'BANK'
        when collectionAccountType = 'PAYONEER_ACC' and bindStatus = 'SUCCESS' then 'PAYONEER_ACC' 
        else 'NONE' end collectionAccountType
from (
    select landlord_id
        ,hotel_id
        ,content
        ,get_json_object(content,concat('$.collectionAccountInfoMap.', get_json_object(content, '$.currentCollectionAccountKey'),'.collectionAccountType')) AS collectionAccountType
        ,get_json_object(content,concat('$.collectionAccountInfoMap.',get_json_object(content, '$.currentCollectionAccountKey'),'.payoneerAccInfo.bindStatus')) AS bindStatus
    from (
        select *
            ,row_number() over(partition by landlord_id order by update_time desc) rn 
        from ods_tns_order_settle.settle_account
    ) tmp
    where rn = 1 
) a 


),
o1 as (
    select distinct
        create_date
        ,create_time
        ,country_name
        ,city_name
        ,sell_channel
        ,user_id
        ,house_id
        ,order_no
        ,o.landlord_id
        ,is_risk_order
        ,base_amount
        ,exchange_rate_to_rmb --汇率快照
        ,room_total_amount
        ,order_room_night_count
        ,case when collectionAccountType = 'BANK' and country_name = '日本' then '美国银行（日本）'
            when collectionAccountType = 'BANK' and country_name = '泰国' then '美国银行（泰国）'
            when collectionAccountType = 'PAYONEER_ACC' and country_name = '日本' then '派安盈账户（日本）'
            when collectionAccountType = 'PAYONEER_ACC' and country_name = '泰国' then '派安盈账户（泰国）'
        else 'NONE' end collectionAccountType
        ,case 
            when (terminal_type_name like '%本站-%' or terminal_type_name like '%蚂蚁%') then 'T'
            when terminal_type_name like '%携程-%' then 'C'
            when terminal_type_name like '%去哪儿-%' then 'Q'
            when terminal_type_name like '%酒店%' then '分销'
            else '其他' end channel
        ,case
            when landlord_channel = 1 then '直采'
            when landlord_channel = 303 then '携程接入'
            else '小集团'
            end as housechannel
        ,case when country_name='日本' then  CEIL(nvl(base_amount,0)/exchange_rate_to_rmb) 
            else nvl(base_amount,0)/exchange_rate_to_rmb end foreign_order_base_amount
        ,case when country_name='日本' then  CEIL(nvl(room_total_amount,0)/exchange_rate_to_rmb) 
            else nvl(room_total_amount,0)/exchange_rate_to_rmb end foreign_room_total_amount
    from (
        select distinct *
            ,case when commission_rate>1 then commission_rate/100 else commission_rate end commission_ratenew
            ,case when landlord_channel=303 then order_base_amount
                when nvl(landlord_channel,0) !=303 and commission_type=2 then order_base_amount 
                when nvl(landlord_channel,0) !=303 and commission_type=1 then room_total_amount*(1-commission_ratenew)
                else 0 
                end base_amount --底价  
        from dws.dws_order o
        where substr(create_date,1,7) = add_months(current_date,-1)
        and is_paysuccess_order = '1' 
        and is_overseas = 1--国wai 
        and is_confirm_order=1
        and country_name in ('日本','泰国')
        -- and country_name in ('泰国')
        and landlord_channel = 1
    ) o
    left join b 
    on o.landlord_id = b.landlord_id
)
,o2 as (
    select distinct
        create_date
        ,create_time
        ,to_date(cancel_time) cancel_date
        ,country_name
        ,city_name
        ,sell_channel
        ,user_id
        ,house_id
        ,order_no
        ,o.landlord_id
        ,is_risk_order
        ,base_amount
        ,exchange_rate_to_rmb --汇率快照
        ,room_total_amount
        ,order_room_night_count
        ,case when collectionAccountType = 'BANK' and country_name = '日本' then '美国银行（日本）'
            when collectionAccountType = 'BANK' and country_name = '泰国' then '美国银行（泰国）'
            when collectionAccountType = 'PAYONEER_ACC' and country_name = '日本' then '派安盈账户（日本）'
            when collectionAccountType = 'PAYONEER_ACC' and country_name = '泰国' then '派安盈账户（泰国）'
        else 'NONE' end collectionAccountType
        ,case 
            when (terminal_type_name like '%本站-%' or terminal_type_name like '%蚂蚁%') then 'T'
            when terminal_type_name like '%携程-%' then 'C'
            when terminal_type_name like '%去哪儿-%' then 'Q'
            when terminal_type_name like '%酒店%' then '分销'
            else '其他' end channel
        ,case
            when landlord_channel = 1 then '直采'
            when landlord_channel = 303 then '携程接入'
            else '小集团'
            end as housechannel
        ,case when country_name='日本' then  CEIL(nvl(base_amount,0)/exchange_rate_to_rmb) 
            else nvl(base_amount,0)/exchange_rate_to_rmb end foreign_order_base_amount
        ,case when country_name='日本' then  CEIL(nvl(room_total_amount,0)/exchange_rate_to_rmb) 
            else nvl(room_total_amount,0)/exchange_rate_to_rmb end foreign_room_total_amount
    from (
        select distinct *
            ,case when commission_rate>1 then commission_rate/100 else commission_rate end commission_ratenew
            ,case when landlord_channel=303 then order_base_amount
                when nvl(landlord_channel,0) !=303 and commission_type=2 then order_base_amount 
                when nvl(landlord_channel,0) !=303 and commission_type=1 then room_total_amount*(1-commission_ratenew)
                else 0 
                end base_amount --底价  
        from dws.dws_order o
        where -- to_date(cancel_time) =date_sub(current_date,1)
            substr(to_date(cancel_time),1,7) = add_months(current_date,-1)
        and is_cancel_order = '1'
        and is_confirm_order=1
        and is_overseas = 1--国wai 
        --and is_cancel_order = 0 
        --and is_risk_order = 0 --非风控单
        and country_name in ('日本','泰国')
        -- and country_name in ('泰国')
        and landlord_channel = 1
    ) o
    left join b 
    on o.landlord_id = b.landlord_id
)

select
    create_date,
    '支付成功且商户确认订单' as type,
    channel,
    sell_channel,
    country_name,
    city_name,
    housechannel,
    a.trade_currency `交易币种`,
    o.exchange_rate_to_rmb `预定汇率`,
    case when is_risk_order=0 then '否' else '是' end `是否风控单`,
    round(sum(room_total_amount),0) `预定GMV`,
    round(sum(foreign_room_total_amount),2) `预定gmv(原币)`,
    round(sum(base_amount),2) `商户结算`,
    round(sum(foreign_order_base_amount),2) `商户结算(原币)`,
    sum(order_room_night_count) as `间夜数`
    ,nvl(collectionAccountType,'空') `绑卡类型`
    ,order_no
from (
    select * from o1
) o
left join a on o.order_no=a.biz_order_no
group by 1,2,3,4,5,6,7,8,9,10,16,17

union all 

select
    cancel_date,
    '昨天取消的商户确认订单' as type,
    channel,
    sell_channel,
    country_name,
    city_name,
    housechannel,
    a.trade_currency `交易币种`,
    '0' as `预定汇率`,
    case when is_risk_order=0 then '否' else '是' end `是否风控单`,
    round(sum(room_total_amount),0) `预定GMV`,
    round(sum(foreign_room_total_amount),2) `预定gmv(原币)`,
    round(sum(base_amount),2) `商户结算`,
    round(sum(foreign_order_base_amount),2) `商户结算(原币)`,
    sum(order_room_night_count) as `间夜数`
    ,nvl(collectionAccountType,'空') `绑卡类型`
    ,order_no
from (
    select * from o2
) o
left join a on o.order_no=a.biz_order_no
group by 1,2,3,4,5,6,7,8,9,10,16,17