select distinct area_type,
area_id,
area_name,
dynamic_id,
case when dynamic_name is null then '' else dynamic_name end as dynamic_name,
scene,
concat
(
    '{"channel":',Channel,
    ',"checkType":','"',check_type,'"',
    ',"weekType":','"',week_type,'"',
    ',"filterType":',filter_type,
    ',"buckets":','"',Buckets,'"',
    ',"stayDaysRange":','"',stay_days_range,'"',
    ',"l4CanSaleNum":',l4_canSaleNum,
    ',"l4CanSaleMaxNum":',l4_canSaleMaxNum,
    ',"l4CanSalePercent":',l4_canSalePercent,
    ',"l34CanSaleNum":',l34_canSaleNum,
    ',"l34CanSaleMaxNum":',l34_canSaleMaxNum,
    ',"l34CanSalePercent":',l34_canSalePercent,
    ',"l2534CanSaleNum":',l2534_canSaleNum,
    ',"l2534CanSaleMaxNum":',l2534_canSaleMaxNum,
    ',"l2534CanSalePercent":',l2534_canSalePercent,
    ',"l24PlusCanSaleNum":',l24plus_canSaleNum,
    ',"l24PlusCanSaleMaxNum":',l24plus_canSaleMaxNum,
    ',"l24PlusCanSalePercent":',l24plus_canSalePercent,
    ',"peekScore":','2',
    '}') as config,
concat
(
area_type,'_',
Channel,'_',
area_id,'_',
dynamic_id,'_',
scene,'_',
check_type,'_',
week_type,'_',
filter_type,'_',
Buckets,'_',
stay_days_range
) as uniq_str,
"1" as status,
substring(current_timestamp,12,2) as h,
current_date as dt
from
(
    select area_type
    ,channel as Channel
    ,area_id
    ,area_name
    ,dynamic_id
    ,dynamic_name
    ,buckets as Buckets
    ,check_type
    ,week_type
    ,filter_type
    ,stay_days_range
    ,l4_canSaleNum
    ,l4_canSaleMaxNum
    ,l4_canSalePercent
    ,l34_canSaleNum
    ,l34_canSaleMaxNum
    ,l34_canSalePercent
    ,l2534_canSaleNum
    ,l2534_canSaleMaxNum
    ,l2534_canSalePercent
    ,l24plus_canSaleNum
    ,l24plus_canSaleMaxNum
    ,l24plus_canSalePercent
    ,scene
    from pdb_analysis_c.ads_flow_l34_overflow_d
    where dt=date_sub(current_date,1)
) as t1
where dynamic_id is not null
and area_id is not null
and area_type is not null