-- 20260305 迭代
-- ①各个sheet中早于2025年1月1日的数据不再进行展示
-- ②海外天均渗透中（离店），不进行国家维度的计算
with city_detail as (
select
    country_id
    ,country_name
    ,city_id
    ,city_name 
    ,max(city_type) city_type
from (
    -- SABC 等级城市 
    select 
        'SAB城市' city_type
        ,a.country_id
        ,a.country_name
        ,a.city_id
        ,a.city_name 
    from excel_upload.oversea_city_level b
    join (
        select 
            country country_id 
            ,case when countryname = '中国' then '中国大陆' else countryname end country_name
            ,city city_id 
            ,case when cityname = '亚庇' then '哥打京那巴鲁' else cityname end city_name
        from app_ctrip.dimmasterhotel   --C酒店基础信息表
        where d = date_sub(current_date(),1)
        and masterhotelid > 0 -- 母酒店ID有值 
        group by 1,2,3,4 
    ) a 
    on a.city_name = b.city_name
    and a.country_name = b.country_name
    where b.city_level != 'C'
    union all 
    -- 携程Top10
    select 
        '携程Top10' city_type
        ,country_id
        ,country_name
        ,city_id
        ,city_name 
    from (
        select 
            country_id
            ,country_name
            ,city_id
            ,city_name 
            ,row_number() over(order by night desc) rn 
        from (
            select country_id
                ,country_name
                ,city_id
                ,city_name
                ,sum(night) night
            from (
                select masterhotelid
                    ,ciiquantity night 
                from app_ctrip.edw_htl_order_all_split
                where d =  current_date()
                and to_date(departure) between date_sub(current_date,14) and date_sub(current_date,1)
                and submitfrom='client'  --携程app酒店
                and orderstatus in ('P','S') -- 离店口径
                and ordertype = 2 -- 酒店订单
                and (country != 1 or cityname in ('香港','澳门'))
            ) a
            inner join (
                select
                    country country_id 
                    ,case when countryname = '中国' then '中国大陆' else countryname end country_name
                    ,city city_id 
                    ,case when cityname = '亚庇' then '哥打京那巴鲁' else cityname end city_name
                    ,masterhotelid
                from app_ctrip.dimmasterhotel   --C酒店基础信息表
                where d = date_sub(current_date(),1)
                and masterhotelid > 0 -- 母酒店ID有值 
            ) b 
            on a.masterhotelid = b.masterhotelid
            group by 1,2,3,4  
        ) a 
    ) a 
    where rn <= 10 
) a 
group by 1,2,3,4 
)
,jd as (
SELECT  t1.checkout_date                                                   as dt
    ,nvl(country_name,'其他')                                              as country_name
    ,nvl(city_name,'其他')                                                 as city_name
    ,nvl(city_id,'其他')                                                   as city_id
    ,count(distinct t1.orderid)                                            as jd_ods
    ,sum(t1.night)                                                         as jd_night
    ,sum(t1.gmv)                                                           as jd_gmv 
    ,count(distinct case when t11.ord_id is not null then t1.orderid end)  as jd7_ods
    ,sum(case when t11.ord_id is not null then t1.night end)               as jd7_night
    ,sum(case when t11.ord_id is not null then t1.gmv end)                 as jd7_gmv 
FROM (
    select masterhotelid
        ,orderdate
        ,TO_DATE(departure) checkout_date
        ,orderid
        ,sum(ciireceivable) gmv
        ,sum(ciiquantity) night
    from app_ctrip.edw_htl_order_all_split
    WHERE d = date_sub(current_date,1)
    AND TO_DATE(departure) between '2025-01-01' and date_sub("${date}",1)
    AND orderstatus IN ('P','S')
    AND (country <> 1 or cityname in ('香港','澳门'))--海外
    AND ordertype = 2 -- 酒店订单
    -- and submitfrom = 'client'
    group by 1,2,3,4
) t1
left join (
    select ord_id
    from app_ctrip.v_edw_inpr_aa_ovs_ord_d
    where d = date_sub(current_date,1)
    and checkout_date between '2025-01-01' and date_sub("${date}",1)
    and ord_status in ('P','S')
    and is_tcom = 0
    and chl != 'API分销'
) t11
on t1.orderid = t11.ord_id
LEFT JOIN (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select masterhotelid 
        ,case when is_standard = 0 then 0 else 1 end is_standard 
        ,country country_id 
        ,case when countryname = '中国' then '中国大陆' else countryname end country_name
        ,city city_id 
        ,case when cityname = '亚庇' then '哥打京那巴鲁' else cityname end city_name
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub("${date}",2)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    -- and is_standard = 0  --是否标准酒店 1：是、0：否 
    group by 1,2,3,4,5,6
) t2
ON t1.masterhotelid = t2.masterhotelid 
group by 1,2,3,4
),
m as (
select  
    checkout_date dt, 
    country_name,
    city_name,
    city_id,
    count(distinct order_no) ms_ods,
    sum(order_room_night_count) ms_night,
    sum(room_total_amount) ms_gmv
from dws.dws_order
where (checkout_date between '2025-01-01' and  date_sub("${date}",1))
and is_done = 1 -- 离店
and is_cancel_order = 0
and is_overseas = 1 -- 海外
group by 1,2,3,4
)


,jdm as (
select * 
from (
select 
    coalesce(jd.dt,m.dt)                                   as `日期`
    ,'海外汇总'                                            as `国家`
    ,'海外汇总'                                            as `城市`
    ,'海外汇总'                                            as `城市id`
    ,cast(sum(jd_ods) as int)                              as `酒店离店单`
    ,cast(sum(jd_night) as int)                            as `酒店离店间夜`
    ,cast(sum(jd_gmv) as int)                              as `酒店离店gmv`
    ,cast(sum(jd_gmv)/sum(jd_night) as int)                as `酒店离店adr`
    ,cast(sum(jd7_ods) as int)                             as `酒店七大类离店单`
    ,cast(sum(jd7_night) as int)                           as `酒店七大类离店间夜`
    ,cast(sum(jd7_gmv) as int)                             as `酒店七大类离店gmv`
    ,cast(sum(jd7_gmv)/sum(jd7_night) as int)              as `酒店七大类离店adr`
    ,cast(sum(ms_ods) as int)                              as `民宿离店单`
    ,cast(sum(ms_night) as int)                            as `民宿离店间夜`
    ,cast(sum(ms_gmv) as int)                              as `民宿离店gmv`
    ,cast(sum(ms_gmv)/sum(ms_night) as int)                as `民宿离店adr`
    ,concat(round(sum(ms_night)*100/sum(jd7_night),1),'%') as `离店间夜非标占比`
    ,concat(round(sum(ms_gmv)*100/sum(jd7_gmv),1),'%')     as `离店GMV非标占比`
    ,concat(round(sum(ms_night)*100/sum(jd_night),1),'%')  as `酒店离店间夜渗透率`
    ,concat(round(sum(ms_gmv)*100/sum(jd_gmv),1),'%')      as `酒店离店GMV渗透率`
    ,-1                                                    as `酒店离店间夜排名`
from jd 
full join m 
on jd.dt = m.dt 
and jd.city_name = m.city_name
group by coalesce(jd.dt,m.dt)

-- 城市
union all
select 
    coalesce(jd.dt,m.dt)                         as `日期`
    ,coalesce(jd.country_name,m.country_name)    as `国家`
    ,coalesce(jd.city_name,m.city_name)          as `城市`
    ,coalesce(jd.city_id,m.city_id)              as `城市id`
    ,cast(jd_ods as int)                         as `酒店离店单`
    ,cast(jd_night as int)                       as `酒店离店间夜`
    ,cast(jd_gmv as int)                         as `酒店离店gmv`
    ,cast(jd_gmv/jd_night as int)                as `酒店离店adr`
    ,cast(jd7_ods as int)                        as `酒店七大类离店单`
    ,cast(jd7_night as int)                      as `酒店七大类离店间夜`
    ,cast(jd7_gmv as int)                        as `酒店七大类离店gmv`
    ,cast(jd7_gmv/jd7_night as int)              as `酒店七大类离店adr`
    ,cast(ms_ods as int)                         as `民宿离店单`
    ,cast(ms_night as int)                       as `民宿离店间夜`
    ,cast(ms_gmv as int)                         as `民宿离店gmv`
    ,cast(ms_gmv/ms_night as int)                as `民宿离店adr`
    ,concat(round(ms_night*100/jd7_night,1),'%') as `七大类离店间夜非标占比`
    ,concat(round(ms_gmv*100/jd7_gmv,1),'%')     as `七大类离店GMV非标占比`
    ,concat(round(ms_night*100/jd_night,1),'%')  as `酒店离店间夜渗透率`
    ,concat(round(ms_gmv*100/jd_gmv,1),'%')      as `酒店离店GMV渗透率`
    ,row_number() over(partition by coalesce(jd.dt,m.dt) order by jd_night desc)     as `酒店离店间夜排名`
from jd 
full join m 
on jd.dt = m.dt 
and jd.city_name = m.city_name  
join city_detail m2
on coalesce(jd.city_name,m.city_name) = m2.city_name
) t  
order by `日期` desc,`酒店离店间夜` desc
)
 
select h1.*
    ,date_sub("${date}",1) as dt 
from (
    select 
        `日期`
        ,`城市`
        ,`城市id`
        ,cast(`酒店离店单` as bigint) as `酒店离店单`
        ,cast(`酒店离店间夜` as bigint) as `酒店离店间夜`
        ,cast(`酒店离店gmv` as bigint) as `酒店离店gmv`
        ,cast(`酒店离店gmv`/`酒店离店间夜` as bigint)                as `酒店离店adr`
        ,cast(`酒店七大类离店单` as bigint) as `酒店七大类离店单`
        ,cast(`酒店七大类离店间夜` as bigint) as `酒店七大类离店间夜`
        ,cast(`酒店七大类离店gmv` as bigint) as `酒店七大类离店gmv`
        ,cast(`酒店七大类离店gmv`/`酒店七大类离店间夜` as bigint)     as `酒店七大类离店adr`
        ,cast(`民宿离店单` as bigint) as `民宿离店单`
        ,cast(`民宿离店间夜` as bigint) as `民宿离店间夜`
        ,cast(`民宿离店gmv` as bigint) as `民宿离店gmv`
        ,cast(`民宿离店gmv`/`民宿离店间夜` as bigint)                as `民宿离店adr`
        ,concat(round(`民宿离店间夜`*100/`酒店七大类离店间夜`,1),'%') as `离店间夜非标占比`
        ,concat(round(`民宿离店gmv`*100/`酒店七大类离店gmv`,1),'%')   as `离店GMV非标占比`
        ,concat(round(`民宿离店间夜`*100/`酒店离店间夜`,1),'%') as `离店间夜酒店占比`
        ,concat(round(`民宿离店gmv`*100/`酒店离店gmv`,1),'%')   as `离店GMV酒店占比`
        ,null as hotel_uv
        ,null as hotel7_uv
        ,null as ms_uv
        ,row_number() over(partition by `日期` order by `酒店离店间夜` desc) -2 as `酒店离店间夜排名`
    from (
        select 
            `日期`
            ,`城市`
            ,`城市id`
            ,`酒店离店单`
            ,`酒店离店间夜`
            ,`酒店离店gmv`
            ,`酒店七大类离店单`
            ,`酒店七大类离店间夜`
            ,`酒店七大类离店gmv`
            ,`民宿离店单`
            ,`民宿离店间夜`
            ,`民宿离店gmv`
        from jdm 
    ) h
) h1
where `酒店离店间夜排名`<=50
order by `日期` desc,`酒店离店间夜排名`