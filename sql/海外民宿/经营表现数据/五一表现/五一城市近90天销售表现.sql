


with od as (
select '今年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then 1 else 0 end is_13city
    ,city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) cancel_order_cnt
from dws.dws_order 
where checkout_date between '2025-02-06' and '2025-05-05'
and is_cancel_order	= 0
and is_done = 1 
and is_overseas = 1 
group by 1,2,3
union all
select '去年' time1 
    ,case when city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门','札幌','福冈','名古屋') then 1 else 0 end is_13city
    ,city_name
    ,count(distinct order_no) order_cnt 
    ,sum(room_total_amount) room_total_amount
    ,sum(order_room_night_count) order_room_night_count
    ,count(distinct case when is_cancel_order = 1 then order_no end) 
from dws.dws_order 
where checkout_date between '2024-02-06' and '2024-05-05'
and is_cancel_order	= 0
and is_done = 1 
and is_overseas = 1 
group by 1,2,3 
)
,jiudian as (
SELECT '去年' time1 
    ,t4.city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
from (
    select * 
    FROM app_ctrip.edw_htl_order_all_split
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between '2024-02-06' and '2024-05-05'
    AND orderstatus IN ('P','S')
    AND (country <> 1 or cityname in ('香港','澳门'))--海外
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
) t1
LEFT JOIN (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select masterhotelid
        ,city
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date,2)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 0  --是否标准酒店 1：是、0：否 
    group by 1,2 
) t2
ON t1.masterhotelid = t2.masterhotelid 
left join (
    select get_json_object(attrs, '$.ctripRegionId') as cid,id
    from (
        select *
            ,row_number() over(partition by name order by update_time desc) rn 
        from ods_tujia_pg.city 
        where status = 'on'
        and is_inland = 0 
    ) a 
    where rn = 1 
) t3
on t2.city = t3.cid
left join (
    select country
        ,city_id
        ,city_name
    from tujia_dim.dim_region 
    where is_active = 1
    group by 1,2,3 
) t4
on t3.id = t4.city_id
group by 1,2 
union all 
SELECT '今年' time1 
    ,t4.city_name
    ,count(distinct orderid) order_cnt
    ,sum(ciireceivable) gmv
    ,sum(ciiquantity) night 
from (
    select * 
    FROM app_ctrip.edw_htl_order_all_split
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between '2025-02-06' and '2025-05-05'
    AND orderstatus IN ('P','S')
    AND (country <> 1 or cityname in ('香港','澳门'))--海外
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
) t1
LEFT JOIN (
    -- 判断携程7大类(酒店公寓、客栈、民宿、青旅、特色住宿、别墅、农家乐) 
    select masterhotelid
        ,city
    from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
    where d = date_sub(current_date,2)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 0  --是否标准酒店 1：是、0：否 
    group by 1,2 
) t2
ON t1.masterhotelid = t2.masterhotelid 
left join (
    select get_json_object(attrs, '$.ctripRegionId') as cid,id
    from (
        select *
            ,row_number() over(partition by name order by update_time desc) rn 
        from ods_tujia_pg.city 
        where status = 'on'
        and is_inland = 0 
    ) a 
    where rn = 1 
) t3
on t2.city = t3.cid
left join (
    select country
        ,city_id
        ,city_name
    from tujia_dim.dim_region 
    where is_active = 1
    group by 1,2,3 
) t4
on t3.id = t4.city_id
group by 1,2 
)




select od.time1
    ,od.is_13city
    ,od.city_name
    
    ,od.order_cnt `民宿订单量`
    ,od.room_total_amount `民宿gmv`
    ,od.order_room_night_count `民宿间夜`
    ,od.cancel_order_cnt `民宿取消单`

    ,jiudian.order_cnt `酒店订单`
    ,jiudian.gmv `酒店gmv`
    ,jiudian.night `酒店间夜`
from od 
left join jiudian 
on od.time1 = jiudian.time1 
and od.city_name = jiudian.city_name 