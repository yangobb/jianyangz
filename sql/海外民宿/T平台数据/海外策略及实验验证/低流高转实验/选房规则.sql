with qidalei as (
select cityname 
    ,percentile(lpv,0.5) lpv_5
    ,percentile(luv,0.5) luv_5
    ,percentile(fh_price,0.5) price_5
    ,percentile(l2o,0.5) l2o_5
from (
    select a.masterhotelid
        ,cityname 
        ,fh_price
        ,lpv 
        ,luv
        ,dpv
        ,duv
        ,order_cnt
        ,gmv
        ,night 
        ,night / luv l2o 
        ,roomquantity
    from (
        select 
            a.masterhotelid 
            ,cityname 
            ,roomquantity
            ,percentile(fh_price,0.5) fh_price
            ,count(1) lpv
            ,count(distinct d,cid) luv
            ,count(case when is_has_click = 1 then concat(d,cid) end) dpv
            ,count(distinct case when is_has_click = 1 then concat(d,cid) end) duv
        from (
            select *
            from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
            where d between date_sub(current_date, 30) AND date_sub(current_date, 1)
            and fh_price > 0 
        ) a 
        inner join (
            select masterhotelid
                ,is_standard
                ,zonename	
                ,roomquantity
                ,cityname
            from app_ctrip.dimmasterhotel
            where d = date_sub(current_date,2)
            and (countryname != '中国'   or cityname in ('香港','澳门')) 
            and masterhotelid > 0
            and is_standard = 0 
            and cityname in ('吉隆坡','首尔','曼谷','巴厘岛','香港')
        ) b 
        on a.masterhotelid = b.masterhotelid
        group by 1,2,3 
    ) a 
    join (
        SELECT masterhotelid
            ,count(distinct orderid) order_cnt
            ,sum(ciireceivable) gmv
            ,sum(ciiquantity) night 
        FROM app_ctrip.edw_htl_order_all_split
        WHERE submitfrom = 'client'
        AND TO_DATE(departure) between date_sub(current_date, 30) AND date_sub(current_date, 1)
        AND orderstatus IN ('P','S')
        AND (country <> 1 or cityname in ('香港','澳门'))--海外
        AND ordertype = 2 -- 酒店订单
        and d = current_Date()
        and cityname in ('吉隆坡','首尔','曼谷','巴厘岛','香港')
        group by 1
    ) b
    on a.masterhotelid = b.masterhotelid
) a 
group by 1
)
,jiudian  as (
select cityname
    ,percentile(fh_price,0.5) price_jd
from (
    select 
        a.masterhotelid 
        ,cityname
        ,roomquantity
        ,percentile(fh_price,0.5) fh_price 
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(current_date, 30) AND date_sub(current_date, 1)
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
            ,is_standard
            ,zonename	
            ,roomquantity
            ,cityname
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门')) 
        and masterhotelid > 0
        and is_standard = 1 
        and star = 3 
        and cityname in ('吉隆坡','首尔','曼谷','巴厘岛','香港')
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1,2,3
) jd 
group by 1
)
,qidalei_detail as (
select a.masterhotelid
    ,cityname
    ,fh_price
    ,lpv 
    ,luv
    ,dpv
    ,duv
    ,order_cnt
    ,gmv
    ,night 
    ,night / luv l2o
    ,roomquantity
from (
    select 
        a.masterhotelid
        ,cityname
        ,roomquantity
        ,percentile(fh_price,0.5) fh_price
        ,count(1) lpv
        ,count(distinct d,cid) luv
        ,count(case when is_has_click = 1 then concat(d,cid) end) dpv
        ,count(distinct case when is_has_click = 1 then concat(d,cid) end) duv
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between date_sub(current_date, 30) AND date_sub(current_date, 1)
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
            ,is_standard
            ,zonename	
            ,roomquantity
            ,cityname
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门')) 
        and masterhotelid > 0
        and is_standard = 0 
        and cityname in ('吉隆坡','首尔','曼谷','巴厘岛','香港')
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1,2,3
) a 
join (
    SELECT masterhotelid
        ,count(distinct orderid) order_cnt
        ,sum(ciireceivable) gmv
        ,sum(ciiquantity) night 
    FROM app_ctrip.edw_htl_order_all_split
    WHERE submitfrom = 'client'
    AND TO_DATE(departure) between date_sub(current_date, 30) AND date_sub(current_date, 1)
    AND orderstatus IN ('P','S')
    AND (country <> 1 or cityname in ('香港','澳门'))--海外
    AND ordertype = 2 -- 酒店订单
    and d = current_Date()
    and cityname in ('吉隆坡','首尔','曼谷','巴厘岛','香港')
    group by 1
) b
on a.masterhotelid = b.masterhotelid
)
,final as (
select a.masterhotelid
    ,a.cityname
    ,a.fh_price
    ,a.lpv 
    ,a.luv
    ,a.dpv
    ,a.duv
    ,a.order_cnt
    ,a.gmv
    ,a.night 
    ,a.roomquantity
    ,l2o
    ,lpv_5
    ,l2o_5
    ,price_jd
from qidalei_detail a 
left join qidalei b 
on a.cityname = b.cityname
left join jiudian c 
on a.cityname = c.cityname
-- where a.lpv <= b.lpv_5
-- and a.l2o >= b.l2o_5
-- and a.night >= 20
-- and a.fh_price <= (c.price_jd * 0.8)
) 
select * from final 
-- select *
-- from (
--     select *
--         ,row_number() over(partition by cityname order by night desc) rn 
--     from final 
-- ) a 
-- where rn <= 30