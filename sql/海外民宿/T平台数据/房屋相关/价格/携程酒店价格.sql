-- 曝光价
SELECT -- country,
    case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') then cityname else '其他' end city_name
    
    ,avg(fh_price) fh_price
    ,percentile(fh_price,0.3) fh_price3 
    ,percentile(fh_price,0.5) fh_price5
    ,percentile(fh_price,0.7) fh_price7  
from (
    select m_city
        ,masterhotelid
        ,fh_price
        ,case when d between date_sub(current_date,7) and date_sub(current_date,1) then 'W1'
                when d between date_sub(current_date,14) and date_sub(current_date,8) then 'W2'
                when d between date_sub(current_date,21) and date_sub(current_date,15) then 'W3'
                when d between date_sub(current_date,28) and date_sub(current_date,22) then 'W4'
                when d between date_sub(current_date,35) and date_sub(current_date,29) then 'W5'
                when d between date_sub(current_date,42) and date_sub(current_date,36) then 'W6'
                when d between date_sub(current_date,49) and date_sub(current_date,43) then 'W7'
                when d between date_sub(current_date,56) and date_sub(current_date,50) then 'W8'
            end week 
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day 
    where d between '2025-01-01' and '2025-05-20'
) t1 
join (
    select                                          
        masterhotelid
        ,cityname                                                                   
    from app_ctrip.dimmasterhotel                                        
    where d = date_sub(current_Date(),1)              
    and cityname = '大阪'
    and masterhotelid > 0 -- 母酒店ID有值 
    and is_standard = 0
    group by 1,2
) t2
on t1.masterhotelid = t2.masterhotelid
group by 1,2


group by 1 
    
-- adr
SELECT  case when TO_DATE(orderdate) between date_sub(current_date,7) and date_sub(current_date,1) then 'W1'
                when TO_DATE(orderdate) between date_sub(current_date,14) and date_sub(current_date,8) then 'W2'
                when TO_DATE(orderdate) between date_sub(current_date,21) and date_sub(current_date,15) then 'W3'
                when TO_DATE(orderdate) between date_sub(current_date,28) and date_sub(current_date,22) then 'W4'
                when TO_DATE(orderdate) between date_sub(current_date,35) and date_sub(current_date,29) then 'W5'
                when TO_DATE(orderdate) between date_sub(current_date,42) and date_sub(current_date,36) then 'W6'
                when TO_DATE(orderdate) between date_sub(current_date,49) and date_sub(current_date,43) then 'W7'
                when TO_DATE(orderdate) between date_sub(current_date,56) and date_sub(current_date,50) then 'W8'
            end week 
        ,case when cityname in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') then cityname else '其他' end city_name
        ,count(distinct orderid) order_cnt 
        ,sum(ciireceivable) as gmv
        ,sum(ciiquantity) as night
        ,sum(ciireceivable) / sum(ciiquantity) adr 
FROM    app_ctrip.edw_htl_order_all_split
WHERE   submitfrom = 'client'
AND     to_date(orderdate) between date_sub(current_date,56) and date_sub(current_date,1)
AND     orderstatus IN ('P','S')
AND     (country <> 1 or cityname in ('香港','澳门'))--海外
AND     ordertype = 2 -- 酒店订单
and d = current_Date()
group by 1,2 
