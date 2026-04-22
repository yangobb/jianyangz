
select '今年' type1
    ,is_standard
    ,cityname area_name
    ,count(uid) lpv
    ,count(distinct concat(uid,dt)) luv
    ,count(distinct case when is_has_click = 1 then concat(uid,dt) end) duv
    ,count(distinct a.masterhotelid) hotel_cnt
from (
    select d dt 
        ,cid uid
        ,masterhotelid
        ,detail_dingclick_roomlist
        ,fh_price
        ,is_has_click
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
    where d between date_sub(current_date,14) and date_sub(current_date,1)
    and fh_price > 0  
) a 
inner join (
    select 
        masterhotelid 
        ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end cityname
        ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        ,case when goldstar_ori in ('6','5') then '金特牌' else '其他' end is_gold
    from app_ctrip.dimmasterhotel
    where d = date_sub(current_date,2)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门')) 
    and masterhotelid > 0  
    and is_standard in (0,1)
) b 
on a.masterhotelid = b.masterhotelid 
group by 1,2,3 

union all 

select '去年' type1
    ,is_standard
    ,cityname area_name
    ,count(uid) lpv
    ,count(distinct concat(uid,dt)) luv
    ,count(distinct case when is_has_click = 1 then concat(uid,dt) end) duv
    ,count(distinct a.masterhotelid) hotel_cnt
from (
    select d dt 
        ,cid uid
        ,masterhotelid
        ,detail_dingclick_roomlist
        ,fh_price
        ,is_has_click
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
    where d between date_sub(add_months(current_date,-12),14) and date_sub(add_months(current_date,-12),1)
    and fh_price > 0  
) a 
inner join (
    select 
        masterhotelid 
        ,case when cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市') then cityname else '其他' end cityname
        ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        ,case when goldstar_ori in ('6','5') then '金特牌' else '其他' end is_gold
    from app_ctrip.dimmasterhotel
    where d = date_sub(current_date,2)                                         
    and (countryname != '中国'   or cityname in ('香港','澳门'))
    and masterhotelid > 0
    and is_standard in (0,1)
) b 
on a.masterhotelid = b.masterhotelid 
group by 1,2,3 