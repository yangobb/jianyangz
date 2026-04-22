select 
    masterhotelid
    ,percentile(fh_price,0.5) fh_price
    ,count(1) lpv_c
    ,count(distinct d,cid) luv_c
    ,count(case when is_has_click = 1 then concat(d,cid) end) dpv_c 
    ,count(distinct case when is_has_click = 1 then concat(d,cid) end) duv_c 
from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
where d between '2025-04-30' and '2025-05-06'
and fh_price > 0 
group by 1 