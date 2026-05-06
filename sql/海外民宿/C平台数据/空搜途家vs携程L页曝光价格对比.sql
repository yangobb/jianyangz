-- 空搜场景下：途家海外 L 页曝光价格 vs 携程 L 页曝光价格 by 城市
-- 途家空搜口径：dws_path_ldbo_d.empty_filter = 1（与生态监控对齐）
-- 携程空搜近似口径：cdm_traf_ht_ctrip_list_qid_day 中 m_zone 为空（无商圈筛选）
-- 携程 L 页无显式空搜字段，故同时输出"携程全量列表"作为基线对照
-- 时间窗：近 14 天（截至昨天）；可调整为指定日期

with tj_oversea as (
    select house_city_name as city_name
        ,final_price
        ,uid
        ,detail_uid
    from dws.dws_path_ldbo_d
    where dt between date_sub(current_date,14) and date_sub(current_date,1)
      and is_oversea = 1
      and wrapper_name in ('携程','途家','去哪儿')
      and user_type = '用户'
      and empty_filter = 1
      and final_price > 0
)
,tj_agg as (
    select '途家_空搜' as source
        ,city_name
        ,count(uid) as lpv
        ,count(distinct uid) as luv
        ,count(detail_uid) as dpv
        ,count(distinct detail_uid) as duv
        ,avg(final_price) as avg_exp_price
        ,percentile(cast(final_price as bigint),0.25) as p25_exp_price
        ,percentile(cast(final_price as bigint),0.50) as p50_exp_price
        ,percentile(cast(final_price as bigint),0.75) as p75_exp_price
        ,percentile(cast(final_price as bigint),0.95) as p95_exp_price
    from tj_oversea
    group by 1,2
)
,ct_base as (
    select t1.cid
        ,t1.d
        ,t1.fh_price
        ,t1.is_has_click
        ,t1.m_zone
        ,t2.cityname as city_name
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day t1
    join excel_upload.dim_ctrip_list_qid_city t2
      on t1.m_city = t2.m_city
    where t1.d between date_sub(current_date,14) and date_sub(current_date,1)
      and t1.fh_price > 0
)
,ct_empty as (
    select '携程L页_空搜近似' as source
        ,city_name
        ,count(1) as lpv
        ,count(distinct concat_ws('|',d,cid)) as luv
        ,count(case when is_has_click = 1 then 1 end) as dpv
        ,count(distinct case when is_has_click = 1 then concat_ws('|',d,cid) end) as duv
        ,avg(fh_price) as avg_exp_price
        ,percentile(cast(fh_price as bigint),0.25) as p25_exp_price
        ,percentile(cast(fh_price as bigint),0.50) as p50_exp_price
        ,percentile(cast(fh_price as bigint),0.75) as p75_exp_price
        ,percentile(cast(fh_price as bigint),0.95) as p95_exp_price
    from ct_base
    where m_zone is null or m_zone = ''
    group by 1,2
)
,ct_all as (
    select '携程L页_全量基线' as source
        ,city_name
        ,count(1) as lpv
        ,count(distinct concat_ws('|',d,cid)) as luv
        ,count(case when is_has_click = 1 then 1 end) as dpv
        ,count(distinct case when is_has_click = 1 then concat_ws('|',d,cid) end) as duv
        ,avg(fh_price) as avg_exp_price
        ,percentile(cast(fh_price as bigint),0.25) as p25_exp_price
        ,percentile(cast(fh_price as bigint),0.50) as p50_exp_price
        ,percentile(cast(fh_price as bigint),0.75) as p75_exp_price
        ,percentile(cast(fh_price as bigint),0.95) as p95_exp_price
    from ct_base
    group by 1,2
)
select source
    ,city_name
    ,lpv
    ,luv
    ,dpv
    ,duv
    ,round(avg_exp_price,2) as avg_exp_price
    ,p25_exp_price
    ,p50_exp_price
    ,p75_exp_price
    ,p95_exp_price
from tj_agg
where city_name in ('东京','大阪','京都','吉隆坡','首尔','曼谷','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
union all
select source
    ,city_name
    ,lpv
    ,luv
    ,dpv
    ,duv
    ,round(avg_exp_price,2) as avg_exp_price
    ,p25_exp_price
    ,p50_exp_price
    ,p75_exp_price
    ,p95_exp_price
from ct_empty
where city_name in ('东京','大阪','京都','吉隆坡','首尔','曼谷','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
union all
select source
    ,city_name
    ,lpv
    ,luv
    ,dpv
    ,duv
    ,round(avg_exp_price,2) as avg_exp_price
    ,p25_exp_price
    ,p50_exp_price
    ,p75_exp_price
    ,p95_exp_price
from ct_all
where city_name in ('东京','大阪','京都','吉隆坡','首尔','曼谷','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
order by city_name, source
;
