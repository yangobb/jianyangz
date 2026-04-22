
create table  tujia_tmp.list_rank_jianyang_0611_v1 as 
with new_old_info as (
SELECT  
    case when channel = 'ctrip' then '携程'
            when channel = 'qunar' then '去哪儿'
            when channel = 'tujia' then '途家'
            when channel = 'elong' then '艺龙'
            end as channel
    ,dt
    ,uid
    ,user_id
    ,is_new
FROM pdb_analysis_c.app_visit_user_d
WHERE dt in ('2025-04-17','2025-05-15')
GROUP BY 1,2,3,4,5
)
,ab_test as (
select a.*
from (
    select distinct 
        dt
        ,uid
        ,wrapper_name
        ,case when wrapper_name='携程' then get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') else '其他' end as bucket
    from dws.dws_path_ldbo_d
    where dt in ('2025-04-17','2025-05-15')
    and user_type = '用户'
    and source = '102'
    and wrapper_name='携程'
    and get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') is not null
    and uid!='visitor000000'
) a 
join (
    select
        dt
        ,uid
        ,wrapper_name
        ,count(distinct case when wrapper_name='携程' then get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') end) bucket
    from dws.dws_path_ldbo_d 
    where dt in ('2025-04-17','2025-05-15')
    and user_type = '用户'
    and source = '102'
    and wrapper_name='携程'
    and get_json_object(ab_test,'$.wapctripbnb_c_api_cj_pingbi.v') is not null
    group by 1,2,3
    having bucket=1 
) b
on a.dt=b.dt and lower(a.uid)=lower(b.uid) and a.wrapper_name=b.wrapper_name
)
,house_info as (
select t1.*
    ,case when is_prefer=1 then '优选' else '非优选' end as is_you
    ,case when is_prefer_pro=1 then '严臻' when great_tag=1 then '严臻' else '其他' end as is_yanzhen
    ,case when is_prefer=1 or is_prefer_pro=1 or great_tag=1 then 1 else 0 end is_sanxuan
from
    (
        select distinct
        t1.dt,
        t1.hotel_id,
        t1.house_id,
        house_city_id AS city_id,
        is_prefer_pro,
        great_tag,
        is_prefer,
        case when bedroom_count = 1 then 1 else 0 end is_1ju,
        case when landlord_channel = 303 then '携程接入'
            when landlord_channel = 1 then '直采'
        else '其他' end as source_type,
        case when house_type = '标准酒店' then 1 else 0 end is_standard,
        CASE 
            WHEN house_type <> '标准酒店' AND (is_gold_medal = 1 OR is_special_brand_homestay = 1) 
            THEN '金特牌'
            ELSE '非金特牌' 
        END AS is_gold
        from dws.dws_house_d t1
        --where t1.dt between date_sub(current_date,7) and date_sub(current_date,1)
        where dt in ('2025-04-17','2025-05-15')
        and house_is_oversea = '0'
        and house_is_online = 1
    ) as t1
)
,list AS (
SELECT a.*,
    b.is_standard,
    b.source_type,
    b.is_1ju,
    b.is_you,
    b.is_yanzhen,
    b.is_sanxuan, 
    CASE 
        WHEN t2.bucket = 'B' THEN '对照B'
        WHEN t2.bucket = 'C' THEN '对照C'
        WHEN t2.bucket in ('H','I') THEN '纯模型'
        else '实验桶'
    END AS bucket,
    CASE 
        WHEN if_click = 'true' THEN '有点击' 
        WHEN if_click = 'false' THEN '无点击' 
    END as if_click_type
    -- ,cansale_num_l21plus_zc
    -- ,c_commission_rate
    -- ,wrapper_name
    ,case when position_new <= 3 then 1 else 0 end is_top3
    ,nvl(is_new,'其他') is_new
FROM (
    SELECT  
        case
            when fromforlog in ('0','2') then '宫格' 
            when fromforlog in ('300','310') then 'tab'
            when (fromforlog in(60,64) or fromforlog like '6000') then '全站搜索'
            else '其他' end fromforlog,
        case 
            when get_json_object(server_log,'$.searchScene') = 2 then '空搜'
            when get_json_object(server_log,'$.searchScene') = 5 then '地标'
            when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
            when get_json_object(server_log,'$.searchScene') = 6 then '定位'
            else '其他'
            -- when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
            -- when get_json_object(server_log,'$.searchScene') = 3 then '景区'
            -- when get_json_object(server_log,'$.searchScene') = 8 then '县级市空搜'
            -- when get_json_object(server_log,'$.searchScene') = 0 then '无' 
            end as search_type,
        dt,
        wrapper_name,
        house_id,
        rank_trace_id,
        search_id,
        city_id,
        uid,
        user_id,
        detail_uid,
        final_price,
        position_new,
        without_risk_order_num,
        without_risk_order_room_night,
        without_risk_order_gmv,
        without_risk_order_gmv / without_risk_order_room_night adr,
        without_risk_access_order_gmv,
        without_risk_access_order_num,
        without_risk_access_order_room_night,
        nvl(get_json_object(server_log, '$.hasUserClickBehavior'),'其他') AS if_click
    FROM dws.dws_path_ldbo_d
    WHERE dt in ('2025-04-17','2025-05-15')
    and is_oversea = 0
    AND wrapper_name = '携程'
    AND source = '102'
    AND user_type = '用户'
) AS a 
JOIN house_info AS b 
ON a.dt = b.dt AND a.house_id = b.house_id
LEFT JOIN ab_test AS t2 
ON a.dt = t2.dt AND lower(a.uid)=lower(t2.uid) AND a.wrapper_name = t2.wrapper_name
-- left join (
--     select distinct dt
--     ,search_id
--     ,uid
--     ,rank_trace_id
--     ,city_id
--     ,GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(extend_field, '$.rankTagIdCanSaleInfoMap'), '$.102'), '$.canSaleNum') as cansale_num_l21plus_zc
--     FROM pdb_analysis_c.ads_flow_list_price_day_d  t1
--     WHERE dt >= '2025-04-30'
-- ) as t3
-- on a.dt=t3.dt
-- and a.search_id=t3.search_id
-- and lower(a.uid)=lower(t3.uid)
-- and a.rank_trace_id=t3.rank_trace_id
-- and a.city_id=t3.city_id
-- left JOIN (
--     select
--     house_id,
--     dt,
--     get_json_object(feature_obj, '$.c_commission_rate') as c_commission_rate
-- from
--     dw_algorithm.dws_house_commission_rate_d
--     where dt=date_sub(current_date,2)
-- )t4
-- ON a.house_id = t4.house_id
left join new_old_info t5
on a.dt = t5.dt 
and lower(a.uid)=lower(t5.uid)
)
select * from list 