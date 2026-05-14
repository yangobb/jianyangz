with query_info as (
select t1.query
    ,team
    ,case when team = '兼职' then 'tiktok'
        when team = '直营' then pro_channel 
        else '' end pro_channel
    ,channel
    ,is_overseas
    ,is_t1
    ,is_t1_flow
    ,count(distinct dt,uid) uv 
from (
    -- query生成
    select query
        ,'直营' team
        ,create_date create_time -- 帖子生成时间
        ,case when create_date = date_sub(current_date,1) then '1' end is_t1 -- 帖子昨天生成时间
        ,houseid house_id
    from pdb_analysis_c.ads_flow_query_info_d
    where dt = date_sub(current_date,1)
    and create_date >= '2026-03-01'
    and team_name = '海外业务'
    and pro_channel in ('1','5','7','8','9')
    union all
    select a.tiktok_channel_code query
        ,case when tiktok_channel_code in (717397199,251511709,711340326,642187557,715145880) then '直营' else '兼职' end team
        ,to_date(b.create_time) create_time
        ,case when to_date(b.create_time) = date_sub(current_date,1) then '1' end is_t1
        ,a.house_id
    from (
        select *
        from tujia_ods.ods_crm_tujia_text_assignment
        where assignment_type = '3' -- 海外
        and status = '80' -- 已发布
    ) a
    left join tujia_ods.ods_crm_channel_search_code b
    on a.tiktok_channel_code = b.channel_code
) t1
left join (
    -- query 引流渠道
    SELECT
        query 
        ,case when keyword_type = 1 then 'red'
            when keyword_type = 5 then 'tiktok'
            when keyword_type = 7 then 'tiktok'
            when keyword_type = 8 then 'ins'
            when keyword_type = 9 then 'Youtube' 
            end as pro_channel
        ,substr(create_time, 1, 10) AS create_date 
    FROM dwd.dwd_redbook_sug_keyword
    WHERE status = 1 --口令码在线
    group by 1,2,3 
) t11 
on t1.query = t11.query 
left join (
    -- 流量曝光表
    select query
        ,uid
        ,user_id
        ,house_id 
        ,case when channel = 'tujia2' then 'M站' else channel end channel
        ,case when dt = date_sub(current_date,1) then '1' end is_t1_flow
    from ads.ads_flow_tujia_redbook_shuangliu_recommend_d
    where dt >= '2026-03-01'
    and uid != 'visitor000000' 
    group by 1,2,3,4,5,6
) t12 
on t1.query = t12.query 
left join (
    select house_id
        ,hotel_is_oversea is_overseas
    from dws.dws_house_d
    where dt = date_sub(current_date,1)
) t13 
on t1.house_id = t13.house_id 
group by 1,2,3,4,5,6,7 
)
,query_od as (
select * 
    ,case when is_new = 'tujia2' then 'M站' else is_new end channel
from pdb_analysis_c.ads_order_tujia_redbook_shuangliu_recommend_all_d
)


select t.dt
    ,t.is_overseas
    ,t.team
    ,t.pro_channel
    ,t.channel is_new
    ,a.uv
    ,a.yd_uv
    ,a.yd_ord
    ,a.yd_jianye
    ,round(a.yd_gmv,0) yd_gmv
    ,a.ld_uv
    ,a.ld_ord
    ,a.ld_jianye
    ,round(a.ld_gmv,0) ld_gmv
    ,round(a.ld_yongjin,0) ld_yongjin
    ,a.channel
    ,a.query_cnt
from (
    select
        'T-1' dt 
        ,is_overseas
        ,team
        ,pro_channel
        ,nvl(channel,'other') channel
    from query_info
    group by 1,2,3,4,5
    union all 
    select '累计' dt 
        ,is_overseas
        ,team
        ,pro_channel
        ,nvl(channel,'other') channel
    from query_info
    group by 1,2,3,4,5
) t 
left join (
    -- T-1
    select * 
    from (
        select
            'T-1' dt
            ,t1.is_overseas
            ,t1.team
            ,t1.pro_channel
            ,nvl(t1.channel,'other') is_new
            ,count(distinct case when is_t1_flow = 1 then concat(t2.dt,t2.uid) end) as uv
            ,count(distinct case when is_t1_flow = 1 and t2.order_time is not null then concat(to_date(t2.order_time),t4.uid) end) as yd_uv
            ,sum(case when is_t1_flow = 1 then t4.order_num end) as yd_ord
            ,sum(case when is_t1_flow = 1 then t4.night end) as yd_jianye
            ,sum(case when is_t1_flow = 1 then t4.gmv end) as yd_gmv
            ,count(distinct case when is_t1_flow = 1 and t2.order_time is not null then concat(to_date(t2.order_time),t3.uid) end) as ld_uv
            ,sum(case when is_t1_flow = 1 then t3.order_num end) as ld_ord
            ,sum(case when is_t1_flow = 1 then t3.night end) as ld_jianye
            ,sum(case when is_t1_flow = 1 then t3.gmv end) as ld_gmv
            ,sum(case when is_t1_flow = 1 then t3.commission end) as ld_yongjin
            ,nvl(t1.channel,'other') channel
            ,count(distinct case when is_t1 = 1 then t1.query end) query_cnt
        from query_info t1
        left join query_od t2 
        on t1.query = t2.query
        and t1.channel = t2.channel
        left join (
            -- 离店订单业绩
            select  
                order_no,
                checkout_date dt,
                max(uid) uid, 
                COUNT(DISTINCT order_no) AS order_num,
                SUM(ld_gmv) AS gmv,
                SUM(order_room_night_count) AS night,
                SUM(fyh_fh_commission) AS commission
            FROM dws.dws_order_finance
            WHERE nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
            and is_risk_order = 0
            and checkout_date  >= '2026-03-01'
            and checkout_date <= date_sub(current_date,2)
            and is_done = 1
            and is_success_order = 1 
            group by 1,2
        ) t3 
        on t2.order_no = t3.order_no
        left join (
            -- 预定订单业绩
            select  
                order_no,
                create_date dt,
                max(uid) uid, 
                COUNT(DISTINCT order_no) AS order_num,
                SUM(real_pay_amount) AS gmv,
                SUM(order_room_night_count) AS night 
            FROM dws.dws_order_finance
            WHERE nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
            and create_date  >= '2026-03-01'
            and create_date <= date_sub(current_date,2)
            and is_risk_order = 0
            and is_success_order = 1
            group by 1,2
        ) t4
        on t2.order_no = t4.order_no 
        group by 1,2,3,4,5,16 
    ) a 
    union all 
    -- 累计情况
    select * 
    from (
        select  
            '累计' dt
            ,is_overseas
            ,t1.team
            ,t1.pro_channel
            ,nvl(t1.channel,'other') is_new
            ,count(distinct concat(t2.dt,t2.uid,t2.query)) as uv
            ,count(distinct case when t2.order_time is not null then concat(to_date(t2.order_time),t4.uid,t2.query) end) as yd_uv
            ,sum(t4.order_num) as yd_ord
            ,sum(t4.night) as yd_jianye
            ,sum(t4.gmv) as yd_gmv
            ,count(distinct case when t2.order_time is not null then concat(to_date(t2.order_time),t3.uid,t2.query) end) as ld_uv
            ,sum(t3.order_num) as ld_ord
            ,sum(t3.night) as ld_jianye
            ,sum(t3.gmv) as ld_gmv
            ,sum(t3.commission) as ld_yongjin
            ,nvl(t1.channel,'other') channel
            ,count(distinct t1.query) query_cnt
        from query_info t1
        left join query_od t2 
        on t1.query = t2.query
        and t1.channel = t2.channel 
        left join (
            -- 离店订单业绩
            select  
                order_no,
                checkout_date dt,
                max(uid) uid, 
                COUNT(DISTINCT order_no) AS order_num,
                SUM(ld_gmv) AS gmv,
                SUM(order_room_night_count) AS night,
                SUM(fyh_fh_commission) AS commission
            FROM dws.dws_order_finance
            WHERE nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
            and is_risk_order = 0
            and checkout_date  >= '2026-03-01'
            and is_done = 1
            and is_success_order = 1 
            group by 1,2
        ) t3 
        on t2.order_no = t3.order_no
        left join (
            -- 预定订单业绩
            select  
                order_no,
                create_date dt,
                max(uid) uid, 
                COUNT(DISTINCT order_no) AS order_num,
                SUM(real_pay_amount) AS gmv,
                SUM(order_room_night_count) AS night 
            FROM dws.dws_order_finance
            WHERE nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
            and create_date  >= '2026-03-01'
            and is_risk_order = 0
            and is_success_order = 1
            group by 1,2
        ) t4
        on t2.order_no = t4.order_no
        group by 1,2,3,4,5,16
    ) a 
) a 
on t.dt = a.dt
and t.is_overseas = a.is_overseas
and t.team = a.team
and t.pro_channel = a.pro_channel
and t.channel = a.channel