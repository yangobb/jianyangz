with p1 AS
(
SELECT  experiment_key
        ,b AS id
FROM    dwd.dwd_experiment_config_d
LATERAL VIEW EXPLODE(SPLIT(REPLACE(REPLACE(page_data_ids,'[',''),']',''),',')) a AS b
WHERE   dt BETWEEN "2025-03-06" AND DATE_SUB(CURRENT_DATE,1)
AND     state = 3
AND     page_data_ids != '[]'
AND     product_line_id != 18
)
,keys AS
(
    SELECT  uid
            ,dt
            ,k.experiment_key AS `key`
            ,bucket_type
            ,CASE   WHEN wrapper_id = 'waptujia001' THEN 'tujia'
                    WHEN wrapper_id = 'waptujia016' THEN 'qunar'
                    WHEN wrapper_id = 'waptujia003' THEN 'ctrip'
                    WHEN wrapper_id = 'waptujia007' THEN 'yilong'
            END AS wrapper_id
    FROM    ads.ads_abtest_user_key_uid AS a
    LEFT JOIN   (
                    SELECT  c.experiment_key
                            ,p.log_name AS log_name
                    FROM    p1 AS c
                    LEFT JOIN ods_tujiaproductdata.page_data AS p
                    ON      c.id = p.id
                    WHERE   LENGTH(c.id) > 0
                ) AS k
    ON      lower(a.`key`) = lower(k.experiment_key)
    AND     a.logname = k.log_name
    WHERE   dt BETWEEN "2025-03-06" AND DATE_SUB(CURRENT_DATE,1)
    AND     k.log_name IS NOT NULL
    GROUP BY uid
             ,dt
             ,k.experiment_key
             ,bucket_type
             ,wrapper_id
),
ab as (
SELECT  uid
        ,CASE   WHEN bucket_type IN ("B","C") THEN "对照组"
                WHEN bucket_type IN ("D","E") THEN "实验组"
        END AS bucket_type
        --,wrapper_id
        --,key
        ,dt
FROM    keys
WHERE   `key` IN  ('waptujia016_ppzy','wapctripbnb_ppzx','waptujia001_ppzx')
GROUP BY 1
         ,2
         ,3
         --,4
         --,5
),
d as (
SELECT  dt
        ,CASE   WHEN appid = '481001' THEN 'ctrip'
                WHEN appid = '5174' THEN 'tujia'
                WHEN appid = '5186' THEN 'qunar'
        END AS channel
        ,uid
        ,cityid
        ,GET_JSON_OBJECT(attrs,'$.houseid') AS house_id
FROM    dwd.dwd_log_ubt_v2_d
WHERE   dt BETWEEN "2025-03-06" AND DATE_SUB(CURRENT_DATE(),1)
AND     appid IN ('481001','5174','5186')
AND     KEY = 'o_bnb_inn_detail_app' --key
AND     GET_JSON_OBJECT(attrs,'$.is_leave') = 1
AND     osname<>'Harmony'
AND     GET_JSON_OBJECT(attrs,'$.houseid') IN (
            SELECT  house_id
            FROM    excel_upload.ppzx
            GROUP BY 1
        )
GROUP BY 1
         ,2
         ,3
         ,4
         ,5
),
u as (
SELECT  u.channel
        ,u.uid
        ,u.house_id
        ,GET_JSON_OBJECT(videoInfo,'$.video_current_time') AS video_current_time
        ,GET_JSON_OBJECT(videoInfo,'$.video_duration') AS video_duration
        ,GET_JSON_OBJECT(videoInfo,'$.video_rate') AS video_rate
        ,duration
        ,u.dt 
FROM    (
            SELECT  dt
                    ,CASE   WHEN appid = '481001' THEN 'ctrip'
                            WHEN appid = '5174' THEN 'tujia'
                            WHEN appid = '5186' THEN 'qunar'
                    END AS channel
                    ,uid
                    ,cityid
                    ,GET_JSON_OBJECT(attrs,'$.houseid') AS house_id
                    ,GET_JSON_OBJECT(attrs,'$.pagetraceid') pagetraceid
                    ,REGEXP_REPLACE(GET_JSON_OBJECT(attrs,'$.videoInfo'),'\\[|\\]','') AS videoInfo
                    ,GET_JSON_OBJECT(attrs,'$.duration') duration
            FROM    dwd.dwd_log_ubt_v2_d
            WHERE   dt BETWEEN "2025-03-06" AND DATE_SUB(CURRENT_DATE(),1)
            AND     appid IN ('481001','5174','5186')
            AND     KEY = 'o_bnb_inn_detail_app' --key
            AND     GET_JSON_OBJECT(attrs,'$.is_leave') = 2
            AND     GET_JSON_OBJECT(attrs,'$.is_video') = 1
            AND     osname<>'Harmony'
            AND     GET_JSON_OBJECT(attrs,'$.houseid') IN (
                        SELECT  house_id
                        FROM    excel_upload.ppzx
                        GROUP BY 1
                    )
            GROUP BY 1
                     ,2
                     ,3
                     ,4
                     ,5
                     ,6
                     ,7
                     ,8
        ) u
),
o as (
SELECT  create_date AS dt
        ,uid
        ,user_id
        ,order_no
        ,house_id
        ,room_total_amount
        ,order_room_night_count
FROM    dws.dws_order
WHERE   create_date BETWEEN "2025-03-06"
AND     date_sub(CURRENT_DATE(),1)
AND     ( sell_channel_type IN (3,8,12) AND source = 'mobile' )
AND     is_paysuccess_order = 1
AND     is_risk_order = 0
AND     is_overseas = 0
), 
d_c as (
select dt
    ,uid
    ,get_json_object(attrs,'$.houseid') house_id 
    ,'D页相册区域模块点击uv' event_name
from dwd.dwd_log_ubt_d
where dt between "2025-03-06" and date_sub(CURRENT_DATE(),1)
and appid IN ('481001','5174','5186')
and key='c_bnb_inn_detail_operate_app'
and case when source in ('290','491') then 1 else 0 end = 1 
group by 1,2,3,4
),

d_l as (
select dt
    ,uid
    ,get_json_object(attrs,'$.houseid') house_id 
    ,'D页相册区域右滑uv' event_name 
from dwd.dwd_log_ubt_d
where dt between "2025-03-06" and date_sub(CURRENT_DATE(),1)
and appid IN ('481001','5174','5186')
and key='c_bnb_inn_detail_operate_app' 
and source = 107
and get_json_object(attrs, '$.isDragging') = 'true'
group by 1,2,3,4
),



t1 as (
SELECT  d.dt
        ,d.channel
        ,ab.bucket_type
        ,COUNT(DISTINCT d.uid) AS duv
        ,COUNT(DISTINCT o.order_no) AS ord
FROM    ab
JOIN    d
ON      ab.dt = d.dt
AND     ab.uid = d.uid

LEFT JOIN o
ON      d.dt = o.dt
AND     lower(d.uid) = lower(o.uid)
AND     d.house_id = o.house_id
GROUP BY 1
         ,2
         ,3 
),
t2 as (
SELECT  u.dt
        ,u.channel
        ,ab.bucket_type 
        ,COUNT(DISTINCT u.uid) AS `页面曝光uv`
        ,COUNT(u.uid) AS `页面曝光pv`
        ,count(distinct d_l.uid) `D页相册区域右滑uv`
        ,count(distinct d_c.uid) `D页相册区域模块点击uv`
        ,count(distinct nvl(d_c.uid,d_l.uid)) `D页相册区域右滑orD页相册区域模块点击uv`
        ,COUNT(CASE    WHEN video_current_time > 0 THEN u.uid END) AS `视频播放pv`
        ,COUNT(DISTINCT CASE    WHEN video_current_time > 0 THEN u.uid END) AS `播放视频uv`
        ,SUM(duration) / 1000 AS `总停留时长`
        ,SUM(video_current_time) AS `总播放时长`
FROM    ab
JOIN    u
ON      ab.dt = u.dt
AND     ab.uid = u.uid

left join d_c 
ON      u.dt = d_c.dt
AND     u.uid = d_c.uid
AND     u.house_id = d_c.house_id

left join d_l
ON      u.dt = d_l.dt
AND     u.uid = d_l.uid
AND     u.house_id = d_l.house_id
GROUP BY 1
         ,2
         ,3 
)





        
 
SELECT  t1.dt
        ,t1. channel
        ,t1.bucket_type
        ,"all" as house_id
        ,duv
        ,`D页相册区域右滑uv`
        ,`D页相册区域模块点击uv`
        ,`D页相册区域右滑orD页相册区域模块点击uv`
        
        ,ord
        ,`页面曝光uv`
        ,`页面曝光pv`
        ,`视频播放pv`
        ,`播放视频uv`
        ,`总播放时长`
        ,round(`总停留时长` / `页面曝光pv`,0) AS `D页人均停留时长`
FROM    t1
LEFT JOIN t2
ON      t1.dt = t2.dt
AND     t1.channel = t2.channel
AND     t1.bucket_type = t2.bucket_type
--AND     t1.house_id = t2.house_id
union all
SELECT  t1.dt
        ,"all" as channel
        ,t1.bucket_type
        ,"all" as house_id
        ,sum(duv)
        ,sum(`D页相册区域右滑uv`)
        ,sum(`D页相册区域模块点击uv`)
        ,sum(`D页相册区域右滑orD页相册区域模块点击uv`)
        
        ,sum(ord)
        ,sum(`页面曝光uv`)
        ,sum(`页面曝光pv`)
        ,sum(`视频播放pv`)
        ,sum(`播放视频uv`)
        ,sum(`总播放时长`)
        ,round(sum(`总停留时长`) / sum(`页面曝光pv`),0) AS `D页人均停留时长`
FROM    t1
LEFT JOIN t2
ON      t1.dt = t2.dt
AND     t1.channel = t2.channel
AND     t1.bucket_type = t2.bucket_type
--AND     t1.house_id = t2.house_id
group by 1,2,3,4