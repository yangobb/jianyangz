-- 1，当天首个轮次回复时长
with im AS (
  SELECT
    create_date,user_type,user_channel_chinesename,user_id,hotel_id
   ,cast(nvl(round_time_length,999999999) as int) round_time_length
  FROM
    (
      SELECT
        *,
        to_date(round_create_time) AS create_date,-- 会话轮次的创建日期
        row_number() OVER (PARTITION BY user_id,hotel_id,to_date(round_create_time) ORDER BY
            IF(round_time_length<= 60 AND message_type != 14, 0, 1),
            msg_id_create_time -- 某一方在同轮次有多条回复，优先取 非机器人且“及时”回复的 最早一条
        ) AS msg_rn,
   CASE WHEN env_type = '100' THEN '系统' ELSE '人工' END AS user_type -- `发送者类型`,
    -- text AS `消息内容`,
      FROM
        dwd.dwd_comment_im_conversation_d d
         
      WHERE
        to_date(round_create_time) >= date_sub(current_Date(),30)
        -- and user_channel = '101'
        AND hour(round_create_time) >= 8 AND hour(round_create_time) <= 21
        AND message_type NOT IN (16, 19) -- 19:房客身份发出的订单邀评消息，16:单方展示的系统通知消息
        -- AND (
        --   message_type = 14   --AND text  IN (SELECT reply_content FROM b WHERE b.hotel_id = d.hotel_id) --关键词回复消息
        --   OR env_type != 100 OR env_type IS NULL
        -- ) -- 系统发送的消息
        AND round_initiator = '房客' -- 由房客发起的会话轮次
        and sub_stage = '001' -- 售前沟通
        AND user_id NOT IN (
          SELECT user_id FROM ods_pis_ccs.virtual_user_info
          union 
          SELECT local_unique as user_id FROM ods_tns_riskmanagement.blacklist
          union 
          SELECT target_unique_value as user_id FROM ods_tns_riskmanagement.blocklist
        )
    ) a
  WHERE
    msg_rn = 1
),
ord as (
    select create_date,user_id,hotel_id, 
        create_date, --下单日期
        order_no,
        case when terminal_type_name = '携程-APP' then '携程'
            when terminal_type_name = '本站-APP' then '途家'
            when terminal_type_name = '去哪儿-APP' then '去哪儿'
            else '其他'
        end as wrapper_name
    from dws.dws_order
    where create_date BETWEEN date_sub(current_date, 30) AND date_sub(current_date, 1)
        and nvl(landlord_source_channel_code, 0) not IN ('fdlx010901', 'skmy1907') -- 非 合 伙 人 订 单
        and is_overseas = 0 --非 海 外
        and is_paysuccess_order=1 --成功的订单
        and terminal_type_name in ('携程-APP','去哪儿-APP','本站-APP')
),
user_info as (
select create_date,user_channel_chinesename,user_id
,count(distinct case when round_time_length <= 10 then hotel_id end) 10s_num 
,count(distinct case when round_time_length <= 30 and round_time_length > 10 then hotel_id end) 20s_num 
,count(distinct case when round_time_length > 30 then hotel_id end) 30s_num 
,count(distinct hotel_id) total_num
from (
select create_date,user_channel_chinesename,user_id,hotel_id
   ,min(round_time_length) round_time_length
from im
group by 1,2,3,4
) a 
group by 1,2,3
)
select im.create_date ,im.user_channel_chinesename `渠道`
,u.total_num `访问门店数`
,concat('10s内:',10s_num,',10-30s:',20s_num,',30s以上:',30s_num) `门店回复时长`
-- ,case when round_time_length <= 10 then '10s内'
--       when round_time_length <= 30 then '10_30s' 
--       when round_time_length <= 60 then '30_60s' 
--       when round_time_length <= 180 then '1_3min' 
--       when round_time_length <= 300 then '3_5min' 
--       when round_time_length <= 1800 then '5_30min' 
--       else '30min以上' 
-- end as `首轮回复速度`
,count(distinct case when im.user_type = '系统' then im.user_id end) `售前首轮系统回复uv`
,count(distinct case when im.user_type = '人工' then im.user_id end) `售前首轮人工回复uv`
,count(distinct im.user_id) `售前回复uv`

,count(distinct case when im.user_type = '系统' then o.order_no end) `售前首轮系统回复订单量`
,count(distinct case when im.user_type = '人工' then o.order_no end) `售前首轮人工回复订单量`
,count(distinct o.order_no) `售前回复订单量`
from im 
join user_info u 
on im.create_date = u.create_date and lower(im.user_id) = lower(u.user_id) and im.user_channel_chinesename = u.user_channel_chinesename
left join ord o 
on im.create_date = o.create_date and lower(im.user_id) = lower(o.user_id) and im.hotel_id = o.hotel_id
where u.total_num <= 6
group by 1,2,3,4