

with 
hotel as(    --取房屋/门店范围
  select 
    distinct hotel_id,
    hotel_name,
    count(distinct house_id) as house_sum,
    landlord_channel_name,
    hotel_city_name,
    country_name
  from 
    dws.dws_house_d 
  where 
    dt=date_sub(current_Date,1)
    and house_is_oversea = 1
    and landlord_channel_name in ('平台商户')
    and hotel_is_online = 1 
  group by 1,2,4,5,6 
),

credit as(     --取门店对应的诚信分总分
  select
    distinct hotel_id,
    credit_score
  from
    pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d
  where 
    dt=date_sub(current_Date,1)
),

json1 as(   --确认前满房/涨价
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `确认前缺陷次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `确认前缺陷分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "确认前满房/涨价" --改对应的名称)
),

json2 as(   --确认后满房/涨价
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `确认后缺陷次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `确认后缺陷分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "确认后满房/涨价" --改对应的名称)
),

json3 as(   --到店无房
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `到店后缺陷次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `到店后缺陷分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "到店无房" --改对应的名称)
),

json4 as(   --严重入住体验问题
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `严重入住体验问题次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `严重入住体验问题分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "严重入住体验问题" --改对应的名称)
),

json5 as(   --一般入住体验问题
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `一般入住体验问题次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `一般入住体验问题分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "一般入住体验问题" --改对应的名称)
),

json6 as(   --轻微入住体验问题
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `轻微入住体验问题次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `轻微入住体验问题分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "轻微入住体验问题" --改对应的名称)
),

json7 as(   --经营异常
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `经营异常次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `经营异常分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "经营异常" --改对应的名称)
),

json8 as(   --脱离平台交易
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `脱离平台交易次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `脱离平台交易分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "脱离平台交易" --改对应的名称)
),

json9 as(   --引导客人脱离平台交易
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `引导客人脱离平台沟通次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `引导客人脱离平台沟通分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "引导客人脱离平台沟通" --改对应的名称)
),

json10 as(   --配合客人脱离平台交易
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `配合客人脱离平台沟通次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `配合客人脱离平台沟通分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "配合客人脱离平台沟通" --改对应的名称)
),

json11 as(   --虚假交易/点评
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `虚假交易/点评次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `虚假交易/点评分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "虚假交易/点评" --改对应的名称)
),

json12 as(   --欠款>1000
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `欠款大于1000次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `欠款大于1000分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "欠款>1000" --改对应的名称)
),

json13 as(   --欠款<=1000
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `欠款小于1000次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `欠款小于1000分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "欠款<=1000" --改对应的名称)
),

json14 as(   --缴纳保证金
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `缴纳保证金次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `缴纳保证金分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "缴纳保证金" --改对应的名称)
),

json15 as(   --离店订单加分
  select
      distinct hotel_id,
      get_json_object(tmp1.integrity, "$.occurTimes") as `离店订单加分次数`,
      get_json_object(tmp1.integrity, "$.myScore") as `离店订单加分分数`
    from
      pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d LATERAL VIEW EXPLODE(udf.json_split_new(integrity)) tmp1 as integrity
    where
      dt = date_sub(current_date, 1)
      and get_json_object(tmp1.integrity, "$.name") = "离店订单加分" --改对应的名称)
)

select
  distinct a.hotel_id as `门店ID`,
  a.hotel_name as `门店名称`,
  a.house_sum as `房屋数量`,
  a.landlord_channel_name as `货源渠道`,
  a.hotel_city_name as `城市`,
  a.country_name as `国家`,
  b.credit_score as `诚信分`,

  c.`确认前缺陷次数`,
  c.`确认前缺陷分数`,
  d.`确认后缺陷次数`,
  d.`确认后缺陷分数`,
  e.`到店后缺陷次数`,
  e.`到店后缺陷分数`,
  f.`严重入住体验问题次数`,
  f.`严重入住体验问题分数`,
  g.`一般入住体验问题次数`,
  g.`一般入住体验问题分数`,
  h.`轻微入住体验问题次数`,
  h.`轻微入住体验问题分数`,
  i.`经营异常次数`,
  i.`经营异常分数`,
  j.`脱离平台交易次数`,
  j.`脱离平台交易分数`,
  k.`引导客人脱离平台沟通次数`,
  k.`引导客人脱离平台沟通分数`,
  l.`配合客人脱离平台沟通次数`,
  l.`配合客人脱离平台沟通分数`,
  m.`虚假交易/点评次数`,
  m.`虚假交易/点评分数`,
  n.`欠款大于1000次数`,
  n.`欠款大于1000分数`,
  o.`欠款小于1000次数`,
  o.`欠款小于1000分数`,
  p.`缴纳保证金次数`,
  p.`缴纳保证金分数`,
  q.`离店订单加分次数`,
  q.`离店订单加分分数`
from 
  hotel a 
  join credit b on a.hotel_id=b.hotel_id
  left join json1  c on a.hotel_id=c.hotel_id
  left join json2  d on a.hotel_id=d.hotel_id
  left join json3  e on a.hotel_id=e.hotel_id
  left join json4  f on a.hotel_id=f.hotel_id
  left join json5  g on a.hotel_id=g.hotel_id
  left join json6  h on a.hotel_id=h.hotel_id
  left join json7  i on a.hotel_id=i.hotel_id
  left join json8  j on a.hotel_id=j.hotel_id
  left join json9  k on a.hotel_id=k.hotel_id
  left join json10 l on a.hotel_id=l.hotel_id
  left join json11 m on a.hotel_id=m.hotel_id
  left join json12 n on a.hotel_id=n.hotel_id
  left join json13 o on a.hotel_id=o.hotel_id
  left join json14 p on a.hotel_id=p.hotel_id
  left join json15 q on a.hotel_id=q.hotel_id