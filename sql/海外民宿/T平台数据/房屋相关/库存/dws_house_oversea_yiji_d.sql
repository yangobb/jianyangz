-- pdb_analysis_c.ads_house_score_rank_bottom_oversea_d
-- pdb_analysis_c.dws_house_oversea_yiji_d
-- 0808 更换特色引用表，取消房屋渠道来源的限制
-- 0910 增加真视频维度
---0926新增宝藏民宿
-- pdb_analysis_c.ads_house_score_rank_bottom_oversea_d
-- pdb_analysis_c.dws_house_oversea_yiji_d
-- 0808 更换特色引用表，取消房屋渠道来源的限制
-- 0910 增加真视频维度
with h as (
select distinct h.dt,
h.country_name,h.house_city_name,
h.house_id,h.hotel_id,h.house_class
,case when h.house_type = '标准酒店' then '标准酒店' else '非标' end as house_type 
,case when h.bedroom_count >= 3 and h.share_type = '整租' then '3居+'
      when h.bedroom_count = 2 and h.share_type = '整租' then '2居'
      when h.bedroom_count = 1  OR share_type = '单间' THEN '1居'
     else '其他' 
end as bedroom_type
,case when t.house_id is not null then 1 else 0 end as is_feature
,case when t1.unitnumber is not null then 1 else 0 end as is_auditpassvideo
,case when t2.house_id is not null then 1 else 0 end   as is_yx
,case when t3.house_id is not null then 1 else 0 end   as is_bzms
from dws.dws_house_d h 
left join (
select distinct dt,house_id,style_score_rule 
from pdb_analysis_c.ads_house_score_rank_bottom_oversea_d  -- 特色表换新表
where dt=date_sub("${partition}",1) 
and style_score_rule is not null 
and house_city_name in ('东京','大阪','京都','香港','曼谷','清迈','普吉岛','芭堤雅','吉隆坡','澳门','新加坡','首尔','济州市')
) t 
on h.house_id = t.house_id and h.dt = t.dt 
left join (
select distinct unitnumber, auditvideo,auditpassvideo
    from ods_merchantcrm.houseunitedit
    where auditpassvideo is not null and auditpassvideo != '{ }' --判断是否视频审核通过
) t1 on h.house_number=t1.unitnumber
left join pdb_analysis_b.dwd_house_label_1000487_d t2 on h.house_id = t2.house_id and h.dt = t2.dt and t2.dt = date_sub("${partition}",1)
left join  (
    select 
distinct house_id,dt from pdb_analysis_b.dwd_house_label_1000488_d where dt= date_sub("${partition}",1)
) t3 
on h.house_id = t3.house_id and h.dt = t3.dt
where h.dt = date_sub("${partition}",1)
and h.house_is_online = 1 
and h.house_is_oversea = 1 
AND h.house_city_name in ('东京','大阪','京都','香港','曼谷','清迈','普吉岛','芭堤雅','吉隆坡','澳门','新加坡','首尔','济州市')
-- and h.landlord_channel in ('1','334') -- 这里取消了房屋渠道来源的限制
),
h_type as (
select dt,house_city_name,house_id,hotel_id,house_class,house_type,bedroom_type,is_feature,is_auditpassvideo,is_yx,is_bzms
from h 
group by 1,2,3,4,5,6,7,8,9,10,11
union all 
select dt,country_name as house_city_name,house_id,hotel_id,house_class,house_type,bedroom_type,is_feature,is_auditpassvideo,is_yx,is_bzms
from h 
where country_name in ('日本','泰国')
group by 1,2,3,4,5,6,7,8,9,10,11
union all 
select dt,'13城汇总' as house_city_name,house_id,hotel_id,house_class,house_type,bedroom_type,is_feature,is_auditpassvideo,is_yx,is_bzms
from h 
group by 1,2,3,4,5,6,7,8,9,10,11
),
kc as (
select date_sub("${partition}",1) dt
,house_id
,count(distinct checkin_date) 30_dt 
,count(distinct case when checkin_date <= date_add("${partition}",6) then checkin_date end) 7_dt 
from 
(
select dt,house_id,checkout_date,checkin_date,max(can_booking) can_booking
from dwd.dwd_house_daily_price_d
where
  dt = "${partition}"
  -- and is_api_ctrip = 1  -- 这里取消了只计算携程渠道的报价
  and checkin_date between "${partition}" and date_add("${partition}",29)
  group by 1,2,3,4
) h
where can_booking >= 1
group by 1,2
),
o as (
select
    date_sub("${partition}",1) dt,
    house_id,
    count(distinct order_no) ord
  from dws.dws_order b
  where
    create_date between date_sub("${partition}",7) and date_sub("${partition}",1)
    and is_paysuccess_order = 1 -- 限定了未取消订单
    and is_risk_order = 0 
    and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
    and is_overseas	= 1
  group by 1,2
)
select t1.house_city_name,t1.wd
,t1.hc,round(t1.hc/t2.zhc,4) hc_per 
,t1.30_kc
,t1.7_kc
,round(t1.30_kc/(t1.hc*30),4) 30_kclv
,round(t1.7_kc/(t1.hc*7),4) 7_kclv
,round(t1.ohc/t1.hc,4) dxl --动销率
,t1.ohc
,t1.ohl
,t1.dt
from (
select h.dt,h.house_city_name,'全量在线' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
group by 1,2,3 

union all 
select h.dt, h.house_city_name,house_type as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'L25+' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.house_class in ('L25','L3','L4')
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'L34' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.house_class in ('L3','L4')
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'特色房源' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.is_feature = 1
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'2居+' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.bedroom_type in ('2居','3居+')
group by 1,2,3 

union all 
select h.dt,h.house_city_name,h.bedroom_type as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
group by 1,2,3 

union all 
select h.dt, h.house_city_name,'L25+多居' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.house_class in ('L25','L3','L4') and h.bedroom_type in ('2居','3居+')
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'L25+特色' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.house_class in ('L25','L3','L4') and h.is_feature = 1
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'真视频' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.is_auditpassvideo = 1
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'L25+真视频' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.house_class in ('L25','L3','L4') and h.is_auditpassvideo = 1
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'优选' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.is_yx = 1
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'L25+优选' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.house_class in ('L25','L3','L4') and h.is_yx = 1
group by 1,2,3 
-------9月26日新增
union all 
select h.dt,h.house_city_name,'宝藏民宿' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.is_bzms = 1
group by 1,2,3 

union all 
select h.dt,h.house_city_name,'宝藏L25+' as wd 
,count(distinct h.house_id) hc 
,sum(k.30_dt) 30_kc 
,sum(k.7_dt) 7_kc
,count(distinct o.house_id) ohc 
,count(distinct case when o.house_id is not null then h.hotel_id end) ohl

from h_type h
left join kc k
on h.dt = k.dt and h.house_id = k.house_id
left join o 
on h.dt = o.dt and h.house_id = o.house_id
WHERE h.is_bzms = 1  and   h.house_class in ('L25','L3','L4')
group by 1,2,3 

) t1 
left join (
select h.dt,h.house_city_name
,count(distinct h.house_id) zhc 
from h_type h
group by 1,2 
) t2 
on t1.dt = t2.dt and t1.house_city_name = t2.house_city_name
