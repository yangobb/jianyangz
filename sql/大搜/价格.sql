with htl_l as (
select d,cid
-- ,fh_price
-- ,percentile(fh_price,0.5) as  htl_medprice
from (
select d.d,d.cid,d.uid,d.masterhotelid
-- ,fh_price
,if(length(checkin) = 10,checkin,concat(substr(checkin,1,4),'-',substr(checkin,5,2),'-',substr(checkin,7,2))) checkin_date
,if(length(checkout) = 10,checkout,concat(substr(checkout,1,4),'-',substr(checkout,5,2),'-',substr(checkout,7,2))) checkout_date
-- d,cid,'detail' as pv,uid
from dw_htlbizdb.cdm_traf_ht_ctrip_list_qid_day d 
where d.d between '2024-04-03' and '2024-05-05'
and fh_price > 0 
) a 
where checkout_date between '2024-05-01' and '2024-05-05'
group by 1,2
),
bnb_l as (
select dt as d,uid as cid
-- ,final_price
-- ,percentile(final_price,0.5) as bnb_medprice
from bnb_hive_db.edw_path_ldbo_d
where dt between '2024-04-03' and '2024-05-05'
and checkout_date between '2024-05-01' and '2024-05-05'
 and  wrapper_name = '携程'
     and user_type = '用户'
     and final_price > 0 
group by 1,2
),

htl_o as (
select distinct to_date(orderdate) dt
,t1.clientid as cid
,t1.orderid
,t1.ciiquantity -- 间夜
,case when t3.bed_type = '一居' and t1.ciiroomnum >= 2 then t1.ciiquantity/t1.ciiroomnum else t1.ciiquantity end as night -- 多居折合间夜
,t1.ciireceivable
,t1.ciireceivable/t1.ciiquantity as adr
,case when t2.masterhotelid is not null then '七大类' else '非七大类' end as htl_type 
,case when t3.bed_type = '二居' or t1.ciiroomnum = 2 then '二居'
      when t3.bed_type = '三居以上' or t1.ciiroomnum >= 3 then '三居以上'
 else '一居' end as bed_type
from   sharein_htl.edw_htl_order t1
left join (
select distinct 
room
-- ,roomname
-- ,roomquantity
-- ,masterbasicroom_name
,case when person in ('3','4') then '二居' 
      when cast(person as int) >= 5 then '三居以上' 
	  else '一居'
	  end as bed_type 
from dim_hoteldb.dimroom
where d = date_sub(current_date(),1)
) t3 on t1.room = t3.room
left join (
-- 携程7大类
select distinct t1.masterhotelid,goldstar,is_standard  --母酒店ID
from dim_hoteldb.dimmasterhotel  t1                                       
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'                                 
and countryname = '中国'                                        
and masterhotelid > 0 -- 母酒店ID有值
--  and is_standard = '0'  --是否标准酒店 1：是、0：否  
) t2
on t1.masterhotelid=t2.masterhotelid
where t1.d = date_sub(current_date,0)
            --  and submitfrom='client'
            and submitfrom in ('client','wechat','online','H5','alipay') -- 含app和小程序，app是client
            and subordertype = 0 -- 限制非打包单
            and to_date(orderdate) between '2024-04-03' and '2024-05-05'
            and to_date(departure) between '2024-05-01' and '2024-05-05'
            and orderstatus in ('S')   -- 订单状态 C: 取消 P:处理中(己确认用户和酒店) S:成交(包括全部成交和提前离店) W: 提交未处理
            and country = 1 
            and ordertype = 2 -- 酒店订单
            and uid not in ('_A20190122115701366','_A20151130164107749','_A20190725013107744','E275301478','_A20200710175238972','_A20200211153419761','_A20200921154622724','_A20180814102302643','_A20150928110743155','_A20210226104734937')
),
bnb_o as (
select distinct 
create_date dt
,uid as cid
,order_no
,order_room_night_count
,room_total_amount
,room_total_amount/order_room_night_count as adr
,nvl(h.sanxuan_tag,'无') as sanxuan_tag -- 三选一 
,nvl(h.bed_type,'一居') bed_type
from  bnb_dws_db.dws_order o 
left join (
SELECT house_id,sanxuan_tag
,CASE  WHEN bedroom_count = 1 OR share_type = '单间' THEN '一居'
	   WHEN bedroom_count = 2 AND share_type = '整租' THEN '二居'
	   WHEN bedroom_count >= 3 AND share_type = '整租' THEN '三居以上'
END AS bed_type
FROM bnb_hive_db.edw_dws_house_d_orc_test
WHERE dt = date_sub(current_date,1)
AND house_is_oversea = '0'
) h 
on o.house_id = h.house_id
where create_date between '2024-04-03' and '2024-05-05'
    and checkout_date between '2024-05-01' and '2024-05-05'
    and is_done = 1 --离店口径
    and nvl(landlord_source_channel_code, 0) not IN ('fdlx010901','skmy1907') --非合伙人订单
    and is_overseas = 0  --非海外
    and (terminal_type_name REGEXP '携程' and terminal_type_name not REGEXP '酒店')
),
cross_uv as (
-- 五一交叉用户
select a.d,a.cid 
from bnb_l  a
join htl_l b
on a.d = b.d and lower(a.cid) = lower(b.cid) 
group by 1,2 
)
select '202451' as dd,'房屋属性' as `分类` 
,htl_type as type 
,count(distinct orderid) `C酒店订单`
,sum(ciiquantity) `C酒店间夜` 
,sum(ciireceivable) `C酒店gmv` 
,count(distinct case when a.cid is not null then orderid end) `C交叉酒店订单`
,sum(case when a.cid is not null then ciiquantity end) `C交叉酒店间夜`
,sum(case when a.cid is not null then ciireceivable end) `C交叉酒店gmv`
from htl_o 
left join cross_uv a 
on lower(a.cid) = lower(htl_o.cid) and a.d = htl_o.dt
group by 1,2,3
