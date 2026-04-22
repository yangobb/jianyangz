-- -------------海外大盘---------------------------
with cross_uid as (
select a.dt  
    ,a.user_id
    ,nvl(user_level,'其他等级') user_level-- 携程等级
from (
    select 
        dt 
        ,lower(user_id) user_id
    from dws.dws_path_ldbo_d t1 
    where dt between '2025-09-17' and '2025-10-08'
    and wrapper_name = '携程' 
    and source = 102
    and user_type = '用户'
    and is_oversea = 1 
    group by 1,2
) a  
inner join (
    SELECT dt 
        ,case when label_value_text = '0' then '普通会员'
            when label_value_text = '5' then '白银贵宾'
            when label_value_text = '10' then '黄金贵宾'
            when label_value_text = '20' then '铂金贵宾'
            when label_value_text = '30' then '钻石贵宾'
            when label_value_text = '35' then '金钻贵宾'
            when label_value_text = '40' then '黑钻贵宾' 
            else '未知'
            end  user_level
        ,user_id
    FROM (
        select d dt 
            ,a.label
            ,user_id
        from (
            select * 
            from app_ctrip.edw_bnb_dna_user_label_all
            where d between '2025-09-17' and '2025-10-08'
        ) a 
        left join (
            select lower(member_id) user_id 
                ,third_id --三方user_id
            from ods_tujia_member.third_user_mapping
            where channel_code ='CtripId'  
            group by 1,2
        ) b 
        on a.uid = b.third_id
    ) a 
    LATERAL VIEW EXPLODE(
        from_json(label,'array<struct<labelid:string,label_name:string,label_value_text:string>>')
    ) t AS label_obj
    LATERAL VIEW JSON_TUPLE(
        to_json(label_obj),'labelid','label_value_text'
    ) j AS labelid, label_value_text
    WHERE
        j.labelid = '1023'
    group by 1,2,3 
        
) b 
on a.dt = b.dt 
and a.user_id = b.user_id
) 
,list as (
select a.dt
    ,user_level
    ,count(a.user_id) `交叉uv`
    ,count(b.user_id) `携程下单uv`
    ,count(c.user_id) `途家下单uv`
    ,sum(hotel_ord_num) `携程订单量`
    ,sum(hotel_gmv) `酒店GMV`
    ,sum(hotel_night) `酒店间夜`
    ,sum(ms_od_num) `途家订单数`
    ,sum(ms_night)	`途家间夜`
    ,sum(ms_gmv) `途家GMV`
from cross_uid a 
left join (
    select a.bk_date dt 
        ,lower(user_id) user_id
        ,sum(gmv) hotel_gmv
        ,sum(rn_cnt_cii) hotel_night 
        ,count(distinct ord_no) hotel_ord_num
    from (
        select * 
        from app_ctrip.v_edw_inpr_aa_ovs_ord_d
        where d = date_sub(current_date,1)
        and bk_date between '2025-09-17' and '2025-10-08'
        and ord_status in ('P','S')
        and is_tcom = 0
    ) a 
    left join (
        select orderid
            ,lower(uid) uid
            ,country
            ,cityname
            ,orderid
            ,ciiquantity
            ,ciireceivable
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,0) 
        and to_date(orderdate) between '2025-09-17' and '2025-10-08'
    ) b 
    on a.ord_id = b.orderid
    join (
        -- 三方匹配
        select lower(member_id) user_id 
            ,lower(third_id) third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId' 
        group by 1,2
    ) mp 
    on b.uid = mp.third_id
    group by 1,2
) b
ON a.dt = b.dt 
and a.user_id = b.user_id
left join (
    select to_date(create_date) dt 
        ,lower(user_id) user_id 
        ,count(distinct order_no) ms_od_num
        ,sum(order_room_night_count) ms_night	
        ,sum(real_pay_amount) ms_gmv 
        ,sum(real_pay_amount) / sum(order_room_night_count) ms_adr 
    from dws.dws_order a 
    where create_date between '2025-09-17' and '2025-10-08'
    and is_paysuccess_order = 1 --支付成功
    and is_overseas = 1 
    group by 1,2
) c
ON a.dt = c.dt 
and a.user_id = c.user_id
group by 1,2 
)
select  case when dt between '2025-10-01' and '2025-10-08' then '国庆'
            when dt between '2025-09-24' and '2025-09-30' then '节前第一周'
            when dt between '2025-09-17' and '2025-09-23' then '节前第二周'
            end time_type
    ,*
from list

-------------------海外城市---------------------------------

with cross_uid as (
select a.dt  
    ,a.cityname
    ,a.user_id
from (
    select 
        dt 
        ,city_name cityname
        ,lower(user_id) user_id
    from dws.dws_path_ldbo_d t1 
    where dt between '2025-09-17' and '2025-10-08'
    and wrapper_name = '携程' 
    and source = 102
    and user_type = '用户'
    and is_oversea = 1 
    and city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
    group by 1,2,3
) a  
inner join (
    select d dt 
        ,cityname
        ,lower(user_id) user_id  
    from (
        select *
        from app_ctrip.cdm_traf_ht_ctrip_list_qid_day
        where d between '2025-09-17' and '2025-10-08'
        and fh_price > 0 
    ) a 
    inner join (
        select masterhotelid
            ,cityname
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)
        and (countryname != '中国'   or cityname in ('香港','澳门')) 
        and cityname in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
        and masterhotelid > 0
    ) b 
    on a.masterhotelid = b.masterhotelid
    join (
        -- 三方匹配
        select member_id user_id 
            ,third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId' 
        group by 1,2
    ) mp 
    on a.uid = mp.third_id
    group by 1,2,3
) b 
on a.dt = b.dt 
and a.user_id = b.user_id
)
,list as (
select a.dt
    ,a.cityname
    ,count(a.user_id) `交叉uv`
    ,count(b.user_id) `携程下单uv`
    ,count(c.user_id) `途家下单uv`
    ,sum(hotel_ord_num) `携程订单量`
    ,sum(hotel_gmv) `酒店GMV`
    ,sum(hotel_night) `酒店间夜`
    ,sum(ms_od_num) `途家订单数`
    ,sum(ms_night)	`途家间夜`
    ,sum(ms_gmv) `途家GMV`
from cross_uid a 
left join (
    select a.bk_date dt 
        ,city_name cityname
        ,lower(user_id) user_id
        ,sum(gmv) hotel_gmv
        ,sum(rn_cnt_cii) hotel_night 
        ,count(distinct ord_no) hotel_ord_num
    from (
        select * 
        from app_ctrip.v_edw_inpr_aa_ovs_ord_d
        where d = date_sub(current_date,1)
        and bk_date between '2025-09-17' and '2025-10-08'
        and ord_status in ('P','S')
        and is_tcom = 0
        and city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
    ) a 
    left join (
        select orderid
            ,uid
            ,country
            ,cityname
            ,orderid
            ,ciiquantity
            ,ciireceivable
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,0) 
        and to_date(orderdate) between '2025-09-17' and '2025-10-08'
    ) b 
    on a.ord_id = b.orderid
    join (
        -- 三方匹配
        select member_id user_id 
            ,third_id --三方user_id 
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId' 
        group by 1,2
    ) mp 
    on b.uid = mp.third_id
    group by 1,2,3
) b
ON a.dt = b.dt 
and a.user_id = b.user_id
and a.cityname = b.cityname
left join (
    select to_date(create_date) dt 
        ,lower(user_id) user_id 
        ,city_name cityname
        ,count(distinct order_no) ms_od_num
        ,sum(order_room_night_count) ms_night	
        ,sum(real_pay_amount) ms_gmv 
        ,sum(real_pay_amount) / sum(order_room_night_count) ms_adr 
    from dws.dws_order a 
    where create_date between '2025-09-17' and '2025-10-08'
    and is_paysuccess_order = 1 --支付成功
    and is_overseas = 1 
    and city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
    group by 1,2,3
) c
ON a.dt = c.dt 
and a.user_id = c.user_id
and a.cityname = c.cityname
group by 1,2
)
select  case when dt between '2025-10-01' and '2025-10-08' then '国庆'
            when dt between '2025-09-24' and '2025-09-30' then '节前第一周'
            when dt between '2025-09-17' and '2025-09-23' then '节前第二周'
            end time_type 
        ,*
from list
----------------------海外用户等级-------------------------------------------------------
-- -------------海外大盘---------------------------
with cross_uid as (
select a.dt  
    ,a.user_id
    ,nvl(user_level,'其他等级') user_level-- 携程等级
from (
    select 
        dt 
        ,lower(user_id) user_id
    from dws.dws_path_ldbo_d t1 
    where dt between '2025-09-17' and '2025-10-08'
    and wrapper_name = '携程' 
    and source = 102
    and user_type = '用户'
    and is_oversea = 1 
    group by 1,2
) a  
inner join (
    SELECT dt 
        ,case when label_value_text = '0' then '普通会员'
            when label_value_text = '5' then '白银贵宾'
            when label_value_text = '10' then '黄金贵宾'
            when label_value_text = '20' then '铂金贵宾'
            when label_value_text = '30' then '钻石贵宾'
            when label_value_text = '35' then '金钻贵宾'
            when label_value_text = '40' then '黑钻贵宾' 
            else '未知'
            end  user_level
        ,user_id
    FROM (
        select d dt 
            ,a.label
            ,user_id
        from (
            select * 
            from app_ctrip.edw_bnb_dna_user_label_all
            where d between '2025-09-17' and '2025-10-08'
        ) a 
        left join (
            select lower(member_id) user_id 
                ,third_id --三方user_id
            from ods_tujia_member.third_user_mapping
            where channel_code ='CtripId'  
            group by 1,2
        ) b 
        on a.uid = b.third_id
    ) a 
    LATERAL VIEW EXPLODE(
        from_json(label,'array<struct<labelid:string,label_name:string,label_value_text:string>>')
    ) t AS label_obj
    LATERAL VIEW JSON_TUPLE(
        to_json(label_obj),'labelid','label_value_text'
    ) j AS labelid, label_value_text
    WHERE
        j.labelid = '1023'
    group by 1,2,3 
        
) b 
on a.dt = b.dt 
and a.user_id = b.user_id
) 
,list as (
select a.dt
    ,user_level
    ,count(a.user_id) `交叉uv`
    ,count(b.user_id) `携程下单uv`
    ,count(c.user_id) `途家下单uv`
    ,sum(hotel_ord_num) `携程订单量`
    ,sum(hotel_gmv) `酒店GMV`
    ,sum(hotel_night) `酒店间夜`
    
    ,sum(hotel7_gmv) `七大类gmv`
    ,sum(hotel7_night) `七大类间夜`
    ,sum(hotel7_ord_num) `七大类订单`
    
    
    ,sum(ms_od_num) `途家订单数`
    ,sum(ms_night)	`途家间夜`
    ,sum(ms_gmv) `途家GMV`
from cross_uid a 
left join (
    select a.dt 
        ,user_id
        ,sum(a.gmv) hotel_gmv
        ,sum(a.night) hotel_night 
        ,count(distinct a.orderid) hotel_ord_num
 
        ,sum(case when b.ord_id is not null then a.gmv end) hotel7_gmv
        ,sum(case when b.ord_id is not null then a.night end) hotel7_night 
        ,count(distinct case when b.ord_id is not null then a.orderid end ) hotel7_ord_num
    from (
        select orderid
            ,lower(uid) uid
            ,to_date(orderdate) dt 
            ,country
            ,cityname
            ,orderid 
            ,ciiquantity night 
            ,ciireceivable gmv 
            
        from app_ctrip.edw_htl_order_all_split
        where d = date_sub(current_date,0)
        and submitfrom = 'client'
        and to_date(orderdate) between '2025-09-17' and '2025-10-08'
        and orderstatus IN ('P','S')
        and (country <> 1 or cityname in ('香港','澳门'))--海外
        and ordertype = 2 -- 酒店订单
    ) a 
    left join (
        select * 
        from app_ctrip.v_edw_inpr_aa_ovs_ord_d
        where d = date_sub(current_date,1)
        and bk_date between '2025-09-17' and '2025-10-08'
        and ord_status in ('P','S')
        and is_tcom = 0
    ) b 
    on a.orderid = b.ord_id
    join (
        -- 三方匹配
        select lower(member_id) user_id 
            ,lower(third_id) third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId' 
        group by 1,2
    ) mp 
    on a.uid = mp.third_id
    group by 1,2
) b
ON a.dt = b.dt 
and a.user_id = b.user_id
left join (
    select to_date(create_date) dt 
        ,lower(user_id) user_id 
        ,count(distinct order_no) ms_od_num
        ,sum(order_room_night_count) ms_night	
        ,sum(real_pay_amount) ms_gmv 
        ,sum(real_pay_amount) / sum(order_room_night_count) ms_adr 
    from dws.dws_order a 
    where create_date between '2025-09-17' and '2025-10-08'
    and is_paysuccess_order = 1 --支付成功
    and is_overseas = 1 
    group by 1,2
) c
ON a.dt = c.dt 
and a.user_id = c.user_id
group by 1,2 
)
select  case when dt between '2025-10-01' and '2025-10-08' then '国庆'
            when dt between '2025-09-24' and '2025-09-30' then '节前第一周'
            when dt between '2025-09-17' and '2025-09-23' then '节前第二周'
            end time_type
    ,*
from list
