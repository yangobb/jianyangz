-- select * 
-- from pdb_analysis_c.ads_other_lottery_hotel_d 
 


with h as (
select distinct house_id
    ,hotel_id
    ,house_city_id
    ,house_city_name
    ,recommended_guest
    ,dynamic_business_id
    ,dynamic_business
    ,great_tag
    ,house_class
    ,house_first_active_time
    ,bedroom_count
    ,case when bedroom_count = 1 then  '一居'
        when bedroom_count = 2 then  '二居'
        when bedroom_count = 3 then  '三居'
        when bedroom_count >= 4 then  '四居+'
        end as bedroom_count_type
    ,case when house_class in ('L21','L1') then '低'
        when house_class in ('L25') then '中'
        when house_class in ('L3','L4') then '高'
        end as `房屋等级`
    ,case when year(house_first_active_time) >= 2023 then '新'
        when year(house_first_active_time) < 2023 then '旧'
        end as `上房时间`
from dws.dws_house_d
where dt = date_sub('${dt}',1)
AND house_is_oversea = 0 --国内
and hotel_is_oversea = 0
and house_is_online = 1 --在线
and landlord_channel = 1 
and great_tag = 1 
),

hotel_price as (
select distinct	weighted_price
    ,a.house_id
    ,(weighted_price/recommended_guest)/(jd_adr/2) `人均比价`
    ,case when (weighted_price/recommended_guest)/(jd_adr/2) <= 1 then '低分位' else '高分位' end `价格对比酒店`
    
from pdb_analysis_c.dwd_house_price_level_d a 
left join h b 
on a.house_id = b.house_id
left join (
    select city_t
        ,sum(ciireceivable)/sum(ciiquantity) as jd_adr
    from (--判断是否七大类  --母酒店
    select distinct is_standard
        ,star
        ,masterhotelid
        ,hotelname
        ,city_t
        ,zonename
    from (
        select distinct is_standard
            ,star
            ,masterhotelid
            ,hotelname
            ,cityname
            ,zonename 
        from app_ctrip.dimmasterhotel   --C酒店基础信息表                                         
        where d = date_sub('${dt}',1)                                         
        and countryname = '中国'                                         
        and masterhotelid > 0 -- 母酒店ID有值 
        and is_standard = 1 --是否标准酒店 1：是、0：否
        and star = 4 
    ) a 
    join excel_upload.dim_qijin_ctcity_mapping m 
    on a.cityname = m.city_c  
    ) mjd 
    join (--酒店下单用户
        select to_date(orderdate) as dt
            ,cityname --下单时城市名称
            ,room
            ,masterhotelid,
            orderid,clientid as uid
            ,ciiquantity  --night
            ,ciireceivable -- gmv
        from  app_ctrip.edw_htl_order_all_split
        where d =  '${dt}'
        and to_date(departure) between date_sub('${dt}',30)  and date_sub('${dt}',1)
        --and cityname in ( '惠州','博罗','惠东','龙门')
        and  submitfrom='client'  --携程app酒店
        and orderstatus in ('S') -- 离店口径
        and country = 1   --下单时国家id
        and ordertype = 2 -- 酒店订单
    ) jd_o 
    on mjd.masterhotelid = jd_o.masterhotelid 
    group by 1
) c
on b.house_city_name = c.city_t
where dt = date_sub('${dt}',1)
),

tu_pp as (
select house_id
    ,(sum(instancecount)-sum(avaliablecount))/sum(instancecount) `满房率`
    ,sum(unavaliablecount)/(sum(instancecount)-sum(avaliablecount)) `途占比`
from (
    select distinct unitid as house_id
        ,instancecount                           --物理库存
        ,avaliablecount                          --可售库存
        ,unavaliablecount                        --已售库存
        ,createdate
        ,inventorydate
    from  dim_tujiaproduct.unit_inventory_log a
    where a.createdate between date_sub('${dt}',14) and date_sub('${dt}',1)
    and a.createdate = a.inventorydate
    and substr(a.gettime,9,2) = 22
) a
group by 1 
),


avg_lpv as (
select avg(lpv) avg_lpv
from (
    select house_city_name
        ,concat(dynamic_business,'-',bedroom_count_type) dynamic_business_beds
        ,count(uid) lpv
        ,count(distinct case when great_tag = 1 then ldbo.house_id end) zx_cnt
    from dws.dws_path_ldbo_d ldbo
    left join (
        select distinct house_id
                ,house_city_id
                ,house_city_name 
                ,great_tag
                ,case when bedroom_count = 1 then  '一居'
                    when bedroom_count = 2 then  '二居'
                    when bedroom_count = 3 then  '三居'
                    when bedroom_count >= 4 then  '四居+'
                    end as bedroom_count_type
        from dws.dws_house_d
        where dt = date_sub('${dt}',1)
        AND house_is_oversea = 0 --国内
        and landlord_channel = 1 

    ) house
    on ldbo.house_id = house.house_id
    where dt between DATE_SUB('${dt}',7) and DATE_SUB('${dt}',1)
    and checkin_date between date_add('${dt}',1) and date_add('${dt}',15)
    and user_type = '用户'
    and wrapper_name in  ('携程','去哪儿','途家')
    and source = '102'
    and get_json_object(server_log,'$.searchScene') = 5 
    group by 1,2
    having count(distinct case when great_tag = 1 then ldbo.house_id end) > 1 
) a 
),

lpv as (

select house_city_name
    ,concat(dynamic_business,'-',bedroom_count_type) dynamic_business_beds
    ,count(uid) lpv
    ,count(distinct case when great_tag = 1 then ldbo.house_id end) zx_cnt
from dws.dws_path_ldbo_d ldbo
left join (
    select distinct house_id
            ,house_city_id
            ,house_city_name 
            ,great_tag
            ,case when bedroom_count = 1 then  '一居'
                when bedroom_count = 2 then  '二居'
                when bedroom_count = 3 then  '三居'
                when bedroom_count >= 4 then  '四居+'
                end as bedroom_count_type
    from dws.dws_house_d
    where dt = date_sub('${dt}',1)
    AND house_is_oversea = 0 --国内
    and landlord_channel = 1 

) house
on ldbo.house_id = house.house_id
where dt between DATE_SUB('${dt}',7) and DATE_SUB('${dt}',1)
and checkin_date between date_add('${dt}',1) and date_add('${dt}',15)
and user_type = '用户'
and wrapper_name in  ('携程','去哪儿','途家')
and source = '102'
and get_json_object(server_log,'$.searchScene') = 5 
group by 1,2
having count(distinct case when great_tag = 1 then ldbo.house_id end) > 1 

),
nights as (
select house_id 
    ,hotel_id
    ,sum(order_room_night_count) `离店间夜`
    ,sum(room_total_amount) `房费`
from dws.dws_order
where checkout_date between date_sub('${dt}',14) and date_sub('${dt}',1)
and is_done = 1
and is_overseas = 0
group by 1,2
),


--信息分
score_90_max as (
select house_id
    ,`基础分-信息分`*0.1 
        + `基础分-价格实惠分` * 0.05
        + `基础分-可预订率` * 0.05
        + `基础分-取消政策` * 0.05
        + `基础分-及时回复率` * 0.05
        + `特色分` * 0.15
        + `奖励分-膨胀神券` * 1
        + `奖励分-优享家` * 1
        + `奖励分-实拍视频` * 1
        + `奖励分-图片新鲜度` * 1
        + `奖励分-自动接单` * 1
        + `奖励分-取消扣首晚` * 1
        + `奖励分-智能调价` * 1
    as `信息维护分`
from (
    select house_id
        ,max(`基础分-信息分`)`基础分-信息分`
        ,max(`基础分-价格实惠分`) `基础分-价格实惠分`
        ,max(`基础分-可预订率`) `基础分-可预订率`
        ,max(`基础分-取消政策`) `基础分-取消政策`
        ,max(`基础分-及时回复率`) `基础分-及时回复率`
        ,max(`特色分`) `特色分`
        ,max(`奖励分-膨胀神券`) `奖励分-膨胀神券`
        ,max(`奖励分-优享家`)`奖励分-优享家`
        ,max(`奖励分-实拍视频`) `奖励分-实拍视频`
        ,max(`奖励分-图片新鲜度`)`奖励分-图片新鲜度`
        ,max(`奖励分-自动接单`)`奖励分-自动接单`
        ,max(`奖励分-取消扣首晚`)`奖励分-取消扣首晚`
        ,max(`奖励分-智能调价`)`奖励分-智能调价`
    from (
        select distinct dt,
            house_id,
            house_information_score as `基础分-信息分`,
            discounted_price_score as `基础分-价格实惠分`,
            canbooking_rate_score as `基础分-可预订率`,
            cancel_policy_score as `基础分-取消政策`,
            im_reply_rate_score as `基础分-及时回复率`,
            5 as `特色分`,
            inflation_coupon_score as `奖励分-膨胀神券`,
                joinact_score as `奖励分-优享家`,
            realvideo_score as `奖励分-实拍视频`,
                picfresh_score as `奖励分-图片新鲜度`,
            fastbooking_score as `奖励分-自动接单`,
            cancelfirst_score as `奖励分-取消扣首晚`,
            pursueprice_score as  `奖励分-智能调价`
        from pdb_analysis_c.ads_house_detail_d t1 
        where dt between date_sub('${dt}',90) and date_sub('${dt}',1)
    ) a 
    group by 1
) a
),

score_14_max as (
select house_id
    ,`基础分-信息分`*0.1 
        + `基础分-价格实惠分` * 0.05
        + `基础分-可预订率` * 0.05
        + `基础分-取消政策` * 0.05
        + `基础分-及时回复率` * 0.05
        + `特色分` * 0.15
        + `奖励分-膨胀神券` * 1
        + `奖励分-优享家` * 1
        + `奖励分-实拍视频` * 1
        + `奖励分-图片新鲜度` * 1
        + `奖励分-自动接单` * 1
        + `奖励分-取消扣首晚` * 1
        + `奖励分-智能调价` * 1
    as `信息维护分`
from (
    select house_id
        ,max(`基础分-信息分`)`基础分-信息分`
        ,max(`基础分-价格实惠分`) `基础分-价格实惠分`
        ,max(`基础分-可预订率`) `基础分-可预订率`
        ,max(`基础分-取消政策`) `基础分-取消政策`
        ,max(`基础分-及时回复率`) `基础分-及时回复率`
        ,max(`特色分`) `特色分`
        ,max(`奖励分-膨胀神券`) `奖励分-膨胀神券`
        ,max(`奖励分-优享家`)`奖励分-优享家`
        ,max(`奖励分-实拍视频`) `奖励分-实拍视频`
        ,max(`奖励分-图片新鲜度`)`奖励分-图片新鲜度`
        ,max(`奖励分-自动接单`)`奖励分-自动接单`
        ,max(`奖励分-取消扣首晚`)`奖励分-取消扣首晚`
        ,max(`奖励分-智能调价`)`奖励分-智能调价`
    from (
        select distinct dt,
            house_id,
            house_information_score as `基础分-信息分`,
            discounted_price_score as `基础分-价格实惠分`,
            canbooking_rate_score as `基础分-可预订率`,
            cancel_policy_score as `基础分-取消政策`,
            im_reply_rate_score as `基础分-及时回复率`,
            5 as `特色分`,
            inflation_coupon_score as `奖励分-膨胀神券`,
                joinact_score as `奖励分-优享家`,
            realvideo_score as `奖励分-实拍视频`,
                picfresh_score as `奖励分-图片新鲜度`,
            fastbooking_score as `奖励分-自动接单`,
            cancelfirst_score as `奖励分-取消扣首晚`,
            pursueprice_score as  `奖励分-智能调价`
        from pdb_analysis_c.ads_house_detail_d t1
        where dt between date_sub('${dt}',14) and date_sub('${dt}',1)
    ) a 
    group by 1
) a
)

,

score_14_avg as (
select house_id
    ,`基础分-信息分`*0.1 
        + `基础分-价格实惠分` * 0.05
        + `基础分-可预订率` * 0.05
        + `基础分-取消政策` * 0.05
        + `基础分-及时回复率` * 0.05
        + `特色分` * 0.15
        + `奖励分-膨胀神券` * 1
        + `奖励分-优享家` * 1
        + `奖励分-实拍视频` * 1
        + `奖励分-图片新鲜度` * 1
        + `奖励分-自动接单` * 1
        + `奖励分-取消扣首晚` * 1
        + `奖励分-智能调价` * 1
    as `信息维护分`
from (
    select house_id
        ,avg(`基础分-信息分`)`基础分-信息分`
        ,avg(`基础分-价格实惠分`) `基础分-价格实惠分`
        ,avg(`基础分-可预订率`) `基础分-可预订率`
        ,avg(`基础分-取消政策`) `基础分-取消政策`
        ,avg(`基础分-及时回复率`) `基础分-及时回复率`
        ,avg(`特色分`) `特色分`
        ,avg(`奖励分-膨胀神券`) `奖励分-膨胀神券`
        ,avg(`奖励分-优享家`)`奖励分-优享家`
        ,avg(`奖励分-实拍视频`) `奖励分-实拍视频`
        ,avg(`奖励分-图片新鲜度`)`奖励分-图片新鲜度`
        ,avg(`奖励分-自动接单`)`奖励分-自动接单`
        ,avg(`奖励分-取消扣首晚`)`奖励分-取消扣首晚`
        ,avg(`奖励分-智能调价`)`奖励分-智能调价`
    from (
        select distinct dt,
            house_id,
            house_information_score as `基础分-信息分`,
            discounted_price_score as `基础分-价格实惠分`,
            canbooking_rate_score as `基础分-可预订率`,
            cancel_policy_score as `基础分-取消政策`,
            im_reply_rate_score as `基础分-及时回复率`,
            5 as `特色分`,
            inflation_coupon_score as `奖励分-膨胀神券`,
                joinact_score as `奖励分-优享家`,
            realvideo_score as `奖励分-实拍视频`,
                picfresh_score as `奖励分-图片新鲜度`,
            fastbooking_score as `奖励分-自动接单`,
            cancelfirst_score as `奖励分-取消扣首晚`,
            pursueprice_score as  `奖励分-智能调价`
        from pdb_analysis_c.ads_house_detail_d t1
        where dt between date_sub('${dt}',14) and date_sub('${dt}',1)
    ) a 
    group by 1
) a
),
final as (
select h.house_id
    ,h.hotel_id
    ,hotel_price.weighted_price `价格`
    ,hotel_price.`人均比价`
    ,hotel_price.`价格对比酒店`
    ,tu_pp.`满房率`
    ,tu_pp.`途占比`
    ,`离店间夜`
    ,`房费`
    ,nvl(`离店间夜`,0) + 1  `抽奖次数`
    ,case when nvl(lpv,99999) / nvl(avg_lpv,99999) >= 1 then 1 else 0 end `LPV排序`
    ,case when hotel_price.`价格对比酒店` = '低分位' then 1 else 0 end `价格排序` 
    ,score_14_avg.`信息维护分`
    ,case when score_90_max.`信息维护分` <= score_14_max.`信息维护分` then score_14_max.`信息维护分` else score_14_avg.`信息维护分` end `门店维护分`
from h
left join hotel_price 
on h.house_id = hotel_price.house_id 
left join tu_pp
on h.house_id = tu_pp.house_id 
left join lpv 
on concat(h.dynamic_business,'-',h.bedroom_count_type) = dynamic_business_beds
and h.house_city_name = lpv.house_city_name
left join avg_lpv 
on 1 = 1 
left join nights 
on h.house_id = nights.house_id 
left join score_90_max
on h.house_id = score_90_max.house_id
left join score_14_max
on h.house_id = score_14_max.house_id
left join score_14_avg
on h.house_id = score_14_avg.house_id
)

select hotel_id hotel_id
    ,cast(avg(`门店维护分`) as decimal(9,2)) hotel_score
    ,nvl(ceil(avg(`离店间夜`)),0) nights
    ,nvl(case when avg(`门店维护分`) <= 2 then 0.5 
        when avg(`门店维护分`) <= 2.5 then 1 
        when avg(`门店维护分`) < 2.7 then 2 
        when avg(`门店维护分`) < 2.9 then 3 
        when avg(`门店维护分`) > 2.9 then 4 end,1) coef
    ,sum(`金额`) money
    ,udf.round_allocation(cast(if(sum(`抽奖次数`)<=10,sum(`抽奖次数`),10) as int),cast(sum(`金额`) as int),"10,20,30,50,100") money_list
	,sum( `抽奖次数`) `抽奖次数`
from (
    select *
        ,case when rn <= 600 then 100
            when cast(`途占比` as decimal(9,2)) = 0 and `LPV排序` = 0 then 30 
            else 50 end `金额`
    from (
        select *
            ,row_number() over(order by `LPV排序`,`价格排序`,`途占比` desc) rn 
        from (
            select *
            from final 
            where cast(`途占比` as decimal(9,2)) <= 0.35 and cast(`满房率` as decimal(9,2)) between 0.4 and 0.8 
        ) a 
    ) rn_tmp1 
    union all 
    select *
        ,case when cast(`途占比` as decimal(9,2)) <= 0.35 and cast(`满房率` as decimal(9,2)) < 0.4 then 10 
            when cast(`途占比` as decimal(9,2)) <= 0.35 and cast(`满房率` as decimal(9,2)) > 0.8 then 20 
            
            when cast(`途占比` as decimal(9,2)) > 0.35 then if((`离店间夜`+1)<=10,(`离店间夜`+1),10) * 10 
            
            when `途占比` is null then 10 
            else 10 end `金额`
    from (
        select *
            ,row_number() over(order by `LPV排序`,`价格排序`,`途占比` desc) rn 
        from final 
        where case when cast(`途占比` as decimal(9,2)) <= 0.35 and cast(`满房率` as decimal(9,2)) between 0.4 and 0.8 then 1 else 0 end = 0  
    ) rn_tmp2
) final 
group by 1 