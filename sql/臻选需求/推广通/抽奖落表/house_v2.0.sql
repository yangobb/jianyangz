with h as (
select distinct house_id
    ,house_name
    ,landlord_id
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
where dt = date_sub('${partition}',1)
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
        where d = date_sub('${partition}',1)                                         
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
        where d =  '${partition}'
        and to_date(departure) between date_sub('${partition}',30)  and date_sub('${partition}',1)
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
where dt = date_sub('${partition}',1)
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
    where a.createdate between date_sub('${partition}',14) and date_sub('${partition}',1)
    and a.createdate = a.inventorydate
    and substr(a.gettime,9,2) = 22
) a
group by 1 
),

tu_pp1 as (
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
    where a.createdate = date_sub('${partition}',1) 
    -- where a.createdate = current_date 
    and inventorydate between date_add(current_date,1) and date_add(current_date,14)
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
        where dt = date_sub('${partition}',1)
        AND house_is_oversea = 0 --国内
        and landlord_channel = 1 

    ) house
    on ldbo.house_id = house.house_id
    where dt between DATE_SUB('${partition}',7) and DATE_SUB('${partition}',1)
    and checkin_date between date_add('${partition}',1) and date_add('${partition}',15)
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
    where dt = date_sub('${partition}',1)
    AND house_is_oversea = 0 --国内
    and landlord_channel = 1 

) house
on ldbo.house_id = house.house_id
where dt between DATE_SUB('${partition}',7) and DATE_SUB('${partition}',1)
and checkin_date between date_add('${partition}',1) and date_add('${partition}',15)
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
where checkout_date between date_sub('${partition}',14) and date_sub('${partition}',1)
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
        where dt between date_sub('${partition}',90) and date_sub('${partition}',1)
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
        where dt between date_sub('${partition}',14) and date_sub('${partition}',1)
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
        where dt between date_sub('${partition}',14) and date_sub('${partition}',1)
    ) a 
    group by 1
) a
),
zaibiao as (
select house_id
    ,datediff(current_date,max(dt))-1 `当前在标时长` 
from (
    select a.house_id
        ,a.dt 
        ,great_tag - great_tag_d1 gap 
    from (
        select house_id
            ,dt
            ,great_tag
            ,lag(great_tag,1) over(partition by house_id order by dt asc) great_tag_d1
        from dws.dws_house_d
        -- and great_tag = 1 
    ) a 
    inner join (
        select house_id
        from dws.dws_house_d
        where dt = date_sub(current_date,1) 
        AND house_is_oversea = 0 --国内
        and hotel_is_oversea = 0
        and great_tag = 1 
        and landlord_channel = 1 
    ) b
    on a.house_id = b.house_id
) a 
where gap = 1 
group by 1 
),

final as (
select 
    h.house_city_name
    ,h.dynamic_business
    ,h.bedroom_count
    ,h.house_id
    ,h.house_name 
    ,h.hotel_id
    ,h.landlord_id
    
    ,`当前在标时长` zaibiao_days

    ,`离店间夜` nights 
    ,`房费` live_money
    ,case when score_90_max.`信息维护分` <= score_14_max.`信息维护分` then score_14_max.`信息维护分` 
        else score_14_avg.`信息维护分` end house_score
    
    ,tu_pp.`满房率` full_pp 
    ,tu_pp.`途占比` tu_pp
    ,tu_pp1.`满房率` future14_full_pp
    ,tu_pp1.`途占比` future14_tu_pp

    ,hotel_price.weighted_price weighted_price
    ,`人均比价` price_bat
    ,case when hotel_price.`价格对比酒店` = '低分位' then 1 else 0 end weighted_price_rn
    ,lpv dynamic_bedrooms_lpv
    ,avg_lpv dynamic_bedrooms_lpv_avg
    ,case when nvl(lpv,99999) / nvl(avg_lpv,99999) >= 1 then 1 else 0 end dynamic_bedrooms_lpv_rn


from h
left join hotel_price 
on h.house_id = hotel_price.house_id 
left join tu_pp
on h.house_id = tu_pp.house_id 
left join tu_pp1
on h.house_id = tu_pp1.house_id 
left join lpv 
on concat(h.dynamic_business,'-',h.bedroom_count_type) = lpv.dynamic_business_beds
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
left join zaibiao
on h.house_id = zaibiao.house_id
) 

-- select * from final 

select house_city_name
    ,dynamic_business
    ,bedroom_count
    ,house_id
    ,house_name 
    ,hotel_id
    ,landlord_id
    ,zaibiao_days
    ,nights 
    ,live_money
    ,house_score
    ,full_pp 
    ,tu_pp
    ,future14_full_pp
    ,future14_tu_pp
    ,weighted_price
    ,weighted_price_rn
    ,dynamic_bedrooms_lpv
    ,dynamic_bedrooms_lpv_avg
    ,dynamic_bedrooms_lpv_rn
    ,act_money
    ,act_house_type
from (
select *
    ,case when rn <= 600 then 100
        when cast(`future14_tu_pp` as decimal(9,2)) = 0 and `dynamic_bedrooms_lpv_rn` = 0 then 30 
        else 50 end act_money
    ,'潜力房源' act_house_type
from (
    select *
        ,row_number() over(order by `dynamic_bedrooms_lpv_rn`,`weighted_price_rn`,`future14_tu_pp` desc) rn 
    from (
        select *
        from final 
        where cast(`future14_tu_pp` as decimal(9,2)) <= 0.35 and cast(`future14_full_pp` as decimal(9,2)) between 0.4 and 0.8 
    ) a 
) rn_tmp1 
union all 
select *
    ,case when cast(`future14_tu_pp` as decimal(9,2)) <= 0.35 and cast(`future14_full_pp` as decimal(9,2)) < 0.4 then 10 
        when cast(`future14_tu_pp` as decimal(9,2)) <= 0.35 and cast(`future14_full_pp` as decimal(9,2)) > 0.8 then 20 
        when cast(`future14_tu_pp` as decimal(9,2)) > 0.35 then if((`nights`+1)<=10,(`nights`+1),10) * 10 
        when `future14_tu_pp` is null then 10 
        else 10 end act_money
    ,case when cast(`future14_tu_pp` as decimal(9,2)) <= 0.35 and cast(`future14_full_pp` as decimal(9,2)) < 0.4 then '普通房源' 
        when cast(`future14_tu_pp` as decimal(9,2)) <= 0.35 and cast(`future14_full_pp` as decimal(9,2)) > 0.8 then '普通房源'
        when cast(`future14_tu_pp` as decimal(9,2)) > 0.35 then '优秀房源'
        when `future14_tu_pp` is null then '全平台无售卖'
        else '全平台无售卖' end act_house_type
from (
    select *
        ,row_number() over(order by `dynamic_bedrooms_lpv_rn`,`weighted_price_rn`,`future14_tu_pp` desc) rn 
    from final 
    where case when cast(`future14_tu_pp` as decimal(9,2)) <= 0.35 and cast(`future14_full_pp` as decimal(9,2)) between 0.4 and 0.8 then 1 else 0 end = 0  
) rn_tmp2
) a