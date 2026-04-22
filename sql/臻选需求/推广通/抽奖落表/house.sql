with h as (
select distinct house_id
    ,hotel_id
    ,house_name
    ,house_city_id
    ,house_city_name
    ,house_is_online
    ,recommended_guest
    ,dynamic_business_id
    ,dynamic_business
    ,great_tag
    ,house_class
    ,house_first_active_time
    ,case when house_class in ('L21','L1') then '低'
        when house_class in ('L25') then '中'
        when house_class in ('L3','L4') then '高'
        end as `房屋等级`
    ,case when year(house_first_active_time) >= 2023 then '新'
        when year(house_first_active_time) < 2023 then '旧'
        end as `上房时间`
from dws.dws_house_d
where dt = date_sub(current_date,1)
AND house_is_oversea = 0 --国内
and hotel_is_oversea = 0
and house_is_online = 1 --在线
and landlord_channel = 1 
and great_tag = 1 
),

 h1 as 
(
select distinct house_id
    ,house_class
    ,case when house_class in ('L21','L1') then '低'
        when house_class in ('L25') then '中'
        when house_class in ('L3','L4') then '高'
        end as `房屋等级`
from dws.dws_house_d
where dt = '2025-03-01'
AND house_is_oversea = 0 --国内
and hotel_is_oversea = 0
-- and great_tag = 1 
and landlord_channel = 1 
),


--信息分
score_0301 as (
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
        where dt = '2025-03-01'
    ) a 
    group by 1
) a
),

score_t1 as (
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
        where dt = date_sub(current_date,1)
    ) a 
    group by 1
) a
),


lingqu as (
select house_id
    ,max(case when information_packet_state = 2 then 1 else 0 end) `领取信息维护`
    ,max(case when operate_packet_state = 2 then 1 else 0 end) `领取经营业绩`
from
  tujia_ods.ods_tj_product_data_bi_best_pick_flow_landlord_pool_data_v2	
group by 1 
),

use_info as (
select business_id as hotel_id
    ,max(case when strategy_id in ('3747810','3747825','3747828') then 1 end) `使用信息维护`
    ,max(case when strategy_id in ('3747861','3747873') then 1 end) `使用经营业绩`
from ods_tujiaonlinepromo.merchant_coupon  --券发放表
where strategy_id in ('3747810','3747825','3747828','3747861','3747873')  --券对应ID
and status = 4 
group by 1
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
        where dt between date_sub(current_date,90) and date_sub(current_date,1)
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
        where dt between date_sub(current_date,14) and date_sub(current_date,1)
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
        where dt between date_sub(current_date,14) and date_sub(current_date,1)
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
        from (
            select house_id
                ,dt 
                ,nvl(great_tag,0) great_tag
                
            from dws.dws_house_d
        ) ba 
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
nights as (
select house_id 
    ,hotel_id
    ,sum(order_room_night_count) `离店间夜`
    ,sum(room_total_amount) `房费`
from dws.dws_order
where checkout_date between date_sub(current_date,14) and date_sub(current_date,1)
and is_done = 1
and is_overseas = 0
group by 1,2
)
 
select h.hotel_id hotel_id
    ,h. house_id house_id
    ,h.house_name house_name
    ,cast(nvl(case when score_90_max.`信息维护分` <= score_14_max.`信息维护分` then score_14_max.`信息维护分` else score_14_avg.`信息维护分` end,0) as decimal(9,2)) `门店维护分`
    ,nvl(`离店间夜`,0) nights


from h
left join h1 
on h.house_id = h1.house_id  
left join nights 
on h.house_id = nights.house_id
left join score_90_max
on h.house_id = score_90_max.house_id
left join score_14_max
on h.house_id = score_14_max.house_id
left join score_14_avg
on h.house_id = score_14_avg.house_id