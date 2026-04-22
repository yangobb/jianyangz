with h as (
select distinct house_id
    ,hotel_id
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
and great_tag = 1 
and landlord_channel = 1 
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
)


select h. house_id
    ,h.hotel_id
    ,h.house_city_name
    ,h.house_is_online
    ,h.dynamic_business
    ,h.great_tag
    ,h.house_first_active_time
    ,h.`上房时间`
    ,`当前在标时长` 
    ,h.house_class `t1房屋等级detil`
    ,h1.house_class `房屋等级detil_0301`
    ,h.`房屋等级` `t1房屋等级`
    ,h1.`房屋等级` `房屋等级_0301`
    ,score_0301.`信息维护分` `信息维护分_0301`
    ,score_t1.`信息维护分` `t1信息维护分`
    ,lingqu.`领取信息维护`
    ,lingqu.`领取经营业绩`
    ,`使用信息维护`
    ,`使用经营业绩`


from h
left join h1 
on h.house_id = h1.house_id 
left join score_0301 
on h.house_id = score_0301.house_id 
left join score_t1 
on h.house_id = score_t1.house_id 
left join lingqu
on h.house_id = lingqu.house_id 
left join use_info
on h.hotel_id = use_info.hotel_id 
left join zaibiao
on h.house_id = zaibiao.house_id 
