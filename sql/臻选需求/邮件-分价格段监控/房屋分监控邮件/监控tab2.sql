with final as (
select a.dt
        ,a.house_id
        ,c.hotel_id
        ,a.`基础分-信息分`  `基础分-信息分本期` 
        ,a.`基础分-价格实惠分` `基础分-价格实惠分本期`
        ,a.`基础分-可预订率` `基础分-可预订率本期`
        ,a.`基础分-取消政策` `基础分-取消政策本期`
        ,a.`基础分-及时回复率` `基础分-及时回复率本期`
        ,a.`特色分` `特色分本期`
        ,a.`奖励分-膨胀神券` `奖励分-膨胀神券本期`
        ,a.`奖励分-优享家` `奖励分-优享家本期`
        ,a.`奖励分-实拍视频` `奖励分-实拍视频本期`
        ,a.`奖励分-图片新鲜度` `奖励分-图片新鲜度本期`
        ,a.`奖励分-自动接单` `奖励分-自动接单本期`
        ,a.`奖励分-取消扣首晚` `奖励分-取消扣首晚本期`
        ,a.`奖励分-智能调价` `奖励分-智能调价本期`
        ,a.`房屋可控经营分` `房屋可控经营分本期`
        ,a.house_class_2 `house_class_2本期`
        ,a.hotel_level `hotel_level本期`
        ,b.`基础分-信息分` `基础分-信息分上期`
        ,b.`基础分-价格实惠分` `基础分-价格实惠分上期`
        ,b.`基础分-可预订率` `基础分-可预订率上期`
        ,b.`基础分-取消政策` `基础分-取消政策上期`
        ,b.`基础分-及时回复率` `基础分-及时回复率上期`
        ,b.`特色分` `特色分上期`
        ,b.`奖励分-膨胀神券` `奖励分-膨胀神券上期`
        ,b.`奖励分-优享家` `奖励分-优享家上期`
        ,b.`奖励分-实拍视频` `奖励分-实拍视频上期`
        ,b.`奖励分-图片新鲜度` `奖励分-图片新鲜度上期`
        ,b.`奖励分-自动接单` `奖励分-自动接单上期`
        ,b.`奖励分-取消扣首晚` `奖励分-取消扣首晚上期`
        ,b.`奖励分-智能调价` `奖励分-智能调价上期`
        ,b.`房屋可控经营分` `房屋可控经营分上期`
        ,b.house_class_2 `house_class_2上期`
        ,b.hotel_level `hotel_level上期`
        ,a.`基础分-信息分`- b.`基础分-信息分` `基础分-信息分gap`
        ,a.`基础分-价格实惠分`- b.`基础分-价格实惠分` `基础分-价格实惠分gap`
        ,a.`基础分-可预订率`- b.`基础分-可预订率` `基础分-可预订率gap`
        ,a.`基础分-取消政策`- b.`基础分-取消政策` `基础分-取消政策gap`
        ,a.`基础分-及时回复率`- b.`基础分-及时回复率` `基础分-及时回复率gap`
        ,a.`特色分`- b.`特色分` `特色分gap`
        ,a.`奖励分-膨胀神券`- b.`奖励分-膨胀神券` `奖励分-膨胀神券gap`
        ,a.`奖励分-优享家`- b.`奖励分-优享家` `奖励分-优享家gap`
        ,a.`奖励分-实拍视频`- b.`奖励分-实拍视频` `奖励分-实拍视频gap`
        ,a.`奖励分-图片新鲜度`- b.`奖励分-图片新鲜度` `奖励分-图片新鲜度gap`
        ,a.`奖励分-自动接单`- b.`奖励分-自动接单` `奖励分-自动接单gap`
        ,a.`奖励分-取消扣首晚`- b.`奖励分-取消扣首晚` `奖励分-取消扣首晚gap`
        ,a.`奖励分-智能调价`- b.`奖励分-智能调价` `奖励分-智能调价gap`
        ,a.`房屋可控经营分`- b.`房屋可控经营分` `房屋可控经营分gap`
from (
    select distinct dt,
        house_id,
        0.1*house_information_score as `基础分-信息分`,
        0.05*discounted_price_score as `基础分-价格实惠分`,
        0.05*canbooking_rate_score as `基础分-可预订率`,
        0.05*cancel_policy_score as `基础分-取消政策`,
        0.05*im_reply_rate_score as `基础分-及时回复率`,
        0.15*5 as `特色分`,
        inflation_coupon_score as `奖励分-膨胀神券`,
        joinact_score as `奖励分-优享家`,
        realvideo_score as `奖励分-实拍视频`,
        picfresh_score as `奖励分-图片新鲜度`,
        fastbooking_score as `奖励分-自动接单`,
        cancelfirst_score as `奖励分-取消扣首晚`,
        pursueprice_score as  `奖励分-智能调价`,
        0.1*house_information_score
            +0.05*discounted_price_score
            +0.05*canbooking_rate_score
            +0.05*cancel_policy_score
            +0.05*im_reply_rate_score
            +0.15*5
            +inflation_coupon_score
            +joinact_score
            +realvideo_score
            +picfresh_score
            +cancelfirst_score
            +pursueprice_score
            +fastbooking_score
            as `房屋可控经营分`,
        house_class_2,
        hotel_level
    from pdb_analysis_c.ads_house_detail_d t1
    where dt = date_sub(current_date,2)
) a
left join (
    select distinct dt,
        house_id,
        0.1*house_information_score as `基础分-信息分`,
        0.05*discounted_price_score as `基础分-价格实惠分`,
        0.05*canbooking_rate_score as `基础分-可预订率`,
        0.05*cancel_policy_score as `基础分-取消政策`,
        0.05*im_reply_rate_score as `基础分-及时回复率`,
        0.15*5 as `特色分`,
        inflation_coupon_score as `奖励分-膨胀神券`,
        joinact_score as `奖励分-优享家`,
        realvideo_score as `奖励分-实拍视频`,
        picfresh_score as `奖励分-图片新鲜度`,
        fastbooking_score as `奖励分-自动接单`,
        cancelfirst_score as `奖励分-取消扣首晚`,
        pursueprice_score as  `奖励分-智能调价`,
        0.1*house_information_score
            +0.05*discounted_price_score
            +0.05*canbooking_rate_score
            +0.05*cancel_policy_score
            +0.05*im_reply_rate_score
            +0.15*5
            +inflation_coupon_score
            +joinact_score
            +realvideo_score
            +picfresh_score
            +cancelfirst_score
            +pursueprice_score
            +fastbooking_score
            as `房屋可控经营分`,
        house_class_2,
        hotel_level
    from pdb_analysis_c.ads_house_detail_d t1
    where dt = date_sub(current_date,15)
) b
on a.house_id = b.house_id
join (
select house_id
    ,hotel_id
from dws.dws_house_d
where dt = date_sub(current_date,1)
AND house_is_oversea = 0 --国内
and hotel_is_oversea = 0
and house_is_online = 1 --在线
and landlord_channel = 1 
and great_tag = 1 
) c 
on a.house_id = c.house_id 

)


select `基础分明细` `基础分明细` 
    ,round(`基础分-信息分上期`,2) `基础分-信息分上期`
    ,round(`基础分-信息分本期`,2) `基础分-信息分本期`
    ,round(`下降分值`,2) `下降分值`
    ,`房屋数`
from (
    select '基础分-信息分' `基础分明细` 
        ,`基础分-信息分上期`
        ,`基础分-信息分本期`
        ,`基础分-信息分上期` - `基础分-信息分本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `基础分-信息分上期` > `基础分-信息分本期`
    group by 1,2,3,4 union all 
    select '基础分-价格实惠分' `基础分明细` 
        ,`基础分-价格实惠分上期`
        ,`基础分-价格实惠分本期`
        ,`基础分-价格实惠分上期` - `基础分-价格实惠分本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `基础分-价格实惠分上期` > `基础分-价格实惠分本期`
    group by 1,2,3,4 union all 
    select '基础分-可预订率' `基础分明细` 
        ,`基础分-可预订率上期`
        ,`基础分-可预订率本期`
        ,`基础分-可预订率上期` - `基础分-可预订率本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `基础分-可预订率上期` > `基础分-可预订率本期`
    group by 1,2,3,4 union all 
    select '基础分-取消政策' `基础分明细` 
        ,`基础分-取消政策上期`
        ,`基础分-取消政策本期`
        ,`基础分-取消政策上期` - `基础分-取消政策本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `基础分-取消政策上期` > `基础分-取消政策本期`
    group by 1,2,3,4 union all 
    select '基础分-及时回复率' `基础分明细` 
        ,`基础分-及时回复率上期`
        ,`基础分-及时回复率本期`
        ,`基础分-及时回复率上期` - `基础分-及时回复率本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `基础分-及时回复率上期` > `基础分-及时回复率本期`
    group by 1,2,3,4 union all 
    select '特色分' `基础分明细` 
        ,`特色分上期`
        ,`特色分本期`
        ,`特色分上期` - `特色分本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `特色分上期` > `特色分本期`
    group by 1,2,3,4 union all 
    select '奖励分-膨胀神券' `基础分明细` 
        ,`奖励分-膨胀神券上期`
        ,`奖励分-膨胀神券本期`
        ,`奖励分-膨胀神券上期` - `奖励分-膨胀神券本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-膨胀神券上期` > `奖励分-膨胀神券本期`
    group by 1,2,3,4 union all 
    select '奖励分-优享家' `基础分明细` 
        ,`奖励分-优享家上期`
        ,`奖励分-优享家本期`
        ,`奖励分-优享家上期` - `奖励分-优享家本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-优享家上期` > `奖励分-优享家本期`
    group by 1,2,3,4 union all 
    select '奖励分-实拍视频' `基础分明细` 
        ,`奖励分-实拍视频上期`
        ,`奖励分-实拍视频本期`
        ,`奖励分-实拍视频上期` - `奖励分-实拍视频本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-实拍视频上期` > `奖励分-实拍视频本期`
    group by 1,2,3,4 union all 
    select '奖励分-图片新鲜度' `基础分明细` 
        ,`奖励分-图片新鲜度上期`
        ,`奖励分-图片新鲜度本期`
        ,`奖励分-图片新鲜度上期` - `奖励分-图片新鲜度本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-图片新鲜度上期` > `奖励分-图片新鲜度本期`
    group by 1,2,3,4 union all 
    select '奖励分-自动接单' `基础分明细` 
        ,`奖励分-自动接单上期`
        ,`奖励分-自动接单本期`
        ,`奖励分-自动接单上期` - `奖励分-自动接单本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-自动接单上期` > `奖励分-自动接单本期`
    group by 1,2,3,4 union all 
    select '奖励分-取消扣首晚' `基础分明细` 
        ,`奖励分-取消扣首晚上期`
        ,`奖励分-取消扣首晚本期`
        ,`奖励分-取消扣首晚上期` - `奖励分-取消扣首晚本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-取消扣首晚上期` > `奖励分-取消扣首晚本期`
    group by 1,2,3,4 union all 
    select '奖励分-智能调价' `基础分明细` 
        ,`奖励分-智能调价上期`
        ,`奖励分-智能调价本期`
        ,`奖励分-智能调价上期` - `奖励分-智能调价本期` `下降分值`
        ,count(distinct house_id) `房屋数`
    from final 
    where `房屋可控经营分本期` < 2 and `房屋可控经营分上期` >= 2
    and `奖励分-智能调价上期` > `奖励分-智能调价本期`
    group by 1,2,3,4 
) a 