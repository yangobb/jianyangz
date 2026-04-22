


select landlord_id `业主id`

        ,a.hotel_id `门店id`
        ,hotel_city_name `门店城市`
        ,hotel_name `门店名称`
        
        ,hotel_level `门店等级`
        ,bd `销售`
        ,bd_zhuzhan `主站`
        ,bd_zhanqu `战区`
        ,is_qiwei `是否添加企微`

        ,house_cnt `房屋数量`
        
        ,money `预发总金额`
        ,case when money >= percentile_33_approx then 'P1'
            when money >= percentile_66_approx then 'P2'
            else 'P3' end `优先级`

        ,lot_cnts - nvl(cishu,0) `剩余抽奖次数`
        ,hotel_score `门店维护分`
        ,coef `膨胀系数`

        ,case when (b1.hotel_id is not null or nvl(cishu,0) > 0) then 1 else 0 end `是否进入页面` 
        
        ,nvl(cishu,0) `已抽次数`
        
        ,nvl(zjje,0) `已发金额`
        
        ,date_sub(next_day(current_date, 'Sunday'), 7) `日期`
from (
    select hotel_id
        ,landlord_id
        ,hotel_level
        ,hotel_city_name
        ,hotel_name
        ,bd
        ,bd_zhuzhan
        ,bd_zhanqu
        ,is_qiwei 
    from ads.ads_landlord_best_pick_flow_pool_d 
    where dt = date_sub(next_day(current_date, 'Sunday'), 7)
) a 
left join (
    select 
        get_json_object(event_ext,'$.hotel_id') hotel_id
        ,min(get_json_object(event_ext,'$.text')) `剩余抽奖次数`
        ,sum(get_json_object(event_ext,'$.value')) `中奖金额`
    from dim_tujialog.feweblog
    where create_date > date_sub(next_day(current_date, 'Sunday'), 7)
    and eventname = 'B_ZHENXUAN_LOTTERY_BUTTON_CLICK'
    group by 1
) b 
on a.hotel_id = b.hotel_id
left join (
    select 
        get_json_object(event_ext,'$.hotel_id') hotel_id
    from dim_tujialog.feweblog
    where create_date > date_sub(next_day(current_date, 'Sunday'), 7)
    and eventname in ('B_ZHENXUAN_PAGE_EXPOSURE','B_ZHENXUAN_LOTTERY_BUTTON_CLICK')
    group by 1
) b1 
on a.hotel_id = b1.hotel_id
join (
    select hotel_id 
        ,count(distinct house_id) house_cnt 
    from pdb_analysis_c.ads_other_lottery_house_d
    where dt  = date_sub(next_day(current_date, 'Sunday'), 7)
    group by 1 
) c
on a.hotel_id = c.hotel_id
left join (
    select hotel_id
        ,hotel_score
        ,coef
        ,money
        ,lot_cnts
    from pdb_analysis_c.ads_other_lottery_hotel_d
    where dt = date_sub(next_day(current_date, 'Sunday'), 7)
) d 
on a.hotel_id = d.hotel_id
left join (
    select hotel_id 
        ,count(1) cishu
        ,sum(prize) prize  
        ,sum(coefficient_prize) zjje
    from tujia_ods.ods_tj_product_data_bi_best_pick_flow_raffle_hotel_record_data
    where raffle_version = date_sub(next_day(current_date, 'Sunday'), 7)
    group by 1 
) e
on a.hotel_id = e.hotel_id
left join (
    select percentile_approx(money, 0.33, 1000) AS percentile_33_approx
        ,percentile_approx(money, 0.66, 1000) AS percentile_66_approx
    from pdb_analysis_c.ads_other_lottery_hotel_d
    where dt = date_sub(next_day(current_date, 'Sunday'), 7)
) pp110 
on 1 = 1
