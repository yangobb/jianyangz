
with h as (
select h.house_city_name
    ,h.hotel_id 
    ,h.house_id
    ,h.house_class
    ,h.landlord_channel
    ,h.bedroom_count
    ,case when t1.house_id is not null then 1 else 0 end is_youxuan
    ,case when t2.house_id is not null then 1 else 0 end is_baozang
    ,case when t3.house_id is not null then 1 else 0 end is_zhenshipin
from (
    select house_id
        ,hotel_id 
        ,house_class
        ,house_city_name
        ,case when landlord_channel = 1 then '直采'
            when landlord_channel = 334 then 'C接' end landlord_channel
        ,case when bedroom_count = 1 then '一居' 
            when bedroom_count = 2 then '二居'
            when bedroom_count >=3 then '三居以上' end bedroom_count
    from dws.dws_house_d 
    where dt = date_sub(next_day(current_date(), 'SU'), 7)
    and house_is_online = 1 
    and house_is_oversea = 1
  	and house_class is not null 
) h
left join (
    -- 优选
    SELECT DISTINCT house_id
    FROM pdb_analysis_b.dwd_house_label_1000487_d
    WHERE dt = date_sub(next_day(current_date(), 'SU'), 7)
) t1
ON h.house_id = t1.house_id 
left join (
    -- 宝藏
    SELECT DISTINCT house_id
    FROM pdb_analysis_b.dwd_house_label_1000488_d
    WHERE dt = date_sub(next_day(current_date(), 'SU'), 7)
) t2
ON h.house_id = t2.house_id
left join (
    -- 头图视频
    select house_id
    from dws.dws_house_video_info_d
    where dt = date_sub(next_day(current_date(), 'SU'), 7)
    and source = 1 
    group by 1 
) t3
on h.house_id = t3.house_id
)
,base as (
select h.house_city_name
    ,h.hotel_id
    ,h.house_id
    ,h.house_class
    ,h.landlord_channel
    ,h.bedroom_count
    ,h.is_youxuan
    ,h.is_baozang
    ,h.is_zhenshipin
    ,a.T0Tn
    ,a.empty_filter
    ,a.dt
    ,a.geo_position_id
    ,a.final_price
    ,without_risk_order_num order_num
    ,without_risk_order_room_night night
    ,without_risk_order_gmv gmv
    ,without_risk_order_gmv / without_risk_order_room_night adr
    ,uid
    ,detail_uid
    ,case when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
        when get_json_object(server_log,'$.searchScene') = 2 then '空搜'
        when get_json_object(server_log,'$.searchScene') = 3 then '景区'
        when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
        when get_json_object(server_log,'$.searchScene') = 5 then '地标'
        when get_json_object(server_log,'$.searchScene') = 6 then '定位'
        when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
        when get_json_object(server_log,'$.searchScene') = 8 then '行政区'
        when get_json_object(server_log,'$.searchScene') = 0 then '无' 
        end as search_type
    ,rank_trace_id
    ,get_json_object(server_log,'$.canSalePercentOfL25L34') as canSalePercentOfL25L34
    ,get_json_object(server_log,'$.canSalePercentOfL34') as canSalePercentOfL34
    ,get_json_object(server_log,'$.houseClassScene') as houseClassScene
    ,get_json_object(server_log,'$.releaseCpcDistance') as  releaseCpcDistance
from (
    select  
         case when checkin_date = dt then 'T0'
            when datediff(checkin_date,dt) <= 7 then 'T7'
            when datediff(checkin_date,dt) <= 14 then 'T14'
            else 'T14+' end T0Tn 
        ,*
    from dws.dws_path_ldbo_d 
    where dt between date_sub(next_day(current_date(), 'SU'), 42) and date_sub(next_day(current_date(), 'SU'), 7)
    and wrapper_name in ('携程','途家','去哪儿') 
    and is_oversea = 1 
    and user_type = '用户' 
) a 
inner join h
on a.house_id = h.house_id
)
,lpv_rank as (
select house_city_name
    ,geo_position_id
    ,lpv
    ,lpv_rank
from (
select house_city_name
    ,geo_position_id
    ,lpv
    ,row_number() over(partition by house_city_name order by lpv desc) as lpv_rank
from (
    select house_city_name
        ,geo_position_id 
        ,sum(1) lpv
    from base 
    where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
    group by 1,2 
) a 
) a 
where lpv_rank <= 3 
)
-- --------------------海外流量监控--------------------
-- 汇总
select '汇总' area_name
    ,'汇总' type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
group by 1,2,31
union all 
-- 城市
select 
    house_city_name area_name 
    ,'汇总' type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
group by 1,2,31
union all 
-- 城市 + c接直采
select 
    house_city_name area_name 
    ,landlord_channel type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
group by 1,2,31
union all 
-- 城市 + 等级
select
    house_city_name area_name 
    ,house_class type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
group by 1,2,31
union all 
-- 城市 + 居室
select 
    house_city_name area_name 
    ,bedroom_count type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
group by 1,2,31
union all 
-- 城市 + 空搜
select 
    house_city_name area_name 
    ,'空搜' type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
and empty_filter = 1 
group by 1,2,31
union all 
-- 城市 + 优选
select 
    house_city_name area_name 
    ,'优选' type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
and is_youxuan = 1 
group by 1,2,31
union all 
-- 城市 + 宝藏
select 
    house_city_name area_name 
    ,'宝藏' type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
and is_baozang = 1 
group by 1,2,31
union all 
-- 城市 + 真视频
select 
    house_city_name area_name 
    ,'真视频' type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base 
where house_city_name in ('东京','大阪','吉隆坡','首尔','曼谷','京都','巴厘岛','香港','澳门','新加坡','芭堤雅','普吉岛','清迈','济州市','胡志明市')
and is_zhenshipin = 1 
group by 1,2,31
union all 
-- 城市 + top3地标
select
    a.house_city_name area_name 
    ,a.geo_position_id type_filer
    ,count(distinct house_id) `曝光房屋数`
    ,count(distinct hotel_id) `曝光门店数`
    ,count(distinct case when gmv > 0 then house_id end) `动销房屋数`
    ,count(distinct case when gmv > 0 then hotel_id end) `动销门店数`
    ,avg(final_price) `曝光均价`
    ,avg(case when detail_uid is not null then final_price end) `点击均价`
    ,percentile(final_price,0.25) `曝光25分位价`
    ,percentile(final_price,0.50) `曝光50分位价`
    ,percentile(final_price,0.75) `曝光75分位价`
    ,percentile(final_price,0.95) `曝光95分位价`
    ,avg(adr) `间夜均价`
    ,percentile(adr,0.25) `间夜25分位价`
    ,percentile(adr,0.50) `间夜50分位价`
    ,percentile(adr,0.75) `间夜75分位价`
    ,percentile(adr,0.95) `间夜95分位价`
    ,count(uid) lpv
    ,count(distinct uid) luv
    ,sum(order_num) `订单量`
    ,sum(night) `间夜`
    ,sum(gmv) gmv 
    ,sum(gmv) / sum(night) `adr`
    ,sum(gmv) / count(distinct uid) `uv价值`
    ,concat(round(percentile_approx(canSalePercentOfL34,0.5),2),'%') as `请求维度L34可售率`
    ,concat(round(percentile_approx(canSalePercentOfL25L34,0.5),2),'%') as `请求维度L25+可售率`
    ,concat(round(count(distinct if(houseClassScene='L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34不溢出请求占比`
    ,concat(round(count(distinct if(houseClassScene='L25L34',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L34溢出到L25+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+',rank_trace_id,null))/count(distinct rank_trace_id)*100,2),'%') as `L25溢出到L21+请求占比`
    ,concat(round(count(distinct if(houseClassScene='L24+' AND releaseCpcDistance='true' and search_type='地标',rank_trace_id,null))/count(distinct if(search_type='地标',rank_trace_id,null))*100,2),'%') as `地标下放开降权距离请求占比`
    ,weekofyear(dt) time_type
from base a 
inner join lpv_rank b
on a.house_city_name = b.house_city_name
and a.geo_position_id = b.geo_position_id
group by 1,2,31



