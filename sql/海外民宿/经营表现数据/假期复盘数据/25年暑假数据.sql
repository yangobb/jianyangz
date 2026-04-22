-- -----------------------基期数据------------------------------------
select case when area_name = '日本' and u.uid is not null then '日本小红书引流成单间夜'
        when area_name = '日本' and house_type = '直采' and is_online_30 = 2 then '日本上线>30天房东间夜（剔除小红书+营销）'
        when area_name = '日本' and house_type = '直采' and is_online_30 = 1 then '日本上线<=30天房东间夜（剔除小红书+营销）'
        when area_name = '日本' and house_type = 'c接' then '日本C接房屋间夜（剔除小红书+营销）'
        when area_name = '亚太其他' and u.uid is not null then '亚太其他小红书引流成单间夜'
        when area_name = '亚太其他' and house_type = '直采' then '亚太其他直采成单间夜（剔除小红书+营销）'
        when area_name = '亚太其他' and house_type = 'c接' then '亚太其他C接房屋间夜（剔除小红书+营销）'
        else '其他' end tongji_info    
    ,round(sum(ms_gmv) / count(distinct checkout_date),1) ms_gmv
    ,round(sum(ms_nights) / count(distinct checkout_date),1) ms_nights
    ,round(sum(ms_od_cnt) / count(distinct checkout_date),1) ms_od_cnt
from (
    select house_id
        ,uid 
        ,checkout_date
        ,sum(real_pay_amount) ms_gmv 
        ,sum(order_room_night_count) ms_nights 
        ,count(distinct order_no) ms_od_cnt 
    from dws.dws_order
    where checkout_date between date_sub('2025-05-30',25) and '2025-05-30'
    and is_paysuccess_order = 1 --支付成功
    and is_success_order = 1 
    and is_done = 1 
    and is_overseas = 1 
    group by 1,2,3 
) od 
left join (
    SELECT dt
        ,country_name
        ,house_city_name
        ,house_id
        ,hotel_id
        ,house_class
        ,house_number
        ,case when datediff(dt,house_first_active_time) < 30 then 1 
            when datediff(dt,house_first_active_time) >= 30 then 2 
            else 0 end is_online_30
        ,case when landlord_channel_name = '平台商户' then '直采' else 'c接' end house_type
        ,case when country_name = '日本' then '日本' when country_name in ('泰国','马来西亚','韩国') then '亚太其他' else '其他' end area_name 
    FROM dws.dws_house_d
    WHERE dt between date_sub('2025-05-30',25) and '2025-05-30'
    AND house_is_online = 1
    AND house_is_oversea = 1
) h 
on od.house_id = h.house_id 
and od.checkout_date = h.dt 
left join (
    select uid
    from (
        select 
            house_city_name
            ,uid
            ,user_id
        from ads.ads_flow_tujia_redbook_recommend_d
    ) a 
    inner join (
        select house_city_name
        FROM dws.dws_house_d
        WHERE dt = date_sub(current_date,1)
        AND house_is_online = 1
        AND house_is_oversea = 1
        group by 1 
    ) b 
    on a.house_city_name = b.house_city_name 
    group by 1 
) u 
on od.uid = u.uid 
group by 1



-- -----------------------by天数据------------------------------------
select checkout_date
    ,case when area_name = '日本' and u.uid is not null then '日本小红书引流成单间夜'
        when area_name = '日本' and house_type = '直采' and is_online_30 = 2 then '日本上线>30天房东间夜（剔除小红书+营销）'
        when area_name = '日本' and house_type = '直采' and is_online_30 = 1 then '日本上线<=30天房东间夜（剔除小红书+营销）'
        when area_name = '日本' and house_type = 'c接' then '日本C接房屋间夜（剔除小红书+营销）'
        when area_name = '亚太其他' and u.uid is not null then '亚太其他小红书引流成单间夜'
        when area_name = '亚太其他' and house_type = '直采' then '亚太其他直采成单间夜（剔除小红书+营销）'
        when area_name = '亚太其他' and house_type = 'c接' then '亚太其他C接房屋间夜（剔除小红书+营销）'
        else '其他' end tongji_info    
    ,round(sum(ms_gmv) / count(distinct checkout_date),1) ms_gmv
    ,round(sum(ms_nights) / count(distinct checkout_date),1) ms_nights
    ,round(sum(ms_od_cnt) / count(distinct checkout_date),1) ms_od_cnt
from (
    select house_id
        ,uid 
        ,checkout_date
        ,sum(real_pay_amount) ms_gmv 
        ,sum(order_room_night_count) ms_nights 
        ,count(distinct order_no) ms_od_cnt 
    from dws.dws_order
    where checkout_date between date_sub('2025-05-30',25) and '2025-05-30'
    and is_paysuccess_order = 1 --支付成功
    and is_success_order = 1 
    and is_done = 1 
    and is_overseas = 1 
    group by 1,2,3 
) od 
left join (
    SELECT dt
        ,country_name
        ,house_city_name
        ,house_id
        ,hotel_id
        ,house_class
        ,house_number
        ,case when datediff(dt,house_first_active_time) < 30 then 1 
            when datediff(dt,house_first_active_time) >= 30 then 2 
            else 0 end is_online_30
        ,case when landlord_channel_name = '平台商户' then '直采' else 'c接' end house_type
        ,case when country_name = '日本' then '日本' when country_name in ('泰国','马来西亚','韩国') then '亚太其他' else '其他' end area_name 
    FROM dws.dws_house_d
    WHERE dt between date_sub('2025-05-30',25) and '2025-05-30'
    AND house_is_online = 1
    AND house_is_oversea = 1
) h 
on od.house_id = h.house_id 
and od.checkout_date = h.dt 
left join (
    select uid
    from (
        select 
            house_city_name
            ,uid
            ,user_id
        from ads.ads_flow_tujia_redbook_recommend_d
    ) a 
    inner join (
        select house_city_name
        FROM dws.dws_house_d
        WHERE dt = date_sub(current_date,1)
        AND house_is_online = 1
        AND house_is_oversea = 1
        group by 1 
    ) b 
    on a.house_city_name = b.house_city_name 
    group by 1 
) u 
on od.uid = u.uid 
group by 1,2 