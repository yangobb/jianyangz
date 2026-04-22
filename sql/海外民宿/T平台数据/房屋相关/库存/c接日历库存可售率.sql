
select weekofyear(dt) week1
    ,house_city_name
    ,round(sum(30_kc) / (sum(hc) * 30),4) 30_kclv
    ,round(sum(7_kc) / (sum(hc) * 7),4) 7_kclv
    
from (
    select dt
        ,country_name
        ,house_city_name
        ,hotel_id
        ,house_class
        ,house_type 
        ,bedroom_type
        ,count(distinct house_id) hc 
        ,sum(30_dt) 30_kc
        ,sum(7_dt) 7_kc 
    from (
        select p.dt
            ,h.country_name
            ,h.house_city_name
            ,h.hotel_id
            ,h.house_class
            ,h.house_type 
            ,h.bedroom_type
            ,p.house_id
            ,count(distinct checkin_date) 30_dt 
            ,count(distinct case when checkin_date <= date_add(current_date,6) then checkin_date end) 7_dt 
        from (
            select dt
                ,house_id
                ,checkout_date
                ,checkin_date
                ,max(can_booking) can_booking
            from dwd.dwd_house_daily_price_d
            where dt between date_sub(current_date,60) and current_date
            and checkin_date between current_date and date_add(current_date,29)
            group by 1,2,3,4
            having(can_booking) >= 1 
        ) p
        inner join (
           select country_name
                ,house_city_name
                ,house_id
                ,hotel_id
                ,house_class
                ,case when house_type = '标准酒店' then '标准酒店' else '非标' end as house_type 
                ,case when bedroom_count >= 3 and share_type = '整租' then '3居+'
                      when bedroom_count = 2 and share_type = '整租' then '2居'
                      when bedroom_count = 1 or share_type = '单间' THEN '1居'
                     else '其他' 
                end as bedroom_type
            from dws.dws_house_d
            where dt = date_sub(current_date,1)
            and house_is_online = 1 
            and house_is_oversea = 1
            and landlord_channel = 334 
            and house_city_name in ('东京','大阪','京都','香港','曼谷','清迈','普吉岛','芭堤雅','吉隆坡','澳门','新加坡','首尔','济州市')
            and house_city_name = '大阪'
        ) h 
        on p.house_id = h.house_id
        group by 1,2,3,4,5,6,7,8 
    ) a 
    group by 1,2,3,4,5,6,7
) a 
group by 1,2
