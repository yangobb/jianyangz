with house_qd as (
    select a.*
    from excel_upload.spring_festival_tailand_house_detail a 
    left join (
        -- 关联大型连锁
        select type 
            ,concat_ws('|',collect_set(brand)) check_info 
        from excel_upload.houses_level_info0312v1	
        group by 1
    ) b
    on 1 = 1
    where regexp_like(house_name,check_info) = 0
)
,final as (
select *
from house_qd
where house_city_name = '清迈'
union all 
select a.*
from (
    select *
    from house_qd
    where house_city_name = '曼谷'
) a 
left join (
    select house_id 
        ,percentile(final_price,0.5) final_price 
    from dws.dws_path_ldbo_d 
    where dt between date_sub(current_date,14) and date_sub(current_date,14)
    and checkout_date between '2026-02-09' and '2026-02-28'
    and source = 102 
    and user_type = '用户'
    and city_name = '曼谷'
    group by 1 
    having final_price < 700 
) b 
on a.house_id = b.house_id
where b.house_id is not null 
)

-- select house_city_name
--     ,count(1)
-- from final 
-- group by 1 

select 
    a1.dt 
    ,a.house_city_name
    ,count(distinct a.house_id) house_cnt 
    ,count(distinct h.house_id) cb_house_cnt
    ,sum(is_canbook) `物理库存`
    ,sum(can_booking) `可售库存` 
    ,sum(can_booking) / sum(is_canbook) empty_rate
from final a 
left join (
    select day_date dt
        ,1 is_canbook
    from tujia_dim.dim_date_info
    where day_date between '2026-02-15' and '2026-02-24'
) a1 
on 1 = 1
left join (
    select house_id 
        ,can_booking
        ,checkin_date dt  
    from dwd.dwd_house_daily_price_d 
    where dt = date_sub(current_date,1)
    and checkin_date between '2026-02-15' and '2026-02-24'
) h 
on a.house_id = h.house_id
and a1.dt = h.dt 
group by 1,2