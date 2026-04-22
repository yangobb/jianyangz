select house_id
    ,max(gap) `连续最大天数`
from (
    SELECT house_id
        ,date_sub(dt,rn) as diff_date
        ,count(1) gap
    FROM (
        select a.house_id
            ,a.dt 
            ,row_number() over(partition by a.house_id order by a.dt asc) as rn 
        from (
            select house_id
                ,dt
                ,great_tag
            from dws.dws_house_d
            and great_tag = 1 
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
    ) t1
    group by 1,2
) aa 
group by 1 