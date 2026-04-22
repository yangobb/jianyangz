

select hosue_id
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