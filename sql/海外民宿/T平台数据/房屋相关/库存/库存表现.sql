
-- 库存
select
    unitid as house_id,
    sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),7) then avaliablecount end) as avaliablecount_7,
    sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then avaliablecount end) as avaliablecount_30,
    sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),90) then avaliablecount end) as avaliablecount_90,
    sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then unavaliablecount end) as unavaliablecount_30,
    -- sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then unavaliablecount end) /
    -- sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then inventorycount end) tu_pp_30,
    sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then inventorycount - avaliablecount end) /
    sum(case when inventorydate between date_add(current_date(),1) and date_add(current_date(),30) then inventorycount end) full_pp_30
from dim_tujiaproduct.unit_inventory_log
where createdate = date_sub(current_date(),1)
and inventorydate between date_add(current_date(),1) and date_add(current_date(),90)
and substr(gettime, 9, 2) = '22'
and inventorycount is not null
and avaliablecount is not null
and unavaliablecount is not null
group by 1