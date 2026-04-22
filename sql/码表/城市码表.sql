select 
    id 
    ,name
    ,get_json_object(attrs,'$.ctripGeoId') ctrip_cityid
from ods_tujia_pg.city
where name in ('雅加达','北革')



left join (
    SELECT get_json_object(attrs, '$.ctripRegionId') as cid
        , max(id) as id
    FROM ods_tujia_pg.city
    WHERE is_inland = 0
    group by get_json_object(attrs, '$.ctripRegionId')
) o_t
on a.cityid=o_t.cid
left join ( 
    select country_id
        ,country
        ,city_id	
    from tujia_dim.dim_region
    where city_id is not null
    group by 1,2,3 
) country 
on o_t.id = country.city_id