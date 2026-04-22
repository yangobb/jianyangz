with
data_all as (
    select
      *
      ,concat(house_tag,':',house_class_new,':',new_house_class_v2,':',channel_house_class_data) as uniq_str
    from
      ads.ads_house_rank_tag_h
    where
      dt >= date_sub(current_date,3)
),
dt_num as (
    select
    distinct dt,
    hours,
    row_number() over(
      order by
        dt desc,
        hours desc
    ) as num
    from
    (select distinct dt,hours from data_all)   a
),
now_data as (
    select distinct
    a.house_id
    ,a.house_tag
    ,a.house_class_new
    ,a.new_house_class_v2
    ,a.channel_house_class_data
    ,a.uniq_str
    from data_all a
    join (select * from dt_num where num = 1)b on a.dt = b.dt and a.hours = b.hours
),
old_data as (
    select distinct
    a.house_id
    ,a.house_tag
    ,a.house_class_new
    ,a.new_house_class_v2
    ,a.channel_house_class_data
    ,a.uniq_str
    from data_all a
    join (select * from dt_num where num = 2)b on a.dt = b.dt and a.hours = b.hours
)
select distinct 
now_data.house_id as house_id
,concat('{"rankTagIds":',if(now_data.house_tag is null,'[]',now_data.house_tag),'}') as house_tag
,1 as state
,if(now_data.house_class_new is null,0,now_data.house_class_new) as house_class_new
,if(now_data.new_house_class_v2 is null,0,now_data.new_house_class_v2) as new_house_class_v2
,if(now_data.channel_house_class_data is null,'',now_data.channel_house_class_data) as channel_house_class_data
from 
now_data 
left join old_data on now_data.house_id = old_data.house_id 
where 
now_data.uniq_str <> old_data.uniq_str 
or 
old_data.house_id is null 

union all 
select 
distinct old_data.house_id as house_id
,concat('{"rankTagIds":',if(old_data.house_tag is null,'[]',old_data.house_tag),'}') as house_tag
,0 as state
,if(old_data.house_class_new is null,0,old_data.house_class_new) as house_class_new 
,if(old_data.new_house_class_v2 is null,0,old_data.new_house_class_v2) as new_house_class_v2 
,if(old_data.channel_house_class_data is null,'',old_data.channel_house_class_data) as channel_house_class_data 
from 
old_data 
left join now_data on now_data.house_id = old_data.house_id
where now_data.house_id is null

union all 
select 
distinct old_data.house_id as house_id
,concat('{"rankTagIds":',if(old_data.house_tag is null,'[]',old_data.house_tag),'}') as house_tag
,1 as state
,if(old_data.house_class_new is null,0,old_data.house_class_new) as house_class_new 
,if(old_data.new_house_class_v2 is null,0,old_data.new_house_class_v2) as house_class_new 
,if(old_data.channel_house_class_data is null,'',old_data.channel_house_class_data) as channel_house_class_data 
from 
old_data 
left join now_data on now_data.house_id = old_data.house_id
where now_data.uniq_str = old_data.uniq_str 
