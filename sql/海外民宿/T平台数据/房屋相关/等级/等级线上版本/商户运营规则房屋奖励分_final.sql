with act_rule as (

        select distinct
            type, --规则类型 1=全国  2=城市 3=商圈
            id,  --城市id,商圈id  -1=全国
            get_json_object(json,'$.key') key,
            get_json_object(json,'$.name') name,
            get_json_object(json,'$.score') score
        from (
        select distinct
            type, --规则类型 1=全国  2=城市 3=商圈
            id,  --城市id,商圈id  -1=全国
            act_detail
        from pdb_analysis_c.dwd_house_reward_rule_d
        where dt=date_sub(current_date,1)--取最新一次更新的规则
        and  current_date between to_date(start_date) and to_date(end_date)  --当前日期在规则生效日期之间
        )a
        lateral view explode(udf.json_split_new(act_detail)) r as json
),
act_house as (
select
    distinct 
    a.house_id,a.landlord_channel,a.dynamic_business_id,a.house_city_id,b.key,b.name,b.is_act,
    case when c.dynamic_business_id is not null then 3 
        else 1 end as type 
from (
select distinct house_id,
nvl(dynamic_business_id,-1) dynamic_business_id , --商圈配置，商圈id为空用-1
house_city_id,landlord_channel
from dws.dws_house_d
where dt = date_sub(current_date(),1)
    and landlord_channel !=334
    and house_is_oversea=1
    and hotel_is_oversea=1
) a 
left join (
select
    house_id,
    get_json_object(json,'$.key') key,
    get_json_object(json,'$.name') name,
    get_json_object(json,'$.is_act') is_act
from pdb_analysis_c.dwd_house_reward_detail_d
lateral view explode(udf.json_split_new(act_detail)) r as json
where dt=date_sub(current_date,1)
) b on a.house_id=b.house_id
left join excel_upload.changzu_dynamic_business_test c on a.dynamic_business_id=c.dynamic_business_id
),
 
house_score as (
--商圈
select
    a.house_id,
    a.landlord_channel,
    a.key,
    a.name,
    cast(a.is_act*b.score as decimal(5,2)) as score
from act_house a
left join act_rule b on  a.type=b.type and a.key=b.key 
where b.key not in ('comment_3','comment_2','comment_1','picFresh_1','picFresh_2')

union all 

select
    a.house_id,
    a.landlord_channel,
    'comment' key,
    '新增点评数' name,
    cast(max(a.is_act*b.score) as decimal(5,2)) as score
from act_house a
join act_rule b on  a.type=b.type and a.key=b.key 
where b.key in ('comment_3','comment_2','comment_1')
group by 1,2,3,4

union all 

select
    a.house_id,
    a.landlord_channel,
    'picFresh' key,
    '图片新鲜度'name,
    cast(max(a.is_act*b.score) as decimal(5,2)) as score
from act_house a
join act_rule b on  a.type=b.type and a.key=b.key 
where b.key in ('picFresh_1','picFresh_2')
group by 1,2,3,4
),
 
 
total_score as (
select
    house_id,
    landlord_channel,
    cast(sum(score) as decimal(5,2)) reward_score
from house_score a
group by 1,2
)
 
select
    t1.house_id,
    t2.landlord_channel,
    t2.reward_score,
    udf.object_to_string(collect_list(map('key',key,'myScore',score,'name',name))) reward
from house_score t1
left join total_score t2 on t1.house_id=t2.house_id
group by 1,2,3