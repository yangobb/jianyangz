
with score as
(
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"ALL" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"L34" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"0" as `l24`
,"0" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"ALL" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"L25L34" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"0" as `l24`
,"0" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"ALL" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"INITIAL_SCORE" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"0" as `l24`
,"0" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"ALL" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"L24Plus" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"1" as `l24`
,"1" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"TX" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"L34" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"0" as `l24`
,"0" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_holiday_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"TX" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"L25L34" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"0" as `l24`
,"0" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_holiday_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"TX" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"INITIAL_SCORE" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"0" as `l24`
,"0" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_holiday_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
union
select "0" as `area_type`
,"0" as `channel`
,city_id as area_id
,city_name as area_name
,dynamic_business_id as dynamic_id
,dynamic_business as dynamic_name
,"city" as `scene`
,"TX" as check_type
,"ALL" as `week_type`
,"2" as `filter_type`
,"ALL" as `bedroom_count`
,"L24Plus" as `rule_type`
,"D,E,F,G,H,I,J,K" as `buckets`
,"0,9999" as `stay_days_range`
,'1' as `l4`
,'1' as `l3`
,"1" as `l25`
,"1" as `l24`
,"1" as `l21`
,"0" as `l2`
,"0" as `l1`
from 
(
    select distinct *
    from pdb_analysis_c.ads_flow_search_l34cr_holiday_old_d
    where dt=date_sub(current_date,1)
    and search_type='空搜'
) as t1
)
select `area_type`
,`channel`
,area_id
,area_name
,dynamic_id
,dynamic_name
,`scene`
,t1.check_type as `check_type`
,`week_type`
,`filter_type`
,`bedroom_count`
,`rule_type`
,`buckets`
,`stay_days_range`
,case when ord_zhanbi<0.03 and ord_z_city>=100 then 0
else `l4` end as `l4`
,case when ord_zhanbi<0.03 and ord_z_city>=100 then 0
else `l3` end as `l3`
,case when ord_zhanbi<0.03 and ord_z_city>=100 then 0
else `l25` end as `l25`
,case when ord_zhanbi<0.03 and ord_z_city>=100 then 0
else `l24` end as `l24`
,case when ord_zhanbi<0.03 and ord_z_city>=100 then 0
else `l21` end as `l21`
,`l2`
,`l1`
,sameCityType
from
(
select `area_type`
,`channel`
,area_id
,area_name
,dynamic_id
,dynamic_name
,`scene`
,t1.check_type as `check_type`
,`week_type`
,`filter_type`
,`bedroom_count`
,`rule_type`
,`buckets`
,`stay_days_range`
,case 
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr>=2 and t2.ord_z_city>=100 then `l4`*1.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<2 and t2.ord_z_city>=100 and t2.jiaquan_cr>=1.5 then `l4`*1.3
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<1.5 and t2.ord_z_city>=100 and t2.jiaquan_cr>=1.2 then `l4`*1.2
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<1.2 and t2.ord_z_city>=100 and t2.jiaquan_cr>=1.1 then `l4`*1.15
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<1.1 and t2.ord_z_city>=100 and t2.jiaquan_cr>1 then `l4`*1.1
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr=1 and t2.ord_z_city>=100 then `l4`
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<1 and t2.ord_z_city>=100 and t2.jiaquan_cr>=0.9 then `l4`*0.9
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<0.9 and t2.ord_z_city>=100 and t2.jiaquan_cr>=0.8 then `l4`*0.8
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<0.8 and t2.ord_z_city>=100 and t2.jiaquan_cr>=0.7 then `l4`*0.7
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<0.7 and t2.ord_z_city>=100 and t2.jiaquan_cr>=0.6 then `l4`*0.6
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<0.6 and t2.ord_z_city>=100 and t2.jiaquan_cr>=0.5 then `l4`*0.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<0.5 and t2.ord_z_city>=100 and t2.jiaquan_cr>=0.4 then `l4`*0
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.jiaquan_cr<0.4 and t2.ord_z_city>=100 then `l4`*0

when t2.dynamic_business_id is null and t3.jiaquan_cr>=2 and t3.ord_z_city>=100 then `l4`*1.5
when t2.dynamic_business_id is null and t3.jiaquan_cr<2 and t3.ord_z_city>=100 and t3.jiaquan_cr>=1.5 then `l4`*1.3
when t2.dynamic_business_id is null and t3.jiaquan_cr<1.5 and t3.ord_z_city>=100 and t3.jiaquan_cr>=1.2 then `l4`*1.2
when t2.dynamic_business_id is null and t3.jiaquan_cr<1.2 and t3.ord_z_city>=100 and t3.jiaquan_cr>=1.1 then `l4`*1.15
when t2.dynamic_business_id is null and t3.jiaquan_cr<1.1 and t3.ord_z_city>=100 and t3.jiaquan_cr>1 then `l4`*1.1
when t2.dynamic_business_id is null and t3.jiaquan_cr=1 and t3.ord_z_city>=100 then `l4`
when t2.dynamic_business_id is null and t3.jiaquan_cr<1 and t3.ord_z_city>=100 and t3.jiaquan_cr>=0.9 then `l4`*0.9
when t2.dynamic_business_id is null and t3.jiaquan_cr<0.9 and t3.ord_z_city>=100 and t3.jiaquan_cr>=0.8 then `l4`*0.8
when t2.dynamic_business_id is null and t3.jiaquan_cr<0.8 and t3.ord_z_city>=100 and t3.jiaquan_cr>=0.7 then `l4`*0.7
when t2.dynamic_business_id is null and t3.jiaquan_cr<0.7 and t3.ord_z_city>=100 and t3.jiaquan_cr>=0.6 then `l4`*0.6
when t2.dynamic_business_id is null and t3.jiaquan_cr<0.6 and t3.ord_z_city>=100 and t3.jiaquan_cr>=0.5 then `l4`*0.5
when t2.dynamic_business_id is null and t3.jiaquan_cr<0.5 and t3.ord_z_city>=100 and t3.jiaquan_cr>=0.4 then `l4`*0
when t2.dynamic_business_id is null and t3.jiaquan_cr<0.4 and t3.ord_z_city>=100 then `l4`*0

else `l4`
end as `l4`


,case
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr>=2 then `l3`*1.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<2 and t2.jiaquan_cr>=1.5 then `l3`*1.3
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.5 and t2.jiaquan_cr>=1.2 then `l3`*1.2
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.2 and t2.jiaquan_cr>=1.1 then `l3`*1.15
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.1 and t2.jiaquan_cr>1 then `l3`*1.1
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr=1 then `l3`
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1 and t2.jiaquan_cr>=0.9 then `l3`*0.9
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.9 and t2.jiaquan_cr>=0.8 then `l3`*0.8
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.8 and t2.jiaquan_cr>=0.7 then `l3`*0.7
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.7 and t2.jiaquan_cr>=0.6 then `l3`*0.6
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.6 and t2.jiaquan_cr>=0.5 then `l3`*0.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.5 and t2.jiaquan_cr>=0.4 then `l3`*0
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.4 then `l3`*0

when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr>=2 then `l3`*1.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<2 and t3.jiaquan_cr>=1.5 then `l3`*1.3
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.5 and t3.jiaquan_cr>=1.2 then `l3`*1.2
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.2 and t3.jiaquan_cr>=1.1 then `l3`*1.15
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.1 and t3.jiaquan_cr>1 then `l3`*1.1
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr=1 then `l3`
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1 and t3.jiaquan_cr>=0.9 then `l3`*0.9
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.9 and t3.jiaquan_cr>=0.8 then `l3`*0.8
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.8 and t3.jiaquan_cr>=0.7 then `l3`*0.7
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.7 and t3.jiaquan_cr>=0.6 then `l3`*0.6
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.6 and t3.jiaquan_cr>=0.5 then `l3`*0.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.5 and t3.jiaquan_cr>=0.4 then `l3`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.4 then `l3`*0
else `l3`
end as `l3`

,case 
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr>=2 then `l25`*1.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<2 and t2.jiaquan_cr>=1.5 then `l25`*1.3
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.5 and t2.jiaquan_cr>=1.2 then `l25`*1.2
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.2 and t2.jiaquan_cr>=1.1 then `l25`*1.15
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.1 and t2.jiaquan_cr>1 then `l25`*1.1
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr=1 then `l25`
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1 and t2.jiaquan_cr>=0.9 then `l25`*0.9
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.9 and t2.jiaquan_cr>=0.8 then `l25`*0.8
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.8 and t2.jiaquan_cr>=0.7 then `l25`*0.7
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.7 and t2.jiaquan_cr>=0.6 then `l25`*0.6
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.6 and t2.jiaquan_cr>=0.5 then `l25`*0.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.5 and t2.jiaquan_cr>=0.4 then `l25`*0
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.4 then `l25`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr>=2 then `l25`*1.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<2 and t3.jiaquan_cr>=1.5 then `l25`*1.3
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.5 and t3.jiaquan_cr>=1.2 then `l25`*1.2
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.2 and t3.jiaquan_cr>=1.1 then `l25`*1.15
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.1 and t3.jiaquan_cr>1 then `l25`*1.1
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr=1 then `l25`
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1 and t3.jiaquan_cr>=0.9 then `l25`*0.9
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.9 and t3.jiaquan_cr>=0.8 then `l25`*0.8
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.8 and t3.jiaquan_cr>=0.7 then `l25`*0.7
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.7 and t3.jiaquan_cr>=0.6 then `l25`*0.6
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.6 and t3.jiaquan_cr>=0.5 then `l25`*0.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.5 and t3.jiaquan_cr>=0.4 then `l25`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.4 then `l25`*0
else `l25`
end as `l25`
,case 
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr>=2 then `l24`*1.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<2 and t2.jiaquan_cr>=1.5 then `l24`*1.3
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.5 and t2.jiaquan_cr>=1.2 then `l24`*1.2
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.2 and t2.jiaquan_cr>=1.1 then `l24`*1.15
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.1 and t2.jiaquan_cr>1 then `l24`*1.1
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr=1 then `l24`
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1 and t2.jiaquan_cr>=0.9 then `l24`*0.9
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.9 and t2.jiaquan_cr>=0.8 then `l24`*0.8
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.8 and t2.jiaquan_cr>=0.7 then `l24`*0.7
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.7 and t2.jiaquan_cr>=0.6 then `l24`*0.6
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.6 and t2.jiaquan_cr>=0.5 then `l24`*0.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.5 and t2.jiaquan_cr>=0.4 then `l24`*0
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.4 then `l24`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr>=2 then `l24`*1.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<2 and t3.jiaquan_cr>=1.5 then `l24`*1.3
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.5 and t3.jiaquan_cr>=1.2 then `l24`*1.2
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.2 and t3.jiaquan_cr>=1.1 then `l24`*1.15
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.1 and t3.jiaquan_cr>1 then `l24`*1.1
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr=1 then `l24`
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1 and t3.jiaquan_cr>=0.9 then `l24`*0.9
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.9 and t3.jiaquan_cr>=0.8 then `l24`*0.8
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.8 and t3.jiaquan_cr>=0.7 then `l24`*0.7
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.7 and t3.jiaquan_cr>=0.6 then `l24`*0.6
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.6 and t3.jiaquan_cr>=0.5 then `l24`*0.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.5 and t3.jiaquan_cr>=0.4 then `l24`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.4 then `l24`*0
else `l24`
end as `l24`

,case 
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr>=2 then `l21`*1.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<2 and t2.jiaquan_cr>=1.5 then `l21`*1.3
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.5 and t2.jiaquan_cr>=1.2 then `l21`*1.2
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.2 and t2.jiaquan_cr>=1.1 then `l21`*1.15
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1.1 and t2.jiaquan_cr>1 then `l21`*1.1
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr=1 then `l21`
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<1 and t2.jiaquan_cr>=0.9 then `l21`*0.9
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.9 and t2.jiaquan_cr>=0.8 then `l21`*0.8
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.8 and t2.jiaquan_cr>=0.7 then `l21`*0.7
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.7 and t2.jiaquan_cr>=0.6 then `l21`*0.6
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.6 and t2.jiaquan_cr>=0.5 then `l21`*0.5
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.5 and t2.jiaquan_cr>=0.4 then `l21`*0
when t2.dynamic_business_id is not null and t2.sameCityType is not null and t2.ord_z_city>=100 and t2.jiaquan_cr<0.4 then `l21`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr>=2 then `l21`*1.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<2 and t3.jiaquan_cr>=1.5 then `l21`*1.3
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.5 and t3.jiaquan_cr>=1.2 then `l21`*1.2
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.2 and t3.jiaquan_cr>=1.1 then `l21`*1.15
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1.1 and t3.jiaquan_cr>1 then `l21`*1.1
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr=1 then `l21`
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<1 and t3.jiaquan_cr>=0.9 then `l21`*0.9
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.9 and t3.jiaquan_cr>=0.8 then `l21`*0.8
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.8 and t3.jiaquan_cr>=0.7 then `l21`*0.7
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.7 and t3.jiaquan_cr>=0.6 then `l21`*0.6
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.6 and t3.jiaquan_cr>=0.5 then `l21`*0.5
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.5 and t3.jiaquan_cr>=0.4 then `l21`*0
when t2.dynamic_business_id is null and t3.ord_z_city>=100 and t3.jiaquan_cr<0.4 then `l21`*0
else `l21`
end as `l21`
,`l2`
,`l1`
,t1.sameCityType

,case when t1.sameCityType='ALL' then t3.ord_zhanbi
else t2.ord_zhanbi end as ord_zhanbi

,case when t1.sameCityType='ALL' then t3.ord_z_city
else t2.ord_z_city end as ord_z_city
from(
    select *
    ,"SAME_CITY" as sameCityType
    from score
    union
    select *
    ,"OTHER_CITY" as sameCityType
    from score
    union
    select *
    ,"ALL" as sameCityType
    from score
) as t1
left join 
(
        select distinct *
        ,case when loc_or_other='本地' then 'SAME_CITY'
        when loc_or_other='异地' then 'OTHER_CITY'
        else '其他' end as sameCityType
        ,'ALL' as check_type
        ,ord_cr*0.5+gmv_cr*0.5 as jiaquan_cr
        ,case when ord_z_city>=100 and (pv_zhanbi>=0.1 or ord_zhanbi>=0.1 or gmv_zhanbi>=0.1) and healthy_num>=0.05 then 1 else 0 end as is_select
        from pdb_analysis_c.ads_flow_city_dynamic_loc_or_other_d
        where dt=date_sub(current_date,1)

        union

        select distinct *
        ,case when loc_or_other='本地' then 'SAME_CITY'
        when loc_or_other='异地' then 'OTHER_CITY'
        else '其他' end as sameCityType
        ,'TX' as check_type
        ,ord_cr*0.5+gmv_cr*0.5 as jiaquan_cr
        ,case when ord_z_city>=100 and (pv_zhanbi>=0.1 or ord_zhanbi>=0.1 or gmv_zhanbi>=0.1) and healthy_num>=0.05 then 1 else 0 end as is_select
        from pdb_analysis_c.ads_flow_city_dynamic_loc_or_other_d
        where dt=date_sub(current_date,1)
) as t2
on t1.area_id=t2.city_id
and t1.dynamic_id=t2.dynamic_business_id
and t1.sameCityType=t2.sameCityType
and t1.check_type=t2.check_type
left join 
(
        select distinct *
        ,case when loc_or_other='本地' then 'SAME_CITY'
        when loc_or_other='异地' then 'OTHER_CITY'
        else '其他' end as sameCityType
        ,'ALL' as check_type
        ,ord_cr*0.5+gmv_cr*0.5 as jiaquan_cr
        ,case when ord_z_city>=100 and (pv_zhanbi>=0.1 or ord_zhanbi>=0.1 or gmv_zhanbi>=0.1) and healthy_num>=0.05 then 1 else 0 end as is_select
        from pdb_analysis_c.ads_flow_city_dynamic_loc_or_other_d
        where dt=date_sub(current_date,1)
        AND loc_or_other='异地'

        union

        select distinct *
        ,case when loc_or_other='本地' then 'SAME_CITY'
        when loc_or_other='异地' then 'OTHER_CITY'
        else '其他' end as sameCityType
        ,'TX' as check_type
        ,ord_cr*0.5+gmv_cr*0.5 as jiaquan_cr
        ,case when ord_z_city>=100 and (pv_zhanbi>=0.1 or ord_zhanbi>=0.1 or gmv_zhanbi>=0.1) and healthy_num>=0.05 then 1 else 0 end as is_select
        from pdb_analysis_c.ads_flow_city_dynamic_loc_or_other_d
        where dt=date_sub(current_date,1)
        AND loc_or_other='异地'
) as t3
on t1.area_id=t3.city_id
and t1.dynamic_id=t3.dynamic_business_id
and t1.check_type=t3.check_type
) as t1