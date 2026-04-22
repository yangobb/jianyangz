-- 1. 过去60天，早订用户（按天进行分析，可以用7天为一个维度进行聚合）的决策周期（从第一次进入页面到最后一次进入页面，或者完成订单），确定券的生效和失效时间，以及早订用户不同时间段的转化率。
-- 1.5 再添加一个频次字段，早订用户（不用从产单维度看，只要搜索的入离和当天的时间有差异即可），她从第一查询到最后一次（无论下单与否）一共来过几次页面
-- 2. 过去60天，不同层级用户的早订订单占比和转化是否有明显差异（钻石，铂金，黄金以下，判断是否补贴策略分用户群体）
-- 3. 过去60天，不同端口的早订订单占比和转化是否有差异，携程tab，携程宫格，去哪儿tab，去哪儿宫格；

-- 早订用户,转化率(全量用户,已经下单的用户,用户占比)


with fst_60_avtive as (
select uid 
    ,user_id
    ,wrapper_name
    ,dt 
from (
    select uid 
        ,user_id
        ,wrapper_name
        ,dt 
        ,row_number() over(partition by uid order by dt) rn 
    from dws.dws_path_ldbo_d 
    where dt >= date_sub(current_date,60)
    and checkout_date between '2025-11-15' and '2025-12-15'
    and wrapper_name in ('携程','途家','去哪儿') 
    and city_name in ('大阪','东京','京都')
    and is_oversea = 1 
    and user_type = '用户'  
) a 
where rn = 1 
)
,pinci_60_avtive as (
select uid 
    ,count(distinct dt) pinci 
from (
    select uid 
        ,dt 
    from dws.dws_path_ldbo_d 
    where dt >= date_sub(current_date,60)
    and checkout_date between '2025-11-15' and '2025-12-15'
    and wrapper_name in ('携程','途家','去哪儿') 
    and city_name in ('大阪','东京','京都')
    and is_oversea = 1 
    and user_type = '用户'  
    group by 1,2 
) a 
group by 1 
)
,last_60_avtive as (
select uid 
    ,wrapper_name
    ,dt 
from (
    select uid 
        ,wrapper_name
        ,dt 
        ,row_number() over(partition by uid order by dt desc) rn 
    from dws.dws_path_ldbo_d 
    where dt >= date_sub(current_date,60)
    and checkout_date between '2025-11-15' and '2025-12-15'
    and wrapper_name in ('携程','途家','去哪儿') 
    and city_name in ('大阪','东京','京都')
    and is_oversea = 1 
    and user_type = '用户'  
) a 
where rn = 1 
)
,fst_60_order as (
select uid
    ,dt 
from (
    select uid 
        ,create_date dt 
        ,row_number() over(partition by uid order by create_date) rn 
    from dws.dws_order 
    where create_date >= date_sub(current_date,60)
    and checkout_date between '2025-11-15' and '2025-12-15'
    and is_overseas = 1 
    and is_paysuccess_order = 1 
    and is_cancel_order = 0 
) a 
where rn = 1 
)
,member_info as (
SELECT case when label_value_text = '0' then '普通会员'
        when label_value_text = '5' then '白银贵宾'
        when label_value_text = '10' then '黄金贵宾'
        when label_value_text = '20' then '铂金贵宾'
        when label_value_text = '30' then '钻石贵宾'
        when label_value_text = '35' then '金钻贵宾'
        when label_value_text = '40' then '黑钻贵宾' 
        else '未知'
        end  memberlevel
    ,user_id
FROM (
    select a.label
        ,user_id
    from (
        select * 
        from app_ctrip.edw_bnb_dna_user_label_all
        where d = '2025-10-08'
    ) a 
    left join (
        select member_id user_id 
            ,third_id --三方user_id
        from ods_tujia_member.third_user_mapping
        where channel_code ='CtripId'  
        group by 1,2
    ) b 
    on a.uid = b.third_id
) a 
LATERAL VIEW EXPLODE(
    from_json(label,'array<struct<labelid:string,label_name:string,label_value_text:string>>')
) t AS label_obj
-- 步骤3：解析单个JSON对象的字段
LATERAL VIEW JSON_TUPLE(
    to_json(label_obj),'labelid','label_value_text'
) j AS labelid, label_value_text
-- 步骤4：筛选目标labelid
WHERE
    j.labelid = '1023'
group by 1,2 
)


select a.wrapper_name
    ,datediff(b.dt,a.dt) date_gap  
    ,pinci
    ,case when d.uid is not null then 1 else 0 end is_order 
    ,e.memberlevel
    ,count(a.uid)
from fst_60_avtive a 
left join last_60_avtive b 
on a.uid = b.uid  
left join pinci_60_avtive c
on a.uid = c.uid  
left join fst_60_order d
on a.uid = d.uid  
left join member_info e
on a.user_id = e.user_id
where (b.dt <= d.dt or d.uid is null) 
group by 1,2,3,4,5