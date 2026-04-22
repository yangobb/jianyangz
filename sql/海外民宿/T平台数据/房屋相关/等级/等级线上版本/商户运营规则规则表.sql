--配置表,type=1 全国 type=2 城市 type=3 商圈
with total_rule as( --统一规则的部分
select
    'joinact' as key,--优享家
    '参与优享家' as name,
    '优享家是针对平台优质会员的专属折扣，与常规商促叠加，
    参加后用户可在您房屋详情页看到用户专属等级折扣，可以有效提升转化率' as rule,
     0.2 score
union
select
    'comment_3' as key,--新增点评数
    '新增点评数_3' as name,
    '近30天新增点评数≥3个可获得0.2分' as rule,
     0.2 score

union  
select 
    'comment_2' as key,--新增点评数
    '新增点评数_2' as name,
    '新增点评数2个可获得0.1分' as rule,
    0.1 score

union  
select 
    'comment_1' as key,--新增点评数
    '新增点评数_1' as name,
    '新增点评数1个可获得0.05分' as rule,
    0.05 score


union  
select distinct
    'picnum' as key,--图片数量达标
    '图片数量达标' as name,
    '房屋图片总张数达到35张，或图片总张数/核心空间数>7获得0.1分' as rule,
     0.1 score


union  
select distinct
    'houseChar' as key,--房屋特色描述
    '房屋特色描述' as name,
    '房源描述编辑【房源特色上传】模块，并且特色标题、特色描述、图片均填写内容获得0.1分' as rule,
     0.1 score




union  
select distinct
    'cancelFirst' as key,--取消扣首晚
    '取消扣首晚' as name,
    '取消扣首晚是对连住多晚订单房客一种宽松规则展示，房屋更容易获得曝光，为您带来更多连住订单收益' as rule,
    0.05 as score

--限时活动定时上下线:2024/4/1二次上线
union 
select distinct
    'actDiamond' as key,--【限时】优享家加码奖励
    '【限时】优享家加码' as name,
    '参与优享家，并设置极优用户折扣≤8.5折，可获得0.1分' as rule,
    0.1 as score


union 
select distinct
    'picFresh_1' as key, --图片新鲜度
    '图片新鲜度_1' as name,
    '房屋核心空间图片在1年内拍摄占比>=60%加0.15分' as rule,
    0.15 as score

union  
select distinct
    'picFresh_2' as key, --图片新鲜度
    '图片新鲜度_2' as name,
    '在2年内拍摄占比>=60%加0.1分' as rule,
    0.1 as score
),

replace_rule as ( --要被替换的规则
select distinct
    'pursuePrice' as key,--自动跟价
    '自动跟价' as name,
    '自动追平房价，高效管理，提升房源竞争力' as rule,
    0.05 as score
union 
select distinct
    'freeCancel' as key,--30分钟免费取消
    '30分钟免费取消 ' as name,
    '限免取消是房东对服务质量的象征，
    开通后可享专属房屋标签，为房屋获得更多曝光，
    同时降低房客预订心里门槛，为您带来更多订单收益' as rule,
    0.05 score
),
dynamic_business_rule as(--商圈规则,替换的新规则
select distinct
    'weekRates' as key,
    '周月租优惠' as name,
    '参与周月租特惠活动，并设置连住7天折扣 <= 9.0 折，可获得0.1分' as rule,
     0.1 as score 
)

--通配规则
select
1 as type,
array(-1) id, --通配
'2024-01-31' start_date,
'2030-01-01' end_date,
to_json(collect_list(
    named_struct('key',key,'name',name,'score',cast(score as decimal(5,2)),'rule',rule)
    )) act_detail
from (
    select *
    from total_rule
    union 
    select *
    from replace_rule
    ) t1 
group by 1,2,3,4
 

union all
--商圈
select
3 as type,
id, --商圈列表
'2024-01-31' start_date,
'2030-01-01' end_date,
to_json(collect_list(
    named_struct('key',key,'name',name,'score',cast(score as decimal(5,2)),'rule',rule)
    )) act_detail
from (
    
    select *
    from total_rule
    union 
    select *
    from dynamic_business_rule
   
    ) a 
left join (
    select collect_list(distinct dynamic_business_id) id
    from excel_upload.changzu_dynamic_business_test
) b on 1=1
group by 1,2,3,4
