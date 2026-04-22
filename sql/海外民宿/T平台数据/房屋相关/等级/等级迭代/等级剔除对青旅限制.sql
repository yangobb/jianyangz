
-- 1、复制国内调度
-- 1）表名修改：全部增加oversea
-- 2）范围修改：1:house_is_oversea = 1,landlord_channel=303修改为334
-- 2、基础分：对比国内修改
-- 1）营业额：海外携程营业额（剔除标准酒店：90日未取消订单）+途家营业额（90日未取消订单；国内为直采和c接在途家的营业额）
-- 2）评论分：直接照抄
-- 3）信息分：区分了直采(dwd_house_infor_score_oversea_d)和c接(edw_htl_hotel_psi_score)，直接照抄
-- 4)布局和特色为海外旧规则
-- 3、附加分
-- 1）奖励分：直接照抄国内
-- 2）诚信分：直接照抄国内
-- 3）处罚分、黑白名单等使用业务定义线下表
-- 4、表设计
-- 1）表结构：基础信息（规则未使用的不写进来，旧表关联：ads_house_oversea_core13_level_all_d）+计分规则(可用于前端展示)+实际分数+等级转换；
-- 2）规则按照json格式为主
-- 3）分数转换为等级@安帅 提供规则
-- 4）数据解析：get_json_object()
-- 5、临时表：tujia_tmp.oversea_house_class_20240427_d
-- 1）先刷新下调度创建表

--2024/5/7
--1）信息分加oversea：pdb_analysis_c.dwd_house_infor_score_oversea_d
--2）携程ord_month1临时表 from ongoing_order_ctrip 
--3）通过改2），可能解决1、tujia_tmp.oversea_house_class_20240427_d_v1中 直采：找1-2个house_id，去直接查携程营业分；有交易，分是否保存，2、通过查case来看，是否新版本有漏算

--2024/5/8
--1) infor1 中限定merchant_guid = "1da8e4e1-5ab3-4434-b23e-122a5884334f" 
--2)加入两列,主要改了create_order和create_order_ctrip两个临时表，增加t_365d_gmv、c_365d_gmv两列。
--3）credit_score没有数据，刷数，重新建立临时表 tujia_tmp.oversea_house_class_20240507_d_v4

--2024/5/9
--1）score为字符串
--2) 营业额asc改成desc


--2024/5/13
--1)房屋品质和特色标签的占比从 10%，调整为 15%

--2024/5/20
--1)房源信息分数值阈值调动
--2)无商圈的城市没有营业额分:按照城市排名
--nvl(if(round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4) is not null ,round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4),round(a.rk_city_gmv/b.rk_city_gmv,4)) ,0.0) as rk_dynamic_gmv
--3) 无特色标签赋值为1，因为打标的房屋id至少都有一个标签，style_score都会大于1，所以在最后改 nvl(j.style_score,1) as style_score
--4）---处罚分 penalty_score

--2023/5/23
--1）调等级

--2024/06/12
--1)黑白名单测算不对
--2）agoda新房保护期加分


--2024/06/18
-- 直采加权修改为 4 分
-- 增加城市分级，房屋类型，国家，房屋系统等级,房屋链接字段
-- 非核心城市营业额计算规则和等级得分阈值修改

--2024/07/19
-- 增加大型连锁酒店命中减分

--2024/08/01
-- 复原非 13 城营业额加分途家和携程加分权重

--2024/08/15
-- 更换黑名单L0 降权表为庞博提供调度表

--2024/09/14
-- 修改青旅类房屋减分，减分分值从-0.5 改为减-2.0 分

--2024/09/19
-- 修改海外核心城市高等级得分阈值，高等级房源量级分布
--create table tujia_tmp.oversea_house_class_20240904 as 

--2024/09/26
-- 增加非 13 城标准酒店减分 减2分 
--create table tujia_tmp.oversea_house_class_20240926 as 

--2024/10/17
-- 删除了此前等级表中针对拒单的减分

--2024/11/4
--13城和非13城等级计算标准合并，计算分值仍分开计算，仅最后分值转化为等级生效
--tujia_tmp.oversea_house_class_20241031 as
--2025/2/14
--L0统一收口由诚信分约束，房屋分最低等级改为L1
--修改青旅以及非十三城酒店类型房屋减分分值从-2 改为-1
--2025/2/26诚信分阈值更改-2—-3.5

with h as (--全量海外房屋
select distinct dt,
    house_id,
    house_name,
    house_is_online,
    hotel_id,
    hotel_name,
    hotel_is_online,
    landlord_channel,
    landlord_channel_name,
    to_date(house_first_active_time)   house_first_active_time,--首次上架时间
    CASE 
    WHEN landlord_channel = 339 THEN 
    CASE 
      WHEN house_first_active_time <= '2024-06-12' THEN 
        CASE 
          WHEN to_date('2024-06-12') > date_sub('${date}', 30) THEN 1 ELSE 0 END
      ELSE 
        CASE 
          WHEN to_date(house_first_active_time) > date_sub('${date}', 30) THEN 1 ELSE 0 END
    END
  ELSE 
    CASE 
      WHEN to_date(house_first_active_time) > date_sub('${date}', 30) THEN 1 ELSE 0 END
END AS is_new_house, --30天新房
    house_city_id,
    house_city_name as city_name,
    level2_area_name,
    dynamic_business,
    dynamic_business_id,
    bedroom_count,
    recommended_guest,
    bedcount,
    gross_area,
    nvl(cast(comment_score as decimal(5,1)),0) comment_score,  --历史点评分  跟其他页面前 端保持一致，先进行四舍五入
    is_fast_booking,--是否开通闪订
    house_type,
    case when house_city_name in ("巴厘岛", "巴黎", "伦敦", "仙本那", "墨尔本", "名古屋", "哥打京那巴鲁", "胡志明市", "纽约", "巴塞罗那", "富士河口湖町", "釜山", "仁川", "乔治市", "福冈", "迪拜", "台北", "悉尼", "洛杉矶") then 'A级'
          when house_city_name in ("罗马", "开罗", "莫斯科", "河内", "马德里", "箱根", "伊斯坦布尔", "札幌", "万象", "兰卡威", "甲米", "旧金山", "威尼斯", "横滨", "神户", "米兰", "琅勃拉邦", "阿姆斯特丹", "爱丁堡", "奈良", "镰仓市", "新山", "高雄", "利雅得", "维也纳", "热海市", "新北") then 'B级'
          when house_city_name in ("西归浦市", "芽庄", "佛罗伦萨", "尼斯", "里斯本", "圣彼得堡", "金边", "雅典", "慕尼黑", "布达佩斯", "柏林", "苏黎世", "泗水", "熊本", "哥本哈根", "苏梅岛", "雷克雅未克", "温哥华", "多伦多", "布里斯班", "皇后镇", "富士宫市", "沙美岛", "波士顿", "马六甲", "阿拉木图", "华欣", "雅加达", "马尼拉", "珀斯", "丽贝岛", "因特拉肯", "贝尔格莱德", "法兰克福", "台中", "加德满都", "岘港", "基多", "那霸", "都柏林") then 'C级'
          when house_city_name in ('新加坡','香港','首尔','清迈','普吉岛','曼谷','京都','济州市','吉隆坡','东京','大阪','芭堤雅','澳门') then 'S级'
          else 'D级'
          end as city_level,   -- 新增城市等级字段
    country_name, -- 新增国家字段
    house_class, -- 新增房屋系统房屋等级字段
    concat('https://nedit.tujia.com/housingSystem/housingDetail?houseId=',house_id) as house_url -- 新增房屋链接字段
from dws.dws_house_d
where dt = date_sub('${date}',1)
    and landlord_channel is not null 
    and house_is_oversea=1
    and hotel_is_oversea=1
),
--有订单底线限制的，统一用这个
--2024/4/1统一用预订口径
create_order as(
    select 
        house_id,
        count(distinct if(create_date between date_sub('${date}', 90) and date_sub('${date}', 1),order_no,null)) as 90d_ord,
        sum(if(create_date between date_sub('${date}', 90) and date_sub('${date}', 1),order_room_night_count,0)) as 90d_night,
        sum(if(create_date between date_sub('${date}', 90) and date_sub('${date}', 1),room_total_amount,0)) as 90d_gmv,
        sum(if(create_date between date_sub('${date}', 364) and date_sub('${date}', 1),room_total_amount,0)) as 365d_gmv
    from dws.dws_order 
    --where create_date between date_sub('${date}',90) and date_sub('${date}',1) --海外用近90天
    where is_paysuccess_order = 1 -- 支付成功
    and is_overseas = 1 -- 海外
    --and is_risk_order = 0 -- 不剔除风控
    and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907') -- 剔除合伙人订单
    group by 1
),

--历史营业额指标（商圈排名） 直采只跟直采比  接入和全量房屋比  
-- 2024/4/1将离店口径改成在店口径
ongoing_order as(
select 
    to_date(a.checkin_date_new) checkin_date_new,
    b.house_id,
    sum(a.real_unit_rate*b.booking_count) as gmv,
    sum(b.booking_count) as night,
    count(distinct a.order_no) as ord
from(
    select distinct
        order_no, --不唯一，同一个订单有10晚，则会记10次
        nvl(real_unit_rate,0) real_unit_rate,  --每天实付
        concat(get_json_object(real_day,'$.year'),'-',
            lpad(cast(get_json_object(real_day,'$.month') as string),2,'0'),'-',
            lpad(cast(get_json_object(real_day,'$.day') as string),2,'0')
            ) as checkin_date_new
    from dwd.dwd_order_product_d --全量分区表
    where dt=date_sub('${date}',1)
)a
inner join(--取套数、房屋id、限制订单状态
    select distinct
        order_no, 
        booking_count,
        house_id,
        room_total_amount,
        checkin_date,
        checkout_date
    from dws.dws_order 
    where create_date<=date_sub('${date}',1)
    and is_paysuccess_order = 1 -- 支付成功
    and is_overseas = 1 -- 海外
    --and is_risk_order = 0 -- 不剔除风控
    and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907') -- 剔除合伙人订单
)b on a.order_no=b.order_no
where 
    to_date(a.checkin_date_new) between date_sub('${date}',364) and date_sub('${date}',1)
    and to_date(a.checkin_date_new) between b.checkin_date and date_sub(b.checkout_date,1) --部分退款 也会被记录到dwd_order_product_d，将这部分订单排除掉
group by 1,2
),

--途家(不区分是否直采)：国内为直采
ord_month as ( 
select distinct 
    a.checkin_date_new, --记得改ord_risk中相关的checkout_date
    b.city_name,
    a.house_id,
    b.dynamic_business_id,
    b.dynamic_business,
    a.night,
    cast(a.gmv as DECIMAL(15,8)) gmv
from ongoing_order a
join (select * from h --where landlord_channel=1 海外不区分是否直采
     )b on a.house_id=b.house_id
),

ord_risk as (--房屋城市、商圈gmv排名
select distinct
    t1.*,
    rank() over(partition by city_name order by gmv asc ) rk_city_gmv,-- 相同数字排名相同，1,1,3,4,5
    rank() over(partition by city_name,dynamic_business_id order by gmv asc ) rk_dynamic_gmv
from (
    select distinct
        city_name,
        dynamic_business_id,
        dynamic_business,
        house_id,
        (quarter4_gmv+quarter3_gmv+quarter2_gmv+quarter1_gmv) as gmv
    from
    (
      select
       city_name,
       dynamic_business_id,
       dynamic_business,
       house_id,
       sum(case when rk <=91 and rk >=1 then gmv else 0 end) * 0.5 as quarter4_gmv,
       sum(case when rk<=182 and rk >=92 then gmv else 0 end) * 0.25 as quarter3_gmv,
       sum(case when rk <=273 and rk >=183 then gmv else 0 end) * 0.15 as quarter2_gmv,
       sum(case when rk <=364 and rk >=274 then gmv else 0 end) * 0.1 as quarter1_gmv
      from
        (
        select *,
         dense_rank() over(partition by city_name order by checkin_date_new desc) rk --不会递补日期,写死364天范围内 dense_rank112
        from
          ord_month
        ) a 
        group by 1,2,3,4
    ) b
) t1
),
ord_rank as (
select distinct  
    a.house_id,
    a.city_name,
    a.dynamic_business_id,
    a.gmv,
    nvl(if(round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4) is not null ,round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4),round(a.rk_city_gmv/b.rk_city_gmv,4)) ,0.0) as rk_dynamic_gmv
    -- round(a.rk_city_gmv/b.rk_city_gmv,4) rk_city_gmv, --第四位小数进位没关系
    -- round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4) rk_dynamic_gmv
from ord_risk a
left join (
    select
        city_name,
        max(rk_city_gmv) rk_city_gmv
    from ord_risk
    group by 1
    ) b on a.city_name=b.city_name
left join (
    select
        dynamic_business_id,
        max(rk_dynamic_gmv) rk_dynamic_gmv
    from ord_risk
    group by 1
    ) c on a.dynamic_business_id=c.dynamic_business_id
),

----携程(只取非标酒店)：国内为接入在途家的营业额  接入房屋全量比
--历史营业额指标
--携程表没有chenk_in_date_new直接用check_in_date
create_order_ctrip as(
    select
        h.house_id
        ,count(distinct if(to_date(a.orderdate) between date_sub('${date}',90) and date_sub('${date}',1),a.orderid,null)) as 90d_ord
        ,sum(if(to_date(a.orderdate) between date_sub('${date}',90) and date_sub('${date}',1),a.ciiquantity,0)) as 90d_night
        ,sum(if(to_date(a.orderdate) between date_sub('${date}',90) and date_sub('${date}',1),a.ciireceivable,0)) as 90d_gmv
        ,sum(if(to_date(a.orderdate) between date_sub('${date}',364) and date_sub('${date}',1),a.ciireceivable,0)) as 365d_gmv
    from app_ctrip.edw_htl_order_all_split as a 
        left join (select distinct * from ods_distributionmanager.ctrip_pre_analyze_room) b on a.room = b.room_id --物理房型
        left join (
                    select
                        distinct
                        partner_hotel_id,
                        hotel_id,
                        partner_unit_id,
                        unit_id
                    from ods_houseimport_config.api_unit
                    where merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
                    ) c on b.room_type_id = c.partner_unit_id
    --限定携程酒店c接的房子
        inner join h on c.unit_id = h.house_id and h.landlord_channel = 334 and h.dt = date_sub('${date}',1) 
    where a.d = date_sub('${date}',2) --取T+2分区
        and a.orderstatus IN ('P','S') -- 订单状态 C: 取消 P:处理中(己确认用户和酒店) S:成交(包括全部成交和提前离店) W: 提交未处理
        --and to_date(a.orderdate) between date_sub('${date}',90) and date_sub('${date}',1) --海外用近90天
        and house_type != '标准酒店'
    group by 1
),
ongoing_order_ctrip as (
    --c酒店    
    select
        to_date(a.arrival)                                  as checkin_date_new
        ,h.house_id
        ,sum(a.ciireceivable)                                as gmv
        ,count(distinct a.orderid)                           as ord
        ,sum(a.ciiquantity)                                  as night
    from app_ctrip.edw_htl_order_all_split as a 
        left join (select distinct * from ods_distributionmanager.ctrip_pre_analyze_room) b on a.room = b.room_id --物理房型
        left join (
                    select
                        distinct
                        partner_hotel_id,
                        hotel_id,
                        partner_unit_id,
                        unit_id
                    from ods_houseimport_config.api_unit
                    where merchant_guid = '1da8e4e1-5ab3-4434-b23e-122a5884334f'
                    ) c on b.room_type_id = c.partner_unit_id
    --限定携程酒店c接的房子
        inner join h on c.unit_id = h.house_id and h.landlord_channel = 334 and h.dt = date_sub('${date}',1) 
    where d = date_sub('${date}',2) --取T+2分区
        and a.orderstatus IN ('P','S') -- 订单状态 C: 取消 P:处理中(己确认用户和酒店) S:成交(包括全部成交和提前离店) W: 提交未处理
        and to_date(a.arrival) between date_sub('${date}',364) and date_sub('${date}',1) --使用chenck_in_date
        and house_type != '标准酒店'
    group by 1,2
    )

,ord_month1 as ( --不剔除风控 ord_risk
select distinct 
    a.checkin_date_new,
    b.city_name,
    b.landlord_channel,
    a.house_id,
    b.dynamic_business_id,
    b.dynamic_business,
    cast(a.gmv as DECIMAL(15,8)) gmv
from ongoing_order_ctrip a
join h b on a.house_id=b.house_id
),

ord_risk1 as (--房屋城市、商圈gmv排名
select distinct
    t1.*,
    rank() over(partition by city_name order by gmv asc ) rk_city_gmv,-- 相同数字排名相同，1,1,3,4,5
    rank() over(partition by city_name,dynamic_business_id order by gmv asc ) rk_dynamic_gmv
from (
    select distinct
        city_name,
        dynamic_business_id,
        dynamic_business,
        house_id,
        landlord_channel,
        (quarter4_gmv+quarter3_gmv+quarter2_gmv+quarter1_gmv) as gmv
    from
    (
      select
       city_name,
       dynamic_business_id,
       dynamic_business,
       house_id,
       landlord_channel,
       sum(case when rk <=91 and rk >=1 then gmv else 0 end) * 0.5 as quarter4_gmv,
       sum(case when rk<=182 and rk >=92 then gmv else 0 end) * 0.25 as quarter3_gmv,
       sum(case when rk <=273 and rk >=183 then gmv else 0 end) * 0.15 as quarter2_gmv,
       sum(case when rk <=364 and rk >=274 then gmv else 0 end) * 0.1 as quarter1_gmv
      from
        (
        select *,
         dense_rank() over(partition by city_name order by checkin_date_new desc) rk --不会递补日期,写死364天范围内 dense_rank112
        from
          ord_month1
        ) a 
        group by 1,2,3,4,5
    ) b
) t1
),
ord_rank1 as (
select distinct  
    a.house_id,
    a.city_name,
    a.dynamic_business_id,
    a.gmv,
    nvl(if(round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4) is not null ,round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4),round(a.rk_city_gmv/b.rk_city_gmv,4)) ,0.0) as rk_dynamic_gmv
    --round(a.rk_city_gmv/b.rk_city_gmv,4) rk_city_gmv, --第四位小数进位没关系
    --round(a.rk_dynamic_gmv/c.rk_dynamic_gmv,4) rk_dynamic_gmv
from ord_risk1 a
left join (
    select
        city_name,
        max(rk_city_gmv) rk_city_gmv
    from ord_risk1
    group by 1
    ) b on a.city_name=b.city_name
left join (
    select
        dynamic_business_id,
        max(rk_dynamic_gmv) rk_dynamic_gmv
    from ord_risk1
    group by 1
    ) c on a.dynamic_business_id=c.dynamic_business_id
--where a.landlord_channel!=1 --不区分是否海外直采
),


comment as ( --房屋累计点评数
--直采+集团 房屋维度
select 
      a.house_id,
      nvl(count(distinct Commentid),0) as comment_num
from (select * from h where landlord_channel!=334)  a 
left join (
    select 
        CommentID,
        unitid as house_id
    from ods_tujiacustomer.comment
    where (IsRepeat<>1  or IsRepeat is null )
    and enumDataEntityStatus=0 --数据状态正常
    and enumcommentstatus = 0--评论状态正常
    and detailauditstatus = 2--审核通过
    AND totalscore > 0
    ) c on a.house_id=c.house_id
group by 1

union all 
--C接 酒店维度
select distinct
    h.house_id,
    t1.comment_num
from(
    select 
        a.hotel_id,
        nvl(count(distinct Commentid),0) as comment_num
    from (select * from h where landlord_channel=334) a 
    left join (
        select 
            CommentID,
            unitid as house_id
        from ods_tujiacustomer.comment
        where (IsRepeat<>1  or IsRepeat is null )
        and enumDataEntityStatus=0 --数据状态正常
        and enumcommentstatus = 0--评论状态正常
        and detailauditstatus = 2--审核通过
        AND totalscore > 0
        ) c on a.house_id=c.house_id
    group by 1
    ) t1 
join h on t1.hotel_id=h.hotel_id  --换成房屋维度
),

--信息完整度——非C接
infor as (
select
  a.house_id,
  a.landlord_channel,
  a.total_score as infor_score,
  b.scoreDetail as infor 
from (select 
        house_id,
        landlord_channel,
        total_score
    from pdb_analysis_c.dwd_house_infor_score_oversea_d
    where dt=date_sub(current_date,1)
    )a  
left join (
    select
        house_id,
        collect_list(named_struct(
                    'thirdKey',thirdKey,'indScore',indScore
                    )) as scoreDetail
    from (
        select distinct
            house_id,
            'picNum' as thirdKey, --图片张数
            cast(picNum as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'picQual' as thirdKey, --图片质量
            cast(picQual as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'kitFacility' as thirdKey, --餐厨
            cast(kitFacility as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'childFacility' as thirdKey, --儿童设施
            cast(childFacility as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'landlordServe' as thirdKey, --房东服务
            cast(landlordServe as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'coreFacility' as thirdKey, --核心设施
            cast(coreFacility as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'scenery' as thirdKey, --景观
            cast(scenery as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'livingView' as thirdKey, --居家
            cast(livingView as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'supportFacility' as thirdKey, --配套设施
            cast(supportFacility as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'sanitaryWare' as thirdKey, --卫浴
            cast(sanitaryWare as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'relaxFacility' as thirdKey, --休闲
            cast(relaxFacility as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'surroundings' as thirdKey, --周边
            cast(surroundings as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'unsubscribeRule' as thirdKey, --退订规则
            cast(unsubscribe_rule_score as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'houseChar' as thirdKey, --房屋特色
            cast(houseChar as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'houseName' as thirdKey, --房屋名称
            cast(houseName as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'video' as thirdKey, --视频
            cast(video_score as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

        union  
        select distinct
            house_id,
            'hostRequire' as thirdKey, --对客要求
            cast(guest_request_score as decimal(5,2)) indScore
        from pdb_analysis_c.dwd_house_infor_score_oversea_d
        where dt=date_sub(current_date,1)
        and house_is_online=1

    ) t1 
    group by 1
    
    union 
    --前端不在线房屋不展示具体分值明细
    select distinct
        house_id,
        collect_list(null) scoreDetail
    from pdb_analysis_c.dwd_house_infor_score_oversea_d
    where dt=date_sub(current_date,1)
    and house_is_online=0
    group by 1
) b on a.house_id=b.house_id
),

--信息完整度——C接
infor1 as (--包含处罚分和奖励分
select 
     a.house_id,
     nvl(punish_score,0)+nvl(honest_score,0) punish_score,   --C接处罚分
     -- nvl(reward_score,0) reward_score,                    --C接奖励分
     cast(nvl(reward_score,0)-nvl(psi_limited_time,0) as DECIMAL(15,2)) reward_score,  --C接奖励分：20240103增加逻辑：为了保证数据稳定，去除有限时活动的奖励分
     cast(nvl(c.score,0) as DECIMAL(15, 4) ) infor_score     --C接信息分
from (select * from h where landlord_channel=334) a 
left join(
    --匹配酒店id
    select distinct
            house_id,
            partner_hotel_id 
            from (
                    select distinct unit_id as house_id--途家房屋id
                    ,partner_hotel_id--第三方酒店id（携程母酒店id）
                    ,partner_unit_id
            from ods_houseimport_config.api_unit
            where merchant_guid = "1da8e4e1-5ab3-4434-b23e-122a5884334f" --携程接入
            and unit_id <> 0
            ) a 
        ) b on a.house_id=b.house_id
left join (
    --取酒店id的惩罚分，奖励分和信息分（信息分在psi的基础分当中）
    select
        masterhotelid,
        punish_score,
        honest_score,
        reward_score,
        round(get_json_object (aa, '$.score'),8) score
    from (
    select 
        masterhotelid,
        punish_score,
        honest_score,
        reward_score,
        basic_score_ext
    from app_ctrip.edw_htl_hotel_psi_score --酒店psi基础分数汇总表
    where d = date_sub(current_date(),1)
    ) a  
    lateral view explode(udf.json_split_new(basic_score_ext)) r as aa 
    where get_json_object (aa, '$.name') ='InfoScore'
) c on b.partner_hotel_id = c.masterhotelid
left join(--20240103 增加逻辑：为了保证数据稳定，去除有限时活动的奖励分
    select
        masterhotelid,max(psi_score) psi_limited_time
    from app_ctrip.adm_htl_reward
    where d=date_sub(current_date,1)
        and is_finished_1=1
        and region_type=0
        and name rlike '【限时】'
    group by 1
)d on c.masterhotelid=d.masterhotelid
), 

house_quality_score as (
    select 
        house_id
        ,a.soft_hard
        ,a.house_layout
        ,a.sanitation_facilities
        ,a.picture_quality
        ,nvl(if(sum(b.score*0.60 + c.score*0.133 + d.score*0.133 + e.score*0.133)>=10,10,sum(b.score*0.60 + c.score*0.133 + d.score*0.133 + e.score*0.133)),0) as house_quality_score
    from excel_upload.overseasrm a
    left join excel_upload.overseas_house_quality13 b on a.soft_hard = b.type and b.house_quality = '房屋软硬装'
    left join excel_upload.overseas_house_quality13 c on a.house_layout = c.type and c.house_quality = '空间布局'
    left join excel_upload.overseas_house_quality13 d on a.sanitation_facilities = d.type and d.house_quality = '设施与卫生'
    left join excel_upload.overseas_house_quality13 e on a.picture_quality = e.type and e.house_quality = '图片质量'
    group by 
        house_id
        ,a.soft_hard
        ,a.house_layout
        ,a.sanitation_facilities
        ,a.picture_quality
    ),
style_score as (--一个房屋命中多个风格，多个都计算,但最高分值为5分
    select 
        house_id,
        case when count(house_label)>=4 then 5 
            when count(house_label)>=3 then 4
            when count(house_label)>=2 then 3
            when count(house_label)>=1 then 2
            else 1 end                                      as style_score, --标签数
        collect_list(house_label)                           as style_list
    from 
        (select 
        a.house_id
        ,b.house_label
    from excel_upload.overseasrm a
    LATERAL VIEW explode(split(concat(design_style,',',landscape,',',special_device,',',characteristic_buildings,',',featured_scenes,',',special_services,',',special_experiences,',',special_location,',',landmark,',',infrastructure),",")) b as house_label
    where house_label != '' --合并时会有空字符串
    group by a.house_id
        ,b.house_label
        ) t 
    group by house_id
    ),
reward_score as (select distinct house_id,reward_score,reward from pdb_analysis_c.dwd_house_reward_score_oversea_d
    where dt=date_sub('${date}',1)),

credit_score as (select distinct hotel_id,credit_score,integrity from pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d
    where dt=date_sub('${date}',1))

---处罚分
,house_type_judan_score as (
select distinct a.house_id,
house_type_score,
judan_score
from 
(select distinct house_id,
case when house_type = '青旅' OR
    house_name LIKE '%睡眠报告%' OR
    house_name LIKE '%睡眠舱%' OR
    house_name LIKE '%胶囊%' OR
    house_name LIKE '%男性%' OR
    house_name LIKE '%女性%' OR
    house_name LIKE '%男女%' OR
    house_name LIKE '%混合%' OR
    house_name LIKE '%床位%' OR
    house_name LIKE '%青年%' OR
    house_name LIKE '%宿舍%' OR
    house_name LIKE '%背包客%' then '-1.0'--2025-02-14修改青旅惩罚分降低原-2.0
    when regexp_like(house_name,check_info) = 1 then '-2.0'
    else 0 end as house_type_score
from h 
left join (
    select type 
        ,concat_ws('|',collect_set(brand)) check_info 
    from excel_upload.houses_level_info0312v1	
    group by 1
) h1 
on 1 = 1 

) a
left join (
select distinct house_id,
case when cancel_count_7=1 then '0.0'
when cancel_count_30=2 then '0.0'
when cancel_count_forever >= 3 then '0.0'
else 0
end as judan_score
from
(select house_id,
count(distinct if(create_date between date_sub('${date}',7) and date_sub('${date}',1),house_id,null)) as cancel_count_7, --过去7天拒单
count(distinct if(create_date between date_sub('${date}',30) and date_sub('${date}',1),house_id,null)) as cancel_count_30, --过去30天拒单
count(distinct if(create_date between '2024-04-12' and date_sub('${date}',1),house_id,null)) as cancel_count_forever   --4.12到昨天拒单
from dws.dws_order
where create_date >= '2024-04-12'
AND   is_paysuccess_order = 1    --支付成功
AND   is_overseas = 1            --海外订单
AND   nvl(landlord_source_channel_code,0) NOT IN ('fdlx010901','skmy1907')    --非合伙人订单
and (cancel_remark like '%订单超时未确认，自动取消订单%'
or cancel_remark like '%房东让我取消%'
or cancel_remark like '%非速订拒单并取消订单%')
group by 1) k ) b 
on a.house_id=b.house_id
) 




--计算各基础分+诚信分+奖励分;score 前端展示,score_d计算等级分

,all_score_info as (select distinct  
    a.house_id,
    a.hotel_id,
    a.house_first_active_time,
    a.is_new_house,             --是否30日新房，新房自己的判断逻辑，需要处理
    a.house_name,
    a.house_is_online,
    a.house_city_id,
    a.city_name as house_city_name,
    a.level2_area_name,
    a.dynamic_business,
    a.dynamic_business_id,
    a.bedroom_count,
    a.recommended_guest,
    a.bedcount,
    a.gross_area,
    a.landlord_channel_name,
    a.landlord_channel,
    a.house_type ,
    a.house_class , 
    a.country_name,
    a.city_level,
    a.house_url, 
    a.hotel_name,
    if(a.city_name in ("东京","香港","曼谷","首尔","大阪","吉隆坡","京都","芭堤雅","普吉岛","新加坡","澳门","清迈","济州市"),1,0) as is_13city,
    -- comment_num,                         --旧表字段,新规则未使用
    -- comment_score,                       --旧表字段,新规则未使用
    -- null as comment_num_score4_5,        --旧表字段,新规则未使用
    -- null as comment_num_score3_4,        --旧表字段,新规则未使用
    -- null as comment_num_score0_3,        --旧表字段,新规则未使用
    -- null as picture_count,               --旧表字段,新规则未使用
    -- null as is_cansale,                  --旧表字段,新规则未使用
    -- null as avg_price_30d,               --旧表字段,新规则未使用
    -- null as can_check_in_ratio_30d,      --旧表字段,新规则未使用
    -- null as can_check_in_ratio_7d,       --旧表字段,新规则未使用
--途家经营分
    nvl(
    case
        when a.landlord_channel = 339 and a.house_first_active_time<='2024-06-12' and a.is_new_house = 1  then '1.50' ---6.12以前的房屋在上线时进入新房保护期
        when a.landlord_channel = 339 and a.house_first_active_time > '2024-06-12' and a.is_new_house = 1  then '3.00' --6.12以后的房屋和其他房源保持一致
        when b.rk_dynamic_gmv >= 0.75 and (d.90d_night>=5 or d.90d_gmv>=3000)then '5.00'  --商圈前25%, 且过去90天间夜≥5或营业额≥3000
        when b.rk_dynamic_gmv >= 0.5  and (d.90d_night>=3 or d.90d_gmv>=1800) then '4.00'  --商圈前50%, 且过去90天间夜≥3或营业额≥1800
        when (a.is_new_house = 1 and a.landlord_channel != 339)  or (b.rk_dynamic_gmv >= 0.25 and (d.90d_night>=1 or d.90d_gmv>=600)) then '3.00'  --商圈前75%, 且过去90天间夜≥1或营业额≥600或 新上房90天内
        when b.rk_dynamic_gmv > 0.0 then '2.00'  --商圈后25%  90天无产，365天有产，都是2分
        when b.rk_dynamic_gmv = 0.0 then '1.00'  --365天无产
    end,
    '1.00'  -- 如果上面的CASE表达式结果为NULL，则返回'1.00'
    ) as t_rk_dynamic_gmv_score,  
    nvl(d.90d_gmv,0)   as t_90d_gmv,   --90天t预订成单gmv
    nvl(d.90d_night,0) as t_90d_night, --90天t预订成单间夜
    nvl(d.90d_ord,0)   as t_90d_ord,   --90天t预订成单单量
    nvl(d.365d_gmv,0)   as t_365d_gmv,     --365天t预订成单gmv
--携程经营分     
    -- 测试版本（2025-05-21 jianyangz） 调低途家直采房屋的携程经营默认分为 2 
    if(a.landlord_channel=1,
    '2.00',                                                                               --直采携程分赋为3，6月17 日 修改为 4 分。根据当前直采表现产单计算
        --  20250519 需要修改途家直采房屋的携程经营分默认值
    nvl(case
        when a.landlord_channel = 339 and a.house_first_active_time<='2024-06-12' and a.is_new_house = 1  then '1.50' ---6.12以前的房屋在上线时进入新房保护期
        when a.landlord_channel = 339 and a.house_first_active_time > '2024-06-12' and a.is_new_house = 1  then '3.00' --6.12以后的房屋和其他房源保持一致
        when c.rk_dynamic_gmv >= 0.75 and (e.90d_night>=5 or e.90d_gmv>=3000)then '5.00'  --商圈前25%, 且过去90天间夜≥5或营业额≥3000
        when c.rk_dynamic_gmv >= 0.5  and (e.90d_night>=3 or e.90d_gmv>=1800) then '4.00'  --商圈前50%, 且过去90天间夜≥3或营业额≥1800
        when (a.is_new_house = 1 and a.landlord_channel != 339) or (c.rk_dynamic_gmv >= 0.25 and (e.90d_night>=1 or e.90d_gmv>=600)) then '3.00'  --商圈前75%, 且过去90天间夜≥1或营业额≥600或 新上房90天内
        when c.rk_dynamic_gmv > 0.0 then '2.00'  --商圈后25%  90天无产，365天有产，都是2分
        when c.rk_dynamic_gmv = 0.0 then '1.00'  --365天无产
        end,
        '1.00'  -- 如果上面的CASE表达式结果为NULL，则返回'1.00'
    )) as c_rk_dynamic_gmv_score,     
    nvl(e.90d_gmv,0)   as c_90d_gmv,   --90天c预订成单gmv
    nvl(e.90d_night,0) as c_90d_night, --90天c预订成单间夜
    nvl(e.90d_ord,0)   as c_90d_ord,   --90天c预订成单单量
    nvl(e.365d_gmv,0)   as c_365d_gmv,   --365天c预订成单gmv  
--点评分
    case when comment_score >=4.9 and comment_num>=10 then '5.00'  --n ≥ 4.9，且累计点评数≥10
        when comment_score >=4.7 and comment_num>=7 then '4.00'  --4.7 ≤ n＜ 4.9，且累计点评数≥7
        when is_new_house = 1 or (comment_score >=4.5 and comment_num>=4) then '3.00'  --4.5 ≤ n＜ 4.7，且累计点评数≥4 或 新上房30天内
        when comment_score >=3.5 and comment_num>=1 then '2.00'  --3.5 ≤ n＜ 4.5，且累计点评数≥1
        else  '1.00'    --n ＜ 3.5 或 无评分
        end as comment_score,
--房源信息分
    if(a.landlord_channel!=334,
        case when g.infor_score >55.0 then '5.00'   --n ＞ 55
            when g.infor_score > 50.0 and g.infor_score<=55.0 then '4.00' --50 ＜ n ≤ 55
            when g.infor_score > 40.0 and g.infor_score<=50.0 then '3.00' --40 ＜ n ≤ 50
            when g.infor_score > 25.0 and g.infor_score<=40.0 then '2.00' --25 ＜ n ≤ 40
            when g.infor_score >= 0.0 and g.infor_score<=25.0 then '1.00' --n ≤ 25
            when g.infor_score is null then '1.00' 
        end ,
        case when h.infor_score =5.0 then '5.00' 
            when h.infor_score > 4.8 and h.infor_score<5.0 then '4.00' 
            when h.infor_score > 4.4 and h.infor_score<=4.8 then '3.00' 
            when h.infor_score >= 3.5 and h.infor_score<=4.4 then '2.00' 
            else '1.00' end ) as infor_score
--房屋品质分(分值和权重未添加)
    ,i.house_quality_score
--特色标签分(分值和权重未添加)
    ,j.style_list as style_score_rule                                                                                                                                                    --列表格式
    ,nvl(j.style_score,1) as style_score
--奖励分
    ,k.reward as reward_rule
    ,k.reward_score
--诚信分
    ,integrity as credit_score_rule
    ,l.credit_score


    ,case when b.rk_dynamic_gmv>=0.99 
                    then concat('商圈前1%',";90天间夜:",cast(d.90d_night as int),";90天营业额:",cast(d.90d_gmv as int))  --1-0.741=0.259  变成0.26
        when b.rk_dynamic_gmv<0.25 and b.rk_dynamic_gmv>0.01
                    then concat('商圈后',floor((b.rk_dynamic_gmv) * 100),'%',";90天间夜:",cast(d.90d_night as int),";90天营业额:",cast(d.90d_gmv as int))
        when b.rk_dynamic_gmv>0.0 and b.rk_dynamic_gmv<=0.01 
                    then concat('商圈后1%',";90天间夜:",cast(d.90d_night as int),";90天营业额:",cast(d.90d_gmv as int))
        when b.rk_dynamic_gmv=0.0 then '无营业额'
        else concat('商圈前',CEILING((1-b.rk_dynamic_gmv) * 100),'%',";90天间夜:",cast(d.90d_night as int),";90天营业额:",cast(d.90d_gmv as int)) --向上取整
        end as t_rk_dynamic_gmv_rule


    ,if(a.landlord_channel=1,
    '平台商户',
    case when c.rk_dynamic_gmv>=0.99 
                    then concat('商圈前1%',";90天间夜:",cast(e.90d_night as int),";90天营业额:",cast(e.90d_gmv as int))  --1-0.741=0.259  变成0.26
        when c.rk_dynamic_gmv<0.25 and c.rk_dynamic_gmv>0.01
                    then concat('商圈后',floor((c.rk_dynamic_gmv) * 100),'%',";90天间夜:",cast(e.90d_night as int),";90天营业额:",cast(e.90d_gmv as int))
        when c.rk_dynamic_gmv>0.0 and c.rk_dynamic_gmv<=0.01 
                    then concat('商圈后1%',";90天间夜:",cast(e.90d_night as int),";90天营业额:",cast(e.90d_gmv as int))
        when c.rk_dynamic_gmv=0.0 then '无营业额'
        else concat('商圈前',CEILING((1-c.rk_dynamic_gmv) * 100),'%',";90天间夜:",cast(e.90d_night as int),";90天营业额:",cast(e.90d_gmv as int)) --向上取整
        end) as c_rk_dynamic_gmv_rule
    ,case when comment_score >=0.0 then concat("点评分:",ROUND(FLOOR(comment_score * 10)/10,1),";点评量:",comment_num) --点评分，保留1位小数
        else '无评分' end as comment_score_rule

    ,if(a.landlord_channel!=334,
        ROUND(FLOOR(g.infor_score*1000)/1000,2) ,h.infor_score)  infor_score_rule--信息完整度
    ,concat("soft_hard:",soft_hard,"house_layout:",house_layout,"sanitation_facilities:",sanitation_facilities,"picture_quality:",picture_quality) as house_quality_score_rule --json格式

    --处罚分
    ,nvl(m.house_type_score,0) +nvl(m.judan_score,0)+nvl(n.extra_credit,0) as penalty_score
    ,CONCAT("house_type_score:", IFNULL(house_type_score, 0), "judan_score:", IFNULL(judan_score, 0), "extra_credit:", IFNULL(extra_credit, 0)) AS penalty_score_rule --json格式
    
    --房屋类型分 （只在分核心城市生效）
    ,case when house_type in ('标准酒店') then '-1.00' end as house_type_jiudian_score --2025-02-14修改非十三城酒店类型房屋惩罚分降低
from h a 
left join ord_rank                                  b on a.house_id=b.house_id --途家营业额排名
left join ord_rank1                                 c on a.house_id=c.house_id --携程营业额排名
left join create_order                              d on a.house_id=d.house_id --途家营业额
left join create_order_ctrip                        e on a.house_id=e.house_id --携程营业额
left join comment                                   f on a.house_id=f.house_id --点评数
left join infor                                     g on a.house_id=g.house_id --非c接信息分
left join infor1                                    h on a.house_id=h.house_id --c接信息分
left join house_quality_score                       i on a.house_id=i.house_id --品质分
left join style_score                               j on a.house_id=j.house_id --风格分
left join reward_score                              k on a.house_id=k.house_id --奖励分
left join credit_score                              l on a.hotel_id=l.hotel_id --诚信分
left join house_type_judan_score                    m on a.house_id=m.house_id --青旅+拒单处罚分
left join excel_upload.overseas_credit_oprules13    n on a.house_id=n.house_id --处罚名单减分
)      



,all_score_rank as (select 
    a.house_id
    ,hotel_id
    ,house_first_active_time
    ,is_new_house               --是否30日新房，新房自己的判断逻辑，需要处理
    --a.house_url,              --房屋链接字段没找到
    ,a.house_name
    ,a.house_is_online
    ,house_city_id
    ,a.house_city_name
    ,level2_area_name
    ,dynamic_business
    ,dynamic_business_id
    ,bedroom_count
    ,recommended_guest
    ,bedcount
    ,gross_area
    ,landlord_channel_name
    ,landlord_channel
    ,is_13city
--途家经营分
    ,t_rk_dynamic_gmv_rule
    ,t_90d_gmv
    ,t_90d_night
    ,t_90d_ord
    ,t_365d_gmv
--携程经营分     
    ,c_rk_dynamic_gmv_rule
    ,c_90d_gmv
    ,c_90d_night
    ,c_90d_ord
    ,c_365d_gmv
--点评分
    ,comment_score_rule
--房源信息分
    ,infor_score_rule
--房屋品质分(分值和权重未添加)
    ,house_quality_score_rule --json格式
--特色标签分(分值和权重未添加)
    ,style_score_rule        --列表格式
--奖励分
    ,reward_rule
--诚信分
    ,credit_score_rule
    ,t_rk_dynamic_gmv_score
    ,c_rk_dynamic_gmv_score  
    ,comment_score
    ,infor_score
    ,house_quality_score
    ,style_score

    ,case when city_level = 'S级' and landlord_channel = 1 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.35+ nvl(c_rk_dynamic_gmv_score,0)*0.15 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15)
        when city_level = 'S级' and landlord_channel = 334 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.25+ nvl(c_rk_dynamic_gmv_score,0)*0.25 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15)
        when landlord_channel = 1 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.35+ nvl(c_rk_dynamic_gmv_score,0)*0.15 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15)
        when landlord_channel = 334 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.25+ nvl(c_rk_dynamic_gmv_score,0)*0.25 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15)
    else 
    (nvl(t_rk_dynamic_gmv_score,0)*0.25+ nvl(c_rk_dynamic_gmv_score,0)*0.25 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15)
    end as Base_Score  


    ,case when city_level = 'S级' and landlord_channel = 1 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.35+ nvl(c_rk_dynamic_gmv_score,0)*0.15 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15 + nvl(reward_score,0) + nvl(credit_score,0) + nvl(penalty_score,0))
        when city_level = 'S级' and landlord_channel = 334 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.25+ nvl(c_rk_dynamic_gmv_score,0)*0.25 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15 + nvl(reward_score,0) + nvl(credit_score,0) + nvl(penalty_score,0))
        when landlord_channel = 1 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.35+ nvl(c_rk_dynamic_gmv_score,0)*0.15 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15 + nvl(reward_score,0) + nvl(credit_score,0) + nvl(penalty_score,0) + nvl(house_type_jiudian_score,0))
        when landlord_channel = 334 then 
    (nvl(t_rk_dynamic_gmv_score,0)*0.25+ nvl(c_rk_dynamic_gmv_score,0)*0.25 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15 + nvl(reward_score,0) + nvl(credit_score,0) + nvl(penalty_score,0) + nvl(house_type_jiudian_score,0))
    else 
    (nvl(t_rk_dynamic_gmv_score,0)*0.25+ nvl(c_rk_dynamic_gmv_score,0)*0.25 + nvl(comment_score,0)*0.1 + nvl(infor_score,0)*0.1 + nvl(house_quality_score,0)*0.15 + nvl(style_score,0)*0.15 + nvl(reward_score,0) + nvl(credit_score,0) + nvl(penalty_score,0) + nvl(house_type_jiudian_score,0))
    end as House_Score



    ,reward_score
    ,credit_score
    ,a.penalty_score_rule
    ,a.Penalty_Score
    ,n.house_class  as Blacklist_Level 
    ,o.house_class  as Allowlist_Level
    
    ,a.house_class    -- 0617 新增字段
    ,a.city_level     -- 0617 新增字段
    ,a.country_name   -- 0617 新增字段
    ,a.house_type    -- 0617 新增字段
    ,a.house_url      -- 0617 新增字段
    ,a.hotel_name     -- 0617 新增字段 
    
from all_score_info a 
left join excel_upload.overseas_allowlist_oprules13 o on a.house_id=o.house_id
left join (
select house_id,house_class  from pdb_analysis_else.dws_house_oversea_l0_v2_d where dt = date_sub(current_date,1)
) n on a.house_id=n.house_id
-- where a.house_city_name = '大阪'
)

select distinct 
house_id        ,
hotel_id        ,
house_first_active_time        ,
is_new_house        ,
house_name        ,
house_city_id        ,
house_city_name        ,
level2_area_name        ,
dynamic_business        ,
dynamic_business_id        ,
bedroom_count        ,
recommended_guest        ,
bedcount        ,
gross_area        ,
landlord_channel_name        ,
landlord_channel        ,
is_13city        ,
t_rk_dynamic_gmv_rule        ,
t_90d_gmv        ,
t_90d_night        ,
t_90d_ord        ,
t_365d_gmv        ,
c_rk_dynamic_gmv_rule        ,
c_90d_gmv        ,
c_90d_night        ,
c_90d_ord        ,
c_365d_gmv        ,
comment_score_rule        ,
infor_score_rule        ,
house_quality_score_rule        ,
style_score_rule        ,
reward_rule        ,
credit_score_rule        ,
t_rk_dynamic_gmv_score        ,
c_rk_dynamic_gmv_score        ,
comment_score        ,
infor_score        ,
house_quality_score        ,
style_score        ,
base_score        ,
reward_score        ,
credit_score        ,
penalty_score        ,
house_score        ,
blacklist_level        ,
allowlist_level        ,
case
when house_score >4 and (credit_score>-3.5 or credit_score is null) then 'L4'
when (house_score >3.25 and house_score<=4) and (credit_score>-3.5 or credit_score is null) then 'L3'
when (house_score >2.5 and house_score<=3.25) and (credit_score>-3.5 or credit_score is null) then 'L25'
when (house_score >2 and house_score<=2.5) and (credit_score>-3.5 or credit_score is null) then 'L24'
when (house_score >1.5 and house_score<=2) and (credit_score>-3.5 or credit_score is null) then 'L21'
when  house_score<=1.5 and (credit_score>-3.5 or credit_score is null) then 'L1'--2025-02-14修改L0统一收口到治理侧
when  credit_score <=-3.5 then 'L0' --2025-02-14修改L0统一收口到治理侧
-- when city_level != 'S级'and house_score >2.6 and (credit_score>-2 or credit_score is null) then 'L4'
-- when city_level != 'S级'and (house_score >2.2 and house_score<=2.6) and (credit_score>-2 or credit_score is null) then 'L3'
-- when city_level != 'S级'and (house_score >1.8 and house_score<=2.2) and (credit_score>-2 or credit_score is null) then 'L25'
-- when city_level != 'S级'and (house_score >1.4 and house_score<=1.8) and (credit_score>-2 or credit_score is null) then 'L24'
-- when city_level != 'S级'and (house_score >1.0 and house_score<=1.4) and (credit_score>-2 or credit_score is null) then 'L21'
-- when city_level != 'S级'and (house_score >0 and house_score<=1.0) and (credit_score>-2 or credit_score is null) then 'L1'
-- when city_level != 'S级'and (house_score <=0 or credit_score <=-2) then 'L0' 
end as house_level_jisuan,
if(blacklist_level is null and allowlist_level is null,


case

when house_score >4 and (credit_score>-3.5 or credit_score is null) then 'L4'
when (house_score >3.25 and house_score<=4) and (credit_score>-3.5 or credit_score is null) then 'L3'
when (house_score >2.5 and house_score<=3.25) and (credit_score>-3.5 or credit_score is null) then 'L25'
when (house_score >2 and house_score<=2.5) and (credit_score>-3.5 or credit_score is null) then 'L24'
when (house_score >1.5 and house_score<=2) and (credit_score>-3.5 or credit_score is null) then 'L21'
when  house_score<=1.5 and (credit_score>-3.5 or credit_score is null) then 'L1'--2025-02-14修改L0统一收口到治理侧
when  credit_score <=-3.5  then 'L0' --2025-02-14修改L0统一收口到治理侧-2025-2-26阈值-2改为-3.5
-- when city_level != 'S级'and house_score >2.6 and (credit_score>-2 or credit_score is null) then 'L4'
-- when city_level != 'S级'and (house_score >2.2 and house_score<=2.6) and (credit_score>-2 or credit_score is null) then 'L3'
-- when city_level != 'S级'and (house_score >1.8 and house_score<=2.2) and (credit_score>-2 or credit_score is null) then 'L25'
-- when city_level != 'S级'and (house_score >1.4 and house_score<=1.8) and (credit_score>-2 or credit_score is null) then 'L24'
-- when city_level != 'S级'and (house_score >1.0 and house_score<=1.4) and (credit_score>-2 or credit_score is null) then 'L21'
-- when city_level != 'S级'and (house_score >0 and house_score<=1.0) and (credit_score>-2 or credit_score is null) then 'L1'
-- when city_level != 'S级'and (house_score <=0 or credit_score <=-2) then 'L0' 
end
,nvl(blacklist_level,allowlist_level)
) as house_level,
house_is_online,
penalty_score_rule,
house_class ,   -- 0617 新增字段
city_level,     -- 0617 新增字段
country_name,   -- 0617 新增字段
house_type,     -- 0617 新增字段
house_url,      -- 0617 新增字段
hotel_name,     -- 0617 新增字段 
date_sub('${date}', 1) as dt
from all_score_rank