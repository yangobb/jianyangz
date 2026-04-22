--诚信分表改造
-- pdb_analysis_c.dwd_landlord_credit_score_new_d
with house_d as (--全量房屋,不限制渠道
select distinct
    house_id,
    hotel_id,
    landlord_channel
from dws.dws_house_d
where dt=date_sub(current_date,1)
and house_is_oversea=1
and hotel_is_oversea=1 
),

/*problem1 as (--原始诚信分判断
select
    hotel_id, 
    count(distinct case when level!='0' and complaintIssue in (6,24) then order_no end) confir_before,
    count(distinct case when level!='0' and complaintIssue in (1,5) then order_no end) confir_after,
    count(distinct case when level!='0' and complaintIssue in (3,4) then order_no end) no_house,

-- 原切单拆分
    count(distinct case when level!='0' and type=2 then violation_id end) s_qie, --引导客人脱离平台沟通
    0 as k_qie,  --配合客人脱离平台沟通
    0 as shichui_qie,  --脱离平台交易
    count(distinct case when level!='0' and type=5 then violation_id end) shua,

    count(distinct case when level=13 then order_no end) s_problem,
    count(distinct case when level=12 then order_no end) a_problem,
    count(distinct case when level=11 then order_no end) b_problem,
    0 as s_zg_problem,
    0 as a_zg_problem,
    0 as b_zg_problem,
    0 as jingying
from(
select distinct 
    a.hotel_id,
    a.violation_id,
    b.level,  --剔除不处罚的case
    b.type,
    get_json_object(b.extend_info, '$.complaintOrder') as order_no,
    get_json_object(extend_info,'$.complaintIssue') complaintIssue
from ods_tns_quality.punishment a 
left join ods_tns_quality.violation b on a.violation_id=b.violation_id
where  
 a.status in('2','3')--剔除撤销处罚的case
and b.status in ('1','2')  --剔除保留处罚但撤销违规的case
-- b.level!='0'  --处罚级别: 0~4 0:不处罚 
-- --剔除撤销处罚的case
-- and a.status!='-1'  --处罚状态:-1 已撤销 0 未知 1 待执行 2执行中 3已结束  --处罚表
-- --剔除保留处罚但撤销违规的case
-- and b.status!='3'  --违规状态: 0:待处理,1:已处理 -1:已撤销  -2保留处罚但不撤销违规 -3保留处罚但撤销违规  --违规表
--筛选违规类型
and (
    get_json_object(extend_info,'$.complaintIssue') in (6,24,1,5,3,4)
    or b.type in (2,5)
    or --限制入住体验治理的case
        (
            b.level in (11,12,13) 
            and get_json_object(b.extend_info, '$.complaintIssue') in (10,9,2,17,11,14,13,19,12)
        )
    )
and to_date(a.create_time) between '2023-06-01' and '2023-07-05'  --处罚时间
and to_date(b.create_time)>='2023-06-01' --违规时间
) t1 
group by 1 
) ,

problem2 as (--新诚信分判断
select
    hotel_id,
    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type in (15) then order_no end) confir_before,
    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type in (13) then order_no end) confir_after,
    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type in (12) then order_no end) no_house,

    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type=16 and scene_id in('58','55')  then violation_id end) s_qie, -- 商户
    count(distinct case when vio_status in('1') and punish_start_time>'2023-09-04' and punish_status in('2','3')  
                and vio_type=17 and scene_id in('64','61') then violation_id end) k_qie, -- 客户
    ------脱离平台交易 
    0 as shichui_qie,

    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type=18 then violation_id end) shua,

    count(distinct case when punish_status in ('2','3') and vio_type=14 then order_no end)  s_problem,  --处罚生效但未整改
    count(distinct case when 
                -- 已撤销的处罚,整改申诉成功
                (punish_status= '-1' and type=2 and status=2   )
                -- 已撤销违规,整改申诉成功
                and (vio_status= '-1' and type=2 and status=2 )
                and vio_type=14 then order_no end ) s_zg_problem,  --s类问题已撤销，且已整改 
    
    count(distinct case when punish_status in ('2','3') and vio_type=19 then order_no end)  a_problem,  --处罚生效但未整改
  
    count(distinct case when 
                -- 已撤销的处罚,整改申诉成功
                (punish_status= '-1' and type=2 and status=2  )   --代表处罚撤销但已整改
                -- 已撤销违规,整改申诉成功
                and (vio_status= '-1' and type=2 and status=2 )
                and vio_type=19 then order_no end ) a_zg_problem,
    count(distinct case when punish_status in ('2','3') and vio_type=20 then order_no end)  b_problem,  --处罚生效但未整改
    count(distinct case when    
                -- 已撤销的处罚,整改申诉成功
                (punish_status= '-1' and type=2 and status=2  ) 
                -- 已撤销违规,整改申诉成功
                and (vio_status= '-1' and type=2 and status=2 )
                and vio_type=20 then order_no end ) b_zg_problem,
    -- 经营异常
    count(distinct case when  vio_status in('1') and punish_status in('2','3') and vio_type in (24,25,27,28) then violation_id end) jingying

from(
select 
    a.hotel_id,
    a.id as violation_id,
    a.order_number order_no,
    a.vio_type, --12 到店 13确认后 15确认前 16切单 18刷单 19一般 20轻微  21价格保障协议  14严重入住体验
    a.vio_status, --0待处理 1确认违规 2不是违规 -1已撤销
    b.punish_status,--1待执行  2执行中 3已结束 -1已撤销
    c.type, --申诉类型：1一般申诉 2整改
    c.status, --1申诉中 2申诉成功 3申诉失败
    a.scene_id, -- rule_id
    b.punish_start_time
from
tujia_ods.fuwu_themis_violation a 
join tujia_ods.fuwu_themis_punishment b on a.id=b.violation_id
left join tujia_ods.fuwu_themis_appeal c on a.id=c.violation_id
where 
a.vio_type in (12,13,15,16,18,19,20,14,24,25,27,28)
--12 到店 13确认后 15确认前 16切单 18刷单 19一般 20轻微  21价格保障协议  14严重入住体验

-- and b.punish_status !='-1' -- 剔除待执行和已撤销的处罚
-- and a.vio_status != '-1' -- 剔除撤销违规
and to_date(b.punish_start_time) between date_sub(current_date(), 365) and date_sub(current_date(), 1)  
and to_date(b.punish_start_time)>='2023-07-06'  --处罚时间
) t1 
group by 1  
),

problem as (
select
    a.hotel_id, 
    nvl(a.confir_before,0)+nvl(b.confir_before,0) confir_before,
    nvl(a.confir_after,0)+nvl(b.confir_after,0) confir_after,
    nvl(a.no_house,0)+nvl(b.no_house,0) no_house,

    -- 切单拆分
    nvl(a.k_qie,0)+nvl(b.k_qie,0) k_qie, -- 客户切单
    nvl(a.s_qie,0)+nvl(b.s_qie,0) s_qie ,-- 商户切单
    nvl(a.shichui_qie,0)+nvl(b.shichui_qie,0) shichui_qie, --实锤切单

    nvl(a.shua,0)+nvl(b.shua,0) shua,

    nvl(a.s_problem,0)+nvl(b.s_problem,0) s_problem,
    nvl(a.a_problem,0)+nvl(b.a_problem,0) a_problem,
    nvl(a.b_problem,0)+nvl(b.b_problem,0) b_problem,

    nvl(a.s_zg_problem,0)+nvl(b.s_zg_problem,0) s_zg_problem,
    nvl(a.a_zg_problem,0)+nvl(b.a_zg_problem,0) a_zg_problem,
    nvl(a.b_zg_problem,0)+nvl(b.b_zg_problem,0) b_zg_problem,
    nvl(a.jingying,0)+nvl(b.jingying,0) abnormal_oprate
from problem1 a 
left join problem2 b on a.hotel_id=b.hotel_id

union --去重 

select
    a.hotel_id, 
    nvl(a.confir_before,0)+nvl(b.confir_before,0) confir_before,
    nvl(a.confir_after,0)+nvl(b.confir_after,0) confir_after,
    nvl(a.no_house,0)+nvl(b.no_house,0) no_house,

    -- 切单拆分
    nvl(a.k_qie,0)+nvl(b.k_qie,0) k_qie, -- 客户切单
    nvl(a.s_qie,0)+nvl(b.s_qie,0) s_qie ,-- 商户切单
    nvl(a.shichui_qie,0)+nvl(b.shichui_qie,0) shichui_qie, --实锤切单

    nvl(a.shua,0)+nvl(b.shua,0) shua,

    nvl(a.s_problem,0)+nvl(b.s_problem,0) s_problem,
    nvl(a.a_problem,0)+nvl(b.a_problem,0) a_problem,
    nvl(a.b_problem,0)+nvl(b.b_problem,0) b_problem,
    nvl(a.s_zg_problem,0)+nvl(b.s_zg_problem,0) s_zg_problem,
    nvl(a.a_zg_problem,0)+nvl(b.a_zg_problem,0) a_zg_problem,
    nvl(a.b_zg_problem,0)+nvl(b.b_zg_problem,0) b_zg_problem,
    nvl(a.jingying,0)+nvl(b.jingying,0) abnormal_oprate
from problem2 a 
left join problem1 b on a.hotel_id=b.hotel_id
),

-- 连续欠款>=7天，欠款金额>50,>1000扣1分，<=1000扣 0.4分
  main as (
  select
    hotel_id,
    dt
  from
    (
      select
        hotel_id,
        dt, --昨天  前天
        datediff(current_date, dt) date_diff, --1  2 
        row_number() over(
          partition by hotel_id
          order by
            dt desc
        ) rk  --1 2  第一条
      from
        (
          select distinct
            hotel_id,
            dt
          from
            pdb_analysis_c.dwd_landlord_balance_debt_d
          where
            dt >= date_sub(current_date, 10)
            AND dt <= date_sub(current_date, 1)
        ) b
    ) a
  where
    date_diff - rk = 0
),
--连续欠款至昨日
debt_date as (
  select
    hotel_id,
    case
      when debt_date_count < 4 then 'warn'
      when debt_date_count = 4 then 'first'
      when debt_date_count > 4 and debt_date_count <= 6 then 'first_period'
      when debt_date_count = 7 then 'second'
      when debt_date_count = 8 then 'third'
      when debt_date_count > 8 and debt_date_count <= 9 then 'third_period'
      when debt_date_count = 10 then 'fourth'
      else 'serious' end as debt_type,
    debt_date_count
  from
    (
      select
        hotel_id,
        count(distinct dt) as debt_date_count
      from
        main
      group by
        hotel_id
    ) a
),

balance  as(
select
  account_no,--唯一
  t.hotel_id,
  wait_bill + cash - credit as balance
from
  (
    select
      acc.account_no,
      cast(xian.balance as decimal(15, 4)) as cash, -- 现金户
      cast(xin.balance as decimal(15, 4)) as credit,-- 信用户
      nvl(jian.balance, 0) as jian, -- 见证宝
      cast(nvl(dai.balance, 0) as decimal(15, 4)) as wait_bill, -- 待结算待入帐
      cert_id as landlord_id,
      sub_cert_id as hotel_id
    from
      ods_ploutos_fas.account acc
    left join ods_ploutos_fas.sub_account xian on xian.account_no = acc.account_no
        and xian.sub_account_type = 1
        and xian.sec_level_account_type = 2
    left join ods_ploutos_fas.sub_account xin on xin.account_no = acc.account_no
        and xin.sub_account_type = 2
        and xin.sec_level_account_type = 0
    left join ods_ploutos_fas.sub_account jian on jian.account_no = acc.account_no
        and jian.sub_account_type = 8
        and jian.sec_level_account_type = 0
    left join ods_ploutos_fas.sub_account dai on dai.account_no = acc.account_no
        and dai.sub_account_type = 1
        and dai.sec_level_account_type = 1
    where
      account_type = 1  --账户类型 1->企业  2->途家内部户  3->个人 待确认 1代表所有商户：包含个人房东、企业商户、直连商户、分销商
  ) t
  left join (
    select
      hotel_id,
      rba --是否途掌柜：0 否，1 是
    from
      ods_tns_baseinfo.hotel
  ) h on t.hotel_id = h.hotel_id
where
  wait_bill + cash - credit < -50
  and t.hotel_id != ''
  and h.rba = 0
  ),

hotel_account as (
select distinct
  debt_date.hotel_id,
  -c.balance balance
from
  debt_date
join balance c on debt_date.hotel_id=c.hotel_id
where
  c.balance<-50
  and debt_date.debt_date_count>=7
),
*/
order_cnt as ( -- 加分项-离店订单
select
    hotel_id,
    order_cnt,
    (ceil((if(order_cnt<0,0,order_cnt) + 1) / 12) - 1) * 0.5 order_score
from(
  select
      distinct hotel_id
      ,count(distinct order_no) as order_cnt
      ,sum(fyh_fh_commission) as gmv_sum
  from 
      dws.dws_order_finance    --支付成功且离店完单的订单
  where 
      create_date > '2024-08-01'
      and is_overseas = 1 
      and order_status = 'CHECKED_OUT'
  group by 1  
) t1 
),
/*
baozhengjin as (-- 加分项-保证金
select 
    hotel_id,
    3 as baozhengjin_score
from excel_upload.baozhengjin
where riqi between date_sub(current_date,365) and date_sub(current_date,1) 
and riqi>='2023-06-01'
group by 1
union 
select 
    hotel_id,
    3 as baozhengjin_score
from tujia_ods.ploutos_panda_merchant_sign
where to_date(create_time)>='2023-06-01'
and get_json_object(content,'$.payAmount')>0
and get_json_object(content,'$.refundAmount')=0
and sign_type=1 -- 签约类型：保证金
group by 1
),
*/
problem as (
  select
    hotel_id,
    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type in (15) then order_no end) confir_before,
    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type in (13) then order_no end) confir_after,
    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type in (12) then order_no end) no_house,

    count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type=16 and scene_id in('289') then violation_id end) s_qie, -- 商户
    count(distinct case when vio_status in('1') and punish_start_time>'2023-09-04' and punish_status in('2','3')  
                and vio_type=17 and scene_id in('295') then violation_id end) k_qie, -- 客户
    --脱离平台交易 
    0 as shichui_qie,

    --count(distinct case when vio_status in('1') and punish_status in('2','3') and vio_type=18 then violation_id end) shua,

    count(distinct case when punish_status in ('2','3') and vio_type=14 then order_no end)  s_problem,  --处罚生效但未整改
    /*count(distinct case when 
                -- 已撤销的处罚,整改申诉成功
                (punish_status= '-1' and type=2 and status=2   )
                -- 已撤销违规,整改申诉成功
                and (vio_status= '-1' and type=2 and status=2 )
                and vio_type=14 then order_no end ) s_zg_problem,  --s类问题已撤销，且已整改 
    */
    count(distinct case when punish_status in ('2','3') and vio_type=19 then order_no end)  a_problem,  --处罚生效但未整改
    /*
    count(distinct case when 
                -- 已撤销的处罚,整改申诉成功
                (punish_status= '-1' and type=2 and status=2  )   --代表处罚撤销但已整改
                -- 已撤销违规,整改申诉成功
                and (vio_status= '-1' and type=2 and status=2 )
                and vio_type=19 then order_no end ) a_zg_problem,*/
    count(distinct case when punish_status in ('2','3') and vio_type=20 then order_no end)  b_problem  --处罚生效但未整改
    /*count(distinct case when    
                -- 已撤销的处罚,整改申诉成功
                (punish_status= '-1' and type=2 and status=2  ) 
                -- 已撤销违规,整改申诉成功
                and (vio_status= '-1' and type=2 and status=2 )
                and vio_type=20 then order_no end ) b_zg_problem,
    -- 经营异常
    count(distinct case when  vio_status in('1') and punish_status in('2','3') and vio_type in (24,25,27,28) then violation_id end) jingying*/

from(
  select 
      a.hotel_id,
      a.id as violation_id,
      a.order_number order_no,
      a.vio_type, --12 到店 13确认后 15确认前 16切单 18刷单 19一般 20轻微  21价格保障协议  14严重入住体验
      a.vio_status, --0待处理 1确认违规 2不是违规 -1已撤销
      b.punish_status,--1待执行  2执行中 3已结束 -1已撤销
      --c.type, --申诉类型：1一般申诉 2整改
      --c.status, --1申诉中 2申诉成功 3申诉失败
      a.scene_id, -- rule_id
      b.punish_start_time
  from
  tujia_ods.fuwu_themis_violation a 
  join tujia_ods.fuwu_themis_punishment b on a.id=b.violation_id
  left join excel_upload.weiguitichu0416 c 
  on a.id = c.id 
  --left join tujia_ods.fuwu_themis_appeal c on a.id=c.violation_id
  where 
  a.vio_type in (12,13,15,16,18,19,20,14,24,25,27,28)
  --12 到店 13确认后 15确认前 16切单 18刷单 19一般 20轻微  21价格保障协议  14严重入住体验

  and b.punish_status in ('2','3') -- 限制处罚执行状态
  and a.vio_status = '1' -- 限制确认违规
  and to_date(b.punish_start_time) between date_sub(current_date(), 365) and date_sub(current_date(), 1)  
  and to_date(b.punish_start_time)>='2023-07-06'  --处罚时间
  and c.id is null 
) t1 
group by 1 
),
result as (
select
    a.hotel_id,
    a.landlord_channel,
    --nvl(abnormal_oprate,0) abnormal_oprate,
    nvl(no_house,0) no_house,
    nvl(confir_after,0) confir_after,
    nvl(confir_before,0) confir_before,
    nvl(s_problem,0) s_problem,
    --nvl(s_zg_problem,0) s_zg_problem,
    nvl(a_problem,0) a_problem,
    --nvl(a_zg_problem,0) a_zg_problem,
    nvl(b_problem,0) b_problem,
    --nvl(b_zg_problem,0) b_zg_problem, 
    --nvl(shua,0) shua,

    -- 切单拆分
    nvl(k_qie,0) k_qie,
    nvl(s_qie,0) s_qie,
    nvl(shichui_qie,0) shichui_qie,

    --nvl(balance,0) balance, --欠款
    --case when balance between 0.0 and 1000.0 then 1 else 0 end as balance_1, --本身就是大于0的数
    --case when balance between 0.0 and 1000.0 then 0.4 else 0 end as balance_1_score,
    --case when balance >1000.0 then 1 else 0 end as balance_2,
    --case when balance >1000.0 then 1 else 0 end as balance_2_score,
    nvl(order_cnt,0) order_cnt,  --订单加分次数
    nvl(order_score,0) order_score --订单加分
    --case when e.hotel_id is not null then 1 else 0 end as baozhengjin, --交保证金
    --nvl(baozhengjin_score,0) as baozhengjin_score --保证金加分
from house_d a 
left join problem b on a.hotel_id=b.hotel_id --缺陷、sab、切单刷单
--left join hotel_account c on a.hotel_id=c.hotel_id --账户余额门店维度
left join order_cnt d  on a.hotel_id=d.hotel_id  --订单加分
--left join baozhengjin e on a.hotel_id=e.hotel_id  --保证金加分
),

total_score as (--关联白名单门店
select
    *,
    if(landlord_channel=1, 
        cast(
            if (  --大于0的要调成0 
                nvl(no_house_score+confir_after_score+confir_before_score+s_problem_score+a_problem_score+b_problem_score
                +k_qie_score+s_qie_score+shichui_qie_score+order_score,0)>0.0,0.0,
                nvl(no_house_score+confir_after_score+confir_before_score+s_problem_score+a_problem_score+b_problem_score
                +k_qie_score+s_qie_score+shichui_qie_score+order_score,0)
                ) AS DECIMAL(5, 2)),
        cast(
            if( --接入房屋没有账户余额
                (no_house_score+confir_after_score+confir_before_score+s_problem_score+
                a_problem_score+b_problem_score+k_qie_score+s_qie_score+shichui_qie_score+order_score)>0.0,
                0.0 ,
                nvl(no_house_score+confir_after_score+confir_before_score+s_problem_score+
                a_problem_score+b_problem_score+k_qie_score+s_qie_score+shichui_qie_score+order_score,0.0)
             ) AS DECIMAL(5, 2))
        )  credit_score --诚信分分值  --保证金和订单是正数，其他是负数
from(
    select distinct
        t1.hotel_id,
        landlord_channel,
        no_house,
        cast(if(t2.no_house_score is not null and t2.no_house_score != '',t2.no_house_score,t1.no_house*-1) as DECIMAL(5, 2)) no_house_score,
        confir_after,
        cast(if(t2.confir_after_score is not null and t2.confir_after_score != '',t2.confir_after_score,t1.confir_after*-1) as DECIMAL(5, 2)) confir_after_score,
        confir_before,
        cast(if(t2.confir_before_score is not null and t2.confir_before_score != '',t2.confir_before_score,t1.confir_before*-0.4 ) as DECIMAL(5, 2)) confir_before_score,
        s_problem,
        cast(if(t2.s_problem_score is not null and t2.s_problem_score != '',t2.s_problem_score,(t1.s_problem*-0.4)) as DECIMAL(5, 2)) s_problem_score,
        a_problem,
        cast(if(t2.a_problem_score is not null and t2.a_problem_score != '',t2.a_problem_score,(t1.a_problem*-0.4)) as DECIMAL(5, 2)) a_problem_score,
        b_problem, 
        cast(if(t2.b_problem_score is not null and t2.b_problem_score != '',t2.b_problem_score,(t1.b_problem*-0.4)) as DECIMAL(5, 2)) b_problem_score,
        /*shua,
        cast(if(t2.shua_score is not null and t2.shua_score != '',t2.shua_score,t1.shua*-0.4) as DECIMAL(5, 2)) shua_score,
        */
        k_qie,
        cast(if(t2.k_qie_score is not null and t2.k_qie_score != '',t2.k_qie_score,t1.k_qie*-0.2) as DECIMAL(5, 2)) k_qie_score,

        s_qie,
        cast(if(t2.s_qie_score is not null and t2.s_qie_score != '',t2.s_qie_score,t1.s_qie*-0.4) as DECIMAL(5, 2)) s_qie_score,
        
        shichui_qie,
        cast(if(t2.shichui_qie_score is not null and t2.shichui_qie_score != '',t2.shichui_qie_score,t1.shichui_qie*-1) as DECIMAL(5, 2)) shichui_qie_score,
        /*
        abnormal_oprate,
        cast(if(t2.abnormal_oprate_score is not null and t2.abnormal_oprate_score != '',t2.abnormal_oprate_score,t1.abnormal_oprate*-2) as DECIMAL(5, 2)) abnormal_oprate_score,


        balance_1,
        cast(if(t2.balance_1_score is not null and t2.balance_1_score != '',t2.balance_1_score,-t1.balance_1_score) as DECIMAL(5, 2)) balance_1_score,
        balance_2,
        cast(if(t2.balance_2_score is not null and t2.balance_2_score != '',t2.balance_2_score,-t1.balance_2_score) as DECIMAL(5, 2)) balance_2_score,*/
        order_cnt, 
        cast(if(t2.order_score is not null and t2.order_score != '',t2.order_score,t1.order_score) as DECIMAL(5, 2)) order_score
        /*baozhengjin,
        cast(if(t2.baozhengjin_score is not null and t2.baozhengjin_score != '',t2.baozhengjin_score,t1.baozhengjin_score) as DECIMAL(5, 2)) baozhengjin_score*/
    from result t1 
    left join excel_upload.house_subindexs_score_whitelist t2 on t1.hotel_id=t2.hotel_id
            and to_date(t2.dt)<=current_date
            and current_date<=to_date(end_date)
) final  

)

select 
    t1.hotel_id,
    t2.landlord_channel,
    t2.credit_score,
    to_json(collect_list(named_struct('key',key,'myScore',myScore,
            'occurTimes',cast(occurTimes as int),'name',name
            )))  integrity
from (
/*select distinct
    hotel_id,
    'abnormal_oprate' as key,--经营异常
    '经营异常' as name,
    abnormal_oprate as occurTimes,
    abnormal_oprate_score as myScore
from total_score

union  
*/
select distinct
    hotel_id,
    'arrival_nohouse' as key,--到店无房
    '到店无房' as name,
    no_house as occurTimes,
    no_house_score as myScore
from total_score

union  
select distinct
    hotel_id,
    'after_confirm_problem' as key,--确认后满房/涨价
    '确认后满房/涨价' as name,
    confir_after as occurTimes,
    confir_after_score as myScore
from total_score

union  
select distinct
    hotel_id,
    'before_confirm_problem' as key,--确认前满房/涨价
    '确认前满房/涨价' as name,
    confir_before as occurTimes,
    confir_before_score as myScore
from total_score

union  
select distinct
    hotel_id,
    's_problem' as key,--严重入住体验问题
    '严重入住体验问题' as name,
    s_problem as occurTimes,
    s_problem_score as myScore
from total_score

union  
select distinct
    hotel_id,
    'a_problem' as key,--一般入住体验问题
    '一般入住体验问题' as name,
    a_problem as occurTimes,
    a_problem_score as myScore
from total_score

union  
select distinct
    hotel_id,
    'b_problem' as key,--轻微入住体验问题
    '轻微入住体验问题' as name,
    b_problem as occurTimes,
    b_problem_score as myScore
from total_score

/*union  
select distinct
    hotel_id,
    'shuadan' as key,--虚假交易/点评
    '虚假交易/点评' as name,
    shua as occurTimes,
    shua_score as myScore
from total_score
*/
-- union  
-- select distinct
--     hotel_id,
--     'k_qiedan' as key,--切单
--     '配合客人脱离平台沟通' as name,
--     k_qie as occurTimes,
--     k_qie_score as myScore
-- from total_score

-- union  
-- select distinct
--     hotel_id,
--     's_qiedan' as key,--切单
--     '引导客人脱离平台沟通' as name,
--     s_qie as occurTimes,
--     s_qie_score as myScore
-- from total_score

-- union  
-- select distinct
--     hotel_id,
--     'shichui_qiedan' as key,--切单
--     '脱离平台交易' as name,
--     shichui_qie as occurTimes,
--     shichui_qie_score as myScore
-- from total_score

/*union  
select distinct
    hotel_id,
    'debt_morethan1000' as key,--欠款>1000
    '欠款>1000' as name,
    balance_2 as occurTimes,
    balance_2_score as myScore
from total_score
where landlord_channel=1

union  
select distinct
    hotel_id,
    'debt_lowerthan1000' as key,--欠款<=1000
    '欠款<=1000' as name,
    balance_1 as occurTimes,
    balance_1_score as myScore
from total_score
where landlord_channel=1
*/
-- union  
-- select distinct
--     hotel_id,
--     'checkout_order' as key,--离店订单加分
--     '离店订单加分' as name,
--     order_cnt as occurTimes,
--     order_score as myScore
-- from total_score
-- 20250526 修改离店订单加分逻辑
union  
select hotel_id
  ,'checkout_order' as key--离店订单加分
  ,'离店订单加分' as name
  ,order_sum occurTimes
  ,(ceil((if(order_sum<0,0,order_sum) + 1) / 12) - 1) * 0.5 myScore
  
from (
    select
        distinct hotel_id,
        count(distinct order_no) as order_sum,
        sum(fyh_fh_commission) as gmv_sum
    from 
        dws.dws_order_finance    --支付成功且离店完单的订单
    where 
        create_date > '2024-08-01'
        and is_overseas = 1 
        and order_status = 'CHECKED_OUT'
    group by 1 
) a 
/*
union  
select distinct
    hotel_id,
    'pay_deposit' as key,--缴纳保证金
    '缴纳保证金' as name,
    baozhengjin as occurTimes,
    baozhengjin_score as myScore
from total_score
where landlord_channel=1*/

union 
select hotel_id 
    ,'peifu_order' key 
    ,'赔付金额' name 
    ,nvl(peifu_pp,0) occurTimes
    ,case when peifu_pp > 1 then '-2'
        when peifu_pp between 0.3 and 1 then '-1.5'
        when peifu_pp between 0.1 and 0.3 then '-1'
        when peifu_pp < 0.1 then '-0.4'
        else 0 end myScore 
from (
    select city_name
        ,landlord_id
        ,hotel_id
        ,hotel_name 
        ,sum(fyh_fh_commission) fyh_fh_commission
        ,sum(payamount) payamount
        
        ,sum(payamount) / sum(fyh_fh_commission) peifu_pp
        ,sum(payamount_tob) payamount_tob
        ,sum(payamount_toc) payamount_toc
    from (
        select 
            order_no order_num
            ,city_name
            ,hotel_id	
            ,hotel_name
            ,house_id
            ,landlord_id
            ,fyh_fh_commission
        from dws.dws_order_finance 
        where checkout_date between date_sub(current_date,365) and date_sub(current_date,1)
        and is_overseas = 1 
        and nvl(fyh_fh_commission,0) > 0 
    ) a 
    left join (
        select a.order_num
            ,sum(payAmount) payamount
            ,sum(case when pay_order_type = 1 then payAmount end) payamount_tob
            ,sum(case when pay_order_type = 2 then payAmount end) payamount_toc
        from (
            select distinct
                order_num,
                payType,--1 现金  2 积分 3 途游卡 4 优惠券
                payStatus,
                payAmount,
                pay_order_type,
                case when pay_order_type=1 then '客户'
                     when pay_order_type=2 then '商户' end `赔付类型`
            from (
                select distinct
                    order_num
                    ,pay_result_info
                    ,explode(split(regexp_replace(regexp_replace(pay_result_info, '\\}\\,\\{','\\}\\;\\{'),'\\[|\\]',''),'\\;')) as json
                    ,pay_order_type
                from ods_tns_callcenter.pay_order
                where 1=1
                and job_status in (6,9,48,49,50,51,53,55)--	52-赔付失败；55-赔付成功；56-已作废；
            ) t
            lateral view json_tuple(json,'payType','payStatus','payAmount') b as payType,payStatus,payAmount
            where payAmount != 0 
        ) a 
        inner join (
            select order_no order_num 
            from dws.dws_order 
            where is_overseas = 1 
            and checkout_date between date_sub(current_date,365) and date_sub(current_date,1)
        ) b 
        on a.order_num = b.order_num
        group by 1
    ) b 
    on a.order_num = b.order_num
    group by 1,2,3,4 
) a 
) t1 
left join total_score t2 on t1.hotel_id=t2.hotel_id
group by 1,2,3