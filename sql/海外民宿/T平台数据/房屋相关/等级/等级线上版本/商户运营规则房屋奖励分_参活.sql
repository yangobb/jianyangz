with act_house as (
select distinct
    a.house_id,
    a.landlord_channel,
    a.city_name,
    nvl(b.youxiang,0) joinact, --0.2
    -- case when f.hotel_id is not null then '0.20' else '0.00' end as pursuePrice, --20231201结束
    case when f.hotel_id is not null then 1 else 0 end as pursuePrice, --20231201开始：调整成0.05
    case when g.comment_num>=3 then 1 else 0 end as comment_3,
    case when  g.comment_num=2 then 1 else 0 end as comment_2,
    case when  g.comment_num=1 then 1 else 0 end as comment_1 ,
    
    
    nvl(i.picnum ,0) picnum, --0.1
    nvl(i.housechar ,0) houseChar, --0.1
    nvl(c.cancelfirst,0) cancelFirst, --0.05
    nvl(d.cancel_free,0) freeCancel, --0.05
    -- nvl(e.reserve_add_score,'0.00') reserve_add_score --0.1
    -- nvl(pc.price_cut,'0.00') priceCut,  --0.1 -- 20231229结束
    nvl(pf.picFresh_1,0) picFresh_1,  --20231201加入图片新鲜度,在一年内拍摄
    nvl(pf.picFresh_2,0) picFresh_2,  --20231201加入图片新鲜度，在两年内拍摄
    nvl(b.actDiamond,0) actDiamond, --20231229~20240220 优享家加码奖励 0.1
    if(zk.zhekou is not null,1,0) weekRates --周月租优惠
from (
--C接奖励分是单独的，这里没有
    select distinct 
        dt,
        house_id,
        hotel_id,
        landlord_channel,
        house_city_name as city_name
        -- is_fast_booking  --打开自动接单：开始时间：2023-07-03；结束时间：2023-09-14
    from dws.dws_house_d
    where dt = date_sub(current_date(),1)
        and landlord_channel !=334
        and house_is_oversea=1
        and hotel_is_oversea=1
    ) a 
left join (
    select 
      house_id,
      max(case when activity_id = '1000002' then 1 else 0 end) as youxiang, --优享家
      max(case when activity_id = '1000002' and merchantRate<=85 then 1 else 0 end) actDiamond --20231229~20240220：参加优享家且针对钻石用户折扣小于85折
      -- max(case when activity_id = '1000015' then '0.05' else '0.00' end) as lianzhu--连住
    FROM(
       select
          act_unit_id house_id,
          activity_id,
          min(get_json_object(a.json_string,'$.merchantRate')) merchantRate 
       from dwd.dwd_tns_salespromotion_activity_detail_d d
       lateral view explode(udf.json_split(ladder_level_rule)) r as a
       where d.dt = date_sub(current_date,1)
       and activity_id in ('1000002')
         -- ,'1000012','1000015','1000085')
       and audit_status = 2 
       and check_out_date >= d.dt
       and (concat(get_json_object(booking_date,'$.endDate.year'),'-',lpad(cast(get_json_object(booking_date,'$.endDate.month')as string),2,'0'),'-',lpad(cast(get_json_object(booking_date,'$.endDate.day') as string),2,'0')) >= d.dt 
       or booking_date is null)
       group by 1,2
    ) d
    group by 1 

    --  select 
    --     act_unit_id house_id,
    --     max(case when activity_id = '1000002' then '0.20' else '0.00' end) as youxiang --优享家
    --     -- max(case when activity_id = '1000015' then '0.05' else '0.00' end) as lianzhu--连住
    -- FROM dwd.dwd_tns_salespromotion_activity_detail_d d 
    -- where d.dt = date_sub(current_date,1)
    -- and activity_id in ('1000002')
    --     -- ,'1000012','1000015','1000085')
    -- and audit_status = 2 
    -- and check_out_date >= d.dt
    -- and (concat(get_json_object(booking_date,'$.endDate.year'),'-',lpad(cast(get_json_object(booking_date,'$.endDate.month') 
    -- as string),2,'0'),'-',lpad(cast(get_json_object(booking_date,'$.endDate.day') as string),2,'0')) >= d.dt 
    -- or booking_date is null)
    -- group by 1 
    ) b on a.house_id=b.house_id
left join (--是否自动跟价
    select distinct 
        hotel_id
    from ods_crm_competitor.cp_hotel_auto_pursue_price a 
    where auto_pursue_price_authorization=1
) f on a.hotel_id=f.hotel_id
left join ( --近30日新增点评，因为不包含C接，所以都是房屋维度，如果包含C接，C接需要用门店维度
    select 
        unitid as house_id,
        count(distinct CommentID ) comment_num 
    from ods_tujiacustomer.comment
    where to_date(submittime) between date_sub(current_date,30) and date_sub(current_date,1)
    and (IsRepeat<>1 OR IsRepeat IS NULL)
    AND enumDataEntityStatus=0 --数据状态正常
    AND enumCommentStatus=0 --评论状态正常
    AND detailauditstatus = 2 --审核通过
    AND totalscore > 0
    group by 1
) g on a.house_id=g.house_id
left join (--图片数量达标    房屋特色描述  直接在信息分中取这两个分值为满分
select distinct 
    house_id ,
    case when picnum=25.0 then 1 else 0 end as picnum,
    case when housechar=8.0 then 1 else 0 end as housechar
from pdb_analysis_c.dwd_house_infor_score_d 
where dt=date_sub(current_date,1) 
) i on a.house_id=i.house_id

left join (--取消扣首晚加分
    select  distinct t1.house_id,t1.cancelfirst
    from (select distinct   
            unit_id as house_id,
            rate_plan_id,
            case when fineType = 3 then '扣首晚' else '扣整单' end as koukuanzhengce,
            case when fineType = 3 then 1 else 0 end as cancelfirst
        from ods_tns_product.rate_plan_shard
        lateral view json_tuple(cancel_rule,'preDay','cancelable','fineAmount','fineType') rr as preDay,cancelable,fineAmount,fineType
        where
           deleted='0'
           and enum_rate_plan_type=1  --基础价规则
    ) t1  
    join (
        select * from ods_tns_product.product_shard  --这个表中还存在5条房屋并发数据，导致出现了10条数据，所以结果中必须用distinct，否则匹配两次，会出现两行数据
          where  active=1 --一个 product_shard  对应一条rate_plan_shard ，如果product被删除了，那rateplan也没用,需要剔除
          and deleted=0   
           and enum_product_type=1
      ) t2 on t1.rate_plan_id=t2.rate_plan_id
) c on a.house_id=c.house_id
left join (--30分钟免费取消
select distinct 
   hotel_id,
   if( cancelWithoutDuty=1,1,0) as cancel_free
from ods_tns_baseinfo.hotel
lateral view json_tuple(hotel_extended_field,'cancelWithoutDuty') rr as cancelWithoutDuty
) d on a.hotel_id=d.hotel_id
-- left join (--部分城市一口价活动：20231113~20231228；20231201开始全量
--     select
--         house_id,
--         max(case 
--             when weekDayTypes in ('[1,2]','[2,1]') then '0.10'
--             when weekDayTypes = '[1]' or weekDayTypes = '[2]' then '0.05'
--             else '0.00' end) as price_cut
--     from(
--         select 
--             act_unit_id house_id,
--             GET_JSON_OBJECT(ext_field,'$.weekDayTypes') as weekDayTypes --1-周中、2-周末 
--         from dwd.dwd_tns_salespromotion_activity_detail_d a
--         where
--             audit_status = 2 -- 参活状态
--             and a.dt = date_sub(current_date,1)
--             and a.dt between check_in_date and check_out_date --限制当天在可用日期内
--             and activity_id='1000386' --一口价活动
--             and (concat(get_json_object(booking_date,'$.endDate.year'),'-',lpad(cast(get_json_object(booking_date,'$.endDate.month') 
--             as string),2,'0'),'-',lpad(cast(get_json_object(booking_date,'$.endDate.day') as string),2,'0')) >= a.dt 
--             or booking_date is null)
--     )a
--     group by 1
-- )pc on a.house_id=pc.house_id
left join(
    --20231201加入图片新鲜度：房屋核心空间图片在1年以内拍摄占比>=60%加0.15分，在2年拍摄占比>=60%加0.1分
    --2024/4/1 图片上传时间改成拍摄时间
    select 
        house_id,
        case when pic_pp_1year>=0.6 then 1 else 0 end as picFresh_1,
        case when pic_pp_1year<0.6 and pic_pp_2year>=0.6 then 1 else 0 end as picFresh_2
    from(
        select
            house_id,
            count(distinct case when capture_date>date_sub(current_date(),365) then picture_guid else null end )/count(distinct picture_guid) as pic_pp_1year,
            count(distinct case when capture_date>date_sub(current_date(),730) then picture_guid else null end )/count(distinct picture_guid) as pic_pp_2year
        from(
            select 
                a.house_id
                ,nvl(d.capture_date,to_date(a.create_time)) as capture_date --三分之二房屋都没有拍摄时间，此时取上传时间
                ,a.picture_guid
            from ods_tns_house.house_picture a 
            inner join(
                select house_id
                from dws.dws_house_d
                where dt=date_sub(current_date(),1)
                    and landlord_channel!=334
                    and house_is_oversea=1
                    and hotel_is_oversea=1
            )b on a.house_id=b.house_id
            inner join(
                select distinct house_id,picture_guid
                from (
                    SELECT  
                        house_id
                        ,CONCAT('https://pic.tujia.com',get_json_object(ss.col, '$.pictureURL')) as url
                        ,get_json_object(ss.col, '$.pictureGuid') picture_guid
                        ,CASE    WHEN get_json_object(ss.col, '$.enumPictureCategory') ='1' THEN '客厅'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='2' THEN '卧室'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='3' THEN '厨房'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='4' THEN '卫生间'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='5' THEN '阳台'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='6' THEN '书房'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='7' THEN '外景'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='8' THEN '周边'
                                 WHEN get_json_object(ss.col, '$.enumPictureCategory') ='9' THEN '其他' 
                                 ELSE '未知' 
                         END AS picture_type
                    FROM(
                        SELECT  
                            house_id
                            ,split(
                                regexp_replace(
                                    regexp_extract(house_picture,'^\\[(.+)\\]$',1)
                                    ,'\\}\\,\\{'
                                    ,'\\}\\|\\|\\{'
                                )
                                ,'\\|\\|'
                            ) AS str
                        FROM ods_tns_baseinfo.house_info
                    ) pp
                    LATERAL VIEW explode(pp.str) ss AS col
                )a 
                where picture_type IN ('客厅','卧室','厨房','卫生间')
            )c on a.picture_guid=c.picture_guid and a.house_id=c.house_id
            left join(
                select distinct 
                    house_id,
                    get_json_object(json,'$.pictureGuid') picture_guid,
                    from_unixtime(get_json_object(json,'$.captureTime')/1000,'yyy-MM-dd') as capture_date
                from ods_tns_baseinfo.house_info a 
                lateral view explode(udf.json_split_new(house_picture)) t as json
                where house_picture like '%capture%'
                    and get_json_object(json,'$.captureTime') is not null 
                    and get_json_object(json,'$.captureTime') !='0'
            )d on a.picture_guid=d.picture_guid and a.house_id=d.house_id
        )t
        group by 1
    )z
)pf on a.house_id=pf.house_id

-- 周月租优惠
left join(
    select 
            a.act_unit_id as house_id,
            get_json_object(json.json_string,'$.merchantRate') as zhekou--折扣
        from dwd.dwd_tns_salespromotion_activity_detail_d a  
        lateral view explode(udf.json_split(ladder_level_rule)) r as json
        where 
            audit_status = 2 -- 在 线
            and a.dt = date_sub(current_date,1)
            and activity_id IN ('1000343') 
            and cast(get_json_object(json.json_string, '$.roomNights') as int) = 7
            and get_json_object(json.json_string,'$.merchantRate') <= 90

) zk on a.house_id=zk.house_id

-- 保留房国庆开始时间：2023-9-15；结束时间：2023-10-7

-- left join (--保留房限定9月28至10月3号
--     select house_info.house_id,
--         -- instance_count `物理库存`,
--         -- instance_type `库存类型`,
--         -- open_days `开房态天数`,
--         -- reserved_inventory `锁保留房库存`,
--         -- concat(round(reserved_inventory/(instance_count*open_days)*100,2),'%') `锁库存占比`,
--         case when instance_count = 1 and open_days >= 3 and reserved_inventory/(instance_count*open_days) >= 0.5 then '0.10'
--                 when instance_count > 1 and open_days >= 3 and reserved_inventory/(instance_count*open_days) >= 0.3 then '0.10'
--                 else '0.00' end as reserve_add_score
--     from
--     (
--     select distinct house_id,
--                     instance_count,
--                     case when instance_count <= 1 then '单库存' else '多库存' end as instance_type
--     from dws.dws_house_d
--     where dt = date_sub(current_date,1)
--     -- and house_is_oversea = 1
--     and landlord_channel_name='平台商户'
--     --and house_is_online = 1
--     ) house_info
-- join (
--     select house_id
--         ,sum(reserved_inventory) as reserved_inventory
--         ,min(to_date(create_time)) as  min_create_time --第一次更新时间
--     from  ods_crm.reserved_house_inventory  --每小时更新一次
--     where `date` between '20230928' and '20231003'
--     and reserved_inventory>0      --有保留房库存
--     and enum_data_entity_status=0 --去除失效保留房
--     group by house_id
--     ) reserve on reserve.house_id = house_info.house_id
-- left join (
--     select house_id,
--         count(distinct case when inventorycount > 0 then inventorydate end) open_days,
--         sum(instancecount) as instancecount_all,
--         sum(inventorycount) as inventorycounts_all,
--         sum(avaliablecount) avaliablecount_all,
--         sum(unavaliablecount) as unavaliablecount_all
--     from
--         (
--         select unitid as house_id,
--                 inventorydate,--入住日期
--                 if(instancecount < 0, 0, instancecount) as instancecount,
--                 if(inventorycount < 0, 0, inventorycount) as inventorycount,
--                 if(avaliablecount < 0, 0, avaliablecount) as avaliablecount,--可售库存
--                 if(unavaliablecount < 0, 0, unavaliablecount) as unavaliablecount,--占用库存
--                 row_number () over (partition by unitid, inventorydate order by gettime desc) as r --每2H抓一次，取最新
--         from dim_tujiaproduct.unit_inventory_log
--         where if(current_date <'2023-10-04',createdate = current_date,createdate = '2023-10-03')
--         and inventorydate between '2023-09-28' and '2023-10-03'
--         )k
--     where r=1
--     group by 1
--     ) kc on kc.house_id = house_info.house_id
-- ) e on a.house_id=e.house_id
)


select 
    t1.house_id,
    udf.object_to_string(collect_list(map('key',key,'name',name,'is_act',is_act))) act_detail
from (
select distinct
    house_id,
    'joinact' as key,--优享家
    '参与优享家' as name,
    joinact as is_act
from act_house

union  
select distinct
    house_id,
    'pursuePrice' as key,--自动跟价
    '自动跟价' as name,
    pursuePrice as is_act
from act_house

union  
select distinct
    house_id,
    'comment_3' as key,--新增点评数
    '新增点评数_3' as name,
    comment_3 as is_act
from act_house

union  
select distinct
    house_id,
    'comment_2' as key,--新增点评数
    '新增点评数_2' as name,
    comment_2 as is_act
from act_house

union  
select distinct
    house_id,
    'comment_1' as key,--新增点评数
    '新增点评数_1' as name,
    comment_1 as is_act
from act_house

union  
select distinct
    house_id,
    'picnum' as key,--图片数量达标
    '图片数量达标' as name,
    picnum as is_act
from act_house

union  
select distinct
    house_id,
    'houseChar' as key,--房屋特色描述
    '房屋特色描述' as name,
    houseChar as is_act
from act_house

--打开自动接单：开始时间：2023-07-03；结束时间：2023-09-14
-- union  
-- select distinct
--     house_id,
--     'fastBooking' as key,--打开自动接单
--     '打开自动接单' as name,
--     fastBooking as is_act
-- from act_house
-- where date_sub(current_date,1) <'2023-09-15' or date_sub(current_date,1) >= '2023-10-07' --10月8号切换,使用10月7号的的分区


-- 保留房国庆开始时间：2023-9-15；结束时间：2023-10-7
-- union  
-- select distinct
--     house_id,
--     'holidayExclusiveReserveHouse' as key,--保留房
--     '十一专属-保留房' as name,
--     reserve_add_score as is_act
-- from act_house
-- where date_sub(current_date,1) >= '2023-09-15' and date_sub(current_date,1) < '2023-10-07' --9/16至10/7号展示保留房,分区使用9/15至10/6号（含两端）


union  
select distinct
    house_id,
    'cancelFirst' as key,--取消扣首晚
    '取消扣首晚' as name,
    cancelFirst as is_act
from act_house

union  
select distinct
    house_id,
    'freeCancel' as key,--30分钟免费取消
    '30分钟免费取消' as name,
    freeCancel as is_act
from act_house

--部分城市一口价活动：20231113~20231231
-- union  
-- select distinct
--     house_id,
--     'priceCut' as key,--【限时】一口价
--     '【限时】一口价' as name,
--     priceCut as is_act
-- from act_house a

--20231201加入图片新鲜度
union  
select distinct
    house_id,
    'picFresh_1' as key, --图片新鲜度
    '图片新鲜度_1' as name,
    picFresh_1 as is_act
from act_house

union  
select distinct
    house_id,
    'picFresh_2' as key, --图片新鲜度
    '图片新鲜度_2' as name,
    picFresh_2 as is_act
from act_house

--2024/2/20下线,4/1二次上线
union  
select distinct
    house_id,
    'actDiamond' as key,--【限时】优享家加码奖励
    '【限时】优享家加码' as name,
    actDiamond as is_act
from act_house

union  
select distinct
    house_id,
    'weekRates' as key,
    '周月租优惠' as name,
     weekRates as is_act
from act_house

)t1 
group by 1
