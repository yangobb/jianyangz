with new_old_info as (
SELECT  
    case when channel = 'ctrip' then '携程'
            when channel = 'qunar' then '去哪儿'
            when channel = 'tujia' then '途家'
            when channel = 'elong' then '艺龙'
            end as channel
    ,dt as date_t
    ,uid as uid_1
    ,user_id as user_id_1
    ,is_new
FROM   pdb_analysis_c.app_visit_user_d
WHERE  dt between date_add("${partition}",-14) and date_add("${partition}",-1)
GROUP BY 1,2,3,4,5
)
,mapp as(
    select
        member_id as tujia_user_id,third_id as  ctrip_user_id
    from
        ods_tujia_member.third_user_mapping
    where channel_code = 'CtripId'
)
,htl_ord as (
    select
        tujia_user_id
        ,avg(gmv_k/nights_k) as adr_k
    from
        (
            select
                to_date(orderdate) as orderdate
                ,uid
                ,clientid
                ,orderid
                ,ciireceivable as gmv_k
                ,ciiquantity as nights_k
                ,ciireceivable/ciiquantity as adr_k
            from app_ctrip.edw_htl_order_all_split
            where 
                d = date_sub("${partition}",14)
                and submitfrom='client'
                and to_date(orderdate) between date_sub(date_sub("${partition}",14),365) and date_sub(date_sub("${partition}",14),1)
                and orderstatus in ('S','P')
                and country = 1
                and ordertype = 2 -- 酒店订单
                and uid not in ('_A20190122115701366','_A20151130164107749','_A20190725013107744','E275301478','_A20200710175238972','_A20200211153419761','_A20200921154622724','_A20180814102302643','_A20150928110743155','_A20210226104734937')
                and clientid <> ''
                and clientid is not null
        )ord
    left join
        mapp on  lower(ord.uid) = lower(mapp.ctrip_user_id)
    group by 1
)
,age_info as(
        select distinct
            '携程' as wrapper_name
            ,tujia_user_id
            ,age
        from
            mapp
        left join
            (select
                    d as dt
                    ,uid
                    --,ltrim(regexp_replace(split(regexp_extract(regexp_extract(label, '(1012[^}]+)', 1),'("label_value_text":[^,]+)',1),':')[1],'"','')) as city
                    ,cast(ltrim(regexp_replace(split(regexp_extract(regexp_extract(label, '(1175[^}]+)', 1),'("label_value_text":[^,]+)',1),':')[1],'"','')) as int) as age
                from app_ctrip.edw_bnb_dna_user_label_all
                where d in (select max(d) from app_ctrip.edw_bnb_dna_user_label_all where d>=date_sub("${partition}",14))
            ) t on lower(mapp.ctrip_user_id) = lower(t.uid)
        where age is not null

        union
        
        select distinct
            '去哪儿' as wrapper_name
            ,tujia_user_id
            ,account_age as age
            -- ,account_gender
        from
            (
                select
                    member_id as tujia_user_id,third_id as  quner_user_id
                from
                    ods_tujia_member.third_user_mapping
                where channel_code = 'QunarId'
            )mapp
        left join
            (select
                user_id,
                account_age,
                account_gender
            from
                tujia_share.dw_alita_user_main_tujia
            where 	
                account_age >0 and 	account_age<100
            ) t on lower(mapp.quner_user_id) = lower(t.user_id)
)

-----------------------------------------------------正文

select
    -- case
    --     when wrapper_name = '途家' then 'T途家'
    --     when wrapper_name = '去哪儿' then 'Q去哪儿'
    --     when wrapper_name = '携程' then 'C携程'
    --     when wrapper_name = '艺龙'  then '艺龙'
    --     end as channel
    type
    ,date_sub("${partition}",1) as create_date
    ,`uv周环比7d`      as uv_wk_7d
    ,`pv周环比7d`      as pv_wk_7d
    ,`近7天l2o_uv`     as 7d_l2o_uv
    ,`l2o_uv周环比7d`  as l2o_uv_wk_7d
    ,`近7天gmv/uv`     as 7d_gmv_uv
    ,`gmv/uv周环比7d`  as gmv_uv_wk_7d
    -- ,concat(round(`近7天列表页pv`/7d_total_list_pv*100,2),'%') as 7d_pv_percent  --`近7天pv占比`
    -- ,concat(round(((`近7天列表页pv`/7d_total_list_pv)/(`上周7天列表页pv`/last7d_total_list_pv)-1)*100,2),'%') as pv_percent_wk_7d  --`pv占比周环比7d`
    ,`近7天pv占比` as 7d_pv_percent
    ,`pv占比周环比7d` as pv_percent_wk_7d
    ,`近7天ord占比` as 7d_ord_percent  --`近7天订单占比`
    ,`ord占比周环比7d` as ord_percent_wk_7d  --`订单占比周环比7d`
    ,`近7天nights占比` as 7d_nights_percent  --`近7天间夜占比`
    ,`nights占比周环比7d` as nights_percent_wk_7d --`间夜占比周环比7d`    
    ,`近7天订单量`     as 7d_ord
    ,`单量周环比7d`    as ord_wk_7d
    ,`近7天间夜数`     as 7d_nights
    ,`间夜数周环比7d`  as nights_wk_7d
    ,`近7天列表页uv`   as 7d_uv
    ,`近7天列表页pv`   as 7d_p
    ,`近7天l2d_pv`     as 7d_l2d_pv
    ,`l2d_pv周环比7d`  as l2d_pv_wk_7d
    ,`近7天uv占比`    as 7d_uv_percent
    ,`uv占比周环比7d` as uv_percent_wk_7d
    ,date_sub("${partition}",1) as dt
from
--     total_info aaa
-- join
(
    -----------------------------------------------------------大盘
    select
        -- list.wrapper_name
        case when list.type = '大盘' then '1_大盘' end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_k/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_k/7d_total_list_uv)/(last7d_total_ord_device_k/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_k/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_k/7d_total_list_uv)/(last7d_total_gmv_k/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_k as `近7天订单量`
        ,concat(round((7d_total_ord_k/last7d_total_ord_k-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_k as `近7天间夜数`
        ,concat(round((7d_total_nights_k/last7d_total_nights_k-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_k    as `上周7天订单量`
        ,last7d_total_nights_k as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_k/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_k/7d_sum_ord)/(last7d_total_ord_k/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_k/7d_sum_ord)-(last7d_total_ord_k/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_k/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_k/7d_sum_nights)/(last7d_total_nights_k/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_k/7d_sum_nights)-(last7d_total_nights_k/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                -- wrapper_name
                '大盘' as type
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z
            -----环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv
            
            from  
                (
                    select
                        wrapper_name
                        ,dt
                        ,uid
                        ,user_id
                        ,detail_uid
                        ,without_risk_access_order_num
                        ,without_risk_access_order_room_night
                        ,without_risk_access_order_gmv 
                    from
                        dws.dws_path_ldbo_d
                    where
                        dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                    and user_type = '用户'
                    and is_oversea=1
                    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102'))-- or (wrapper_name = '艺龙' and front_display = 'true'))
                )list
            group by 1
        )list

    left join

        (
            select
                --wrapper_name
                '大盘' as type
        ----近7天
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null)) 7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),room_total_amount,null)) as 7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) 7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null)) 7d_total_ord_k
        ----环比近7天
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_room_night_count,null)) last7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),room_total_amount,null)) as last7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) last7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_no,null)) last7d_total_ord_k

                ,sum(count(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null))) over() as 7d_sum_ord
                ,sum(count(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_no,null))) over() as last7d_sum_ord

                ,sum(sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null))) over() as 7d_sum_nights
                ,sum(sum(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_room_night_count,null))) over() as last7d_sum_nights

            from
                (
                    select
                        *
                        ,case
                            when terminal_type_name = '本站-APP' then '途家'
                            when terminal_type_name = '去哪儿-APP' then '去哪儿'
                            when terminal_type_name = '携程-APP' then '携程'
                            when sell_channel_type='10'  then '蚂蚁'
                            when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
                            else 'other' end as wrapper_name
                        ,case when checkin_date = create_date or checkin_date = date_sub(create_date,1) then 'T+0'
                            else 'T+n' end as date_type
                    from
                        dws.dws_order
                    where
                        -- is_success_order = '1'
                        is_paysuccess_order ='1'
                        --and landlordSourceChannelCode not in ('fdlx010901','skmy1907')------------非拉新
                        --and SELL_CHANNEL_TYPE IN (3,8,12,6,10,6,43)
                        and terminal_type_name in ('本站-APP','去哪儿-APP','携程-APP')--,'艺龙-小程序','艺龙-APP')
                        and	is_overseas=1
                            -- or sell_channel_type='10' )
                        and create_date between date_add("${partition}",-14) and date_add("${partition}",-1)
                )ord
            group by 1
        ) ord on list.type = ord.type 
        -- and list.wrapper_name = ord.wrapper_name 
    -- where list.wrapper_name <> '去哪儿'

    -------------------------------------------------------t0tn
    union all

    select
        -- list.wrapper_name
        case when list.type = 'T+0' then '2_T+0'
            when list.type = 'Tn节假日' then '2_Tn节假日'
            when list.type = 'Tn非节假日' then '2_Tn非节假日'
            end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_k/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_k/7d_total_list_uv)/(last7d_total_ord_device_k/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_k/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_k/7d_total_list_uv)/(last7d_total_gmv_k/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_k as `近7天订单量`
        ,concat(round((7d_total_ord_k/last7d_total_ord_k-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_k as `近7天间夜数`
        ,concat(round((7d_total_nights_k/last7d_total_nights_k-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_k    as `上周7天订单量`
        ,last7d_total_nights_k as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_k/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_k/7d_sum_ord)/(last7d_total_ord_k/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_k/7d_sum_ord)-(last7d_total_ord_k/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_k/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_k/7d_sum_nights)/(last7d_total_nights_k/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_k/7d_sum_nights)-(last7d_total_nights_k/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                --wrapper_name
                date_type as type
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z
            -----环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv
            from  
                (
                    select
                        a.*
                        ,case when checkin_date = dt or checkin_date = date_sub(dt,1) then 'T+0'
                              when b.holiday_dt is not null then 'Tn节假日'
                              else 'Tn非节假日' end as date_type
                    from    
                        (
                            select
                                dt,uid,detail_uid,wrapper_name,house_id,without_risk_access_order_num,without_risk_access_order_gmv ,without_risk_access_order_room_night
                                ,server_log,checkin_date,checkout_date
                            from
                                dws.dws_path_ldbo_d
                            where
                                dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                            and user_type = '用户'
                            and is_oversea=1
                            and ((wrapper_name in ('途家','携程','去哪儿') and source = '102'))  --or (wrapper_name = '艺龙' and front_display = 'true')
                        )a
                    left join
                        (
                            select
                                day_date as holiday_dt
                            from tujia_dim.dim_date_info
                            where day_type = '节假日'
                            and datediff(day_date,"${partition}")<=60
                            and datediff(day_date,"${partition}")>0
                        )b
                    -- on a.checkin_date = b.holiday_dt
                    on a.checkout_date = b.holiday_dt
                )list
            group by 1
        )list

    left join

        (
            select
                -- wrapper_name
                date_type as type
        ----近7天
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null)) 7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),room_total_amount,null)) as 7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) 7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null)) 7d_total_ord_k
        ----环比近7天
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_room_night_count,null)) last7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),room_total_amount,null)) as last7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) last7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_no,null)) last7d_total_ord_k

                ,sum(count(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null))) over() as 7d_sum_ord
                ,sum(count(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_no,null))) over() as last7d_sum_ord

                ,sum(sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null))) over()  as 7d_sum_nights
                ,sum(sum(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_room_night_count,null))) over()  as last7d_sum_nights
            from
                (
                    select
                            a.*
                            ,case
                                when terminal_type_name = '本站-APP' then '途家'
                                when terminal_type_name = '去哪儿-APP' then '去哪儿'
                                when terminal_type_name = '携程-APP' then '携程'
                                when sell_channel_type='10'  then '蚂蚁'
                                when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
                                else 'other' end as wrapper_name
                            ,case when checkin_date = create_date or checkin_date = date_sub(create_date,1) then 'T+0'
                                    when checkin_date = b.holiday_dt is not null then 'Tn节假日'
                                    else 'Tn非节假日' end as date_type
                    from    
                        (
                            select
                                city_id,city_name,create_date,house_id,uid,order_no,room_total_amount,order_room_night_count,dynamic_business
                                ,terminal_type_name,checkin_date,sell_channel_type
                            from
                                dws.dws_order
                            where
                                -- is_success_order = '1'
                                is_paysuccess_order ='1'
                                --and landlordSourceChannelCode not in ('fdlx010901','skmy1907')------------非拉新
                                --and SELL_CHANNEL_TYPE IN (3,8,12,6,10,6,43)
                                and terminal_type_name in ('本站-APP','去哪儿-APP','携程-APP')--,'艺龙-小程序','艺龙-APP')
                                    -- or sell_channel_type='10' )
                                and	is_overseas=1
                                and create_date between date_add("${partition}",-14) and date_add("${partition}",-1)
                        )a
                    left join
                        (
                            select distinct
                                day_date as holiday_dt
                            from tujia_dim.dim_date_info
                            where day_type = '节假日'
                            and datediff(day_date,"${partition}")<=60
                            and datediff(day_date,"${partition}")>0
                        )b on a.checkin_date = b.holiday_dt
                )ord
            group by 1
        ) ord on  list.type = ord.type
        -- and list.wrapper_name = ord.wrapper_name 
    -- where list.wrapper_name <> '去哪儿'

    --------------------------------------------搜索场景
    union all

    select
        -- list.wrapper_name
        case when list.type = '城市空搜' then '3_城市空搜'
            when list.type = '行政区' then '3_行政区'
            when list.type = '地标' then '3_地标'
            when list.type = '身边' then '3_身边'
            when list.type = '文本直搜' then '3_文本直搜1'
            when list.type = '房屋搜索' then '3_房屋搜索7'
            end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_z/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_z/7d_total_list_uv)/(last7d_total_ord_device_z/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_z/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_z/7d_total_list_uv)/(last7d_total_gmv_z/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_z as `近7天订单量`
        ,concat(round((7d_total_ord_z/last7d_total_ord_z-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_z as `近7天间夜数`
        ,concat(round((7d_total_nights_z/last7d_total_nights_z-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_z    as `上周7天订单量`
        ,last7d_total_nights_z as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_z/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_z/7d_sum_ord)/(last7d_total_ord_z/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_z/7d_sum_ord)-(last7d_total_ord_z/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_z/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_z/7d_sum_nights)/(last7d_total_nights_z/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_z/7d_sum_nights)-(last7d_total_nights_z/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                -- wrapper_name
                search_type as type
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and without_risk_access_order_num>0,uid,null)) as 7d_total_ord_device_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z
            ------环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and without_risk_access_order_num>0,uid,null)) as last7d_total_ord_device_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over()  as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv

                ,sum(sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null))) over()  as 7d_sum_ord
                ,sum(sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null))) over() as last7d_sum_ord

                ,sum(sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null))) over() as 7d_sum_nights
                ,sum(sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null))) over() as last7d_sum_nights
            from  
                (
                    select
                        wrapper_name
                        ,dt
                        ,uid
                        ,detail_uid
                        ,without_risk_access_order_num
                        ,without_risk_access_order_room_night
                        ,without_risk_access_order_gmv 
                        -- ,case 
                        --     when get_json_object(server_log,'$.searchScene') = 1 then '文本'
                        --     when get_json_object(server_log,'$.searchScene') = 2 then '城市空搜'
                        --     when get_json_object(server_log,'$.searchScene') = 3 then '景区地区'
                        --     when get_json_object(server_log,'$.searchScene') = 4 then '行政区'
                        --     when get_json_object(server_log,'$.searchScene') = 5 then '地标'
                        --     when get_json_object(server_log,'$.searchScene') = 6 then '身边' 
                        --     when get_json_object(server_log,'$.searchScene') = 0 then '无' 
                        --     end as search_type
                        ,case 
                            when get_json_object(server_log,'$.searchScene') = 1 then '文本直搜'
                            when get_json_object(server_log,'$.searchScene') = 2 then '城市空搜'
                            when get_json_object(server_log,'$.searchScene') in ('4','8') then '行政区'
                            when get_json_object(server_log,'$.searchScene') = 5 then '地标'
                            when get_json_object(server_log,'$.searchScene') = 6 then '身边'
                            when get_json_object(server_log,'$.searchScene') = 7 then '房屋搜索'
                            when get_json_object(server_log,'$.searchScene') = 0 then '无' 
                            end as search_type
                        ,case when checkin_date = dt or checkin_date = date_sub(dt,1) then 'T+0'
                            else 'T+n' end as date_type
                    from
                        dws.dws_path_ldbo_d
                    where
                        dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                    and user_type = '用户'
                    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102') )--or (wrapper_name = '艺龙' and front_display = 'true'))
                    and is_oversea=1
                    and get_json_object(server_log,'$.searchScene') in (1,2,3,4,5,6,7,8,9)
                )list
            group by 1
        )list
    -- where list.wrapper_name <> '去哪儿'

    -------------------------------------------------新老客

    union all

    select
        -- list.wrapper_name
        case when list.type = '老客' then '4_老客'
            when list.type = '新客' then '4_新客'
            end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_k/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_k/7d_total_list_uv)/(last7d_total_ord_device_k/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_k/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_k/7d_total_list_uv)/(last7d_total_gmv_k/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_k as `近7天订单量`
        ,concat(round((7d_total_ord_k/last7d_total_ord_k-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_k as `近7天间夜数`
        ,concat(round((7d_total_nights_k/last7d_total_nights_k-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_k    as `上周7天订单量`
        ,last7d_total_nights_k as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_k/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_k/7d_sum_ord)/(last7d_total_ord_k/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_k/7d_sum_ord)-(last7d_total_ord_k/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_k/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_k/7d_sum_nights)/(last7d_total_nights_k/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_k/7d_sum_nights)-(last7d_total_nights_k/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                -- wrapper_name
                nvl(is_new,"新客") as type
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z

            -----环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over()  as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv
            from  
                (
                    select
                        wrapper_name
                        ,dt
                        ,uid
                        ,detail_uid
                        ,without_risk_access_order_num
                        ,without_risk_access_order_room_night
                        ,without_risk_access_order_gmv 
                    from
                        dws.dws_path_ldbo_d
                    where
                        dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                    and user_type = '用户'
                    and is_oversea=1
                    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102') )--or (wrapper_name = '艺龙' and front_display = 'true'))
                )list
            left join
                new_old_info on lower(list.uid) = lower(new_old_info.uid_1) and list.dt = new_old_info.date_t and list.wrapper_name = new_old_info.channel 
            group by 1
        )list

    left join

        (
            select
                -- wrapper_name
                nvl(is_new,"新客") as type
        ----近7天
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null)) 7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),room_total_amount,null)) as 7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) 7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null)) 7d_total_ord_k
        ----环比近7天
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_room_night_count,null)) last7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),room_total_amount,null)) as last7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) last7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_no,null)) last7d_total_ord_k

                ,sum(count(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null))) over() as 7d_sum_ord
                ,sum(count(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_no,null))) over() as last7d_sum_ord

                ,sum(sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null))) over() as 7d_sum_nights
                ,sum(sum(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_room_night_count,null))) over()  as last7d_sum_nights
            from
                (
                    select
                        *
                        ,case
                            when terminal_type_name = '本站-APP' then '途家'
                            when terminal_type_name = '去哪儿-APP' then '去哪儿'
                            when terminal_type_name = '携程-APP' then '携程'
                            when sell_channel_type='10'  then '蚂蚁'
                            when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
                            else 'other' end as wrapper_name
                        ,case when checkin_date = create_date or checkin_date = date_sub(create_date,1) then 'T+0'
                            else 'T+n' end as date_type
                    from
                        dws.dws_order
                    where
                        -- is_success_order = '1'
                        is_paysuccess_order ='1'
                        --and landlordSourceChannelCode not in ('fdlx010901','skmy1907')------------非拉新
                        --and SELL_CHANNEL_TYPE IN (3,8,12,6,10,6,43)
                        and terminal_type_name in ('本站-APP','去哪儿-APP','携程-APP')--,'艺龙-小程序','艺龙-APP')
                            -- or sell_channel_type='10' )
                        and	is_overseas=1
                        and create_date between date_add("${partition}",-14) and date_add("${partition}",-1)
                )ord
            left join
                new_old_info on lower(ord.uid) = lower(new_old_info.uid_1) and ord.create_date = new_old_info.date_t and ord.wrapper_name = new_old_info.channel 
            group by 1
        ) ord on list.type = ord.type
        -- and list.wrapper_name = ord.wrapper_name 
    -- where list.wrapper_name <> '去哪儿'
    
    --------------------------------------------流量来源
    union all

    select
        -- list.wrapper_name
        case when list.type = '直访'       then '5_直访'
            when list.type = '全站搜索'     then '5_全站搜索'
            when list.type = '民宿tab'      then '5_民宿tab'
            when list.type = '抢票浏览任务' then '5_抢票浏览任务'
            when list.type = '其他'         then '5_其他'
            end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_z/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_z/7d_total_list_uv)/(last7d_total_ord_device_z/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_z/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_z/7d_total_list_uv)/(last7d_total_gmv_z/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_z as `近7天订单量`
        ,concat(round((7d_total_ord_z/last7d_total_ord_z-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_z as `近7天间夜数`
        ,concat(round((7d_total_nights_z/last7d_total_nights_z-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_z    as `上周7天订单量`
        ,last7d_total_nights_z as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_z/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_z/7d_sum_ord)/(last7d_total_ord_z/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_z/7d_sum_ord)-(last7d_total_ord_z/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_z/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_z/7d_sum_nights)/(last7d_total_nights_z/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_z/7d_sum_nights)-(last7d_total_nights_z/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                -- wrapper_name
                case when (wrapper_name = '携程' and log in ('直访','全站搜索','民宿tab','抢票浏览任务'))
                        or (wrapper_name = '去哪儿' and log in ('直访','全站搜索','民宿tab')) then log
                      else '其他'
                      end as type
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and without_risk_access_order_num>0,uid,null)) as 7d_total_ord_device_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z
            ------环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and without_risk_access_order_num>0,uid,null)) as last7d_total_ord_device_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over()  as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv

                ,sum(sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null))) over() as 7d_sum_ord
                ,sum(sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null))) over() as last7d_sum_ord

                ,sum(sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null))) over() as 7d_sum_nights
                ,sum(sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null))) over() as last7d_sum_nights
            from  
                (
                    select
                        wrapper_name
                        ,dt
                        ,uid
                        ,detail_uid
                        ,without_risk_access_order_num
                        ,without_risk_access_order_room_night
                        ,without_risk_access_order_gmv 
                        ,case
                            when fromforlog in ('0', '2') and wrapper_name = '携程' then '直访' -- 携 程 直 访
                            when fromforlog in('300','310') and wrapper_name = '携程' then '民宿tab'
                            when fromforlog = '13' and wrapper_name = '携程' then '蜂鸟入口' -- 无 对 应
                            when fromforlog = '288' and wrapper_name = '携程' then '签到任务' -- 无 对 应
                            when fromforlog = '6' and wrapper_name = '携程' then '攻略'
                            -- when (fromforlog in('60', '64') or (length(fromforlog) = 6 and fromforlog like '6000__') )
                            -- and wrapper_name = '携程' then '全站搜索' -- '64'-'搜索单品跳列表'并入
                            when (fromforlog in (60,64) or fromforlog like '6000__') and wrapper_name = '携程' then '全站搜索'
                            when fromforlog in('53') and wrapper_name = '携程' then '全部订单'
                            when fromforlog in('120') and wrapper_name = '携程' then '浏览历史'
                            when fromforlog in('130') and wrapper_name = '携程' then '我的收藏'

                            --0823加
                            when fromforlog = '10' and wrapper_name = '携程' then 'IM跳详情页'
                            --when fromforlog = '65' and wrapper_name = '携程' then '大搜-其他'
                            when fromforlog = '501' and wrapper_name = '携程' then '我携-浏览历史(新)'

                            when fromforlog = '603' and wrapper_name = '携程' then '火车个人中心'
                            when fromforlog = '604' and wrapper_name = '携程' then '火车票订单详情页icon'
                            when fromforlog = '610' and wrapper_name = '携程' then '火车完成页'
                            when fromforlog = '612' and wrapper_name = '携程' then '抢票浏览任务'
                            when fromforlog = '613' and wrapper_name = '携程' then '买票优惠先享任务'
                            
                            when fromforlog = '902' and wrapper_name = '携程' then '携程外部投放广告引流到c宫格'
                            when fromforlog = '910' and wrapper_name = '携程' then '浏览未预订运营'
                            when fromforlog = '920' and wrapper_name = '携程' then '周末游运营'
                            when fromforlog = '930' and wrapper_name = '携程' then '周中出行运营'
                            when fromforlog = '940' and wrapper_name = '携程' then '高峰运营'
                            when fromforlog = '950' and wrapper_name = '携程' then '携程订单通知push与站内信'
                            when fromforlog = '960' and wrapper_name = '携程' then 'IM卡片跳转点评'
                            when fromforlog = '970' and wrapper_name = '携程' then '途家'
                            when fromforlog = '980' and wrapper_name = '携程' then '用户运营常规召回'
                            when fromforlog = '987' and wrapper_name = '携程' then '携程SEM外投L页链接'
                            when fromforlog = '990' and wrapper_name = '携程' then '携程攻略民宿卡片'
                            when fromforlog = '999' and wrapper_name = '携程' then '首页二屏推荐民宿'
                            when fromforlog = '1300' and wrapper_name = '携程' then '首页订单卡片-跳转点评'
                            when fromforlog = '1301' and wrapper_name = '携程' then '订单详情跳转点评'

                            
                            when fromforlog in('0', '2', '60') and wrapper_name = '去哪儿' then '直访' -- 去 哪 儿 直 访
                            when fromforlog in('8519', '7102') and wrapper_name = '去哪儿' then '民宿tab'
                            -- when fromforlog in('359', '9204', '9205', '9206', '9207') and wrapper_name = '去哪儿'  then '全站搜索'
                            when fromforlog in (359, 9204, 9205, 9206, 9207, 920502, 920504) then '全站搜索'
                            when fromforlog in('5338') and wrapper_name = '去哪儿' then '全部订单'
                            when fromforlog in('6486') and wrapper_name = '去哪儿' then '我的收藏'
                            when fromforlog in('6485') and wrapper_name = '去哪儿' then '我的足迹'
                            when fromforlog in('6474') and wrapper_name = '去哪儿' then '攻略'
                            else '其他' end as log
                    from
                        dws.dws_path_ldbo_d
                    where
                        dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                    and user_type = '用户'
                    and is_oversea=1
                    and ((wrapper_name in ('途家','携程','去哪儿') and source = '102')) -- or (wrapper_name = '艺龙' and front_display = 'true'))
                )list
            group by 1
        )list


    -----------------------------------------消费能力

    union all


    select
        -- list.wrapper_name
        case -- when list.type = 0 then '5_消费能力0'
            when list.type = 1 then '6_消费能力1'
            when list.type = 2 then '6_消费能力2'
            when list.type = 3 then '6_消费能力3'
            when list.type = '其他' then '6_消费能力其他'
            end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_k/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_k/7d_total_list_uv)/(last7d_total_ord_device_k/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_k/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_k/7d_total_list_uv)/(last7d_total_gmv_k/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_k as `近7天订单量`
        ,concat(round((7d_total_ord_k/last7d_total_ord_k-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_k as `近7天间夜数`
        ,concat(round((7d_total_nights_k/last7d_total_nights_k-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_k    as `上周7天订单量`
        ,last7d_total_nights_k as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_k/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_k/7d_sum_ord)/(last7d_total_ord_k/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_k/7d_sum_ord)-(last7d_total_ord_k/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_k/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_k/7d_sum_nights)/(last7d_total_nights_k/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_k/7d_sum_nights)-(last7d_total_nights_k/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                -- wrapper_name
                case when b.adr_k>=0 and b.adr_k<=250 then 1
                      when b.adr_k>250 and b.adr_k<=400 then 2
                      when b.adr_k>400 then 3
                      else '其他' end as type
                -- ,case when `消费能力`=0 then 0
                --     when `消费能力`=1 then 1
                --     when `消费能力`=2 then 2
                --     when `消费能力`=3 then 3
                --     else '其他' end as type
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z
            -----环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                -- ,count(distinct house_id, trace_id) as total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv
            from  
                (
                    select
                        wrapper_name
                        ,dt
                        ,uid
                        ,user_id
                        ,detail_uid
                        ,without_risk_access_order_num
                        ,without_risk_access_order_room_night
                        ,without_risk_access_order_gmv 
                    from
                        dws.dws_path_ldbo_d
                    where
                        dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                    and user_type = '用户'
                    and is_oversea=1
                    and ((wrapper_name in ('携程') and source = '102'))
                )a
            left join
                htl_ord b on lower(a.user_id) = lower(b.tujia_user_id)
            group by 1
        )list

    left join

        (
            select
                -- wrapper_name
                case when b.adr_k>=0 and b.adr_k<=250 then 1
                      when b.adr_k>250 and b.adr_k<=400 then 2
                      when b.adr_k>400 then 3
                      else '其他' end as type
                -- ,case when `消费能力`=0 then 0
                --     when `消费能力`=1 then 1
                --     when `消费能力`=2 then 2
                --     when `消费能力`=3 then 3
                --     else '其他' end as type
        ----近7天
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null)) 7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),room_total_amount,null)) as 7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) 7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null)) 7d_total_ord_k
        ----环比近7天
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_room_night_count,null)) last7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),room_total_amount,null)) as last7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) last7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_no,null)) last7d_total_ord_k

                ,sum(count(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null))) over() as 7d_sum_ord
                ,sum(count(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_no,null))) over() as last7d_sum_ord

                ,sum(sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null))) over() as 7d_sum_nights
                ,sum(sum(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_room_night_count,null))) over() as last7d_sum_nights
            from
                (
                    select
                        *
                        ,case
                            when terminal_type_name = '本站-APP' then '途家'
                            when terminal_type_name = '去哪儿-APP' then '去哪儿'
                            when terminal_type_name = '携程-APP' then '携程'
                            when sell_channel_type='10'  then '蚂蚁'
                            when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
                            else 'other' end as wrapper_name
                        ,case when checkin_date = create_date or checkin_date = date_sub(create_date,1) then 'T+0'
                            else 'T+n' end as date_type
                    from
                        dws.dws_order
                    where
                        -- is_success_order = '1'
                        is_paysuccess_order ='1'
                        --and landlordSourceChannelCode not in ('fdlx010901','skmy1907')------------非拉新
                        --and SELL_CHANNEL_TYPE IN (3,8,12,6,10,6,43)
                        and (terminal_type_name in ('携程-APP'))
                        and	is_overseas=1
                        and create_date between date_add("${partition}",-14) and date_add("${partition}",-1)
                )a
            left join
                htl_ord b on lower(a.user_id) = lower(b.tujia_user_id)
            group by 1
        ) ord on list.type = ord.type
    --    and list.wrapper_name = ord.wrapper_name 
    

    -----------------------------------------用户年龄
    union all
    select
        -- list.wrapper_name
        -- case
        --     when list.wrapper_name='携程' and list.age = '0-22' then '7_0-22岁'
        --     when list.wrapper_name='携程' and list.age = '23-28' then '7_23-28岁'
        --     when list.wrapper_name='携程' and list.age = '29-35' then '7_29-35岁'
        --     when list.wrapper_name='携程' and list.age = '36-49' then '7_36-49岁'
        --     when list.wrapper_name='携程' and list.age = '50+' then '7_50岁+'
        --     when list.wrapper_name='携程' and list.age = '其他' then '7_其他年龄'

        --     when list.wrapper_name='去哪儿' and list.age = '0-22'  then '6_0-22岁'
        --     when list.wrapper_name='去哪儿' and list.age = '23-28' then '6_23-28岁'
        --     when list.wrapper_name='去哪儿' and list.age = '29-35' then '6_29-35岁'
        --     when list.wrapper_name='去哪儿' and list.age = '36-49' then '6_36-49岁'
        --     when list.wrapper_name='去哪儿' and list.age = '50+'   then '6_50岁+'
        --     when list.wrapper_name='去哪儿' and list.age = '其他'  then '6_其他年龄'
        --     end as type
        case
            when list.age = '0-22' then '7_0-22岁'
            when list.age = '23-28' then '7_23-28岁'
            when list.age = '29-35' then '7_29-35岁'
            when list.age = '36-49' then '7_36-49岁'
            when list.age = '50+' then '7_50岁+'
            when list.age = '其他' then '7_其他年龄'
            end as type
        ,7d_total_list_uv as `近7天列表页uv`
        ,concat(round((7d_total_list_uv/last7d_total_list_uv-1)*100,2),'%') as `uv周环比7d`
        ,7d_total_list_pv as `近7天列表页pv`
        ,concat(round((7d_total_list_pv/last7d_total_list_pv-1)*100,2),'%') as `pv周环比7d`
        ,concat(round(7d_total_ord_device_k/7d_total_list_uv*100,2),'%') as `近7天l2o_uv`
        ,concat(round(((7d_total_ord_device_k/7d_total_list_uv)/(last7d_total_ord_device_k/last7d_total_list_uv)-1)*100,2),'%') as `l2o_uv周环比7d`
        ,round(7d_total_gmv_k/7d_total_list_uv,2) as `近7天gmv/uv`
        ,concat(round(((7d_total_gmv_k/7d_total_list_uv)/(last7d_total_gmv_k/last7d_total_list_uv)-1)*100,2),'%') as `gmv/uv周环比7d`
        ,7d_total_ord_k as `近7天订单量`
        ,concat(round((7d_total_ord_k/last7d_total_ord_k-1)*100,2),'%') as `单量周环比7d`
        ,7d_total_nights_k as `近7天间夜数`
        ,concat(round((7d_total_nights_k/last7d_total_nights_k-1)*100,2),'%') as `间夜数周环比7d`

        ,last7d_total_list_pv  as `上周7天列表页pv`
        ,last7d_total_ord_k    as `上周7天订单量`
        ,last7d_total_nights_k as `上周7天间夜数`

        ,concat(round(7d_total_detail_pv/7d_total_list_pv*100,2),'%') as `近7天l2d_pv`
        ,concat(round(((7d_total_detail_pv/7d_total_list_pv)/(last7d_total_detail_pv/last7d_total_list_pv)-1)*100,2),'%') as `l2d_pv周环比7d`

        ,concat(round(7d_total_list_uv/7d_sum_uv*100,2),'%') as `近7天uv占比`
        -- ,concat(round(((7d_total_list_uv/7d_sum_uv)/(last7d_total_list_uv/last7d_sum_uv)-1)*100,2),'%') as `uv占比周环比7d`
        ,concat(round(((7d_total_list_uv/7d_sum_uv)-(last7d_total_list_uv/last7d_sum_uv))*100,2),'%') as `uv占比周环比7d`

        ,concat(round(7d_total_list_pv/7d_sum_pv*100,2),'%') as `近7天pv占比`
        -- ,concat(round(((7d_total_list_pv/7d_sum_pv)/(last7d_total_list_pv/last7d_sum_pv)-1)*100,2),'%') as `pv占比周环比7d`
        ,concat(round(((7d_total_list_pv/7d_sum_pv)-(last7d_total_list_pv/last7d_sum_pv))*100,2),'%') as `pv占比周环比7d`

        ,concat(round(7d_total_ord_k/7d_sum_ord*100,2),'%') as `近7天ord占比`
        -- ,concat(round(((7d_total_ord_k/7d_sum_ord)/(last7d_total_ord_k/last7d_sum_ord)-1)*100,2),'%') as `ord占比周环比7d`
        ,concat(round(((7d_total_ord_k/7d_sum_ord)-(last7d_total_ord_k/last7d_sum_ord))*100,2),'%') as `ord占比周环比7d`

        ,concat(round(7d_total_nights_k/7d_sum_nights*100,2),'%') as `近7天nights占比`
        -- ,concat(round(((7d_total_nights_k/7d_sum_nights)/(last7d_total_nights_k/last7d_sum_nights)-1)*100,2),'%') as `nights占比周环比7d`
        ,concat(round(((7d_total_nights_k/7d_sum_nights)-(last7d_total_nights_k/last7d_sum_nights))*100,2),'%') as `nights占比周环比7d`
    from
        (
            select
                -- a.wrapper_name
                case when age < 23 then '0-22'
                    when age < 29 then '23-28'
                    when age < 36 then '29-35'
                    when age < 50 then '36-49'
                    when age >= 50 then '50+'
                    else '其他' end as age
            -----近7天
                ,count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_uv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) as 7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1) and detail_uid is not null,uid,null)) as 7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_num,null)) as 7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_room_night,null)) as 7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),without_risk_access_order_gmv ,null)) as 7d_total_gmv_z
            -----环比近7天
                ,count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_uv
                -- ,count(distinct house_id, trace_id) as total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) as last7d_total_list_pv
                ,count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8) and detail_uid is not null,uid,null)) as last7d_total_detail_pv
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_num,null)) as last7d_total_ord_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_room_night,null)) as last7d_total_nights_z
                ,sum(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),without_risk_access_order_gmv ,null)) as last7d_total_gmv_z

                ,sum(count(distinct if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_uv
                ,sum(count(distinct if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_uv

                ,sum(count(if(dt between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null))) over() as 7d_sum_pv
                ,sum(count(if(dt between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null))) over() as last7d_sum_pv
            from  
                (
                    select
                        wrapper_name
                        ,dt
                        ,uid
                        ,user_id
                        ,detail_uid
                        ,without_risk_access_order_num
                        ,without_risk_access_order_room_night
                        ,without_risk_access_order_gmv 
                    from
                        dws.dws_path_ldbo_d
                    where
                        dt between date_add("${partition}",-14) and date_add("${partition}",-1)
                    and user_type = '用户'
                    and is_oversea=1
                    and ((wrapper_name in ('携程','去哪儿') and source = '102'))
                )a
            left join
                age_info b on lower(a.user_id) = lower(b.tujia_user_id) and a.wrapper_name = b.wrapper_name
            group by 1
        )list

    left join

        (
            select
                -- a.wrapper_name
                case when age < 23 then '0-22'
                    when age < 29 then '23-28'
                    when age < 36 then '29-35'
                    when age < 50 then '36-49'
                    when age >= 50 then '50+'
                    else '其他' end as age
        ----近7天
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null)) 7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),room_total_amount,null)) as 7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),uid,null)) 7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null)) 7d_total_ord_k
        ----环比近7天
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_room_night_count,null)) last7d_total_nights_k
                ,sum(if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),room_total_amount,null)) as last7d_total_gmv_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),uid,null)) last7d_total_ord_device_k
                ,count(distinct if(create_date between date_add("${partition}",-14)and date_add("${partition}",-8),order_no,null)) last7d_total_ord_k

                ,sum(count(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_no,null))) over () as 7d_sum_ord
                ,sum(count(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_no,null))) over () as last7d_sum_ord

                ,sum(sum(if(create_date between date_add("${partition}",-7) and date_add("${partition}",-1),order_room_night_count,null))) over () as 7d_sum_nights
                ,sum(sum(if(create_date between date_add("${partition}",-14) and date_add("${partition}",-8),order_room_night_count,null))) over () as last7d_sum_nights
            from
                (
                    select
                        *
                        ,case
                            when terminal_type_name = '本站-APP' then '途家'
                            when terminal_type_name = '去哪儿-APP' then '去哪儿'
                            when terminal_type_name = '携程-APP' then '携程'
                            when sell_channel_type='10'  then '蚂蚁'
                            when terminal_type_name in ('艺龙-小程序','艺龙-APP')  then '艺龙'
                            else 'other' end as wrapper_name
                        ,case when checkin_date = create_date or checkin_date = date_sub(create_date,1) then 'T+0'
                            else 'T+n' end as date_type
                    from
                        dws.dws_order
                    where
                        -- is_success_order = '1'
                        is_paysuccess_order ='1'
                        --and landlordSourceChannelCode not in ('fdlx010901','skmy1907')------------非拉新
                        --and SELL_CHANNEL_TYPE IN (3,8,12,6,10,6,43)
                        and (terminal_type_name in ('携程-APP','去哪儿-APP'))
                        and	is_overseas=1
                        and create_date between date_add("${partition}",-14) and date_add("${partition}",-1)
                )a
            left join
                age_info b on lower(a.user_id) = lower(b.tujia_user_id) and a.wrapper_name = b.wrapper_name 
            group by 1
        ) ord on  list.age = ord.age
        -- and list.wrapper_name = ord.wrapper_name 
)bbb
order by type