SELECT 
    a.*
    ,a1.tag
    ,CASE 
        WHEN b.state = '1' THEN '未领取'
        WHEN b.state = '2' THEN '已领取'
        ELSE NULL 
    END AS state_display
    ,`BD`
    ,`BD主站`
    ,`BD战区`
    ,`企微是否添加`
	,case when llb.house_id  is not null then 1 else 0 end as `是否在计划表`
	,`曝光量` as  `流量包曝光量`
    ,`点击量` as `流量包点击量`
    ,`订单量` as  `流量包订单量` --预定成功（不限制取消）
    ,`gmv` as `流量包gmv` 
    ,`间夜` as `流量包间夜`
    ,`花费`
	,lpv  as `总lpv`
	,dpv as `总dpv`
	,`总间夜`
	,`总gmv`
    , `流量包曝光量`/`总lpv` as `流量包pv占比`

FROM 
    (
        SELECT 
            *
        FROM 
            ads.ads_house_best_pick_flow_pool_d
		where dt = '2025-02-16'
    ) a
left join (
    select a.house_id
        ,b.tag
    from (
        select house_id 
            ,city_name 
        from dwd.dwd_house_d 
        where dt = date_sub(current_date,1)
        group by 1,2
    ) a 
    right join (
        select city_name
            ,tag
        from excel_upload.city_list_2024 
        group by 1,2
    ) b
    on a.city_name = b.city_name
) a1
on a.house_id = a1.house_id 
JOIN 
    (
        select * 
		from tujia_ods.ods_other_bigdata_best_pick_flow_landlord_pool_d
		where dt = '2025-02-23' --第一期
    ) b
ON 
    a.landlord_id = b.landlord_id 
left join (        SELECT DISTINCT 
                        landlord_id, 
                        bd as `BD`,
                        bd_zhuzhan as `BD主站`,
                        bd_zhanqu as `BD战区`,
                        is_qiwei AS `企微是否添加`
                FROM 
                        ads.ads_landlord_best_pick_flow_pool_d
                where
                        dt = '2025-02-16' --date_sub(current_date() ,5) --周五跑上周日数据
                        ) c 
on a.landlord_id = c.landlord_id 

left join (
--区分哪个计划
select
    t0.house_id
    ,daily_budget --30
    --,plan_status
    ,sum(`曝光量`) as `曝光量`
    ,sum(`点击量`) as `点击量`
    ,sum(`订单量`) as `订单量`  --预定成功（不限制取消）
    ,sum(`gmv_value`) as `gmv`
    ,sum(`间夜`) as `间夜`
    ,sum(`花费`) as `花费`
    from
    (
        select
        t1.dt
        ,id as plan_id
        ,t1.unit_id as house_id
        ,hotel_id
        ,case  
        when plan_status in (2) then '全天计划'
        when plan_status in (1,7) then '无效计划'
        when plan_status in (4) then '日预算耗尽'
        when plan_status in (8) then '总预算耗尽'
        when plan_status in (0,3,4,5,6,8,9) then '终止计划'
        end as plan_status
        ,plan_status as plan_status_info
        ,daily_budget/100 as daily_budget
        ,city_id
        from dwd.dwd_house_promotion_plan_d as t1 --推广通投放计划表
        join
        (
            select distinct unit_id
            from pdb_analysis_c.ads_flow_zhenxuan_cpc_update_d
            where status=1
            and dt='2025-02-23'
        ) as t2
        on t1.unit_id=t2.unit_id
        where t1.dt=date_sub(current_date,1)
        and source=1 --计划来源，0：房东创建，1：运营创建
        and plan_status in (2,4,8) --计划状态
        and daily_budget='3000' --￥30  日预算,（单位分）
    ) t0
    left join
    (
        select
        concat(substr(date_date,1,4),'-',substr(date_date,5,2),'-',substr(date_date,7,2)) as dt
        ,hotel_id
        ,unit_id as house_id
        ,plan_id
        ,sum(exposure_count) as `曝光量`
        ,sum(click_count) as `点击量`
        ,sum(order_count) as `订单量`
        ,sum(gmv_value) as `gmv_value`
        ,sum(nights) as `间夜`
        -- ,sum(cost)/100 as `花费`
        ,(sum(physical_cost)/100)+(sum(if(real_cost = 0,pre_cost,virtual_cost))/100) as `花费`
    from
        -- dwd.dwd_flow_cpc_poi_click_d
        dwd.dwd_flow_cpc_poi_cost_d
    where 
        dt = date_sub(current_date,1)
    and concat(substr(date_date,1,4),'-',substr(date_date,5,2),'-',substr(date_date,7,2)) >= '2025-02-24' --第一期时间
    and concat(substr(date_date,1,4),'-',substr(date_date,5,2),'-',substr(date_date,7,2)) <= '2025-03-02' --date_sub(current_date,1)
    group by 1,2,3,4
    ) cost
    on t0.plan_id = cost.plan_id
group by 1,2
) llb on a.house_id = llb.house_id 

left join (
			select house_id,count(uid) lpv,count(detail_uid) dpv
			from dws.dws_path_ldbo_d
			where dt between '2025-02-24' and '2025-03-02'
			--and user_type = '用户'
			and wrapper_name in  ('携程','去哪儿','途家')
			--and source = '102'
			group by 1
		) list  on a.house_id = list.house_id 

left join (	select house_id,sum(order_room_night_count) as `总间夜`,sum(room_total_amount) as `总gmv`
			from dws.dws_order
			where create_date between '2025-02-24' and '2025-03-02'
			--and city_id in (select city_id from excel_upload.wanghongcityid)
			and terminal_type_name in ('携程-APP','本站-APP','去哪儿-APP','携程-小程序','本站-小程序','去哪儿-小程序')
			and is_paysuccess_order = '1'
			and is_cancel_order = 0 --非取消
			--and is_done = 1
			and is_overseas = 0 --国内
			group by 1			
			) ord_k on a.house_id = ord_k.house_id 