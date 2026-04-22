


-- part1 预定口径途家近两周订单 
with all_data as (
    select sum(order_room_night_count) as night,
            sum(room_total_amount) as gmv,
            create_date,weekofyear,day_name as dayofweek,yoy_date,
            case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end as channel
        from (select order_room_night_count ,room_total_amount,sell_channel,sell_channel_type,api_sell_channel_type,create_date,is_success_order,is_overseas,landlord_source_channel_code
            from dws.dws_order_day
            where dt = create_date 
            and dt =date_add(current_date,-1)
            ) as ooi
                left join tujia_dim.date_week_of_year_YOY as week
                        on week.`date` = ooi.create_date
        where is_success_order = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
        group by create_date,weekofyear,day_name,yoy_date,
                case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end
    union all 
    select * 
    from `ads`.`ads_order_haiwai_night_gmv_report` where create_date < date_add(current_date,-1)
)
select '最近7天日均' as `createdate`,
       cast(sum(day_night.night)/7 as int) as `总计-间夜`,
       concat(round(sum(day_night.night-history_night.night)/sum(history_night.night)*100,2), '%') as `总计-间夜YOY`,
       concat(round(sum(day_night.gmv-history_night.gmv)/sum(history_night.gmv)*100,2), '%') as `总计-GMVYOY`,
       cast(sum(if(day_night.channel= 'ctrip',day_night.night,0))/7 as int) as `C宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'ctrip',history_night.night,0))*100,2), '%')as `C宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'ctrip',history_night.gmv,0))*100,2), '%')as `C宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'qunar',day_night.night,0))/7 as int) as `Q宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'qunar',history_night.night,0))*100,2), '%')as `Q宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'qunar',history_night.gmv,0))*100,2), '%')as `Q宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'tujia',day_night.night,0))/7 as int) as `本站-间夜`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'tujia',history_night.night,0))*100,2), '%')as `本站-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'tujia',history_night.gmv,0))*100,2), '%')as `本站-GMVYOY`
    --    cast(sum(if(day_night.channel= 'C分销',day_night.night,0))/7 as int) as `C分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'C分销',history_night.night,0))*100,2), '%')as `C分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'C分销',history_night.gmv,0))*100,2), '%')as `C分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'Q分销',day_night.night,0))/7 as int) as `Q分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'Q分销',history_night.night,0))*100,2), '%')as `Q分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'Q分销',history_night.gmv,0))*100,2), '%')as `Q分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'E分销',day_night.night,0))/7 as int) as `E分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'E分销',history_night.night,0))*100,2), '%')as `E分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'E分销',history_night.gmv,0))*100,2), '%')as `E分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'G分销',day_night.night,0))/7 as int) as `G分销-间夜`,
    --        concat(round(sum(if(day_night.channel = 'G分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'G分销',history_night.night,0))*100,2), '%')as `G分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'G分销',history_night.gmv,0))*100,2), '%')as `G分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'other',day_night.night,0))/7 as int) as `其他-间夜`
from (
    select *
    from all_data
    where create_date >= date_add(current_date,-7) and create_date < current_date
) as day_night
left join (
    select *
    from all_data
) as history_night
on day_night.yoy_date = history_night.create_date 
and day_night.channel = history_night.channel
union all
select '最近8-14天日均' as `createdate`,
       cast(sum(day_night.night)/7 as int) as `总计-间夜`,
       concat(round(sum(day_night.night-history_night.night)/sum(history_night.night)*100,2), '%') as `总计-间夜YOY`,
       concat(round(sum(day_night.gmv-history_night.gmv)/sum(history_night.gmv)*100,2), '%') as `总计-GMVYOY`,
       cast(sum(if(day_night.channel= 'ctrip',day_night.night,0))/7 as int) as `C宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'ctrip',history_night.night,0))*100,2), '%')as `C宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'ctrip',history_night.gmv,0))*100,2), '%')as `C宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'qunar',day_night.night,0))/7 as int) as `Q宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'qunar',history_night.night,0))*100,2), '%')as `Q宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'qunar',history_night.gmv,0))*100,2), '%')as `Q宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'tujia',day_night.night,0))/7 as int) as `本站-间夜`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'tujia',history_night.night,0))*100,2), '%')as `本站-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'tujia',history_night.gmv,0))*100,2), '%')as `本站-GMVYOY`
    --    cast(sum(if(day_night.channel= 'C分销',day_night.night,0))/7 as int) as `C分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'C分销',history_night.night,0))*100,2), '%')as `C分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'C分销',history_night.gmv,0))*100,2), '%')as `C分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'Q分销',day_night.night,0))/7 as int) as `Q分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'Q分销',history_night.night,0))*100,2), '%')as `Q分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'Q分销',history_night.gmv,0))*100,2), '%')as `Q分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'E分销',day_night.night,0))/7 as int) as `E分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'E分销',history_night.night,0))*100,2), '%')as `E分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'E分销',history_night.gmv,0))*100,2), '%')as `E分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'G分销',day_night.night,0))/7 as int) as `G分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'G分销',history_night.night,0))*100,2), '%')as `G分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'G分销',history_night.gmv,0))*100,2), '%')as `G分销-GMVYOY`,
    --        cast(sum(if(day_night.channel= 'other',day_night.night,0))/7 as int) as `其他-间夜`
from (
    select *
    from all_data
    where create_date >= date_add(current_date,-14) and create_date < date_add(current_date,-7)
) as day_night
left join (
    select *
    from all_data
) as history_night
on day_night.yoy_date = history_night.create_date 
and day_night.channel = history_night.channel
union all
select concat(day_night.create_date,'(',day_night.dayofweek ,')') as `createdate`,
       cast(sum(day_night.night) as int) as `总计-间夜`,
       concat(round(sum(day_night.night-history_night.night)/sum(history_night.night)*100,2), '%') as `总计-间夜YOY`,
       concat(round(sum(day_night.gmv-history_night.gmv)/sum(history_night.gmv)*100,2), '%') as `总计-GMVYOY`,
       cast(sum(if(day_night.channel= 'ctrip',day_night.night,0)) as int) as `C宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'ctrip',history_night.night,0))*100,2), '%')as `C宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'ctrip',history_night.gmv,0))*100,2), '%')as `C宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'qunar',day_night.night,0)) as int) as `Q宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'qunar',history_night.night,0))*100,2), '%')as `Q宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'qunar',history_night.gmv,0))*100,2), '%')as `Q宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'tujia',day_night.night,0)) as int) as `本站-间夜`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'tujia',history_night.night,0))*100,2), '%')as `本站-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'tujia',history_night.gmv,0))*100,2), '%')as `本站-GMVYOY`
    --    cast(sum(if(day_night.channel= 'C分销',day_night.night,0)) as int) as `C分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'C分销',history_night.night,0))*100,2), '%')as `C分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'C分销',history_night.gmv,0))*100,2), '%')as `C分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'Q分销',day_night.night,0)) as int) as `Q分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'Q分销',history_night.night,0))*100,2), '%')as `Q分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'Q分销',history_night.gmv,0))*100,2), '%')as `Q分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'E分销',day_night.night,0)) as int) as `E分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'E分销',history_night.night,0))*100,2), '%')as `E分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'E分销',history_night.gmv,0))*100,2), '%')as `E分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'G分销',day_night.night,0)) as int) as `G分销-间夜`,
    --        concat(round(sum(if(day_night.channel = 'G分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'G分销',history_night.night,0))*100,2), '%')as `G分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'G分销',history_night.gmv,0))*100,2), '%')as `G分销-GMVYOY`,
    --    sum(if(day_night.channel= 'other',day_night.night,0)) as `其他-间夜`
from (
    select *
    from all_data
    where create_date >= date_add(current_date,-14) and create_date < current_date
) as day_night
left join (
    select *
    from all_data
) as history_night
on day_night.yoy_date  =  history_night.create_date 
and day_night.channel = history_night.channel
group by day_night.create_date,day_night.dayofweek
order by `createdate` desc;

-- part2 离店口径途家近两周订单 
with all_data as  (
select sum(order_room_night_count) as night,
    sum(room_total_amount) as gmv,
    checkout_date as create_date,weekofyear,day_name as dayofweek,yoy_date,
    case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
        when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
        when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
        when api_sell_channel_type in(22)  or sell_channel_type = 18 then 'elong'
        when sell_channel like '%携程%' then 'C分销'
        when sell_channel like '%去哪儿%' then 'Q分销'
        when sell_channel like '%艺龙%' then 'E分销'
        when sell_channel like '%高德%' then 'G分销'
        else 'other' end as channel
from dws.dws_order as ooi
left join tujia_dim.date_week_of_year_YOY as week
on week.`date` = ooi.checkout_date
where is_done = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
group by checkout_date,weekofyear,day_name,yoy_date,
    case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
        when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
        when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
        when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
        when sell_channel like '%携程%' then 'C分销'
        when sell_channel like '%去哪儿%' then 'Q分销'
        when sell_channel like '%艺龙%' then 'E分销'
        when sell_channel like '%高德%' then 'G分销'
        else 'other' end
)
select '最近7天日均' as `createdate`,
       cast(sum(day_night.night)/7 as int) as `总计-间夜`,
       concat(round(sum(day_night.night-history_night.night)/sum(history_night.night)*100,2), '%') as `总计-间夜YOY`,
       concat(round(sum(day_night.gmv-history_night.gmv)/sum(history_night.gmv)*100,2), '%') as `总计-GMVYOY`,
       cast(sum(if(day_night.channel= 'ctrip',day_night.night,0))/7 as int) as `C宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'ctrip',history_night.night,0))*100,2), '%')as `C宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'ctrip',history_night.gmv,0))*100,2), '%')as `C宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'qunar',day_night.night,0))/7 as int) as `Q宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'qunar',history_night.night,0))*100,2), '%')as `Q宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'qunar',history_night.gmv,0))*100,2), '%')as `Q宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'tujia',day_night.night,0))/7 as int) as `本站-间夜`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'tujia',history_night.night,0))*100,2), '%')as `本站-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'tujia',history_night.gmv,0))*100,2), '%')as `本站-GMVYOY`
    --    cast(sum(if(day_night.channel= 'C分销',day_night.night,0))/7 as int) as `C分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'C分销',history_night.night,0))*100,2), '%')as `C分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'C分销',history_night.gmv,0))*100,2), '%')as `C分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'Q分销',day_night.night,0))/7 as int) as `Q分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'Q分销',history_night.night,0))*100,2), '%')as `Q分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'Q分销',history_night.gmv,0))*100,2), '%')as `Q分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'E分销',day_night.night,0))/7 as int) as `E分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'E分销',history_night.night,0))*100,2), '%')as `E分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'E分销',history_night.gmv,0))*100,2), '%')as `E分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'G分销',day_night.night,0))/7 as int) as `G分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'G分销',history_night.night,0))*100,2), '%')as `G分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'G分销',history_night.gmv,0))*100,2), '%')as `G分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'other',day_night.night,0))/7 as int) as `其他-间夜`
       --concat(round(sum(if(day_night.channel = 'other',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'other',history_night.night,0))*100,2), '%')as `其他-间夜YOY`,
       --concat(round(sum(if(day_night.channel = 'other',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'other',history_night.gmv,0))*100,2), '%')as `其他-GMVYOY`
from (select *
      from all_data
      where create_date >= date_add(current_date,-7) and create_date < current_date
     ) as day_night
         left join (
    select *
    from all_data
) as history_night
on day_night.yoy_date  =  history_night.create_date and day_night.channel = history_night.channel
union all
select '最近8-14天日均' as `日期`,
       cast(sum(day_night.night)/7 as int) as `总计-间夜`,
       concat(round(sum(day_night.night-history_night.night)/sum(history_night.night)*100,2), '%') as `总计-间夜YOY`,
       concat(round(sum(day_night.gmv-history_night.gmv)/sum(history_night.gmv)*100,2), '%') as `总计-GMVYOY`,
       cast(sum(if(day_night.channel= 'ctrip',day_night.night,0))/7 as int) as `C宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'ctrip',history_night.night,0))*100,2), '%')as `C宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'ctrip',history_night.gmv,0))*100,2), '%')as `C宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'qunar',day_night.night,0))/7 as int) as `Q宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'qunar',history_night.night,0))*100,2), '%')as `Q宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'qunar',history_night.gmv,0))*100,2), '%')as `Q宫格-GMVYOY`,
       cast(sum(if(day_night.channel= 'tujia',day_night.night,0))/7 as int) as `本站-间夜`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'tujia',history_night.night,0))*100,2), '%')as `本站-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'tujia',history_night.gmv,0))*100,2), '%')as `本站-GMVYOY`
    --    cast(sum(if(day_night.channel= 'C分销',day_night.night,0))/7 as int) as `C分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'C分销',history_night.night,0))*100,2), '%')as `C分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'C分销',history_night.gmv,0))*100,2), '%')as `C分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'Q分销',day_night.night,0))/7 as int) as `Q分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'Q分销',history_night.night,0))*100,2), '%')as `Q分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'Q分销',history_night.gmv,0))*100,2), '%')as `Q分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'E分销',day_night.night,0))/7 as int) as `E分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'E分销',history_night.night,0))*100,2), '%')as `E分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'E分销',history_night.gmv,0))*100,2), '%')as `E分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'G分销',day_night.night,0))/7 as int) as `G分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'G分销',history_night.night,0))*100,2), '%')as `G分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'G分销',history_night.gmv,0))*100,2), '%')as `G分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'other',day_night.night,0))/7 as int) as `其他-间夜`
       --concat(round(sum(if(day_night.channel = 'other',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'other',history_night.night,0))*100,2), '%')as `其他-间夜YOY`,
       --concat(round(sum(if(day_night.channel = 'other',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'other',history_night.gmv,0))*100,2), '%')as `其他-GMVYOY`
from (select *
      from all_data
      where create_date >= date_add(current_date,-14) and create_date < date_add(current_date,-7)
     ) as day_night
         left join (
    select *
    from all_data
) as history_night
on day_night.yoy_date  =  history_night.create_date and day_night.channel = history_night.channel
union all
select concat(day_night.create_date,'(',day_night.dayofweek ,')') as `日期`,
       cast(sum(day_night.night) as int) as `总计-间夜`,
       concat(round(sum(day_night.night-history_night.night)/sum(history_night.night)*100,2), '%') as `总计-间夜YOY`,
       concat(round(sum(day_night.gmv-history_night.gmv)/sum(history_night.gmv)*100,2), '%') as `总计-GMVYOY`,
       sum(if(day_night.channel= 'ctrip',day_night.night,0)) as `C宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'ctrip',history_night.night,0))*100,2), '%')as `C宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'ctrip',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'ctrip',history_night.gmv,0))*100,2), '%')as `C宫格-GMVYOY`,
       sum(if(day_night.channel= 'qunar',day_night.night,0)) as `Q宫格-间夜`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'qunar',history_night.night,0))*100,2), '%')as `Q宫格-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'qunar',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'qunar',history_night.gmv,0))*100,2), '%')as `Q宫格-GMVYOY`,
       sum(if(day_night.channel= 'tujia',day_night.night,0)) as `本站-间夜`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'tujia',history_night.night,0))*100,2), '%')as `本站-间夜YOY`,
       concat(round(sum(if(day_night.channel = 'tujia',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'tujia',history_night.gmv,0))*100,2), '%')as `本站-GMVYOY`
    --    cast(sum(if(day_night.channel= 'C分销',day_night.night,0)) as int) as `C分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'C分销',history_night.night,0))*100,2), '%')as `C分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'C分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'C分销',history_night.gmv,0))*100,2), '%')as `C分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'Q分销',day_night.night,0)) as int) as `Q分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'Q分销',history_night.night,0))*100,2), '%')as `Q分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'Q分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'Q分销',history_night.gmv,0))*100,2), '%')as `Q分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'E分销',day_night.night,0)) as int) as `E分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'E分销',history_night.night,0))*100,2), '%')as `E分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'E分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'E分销',history_night.gmv,0))*100,2), '%')as `E分销-GMVYOY`,
    --    cast(sum(if(day_night.channel= 'G分销',day_night.night,0)) as int) as `G分销-间夜`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'G分销',history_night.night,0))*100,2), '%')as `G分销-间夜YOY`,
    --    concat(round(sum(if(day_night.channel = 'G分销',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'G分销',history_night.gmv,0))*100,2), '%')as `G分销-GMVYOY`,
    --    sum(if(day_night.channel= 'other',day_night.night,0)) as `其他-间夜`
       --concat(round(sum(if(day_night.channel = 'other',day_night.night-history_night.night,0))/sum(if(history_night.channel = 'other',history_night.night,0))*100,2), '%')as `其他-间夜YOY`,
       --concat(round(sum(if(day_night.channel = 'other',day_night.gmv-history_night.gmv,0))/sum(if(history_night.channel = 'other',history_night.gmv,0))*100,2), '%')as `其他-GMVYOY`
from (select *
      from all_data
      where create_date >= date_add(current_date,-14) and create_date < current_date
     ) as day_night
         left join (
    select *
    from all_data
) as history_night
on day_night.yoy_date  =  history_night.create_date and day_night.channel = history_night.channel
group by day_night.create_date,day_night.dayofweek
order by `createdate` desc;





-- part 3 今日间夜、GMV排名
with l as (--离店
select *
    ,row_number() over(order by gmv desc) rk_gmv
    ,row_number() over(order by night desc) rk_night
    ,'1' as j
from (
    select create_date,sum(night) night,sum(gmv) gmv
    from (
    select sum(order_room_night_count) as night,
            sum(room_total_amount) as gmv,
            checkout_date as create_date,
            weekofyear,
            day_name as dayofweek,
            yoy_date,
            case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22)  or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end as channel
    from dws.dws_order as ooi
    left join tujia_dim.date_week_of_year_YOY as week
    on week.`date` = ooi.checkout_date
    -- left join tujia_dim.dim_order_channel as channel
    -- on nvl(channel.api_sell_channel_type,'99') =  nvl(ooi.api_sell_channel_type,'99') and nvl(channel.sell_channel,'99') = nvl(ooi.sell_channel,'99')
    where is_done = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
    group by checkout_date,weekofyear,day_name,yoy_date,
        case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
            when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
            when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
            when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
            when sell_channel like '%携程%' then 'C分销'
            when sell_channel like '%去哪儿%' then 'Q分销'
            when sell_channel like '%艺龙%' then 'E分销'
            when sell_channel like '%高德%' then 'G分销'
            else 'other' end
) a
group by 1
) aa
)
,c as (-- 支付
    select *
        ,row_number() over(order by night desc) rk_night 
        ,row_number() over(order by gmv desc) rk_gmv
    from (
    select create_date,sum(night) night,sum(gmv) gmv
    from (
        select sum(order_room_night_count) as night,
                sum(room_total_amount) as gmv,
                create_date,weekofyear,day_name as dayofweek,yoy_date,
                case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                        when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                        when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                        when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                        when sell_channel like '%携程%' then 'C分销'
                        when sell_channel like '%去哪儿%' then 'Q分销'
                        when sell_channel like '%艺龙%' then 'E分销'
                        when sell_channel like '%高德%' then 'G分销'
                        else 'other' end as channel
            from (select order_room_night_count ,room_total_amount,sell_channel,sell_channel_type,api_sell_channel_type,create_date,is_success_order,is_overseas,landlord_source_channel_code
                from dws.dws_order_day
                where dt = create_date and dt =date_add(current_date,-1)) as ooi
                    left join tujia_dim.date_week_of_year_YOY as week
                            on week.`date` = ooi.create_date
            where is_success_order = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
            group by create_date,weekofyear,day_name,yoy_date,
                    case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                        when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                        when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                        when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                        when sell_channel like '%携程%' then 'C分销'
                        when sell_channel like '%去哪儿%' then 'Q分销'
                        when sell_channel like '%艺龙%' then 'E分销'
                        when sell_channel like '%高德%' then 'G分销'
                        else 'other' end
        union all 
        select * from 
        `ads`.`ads_order_haiwai_night_gmv_report` where create_date < date_add(current_date,-1)
    ) a 
    group by 1
    ) aa
)
select a.create_date as `日期`
    ,b.night as `预定间夜数`, b.rk_night as `间夜数排名`,b.`间夜数百分位`, a.night `离店间夜数`, a.rk_night as `间夜数排名`,a.`间夜数百分位`
--     ,round(c.gmv,1) as `预定GMV`, c.rk_gmv as `GMV排名`,c.`GMV百分位`, round(d.gmv,1) `离店GMV`, d.rk_gmv as `GMV排名`,d.`GMV百分位`
    ,round(d.gmv,1) as `预定GMV`, d.rk_gmv as `GMV排名`,d.`GMV百分位`, round(c.gmv,1) `离店GMV`, c.rk_gmv as `GMV排名`,c.`GMV百分位`
from (
    select * 
    from (
        select 
            *,
            concat(round(percent_rank() over (order by rk_night) * 100,2),'%') as `间夜数百分位`
        from l 
    ) as aaa
    where create_date = date_sub(current_date(),1)
) a
left join (
    select *
    from (
        select
            *,
            concat(round(percent_rank() over (order by rk_night) * 100,2),'%') as `间夜数百分位`
        from c
    ) as aa
    where
    create_date = date_sub(current_date(), 1)
)  b
on a.create_date = b.create_date
left join (
    select *
    from (
        select
            *,
            concat(round(percent_rank() over (order by rk_gmv) * 100,2),'%') as `GMV百分位`
        from l
    ) as aa
    where
    create_date = date_sub(current_date(), 1)
)  c
on a.create_date = c.create_date
left join (
    select *
    from (
        select
            *,
            concat(round(percent_rank() over (order by rk_gmv) * 100,2),'%') as `GMV百分位`
        from c
    ) as aa
    where
    create_date = date_sub(current_date(), 1)
)  d
on a.create_date = d.create_date;



-- part4 拉长周期间夜排名
with l as (--离店
select *
    ,row_number() over(order by gmv desc) rk_gmv
    ,row_number() over(order by night desc) rk_night
    ,'1' as j
from (
    select create_date,sum(night) night,sum(gmv) gmv
    from (
    select sum(order_room_night_count) as night,
            sum(room_total_amount) as gmv,
            checkout_date as create_date,
            weekofyear,
            day_name as dayofweek,
            yoy_date,
            case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22)  or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end as channel
    from dws.dws_order as ooi
    left join tujia_dim.date_week_of_year_YOY as week
    on week.`date` = ooi.checkout_date
    -- left join tujia_dim.dim_order_channel as channel
    -- on nvl(channel.api_sell_channel_type,'99') =  nvl(ooi.api_sell_channel_type,'99') and nvl(channel.sell_channel,'99') = nvl(ooi.sell_channel,'99')
    where is_done = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
    group by checkout_date,weekofyear,day_name,yoy_date,
        case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
            when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
            when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
            when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
            when sell_channel like '%携程%' then 'C分销'
            when sell_channel like '%去哪儿%' then 'Q分销'
            when sell_channel like '%艺龙%' then 'E分销'
            when sell_channel like '%高德%' then 'G分销'
            else 'other' end
) a
group by 1
) aa
)
,c as (-- 支付
select *
    ,row_number() over(order by night desc) rk_night 
    ,row_number() over(order by gmv desc) rk_gmv
from (
select create_date,sum(night) night,sum(gmv) gmv
from (
    select sum(order_room_night_count) as night,
            sum(room_total_amount) as gmv,
            create_date,weekofyear,day_name as dayofweek,yoy_date,
            case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end as channel
        from (select order_room_night_count ,room_total_amount,sell_channel,sell_channel_type,api_sell_channel_type,create_date,is_success_order,is_overseas,landlord_source_channel_code
            from dws.dws_order_day
            where dt = create_date and dt =date_add(current_date,-1)) as ooi
                left join tujia_dim.date_week_of_year_YOY as week
                        on week.`date` = ooi.create_date
        where is_success_order = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
        group by create_date,weekofyear,day_name,yoy_date,
                case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end
    union all 
    select * from 
    `ads`.`ads_order_haiwai_night_gmv_report` where create_date < date_add(current_date,-1)
) a 
group by 1
) aa
)
select distinct m.*,n.`离店日期`,n.`离店间夜`,n.`离店GAP`
from (
    select distinct a.rk_night as `排名`
        ,a.create_date as `预定日期`
        ,a.night as `预定间夜`
        ,concat(round((a.night/b.night-1)*100,2),'%') as `预定GAP`
    from c as a
    left join (    
        select *
        from c
        where create_date = date_sub(current_date(),1)
    )  b
    --where a.night >= b.night
    where a.rk_night <=50
) m
left join (
    select distinct a.rk_night as `排名`
        ,a.create_date as `离店日期`
        ,a.night as `离店间夜`
        ,concat(round((a.night/b.night-1)*100,2),'%') as `离店GAP`
    from l as a
    left join (    
        select *
        from l
        where create_date = date_sub(current_date(),1)
    ) b
    on a.j = b.j
    --where a.night >= b.night
    where a.rk_night <=50
) n
on m.`排名` = n.`排名`
order by 1;



-- part5 拉长周期GMV排名
with l as (--离店
select *
    ,row_number() over(order by gmv desc) rk_gmv
    ,row_number() over(order by night desc) rk_night
    ,'1' as j
from (
    select create_date,sum(night) night,sum(gmv) gmv
    from (
    select sum(order_room_night_count) as night,
            sum(room_total_amount) as gmv,
            checkout_date as create_date,
            weekofyear,
            day_name as dayofweek,
            yoy_date,
            case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22)  or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end as channel
    from dws.dws_order as ooi
    left join tujia_dim.date_week_of_year_YOY as week
    on week.`date` = ooi.checkout_date
    -- left join tujia_dim.dim_order_channel as channel
    -- on nvl(channel.api_sell_channel_type,'99') =  nvl(ooi.api_sell_channel_type,'99') and nvl(channel.sell_channel,'99') = nvl(ooi.sell_channel,'99')
    where is_done = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
    group by checkout_date,weekofyear,day_name,yoy_date,
        case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
            when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
            when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
            when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
            when sell_channel like '%携程%' then 'C分销'
            when sell_channel like '%去哪儿%' then 'Q分销'
            when sell_channel like '%艺龙%' then 'E分销'
            when sell_channel like '%高德%' then 'G分销'
            else 'other' end
) a
group by 1
) aa
)
,c as (-- 支付
select *
    ,row_number() over(order by night desc) rk_night 
    ,row_number() over(order by gmv desc) rk_gmv
from (
select create_date,sum(night) night,sum(gmv) gmv
from (
    select sum(order_room_night_count) as night,
            sum(room_total_amount) as gmv,
            create_date,weekofyear,day_name as dayofweek,yoy_date,
            case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end as channel
        from (select order_room_night_count ,room_total_amount,sell_channel,sell_channel_type,api_sell_channel_type,create_date,is_success_order,is_overseas,landlord_source_channel_code
            from dws.dws_order_day
            where dt = create_date and dt =date_add(current_date,-1)) as ooi
                left join tujia_dim.date_week_of_year_YOY as week
                        on week.`date` = ooi.create_date
        where is_success_order = 1 and is_overseas = 1  and nvl(landlord_source_channel_code, 0) NOT IN ('fdlx010901', 'skmy1907')
        group by create_date,weekofyear,day_name,yoy_date,
                case when (sell_channel_type = '8' or api_sell_channel_type = '42')  then 'qunar'
                    when api_sell_channel_type in('6','19') or sell_channel_type='12' then 'ctrip'
                    when sell_channel like '%本站%' or sell_channel like '%蚂蚁%' then 'tujia'
                    when api_sell_channel_type in(22) or sell_channel_type = 18 then 'elong'
                    when sell_channel like '%携程%' then 'C分销'
                    when sell_channel like '%去哪儿%' then 'Q分销'
                    when sell_channel like '%艺龙%' then 'E分销'
                    when sell_channel like '%高德%' then 'G分销'
                    else 'other' end
    union all 
    select * from 
    `ads`.`ads_order_haiwai_night_gmv_report` where create_date < date_add(current_date,-1)
) a 
group by 1
) aa
)
select distinct m.*,n.`离店日期`,n.`离店GMV`,n.`离店GAP`
from (
    select distinct a.rk_gmv as `排名`
        ,a.create_date as `预定日期`
        ,round(a.gmv,1) as `预定GMV`
        ,concat(round((a.night/b.night-1)*100,2),'%') as `预定GAP`
    from c as a
    left join (
        select *
        from c
        where create_date = date_sub(current_date(),1)
    )  b
    --where a.night >= b.night
    where a.rk_gmv <=50
) m
left join (
    select distinct a.rk_gmv as `排名`
        ,a.create_date as `离店日期`
        ,round(a.gmv,1) as `离店GMV`
        ,concat(round((a.night/b.night-1)*100,2),'%') as `离店GAP`
    from l as a
    left join (    
        select *
        from l
        where create_date = date_sub(current_date(),1)
    ) b
    on a.j = b.j
    --where a.night >= b.night
    where a.rk_gmv <=50
) n
on m.`排名` = n.`排名`
order by 1;




-- 离店七大类
with l as (
select concat(a.create_date,'(',c.dayofweek ,')')  create_date
    ,sum(a.night) as night
    ,round(sum(a.gmv),1) as gmv
    ,sum(d.night) as night_ly
    ,round(sum(d.gmv),1) as gmv_ly
from (
    select create_date 
        ,sum(gmv) gmv  
        ,sum(night) night 
    from (
        select 
            TO_DATE(departure) create_date
            ,masterhotelid
            ,ciireceivable gmv 
            ,ciiquantity night 
        FROM app_ctrip.edw_htl_order_all_split
        WHERE submitfrom = 'client'
        AND TO_DATE(departure) >= date_add(current_date,-14) 
        and TO_DATE(departure) < current_date
        AND orderstatus IN ('P','S')
        -- AND cityname = '大阪'
        AND ordertype = 2 -- 酒店订单
        and d = current_Date()
    ) a 
    join (
        select masterhotelid
            ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)                                         
        and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
        and masterhotelid > 0
        and is_standard = 0 
        group by 1,2 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) a 
left join tujia_dim.date_week_of_year_YOY c 
on a.create_date = c.`date`
left join (
    select create_date 
            ,sum(gmv) gmv
            ,sum(night) night  
    from (
        select 
            TO_DATE(departure) create_date
            ,masterhotelid
            ,ciireceivable gmv 
            ,ciiquantity night 
        FROM app_ctrip.edw_htl_order_all_split
        WHERE submitfrom = 'client'
        AND TO_DATE(departure) >= add_months(date_add(current_date,-16),-12)
        and TO_DATE(departure) < add_months(date_add(current_date,2),-12)
        AND orderstatus IN ('P','S')
        -- AND cityname = '大阪'
        AND ordertype = 2 -- 酒店订单
        and d = current_Date()
    ) a 
    join (
        select masterhotelid
            ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)                                         
        and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
        and masterhotelid > 0
        and is_standard = 0 
        group by 1,2 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) d 
on c.yoy_date = d.create_date
group by 1 
order by `create_date` desc
)

 
select '最近8-14天日均' create_date
    ,cast(sum(gmv) as decimal(18,2)) `总计GMV`
    ,concat(round((sum(gmv) / sum(gmv_ly) - 1) * 100,2),'%') `总计GMVYoY`
    ,sum(night) night 
    ,concat(round((sum(night) / sum(night_ly) - 1) * 100,2),'%') `总计nightYoY`
from l 
where substr(create_date,1,10) between date_sub(current_date,14) and date_sub(current_date,8)
union all
select '最近7天日均' create_date
    ,cast(sum(gmv) as decimal(18,2)) `总计GMV`
    ,concat(round((sum(gmv) / sum(gmv_ly) - 1) * 100,2),'%') `总计GMVYoY`
    ,sum(night) night 
    ,concat(round((sum(night) / sum(night_ly) - 1) * 100,2),'%') `总计nightYoY`
from l 
where substr(create_date,1,10) between date_sub(current_date,7) and date_sub(current_date,1)
union all
select create_date
    ,cast(gmv as decimal(18,2)) `总计GMV`
    ,concat(round((gmv / gmv_ly - 1) * 100,2),'%') `总计GMVYoY`
    ,night night 
    ,concat(round((night / night_ly - 1) * 100,2),'%') `总计nightYoY`
from l ;



-- 七大类支付
with l as (
select concat(a.create_date,'(',c.dayofweek ,')')  create_date
    ,sum(a.night) as night
    ,round(sum(a.gmv),1) as gmv
    ,sum(d.night) as night_ly
    ,round(sum(d.gmv),1) as gmv_ly
from (
    select create_date 
        ,sum(gmv) gmv  
        ,sum(night) night 
    from (
        select 
            TO_DATE(orderdate) create_date
            ,masterhotelid
            ,ciireceivable gmv 
            ,ciiquantity night 
        FROM app_ctrip.edw_htl_order_all_split
        WHERE submitfrom = 'client'
        AND TO_DATE(orderdate) >= date_add(current_date,-14) 
        and TO_DATE(orderdate) < current_date
        AND orderstatus IN ('P','S')
        -- AND cityname = '大阪'
        AND ordertype = 2 -- 酒店订单
        and d = current_Date()
    ) a 
    join (
        select masterhotelid
            ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)                                         
        and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
        and masterhotelid > 0
        and is_standard = 0 
        group by 1,2 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) a 
left join tujia_dim.date_week_of_year_YOY c 
on a.create_date = c.`date`
left join (
    select create_date 
            ,sum(gmv) gmv
            ,sum(night) night  
    from (
        select 
            TO_DATE(orderdate) create_date
            ,masterhotelid
            ,ciireceivable gmv 
            ,ciiquantity night 
        FROM app_ctrip.edw_htl_order_all_split
        WHERE submitfrom = 'client'
        AND TO_DATE(orderdate) >= add_months(date_add(current_date,-16),-12)
        and TO_DATE(orderdate) < add_months(date_add(current_date,2),-12)
        AND orderstatus IN ('P','S')
        -- AND cityname = '大阪'
        AND ordertype = 2 -- 酒店订单
        and d = current_Date()
    ) a 
    join (
        select masterhotelid
            ,case when is_standard = 0 then '七大类' else '标准酒店' end is_standard 
        from app_ctrip.dimmasterhotel
        where d = date_sub(current_date,2)                                         
        and (countryname != '中国'   or cityname in ('香港','澳门'))                                      
        and masterhotelid > 0
        and is_standard = 0 
        group by 1,2 
    ) b 
    on a.masterhotelid = b.masterhotelid
    group by 1 
) d 
on c.yoy_date = d.create_date
group by 1 
order by `create_date` desc
)


select '最近8-14天日均' create_date
    ,cast(sum(gmv) as decimal(18,2)) `总计GMV`
    ,concat(round((sum(gmv) / sum(gmv_ly) - 1) * 100,2),'%') `总计GMVYoY`
    ,sum(night) night 
    ,concat(round((sum(night) / sum(night_ly) - 1) * 100,2),'%') `总计nightYoY`
from l 
where substr(create_date,1,10) between date_sub(current_date,14) and date_sub(current_date,8)
union all
select '最近7天日均' create_date
    ,cast(sum(gmv) as decimal(18,2)) `总计GMV`
    ,concat(round((sum(gmv) / sum(gmv_ly) - 1) * 100,2),'%') `总计GMVYoY`
    ,sum(night) night 
    ,concat(round((sum(night) / sum(night_ly) - 1) * 100,2),'%') `总计nightYoY`
from l 
where substr(create_date,1,10) between date_sub(current_date,7) and date_sub(current_date,1)
union all
select create_date
    ,cast(gmv as decimal(18,2)) `总计GMV`
    ,concat(round((gmv / gmv_ly - 1) * 100,2),'%') `总计GMVYoY`
    ,night night 
    ,concat(round((night / night_ly - 1) * 100,2),'%') `总计nightYoY`
from l ;
