with house_info as (
  select
    distinct hotel_id,
    hotel_name,
    landlord_id,
    house_id,
    landlord_channel_name,
    comment_score,
    recommended_guest,
    house_city_id,
    house_city_name
  from
    dws.dws_house_d
  where
    dt = date_sub(current_date, 1)
    and house_is_online = 1
    and hotel_is_online = 1
    and house_is_oversea = 1
),
trans_time as (
  select
    distinct id as city_id,
    cast(
      SUBSTRING(get_json_object(attrs, '$.gmt'), 5, 2) as int
    ) -8 beijing_hour_diff_gmt,
    cast(
      SUBSTRING(get_json_object(attrs, '$.dst'), 5, 2) as int
    ) -8 beijing_hour_diff_dst,
    concat(
      year(current_date),
      "-",
      regexp_replace(get_json_object(attrs, "$.dstBt"), "/", "-")
    ) as dst_begin,
    concat(
      year(current_date),
      "-",
      regexp_replace(get_json_object(attrs, "$.dstEt"), "/", "-")
    ) as dst_end
  from
    ods_tujia_pg.city
  where
    status = 'on'
    and is_inland = false
),
comment_info as (
    select hotelmanagementcompanyid as hotel_id,
        round(count(distinct case when totalscore >= 4 then commentid end) / count(distinct commentid),4) as good_comment_rate
    from ods_tujiacustomer.comment a
    join house_info b on b.house_id = a.unitid
    where to_date(checkoutdate) >= date_sub(current_date, 90)
    and totalscore > 0
    group by 1
),
order_info as (
select
*,
regexp_replace( CASE  
    WHEN (paid_order_cnt <= 3 or paid_order_cnt is null) THEN concat(round( GREATEST(1-cast(reject_rate as double), 0.9)*100,0),'%') 
    when paid_order_cnt>3 then
        case
            WHEN reject_rate = 0 THEN '100%'  
            WHEN reject_rate BETWEEN 0 AND 0.05 THEN '99%' 
            WHEN reject_rate BETWEEN 0.05 AND 0.10 THEN '97%'  
            WHEN reject_rate BETWEEN 0.10 AND 0.20 THEN '95%'  
            WHEN reject_rate BETWEEN 0.20 AND 0.30 THEN '90%'  
            WHEN reject_rate BETWEEN 0.30 AND 0.40 THEN '85%'  
            WHEN reject_rate BETWEEN 0.40 AND 0.50 THEN '80%'  
            WHEN reject_rate BETWEEN 0.50 AND 0.60 THEN '75%'  
            ELSE concat(round(LEAST(1 - cast(reject_rate as double), 0.5)*100,0),'%')
        END 
end,'\\.0%','%') AS confirm_rate_v1
from
(
  select
    hotel_id,
    count(
      distinct case
        when is_paysuccess_order = 1 then order_no
      end
    ) as paid_order_cnt,
    count(
      distinct case
        when is_paysuccess_order = 1 
        and is_cancel_order = 0 then order_no
      end
    ) as valid_paid_order_cnt,
    sum(
      distinct case
        when is_paysuccess_order = 1
        and is_cancel_order = 0 then order_room_night_count
      end
    ) as valid_paid_nights_cnt,
    round(
      sum(
        distinct case
          when is_paysuccess_order = 1
          and is_cancel_order = 0 then room_total_amount
        end
      ),
      2
    ) as valid_paid_gmv,
    count(
      distinct case
        when is_cancel_order = 1 then order_no
      end
    ) as cancel_order_cnt,
    round(
      count(
        distinct case
          when is_paysuccess_order = 1
          and is_cancel_order = 1 then order_no
        end
      ) / count(
        distinct case
          when is_paysuccess_order = 1 then order_no
        end
      ),
      2
    ) cancel_rate,
    count(
      distinct case
        when is_reject_order = 1 then order_no
      end
    ) as reject_order_cnt,
    round(
      count(
        distinct case
          when is_reject_order = 1 then order_no
        end
      ) / count(
        distinct case
          when is_paysuccess_order = 1 then order_no
        end
      ),
      2
    ) reject_rate
  from
    dws.dws_order
  where
    to_date(create_time) >= date_sub(current_date, 90)
  group by
    1
) t1

),
order_taking_time as (
  --接单时长
  select
    hotel_id,
    cast(avg(order_confirm_minutes) as int) as order_confirm_minutes
  from
    (
      select
        order_no,
        house_id,
        create_time,
        confirm_time,
        difference_in_minutes,
        city_name,
        city_id,
        hotel_id,
        beijing_hour_diff_gmt,
        beijing_hour_diff_dst,
        dst_begin,
        dst_end,
        create_time_beijing,
        confirm_time_beijing,
        orderLatestTime,
        start_order_time,
        start_order_time_beijing,
        second_day_start_order_time_local,
        second_day_start_order_time_beijing,
        end_order_time,
        end_order_time_beijing,
        second_day_end_order_time_beijing,
        cast(
          case
            when create_time_beijing < start_order_time_beijing
            and confirm_time_beijing < start_order_time_beijing then 60
            when create_time_beijing < start_order_time_beijing
            and confirm_time_beijing between start_order_time_beijing
            and end_order_time_beijing then unix_timestamp(confirm_time_beijing) - unix_timestamp(start_order_time_beijing)
            when create_time_beijing < start_order_time_beijing
            and confirm_time_beijing between end_order_time
            and second_day_start_order_time_beijing then unix_timestamp(end_order_time) - unix_timestamp(create_time_beijing)
            when create_time_beijing < start_order_time_beijing
            and confirm_time_beijing between second_day_start_order_time_beijing
            and second_day_end_order_time_beijing then unix_timestamp(confirm_time_beijing) - unix_timestamp(second_day_start_order_time_beijing) + unix_timestamp(end_order_time_beijing) - unix_timestamp(start_order_time_beijing)
            when create_time_beijing < start_order_time_beijing
            and confirm_time_beijing > second_day_end_order_time_beijing then unix_timestamp(second_day_end_order_time_beijing) - unix_timestamp(second_day_start_order_time_beijing) + unix_timestamp(end_order_time_beijing) - unix_timestamp(start_order_time_beijing)
            when create_time_beijing < end_order_time_beijing
            and confirm_time_beijing between end_order_time_beijing
            and second_day_start_order_time_beijing then unix_timestamp(end_order_time_beijing) - unix_timestamp(create_time_beijing)
            when create_time_beijing < end_order_time_beijing
            and confirm_time_beijing between second_day_start_order_time_beijing
            and second_day_end_order_time_beijing then unix_timestamp(confirm_time_beijing) - unix_timestamp(second_day_start_order_time_beijing) + unix_timestamp(end_order_time_beijing) - unix_timestamp(create_time_beijing)
            when create_time_beijing < end_order_time_beijing
            and confirm_time_beijing > second_day_end_order_time_beijing then unix_timestamp(confirm_time_beijing) - unix_timestamp(second_day_end_order_time_beijing) + unix_timestamp(second_day_end_order_time_beijing) - unix_timestamp(second_day_start_order_time_beijing) + unix_timestamp(end_order_time_beijing) - unix_timestamp(create_time_beijing)
            when create_time_beijing between end_order_time_beijing
            and second_day_start_order_time_beijing
            and confirm_time_beijing < second_day_start_order_time_beijing then 60
            when create_time_beijing between end_order_time_beijing
            and second_day_start_order_time_beijing
            and confirm_time_beijing between second_day_start_order_time_beijing
            and second_day_end_order_time_beijing then unix_timestamp(confirm_time_beijing) - unix_timestamp(second_day_start_order_time_beijing)
            when create_time_beijing between end_order_time_beijing
            and second_day_start_order_time_beijing
            and confirm_time_beijing > second_day_end_order_time_beijing then unix_timestamp(second_day_end_order_time_beijing) - unix_timestamp(second_day_start_order_time_beijing)
            else unix_timestamp(confirm_time_beijing) - unix_timestamp(create_time_beijing)
          end / 60 as int
        ) as order_confirm_minutes
      from
        (
          select
            order_no,
            house_id,
            create_time,
            confirm_time,
            difference_in_minutes,
            city_name,
            city_id,
            hotel_id,
            beijing_hour_diff_gmt,
            beijing_hour_diff_dst,
            dst_begin,
            dst_end,
            create_time_beijing,
            confirm_time_beijing,
            orderLatestTime,
            start_order_time,
            case
              when to_date(start_order_time) between dst_begin
              and dst_end then from_unixtime(
                unix_timestamp(start_order_time) - beijing_hour_diff_dst * 3600
              )
              else from_unixtime(
                unix_timestamp(start_order_time) - beijing_hour_diff_gmt * 3600
              )
            end as start_order_time_beijing,
            second_day_start_order_time_local,
            second_day_start_order_time_beijing,
            end_order_time,
            end_order_time_beijing,
            from_unixtime(
              unix_timestamp(end_order_time_beijing) + 24 * 3600
            ) as second_day_end_order_time_beijing
          from
            (
              select
                order_no,
                house_id,
                create_time,
                confirm_time,
                difference_in_minutes,
                city_name,
                city_id,
                hotel_id,
                beijing_hour_diff_gmt,
                beijing_hour_diff_dst,
                dst_begin,
                dst_end,
                create_time_beijing,
                confirm_time_beijing,
                orderLatestTime,
                start_order_time,
                concat(date_add(start_order_time, 1), " 09:00:00") as second_day_start_order_time_local,
                from_unixtime(
                  unix_timestamp(start_order_time_beijing) + 24 * 3600
                ) as second_day_start_order_time_beijing,
                end_order_time,
                case
                  when to_date(confirm_time_beijing) between dst_begin
                  and dst_end then from_unixtime(
                    unix_timestamp(end_order_time) - beijing_hour_diff_dst * 3600
                  )
                  else from_unixtime(
                    unix_timestamp(end_order_time) - beijing_hour_diff_gmt * 3600
                  )
                end as end_order_time_beijing
              from
                (
                  select
                    order_no,
                    house_id,
                    create_time,
                    confirm_time,
                    difference_in_minutes,
                    city_name,
                    city_id,
                    hotel_id,
                    beijing_hour_diff_gmt,
                    beijing_hour_diff_dst,
                    dst_begin,
                    dst_end,
                    create_time_beijing,
                    confirm_time_beijing,
                    orderLatestTime,
                    concat(to_date(create_time_beijing), " 09:00:00") as start_order_time,
                    case
                      when to_date(
                        concat(to_date(create_time_beijing), " 09:00:00")
                      ) between dst_begin
                      and dst_end then from_unixtime(
                        unix_timestamp(
                          concat(to_date(create_time_beijing), " 09:00:00")
                        ) - beijing_hour_diff_dst * 3600
                      )
                      else from_unixtime(
                        unix_timestamp(
                          concat(to_date(create_time_beijing), " 09:00:00")
                        ) - beijing_hour_diff_gmt * 3600
                      )
                    end as start_order_time_beijing,
                    case
                      when (
                        orderLatestTime = -1
                        or orderLatestTime is null
                      ) then null
                      else from_unixtime(
                        unix_timestamp(
                          concat(to_date(create_time_beijing), " 00:00:00")
                        ) + orderLatestTime * 60
                      )
                    end as end_order_time
                  from
                    (
                      select
                        order_no,
                        a.house_id,
                        create_time,
                        confirm_time,
                        difference_in_minutes,
                        city_name,
                        a.city_id,
                        beijing_hour_diff_gmt,
                        beijing_hour_diff_dst,
                        dst_begin,
                        hotel_id,
                        dst_end,
                        create_time as create_time_beijing,
                        confirm_time as confirm_time_beijing,
                        orderLatestTime
                      from
                        (
                          select
                            order_no,
                            house_id,
                            create_time,
                            confirm_time,
                            cast(
                              (
                                unix_timestamp(confirm_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')
                              ) / 60 as int
                            ) AS difference_in_minutes,
                            city_id,
                            city_name,
                            hotel_id
                          from
                            dws.dws_order
                          where
                            (
                              is_cancel_order = 0
                              or is_reject_order = 1
                            )
                            and is_overseas = 1
                            and create_time >= date_sub(current_date, 90)
                        ) a
                        left join trans_time b on a.city_id = b.city_id
                        left join (
                          select
                            house_id,
                            get_json_object(checkin_instructions, "$.orderLatestTime") orderLatestTime
                          from
                            ods_tns_baseinfo.house_info
                        ) b on a.house_id = b.house_id
                    ) aa
                ) aaa
            ) b
        ) bb
    ) bbb
  where
    order_confirm_minutes >= 0
  group by
    1
),
reply_minutes as (
),
landlord_profile_info as (
select
hotel_id,
to_json(
  named_struct(
    'hotel_id',
    hotel_id,
    'landlord_born_date',
    landlord_born_date,
    'landlord_sex',
    landlord_sex,
    'base_city',
    base_city,
    'is_chinese_able',
    is_chinese_able,
    'language_besides_chinese',
    language_besides_chinese,
    'experience',
    experience,
    'service',
    service,
    'why_open_bnb',
    why_open_bnb,
    'bnb_design_philosophy',
    bnb_design_philosophy,
    'hobby',
    hobby,
    'highlight_moment',
    highlight_moment,
    'guide_list',
    guide_list,
    'generation',
    generation,
    'language',
    language
  )
) as json_info
from
    (
select
    hotel_id,
    landlord_born_date,
    landlord_sex,
    base_city,
    if(is_chinese_able>0,1,0) as is_chinese_able,
    language_besides_chinese,
    experience,
    service,
    why_open_bnb,
    bnb_design_philosophy,
    hobby,
    highlight_moment,
    guide_list,
    case when concat(substr(landlord_born_date,3,1),'0后')='0后' then null end as generation,
    if(is_chinese_able>0,regexp_replace(regexp_replace(language_besides_chinese,'\\[','[\'中文\'\,'),',\'\'',''),language_besides_chinese) as language
    from
    excel_upload.overseas_platform_hotel_profile_20240729
    ) json_tab_T
    union all
select
    hotel_id,
    to_json(
    named_struct(
    'hotel_id',
    hotel_id,
    'hotel_name_en',
    hotel_name_eng,
    'language',
    language,
    'is_chinese_able',
    is_chinese_able,
    'is_english_able',
    is_english_able,
    'open_date',
    openTime,
    'furnish_date',
    furnish_date
    ) 
    )as json_info
    from
        (
        select distinct 
            t1.hotel_id,
            hotel_name_eng,
            if(is_chinese_able>0,regexp_replace(regexp_replace(language,'\\[','[\'中文\'\,'),',\'\'',''),language) as language,
            if(is_chinese_able>0,1,0) is_chinese_able,
            if(is_chinese_able=0 and is_english_able>0,1,0) is_english_able,
            openTime,
            furnish_date
        from(
            select a.hotel_id,service_language,
            concat('[\'',concat_ws('\',\'',collect_set(if(language_cn='中文',null,language_cn))),'\']') as language,
                sum(if(language_cn='中文',1,0)) is_chinese_able,
                sum(if(language_cn='英语',1,0)) is_english_able
            from(
                select distinct house_id,hotel_id,service_language
                    ,EXPLODE(SPLIT( regexp_replace(service_language,'\\[|\\]|"',''), ',')) AS language_en --str型list 行转列
                from dwd.dwd_house_ctrip_hotel_facility_d
                where dt=date_sub(current_date(),1) and service_language is not null
            )a
            join excel_upload.ctrip_frontdest_language_mp_0719 b on a.language_en=b.language_en
            join (select hotel_id from dws.dws_house_d where dt=date_sub(current_date(),1) and  landlord_channel_name='API_携程海外') as c on a.hotel_id=c.hotel_id
            group by a.hotel_id,service_language
        )t1
           left join
            (select
            hotel_id,
            max(openTime) as openTime,
            max(furnish_date) as furnish_date
            from
            (select
            hotel_id,
              get_json_object(base_info, '$.openTime') openTime,
              get_json_object(base_info, '$.renovationTime') furnish_date,
              *
            from(
                select
                  hotel_id,
                  base_info,
                  sum(1) over(
                    partition by house_id
                    order by
                      last_update_time desc
                  ) rn
                from
                  ods_tns_baseinfo.house_info
                where
                  active = 1
                  and can_sale = 1
                  and audit_status = 2
              ) a
            where
              rn = 1 
            ) b
            where
             openTime > '1990-01-01'
            and openTime < date_sub(current_date(), 1)
            and furnish_date >'1990-01-01'
            and furnish_date <date_sub(current_date(),1)
            and furnish_date>=openTime
            group by hotel_id
            ) t2
            on
            t1.hotel_id=t2.hotel_id
            join 
            ods_tns_baseinfo.hotel t3 on t1.hotel_id=t3.hotel_id
    ) json_tab_C
union all
select
hotel_id,
to_json(
named_struct(
'hotel_id',
hotel_id,
'hotel_name_en',
hotel_name_en,
'is_chinese_able',
is_chinese_able,
'is_english_able',
is_english_able,
'language',
language
)
) as json_info
from
(select
  distinct t1.hotel_id,
  hotel_name_eng as hotel_name_en,
  if(is_chinese_able>0,regexp_replace(regexp_replace(language,'\\[','[\'中文\'\,'),',\'\'',''),language) as language,
  if(is_chinese_able > 0, 1, 0) is_chinese_able,
  if(
    is_chinese_able = 0
    and is_english_able > 0,
    1,
    0
  ) is_english_able
from(
    select
      t1.hotel_id,
    concat('[\'',concat_ws('\',\'',collect_set(if(language_cn='中文',null,language_cn))),'\']') as language,
      sum(if(language_cn = '中文', 1, 0)) is_chinese_able,
      sum(if(language_cn = '英语', 1, 0)) is_english_able
    from(
        select
           hotel_id,
          translatedName
        from
          (
            select
              get_json_object(c.json_string, '$.propertyGroupDescription') propertyGroupDescription,
              get_json_object(c.json_string, '$.propertyTranslatedName') translatedName,
              t_hotel_id hotel_id
            from
              (
                select
                t1.*,
                t2.hotel_id as t_hotel_id
                from
                (select
                  hotel_id,
                  get_json_object(url_data, '$.hotelFullFeed.facilities.facility') as Facility
                from
                  dwd.dwd_other_agoda_info_d
                where
                  dt = date_sub(current_date(), 1)
                  and feed_id = 19) t1
                join 
                (
                  select * 
                  from ods_houseimport_config.api_unit 
                  where status=1 
                  and merchant_guid ='bf28d03e-5b25-4c2e-8789-486cf587d978' and unit_id > 0 
                ) t2 
                on t2.partner_hotel_id = t1.hotel_id
              ) a lateral view explode(udf.json_split(Facility)) b as c
          ) t1
        where
          propertyGroupDescription='Languages spoken'
      ) t1
      join  excel_upload.agoda_frontdesk_language_mp_0729 t2 on t1.translatedName = t2.translatedName
    group by
      t1.hotel_id
  ) t1
  join ods_tns_baseinfo.hotel t3 on t1.hotel_id=t3.hotel_id
) json_info_A
)
select
  distinct a.hotel_id,
  hotel_name,
  a.landlord_channel_name,
  good_comment_rate,
  paid_order_cnt,
  valid_paid_nights_cnt,
  valid_paid_gmv,
  cancel_order_cnt,
  cancel_rate,
  reject_order_cnt,
  reject_rate,
  order_confirm_minutes,
  reply_minutes_avg,
  valid_reply_minutes_avg,
  reply_rate,
  without_order_reply_rate,
  with_order_reply_rate,
  json_info landlord_profile_ex,
  c.valid_paid_order_cnt,
  confirm_rate_v1,
  CASE  
        -- 已支付订单≤3 且 order_confirm_minutes>2小时  
        WHEN (paid_order_cnt <= 3 or paid_order_cnt is null ) AND order_confirm_minutes > 120 THEN NULL  
        WHEN paid_order_cnt <= 3 AND order_confirm_minutes <= 120 THEN CONCAT('平均',round(if(order_confirm_minutes=0,1,order_confirm_minutes),0), '分钟确认')   
        -- 已支付订单>3 的情况  
        WHEN paid_order_cnt > 3 THEN  
            CASE  
                when order_confirm_minutes =0 then '平均1分钟确认'
                WHEN order_confirm_minutes BETWEEN 1 AND 120 THEN CONCAT('平均', round(order_confirm_minutes,0), '分钟确认')  
                WHEN order_confirm_minutes BETWEEN 121 AND 180 THEN '平均3小时确认'  
                WHEN order_confirm_minutes BETWEEN 181 AND 240 THEN '平均4小时确认'  
                WHEN order_confirm_minutes BETWEEN 241 AND 300 THEN '平均5小时确认'  
                WHEN order_confirm_minutes BETWEEN 301 AND 360 THEN '平均6小时确认'  
                WHEN order_confirm_minutes > 360 THEN NULL  
            END    
    END AS order_confirm_time_v1,
    CASE
        WHEN (paid_order_cnt <= 3 or paid_order_cnt is null) and valid_reply_minutes_avg is not null THEN if(LEAST(round(cast(valid_reply_minutes_avg as int) / 60,0), 2)=2,'平均2小时回复',concat('平均',round(if(valid_reply_minutes_avg=0,1,valid_reply_minutes_avg),0),'分钟回复')) -- 假设valid_reply_minutes_avg是以分钟为单位，转换为小时  
        -- 已支付订单>3  
        WHEN paid_order_cnt > 3 THEN  
            CASE  
                when valid_reply_minutes_avg =0 then '平均1分钟回复'
                WHEN valid_reply_minutes_avg BETWEEN 1 AND 120 THEN CONCAT('平均', round(valid_reply_minutes_avg,0), '分钟回复')  
                WHEN valid_reply_minutes_avg BETWEEN 120 AND 180 THEN '平均3小时回复'  
                WHEN valid_reply_minutes_avg BETWEEN 180 AND 240 THEN '平均4小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 240 AND 300 THEN '平均5小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*5 AND 60*6 THEN '平均6小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*6 AND 60*7 THEN '平均7小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*7 AND 60*8 THEN '平均8小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*8 AND 60*9 THEN '平均9小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*9 AND 60*10 THEN '平均10小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*10 AND 60*11 THEN '平均11小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*11 AND 60*12 THEN '平均12小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*12 AND 60*13 THEN '平均13小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*13 AND 60*14 THEN '平均14小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*14 AND 60*15 THEN '平均15小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*15 AND 60*16 THEN '平均16小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*16 AND 60*17 THEN '平均17小时回复'
                WHEN valid_reply_minutes_avg BETWEEN 60*17 AND 60*18 THEN '平均18小时回复'  
                WHEN valid_reply_minutes_avg > 60*18 THEN '消息回复不及时'  
            END  
    END AS valid_reply_minutes_avg_v1,
    regexp_replace( CASE  
        WHEN consult_cnt <= 5 and reply_rate < 0.8 THEN NULL  
        when consult_cnt <=5 and reply_rate>=0.8 then concat(round(cast(reply_rate as double)*100,0),'%')
        WHEN consult_cnt > 5 THEN
            case  
                when reject_consult_cnt<=3 then '95%'
                when reject_consult_cnt>3 then concat(round(100-(cast(reject_consult_cnt as int)-3)*100/consult_cnt,0),'%')
            end
    END,'\\.0%','%') AS reply_rate_v1,
    reject_consult_cnt,
    consult_cnt
from
  house_info a
  left join comment_info b on a.hotel_id = b.hotel_id
  left join order_info c on a.hotel_id = c.hotel_id
  left join order_taking_time d on a.hotel_id = d.hotel_id
  left join reply_minutes e on a.hotel_id = e.hotel_id
  left join landlord_profile_info f on a.hotel_id = f.hotel_id