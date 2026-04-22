with house_info as
         (select distinct t1.dt,
                          case when house_city_name = '陵水' then '陵水(三亚)' else house_city_name end as city_name,
                          house_city_id                                                                 as city_id,
                          t1.house_id,
                          hotel_id,
                          hotel_level,
                          is_prefer,
                          is_prefer_pro,
                          bedroom_count,
                          t1.dynamic_business,
                          t1.dynamic_business_id,
                          case
                              when landlord_channel = 303 then '携程接入'
                              else '直采' end                                                           as source_type,
                          instance_count,
                          share_type,
                          house_type,
                          house_level,
                          house_class
          from dws.dws_house_d t1
          where t1.dt = date_sub(current_date, 1)
            and house_is_oversea = '0'
            and house_is_online = 1),
     langterm as
         (
--C接降权扩量
             select "8"                                       as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id                    as dynamic_id
                  , t1.dynamic_business
                  , "landmark"                                as scene
                  , "2"                                       as filter_type
                  , "2025-02-05"                              as begin_date
                  , "2030-12-31"                              as end_date
                  , "2"                                       as rank_tag_id
                  , "0"                                       as can_sale_num
                  , "0"                                       as can_sale_max_num
                  , "0"                                       as can_sale_percent
                  , "0"                                       as price_rise_limit
                  , "0,9999"                                  as stay_days_range
                  , "2"                                       as is_major_price
                  , "0,99999"                                 as price_range
                  , "D,E,F,G,H,I,J,K"                         as buckets
                  , case
                        when jiaquan_cr <= 0.6 then "-3.5"
                        when jiaquan_cr <= 0.7 then "-3"
                        when jiaquan_cr <= 0.8 then "-2.5"
                        when jiaquan_cr <= 0.9 then "-2"
                        when jiaquan_cr <= 1 then "-1.5"
                 end                                          as score
                  , "1"                                       as status
                  , "ALL"                                     AS search_option_labels
                  , "2,310,603,980,1039,613,1037,6,1013,1596" AS from_for_logs
                  , "1"                                       AS personalize_type
                  , "0"                                       AS user_value_type
                  , "true"                                    AS enablescoreaccumulation
                  , "ALL"                                     AS check_type
                  , "3"                                       AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , cast(jiaquan_cr as double) as jiaquan_cr
                   from pdb_analysis_c.ads_flow_cj_low_d
                   where search_type = '地标'
                     and ord_all_z >= 50
                     and ord_cj >= 5
                     and cast(jiaquan_cr as double) < 1
                     and dt = date_sub(current_date, 1)
                     and cast(l21pluszc_cr as double) > 1
                     and cast(l2d_diff as double) > '-0.03') as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
             union
             select t2.wrapper_name        as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , t2.search_type         as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "2"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "200,99999"            as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , case
                        when detail_cr <= 0.6 then "-3.5"
                        when detail_cr <= 0.7 then "-3"
                        when detail_cr <= 0.8 then "-2.5"
                        when detail_cr <= 0.9 then "-2"
                        when detail_cr <= 1 then "-1.5"
                 end                       as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "1"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , cast(detail_cr as double) as detail_cr
                        , case
                              when wrapper_name = '途家' then '1'
                              when wrapper_name = '携程' then '8'
                              when wrapper_name = '去哪儿' then '2'
                          end                       as wrapper_name
                        , case
                              when search_type = '地标' then 'landmark'
                              when search_type = '空搜' then 'city'
                              when search_type = '行政区' then 'district'
                          end                       as search_type
                   from pdb_analysis_c.ads_flow_cj_decr_all_d
                   where search_type in ('地标', '空搜', '行政区')
                     and dt = date_sub(current_date, 1)
                     and wrapper_name <> '途家'
                     and cast(l21pluszc_cr as double) > 1
                     and cast(l2d_diff as double) > '-0.03') as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
                      left join
                  (select city_id
                        , dynamic_business_id
                        , jiaquan_cr
                        , "landmark" as search_type
                        , "8"        as wrapper_name
                   from pdb_analysis_c.ads_flow_cj_low_d
                   where search_type = '地标'
                     and ord_all_z >= 50
                     and ord_cj >= 5
                     and cast(jiaquan_cr as double) < 1
                     and dt = date_sub(current_date, 1)
                     and cast(l21pluszc_cr as double) > 1
                     and cast(l2d_diff as double) > '-0.03') as t3
                  on t2.city_id = t3.city_id
                      and t2.dynamic_business_id = t3.dynamic_business_id
                      and t2.search_type = t3.search_type
                      and t2.wrapper_name = t3.wrapper_name
             where t3.dynamic_business_id is null

--C接降权扩量-低价值用户只对高价降权
             union
             select "8"                                       as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id                    as dynamic_id
                  , t1.dynamic_business
                  , "landmark"                                as scene
                  , "2"                                       as filter_type
                  , "2025-02-05"                              as begin_date
                  , "2030-12-31"                              as end_date
                  , "2"                                       as rank_tag_id
                  , "0"                                       as can_sale_num
                  , "0"                                       as can_sale_max_num
                  , "0"                                       as can_sale_percent
                  , "0"                                       as price_rise_limit
                  , "0,9999"                                  as stay_days_range
                  , "2"                                       as is_major_price
                  , "200,99999"                               as price_range
                  , "D,E,F,G,H,I,J,K"                         as buckets
                  , case
                        when jiaquan_cr <= 0.6 then "-3.5"
                        when jiaquan_cr <= 0.7 then "-3"
                        when jiaquan_cr <= 0.8 then "-2.5"
                        when jiaquan_cr <= 0.9 then "-2"
                        when jiaquan_cr <= 1 then "-1.5"
                 end                                          as score
                  , "1"                                       as status
                  , "ALL"                                     AS search_option_labels
                  , "2,310,603,980,1039,613,1037,6,1013,1596" AS from_for_logs
                  , "1"                                       AS personalize_type
                  , "1"                                       AS user_value_type
                  , "true"                                    AS enablescoreaccumulation
                  , "ALL"                                     AS check_type
                  , "3"                                       AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , cast(jiaquan_cr as double) as jiaquan_cr
                   from pdb_analysis_c.ads_flow_cj_low_d
                   where search_type = '地标'
                     and ord_all_z >= 50
                     and ord_cj >= 5
                     and cast(jiaquan_cr as double) < 1
                     and dt = date_sub(current_date, 1)
                     and cast(l21pluszc_cr as double) > 1
                     and cast(l2d_diff as double) > '-0.03') as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
             union
             select t2.wrapper_name        as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , t2.search_type         as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "2"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , case
                        when detail_cr <= 0.6 then "-3.5"
                        when detail_cr <= 0.7 then "-3"
                        when detail_cr <= 0.8 then "-2.5"
                        when detail_cr <= 0.9 then "-2"
                        when detail_cr <= 1 then "-1.5"
                 end                       as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "1"                    AS personalize_type
                  , "1"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , cast(detail_cr as double) as detail_cr
                        , case
                              when wrapper_name = '途家' then '1'
                              when wrapper_name = '携程' then '8'
                              when wrapper_name = '去哪儿' then '2'
                          end                       as wrapper_name
                        , case
                              when search_type = '地标' then 'landmark'
                              when search_type = '空搜' then 'city'
                              when search_type = '行政区' then 'district'
                          end                       as search_type
                   from pdb_analysis_c.ads_flow_cj_decr_all_d
                   where search_type in ('地标', '空搜', '行政区')
                     and dt = date_sub(current_date, 1)
                     and wrapper_name <> '途家'
                     and cast(l21pluszc_cr as double) > 1
                     and cast(l2d_diff as double) > '-0.03') as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
                      left join
                  (select city_id
                        , dynamic_business_id
                        , jiaquan_cr
                        , "landmark" as search_type
                        , "8"        as wrapper_name
                   from pdb_analysis_c.ads_flow_cj_low_d
                   where search_type = '地标'
                     and ord_all_z >= 50
                     and ord_cj >= 5
                     and cast(jiaquan_cr as double) < 1
                     and dt = date_sub(current_date, 1)
                     and cast(l21pluszc_cr as double) > 1
                     and cast(l2d_diff as double) > '-0.03') as t3
                  on t2.city_id = t3.city_id
                      and t2.dynamic_business_id = t3.dynamic_business_id
                      and t2.search_type = t3.search_type
                      and t2.wrapper_name = t3.wrapper_name
             where t3.dynamic_business_id is null

----L1扣分

             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "3"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,H"                as buckets
                  , "-3"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "3"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,H"                as buckets
                  , "-3"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "3"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,H"                as buckets
                  , "-3"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "3"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,H"                as buckets
                  , "-3"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1

---C接空搜场景基于商圈cr降权
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "2"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-3"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join(select distinct city_id
                                         , dynamic_business_id
                                         , cast(ord_zhanbi as double) as ord_zhanbi
                           from pdb_analysis_c.ads_flow_city_dynamic_loc_or_other_d
                           where dt = date_sub(current_date, 1)
                             and loc_or_other = '异地'
                             and cast(ord_zhanbi as double) < 0.03) t2
                          on t1.city_id = t2.city_id
                              and t1.dynamic_business_id = t2.dynamic_business_id

             --C接扣分
--     union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"district" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"4" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-10" as score
--      ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--   union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"landmark" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"4" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-10" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--      union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"city" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"4" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-10" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--  union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"locating" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"4" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-10" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1


--C接金特扣分20---25年1月21日暂停
--     union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"district" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--      ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--   union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"landmark" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--      union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"city" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--  union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"locating" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1

--C接金特扣分21---25年1月21日暂停
--         union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"district" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--      ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--   union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"landmark" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--      union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"city" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--  union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"locating" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-3" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--历史全量配置
             union
             select channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_id
                  , t1.dynamic_business
                  , scene
                  , filter_type
                  , begin_date
                  , end_date
                  , rank_tag_id
                  , can_sale_num
                  , can_sale_max_num
                  , can_sale_percent
                  , price_rise_limit
                  , stay_days_range
                  , is_major_price
                  , price_range
                  , buckets
                  , score
                  , status
                  , search_option_labels
                  , from_for_logs
                  , personalize_type
                  , user_value_type
                  , enablescoreaccumulation
                  , check_type
                  , bucket_type
             from (select *
                   from pdb_analysis_c.ads_flow_rare_house_jiaquan_all_d
                   where dt = date_sub(current_date, 1)) as t1

--不活跃房屋扣分L1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "41"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "41"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "41"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "41"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
--不活跃房屋扣分L0
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "42"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "42"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "42"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "42"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E"                  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
--海外低点评降权
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "5"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "B,C,D,E,F,G,H,I,J,K"  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct house_city_id   as city_id
                                 , house_city_name as city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from dws.dws_house_d
                   where dt = date_sub(current_date, 1)
                     and house_is_oversea = '1'
                     and dynamic_business_id > 0) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "5"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "B,C,D,E,F,G,H,I,J,K"  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct house_city_id   as city_id
                                 , house_city_name as city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from dws.dws_house_d
                   where dt = date_sub(current_date, 1)
                     and house_is_oversea = '1'
                     and dynamic_business_id > 0) as t1
--海外低点评门店降权
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "6"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "B,C,D,E,F,G,H,I,J,K"  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct house_city_id   as city_id
                                 , house_city_name as city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from dws.dws_house_d
                   where dt = date_sub(current_date, 1)
                     and house_is_oversea = '1'
                     and dynamic_business_id > 0) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "6"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "B,C,D,E,F,G,H,I,J,K"  as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct house_city_id   as city_id
                                 , house_city_name as city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from dws.dws_house_d
                   where dt = date_sub(current_date, 1)
                     and house_is_oversea = '1'
                     and dynamic_business_id > 0) as t1

--C渠道酒店三星七折
             union
             select "8"                              as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id           as dynamic_id
                  , t1.dynamic_business
                  , "landmark"                       as scene
                  , "2"                              as filter_type
                  , "2023-09-01"                     as begin_date
                  , "2030-12-31"                     as end_date
                  , "10"                             as rank_tag_id
                  , "0"                              as can_sale_num
                  , "0"                              as can_sale_max_num
                  , "0"                              as can_sale_percent
                  , "0"                              as price_rise_limit
                  , "0,9999"                         as stay_days_range
                  , "2"                              as is_major_price
                  , concat('0,', cast(price as int)) as price_range
                  , "D,E,F,G,H,I,J,K"                as buckets
                  , "-1.5"                           as score
                  , "1"                              as status
                  , "ALL"                            AS search_option_labels
                  , "ALL"                            AS from_for_logs
                  , "1"                              AS personalize_type
                  , "0"                              AS user_value_type
                  , "true"                           AS enablescoreaccumulation
                  , "ALL"                            AS check_type
                  , "3"                              AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , case
                              when cast(hotel_three_adr_middle as double) >= cast(bnb_one_adr_middle as double)
                                  then bnb_one_adr_middle
                              else hotel_three_adr_middle end as price
                   from pdb_analysis_c.ads_flow_hotel_dy_adr_d
                   where dt = date_sub(current_date, 1)) as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
             union
             select "8"                              as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id           as dynamic_id
                  , t1.dynamic_business
                  , "city"                           as scene
                  , "2"                              as filter_type
                  , "2023-09-01"                     as begin_date
                  , "2030-12-31"                     as end_date
                  , "10"                             as rank_tag_id
                  , "0"                              as can_sale_num
                  , "0"                              as can_sale_max_num
                  , "0"                              as can_sale_percent
                  , "0"                              as price_rise_limit
                  , "0,9999"                         as stay_days_range
                  , "2"                              as is_major_price
                  , concat('0,', cast(price as int)) as price_range
                  , "D,E,F,G,H,I,J,K"                as buckets
                  , "-1.5"                           as score
                  , "1"                              as status
                  , "ALL"                            AS search_option_labels
                  , "ALL"                            AS from_for_logs
                  , "1"                              AS personalize_type
                  , "0"                              AS user_value_type
                  , "true"                           AS enablescoreaccumulation
                  , "ALL"                            AS check_type
                  , "3"                              AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , case
                              when cast(hotel_three_adr_middle as double) >= cast(bnb_one_adr_middle as double)
                                  then bnb_one_adr_middle
                              else hotel_three_adr_middle end as price
                   from pdb_analysis_c.ads_flow_hotel_dy_adr_d
                   where dt = date_sub(current_date, 1)) as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
             union
             select "8"                              as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id           as dynamic_id
                  , t1.dynamic_business
                  , "district"                       as scene
                  , "2"                              as filter_type
                  , "2023-09-01"                     as begin_date
                  , "2030-12-31"                     as end_date
                  , "10"                             as rank_tag_id
                  , "0"                              as can_sale_num
                  , "0"                              as can_sale_max_num
                  , "0"                              as can_sale_percent
                  , "0"                              as price_rise_limit
                  , "0,9999"                         as stay_days_range
                  , "2"                              as is_major_price
                  , concat('0,', cast(price as int)) as price_range
                  , "D,E,F,G,H,I,J,K"                as buckets
                  , "-1.5"                           as score
                  , "1"                              as status
                  , "ALL"                            AS search_option_labels
                  , "ALL"                            AS from_for_logs
                  , "1"                              AS personalize_type
                  , "0"                              AS user_value_type
                  , "true"                           AS enablescoreaccumulation
                  , "ALL"                            AS check_type
                  , "3"                              AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , case
                              when cast(hotel_three_adr_middle as double) >= cast(bnb_one_adr_middle as double)
                                  then bnb_one_adr_middle
                              else hotel_three_adr_middle end as price
                   from pdb_analysis_c.ads_flow_hotel_dy_adr_d
                   where dt = date_sub(current_date, 1)) as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
             union
             select "8"                              as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id           as dynamic_id
                  , t1.dynamic_business
                  , "locating"                       as scene
                  , "2"                              as filter_type
                  , "2023-09-01"                     as begin_date
                  , "2030-12-31"                     as end_date
                  , "10"                             as rank_tag_id
                  , "0"                              as can_sale_num
                  , "0"                              as can_sale_max_num
                  , "0"                              as can_sale_percent
                  , "0"                              as price_rise_limit
                  , "0,9999"                         as stay_days_range
                  , "2"                              as is_major_price
                  , concat('0,', cast(price as int)) as price_range
                  , "D,E,F,G,H,I,J,K"                as buckets
                  , "-1.5"                           as score
                  , "1"                              as status
                  , "ALL"                            AS search_option_labels
                  , "ALL"                            AS from_for_logs
                  , "1"                              AS personalize_type
                  , "0"                              AS user_value_type
                  , "true"                           AS enablescoreaccumulation
                  , "ALL"                            AS check_type
                  , "3"                              AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
                      join
                  (select city_id
                        , dynamic_business_id
                        , case
                              when cast(hotel_three_adr_middle as double) >= cast(bnb_one_adr_middle as double)
                                  then bnb_one_adr_middle
                              else hotel_three_adr_middle end as price
                   from pdb_analysis_c.ads_flow_hotel_dy_adr_d
                   where dt = date_sub(current_date, 1)) as t2
                  on t1.city_id = t2.city_id
                      and t1.dynamic_business_id = t2.dynamic_business_id
--价格lose流量惩罚
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "29"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "29"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "29"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "29"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1

             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "28"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "28"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "28"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "28"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1


             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "27"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "27"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "27"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "27"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-5"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1


--途优客非超赞房屋加权
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "11"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "1.5"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)
                     and city_name = '西双版纳') as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "11"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "1.5"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)
                     and city_name = '西双版纳') as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "11"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "1.5"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)
                     and city_name = '西双版纳') as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "11"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "1.5"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)
                     and city_name = '西双版纳') as t1

             --C接扣分--不区分春节下调降权分（25年1月21日全面下调）
----4月11日扣20分调整为CDEFGHI桶，扣4分调整为JK桶
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "J,K"                  as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "J,K"                  as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "J,K"                  as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "J,K"                  as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1


             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I"        as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I"        as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I"        as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "4"                    as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I"        as buckets
                  , "-20"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             -- union
--2
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"city" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)

--     ) as t1
--         union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"district" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--     union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"landmark" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)

--     ) as t1
--     union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"locating" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"20" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)

--     ) as t1
-- union
--  --3
--         select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"city" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)

--     ) as t1
--         union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"district" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1
--     union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"landmark" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)

--     ) as t1
--     union
--     select "0" as channel
--     ,t1.city_id
--     ,t1.city_name
--     ,t1.dynamic_business_id as dynamic_id
--     ,t1.dynamic_business
--     ,"locating" as scene
--     ,"2" as filter_type
--     ,"2023-09-01" as begin_date
--     ,"2030-12-31" as end_date
--     ,"21" as rank_tag_id
--     ,"0" as can_sale_num
--     ,"0" as can_sale_max_num
--     ,"0" as can_sale_percent
--     ,"0" as price_rise_limit
--     ,"0,9999" as stay_days_range
--     ,"2" as is_major_price
--     ,"0,99999" as price_range
--     ,"C,D,E,F,G,H,I,J,K" as buckets
--     ,"-2" as score
--     ,"1" as status
--     ,"ALL" AS search_option_labels
--     ,"ALL" AS from_for_logs
--     ,"0" AS personalize_type
--     ,"0" AS user_value_type
--     ,"true" AS enablescoreaccumulation
--     ,"ALL" AS check_type
--     ,"3" AS bucket_type
--     from
--     (
--         select distinct city_id
--         ,city_name
--         ,dynamic_business_id
--         ,dynamic_business
--         from house_info
--         where dt=date_sub(current_date,1)
--     ) as t1

--负反馈扣分
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "37"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-15"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "37"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-15"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "37"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-15"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "37"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-15"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1

--负反馈扣分
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "38"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "38"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "38"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "38"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "H,I,J,K"              as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1

----名称带酒店/宾馆的房屋降权
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "39"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I,J,K"    as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "39"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I,J,K"    as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "39"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I,J,K"    as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2023-09-01"           as begin_date
                  , "2030-12-31"           as end_date
                  , "39"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "C,D,E,F,G,H,I,J,K"    as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
--海外优选宝藏真视频加权
             union
             select channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_id
                  , t1.dynamic_business
                  , scene
                  , filter_type
                  , begin_date
                  , end_date
                  , rank_tag_id
                  , can_sale_num
                  , can_sale_max_num
                  , can_sale_percent
                  , price_rise_limit
                  , stay_days_range
                  , is_major_price
                  , price_range
                  , buckets
                  , score
                  , status
                  , search_option_labels
                  , from_for_logs
                  , personalize_type
                  , user_value_type
                  , enablescoreaccumulation
                  , check_type
                  , bucket_type
             from excel_upload.oversea_jiaquan as t1

--非优选扣分
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "40"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "40"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "40"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "40"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-4"                   as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1


--无实拍视频降流
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "district"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "45"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "landmark"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "45"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "city"                 as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "45"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union
             select "0"                    as channel
                  , t1.city_id
                  , t1.city_name
                  , t1.dynamic_business_id as dynamic_id
                  , t1.dynamic_business
                  , "locating"             as scene
                  , "2"                    as filter_type
                  , "2025-02-05"           as begin_date
                  , "2030-12-31"           as end_date
                  , "45"                   as rank_tag_id
                  , "0"                    as can_sale_num
                  , "0"                    as can_sale_max_num
                  , "0"                    as can_sale_percent
                  , "0"                    as price_rise_limit
                  , "0,9999"               as stay_days_range
                  , "2"                    as is_major_price
                  , "0,99999"              as price_range
                  , "D,E,F,G,H,I,J,K"      as buckets
                  , "-10"                  as score
                  , "1"                    as status
                  , "ALL"                  AS search_option_labels
                  , "ALL"                  AS from_for_logs
                  , "0"                    AS personalize_type
                  , "0"                    AS user_value_type
                  , "true"                 AS enablescoreaccumulation
                  , "ALL"                  AS check_type
                  , "3"                    AS bucket_type
             from (select distinct city_id
                                 , city_name
                                 , dynamic_business_id
                                 , dynamic_business
                   from house_info
                   where dt = date_sub(current_date, 1)) as t1
             union

             select channel,
                    city_id,
                    city_name,
                    dynamic_id,
                    dynamic_business,
                    scene,
                    filter_type,
                    begin_date,
                    end_date,
                    rank_tag_id,
                    can_sale_num,
                    can_sale_max_num,
                    can_sale_percent,
                    price_rise_limit,
                    stay_days_range,
                    is_major_price,
                    price_range,
                    buckets,
                    score,
                    status,
                    search_option_labels,
                    from_for_logs,
                    personalize_type,
                    user_value_type,
                    enablescoreaccumulation,
                    check_type,
                    bucket_type
             from pdb_analysis_c.ads_flow_scarce_house_config_tag_46_d
             where dt = date_sub(current_date(), 1))
select channel
     , city_id
     , city_name
     , case when dynamic_id > 0 then dynamic_id else '0' end      as dynamic_id
     , case when dynamic_id > 0 then dynamic_business else '' end as dynamic_business
     , scene
     , filter_type
     , begin_date
     , end_date
     , rank_tag_id
     , can_sale_num
     , can_sale_max_num
     , can_sale_percent
     , price_rise_limit
     , stay_days_range
     , is_major_price
     , price_range
     , buckets
     , score
     , status
     , substring(current_timestamp, 12, 2)                        as hours
     , search_option_labels
     , from_for_logs
     , personalize_type
     , user_value_type
     , enablescoreaccumulation
     , check_type
     , bucket_type
from langterm
where score <> "0"
  and channel >= 0