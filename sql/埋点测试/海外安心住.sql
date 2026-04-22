select 
    from_unixtime(send_timestamp / 1000) `时间`
    ,key
    ,get_json_object(data_info,'$.source') source 
    ,case when key = 'c_bnb_inn_home_filter_app' and get_json_object(data_info,'$.source') = '205' then 1 -- 首页
            when key = 'c_bnb_inn_list_ops_app' and get_json_object(data_info,'$.source') = '572' then 2 -- L页
            when key = 'c_bnb_inn_detail_operate_app' and get_json_object(data_info,'$.source') = '492' then 3 -- D页
            when key = 'c_bnb_inn_order_filling_operate_app' and get_json_object(data_info,'$.source') = '186' then 4 -- B页
            when key= 'c_bnb_inn_order_detail_operate_app' and get_json_object(data_info,'$.source') = '316' then 5 
    end info
    ,data_info
    ,get_json_object(data_info,'$.env_tujia_uid') uid
    ,get_json_object(data_info,'$.cityid') cityid
    ,get_json_object(data_info,'$.checkin_date') checkin_date
    ,get_json_object(data_info,'$.checkout_date') checkout_date
    ,get_json_object(data_info,'$.checkin') checkin
    ,get_json_object(data_info,'$.checkout') checkout
    ,get_json_object(data_info,'$.house_id') house_id
    ,get_json_object(data_info,'$.orderNo') orderNo 

from dwd.dwd_log_ubt_d_iceberg 
where dt = current_date
and client_id = '51741169391265124033'
-- and get_json_object(data_info,'$.env_tujia_uid') = 'E50F2EF4FEAE21AF4BE0BC137E652141'
and  key in ('c_bnb_inn_home_filter_app','c_bnb_inn_list_ops_app','c_bnb_inn_detail_operate_app','c_bnb_inn_order_filling_operate_app','c_bnb_inn_order_detail_operate_app')
and case when key = 'c_bnb_inn_home_filter_app' and get_json_object(data_info,'$.source') = '205' then 1 -- 首页
        when key = 'c_bnb_inn_list_ops_app' and get_json_object(data_info,'$.source') = '572' then 2 -- L页
        when key = 'c_bnb_inn_detail_operate_app' and get_json_object(data_info,'$.source') = '492' then 3 -- D页
        when key = 'c_bnb_inn_order_filling_operate_app' and get_json_object(data_info,'$.source') = '186' then 4 -- B页
        when key= 'c_bnb_inn_order_detail_operate_app' and get_json_object(data_info,'$.source') = '316' then 5 
end in (1,2,3,4,5)
 