select dt 
    ,city_name
    ,count(distinct uid) `总uv`
    ,count(distinct case when is_shuqi = 1 then uid end) `暑期uv`
    ,round(count(distinct case when is_shuqi = 1 then uid end) / count(distinct uid),3) `暑期uv占比`
    ,round(count(distinct case when time_type = '0716' then uid end) / count(distinct uid),3) `0710到0716暑期uv占比`
    ,round(sum(if(time_type = '0716',kss_zc,null))/sum(if(time_type = '0716',kss_all_zc,null)),3) `0710到0716直采可售率`
    ,round(sum(if(time_type = '0716',kss_cj,null))/sum(if(time_type = '0716',kss_all_cj,null)),3) `0710到0716c接可售率`
    ,round(count(distinct case when time_type = '0723' then uid end) / count(distinct uid),3) `0717到0723暑期uv占比`
    ,round(sum(if(time_type = '0723',kss_zc,null))/sum(if(time_type = '0723',kss_all_zc,null)),3) `0717到0723直采可售率`
    ,round(sum(if(time_type = '0723',kss_cj,null))/sum(if(time_type = '0723',kss_all_cj,null)),3) `0717到0723c接可售率`
    ,round(count(distinct case when time_type = '0730' then uid end) / count(distinct uid),3) `0724到0730暑期uv占比`
    ,round(sum(if(time_type = '0730',kss_zc,null))/sum(if(time_type = '0730',kss_all_zc,null)),3) `0724到0730直采可售率`
    ,round(sum(if(time_type = '0730',kss_cj,null))/sum(if(time_type = '0730',kss_all_cj,null)),3) `0724到0730c接可售率`
    ,round(count(distinct case when time_type = '0806' then uid end) / count(distinct uid),3) `0731到0806暑期uv占比`
    ,round(sum(if(time_type = '0806',kss_zc,null))/sum(if(time_type = '0806',kss_all_zc,null)),3) `0731到0806直采可售率`
    ,round(sum(if(time_type = '0806',kss_cj,null))/sum(if(time_type = '0806',kss_all_cj,null)),3) `0731到0806c接可售率`
    ,round(count(distinct case when time_type = '0813' then uid end) / count(distinct uid),3) `0731到0813暑期uv占比`
    ,round(sum(if(time_type = '0813',kss_zc,null))/sum(if(time_type = '0813',kss_all_zc,null)),3) `0731到0813直采可售率`
    ,round(sum(if(time_type = '0813',kss_cj,null))/sum(if(time_type = '0813',kss_all_cj,null)),3) `0731到0813c接可售率`
    ,round(count(distinct case when time_type = '0820' then uid end) / count(distinct uid),3) `0814到0820暑期uv占比`
    ,round(sum(if(time_type = '0820',kss_zc,null))/sum(if(time_type = '0820',kss_all_zc,null)),3) `0814到0820直采可售率`
    ,round(sum(if(time_type = '0820',kss_cj,null))/sum(if(time_type = '0820',kss_all_cj,null)),3) `0814到0820c接可售率`
from (
    select get_json_object(server_log,'$.canSalePercentOfSelfChannel') ksl_zc
        ,get_json_object(server_log,'$.canSaleNumOfSelfChannel') kss_zc
        ,get_json_object(server_log,'$.canSaleNumOfSelfChannel') / get_json_object(server_log,'$.canSalePercentOfSelfChannel') / 100 kss_all_zc
        ,get_json_object(server_log,'$.canSalePercentOfCtripChannel') ksl_cj
        ,get_json_object(server_log,'$.canSaleNumOfCtripChannel') kss_cj
        ,get_json_object(server_log,'$.canSaleNumOfCtripChannel') / get_json_object(server_log,'$.canSalePercentOfCtripChannel') / 100 kss_all_cj
        ,case when checkout_date between '2025-07-10' and '2025-07-16' then '0716'
            when checkout_date between '2025-07-17' and '2025-07-23' then '0723'
            when checkout_date between '2025-07-24' and '2025-07-30' then '0730'
            when checkout_date between '2025-07-31' and '2025-08-06' then '0806'
            when checkout_date between '2025-07-07' and '2025-08-13' then '0813'
            when checkout_date between '2025-07-14' and '2025-08-20' then '0820'
        end time_type
        ,case when city_name in ('大阪','东京') then city_name else '其他' end city_name
        ,dt 
        ,uid
    from dws.dws_path_ldbo_d 
    where dt = '2025-05-01'
    and wrapper_name in ('携程','去哪儿','途家')
    and client_name = 'APP' 
    and user_type = '用户' 
    and is_oversea = 1 
) a 
group by 1,2
    


select get_json_object(server_log,'$.canSalePercentOfSelfChannel') ksl_zc
    ,get_json_object(server_log,'$.canSaleNumOfSelfChannel') kss_zc
    ,get_json_object(server_log,'$.canSaleNumOfSelfChannel') / get_json_object(server_log,'$.canSalePercentOfSelfChannel') * 100 kss_all_zc
    ,get_json_object(server_log,'$.canSalePercentOfCtripChannel') ksl_cj
    ,get_json_object(server_log,'$.canSaleNumOfCtripChannel') kss_cj
    ,get_json_object(server_log,'$.canSaleNumOfCtripChannel') / get_json_object(server_log,'$.canSalePercentOfCtripChannel') * 100 kss_all_cj
    ,case when checkout_date between '2025-07-10' and '2025-07-16' then '0716'
        when checkout_date between '2025-07-17' and '2025-07-23' then '0723'
        when checkout_date between '2025-07-24' and '2025-07-30' then '0730'
        when checkout_date between '2025-07-31' and '2025-08-06' then '0806'
        when checkout_date between '2025-07-07' and '2025-08-13' then '0813'
        when checkout_date between '2025-07-14' and '2025-08-20' then '0820'
    end time_type
    ,case when city_name in ('大阪','东京') then city_name else '其他' end city_name
    ,dt 
    ,uid
from dws.dws_path_ldbo_d 
where dt = '2025-07-01'
and wrapper_name in ('携程','去哪儿','途家')
and client_name = 'APP' 
and user_type = '用户' 
and is_oversea = 1