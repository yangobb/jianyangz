 
select case when dt between date_sub(current_date,7) and date_sub(current_date,1) then 'W1'
        when dt between date_sub(current_date,14) and date_sub(current_date,8) then 'W2'
        when dt between date_sub(current_date,21) and date_sub(current_date,15) then 'W3'
        when dt between date_sub(current_date,28) and date_sub(current_date,22) then 'W4'
        when dt between date_sub(current_date,35) and date_sub(current_date,29) then 'W5'
        when dt between date_sub(current_date,42) and date_sub(current_date,36) then 'W6'
        when dt between date_sub(current_date,49) and date_sub(current_date,43) then 'W7'
        when dt between date_sub(current_date,56) and date_sub(current_date,50) then 'W8'
    end week
    ,country_name
    ,house_type
        ,dynamic_business
        ,bedroom_count
    ,is_yx
    ,is_13city
    ,sum(30_dt) / 30 30_dt
    ,sum(7_dt) / 7 7_dt
from ( 
        select dt
                ,house_id
                ,keshoukucun_7
                ,wulifangliang_7
                ,yishoukucun_7	
                ,tuzhanbi_7
        from pdb_analysis_c.dwd_house_tuzhanbi_d
        where dt in ('2025-04-08','2025-04-01','2025-03-25','2025-03-18','2025-03-11','2025-03-04')
) a 
inner join (
    SELECT  DISTINCT  
        h.country_name
        ,h.house_city_name
        ,h.house_id
        ,h.hotel_id
        ,h.house_class 
        ,dynamic_business
        ,bedroom_count
        ,CASE WHEN t2.house_id IS NOT NULL THEN 1 ELSE 0 END AS is_yx --优选
        ,CASE WHEN t3.house_id IS NOT NULL THEN 1 ELSE 0 END AS is_bzms --宝藏
        ,house_type
        ,is_13city
    FROM (
        SELECT distinct h.dt
            ,case when country_name in ('日本','泰国','马来西亚','韩国','新加坡') then country_name when country_name = '中国大陆' then '港澳' else '其他' end country_name
            ,h.house_city_name
            ,h.house_id
            ,h.hotel_id
            ,h.house_class
            ,h.house_number
            ,case when country_name in ('中国大陆','日本','泰国','马来西亚','韩国','新加坡') then dynamic_business else '其他' end dynamic_business
            ,bedroom_count
            ,case when h.landlord_channel_name = '平台商户' then '自采' else 'c接' end house_type
            ,case when house_city_name in ('海外汇总','13城汇总','香港','曼谷','东京','大阪','澳门','首尔','新加坡','吉隆坡','普吉岛','芭堤雅','京都','清迈','济州市','札幌','福冈','名古屋','冲绳') then 1 else 0 end is_13city
        FROM dws.dws_house_d h
        WHERE h.dt = date_sub(CURRENT_DATE(),1)
        AND h.house_is_online = 1
        AND h.house_is_oversea = 1
    ) h
    LEFT JOIN pdb_analysis_b.dwd_house_label_1000487_d t2
    ON h.house_id = t2.house_id
    AND h.dt = t2.dt
    AND t2.dt = date_sub(CURRENT_DATE(),1)
    LEFT JOIN (
        SELECT  DISTINCT house_id
            ,dt
        FROM pdb_analysis_b.dwd_house_label_1000488_d
        WHERE dt = date_sub(CURRENT_DATE(),1)
    ) t3
    ON h.house_id = t3.house_id
    AND h.dt = t3.dt
) b 
on a.house_id = b.house_id
group by 1,2,3,4,5,6,7