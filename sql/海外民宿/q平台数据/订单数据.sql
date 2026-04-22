
select substr(checkout_date,1,7) month 
    ,country_name
    ,city_name
	,count(distinct order_no) order_cnt
    ,sum(room_night) night 
    ,sum(final_room_fee) gmv 
from (
    select *
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
    and is_valid = '1'
    and order_status not in ('CANCELLED','REJECTED')
    and checkout_date between '2024-01-01' and date_sub(current_date,1)
) a 
inner join (
    select hotel_seq
        ,case when hotelSubCategory in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标' else '标' end hotelSubCategory
    from (
    select hotel_seq
        ,max(attrs['hotelSubCategory']) as hotelSubCategory
    from ihotel_default.dim_hotel_info_intl_v3 a
    where dt = '%(DATE)s'
    and hotel_operating_status = '营业中'
    group by 1
    ) tmp
) b 
on a.hotel_seq = b.hotel_seq
group by 1,2,3 
