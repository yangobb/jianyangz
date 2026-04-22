with ods_comment as (
    select *
    from ods_tujiacustomer.comment
    where EnumCommentStatus = 0               --评论状态 0:正常 1:过期
      and EnumDataEntityStatus = 0            --数据状态 0:正常 1:删除
      and (IsRepeat <> 1 or IsRepeat is null) --是否重复
      and DetailAuditStatus = 2 --审核状态 0:无具体含义 1:未审核 2:通过 3:拒绝
),

repeat_user as (
      select unitid, same_user_comment_num --90天内同一房屋同一用户有内容点评数的最大值
      from (
            select unitid,
                        same_user_comment_num,
                        row_number() over (partition by unitid order by same_user_comment_num desc) as num
            from (
                        select unitid, customerloginid, count(*) as same_user_comment_num
                        from ods_comment
                        where to_date(createtime) between date_sub(current_date(), 90) and date_sub(current_date(), 1)
                        and commentdetail <> ''
                        and commentdetail is not null
                        and customerloginid <> 0
                        group by unitid, customerloginid) a) b
      where num = 1
)

select house_id,
       has_content_comment_num,
       has_picture_comment_num,
       user_num,
       comment_num,
       comment_score,
       decoration_comment_score,
       service_comment_score,
       traffic_comment_score,
       hygiene_comment_score,
       price_comment_score,
       convenience_comment_score,
       round((good_comment_num / good_comment_denominator), 2) as good_comment_rate,
       nvl(same_user_comment_num, 0)              as same_user_comment_num,
       bad_comment_num,
       good_comment_num
from (
         select unitid                                                           as house_id,
                sum(if(commentdetail <> '' and commentdetail is not null, 1, 0)) as has_content_comment_num, --有入住体验内容点评条数
                sum(ishaspic)                                                    as has_picture_comment_num, --有图片点评条数
                count(distinct customerloginid)                                  as user_num,
                count(commentid)                                                 as comment_num,
                round(avg(totalscore), 1)                                        as comment_score,
                round(avg(Decoration), 1)                                        as decoration_comment_score,
                round(avg(Service), 1)                                           as service_comment_score,
                round(avg(Traffic), 1)                                           as traffic_comment_score,
                round(avg(Hygiene), 1)                                           as hygiene_comment_score,
                round(sum(if(price = 0 or price is null, 0, price)) / sum(if(price = 0 or price is null, 0, 1)),
                      1)                                                         as price_comment_score,
                round(sum(if(convenience = 0 or convenience is null, 0, convenience)) /
                      sum(if(convenience = 0 or convenience is null, 0, 1)),
                      1)                                                         as convenience_comment_score,
                sum(if(totalscore >= 4.5 and 
                	to_date(createtime) between date_sub(current_date(), 90) and date_sub(current_date(), 1) and 
                	!(int(nvl(enumcommentflag,0))&2==2), 1, 0))                  as good_comment_num,
                count(if(to_date(createtime) between date_sub(current_date(), 90) and date_sub(current_date(), 1) and 
                	!(int(nvl(enumcommentflag,0))&2==2),commentid,null)) 	     as good_comment_denominator,-- 计算好评率时剔除风控标识的评论
                sum(if(totalscore <= 3, 1, 0))                                   as bad_comment_num
         from ods_comment
         group by unitid) a
         left join repeat_user on a.house_id = repeat_user.unitid