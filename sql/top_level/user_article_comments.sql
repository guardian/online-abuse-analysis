create table public.mm_user_article_comments as
select cc.posted_by_id as user_id, cd.url, 
sum(case when cc.status = 'visible' then 1 else 0 end) as visible_comments,
sum(case when cc.status = 'visible' then 0 else 1 end) as invisible_comments,
random() * 1000 + 1 as n
from comments_comment cc
join comments_discussion cd
on cc.discussion_id = cd.id
group by 1, 2;

commit;