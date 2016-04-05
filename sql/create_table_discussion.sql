create table public.mm_comment_body_url_blocked as
  select cc.body, cd.url, CASE WHEN ma.type = 'blocked' THEN true ELSE false END as blocked, random() * 1000 + 1 as n
  from comments_comment cc
    join comments_discussion cd on cc.discussion_id = cd.id
    LEFT OUTER JOIN moderation_action ma ON ma.comment_id = cc.id;


create table public.mm_comment_body_url_blocked_small as
  select cc.body, cd.url, CASE WHEN ma.type = 'blocked' THEN true ELSE false END as blocked, random() * 1000 + 1 as n
  from comments_comment cc
    join comments_discussion cd on cc.discussion_id = cd.id
    LEFT OUTER JOIN moderation_action ma ON ma.comment_id = cc.id
  where cc.created_on > current_date - 20;

commit;

select max(n), min(n), count(*)
from public.mc_comment_body_url;

select *
from public.mm_comment_body_url
limit 43;
