
create table _mc.content_comments_summary as
select web_url, section_name, date(min(web_publication_date)) as dt, 
sum(case when commentable_flag then 1 else 0 end) > 0 as commentable, 
sum(case when is_premoderated_flag then 1 else 0 end)>0 as premoderated
from content_dim
group by 1, 2;
