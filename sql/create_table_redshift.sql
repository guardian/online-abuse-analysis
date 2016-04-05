drop table _mc.authorurl_articleurl_author;
drop table _mc.article_year_section;
drop table _mc.tag_articleurl;
drop table _mc.authorname_authorurl;

create table _mc.authorurl_articleurl_author as
select ctl.tag_web_url as author_url, cd.web_url as article_url, ctl.tag_web_title as author
from content_dim cd
join content_tag_lookup ctl
on cd.content_key = ctl.content_key
where tag_type = 'contributor'
group by 1, 2, 3;

create table _mc.article_year_section as
select web_url as url, extract(year from min(web_publication_date) ) as year, min(section_name) as section
from content_dim
group by 1;

create table _mc.tag_articleurl as
select ctl.tag_web_title as keyword, cd.web_url as url
from content_dim cd
join content_tag_lookup ctl
on cd.content_key = ctl.content_key
where tag_type = 'keyword'
group by 1, 2;

create table _mc.authorname_authorurl as
select tag_web_title as name, tag_web_url as url
from content_tag_lookup
where tag_type = 'contributor';
