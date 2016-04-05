create table temp_mm.authorurl_articleurl_author as
  select t.web_url as author_url, cd.url as article_url, t.web_title as author
  from raw.content cd
    join raw.tag_to_content ctl
      on cd.id = ctl.content_id
    join clean.tag t
      on ctl.tag_id = t.id
  where tag_type = 'Contributor'
  group by 1, 2, 3;

create table temp_mm.article_year_section as
  select 'http://www.theguardian.com' || content_path as url, extract(year from web_publication_date ) as year, section_name as section
  from clean.content;

create table temp_mm.tag_articleurl as
  select t.web_title as keyword, cd.url as article_url
  from raw.content cd
    join raw.tag_to_content ctl
      on cd.id = ctl.content_id
    join clean.tag t
      on ctl.tag_id = t.id
  where tag_type = 'Keyword';

create table temp_mm.authorname_authorurl as
  select web_title as name, web_url as url
  from clean.tag
  where tag_type = 'Contributor'
