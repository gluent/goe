set echo on timing on
REM Products with most website hits
select * from (
  select prod_id, count(*) hits
  from website_data
  group by prod_id
  order by count(*) desc
)
where rownum <= 10;

pause

REM products with most website hits with sales figures alongside them
select * from (
  select s.prod_id, s.sales_count, w.hits
  from (select prod_id, count(*) sales_count from sales group by prod_id) s
  , (select prod_id, count(*) hits from website_data group by prod_id) w
  where s.prod_id = w.prod_id
  order by w.hits desc
)
where rownum <= 10;

