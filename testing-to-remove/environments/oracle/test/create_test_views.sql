
define _app_schema = &1
define _hybrid_schema = &_app_schema._H

prompt *********************************************************************************
prompt
prompt Creating test views in the following schemas:
prompt
prompt * Application Schema = &_app_schema
prompt * Hybrid Schema      = &_hybrid_schema
prompt
prompt Enter to Continue, Ctrl-C to Cancel
prompt
prompt *********************************************************************************
prompt

pause

alter session set current_schema = &_app_schema;

declare
   procedure do_drop ( p_name in varchar2 ) is
      x_no_view exception;
      pragma exception_init(x_no_view, -00942);
   begin
      execute immediate 'drop view ' || p_name;
   exception
      when x_no_view then
         null;
   end do_drop;
begin
    do_drop('GL_SALES_100_DV');
    do_drop('GL_SALES_100_NESTED_DV');
    do_drop('GL_SALES_100_NESTED_DV_NM');
    do_drop('GL_SALES_100_JOIN_DV');
    do_drop('GL_SALES_100_UA_DV');
    do_drop('GL_SALES_100_UA_DV_NM');
    do_drop('GL_SALES_DV');
    do_drop('GL_SALES_NESTED_DV');
    do_drop('GL_SALES_NESTED_DV_NM');
    do_drop('GL_SALES_JOIN_DV');
    do_drop('GL_SALES_UA_DV');
    do_drop('GL_SALES_LARGE_DV');
end;
/

create or replace view gl_sales_100_dv 
as 
    select * 
    from   gl_sales_100;

create or replace view gl_sales_100_nested_dv 
as 
    select s.* 
    from   gl_sales_100_dv s;

create or replace view gl_sales_100_nested_dv_nm 
as 
    select /*+ no_merge(s) no_merge(s.gl_sales_100) */ s.* 
    from   gl_sales_100_dv s;

create or replace view gl_sales_100_join_dv 
as 
    select s.*, ct.country_name 
    from   gl_sales_100 s, &_app_schema..customers c, &_app_schema..countries ct 
    where  s.cust_id = c.cust_id and c.country_id = ct.country_id and c.cust_year_of_birth > 1950 and s.amount_sold > 100;

create or replace view gl_sales_100_ua_dv 
as 
    select s.* 
    from   gl_sales_100 s 
    where  s.amount_sold <= 100 
    union all 
    select s.* 
    from   gl_sales_100 s 
    where  s.amount_sold > 100;

create or replace view gl_sales_100_ua_dv_nm 
as 
    select /*+ no_merge(s) */ s.* 
    from   gl_sales_100 s 
    where  s.amount_sold <= 100 
    union all 
    select /*+ no_merge(s) */ s.* 
    from   gl_sales_100 s 
    where  s.amount_sold > 100;

create or replace view gl_sales_dv 
as 
    select * 
    from   gl_sales;

create or replace view gl_sales_nested_dv 
as 
    select s.* 
    from   gl_sales_dv s;

create or replace view gl_sales_nested_dv_nm 
as 
    select /*+ no_merge(s) */ s.* 
    from  gl_sales_dv s;

create or replace view gl_sales_join_dv 
as 
    select s.*, ct.country_name 
    from   gl_sales s, &_app_schema..customers c, &_app_schema..countries ct 
    where  s.cust_id = c.cust_id and c.country_id = ct.country_id and c.cust_year_of_birth > 1950 and s.amount_sold > 100;

create or replace view gl_sales_ua_dv 
as 
    select s.* 
    from   gl_sales s
    where  s.amount_sold <= 100 
    union all 
    select s.* 
    from   gl_sales s 
    where  s.amount_sold > 100;

declare
  v_view_text  clob := 'create or replace view gl_sales_large_dv as ' || chr(10);
  v_projection clob;
begin
  for i in 1 .. 30 loop
    for r_col in (select listagg(column_name ||' as ' || lower(column_name) || '_'||to_char(i), ',') 
                            within group (order by column_id) as cols
                  from   dba_tab_cols 
                  where  owner = upper('&_app_schema') 
                  and    table_name = 'GL_SALES')
    loop
      v_projection := v_projection || ', ' || r_col.cols || chr(10);
    end loop;
  end loop;
  v_projection := rtrim(ltrim(v_projection, ','), chr(10));
  while length(v_view_text) < 2097152 loop
    v_view_text := v_view_text || 'select ' || v_projection || chr(10) || 'from gl_sales O  ' || chr(10) || ' union all ' || chr(10);
  end loop;
  v_view_text := v_view_text || 'select ' || v_projection || chr(10) || 'from gl_sales';
  execute immediate v_view_text;
end;
/

undefine 1
undefine _app_schema
undefine _hybrid_schema


