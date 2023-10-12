-- hybrid_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
set serveroutput on
set feedback off termout off

col app_schema new_value app_schema
col hybrid_schema new_value hybrid_schema
col app_schema_u new_value app_schema_u
col hybrid_schema_u new_value hybrid_schema_u
select lower('&1') app_schema
     , lower('&1._'||NVL('&2','h')) hybrid_schema
     , upper('&1') app_schema_u
     , upper('&1._'||NVL('&2','H')) hybrid_schema_u
  from dual;

set termout on

declare
  l_user number;
begin
  select count(*)
  into   l_user
  from   dba_users
  where  username = upper('&app_schema');

  if l_user != 1 AND NVL('&3','x') != 'NOUSERCHECK' then
    dbms_output.put_line('WARNING: &app_schema_u. not found in this database, proceeding without schema integration');
  end if;

end;
/

set feedback on
