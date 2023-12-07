define _app_schema = &1
define _hybrid_schema = &_app_schema._H

prompt *********************************************************************************
prompt
prompt Granting privileges on STE patch views to/from the following schemas:
prompt
prompt * Application Schema = &_app_schema
prompt * Hybrid Schema      = &_hybrid_schema
prompt
prompt *********************************************************************************
prompt

alter session set current_schema = &_app_schema;

begin
    for r in (select view_name
              from   dba_views
              where  owner = upper('&_hybrid_schema')
              and    view_name like '%\_UV' escape '\')
    loop
        execute immediate 'grant select,insert,update,delete on &_hybrid_schema..' || r.view_name || ' to &_app_schema.';
    end loop;
end;
/

