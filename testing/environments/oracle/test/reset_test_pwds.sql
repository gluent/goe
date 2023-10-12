define _app_schema    = &1
define _hybrid_schema = &_app_schema._h

whenever sqlerror exit sql.sqlcode

--column app_schema new_value app_schema_lower
--select lower('&_app_schema.') as app_schema from dual;
column hybrid_schema new_value hybrid_schema_adjusted
select case when '&_app_schema.' = upper('&_app_schema.') then upper('&_hybrid_schema.') else lower('&_hybrid_schema.') end as hybrid_schema from dual;

prompt "Resetting &_app_schema. password..."
alter user &_app_schema. identified by "&_app_schema.";

--prompt "Resetting &_hybrid_schema. password..."
--alter user &_hybrid_schema. identified by "&hybrid_schema_adjusted.";

undefine _app_schema
undefine _hybrid_schema
--undefine app_schema_lower
--undefine hybrid_schema_lower
