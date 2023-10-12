define _app_schema    = sh_sync
define _hybrid_schema = &_app_schema._h

whenever sqlerror exit sql.sqlcode

prompt "Resetting &_app_schema. password..."
alter user &_app_schema. identified by &_app_schema.;

prompt "Resetting &_hybrid_schema. password..."
alter user &_hybrid_schema. identified by &_hybrid_schema.;

undefine _app_schema
undefine _hybrid_schema
