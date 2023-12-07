define _app_schema    = SH_DEMO

whenever sqlerror exit sql.sqlcode

prompt "Granting EXECUTE on GLUENT_ADM.OFFLOAD to &_app_schema. to allow sqlmon.sql to be run"
grant execute on gluent_adm.offload to &_app_schema.;

undefine _app_schema
