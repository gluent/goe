
define _app_schema = "&1"

@get_adm_app_schema_names.sql

GRANT EXECUTE ON &_gluent_adm_schema..offload TO &_app_schema;
GRANT EXECUTE ON &_gluent_adm_schema..offload_file TO &_app_schema;
--GRANT EXECUTE ON &_gluent_adm_schema..offload_filter TO &_app_schema, &_app_schema._H;
GRANT SELECT ON &_gluent_adm_schema..offload_objects TO &_app_schema;
GRANT EXECUTE ON SYS.DBMS_CRYPTO TO &_app_schema;
GRANT EXECUTE ON SYS.DBMS_FGA TO &_app_schema;
--GRANT EXECUTE ON SYS.DBMS_CRYPTO TO &_app_schema, &_app_schema._H;
--GRANT EXECUTE ON SYS.DBMS_FGA TO &_app_schema, &_app_schema._H;

undefine _app_schema
undefine _gluent_adm_schema
undefine _gluent_app_schema
undefine 1
