-- create_offload_privs.sql
--
-- LICENSE_TEXT
--

set serveroutput on

prompt Granting system/role privileges for &goe_db_app_user user...
GRANT CREATE SESSION TO &goe_db_app_user.;
GRANT SELECT ANY DICTIONARY TO &goe_db_app_user.;
GRANT SELECT ANY TABLE TO &goe_db_app_user.;
GRANT FLASHBACK ANY TABLE TO &goe_db_app_user.;

prompt Granting system/role privileges for &goe_db_adm_user user...
GRANT CREATE SESSION TO &goe_db_adm_user.;
GRANT SELECT ANY DICTIONARY TO &goe_db_adm_user.;
GRANT SELECT ANY TABLE TO &goe_db_adm_user.;
GRANT SELECT_CATALOG_ROLE TO &goe_db_adm_user.;
