-- create_offload_repo_privs.sql
--
-- LICENSE_TEXT
--

set serveroutput on

prompt Granting system privileges for &goe_db_repo_user user...
GRANT CREATE SESSION TO &goe_db_repo_user.;
GRANT SELECT ANY DICTIONARY TO &goe_db_repo_user.;
