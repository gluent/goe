-- upgrade_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
whenever oserror exit failure

undefine gluent_db_user_prefix
undefine gluent_db_adm_user
undefine gluent_db_app_user

define raise_existing_role = "N"
define gluent_gdp_version = "VERSION"
define gluent_gdp_build = "BUILD"

set termout off
col gluent_db_user_prefix new_value gluent_db_user_prefix
select substr(owner,1,length(owner) - 4) gluent_db_user_prefix
  from dba_objects
 where object_type = 'PACKAGE'
   and object_name = 'OFFLOAD'
   and substr(owner,-4) = '_ADM';

define gluent_db_adm_user = &gluent_db_user_prefix._ADM
define gluent_db_app_user = &gluent_db_user_prefix._APP
define gluent_db_repo_user = &gluent_db_user_prefix._REPO

col gluent_db_user_profile new_value gluent_db_user_profile
select profile gluent_db_user_profile
  from dba_users
 where username = '&gluent_db_adm_user';

@@repo_env.sql

set termout on
@@verify_env.sql
