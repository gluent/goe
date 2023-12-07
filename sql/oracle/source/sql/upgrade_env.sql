-- upgrade_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
whenever oserror exit failure

undefine goe_db_user_prefix
undefine goe_db_adm_user
undefine goe_db_app_user

define goe_version = "VERSION"
define goe_build = "BUILD"

set termout off
col goe_db_user_prefix new_value goe_db_user_prefix
select substr(owner,1,length(owner) - 4) goe_db_user_prefix
  from dba_objects
 where object_type = 'PACKAGE'
   and object_name = 'OFFLOAD'
   and substr(owner,-4) = '_ADM';

define goe_db_adm_user = &goe_db_user_prefix._ADM
define goe_db_app_user = &goe_db_user_prefix._APP
define goe_db_repo_user = &goe_db_user_prefix._REPO

col goe_db_user_profile new_value goe_db_user_profile
select profile goe_db_user_profile
  from dba_users
 where username = '&goe_db_adm_user';

@@repo_env.sql

set termout on
@@verify_env.sql
