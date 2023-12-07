-- install_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
whenever oserror exit failure

undefine goe_db_user_prefix
undefine goe_db_adm_user
undefine goe_db_app_user
undefine goe_db_repo_user

define goe_version = "VERSION"
define goe_build = "BUILD"

ACCEPT goe_db_user_prefix PROMPT 'GOE database user prefix [GOE]: '
ACCEPT goe_db_user_profile PROMPT 'Profile for GOE database users [DEFAULT]: '

set termout off

col goe_db_user_prefix new_value goe_db_user_prefix
col goe_db_user_profile new_value goe_db_user_profile
select upper(coalesce('&goe_db_user_prefix', 'GOE'))      as goe_db_user_prefix
     , upper(coalesce('&goe_db_user_profile', 'DEFAULT')) as goe_db_user_profile
  from dual;

define goe_db_adm_user = &goe_db_user_prefix._ADM
define goe_db_app_user = &goe_db_user_prefix._APP
define goe_db_repo_user = &goe_db_user_prefix._REPO

@@repo_env.sql

@@verify_env.sql
