-- install_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
whenever oserror exit failure

undefine gluent_db_user_prefix
undefine gluent_db_adm_user
undefine gluent_db_app_user
undefine gluent_db_repo_user
undefine raise_existing_role

define raise_existing_role = "Y"
define gluent_gdp_version = "VERSION"
define gluent_gdp_build = "BUILD"

ACCEPT gluent_db_user_prefix PROMPT 'Gluent database user prefix [GLUENT]: '
ACCEPT gluent_db_user_profile PROMPT 'Profile for Gluent database users [DEFAULT]: '

set termout off

col gluent_db_user_prefix new_value gluent_db_user_prefix
col gluent_db_user_profile new_value gluent_db_user_profile
select upper(coalesce('&gluent_db_user_prefix', 'GLUENT'))      as gluent_db_user_prefix
     , upper(coalesce('&gluent_db_user_profile', 'DEFAULT'))    as gluent_db_user_profile
  from dual;

define gluent_db_adm_user = &gluent_db_user_prefix._ADM
define gluent_db_app_user = &gluent_db_user_prefix._APP
define gluent_db_repo_user = &gluent_db_user_prefix._REPO

@@repo_env.sql

@@verify_env.sql
