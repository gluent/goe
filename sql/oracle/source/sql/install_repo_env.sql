-- install_repo_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
whenever oserror exit failure

undefine gluent_repo_tablespace
undefine gluent_repo_ts_quota

define raise_existing_repo_role = "Y"
define default_repo_tablespace = "USERS"
define default_repo_ts_quota = "1G"

set termout on
ACCEPT gluent_repo_tablespace PROMPT 'Existing tablespace to use for &gluent_db_repo_user user [&default_repo_tablespace]: '
ACCEPT gluent_repo_ts_quota PROMPT 'Tablespace quota to assign to &gluent_db_repo_user (format <integer>[K|M|G]) [&default_repo_ts_quota]: '

set termout off
col gluent_repo_tablespace new_value gluent_repo_tablespace
col gluent_repo_ts_quota new_value gluent_repo_ts_quota
select upper(nvl('&gluent_repo_tablespace', '&default_repo_tablespace'))  as gluent_repo_tablespace
     , upper(coalesce('&gluent_repo_ts_quota', '&default_repo_ts_quota')) as gluent_repo_ts_quota
  from dual;

undefine default_repo_tablespace
undefine default_repo_ts_quota
