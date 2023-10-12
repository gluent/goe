-- verify_env.sql
--
-- LICENSE_TEXT
-- 
set heading off feedback off termout on
prompt
prompt Confirm installation values:
prompt
prompt * Gluent Database User Prefix...................: &gluent_db_user_prefix
prompt * Gluent Database User Profile..................: &gluent_db_user_profile
prompt * Gluent Database Admin User....................: &gluent_db_adm_user
prompt * Gluent Database Application User..............: &gluent_db_app_user
prompt * Gluent Database Repository User...............: &gluent_db_repo_user
prompt * Gluent Database Repository Tablespace.........: &gluent_repo_tablespace
prompt * Gluent Database Repository Tablespace Quota...: &gluent_repo_ts_quota
set heading on feedback on

PROMPT
PROMPT Hit <Enter> to continue or <Ctrl>-C to abort
PAUSE
