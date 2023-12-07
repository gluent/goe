-- verify_env.sql
--
-- LICENSE_TEXT
-- 
set heading off feedback off termout on
prompt
prompt Confirm installation values:
prompt
prompt * GOE Database User Prefix...................: &goe_db_user_prefix
prompt * GOE Database User Profile..................: &goe_db_user_profile
prompt * GOE Database Admin User....................: &goe_db_adm_user
prompt * GOE Database Application User..............: &goe_db_app_user
prompt * GOE Database Repository User...............: &goe_db_repo_user
prompt * GOE Database Repository Tablespace.........: &goe_repo_tablespace
prompt * GOE Database Repository Tablespace Quota...: &goe_repo_ts_quota
set heading on feedback on

PROMPT
PROMPT Hit <Enter> to continue or <Ctrl>-C to abort
PAUSE
