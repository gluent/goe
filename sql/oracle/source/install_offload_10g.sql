-- install_offload.sql
--
-- LICENSE_TEXT
--

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Installing OFFLOAD...
prompt ================================================================================
prompt

@@sql/install_env.sql
@@sql/create_offload_users.sql
@@sql/create_offload_roles.sql
@@sql/create_gluent_offload_grants.sql
@@sql/create_offload_repo_10g.sql
alter session set current_schema = &gluent_db_adm_user;
@@sql/install_offload_code.sql
@@sql/upgrade_gdp_version.sql

prompt
prompt ================================================================================
prompt OFFLOAD successfully installed.
prompt ================================================================================
prompt

@@sql/check_gluent_user_expiration.sql

undefine offload_db_user
undefine passwd

@@sql/restore_sqlplus_env.sql
