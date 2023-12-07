-- install_offload.sql
--
-- LICENSE_TEXT
--

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Installing GOE...
prompt ================================================================================
prompt

@@sql/install_env.sql
@@sql/create_offload_users.sql
@@sql/create_offload_privs.sql
alter session set current_schema = &goe_db_adm_user;
@@sql/install_offload_code.sql
@@sql/create_offload_repo.sql
@@sql/upgrade_goe_version.sql

prompt
prompt ================================================================================
prompt GOE successfully installed.
prompt ================================================================================
prompt

@@sql/check_goe_user_expiration.sql
@@sql/restore_sqlplus_env.sql
