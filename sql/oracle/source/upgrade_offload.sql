-- upgrade_offload.sql --
--
-- LICENSE_TEXT
--

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Upgrading GOE...
prompt ================================================================================
prompt

@@sql/upgrade_env.sql
@@sql/check_offload_install.sql
@@sql/create_offload_privs.sql
@@sql/drop_offload_obsolete_objects.sql
alter session set current_schema = &goe_db_adm_user;
@@sql/drop_offload_code.sql
@@sql/install_offload_code.sql
@@sql/create_offload_repo.sql
@@sql/upgrade_goe_version.sql

prompt
prompt ================================================================================
prompt GOE successfully upgraded.
prompt ================================================================================
prompt

@@sql/check_goe_user_expiration.sql
@@sql/restore_sqlplus_env.sql
