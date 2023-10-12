-- upgrade_offload.sql --
--
-- LICENSE_TEXT
--

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Upgrading OFFLOAD...
prompt ================================================================================
prompt

@@sql/upgrade_env.sql
@@sql/check_offload_install.sql
@@sql/create_offload_roles.sql
@@sql/create_gluent_offload_grants.sql
@@sql/drop_offload_obsolete_objects.sql
@@sql/create_offload_repo_10g.sql
alter session set current_schema = &gluent_db_adm_user;
@@sql/drop_offload_code.sql
@@sql/install_offload_code.sql
@@sql/upgrade_gdp_version.sql

prompt
prompt ================================================================================
prompt OFFLOAD successfully upgraded.
prompt ================================================================================
prompt

@@sql/check_gluent_user_expiration.sql

undefine offload_db_user

@@sql/restore_sqlplus_env.sql
