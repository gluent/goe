-- uninstall_offload.sql
--
-- LICENSE_TEXT
--

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Uninstalling GOE...
prompt ================================================================================
prompt

@@sql/uninstall_env.sql
@@sql/check_offload_install.sql

WHENEVER SQLERROR CONTINUE

drop user &goe_db_adm_user cascade;
drop user &goe_db_app_user cascade;

@@sql/uninstall_offload_repo.sql

prompt
prompt ================================================================================
prompt GOE successfully removed.
prompt ================================================================================
prompt
undefine goe_db_user_prefix
undefine goe_db_adm_user
undefine goe_db_app_user
undefine goe_db_repo_user
undefine goe_repo_installed
undefine goe_repo_uninstall

@@sql/restore_sqlplus_env.sql
