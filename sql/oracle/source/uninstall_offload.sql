-- uninstall_offload.sql
--
-- LICENSE_TEXT
--

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Uninstalling OFFLOAD...
prompt ================================================================================
prompt

@@sql/uninstall_env.sql
@@sql/check_offload_install.sql

WHENEVER SQLERROR CONTINUE

prompt Dropping users...
drop user &gluent_db_adm_user cascade;
drop user &gluent_db_app_user cascade;

prompt Dropping roles...
drop role gluent_offload_role;

prompt Dropping public synonyms...
--drop public synonym offload;
--drop public synonym offload_objects;

@@sql/drop_offload_obsolete_objects.sql

@@sql/uninstall_offload_repo.sql

prompt
prompt ================================================================================
prompt OFFLOAD successfully removed.
prompt ================================================================================
prompt
undefine gluent_db_user_prefix
undefine gluent_db_adm_user
undefine gluent_db_app_user
undefine gluent_db_repo_user
undefine gluent_repo_installed
undefine gluent_repo_uninstall

@@sql/restore_sqlplus_env.sql
