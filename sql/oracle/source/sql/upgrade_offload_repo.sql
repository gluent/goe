-- upgrade_offload_repo.sql
--
-- LICENSE_TEXT
--

prompt Upgrading Gluent Metadata Repository...

@@create_gluent_offload_repo_grants.sql
@@create_offload_repo_roles.sql
alter session set current_schema = &gluent_db_repo_user;
@@upgrade_offload_repo_deltas.sql
@@drop_offload_repo_code.sql
@@install_offload_repo_code.sql
