-- upgrade_offload_repo.sql
--
-- LICENSE_TEXT
--

prompt Upgrading GOE repository...

@@create_offload_repo_privs.sql
alter session set current_schema = &goe_db_repo_user;
@@upgrade_offload_repo_deltas.sql
@@drop_offload_repo_code.sql
@@install_offload_repo_code.sql
