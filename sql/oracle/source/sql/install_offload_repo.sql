-- install_offload_repo.sql
--
-- LICENSE_TEXT
--

prompt Installing GOE repository...

@@create_goe_repo_user.sql
@@create_offload_repo_privs.sql
alter session set current_schema = &goe_db_repo_user;
-- Start offload repo version files...
@@create_offload_repo_100.sql
-- End offload repo version files.
@@install_offload_repo_code.sql
