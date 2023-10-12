-- install_offload_repo.sql
--
-- LICENSE_TEXT
--

prompt Installing Gluent Metadata Repository...

@@create_gluent_repo_user.sql
@@create_offload_repo_roles.sql
@@create_gluent_offload_repo_grants.sql
alter session set current_schema = &gluent_db_repo_user;
-- Start offload repo version files...
@@create_offload_repo_330_10g.sql
@@create_offload_repo_340_10g.sql
@@create_offload_repo_420.sql
@@create_offload_repo_421.sql
@@create_offload_repo_500_10g.sql
-- End offload repo version files.
@@install_offload_repo_code.sql
