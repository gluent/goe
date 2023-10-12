-- create_offload_repo_330_package_spec.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE offload_repo_330 AS

    gc_version CONSTANT VARCHAR2(512) := '%s-SNAPSHOT';
    FUNCTION version RETURN VARCHAR2;

    PROCEDURE migrate_metadata;

END offload_repo_330;
/
