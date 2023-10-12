-- create_offload_tools_package_body.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE BODY offload_tools AS

    FUNCTION version RETURN VARCHAR2 IS
    BEGIN
        RETURN gc_version;
    END version;

END offload_tools;
/
