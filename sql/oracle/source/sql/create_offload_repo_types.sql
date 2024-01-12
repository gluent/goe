-- create_offload_repo_types.sql
--
-- LICENSE_TEXT
--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_metadata_ot AS OBJECT
(
    frontend_object_owner       VARCHAR2(128)
,   frontend_object_name        VARCHAR2(128)
,   backend_object_owner        VARCHAR2(1024)
,   backend_object_name         VARCHAR2(1024)
,   offload_type                VARCHAR2(30)
,   offload_range_type          VARCHAR2(30)
,   offload_key                 VARCHAR2(1000)
,   offload_high_value          CLOB
,   offload_predicate_type      VARCHAR2(30)
,   offload_predicate_value     CLOB
,   offload_snapshot            INTEGER
,   offload_hash_column         VARCHAR2(128)
,   offload_sort_columns        VARCHAR2(1000)
,   offload_partition_functions VARCHAR2(1000)
,   command_execution           RAW(32)
);
/
SHOW ERRORS

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_partition_ot AS OBJECT
(
    table_owner        VARCHAR2(128),
    table_name         VARCHAR2(128),
    partition_name     VARCHAR2(128),
    partition_level    INTEGER,
    partition_bytes    INTEGER,
    partition_boundary CLOB,
    CONSTRUCTOR FUNCTION offload_partition_ot RETURN SELF AS RESULT
);
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE BODY offload_partition_ot AS

    CONSTRUCTOR FUNCTION offload_partition_ot RETURN SELF AS RESULT IS
    BEGIN
        RETURN;
    END;

END;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_partition_ntt AS
    TABLE OF offload_partition_ot;
/
