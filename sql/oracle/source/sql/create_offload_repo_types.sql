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
,   offload_hash_column         VARCHAR2(128)
,   offload_sort_columns        VARCHAR2(1000)
,   offload_partition_functions VARCHAR2(1000)
,   command_execution           RAW(32)
,   offload_version             VARCHAR2(30) 

--    hybrid_owner                VARCHAR2(128)
--,   hybrid_view                 VARCHAR2(128)
--,   hybrid_view_type            VARCHAR2(64)
--,   hybrid_external_table       VARCHAR2(128)
--,   hadoop_owner                VARCHAR2(1024)
--,   hadoop_table                VARCHAR2(1024)
--,   offload_type                VARCHAR2(30)
--,   offloaded_owner             VARCHAR2(128)
--,   offloaded_table             VARCHAR2(128)
--,   incremental_key             VARCHAR2(1000)
--,   incremental_high_value      CLOB
--,   incremental_range           VARCHAR2(20)
--,   incremental_predicate_type  VARCHAR2(30)
--,   incremental_predicate_value CLOB
--,   offload_bucket_column       VARCHAR2(128)
--,   offload_bucket_method       VARCHAR2(30)
--,   offload_bucket_count        NUMBER
--,   offload_version             VARCHAR2(30)
--,   offload_sort_columns        VARCHAR2(1000)
--,   transformations             VARCHAR2(1000)
--,   object_hash                 VARCHAR2(64)
--,   offload_scn                 NUMBER
--,   offload_partition_functions VARCHAR2(1000)
--,   iu_key_columns              VARCHAR2(1000)
--,   iu_extraction_method        VARCHAR2(30)
--,   iu_extraction_scn           NUMBER
--,   iu_extraction_time          NUMBER
--,   changelog_table             VARCHAR2(128)
--,   changelog_trigger           VARCHAR2(128)
--,   changelog_sequence          VARCHAR2(128)
--,   updatable_view              VARCHAR2(128)
--,   updatable_trigger           VARCHAR2(128)
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
    partition_boundary VARCHAR2(4000),
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
