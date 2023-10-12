-- create_offload_repo_types.sql
--
-- LICENSE_TEXT
--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_metadata_ot AS OBJECT
(
    hybrid_owner                VARCHAR2(128)
,   hybrid_view                 VARCHAR2(128)
,   hybrid_view_type            VARCHAR2(64)
,   hybrid_external_table       VARCHAR2(128)
,   hadoop_owner                VARCHAR2(1024)
,   hadoop_table                VARCHAR2(1024)
,   offload_type                VARCHAR2(30)
,   offloaded_owner             VARCHAR2(128)
,   offloaded_table             VARCHAR2(128)
,   incremental_key             VARCHAR2(1000)
,   incremental_high_value      CLOB
,   incremental_range           VARCHAR2(20)
,   incremental_predicate_type  VARCHAR2(30)
,   incremental_predicate_value CLOB
,   offload_bucket_column       VARCHAR2(128)
,   offload_bucket_method       VARCHAR2(30)
,   offload_bucket_count        NUMBER
,   offload_version             VARCHAR2(30)
,   offload_sort_columns        VARCHAR2(1000)
,   transformations             VARCHAR2(1000)
,   object_hash                 VARCHAR2(64)
,   offload_scn                 NUMBER
,   offload_partition_functions VARCHAR2(1000)
,   iu_key_columns              VARCHAR2(1000)
,   iu_extraction_method        VARCHAR2(30)
,   iu_extraction_scn           NUMBER
,   iu_extraction_time          NUMBER
,   changelog_table             VARCHAR2(128)
,   changelog_trigger           VARCHAR2(128)
,   changelog_sequence          VARCHAR2(128)
,   updatable_view              VARCHAR2(128)
,   updatable_trigger           VARCHAR2(128)

,   CONSTRUCTOR FUNCTION offload_metadata_ot RETURN SELF AS RESULT

,   CONSTRUCTOR FUNCTION offload_metadata_ot ( p_hybrid_owner                IN VARCHAR2,
                                               p_hybrid_view                 IN VARCHAR2,
                                               p_hybrid_view_type            IN VARCHAR2,
                                               p_hybrid_external_table       IN VARCHAR2,
                                               p_hadoop_owner                IN VARCHAR2,
                                               p_hadoop_table                IN VARCHAR2,
                                               p_offload_type                IN VARCHAR2,
                                               p_offload_bucket_column       IN VARCHAR2,
                                               p_offload_bucket_method       IN VARCHAR2,
                                               p_offload_bucket_count        IN NUMBER,
                                               p_offload_partition_functions IN VARCHAR2 ) RETURN SELF AS RESULT

,   MEMBER FUNCTION offload_metadata_json RETURN CLOB
);
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE BODY offload_metadata_ot AS

    CONSTRUCTOR FUNCTION offload_metadata_ot RETURN SELF AS RESULT IS
    BEGIN
        RETURN;
    END;

    CONSTRUCTOR FUNCTION offload_metadata_ot ( p_hybrid_owner                IN VARCHAR2,
                                               p_hybrid_view                 IN VARCHAR2,
                                               p_hybrid_view_type            IN VARCHAR2,
                                               p_hybrid_external_table       IN VARCHAR2,
                                               p_hadoop_owner                IN VARCHAR2,
                                               p_hadoop_table                IN VARCHAR2,
                                               p_offload_type                IN VARCHAR2,
                                               p_offload_bucket_column       IN VARCHAR2,
                                               p_offload_bucket_method       IN VARCHAR2,
                                               p_offload_bucket_count        IN NUMBER,
                                               p_offload_partition_functions IN VARCHAR2 ) RETURN SELF AS RESULT IS
    BEGIN
        SELF.hybrid_owner                := p_hybrid_owner;
        SELF.hybrid_view                 := p_hybrid_view;
        SELF.hybrid_view_type            := p_hybrid_view_type;
        SELF.hybrid_external_table       := p_hybrid_external_table;
        SELF.hadoop_owner                := p_hadoop_owner;
        SELF.hadoop_table                := p_hadoop_table;
        SELF.offload_type                := p_offload_type;
        SELF.offload_bucket_column       := p_offload_bucket_column;
        SELF.offload_bucket_method       := p_offload_bucket_method;
        SELF.offload_bucket_count        := p_offload_bucket_count;
        SELF.offload_partition_functions := p_offload_partition_functions;
        RETURN;
    END;

    MEMBER FUNCTION offload_metadata_json RETURN CLOB IS
        FUNCTION NVL2_FN(p1 IN VARCHAR2, p2 IN VARCHAR2, p3 IN VARCHAR2) RETURN CLOB IS
        BEGIN
            RETURN CASE WHEN p1 IS NOT NULL THEN p2 ELSE p3 END;
        END NVL2_FN;
        FUNCTION NVL2_FN(p1 IN CLOB, p2 IN CLOB, p3 IN VARCHAR2) RETURN CLOB IS
        BEGIN
            RETURN CASE WHEN p1 IS NOT NULL THEN p2 ELSE p3 END;
        END NVL2_FN;
    BEGIN
        RETURN '{' ||
               '"HYBRID_OWNER": "'               || SELF.hybrid_owner || '", ' ||
               '"HYBRID_VIEW": "'                || SELF.hybrid_view || '", ' ||
               '"OBJECT_TYPE": "'                || SELF.hybrid_view_type || '", ' ||
               '"EXTERNAL_TABLE": "'             || SELF.hybrid_external_table || '", ' ||
               '"HADOOP_OWNER": "'               || SELF.hadoop_owner || '", ' ||
               '"HADOOP_TABLE": "'               || SELF.hadoop_table || '", ' ||
               '"OFFLOAD_TYPE": '                || NVL2_FN(SELF.offload_type, '"' || SELF.offload_type || '"', 'null') || ', ' ||
               '"OFFLOADED_OWNER": '             || NVL2_FN(SELF.offloaded_owner, '"' || SELF.offloaded_owner || '"', 'null') || ', ' ||
               '"OFFLOADED_TABLE": '             || NVL2_FN(SELF.offloaded_table, '"' || SELF.offloaded_table || '"', 'null') || ', ' ||
               '"INCREMENTAL_KEY": '             || NVL2_FN(SELF.incremental_key, '"' || SELF.incremental_key || '"', 'null') || ', ' ||
               '"INCREMENTAL_HIGH_VALUE": '      || NVL2_FN(SELF.incremental_high_value, '"' || SELF.incremental_high_value || '"', 'null') || ', ' ||
               '"INCREMENTAL_RANGE": '           || NVL2_FN(SELF.incremental_range, '"' || SELF.incremental_range || '"', 'null') || ', ' ||
               '"INCREMENTAL_PREDICATE_TYPE": '  || NVL2_FN(SELF.incremental_predicate_type, '"' || SELF.incremental_predicate_type || '"', 'null') || ', ' ||
               '"INCREMENTAL_PREDICATE_VALUE": ' || NVL2_FN(SELF.incremental_predicate_value, SELF.incremental_predicate_value, 'null') || ', ' ||
               '"OFFLOAD_BUCKET_COLUMN": '       || NVL2_FN(SELF.offload_bucket_column, '"' || SELF.offload_bucket_column || '"', 'null') || ', ' ||
               '"OFFLOAD_BUCKET_METHOD": '       || NVL2_FN(SELF.offload_bucket_method, '"' || SELF.offload_bucket_method || '"', 'null') || ', ' ||
               '"OFFLOAD_BUCKET_COUNT": '        || NVL(TO_CHAR(SELF.offload_bucket_count), 'null') || ', ' ||
               '"OFFLOAD_VERSION": '             || NVL2_FN(SELF.offload_version, '"' || SELF.offload_version || '"', 'null') || ', ' ||
               '"OFFLOAD_SORT_COLUMNS": '        || NVL2_FN(SELF.offload_sort_columns, '"' || SELF.offload_sort_columns || '"', 'null') || ', ' ||
               '"TRANSFORMATIONS": '             || NVL2_FN(SELF.transformations, SELF.transformations, 'null') || ', ' ||
               '"OBJECT_HASH": '                 || NVL2_FN(SELF.object_hash, '"' || SELF.object_hash || '"', 'null') || ', ' ||
               '"OFFLOAD_SCN": '                 || NVL(TO_CHAR(SELF.offload_scn), 'null') || ', ' ||
               '"OFFLOAD_PARTITION_FUNCTIONS": ' || NVL2_FN(SELF.offload_partition_functions, '"' || SELF.offload_partition_functions || '"', 'null') || ', ' ||
               '"IU_KEY_COLUMNS": '              || NVL2_FN(SELF.iu_key_columns, '"' || SELF.iu_key_columns || '"', 'null') || ', ' ||
               '"IU_EXTRACTION_METHOD": '        || NVL2_FN(SELF.iu_extraction_method, '"' || SELF.iu_extraction_method || '"', 'null') || ', ' ||
               '"IU_EXTRACTION_SCN": '           || NVL(TO_CHAR(SELF.iu_extraction_scn), 'null') || ', ' ||
               '"IU_EXTRACTION_TIME": '          || NVL(TO_CHAR(SELF.iu_extraction_time), 'null') || ', ' ||
               '"CHANGELOG_TABLE": '             || NVL2_FN(SELF.changelog_table, '"' || SELF.changelog_table || '"', 'null') || ', ' ||
               '"CHANGELOG_TRIGGER": '           || NVL2_FN(SELF.changelog_trigger, '"' || SELF.changelog_trigger || '"', 'null') || ', ' ||
               '"CHANGELOG_SEQUENCE": '          || NVL2_FN(SELF.changelog_sequence, '"' || SELF.changelog_sequence || '"', 'null') || ', ' ||
               '"UPDATABLE_VIEW": '              || NVL2_FN(SELF.updatable_view, '"' || SELF.updatable_view || '"', 'null') || ', ' ||
               '"UPDATABLE_TRIGGER": '           || NVL2_FN(SELF.updatable_trigger, '"' || SELF.updatable_trigger || '"', 'null') ||
               '}';
    END;
END;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_partition_ot AS OBJECT
(
    database_name      VARCHAR2(128),
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
