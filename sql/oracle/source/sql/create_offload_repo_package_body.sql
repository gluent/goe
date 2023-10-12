-- create_offload_repo_package_body.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE BODY offload_repo AS

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Types...
    TYPE args_ntt IS TABLE OF VARCHAR2(32767);

    TYPE offload_metadata_basic_rt IS RECORD
    ( hybrid_owner                offload_metadata.hybrid_owner%TYPE
    , hybrid_view                 offload_metadata.hybrid_view%TYPE
    , hybrid_view_type            offload_metadata.hybrid_view_type%TYPE
    , hybrid_external_table       offload_metadata.hybrid_external_table%TYPE
    , hadoop_owner                offload_metadata.hadoop_owner%TYPE
    , hadoop_table                offload_metadata.hadoop_table%TYPE
    , offload_type                offload_metadata.offload_type%TYPE
    , offload_bucket_column       offload_metadata.offload_bucket_column%TYPE
    , offload_bucket_method       offload_metadata.offload_bucket_method%TYPE
    , offload_bucket_count        offload_metadata.offload_bucket_count%TYPE
    , offload_partition_functions offload_metadata.offload_partition_functions%TYPE
    );

    TYPE lookup_cache_aat IS TABLE OF INTEGER
        INDEX BY VARCHAR2(30);

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Subtypes...
    SUBTYPE offload_metadata_full_rt IS offload_metadata%ROWTYPE;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Constants...

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Globals...
    g_status_cache       lookup_cache_aat;
    g_command_step_cache lookup_cache_aat;
    g_command_type_cache lookup_cache_aat;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION version RETURN VARCHAR2 IS
    BEGIN
        RETURN gc_version;
    END version;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION format_string ( p_str  IN VARCHAR2,
                             p_args IN args_ntt DEFAULT args_ntt(),
                             p_sub  IN VARCHAR2 DEFAULT 's' )
        RETURN VARCHAR2 IS
        v_sub  VARCHAR2(2)          := '%' || p_sub;
        v_str  VARCHAR2(32767)      := p_str;
        v_args PLS_INTEGER          := LEAST((LENGTH(v_str)-LENGTH(REPLACE(v_str,v_sub)))/LENGTH(v_sub),p_args.COUNT);
        v_pos  PLS_INTEGER;
    BEGIN
        FOR i IN 1 .. v_args LOOP
            v_pos := INSTR( v_str, v_sub );
            v_str := REPLACE(
                        SUBSTR( v_str, 1, v_pos + LENGTH(v_sub)-1 ), v_sub, p_args(i)
                        ) || SUBSTR( v_str, v_pos + LENGTH(v_sub) );
        END LOOP;
        RETURN v_str;
    END format_string;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION extract_clob_attribute ( p_metadata  IN CLOB,
                                      p_attribute IN VARCHAR2,
                                      p_regexp    IN VARCHAR2 DEFAULT '(")([^"]+)(")',
                                      p_subexp    IN NUMBER DEFAULT 3 )
        RETURN CLOB IS
        v_match  CLOB;
        v_prefix VARCHAR2(256);
    BEGIN
        v_prefix := '("' || p_attribute || '": )';
--$IF DBMS_DB_VERSION.VER_LE_10_2
--$THEN
        /* REGEXP_SUBSTR has no subexp parameter on 10g */
        v_match := REGEXP_SUBSTR(p_metadata, v_prefix || p_regexp, 1, 1, 'm');
        IF v_match IS NOT NULL THEN
            -- Remove the attribute name prefix so we are left with the value only.
            v_match := SUBSTR(v_match, LENGTH(v_prefix)-1);
            -- Match the provided pattern again and replace with subexp, this removes
            -- double quotes from string values.
            v_match := REGEXP_REPLACE(v_match, p_regexp, '\' || TO_CHAR(p_subexp-1));
        END IF;
        RETURN v_match;
--$ELSE
--        RETURN REGEXP_SUBSTR(p_metadata, v_prefix || p_regexp, 1, 1, 'm', p_subexp);
--$END
    END extract_clob_attribute;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION extract_varchar2_attribute ( p_metadata  IN CLOB,
                                          p_attribute IN VARCHAR2,
                                          p_regexp    IN VARCHAR2 DEFAULT '(")([^"]+)(")',
                                          p_subexp    IN NUMBER DEFAULT 3 )
        RETURN VARCHAR2 IS
    BEGIN
        RETURN TO_CHAR(extract_clob_attribute( p_metadata  => p_metadata,
                                               p_attribute => p_attribute,
                                               p_regexp    => p_regexp,
                                               p_subexp    => p_subexp ));
    END extract_varchar2_attribute;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION extract_number_attribute ( p_metadata  IN CLOB,
                                        p_attribute IN VARCHAR2,
                                        p_regexp    IN VARCHAR2 DEFAULT '([0-9]+)',
                                        p_subexp    IN NUMBER DEFAULT 2 )
        RETURN NUMBER IS
    BEGIN
        RETURN TO_NUMBER(extract_clob_attribute( p_metadata  => p_metadata,
                                                 p_attribute => p_attribute,
                                                 p_regexp    => p_regexp,
                                                 p_subexp    => p_subexp ));
    END extract_number_attribute;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_metadata_row_from_json ( p_metadata_json IN CLOB )
        RETURN offload_metadata_ot IS
        v_row offload_metadata_ot := offload_metadata_ot();
    BEGIN
        v_row.hybrid_owner                := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'HYBRID_OWNER' );
        v_row.hybrid_view                 := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'HYBRID_VIEW' );
        v_row.hybrid_view_type            := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OBJECT_TYPE' );
        v_row.hybrid_external_table       := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'EXTERNAL_TABLE' );
        v_row.hadoop_owner                := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'HADOOP_OWNER' );
        v_row.hadoop_table                := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'HADOOP_TABLE' );
        v_row.offload_type                := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_TYPE', p_regexp => '(")(INCREMENTAL|FULL)(")' );
        v_row.offloaded_owner             := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOADED_OWNER' );
        v_row.offloaded_table             := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOADED_TABLE' );
        v_row.incremental_key             := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'INCREMENTAL_KEY' );
        v_row.incremental_high_value      := EMPTY_CLOB(); -- avoid bug 27243840
        v_row.incremental_high_value      := extract_clob_attribute( p_metadata => p_metadata_json, p_attribute => 'INCREMENTAL_HIGH_VALUE' );
        v_row.incremental_high_value      := NULLIF(v_row.incremental_high_value, EMPTY_CLOB());  -- reverse the workaround for bug 27243840
        v_row.incremental_range           := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'INCREMENTAL_RANGE' );
        v_row.incremental_predicate_type  := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'INCREMENTAL_PREDICATE_TYPE' );
        v_row.incremental_predicate_value := EMPTY_CLOB(); -- avoid bug 27243840
        v_row.incremental_predicate_value := extract_clob_attribute( p_metadata => p_metadata_json, p_attribute => 'INCREMENTAL_PREDICATE_VALUE', p_regexp => '(\[.+?\])', p_subexp => 2 );
        v_row.incremental_predicate_value := NULLIF(v_row.incremental_predicate_value, EMPTY_CLOB());  -- reverse the workaround for bug 27243840
        v_row.offload_bucket_column       := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_BUCKET_COLUMN' );
        v_row.offload_bucket_method       := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_BUCKET_METHOD' );
        v_row.offload_bucket_count        := extract_number_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_BUCKET_COUNT' );
        v_row.offload_version             := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_VERSION' );
        v_row.offload_sort_columns        := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_SORT_COLUMNS' );
        v_row.offload_scn                 := extract_number_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_SCN' );
        v_row.object_hash                 := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OBJECT_HASH' );
        v_row.transformations             := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'TRANSFORMATIONS', p_regexp => '({.+?})', p_subexp => 2 );
        v_row.offload_partition_functions := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'OFFLOAD_PARTITION_FUNCTIONS' );
        v_row.iu_key_columns              := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'IU_KEY_COLUMNS' );
        v_row.iu_extraction_method        := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'IU_EXTRACTION_METHOD' );
        v_row.iu_extraction_scn           := extract_number_attribute( p_metadata => p_metadata_json, p_attribute => 'IU_EXTRACTION_SCN' );
        v_row.iu_extraction_time          := extract_number_attribute( p_metadata => p_metadata_json, p_attribute => 'IU_EXTRACTION_TIME' );
        v_row.changelog_table             := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'CHANGELOG_TABLE' );
        v_row.changelog_trigger           := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'CHANGELOG_TRIGGER' );
        v_row.changelog_sequence          := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'CHANGELOG_SEQUENCE' );
        v_row.updatable_view              := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'UPDATABLE_VIEW' );
        v_row.updatable_trigger           := extract_varchar2_attribute( p_metadata => p_metadata_json, p_attribute => 'UPDATABLE_TRIGGER' );
        RETURN v_row;
    END get_metadata_row_from_json;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_offload_partitions ( p_offload_chunk_id   IN offload_partition.offload_chunk_id%TYPE,
                                        p_frontend_object_id IN offload_partition.frontend_object_id%TYPE,
                                        p_partition_details  IN offload_partition_ntt ) IS
    BEGIN
        INSERT INTO offload_partition
            ( id
            , name
            , bytes
            , partitioning_level
            , boundary
            , offload_chunk_id
            , frontend_object_id
            )
        SELECT offload_partition_seq.NEXTVAL
        ,      partition_name
        ,      partition_bytes
        ,      partition_level
        ,      partition_boundary
        ,      p_offload_chunk_id
        ,      p_frontend_object_id
        FROM   TABLE(p_partition_details);
    END save_offload_partitions;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_frontend_object ( p_database_name      IN  frontend_object.database_name%TYPE,
                                     p_object_name        IN  frontend_object.object_name%TYPE,
                                     p_frontend_object_id OUT frontend_object.id%TYPE ) IS
    BEGIN
        INSERT INTO frontend_object
            (id, database_name, object_name, create_time)
        VALUES
            (frontend_object_seq.NEXTVAL, p_database_name, p_object_name, SYSTIMESTAMP)
        RETURNING id INTO p_frontend_object_id;
    END save_frontend_object;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_frontend_object_id ( p_database_name IN frontend_object.database_name%TYPE,
                                      p_object_name   IN frontend_object.object_name%TYPE )
        RETURN frontend_object.id%TYPE IS
        v_frontend_object_id frontend_object.id%TYPE;
    BEGIN
        BEGIN
            SELECT id
            INTO   v_frontend_object_id
            FROM   frontend_object
            WHERE  database_name = p_database_name
            AND    object_name = p_object_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                save_frontend_object( p_database_name      => p_database_name,
                                      p_object_name        => p_object_name,
                                      p_frontend_object_id => v_frontend_object_id );
        END;
        RETURN v_frontend_object_id;
    END get_frontend_object_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_backend_object ( p_database_name     IN  backend_object.database_name%TYPE,
                                    p_object_name       IN  backend_object.object_name%TYPE,
                                    p_backend_object_id OUT backend_object.id%TYPE ) IS
    BEGIN
        INSERT INTO backend_object
            (id, database_name, object_name, create_time)
        VALUES
            (backend_object_seq.NEXTVAL, p_database_name, p_object_name, SYSTIMESTAMP)
        RETURNING id INTO p_backend_object_id;
    END save_backend_object;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_backend_object_id ( p_database_name IN backend_object.database_name%TYPE,
                                     p_object_name   IN backend_object.object_name%TYPE )
        RETURN backend_object.id%TYPE IS
        v_backend_object_id backend_object.id%TYPE;
    BEGIN
        BEGIN
            SELECT id
            INTO   v_backend_object_id
            FROM   backend_object
            WHERE  database_name = p_database_name
            AND    object_name = p_object_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                save_backend_object( p_database_name     => p_database_name,
                                     p_object_name       => p_object_name,
                                     p_backend_object_id => v_backend_object_id );
        END;
        RETURN v_backend_object_id;
    END get_backend_object_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_command_execution_id ( p_command_execution_uuid IN command_execution.uuid%TYPE )
        RETURN command_execution.id%TYPE IS
        v_command_execution_id command_execution.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_command_execution_id
        FROM   command_execution
        WHERE  uuid = p_command_execution_uuid;
        RETURN v_command_execution_id;
    END get_command_execution_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_command_type_id ( p_command_type_code IN command_type.code%TYPE )
        RETURN command_type.id%TYPE IS
        v_command_type_id command_type.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_command_type_id
        FROM   command_type
        WHERE  code = p_command_type_code;
        RETURN v_command_type_id;
    END get_command_type_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_command_step_id ( p_command_step_code IN command_step.code%TYPE )
        RETURN command_step.id%TYPE IS
        v_command_step_id command_step.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_command_step_id
        FROM   command_step
        WHERE  code = p_command_step_code;
        RETURN v_command_step_id;
    END get_command_step_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_status_id ( p_status_code IN status.code%TYPE )
        RETURN status.id%TYPE IS
        v_status_id status.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_status_id
        FROM   status
        WHERE  code = p_status_code;
        RETURN v_status_id;
    END get_status_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_gdp_version_id ( p_version IN gdp_version.version%TYPE )
        RETURN gdp_version.id%TYPE IS
        v_gdp_version_id gdp_version.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_gdp_version_id
        FROM   gdp_version
        WHERE  version = p_version;
        RETURN v_gdp_version_id;
    END get_gdp_version_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_latest_gdp_version_id
        RETURN gdp_version.id%TYPE IS
        v_gdp_version_id gdp_version.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_gdp_version_id
        FROM   gdp_version
        WHERE  latest = 'Y';
        RETURN v_gdp_version_id;
    END get_latest_gdp_version_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_offload_metadata ( p_hybrid_owner IN VARCHAR2,
                                      p_hybrid_view  IN VARCHAR2,
                                      p_metadata     IN CLOB ) IS
    BEGIN
        save_offload_metadata( p_hybrid_owner         => p_hybrid_owner,
                               p_hybrid_view          => p_hybrid_view,
                               p_metadata             => get_metadata_row_from_json( p_metadata_json => p_metadata ) );
    END save_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_offload_metadata ( p_hybrid_owner           IN VARCHAR2,
                                      p_hybrid_view            IN VARCHAR2,
                                      p_metadata               IN offload_metadata_ot,
                                      p_command_execution_uuid IN RAW DEFAULT NULL ) IS

        v_command_execution_id offload_metadata.command_execution_id_current%TYPE;
        v_frontend_object_id   offload_metadata.frontend_object_id%TYPE;
        v_backend_object_id    offload_metadata.backend_object_id%TYPE;
        v_gdp_version_id       offload_metadata.gdp_version_id_current%TYPE;

    BEGIN

        -- Note: because the OFFLOAD_METADATA table is going to be refactored in a future release, and because
        --       of the code and tests that are supported by SAVE_OFFLOAD_METADATA, we've not changed the
        --       supporting OFFLOAD_METADATA_OT type to include the new PK and FK attributes. These are all
        --       derivable so will not be added to the OT for now. The COMMAND_EXECUTION_ID_CURRENT FK value will be
        --       provided as a separate scalar parameter.

        IF p_metadata.hybrid_owner IS NULL OR p_metadata.hybrid_view IS NULL OR p_metadata.hybrid_view_type IS NULL
        OR p_metadata.hybrid_external_table IS NULL OR p_metadata.hadoop_owner IS NULL OR p_metadata.hadoop_table IS NULL
        OR p_metadata.offload_type IS NULL THEN
            RAISE_APPLICATION_ERROR(-20000, 'Offload metadata is malformed or is missing mandatory attributes', TRUE);
        END IF;

        IF NOT (p_metadata.hybrid_owner = p_hybrid_owner AND p_metadata.hybrid_view = p_hybrid_view) THEN
            RAISE_APPLICATION_ERROR(-20001, format_string( p_str => 'Offload metadata and hybrid view mismatch (requested=%s.%s, supplied=%s.%s)',
                                                           p_args => args_ntt(p_hybrid_owner, p_hybrid_view, p_metadata.hybrid_owner, p_metadata.hybrid_view)), TRUE);
        END IF;

        -- Prepare the additional FK values...
        v_gdp_version_id := get_latest_gdp_version_id();

        v_backend_object_id := get_backend_object_id( p_database_name => p_metadata.hadoop_owner,
                                                      p_object_name   => p_metadata.hadoop_table );

        IF p_metadata.offloaded_owner IS NOT NULL AND p_metadata.offloaded_table IS NOT NULL THEN
            v_frontend_object_id := get_frontend_object_id( p_database_name => p_metadata.offloaded_owner,
                                                            p_object_name   => p_metadata.offloaded_table );
        END IF;

        IF p_command_execution_uuid IS NOT NULL THEN
            v_command_execution_id := get_command_execution_id( p_command_execution_uuid => p_command_execution_uuid );
        END IF;

        MERGE
            INTO offload_metadata tgt
            USING (
                    SELECT p_metadata.hybrid_owner AS hybrid_owner
                    ,      p_metadata.hybrid_view  AS hybrid_view
                    FROM   dual
                  ) src
            ON (    tgt.hybrid_owner = src.hybrid_owner
                AND tgt.hybrid_view = src.hybrid_view )
        WHEN MATCHED
        THEN
            UPDATE
            SET    tgt.hybrid_view_type             = p_metadata.hybrid_view_type
            ,      tgt.hybrid_external_table        = p_metadata.hybrid_external_table
            ,      tgt.hadoop_owner                 = p_metadata.hadoop_owner
            ,      tgt.hadoop_table                 = p_metadata.hadoop_table
            ,      tgt.offloaded_owner              = p_metadata.offloaded_owner
            ,      tgt.offloaded_table              = p_metadata.offloaded_table
            ,      tgt.offload_type                 = p_metadata.offload_type
            ,      tgt.incremental_key              = p_metadata.incremental_key
            ,      tgt.incremental_high_value       = p_metadata.incremental_high_value
            ,      tgt.incremental_range            = p_metadata.incremental_range
            ,      tgt.incremental_predicate_type   = p_metadata.incremental_predicate_type
            ,      tgt.incremental_predicate_value  = p_metadata.incremental_predicate_value
            ,      tgt.offload_bucket_column        = p_metadata.offload_bucket_column
            ,      tgt.offload_bucket_method        = p_metadata.offload_bucket_method
            ,      tgt.offload_bucket_count         = p_metadata.offload_bucket_count
            ,      tgt.offload_version              = p_metadata.offload_version
            ,      tgt.offload_sort_columns         = p_metadata.offload_sort_columns
            ,      tgt.offload_scn                  = p_metadata.offload_scn
            ,      tgt.object_hash                  = p_metadata.object_hash
            ,      tgt.transformations              = p_metadata.transformations
            ,      tgt.offload_partition_functions  = p_metadata.offload_partition_functions
            ,      tgt.iu_key_columns               = p_metadata.iu_key_columns
            ,      tgt.iu_extraction_method         = p_metadata.iu_extraction_method
            ,      tgt.iu_extraction_scn            = p_metadata.iu_extraction_scn
            ,      tgt.iu_extraction_time           = p_metadata.iu_extraction_time
            ,      tgt.changelog_table              = p_metadata.changelog_table
            ,      tgt.changelog_trigger            = p_metadata.changelog_trigger
            ,      tgt.changelog_sequence           = p_metadata.changelog_sequence
            ,      tgt.updatable_view               = p_metadata.updatable_view
            ,      tgt.updatable_trigger            = p_metadata.updatable_trigger
            ,      tgt.command_execution_id_current = v_command_execution_id
            ,      tgt.gdp_version_id_current       = v_gdp_version_id
            ,      tgt.frontend_object_id           = v_frontend_object_id
            ,      tgt.backend_object_id            = v_backend_object_id
        WHEN NOT MATCHED
        THEN
            INSERT ( id
                   , hybrid_owner
                   , hybrid_view
                   , hybrid_view_type
                   , hybrid_external_table
                   , hadoop_owner
                   , hadoop_table
                   , offloaded_owner
                   , offloaded_table
                   , offload_type
                   , incremental_key
                   , incremental_high_value
                   , incremental_range
                   , incremental_predicate_type
                   , incremental_predicate_value
                   , offload_bucket_column
                   , offload_bucket_method
                   , offload_bucket_count
                   , offload_version
                   , offload_sort_columns
                   , offload_scn
                   , object_hash
                   , transformations
                   , offload_partition_functions
                   , iu_key_columns
                   , iu_extraction_method
                   , iu_extraction_scn
                   , iu_extraction_time
                   , changelog_table
                   , changelog_trigger
                   , changelog_sequence
                   , updatable_view
                   , updatable_trigger
                   , command_execution_id_original
                   , command_execution_id_current
                   , gdp_version_id_original
                   , gdp_version_id_current
                   , frontend_object_id
                   , backend_object_id
                   )
            VALUES ( offload_metadata_seq.NEXTVAL
                   , p_metadata.hybrid_owner
                   , p_metadata.hybrid_view
                   , p_metadata.hybrid_view_type
                   , p_metadata.hybrid_external_table
                   , p_metadata.hadoop_owner
                   , p_metadata.hadoop_table
                   , p_metadata.offloaded_owner
                   , p_metadata.offloaded_table
                   , p_metadata.offload_type
                   , p_metadata.incremental_key
                   , p_metadata.incremental_high_value
                   , p_metadata.incremental_range
                   , p_metadata.incremental_predicate_type
                   , p_metadata.incremental_predicate_value
                   , p_metadata.offload_bucket_column
                   , p_metadata.offload_bucket_method
                   , p_metadata.offload_bucket_count
                   , p_metadata.offload_version
                   , p_metadata.offload_sort_columns
                   , p_metadata.offload_scn
                   , p_metadata.object_hash
                   , p_metadata.transformations
                   , p_metadata.offload_partition_functions
                   , p_metadata.iu_key_columns
                   , p_metadata.iu_extraction_method
                   , p_metadata.iu_extraction_scn
                   , p_metadata.iu_extraction_time
                   , p_metadata.changelog_table
                   , p_metadata.changelog_trigger
                   , p_metadata.changelog_sequence
                   , p_metadata.updatable_view
                   , p_metadata.updatable_trigger
                   , v_command_execution_id
                   , v_command_execution_id
                   , v_gdp_version_id
                   , v_gdp_version_id
                   , v_frontend_object_id
                   , v_backend_object_id
                   );

        COMMIT;

    END save_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE delete_offload_metadata ( p_hybrid_owner IN VARCHAR2,
                                        p_hybrid_view  IN VARCHAR2 ) IS
    BEGIN
        DELETE
        FROM   offload_metadata
        WHERE  hybrid_owner = p_hybrid_owner
        AND    hybrid_view  = p_hybrid_view;

        IF SQL%ROWCOUNT > 0 THEN
            COMMIT;
        END IF;
    END delete_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE validate_object_type ( p_object_type IN VARCHAR2 ) IS
    BEGIN
        IF p_object_type NOT IN (gc_metadata_source_type_table, gc_metadata_source_type_view) THEN
            RAISE_APPLICATION_ERROR(-20002, format_string( p_str  => 'Invalid value (%s) for object type. Valid values are %s or %s',
                                                           p_args => args_ntt(p_object_type, gc_metadata_source_type_table, gc_metadata_source_type_view) ), TRUE);
        END IF;
    END validate_object_type;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE validate_content_level ( p_content_level IN VARCHAR2 ) IS
    BEGIN
        IF p_content_level NOT IN (gc_metadata_content_full, gc_metadata_content_basic) THEN
            RAISE_APPLICATION_ERROR(-20003, format_string( p_str  => 'Invalid value (%s) for content level. Valid values are %s or %s',
                                                           p_args => args_ntt(p_content_level, gc_metadata_content_full, gc_metadata_content_basic) ), TRUE);
        END IF;
    END validate_content_level;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_offload_metadata ( p_object_owner  IN VARCHAR2,
                                     p_object_name   IN VARCHAR2,
                                     p_object_type   IN VARCHAR2,
                                     p_content_level IN VARCHAR2 DEFAULT gc_metadata_content_full,
                                     p_metadata      OUT SYS_REFCURSOR ) IS

        v_pred_column   VARCHAR2(128);
        v_proj_columns  VARCHAR2(1000);
        v_sql           VARCHAR2(4000);

    BEGIN

        validate_object_type( p_object_type => p_object_type );
        validate_content_level( p_content_level => p_content_level );

        v_pred_column := CASE p_object_type
                            WHEN gc_metadata_source_type_table
                            THEN 'hybrid_external_table'
                            WHEN gc_metadata_source_type_view
                            THEN 'hybrid_view'
                            ELSE '''NULL'''
                         END;

        v_proj_columns := CASE p_content_level
                             WHEN gc_metadata_content_full
                             THEN '*'
                             ELSE 'hybrid_owner, hybrid_view, hybrid_view_type, hybrid_external_table, hadoop_owner, ' ||
                                  'hadoop_table, offload_type, offload_bucket_column, offload_bucket_method, ' ||
                                  'offload_bucket_count, offload_partition_functions'
                          END;

        v_sql := format_string( p_str => 'SELECT /*+ INDEX(offload_metadata (hybrid_owner, %s)) */ %s ' ||
                                         'FROM offload_metadata ' ||
                                         'WHERE hybrid_owner = :p_object_owner ' ||
                                         'AND %s = :p_object_name',
                                p_args => args_ntt(v_pred_column, v_proj_columns, v_pred_column) );

        OPEN p_metadata FOR v_sql USING IN p_object_owner, IN p_object_name;

    END get_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_metadata ( p_object_owner  IN VARCHAR2,
                                    p_object_name   IN VARCHAR2,
                                    p_object_type   IN VARCHAR2,
                                    p_content_level IN VARCHAR2 DEFAULT gc_metadata_content_full ) RETURN offload_metadata_ot IS

        v_full_metadata    offload_metadata_full_rt;
        v_basic_metadata   offload_metadata_basic_rt;
        v_offload_metadata offload_metadata_ot;
        v_cursor           SYS_REFCURSOR;

    BEGIN

        validate_object_type( p_object_type => p_object_type );
        validate_content_level( p_content_level => p_content_level );

        get_offload_metadata( p_object_owner  => p_object_owner,
                              p_object_name   => p_object_name,
                              p_object_type   => p_object_type,
                              p_content_level => p_content_level,
                              p_metadata      => v_cursor );

        IF p_content_level = gc_metadata_content_full THEN
            FETCH v_cursor INTO v_full_metadata;
            IF v_full_metadata.hybrid_view IS NOT NULL THEN
                v_offload_metadata := offload_metadata_ot( hybrid_owner                => v_full_metadata.hybrid_owner,
                                                           hybrid_view                 => v_full_metadata.hybrid_view,
                                                           hybrid_view_type            => v_full_metadata.hybrid_view_type,
                                                           hybrid_external_table       => v_full_metadata.hybrid_external_table,
                                                           hadoop_owner                => v_full_metadata.hadoop_owner,
                                                           hadoop_table                => v_full_metadata.hadoop_table,
                                                           offload_type                => v_full_metadata.offload_type,
                                                           offloaded_owner             => v_full_metadata.offloaded_owner,
                                                           offloaded_table             => v_full_metadata.offloaded_table,
                                                           incremental_key             => v_full_metadata.incremental_key,
                                                           incremental_high_value      => v_full_metadata.incremental_high_value,
                                                           incremental_range           => v_full_metadata.incremental_range,
                                                           incremental_predicate_type  => v_full_metadata.incremental_predicate_type,
                                                           incremental_predicate_value => v_full_metadata.incremental_predicate_value,
                                                           offload_bucket_column       => v_full_metadata.offload_bucket_column,
                                                           offload_bucket_method       => v_full_metadata.offload_bucket_method,
                                                           offload_bucket_count        => v_full_metadata.offload_bucket_count,
                                                           offload_version             => v_full_metadata.offload_version,
                                                           offload_sort_columns        => v_full_metadata.offload_sort_columns,
                                                           object_hash                 => v_full_metadata.object_hash,
                                                           offload_scn                 => v_full_metadata.offload_scn,
                                                           transformations             => v_full_metadata.transformations,
                                                           offload_partition_functions => v_full_metadata.offload_partition_functions,
                                                           iu_key_columns              => v_full_metadata.iu_key_columns,
                                                           iu_extraction_method        => v_full_metadata.iu_extraction_method,
                                                           iu_extraction_scn           => v_full_metadata.iu_extraction_scn,
                                                           iu_extraction_time          => v_full_metadata.iu_extraction_time,
                                                           changelog_table             => v_full_metadata.changelog_table,
                                                           changelog_trigger           => v_full_metadata.changelog_trigger,
                                                           changelog_sequence          => v_full_metadata.changelog_sequence,
                                                           updatable_view              => v_full_metadata.updatable_view,
                                                           updatable_trigger           => v_full_metadata.updatable_trigger );
            END IF;
        ELSE
            FETCH v_cursor INTO v_basic_metadata;
            IF v_basic_metadata.hybrid_view IS NOT NULL THEN
                v_offload_metadata := offload_metadata_ot( p_hybrid_owner                => v_basic_metadata.hybrid_owner,
                                                           p_hybrid_view                 => v_basic_metadata.hybrid_view,
                                                           p_hybrid_view_type            => v_basic_metadata.hybrid_view_type,
                                                           p_hybrid_external_table       => v_basic_metadata.hybrid_external_table,
                                                           p_hadoop_owner                => v_basic_metadata.hadoop_owner,
                                                           p_hadoop_table                => v_basic_metadata.hadoop_table,
                                                           p_offload_type                => v_basic_metadata.offload_type,
                                                           p_offload_bucket_column       => v_basic_metadata.offload_bucket_column,
                                                           p_offload_bucket_method       => v_basic_metadata.offload_bucket_method,
                                                           p_offload_bucket_count        => v_basic_metadata.offload_bucket_count,
                                                           p_offload_partition_functions => v_basic_metadata.offload_partition_functions );
            END IF;
        END IF;

        CLOSE v_cursor;
        RETURN v_offload_metadata;

    EXCEPTION
        WHEN OTHERS THEN
            IF v_cursor%ISOPEN THEN
                CLOSE v_cursor;
            END IF;
            RAISE;
    END get_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_metadata_json ( p_object_owner  IN VARCHAR2,
                                         p_object_name   IN VARCHAR2,
                                         p_object_type   IN VARCHAR2,
                                         p_content_level IN VARCHAR2 DEFAULT gc_metadata_content_full ) RETURN CLOB IS
    BEGIN

        validate_object_type( p_object_type => p_object_type );
        validate_content_level( p_content_level => p_content_level );

        RETURN get_offload_metadata( p_object_owner  => p_object_owner,
                                     p_object_name   => p_object_name,
                                     p_object_type   => p_object_type,
                                     p_content_level => p_content_level ).offload_metadata_json();
    END get_offload_metadata_json;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_metadata_json ( p_hybrid_owner                VARCHAR2,
                                         p_hybrid_view                 VARCHAR2,
                                         p_hybrid_view_type            VARCHAR2,
                                         p_hybrid_external_table       VARCHAR2,
                                         p_hadoop_owner                VARCHAR2,
                                         p_hadoop_table                VARCHAR2,
                                         p_offload_type                VARCHAR2,
                                         p_offloaded_owner             VARCHAR2,
                                         p_offloaded_table             VARCHAR2,
                                         p_incremental_key             VARCHAR2,
                                         p_incremental_high_value      CLOB,
                                         p_incremental_range           VARCHAR2,
                                         p_incremental_predicate_type  VARCHAR2,
                                         p_incremental_predicate_value CLOB,
                                         p_offload_bucket_column       VARCHAR2,
                                         p_offload_bucket_method       VARCHAR2,
                                         p_offload_bucket_count        NUMBER,
                                         p_offload_version             VARCHAR2,
                                         p_offload_sort_columns        VARCHAR2,
                                         p_transformations             VARCHAR2,
                                         p_object_hash                 VARCHAR2,
                                         p_offload_scn                 NUMBER,
                                         p_offload_partition_functions VARCHAR2,
                                         p_iu_key_columns              VARCHAR2,
                                         p_iu_extraction_method        VARCHAR2,
                                         p_iu_extraction_scn           NUMBER,
                                         p_iu_extraction_time          NUMBER,
                                         p_changelog_table             VARCHAR2,
                                         p_changelog_trigger           VARCHAR2,
                                         p_changelog_sequence          VARCHAR2,
                                         p_updatable_view              VARCHAR2,
                                         p_updatable_trigger           VARCHAR2)
        RETURN CLOB IS
    BEGIN
        RETURN offload_metadata_ot(
            p_hybrid_owner,
            p_hybrid_view,
            p_hybrid_view_type,
            p_hybrid_external_table,
            p_hadoop_owner,
            p_hadoop_table,
            p_offload_type,
            p_offloaded_owner,
            p_offloaded_table,
            p_incremental_key,
            p_incremental_high_value,
            p_incremental_range,
            p_incremental_predicate_type,
            p_incremental_predicate_value,
            p_offload_bucket_column,
            p_offload_bucket_method,
            p_offload_bucket_count,
            p_offload_version,
            p_offload_sort_columns,
            p_transformations,
            p_object_hash,
            p_offload_scn,
            p_offload_partition_functions,
            p_iu_key_columns,
            p_iu_extraction_method,
            p_iu_extraction_scn,
            p_iu_extraction_time,
            p_changelog_table,
            p_changelog_trigger,
            p_changelog_sequence,
            p_updatable_view,
            p_updatable_trigger).offload_metadata_json();
    END get_offload_metadata_json;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE start_command_execution ( p_uuid                 IN  RAW,
                                        p_command_type         IN  VARCHAR2,
                                        p_command_log_path     IN  VARCHAR2,
                                        p_command_input        IN  CLOB,
                                        p_command_parameters   IN  CLOB,
                                        p_command_execution_id OUT INTEGER ) IS

        v_command_type_id command_execution.command_type_id%TYPE;
        v_status_id       command_execution.status_id%TYPE;
        v_gdp_version_id  command_execution.gdp_version_id%TYPE;

    BEGIN

        v_command_type_id := get_command_type_id( p_command_type_code => p_command_type );
        v_status_id := get_status_id( p_status_code => gc_status_executing );
        v_gdp_version_id := get_latest_gdp_version_id();

        INSERT INTO command_execution
            ( id
            , uuid
            , start_time
            , command_log_path
            , command_input
            , command_parameters
            , command_type_id
            , status_id
            , gdp_version_id
            )
        VALUES
            ( command_execution_seq.NEXTVAL
            , p_uuid
            , SYSTIMESTAMP
            , p_command_log_path
            , p_command_input
            , p_command_parameters
            , v_command_type_id
            , v_status_id
            , v_gdp_version_id
            )
        RETURNING id INTO p_command_execution_id;

        COMMIT;

    END start_command_execution;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE end_command_execution ( p_command_execution_id IN INTEGER,
                                      p_status               IN VARCHAR2 ) IS

        v_status_id command_execution.status_id%TYPE;

    BEGIN

        v_status_id := get_status_id( p_status_code => p_status );

        UPDATE command_execution
        SET    end_time  = SYSTIMESTAMP
        ,      status_id = v_status_id
        WHERE  id        = p_command_execution_id;

        COMMIT;

    END end_command_execution;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE start_command_execution_step ( p_command_execution_uuid    IN  RAW,
                                             p_command_type              IN  VARCHAR2,
                                             p_command_step              IN  VARCHAR2,
                                             p_command_execution_step_id OUT INTEGER ) IS

        v_command_execution_id command_execution_step.command_execution_id%TYPE;
        v_command_step_id      command_execution_step.command_step_id%TYPE;
        v_command_type_id      command_execution_step.command_type_id%TYPE;
        v_status_id            command_execution_step.status_id%TYPE;

    BEGIN

        v_command_execution_id := get_command_execution_id( p_command_execution_uuid => p_command_execution_uuid );
        v_command_step_id := get_command_step_id( p_command_step_code => p_command_step );
        v_command_type_id := get_command_type_id( p_command_type_code => p_command_type );
        v_status_id := get_status_id( p_status_code => gc_status_executing );

        INSERT INTO command_execution_step
            ( id
            , start_time
            , status_id
            , command_step_id
            , command_type_id
            , command_execution_id
            )
        VALUES
            ( command_execution_step_seq.NEXTVAL
            , SYSTIMESTAMP
            , v_status_id
            , v_command_step_id
            , v_command_type_id
            , v_command_execution_id
            )
        RETURNING id INTO p_command_execution_step_id;

        COMMIT;

    END start_command_execution_step;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE end_command_execution_step ( p_command_execution_step_id IN INTEGER,
                                           p_step_details              IN CLOB,
                                           p_status                    IN VARCHAR2 ) IS

        v_status_id command_execution_step.status_id%TYPE;

    BEGIN

        v_status_id := get_status_id( p_status_code => p_status );

        UPDATE command_execution_step
        SET    end_time     = SYSTIMESTAMP
        ,      status_id    = v_status_id
        ,      step_details = p_step_details
        WHERE  id           = p_command_execution_step_id;

        COMMIT;

    END end_command_execution_step;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE start_offload_chunk ( p_command_execution_uuid IN  RAW,
                                    p_frontend_db_name       IN  VARCHAR2,
                                    p_frontend_object_name   IN  VARCHAR2,
                                    p_backend_db_name        IN  VARCHAR2,
                                    p_backend_object_name    IN  VARCHAR2,
                                    p_chunk_number           IN  INTEGER,
                                    p_partition_details      IN  offload_partition_ntt,
                                    p_offload_chunk_id       OUT INTEGER ) IS

        v_command_execution_id offload_chunk.command_execution_id%TYPE;
        v_frontend_object_id   offload_chunk.frontend_object_id%TYPE;
        v_backend_object_id    offload_chunk.backend_object_id%TYPE;
        v_status_id            offload_chunk.status_id%TYPE;

    BEGIN

        v_command_execution_id := get_command_execution_id( p_command_execution_uuid => p_command_execution_uuid );
        v_frontend_object_id := get_frontend_object_id( p_database_name => p_frontend_db_name,
                                                        p_object_name   => p_frontend_object_name );
        v_backend_object_id := get_backend_object_id( p_database_name => p_backend_db_name,
                                                      p_object_name   => p_backend_object_name );
        v_status_id := get_status_id( p_status_code => gc_status_executing );

        INSERT INTO offload_chunk
            ( id
            , chunk_number
            , start_time
            , status_id
            , frontend_object_id
            , backend_object_id
            , command_execution_id
            )
        VALUES
            ( offload_chunk_seq.NEXTVAL
            , p_chunk_number
            , SYSTIMESTAMP
            , v_status_id
            , v_frontend_object_id
            , v_backend_object_id
            , v_command_execution_id
            )
        RETURNING id INTO p_offload_chunk_id;

        IF p_partition_details IS NOT NULL THEN
            save_offload_partitions ( p_offload_chunk_id   => p_offload_chunk_id,
                                      p_frontend_object_id => v_frontend_object_id,
                                      p_partition_details  => p_partition_details );
        END IF;

        COMMIT;

    END start_offload_chunk;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE end_offload_chunk ( p_offload_chunk_id IN INTEGER,
                                  p_rows             IN INTEGER,
                                  p_frontend_bytes   IN INTEGER,
                                  p_transport_bytes  IN INTEGER,
                                  p_backend_bytes    IN INTEGER,
                                  p_status           IN VARCHAR2 ) IS

        v_status_id offload_chunk.status_id%TYPE;

    BEGIN

        v_status_id := get_status_id( p_status_code => p_status );

        UPDATE offload_chunk
        SET    end_time        = SYSTIMESTAMP
        ,      chunk_rows      = p_rows
        ,      frontend_bytes  = p_frontend_bytes
        ,      transport_bytes = p_transport_bytes
        ,      backend_bytes   = p_backend_bytes
        ,      status_id       = v_status_id
        WHERE  id              = p_offload_chunk_id;

        COMMIT;

    END end_offload_chunk;

END offload_repo;
/
