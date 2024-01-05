-- create_offload_repo_package_body.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE BODY offload_repo AS

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Types...
    TYPE args_ntt IS TABLE OF VARCHAR2(32767);

    TYPE lookup_cache_aat IS TABLE OF INTEGER
        INDEX BY VARCHAR2(30);

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Globals...
    g_status_cache                 lookup_cache_aat;
    g_command_step_cache           lookup_cache_aat;
    g_command_type_cache           lookup_cache_aat;
    g_offload_predicate_type_cache lookup_cache_aat;
    g_offload_range_type_cache     lookup_cache_aat;
    g_offload_type_cache           lookup_cache_aat;

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
    PROCEDURE save_frontend_object ( p_object_owner       IN  frontend_object.object_owner%TYPE,
                                     p_object_name        IN  frontend_object.object_name%TYPE,
                                     p_frontend_object_id OUT frontend_object.id%TYPE ) IS
    BEGIN
        INSERT INTO frontend_object
            (id, object_owner, object_name, create_time)
        VALUES
            (frontend_object_seq.NEXTVAL, p_object_owner, p_object_name, SYSTIMESTAMP)
        RETURNING id INTO p_frontend_object_id;
    END save_frontend_object;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_frontend_object_id ( p_object_owner IN frontend_object.object_owner%TYPE,
                                      p_object_name  IN frontend_object.object_name%TYPE )
        RETURN frontend_object.id%TYPE IS
        v_frontend_object_id frontend_object.id%TYPE;
    BEGIN
        BEGIN
            SELECT id
            INTO   v_frontend_object_id
            FROM   frontend_object
            WHERE  object_owner = p_object_owner
            AND    object_name = p_object_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                save_frontend_object( p_object_owner       => p_object_owner,
                                      p_object_name        => p_object_name,
                                      p_frontend_object_id => v_frontend_object_id );
        END;
        RETURN v_frontend_object_id;
    END get_frontend_object_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_backend_object ( p_object_owner      IN  backend_object.object_owner%TYPE,
                                    p_object_name       IN  backend_object.object_name%TYPE,
                                    p_backend_object_id OUT backend_object.id%TYPE ) IS
    BEGIN
        INSERT INTO backend_object
            (id, object_owner, object_name, create_time)
        VALUES
            (backend_object_seq.NEXTVAL, p_object_owner, p_object_name, SYSTIMESTAMP)
        RETURNING id INTO p_backend_object_id;
    END save_backend_object;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_backend_object_id ( p_object_owner IN backend_object.object_owner%TYPE,
                                     p_object_name  IN backend_object.object_name%TYPE )
        RETURN backend_object.id%TYPE IS
        v_backend_object_id backend_object.id%TYPE;
    BEGIN
        BEGIN
            SELECT id
            INTO   v_backend_object_id
            FROM   backend_object
            WHERE  object_owner = p_object_owner
            AND    object_name = p_object_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                save_backend_object( p_object_owner      => p_object_owner,
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
    FUNCTION get_offload_predicate_type_id ( p_offload_predicate_type_code IN offload_predicate_type.code%TYPE )
        RETURN offload_predicate_type.id%TYPE IS
        v_offload_predicate_type_id offload_predicate_type.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_offload_predicate_type_id
        FROM   offload_predicate_type
        WHERE  code = p_offload_predicate_type_code;
        RETURN v_offload_predicate_type_id;
    END get_offload_predicate_type_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_range_type_id ( p_offload_range_type_code IN offload_range_type.code%TYPE )
        RETURN offload_range_type.id%TYPE IS
        v_offload_range_type_id offload_range_type.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_offload_range_type_id
        FROM   offload_range_type
        WHERE  code = p_offload_range_type_code;
        RETURN v_offload_range_type_id;
    END get_offload_range_type_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_type_id ( p_offload_type_code IN offload_type.code%TYPE )
        RETURN offload_type.id%TYPE IS
        v_offload_type_id offload_type.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_offload_type_id
        FROM   offload_type
        WHERE  code = p_offload_type_code;
        RETURN v_offload_type_id;
    END get_offload_type_id;

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
    FUNCTION get_goe_version_id ( p_version IN goe_version.version%TYPE )
        RETURN goe_version.id%TYPE IS
        v_goe_version_id goe_version.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_goe_version_id
        FROM   goe_version
        WHERE  version = p_version;
        RETURN v_goe_version_id;
    END get_goe_version_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_latest_goe_version_id
        RETURN goe_version.id%TYPE IS
        v_goe_version_id goe_version.id%TYPE;
    BEGIN
        SELECT id
        INTO   v_goe_version_id
        FROM   goe_version
        WHERE  latest = 'Y';
        RETURN v_goe_version_id;
    END get_latest_goe_version_id;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_latest_goe_version
        RETURN VARCHAR2 IS
        v_version goe_version.version%TYPE;
    BEGIN
        SELECT version
        INTO   v_version
        FROM   goe_version
        WHERE  latest = 'Y';
        RETURN v_version;
    END get_latest_goe_version;


    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_offload_metadata ( p_frontend_object_owner IN VARCHAR2,
                                      p_frontend_object_name  IN VARCHAR2,
                                      p_metadata              IN offload_metadata_ot ) IS

        v_command_execution_id      offload_metadata.command_execution_id_current%TYPE;
        v_frontend_object_id        offload_metadata.frontend_object_id%TYPE;
        v_backend_object_id         offload_metadata.backend_object_id%TYPE;
        v_goe_version_id            offload_metadata.goe_version_id_current%TYPE;
        v_offload_predicate_type_id offload_metadata.offload_predicate_type_id%TYPE;
        v_offload_range_type_id     offload_metadata.offload_range_type_id%TYPE;
        v_offload_type_id           offload_metadata.offload_type_id%TYPE;

    BEGIN

        IF p_metadata.frontend_object_owner IS NULL OR p_metadata.frontend_object_name IS NULL
        OR p_metadata.backend_object_owner IS NULL OR p_metadata.backend_object_name IS NULL
        OR p_metadata.offload_type IS NULL OR p_metadata.command_execution IS NULL THEN
            RAISE_APPLICATION_ERROR(-20000, 'Offload metadata is missing mandatory attributes', TRUE);
        END IF;

        IF NOT (p_metadata.frontend_object_owner = p_frontend_object_owner AND p_metadata.frontend_object_name = p_frontend_object_name) THEN
            RAISE_APPLICATION_ERROR(-20001, format_string( p_str => 'Offload metadata and frontend object mismatch (requested=%s.%s, supplied=%s.%s)',
                                                           p_args => args_ntt(p_frontend_object_owner, p_frontend_object_name,
                                                                              p_metadata.frontend_object_owner, p_metadata.frontend_object_name)), TRUE);
        END IF;

        -- Prepare the additional FK values...
        v_frontend_object_id := get_frontend_object_id( p_object_owner => p_metadata.frontend_object_owner,
                                                        p_object_name  => p_metadata.frontend_object_name );

        v_backend_object_id := get_backend_object_id( p_object_owner => p_metadata.backend_object_owner,
                                                      p_object_name  => p_metadata.backend_object_name );

        v_offload_type_id := get_offload_type_id( p_offload_type_code => p_metadata.offload_type );

        v_command_execution_id := get_command_execution_id( p_command_execution_uuid => p_metadata.command_execution );

        v_goe_version_id := get_latest_goe_version_id();

        IF p_metadata.offload_predicate_type IS NOT NULL THEN
            v_offload_predicate_type_id := get_offload_predicate_type_id( p_offload_predicate_type_code => p_metadata.offload_predicate_type );
        END IF;

        IF p_metadata.offload_range_type IS NOT NULL THEN
            v_offload_range_type_id := get_offload_range_type_id( p_offload_range_type_code => p_metadata.offload_range_type );
        END IF;

        MERGE
            INTO offload_metadata tgt
            USING (SELECT v_frontend_object_id AS frontend_object_id FROM dual) src
            ON (tgt.frontend_object_id = src.frontend_object_id)
        WHEN MATCHED
        THEN
            UPDATE
            SET    backend_object_id             = v_backend_object_id
            ,      offload_type_id               = v_offload_type_id
            ,      offload_range_type_id         = v_offload_range_type_id
            ,      offload_key                   = p_metadata.offload_key
            ,      offload_high_value            = p_metadata.offload_high_value
            ,      offload_predicate_type_id     = v_offload_predicate_type_id
            ,      offload_predicate_value       = p_metadata.offload_predicate_value
            ,      offload_hash_column           = p_metadata.offload_hash_column
            ,      offload_sort_columns          = p_metadata.offload_sort_columns
            ,      offload_partition_functions   = p_metadata.offload_partition_functions
            ,      command_execution_id_current  = v_command_execution_id
            ,      goe_version_id_current        = v_goe_version_id
        WHEN NOT MATCHED
        THEN
            INSERT ( id
                   , frontend_object_id
                   , backend_object_id
                   , offload_type_id
                   , offload_range_type_id
                   , offload_key
                   , offload_high_value
                   , offload_predicate_type_id
                   , offload_predicate_value
                   , offload_snapshot
                   , offload_hash_column
                   , offload_sort_columns
                   , offload_partition_functions
                   , command_execution_id_initial
                   , command_execution_id_current
                   , goe_version_id_initial
                   , goe_version_id_current
                   )
            VALUES ( offload_metadata_seq.NEXTVAL
                   , v_frontend_object_id
                   , v_backend_object_id
                   , v_offload_type_id
                   , v_offload_range_type_id
                   , p_metadata.offload_key
                   , p_metadata.offload_high_value
                   , v_offload_predicate_type_id
                   , p_metadata.offload_predicate_value
                   , p_metadata.offload_snapshot
                   , p_metadata.offload_hash_column
                   , p_metadata.offload_sort_columns
                   , p_metadata.offload_partition_functions
                   , v_command_execution_id
                   , v_command_execution_id
                   , v_goe_version_id
                   , v_goe_version_id
                   );

        COMMIT;

    END save_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE delete_offload_metadata ( p_frontend_object_owner IN VARCHAR2,
                                        p_frontend_object_name  IN VARCHAR2 ) IS
        v_frontend_object_id offload_metadata.frontend_object_id%TYPE;
    BEGIN
        v_frontend_object_id := get_frontend_object_id( p_object_owner => p_frontend_object_owner,
                                                        p_object_name  => p_frontend_object_name );
        DELETE
        FROM   offload_metadata
        WHERE  frontend_object_id = v_frontend_object_id;

        IF SQL%ROWCOUNT > 0 THEN
            COMMIT;
        END IF;
    END delete_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_metadata ( p_frontend_object_owner IN VARCHAR2,
                                    p_frontend_object_name  IN VARCHAR2 )
        RETURN offload_metadata_ot IS

        v_offload_metadata offload_metadata_ot;

    BEGIN

        SELECT offload_metadata_ot(
                    v.frontend_object_owner,
                    v.frontend_object_name,
                    v.backend_object_owner,
                    v.backend_object_name,
                    v.offload_type,
                    v.offload_range_type,
                    v.offload_key,
                    v.offload_high_value,
                    v.offload_predicate_type,
                    v.offload_predicate_value,
                    v.offload_snapshot,
                    v.offload_hash_column,
                    v.offload_sort_columns,
                    v.offload_partition_functions,
                    v.command_execution_current
                    )
        INTO   v_offload_metadata
        FROM   offload_metadata_v v
        WHERE  v.frontend_object_owner = p_frontend_object_owner
        AND    v.frontend_object_name = p_frontend_object_name;

        RETURN v_offload_metadata;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN v_offload_metadata;
    END get_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE start_command_execution ( p_uuid                 IN  RAW,
                                        p_command_type         IN  VARCHAR2,
                                        p_command_log_path     IN  VARCHAR2,
                                        p_command_input        IN  CLOB,
                                        p_command_parameters   IN  CLOB,
                                        p_command_execution_id OUT INTEGER ) IS

        v_command_type_id command_execution.command_type_id%TYPE;
        v_status_id       command_execution.status_id%TYPE;
        v_goe_version_id  command_execution.goe_version_id%TYPE;

    BEGIN

        v_command_type_id := get_command_type_id( p_command_type_code => p_command_type );
        v_status_id := get_status_id( p_status_code => gc_status_executing );
        v_goe_version_id := get_latest_goe_version_id();

        INSERT INTO command_execution
            ( id
            , uuid
            , start_time
            , command_log_path
            , command_input
            , command_parameters
            , command_type_id
            , status_id
            , goe_version_id
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
            , v_goe_version_id
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
                                    p_frontend_object_owner  IN  VARCHAR2,
                                    p_frontend_object_name   IN  VARCHAR2,
                                    p_backend_object_owner   IN  VARCHAR2,
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
        v_frontend_object_id := get_frontend_object_id( p_object_owner => p_frontend_object_owner,
                                                        p_object_name  => p_frontend_object_name );
        v_backend_object_id := get_backend_object_id( p_object_owner => p_backend_object_owner,
                                                      p_object_name  => p_backend_object_name );
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
