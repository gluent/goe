-- create_offload_repo_500_package_body.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE BODY offload_repo_500 AS

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Types...
    TYPE args_ntt IS TABLE OF VARCHAR2(32767);

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Subtypes...

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Constants...

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Globals...

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION version RETURN VARCHAR2 IS
    BEGIN
        RETURN gc_version;
    END version;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE populate_backend_object IS
    BEGIN
        MERGE
            INTO backend_object bo
            USING (
                    SELECT DISTINCT
                           hadoop_owner
                    ,      hadoop_table
                    FROM   offload_metadata
                    ORDER  BY
                           hadoop_owner
                    ,      hadoop_table
                  ) om
            ON (    bo.database_name = om.hadoop_owner
                AND bo.object_name   = om.hadoop_table)
        WHEN NOT MATCHED
        THEN
            INSERT
                ( bo.id
                , bo.database_name
                , bo.object_name
                , bo.create_time
                )
            VALUES
                ( backend_object_seq.NEXTVAL
                , om.hadoop_owner
                , om.hadoop_table
                , SYSTIMESTAMP
                );
    END populate_backend_object;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE populate_frontend_object IS
    BEGIN
        MERGE
            INTO frontend_object fo
            USING (
                    SELECT DISTINCT
                           offloaded_owner
                    ,      offloaded_table
                    FROM   offload_metadata
                    WHERE  offloaded_owner IS NOT NULL
                    ORDER  BY
                           offloaded_owner
                    ,      offloaded_table
                  ) om
            ON (    fo.database_name = om.offloaded_owner
                AND fo.object_name   = om.offloaded_table)
        WHEN NOT MATCHED
        THEN
            INSERT
                ( fo.id
                , fo.database_name
                , fo.object_name
                , fo.create_time
                )
            VALUES
                ( frontend_object_seq.NEXTVAL
                , om.offloaded_owner
                , om.offloaded_table
                , SYSTIMESTAMP
                );
    END populate_frontend_object;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE populate_command_type IS
        PROCEDURE add_command_type ( p_code IN command_type.code%TYPE,
                                     p_name IN command_type.name%TYPE ) IS
        BEGIN
            INSERT INTO command_type
                (id, code, name, create_time)
            VALUES
                (command_type_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
        END add_command_type;
    BEGIN
        add_command_type('OFFLOAD', 'Offload');
        add_command_type('OFFJOIN', 'Offload Join');
        add_command_type('PRESENT', 'Present');
        add_command_type('PRESJOIN', 'Present Join');
        add_command_type('IUENABLE', 'Enable Incremental Update');
        add_command_type('IUDISABLE', 'Disable Incremental Update');
        add_command_type('IUEXTRACT', 'Incremental Update Extraction');
        add_command_type('IUCOMPACT', 'Incremental Update Compaction');
        add_command_type('SCHSYNC', 'Schema Sync');
        add_command_type('DIAGNOSE', 'Diagnose');
        add_command_type('OSR', 'Offload Status Report');
    END populate_command_type;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE populate_command_step IS
        PROCEDURE add_command_step ( p_code  IN command_step.code%TYPE,
                                     p_title IN command_step.title%TYPE ) IS
        BEGIN
            INSERT INTO command_step
                (id, code, title, create_time)
            VALUES
                (command_step_seq.NEXTVAL, p_code, p_title, SYSTIMESTAMP);
        END add_command_step;
    BEGIN
        add_command_step('ALTER_TABLE', 'Alter backend table');
        add_command_step('ANALYZE_DATA_TYPES', 'Analyzing data types');
        add_command_step('COMPACTION_ADD_DELTA_PARTITION', 'Add partition to delta table');
        add_command_step('COMPACTION_BACKUP_PARTITION', 'Backup pre-compaction partition');
        add_command_step('COMPACTION_BACKUP_TABLE', 'Create pre-compaction backup table');
        add_command_step('COMPUTE_BASE_STATS', 'Compute stats for new base table');
        add_command_step('COMPUTE_PART_STATS', 'Compute stats for compacted partitions');
        add_command_step('COMPUTE_STATS', 'Compute backend statistics');
        add_command_step('COPY_EXTERNAL_STATS', 'Copy stats to external table');
        add_command_step('COPY_STATS_TO_BACKEND', 'Copy RDBMS stats to Backend');
        add_command_step('COUNT_AAPD', 'Create count star aggregation rule');
        add_command_step('CREATE_BASE_TABLE', 'Create new base table');
        add_command_step('CREATE_CONV_VIEW', 'Create data-conversion view');
        add_command_step('CREATE_DB', 'Create backend database');
        add_command_step('CREATE_DELTA_TABLE', 'Create delta table');
        add_command_step('CREATE_EXTERNAL_TABLE', 'Create external table');
        add_command_step('CREATE_EXTRACTION_TMP_DIR', 'Create temporary staging directory');
        add_command_step('CREATE_HDFS_SNAPSHOT', 'Create HDFS Snapshot');
        add_command_step('CREATE_HYBRID_VIEW', 'Create hybrid view');
        add_command_step('CREATE_INTERNAL_VIEW', 'Create internal user view');
        add_command_step('CREATE_JOIN_VIEW', 'Create join view');
        add_command_step('CREATE_REWRITE_RULES', 'Create query rewrite rules');
        add_command_step('CREATE_TABLE', 'Create backend table');
        add_command_step('CREATE_USER_VIEW', 'Create external user view');
        add_command_step('DELTA_COUNT', 'Check delta table row count before compaction');
        add_command_step('DELTA_COUNT1', 'Fetch delta table row count before change extraction');
        add_command_step('DELTA_COUNT2', 'Fetch delta table row count after change extraction');
        add_command_step('DELTA_STATS', 'Compute delta table stats');
        add_command_step('DEPENDENT_HYBRID_OBJECTS', 'Processing dependent hybrid objects');
        add_command_step('DEPENDENT_HYBRID_VIEWS', 'Generating dependent views in hybrid schema');
        add_command_step('DEPENDENT_SCHEMA_OBJECTS', 'Schemas with dependent stored objects');
        add_command_step('DROP_BACKEND_OBJECTS', 'Drop Backend Incremental Update objects');
        add_command_step('DROP_CHANGELOG', 'Drop extraction log for base table');
        add_command_step('DROP_EXTERNAL_TABLE', 'Remove external table');
        add_command_step('DROP_OBSOLETE_METADATA', 'Delete metadata for obsolete hybrid views');
        add_command_step('DROP_REWRITE_RULES', 'Drop query rewrite rules');
        add_command_step('DROP_SYNONYM', 'Drop existing hybrid synonym');
        add_command_step('DROP_TABLE', 'Drop backend table');
        add_command_step('DROP_USER_VIEW', 'Drop user view');
        add_command_step('ESTIMATING_STATS_FULL', 'Generating stats by FULL scan');
        add_command_step('ESTIMATING_STATS_SAMPLE', 'Estimating stats by sampling');
        add_command_step('EXTRACT_CHANGES', 'Extract changes');
        add_command_step('FETCH_SCN', 'Fetch extractor statistics');
        add_command_step('FINAL_LOAD', 'Load staged data');
        add_command_step('FINAL_MERGE', 'Merge staged data');
        add_command_step('FIND_JOIN_DATA', 'Find partitions to present');
        add_command_step('FIND_MJOIN_DATA', 'Find partitions to materialize');
        add_command_step('FIND_MODIFIED_PARTITIONS', 'Identify modified partitions');
        add_command_step('FIND_OFFLOAD_DATA', 'Find data to offload');
        add_command_step('GENERAL_AAPD', 'Create general aggregation rule');
        add_command_step('GET_BACKEND_STATS', 'Fetching backend stats');
        add_command_step('GRANT_ADMIN', 'Create grant with grant option');
        add_command_step('GRANT_HYBRID_OBJECTS', 'Grant hybrid objects');
        add_command_step('INSERT_PARTITION', 'Populate partition in new base table from user view');
        add_command_step('INSERT_TABLE', 'Populate new base table from user view');
        add_command_step('LOCK_EXTERNAL_STATS', 'Lock external table stats');
        add_command_step('MATERIALIZE_JOIN', 'Materialize staged data');
        add_command_step('MESSAGES', 'Messages');
        add_command_step('OVERWRITE_PARTITION', 'Compact partition in base table from user view');
        add_command_step('PERSIST_IU_CONFIG', 'Persist incremental update config metadata to tblproperties');
        add_command_step('PURGE_ARTIFACTS', 'Purge previous artifacts after compaction');
        add_command_step('PURGE_CHANGELOG', 'Purge changelog');
        add_command_step('REGISTER_DATA_GOV', 'Register data governance metadata');
        add_command_step('RENAME_INITIAL', 'Rename initial base table');
        add_command_step('RENAME_RESET', 'Rename base table');
        add_command_step('RESULT_CACHE_SETUP', 'Setup result cache area');
        add_command_step('SAVE_METADATA', 'Save offload metadata');
        add_command_step('SAVE_PARTITIONS', 'Save partition list');
        add_command_step('SET_EXTERNAL_COLUMN_STATS', 'Set external table column stats');
        add_command_step('SET_EXTERNAL_STATS', 'Set external table stats');
        add_command_step('SETUP_CHANGELOG', 'Setup changelog');
        add_command_step('SHIP_TO_HDFS', 'Ship incremental changes to HDFS');
        add_command_step('STAGING_CLEANUP', 'Cleanup staging area');
        add_command_step('STAGING_MINI_CLEANUP', 'Empty staging area');
        add_command_step('STAGING_SETUP', 'Setup staging area');
        add_command_step('STAGING_TRANSPORT', 'Transport data to staging');
        add_command_step('UPDATE_USER_VIEW', 'Update external user view');
        add_command_step('VALIDATE_CASTS', 'Validate type conversions');
        add_command_step('VALIDATE_DATA', 'Validate staged data');
        add_command_step('VERIFY_EXPORTED_DATA', 'Verify exported data');
    END populate_command_step;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE populate_status IS
        PROCEDURE add_status ( p_code IN status.code%TYPE,
                               p_name IN status.name%TYPE ) IS
        BEGIN
            INSERT INTO status
                (id, code, name, create_time)
            VALUES
                (status_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
        END add_status;
    BEGIN
        add_status('SUCCESS', 'Success');
        add_status('ERROR', 'Error');
        add_status('EXECUTING', 'Executing');
    END populate_status;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE populate_gdp_version IS
        PROCEDURE add_gdp_version ( p_version IN gdp_version.version%TYPE,
                                    p_build   IN gdp_version.build%TYPE ) IS
        BEGIN
            INSERT INTO gdp_version
                (id, version, build, create_time, latest)
            VALUES
                (gdp_version_seq.NEXTVAL, p_version, p_build, SYSTIMESTAMP, 'N');
        END add_gdp_version;
    BEGIN
        add_gdp_version('3.3.0', 'a35009e');
        add_gdp_version('3.3.1', 'ba4c911');
        add_gdp_version('3.3.2', '0b6102d');
        add_gdp_version('3.4.0', '2e68b34');
        add_gdp_version('4.0.0', 'a2c7c28');
        add_gdp_version('4.0.1', '7cbd8c0');
        add_gdp_version('4.0.2', '15f441f');
        add_gdp_version('4.1.0', '2490e79');
        add_gdp_version('4.2.0', '6b90d09');
        add_gdp_version('4.2.0.1', 'b6b160b');
        add_gdp_version('4.2.0.2', '88b8b50');
        add_gdp_version('4.2.0.3', '52d8e6f');
        add_gdp_version('4.2.1', '8d0ec3b');
        add_gdp_version('4.2.2', '0130c32');
        add_gdp_version('4.2.3', '5285947');
        add_gdp_version('4.2.4', '209242a');
        add_gdp_version('4.2.5', '9c10f31');
        add_gdp_version('4.3.0', '41ede51');
        add_gdp_version('4.3.1', 'e54408c');
        add_gdp_version('4.3.2', '3b72507');
        add_gdp_version('4.3.3', '20ed593');
        add_gdp_version('4.3.4', '6806086');
        add_gdp_version('4.3.5', '6325b6e');
        add_gdp_version('4.3.6', '439efa5');
    END populate_gdp_version;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE update_offload_metadata IS
    BEGIN
        -- Populate the new ID PK column...
        UPDATE offload_metadata
        SET    id = offload_metadata_seq.NEXTVAL;

        -- Populate the new current GDP version FK column...
        MERGE
            INTO offload_metadata om
            USING gdp_version     gv
            ON (om.offload_version = gv.version)
        WHEN MATCHED
        THEN
            UPDATE
            SET    om.gdp_version_id_current = gv.id;

        -- Populate the frontend object FK column for the offloaded table...
        MERGE
            INTO offload_metadata om
            USING frontend_object fo
            ON (    om.offloaded_owner = fo.database_name
                AND om.offloaded_table = fo.object_name )
        WHEN MATCHED
        THEN
            UPDATE
            SET    om.frontend_object_id = fo.id;

        -- Populate the backend object FK column for the offloaded or presented table...
        MERGE
            INTO offload_metadata om
            USING backend_object  bo
            ON (    om.hadoop_owner = bo.database_name
                AND om.hadoop_table = bo.object_name )
        WHEN MATCHED
        THEN
            UPDATE
            SET    om.backend_object_id = bo.id;
    END update_offload_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE migrate_metadata IS
    BEGIN
        populate_backend_object();
        populate_frontend_object();
        populate_command_type();
        populate_command_step();
        populate_status();
        populate_gdp_version();
        update_offload_metadata();
        COMMIT;
    END migrate_metadata;

END offload_repo_500;
/
