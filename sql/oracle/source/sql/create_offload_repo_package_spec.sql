-- create_offload_repo_package_spec.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE offload_repo AS

    gc_version CONSTANT VARCHAR2(512) := '%s-SNAPSHOT';
    FUNCTION version RETURN VARCHAR2;

    gc_offload_type_full          CONSTANT VARCHAR2(30) := 'FULL';
    gc_offload_type_incremental   CONSTANT VARCHAR2(30) := 'INCREMENTAL';

    gc_metadata_source_type_view  CONSTANT VARCHAR2(6) := 'VIEW';
    gc_metadata_source_type_table CONSTANT VARCHAR2(6) := 'TABLE';

    gc_metadata_content_basic     CONSTANT VARCHAR2(6) := 'BASIC';
    gc_metadata_content_full      CONSTANT VARCHAR2(6) := 'FULL';

    gc_status_success             CONSTANT VARCHAR2(30) := 'SUCCESS';
    gc_status_error               CONSTANT VARCHAR2(30) := 'ERROR';
    gc_status_executing           CONSTANT VARCHAR2(30) := 'EXECUTING';

    FUNCTION get_offload_metadata ( p_object_owner  IN VARCHAR2,
                                    p_object_name   IN VARCHAR2,
                                    p_object_type   IN VARCHAR2,
                                    p_content_level IN VARCHAR2 DEFAULT gc_metadata_content_full )
        RETURN offload_metadata_ot;

    FUNCTION get_offload_metadata_json ( p_object_owner  IN VARCHAR2,
                                         p_object_name   IN VARCHAR2,
                                         p_object_type   IN VARCHAR2,
                                         p_content_level IN VARCHAR2 DEFAULT gc_metadata_content_full )
        RETURN CLOB;

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
        RETURN CLOB;

    PROCEDURE get_offload_metadata ( p_object_owner  IN VARCHAR2,
                                     p_object_name   IN VARCHAR2,
                                     p_object_type   IN VARCHAR2,
                                     p_content_level IN VARCHAR2 DEFAULT gc_metadata_content_full,
                                     p_metadata      OUT SYS_REFCURSOR );

    PROCEDURE save_offload_metadata ( p_hybrid_owner IN VARCHAR2,
                                      p_hybrid_view  IN VARCHAR2,
                                      p_metadata     IN CLOB );

    PROCEDURE save_offload_metadata ( p_hybrid_owner           IN VARCHAR2,
                                      p_hybrid_view            IN VARCHAR2,
                                      p_metadata               IN offload_metadata_ot,
                                      p_command_execution_uuid IN RAW DEFAULT NULL );

    PROCEDURE delete_offload_metadata ( p_hybrid_owner IN VARCHAR2,
                                        p_hybrid_view  IN VARCHAR2 );

    PROCEDURE start_command_execution ( p_uuid                 IN  RAW,
                                        p_command_type         IN  VARCHAR2,
                                        p_command_log_path     IN  VARCHAR2,
                                        p_command_input        IN  CLOB,
                                        p_command_parameters   IN  CLOB,
                                        p_command_execution_id OUT INTEGER );

    PROCEDURE end_command_execution ( p_command_execution_id IN INTEGER,
                                      p_status               IN VARCHAR2 );

    PROCEDURE start_command_execution_step ( p_command_execution_uuid    IN  RAW,
                                             p_command_type              IN  VARCHAR2,
                                             p_command_step              IN  VARCHAR2,
                                             p_command_execution_step_id OUT INTEGER );

    PROCEDURE end_command_execution_step ( p_command_execution_step_id IN INTEGER,
                                           p_step_details              IN CLOB,
                                           p_status                    IN VARCHAR2 );

    PROCEDURE start_offload_chunk ( p_command_execution_uuid IN  RAW,
                                    p_frontend_db_name       IN  VARCHAR2,
                                    p_frontend_object_name   IN  VARCHAR2,
                                    p_backend_db_name        IN  VARCHAR2,
                                    p_backend_object_name    IN  VARCHAR2,
                                    p_chunk_number           IN  INTEGER,
                                    p_partition_details      IN  offload_partition_ntt,
                                    p_offload_chunk_id       OUT INTEGER );

    PROCEDURE end_offload_chunk ( p_offload_chunk_id IN INTEGER,
                                  p_rows             IN INTEGER,
                                  p_frontend_bytes   IN INTEGER,
                                  p_transport_bytes  IN INTEGER,
                                  p_backend_bytes    IN INTEGER,
                                  p_status           IN VARCHAR2 );

END offload_repo;
/
