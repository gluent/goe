-- create_offload_repo_package_spec.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE offload_repo AS

    gc_version CONSTANT VARCHAR2(512) := '%s-SNAPSHOT';
    FUNCTION version RETURN VARCHAR2;

    gc_status_success   CONSTANT VARCHAR2(30) := 'SUCCESS';
    gc_status_error     CONSTANT VARCHAR2(30) := 'ERROR';
    gc_status_executing CONSTANT VARCHAR2(30) := 'EXECUTING';

    FUNCTION get_latest_goe_version RETURN VARCHAR2;

    FUNCTION get_offload_metadata ( p_frontend_object_owner IN VARCHAR2,
                                    p_frontend_object_name  IN VARCHAR2 )
        RETURN offload_metadata_ot;

    PROCEDURE save_offload_metadata ( p_frontend_object_owner IN VARCHAR2,
                                      p_frontend_object_name  IN VARCHAR2,
                                      p_metadata              IN offload_metadata_ot );

    PROCEDURE delete_offload_metadata ( p_frontend_object_owner IN VARCHAR2,
                                        p_frontend_object_name  IN VARCHAR2 );

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
                                    p_frontend_object_owner  IN  VARCHAR2,
                                    p_frontend_object_name   IN  VARCHAR2,
                                    p_backend_object_owner   IN  VARCHAR2,
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
