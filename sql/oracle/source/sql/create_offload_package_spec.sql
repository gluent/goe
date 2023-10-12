-- create_offload_package_spec.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE offload AS

    gc_version CONSTANT VARCHAR2(512) := '%s-SNAPSHOT';
    FUNCTION version RETURN VARCHAR2;

    -----------------------------------------------------------------------------------------------
    -- Constants for CLOB splitter...
    gc_split_by_amount  CONSTANT NUMBER := 0;
    gc_split_by_line    CONSTANT NUMBER := 1;

    -----------------------------------------------------------------------------------------------
    -- Assorted utils...
    FUNCTION get_part_key_val ( p_owner          IN VARCHAR2,
                                p_object_name    IN VARCHAR2,
                                p_partition_name IN VARCHAR2 )
        RETURN VARCHAR2;

    FUNCTION split_clob ( p_clob   IN CLOB,
                          p_amount IN NUMBER DEFAULT 1000,
                          p_method IN NUMBER DEFAULT offload.gc_split_by_amount )
        RETURN offload_split_clob_ntt PIPELINED;

    FUNCTION get_init_param ( p_name IN v$parameter.name%TYPE )
        RETURN VARCHAR2;

    FUNCTION get_db_block_size
        RETURN NUMBER;

    FUNCTION format_string ( p_str  IN VARCHAR2,
                             p_args IN offload_vc2_ntt DEFAULT offload_vc2_ntt(),
                             p_sub  IN VARCHAR2 DEFAULT 's' )
        RETURN VARCHAR2;

    PROCEDURE get_column_low_high_dates(p_owner       IN VARCHAR2,
                                        p_table_name  IN VARCHAR2,
                                        p_column_name IN VARCHAR2,
                                        p_low_value   OUT DATE,
                                        p_high_value  OUT DATE);

    -----------------------------------------------------------------------------------------------
    -- Offload metadata...

    FUNCTION get_offload_metadata_json ( p_object_owner  IN VARCHAR2,
                                         p_object_name   IN VARCHAR2,
                                         p_object_type   IN VARCHAR2,
                                         p_content_level IN VARCHAR2 DEFAULT offload_repo.gc_metadata_content_full )
        RETURN CLOB;

    FUNCTION get_hybrid_schema ( p_app_schema IN VARCHAR2 )
        RETURN VARCHAR2;

    -----------------------------------------------------------------------------------------------
    -- Pipe rowid ranges for an offload to consume
    FUNCTION offload_rowid_ranges(  p_owner         IN VARCHAR2,
                                    p_table_name    IN VARCHAR2,
                                    p_partitions    IN offload_vc2_ntt,
                                    p_subpartitions IN offload_vc2_ntt,
                                    p_import_degree IN NUMBER DEFAULT 1,
                                    p_import_batch  IN NUMBER DEFAULT 0)
        RETURN offload_rowid_range_ntt PIPELINED;

    -----------------------------------------------------------------------------------------------
    -- Return a collection of Hybrid object dependencies...
    gc_view_deps CONSTANT offload_vc2_ntt := offload_vc2_ntt('VIEW', 'MATERIALIZED VIEW');
    gc_all_deps  CONSTANT offload_vc2_ntt := offload_vc2_ntt('VIEW', 'MATERIALIZED VIEW', 'TABLE', 'SEQUENCE', 'TRIGGER');

    gc_view_type_hybrid CONSTANT VARCHAR2(64) := 'GLUENT_OFFLOAD_HYBRID_VIEW';
    gc_view_type_aapd   CONSTANT VARCHAR2(64) := 'GLUENT_OFFLOAD_AGGREGATE_HYBRID_VIEW';
    gc_view_type_join   CONSTANT VARCHAR2(64) := 'GLUENT_OFFLOAD_JOIN_HYBRID_VIEW';

END offload;
/
