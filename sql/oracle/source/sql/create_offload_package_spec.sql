-- create_offload_package_spec.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE offload AS

    gc_version CONSTANT VARCHAR2(512) := '%s-SNAPSHOT';
    FUNCTION version RETURN VARCHAR2;

    FUNCTION get_init_param ( p_name IN v$parameter.name%TYPE )
        RETURN VARCHAR2;

    FUNCTION get_db_block_size
        RETURN NUMBER;

    PROCEDURE get_column_low_high_dates(p_owner       IN VARCHAR2,
                                        p_table_name  IN VARCHAR2,
                                        p_column_name IN VARCHAR2,
                                        p_low_value   OUT DATE,
                                        p_high_value  OUT DATE);

    FUNCTION offload_rowid_ranges(  p_owner         IN VARCHAR2,
                                    p_table_name    IN VARCHAR2,
                                    p_partitions    IN offload_vc2_ntt,
                                    p_subpartitions IN offload_vc2_ntt,
                                    p_import_degree IN NUMBER DEFAULT 1,
                                    p_import_batch  IN NUMBER DEFAULT 0)
        RETURN offload_rowid_range_ntt PIPELINED;

END offload;
/
