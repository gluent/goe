-- create_offload_repo_views.sql
--
-- LICENSE_TEXT
--

CREATE OR REPLACE VIEW offload_metadata_v
AS
    SELECT hybrid_owner
    ,      hybrid_view
    ,      hybrid_view_type
    ,      hybrid_external_table
    ,      hadoop_owner
    ,      hadoop_table
    ,      offload_type
    ,      offloaded_owner
    ,      offloaded_table
    ,      incremental_key
    ,      incremental_high_value
    ,      incremental_range
    ,      incremental_predicate_type
    ,      incremental_predicate_value
    ,      offload_bucket_column
    ,      offload_bucket_method
    ,      offload_bucket_count
    ,      offload_version
    ,      offload_sort_columns
    ,      transformations
    ,      object_hash
    ,      offload_scn
    ,      offload_partition_functions
    ,      iu_key_columns
    ,      iu_extraction_method
    ,      iu_extraction_scn
    ,      iu_extraction_time
    ,      changelog_table
    ,      changelog_trigger
    ,      changelog_sequence
    ,      updatable_view
    ,      updatable_trigger
    ,      offload_repo.get_offload_metadata_json(
                hybrid_owner,
                hybrid_view,
                hybrid_view_type,
                hybrid_external_table,
                hadoop_owner,
                hadoop_table,
                offload_type,
                offloaded_owner,
                offloaded_table,
                incremental_key,
                incremental_high_value,
                incremental_range,
                incremental_predicate_type,
                incremental_predicate_value,
                offload_bucket_column,
                offload_bucket_method,
                offload_bucket_count,
                offload_version,
                offload_sort_columns,
                transformations,
                object_hash,
                offload_scn,
                offload_partition_functions,
                iu_key_columns,
                iu_extraction_method,
                iu_extraction_scn,
                iu_extraction_time,
                changelog_table,
                changelog_trigger,
                changelog_sequence,
                updatable_view,
                updatable_trigger) AS offload_metadata_json
    FROM   offload_metadata
;
