
define ORA_REPO_USER = &1
set verify off

begin
    -- Sales...
    &ORA_REPO_USER..offload_repo.save_offload_metadata(
        p_hybrid_owner => 'SH_H',
        p_hybrid_view  => 'SALES',
        p_metadata     => &ORA_REPO_USER..offload_metadata_ot(
                                hybrid_owner                => 'SH_H',
                                hybrid_view                 => 'SALES',
                                hybrid_view_type            => 'GLUENT_OFFLOAD_HYBRID_VIEW',
                                hybrid_external_table       => 'SALES_EXT',
                                hadoop_owner                => 'sh',
                                hadoop_table                => 'sales',
                                offload_type                => 'INCREMENTAL',
                                offloaded_owner             => 'SH',
                                offloaded_table             => 'SALES',
                                incremental_key             => 'TIME_ID',
                                incremental_high_value      => 'TO_DATE('' 2013-01-01 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')',
                                incremental_range           => 'PARTITION',
                                incremental_predicate_type  => 'RANGE',
                                incremental_predicate_value => null,
                                offload_bucket_column       => null,
                                offload_bucket_method       => null,
                                offload_bucket_count        => null,
                                offload_version             => '4.2.0',
                                offload_sort_columns        => null,
                                transformations             => null,
                                object_hash                 => null,
                                offload_scn                 => null));

    &ORA_REPO_USER..offload_repo.save_offload_metadata(
        p_hybrid_owner => 'SH_H',
        p_hybrid_view  => 'SALES_AGG',
        p_metadata     => &ORA_REPO_USER..offload_metadata_ot(
                                hybrid_owner                => 'SH_H',
                                hybrid_view                 => 'SALES_AGG',
                                hybrid_view_type            => 'GLUENT_OFFLOAD_AGGREGATE_HYBRID_VIEW',
                                hybrid_external_table       => 'SALES_AGG_EXT',
                                hadoop_owner                => 'sh',
                                hadoop_table                => 'sales',
                                offload_type                => 'INCREMENTAL',
                                offloaded_owner             => 'SH',
                                offloaded_table             => 'SALES',
                                incremental_key             => 'TIME_ID',
                                incremental_high_value      => 'TO_DATE('' 2013-01-01 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')',
                                incremental_range           => 'PARTITION',
                                incremental_predicate_type  => 'RANGE',
                                incremental_predicate_value => null,
                                offload_bucket_column       => null,
                                offload_bucket_method       => null,
                                offload_bucket_count        => null,
                                offload_version             => '4.2.0',
                                offload_sort_columns        => null,
                                transformations             => null,
                                object_hash                 => null,
                                offload_scn                 => null));

    &ORA_REPO_USER..offload_repo.save_offload_metadata(
        p_hybrid_owner => 'SH_H',
        p_hybrid_view  => 'SALES_CNT_AGG',
        p_metadata     => &ORA_REPO_USER..offload_metadata_ot(
                                hybrid_owner                => 'SH_H',
                                hybrid_view                 => 'SALES_CNT_AGG',
                                hybrid_view_type            => 'GLUENT_OFFLOAD_AGGREGATE_HYBRID_VIEW',
                                hybrid_external_table       => 'SALES_CNT_AGG_EXT',
                                hadoop_owner                => 'sh',
                                hadoop_table                => 'sales',
                                offload_type                => 'INCREMENTAL',
                                offloaded_owner             => 'SH',
                                offloaded_table             => 'SALES',
                                incremental_key             => 'TIME_ID',
                                incremental_high_value      => 'TO_DATE('' 2013-01-01 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')',
                                incremental_range           => 'PARTITION',
                                incremental_predicate_type  => 'RANGE',
                                incremental_predicate_value => null,
                                offload_bucket_column       => null,
                                offload_bucket_method       => null,
                                offload_bucket_count        => null,
                                offload_version             => '4.2.0',
                                offload_sort_columns        => null,
                                transformations             => null,
                                object_hash                 => null,
                                offload_scn                 => null));

    -- Costs...
    &ORA_REPO_USER..offload_repo.save_offload_metadata(
        p_hybrid_owner => 'SH_H',
        p_hybrid_view  => 'COSTS',
        p_metadata     => &ORA_REPO_USER..offload_metadata_ot(
                                hybrid_owner                => 'SH_H',
                                hybrid_view                 => 'COSTS',
                                hybrid_view_type            => 'GLUENT_OFFLOAD_HYBRID_VIEW',
                                hybrid_external_table       => 'COSTS_EXT',
                                hadoop_owner                => 'sh',
                                hadoop_table                => 'costs',
                                offload_type                => 'INCREMENTAL',
                                offloaded_owner             => 'SH',
                                offloaded_table             => 'COSTS',
                                incremental_key             => 'TIME_ID',
                                incremental_high_value      => 'TO_DATE('' 2013-01-01 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')',
                                incremental_range           => 'PARTITION',
                                incremental_predicate_type  => 'RANGE',
                                incremental_predicate_value => null,
                                offload_bucket_column       => null,
                                offload_bucket_method       => null,
                                offload_bucket_count        => null,
                                offload_version             => '4.2.0',
                                offload_sort_columns        => null,
                                transformations             => null,
                                object_hash                 => null,
                                offload_scn                 => null));

    &ORA_REPO_USER..offload_repo.save_offload_metadata(
        p_hybrid_owner => 'SH_H',
        p_hybrid_view  => 'COSTS_AGG',
        p_metadata     => &ORA_REPO_USER..offload_metadata_ot(
                                hybrid_owner                => 'SH_H',
                                hybrid_view                 => 'COSTS_AGG',
                                hybrid_view_type            => 'GLUENT_OFFLOAD_AGGREGATE_HYBRID_VIEW',
                                hybrid_external_table       => 'COSTS_AGG_EXT',
                                hadoop_owner                => 'sh',
                                hadoop_table                => 'costs',
                                offload_type                => 'INCREMENTAL',
                                offloaded_owner             => 'SH',
                                offloaded_table             => 'COSTS',
                                incremental_key             => 'TIME_ID',
                                incremental_high_value      => 'TO_DATE('' 2013-01-01 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')',
                                incremental_range           => 'PARTITION',
                                incremental_predicate_type  => 'RANGE',
                                incremental_predicate_value => null,
                                offload_bucket_column       => null,
                                offload_bucket_method       => null,
                                offload_bucket_count        => null,
                                offload_version             => '4.2.0',
                                offload_sort_columns        => null,
                                transformations             => null,
                                object_hash                 => null,
                                offload_scn                 => null));

    &ORA_REPO_USER..offload_repo.save_offload_metadata(
        p_hybrid_owner => 'SH_H',
        p_hybrid_view  => 'COSTS_CNT_AGG',
        p_metadata     => &ORA_REPO_USER..offload_metadata_ot(
                                hybrid_owner                => 'SH_H',
                                hybrid_view                 => 'COSTS_CNT_AGG',
                                hybrid_view_type            => 'GLUENT_OFFLOAD_AGGREGATE_HYBRID_VIEW',
                                hybrid_external_table       => 'COSTS_CNT_AGG_EXT',
                                hadoop_owner                => 'sh',
                                hadoop_table                => 'costs',
                                offload_type                => 'INCREMENTAL',
                                offloaded_owner             => 'SH',
                                offloaded_table             => 'COSTS',
                                incremental_key             => 'TIME_ID',
                                incremental_high_value      => 'TO_DATE('' 2013-01-01 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')',
                                incremental_range           => 'PARTITION',
                                incremental_predicate_type  => 'RANGE',
                                incremental_predicate_value => null,
                                offload_bucket_column       => null,
                                offload_bucket_method       => null,
                                offload_bucket_count        => null,
                                offload_version             => '4.2.0',
                                offload_sort_columns        => null,
                                transformations             => null,
                                object_hash                 => null,
                                offload_scn                 => null));

end;
/

