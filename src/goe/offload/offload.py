# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""General functions for use in Offload.

Ideally these would migrate to better locations in time.
"""

from datetime import datetime, timedelta
from optparse import SUPPRESS_HELP
from typing import TYPE_CHECKING

from goe.config import option_descriptions, orchestration_defaults
from goe.exceptions import OffloadException
from goe.filesystem.goe_dfs import VALID_OFFLOAD_FS_SCHEMES
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload import offload_constants
from goe.offload.offload_messages import OffloadMessages, VVERBOSE
from goe.offload.offload_metadata_functions import (
    decode_metadata_incremental_high_values_from_metadata,
    incremental_key_csv_from_part_keys,
)
from goe.offload.offload_source_data import offload_source_data_factory
from goe.offload.offload_source_table import (
    OFFLOAD_PARTITION_TYPE_LIST,
)
from goe.offload.offload_transport import VALID_OFFLOAD_TRANSPORT_METHODS
from goe.offload.operation.sort_columns import check_and_alter_backend_sort_columns
from goe.offload.operation.data_type_controls import DECIMAL_COL_TYPE_SYNTAX_TEMPLATE
from goe.offload.operation.ddl_file import write_ddl_to_ddl_file
from goe.offload.option_validation import (
    active_data_append_options,
    check_ipa_predicate_type_option_conflicts,
    check_opt_is_posint,
)
from goe.orchestration import command_steps
from goe.persistence.orchestration_metadata import (
    hwm_column_names_from_predicates,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.goe import OffloadOperation
    from goe.offload.backend_table import BackendTableInterface
    from goe.offload.offload_source_data import OffloadSourceDataInterface
    from goe.offload.offload_source_table import OffloadSourceTableInterface
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


def create_ddl_file_step(
    offload_target_table: "BackendTableInterface",
    offload_operation: "OffloadOperation",
    config: "OrchestrationConfig",
    messages: OffloadMessages,
):
    """Create a DDL file for the final backend table."""
    if not offload_operation.ddl_file:
        return

    def step_fn():
        ddl = []
        if (
            offload_operation.create_backend_db
            and offload_target_table.create_database_supported()
        ):
            ddl.extend(offload_target_table.create_db(with_terminator=True))
            ddl.extend(offload_target_table.create_load_db(with_terminator=True))
        ddl.extend(offload_target_table.create_backend_table(with_terminator=True))
        write_ddl_to_ddl_file(offload_operation.ddl_file, ddl, config, messages)

    messages.offload_step(command_steps.STEP_DDL_FILE, step_fn, execute=False)


def create_final_backend_table_step(
    offload_target_table: "BackendTableInterface",
    offload_operation: "OffloadOperation",
):
    """Create the final backend table."""
    if not offload_target_table.table_exists() or offload_operation.reset_backend_table:
        offload_target_table.create_backend_table_step()
    else:
        check_and_alter_backend_sort_columns(offload_target_table, offload_operation)


def drop_backend_table_step(
    owner: str,
    table_name: str,
    backend_table: "BackendTableInterface",
    messages: OffloadMessages,
    repo_client: "OrchestrationRepoClientInterface",
    execute,
    purge=False,
):
    def step_fn():
        if backend_table.table_exists():
            backend_table.drop_table(purge=purge)
        # Also remove any metadata attached to the table
        repo_client.drop_offload_metadata(owner, table_name)

    messages.offload_step(command_steps.STEP_DROP_TABLE, step_fn, execute=execute)


def get_current_offload_hv(
    offload_source_table: "OffloadSourceTableInterface",
    source_data_client: "OffloadSourceDataInterface",
    offload_operation,
    messages: OffloadMessages,
):
    """Identifies the HV for an IPA offload
    If there are partitions in flight then we can use those otherwise we will fall back to using pre-offload metadata
    Returns a list of length 2 containing:
      [RDBMS hv literals, python hv equivalents]
    """
    new_hvs = None

    if source_data_client.partitions_to_offload.count() > 0:
        if offload_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
            hvs_individual, hvs_python = [], []
            for p in source_data_client.partitions_to_offload.get_partitions():
                hvs_individual.append(p.partition_values_individual)
                hvs_python.append(p.partition_values_python)
            new_hvs = [hvs_individual, hvs_python]
        else:
            # we are offloading partitions so use the latest one from that list, this works for verification mode too
            max_partition = (
                source_data_client.partitions_to_offload.get_partition_by_index(0)
            )
            new_hvs = [
                max_partition.partition_values_individual,
                max_partition.partition_values_python,
            ]
        messages.log(
            "Identified new_hvs from partitions_to_offload: %s" % str(new_hvs),
            detail=VVERBOSE,
        )
    elif (
        offload_operation.offload_type == "INCREMENTAL"
        and not offload_operation.reset_backend_table
    ):
        # if no partitions in flight then get metadata in order to understand HWM
        current_metadata = offload_operation.get_hybrid_metadata()
        (
            _,
            _,
            partition_literal_hvs,
        ) = decode_metadata_incremental_high_values_from_metadata(
            current_metadata, offload_source_table
        )
        new_hvs = [partition_literal_hvs, partition_literal_hvs]
        messages.log(
            "Identified new_hvs from metadata: %s" % str(new_hvs), detail=VVERBOSE
        )

    return new_hvs


def get_prior_offloaded_hv(
    rdbms_table: "OffloadSourceTableInterface",
    source_data_client: "OffloadSourceDataInterface",
    offload_operation,
    messages: OffloadMessages,
):
    """Identifies the HV for a RANGE offload of the partition prior to the offload
    If there is pre-offload metadata we can use that otherwise we need to go back to the list of partitions
    """
    prior_hvs = None

    if (
        offload_operation
        and offload_operation.pre_offload_hybrid_metadata
        and offload_operation.pre_offload_hybrid_metadata.incremental_high_value
    ):
        # use metadata to get the prior high value
        messages.log("Assigning prior_hv from pre-offload metadata", detail=VVERBOSE)
        (
            _,
            real_hvs,
            literal_hvs,
        ) = decode_metadata_incremental_high_values_from_metadata(
            offload_operation.pre_offload_hybrid_metadata, rdbms_table
        )
        messages.log(
            "pre-offload metadata real values: %s" % str(real_hvs), detail=VVERBOSE
        )
        messages.log(
            "pre-offload metadata rdbms literals: %s" % str(literal_hvs),
            detail=VVERBOSE,
        )
        prior_hvs = [literal_hvs, real_hvs]

    if not prior_hvs and source_data_client:
        min_offloaded_partition = (
            source_data_client.partitions_to_offload.get_partition_by_index(-1)
        )
        if min_offloaded_partition:
            # we haven't gotten values from metadata so let's look at the partition list for the prior HV
            messages.log(
                "Searching original partitions for partition prior to: %s"
                % str(min_offloaded_partition.partition_values_python),
                detail=VVERBOSE,
            )
            prior_partition = source_data_client.source_partitions.get_prior_partition(
                partition=min_offloaded_partition
            )
            if prior_partition:
                prior_hvs = [
                    prior_partition.partition_values_individual,
                    prior_partition.partition_values_python,
                ]

    messages.log("Found prior_hvs: %s" % str(prior_hvs), detail=VVERBOSE)
    return prior_hvs


def get_offload_data_manager(
    offload_source_table: "OffloadSourceTableInterface",
    offload_target_table: "BackendTableInterface",
    offload_operation: "OffloadOperation",
    offload_options,
    messages: OffloadMessages,
    existing_metadata,
    source_client_type,
    partition_columns=None,
    include_col_offload_source_table=False,
) -> "OffloadSourceDataInterface":
    """Return a source data manager object which has methods for slicing and dicing RDBMS partitions and state
    containing which partitions to offload and data to construct hybrid view/verification predicates
    """
    if (
        offload_target_table.exists()
        and not offload_target_table.is_view()
        and not offload_operation.reset_backend_table
    ):
        # "not offload_target_table.is_view()" because we pass through here for presented joins too and do not expect previous metadata
        messages.log("Pre-offload metadata: " + str(existing_metadata), detail=VVERBOSE)

    if include_col_offload_source_table and existing_metadata:
        col_offload_source_table_override = OffloadSourceTable.create(
            existing_metadata.hybrid_owner,
            existing_metadata.hybrid_view,
            offload_options,
            messages,
            offload_by_subpartition=offload_source_table.offload_by_subpartition,
        )
    else:
        col_offload_source_table_override = None

    source_data_client = offload_source_data_factory(
        offload_source_table,
        offload_target_table,
        offload_operation,
        existing_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=partition_columns,
        col_offload_source_table_override=col_offload_source_table_override,
    )

    source_data_client.offload_data_detection()

    return source_data_client


def offload_backend_db_message(
    messages: OffloadMessages, db_type, backend_table, execute_mode
):
    """Construct messages when backend databases do not exists.
    Either throw exception in execute mode or output warnings in preview mode.
    """
    if backend_table.create_database_supported():
        message = (
            "Offload %s does not exist or is incomplete, please create it or include --create-backend-db option"
            % db_type
        )
    else:
        message = (
            "Offload %s does not exist or is incomplete, please create it before re-executing command"
            % db_type
        )
    if execute_mode:
        # Stop here to avoid subsequent exception further along the process
        raise OffloadException(message)
    else:
        messages.warning(message, ansi_code="red")


def offload_type_force_effects(
    hybrid_operation: "OffloadOperation",
    source_data_client: "OffloadSourceDataInterface",
    original_metadata,
    offload_source_table: "OffloadSourceTableInterface",
    messages: OffloadMessages,
):
    if source_data_client.is_incremental_append_capable():
        original_offload_type = original_metadata.offload_type
        new_offload_type = hybrid_operation.offload_type
        if hybrid_operation.offload_by_subpartition and (
            original_offload_type,
            new_offload_type,
        ) == ("FULL", "INCREMENTAL"):
            # Once we have switched to FULL we cannot trust the HWM with subpartition offloads,
            # what if another HWM appeared for already offloaded HWM!
            raise OffloadException(
                offload_constants.OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT
            )

        if hybrid_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST and (
            original_offload_type,
            new_offload_type,
        ) == ("FULL", "INCREMENTAL"):
            # We're switching OFFLOAD_TYPE for a LIST table, this is tricky for the user because we don't know the correct
            # INCREMENTAL_HIGH_VALUE value for metadata/hybrid view. So best we can do is try to alert them.
            active_lpa_opts = active_data_append_options(
                hybrid_operation, partition_type=OFFLOAD_PARTITION_TYPE_LIST
            )
            if active_lpa_opts:
                messages.notice(
                    "%s, only the partitions identified by %s will be queried from offloaded data"
                    % (
                        offload_constants.OFFLOAD_TYPE_CHANGE_FOR_LIST_MESSAGE_TEXT,
                        active_lpa_opts[0],
                    )
                )
            else:
                raise OffloadException(
                    offload_constants.OFFLOAD_TYPE_CHANGE_FOR_LIST_EXCEPTION_TEXT
                )

        if not hybrid_operation.force:
            new_inc_key = None
            original_inc_key = original_metadata.incremental_key
            if (
                hybrid_operation.hwm_in_hybrid_view
                and source_data_client.get_incremental_high_values()
            ):
                new_inc_key = incremental_key_csv_from_part_keys(
                    offload_source_table.partition_columns
                )

            original_pred_cols, new_pred_cols = None, None
            if (
                hybrid_operation.ipa_predicate_type
                in INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV
            ):
                original_pred_cols = original_metadata.hwm_column_names()
                new_pred_cols = hwm_column_names_from_predicates(
                    source_data_client.get_post_offload_predicates()
                )

            if original_offload_type != new_offload_type:
                messages.notice(
                    'Enabling force option when switching OFFLOAD_TYPE ("%s" -> "%s")'
                    % (original_offload_type, new_offload_type)
                )
                hybrid_operation.force = True
            elif original_inc_key != new_inc_key:
                messages.notice(
                    'Enabling force option when switching INCREMENTAL_KEY ("%s" -> "%s")'
                    % (original_inc_key, new_inc_key)
                )
                hybrid_operation.force = True
            elif (
                original_pred_cols != new_pred_cols
                and not source_data_client.nothing_to_offload()
            ):
                messages.notice(
                    'Enabling force option when columns in INCREMENTAL_PREDICATE_VALUE change ("%s" -> "%s")'
                    % (original_pred_cols, new_pred_cols)
                )
                hybrid_operation.force = True


def normalise_less_than_options(options, exc_cls=OffloadException):
    if not hasattr(options, "older_than_date"):
        # We mustn't be in offload so should just drop out
        return

    active_pa_opts = active_data_append_options(options, from_options=True)
    if len(active_pa_opts) > 1:
        raise exc_cls(
            "%s: %s"
            % (
                offload_constants.CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT,
                ", ".join(active_pa_opts),
            )
        )

    if options.reset_hybrid_view and len(active_pa_opts) == 0:
        raise exc_cls(offload_constants.RESET_HYBRID_VIEW_EXCEPTION_TEXT)

    if options.older_than_date:
        try:
            # move the value into less_than_value
            options.less_than_value = parse_yyyy_mm_dd(options.older_than_date)
            options.older_than_date = None
        except ValueError as exc:
            raise exc_cls("option --older-than-date: %s" % str(exc))
    elif options.older_than_days:
        options.older_than_days = check_opt_is_posint(
            "--older-than-days", options.older_than_days, allow_zero=True
        )
        # move the value into less_than_value
        options.less_than_value = datetime.today() - timedelta(options.older_than_days)
        options.older_than_days = None

    check_ipa_predicate_type_option_conflicts(options, exc_cls=exc_cls)


def parse_yyyy_mm_dd(ds):
    return datetime.strptime(ds, "%Y-%m-%d")


def list_for_option_help(opt_list):
    return "|".join(opt_list) if opt_list else None


def get_offload_options(opt):
    """Options applicable to offload only"""

    opt.add_option(
        "--allow-decimal-scale-rounding",
        dest="allow_decimal_scale_rounding",
        default=orchestration_defaults.allow_decimal_scale_rounding_default(),
        action="store_true",
        help="Confirm it is acceptable for offload to round decimal places when loading data into a backend system",
    )
    opt.add_option(
        "--allow-floating-point-conversions",
        dest="allow_floating_point_conversions",
        default=orchestration_defaults.allow_floating_point_conversions_default(),
        action="store_true",
        help="Confirm it is acceptable for offload to convert NaN/Inf values to NULL when loading data into a backend system",
    )
    opt.add_option(
        "--allow-nanosecond-timestamp-columns",
        dest="allow_nanosecond_timestamp_columns",
        default=orchestration_defaults.allow_nanosecond_timestamp_columns_default(),
        action="store_true",
        help="Confirm it is safe to offload timestamp column with nanosecond capability",
    )
    opt.add_option(
        "--compress-load-table",
        dest="compress_load_table",
        action="store_true",
        default=orchestration_defaults.compress_load_table_default(),
        help="Compress the contents of the load table during offload",
    )
    opt.add_option(
        "--compute-load-table-stats",
        dest="compute_load_table_stats",
        action="store_true",
        default=orchestration_defaults.compute_load_table_stats_default(),
        help="Compute statistics on the load table during each offload chunk",
    )
    opt.add_option(
        "--data-sample-percent",
        dest="data_sample_pct",
        default=orchestration_defaults.data_sample_pct_default(),
        help="Sample RDBMS data for columns with no precision/scale properties. 0 = no sampling",
    )
    opt.add_option(
        "--data-sample-parallelism",
        type=int,
        dest="data_sample_parallelism",
        default=orchestration_defaults.data_sample_parallelism_default(),
        help=option_descriptions.DATA_SAMPLE_PARALLELISM,
    )
    opt.add_option(
        "--ddl-file",
        dest="ddl_file",
        help=(
            "Output generated target table DDL to a file, should include full path or literal AUTO. "
            "Supports local paths or cloud storage URIs"
        ),
    )
    opt.add_option(
        "--not-null-columns",
        dest="not_null_columns_csv",
        help="CSV list of columns to offload with a NOT NULL constraint",
    )
    opt.add_option(
        "--offload-predicate-type",
        dest="ipa_predicate_type",
        help=(
            "Override the default INCREMENTAL_PREDICATE_TYPE for a partitioned table. "
            f"Used to offload LIST partitioned tables using RANGE logic with --offload-predicate-type={INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE} or "
            "used for specialized cases of Incremental Partition Append and Predicate-Based Offload offloading"
        ),
    )
    opt.add_option(
        "--offload-fs-scheme",
        dest="offload_fs_scheme",
        default=orchestration_defaults.offload_fs_scheme_default(),
        help="%s. Filesystem type for Offloaded tables"
        % list_for_option_help(VALID_OFFLOAD_FS_SCHEMES),
    )
    opt.add_option(
        "--offload-fs-prefix",
        dest="offload_fs_prefix",
        help='The path with which to prefix offloaded table paths. Takes precedence over --hdfs-data when --offload-fs-scheme != "inherit"',
    )
    opt.add_option(
        "--offload-fs-container",
        dest="offload_fs_container",
        help="A valid bucket name when offloading to cloud storage",
    )
    opt.add_option(
        "--offload-type",
        dest="offload_type",
        help=(
            "Identifies a range partitioned offload as FULL or INCREMENTAL. FULL dictates that all data is offloaded. "
            "INCREMENTAL dictates that data up to an incremental threshold will be offloaded"
        ),
    )

    opt.add_option(
        "--integer-1-columns",
        dest="integer_1_columns_csv",
        help="CSV list of columns to offload as a 1-byte integer (only effective for numeric columns)",
    )
    opt.add_option(
        "--integer-2-columns",
        dest="integer_2_columns_csv",
        help="CSV list of columns to offload as a 2-byte integer (only effective for numeric columns)",
    )
    opt.add_option(
        "--integer-4-columns",
        dest="integer_4_columns_csv",
        help="CSV list of columns to offload as a 4-byte integer (only effective for numeric columns)",
    )
    opt.add_option(
        "--integer-8-columns",
        dest="integer_8_columns_csv",
        help="CSV list of columns to offload as a 8-byte integer (only effective for numeric columns)",
    )
    opt.add_option(
        "--integer-38-columns",
        dest="integer_38_columns_csv",
        help="CSV list of columns to offload as a 38 digit integer (only effective for numeric columns)",
    )

    opt.add_option(
        "--decimal-columns",
        dest="decimal_columns_csv_list",
        action="append",
        help=(
            'CSV list of columns to offload as DECIMAL(p,s) where "p,s" is specified in a paired --decimal-columns-type option. '
            "--decimal-columns and --decimal-columns-type allow repeat inclusion for flexible data type specification, "
            'for example "--decimal-columns-type=18,2 --decimal-columns=price,cost --decimal-columns-type=6,4 --decimal-columns=location" '
            "(only effective for numeric columns)"
        ),
    )
    opt.add_option(
        "--decimal-columns-type",
        dest="decimal_columns_type_list",
        action="append",
        help="State the precision and scale of columns listed in a paired --decimal-columns option, "
        + DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(p="38", s="38")
        + '. e.g. "--decimal-columns-type=18,2"',
    )

    opt.add_option(
        "--date-columns",
        dest="date_columns_csv",
        help="CSV list of columns to offload as a date (no time element, only effective for date based columns)",
    )

    opt.add_option(
        "--unicode-string-columns",
        dest="unicode_string_columns_csv",
        help="CSV list of columns to offload as Unicode string (only effective for string columns)",
    )
    opt.add_option(
        "--double-columns",
        dest="double_columns_csv",
        help="CSV list of columns to offload as a double-precision floating point number (only effective for numeric columns)",
    )
    opt.add_option(
        "--variable-string-columns",
        dest="variable_string_columns_csv",
        help="CSV list of columns to offload as a variable string type (only effective for date based columns)",
    )
    opt.add_option(
        "--timestamp-tz-columns",
        dest="timestamp_tz_columns_csv",
        help="CSV list of columns to offload as a time zoned column (only effective for date based columns)",
    )

    opt.add_option(
        "--offload-transport-consistent-read",
        dest="offload_transport_consistent_read",
        default=orchestration_defaults.offload_transport_consistent_read_default(),
        help="Parallel data transport tasks should have a consistent point in time when reading RDBMS data",
    )
    opt.add_option(
        "--offload-transport-dsn",
        dest="offload_transport_dsn",
        default=orchestration_defaults.offload_transport_dsn_default(),
        help="DSN override for RDBMS connection during data transport.",
    )
    opt.add_option(
        "--offload-transport-fetch-size",
        dest="offload_transport_fetch_size",
        default=orchestration_defaults.offload_transport_fetch_size_default(),
        help="Number of records to fetch in a single batch from the RDBMS during Offload",
    )
    opt.add_option(
        "--offload-transport-jvm-overrides",
        dest="offload_transport_jvm_overrides",
        help='JVM overrides (inserted right after "sqoop import" or "spark-submit")',
    )
    opt.add_option(
        "--offload-transport-method",
        dest="offload_transport_method",
        choices=VALID_OFFLOAD_TRANSPORT_METHODS,
        help=SUPPRESS_HELP,
    )
    opt.add_option(
        "--offload-transport-parallelism",
        dest="offload_transport_parallelism",
        default=orchestration_defaults.offload_transport_parallelism_default(),
        help="Number of slaves to use when transporting data during an Offload.",
    )
    opt.add_option(
        "--offload-transport-queue-name",
        dest="offload_transport_queue_name",
        help="Yarn queue name to be used for Offload transport jobs.",
    )
    opt.add_option(
        "--offload-transport-small-table-threshold",
        dest="offload_transport_small_table_threshold",
        default=orchestration_defaults.offload_transport_small_table_threshold_default(),
        help=(
            "Threshold above which Query Import is no longer considered the correct offload choice "
            "for non-partitioned tables. [\\d.]+[MG] eg. 100M, 0.5G, 1G"
        ),
    )
    opt.add_option(
        "--offload-transport-spark-properties",
        dest="offload_transport_spark_properties",
        default=orchestration_defaults.offload_transport_spark_properties_default(),
        help="Override defaults for Spark configuration properties using key/value pairs in JSON format",
    )
    opt.add_option(
        "--offload-transport-validation-polling-interval",
        dest="offload_transport_validation_polling_interval",
        default=orchestration_defaults.offload_transport_validation_polling_interval_default(),
        help=(
            "Polling interval in seconds for validation of Spark transport row count. "
            "-1 disables retrieval of RDBMS SQL statistics. "
            "0 disables polling resulting in a single capture of SQL statistics. "
            "A value greater than 0 polls transport SQL statistics using the specified interval"
        ),
    )

    opt.add_option(
        "--sqoop-additional-options",
        dest="sqoop_additional_options",
        default=orchestration_defaults.sqoop_additional_options_default(),
        help="Sqoop additional options (added to the end of the command line)",
    )
    opt.add_option(
        "--sqoop-mapreduce-map-memory-mb",
        dest="sqoop_mapreduce_map_memory_mb",
        help="Sqoop specific setting for mapreduce.map.memory.mb",
    )
    opt.add_option(
        "--sqoop-mapreduce-map-java-opts",
        dest="sqoop_mapreduce_map_java_opts",
        help="Sqoop specific setting for mapreduce.map.java.opts",
    )

    opt.add_option("--no-verify", dest="verify_row_count", action="store_false")
    opt.add_option(
        "--verify",
        dest="verify_row_count",
        choices=["minus", "aggregate"],
        default=orchestration_defaults.verify_row_count_default(),
    )
    opt.add_option(
        "--verify-parallelism",
        dest="verify_parallelism",
        type=int,
        default=orchestration_defaults.verify_parallelism_default(),
        help=option_descriptions.VERIFY_PARALLELISM,
    )
