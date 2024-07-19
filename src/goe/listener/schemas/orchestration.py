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

# Orchestration related schemas

# Standard Library
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union
from uuid import UUID

# Third Party Libraries
from pydantic import UUID4, Field, Json, PositiveInt, validator

# GOE
from goe.config import option_descriptions
import goe.config.orchestration_defaults as defaults
from goe.listener.schemas.base import BaseSchema, TotaledResults
from goe.orchestration.execution_id import ExecutionId


def try_cast_int(val, default_val=None):
    try:
        if val:
            return int(val)
        return default_val
    except TypeError:
        return default_val


class CommandExecutionLog(BaseSchema):
    """Command Execution Log Schema"""

    logged_at: Optional[datetime]
    contains_error: Optional[bool]
    log_type: str = Field(default="offload")
    name: str = Field(default="")
    is_file: Optional[bool]
    log_contents: str = Field(alias="message")


class CommandScheduled(BaseSchema):
    """OperationResponse

    Attributes:
        execution_id (uuid4): The execution ID that can be used to look up the job.

    Raises:
        pydantic.error_wrappers.ValidationError: If any provided attribute
            doesn't pass type validation.
    """

    execution_id: UUID4


class CommandExecution(BaseSchema):
    """OperationResponse

    Attributes:
        execution_id (uuid4): The execution ID that can be used to look up the job.

    Raises:
        pydantic.error_wrappers.ValidationError: If any provided attribute
            doesn't pass type validation.
    """

    execution_id: UUID4
    command_type_code: str
    command_type: str
    status_code: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    command_log_path: str
    command_input: str
    command_parameters: Json
    goe_version: str
    goe_build: str
    steps: Optional[List["CommandExecutionStep"]] = []

    @validator("execution_id")
    def coerce_uuid(cls, v: Union[UUID, str, bytes, ExecutionId]):
        if isinstance(v, UUID):
            return v
        elif isinstance(v, str):
            return UUID(v)
        elif isinstance(v, bytes):
            return UUID(bytes=v)
        elif isinstance(v, ExecutionId):
            return UUID(bytes=v.id)
        raise ValueError(v)


class CommandExecutionStep(BaseSchema):
    """OperationResponse

    Attributes:
        execution_id (uuid4): The execution ID that can be used to look up the job.

    Raises:
        pydantic.error_wrappers.ValidationError: If any provided attribute
            doesn't pass type validation.
    """

    step_id: int
    step_code: str
    step_title: str
    step_status_code: str
    step_status: str
    started_at: datetime
    completed_at: Optional[datetime]
    step_details: Optional[str]


class CommandExecutions(TotaledResults[CommandExecution]):
    """Stores and Returns a collection of command executions"""


# important
# Since we defined the step after the command above, we need to update the ref
# the code is easier to read if you lay out it this way.
CommandExecution.update_forward_refs()


# todo: update default values
# todo: update documentation details
# todo: update sample objects
class OffloadOptions(BaseSchema):
    """
    Mixin class to add offload options to a pydantic model
    """

    owner_table: str = Field(
        ...,
        title="Table to Offload (SCHEMA.TABLE)",
        description=(
            "This this is the table to offload.  "
            "This parameter expects a fully qualified table object "
            "in the form of [SCHEMA.TABLE]"
        ),
        cli=("-t", "--table"),
    )

    @validator("owner_table")
    def validate_table_should_have_schema_prefix(cls, value: str) -> str:
        """
        Validates that a table should be fully qualified'

        Validation fails if the table name is not in the <schema>.<table> format
        """
        if len(value.split(".")) == 2:
            return value
        raise ValueError(value)

    allow_decimal_scale_rounding: Optional[bool] = Field(
        default=False,
        title="Allow decimal scale rounding",
        description="Allow decimal scale rounding for numeric columns",
        cli=("--allow-decimal-scale-rounding"),
    )
    allow_floating_point_conversions: Optional[bool] = Field(
        default=False,
        title="Allow floating point conversions",
        description="Allow floating point conversions for numeric columns",
        cli=("--allow-floating-point-conversions"),
    )
    allow_nanosecond_timestamp_columns: Optional[bool] = Field(
        default=False,
        title="Allow nanosecond timestamp columns",
        description="Allow nanosecond timestamp columns",
        cli=("--allow-nanosecond-timestamp-columns"),
    )
    bucket_hash_col: Optional[str] = Field(
        default=None,
        title="Bucket hash column",
        description="Column to use when calculating offload bucket, defaults to first column.",
        cli=("--bucket-hash-column"),
    )
    compress_load_table: Optional[bool] = Field(
        default=False,
        title="Compress load table",
        description="Compress load table",
        cli=("--compress-load-table"),
    )
    compute_load_table_stats: Optional[bool] = Field(
        default=False,
        title="Compute load table stats",
        description="Compute load table stats",
        cli=("--compute-load-table-stats"),
    )
    create_backend_db: Optional[bool] = Field(
        default=False,
        title="Create backend database",
        description="Create backend database",
        cli=("--create-backend-db"),
    )
    data_sample_parallelism: Optional[int] = Field(
        default=0,
        title="Data sample parallelism",
        # description=f"{constants.DATA_SAMPLE_PARALLELISM}",
        cli=("--data-sample-parallelism"),
    )
    data_sample_pct: Optional[Union[int, str]] = Field(
        default="AUTO",
        title="Data sample size percent",
        description="Data sample size percent can be AUTO, or a value between 0 and 100",
        cli=("--data-sample-size-percent"),
    )

    @validator("data_sample_pct", always=True)
    def data_sample_pct_validator(cls, v):
        if v == "AUTO":
            return v
        elif isinstance(v, int) and (100 >= v >= 0):
            return int(v)
        else:
            raise ValueError(f"data_sample_pct must be 0-100 or AUTO: ({type(v)}) {v}")

    date_columns_csv: Optional[str] = Field(
        default=None,
        title="Date columns in CSV",
        description="CSV list of columns to treat as date columns",
        cli=("--date-columns"),
    )
    ddl_file: Optional[str] = Field(
        default=None,
        title="Path to output generated target table DDL",
        description=(
            "Output generated target table DDL to a file, should include full path or literal AUTO. "
            "Supports local paths or cloud storage URIs"
        ),
        cli=("--ddl-file"),
    )
    decimal_columns_csv_list: Optional[List[str]] = Field(
        default=None,
        title="Decimal columns",
        description="CSV list of columns to treat as decimal columns",
        cli=("--decimal-columns"),
    )
    decimal_columns_type_list: Optional[List[str]] = Field(
        default=None,
        title="Decimal columns type",
        description=(
            "State the precision and scale of columns listed in a paired "
            "--decimal-columns option. e.g. --decimal-columns-type=18,2"
        ),
        cli=("--decimal-columns-type"),
    )
    decimal_padding_digits: Optional[int] = Field(
        default=2,
        title="Decimal padding digits",
        description="Decimal padding digits",
        cli=("--decimal-padding-digits"),
    )

    double_columns_csv: Optional[str] = Field(
        default=None,
        title="Double columns in CSV",
        description=(
            "CSV list of columns to offload as a double-precision "
            "floating point number (only effective for numeric columns"
        ),
        cli=("--double-columns"),
    )
    equal_to_values: Optional[List[str]] = Field(
        default=None,
        title="Equal-to values",
        description=(
            "Offload partitions with a matching partition key list. "
            "This option can be included multiple times to match multiple partitions."
        ),
        cli=("--equal-to-values"),
    )
    force: Optional[bool] = Field(
        default=False,
        title="Force offload",
        description="Replace tables/views as required. Use with caution.",
        cli=("-f", "--force"),
    )
    hive_column_stats: Optional[bool] = Field(
        default=False,
        title="Hive column statistics",
        description=(
            'Enable computation of column stats with "NATIVE" or "HISTORY" '
            "offload stats methods. Applies to Hive only"
        ),
        cli=("--hive-column-stats"),
    )
    integer_1_columns_csv: Optional[str] = Field(
        default=None,
        title="Integer 1 columns",
        description="CSV list of columns to treat as 1-byte integer (only effective for numeric columns)",
        cli=("--integer-1-columns"),
    )
    integer_2_columns_csv: Optional[str] = Field(
        default=None,
        title="Integer 2 columns",
        description="CSV list of columns to treat as 2-byte integer (only effective for numeric columns)",
        cli=("--integer-2-columns"),
    )
    integer_4_columns_csv: Optional[str] = Field(
        default=None,
        title="Integer 4 columns",
        description="CSV list of columns to treat as 4-byte integer (only effective for numeric columns)",
        cli=("--integer-4-columns"),
    )
    integer_8_columns_csv: Optional[str] = Field(
        default=None,
        title="Integer 8 columns",
        description="CSV list of columns to treat as 8-byte integer (only effective for numeric columns)",
        cli=("--integer-8-columns"),
    )
    integer_38_columns_csv: Optional[str] = Field(
        default=None,
        title="Integer 38 columns",
        description="CSV list of columns to offload as a 38 digit integer [only effective for numeric columns]",
        cli=("--integer-38-columns"),
    )
    less_than_value: Optional[str] = Field(
        default=None,
        title="Less-than value",
        description=("Offload partitions with high water mark less than this value. "),
        cli=("--less-than-value"),
    )
    max_offload_chunk_count: Optional[PositiveInt] = Field(
        default=try_cast_int(defaults.max_offload_chunk_count_default(), 100),
        title="Max offload chunk count",
        description="Restrict number of partitions offloaded per cycle. Allowable values between 1 and 1000.",
        cli=("--max-offload-chunk-count"),
    )

    @validator("max_offload_chunk_count")
    def validate_max_offload_chunk_count(cls, v):
        if v < 1 or v > 1000:
            raise ValueError("Max offload chunk count must be between 1 and 1000")
        return v

    max_offload_chunk_size: Optional[str] = Field(
        default=defaults.max_offload_chunk_size_default(),
        title="Max offload chunk size",
        description="Restrict size of partitions offloaded per cycle. eg. 100M, 1G, 1.5G",
        cli=("--max-offload-chunk-size"),
        regex=r"^(?P<value>[\d+\.?]*[KMGT]?[\d]?)$",
    )
    ansi: Optional[bool] = Field(
        default=False,
        title="ANSI",
        description="Use ANSI SQL syntax",
        cli=("--no-ansi"),
        no_api=True,
    )
    offload_predicate_modify_hybrid_view: Optional[bool] = Field(
        default=True,
        title="Offload predicate modify hybrid view",
        description=(
            "Prevent an offload predicate from being added to the boundary conditions in a hybrid view. "
            " Can only be used in conjunction with --offload-predicate for --offload-predicate-type values of "
            # "{constants.INCREMENTAL_PREDICATE_TYPE_RANGE}|{constants.INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE}|"
            # "{constants.INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE}|{constants.INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE}"  # noqa: E501
        ),
        cli=("--no-modify-hybrid-view"),
    )
    verify_row_count: Optional[str] = Field(
        default=None,
        title="Verify row count",
        description=(
            "Verify that the number of rows in the source table matches the number of rows in the "
            "target table after the load completes."
        ),
        cli=("--verify", "--no-verify"),
        regex=r"^(False|minus|aggregate)$",
    )
    ver_check: Optional[bool] = Field(
        default=True,
        title="No Version Check",
        description="Do not check for a newer version of the offload tool",
        cli=("--no-version-check"),
        no_api=True,
    )

    offload_by_subpartition: Optional[bool] = Field(
        default=False,
        title="Offload by subpartition",
        description=(
            "Identifies that a range partitioned offload should use subpartition "
            " partition keys and high values in place of top level information"
        ),
        cli=("--offload-by-subpartition"),
    )
    offload_chunk_column: Optional[str] = Field(
        default=None,
        title="Offload chunk column",
        description=(
            "Splits load data by this column during insert from the load table to "
            " the final table. This can be used to manage memory usage"
        ),
        cli=("--offload-chunk-column"),
        no_api=True,
    )
    impala_insert_hint: Optional[str] = Field(
        default=None,
        title="Impala insert hint",
        description=(
            "SHUFFLE|NOSHUFFLE. Used to inject a hint into the insert/select moving "
            "data from load table to final destination. The absence of a value injects no hint. Impala only"
        ),
        cli=("--offload-chunk-impala-insert-hint"),
    )
    offload_distribute_enabled: Optional[bool] = Field(
        default=defaults.offload_distribute_enabled_default(),
        title="Offload distribute enabled",
        decription="Distribute data by partition key(s) during the final INSERT operation of an offload",
        cli=("--offload-distribute-enabled"),
    )
    offload_fs_container: Optional[str] = Field(
        default=None,
        title="Offload fs container",
        description="A valid bucket name when offloading to cloud storage",
        cli=("--offload-fs-container"),
    )
    offload_fs_prefix: Optional[str] = Field(
        default=None,
        title="Offload fs prefix",
        description=(
            "The path with which to prefix offloaded table paths. Takes precedence over "
            '--hdfs-data when --offload-fs-scheme != "inherit"'
        ),
        cli=("--offload-fs-prefix"),
    )
    offload_fs_scheme: Optional[str] = Field(
        default=None,
        title="Offload fs scheme",
        description=(
            "{', '.join(constants.VALID_OFFLOAD_FS_SCHEMES)}. Filesystem type for Offloaded tables"
        ),
        cli=("--offload-fs-scheme"),
    )
    offload_predicate: Optional[str] = Field(
        default=None,
        title="Offload predicate",
        description="Offload predicate to use. Can be used in conjunction with --offload-predicate-type",
        cli=("--offload-predicate"),
    )
    ipa_predicate_type: Optional[str] = Field(
        default=None,
        title="IPA predicate type",
        description=(
            "Override the default INCREMENTAL_PREDICATE_TYPE for a partitioned table. Used to "
            "offload LIST partitioned tables using RANGE logic with "
            # "--offload-predicate-type={constants.INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE} or used for specialized "
            "cases of Incremental Partition Append and Predicate-Based Offload offloading"
        ),
        cli=("--offload-predicate-type"),
    )
    offload_stats_method: Optional[str] = Field(
        default=defaults.offload_stats_method_default(),
        title="Offload stats method",
        description=(
            "NATIVE|HISTORY|COPY|NONE. Method used to manage backend table stats during an Offload."
            " NATIVE is the default. HISTORY will gather stats on all partitions without stats (applicable "
            " to Hive only and will automatically be replaced with NATIVE on Impala). COPY will copy table "
            "statistics from the RDBMS to an offloaded table (applicable to Impala only)"
        ),
        cli=("--offload-stats-method"),
        regex=r"^(NATIVE|HISTORY|COPY|NONE)$",
    )
    # https://pydantic-docs.helpmanual.io/usage/schema/#field-customization
    # @validator("offload_stats_method", pre=True, always=True)
    # def set_offload_stats_method(cls, operation_name):
    #     if (
    #         operation_name == constants.PRESENT_OP_NAME
    #         and environment.get("OFFLOAD_STATS_METHOD") == constants.OFFLOAD_STATS_METHOD_COPY
    #     ):
    #         # COPY is not valid for present so ignore any COPY setting in the env
    #           file which would be in place for offload
    #         return constants.OFFLOAD_STATS_METHOD_NATIVE
    #     else:
    #         return operation_name or environment.get("OFFLOAD_STATS_METHOD") or constants.OFFLOAD_STATS_METHOD_NATIVE

    offload_transport_consistent_read: Optional[bool] = Field(
        default=bool(defaults.offload_transport_consistent_read_default()),
        title="Offload transport consistent read",
        description=(
            "Parallel data transport tasks should have a consistent point in time when reading RDBMS data"
        ),
        cli=("--offload-transport-consistent-read"),
    )
    offload_transport_dsn: Optional[str] = Field(
        default=defaults.offload_transport_dsn_default(),
        title="Offload transport dsn",
        description="DSN override for RDBMS connection during data transport.",
        cli=("--offload-transport-dsn"),
    )
    offload_transport_fetch_size: Optional[PositiveInt] = Field(
        default=try_cast_int(defaults.offload_transport_fetch_size_default(), 5000),
        title="Offload transport fetch size",
        description="Number of records to fetch in a single batch from the RDBMS during Offload",
        cli=("--offload-transport-fetch-size"),
    )
    offload_transport_jvm_overrides: Optional[str] = Field(
        default=None,
        title="Offload transport jvm overrides",
        description=(
            "JVM overrides (inserted right after 'sqoop import' or 'spark-submit' "
        ),
        cli=("--offload-transport-jvm-overrides"),
    )
    offload_transport_method: Optional[str] = Field(
        default=None,
        title="Offload transport method",
        description="VALID_OFFLOAD_TRANSPORT_METHODS Method used to transport data during an Offload",
        cli=("--offload-transport-method"),
        # regex=f"^({'|'.join(constants.VALID_OFFLOAD_TRANSPORT_METHODS)})$",
        no_api=True,
    )
    offload_transport_parallelism: Optional[PositiveInt] = Field(
        default=try_cast_int(defaults.offload_transport_parallelism_default(), 2),
        title="Offload transport parallelism",
        description="Number of parallel data transport tasks to use during an Offload",
        cli=("--offload-transport-parallelism"),
    )
    offload_transport_queue_name: Optional[str] = Field(
        default=None,
        title="Offload transport queue name",
        description="Yarn queue name to be used for Offload transport jobs.",
        cli=("--offload-transport-queue-name"),
    )
    offload_transport_small_table_threshold: Optional[str] = Field(
        default=defaults.offload_transport_small_table_threshold_default(),
        title="Offload transport small table threshold",
        description=(
            "Threshold above which Query Import is no longer considered the correct "
            " offload choice for non-partitioned tables. eg. 100M, 0.5G, 1G"
        ),
        cli=("--offload-transport-small-table-threshold"),
        regex=r"^(\d+[MG]|\d+[MG]?[B]?)$",
    )
    # TODO: this is probably really insecure! Figure out default values
    offload_transport_spark_properties: Optional[Json] = Field(
        default=defaults.offload_transport_spark_properties_default(),
        title="Offload transport spark properties",
        description="Override defaults for Spark configuration properties using key/value pairs in JSON format",
        cli=("--offload-transport-spark-properties"),
    )
    offload_transport_spark_files: Optional[str] = Field(
        default=defaults.offload_transport_spark_files_default(),
        title="Offload transport spark files",
        description="CSV of files to pass to Spark",
        cli=("--offload-transport-spark-files"),
    )
    offload_transport_spark_jars: Optional[str] = Field(
        default=defaults.offload_transport_spark_jars_default(),
        title="Offload transport spark JAR files",
        description="CSV of JAR files to pass to Spark",
        cli=("--offload-transport-spark-jars"),
    )
    offload_transport_validation_polling_interval: Optional[int] = Field(
        default=try_cast_int(
            defaults.offload_transport_validation_polling_interval_default(), 0
        ),
        title="Offload transport validation polling interval",
        description=(
            "Polling interval in seconds for validation of Spark transport row count. "
            "-1 disables retrieval of RDBMS SQL statistics. 0 disables polling resulting in "
            "a single capture of SQL statistics. A value greater than 0 polls transport SQL "
            "statistics using the specified interval"
        ),
        cli=("--offload-transport-validation-polling-interval"),
    )

    @validator("offload_transport_validation_polling_interval")
    def set_offload_transport_validation_polling_interval(cls, value):
        if value == -1:
            return value
        elif value >= 0:
            return value
        else:
            raise ValueError(
                "Invalid value for offload_transport_validation_polling_interval"
            )

    offload_type: Optional[str] = Field(
        default="FULL",
        title="Offload type",
        description=(
            "Identifies a range partitioned offload as FULL or INCREMENTAL. FULL dictates "
            "that all data is offloaded. INCREMENTAL dictates that data up to an incremental "
            "threshold will be offloaded"
        ),
        cli=("--offload-type"),
        regex=r"^(FULL|INCREMENTAL)$",
    )
    older_than_date: Optional[str] = Field(
        default=None,
        title="Older than date",
        description="Offload partitions older than this date (use YYYY-MM-DD format).",
        cli=("--older-than-date"),
        regex=r"^\d{4}-\d{2}-\d{2}$",
    )
    older_than_days: Optional[int] = Field(
        default=None,
        title="Older than days",
        description=(
            "Offload partitions older than this number of days "
            "(exclusive, ie. the boundary partition is not offloaded). "
            "Suitable for keeping data up to a certain age in the source table. "
            "Alternative to --older-than-date option."
        ),
        cli=("--older-than-days"),
    )
    # TODO: Ignore when Snowflake is used
    offload_partition_columns: Optional[List[str]] = Field(
        default=[],
        title="Offload partition columns",
        description=(
            "Comma separated list of partition columns to be used for offloading. "
            "Override column used by Offload to partition backend data. Defaults to source-table partition columns"
        ),
        cli=("--offload-partition-columns"),
    )
    not_null_columns_csv: Optional[str] = Field(
        default=None,
        title="Not null columns",
        description=("Comma separated list of columns that are not null. "),
        cli=("--not-null-columns"),
    )
    synthetic_partition_digits: Optional[PositiveInt] = Field(
        default=15,
        title="Synthetic partition digits",
        description=(
            "Maximum digits allowed for a numeric partition value, defaults to 15"
        ),
        cli=("--partition-digits"),
    )

    offload_partition_functions: Optional[List[str]] = Field(
        default=[],
        title="Offload partition functions",
        description=("External UDF(s) used by Offload to partition backend data."),
        cli=("--partition-functions"),
    )
    # TODO: Double check validation
    offload_partition_granularity: Optional[str] = Field(
        default="M",
        title="Offload partition granularity",
        description=(
            "partition level/granularity. Use integral size for numeric partitions. "
            " Use sub-string length for string partitions. eg. 'M' partitions by Year-Month, "
            " 'D' partitions by Year-Month-Day, '5000' partitions in blocks of 5000 values, "
            " '2' on a string partition key partitions using the first two characters."
        ),
        cli=("--partition-granularity"),
        regex=r"^(Y|M|D|\d+|\d+[M]?[D]?)$",
    )
    offload_partition_lower_value: Optional[int] = Field(
        default=None,
        title="Offload partition lower value",
        description=(
            "Integer value defining the lower bound of a range values used "
            "for backend integer range partitioning."
        ),
        cli=("--partition-lower-value"),
    )
    offload_partition_upper_value: Optional[int] = Field(
        default=None,
        title="Offload partition upper value",
        description=(
            "Integer value defining the upper bound of a range values "
            "used for backend integer range partitioning."
        ),
        cli=("--partition-upper-value"),
    )
    partition_names_csv: Optional[str] = Field(
        default=None,
        title="Partition names CSV",
        description=(
            "CSV of RDBMS partition names to be used to derive values "
            "for --less-than-value, --older-than-date "
            " or --equal-to-values as appropriate. Specifying multiple "
            "partitions is only valid for list partitioned tables."
        ),
        cli=("--partition-names-csv"),
    )
    preserve_load_table: Optional[bool] = Field(
        default=False,
        title="Preserve load table",
        description="Stops the load table from being dropped on completion of offload",
        cli=("--preserve-load-table"),
    )
    purge_backend_table: Optional[bool] = Field(
        default=False,
        title="Purge backend table",
        description=(
            "Include PURGE operation (if appropriate for the backend system) when removing data table with "
            " --reset-backend-table. Use with caution"
        ),
        cli=("--purge-backend-table"),
    )
    reset_backend_table: Optional[bool] = Field(
        default=False,
        title="Reset backend table",
        description=option_descriptions.RESET_BACKEND_TABLE,
        cli=("--reset-backend-table"),
    )
    reset_hybrid_view: Optional[bool] = Field(
        default=False,
        title="Reset hybrid view",
        description=(
            "Reset Incremental Partition Append or Predicate-Based Offload predicates in the hybrid view."
        ),
        cli=("--reset-hybrid-view"),
    )
    reuse_backend_table: Optional[bool] = Field(
        default=False,
        title="Reuse backend table",
        description=option_descriptions.REUSE_BACKEND_TABLE,
        cli=("--reuse-backend-table"),
    )
    skip: Optional[str] = Field(
        default=None,
        title="Skip Steps",
        description=(
            "Skip given steps. Csv of step ids you want to skip. Step ids are found by "
            " replacing spaces with underscore. Case insensitive."
        ),
        cli=("--skip-steps"),
    )
    sort_columns_csv: Optional[str] = Field(
        default=None,
        title="Sort columns CSV",
        description=(
            "CSV list of sort/cluster columns to use when storing data in a backend table."
        ),
        cli=("--sort-columns-csv"),
    )
    sqoop_additional_options: Optional[str] = Field(
        default=defaults.sqoop_additional_options_default(),
        title="Sqoop additional options",
        description="Sqoop additional options (added to the end of the command line)",
        cli=("--sqoop-additional-options"),
    )
    sqoop_mapreduce_map_java_opts: Optional[str] = Field(
        default=None,
        title="Sqoop mapreduce map java opts",
        description="Sqoop specific setting for mapreduce.map.java.opts",
        cli=("--sqoop-mapreduce-map-java-opts"),
    )
    sqoop_mapreduce_map_memory_mb: Optional[int] = Field(
        default=None,
        title="Sqoop mapreduce map memory mb",
        description="Sqoop specific setting for mapreduce.map.memory.mb",
        cli=("--sqoop-mapreduce-map-memory-mb"),
    )
    # TODO: Need to add special rules based on frontent, Google BigQuery, Snowflake, etc.
    storage_compression: Optional[str] = Field(
        default=None,
        title="Storage compression",
        description=(
            "Backage storage compression, valid values are: HIGH|MED|NONE|GZIP|ZLIB|SNAPPY."
        ),
        cli=("--storage-compression"),
        regex=r"^(HIGH|MED|NONE|GZIP|ZLIB|SNAPPY)$",
    )
    storage_format: Optional[str] = Field(
        default="ORC",
        title="Storage format",
        description=(
            "Backage storage format,(ORC|PARQUET). Defaults to ORC when offloading to Hive."
        ),
        cli=("--storage-format"),
        regex=r"^(PARQUET|ORC)$",
    )

    target_owner_name: Optional[str] = Field(
        default=None,
        title="Target owner name",
        description=(
            "Override owner and/or name of created frontend or backend object as "
            " appropriate for a command. Format is OWNER.VIEW_NAME"
        ),
        cli=("--target-owner-name"),
    )
    timestamp_tz_columns_csv: Optional[List[str]] = Field(
        default=None,
        title="Timestamp timezone columns CSV",
        description=(
            "CSV list of columns to offload as a time zoned column [only effective for date based columns]."
        ),
        cli=("--timestamp-tz-columns"),
    )
    unicode_string_columns_csv: Optional[str] = Field(
        default=None,
        title="Unicode string columns CSV",
        description=(
            "CSV list of columns to offload as "
            "Unicode string (only effective for string columns) "
        ),
        cli=("--unicode-string-columns"),
    )

    variable_string_columns_csv: Optional[str] = Field(
        default=None,
        title="Variable string columns CSV",
        description=(
            "CSV list of columns to offload as a variable string type (only effective for date based columns)"
        ),
        cli=("--variable-string-columns"),
    )
    verify_parallelism: Optional[int] = Field(
        default=try_cast_int(defaults.verify_parallelism_default(), 0),
        title="Verify parallelism",
        # description="{constants.DATA_SAMPLE_PARALLELISM}",
        cli=("--verify-parallelism"),
    )

    version: Optional[bool] = Field(
        default=False,
        title="Display version",
        description=("Print version and exit"),
        cli=("--version"),
        no_api=True,
    )
    verbose: Optional[bool] = Field(
        default=False,
        title="Verbose",
        description=("Verbose output"),
        cli=("--verbose"),
        np_api=True,
    )
    verbose_level: Optional[int] = Field(
        default=0,
        title="Verbose level",
        description=("Verbose level"),
        cli=("--verbose-level"),
        np_api=True,
    )

    @validator(
        "decimal_columns_csv_list",
        "decimal_columns_type_list",
        "equal_to_values",
        "offload_partition_columns",
        "offload_partition_functions",
        pre=True,
        always=True,
    )
    def validate_comma_delimitted_option(
        cls,
        value: Union[None, str, List[str]],
    ) -> Union[None, List[str], str]:
        """
        Validates that the input is comma delimitted

        Validates and parses comma separated list.  Returns a list of parsed values
        """
        if value is None:
            return None
        elif isinstance(value, str) and not value.startswith("["):
            return [split_value.strip() for split_value in value.split(",")]
        elif isinstance(value, (list, str)):
            return value
        raise ValueError(value)


class OffloadType(str, Enum):
    """User event valid values"""

    full = "FULL"
    incremental = "INCREMENTAL"
