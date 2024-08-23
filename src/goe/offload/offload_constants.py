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

""" Constants for use in both goelib modules and goe.py
    We needed a new home that would avoid circular dependencies
"""

# Try not to import any modules in here, this module is widely imported and we've previously had subtle side effects

LOG_LEVEL_INFO = "info"
LOG_LEVEL_DETAIL = "detail"
LOG_LEVEL_DEBUG = "debug"

# DB types
DBTYPE_BIGQUERY = "bigquery"
DBTYPE_HIVE = "hive"
DBTYPE_IMPALA = "impala"
DBTYPE_MSSQL = "mssql"
DBTYPE_ORACLE = "oracle"
DBTYPE_SPARK = "spark"
DBTYPE_SYNAPSE = "synapse"
DBTYPE_SNOWFLAKE = "snowflake"
DBTYPE_TERADATA = "teradata"

PART_COL_GRANULARITY_DAY = "D"
PART_COL_GRANULARITY_MONTH = "M"
PART_COL_GRANULARITY_YEAR = "Y"
PART_COL_DATE_GRANULARITIES = [
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
    PART_COL_GRANULARITY_YEAR,
]

# These constants are visible in some exception messages so I've used hyphens to beautify a little
PART_OUT_OF_LIST = "OUT-OF-LIST"
PART_OUT_OF_RANGE = "OUT-OF-RANGE"

# Table/file storage formats, currently used for final and load table definitions
FILE_STORAGE_FORMAT_AVRO = "AVRO"
FILE_STORAGE_FORMAT_BIGTABLE = "BIGTABLE"
FILE_STORAGE_FORMAT_ORC = "ORC"
FILE_STORAGE_FORMAT_PARQUET = "PARQUET"
FILE_STORAGE_FORMAT_PARQUET_IMPALA = "PARQUETFILE"

# Table/file storage compression codecs, currently used for final table definitions and option values
FILE_STORAGE_COMPRESSION_CODEC_GZIP = "GZIP"
FILE_STORAGE_COMPRESSION_CODEC_SNAPPY = "SNAPPY"
FILE_STORAGE_COMPRESSION_CODEC_ZLIB = "ZLIB"

# Backend distribution constants for those little variations between the backends.
# These should not be used where capabilities and the BackendApi can deal with
# differences. Mainly used for testing but occasionally used for other purposes.
# Cloudera Data Hub
BACKEND_DISTRO_CDH = "CDH"
# Amazon Elastic Map Reduce
BACKEND_DISTRO_EMR = "EMR"
# Google Cloud Platform
BACKEND_DISTRO_GCP = "GCP"
# Hortonworks Data Platform
BACKEND_DISTRO_HDP = "HDP"
# MapR Data Platform
BACKEND_DISTRO_MAPR = "MAPR"
# Snowflake Cloud Warehouse
BACKEND_DISTRO_SNOWFLAKE = "SNOWFLAKE"
# Microsoft Azure Synapse Analytics
BACKEND_DISTRO_MSAZURE = "MSAZURE"
HADOOP_BASED_BACKEND_DISTRIBUTIONS = [
    BACKEND_DISTRO_CDH,
    BACKEND_DISTRO_EMR,
    BACKEND_DISTRO_HDP,
    BACKEND_DISTRO_MAPR,
]

# Stats related constants.
OFFLOAD_STATS_METHOD_COPY = "COPY"
OFFLOAD_STATS_METHOD_HISTORY = "HISTORY"
OFFLOAD_STATS_METHOD_NATIVE = "NATIVE"
OFFLOAD_STATS_METHOD_NONE = "NONE"

EMPTY_BACKEND_TABLE_STATS_LIST = [-1, 0, 0]  # num_rows, num_bytes, avg_row_len
EMPTY_BACKEND_TABLE_STATS_DICT = {"num_rows": -1, "num_bytes": 0, "avg_row_len": 0}
EMPTY_BACKEND_COLUMN_STATS_LIST = []
EMPTY_BACKEND_COLUMN_STATS_DICT = {}

RDBMS_MAX_DESCRIPTOR_LENGTH = 30

SORT_COLUMNS_NO_CHANGE = "GOE_SORT_NOT_SET"
SORT_COLUMNS_NONE = "NONE"

PRESENT_OP_NAME = "present"

NOT_NULL_PROPAGATION_AUTO = "AUTO"
NOT_NULL_PROPAGATION_NONE = "NONE"

# This parameter value is documented so do not change without reviewing config template and documentation
OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED = -1

# Offload transport constants
LIVY_IDLE_SESSION_TIMEOUT = 600
LIVY_MAX_SESSIONS = 10
OFFLOAD_TRANSPORT_AUTO = "AUTO"
OFFLOAD_TRANSPORT_GOE = "GOE"
OFFLOAD_TRANSPORT_GCP = "GCP"
OFFLOAD_TRANSPORT_SQOOP = "SQOOP"

# DDL file
DDL_FILE_AUTO = "AUTO"

# Exception markers
ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT = "Using adjusted backend table name"
CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT = "Conflicting data identification options"
INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT = "Invalid data type conversion for column"
IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT = (
    "INCREMENTAL_PREDICATE_TYPE cannot be changed for offloaded table"
)
IPA_PREDICATE_TYPE_EXCEPTION_TEXT = (
    "--offload-predicate-type/RDBMS partition type combination is not valid"
)
IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT = (
    "partition identification options are not compatible"
)
IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT = (
    "--offload-predicate-type is not valid for a first time predicate-based offload"
)
IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT = (
    "Missing --offload-predicate option. "
    "This option is mandatory to offload tables with an INCREMENTAL_PREDICATE_TYPE configuration of PREDICATE"
)
MISSING_METADATA_EXCEPTION_TEMPLATE = "Missing metadata for table %s.%s. Offload with --reset-backend-table to overwrite table data"
METADATA_EMPTY_TABLE_EXCEPTION_TEMPLATE = "Empty table %s.%s has metadata. Offload with --reuse-backend-table to populate this table"
OFFLOAD_TYPE_CHANGE_FOR_LIST_EXCEPTION_TEXT = "Switching to offload type INCREMENTAL for LIST partitioned table requires --equal-to-values/--partition-names"
OFFLOAD_TYPE_CHANGE_FOR_LIST_MESSAGE_TEXT = (
    "Switching to INCREMENTAL for LIST partitioned table"
)
OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT = "Switching from offload type FULL to INCREMENTAL is not supported for Subpartition-Based Offload"
RESET_HYBRID_VIEW_EXCEPTION_TEXT = (
    "Offload data identification options required with --reset-hybrid-view"
)
TARGET_HAS_DATA_MESSAGE_TEMPLATE = "Target table %s.%s already has data. Offload with --reset-backend-table to overwrite table data"
TOTAL_ROWS_OFFLOADED_LOG_TEXT = "Total rows offloaded"
DDL_FILE_EXECUTE_MESSAGE_TEXT = (
    "Switching command to non-execute mode due to --ddl-file option"
)
DDL_FILE_WRITE_MESSAGE_TEMPLATE = "Table DDL has been written to file: {}"

# Offload capabilities we can switch on/off by backend db type
# Any capabilities that are version specific will have extra code in the BackendApi method
# You must add an explanation as to what the capability does so that developers know what
# to research for future backends

""" CAPABILITY_BUCKET_HASH_COLUMN
    Can the backend make use of a hash column.
"""
CAPABILITY_BUCKET_HASH_COLUMN = "bucket_hash_column"
""" CAPABILITY_CANONICAL_X
         DATE: Does the backend have a pure date data type (no time part)
        FLOAT: Does the backend have a pure 4 byte binary floating point data type
         TIME: Does the backend have a pure time data type (no date part)
"""
CAPABILITY_CANONICAL_DATE = "canonical_date"
CAPABILITY_CANONICAL_FLOAT = "canonical_float"
CAPABILITY_CANONICAL_TIME = "canonical_time"
""" CAPABILITY_CASE_SENSITIVE
        Does the backend have case sensitive db/table name identifiers
"""
CAPABILITY_CASE_SENSITIVE = "case_sensitive"
""" CAPABILITY_COLUMN_STATS_SET
        Does the backend allow columns stats to be explicitly set
"""
CAPABILITY_COLUMN_STATS_SET = "column_stats_set"
""" CAPABILITY_CREATE_DB
        Can we create our logical container (database/schema/dataset) to contain our objects from our
        code, or will these need to be pre-created for us by the customer
"""
CAPABILITY_CREATE_DB = "create_db"
""" CAPABILITY_DROP_COLUMN
        Does the backend support dropping a column
"""
CAPABILITY_DROP_COLUMN = "drop_column"
""" CAPABILITY_FS_SCHEME_X
        Two scenarios depending on the backend.

        1) The backend gives the option to state where the final table data should be located.
            An existing backend that we support that does this is Cloudera (Impala). When using
            an FS scheme with this backend the table is created with a LOCATION clause referencing
            the FS scheme. INHERIT is a special case and indicates that we do not include a
            LOCATION clause when creating a table and inherit the value from the container database
            instead.

        2) The backend does not give the option to state where the final data should be located.
            Existing backends that we support where this is the case are BigQuery and Snowflake.
            In these backends, the supported FS schemes are those that can be used to stage the
            data before loading. The supported list will be those that our load method is able
            to read from.
"""
CAPABILITY_FS_SCHEME_ABFS = "fs_schema_abfs"
CAPABILITY_FS_SCHEME_ADL = "fs_schema_adl"
CAPABILITY_FS_SCHEME_GS = "fs_schema_gs"
CAPABILITY_FS_SCHEME_HDFS = "fs_schema_hdfs"
CAPABILITY_FS_SCHEME_INHERIT = "fs_scheme_inherit"
CAPABILITY_FS_SCHEME_S3A = "fs_schema_s3a"
CAPABILITY_FS_SCHEME_WASB = "fs_schema_wasb"
""" Do we support --transform-column on a particular backend system.
    This constant was introduced for MVP Synapse implementation because Synapse does not have REGEXP support.
    If this feature is completed in the future and we revisit Synapse we may choose to split the constant into
    multiple constants for different transformation keywords.
"""
CAPABILITY_GOE_COLUMN_TRANSFORMATIONS = "goe_column_transformations"
CAPABILITY_GOE_JOIN_PUSHDOWN = "goe_join_pushdown"
CAPABILITY_GOE_LIST_PARTITION_APPEND = "goe_list_partition_append"
""" CAPABILITY_GOE_MATERIALIZED_JOIN
        Can we create a table in the backend that is a materialized join generated using our
        Offload or Present join pushdown functionality. This is only used for enabling/disabling
        tests per backend
"""
CAPABILITY_GOE_MATERIALIZED_JOIN = "goe_mat_join"
""" CAPABILITY_GOE_PARTITION_FUNCTIONS
        Does the backend support our Partition Functions feature:
            - User creates a SQL UDF to convert source partition column data to synthetic partition
                column values.
            - User provides the name of the SQL UDF to GOE with a new
                --partition-functions=<UDF_NAME> option.
            - Offload will use the UDF when generating SQL statements/predicates
                involving the natural and synthetic partition columns.
"""
CAPABILITY_GOE_PARTITION_FUNCTIONS = "goe_partition_functions"
""" CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY
        Does the frontend system support IPA by composite partition.
"""
CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY = "goe_multi_column_incremental_key"
CAPABILITY_GOE_OFFLOAD_STATUS_REPORT = "goe_offload_status_report"
CAPABILITY_GOE_SCHEMA_SYNC = "goe_schema_sync"
""" CAPABILITY_GOE_SEQ_TABLE
        Does the backend require the GOE sequence table.

        Cloudera Data Hub versions earlier than 5.10.x contain a performance issue with Impala queries
        that contain a large number of constants in an in-list (refer to IMPALA-4302 for details).
        GOE includes an optimization that overcomes this by transforming large in-lists
        into a semi-join using a sequence table.

        Only currently required on Cloudera Data Hub versions earlier than 5.10.x.
"""
CAPABILITY_GOE_SEQ_TABLE = "goe_seq_table"
""" CAPABILITY_GOE_UDFS
        Does the backend require the GOE User-defined Functions (UDFs).

        For Impala-supported backends, these are used for performance optimization reasons and are recommended,
        but not mandatory.
"""
CAPABILITY_GOE_UDFS = "goe_udfs"
""" CAPABILITY_LOAD_DB_TRANSPORT
        Do we use a load database to stage data.
"""
CAPABILITY_LOAD_DB_TRANSPORT = "load_db_transport"
""" CAPABILITY_LOW_HIGH_VALUE_FROM_STATS
        Can we get column low/high values from optimizer statistics
"""
CAPABILITY_LOW_HIGH_VALUE_FROM_STATS = "low_high_value_from_stats"
""" CAPABILITY_NAN
        Does the backend have any floating point datatypes that support Nan/Infinity/-Infinity values.
        If not then --allow-floating-point-conversions must be used to offload RDBMS
        nan capable datatypes (for Oracle these are BINARY_%).
        Note that ColumnMetadataInterface has an is_nan_capable method that determines
        which column datatypes are nan capable.
"""
CAPABILITY_NAN = "nan"
""" CAPABILITY_NANOSECONDS
        Nanosecond support appears to be driven from db_api.max_datetime_scale
        rather than this capability in our code, although test appears to use this.
"""
CAPABILITY_NANOSECONDS = "nanoseconds"
""" CAPABILITY_NOT_NULL_COLUMN
        Does the backend support NOT NULL column constraints.
"""
CAPABILITY_NOT_NULL_COLUMN = "not_null_column"
""" CAPABILITY_PARAMETERIZED_QUERIES
        Does the backend allow parameterized queries.
        E.g. SELECT col FROM db.table WHERE col < @{} LIMIT 10
"""
CAPABILITY_PARAMETERIZED_QUERIES = "parameterized_queries"
""" CAPABILITY_PARTITION_BY_X
        COLUMN: Does the backend support tables partitioned by a column(s)
        STRING: Does the backend support tables partitioned by a string value
"""
CAPABILITY_PARTITION_BY_COLUMN = "partition_by_column"
CAPABILITY_PARTITION_BY_STRING = "partition_by_string"
""" CAPABILITY_QUERY_SAMPLE_CLAUSE
        Does the backend allow sampling clause.
        E.g. Snowflake supports SAMPLE ROW (x)/SAMPLE BLOCK (x)
"""
CAPABILITY_QUERY_SAMPLE_CLAUSE = "query_sample_clause"
""" CAPABILITY_RANGER
        Does the backend use Apache Ranger to control authorization.
"""
CAPABILITY_RANGER = "ranger"
""" CAPABILITY_REFRESH_FUNCTIONS
        Does the backend support refreshing UDFs.
"""
CAPABILITY_REFRESH_FUNCTIONS = "refresh_functions"
""" CAPABILITY_SCHEMA_EVOLUTION
        Does the backend support automatic schema evolution.
        Currently restricted in scope to: does the backend support ALTER TABLE...ADD COLUMN.
"""
CAPABILITY_SCHEMA_EVOLUTION = "schema_evolution"
""" CAPABILITY_SENTRY
        Does the backend use Apache Sentry to control authorization.
"""
CAPABILITY_SENTRY = "sentry"
""" CAPABILITY_SORTED_TABLE
    CAPABILITY_SORTED_TABLE_MODIFY
        Does the backend support sorting/clustering table columns.
        If so, does the backend also support modifying the sorting/clustering columns.
"""
CAPABILITY_SORTED_TABLE = "pre_sorted_tables"
CAPABILITY_SORTED_TABLE_MODIFY = "pre_sorted_tables_mod"
""" CAPABILITY_SQL_MICROSECOND_PREDICATE
        Can the orchestration SQL engine cope with literal predicates with microsecond precision.
        See GOE-1182 for example of why this is needed
"""
CAPABILITY_SQL_MICROSECOND_PREDICATE = "sql_pred_microseconds"
""" CAPABILITY_SYNTHETIC_PARTITIONING
        Does the backend support GOE's synthetic partition keys (e.g. GOE_PART_M_TIME_ID or
        GOE_PART_U0_SOURCE_CODE).

        This will be required when the backend natively supports partitioning but either does not
        support native range partitioning, or when the natural partition column datatype means
        we cannot use native partitioning.
"""
CAPABILITY_SYNTHETIC_PARTITIONING = "synthetic_partitioning"
""" CAPABILITY_TABLE_STATS_X
        COMPUTE: Does the backend support computing table stats
            GET: Does the backend support getting table stats
            SET: Does the backend support setting table stats
"""
CAPABILITY_TABLE_STATS_COMPUTE = "table_stats_compute"
CAPABILITY_TABLE_STATS_GET = "table_stats_get"
CAPABILITY_TABLE_STATS_SET = "table_stats_set"

# Frontend capabilities
CAPABILITY_GOE_HAS_DB_CODE_COMPONENT = "goe_has_db_code_component"
CAPABILITY_HYBRID_SCHEMA = "hybrid_schema"

HIVE_BACKEND_CAPABILITIES = {
    CAPABILITY_BUCKET_HASH_COLUMN: False,
    CAPABILITY_CANONICAL_DATE: True,
    CAPABILITY_CANONICAL_FLOAT: True,
    CAPABILITY_CANONICAL_TIME: False,
    CAPABILITY_CASE_SENSITIVE: False,
    CAPABILITY_COLUMN_STATS_SET: False,
    CAPABILITY_CREATE_DB: True,
    CAPABILITY_DROP_COLUMN: False,
    CAPABILITY_FS_SCHEME_ABFS: False,
    CAPABILITY_FS_SCHEME_ADL: True,
    CAPABILITY_FS_SCHEME_GS: False,
    CAPABILITY_FS_SCHEME_HDFS: True,
    CAPABILITY_FS_SCHEME_INHERIT: True,
    CAPABILITY_FS_SCHEME_S3A: True,
    CAPABILITY_FS_SCHEME_WASB: True,
    CAPABILITY_GOE_COLUMN_TRANSFORMATIONS: True,
    CAPABILITY_GOE_JOIN_PUSHDOWN: True,
    CAPABILITY_GOE_MATERIALIZED_JOIN: True,
    CAPABILITY_GOE_PARTITION_FUNCTIONS: False,
    CAPABILITY_GOE_SEQ_TABLE: False,
    CAPABILITY_GOE_UDFS: False,
    CAPABILITY_LOAD_DB_TRANSPORT: True,
    CAPABILITY_NAN: True,
    CAPABILITY_NANOSECONDS: True,
    CAPABILITY_NOT_NULL_COLUMN: False,
    CAPABILITY_PARAMETERIZED_QUERIES: False,
    CAPABILITY_PARTITION_BY_COLUMN: True,
    CAPABILITY_PARTITION_BY_STRING: True,
    CAPABILITY_QUERY_SAMPLE_CLAUSE: False,
    # EMR does support Ranger so this could be set to True once properly tested (GOE-1878)
    CAPABILITY_RANGER: False,
    CAPABILITY_REFRESH_FUNCTIONS: False,
    CAPABILITY_SCHEMA_EVOLUTION: True,
    CAPABILITY_SENTRY: False,
    CAPABILITY_SORTED_TABLE: True,
    CAPABILITY_SORTED_TABLE_MODIFY: True,
    CAPABILITY_SQL_MICROSECOND_PREDICATE: False,
    CAPABILITY_SYNTHETIC_PARTITIONING: True,
    CAPABILITY_TABLE_STATS_COMPUTE: True,
    CAPABILITY_TABLE_STATS_GET: True,
    CAPABILITY_TABLE_STATS_SET: False,
}

# This may be an inefficient copy but importing copy into this widely used module was causing side effects
IMPALA_BACKEND_CAPABILITIES = {k: v for k, v in HIVE_BACKEND_CAPABILITIES.items()}

IMPALA_BACKEND_CAPABILITIES.update(
    {
        CAPABILITY_CANONICAL_DATE: True,
        CAPABILITY_COLUMN_STATS_SET: True,
        CAPABILITY_DROP_COLUMN: True,
        CAPABILITY_FS_SCHEME_ABFS: True,
        CAPABILITY_FS_SCHEME_WASB: False,
        CAPABILITY_GOE_SEQ_TABLE: True,
        CAPABILITY_NOT_NULL_COLUMN: False,
        CAPABILITY_RANGER: True,
        CAPABILITY_REFRESH_FUNCTIONS: True,
        CAPABILITY_SENTRY: True,
        CAPABILITY_SQL_MICROSECOND_PREDICATE: True,
        CAPABILITY_TABLE_STATS_SET: True,
    }
)

# While Spark is a bare-minimum implementation we set all capabilities to False
SPARK_BACKEND_CAPABILITIES = {k: False for k, v in HIVE_BACKEND_CAPABILITIES.items()}

BIGQUERY_BACKEND_CAPABILITIES = {
    CAPABILITY_BUCKET_HASH_COLUMN: False,
    CAPABILITY_CANONICAL_DATE: True,
    CAPABILITY_CANONICAL_FLOAT: False,
    CAPABILITY_CANONICAL_TIME: True,
    CAPABILITY_CASE_SENSITIVE: True,
    CAPABILITY_COLUMN_STATS_SET: False,
    CAPABILITY_CREATE_DB: True,
    CAPABILITY_DROP_COLUMN: False,
    CAPABILITY_FS_SCHEME_ABFS: False,
    CAPABILITY_FS_SCHEME_ADL: False,
    CAPABILITY_FS_SCHEME_GS: True,
    CAPABILITY_FS_SCHEME_HDFS: False,
    CAPABILITY_FS_SCHEME_INHERIT: False,
    CAPABILITY_FS_SCHEME_S3A: False,
    CAPABILITY_FS_SCHEME_WASB: False,
    CAPABILITY_GOE_COLUMN_TRANSFORMATIONS: True,
    CAPABILITY_GOE_JOIN_PUSHDOWN: True,
    CAPABILITY_GOE_MATERIALIZED_JOIN: True,
    CAPABILITY_GOE_PARTITION_FUNCTIONS: True,
    CAPABILITY_GOE_SEQ_TABLE: False,
    CAPABILITY_GOE_UDFS: False,
    CAPABILITY_LOAD_DB_TRANSPORT: True,
    CAPABILITY_NAN: True,
    CAPABILITY_NANOSECONDS: False,
    CAPABILITY_NOT_NULL_COLUMN: True,
    CAPABILITY_PARAMETERIZED_QUERIES: True,
    CAPABILITY_PARTITION_BY_COLUMN: True,
    CAPABILITY_PARTITION_BY_STRING: False,
    CAPABILITY_QUERY_SAMPLE_CLAUSE: False,
    CAPABILITY_RANGER: False,
    CAPABILITY_REFRESH_FUNCTIONS: False,
    CAPABILITY_SCHEMA_EVOLUTION: True,
    CAPABILITY_SENTRY: False,
    CAPABILITY_SORTED_TABLE: True,
    CAPABILITY_SORTED_TABLE_MODIFY: True,
    CAPABILITY_SQL_MICROSECOND_PREDICATE: True,
    CAPABILITY_SYNTHETIC_PARTITIONING: True,
    CAPABILITY_TABLE_STATS_COMPUTE: False,
    CAPABILITY_TABLE_STATS_GET: True,
    CAPABILITY_TABLE_STATS_SET: False,
}

SNOWFLAKE_BACKEND_CAPABILITIES = {
    CAPABILITY_BUCKET_HASH_COLUMN: False,
    CAPABILITY_CANONICAL_DATE: True,
    CAPABILITY_CANONICAL_FLOAT: False,
    CAPABILITY_CANONICAL_TIME: True,
    CAPABILITY_CASE_SENSITIVE: True,
    CAPABILITY_COLUMN_STATS_SET: False,
    CAPABILITY_CREATE_DB: True,
    CAPABILITY_DROP_COLUMN: True,
    CAPABILITY_FS_SCHEME_ABFS: True,
    CAPABILITY_FS_SCHEME_ADL: True,
    CAPABILITY_FS_SCHEME_GS: True,
    CAPABILITY_FS_SCHEME_HDFS: False,
    CAPABILITY_FS_SCHEME_INHERIT: False,
    CAPABILITY_FS_SCHEME_S3A: True,
    CAPABILITY_FS_SCHEME_WASB: True,
    CAPABILITY_GOE_COLUMN_TRANSFORMATIONS: True,
    CAPABILITY_GOE_JOIN_PUSHDOWN: True,
    CAPABILITY_GOE_MATERIALIZED_JOIN: True,
    CAPABILITY_GOE_PARTITION_FUNCTIONS: False,
    CAPABILITY_GOE_SEQ_TABLE: False,
    CAPABILITY_GOE_UDFS: False,
    CAPABILITY_LOAD_DB_TRANSPORT: False,
    CAPABILITY_NAN: True,
    CAPABILITY_NANOSECONDS: True,
    CAPABILITY_NOT_NULL_COLUMN: True,
    CAPABILITY_PARAMETERIZED_QUERIES: True,
    CAPABILITY_PARTITION_BY_COLUMN: False,
    CAPABILITY_PARTITION_BY_STRING: False,
    CAPABILITY_QUERY_SAMPLE_CLAUSE: True,
    CAPABILITY_RANGER: False,
    CAPABILITY_REFRESH_FUNCTIONS: False,
    CAPABILITY_SCHEMA_EVOLUTION: True,
    CAPABILITY_SENTRY: False,
    CAPABILITY_SORTED_TABLE: True,
    CAPABILITY_SORTED_TABLE_MODIFY: True,
    CAPABILITY_SQL_MICROSECOND_PREDICATE: True,
    CAPABILITY_SYNTHETIC_PARTITIONING: False,
    CAPABILITY_TABLE_STATS_COMPUTE: False,
    CAPABILITY_TABLE_STATS_GET: True,
    CAPABILITY_TABLE_STATS_SET: False,
}

SYNAPSE_BACKEND_CAPABILITIES = {
    CAPABILITY_BUCKET_HASH_COLUMN: True,
    CAPABILITY_CANONICAL_DATE: True,
    CAPABILITY_CANONICAL_FLOAT: True,
    CAPABILITY_CANONICAL_TIME: True,
    CAPABILITY_CASE_SENSITIVE: True,
    CAPABILITY_COLUMN_STATS_SET: True,
    CAPABILITY_CREATE_DB: True,
    CAPABILITY_DROP_COLUMN: True,
    CAPABILITY_FS_SCHEME_ABFS: True,
    CAPABILITY_FS_SCHEME_ADL: True,
    CAPABILITY_FS_SCHEME_GS: False,
    CAPABILITY_FS_SCHEME_HDFS: False,
    CAPABILITY_FS_SCHEME_INHERIT: False,
    CAPABILITY_FS_SCHEME_S3A: False,
    CAPABILITY_FS_SCHEME_WASB: True,
    CAPABILITY_GOE_COLUMN_TRANSFORMATIONS: False,
    CAPABILITY_GOE_JOIN_PUSHDOWN: True,
    CAPABILITY_GOE_MATERIALIZED_JOIN: True,
    CAPABILITY_GOE_PARTITION_FUNCTIONS: False,
    CAPABILITY_GOE_SEQ_TABLE: False,
    CAPABILITY_GOE_UDFS: False,
    CAPABILITY_LOAD_DB_TRANSPORT: True,
    CAPABILITY_NAN: False,
    CAPABILITY_NANOSECONDS: False,
    CAPABILITY_NOT_NULL_COLUMN: True,
    CAPABILITY_PARAMETERIZED_QUERIES: True,
    CAPABILITY_PARTITION_BY_COLUMN: False,
    CAPABILITY_PARTITION_BY_STRING: False,
    CAPABILITY_QUERY_SAMPLE_CLAUSE: True,
    CAPABILITY_RANGER: False,
    CAPABILITY_REFRESH_FUNCTIONS: False,
    CAPABILITY_SCHEMA_EVOLUTION: True,
    CAPABILITY_SENTRY: False,
    CAPABILITY_SORTED_TABLE: True,
    CAPABILITY_SORTED_TABLE_MODIFY: False,
    CAPABILITY_SQL_MICROSECOND_PREDICATE: True,
    CAPABILITY_SYNTHETIC_PARTITIONING: False,
    CAPABILITY_TABLE_STATS_COMPUTE: True,
    CAPABILITY_TABLE_STATS_GET: True,
    CAPABILITY_TABLE_STATS_SET: False,
}

MSSQL_FRONTEND_CAPABILITIES = {
    CAPABILITY_CANONICAL_DATE: True,
    CAPABILITY_CANONICAL_FLOAT: True,
    CAPABILITY_CANONICAL_TIME: True,
    # This may actually be actually true but we don't support it
    CAPABILITY_CASE_SENSITIVE: False,
    CAPABILITY_GOE_HAS_DB_CODE_COMPONENT: False,
    CAPABILITY_HYBRID_SCHEMA: False,
    CAPABILITY_GOE_JOIN_PUSHDOWN: False,
    CAPABILITY_GOE_LIST_PARTITION_APPEND: False,
    CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY: False,
    CAPABILITY_GOE_OFFLOAD_STATUS_REPORT: False,
    CAPABILITY_LOW_HIGH_VALUE_FROM_STATS: False,
    CAPABILITY_NAN: True,
    CAPABILITY_PARAMETERIZED_QUERIES: False,
    # Once we support Schema Sync we can enable these attributes.
    CAPABILITY_SCHEMA_EVOLUTION: False,
    CAPABILITY_GOE_SCHEMA_SYNC: False,
}

ORACLE_FRONTEND_CAPABILITIES = {
    CAPABILITY_CANONICAL_DATE: False,
    CAPABILITY_CANONICAL_FLOAT: True,
    CAPABILITY_CANONICAL_TIME: False,
    # This is actually true but we don't support it
    CAPABILITY_CASE_SENSITIVE: False,
    CAPABILITY_GOE_HAS_DB_CODE_COMPONENT: True,
    CAPABILITY_GOE_JOIN_PUSHDOWN: False,
    CAPABILITY_GOE_LIST_PARTITION_APPEND: True,
    CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY: True,
    CAPABILITY_GOE_OFFLOAD_STATUS_REPORT: True,
    CAPABILITY_HYBRID_SCHEMA: False,
    CAPABILITY_LOW_HIGH_VALUE_FROM_STATS: True,
    CAPABILITY_NAN: True,
    CAPABILITY_PARAMETERIZED_QUERIES: True,
    CAPABILITY_SCHEMA_EVOLUTION: True,
    CAPABILITY_GOE_SCHEMA_SYNC: True,
}

TERADATA_FRONTEND_CAPABILITIES = {
    CAPABILITY_CANONICAL_DATE: True,
    CAPABILITY_CANONICAL_FLOAT: True,
    CAPABILITY_CANONICAL_TIME: True,
    CAPABILITY_CASE_SENSITIVE: False,
    CAPABILITY_GOE_HAS_DB_CODE_COMPONENT: False,
    CAPABILITY_GOE_JOIN_PUSHDOWN: False,
    CAPABILITY_GOE_LIST_PARTITION_APPEND: False,
    CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY: False,
    CAPABILITY_GOE_OFFLOAD_STATUS_REPORT: False,
    CAPABILITY_HYBRID_SCHEMA: False,
    CAPABILITY_LOW_HIGH_VALUE_FROM_STATS: False,
    CAPABILITY_NAN: False,
    CAPABILITY_PARAMETERIZED_QUERIES: True,
    # Once we support Schema Sync we can enable these attributes.
    CAPABILITY_SCHEMA_EVOLUTION: False,
    CAPABILITY_GOE_SCHEMA_SYNC: False,
}
