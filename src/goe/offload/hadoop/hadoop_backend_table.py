#! /usr/bin/env python3

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

""" BackendHadoopTable: Library for logic/interaction with a table that will
    be either:
      1) The target of an offload
      2) The source of a present
    This module enforces an interface with common, highlevel, methods shared
    by Impala and Hive implementations.
"""

import logging
import os
from typing import TYPE_CHECKING

from goe.filesystem.goe_dfs import gen_load_uri_from_options
from goe.offload.column_metadata import ColumnMetadataInterface, get_column_names
from goe.offload.hadoop import hadoop_predicate
from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_STRING,
)
from goe.offload.offload_constants import (
    FILE_STORAGE_FORMAT_AVRO,
    OFFLOAD_STATS_METHOD_NATIVE,
    OFFLOAD_STATS_METHOD_HISTORY,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_transport_functions import load_db_hdfs_path, schema_path
from goe.offload.backend_table import (
    BackendTableInterface,
    TYPICAL_DATE_GRANULARITY_TERMS,
)
from goe.offload.staging.staging_file import OffloadStagingFileInterface
from goe.offload.staging.avro.avro_staging_file import (
    AVRO_TYPE_DOUBLE,
    AVRO_TYPE_FLOAT,
    AVRO_TYPE_INT,
    AVRO_TYPE_LONG,
)
from goe.offload.staging.parquet.parquet_column import (
    PARQUET_TYPE_DOUBLE,
    PARQUET_TYPE_FLOAT,
    PARQUET_TYPE_INT32,
    PARQUET_TYPE_INT64,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig


###############################################################################
# CONSTANTS
###############################################################################

GOE_BUCKET_UDF = "GOE_BUCKET"
GOE_BUCKET_UDF_EXPRESSION = "%(udf_db)sGOE_BUCKET(%(dividend)s,%(divisor)s)"

# Used for test assertions
COMPUTE_LOAD_TABLE_STATS_LOG_TEXT = "Config requires compute of stats on load table"

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendHadoopTable
###########################################################################


class BackendHadoopTable(BackendTableInterface):
    """Common Hadoop backend implementation
    Impala and Hive share a lot of functionality, this class holds those
    common methods. Also Hadoop specifics, such as HDFS, should be found
    in this Hadoop class.
    """

    def __init__(
        self,
        db_name: str,
        table_name: str,
        backend_type: str,
        orchestration_options: "OrchestrationConfig",
        messages,
        orchestration_operation=None,
        hybrid_metadata=None,
        dry_run=False,
        existing_backend_api=None,
        do_not_connect=False,
    ):
        """CONSTRUCTOR"""
        assert db_name and table_name

        # Some backends are case sensitive, Hadoop is not and we lower case object names
        self.db_name = db_name.lower()
        self.table_name = table_name.lower()

        super(BackendHadoopTable, self).__init__(
            self.db_name,
            self.table_name,
            backend_type,
            orchestration_options,
            messages,
            orchestration_operation=orchestration_operation,
            hybrid_metadata=hybrid_metadata,
            dry_run=dry_run,
            existing_backend_api=existing_backend_api,
            do_not_connect=do_not_connect,
        )

        self._sql_engine_name = "Hadoop"
        self._default_location = None
        self._delta_db_name = self._load_db_name
        self._delta_table_name = None

        # Hadoop has HDFS
        self._data_db_hdfs_dir = None
        self._load_db_hdfs_dir = None
        self._load_table_hdfs_dir = None
        self._result_cache_db_hdfs_dir = None

        # Pickup some orchestration_operation/offload_options attributes
        if hasattr(orchestration_operation, "hive_column_stats"):
            # Having to do hasattr checking because SchemaSync doesn't have adequate options (and shouldn't really need them)
            self._hive_column_stats_enabled = orchestration_operation.hive_column_stats
            self._hive_max_dynamic_partitions = (
                self._orchestration_config.hive_max_dynamic_partitions
            )
            self._hive_max_dynamic_partitions_pernode = (
                self._orchestration_config.hive_max_dynamic_partitions_pernode
            )
            self._hive_optimize_sort_dynamic_partition = (
                self._orchestration_config.hive_optimize_sort_dynamic_partition
            )
        else:
            self._hive_column_stats_enabled = None
            self._hive_max_dynamic_partitions = None
            self._hive_max_dynamic_partitions_pernode = None
            self._hive_optimize_sort_dynamic_partition = None

        if self._target_owner:
            self._avro_schema_hdfs_path = schema_path(
                self._target_owner,
                self.table_name,
                self._load_db_name,
                self._orchestration_config,
            )
        else:
            self._avro_schema_hdfs_path = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _compute_load_table_statistics(self):
        if not self._user_requested_compute_load_table_stats:
            return
        self._log(COMPUTE_LOAD_TABLE_STATS_LOG_TEXT, detail=VVERBOSE)
        self._db_api.compute_stats(
            self._load_db_name, self._load_table_name, incremental=True
        )

    def _compute_hive_table_statistics(
        self, incremental_stats, materialized_join=False
    ):
        def progress_message(partitions_done, partitions_total):
            perc = float(partitions_done) / partitions_total * 100
            self._log(
                "Partition progress %d%% (%d/%d)"
                % (int(perc), partitions_done, partitions_total),
                detail=VERBOSE,
            )

        def gather_partition_stats(columns, partition):
            return self._db_api.compute_stats(
                self.db_name,
                self.table_name,
                for_columns=columns,
                partition_tuples=partition,
            )

        def partition_str_to_tuples(partition_str):
            part_kvs = partition_str.split(",")
            partition_tuples = []
            for kv in part_kvs:
                k, v = kv.split("=")
                partition_tuples.append((k.strip("'\""), v.strip("'\"")))
            return partition_tuples

        def partitions_from_load_table():
            """Determine the new partitions we created during the INSERT-SELECT by querying the load table"""
            part_exprs = "\n,    ".join(
                "%s AS %s" % (self.get_final_table_cast(_), self.enclose_identifier(_))
                for _ in get_column_names(self.get_partition_columns())
            )

            sql = """SELECT DISTINCT
     %s
FROM %s.%s""" % (
                part_exprs,
                self.enclose_identifier(self._load_db_name),
                self.enclose_identifier(self.table_name),
            )
            partitions = self._execute_query_fetch_all(
                sql, log_level=VERBOSE, not_when_dry_running=True
            )

            process_partitions = []
            for partition in partitions:
                partition_spec = []
                for num, partition_value in enumerate(partition):
                    partition_spec.append(
                        (self.get_partition_columns()[num].name, partition_value)
                    )
                process_partitions.append(partition_spec)

            return process_partitions

        self._log(
            "Stats automatically gathered by Hive for new partitions during insert",
            detail=VVERBOSE,
        )

        """
        Non-IPA offload / Materialized Join
        -----------------------------------
        (HISTORY or NATIVE) and hive_column_stats:
          1. All offload_bucket_id partitions will have already had stats gathered (as we now force hive.stats.autogather=true)
          2. Analyze column stats on all offload_bucket_id partitions (even if there are subpartitions, this will be the quickest method)

        IPA offload
        -----------
        NATIVE and hive_column_stats:
          1. All newly created partitions will have had stats gathered (as we now force hive.stats.autogather=true)
          2. Detect new partitions by analyzing load table and gather column stats on these partitions

        HISTORY:
          1. Scan all partitions for missing partition and column stats and gather on those without partition stats
          2. If hive_column_stats: gather on those partitions missing column stats
        """
        if incremental_stats and not materialized_join:
            if (
                self._offload_stats_method == OFFLOAD_STATS_METHOD_NATIVE
                and self._hive_column_stats_enabled
            ):
                self._log("Gathering column stats for new partitions", detail=VVERBOSE)
                if not self._dry_run:
                    new_partitions = partitions_from_load_table()
                    for index, partition in enumerate(new_partitions):
                        gather_partition_stats(True, partition)
                        progress_message(index + 1, len(new_partitions))
            elif self._offload_stats_method == OFFLOAD_STATS_METHOD_HISTORY:
                self._log("Detecting partitions with no stats", detail=VVERBOSE)
                if not self._dry_run:
                    (
                        tab_stats,
                        part_stats,
                        col_stats,
                    ) = self._db_api.get_missing_hive_table_stats(
                        self.db_name,
                        self.table_name,
                        colstats=self._hive_column_stats_enabled,
                        as_dict=True,
                    )
                    self._log(
                        "Gathering stats on partitions that have none: %s"
                        % str(list(part_stats.keys())),
                        detail=VVERBOSE,
                    )
                    for index, partition_str in enumerate(part_stats.keys()):
                        partition_tuples = partition_str_to_tuples(partition_str)
                        gather_partition_stats(False, partition_tuples)
                        progress_message(index + 1, len(part_stats))
                    if self._hive_column_stats_enabled:
                        self._log(
                            "Gathering stats on columns that have none", detail=VVERBOSE
                        )
                        for index, partition_str in enumerate(col_stats.keys()):
                            partition_tuples = partition_str_to_tuples(partition_str)
                            gather_partition_stats(True, partition_tuples)
                            progress_message(index + 1, len(col_stats))

    def _create_hadoop_load_database(self):
        """Hadoop specific method to create a HDFS path and load database"""
        db_hdfs_dir = self._get_load_db_hdfs_dir()
        self._log_dfs_cmd('mkdir("%s")' % db_hdfs_dir)
        if not self._dry_run:
            self._get_dfs_client().mkdir(db_hdfs_dir)
        self._log_dfs_cmd('chmod("%s", "g+w")' % db_hdfs_dir)
        if not self._dry_run:
            self._get_dfs_client().chmod(db_hdfs_dir, mode="g+w")

        if self.create_database_supported() and self._user_requested_create_backend_db:
            self._create_load_db(location=db_hdfs_dir)

    def _create_hadoop_result_cache_database(self):
        """Hadoop specific method to create a HDFS path and result cache database"""
        db_hdfs_dir = self._get_result_cache_db_hdfs_dir()
        self._log_dfs_cmd('mkdir("%s")' % db_hdfs_dir)
        if not self._dry_run:
            self._get_dfs_client().mkdir(db_hdfs_dir)
        self._log_dfs_cmd('chmod("%s", "g+w")' % db_hdfs_dir)
        if not self._dry_run:
            self._get_dfs_client().chmod(db_hdfs_dir, mode="g+w")

        if self.create_database_supported() and self._user_requested_create_backend_db:
            self._create_result_cache_db(location=db_hdfs_dir)

    def _copy_load_table_avro_schema(self, staging_file):
        avro_schema_str = staging_file.get_file_schema_json()
        self._log("Staging Avro schema: %s" % avro_schema_str, detail=VVERBOSE)
        self._log_dfs_cmd('write("%s")' % self._avro_schema_hdfs_path)
        if not self._dry_run:
            self._get_dfs_client().write(
                self._avro_schema_hdfs_path, data=avro_schema_str, overwrite=True
            )

    def _create_load_table(self, staging_file, with_terminator=False) -> list:
        """Create the staging/load table and supporting HDFS directory"""
        self._recreate_load_table_dir(include_remove=False)
        no_partition_cols = []
        location = gen_load_uri_from_options(
            self._orchestration_config,
            hadoop_db=self._load_db_name,
            table_name=self._load_table_name,
        )
        schema_fs_prefix = "hdfs://" if self._avro_schema_hdfs_path[:1] == "/" else ""
        table_properties = {
            "avro.schema.url": "%s%s" % (schema_fs_prefix, self._avro_schema_hdfs_path)
        }
        return self._db_api.create_table(
            self._load_db_name,
            self._load_table_name,
            self.convert_canonical_columns_to_backend(
                staging_file.get_canonical_staging_columns(use_staging_file_names=True)
            ),
            no_partition_cols,
            storage_format=staging_file.file_format,
            location=location,
            external=True,
            table_properties=table_properties,
            sync=True,
            with_terminator=with_terminator,
        )

    def _create_new_backend_table(self, sort_column_names=None):
        raise NotImplementedError(
            "_create_new_backend_table() is not implemented for common Hadoop class"
        )

    def _drop_load_table(self, sync=None):
        """Drop the staging/load table and any supporting filesystem directory"""
        self._db_api.drop_table(
            self._load_db_name, self._load_table_name, purge=True, sync=sync
        )
        # Use the default load table location (from options) and not that of any existing table,
        # this ensures we clear any problem files before we create a new load table and start offloading
        if self._load_table_hdfs_dir:
            self._recreate_load_table_dir(include_create=False)

    def _final_insert_format_sql(
        self,
        select_expr_tuples,
        filter_clauses,
        sort_expr_list,
        for_materialized_join=False,
    ):
        raise NotImplementedError(
            "_final_insert_format_sql() is not implemented for common Hadoop class"
        )

    def _gen_synthetic_part_number_granularity_sql_expr(
        self,
        column_expr,
        backend_col,
        granularity,
        synthetic_partition_digits,
        with_padding=True,
    ):
        raise NotImplementedError(
            "_gen_synthetic_part_number_granularity_sql_expr() is not implemented for common Hadoop class"
        )

    def _gen_synthetic_partition_column_object(self, synthetic_name, canonical_column):
        """On Hadoop all synthetic columns are STRING"""
        return self._db_api.gen_column_object(
            synthetic_name,
            data_type=HADOOP_TYPE_STRING,
            partition_info=canonical_column.partition_info,
        )

    def _gen_synthetic_partition_date_as_string_sql_expr(
        self, extract_name, pad_size, source_column_cast
    ):
        """Hadoop implementation"""
        return "LPAD(CAST(%s(%s) AS STRING), %s, '0')" % (
            extract_name,
            source_column_cast,
            pad_size,
        )

    def _gen_synthetic_partition_date_truncated_sql_expr(
        self, synthetic_name, column_name, granularity, source_column_cast
    ):
        """Return a SQL expression that truncates a column value to granularity.
        Hadoop override.
        """
        assert column_name
        assert granularity
        assert source_column_cast
        return "TRUNC(%s, '%s')" % (
            source_column_cast,
            TYPICAL_DATE_GRANULARITY_TERMS[granularity],
        )

    def _gen_synthetic_part_string_granularity_sql_expr(self, column_expr, granularity):
        return "SUBSTR(%s,1,%s)" % (column_expr, granularity)

    def _get_data_db_hdfs_dir(self):
        if self._data_db_hdfs_dir is None:
            self._data_db_hdfs_dir = self._get_dfs_client().gen_uri(
                self._orchestration_config.offload_fs_scheme,
                self._orchestration_config.offload_fs_container,
                self._orchestration_config.offload_fs_prefix,
                backend_db=self.db_name,
            )
        return self._data_db_hdfs_dir

    def _get_data_table_hdfs_dir(self):
        return os.path.join(self._get_data_db_hdfs_dir(), self.table_name)

    def _get_load_db_hdfs_dir(self):
        if self._load_db_hdfs_dir is None:
            self._load_db_hdfs_dir = load_db_hdfs_path(
                self._load_db_name, self._orchestration_config
            )
        return self._load_db_hdfs_dir

    def _get_load_table_hdfs_dir(self):
        if self._load_table_hdfs_dir is None:
            self._load_table_hdfs_dir = os.path.join(
                self._get_load_db_hdfs_dir(), self._load_table_name
            )
        return self._load_table_hdfs_dir

    def _get_result_cache_db_hdfs_dir(self):
        if self._result_cache_db_hdfs_dir is None:
            self._result_cache_db_hdfs_dir = load_db_hdfs_path(
                self._result_cache_db_name, self._orchestration_config
            )
        return self._result_cache_db_hdfs_dir

    def _recreate_load_table_dir(self, include_remove=True, include_create=True):
        """Doing this ensure that the staging table directory is owned by the "goe" account
        This is a requirement of some transport methods which run outside of a SQL engine
        """
        load_table_hdfs_dir = self._get_load_table_hdfs_dir()
        if include_remove:
            self._log_dfs_cmd('rmdir("%s")' % load_table_hdfs_dir)
            if not self._dry_run:
                status = self._get_dfs_client().rmdir(
                    load_table_hdfs_dir, recursive=True
                )
                self._messages.log("rmdir status: %s" % status, detail=VVERBOSE)
        if include_create:
            self._log_dfs_cmd('mkdir("%s")' % load_table_hdfs_dir)
            if not self._dry_run:
                self._get_dfs_client().mkdir(load_table_hdfs_dir)
            self._log_dfs_cmd('chmod("%s", "g+w")' % load_table_hdfs_dir)
            if not self._dry_run:
                self._get_dfs_client().chmod(load_table_hdfs_dir, mode="g+w")

    def _refresh_load_table_files(self):
        """Some query engines would not see the newly written load table files because
        they were written with other tools.
        This step gives the engine opportunity to rescan the files.
        """
        self._db_api.refresh_table_files(self._load_db_name, self._load_table_name)

    def _staging_to_backend_cast(
        self, rdbms_column, backend_column, staging_column
    ) -> tuple:
        """Returns correctly cast or overridden columns ready for insert/select to final table.
        rdbms_column: Required because we need to know what data is actually inside the staging column.
        Common Hadoop version.
        """

        def staging_and_backend_types_match(staging_column, backend_column):
            if (
                staging_column.format_data_type().upper()
                == backend_column.format_data_type().upper()
            ):
                self._log(
                    "No CAST() required for %s: %s->%s"
                    % (
                        staging_column.name,
                        staging_column.format_data_type(),
                        backend_column.format_data_type(),
                    ),
                    detail=VVERBOSE,
                )
                return True
            else:
                return False

        def staging_file_int_match(backend_column, staging_column):
            return bool(
                (
                    backend_column.data_type == HADOOP_TYPE_BIGINT
                    and staging_column.data_type in [AVRO_TYPE_LONG, PARQUET_TYPE_INT64]
                )
                or (
                    backend_column.data_type == HADOOP_TYPE_INT
                    and staging_column.data_type in [AVRO_TYPE_INT, PARQUET_TYPE_INT32]
                )
            )

        def staging_file_float_match(backend_column, staging_column):
            return bool(
                (
                    backend_column.data_type == HADOOP_TYPE_FLOAT
                    and staging_column.data_type
                    in [AVRO_TYPE_FLOAT, PARQUET_TYPE_FLOAT]
                )
                or (
                    backend_column.data_type == HADOOP_TYPE_DOUBLE
                    and staging_column.data_type
                    in [AVRO_TYPE_DOUBLE, PARQUET_TYPE_DOUBLE]
                )
            )

        assert backend_column
        assert isinstance(backend_column, ColumnMetadataInterface)
        assert rdbms_column, (
            "RDBMS column missing for backend column: %s" % backend_column.name
        )
        assert isinstance(rdbms_column, ColumnMetadataInterface)
        assert staging_column
        assert isinstance(staging_column, ColumnMetadataInterface)

        return_cast = None
        return_type = backend_column.format_data_type().upper()

        if backend_column.is_number_based():
            if staging_file_int_match(
                backend_column, staging_column
            ) or staging_file_float_match(backend_column, staging_column):
                # Same data type so no need to CAST
                self._log(
                    "No cast required for matching data types: %s/%s"
                    % (staging_column.data_type, backend_column.data_type),
                    detail=VVERBOSE,
                )
            else:
                return_cast = "CAST(%s AS %s)" % (
                    self._format_staging_column_name(staging_column),
                    backend_column.format_data_type().upper(),
                )
        elif rdbms_column.is_time_zone_based():
            if not backend_column.is_string_based():
                # normalise timestamps with time zones to UTC
                # if the time zone is of format [+-]nn:nn then we extract hours and mins and subtract from the timestamp
                # if the time zone is anything else then we assume it is a valid timzone (timezone_db.cc) and pass to to_utc_timestamp()
                timestamp_conversion = self._tzoffset_to_timestamp_sql_expression(
                    staging_column.staging_file_column_name
                )
                return_cast = """CASE WHEN %(tstz)s REGEXP '.*[+-][01]?[0-9]:[0-9][05]$' THEN
                        %(timestamp_conversion)s
                    ELSE
                        TO_UTC_TIMESTAMP(CAST(REGEXP_EXTRACT(%(tstz)s,'([^ ]+ [^ ]+) (.+)',1) AS TIMESTAMP),REGEXP_EXTRACT(%(tstz)s,'([^ ]+ [^ ]+) (.+)',2))
                    END""" % {
                    "tstz": self._format_staging_column_name(staging_column),
                    "timestamp_conversion": timestamp_conversion,
                }
        elif rdbms_column.is_date_based():
            if backend_column.is_string_based():
                # We need a string containing the full timestamp spec including nanoseconds and 4 digit year.
                # Offload transport (e.g. Sqoop) may leave some values with ".0" time fraction, therefore need to pad.
                # Offload transport (looking at your Sqoop) qoop doesn't pad years to 4 digits,
                # e.g. "1-01-01 00:00:00.000" so we # cannot rely on fixed position of fractional part.
                return_cast = """CASE
                WHEN %(col)s IS NULL THEN
                    CAST(%(col)s AS STRING)
                WHEN INSTR(CAST(%(col)s AS STRING),'.') > 0 THEN
                    LPAD(CONCAT(CAST(%(col)s AS STRING),RPAD('0',10-LENGTH(REGEXP_EXTRACT(%(col)s,'([^ ]+ [0-9:]+)(\\.[0-9]*)?',2)),'0')),29,'0')
                ELSE
                    LPAD(CONCAT(CAST(%(col)s AS STRING),'.000000000'),29,'0')
                END""" % {
                    "col": self._format_staging_column_name(staging_column)
                }
            elif not staging_and_backend_types_match(staging_column, backend_column):
                if backend_column.data_type == HADOOP_TYPE_DATE:
                    return_cast = "TO_DATE(%s)" % (
                        self._format_staging_column_name(staging_column)
                    )
                else:
                    return_cast = "CAST(%s AS %s)" % (
                        self._format_staging_column_name(staging_column),
                        return_type,
                    )

        # There is no concept of safe cast on Hadoop so return same cast twice
        if return_cast:
            return return_cast, return_type, return_cast
        else:
            return_cast = self._format_staging_column_name(staging_column)
            return return_cast, None, return_cast

    def _synthetic_bucket_filter_non_udf_sql_expression(self):
        raise NotImplementedError(
            "_synthetic_bucket_filter_non_udf_sql_expression() is not implemented for common Hadoop class"
        )

    def _tzoffset_to_timestamp_sql_expression(self, col_name):
        raise NotImplementedError(
            "_tzoffset_to_timestamp_sql_expression() is not implemented for common Hadoop class"
        )

    ###########################################################################
    # PUBLIC METHODS - HIGH LEVEL STEP METHODS
    ###########################################################################

    def cleanup_staging_area(self):
        self._drop_load_table(sync=True)

    def create_backend_table(self, with_terminator=False) -> list:
        """Create a table in the backend based on object state.
        Creating a new table may change our world view so the function drops state if in execute mode.
        If dry_run then we leave state in place to allow other operations to preview.
        """
        cmds = self._create_new_backend_table(
            sort_column_names=self._sort_columns, with_terminator=with_terminator
        )
        if not self._dry_run:
            # The CREATE TABLE above may have changed our world view so let's reset what we already know
            self._drop_state()
        return cmds

    def empty_staging_area(self, staging_file):
        self._recreate_load_table(staging_file)

    def incremental_merge_final_table(
        self, extraction_key_columns, staging_columns, sync=None
    ):
        raise NotImplementedError(
            "incremental_merge_final_table() is not implemented for common Hadoop class"
        )

    def load_final_table(self, sync=None):
        """Copy data from the staged load table into the final backend table
        If this fails with ERROR_STATE then we offer up some SQL to assist the user
        """
        self._debug(
            "Loading %s.%s from %s.%s"
            % (
                self.db_name,
                self.table_name,
                self._load_db_name,
                self._load_table_name,
            )
        )

        select_expression_tuples = [
            (self.get_final_table_cast(col), col.name.upper())
            for col in self.get_non_synthetic_columns()
        ]
        # Any SORT BY should be of the correct data type
        sort_expressions = [
            self.get_final_table_cast(col_name)
            for col_name in (self._sort_columns or [])
        ]

        sqls, query_options = self._gen_final_insert_sqls(
            select_expression_tuples, sort_expr_list=sort_expressions
        )

        try:
            self._execute_dml(
                sqls,
                query_options=query_options,
                sync=sync,
                profile=self._log_profile_after_final_table_load,
            )
        except Exception as exc:
            if "ERROR_STATE" in str(exc):
                partition_expressions = ", ".join(
                    self.get_final_table_cast(_)
                    for _ in get_column_names(self.get_partition_columns())
                )
                suggest_sql = """SELECT COUNT(*)
        FROM (
            SELECT DISTINCT %(part_expr)s
            FROM %(db)s.%(table)s
        ) v""" % {
                    "part_expr": partition_expressions,
                    "db": self._load_db_name,
                    "table": self._load_table_name,
                }
                self._log(
                    "If memory pressure is the cause of INSERT failure then the number of partitions being created may be a factor."
                )
                self._log(
                    "The SQL below will identify the number of partitions the failing INSERT attempted to create:\n%s"
                    % suggest_sql
                )
            raise

    def load_materialized_join(
        self,
        threshold_cols=None,
        gte_threshold_vals=None,
        lt_threshold_vals=None,
        insert_predicates=None,
        sync=None,
    ):
        """Copy data from the load view into the final backend table"""
        if insert_predicates:
            assert isinstance(insert_predicates, list)
        self._debug(
            "Loading %s.%s from %s.%s"
            % (
                self.db_name,
                self.table_name,
                self._load_db_name,
                self._load_table_name,
            )
        )
        select_expression_tuples = [
            (self.get_final_table_cast(col), col.name.upper())
            for col in self.get_non_synthetic_columns()
        ]
        sqls, query_options = self._gen_mat_join_insert_sqls(
            select_expression_tuples,
            threshold_cols,
            gte_threshold_vals,
            lt_threshold_vals,
            insert_predicates=insert_predicates,
            sort_expr_list=self._sort_columns or [],
        )
        self._execute_dml(
            sqls,
            query_options=query_options,
            sync=sync,
            profile=self._log_profile_after_final_table_load,
        )

    def post_transport_tasks(self, staging_file):
        """Tasks required to ensure we are ready to process the staged data.
        staging_file ignored in Hadoop.
        """
        self._refresh_load_table_files()
        if self.table_stats_compute_supported():
            self._compute_load_table_statistics()

    def setup_result_cache_area(self):
        """Prepare result cache area for Hybrid Queries"""
        self._create_hadoop_load_database()

    def setup_staging_area(self, staging_file):
        """Prepare any staging area for Hadoop OffloadTransport"""
        if staging_file:
            assert isinstance(staging_file, OffloadStagingFileInterface)
        self._create_hadoop_load_database()
        if not staging_file:
            # no table specific actions required
            return
        if staging_file.file_format == FILE_STORAGE_FORMAT_AVRO:
            self._copy_load_table_avro_schema(staging_file)
        self._recreate_load_table(staging_file)

    def validate_type_conversions(self, staging_columns: list):
        self._validate_final_table_casts(staging_columns)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def create_db(self, with_terminator=False) -> list:
        return self._create_final_db(
            location=self._get_data_db_hdfs_dir(),
            with_terminator=with_terminator,
        )

    def create_load_db(self, with_terminator=False) -> list:
        """Create a load database."""
        if self._db_api.load_db_transport_supported():
            return self._create_load_db(
                location=self._get_load_db_hdfs_dir(),
                with_terminator=with_terminator,
            )
        else:
            return []

    def default_udf_db_name(self):
        """By default we support UDF_DB but on Hadoop we use 'default' as a fall back"""
        return self._udf_db or "default"

    def get_default_location(self):
        if self._default_location is None:
            self._default_location = self._db_api.get_table_location(
                self.db_name, self.table_name
            )
        return self._default_location

    def get_staging_table_location(self):
        return self._get_load_table_hdfs_dir()

    def partition_function_requires_granularity(self):
        return False

    def result_cache_area_exists(self):
        return self._result_cache_db_exists()

    def staging_area_exists(self):
        if not self._load_db_exists():
            self._warning("Staging database %s does not exist" % self._load_db_name)
        return self._load_db_exists()

    def synthetic_bucket_data_type(self):
        return HADOOP_TYPE_SMALLINT

    def synthetic_bucket_filter_capable_column(self, backend_column):
        assert backend_column
        assert isinstance(backend_column, ColumnMetadataInterface)
        if (
            self.goe_udfs_supported()
            and backend_column.is_number_based()
            and (
                backend_column.data_type in self._db_api.native_integer_types()
                or backend_column.data_scale == 0
            )
        ):
            return GOE_BUCKET_UDF, GOE_BUCKET_UDF_EXPRESSION
        else:
            return None, self._synthetic_bucket_filter_non_udf_sql_expression()

    def predicate_has_rows(self, predicate):
        if not self.exists():
            return False

        sql = "SELECT 1 FROM %s WHERE (%s) LIMIT 1" % (
            self._db_api.enclose_object_reference(self.db_name, self.table_name),
            hadoop_predicate.predicate_to_where_clause(self.get_columns(), predicate),
        )
        return bool(
            self._execute_query_fetch_one(
                sql, log_level=VERBOSE, not_when_dry_running=False
            )
        )

    def predicate_to_where_clause(self, predicate, columns_override=None):
        return hadoop_predicate.predicate_to_where_clause(
            columns_override or self.get_columns(), predicate
        )
