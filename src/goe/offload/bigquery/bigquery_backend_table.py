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

""" BackendBigqueryTable: Library for logic/interaction with a table that will
    be either:
      1) The target of an offload
      2) The source of a present
"""

import logging
from typing import TYPE_CHECKING

from google.cloud import bigquery

from goe.offload.bigquery.bigquery_column import (
    BigQueryColumn,
    BIGQUERY_TYPE_DATE,
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_FLOAT64,
    BIGQUERY_TYPE_INT64,
    BIGQUERY_TYPE_NUMERIC,
    BIGQUERY_TYPE_BIGNUMERIC,
    BIGQUERY_TYPE_TIME,
)
from goe.offload.bigquery import bigquery_predicate
from goe.offload.column_metadata import ColumnMetadataInterface, get_column_names
from goe.offload.backend_table import (
    BackendTableInterface,
    PARTITION_KEY_OUT_OF_RANGE,
    TYPICAL_DATE_GRANULARITY_TERMS,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.staging.avro.avro_staging_file import AVRO_TYPE_DOUBLE, AVRO_TYPE_LONG
from goe.offload.staging.parquet.parquet_column import (
    PARQUET_TYPE_DOUBLE,
    PARQUET_TYPE_INT64,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendBigQueryTable
###########################################################################


class BackendBigQueryTable(BackendTableInterface):
    """BigQuery backend implementation"""

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
        super(BackendBigQueryTable, self).__init__(
            db_name,
            table_name,
            backend_type,
            orchestration_options,
            messages,
            orchestration_operation=orchestration_operation,
            hybrid_metadata=hybrid_metadata,
            dry_run=dry_run,
            existing_backend_api=existing_backend_api,
            do_not_connect=do_not_connect,
        )

        self._sql_engine_name = "BigQuery"
        self._log_profile_after_final_table_load = True
        self._log_profile_after_verification_queries = True
        self._load_table_path = self._get_dfs_client().gen_uri(
            self._orchestration_config.offload_fs_scheme,
            self._orchestration_config.offload_fs_container,
            self._orchestration_config.offload_fs_prefix,
            backend_db=self._load_db_name,
            table_name=self._load_table_name,
        )
        self._kms_key_name = self._db_api.kms_key_name()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _create_load_table(self, staging_file, with_terminator=False) -> list:
        """Create the staging/load table in BigQuery
        Defining a URI for location is tricky. Initially we had *.avro but Avro files written by Spark ThriftServer
        didn't match. Restrictions on the URI:
            For Google Cloud Storage URIs: Each URI can contain one '*' wildcard character and
            it must come after the 'bucket' name.

        Examples of Avro files we have to deal with:
        Query Import:
            Staged path: gs://bucket/.../part-m-00000.avro
        spark-submit:
            Staged path: gs://bucket/.../_SUCCESS
            Staged path: gs://bucket/.../part-00000-009774b7-f994-45c9-870f-a346aaa5e59c-c000.avro
        Spark ThriftServer:
            Staged path: gs://bucket/.../part-00000-1267a519-b931-445a-beed-2ee32f4c4c00-c000

        We can't control file name for Spark ThriftServer:
            The files that are written by the Hive job are valid Avro files, however, MapReduce doesn't add the
            standard .avro extension. If you copy these files out, you'll likely want to rename them with .avro.

        We've gone with part* to identify Avro files.
        """
        no_columns = no_partition_cols = []
        load_table_location = "%s/part*" % (self.get_staging_table_location())
        return self._db_api.create_table(
            self._load_db_name,
            self._load_table_name,
            no_columns,
            no_partition_cols,
            storage_format=staging_file.file_format,
            location=load_table_location,
            external=True,
            with_terminator=with_terminator,
        )

    def _drop_load_table(self, sync=None):
        """Drop the staging/load table."""
        self._db_api.drop_table(
            self._load_db_name, self._load_table_name, purge=True, sync=sync
        )

    def _final_insert_format_sql(
        self,
        select_expr_tuples,
        filter_clauses,
        sort_expr_list,
        for_materialized_join=False,
    ):
        """Format INSERT/SELECT for BigQuery."""
        assert select_expr_tuples
        assert isinstance(select_expr_tuples, list)
        if filter_clauses:
            assert isinstance(filter_clauses, list)

        return self._db_api.gen_insert_select_sql_text(
            self.db_name,
            self.table_name,
            self._load_db_name,
            self._load_table_name,
            select_expr_tuples=select_expr_tuples,
            filter_clauses=filter_clauses,
        )

    def _final_insert_query_options(self):
        if self._kms_key_name:
            return {
                "destination_encryption_configuration": bigquery.EncryptionConfiguration(
                    kms_key_name=self._kms_key_name
                )
            }
        else:
            return None

    def _gen_synthetic_partition_column_object(self, synthetic_name, canonical_column):
        """Return BigQuery column object for synthetic partition column"""
        if canonical_column.is_date_based():
            return self._db_api.gen_column_object(
                synthetic_name,
                data_type=BIGQUERY_TYPE_DATE,
                partition_info=canonical_column.partition_info,
            )
        elif canonical_column.is_number_based():
            return self._db_api.gen_column_object(
                synthetic_name,
                data_type=BIGQUERY_TYPE_INT64,
                partition_info=canonical_column.partition_info,
            )
        else:
            raise NotImplementedError(
                "BigQuery synthetic partitioning not implemented for column type: %s"
                % canonical_column.data_type
            )

    def _gen_synthetic_partition_date_as_string_sql_expr(
        self, extract_name, pad_size, source_column_cast
    ):
        """BigQuery implementation
        Dates are strings in the load table so we work from strings in here.
        """
        return "LPAD(CAST(EXTRACT(%s FROM %s) AS STRING), %s, '0')" % (
            extract_name,
            source_column_cast,
            pad_size,
        )

    def _gen_synthetic_partition_date_truncated_sql_expr(
        self, synthetic_name, column_name, granularity, source_column_cast
    ):
        """Return a SQL expression that truncates a column value to granularity.
        On BigQuery a synthetic column is required for DATETIME. The synthetic column itself is a DATE containing
        the source column data truncated to the granularity.
        Date based columns are strings in the load table so we work from strings in here.
        BigQuery override.
        """
        assert column_name
        assert granularity
        assert source_column_cast
        backend_column = self.get_column(column_name)
        synthetic_column = self.get_column(synthetic_name)
        sql_trunc = "SAFE.%s_TRUNC(%s, %s)" % (
            backend_column.data_type,
            source_column_cast,
            TYPICAL_DATE_GRANULARITY_TERMS[granularity],
        )
        if backend_column.data_type != synthetic_column.data_type:
            sql_trunc = "CAST(%s AS %s)" % (sql_trunc, synthetic_column.data_type)
        return sql_trunc

    def _gen_synthetic_part_number_granularity_sql_expr(
        self,
        column_expr,
        backend_col,
        granularity,
        synthetic_partition_digits,
        with_padding=True,
    ):
        """We use INT64 synthetic values on BigQuery, synthetic_partition_digits is ignored"""
        to_synth_expr = column_expr
        if backend_col.data_type == BIGQUERY_TYPE_INT64:
            # There are a number of values around 15-18 digits that round weirdly when using INT64, for example:
            # > SELECT FLOOR(CAST(9999999999999999 AS INT64));
            # 1.0E16
            # Therefore we use NUMERIC for intermediate arithmetic instead
            to_synth_expr = f"CAST({to_synth_expr} AS {BIGQUERY_TYPE_NUMERIC})"
        elif backend_col.data_type == BIGQUERY_TYPE_NUMERIC:
            # NUMERIC only supports 9 decimal places so any value containing many zeros or
            # a power of 10 granularity > 9 digits poses a threat. Safe option is to use BIGNUMERIC.
            to_synth_expr = f"CAST({to_synth_expr} AS {BIGQUERY_TYPE_BIGNUMERIC})"
        to_synth_expr = "CAST(FLOOR(%s / %s) * %s AS %s)" % (
            to_synth_expr,
            granularity,
            granularity,
            BIGQUERY_TYPE_INT64,
        )
        return to_synth_expr

    def _gen_synthetic_part_string_granularity_sql_expr(self, column_expr, granularity):
        return "SUBSTR(%s,1,%s)" % (column_expr, granularity)

    def _partition_key_out_of_range_message(self, column):
        """Return a BigQuery specific warning that data outside the partition range will use _UNPARTITIONED_ partition."""
        base_message = PARTITION_KEY_OUT_OF_RANGE.format(
            column=column.name.upper(),
            start=column.partition_info.range_start,
            end=column.partition_info.range_end,
        )
        return (
            base_message
            + ", this data will be stored in the __UNPARTITIONED__ partition"
        )

    def _rm_load_table_location(self):
        self._rm_dfs_dir(self.get_staging_table_location())

    def _staging_to_backend_cast(
        self, rdbms_column, backend_column, staging_column
    ) -> tuple:
        """Returns correctly cast or overridden columns ready for insert/select to final table.
        BigQuery implementation.
        """

        def cast(expr, data_type, safe_cast=False):
            if safe_cast:
                return "SAFE_CAST(%s AS %s)" % (expr, data_type)
            else:
                return "CAST(%s AS %s)" % (expr, data_type)

        def parse_date(expr, safe_cast=False):
            if safe_cast:
                return "SAFE.PARSE_DATE('%F', {})".format(expr)
            else:
                return "PARSE_DATE('%F', {})".format(expr)

        def parse_datetime(expr, safe_cast=False):
            if safe_cast:
                return "SAFE.PARSE_DATETIME('%F %H:%M:%E*S', {})".format(expr)
            else:
                return "PARSE_DATETIME('%F %H:%M:%E*S', {})".format(expr)

        def parse_time(expr, safe_cast=False):
            if safe_cast:
                return "SAFE.PARSE_TIME('%H:%M:%E*S', {})".format(expr)
            else:
                return "PARSE_TIME('%H:%M:%E*S', {})".format(expr)

        def parse_timestamp(expr, safe_cast=False):
            # PARSE_TIMESTAMP() understands time zone names and numeric formats ([+-]nn:nn) so no complex logic required
            if safe_cast:
                return "SAFE.PARSE_TIMESTAMP('%F %H:%M:%E*S %Z', {})".format(expr)
            else:
                return "PARSE_TIMESTAMP('%F %H:%M:%E*S %Z', {})".format(expr)

        def date_to_string_format(expr, has_time_element):
            if has_time_element:
                # We need a string containing the full timestamp spec including microseconds and 4 digit year.
                return "FORMAT_DATETIME('%E4Y-%m-%d %H:%M:%E6S', PARSE_DATETIME('%F %H:%M:%E*S', {col}))".format(
                    col=expr
                )
            else:
                return "FORMAT_DATE('%E4Y-%m-%d', PARSE_DATE('%F', {col}))".format(
                    col=expr
                )

        def staging_file_int64_match(backend_column, staging_column):
            return bool(
                backend_column.data_type == BIGQUERY_TYPE_INT64
                and staging_column.data_type in [AVRO_TYPE_LONG, PARQUET_TYPE_INT64]
            )

        def staging_file_float64_match(backend_column, staging_column):
            return bool(
                backend_column.data_type == BIGQUERY_TYPE_FLOAT64
                and staging_column.data_type in [AVRO_TYPE_DOUBLE, PARQUET_TYPE_DOUBLE]
            )

        assert backend_column
        assert isinstance(backend_column, BigQueryColumn)
        assert rdbms_column, (
            "RDBMS column missing for backend column: %s" % backend_column.name
        )
        assert isinstance(rdbms_column, ColumnMetadataInterface)
        assert staging_column
        assert isinstance(staging_column, ColumnMetadataInterface)

        return_cast = None
        return_vcast = None
        return_type = backend_column.format_data_type().upper()

        if (
            isinstance(staging_column, BigQueryColumn)
            and staging_column.data_type == backend_column.data_type
        ):
            # Same column type and data type so no need to CAST, this is only possible when materializing a join
            self._log(
                "No cast of %s required for matching load data type: %s"
                % (backend_column.name, staging_column.data_type),
                detail=VVERBOSE,
            )
        elif backend_column.is_number_based():
            if staging_file_int64_match(
                backend_column, staging_column
            ) or staging_file_float64_match(backend_column, staging_column):
                # Same data type so no need to CAST
                self._log(
                    "No cast required for matching data types: %s/%s"
                    % (staging_column.data_type, backend_column.data_type),
                    detail=VVERBOSE,
                )
            else:
                return_cast = cast(
                    self._format_staging_column_name(staging_column),
                    backend_column.format_data_type().upper(),
                )
                return_vcast = cast(
                    self._format_staging_column_name(staging_column),
                    backend_column.format_data_type().upper(),
                    safe_cast=True,
                )
        elif backend_column.data_type == BIGQUERY_TYPE_TIME:
            return_cast = parse_time(self._format_staging_column_name(staging_column))
            return_vcast = parse_time(
                self._format_staging_column_name(staging_column), safe_cast=True
            )
        elif rdbms_column.is_time_zone_based():
            if not backend_column.is_string_based():
                # BigQuery TIMESTAMP is time zoned so we do not normalise to UTC.
                return_cast = parse_timestamp(
                    self._format_staging_column_name(staging_column)
                )
                return_vcast = parse_timestamp(
                    self._format_staging_column_name(staging_column), safe_cast=True
                )
                if not backend_column.is_time_zone_based():
                    # Cast the time zoned data to the type of the backend column.
                    return_cast = cast(return_cast, backend_column.data_type)
                    return_vcast = cast(
                        return_cast, backend_column.data_type, safe_cast=True
                    )
        elif rdbms_column.is_date_based():
            if backend_column.is_string_based():
                return_cast = """CASE
                WHEN {col} IS NULL THEN
                    CAST({col} AS STRING)
                ELSE
                    {date_format}
                END""".format(
                    col=self._format_staging_column_name(staging_column),
                    date_format=date_to_string_format(
                        self._format_staging_column_name(staging_column),
                        rdbms_column.has_time_element(),
                    ),
                )
                return_vcast = return_cast
            else:
                if rdbms_column.has_time_element():
                    return_cast = parse_datetime(
                        self._format_staging_column_name(staging_column)
                    )
                    return_vcast = parse_datetime(
                        self._format_staging_column_name(staging_column), safe_cast=True
                    )
                    no_cast_type = BIGQUERY_TYPE_DATETIME
                else:
                    return_cast = parse_date(
                        self._format_staging_column_name(staging_column)
                    )
                    return_vcast = parse_date(
                        self._format_staging_column_name(staging_column), safe_cast=True
                    )
                    no_cast_type = BIGQUERY_TYPE_DATE
                if backend_column.data_type != no_cast_type:
                    return_cast = cast(return_cast, backend_column.data_type)
                    return_vcast = cast(return_vcast, backend_column.data_type)

        if return_cast:
            return return_cast, return_type, return_vcast
        else:
            return_cast = self._format_staging_column_name(staging_column)
            return return_cast, None, return_cast

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def cleanup_staging_area(self):
        self._drop_load_table()
        self._rm_load_table_location()

    def compute_final_table_stats(self, incremental_stats, materialized_join=False):
        """Do nothing on BigQuery"""
        pass

    def create_backend_table(self, with_terminator=False) -> list:
        """Create a table in BigQuery based on object state.
        Creating a new table may change our world view so the function drops state if in execute mode.
        If dry_run then we leave state in place to allow other operations to preview.
        """
        partition_column_names = get_column_names(self.get_partition_columns())
        cmds = self._db_api.create_table(
            self.db_name,
            self.table_name,
            self.get_columns(),
            partition_column_names,
            sort_column_names=self._sort_columns,
            with_terminator=with_terminator,
        )
        if not self._dry_run:
            self._drop_state()
        return cmds

    def create_db(self, with_terminator=False) -> list:
        return self._create_final_db(
            location=self._orchestration_config.bigquery_dataset_location,
            with_terminator=with_terminator,
        )

    def create_load_db(self, with_terminator=False) -> list:
        """Create a load database."""
        if self._db_api.load_db_transport_supported():
            return self._create_load_db(
                location=self._orchestration_config.bigquery_dataset_location,
                with_terminator=with_terminator,
            )
        else:
            return []

    def default_udf_db_name(self):
        """By default we support UDF_DB but on BigQuery we use the data db as a fall back"""
        return self._udf_db or self.db_name

    def empty_staging_area(self, staging_file):
        self._rm_load_table_location()

    def get_default_location(self):
        """Not applicable to BigQuery"""
        return None

    def get_staging_table_location(self):
        # Use cached location to avoid re-doing same thing multiple times
        return self._load_table_path

    def load_final_table(self, sync=None):
        """Copy data from the staged load table into the final BigQuery table"""
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
            for col in self.get_columns()
        ]
        sqls, query_options = self._gen_final_insert_sqls(select_expression_tuples)
        self._execute_dml(
            sqls,
            query_options=query_options,
            profile=self._log_profile_after_final_table_load,
        )

    def load_materialized_join(
        self,
        threshold_cols=None,
        gte_threshold_vals=None,
        lt_threshold_vals=None,
        insert_predicates=None,
        sync=None,
    ):
        """Copy data from the load view into the final BigQuery table"""
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
            for col in self.get_columns()
        ]
        sqls, query_options = self._gen_mat_join_insert_sqls(
            select_expression_tuples,
            threshold_cols,
            gte_threshold_vals,
            lt_threshold_vals,
            insert_predicates=insert_predicates,
        )
        self._execute_dml(
            sqls,
            query_options=query_options,
            profile=self._log_profile_after_final_table_load,
        )

    def partition_function_requires_granularity(self):
        return True

    def post_transport_tasks(self, staging_file):
        """On BigQuery we create the load table AFTER creating the Avro datafiles."""
        self._recreate_load_table(staging_file)

    def predicate_has_rows(self, predicate):
        if not self.exists():
            return False

        sql = "SELECT 1 FROM %s WHERE (%s) LIMIT 1" % (
            self._db_api.enclose_object_reference(self.db_name, self.table_name),
            bigquery_predicate.predicate_to_where_clause(self.get_columns(), predicate),
        )
        return bool(
            self._execute_query_fetch_one(
                sql, log_level=VERBOSE, not_when_dry_running=False
            )
        )

    def predicate_to_where_clause(self, predicate, columns_override=None):
        return bigquery_predicate.predicate_to_where_clause(
            columns_override or self.get_columns(), predicate
        )

    def result_cache_area_exists(self):
        return self._result_cache_db_exists()

    def setup_result_cache_area(self):
        """Prepare result cache area for Hybrid Queries"""
        if self.create_database_supported() and self._user_requested_create_backend_db:
            self._create_result_cache_db(
                location=self._orchestration_config.bigquery_dataset_location
            )

    def setup_staging_area(self, staging_file):
        """Prepare any staging area for BigQuery OffloadTransport"""
        if self.create_database_supported() and self._user_requested_create_backend_db:
            self._create_load_db(
                location=self._orchestration_config.bigquery_dataset_location
            )
        self._rm_load_table_location()
        self.get_staging_table_location()

    def staging_area_exists(self):
        if not self._load_db_exists():
            self._warning("Staging database %s does not exist" % self._load_db_name)
        return self._load_db_exists()

    def synthetic_bucket_data_type(self):
        raise NotImplementedError("Synthetic bucketing is not supported on BigQuery")

    def synthetic_bucket_filter_capable_column(self, backend_column):
        raise NotImplementedError("Synthetic bucketing is not supported on BigQuery")

    def validate_type_conversions(self, staging_columns: list):
        """Validate the staged data before we insert it into the final backend table.
        BigQuery catches overflowing casts but this way we can spot them beforehand and
        provide the user with SQL to view offending values.
        """
        self._validate_final_table_casts(
            staging_columns, log_profile=self._log_profile_after_verification_queries
        )
