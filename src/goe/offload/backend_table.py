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

""" BackendTable: Library for logic/interaction with a table that will
    be either:
      1) The target of an offload
      2) The source of a present
    This module enforces an interface with common, high level, methods for implementation
    by each supported backend system, e.g. Impala, Hive, Google BigQuery
"""

from abc import ABCMeta, abstractmethod
import collections
import inspect
import logging
from typing import Callable, Optional, TYPE_CHECKING

from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.offload.backend_api import VALID_REMOTE_DB_TYPES
from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    ColumnPartitionInfo,
    get_column_names,
    get_partition_columns,
    invalid_column_list_message,
    match_table_column,
    regex_real_column_from_part_column,
    str_list_of_columns,
    valid_column_list,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
)
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.offload_constants import (
    DBTYPE_IMPALA,
    INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    PART_COL_DATE_GRANULARITIES,
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
    PART_COL_GRANULARITY_YEAR,
)
from goe.offload.offload_functions import (
    get_hybrid_threshold_clauses,
    hvs_to_backend_sql_literals,
    load_db_name,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.synthetic_partition_literal import SyntheticPartitionLiteral
from goe.orchestration import command_steps
from goe.offload.hadoop.hadoop_column import HADOOP_TYPE_STRING
from goe.util.misc_functions import csv_split

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig


class BackendTableException(Exception):
    pass


class DataValidationException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# These values crop up for numerous uses in various backends, such as for TRUNC and EXTRACT SQL functions.
TYPICAL_DATE_GRANULARITY_TERMS = {
    PART_COL_GRANULARITY_YEAR: "YEAR",
    PART_COL_GRANULARITY_MONTH: "MONTH",
    PART_COL_GRANULARITY_DAY: "DAY",
}

# Used for test assertions
BACKEND_DB_COMMENT_TEMPLATE = "{db_name_type} {db_name_label} for GOE"
CAST_VALIDATION_EXCEPTION_TEXT = "Data type conversion issue in load data"
DATA_VALIDATION_SCALE_EXCEPTION_TEXT = (
    "Include --allow-decimal-scale-rounding option to proceed with the offload"
)
DATA_VALIDATION_NOT_NULL_EXCEPTION_TEXT = "NOT NULL column has NULL values"
NULL_BACKEND_PARTITION_VALUE_WARNING_TEXT = "Backend partition column has NULL values"
OFFLOAD_CHUNK_COLUMN_MESSAGE_PATTERN = "Identified %s sub-chunks"
PARTITION_KEY_OUT_OF_RANGE = (
    "Value(s) for {column} exceed configured range ({start} - {end})"
)
PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT = "must only have 1 input parameter"
PARTITION_FUNCTION_DOES_NOT_EXIST_EXCEPTION_TEXT = "Partition function does not exist"
PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT = (
    "Partition function parameter type does not match data type"
)


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendTableInterface
###########################################################################


class BackendTableInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for backend specific sub-classes"""

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
        assert db_name and table_name
        assert orchestration_options
        assert backend_type in VALID_REMOTE_DB_TYPES, "%s not in %s" % (
            backend_type,
            VALID_REMOTE_DB_TYPES,
        )

        # Cache parameters in state
        self.db_name = db_name
        self.table_name = table_name
        self._backend_type = backend_type
        self._messages = messages
        self._hybrid_metadata = hybrid_metadata
        self._dry_run = dry_run
        self._do_not_connect = do_not_connect
        # One day we may split connectivity options from other config
        self._connection_options = orchestration_options
        self._orchestration_config = orchestration_options

        self._sql_engine_name = "Backend"
        self.table_name = self.table_name
        if existing_backend_api:
            self._db_api = existing_backend_api
        else:
            self._db_api = backend_api_factory(
                backend_type,
                self._connection_options,
                self._messages,
                dry_run=dry_run,
                do_not_connect=do_not_connect,
            )
        self._dfs_client = None
        self._backend_dfs = None

        if len(self.db_name) > self.max_db_name_length():
            raise BackendTableException(
                "Database name %s is too long for %s: %s > %s"
                % (
                    self.db_name,
                    self._db_api.backend_db_name(),
                    len(self.db_name),
                    self.max_db_name_length(),
                )
            )

        # Details relating to a load db
        self._load_db_name = load_db_name(self.db_name)
        if (
            self._db_api.load_db_transport_supported()
            and len(self._load_db_name) > self.max_db_name_length()
        ):
            raise BackendTableException(
                "Load database name %s is too long for %s: %s > %s"
                % (
                    self._load_db_name,
                    self._db_api.backend_db_name(),
                    len(self._load_db_name),
                    self.max_db_name_length(),
                )
            )
        self._load_table_name = self.table_name
        self._result_cache_db_name = self._load_db_name
        self._load_view_db_name = self._load_db_name

        # Cache some attributes in state
        self._columns = None
        self._partition_columns = None
        self._has_rows = None
        self._log_profile_after_final_table_load = None
        self._log_profile_after_verification_queries = None

        # Pickup some orchestration_operation/offload_options attributes
        self._offload_staging_format = getattr(
            self._orchestration_config, "offload_staging_format", None
        )
        self._udf_db = getattr(self._orchestration_config, "udf_db", None)
        # If orchestration_operation is not set then we are not doing anything significant by way of offload/present
        self._ipa_predicate_type = None
        self._offload_distribute_enabled = None
        self._offload_stats_method = None
        self._partition_functions = None
        self._sort_columns = None
        self._final_table_casts = {}
        self._user_requested_offload_chunk_column = None
        if orchestration_operation:
            self._conv_view_db = getattr(orchestration_operation, "cast_owner", None)
            self._conv_view_name = getattr(orchestration_operation, "cast_name", None)
            self._decimal_padding_digits = getattr(
                orchestration_operation, "decimal_padding_digits", 0
            )
            self._execution_id = getattr(orchestration_operation, "execution_id", None)
            self._target_owner = orchestration_operation.target_owner
            self._user_requested_impala_insert_hint = (
                orchestration_operation.impala_insert_hint
            )
            if orchestration_operation.offload_chunk_column:
                self._user_requested_offload_chunk_column = (
                    orchestration_operation.offload_chunk_column.upper()
                )
            self._user_requested_allow_decimal_scale_rounding = getattr(
                orchestration_operation, "allow_decimal_scale_rounding", False
            )
            self._user_requested_compute_load_table_stats = getattr(
                orchestration_operation, "compute_load_table_stats", False
            )
            self._user_requested_create_backend_db = getattr(
                orchestration_operation, "create_backend_db", False
            )
            self._user_requested_storage_compression = getattr(
                orchestration_operation, "storage_compression", None
            )
            self._user_requested_storage_format = getattr(
                orchestration_operation, "storage_format", None
            )
        else:
            # Some tests cannot (and do not need to) pass a well formed orchestration_operation object.
            self._conv_view_db = None
            self._conv_view_name = None
            self._decimal_padding_digits = None
            self._execution_id = None
            self._target_owner = None
            self._user_requested_impala_insert_hint = None
            self._user_requested_allow_decimal_scale_rounding = None
            self._user_requested_compute_load_table_stats = None
            self._user_requested_create_backend_db = False
            self._user_requested_storage_compression = None
            self._user_requested_storage_format = None

        self._bucket_hash_col = None
        self.refresh_operational_settings(
            offload_operation=orchestration_operation,
        )

    def __del__(self):
        self.close()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _alter_table_sort_columns(self):
        return self._db_api.alter_sort_columns(
            self.db_name, self.table_name, self._sort_columns or []
        )

    def _cast_validation_columns(self, staging_columns: list):
        """Method returning columns and casts to be checked. That's both data type conversion
        casts and those used to generate synthetic partition columns.
        No sense in checking hash bucket cast because that includes a conversion of NULL to 0.
        Also no checking of column using a partition function because we are not in control of the casting.
        """
        cast_validation_cols = []
        for col in self.get_non_synthetic_columns():
            if self._final_table_casts[col.name.upper()]["cast_type"]:
                staging_col = match_table_column(col.name, staging_columns)
                cast_validation_cols.append(
                    (
                        staging_col.staging_file_column_name,
                        self.get_verification_cast(col),
                    )
                )
        # Use original column and partition cast for checking purposes - not the new synthetic name
        for part_col in self.get_synthetic_columns():
            if part_col.partition_info and not part_col.partition_info.function:
                cast_validation_cols.append(
                    (
                        part_col.partition_info.source_column_name,
                        self.get_verification_cast(part_col),
                    )
                )
        return cast_validation_cols

    def _cast_verification_query_options(self):
        """Returns a dict of query options to be executed before the final insert.
        This method does nothing but individual backends can override as required.
        """
        return {}

    def _check_partition_info_function(self, source_backend_column, partition_info):
        """Validate that any partition function is compatible with GOE and compatible with the source column.
        Returns data type returned by the function.
        """
        if not partition_info.function:
            return None
        func_db, func_name = partition_info.function.split(".")
        udf = self.check_partition_function(func_db, func_name)

        if udf.parameters[0].data_type != source_backend_column.data_type:
            raise BackendTableException(
                "%s for column %s: %s != %s"
                % (
                    PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT,
                    source_backend_column.name,
                    udf.parameters[0].data_type,
                    source_backend_column.data_type,
                )
            )
        return udf.return_type

    def _check_partition_info_range_start_end(self, canonical_column, backend_column):
        range_min = self._db_api.partition_range_min()
        if canonical_column.partition_info.range_start is None:
            raise BackendTableException(
                "--partition-lower-value is required for partition column/type: %s/%s"
                % (backend_column.name, backend_column.data_type)
            )
        elif range_min and int(canonical_column.partition_info.range_start) < range_min:
            raise BackendTableException(
                "--partition-lower-value is below minimum supported value: %s < %s"
                % (canonical_column.partition_info.range_start, range_min)
            )
        range_max = self._db_api.partition_range_max()
        if canonical_column.partition_info.range_end is None:
            raise BackendTableException(
                "--partition-upper-value is required for partition column/type: %s/%s"
                % (backend_column.name, backend_column.data_type)
            )
        elif range_max and int(canonical_column.partition_info.range_end) > range_max:
            raise BackendTableException(
                "--partition-upper-value is above maximum supported value: %s > %s"
                % (canonical_column.partition_info.range_end, range_max)
            )

    def _create_db(self, db_name, comment=None, properties=None, with_terminator=False):
        """Call through to relevant BackendApi to create a final database/dataset/schema.
        location can mean different things to different backends.
        """
        assert db_name
        if self._db_api.database_exists(db_name):
            self._log(
                "%s already exists: %s" % (self.db_name_label(initcap=True), db_name),
                detail=VERBOSE,
            )
            return []
        elif not self.create_database_supported():
            return []
        else:
            return self._db_api.create_database(
                db_name,
                comment=comment,
                properties=properties,
                with_terminator=with_terminator,
            )

    def _create_final_db(self, location=None, with_terminator=False):
        comment = BACKEND_DB_COMMENT_TEMPLATE.format(
            db_name_type="Offload", db_name_label=self.db_name_label()
        )
        return self._create_db(
            self.db_name,
            comment=comment,
            properties={"location": location},
            with_terminator=with_terminator,
        )

    def _create_load_db(self, location=None, with_terminator=False) -> list:
        comment = BACKEND_DB_COMMENT_TEMPLATE.format(
            db_name_type="Offload load", db_name_label=self.db_name_label()
        )
        return self._create_db(
            self._load_db_name,
            comment=comment,
            properties={"location": location},
            with_terminator=with_terminator,
        )

    def _create_result_cache_db(self, location=None):
        comment = BACKEND_DB_COMMENT_TEMPLATE.format(
            db_name_type="Result Cache", db_name_label=self.db_name_label()
        )
        return self._create_db(
            self._result_cache_db_name,
            comment=comment,
            properties={"location": location, "transient": True},
        )

    def _derive_partition_info(self, column, partition_columns):
        """Derive GOE partition attributes from an existing partition column.
        A GOE partition column can come in two forms:
            1) A synthetic GOE_PART_ column. We rely on pattern matching for these.
            2) A genuine backend column that is both a real column and a partition column.
               We rely on backend metadata for these.
        We accept partition_columns as a parameter because this may also need to work from
        a view with pretend synthetic partition columns.
        """

        def decode_synthetic_part_col(synthetic_part_col):
            """Returns (partition granularity, real name and partition digits) tuple by decoding synthetic
            partition column name.
            """
            match = regex_real_column_from_part_column(synthetic_part_col)
            if match and len(match.groups()) == 3:
                return match.group(2), match.group(3), len(match.group(2))
            return None, None, None

        assert column
        column_name = (
            column.name if isinstance(column, ColumnMetadataInterface) else column
        )
        self._debug(
            "Deriving partition info for %s.%s.%s"
            % (self.db_name, self.table_name, column_name)
        )
        partition_info = None
        if not partition_columns:
            # Need to call BackendApi for partition columns because this code is called while
            # populating BackendTable columns/partition columns.
            partition_columns = self._db_api.get_partition_columns(
                self.db_name, self.table_name
            )
        part_col_names = get_column_names(partition_columns, conv_fn=str.upper)

        if column_name.upper() in (part_col_names or []):
            position = part_col_names.index(column_name.upper())
            if self.is_synthetic_partition_column(column_name):
                self._debug(f"Deriving synthetic partition info for {column_name}")
                # Partition functions only apply for synthetic partition columns
                part_fn_name = None
                if self._partition_functions:
                    try:
                        part_fn_name = self._partition_functions[position]
                    except IndexError:
                        self._log(
                            "IndexError reading partition_functions[position]: {}[{}]".format(
                                self._partition_functions, position
                            ),
                            detail=VERBOSE,
                        )
                        raise
                # First see if the backend has its own metadata we can interrogate
                partition_info = self._db_api.derive_native_partition_info(
                    self.db_name, self.table_name, column_name, position
                )
                if partition_info:
                    # Use our in-column "metadata" to get the source column
                    _, partition_info.source_column_name, _ = decode_synthetic_part_col(
                        column_name
                    )
                else:
                    if isinstance(column, ColumnMetadataInterface):
                        backend_column = column
                    else:
                        backend_column = self._db_api.get_column(
                            self.db_name, self.table_name, column_name
                        )
                    # Fall back on our own in-column "metadata" for as much as we can retrieve
                    granularity, source_column_name, digits = decode_synthetic_part_col(
                        column_name
                    )

                    if digits and not backend_column.is_string_based():
                        # Digits is only relevant for padded string synthetic partition columns
                        digits = None
                    partition_info = ColumnPartitionInfo(
                        position=position,
                        granularity=granularity,
                        digits=digits,
                        source_column_name=source_column_name,
                    )

                partition_info.function = part_fn_name
                self._log(
                    "Derived synthetic partition info for %s: %s"
                    % (column_name, partition_info),
                    detail=VVERBOSE,
                )
            else:
                # Backends have their own metadata therefore call overloaded derive_native_partition_info()
                partition_info = self._db_api.derive_native_partition_info(
                    self.db_name, self.table_name, column_name, position
                )
                self._log(
                    "Derived native partition info for %s: %s"
                    % (column_name, partition_info),
                    detail=VVERBOSE,
                )
        return partition_info

    def _drop_state(self):
        # Drop state
        self._columns = None
        self._partition_columns = None
        self._db_api.drop_state()

    def _execute_query_fetch_all(
        self,
        sql,
        query_options=None,
        log_level=VVERBOSE,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
    ):
        assert sql
        return self._db_api.execute_query_fetch_all(
            sql,
            query_options=query_options,
            log_level=log_level,
            time_sql=True,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            profile=profile,
        )

    def _execute_query_fetch_one(
        self,
        sql,
        query_options=None,
        log_level=VVERBOSE,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
    ):
        assert sql
        return self._db_api.execute_query_fetch_one(
            sql,
            query_options=query_options,
            log_level=log_level,
            time_sql=True,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            profile=profile,
        )

    def _execute_ddl(self, sql, query_options=None, log_level=VERBOSE):
        assert sql
        return self._db_api.execute_ddl(
            sql, query_options=query_options, log_level=log_level
        )

    def _execute_dml(
        self, sql, query_options=None, sync=None, log_level=VERBOSE, profile=None
    ):
        assert sql
        return self._db_api.execute_dml(
            sql,
            query_options=query_options,
            sync=sync,
            log_level=log_level,
            profile=profile,
        )

    def _final_insert_query_options(self):
        """Returns a dict of query options to be executed before the final insert.
        This method does nothing but individual backends can override as required.
        """
        return {}

    def _format_staging_column_name(self, column):
        """For most backends this is the same as enclosure rules, some backends may override."""
        if isinstance(column, ColumnMetadataInterface):
            column_name = column.staging_file_column_name or column.name
        else:
            column_name = column
        return self.enclose_identifier(column_name)

    def _format_staging_object_name(self):
        """For most backends this is the same as enclosure rules, some backends may override."""
        return self._db_api.enclose_object_reference(
            self._load_db_name, self._load_table_name
        )

    def _gen_final_insert_sqls(
        self,
        select_expr_tuples,
        threshold_clauses=None,
        sort_expr_list=None,
        for_materialized_join=False,
    ):
        """Generate SQL to copy data from the staged load table into the final backend table.
        This code is used by materialized join pushdown as well as offload so maintaining it as own function.
        For joins the SQL include less-than and greater-than-or-equal-to predicates because it is selecting from
        a view rather than a load table.
        It returns SQL statements in a list and a dict of pre-insert session options.
        Currently this applies to all backends, this may change in future.
        """
        query_options = self._final_insert_query_options()
        chunk_filter_clauses = None
        if self._user_requested_offload_chunk_column:
            chunk_filter_clauses = self._gen_final_insert_subchunk_expressions()

        threshold_clauses = threshold_clauses or []
        sqls = []
        if chunk_filter_clauses:
            self._debug("Subchunk clauses: %s" % str(chunk_filter_clauses))
            for chunk_clause in chunk_filter_clauses:
                sqls.append(
                    self._final_insert_format_sql(
                        select_expr_tuples,
                        threshold_clauses + [chunk_clause],
                        sort_expr_list,
                        for_materialized_join=for_materialized_join,
                    )
                )
        else:
            sqls.append(
                self._final_insert_format_sql(
                    select_expr_tuples,
                    threshold_clauses,
                    sort_expr_list,
                    for_materialized_join=for_materialized_join,
                )
            )
        return sqls, query_options

    def _gen_final_insert_subchunk_expressions(self):
        """This is hidden functionality. If we have an offload that fails due to memory consumption that we cannot
        manage with MAX_OFFLOAD_CHUNK_SIZE/COUNT then we have the option to sub-chunk by partition column.
        This potentially allows us to get an offload to complete with the downside that it is no longer atomic,
        hence why it is hidden.
        Returns a list of filter clauses, each containing the SQL expression used to identify the chunk and a
        literal for the chunk, e.g.:
           ['GOE_BUCKET(CAST(`ID` AS DECIMAL(38,0)),2) = 0',
            'GOE_BUCKET(CAST(`ID` AS DECIMAL(38,0)),2) = 1']
        """
        chunk_col = [
            _
            for _ in self.get_partition_columns()
            if _.partition_info
            and _.partition_info.source_column_name.upper()
            == self._user_requested_offload_chunk_column
        ]
        chunk_col = chunk_col[0] if chunk_col else None
        if not chunk_col:
            self._messages.warning(
                "Unknown partition column, ignoring --offload-chunk-column: %s"
                % self._user_requested_offload_chunk_column
            )
            return None

        # divvy the work up in to partitionwise chunks. Extra work in scanning the load
        # table multiple times but reduced memory requirement for insert/select
        subchunk_expr = self.get_final_table_cast(chunk_col)
        self._debug("Subchunk expression: %s" % subchunk_expr)
        # Can't order by COUNT(*) in Hive, we could alias and order by but then we need to check for column name clashes
        # This is a hidden option so let's not over complicate things
        order_by = (
            "\nORDER BY COUNT(*) DESC" if self._backend_type == DBTYPE_IMPALA else ""
        )
        subchunk_sql = "SELECT %s, COUNT(*)\nFROM %s.%s\nGROUP BY %s%s" % (
            subchunk_expr,
            self.enclose_identifier(self._load_db_name),
            self.enclose_identifier(self._load_table_name),
            subchunk_expr,
            order_by,
        )
        self._log(subchunk_sql, detail=VERBOSE)
        if not self._dry_run:
            subchunks = self._execute_query_fetch_all(
                subchunk_sql, log_level=VERBOSE, not_when_dry_running=True
            )
            self._log(
                OFFLOAD_CHUNK_COLUMN_MESSAGE_PATTERN % len(subchunks), detail=VERBOSE
            )
        else:
            subchunks = [("?", -1)]
        subchunk_filter_clauses = []
        for subchunk_id, _ in subchunks:
            subchunk_id = (
                ("'%s'" % subchunk_id)
                if chunk_col.data_type == HADOOP_TYPE_STRING
                else subchunk_id
            )
            subchunk_filter_clauses.append(
                "%s = %s" % (subchunk_expr, str(subchunk_id))
            )
        return subchunk_filter_clauses

    def _gen_final_table_casts(self, rdbms_columns, staging_columns) -> dict:
        """Return a dict defining casts required when copying data from staging table to final table.
        Hadoop based example:
        {'COLUMN_17': {'cast': 'CAST(`COLUMN_17` AS DECIMAL(38))', 'cast_type': 'DECIMAL(38)'}, ... }
        """
        if not rdbms_columns or not staging_columns:
            return {}
        # Use RDBMS column as source for columns because the backend table may not yet exist
        final_table_casts = {}
        backend_columns = self.get_columns()
        for backend_col in backend_columns:
            # For partition columns rdbms_col needs to be from the source column so the cast is correct
            source_backend_col = backend_col
            if backend_col.partition_info and self.is_synthetic_partition_column(
                backend_col
            ):
                self._log(
                    "Picking up source column for partition column %s: %s"
                    % (backend_col.name, backend_col.partition_info.source_column_name),
                    detail=VVERBOSE,
                )
                source_backend_col = self.get_column(
                    backend_col.partition_info.source_column_name
                )

            rdbms_col = match_table_column(source_backend_col.name, rdbms_columns)
            staging_col = match_table_column(source_backend_col.name, staging_columns)
            cast_expr, cast_type, vcast_expr = self._staging_to_backend_cast(
                rdbms_col, source_backend_col, staging_col
            )

            if backend_col.partition_info and self.is_synthetic_partition_column(
                backend_col
            ):
                partition_expr = self._gen_synthetic_sql_cast(backend_col, cast_expr)
                partition_vexpr = self._gen_synthetic_sql_cast(backend_col, vcast_expr)
                final_table_casts[backend_col.name.upper()] = {
                    "cast": partition_expr,
                    "cast_type": backend_col.data_type,
                    "verify_cast": partition_vexpr,
                }
            else:
                final_table_casts[backend_col.name.upper()] = {
                    "cast": cast_expr,
                    "cast_type": cast_type,
                    "verify_cast": vcast_expr,
                }
        return final_table_casts

    def _gen_mat_join_insert_sqls(
        self,
        select_expr_tuples,
        threshold_cols=None,
        gte_threshold_vals=None,
        lt_threshold_vals=None,
        insert_predicates=None,
        sort_expr_list=None,
    ):
        """Generate SQL to copy data from the load view into the final backend table.
        For joins the SQL include less-than and greater-than-or-equal-to predicates because it is selecting from
        a view rather than a load table.
        It returns SQL statements in a list and a dict of pre-insert session options.
        Currently this applies to all backends, this may change in future.
        """

        def get_threshold_where_clauses(
            threshold_cols, lt_threshold_vals, gte_threshold_vals
        ):
            threshold_clauses = []
            if threshold_cols and lt_threshold_vals:
                lt_threshold_vals = hvs_to_backend_sql_literals(
                    threshold_cols,
                    lt_threshold_vals,
                    self._ipa_predicate_type,
                    self.to_backend_literal,
                )
                lt_threshold_clause, _ = get_hybrid_threshold_clauses(
                    threshold_cols,
                    threshold_cols,
                    self._ipa_predicate_type,
                    lt_threshold_vals,
                    col_enclosure_fn=self.enclose_identifier,
                )
                threshold_clauses.append(lt_threshold_clause)
                if gte_threshold_vals:
                    gte_threshold_vals = hvs_to_backend_sql_literals(
                        threshold_cols,
                        gte_threshold_vals,
                        self._ipa_predicate_type,
                        self.to_backend_literal,
                    )
                    _, gte_threshold_clause = get_hybrid_threshold_clauses(
                        threshold_cols,
                        threshold_cols,
                        self._ipa_predicate_type,
                        gte_threshold_vals,
                        col_enclosure_fn=self.enclose_identifier,
                    )
                    threshold_clauses.append(gte_threshold_clause)
            return threshold_clauses

        if threshold_cols:
            assert valid_column_list(threshold_cols)

        if insert_predicates:
            # We only insert by high value or predicate, not both at the same time
            threshold_clauses = insert_predicates
        else:
            self._debug(
                "Incremental insert columns: %s" % str_list_of_columns(threshold_cols)
            )
            self._debug("Incremental insert lt: %s" % str(lt_threshold_vals))
            self._debug("Incremental insert gte: %s" % str(gte_threshold_vals))
            threshold_clauses = get_threshold_where_clauses(
                threshold_cols, lt_threshold_vals, gte_threshold_vals
            )

        return self._gen_final_insert_sqls(
            select_expr_tuples,
            threshold_clauses=threshold_clauses,
            sort_expr_list=sort_expr_list,
            for_materialized_join=True,
        )

    def _gen_synthetic_part_date_truncated_sql_expr(
        self, date_value, granularity, synthetic_name, source_column_cast
    ):
        """Generates a SQL expression used to insert date synthetic partition key data into a date
        column in SQL, e.g.
          'expr(used(to_insert(column_name)))'
        If as_sql_string is False then we instead return a literal based upon a datetime value, this
        can be used to convert Python values to truncated datetime values for matching partitions.
        This is common to all backends with self._gen_synthetic_partition_date_truncated_sql_expr providing
        the variation.
        """
        assert isinstance(date_value, str), "%s if not of type (str, unicode)" % type(
            date_value
        )
        assert granularity in PART_COL_DATE_GRANULARITIES, (
            "Unexpected granularity: %s" % granularity
        )
        assert synthetic_name
        assert source_column_cast

        if date_value is None:
            # Compare to None because Unix epoch is False in datetime64
            return None

        return self._gen_synthetic_partition_date_truncated_sql_expr(
            synthetic_name, date_value, granularity, source_column_cast
        )

    def _gen_synthetic_part_date_as_string_sql_expr(
        self, date_value, granularity, source_column_cast
    ):
        """Generates a SQL expression used to insert DATE synthetic partition key data into a string
        column in SQL, e.g.
          'expr(used(to_insert(column_name)))'
        This is common to all backends with self._gen_synthetic_partition_date_as_string_sql_expr providing
        the variation.
        """
        assert granularity in PART_COL_DATE_GRANULARITIES, (
            "Unexpected granularity: %s" % granularity
        )
        assert source_column_cast

        if date_value is None:
            # Compare to None because Unix epoch is False in datetime64
            return None

        assert isinstance(date_value, str), "%s if not of type (str, unicode)" % type(
            date_value
        )
        component_fn_list = [["YEAR", 4], ["MONTH", 2], ["DAY", 2]]

        partition_expr = []
        for component_fn, size_or_literal in component_fn_list:
            assert source_column_cast
            partition_expr.append(
                self._gen_synthetic_partition_date_as_string_sql_expr(
                    component_fn, size_or_literal, source_column_cast
                )
            )
            if granularity == component_fn[0]:
                break
        return "CONCAT(%s)" % ",'-',".join(partition_expr)

    def _gen_synthetic_part_number_sql_expr(
        self, number_value, backend_col, granularity, synthetic_partition_digits
    ):
        """Generates a SQL expression used to insert numeric synthetic partition key data in SQL, e.g.
          'expr(used(to_insert(column_name)))'
        This is common to all backends, backend specifics in _gen_synthetic_part_number_granularity_sql_expr().
        """
        assert granularity
        assert isinstance(
            granularity, int
        ), "Granularity must be integral not: %s" % type(granularity)
        if self.synthetic_partition_numbers_are_string():
            assert synthetic_partition_digits is not None
        return self._gen_synthetic_part_number_granularity_sql_expr(
            number_value, backend_col, granularity, synthetic_partition_digits
        )

    def _gen_synthetic_part_string_sql_expr(self, string_value, granularity):
        assert granularity
        assert isinstance(
            granularity, int
        ), "Granularity must be integral not: %s" % type(granularity)
        return self._gen_synthetic_part_string_granularity_sql_expr(
            self._format_staging_column_name(string_value), granularity
        )

    def _gen_synthetic_literal_function(self, partition_column):
        """Returns a Python function to achieve the same outcome as _gen_synthetic_sql_cast() would in SQL.
        For example a month granularity fn that converts:
            datetime64('2011-01-01 12:12:12.1234') -> '2011-01'
        """
        assert isinstance(partition_column, ColumnMetadataInterface)
        if partition_column.partition_info.function:
            # We cannot use Python logic to provide synthetic value conversion because we're using a backend UDF.
            # partition_fn needs to be a call to the backend.

            def partition_fn(source_value):
                source_column = self.get_column(
                    partition_column.partition_info.source_column_name
                )
                sql_literal = self._db_api.to_backend_literal(
                    source_value, data_type=source_column.data_type
                )
                sql = "SELECT {}".format(
                    self._partition_function_sql_expression(
                        partition_column.partition_info, sql_literal
                    )
                )
                row = self._db_api.execute_query_fetch_one(sql)
                return row[0] if row else None

        else:
            partition_fn = SyntheticPartitionLiteral.gen_synthetic_literal_function(
                partition_column, self.get_columns()
            )
        return partition_fn

    def _gen_synthetic_sql_cast(self, partition_column, source_column_cast):
        """Return a SQL expression suitable for the backend system that converts column data into
        values for the synthetic partition column. For example a month granularity cast that converts:
            timestamp'2011-01-01 12:12:12' -> '2011-01'
        """
        source_column_name = partition_column.partition_info.source_column_name
        digits = partition_column.partition_info.digits
        granularity = partition_column.partition_info.granularity
        source_column = self.get_column(source_column_name)
        partition_expr = None
        if partition_column.partition_info.function:
            partition_expr = self._partition_function_sql_expression(
                partition_column.partition_info, source_column_cast
            )
        elif source_column.is_date_based() and partition_column.is_date_based():
            partition_expr = self._gen_synthetic_part_date_truncated_sql_expr(
                source_column_name,
                granularity,
                partition_column.name,
                source_column_cast,
            )
        elif source_column.is_date_based() and partition_column.is_string_based():
            partition_expr = self._gen_synthetic_part_date_as_string_sql_expr(
                source_column_name, granularity, source_column_cast
            )
        elif source_column.is_string_based():
            partition_expr = self._gen_synthetic_part_string_sql_expr(
                source_column_name, int(granularity)
            )
        elif source_column.is_number_based():
            partition_expr = self._gen_synthetic_part_number_sql_expr(
                source_column_cast, source_column, int(granularity), digits
            )
        else:
            raise NotImplementedError(
                "Unsupported synthetic partition column data type: %s"
                % source_column.data_type
            )
        return partition_expr

    def _get_dfs_client(self):
        if self._dfs_client is None:
            self._dfs_client = get_dfs_from_options(
                self._orchestration_config,
                messages=self._messages,
                dry_run=self._dry_run,
                do_not_connect=self._do_not_connect,
            )
            self._backend_dfs = self._dfs_client.backend_dfs
        return self._dfs_client

    def _is_synthetic_partition_column(self, column):
        return self._db_api.is_synthetic_partition_column(column)

    def _load_db_exists(self):
        return self._db_api.database_exists(self._load_db_name)

    def _log_dfs_cmd(self, cmd, detail=VERBOSE):
        if not self._backend_dfs:
            # Get the client cached in state
            self._get_dfs_client()
        self._messages.log("%s cmd: %s" % (self._backend_dfs, cmd), detail=detail)

    def _not_implemented_message(self, exception_text=None):
        # parent method name below, on Python3 we need to use: inspect.stack()[1].function
        msg_text = exception_text or "{}()".format(inspect.stack()[1][3])
        return "%s is not supported on %s" % (msg_text, self.backend_db_name())

    def _offload_step(
        self,
        step_constant: str,
        step_fn: Callable,
        command_type: Optional[str] = None,
        optional=False,
        mandatory_step=False,
    ):
        return self._messages.offload_step(
            step_constant,
            step_fn,
            command_type=command_type,
            execute=bool(not self._dry_run),
            optional=optional,
            mandatory_step=mandatory_step,
        )

    def _partition_column_requires_synthetic_column(
        self, backend_column, partition_info
    ):
        if partition_info.function:
            # A partition function mandates we need a synthetic column
            return True
        else:
            return self._db_api.partition_column_requires_synthetic_column(
                backend_column, partition_info.granularity
            )

    def _partition_function_sql_expression(self, partition_info, sql_input_expression):
        """Return a string containing a call to a partition function UDF.
        e.g. UDF_DB.UDF_NAME(sql_input_expression)
        """
        assert partition_info
        udf_db, udf_name = partition_info.function.split(".")
        return "{}({})".format(
            self._db_api.enclose_object_reference(udf_db, udf_name),
            sql_input_expression,
        )

    def _partition_key_out_of_range_message(self, column):
        """This returns a stock warning for when data exceeds the --partition-lower/upper -value range.
        Some backends may override if they have particular behaviour to describe.
        """
        base_message = PARTITION_KEY_OUT_OF_RANGE.format(
            column=column.name.upper(),
            start=column.partition_info.range_start,
            end=column.partition_info.range_end,
        )
        return (
            base_message
            + ", re-configure backend table or re-offload with larger range for --partition-lower-value/--partition-upper-value"
        )

    def _recreate_load_table(self, staging_file):
        """Drop and create the staging/load table and any supporting filesystem directory"""
        self._drop_load_table()
        return self._create_load_table(staging_file)

    def _result_cache_db_exists(self):
        return self._db_api.database_exists(self._result_cache_db_name)

    def _rm_dfs_dir(self, rm_uri):
        """Delete a URI and any files inside it."""
        self._log_dfs_cmd('rmdir("%s")' % rm_uri)
        if not self._dry_run:
            status = self._get_dfs_client().rmdir(rm_uri, recursive=True)
            self._messages.log("rmdir status: %s" % status, detail=VVERBOSE)

    def _set_operational_attributes(self, orchestration_operation):
        """Set private attributes based on hybrid metadata or orchestration_operation"""
        if self._hybrid_metadata:
            self._bucket_hash_col = self._hybrid_metadata.offload_bucket_column
            self._ipa_predicate_type = self._hybrid_metadata.incremental_predicate_type
            if self._hybrid_metadata.offload_partition_functions:
                self._debug(
                    "Partition functions from metadata: {}".format(
                        self._hybrid_metadata.offload_partition_functions
                    )
                )
                self._partition_functions = csv_split(
                    self._hybrid_metadata.offload_partition_functions
                )
            else:
                self._partition_functions = None
        elif orchestration_operation:
            self._bucket_hash_col = orchestration_operation.bucket_hash_col
            self._ipa_predicate_type = orchestration_operation.ipa_predicate_type
            self._debug(
                "Partition functions from offload options: {}".format(
                    orchestration_operation.offload_partition_functions
                )
            )
            self._partition_functions = (
                orchestration_operation.offload_partition_functions
            )
        else:
            self._bucket_hash_col = None

    def _staging_column_is_not_null_sql_expr(self, column_name):
        """Return a column IS NOT NULL expression.
        Common to most backend/staging format combinations but can be overridden if required.
        """
        return "%s IS NOT NULL" % self._format_staging_column_name(column_name)

    def _validate_final_table_casts_verification_sql(
        self, projection_list, predicate_or_list, limit=None
    ):
        """Verification SQL to display when final table casts have failed.
        Common to most backends but can be overridden if required.
        """
        limit_clause = ("\nLIMIT %s" % limit) if limit else ""
        return """SELECT %s\nFROM   %s\nWHERE  %s%s""" % (
            "\n,      ".join(projection_list),
            self._format_staging_object_name(),
            "\nOR     ".join(predicate_or_list),
            limit_clause,
        )

    def _validate_final_table_casts(self, staging_columns: list, log_profile=None):
        """Before copying data from the staged load table into the final backend table we first
        check that there are not issues with casting staging data types to final data types.
        This function checks for CASTs that silently corrupt data.
        In Hadoop overflowing precision in CAST(<string> AS <number>) results in NULL (both int & decimal).
        Caveat: Overflow of decimal places only results in rounding (Hive/BigQuery) or truncation (Impala)
                and is NOT trapped by these checks.
        """
        cast_validation_cols = self._cast_validation_columns(staging_columns)
        if not cast_validation_cols:
            return

        cast_check_preds = [
            "(%s AND %s IS NULL)"
            % (self._staging_column_is_not_null_sql_expr(cname), ccast)
            for cname, ccast in cast_validation_cols
        ]
        verify_cast_sql = self._validate_final_table_casts_verification_sql(
            ["COUNT(*)"], cast_check_preds
        )
        verify_cast_set = self._execute_query_fetch_one(
            verify_cast_sql,
            query_options=self._cast_verification_query_options(),
            log_level=VERBOSE,
            not_when_dry_running=True,
            profile=log_profile,
        )

        if verify_cast_set and verify_cast_set[0] > 0:
            # This query uses more resources than a simple COUNT(*) so only run it when the COUNT(*) identifies issues
            cast_check_cols = [
                self._format_staging_column_name(cname)
                for cname, _ in cast_validation_cols
            ]
            identify_projection = [
                "MAX(CASE WHEN %s THEN %s END)" % (pred, i)
                for i, pred in enumerate(cast_check_preds)
            ]
            identify_cast_sql = self._validate_final_table_casts_verification_sql(
                identify_projection, cast_check_preds
            )
            identify_cast_set = self._execute_query_fetch_one(
                identify_cast_sql, query_options=self._cast_verification_query_options()
            )
            failing_casts = [
                cast_check_preds[col_number]
                for col_number in identify_cast_set
                if col_number is not None
            ]
            failing_cols = [
                cast_check_cols[col_number]
                for col_number in identify_cast_set
                if col_number is not None
            ]
            self._log(
                "CAST() of load data will cause data loss due to lack of precision in target data type in %s rows"
                % verify_cast_set[0],
                ansi_code="red",
            )
            self._log(
                "Failing casts are:\n%s" % "\n".join(failing_casts), ansi_code="red"
            )
            if failing_casts:
                limit_sample_row_count = 50
                suggest_sql = self._validate_final_table_casts_verification_sql(
                    failing_cols,
                    failing_casts,
                    min(verify_cast_set[0], limit_sample_row_count),
                )
                self._log(
                    "The SQL below will assist identification of problem data:\n%s"
                    % suggest_sql
                )
            raise DataValidationException(CAST_VALIDATION_EXCEPTION_TEXT)

    def _validate_staged_data_rules(
        self, rdbms_part_cols, rdbms_columns, staging_columns
    ):
        """Rules to be used in validate_staged_data().
        Currently appropriate across all backends, this may change as more backends are added.
        """
        # pred_list contains tuples of validations to made on the load data, tuple structure named below
        Validation = collections.namedtuple(
            "Validation", "column_name expression fatal message"
        )

        def validation_rules_for_decimal_scale(pred_list):
            for scale_check_col in [
                _ for _ in self.get_non_synthetic_columns() if _.is_number_based()
            ]:
                staging_col = match_table_column(scale_check_col.name, staging_columns)
                if not staging_col.is_string_based():
                    # We can only validate scale when the data is staged to string
                    continue
                rdbms_col = match_table_column(scale_check_col.name, rdbms_columns)
                if rdbms_col.data_scale == 0:
                    self._debug(
                        "RDBMS column has integral data so no need to check data"
                    )
                    continue
                backend_scale = scale_check_col.data_scale
                if backend_scale is None:
                    backend_scale = self.max_decimal_scale(scale_check_col.data_type)
                if (
                    rdbms_col.data_scale
                    and rdbms_col.data_scale > 0
                    and rdbms_col.data_scale < backend_scale
                ):
                    self._debug(
                        "RDBMS column scale is safely below the backend so no need to check it: %s < %s"
                        % (rdbms_col.data_scale, backend_scale)
                    )
                    continue
                is_invalid_scale_expr = (
                    self._db_api.decimal_scale_validation_expression(
                        column_name=self._format_staging_column_name(staging_col),
                        column_scale=backend_scale,
                    )
                )
                pred_list.append(
                    Validation(
                        column_name=scale_check_col.name.upper(),
                        expression=is_invalid_scale_expr,
                        fatal=True,
                        message="Scale for column %s exceeds column maximum: %s\n%s"
                        % (
                            scale_check_col.name.upper(),
                            backend_scale,
                            DATA_VALIDATION_SCALE_EXCEPTION_TEXT,
                        ),
                    )
                )

        def validation_rules_for_not_null_cols(pred_list, meta_op_filter):
            cols_already_checked = []
            upper_fn = lambda x: x.upper()

            if self.not_null_column_supported():
                nn_columns = get_column_names(
                    [_ for _ in self.get_columns() if _.nullable == False], upper_fn
                )
                for col_name in nn_columns:
                    staging_col = match_table_column(col_name, staging_columns)
                    pred_list.append(
                        Validation(
                            column_name=col_name,
                            expression="%s IS NULL%s"
                            % (
                                self._format_staging_column_name(staging_col),
                                meta_op_filter,
                            ),
                            fatal=True,
                            message=f"{DATA_VALIDATION_NOT_NULL_EXCEPTION_TEXT}: {col_name}",
                        )
                    )
                cols_already_checked.extend(nn_columns)

            if rdbms_part_cols and len(rdbms_part_cols) > 1:
                # Partitions with a single partition column containing a NULL will not be offloaded as they'll be in
                # the MAXVALUE partition.
                # If we change offload to include MAXVALUE partitions then the len() restriction above should be removed
                extra_cols = set(
                    get_column_names(rdbms_part_cols, upper_fn)
                ).difference(cols_already_checked)
                for col_name in extra_cols:
                    staging_col = match_table_column(col_name, staging_columns)
                    pred_list.append(
                        Validation(
                            column_name=col_name,
                            expression="%s IS NULL%s"
                            % (
                                self._format_staging_column_name(staging_col),
                                meta_op_filter,
                            ),
                            fatal=False,
                            message=f"Source RDBMS partition key {col_name} has NULL values",
                        )
                    )
                cols_already_checked.extend(extra_cols)

            if self.get_partition_columns():
                source_column_names = [
                    _.partition_info.source_column_name.upper()
                    for _ in self.get_partition_columns()
                    if _.partition_info
                ]
                extra_cols = set(source_column_names).difference(cols_already_checked)
                for col_name in extra_cols:
                    staging_col = match_table_column(col_name, staging_columns)
                    pred_list.append(
                        Validation(
                            column_name=col_name.upper(),
                            expression="%s IS NULL%s"
                            % (
                                self._format_staging_column_name(staging_col),
                                meta_op_filter,
                            ),
                            fatal=False,
                            message="%s: %s"
                            % (NULL_BACKEND_PARTITION_VALUE_WARNING_TEXT, col_name),
                        )
                    )

        def validation_rules_for_synthetic_part_cols(pred_list):
            for partition_col in self.get_partition_columns():
                if (
                    self._is_synthetic_partition_column(partition_col.name)
                    and partition_col.is_string_based()
                    and partition_col.partition_info
                    and not partition_col.partition_info.function
                ):
                    # Check partition digits for synthetic partition columns
                    source_backend_col = self.get_column(
                        partition_col.partition_info.source_column_name
                    )
                    if source_backend_col.is_number_based():
                        granularity_expr = (
                            self._gen_synthetic_part_number_granularity_sql_expr(
                                self.get_verification_cast(
                                    source_backend_col.name.upper()
                                ),
                                source_backend_col,
                                partition_col.partition_info.granularity,
                                partition_col.partition_info.digits,
                                with_padding=False,
                            )
                        )
                        length_expr = self._db_api.length_sql_expression(
                            granularity_expr
                        )
                        pred_list.append(
                            Validation(
                                column_name=partition_col.partition_info.source_column_name.upper(),
                                expression="%s > %s"
                                % (length_expr, partition_col.partition_info.digits),
                                fatal=True,
                                message="Synthetic value for %s exceeds digits %s, re-offload with larger value for --partition-digits"
                                % (
                                    partition_col.partition_info.source_column_name.upper(),
                                    partition_col.partition_info.digits,
                                ),
                            )
                        )
                elif not self._is_synthetic_partition_column(partition_col.name):
                    # Native partition column, check range start/end
                    if (
                        partition_col.partition_info.range_start is not None
                        and partition_col.partition_info.range_end is not None
                    ):
                        pred_list.append(
                            Validation(
                                column_name=partition_col.name.upper(),
                                expression="%s < %s OR %s > %s"
                                % (
                                    self.get_verification_cast(partition_col.name),
                                    partition_col.partition_info.range_start,
                                    self.get_verification_cast(partition_col.name),
                                    partition_col.partition_info.range_end,
                                ),
                                fatal=False,
                                message=self._partition_key_out_of_range_message(
                                    partition_col
                                ),
                            )
                        )

        pred_list = []
        meta_op_filter = ""

        validation_rules_for_not_null_cols(pred_list, meta_op_filter)

        nan_capable_column_names = [
            col.name for col in rdbms_columns if col.is_nan_capable()
        ]
        if nan_capable_column_names and self._db_api.nan_supported():
            nan_message = 'Column "%s" has NaN values, RDBMS and %s predicate comparisons may not be consistent'
            for n in nan_capable_column_names:
                staging_col = match_table_column(n, staging_columns)
                pred_list.append(
                    Validation(
                        column_name=n.upper(),
                        expression=self._db_api.is_nan_sql_expression(
                            self._format_staging_column_name(staging_col)
                        ),
                        fatal=False,
                        message=nan_message % (n.upper(), self.backend_db_name()),
                    )
                )

        if not self._user_requested_allow_decimal_scale_rounding:
            validation_rules_for_decimal_scale(pred_list)

        validation_rules_for_synthetic_part_cols(pred_list)

        return pred_list

    def _validate_staged_data(
        self,
        rdbms_part_cols,
        rdbms_columns,
        expected_rows,
        staging_columns,
        log_profile=None,
    ):
        """Run some tests on the data offloaded to the load table before moving it to the final table
        Most tests produce warnings only, however some are coded to raise an exception (fatal=True)
        The most important test is that COUNT(*) of the staged data matches what we thought we transferred
        """
        pred_list = self._validate_staged_data_rules(
            rdbms_part_cols, rdbms_columns, staging_columns
        )
        projection = [
            "MAX(CASE WHEN %s THEN %s END)" % (_.expression, i)
            for i, _ in enumerate(pred_list)
        ] + ["COUNT(*)"]
        pred_messages = "\n,      ".join(projection)
        sql = """SELECT %s\nFROM   %s""" % (
            pred_messages,
            self._format_staging_object_name(),
        )
        validation_set = self._execute_query_fetch_one(
            sql,
            query_options=self._validate_staged_data_query_options(),
            log_level=VERBOSE,
            not_when_dry_running=True,
            profile=log_profile,
        )
        if validation_set:
            validation_set = list(validation_set)
            count_star = validation_set.pop()
            if expected_rows is None:
                self._log("Load table row count: %s" % count_star, detail=VERBOSE)
            else:
                if expected_rows == count_star:
                    self._log(
                        "Offload row count matches load table row count (%s == %s)"
                        % (expected_rows, count_star),
                        detail=VERBOSE,
                    )
                else:
                    self._log(
                        "Offload row count != load table row count (%s != %s)"
                        % (expected_rows, count_star),
                        detail=VVERBOSE,
                    )
                    raise DataValidationException(
                        "Offload row count does not match load table row count (%s != %s)"
                        % (expected_rows, count_star)
                    )

            warnings = [
                pred_list[msg_code].message
                for msg_code in validation_set
                if msg_code is not None
            ]
            [self._messages.warning(msg, ansi_code="red") for msg in warnings]
            errors = [
                pred_list[msg_code].message
                for msg_code in validation_set
                if msg_code is not None and pred_list[msg_code].fatal
            ]
            if errors:
                raise DataValidationException("\n".join(errors))
        else:
            self._log("Staged data validation returned an empty set", detail=VERBOSE)

    def _validate_staged_data_query_options(self):
        """Default to no query options for load table validation, Hive has an override."""
        return {}

    # enforced private methods

    @abstractmethod
    def _create_load_table(self, staging_file, with_terminator=False) -> list:
        pass

    @abstractmethod
    def _drop_load_table(self, sync=None):
        pass

    @abstractmethod
    def _final_insert_format_sql(
        self,
        select_expr_tuples,
        filter_clauses,
        sort_expr_list,
        for_materialized_join=False,
    ):
        pass

    @abstractmethod
    def _gen_synthetic_partition_column_object(self, synthetic_name, canonical_column):
        """Return a backend column object suitable for a synthetic partition column"""

    @abstractmethod
    def _gen_synthetic_partition_date_truncated_sql_expr(
        self, synthetic_name, column_name, granularity, source_column_cast
    ):
        pass

    @abstractmethod
    def _gen_synthetic_partition_date_as_string_sql_expr(
        self, extract_name, pad_size, source_column_cast
    ):
        pass

    @abstractmethod
    def _gen_synthetic_part_number_granularity_sql_expr(
        self,
        column_expr,
        backend_col,
        granularity,
        synthetic_partition_digits,
        with_padding=True,
    ):
        """Generates a SQL expression used to truncate NUMERIC synthetic partition key data in SQL, e.g.
          'expr(used(to_insert(column_name)))'
        Something like: FLOOR(input / granularity) * granularity.
        Rounds towards -inf i.e. ABS(FLOOR(123.4)) is different to ABS(FLOOR(-123.4))
        """

    @abstractmethod
    def _gen_synthetic_part_string_granularity_sql_expr(self, column_expr, granularity):
        """Generates a SQL expression used to insert STRING synthetic partition key data in SQL, e.g.
        'expr(used(to_insert(column_expr)))'
        """

    @abstractmethod
    def _staging_to_backend_cast(
        self, rdbms_column, backend_column, staging_column
    ) -> tuple:
        """Returns correctly cast or overridden columns ready for insert/select to final table.
        Be aware that setting return_type will result in validate_avro_to_hadoop_casts() checking the CAST(),
        you may not always want that, e.g. when overriding with NULL intentionally.
        The method returns a tuple with 3 elements, (the SQL cast, data type, verification cast), e.g.:
            ('CAST(`staged_id`) AS BIGINT)', 'BIGINT', 'TRY_CAST(`staged_id`) AS BIGINT)')
        rdbms_column: Required because we need to know what data is actually inside the staging column.
        """

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def _log(self, msg, detail=None, ansi_code=None):
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def backend_db_name(self):
        return self._db_api.backend_db_name()

    def backend_type(self):
        return self._backend_type

    def check_partition_function(self, db_name, udf_name):
        """Check that a partition function is compatible with GOE.
        i.e. it has only a single argument, is not overloaded and returns a supported data type.
        Returns the valid UDFs details for convenience.
        """
        udfs = self._db_api.udf_details(db_name, udf_name)
        if not udfs:
            raise BackendTableException(
                "{} in {}: {}.{}".format(
                    PARTITION_FUNCTION_DOES_NOT_EXIST_EXCEPTION_TEXT,
                    self.backend_db_name(),
                    db_name,
                    udf_name,
                )
            )
        self._log("Partition function details: {}".format(str(udfs)), detail=VVERBOSE)
        if len(udfs) != 1:
            # We do not support UDF overloading
            raise BackendTableException(
                "Multiple partition functions are matched by {}.{}".format(
                    db_name, udf_name
                )
            )

        udf = udfs[0]
        if not self._db_api.is_supported_partition_function_return_data_type(
            udf.return_type
        ):
            raise BackendTableException(
                "Partition function {}.{} has invalid return type: {}".format(
                    db_name, udf_name, udf.return_type
                )
            )
        if len(udf.parameters or []) != 1:
            raise BackendTableException(
                "Partition function {}.{} {}: {}".format(
                    db_name,
                    udf_name,
                    PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT,
                    udf.parameter_spec_string(),
                )
            )
        if (
            udf.parameters[0].data_type
            not in self._db_api.supported_partition_function_parameter_data_types()
        ):
            raise BackendTableException(
                "Partition function {}.{} has invalid parameter type: {} not in {}".format(
                    db_name,
                    udf_name,
                    udf.parameters[0].data_type,
                    ",".join(
                        self._db_api.supported_partition_function_parameter_data_types()
                    ),
                )
            )
        return udf

    def close(self):
        """Free up any resources currently held"""
        if getattr(self, "_db_api", None):
            self._db_api.close()

    def conversion_view_exists(self):
        return self._db_api.view_exists(self._conv_view_db, self._conv_view_name)

    def convert_canonical_columns_to_backend(self, canonical_columns):
        """Takes a full set of canonical columns and converts them to a backend column set.
        Returns the new column set.
        Any resulting synthetic partition columns will be appended to the column list.
        """

        assert canonical_columns
        assert valid_column_list(canonical_columns), invalid_column_list_message(
            canonical_columns
        )

        new_backend_columns = []
        new_synthetic_columns = []

        for canonical_column in canonical_columns:
            backend_column = self.from_canonical_column(canonical_column)
            if canonical_column.partition_info:
                if canonical_column.partition_info.function:
                    partition_data_type = self._check_partition_info_function(
                        backend_column, canonical_column.partition_info
                    )
                else:
                    partition_data_type = backend_column.data_type

                if not self.is_valid_partitioning_data_type(partition_data_type):
                    raise BackendTableException(
                        "Partition column data type is not supported in this version: %s"
                        % partition_data_type
                    )
                if self._partition_column_requires_synthetic_column(
                    backend_column, canonical_column.partition_info
                ):
                    if (
                        backend_column.is_number_based()
                        and not self.synthetic_partition_numbers_are_string()
                    ):
                        self._check_partition_info_range_start_end(
                            canonical_column, backend_column
                        )
                    synthetic_name = canonical_column.partition_info.synthetic_name(
                        canonical_columns
                    )
                    if canonical_column.partition_info.function:
                        new_synthetic_columns.append(
                            self._db_api.gen_column_object(
                                synthetic_name,
                                data_type=partition_data_type,
                                partition_info=canonical_column.partition_info,
                            )
                        )
                    else:
                        new_synthetic_columns.append(
                            self._gen_synthetic_partition_column_object(
                                synthetic_name, canonical_column
                            )
                        )
                    backend_column.partition_info = None
                else:
                    # Native numeric partition columns require lower/upper bounds
                    if backend_column.is_number_based():
                        self._check_partition_info_range_start_end(
                            canonical_column, backend_column
                        )
                    backend_column.partition_info = canonical_column.partition_info
            new_backend_columns.append(backend_column)

        self._log("Converted backend columns:", detail=VVERBOSE)
        [self._log(str(_), detail=VVERBOSE) for _ in new_backend_columns]
        if new_synthetic_columns:
            self._log("Created synthetic columns:", detail=VVERBOSE)
            [self._log(str(_), detail=VVERBOSE) for _ in new_synthetic_columns]
            new_backend_columns += new_synthetic_columns
        return new_backend_columns

    def create_backend_view(
        self,
        column_tuples,
        new_column_list,
        ansi_joined_tables,
        filter_clauses=None,
        db_name_override=None,
    ):
        """Create a view in the backend. Used primarily when the object is describing
        a join view rather than an offload target table.
        column_tuples: A list of (column expression, alias) tuples
        new_column_list: The view columns in the standard format, only used in dry_run mode
        ansi_joined_tables: contents of the from clause which could be
                                db.table
                            or
                                db.t1 inner join db.t2 on (t1.id = t2.id)
        filter_clauses: A list of 'col=something' strings
        """
        assert column_tuples and isinstance(column_tuples, list)
        assert isinstance(column_tuples[0], tuple)
        assert new_column_list and valid_column_list(new_column_list)

        view_db_name = db_name_override or self.db_name

        self._db_api.create_view(
            view_db_name,
            self.table_name,
            column_tuples,
            ansi_joined_tables,
            filter_clauses=filter_clauses,
            sync=True,
        )

        if view_db_name == self.db_name:
            # The CREATE VIEW above may have changed our world view so let's reset what we already know
            self._drop_state()
            if self._dry_run:
                # Because we haven't actually created the view we need to explicitly
                # set some attributes for verification mode to work
                self._columns = new_column_list

    def create_conversion_view(self, column_tuples):
        """Create a conversion view to convert certain attributes of the backend object before presenting to the RDBMS
        An example of why we do this would be column names too long for the RDBMS
        column_tuples: A list of (column expression, alias) tuples
        """
        assert column_tuples and isinstance(column_tuples, list)
        assert isinstance(column_tuples[0], tuple)
        ansi_joined_tables = self._db_api.enclose_object_reference(
            self.db_name, self.table_name
        )
        self._db_api.create_view(
            self._conv_view_db,
            self._conv_view_name,
            column_tuples,
            ansi_joined_tables,
            sync=True,
        )

    def create_load_db(self, with_terminator=False) -> list:
        """Generic code to create a load database, individual backends may have overrides."""
        if self._db_api.load_db_transport_supported():
            return self._create_load_db(with_terminator=with_terminator)
        else:
            return []

    def db_exists(self):
        return self._db_api.database_exists(self.db_name)

    def db_name_label(self, initcap=False):
        return self._db_api.db_name_label(initcap=initcap)

    def default_date_based_partition_granularity(self):
        return self._db_api.default_date_based_partition_granularity()

    def default_udf_db_name(self):
        """By default we support UDF_DB but individual backends may have their own override"""
        return self._udf_db

    def delta_table_exists(self):
        return False

    def derive_unicode_string_columns(self, as_csv=False):
        """Return a list of columns that indicate the user requested were offloaded as unicode.
        Some backends (e.g. Synpase) may override this method.
        """
        unicode_columns = [
            _
            for _ in self.get_canonical_columns()
            if _.char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
        ]
        return ",".join(_.name for _ in unicode_columns) if as_csv else unicode_columns

    def detect_column_has_fractional_seconds(self, backend_column):
        """Detect whether a backend date based column contains fractional seconds
        backend_column can be a column object or a column name
        Returns True/False
        """
        if isinstance(backend_column, str):
            column = self.get_column(backend_column)
        else:
            assert isinstance(backend_column, ColumnMetadataInterface)
            column = backend_column
        return self._db_api.detect_column_has_fractional_seconds(
            self.db_name, self.table_name, column
        )

    def drop(self, purge=False):
        if self.is_view():
            return self._db_api.drop_view(self.db_name, self.table_name)
        else:
            return self.drop_table(purge=purge)

    def drop_conversion_view(self):
        assert self._conv_view_db and self._conv_view_name
        self._db_api.drop_view(self._conv_view_db, self._conv_view_name)

    def drop_table(self, purge=False):
        cmds_executed = [
            self._db_api.drop_table(self.db_name, self.table_name, purge=purge),
        ]
        return cmds_executed

    def drop_load_view(self, sync=None):
        """Drop the staging/load view used when materialized a join"""
        self._db_api.drop_view(
            self._load_view_db_name, self._load_table_name, sync=sync
        )

    def exists(self):
        return self._db_api.exists(self.db_name, self.table_name)

    def enclose_identifier(self, identifier):
        return self._db_api.enclose_identifier(identifier)

    def enclosure_character(self):
        return self._db_api.enclosure_character()

    def from_canonical_column(self, column):
        return self._db_api.from_canonical_column(
            column, decimal_padding_digits=self._decimal_padding_digits
        )

    def gen_default_numeric_column(self, column_name, data_scale=18):
        return self._db_api.gen_default_numeric_column(
            column_name, data_scale=data_scale
        )

    def gen_synthetic_partition_col_expressions(
        self, expr_from_columns=None, as_python_fns=False
    ):
        """Takes the backend synthetic partition column information and returns a list of lists for the
        expressions we will actually partition by. e.g.:
        [
            ['synth_col_name', 'data_type', 'expr(used(to_insert_data))', 'original_pcol_name']
        ]

        The outcome of this function is not cached in state inside this function because we may choose to call
        it multiple times with different values for expr_from_columns/as_python_fns. This class may cache it for
        convenience but this getter always regenerates the lists.

        expr_from_columns: Can be used when the expressions have already been materialized in an underlying
                           object (e.g join view). It is a list of columns in the usual format.
        as_python_fns: Can be used to get the expressions back as Python functions rather than strings of SQL
                       function calls, e.g.:
        [
            ['synth_col_name', 'data_type', expr_fn, 'original_pcol_name']
        ]
        """
        pcs = []

        for partition_col in self.get_partition_columns():
            if partition_col.partition_info and self.synthetic_partitioning_supported():
                partition_expr = None
                if (
                    expr_from_columns
                    and not as_python_fns
                    and match_table_column(partition_col.name, expr_from_columns)
                ):
                    # Check if the column already exists in list of columns, if so use that rather than recalculate
                    partition_expr = self.enclose_identifier(partition_col.name)
                elif as_python_fns:
                    partition_expr = self._gen_synthetic_literal_function(partition_col)
                else:
                    partition_expr = self.get_final_table_cast(partition_col)

                if partition_expr:
                    pcs.append(
                        [
                            partition_col.name,
                            partition_col.format_data_type(),
                            partition_expr,
                            partition_col.partition_info.source_column_name,
                        ]
                    )

        return pcs

    def get_backend_api(self):
        """Sometimes we just want to pass on access to the backend api and not
        have wrappers for the sake of wrappers.
        """
        return self._db_api

    def get_canonical_columns(self):
        """Get canonical columns for the table"""
        return [self.to_canonical_column(_) for _ in self.get_columns()]

    def get_column_names(self, conv_fn=None):
        return get_column_names(self.get_columns(), conv_fn=conv_fn)

    def get_columns(self):
        """Get columns for the table, there is a partner method set_columns() for when the
        table does not yet exist.
        """
        if self._columns is None:
            columns = self._db_api.get_columns(self.db_name, self.table_name)
            if self.is_view(object_name_override=self.table_name):
                # There aren't any real partition columns but, for join pushdown, we may have synthetic columns in
                # the view projection. Therefore we need to fake a partition column list, just in case.
                self._log(
                    "Faking partition columns for view: %s.%s"
                    % (self.db_name, self.table_name),
                    detail=VVERBOSE,
                )
                part_cols = [
                    _ for _ in columns if self.is_synthetic_partition_column(_)
                ]
                self._log('View "partition" columns: %s' % part_cols, detail=VVERBOSE)
            else:
                part_cols = self._db_api.get_partition_columns(
                    self.db_name, self.table_name
                )
            # Run through columns adding partition_info as we go.
            new_columns = []
            for column in columns:
                # Add partition_info attribute to any partition columns
                column.partition_info = self._derive_partition_info(column, part_cols)
                new_columns.append(column)
            self._columns = new_columns
        return self._columns

    def get_column(self, column_name):
        """Get a single column from get_columns()."""
        return match_table_column(column_name, self.get_columns())

    def get_distinct_column_values(
        self, column_name_list, columns_to_cast_to_string=None, order_results=False
    ):
        """Run SQL to get distinct values for a list of columns
        The columns_to_cast_to_string parameter is because our Hive DB API does not support nanoseconds
        therefore we bring certain data types back as strings to avoid truncating to milliseconds
        """
        return self._db_api.get_distinct_column_values(
            self.db_name,
            self.table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            order_results=order_results,
        )

    def get_final_table_cast(self, column):
        """Get the cast used to translate staging data into the final table.
        Column can be a column name or a column object.
        """
        assert column
        assert (
            self._final_table_casts
        ), "set_final_table_casts() has not been called to prepare casts"
        if isinstance(column, ColumnMetadataInterface):
            column_name = column.name.upper()
        else:
            column_name = column.upper()
        return self._final_table_casts[column_name]["cast"]

    def get_load_db_name(self):
        return self._load_db_name

    def get_load_table_name(self):
        return self._load_table_name

    def get_max_column_length(self, column_name):
        return self._db_api.get_max_column_length(
            self.db_name, self.table_name, column_name
        )

    def get_max_column_values(
        self,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
    ):
        """Get max value for a set of columns.
        optimistic_prune_clause is used to limit the partitions scanned when looking for existing rows.
            If no data is returned then we fall back to scanning without the clause.
            optimistic_prune_clause is only applicable for single column partition schemes.
        """
        return self._db_api.get_max_column_values(
            self.db_name,
            self.table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            optimistic_prune_clause=optimistic_prune_clause,
        )

    def get_non_synthetic_columns(self):
        """Same functionality as db_api.get_non_synthetic_columns but caters for fresh offloads where
        the backend table does not yet exist, i.e. uses self._columns rather than fetch from backend.
        """
        if not self._columns and self.exists():
            self.get_columns()
        return [_ for _ in self._columns if not self.is_synthetic_partition_column(_)]

    def get_partition_columns(self):
        """Get partition columns for the base table.
        This is done via get_columns() which ensures partition_info is set correctly.
        """
        if self._partition_columns is None:
            self._partition_columns = get_partition_columns(self.get_columns())
        return self._partition_columns

    def get_synthetic_columns(self):
        """Get synthetic partition/bucket columns for the base table."""
        return [
            _
            for _ in self.get_partition_columns()
            if self.is_synthetic_partition_column(_)
        ]

    def get_table_partitions(self):
        return self._db_api.get_table_partitions(self.db_name, self.table_name)

    def get_table_partition_count(self):
        return self._db_api.get_table_partition_count(self.db_name, self.table_name)

    def get_table_row_count_from_metadata(self):
        return self._db_api.get_table_row_count_from_metadata(
            self.db_name, self.table_name
        )

    def get_table_size(self, no_cache=False):
        return self._db_api.get_table_size(
            self.db_name, self.table_name, no_cache=no_cache
        )

    def get_table_size_and_row_count(self):
        return self._db_api.get_table_size_and_row_count(self.db_name, self.table_name)

    def get_table_stats(self, as_dict=False):
        return self._db_api.get_table_stats(
            self.db_name, self.table_name, as_dict=as_dict
        )

    def get_table_stats_partitions(self):
        return self._db_api.get_table_stats_partitions(self.db_name, self.table_name)

    def get_verification_cast(self, column):
        """Get the cast used to translate staging data into the final table for cast verification query.
        column can be a column name or a column object.
        """
        assert column
        assert (
            self._final_table_casts
        ), "set_final_table_casts() has not been called to prepare casts"
        if isinstance(column, ColumnMetadataInterface):
            column_name = column.name.upper()
        else:
            column_name = column.upper()
        return self._final_table_casts[column_name]["verify_cast"]

    def has_rows(self):
        if self._has_rows is None:
            self._has_rows = self._db_api.table_has_rows(self.db_name, self.table_name)
        return self._has_rows

    def identifier_contains_invalid_characters(self, identifier):
        return self._db_api.identifier_contains_invalid_characters(identifier)

    def is_supported_data_type(self, data_type):
        return self._db_api.is_supported_data_type(data_type)

    def is_synthetic_partition_column(self, column):
        return self._db_api.is_synthetic_partition_column(column)

    def is_valid_partitioning_data_type(self, data_type):
        return self._db_api.is_valid_partitioning_data_type(data_type)

    def is_valid_staging_format(self):
        return self._db_api.is_valid_staging_format(self._offload_staging_format)

    def is_view(self, object_name_override=None):
        return self._db_api.is_view(
            self.db_name, object_name_override or self.table_name
        )

    def max_column_name_length(self):
        return self._db_api.max_column_name_length()

    def max_db_name_length(self):
        return self._db_api.max_db_name_length()

    def max_decimal_integral_magnitude(self):
        return self._db_api.max_decimal_integral_magnitude()

    def max_decimal_precision(self):
        return self._db_api.max_decimal_precision()

    def max_decimal_scale(self, data_type=None):
        return self._db_api.max_decimal_scale(data_type=data_type)

    def max_datetime_scale(self):
        return self._db_api.max_datetime_scale()

    def max_datetime_value(self):
        return self._db_api.max_datetime_value()

    def max_partition_columns(self):
        return self._db_api.max_partition_columns()

    def max_sort_columns(self):
        return self._db_api.max_sort_columns()

    def max_table_name_length(self):
        return self._db_api.max_table_name_length()

    def min_datetime_value(self):
        return self._db_api.min_datetime_value()

    # Methods related to predicate rendering and transformation follow ###
    @abstractmethod
    def predicate_has_rows(self, predicate):
        """Return boolean indicating if table has >0 rows satisfying predicate"""

    @abstractmethod
    def predicate_to_where_clause(self, predicate, columns_override=None):
        pass

    def refresh_operational_settings(
        self,
        offload_operation=None,
        rdbms_columns=None,
        staging_columns=None,
    ):
        """This backend table object is created before we have validated/tuned user inputs in offload_operation
        After the user inputs have been massaged we can call this method to ensure we have correct values
        """
        self._set_operational_attributes(offload_operation)
        if offload_operation:
            self._offload_distribute_enabled = (
                offload_operation.offload_distribute_enabled
            )
            self._offload_stats_method = offload_operation.offload_stats_method
            self._sort_columns = (
                offload_operation.sort_columns
                if self.sorted_table_supported()
                else None
            )
        if rdbms_columns:
            self.set_final_table_casts(rdbms_columns, staging_columns)

    def set_column_stats(self, new_column_stats, ndv_cap, num_null_factor):
        self._db_api.set_column_stats(
            self.db_name,
            self.table_name,
            new_column_stats,
            ndv_cap,
            num_null_factor,
        )

    def set_columns(self, new_columns):
        """Set column lists in state. This should be used sparingly, only in cases where the columns cannot be
        retrieved from the backend in a normal way via get_columns(). Examples: in verification mode or before
        a backend object has been created.
        NOTE: Snowflake has an override of this global method.
        """
        assert valid_column_list(new_columns)
        self._columns = new_columns
        self._partition_columns = get_partition_columns(new_columns)

    def set_final_table_casts(self, rdbms_columns, staging_columns):
        """Set cast expressions in state that are required to convert staged RDBMS data into correct backend data.
        rdbms_columns: Required so we know what is actually in the staged data, e.g. DATEs can be staged as strings
                       and also stored as strings in the backend.
        staging_columns: The column types for the staged data. For regular offload this will be a set of Avro or
                         Parquet columns, for a materialized view it will be a set of backend columns.
        """
        self._final_table_casts = self._gen_final_table_casts(
            rdbms_columns, staging_columns
        )

    def set_partition_stats(self, new_partition_stats, additive_stats):
        return self._db_api.set_partition_stats(
            self.db_name, self.table_name, new_partition_stats, additive_stats
        )

    def set_table_stats(self, new_table_stats, additive_stats):
        return self._db_api.set_table_stats(
            self.db_name, self.table_name, new_table_stats, additive_stats
        )

    def supported_backend_data_types(self):
        return self._db_api.supported_backend_data_types()

    def supported_date_based_partition_granularities(self):
        return self._db_api.supported_date_based_partition_granularities()

    def table_exists(self):
        return self._db_api.table_exists(self.db_name, self.table_name)

    def target_version(self):
        return self._db_api.target_version()

    def transport_binary_data_in_base64(self):
        """Backends may override this in order to stage binary data, during offload transport, encoded as Base64"""
        return False

    def to_backend_literal(self, py_val, data_type=None):
        return self._db_api.to_backend_literal(py_val, data_type=data_type)

    def to_canonical_column_with_overrides(
        self,
        backend_column,
        canonical_overrides,
        detect_sizes=False,
        max_rdbms_time_scale=None,
    ):
        """Translate a backend column to an internal GOE column but only after considering user overrides
        canonical_overrides: Brings user defined overrides into play, they take precedence over default rules
        detect_sizes: Interrogates columns of certain types to check on size of data
        """

        def datetime_column_has_unbound_scale(column):
            return bool(column.has_time_element() and column.data_scale is None)

        assert backend_column
        assert isinstance(
            backend_column, ColumnMetadataInterface
        ), "%s is not an instance of ColumnMetadataInterface" % type(backend_column)
        if canonical_overrides:
            assert valid_column_list(canonical_overrides)

        if match_table_column(backend_column.name, canonical_overrides):
            # There is a user specified override for this column
            new_col = match_table_column(backend_column.name, canonical_overrides)
            # Validate the data type of the source column and override are compatible
            if not self._db_api.valid_canonical_override(backend_column, new_col):
                raise BackendTableException(
                    "%s %s: %s -> %s"
                    % (
                        INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
                        backend_column.name.upper(),
                        backend_column.data_type.upper(),
                        new_col.data_type.upper(),
                    )
                )
        else:
            if detect_sizes and backend_column.is_unbound_string():
                longest_string = self.get_max_column_length(backend_column.name)
                longest_string = longest_string or 0
                # Even hundreds, at least 25% greater than longest source string
                safe_length = (int(1.25 * longest_string / 200) + 1) * 200
                self._log(
                    "Padded length %s to %s" % (str(longest_string), str(safe_length)),
                    detail=VVERBOSE,
                )
                detect_col = backend_column.clone(
                    data_length=safe_length, char_length=safe_length
                )
                new_col = self.to_canonical_column(detect_col)
            elif detect_sizes and datetime_column_has_unbound_scale(backend_column):
                detect_col = backend_column.clone()
                if self.detect_column_has_fractional_seconds(backend_column):
                    detect_col.data_scale = max_rdbms_time_scale or 9
                    self._log(
                        "Detected fractional seconds, using data_scale=%s"
                        % str(detect_col.data_scale),
                        detail=VVERBOSE,
                    )
                else:
                    self._log(
                        "No fractional seconds detected, using data_scale=0",
                        detail=VVERBOSE,
                    )
                    detect_col.data_scale = 0
                new_col = self.to_canonical_column(detect_col)
            else:
                new_col = self.to_canonical_column(backend_column)
        return new_col

    def to_canonical_column(self, column):
        return self._db_api.to_canonical_column(column)

    def validate_staged_data(
        self,
        rdbms_part_cols,
        rdbms_columns,
        expected_rows,
        staging_columns,
    ):
        """Validate the staged data before we insert it into the final backend table.
        There is scope for this to need a backend specific override but for the time being it is common
        across all backends.
        """
        self._validate_staged_data(
            rdbms_part_cols,
            rdbms_columns,
            expected_rows,
            staging_columns,
            log_profile=self._log_profile_after_verification_queries,
        )

    def view_exists(self):
        return self._db_api.view_exists(self.db_name, self.table_name)

    def _warning(self, msg):
        self._messages.warning(msg)
        logger.warn(msg)

    # Final table enforced methods/properties

    @abstractmethod
    def create_db(self, with_terminator=False) -> list:
        pass

    @abstractmethod
    def get_default_location(self):
        """Return the default location for current backend table when the backend supports per table FS locations.
        Only applies to Hadoop based backends.
        """
        pass

    @abstractmethod
    def get_staging_table_location(self):
        pass

    @abstractmethod
    def result_cache_area_exists(self):
        pass

    @abstractmethod
    def staging_area_exists(self):
        pass

    @abstractmethod
    def synthetic_bucket_data_type(self):
        pass

    @abstractmethod
    def synthetic_bucket_filter_capable_column(self, backend_column):
        """Returns the metadata method and SQL expression pattern used to generate the bucket id
        Currently only enabling bucket filtering for integral data
        When we consider opening this up to other types we should consider excluding TZ aware data because
        a backend upgrade could change the result of CASTs
        """

    ###########################################################################
    # PUBLIC METHODS - HIGH LEVEL STEP METHODS AND SUPPORTING ABSTRACT METHODS
    ###########################################################################

    def alter_table_sort_columns_step(self):
        def step_fn():
            return self._alter_table_sort_columns()

        return self._offload_step(command_steps.STEP_ALTER_TABLE, step_fn)

    def cleanup_staging_area_step(self):
        self._offload_step(
            command_steps.STEP_STAGING_CLEANUP, lambda: self.cleanup_staging_area()
        )

    def compute_final_table_stats_step(
        self, incremental_stats, materialized_join=False
    ):
        if self.table_stats_compute_supported():
            self._offload_step(
                command_steps.STEP_COMPUTE_STATS,
                lambda: self.compute_final_table_stats(
                    incremental_stats, materialized_join=materialized_join
                ),
                optional=True,
            )

    def create_backend_db_step(self) -> list:
        executed_commands = []
        if self.create_database_supported() and self._user_requested_create_backend_db:
            executed_commands: list = self._offload_step(
                command_steps.STEP_CREATE_DB, lambda: self.create_db()
            )
        return executed_commands

    def create_backend_table_step(self) -> list:
        executed_commands: list = self._offload_step(
            command_steps.STEP_CREATE_TABLE, lambda: self.create_backend_table()
        )
        return executed_commands

    def empty_staging_area_step(self, staging_file):
        self._offload_step(
            command_steps.STEP_STAGING_MINI_CLEANUP,
            lambda: self.empty_staging_area(staging_file),
        )

    def load_final_table_step(self, sync=None):
        self._offload_step(
            command_steps.STEP_FINAL_LOAD, lambda: self.load_final_table(sync=sync)
        )

    def setup_staging_area_step(self, staging_file):
        self._offload_step(
            command_steps.STEP_STAGING_SETUP,
            lambda: self.setup_staging_area(staging_file),
        )

    def validate_staged_data_step(
        self,
        rdbms_part_cols,
        rdbms_columns,
        expected_rows,
        staging_columns,
    ):
        self._offload_step(
            command_steps.STEP_VALIDATE_DATA,
            lambda: self.validate_staged_data(
                rdbms_part_cols,
                rdbms_columns,
                expected_rows,
                staging_columns,
            ),
        )

    def validate_type_conversions_step(self, staging_columns: list):
        self._offload_step(
            command_steps.STEP_VALIDATE_CASTS,
            lambda: self.validate_type_conversions(staging_columns),
        )

    @abstractmethod
    def cleanup_staging_area(self):
        pass

    @abstractmethod
    def compute_final_table_stats(self, incremental_stats, materialized_join=False):
        pass

    @abstractmethod
    def create_backend_table(self) -> list:
        pass

    @abstractmethod
    def empty_staging_area(self, staging_file):
        pass

    @abstractmethod
    def load_final_table(self, sync=None):
        """Copy data from the staged load table into the final backend table.
        Each backend has its own SQL for doing this.
        """
        pass

    @abstractmethod
    def load_materialized_join(
        self,
        threshold_cols=None,
        gte_threshold_vals=None,
        lt_threshold_vals=None,
        insert_predicates=None,
        sync=None,
    ):
        pass

    @abstractmethod
    def partition_function_requires_granularity(self):
        pass

    @abstractmethod
    def post_transport_tasks(self, staging_file):
        pass

    @abstractmethod
    def setup_result_cache_area(self):
        pass

    @abstractmethod
    def setup_staging_area(self, staging_file):
        pass

    @abstractmethod
    def validate_type_conversions(self, staging_columns: list):
        pass

    # Capability config

    def bucket_hash_column_supported(self):
        return self._db_api.bucket_hash_column_supported()

    def canonical_date_supported(self):
        return self._db_api.canonical_date_supported()

    def canonical_float_supported(self):
        return self._db_api.canonical_float_supported()

    def canonical_time_supported(self):
        return self._db_api.canonical_time_supported()

    def column_stats_set_supported(self):
        return self._db_api.column_stats_set_supported()

    def create_database_supported(self):
        return self._db_api.create_database_supported()

    def goe_column_transformations_supported(self):
        return self._db_api.goe_column_transformations_supported()

    def goe_join_pushdown_supported(self):
        return self._db_api.goe_join_pushdown_supported()

    def goe_materialized_join_supported(self):
        return self._db_api.goe_materialized_join_supported()

    def goe_partition_functions_supported(self):
        return self._db_api.goe_partition_functions_supported()

    def goe_udfs_supported(self):
        return self._db_api.goe_udfs_supported()

    def nan_supported(self):
        return self._db_api.nan_supported()

    def not_null_column_supported(self):
        return self._db_api.not_null_column_supported()

    def partition_by_column_supported(self):
        return self._db_api.partition_by_column_supported()

    def partition_by_string_supported(self):
        return self._db_api.partition_by_string_supported()

    def query_sample_clause_supported(self):
        return self._db_api.query_sample_clause_supported()

    def schema_evolution_supported(self):
        return self._db_api.schema_evolution_supported()

    def sorted_table_supported(self):
        return self._db_api.sorted_table_supported()

    def sorted_table_modify_supported(self):
        return self._db_api.sorted_table_modify_supported()

    def synthetic_partitioning_supported(self):
        return self._db_api.synthetic_partitioning_supported()

    def synthetic_partition_numbers_are_string(self):
        return self._db_api.synthetic_partition_numbers_are_string()

    def table_stats_compute_supported(self):
        return self._db_api.table_stats_compute_supported()

    def table_stats_get_supported(self):
        return self._db_api.table_stats_get_supported()

    def table_stats_set_supported(self):
        return self._db_api.table_stats_set_supported()
