#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

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

""" BackendTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests.
"""

from abc import ABCMeta, abstractmethod
import logging
import subprocess
from subprocess import PIPE, STDOUT
import sys
import time

from goe.offload.column_metadata import (
    CanonicalColumn,
    match_table_column,
    GOE_TYPE_DATE,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_VARIABLE_STRING,
)
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from tests.testlib.test_framework import test_constants


class BackendTestingApiException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

STORY_TEST_BACKEND_BLOB_COL = "blob_col"
STORY_TEST_BACKEND_DOUBLE_COL = "double_col"
STORY_TEST_BACKEND_INT_1_COL = "int_1_col"
STORY_TEST_BACKEND_INT_2_COL = "int_2_col"
STORY_TEST_BACKEND_INT_4_COL = "int_4_col"
STORY_TEST_BACKEND_INT_8_COL = "int_8_col"
STORY_TEST_BACKEND_DATE_COL = "date_col"
STORY_TEST_BACKEND_DATETIME_COL = "datetime_col"
STORY_TEST_BACKEND_DECIMAL_PS_COL = "decimal_col1"
STORY_TEST_BACKEND_DECIMAL_DEF_COL = "decimal_col2"
STORY_TEST_BACKEND_RAW_COL = "raw_col"
STORY_TEST_BACKEND_VAR_STR_COL = "var_str_col"
STORY_TEST_BACKEND_VAR_STR_LONG_COL = "var_str_clob_col"
STORY_TEST_BACKEND_FIX_STR_COL = "fix_str_col"
STORY_TEST_BACKEND_TIMESTAMP_COL = "ts_col"
STORY_TEST_BACKEND_TIMESTAMP_TZ_COL = "tstz_col"
# This is useful for testing --detect-sizes on empty data
STORY_TEST_BACKEND_NULL_STR_COL = "null_string_col"

STORY_TEST_OFFLOAD_NUMS_BARE_NUM = "bare_num"
STORY_TEST_OFFLOAD_NUMS_BARE_FLT = "bare_flt"
STORY_TEST_OFFLOAD_NUMS_NUM_4 = "num_4"
STORY_TEST_OFFLOAD_NUMS_NUM_18 = "num_18"
STORY_TEST_OFFLOAD_NUMS_NUM_19 = "num_19"
STORY_TEST_OFFLOAD_NUMS_NUM_3_2 = "num_3_2"
STORY_TEST_OFFLOAD_NUMS_NUM_13_3 = "num_13_3"
STORY_TEST_OFFLOAD_NUMS_NUM_16_1 = "num_16_1"
STORY_TEST_OFFLOAD_NUMS_NUM_20_5 = "num_20_5"
STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4 = "num_star_4"
STORY_TEST_OFFLOAD_NUMS_NUM_3_5 = "num_3_5"
STORY_TEST_OFFLOAD_NUMS_NUM_10_M5 = "num_10_m5"
STORY_TEST_OFFLOAD_NUMS_DEC_10_0 = "dec_10_0"
STORY_TEST_OFFLOAD_NUMS_DEC_13_9 = "dec_13_9"
STORY_TEST_OFFLOAD_NUMS_DEC_15_9 = "dec_15_9"
STORY_TEST_OFFLOAD_NUMS_DEC_36_3 = "dec_36_3"
STORY_TEST_OFFLOAD_NUMS_DEC_37_3 = "dec_37_3"
STORY_TEST_OFFLOAD_NUMS_DEC_38_3 = "dec_38_3"

TRANSIENT_QUERY_RERUN_MARKER = "Re-running runnable due to transient error"
TRANSIENT_QUERY_RERUN_PAUSE = 2


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def subproc_cmd(cmd, opts, messages, cwd=None, env=None):
    messages.log("Shell cmd: " + " ".join(cmd), detail=VVERBOSE)

    if opts.execute:
        proc = subprocess.Popen(cmd, stdout=PIPE, stderr=STDOUT, cwd=cwd, env=env)
        output = ""
        for line in proc.stdout:
            line = line.decode()
            output += line

            messages.log(line.strip(), detail=VVERBOSE)
            if not opts.vverbose:
                sys.stdout.write(".")
                sys.stdout.flush()

        if not opts.vverbose and not opts.quiet and output:
            sys.stdout.write("\n")
            sys.stdout.flush()

        cmd_returncode = proc.wait()
        if cmd_returncode and not opts.vverbose and not opts.quiet:
            sys.stdout.write(output)

        return cmd_returncode, output
    else:
        return 0, ""


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendTestingApiInterface
###########################################################################


class BackendTestingApiInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for backend specific sub-classes"""

    def __init__(
        self,
        connection_options,
        backend_type,
        messages,
        dry_run=False,
        no_caching=False,
        do_not_connect=False,
    ):
        assert connection_options
        assert messages
        self._connection_options = connection_options
        self._backend_type = backend_type
        self._messages = messages
        self._dry_run = dry_run
        self._db_api = backend_api_factory(
            backend_type,
            self._connection_options,
            self._messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

    ###########################################################################
    # PRIVATE METHODS
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

    def _canonical_integer_precision(self, data_type):
        assert data_type
        precision_map = {
            GOE_TYPE_INTEGER_1: 2,
            GOE_TYPE_INTEGER_2: 4,
            GOE_TYPE_INTEGER_4: 9,
            GOE_TYPE_INTEGER_8: 18,
            GOE_TYPE_INTEGER_38: 38,
        }
        return precision_map.get(data_type)

    def _find_source_column_for_create_partitioned_test_table(self, list_of_columns):
        """Looks for a date based column to use as a source for a YYYYMM column"""
        partition_source_column = match_table_column("time_id", list_of_columns)
        if partition_source_column:
            return partition_source_column
        date_based = [_ for _ in list_of_columns if _.is_date_based()]
        if date_based:
            return date_based[0]
        raise BackendTestingApiException(
            "No source column found for test partitioned table"
        )

    def _goe_type_mapping_column_name(self, *args, uppercase=True):
        """The case of columns is not particularly important, GOE will maintain either.
        However it looks nicer if the COL_ prefix and datatype elements are the same case, hence uppercase arg.
        """
        assert args
        col_name = "COL_" + "_".join(args).replace(" ", "_")
        # SS@2021-10-08 Remove this hack to prevent a conversion view on Synapse once GOE-2140 is resolved
        col_name = col_name.replace("LARGE_BINARY", "LARGE_BIN")
        return col_name.upper() if uppercase else col_name.lower()

    def _goe_type_mapping_interval_ds_test_values(self):
        """Just some test values used by all backends."""
        return [
            "+689991265 07:06:06.298066000",
            "-974757863 13:30:50.716052102",
            "+713917392 23:02:50",
        ]

    def _goe_type_mapping_interval_ym_test_values(self):
        """Just some test values used by all backends."""
        return ["-104258952-04", "+215028206-04", "+99-00"]

    def _select_single_non_null_value_sql_template(self):
        """Simple SQL query template with 4 %s markers.
        Individual backends may override this when they have different syntax.
        """
        return "SELECT %s FROM %s WHERE %s IS NOT NULL%s LIMIT 1"

    def _select_single_non_null_value_common(
        self,
        db_name,
        table_name,
        column_name,
        project_expression,
        null_ne_empty_str=True,
    ):
        """This is used in testing but all backends need to consciously decide whether to support it or not
        The parameters are simply put together to construct a query like:
            SELECT project_expression FROM db_name.table_name WHERE column_name IS NOT NULL
        project_expression: Could be MAX(column_name) or MEDIAN(column_name), something like that
        null_ne_empty_str: Just incase some future backend matches Oracle in treating '' == NULL
        """
        assert db_name and table_name
        assert column_name and project_expression

        empty_string_clause = ""
        if self._db_api.exists(db_name, table_name):
            columns = self.get_columns(db_name, table_name)
            if null_ne_empty_str and [
                _
                for _ in columns
                if _.name.lower() == column_name.lower() and _.is_string_based()
            ]:
                empty_string_clause = " AND %s != ''" % self.enclose_identifier(
                    column_name
                )

        sql = self._select_single_non_null_value_sql_template() % (
            project_expression,
            self._db_api.enclose_object_reference(db_name, table_name),
            self.enclose_identifier(column_name),
            empty_string_clause,
        )

        row = self._db_api.execute_query_fetch_one(sql, log_level=VERBOSE)
        return row[0] if row else None

    def _transient_query_error_identification_global_strings(self) -> list:
        """A list of non-backend implementation specific strings used to identify exceptions that are worth a retry."""
        return [
            # Oracle external table timeout, can happen when backend query does not respond.
            'smart_connector.sh encountered error "pipe read timeout"',
            # JDBC connection pool timeout (applies to any backend where we connect via JDBC).
            "HikariPool-1 - Connection is not available, request timed out",
        ]

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def add_columns(self, db_name, table_name, column_tuples, sync=None):
        return self._db_api.add_columns(db_name, table_name, column_tuples, sync=sync)

    def backend_test_type_canonical_string(self):
        """A number of tests need a string type for testing, this method avoids
        hard coding in multiple places.
        """
        return self._db_api.generic_string_data_type()

    def backend_type(self):
        return self._backend_type

    def backend_version(self):
        return self._db_api.backend_version()

    def bucket_hash_column_supported(self):
        return self._db_api.bucket_hash_column_supported()

    def canonical_date_supported(self):
        return self._db_api.canonical_date_supported()

    def canonical_time_supported(self):
        return self._db_api.canonical_time_supported()

    def canonical_float_supported(self):
        return self._db_api.canonical_float_supported()

    def case_sensitive_identifiers(self):
        return self._db_api.case_sensitive_identifiers()

    def compute_stats(
        self,
        db_name,
        table_name,
        incremental=None,
        for_columns=False,
        partition_tuples=None,
        sync=None,
    ):
        return self._db_api.compute_stats(
            db_name,
            table_name,
            incremental=incremental,
            for_columns=for_columns,
            partition_tuples=partition_tuples,
            sync=sync,
        )

    def create_database_supported(self):
        return self._db_api.create_database_supported()

    def create_table(
        self,
        db_name,
        table_name,
        column_list,
        partition_column_names,
        storage_format=None,
        location=None,
        external=False,
        table_properties=None,
        sort_column_names=None,
        without_db_name=False,
        sync=None,
    ):
        return self._db_api.create_table(
            db_name,
            table_name,
            column_list,
            partition_column_names,
            storage_format=storage_format,
            location=location,
            external=external,
            table_properties=table_properties,
            sort_column_names=sort_column_names,
            without_db_name=without_db_name,
            sync=sync,
        )

    def create_test_partition_functions(self, db_name, udf=None):
        """Create partition function UDFs required to support Orchestration and Hybrid Query testing.
        Each backend defines the UDF details via a private method _define_test_partition_function.
        Not converting case here when creating because then every other consuming test needs to remember
        to do a case conversion and not just use the constants. Unnecessary for test objects.
        """
        if udf:
            udfs = udf if isinstance(udf, list) else [udf]
        else:
            udfs = [
                test_constants.PARTITION_FUNCTION_TEST_FROM_INT8,
                test_constants.PARTITION_FUNCTION_TEST_FROM_DEC1,
                test_constants.PARTITION_FUNCTION_TEST_FROM_DEC2,
                test_constants.PARTITION_FUNCTION_TEST_FROM_STRING,
            ]
        for udf_name in udfs:
            return_type, argument_type, udf_body = self._define_test_partition_function(
                udf_name
            )
            if udf_body:
                self.create_udf(
                    db_name,
                    udf_name,
                    return_type,
                    [("p_arg", argument_type)],
                    udf_body,
                    or_replace=True,
                )

    def create_view(
        self,
        db_name,
        view_name,
        column_tuples,
        ansi_joined_tables,
        filter_clauses=None,
        sync=None,
    ):
        return self._db_api.create_view(
            db_name,
            view_name,
            column_tuples,
            ansi_joined_tables,
            filter_clauses=filter_clauses,
            sync=sync,
        )

    def create_udf(
        self,
        db_name,
        udf_name,
        return_data_type,
        parameter_tuples,
        udf_body,
        or_replace=False,
        spec_as_string=None,
        sync=None,
        log_level=VERBOSE,
    ):
        return self._db_api.create_udf(
            db_name,
            udf_name,
            return_data_type,
            parameter_tuples,
            udf_body,
            or_replace=or_replace,
            spec_as_string=spec_as_string,
            sync=sync,
            log_level=log_level,
        )

    def current_date_sql_expression(self):
        return self._db_api.current_date_sql_expression()

    def data_type_accepts_length(self, data_type):
        return self._db_api.data_type_accepts_length(data_type)

    def database_exists(self, db_name):
        return self._db_api.database_exists(db_name)

    def default_date_based_partition_granularity(self):
        return self._db_api.default_date_based_partition_granularity()

    def default_storage_compression(self, storage_compression, storage_format):
        return self._db_api.default_storage_compression(
            storage_compression, storage_format
        )

    def default_storage_format(self):
        return self._db_api.default_storage_format()

    def drop(self, db_name, object_name, purge=False, sync=None):
        return self._db_api.drop(db_name, object_name, purge=purge, sync=sync)

    def drop_column_supported(self):
        return self._db_api.drop_column_supported()

    def drop_table(self, db_name, table_name, purge=False, sync=None):
        return self._db_api.drop_table(db_name, table_name, purge=purge, sync=sync)

    def drop_view(self, db_name, view_name, sync=None):
        return self._db_api.drop_view(db_name, view_name, sync=sync)

    def enclose_identifier(self, identifier):
        return self._db_api.enclose_identifier(identifier)

    def enclose_object_reference(self, db_name, object_name):
        return self._db_api.enclose_object_reference(db_name, object_name)

    def execute_ddl(self, sql, sync=None, query_options=None, log_level=VERBOSE):
        return self._db_api.execute_ddl(
            sql, sync=sync, query_options=query_options, log_level=log_level
        )

    def execute_query_fetch_all(
        self, sql, query_options=None, log_level=None, query_params=None
    ):
        return self._db_api.execute_query_fetch_all(
            sql,
            query_options=query_options,
            log_level=log_level,
            query_params=query_params,
        )

    def execute_query_fetch_one(
        self, sql, query_options=None, log_level=None, query_params=None
    ):
        return self._db_api.execute_query_fetch_one(
            sql,
            query_options=query_options,
            log_level=log_level,
            query_params=query_params,
        )

    def extract_date_part_sql_expression(self, date_part, column):
        return self._db_api.extract_date_part_sql_expression(date_part, column)

    def filesystem_scheme_hdfs_supported(self):
        return self._db_api.filesystem_scheme_hdfs_supported()

    def format_query_parameter(self, param_name):
        return self._db_api.format_query_parameter(param_name)

    def gen_column_object(self, column_name, **kwargs):
        return self._db_api.gen_column_object(column_name, **kwargs)

    def gen_default_numeric_column(self, column_name, data_scale=None):
        if data_scale is None:
            return self._db_api.gen_default_numeric_column(column_name)
        else:
            return self._db_api.gen_default_numeric_column(
                column_name, data_scale=data_scale
            )

    def get_column(self, db_name, table_name, column_name):
        return self._db_api.get_column(db_name, table_name, column_name)

    def get_column_names(self, db_name, table_name, conv_fn=None):
        return self._db_api.get_column_names(db_name, table_name, conv_fn=conv_fn)

    def get_columns(self, db_name, table_name):
        return self._db_api.get_columns(db_name, table_name)

    def get_partition_columns(self, db_name, table_name):
        return self._db_api.get_partition_columns(db_name, table_name)

    def get_distinct_column_values(
        self, db_name, table_name, column_name_list, order_results=False
    ):
        return self._db_api.get_distinct_column_values(
            db_name, table_name, column_name_list, order_results=order_results
        )

    def get_max_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
        not_when_dry_running=False,
    ):
        return self._db_api.get_max_column_values(
            db_name,
            table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            optimistic_prune_clause=optimistic_prune_clause,
            not_when_dry_running=not_when_dry_running,
        )

    def get_max_column_length(self, db_name, table_name, column_name):
        return self._db_api.get_max_column_length(db_name, table_name, column_name)

    def get_non_synthetic_columns(self, db_name, table_name):
        return self._db_api.get_non_synthetic_columns(db_name, table_name)

    def get_table_and_partition_stats(self, db_name, table_name, as_dict=False):
        return self._db_api.get_table_and_partition_stats(
            db_name, table_name, as_dict=as_dict
        )

    def get_table_location(self, db_name, table_name):
        return self._db_api.get_table_location(db_name, table_name)

    def get_table_partition_count(self, db_name, table_name):
        return self._db_api.get_table_partition_count(db_name, table_name)

    def get_table_row_count(self, db_name, table_name, filter_clause=None):
        return self._db_api.get_table_row_count(
            db_name, table_name, filter_clause=filter_clause
        )

    def get_table_sort_columns(self, db_name, table_name):
        return self._db_api.get_table_sort_columns(db_name, table_name)

    def get_view_ddl(self, db_name, view_name):
        return self._db_api.get_view_ddl(db_name, view_name)

    def goe_identifiers_generated_table_col_specs(self):
        """Return a list of column specs matching how test.generated_tables expects and a list of column names.
        This is not how we want to pass column specs around but it matches existing code in test.
        Returned lists are sorted by column name.
        """
        goe_identifiers_cols = []
        canonical_columns = [
            CanonicalColumn("COL_NUM_UPPER", GOE_TYPE_INTEGER_8),
            CanonicalColumn("COL_STR_UPPER", GOE_TYPE_VARIABLE_STRING, data_length=10),
            CanonicalColumn("COL_DATE_UPPER", GOE_TYPE_DATE),
            CanonicalColumn("col_num_lower", GOE_TYPE_INTEGER_8),
            CanonicalColumn("col_str_lower", GOE_TYPE_VARIABLE_STRING, data_length=10),
            CanonicalColumn("col_date_lower", GOE_TYPE_DATE),
            CanonicalColumn("Col_Num_CamelCase", GOE_TYPE_INTEGER_8),
            CanonicalColumn(
                "Col_Str_CamelCase", GOE_TYPE_VARIABLE_STRING, data_length=10
            ),
            CanonicalColumn("Col_Date_CamelCase", GOE_TYPE_DATE),
        ]
        # Commented out lines below until GOE-2136 is actioned
        # if not self.identifier_contains_invalid_characters('col space'):
        #     # The backend supports spaces in names so add some columns.
        #     # This is a good test that all generated SQL has column enclosure.
        #     canonical_columns.extend([CanonicalColumn('COL NUM SPACE', GOE_TYPE_INTEGER_8),
        #                               CanonicalColumn('COL STR SPACE', GOE_TYPE_DATE),
        #                               CanonicalColumn('COL DATE SPACE', GOE_TYPE_DATE)])
        for column in canonical_columns:
            backend_column = self._db_api.from_canonical_column(column)
            if column.is_number_based():
                literals = [1, 2, 3]
            elif column.is_string_based():
                literals = ["blah1", "blah2", "blah3"]
            else:
                literals = None
            goe_identifiers_cols.append(
                {"column": backend_column, "literals": literals}
            )
        goe_identifiers_names = [_["column"].name for _ in goe_identifiers_cols]
        return goe_identifiers_cols, goe_identifiers_names

    def goe_type_mapping_test_columns(self):
        """Return a list of column names defined for testing"""
        definitions = self._goe_type_mapping_column_definitions()
        return sorted(definitions.keys())

    def goe_type_mapping_present_options(self):
        """Return a dictionary of present datatype control options/values for testing purposes."""
        definitions = self._goe_type_mapping_column_definitions()
        present_options = {}
        for col_dict in definitions.values():
            if col_dict.get("present_options"):
                opts = col_dict["present_options"]
                self._debug(
                    "Processing opts for %s: %s" % (col_dict["column"].name, str(opts))
                )
                for opt in opts:
                    if opt in present_options and isinstance(
                        present_options[opt], list
                    ):
                        assert isinstance(
                            opts[opt], list
                        ), "Expected type of option value is list, not: %s" % type(
                            opts[opt]
                        )
                        present_options[opt] += opts[opt]
                    elif opt in present_options:
                        assert isinstance(
                            opts[opt], str
                        ), "Expected type of option value is str, not: %s" % type(
                            opts[opt]
                        )
                        present_options[opt] += "," + opts[opt]
                    else:
                        present_options[opt] = opts[opt]
        return present_options

    def goe_column_transformations_supported(self):
        return self._db_api.goe_column_transformations_supported()

    def goe_join_pushdown_supported(self):
        return self._db_api.goe_join_pushdown_supported()

    def goe_materialized_join_supported(self):
        return self._db_api.goe_materialized_join_supported()

    def goe_partition_functions_supported(self):
        return self._db_api.goe_partition_functions_supported()

    def goe_sequence_table_supported(self):
        return self._db_api.goe_sequence_table_supported()

    def identifier_contains_invalid_characters(self, identifier):
        return self._db_api.identifier_contains_invalid_characters(identifier)

    def insert_literal_values(
        self,
        db_name,
        table_name,
        literal_list,
        column_list=None,
        max_rows_per_insert=250,
        split_by_cr=True,
    ):
        return self._db_api.insert_literal_values(
            db_name,
            table_name,
            literal_list,
            column_list=column_list,
            max_rows_per_insert=max_rows_per_insert,
            split_by_cr=split_by_cr,
        )

    def insert_table_as_select(
        self,
        db_name,
        table_name,
        from_db_name,
        from_table_name,
        select_expr_tuples,
        partition_expr_tuples,
        compute_stats=None,
        filter_clauses=None,
    ):
        """Insert data from one table into another for test setup purposes.
        This should hopefully cater for all new backends, note there is a Hive override.
        """
        insert_sql = self._db_api.gen_insert_select_sql_text(
            db_name,
            table_name,
            from_db_name,
            from_table_name,
            select_expr_tuples,
            partition_expr_tuples,
            filter_clauses=filter_clauses,
        )
        executed_sqls = self._db_api.execute_dml(insert_sql)
        if compute_stats:
            executed_sqls.extend(self._db_api.compute_stats(db_name, table_name))
        return executed_sqls

    def is_valid_sort_data_type(self, data_type):
        return self._db_api.is_valid_sort_data_type(data_type)

    def is_valid_staging_format(self, staging_format):
        return self._db_api.is_valid_staging_format(staging_format)

    def is_valid_storage_compression(self, storage_compression, storage_format):
        return self._db_api.is_valid_storage_compression(
            storage_compression, storage_format
        )

    def is_valid_storage_format(self, storage_format):
        return self._db_api.is_valid_storage_format(storage_format)

    def is_view(self, db_name, view_name):
        return self._db_api.is_view(db_name, view_name)

    def list_databases(self, db_name_filter=None, case_sensitive=True):
        return self._db_api.list_databases(
            db_name_filter=db_name_filter, case_sensitive=case_sensitive
        )

    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        return self._db_api.list_tables(
            db_name, table_name_filter=table_name_filter, case_sensitive=case_sensitive
        )

    def load_db_transport_supported(self):
        return self._db_api.load_db_transport_supported()

    def max_column_name_length(self):
        return self._db_api.max_column_name_length()

    def max_decimal_integral_magnitude(self):
        return self._db_api.max_decimal_integral_magnitude()

    def max_decimal_precision(self):
        return self._db_api.max_decimal_precision()

    def max_decimal_scale(self, data_type=None):
        return self._db_api.max_decimal_scale(data_type=data_type)

    def max_datetime_scale(self):
        return self._db_api.max_datetime_scale()

    def max_partition_columns(self):
        return self._db_api.max_partition_columns()

    def max_sort_columns(self):
        return self._db_api.max_sort_columns()

    def max_table_name_length(self):
        return self._db_api.max_table_name_length()

    def goe_wide_max_test_column_count(self):
        """Used to limit the columns in frontend table GOE_WIDE. Synapse has an override.
        None means there's no specific limit, i.e. we'll leave it up to the frontend.
        """
        return None

    def min_datetime_value(self):
        return self._db_api.min_datetime_value()

    def nan_supported(self):
        return self._db_api.nan_supported()

    def nanoseconds_supported(self):
        return self._db_api.nanoseconds_supported()

    def not_null_column_supported(self):
        return self._db_api.not_null_column_supported()

    def native_integer_types(self):
        return self._db_api.native_integer_types()

    def parameterized_queries_supported(self):
        return self._db_api.parameterized_queries_supported()

    def partition_by_column_supported(self):
        return self._db_api.partition_by_column_supported()

    def partition_by_string_supported(self):
        return self._db_api.partition_by_string_supported()

    def partition_column_requires_synthetic_column(self, backend_column, granularity):
        return self._db_api.partition_column_requires_synthetic_column(
            backend_column, granularity
        )

    def refresh_functions_supported(self):
        return self._db_api.refresh_functions_supported()

    def schema_evolution_supported(self):
        return self._db_api.schema_evolution_supported()

    def sequence_table_max(self, db_name, table_name):
        return self._db_api.sequence_table_max(db_name, table_name)

    def sorted_table_supported(self):
        return self._db_api.sorted_table_supported()

    def sorted_table_modify_supported(self):
        return self._db_api.sorted_table_modify_supported()

    def sql_microsecond_predicate_supported(self):
        return self._db_api.sql_microsecond_predicate_supported()

    def supported_date_based_partition_granularities(self):
        return self._db_api.supported_date_based_partition_granularities()

    def synthetic_partitioning_supported(self):
        return self._db_api.synthetic_partitioning_supported()

    def synthetic_partition_numbers_are_string(self):
        return self._db_api.synthetic_partition_numbers_are_string()

    def table_exists(self, db_name, table_name):
        return self._db_api.table_exists(db_name, table_name)

    def table_stats_get_supported(self):
        return self._db_api.table_stats_get_supported()

    def table_stats_set_supported(self):
        return self._db_api.table_stats_set_supported()

    def table_stats_compute_supported(self):
        return self._db_api.table_stats_compute_supported()

    def target_version(self):
        return self._db_api.target_version()

    def test_setup_seconds_delay(self):
        """Length of pause after setting up a test before returning control to the next step.
        This is to prevent error below which we see if we get to Transport step too quickly:
            ORA-01466: unable to read data - table definition has changed
        """
        return 5

    def to_canonical_column(self, backend_column):
        return self._db_api.to_canonical_column(backend_column)

    def transient_error_rerunner(self, run_fn, max_retries=1, pause_seconds=None):
        """Runs callable run_fn() and, if an exception is thrown, retries if the reason is a transient backend issue.
        Obviously run_fn() must me idempotent, ideally a read only operation such as a Hybrid Query.
        """
        assert callable(run_fn)
        if pause_seconds is None:
            pause_seconds = TRANSIENT_QUERY_RERUN_PAUSE
        for i in range(max_retries + 1):
            try:
                return run_fn()
            except Exception as exc:
                transient_errors = [
                    _
                    for _ in self.transient_query_error_identification_strings()
                    if _ in str(exc)
                ]
                if transient_errors and i < max_retries:
                    self._log(
                        "{}, exception: {}".format(
                            TRANSIENT_QUERY_RERUN_MARKER, str(exc)
                        ),
                        detail=VERBOSE,
                    )
                    self._log(
                        "Matched transient error is: {}".format(str(transient_errors)),
                        detail=VERBOSE,
                    )
                    self._log(
                        "Sleeping for {} seconds before re-run".format(
                            str(pause_seconds)
                        ),
                        detail=VERBOSE,
                    )
                    time.sleep(pause_seconds)
                else:
                    raise

    def story_test_table_extra_col_setup(self):
        """Return story_test_table_extra_col_info so it can be passed into create_table()."""
        extra_col_info = self.story_test_table_extra_col_info()
        return [
            (col_attribs["sql_expression"], col_name)
            for col_name, col_attribs in extra_col_info.items()
        ]

    def table_distribution(self, db_name, table_name):
        return self._db_api.table_distribution(db_name, table_name)

    def to_backend_literal(self, py_val, data_type=None):
        return self._db_api.to_backend_literal(py_val, data_type=data_type)

    def unit_test_single_row_sql_text(
        self, db_name, table_name, column_name, row_limit=None, where_clause=None
    ):
        """Simple SQL query text. Individual backends may override this if they have different syntax."""
        db_table = self.enclose_object_reference(db_name, table_name)
        where_clause = where_clause or ""
        limit_clause = f" LIMIT {row_limit}" if row_limit else ""
        return f"SELECT {column_name} FROM {db_table} {where_clause}{limit_clause}"

    def valid_staging_formats(self):
        return self._db_api.valid_staging_formats()

    def view_exists(self, db_name, view_name):
        return self._db_api.view_exists(db_name, view_name)

    # Enforced methods/properties

    @abstractmethod
    def _goe_type_mapping_column_definitions(self, filter_column=None):
        pass

    @abstractmethod
    def _define_test_partition_function(self, udf_name):
        """Return the following UDF attributes or raise NotImplementedError:
            return_type, argument_type, udf_body
        Returning None, None, None indicates not to create that specific UDF
        """

    @abstractmethod
    def backend_test_type_canonical_date(self):
        """A number of tests need the most appropriate date (no-time part) type for testing,
        this method avoids hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_time(self):
        """A number of tests need the time type for testing, this method avoids hardcoding."""

    @abstractmethod
    def backend_test_type_canonical_timestamp(self):
        """A number of tests need the most appropriate datetime type for testing,
        this method avoids hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_timestamp_tz(self):
        """A number of tests need the most appropriate time zoned date type for testing,
        this method avoids hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_decimal(self):
        """A number of tests need a decimal data type for testing, this method avoids
        hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_int_2(self):
        """A number of tests need an integral type for testing, this method avoids
        hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_int_4(self):
        """A number of tests need an integral type for testing, this method avoids
        hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_int_8(self):
        """A number of tests need an integral type for testing, this method avoids
        hardcoding in multiple places.
        """

    @abstractmethod
    def backend_test_type_canonical_int_38(self):
        """A number of tests need an integral type for testing, this method avoids
        hardcoding in multiple places.
        """

    @abstractmethod
    def create_backend_offload_location(self, goe_user=None):
        """In old parlance this will create HDFS_DATA in HDFS
        For non-Hadoop backends it may (or may not) do something else
        """

    @abstractmethod
    def create_partitioned_test_table(
        self,
        db_name,
        table_name,
        source_table_name,
        storage_format,
        compute_stats=False,
        filter_clauses=None,
    ):
        """Create a partitioned test table from source_table partitioned by a year/month integral column."""

    @abstractmethod
    def create_table_as_select(
        self,
        db_name,
        table_name,
        storage_format,
        column_tuples,
        from_db_name=None,
        from_table_name=None,
        row_limit=None,
        compute_stats=None,
    ):
        """CTAS a table"""

    @abstractmethod
    def drop_column(self, db_name, table_name, column_name, sync=None):
        pass

    @abstractmethod
    def drop_database(self, db_name, cascade=False):
        pass

    @abstractmethod
    def expected_backend_column(
        self, canonical_column, override_used=None, decimal_padding_digits=None
    ):
        """Returns a backend column object that we might expect to see based on canonical_column"""

    @abstractmethod
    def expected_backend_precision_scale(
        self, canonical_column, decimal_padding_digits=None
    ):
        """For a decimal canonical column return a tuple of expected precision/scale.
        Returning None means no check is required, for example for non-decimal columns.
        """

    @abstractmethod
    def expected_canonical_to_backend_type_map(self, override_used=False):
        """Returns a dict mapping canonical data types to expected backend types.
        Returns:
            {'canonical-type-1': 'backend-type-1',
             'canonical-type-n': 'backend-type-n'}
        We do this instead of use from_canonical_column() because this is verifying that the function does the
        right thing.
        """

    @abstractmethod
    def expected_std_dim_offload_predicates(self) -> list:
        """Return a list of tuples of GOE offload predicates and expected backend predicate"""

    @abstractmethod
    def expected_std_dim_synthetic_offload_predicates(self) -> list:
        """Return a list of tuples of GOE offload predicates and expected backend predicate"""

    @abstractmethod
    def goe_type_mapping_generated_table_col_specs(self):
        """Return a list of column specs matching how test.generated_tables expects and a list of column names.
        This is not how we want to pass column specs around but it matches existing code in test.
        Returned lists are sorted by column name.
        """

    @abstractmethod
    def host_compare_sql_projection(self, column_list: list) -> str:
        """Return a SQL projection (CSV of column expressions) used to validate offloaded data.
        Timestamps:
            Because some systems do not have canonical dates or have differing scales for time elements all
            date based values must be normalised to UTC in format:
                'YYYY-MM-DD HH24:MI:SS.FFF +00:00'
        Intervals:
            Some clients do not support interval data types so convert them to string in the validation SQL.
        Numbers:
            All numbers need to be strings because:
                >>> -8461.633 == Decimal('-8461.633')
                False
        """

    @abstractmethod
    def load_table_fs_scheme_is_correct(self, load_db, table_name):
        pass

    @abstractmethod
    def partition_has_stats(
        self, db_name, table_name, partition_tuples, colstats=False
    ):
        pass

    @abstractmethod
    def rename_column(self, db_name, table_name, column_name, new_name, sync=None):
        pass

    @abstractmethod
    def select_single_non_null_value(
        self, db_name, table_name, column_name, project_expression
    ):
        """This is used in testing but all backends need to consciously decide whether to support it or not
        The parameters are simply put together to construct a query like:
            SELECT project_expression FROM db_name.table_name WHERE column_name IS NOT NULL
        project_expression: Could be MAX(column_name) or MEDIAN(column_name), something like that
        """

    @abstractmethod
    def smart_connector_test_command(self, db_name=None, table_name=None):
        """Returns a command we can run via smart_connect -iq in order to test it works end to end.
        db_name/table_name are optional, may be used by some backends, may be ignored by others.
        """

    @abstractmethod
    def sql_median_expression(self, db_name, table_name, column_name):
        """Some tests use SQL to get the median expression for a particular column, this method allows for backend variations.
        The method returns a string SQL expression with the column name embedded. For example:
            return 'APPX_MEDIAN(col_1)'
        """

    @abstractmethod
    def story_test_offload_nums_expected_backend_types(self, sampling_enabled=True):
        """Expected backend data types used in data type controls story"""

    @abstractmethod
    def story_test_table_extra_col_info(self):
        pass

    @abstractmethod
    def transient_query_error_identification_strings(self) -> list:
        """A list of strings used to identify exceptions that are worth a retry, for example BigQuery exception:
        describeTable: ERROR : 13: Read timed out
        """

    @abstractmethod
    def unit_test_query_options(self):
        """Return a dict of a valid query/session setting in order to unit test BackendApi"""
