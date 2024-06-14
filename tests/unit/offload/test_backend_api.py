# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" TestBackendApi: Unit test library to test API for all supported backends
    This is split into two categories
    1) For all possible backends test API calls that do not need to connect to the system
       Because there is no connection we can fake any backend and test functionality
       These classes have the system in the name: TestHiveBackendApi, TestImpalaBackendApi, etc
"""

from datetime import datetime
from unittest import TestCase, main
import re
import pytest

from numpy import datetime64

from goe.connect.connect_constants import (
    CONNECT_DETAIL,
    CONNECT_STATUS,
    CONNECT_TEST,
)
from goe.offload.backend_api import BackendApiException
from goe.offload.bigquery.bigquery_column import (
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_INT64,
    BIGQUERY_TYPE_TIMESTAMP,
)
from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    ColumnPartitionInfo,
    get_partition_columns,
)
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SPARK,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SYNAPSE,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.offload.offload_messages import OffloadMessages
from goe.util.misc_functions import add_suffix_in_same_case
from tests.testlib.test_framework.factory.backend_testing_api_factory import (
    backend_testing_api_factory,
)
from tests.unit.test_functions import (
    build_mock_options,
    optional_hadoop_dependency_exception,
    optional_snowflake_dependency_exception,
    optional_synapse_dependency_exception,
    FAKE_ORACLE_BQ_ENV,
    FAKE_ORACLE_HIVE_ENV,
    FAKE_ORACLE_IMPALA_ENV,
    FAKE_ORACLE_SNOWFLAKE_ENV,
    FAKE_ORACLE_SYNAPSE_ENV,
)


class TestBackendApi(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestBackendApi, self).__init__(*args, **kwargs)
        self.api = None
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.part_table = None
        self.connect_to_backend = None
        self.target = None

    def setUp(self):
        messages = OffloadMessages()
        self.api = backend_api_factory(
            self.target,
            self.config,
            messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_backend),
        )
        self.test_api = backend_testing_api_factory(
            self.target,
            self.config,
            messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_backend),
        )
        self.db = "any_db"
        self.table = "some_table"

    def _get_mock_config(self, mock_env: dict):
        return build_mock_options(mock_env)

    def _gen_string_column(self, name):
        string_type = self.test_api.backend_test_type_canonical_string()
        return self.api.gen_column_object(
            name,
            data_type=string_type,
            data_length=(
                10 if self.test_api.data_type_accepts_length(string_type) else None
            ),
        )

    def _get_udf_db(self):
        if self.config.udf_db:
            return self.config.udf_db
        else:
            if self.target in [DBTYPE_IMPALA, DBTYPE_HIVE]:
                return "default"
            else:
                return self.db

    def _test_add_columns(self):
        if self.connect_to_backend:
            column_list = [
                self.api.gen_default_numeric_column("col1"),
                self.api.gen_default_numeric_column("col2"),
            ]
            column_tuples = [(_.name, _.format_data_type()) for _ in column_list]
            self.api.add_columns(self.db, self.table, column_tuples, sync=True)

    def _test_backend_version(self):
        if self.connect_to_backend:
            # Some backends do not expose a version so we cannot assert on this fn
            self.api.backend_version()

    def _test_bigquery_dataset_project(self):
        if self.config.bigquery_dataset_project:
            self.assertEqual(
                self.api._backend_project_name(), self.config.bigquery_dataset_project
            )
            self.assertIn(
                self.config.bigquery_dataset_project, self.api._bq_dataset_id("some-db")
            )
        else:
            if self.connect_to_backend:
                # The project should still be set, it will come from the client default.
                self.assertIsNotNone(self.api._backend_project_name())

    def _test_check_backend_supporting_objects(self):
        if self.connect_to_backend:
            test_results = self.api.check_backend_supporting_objects(self.config)
            self.assertIsInstance(test_results, list)
            for test_dict in test_results:
                self.assertTrue(bool(test_dict))
                self.assertIsInstance(test_dict, dict)
                self.assertIn(CONNECT_DETAIL, test_dict)
                if test_dict[CONNECT_DETAIL]:
                    self.assertIsInstance(test_dict[CONNECT_DETAIL], str)
                self.assertIn(CONNECT_STATUS, test_dict)
                self.assertIsInstance(test_dict[CONNECT_STATUS], bool)
                self.assertIn(CONNECT_TEST, test_dict)
                self.assertIsInstance(test_dict[CONNECT_TEST], str)

    def _test_compute_stats(self):
        if self.connect_to_backend:
            try:
                self.api.compute_stats(self.db, self.table)
            except NotImplementedError:
                pass

    def _test_create_database(self):
        try:
            self.assertIsInstance(self.api.create_database(self.db), list)
            self.assertIsInstance(
                self.api.create_database(self.db, comment="Some comment"), list
            )
            self.assertIsInstance(
                self.api.create_database(
                    self.db,
                    properties={"location": "/some/place", "transient": True},
                ),
                list,
            )
        except NotImplementedError:
            pass

    def _test_create_table(self):
        column_list = [
            self.api.gen_column_object(
                "col1", data_type=self.test_api.backend_test_type_canonical_int_8()
            ),
            self.api.gen_default_numeric_column("col2"),
            self._gen_string_column("col3"),
        ]
        self.assertIsInstance(
            self.api.create_table(
                self.db,
                self.table,
                column_list,
                None,
                FILE_STORAGE_FORMAT_PARQUET,
                location="/some/location",
                table_properties={"some_property": "some_value"},
            ),
            list,
        )
        try:
            self.assertIsInstance(
                self.api.create_table(
                    self.db,
                    self.table,
                    column_list,
                    None,
                    FILE_STORAGE_FORMAT_AVRO,
                    location="/some/location",
                    external=True,
                    table_properties={"some_property": "some_value"},
                ),
                list,
            )
        except NotImplementedError:
            pass

        if self.api.partition_by_column_supported():
            if self.target == DBTYPE_BIGQUERY:
                partition_info = ColumnPartitionInfo(
                    position=0, granularity=10, range_start=0, range_end=1000
                )
                partition_column = self.api.gen_column_object(
                    "part_col",
                    data_type=self.test_api.backend_test_type_canonical_int_8(),
                    partition_info=partition_info,
                )
            else:
                partition_info = ColumnPartitionInfo(
                    position=0, granularity=10, range_start=0, range_end=1000, digits=6
                )
                partition_column = self.api.gen_column_object(
                    "goe_part_000010_col1",
                    data_type=self.test_api.backend_test_type_canonical_string(),
                    partition_info=partition_info,
                )
            part_col_list = [partition_column.name]
            column_list.append(partition_column)
            self.assertIsInstance(
                self.api.create_table(
                    self.db,
                    self.table,
                    column_list,
                    part_col_list,
                    FILE_STORAGE_FORMAT_PARQUET,
                    sort_column_names=["col2"],
                ),
                list,
            )

    def _test_create_view(self):
        column_list = [("col1", "col21_alias"), ("col2", "col2_alias")]
        filter_clause = "other_col < %s" % self.api.to_backend_literal(456)
        cmds = self.api.create_view(
            self.db,
            "some_view",
            column_list,
            self.db + "." + self.table,
            filter_clauses=[filter_clause],
        )
        self.assertIsInstance(cmds, list)
        self.assertTrue(bool(cmds))
        self.assertIn("some_view", cmds[-1])

    def _test_current_date_sql_expression(self):
        self.assertIsNotNone(self.api.current_date_sql_expression())
        if self.connect_to_backend:
            self.assertIsNotNone(
                self.api.execute_query_fetch_one(
                    "SELECT %s" % self.api.current_date_sql_expression()
                )
            )

    def _test_database_exists(self):
        if self.connect_to_backend:
            self.assertTrue(self.api.database_exists(self.db))
            self.assertFalse(self.api.database_exists("total-nonsense-db-blah"))

    def _test_derive_native_partition_info(self):
        if self.connect_to_backend:
            part_cols = get_partition_columns(
                self.api.get_partition_columns(self.db, self.part_table),
            )
            part_cols = [
                _ for _ in part_cols if not self.api.is_synthetic_partition_column(_)
            ]
            if part_cols:
                part_info = self.api.derive_native_partition_info(
                    self.db, self.part_table, part_cols[0], 1
                )
                self.assertIsNotNone(part_info)
                self.assertEqual(part_info.position, 1)
                part_info = self.api.derive_native_partition_info(
                    self.db, self.part_table, part_cols[0].name, 2
                )
                self.assertIsNotNone(part_info)
                self.assertEqual(part_info.position, 2)

    def _test_detect_column_has_fractional_seconds(self):
        if self.connect_to_backend:
            if self.target == DBTYPE_BIGQUERY:
                ts_column = [
                    _
                    for _ in self.api.get_columns(self.db, self.table)
                    if _.data_type in [BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_TIMESTAMP]
                ]
            else:
                ts_column = [
                    _
                    for _ in self.api.get_columns(self.db, self.table)
                    if _.is_date_based()
                ]
            if not ts_column:
                # No date based column available so don't test it
                return
            else:
                use_column = ts_column[0]
                self.api.detect_column_has_fractional_seconds(
                    self.db, self.table, use_column
                )

    def _test_drop_table(self):
        if self.connect_to_backend:
            cmds = self.api.drop_table(self.db, self.table, purge=False)
            self.assertIsInstance(cmds, list)
            self.assertTrue(bool(cmds))
            cmds = self.api.drop_table(self.db, self.table, purge=True)
            self.assertIsInstance(cmds, list)
            self.assertTrue(bool(cmds))

    def _test_drop_view(self):
        cmds = self.api.drop_view(self.db, "some_view")
        self.assertIsInstance(cmds, list)
        self.assertTrue(bool(cmds))
        self.assertIn("some_view", cmds[-1])

    def _test_enclose_identifier(self):
        self.api.enclose_identifier(self.db)

    def _test_enclose_object_reference(self):
        self.api.enclose_object_reference(self.db, self.table)

    def _test_execute_ddl(self):
        # Make a call without executing just to shake down Python logic
        self.assertIsInstance(
            self.api.execute_ddl(
                "DDL ON A TABLE",
                sync=True,
                profile=True,
                query_options=self.test_api.unit_test_query_options(),
            ),
            list,
        )

    def _test_execute_query_text(self, column_name, limit):
        return self.test_api.unit_test_single_row_sql_text(
            self.db, self.table, column_name, row_limit=limit
        )

    def _test_execute_query_fetch_all(self):
        if self.connect_to_backend:
            num_columns = [
                _
                for _ in self.api.get_columns(self.db, self.table)
                if _.is_number_based()
            ]
            column_name = num_columns[0].name if num_columns else "*"
            rows = self.api.execute_query_fetch_all(
                self._test_execute_query_text(column_name, 10)
            )
            self.assertIsInstance(rows, list)
            if rows:
                self.assertIsInstance(rows[0], (list, tuple))
            rows = self.api.execute_query_fetch_all(
                self._test_execute_query_text(column_name, 10), as_dict=True
            )
            self.assertIsInstance(rows, list)
            if rows:
                self.assertIsInstance(rows[0], dict)
            if self.api.parameterized_queries_supported() and num_columns:
                if self.api.backend_type() == DBTYPE_BIGQUERY:
                    params = [("num_value", num_columns[0].data_type, 42)]
                    where_clause = "WHERE %s < %s" % (
                        column_name,
                        self.api.format_query_parameter("num_value"),
                    )
                elif self.api.backend_type() == DBTYPE_SNOWFLAKE:
                    params = [42]
                    where_clause = "WHERE %s < :1" % column_name
                elif self.api.backend_type() == DBTYPE_SYNAPSE:
                    params = [42]
                    where_clause = "WHERE %s < %s" % (
                        column_name,
                        self.api.format_query_parameter("not-applicable"),
                    )
                else:
                    raise NotImplementedError(
                        "Missing a test for implementation: %s"
                        % self.api.backend_type()
                    )
                sql = self.test_api.unit_test_single_row_sql_text(
                    self.db,
                    self.table,
                    column_name,
                    row_limit=10,
                    where_clause=where_clause,
                )
                rows = self.api.execute_query_fetch_all(sql, query_params=params)
                self.assertIsInstance(rows, list)

    def _test_execute_query_fetch_one(self):
        if self.connect_to_backend:
            num_columns = [
                _
                for _ in self.api.get_columns(self.db, self.table)
                if _.is_number_based()
            ]
            column_name = num_columns[0].name if num_columns else "*"
            row = self.api.execute_query_fetch_one(
                self._test_execute_query_text(column_name, 1), time_sql=True
            )
            if row:
                self.assertIsInstance(row, (list, tuple))
            row = self.api.execute_query_fetch_one(
                self._test_execute_query_text(column_name, 1),
                time_sql=True,
                as_dict=True,
            )
            if row:
                self.assertIsInstance(row, dict)
            if self.api.parameterized_queries_supported() and num_columns:
                if self.api.backend_type() == DBTYPE_BIGQUERY:
                    params = [("num_value", num_columns[0].data_type, 42)]
                    where_clause = "WHERE %s < %s" % (
                        column_name,
                        self.api.format_query_parameter("num_value"),
                    )
                elif self.api.backend_type() == DBTYPE_SNOWFLAKE:
                    params = [42]
                    where_clause = "WHERE %s < :1" % column_name
                elif self.api.backend_type() == DBTYPE_SYNAPSE:
                    params = [42]
                    where_clause = "WHERE %s < %s" % (
                        column_name,
                        self.api.format_query_parameter("not-applicable"),
                    )
                else:
                    raise NotImplementedError(
                        "Missing a test for implementation: %s"
                        % self.api.backend_type()
                    )
                sql = self.test_api.unit_test_single_row_sql_text(
                    self.db,
                    self.table,
                    column_name,
                    row_limit=1,
                    where_clause=where_clause,
                )
                row = self.api.execute_query_fetch_one(sql, query_params=params)
                self.assertIsInstance(row, tuple)

    def _test_extract_date_part_sql_expression(self):
        self.assertRaises(
            AssertionError,
            lambda: self.api.extract_date_part_sql_expression("x", "col_name"),
        )
        self.assertRaises(
            AssertionError,
            lambda: self.api.extract_date_part_sql_expression("YEAR", 123),
        )
        for date_part in ["YEAR", "MONTH", "YEAR"]:
            self.assertIsInstance(
                self.api.extract_date_part_sql_expression(date_part, "col_name"), str
            )
            self.assertIsInstance(
                self.api.extract_date_part_sql_expression(
                    date_part.lower(), "col_name"
                ),
                str,
            )

    def _test_format_column_comparison(self):
        col1 = self.api.gen_default_numeric_column("some_col")
        self.assertIsInstance(self.api.format_column_comparison(col1, "=", col1), str)
        self.assertIsInstance(
            self.api.format_column_comparison(
                col1, "=", col1, left_alias="a", right_alias="b"
            ),
            str,
        )
        col2 = self._gen_string_column("str_col")
        self.assertIsInstance(self.api.format_column_comparison(col2, "=", col2), str)
        self.assertIsInstance(
            self.api.format_column_comparison(
                col2, "=", col2, left_alias="a", right_alias="b"
            ),
            str,
        )

    def _test_gen_insert_select_sql_text(self):
        column_tuples = [("123", "a_number"), ("'blah'", "a_string")]
        self.assertIsNotNone(
            self.api.gen_insert_select_sql_text(
                self.db,
                self.table,
                self.db,
                "some_other_table",
                column_tuples,
                filter_clauses=["col_1 < 12345"],
            )
        )

    def _test_gen_ctas_sql_text(self):
        column_tuples = [(123, "a_number"), ("'blah'", "a_string")]
        self.assertIsNotNone(
            self.api.gen_ctas_sql_text(
                self.db,
                self.table,
                FILE_STORAGE_FORMAT_PARQUET,
                column_tuples,
                from_db_name=self.db,
                from_table_name="other_table",
                row_limit=2,
            )
        )
        if self.api.backend_type() == DBTYPE_IMPALA:
            self.assertIsNotNone(
                self.api.gen_ctas_sql_text(
                    self.db,
                    self.table,
                    FILE_STORAGE_FORMAT_PARQUET,
                    column_tuples,
                    from_db_name=self.db,
                    from_table_name="other_table",
                    row_limit=2,
                    external=True,
                    table_properties={"GOE": "rocks"},
                )
            )

    def _test_gen_default_numeric_column(self):
        self.assertIsInstance(
            self.api.gen_default_numeric_column("some_col"), ColumnMetadataInterface
        )

    def _test_gen_native_range_partition_key_cast(self):
        def test_granularity_query(source_col, partition_info):
            part_col = source_col.clone()
            part_col.partition_info = partition_info
            cast_expr = self.api.gen_native_range_partition_key_cast(part_col)
            self.assertIsNotNone(cast_expr)
            row = self.api.execute_query_fetch_one(
                "SELECT %s FROM %s.%s LIMIT 1" % (cast_expr, self.db, self.table)
            )
            self.assertTrue(bool(row))

        try:
            if self.connect_to_backend:
                column_list = self.api.get_columns(self.db, self.table)
                date_column = [_ for _ in column_list if _.is_date_based()]
                if date_column:
                    date_column = date_column[0]
                    test_granularity_query(
                        date_column, ColumnPartitionInfo(position=0, granularity="Y")
                    )
                    test_granularity_query(
                        date_column, ColumnPartitionInfo(position=0, granularity="M")
                    )
                    test_granularity_query(
                        date_column, ColumnPartitionInfo(position=0, granularity="D")
                    )
                if self.api.backend_type() == DBTYPE_BIGQUERY:
                    int_column = [
                        _ for _ in column_list if _.data_type == BIGQUERY_TYPE_INT64
                    ]
                    if int_column:
                        int_column = int_column[0]
                        test_granularity_query(
                            int_column,
                            ColumnPartitionInfo(
                                position=0,
                                granularity=10,
                                range_start=0,
                                range_end=1000,
                            ),
                        )
        except NotImplementedError:
            pass

    def _test_gen_sql_text(self):
        column_names = ["col1", "col4"]
        filter_clauses = ["ts_col < timestamp '2011-03-01 00:00:00'"]
        self.assertIn(
            self.table.lower(),
            self.api.gen_sql_text(
                self.db,
                self.table,
                column_names=column_names,
                filter_clauses=filter_clauses,
            ).lower(),
        )
        self.assertIn(
            "col2",
            self.api.gen_sql_text(
                self.db,
                self.table,
                column_names=column_names,
                measures=["col2", "col3"],
                agg_fns=["min", "max", "sum"],
            ),
        )
        self.assertIn(
            "col2",
            self.api.gen_sql_text(
                self.db,
                self.table,
                measures=["col2", "col3"],
                agg_fns=["min", "max", "sum"],
            ),
        )

    def _test_get_column_names(self):
        if self.connect_to_backend:
            column_list = self.api.get_column_names(self.db, self.table)
            self.assertIsInstance(column_list, list)

    def _test_get_columns(self):
        if self.connect_to_backend:
            column_list = self.api.get_columns(self.db, self.table)
            self.assertIsInstance(column_list, list)
            if column_list:
                self.assertIsInstance(column_list[0], ColumnMetadataInterface)

    def _test_get_distinct_column_values(self):
        if self.connect_to_backend:
            two_col_names = self.api.get_column_names(self.db, self.table)[:2]
            one_col_name = two_col_names[0]
            self.assertIsInstance(
                self.api.get_distinct_column_values(
                    self.db,
                    self.table,
                    two_col_names,
                    columns_to_cast_to_string=[one_col_name],
                    order_results=True,
                ),
                list,
            )

    def _test_get_max_column_values(self):
        if self.connect_to_backend:
            two_col_names = self.api.get_column_names(self.db, self.table)[:2]
            one_col_name = two_col_names[0]
            self.assertIsInstance(
                self.api.get_max_column_values(self.db, self.table, [one_col_name]),
                tuple,
            )
            self.assertIsInstance(
                self.api.get_max_column_values(
                    self.db, self.table, [one_col_name], optimistic_prune_clause="1 = 2"
                ),
                tuple,
            )
            self.assertIsInstance(
                self.api.get_max_column_values(
                    self.db,
                    self.table,
                    two_col_names,
                    columns_to_cast_to_string=[one_col_name],
                    optimistic_prune_clause="1 = 2",
                ),
                tuple,
            )

    def _test_get_max_column_length(self):
        if self.connect_to_backend:
            string_columns = [
                _
                for _ in self.api.get_columns(self.db, self.table)
                if _.is_string_based()
            ]
            if string_columns:
                self.assertIsNotNone(
                    self.api.get_max_column_length(
                        self.db, self.table, string_columns[0].name
                    )
                )

    def _test_get_missing_hive_table_stats(self):
        if self.connect_to_backend and hasattr(
            self.api, "get_missing_hive_table_stats"
        ):
            self.api.get_missing_hive_table_stats(
                self.db, self.table, colstats=True, as_dict=True
            )

    def _test_get_partition_columns(self):
        if self.connect_to_backend:
            cols = self.api.get_partition_columns(self.db, self.part_table)
            if cols:
                self.assertIsInstance(cols, list)
                self.assertIsInstance(cols[0], ColumnMetadataInterface)

    def _test_get_session_option(self):
        if self.connect_to_backend:
            test_option = self.test_api.unit_test_query_options()
            if test_option:
                self.assertIsNotNone(
                    self.api.get_session_option(list(test_option.keys()).pop())
                )

    def _test_get_table_ddl(self):
        if self.connect_to_backend:
            self.assertIsInstance(self.api.get_table_ddl(self.db, self.table), str)
            self.assertIsInstance(
                self.api.get_table_ddl(self.db, self.table, as_list=True), list
            )
            # All current backends use ; to terminate SQL
            self.api.drop_state()
            self.assertFalse(
                self.api.get_table_ddl(
                    self.db, self.table, terminate_sql=False
                ).endswith(";")
            )
            self.api.drop_state()
            self.assertTrue(
                self.api.get_table_ddl(
                    self.db, self.table, terminate_sql=True
                ).endswith(";")
            )

    def _test_get_table_partitions(self):
        if self.connect_to_backend:
            table_parts = self.api.get_table_partitions(self.db, self.part_table)
            if table_parts:
                self.assertIsInstance(table_parts, dict)
                table_part = table_parts.popitem()
                self.assertIsInstance(table_part[1], dict)
                self.assertIn("num_rows", table_part[1])
                self.assertIn("size_in_bytes", table_part[1])

    def _test_get_table_partition_count(self):
        if self.connect_to_backend:
            self.assertIsNotNone(
                self.api.get_table_partition_count(self.db, self.table)
            )
            self.assertIsNotNone(
                self.api.get_table_partition_count(self.db, self.part_table)
            )

    def _test_get_table_row_count(self):
        if self.connect_to_backend:
            self.assertIsNotNone(
                self.api.get_table_row_count(
                    self.db, self.table, filter_clause="123 = 123"
                )
            )

    def _test_get_table_row_count_from_metadata(self):
        if self.connect_to_backend:
            self.assertIsNotNone(
                self.api.get_table_row_count_from_metadata(self.db, self.table)
            )

    def _test_get_table_size(self):
        if self.connect_to_backend:
            self.assertIsNotNone(self.api.get_table_size(self.db, self.table))

    def _test_get_table_size_and_row_count(self):
        if self.connect_to_backend:
            tpl = self.api.get_table_size_and_row_count(self.db, self.table)
            self.assertIsInstance(tpl, tuple)
            self.assertIsNotNone(tpl[0])
            self.assertIsNotNone(tpl[1])

    def _test_get_table_sort_columns(self):
        if self.connect_to_backend:
            # In reality this is very unlikely to actually exercise the code, integration tests will properly test it
            sort_cols = self.api.get_table_sort_columns(self.db, self.table)
            if sort_cols:
                self.assertIsInstance(sort_cols, list)

    def _test_get_table_stats(self):
        if self.connect_to_backend:
            self.assertIsNotNone(self.api.get_table_stats(self.db, self.table))

    def _test_get_table_stats_partitions(self):
        if self.connect_to_backend:
            try:
                self.api.get_table_stats_partitions(self.db, self.table)
                self.api.get_table_stats_partitions(self.db, self.part_table)
            except NotImplementedError:
                pass

    def _test_get_table_and_partition_stats(self):
        if self.connect_to_backend:
            self.assertIsNotNone(
                self.api.get_table_and_partition_stats(self.db, self.table)
            )
            self.assertIsNotNone(
                self.api.get_table_and_partition_stats(self.db, self.part_table)
            )

    def _test_get_user_name(self):
        if self.connect_to_backend:
            self.assertIsNotNone(self.api.get_user_name())

    def _test_google_kms_key_ring_unit(self):
        if not self.connect_to_backend:
            return

        if (
            not self.config.google_kms_key_ring_location
            or not self.config.google_kms_key_ring_name
            or not self.config.kms_key_name
        ):
            return

        # Unit test config defines a key.
        self.assertIsNotNone(self.api.kms_key_name())
        self.assertIn(self.config.google_kms_key_ring_location, self.api.kms_key_name())
        self.assertIn(self.config.google_kms_key_ring_name, self.api.kms_key_name())
        self.assertIn(self.config.google_kms_key_name, self.api.kms_key_name())

        if self.config.google_kms_key_ring_project:
            self.assertIn(
                self.config.google_kms_key_ring_project, self.api.kms_key_name()
            )

    def _test_identifier_contains_invalid_characters(self):
        self.api.identifier_contains_invalid_characters("some_name")
        self.api.identifier_contains_invalid_characters("some!name")
        self.api.identifier_contains_invalid_characters("some#na#me")

    def _test_insert_literal_values(self):
        """_test_insert_literal_values
        Uses backend specific types which is not ideal but don't want to over-develop this for unit tests
        """
        column_list = [
            self.api.gen_column_object(
                "col1", data_type=self.test_api.backend_test_type_canonical_int_8()
            ),
            self.api.gen_column_object(
                "col2", data_type=self.test_api.backend_test_type_canonical_timestamp()
            ),
            self.api.gen_column_object(
                "col3", data_type=self.test_api.backend_test_type_canonical_string()
            ),
        ]
        literal_list = [
            [1, "2019-12-01", "row 1"],
            [2, "2019-12-02", "row 2"],
            [3, "2019-12-03", "row 3"],
            [4, None, "row 4"],
            [5, "2019-12-05", None],
        ]
        if self.connect_to_backend:
            self.assertIsInstance(
                self.api.insert_literal_values(
                    self.db,
                    self.table,
                    literal_list,
                    column_list=column_list,
                    max_rows_per_insert=3,
                ),
                list,
            )
            self.assertIsInstance(
                self.api.insert_literal_values(
                    self.db,
                    self.table,
                    literal_list,
                    column_list=column_list,
                    split_by_cr=False,
                ),
                list,
            )

    def _test_is_nan_sql_expression(self):
        if self.api.nan_supported():
            self.assertIsNotNone(self.api.is_nan_sql_expression("a-column"))

    def _test_is_supported_data_type(self):
        self.assertTrue(self.api.is_supported_data_type("wibble") in (True, False))

    def _test_is_valid_staging_format(self):
        self.assertTrue(self.api.is_valid_staging_format("wibble") in (True, False))

    def _test_is_view(self):
        if self.connect_to_backend:
            self.assertTrue(self.api.is_view(self.db, self.table) in (True, False))

    def _test_length_sql_expression(self):
        for literal, expected_length in [("ABCDEF", 6), ("", 0)]:
            length_expr = self.api.length_sql_expression(
                self.api.to_backend_literal(literal)
            )
            self.assertIsNotNone(length_expr)
            if self.connect_to_backend:
                self.assertEqual(
                    self.api.execute_query_fetch_one("SELECT %s" % length_expr)[0],
                    expected_length,
                )

    def _test_list_databases(self):
        if self.connect_to_backend:
            self.assertIsInstance(self.api.list_databases(), list)
            self.assertIsInstance(self.api.list_databases(db_name_filter="*"), list)
            self.assertIsInstance(self.api.list_databases(db_name_filter=self.db), list)
            self.assertIsInstance(
                self.api.list_databases(db_name_filter=self.db, case_sensitive=False),
                list,
            )

    def _test_list_tables(self):
        if self.connect_to_backend:
            self.assertIsInstance(self.api.list_tables(self.db), list)
            self.assertIsInstance(
                self.api.list_tables(self.db, table_name_filter="*"), list
            )
            self.assertIsInstance(
                self.api.list_tables(self.db, table_name_filter=self.table), list
            )
            self.assertIsInstance(
                self.api.list_tables(
                    self.db, table_name_filter=self.table, case_sensitive=False
                ),
                list,
            )

    def _test_list_udfs(self):
        if self.connect_to_backend:
            all_udfs_in_db = self.api.list_udfs(self._get_udf_db())
            self.assertIsInstance(all_udfs_in_db, list)
            self.assertIsInstance(
                self.api.list_udfs(self._get_udf_db(), udf_name_filter="*"), list
            )
            if all_udfs_in_db:
                row = all_udfs_in_db[0]
                # Expect 2 fields in a row
                self.assertEqual(len(row), 2)
                self.assertIsInstance(
                    self.api.list_udfs(self._get_udf_db(), udf_name_filter=row[0]), list
                )
                self.assertIsInstance(
                    self.api.list_udfs(
                        self._get_udf_db(), udf_name_filter=row[0], case_sensitive=False
                    ),
                    list,
                )
                # While we are here we can test udf_exists() and udf_details() too
                self.assertTrue(self.api.udf_exists(self._get_udf_db(), row[0]))
                for details in self.api.udf_details(self._get_udf_db(), row[0]):
                    self.assertIsNotNone(details.db_name)
                    self.assertIsNotNone(details.udf_name)
            self.assertFalse(self.api.udf_details(self._get_udf_db(), "not-a-function"))
            self.assertFalse(self.api.udf_exists(self._get_udf_db(), "not-a-function"))
            self.assertFalse(self.api.udf_exists("not-a-db", "not-a-function"))

    def _test_list_views(self):
        if self.connect_to_backend:
            try:
                self.assertIsInstance(self.api.list_views(self.db), list)
                self.assertIsInstance(
                    self.api.list_views(self.db, view_name_filter="*"), list
                )
                self.assertIsInstance(
                    self.api.list_views(self.db, view_name_filter=self.table), list
                )
            except NotImplementedError:
                pass

    def _test_min_datetime_value(self):
        self.assertIsNotNone(self.api.min_datetime_value())

    def _test_max_column_name_length(self):
        self.assertIsInstance(self.api.max_column_name_length(), int)
        self.assertGreater(self.api.max_column_name_length(), 0)

    def _test_max_db_name_length(self):
        self.assertIsInstance(self.api.max_db_name_length(), int)
        self.assertGreater(self.api.max_db_name_length(), 0)

    def _test_max_datetime_value(self):
        self.assertIsNotNone(self.api.max_datetime_value())

    def _test_max_decimal_integral_magnitude(self):
        self.assertIsNotNone(self.api.max_decimal_integral_magnitude())

    def _test_max_decimal_precision(self):
        self.assertIsNotNone(self.api.max_decimal_precision())

    def _test_max_decimal_scale(self):
        self.assertIsNotNone(self.api.max_decimal_scale())
        self.assertIsNotNone(
            self.api.max_decimal_scale(
                self.test_api.backend_test_type_canonical_decimal()
            )
        )

    def _test_max_partition_columns(self):
        self.assertIsNotNone(self.api.max_partition_columns())

    def _test_max_table_name_length(self):
        self.assertIsInstance(self.api.max_table_name_length(), int)
        self.assertGreater(self.api.max_table_name_length(), 0)

    def _test_refresh_table_files(self):
        self.api.refresh_table_files(self.db, self.table)

    def _test_regexp_extract_decimal_scale_pattern(self):
        """Test regexp_extract_decimal_scale_pattern() using Python re
        Same tests via SQL in _test_regexp_extract_sql_expression()
        """
        pattern = self.api.regexp_extract_decimal_scale_pattern()
        self.assertIsNotNone(pattern)
        # Number strings with no scale - expect no match
        for test_str in ["123", "-123", "", "0", "123.123E5", "-123.123E5"]:
            self.assertIsNone(re.match(pattern, test_str))
        # Number strings with scale - expect match
        for test_str, expected_scale_length in [
            ("123.001", 3),
            ("-123.001", 3),
            ("0.1", 1),
            ("-0.1", 1),
            ("0.000000001", 9),
            ("-0.000000001", 9),
            (".0123", 4),
            ("-.0123", 4),
        ]:
            self.assertIsNotNone(re.match(pattern, test_str))
            self.assertEqual(
                len(re.match(pattern, test_str).group(1)),
                expected_scale_length,
                f"Source string {test_str}",
            )

    def _test_regexp_extract_sql_expression(self):
        regexp_expr = self.api.regexp_extract_sql_expression(
            self.api.to_backend_literal("123.456"),
            self.api.regexp_extract_decimal_scale_pattern(),
        )
        self.assertIsNotNone(regexp_expr)
        if self.connect_to_backend:
            # Test numbers along with the expected scale
            test_pairs = [
                ("123", None),
                ("-123", None),
                ("", None),
                ("0", None),
                ("123.123E5", None),
                ("-123.123E5", None),
                ("123.001", "001"),
                ("-123.001", "001"),
                ("0.1", "1"),
                ("-0.1", "1"),
            ]
            for test_str, expected_scale in test_pairs:
                regexp_expr = self.api.regexp_extract_sql_expression(
                    self.api.to_backend_literal(test_str),
                    self.api.regexp_extract_decimal_scale_pattern(),
                )
                extracted_scale = self.api.execute_query_fetch_one(
                    "SELECT %s" % regexp_expr
                )[0]
                if expected_scale is None:
                    self.assertIn(extracted_scale, [None, ""])
                else:
                    self.assertEqual(extracted_scale, expected_scale)

    def _test_rename_table(self):
        if self.connect_to_backend:
            self.assertIsInstance(
                self.api.rename_table(self.db, self.table, "other_db", "other_table"),
                list,
            )

    def _test_set_session_db(self):
        self.assertIsInstance(self.api.set_session_db(self.db), list)

    def _test_snowflake_file_format_exists(self):
        if self.connect_to_backend:
            self.assertFalse(
                self.api.snowflake_file_format_exists(self.db, "test-no-ff")
            )
            file_format = add_suffix_in_same_case(
                self.config.snowflake_file_format_prefix,
                "_" + self.config.offload_staging_format,
            )
            self.assertTrue(self.api.snowflake_file_format_exists(self.db, file_format))

    def _test_snowflake_integration_exists(self):
        if self.connect_to_backend:
            self.assertFalse(
                self.api.snowflake_integration_exists("test-no-integration")
            )
            self.assertTrue(
                self.api.snowflake_integration_exists(self.config.snowflake_integration)
            )

    def _test_snowflake_stage_exists(self):
        if self.connect_to_backend:
            self.assertFalse(self.api.snowflake_stage_exists(self.db, "test-no-stage"))
            # We cannot guarantee the stage exists when unit tests run therefore any bool is fine below
            self.assertIsInstance(
                self.api.snowflake_stage_exists(self.db, self.config.snowflake_stage),
                bool,
            )

    def _test_supported_backend_data_types(self):
        self.assertIsInstance(self.api.supported_backend_data_types(), list)

    def _test_supported_partition_function_data_types(self):
        if self.api.partition_by_column_supported():
            self.assertIsInstance(
                self.api.supported_partition_function_parameter_data_types(), list
            )
            self.assertIsInstance(
                self.api.supported_partition_function_return_data_types(), list
            )

    def _test_synapse_external_data_source_exists(self):
        if self.connect_to_backend:
            self.assertFalse(
                self.api.synapse_external_data_source_exists("no-data-source")
            )
            self.assertTrue(
                self.api.synapse_external_data_source_exists(
                    self.config.synapse_data_source
                )
            )

    def _test_synapse_file_format_exists(self):
        if self.connect_to_backend:
            self.assertFalse(self.api.synapse_file_format_exists("no-file-format"))
            self.assertTrue(
                self.api.synapse_file_format_exists(self.config.synapse_file_format)
            )

    def _test_table_exists(self):
        if self.connect_to_backend:
            self.assertTrue(self.api.table_exists(self.db, self.table) in (True, False))
            self.assertFalse(self.api.table_exists("not_a_db", "not_a_table"))

    def _test_table_has_rows(self):
        if self.connect_to_backend:
            self.assertTrue(
                self.api.table_has_rows(self.db, self.table) in (True, False)
            )

    def _test_target_version(self):
        if self.connect_to_backend:
            # Some backends do not expose a version so we cannot assert on this fn
            self.api.target_version()

    def _test_to_backend_literal(self):
        def test_by_select(literal):
            self.assertIsNotNone(literal)
            if self.connect_to_backend:
                self.assertIsNotNone(
                    self.api.execute_query_fetch_one("SELECT %s" % literal)
                )

        # Test without data type qualifier
        self.assertIsNotNone(self.api.to_backend_literal(int(123)))
        self.assertIsNotNone(self.api.to_backend_literal(float(1.23)))
        self.assertIsNotNone(self.api.to_backend_literal(int(12345678901234567)))
        self.assertIsNotNone(self.api.to_backend_literal(str("123")))
        self.assertIsNotNone(self.api.to_backend_literal(str("123")))
        self.assertIsNotNone(self.api.to_backend_literal(datetime.now()))
        self.assertIsNotNone(self.api.to_backend_literal(datetime64(datetime.now())))
        # Test with column data type qualifier
        literal = self.api.to_backend_literal(
            int(123456), self.test_api.backend_test_type_canonical_int_8()
        )
        self.assertIn("123456", str(literal))
        test_by_select(literal)
        literal = self.api.to_backend_literal(
            float(1.23), self.test_api.backend_test_type_canonical_decimal()
        )
        self.assertIn("1.23", str(literal))
        test_by_select(literal)
        literal = self.api.to_backend_literal(
            int(12345678901234567), self.test_api.backend_test_type_canonical_decimal()
        )
        self.assertIn("12345678901234567", str(literal))
        test_by_select(literal)
        literal = self.api.to_backend_literal(
            datetime.now(), self.test_api.backend_test_type_canonical_timestamp()
        )
        test_by_select(literal)
        literal = self.api.to_backend_literal(
            datetime64(datetime.now()),
            self.test_api.backend_test_type_canonical_timestamp(),
        )
        test_by_select(literal)
        try:
            literal = self.api.to_backend_literal(
                datetime.now(), self.test_api.backend_test_type_canonical_time()
            )
            test_by_select(literal)
            literal = self.api.to_backend_literal(
                datetime64(datetime.now()),
                self.test_api.backend_test_type_canonical_time(),
            )
            test_by_select(literal)
        except NotImplementedError:
            pass

    def _test_transform_encrypt_data_type(self):
        self.assertIsNotNone(self.api.transform_encrypt_data_type())

    def _test_transform_null_cast(self):
        self.assertIsNotNone(
            self.api.transform_null_cast(
                self.api.gen_default_numeric_column("some_col")
            )
        )

    def _test_transform_tokenize_data_type(self):
        self.assertIsNotNone(self.api.transform_tokenize_data_type())

    def _test_transform_regexp_replace_expression(self):
        backend_column = self.api.gen_default_numeric_column("some_col")
        self.assertIsNotNone(
            self.api.transform_regexp_replace_expression(
                backend_column, "[a-z]+", "[A-Z]+"
            )
        )

    def _test_transform_translate_expression(self):
        backend_column = self.api.gen_default_numeric_column("some_col")
        try:
            self.assertIsNotNone(
                self.api.transform_translate_expression(backend_column, "abc", "ABC")
            )
        except NotImplementedError:
            pass

    def _test_udf_installation_os(self):
        if self.connect_to_backend:
            try:
                self.api.udf_installation_os(None)
            except NotImplementedError:
                pass

    def _test_udf_installation_sql(self):
        if self.connect_to_backend:
            try:
                self.api.udf_installation_sql(True, "any_db")
            except NotImplementedError:
                pass

    def _test_udf_installation_test(self):
        if self.connect_to_backend:
            try:
                self.api.udf_installation_test(udf_db="any_db")
            except NotImplementedError:
                pass

    def _test_valid_staging_formats(self):
        self.assertIsInstance(self.api.valid_staging_formats(), list)

    def _test_view_exists(self):
        if self.connect_to_backend:
            self.assertTrue(self.api.view_exists(self.db, self.table) in (True, False))
            self.assertFalse(self.api.view_exists("not_a_db", "not_a_view"))

    def _run_all_tests(self):
        if not self.api:
            return
        self._test_add_columns()
        self._test_backend_version()
        self._test_compute_stats()
        self._test_check_backend_supporting_objects()
        self._test_create_database()
        self._test_create_table()
        self._test_create_view()
        self._test_current_date_sql_expression()
        self._test_database_exists()
        try:
            self._test_derive_native_partition_info()
        except NotImplementedError:
            pass
        self._test_detect_column_has_fractional_seconds()
        self._test_drop_table()
        self._test_drop_view()
        self._test_enclose_identifier()
        self._test_enclose_object_reference()
        self._test_execute_ddl()
        self._test_execute_query_fetch_all()
        self._test_execute_query_fetch_one()
        self._test_extract_date_part_sql_expression()
        self._test_format_column_comparison()
        self._test_gen_insert_select_sql_text()
        self._test_gen_ctas_sql_text()
        self._test_gen_default_numeric_column()
        self._test_gen_native_range_partition_key_cast()
        self._test_gen_sql_text()
        self._test_get_column_names()
        self._test_get_columns()
        self._test_get_distinct_column_values()
        self._test_get_max_column_values()
        self._test_get_max_column_length()
        try:
            self._test_get_missing_hive_table_stats()
            self._test_get_partition_columns()
            self._test_get_session_option()
        except NotImplementedError:
            pass
        self._test_get_table_ddl()
        try:
            self._test_get_table_partitions()
            self._test_get_table_row_count_from_metadata()
        except NotImplementedError:
            pass
        self._test_get_table_row_count()
        try:
            self._test_get_table_size()
            self._test_get_table_size_and_row_count()
            self._test_get_table_stats()
            self._test_get_table_stats_partitions()
            self._test_get_table_and_partition_stats()
        except NotImplementedError:
            pass
        self._test_get_table_sort_columns()
        try:
            self._test_get_user_name()
        except NotImplementedError:
            pass
        self._test_identifier_contains_invalid_characters()
        self._test_insert_literal_values()
        self._test_is_nan_sql_expression()
        self._test_is_supported_data_type()
        self._test_is_valid_staging_format()
        self._test_is_view()
        self._test_list_databases()
        self._test_list_tables()
        try:
            self._test_list_udfs()
        except NotImplementedError:
            pass
        self._test_list_views()
        self._test_min_datetime_value()
        self._test_max_column_name_length()
        self._test_max_datetime_value()
        self._test_max_decimal_integral_magnitude()
        self._test_max_decimal_precision()
        self._test_max_decimal_scale()
        self._test_max_table_name_length()
        self._test_refresh_table_files()
        try:
            # Synapse does not support regular expressions
            self._test_regexp_extract_decimal_scale_pattern()
            self._test_regexp_extract_sql_expression()
            self._test_rename_table()
            self._test_set_session_db()
        except NotImplementedError:
            pass
        self._test_supported_backend_data_types()
        self._test_supported_partition_function_data_types()
        self._test_table_exists()
        self._test_table_has_rows()
        self._test_target_version()
        self._test_to_backend_literal()
        self._test_transform_encrypt_data_type()
        self._test_transform_null_cast()
        self._test_transform_tokenize_data_type()
        try:
            # Synapse does not support regular expressions
            self._test_transform_regexp_replace_expression()
        except NotImplementedError:
            pass
        self._test_transform_translate_expression()
        self._test_udf_installation_os()
        self._test_udf_installation_sql()
        self._test_udf_installation_test()
        self._test_valid_staging_formats()
        self._test_view_exists()
        # Engine specific tests
        if self.target == DBTYPE_BIGQUERY:
            self._test_bigquery_dataset_project()
            self._test_google_kms_key_ring_unit()
        if self.target == DBTYPE_SNOWFLAKE:
            self._test_snowflake_file_format_exists()
            self._test_snowflake_integration_exists()
            self._test_snowflake_stage_exists()
        if self.target == DBTYPE_SYNAPSE:
            self._test_synapse_external_data_source_exists()
            self._test_synapse_file_format_exists()


class TestHiveBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_HIVE
        self.config = self._get_mock_config(FAKE_ORACLE_HIVE_ENV)
        try:
            super(TestHiveBackendApi, self).setUp()
        except ModuleNotFoundError as e:
            if optional_hadoop_dependency_exception(e):
                pytest.skip("Skipping TestHiveBackendApi due to missing dependencies")
            else:
                raise

    def test_all_non_connecting_hive_tests(self):
        self._run_all_tests()


class TestImpalaBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_IMPALA
        self.config = self._get_mock_config(FAKE_ORACLE_IMPALA_ENV)
        try:
            super(TestImpalaBackendApi, self).setUp()
        except ModuleNotFoundError as e:
            if optional_hadoop_dependency_exception(e):
                pytest.skip("Skipping TestImpalaBackendApi due to missing dependencies")
            else:
                raise

    def test_all_non_connecting_impala_tests(self):
        self._run_all_tests()


class TestBigQueryBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_BIGQUERY
        self.config = self._get_mock_config(FAKE_ORACLE_BQ_ENV)
        super(TestBigQueryBackendApi, self).setUp()

    def test_all_non_connecting_bigquery_tests(self):
        self._run_all_tests()


class TestSparkThriftBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_SPARK
        self.config = self._get_mock_config(FAKE_ORACLE_HIVE_ENV)
        try:
            super(TestSparkThriftBackendApi, self).setUp()
        except ModuleNotFoundError as e:
            if optional_hadoop_dependency_exception(e):
                pytest.skip(
                    "Skipping TestSparkThriftBackendApi due to missing dependencies"
                )
            else:
                raise

    def test_all_non_connecting_spark_thrift_tests(self):
        self._run_all_tests()


class TestSnowflakeBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_SNOWFLAKE
        self.config = self._get_mock_config(FAKE_ORACLE_SNOWFLAKE_ENV)
        if self.config.snowflake_database is None:
            self.config.snowflake_database = "any-db"
        try:
            super(TestSnowflakeBackendApi, self).setUp()
        except ModuleNotFoundError as e:
            if optional_snowflake_dependency_exception(e):
                pytest.skip(
                    "Skipping TestSnowflakeBackendApi due to missing dependencies"
                )
            else:
                raise

    def test_all_non_connecting_snowflake_tests(self):
        self._run_all_tests()


class TestSynapseBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_SYNAPSE
        self.config = self._get_mock_config(FAKE_ORACLE_SYNAPSE_ENV)
        if self.config.synapse_database is None:
            self.config.synapse_database = "any-db"
        try:
            super(TestSynapseBackendApi, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_synapse_dependency_exception(e):
                raise
            else:
                pytest.skip(
                    "Skipping TestSynapseBackendApi due to missing configuration"
                )

    def test_all_non_connecting_synapse_tests(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
