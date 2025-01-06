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

""" TestFrontendApi: Unit test library to test API for all supported RDBMSs
    1) For all possible frontends test API calls that do not need to connect to the system
       Because there is no connection we can fake any frontend and test functionality
       These classes have the system in the name, e.g.: TestOracleFrontendApi, TestMSSQLFrontendApi, etc
"""

from datetime import datetime
from unittest import TestCase, main

from numpy import datetime64

from goe.offload.column_metadata import ColumnMetadataInterface
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.frontend_api import QueryParameter
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)
from goe.offload.offload_messages import OffloadMessages
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.unit.test_functions import (
    build_mock_options,
    optional_sql_server_dependency_exception,
    optional_teradata_dependency_exception,
    FAKE_MSSQL_ENV,
    FAKE_ORACLE_ENV,
    FAKE_TERADATA_ENV,
)


class TestFrontendApi(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFrontendApi, self).__init__(*args, **kwargs)
        self.api = None
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.part_table = None
        self.connect_to_frontend = None
        self.db_type = None

    def setUp(self):
        messages = OffloadMessages()
        self.api = frontend_api_factory(
            self.db_type,
            self.config,
            messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_frontend),
            trace_action="TestFrontendApi",
        )
        self.db = "any_db"
        self.table = "some_table"
        self.test_api = frontend_testing_api_factory(
            self.db_type,
            self.config,
            messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_frontend),
        )

    def _get_mock_config(self, mock_env: dict):
        return build_mock_options(mock_env)

    def _test_capabilities(self):
        self.assertIsInstance(self.api.canonical_date_supported(), bool)
        self.assertIsInstance(self.api.canonical_float_supported(), bool)
        self.assertIsInstance(self.api.canonical_time_supported(), bool)
        self.assertIsInstance(self.api.case_sensitive_identifiers(), bool)
        self.assertIsInstance(self.api.goe_has_db_code_component(), bool)
        self.assertIsInstance(self.api.goe_offload_status_report_supported(), bool)
        self.assertIsInstance(self.api.parameterized_queries_supported(), bool)

    def _test_create_table(self):
        column_list = [
            self.api.gen_column_object(
                "col1", data_type=self.test_api.test_type_canonical_int_8()
            ),
            self.api.gen_column_object(
                "col2", data_type=self.test_api.test_type_canonical_date()
            ),
            self.api.gen_column_object(
                "col3", data_type=self.api.generic_string_data_type()
            ),
        ]
        self.api.create_table(self.db, "new_table", column_list)

    def _test_enclose_identifier(self):
        self.api.enclose_identifier(self.db)

    def _test_enclose_query_hints(self):
        try:
            self.api.enclose_query_hints("some_hint(123)")
            self.api.enclose_query_hints(["some_hint(123)", "another_hint(456)"])
        except NotImplementedError:
            pass

    def _test_execute_ddl(self):
        # Make a call without executing just to shake down Python logic
        self.assertIsInstance(
            self.api.execute_ddl(
                "DDL ON A TABLE", query_options=self.test_api.unit_test_query_options()
            ),
            list,
        )

    def _test_execute_dml(self):
        # Make a call without executing just to shake down Python logic
        self.assertIsInstance(
            self.api.execute_dml(
                "DML ON A TABLE", query_options=self.test_api.unit_test_query_options()
            ),
            list,
        )

    def _test_execute_query_fetch_all(self):
        if self.connect_to_frontend:
            sql = "SELECT COUNT(*) C FROM %s.%s" % (self.db, self.table)
            rows = self.api.execute_query_fetch_all(sql)
            self.assertIsInstance(rows, list)
            self.assertIsInstance(rows[0], (list, tuple))
            rows = self.api.execute_query_fetch_all(sql, as_dict=True)
            self.assertIsInstance(rows, list)
            self.assertIsInstance(rows[0], dict)
            self.assertIsInstance(
                self.api.execute_query_fetch_all(sql, time_sql=True), list
            )

    def _test_execute_query_fetch_one(self):
        if self.connect_to_frontend:
            sql = "SELECT COUNT(*) C FROM %s.%s" % (self.db, self.table)
            self.assertIsInstance(self.api.execute_query_fetch_one(sql), (list, tuple))
            self.assertIsInstance(
                self.api.execute_query_fetch_one(sql, as_dict=True), (dict)
            )
            if self.api.parameterized_queries_supported():
                num_columns = [
                    _
                    for _ in self.api.get_columns(self.db, self.table)
                    if _.is_number_based()
                ]
                self.assertTrue(bool(num_columns))
                column_name = num_columns[0].name
                params = [QueryParameter("num_value", 42)]
                self.assertIsInstance(
                    self.api.execute_query_fetch_one(
                        "SELECT COUNT(*) FROM %s.%s WHERE %s > %s"
                        % (
                            self.db,
                            self.table,
                            column_name,
                            self.api.format_query_parameter("num_value"),
                        ),
                        query_params=params,
                    ),
                    (list, tuple),
                )

    def _test_frontend_db_name(self):
        self.assertIsNotNone(self.api.frontend_db_name())
        self.assertIsInstance(self.api.frontend_db_name(), str)

    def _test_frontend_version(self):
        if self.connect_to_frontend:
            self.assertIsNotNone(self.api.frontend_version())

    def _test_goe_db_component_version(self):
        if self.connect_to_frontend:
            if self.api.goe_has_db_code_component():
                self.assertIsNotNone(self.api.goe_db_component_version())
            else:
                self.assertIsNone(self.api.goe_db_component_version())

    def _test_get_column_names(self):
        if self.connect_to_frontend:
            column_list = self.api.get_column_names(self.db, self.table)
            self.assertIsInstance(column_list, list)

    def _test_get_columns(self):
        if self.connect_to_frontend:
            column_list = self.api.get_columns(self.db, self.table)
            self.assertIsInstance(column_list, list)
            if column_list:
                self.assertIsInstance(column_list[0], ColumnMetadataInterface)

    def _test_get_db_unique_name(self):
        if self.connect_to_frontend:
            try:
                self.assertIsInstance(self.api.get_db_unique_name(), str)
            except NotImplementedError:
                pass

    def _test_get_distinct_column_values(self):
        if self.connect_to_frontend:
            column_list = self.api.get_columns(self.db, self.table)
            rows = self.api.get_distinct_column_values(
                self.db, self.table, column_list[0].name, order_results=False
            )
            self.assertIsInstance(rows, list)
            self.assertGreater(len(rows), 0)
            rows = self.api.get_distinct_column_values(
                self.db, self.table, column_list[0].name, order_results=True
            )
            self.assertIsInstance(rows, list)
            self.assertGreater(len(rows), 0)

    def _test_get_partition_columns(self):
        if self.connect_to_frontend:
            column_list = self.api.get_partition_columns(self.db, self.part_table)
            self.assertIsInstance(column_list, list)
            if column_list:
                self.assertIsInstance(column_list[0], ColumnMetadataInterface)

    def _test_get_primary_key_column_names(self):
        if self.connect_to_frontend:
            column_list = self.api.get_primary_key_column_names(self.db, self.table)
            self.assertIsInstance(column_list, list)
            if column_list:
                self.assertIsInstance(column_list[0], str)

    def _test_get_session_option(self):
        if self.connect_to_frontend:
            test_options = self.test_api.unit_test_query_options()
            if test_options:
                test_option = list(test_options.keys()).pop()
                self.assertIsNotNone(self.api.get_session_option(test_option))

    def _test_get_table_ddl(self):
        if self.connect_to_frontend:
            self.assertIsInstance(self.api.get_table_ddl(self.db, self.table), str)
            self.assertIsInstance(
                self.api.get_table_ddl(self.db, self.table, as_list=True), list
            )

    def _test_get_table_row_count(self):
        if self.connect_to_frontend:
            self.assertGreater(self.api.get_table_row_count(self.db, self.table), 0)

    def _test_get_table_size(self):
        if self.connect_to_frontend:
            self.assertGreater(self.api.get_table_size(self.db, self.table), 0)

    def _test_is_view(self):
        if self.connect_to_frontend:
            self.assertIsInstance(self.api.is_view(self.db, self.table), bool)

    def _test_schema_exists(self):
        if self.connect_to_frontend:
            self.assertFalse(self.api.schema_exists("not-a-user"))

    def _test_table_exists(self):
        if self.connect_to_frontend:
            self.assertTrue(self.api.table_exists(self.db, self.table))

    def _test_to_frontend_literal(self):
        def test_by_select(literal):
            self.assertIsNotNone(literal)
            if self.connect_to_frontend:
                if self.db_type == DBTYPE_ORACLE:
                    self.assertIsNotNone(
                        self.api.execute_query_fetch_one(
                            "SELECT %s FROM dual" % literal
                        )
                    )
                else:
                    self.assertIsNotNone(
                        self.api.execute_query_fetch_one("SELECT %s" % literal)
                    )

        literal = self.api.to_frontend_literal(
            int(123456), self.test_api.test_type_canonical_int_8()
        )
        self.assertIn("123456", str(literal))
        test_by_select(literal)
        literal = self.api.to_frontend_literal(
            float(1.23), self.test_api.test_type_canonical_decimal()
        )
        self.assertIn("1.23", str(literal))
        test_by_select(literal)
        literal = self.api.to_frontend_literal(
            int(12345678901234567), self.test_api.test_type_canonical_decimal()
        )
        self.assertIn("12345678901234567", str(literal))
        test_by_select(literal)
        literal = self.api.to_frontend_literal(
            datetime.now(), self.test_api.test_type_canonical_timestamp()
        )
        test_by_select(literal)
        literal = self.api.to_frontend_literal(
            datetime64(datetime.now()), self.test_api.test_type_canonical_timestamp()
        )
        test_by_select(literal)

    def _test_view_exists(self):
        if self.connect_to_frontend:
            self.assertFalse(self.api.view_exists(self.db, self.table))

    def _run_all_tests(self):
        if not self.api:
            return

        self._test_capabilities()
        self._test_create_table()
        self._test_enclose_identifier()
        self._test_enclose_query_hints()
        self._test_execute_ddl()
        self._test_execute_dml()
        self._test_execute_query_fetch_all()
        self._test_execute_query_fetch_one()
        self._test_frontend_db_name()
        self._test_frontend_version()
        self._test_goe_db_component_version()
        self._test_get_column_names()
        self._test_get_columns()
        self._test_get_db_unique_name()
        self._test_get_distinct_column_values()
        self._test_get_partition_columns()
        self._test_get_primary_key_column_names()
        self._test_get_session_option()
        self._test_get_table_ddl()
        self._test_get_table_row_count()
        self._test_get_table_size()
        self._test_is_view()
        self._test_schema_exists()
        self._test_table_exists()
        self._test_to_frontend_literal()
        self._test_view_exists()


class TestMSSQLFrontendApi(TestFrontendApi):
    def setUp(self):
        self.connect_to_frontend = False
        self.db_type = DBTYPE_MSSQL
        self.config = self._get_mock_config(FAKE_MSSQL_ENV)
        try:
            super().setUp()
        except ModuleNotFoundError as e:
            if not optional_sql_server_dependency_exception(e):
                raise

    def test_all_non_connecting_mssql_tests(self):
        self._run_all_tests()


class TestOracleFrontendApi(TestFrontendApi):
    def setUp(self):
        self.connect_to_frontend = False
        self.db_type = DBTYPE_ORACLE
        self.config = self._get_mock_config(FAKE_ORACLE_ENV)
        super().setUp()

    def test_all_non_connecting_oracle_tests(self):
        self._run_all_tests()


class TestTeradataFrontendApi(TestFrontendApi):
    def setUp(self):
        self.connect_to_frontend = False
        self.db_type = DBTYPE_TERADATA
        self.config = self._get_mock_config(FAKE_TERADATA_ENV)
        try:
            super().setUp()
        except ModuleNotFoundError as e:
            if not optional_teradata_dependency_exception(e):
                raise

    def test_all_non_connecting_teradata_tests(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
