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

""" TestBackendTestingApi: Unit test library to test the testing API for all supported backends
    This is split into two categories
    1) For all possible backends test API calls that do not need to connect to the system
       Because there is no connection we can fake any backend and test functionality
       These classes have the system in the name: TestHiveBackendApi, TestImpalaBackendApi, etc
    2) For the current backend test API calls that need to connect to the system
       This class has Current in the name: TestCurrentBackendApi
"""

from unittest import TestCase, main

from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SYNAPSE,
)
from goe.offload.offload_messages import OffloadMessages
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


transient_error_global_counter = 0


class TestBackendTestingApi(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestBackendTestingApi, self).__init__(*args, **kwargs)
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.connect_to_backend = False
        self.target = None

    def setUp(self):
        messages = OffloadMessages()
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

    def _test_create_table_as_select(self):
        # CTAS with no source table
        self.assertIsInstance(
            self.test_api.create_table_as_select(
                self.db, "new_table", "FORMAT", [("123", "COL1"), ("'abc'", "COL2")]
            ),
            list,
        )
        # CTAS from table
        self.assertIsInstance(
            self.test_api.create_table_as_select(
                self.db,
                "new_table",
                "FORMAT",
                [("COLUMN_1", "COL1"), ("COLUMN_2", "COL2")],
                from_db_name=self.db,
                from_table_name=self.table,
                row_limit=10,
            ),
            list,
        )

    def _test_goe_type_mapping_generated_table_col_specs(self):
        self.assertIsInstance(
            self.test_api.goe_type_mapping_generated_table_col_specs(), tuple
        )
        self.assertIsInstance(
            self.test_api.goe_type_mapping_generated_table_col_specs()[0], list
        )
        self.assertIsInstance(
            self.test_api.goe_type_mapping_generated_table_col_specs()[1], list
        )

    def _test_unit_test_query_options(self):
        self.assertIsInstance(self.test_api.unit_test_query_options(), dict)

    def _run_all_tests(self):
        if not self.test_api:
            return
        self._test_create_table_as_select()
        self._test_goe_type_mapping_generated_table_col_specs()
        self._test_unit_test_query_options()


class TestHiveBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_HIVE
        self.config = self._get_mock_config(FAKE_ORACLE_HIVE_ENV)
        try:
            super(TestHiveBackendTestingApi, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_hadoop_dependency_exception(e):
                raise

    def test_all_non_connecting_hive_tests(self):
        self._run_all_tests()


class TestImpalaBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_IMPALA
        self.config = self._get_mock_config(FAKE_ORACLE_IMPALA_ENV)
        try:
            super(TestImpalaBackendTestingApi, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_hadoop_dependency_exception(e):
                raise

    def test_all_non_connecting_impala_tests(self):
        self._run_all_tests()


class TestBigQueryBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_BIGQUERY
        self.config = self._get_mock_config(FAKE_ORACLE_BQ_ENV)
        super(TestBigQueryBackendTestingApi, self).setUp()

    def test_all_non_connecting_bigquery_tests(self):
        self._run_all_tests()


class TestSnowflakeBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_SNOWFLAKE
        self.config = self._get_mock_config(FAKE_ORACLE_SNOWFLAKE_ENV)
        try:
            super(TestSnowflakeBackendTestingApi, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_snowflake_dependency_exception(e):
                raise

    def test_all_non_connecting_snowflake_tests(self):
        self._run_all_tests()


class TestSynapseBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_SYNAPSE
        self.config = self._get_mock_config(FAKE_ORACLE_SYNAPSE_ENV)
        try:
            super(TestSynapseBackendTestingApi, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_synapse_dependency_exception(e):
                raise

    def test_all_non_connecting_synapse_tests(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
