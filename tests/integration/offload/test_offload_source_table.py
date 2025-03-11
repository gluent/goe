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

"""TestOffloadSourceTable: Unit test library to test API for all supported frontend RDBMSs
This is split into two categories:
1) For all possible frontends test API calls that do not need to connect to the system.
   Because there is no connection we can fake any backend and test functionality. These classes
   have the system in the name: TestOracleOffloadSourceTable, TestMSSQLOffloadSourceTable, etc
2) For the current backend test API calls that need to connect to the system.
   This class has Current in the name: TestCurrentOffloadSourceTable
"""
from unittest import TestCase, main

from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import DBTYPE_ORACLE
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_source_table import OffloadSourceTableException

from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
    run_setup_ddl,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.testlib.test_framework.test_functions import get_test_messages


FACT_NAME = "INTEG_SOURCE_TABLE_FACT"


class TestCurrentOffloadSourceTable(TestCase):
    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.db_type = orchestration_options.db_type
        return orchestration_options

    def _create_test_table(self):
        messages = get_test_messages(self.config, "TestCurrentOffloadSourceTable")
        # Setup partitioned table
        run_setup_ddl(
            self.config,
            self.test_api,
            messages,
            self.test_api.sales_based_fact_create_ddl(
                self.db, self.table, simple_partition_names=True
            ),
        )

    def _test__get_column_low_high_values(self):
        if self.config.db_type == DBTYPE_ORACLE:
            if self.api.columns:
                try:
                    # Testing Oracle only private method
                    self.api._get_column_low_high_values(
                        self.api.columns[0].name, from_stats=True, sample_perc=1
                    )
                    self.api._get_column_low_high_values(
                        self.api.columns[0].name, from_stats=False, sample_perc=1
                    )
                except NotImplementedError:
                    pass

    def _test_check_data_types_supported(self):
        # 6 below is for milliseconds, False is for backend NaN support. Actual values unimportant
        self.assertIn(self.api.check_data_types_supported(6, False), [True, False])

    def _test_columns(self):
        # Referencing the property will load the column from backend
        self.api.columns
        try:
            # Test that setter property works
            self.api.columns = self.api.columns
        except OffloadSourceTableException:
            # We expect "Set of columns is only supported when the table does NOT exist"
            pass

    def _test_data_types_in_use(self):
        self.api.data_types_in_use()

    def _test_exists(self):
        self.api.exists()

    def _test_get_column(self):
        self.api.get_column("some_column")

    def _test_get_current_scn(self):
        try:
            self.api.get_current_scn()
        except NotImplementedError:
            pass

    def _test_get_hash_bucket_candidate(self):
        try:
            self.api.get_hash_bucket_candidate()
        except NotImplementedError:
            pass

    def _test_get_hash_bucket_last_resort(self):
        self.api.get_hash_bucket_last_resort()

    def _test_get_max_partition_size(self):
        self.api.get_max_partition_size()

    def _test_get_minimum_partition_key_data(self):
        if self.api.is_partitioned():
            self.api.get_minimum_partition_key_data()

    def _test_get_partitions(self):
        self.api.get_partitions()
        self.api.get_partitions(populate_hvs=False)

    def _test_get_primary_index_columns(self):
        self.api.get_primary_index_columns()

    def _test_get_primary_key_columns(self):
        self.api.get_primary_key_columns()

    def _test_get_session_option(self):
        if self.test_api:
            test_options = self.test_api.unit_test_query_options()
            if test_options:
                test_option = list(test_options.keys()).pop()
                self.assertIsNotNone(self.api.get_session_option(test_option))

    def _test_get_stored_object_dependent_schemas(self):
        if self.config == DBTYPE_ORACLE:
            self.api.get_stored_object_dependent_schemas()

    def _test_get_suitable_sample_size(self):
        self.api.get_suitable_sample_size()

    def _test_has_rowdependencies(self):
        if self.config == DBTYPE_ORACLE:
            self.api.has_rowdependencies()

    def _test_max_column_name_length(self):
        self.assertIsInstance(self.api.max_column_name_length(), int)
        self.assertGreater(self.api.max_column_name_length(), 0)

    def _test_max_schema_name_length(self):
        self.assertIsInstance(self.api.max_schema_name_length(), int)
        self.assertGreater(self.api.max_schema_name_length(), 0)

    def _test_max_table_name_length(self):
        self.assertIsInstance(self.api.max_table_name_length(), int)
        self.assertGreater(self.api.max_table_name_length(), 0)

    def _test_partition_columns(self):
        self.api.partition_columns

    def _test_partition_has_rows(self):
        if self.api.is_partitioned():
            partitions = self.api.get_partitions()
            if partitions:
                self.api.partition_has_rows(partitions[0].partition_name)

    def _test_sample_rdbms_data_types(self):
        # Sampling only supports numeric and date based columns
        columns_to_sample = [
            _ for _ in self.api.columns if _.is_number_based() or _.is_date_based()
        ]
        if columns_to_sample:
            # The backend limit parameters ought to be from a backend but doesn't really matter for unit testing
            max_integral_magnitude = max_scale = 38
            allow_scale_rounding = True
            for parallel in [0, 2]:
                self.api.sample_rdbms_data_types(
                    columns_to_sample[:2],
                    1,
                    parallel,
                    self.api.min_datetime_value(),
                    max_integral_magnitude,
                    max_scale,
                    allow_scale_rounding,
                )

    def _run_all_tests(self):
        self._test__get_column_low_high_values()
        self._test_check_data_types_supported()
        self._test_columns()
        self._test_data_types_in_use()
        self._test_exists()
        self._test_get_column()
        self._test_get_hash_bucket_candidate()
        self._test_get_hash_bucket_last_resort()
        self._test_get_max_partition_size()
        self._test_get_partitions()
        self._test_get_minimum_partition_key_data()
        self._test_get_primary_key_columns()
        self._test_get_session_option()
        self._test_get_stored_object_dependent_schemas()
        self._test_get_suitable_sample_size()
        self._test_get_suitable_sample_size()
        self._test_has_rowdependencies()
        self._test_max_column_name_length()
        self._test_max_table_name_length()
        self._test_partition_columns()
        self._test_partition_has_rows()
        self._test_sample_rdbms_data_types()

    def test_full_api_on_current_rdbms(self):
        self.db = get_default_test_user()
        self.table = FACT_NAME
        self.messages = OffloadMessages()
        self.config = self._build_current_options()
        self.test_api = frontend_testing_api_factory(
            self.config.db_type,
            self.config,
            self.messages,
            dry_run=False,
            trace_action="frontend_api(TestCurrentOffloadSourceTable)",
        )
        self.api = OffloadSourceTable.create(
            self.db,
            self.table,
            self.config,
            self.messages,
            dry_run=True,
        )

        self._create_test_table()
        self._run_all_tests()


if __name__ == "__main__":
    main()
