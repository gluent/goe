""" TestBackendTestingApi: Unit test library to test the testing API for all supported backends
    This is split into two categories
    1) For all possible backends test API calls that do not need to connect to the system
       Because there is no connection we can fake any backend and test functionality
       These classes have the system in the name: TestHiveBackendApi, TestImpalaBackendApi, etc
    2) For the current backend test API calls that need to connect to the system
       This class has Current in the name: TestCurrentBackendApi
"""

from unittest import TestCase, main

from goe.config.orchestration_config import OrchestrationConfig
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

    def _test_gl_type_mapping_generated_table_col_specs(self):
        self.assertIsInstance(
            self.test_api.gl_type_mapping_generated_table_col_specs(), tuple
        )
        self.assertIsInstance(
            self.test_api.gl_type_mapping_generated_table_col_specs()[0], list
        )
        self.assertIsInstance(
            self.test_api.gl_type_mapping_generated_table_col_specs()[1], list
        )

    def _test_host_compare_sql_projection(self):
        cols = [
            self.test_api.gen_column_object(
                "col1", data_type=self.test_api.backend_test_type_canonical_date()
            ),
            self.test_api.gen_column_object(
                "col2", data_type=self.test_api.backend_test_type_canonical_int_8()
            ),
        ]
        self.assertIsInstance(self.test_api.host_compare_sql_projection(cols), str)

    def _test_transient_error_rerunner(self):
        global transient_error_global_counter
        if self.test_api.transient_query_error_identification_strings():

            class TransientException(Exception):
                pass

            def test_callable():
                global transient_error_global_counter
                transient_error_global_counter += 1
                raise TransientException(
                    "Pretend exception: {}".format(
                        self.test_api.transient_query_error_identification_strings()[0]
                    )
                )

            try:
                transient_error_global_counter = 0
                self.test_api.transient_error_rerunner(test_callable)
            except TransientException:
                # Ran twice
                self.assertEqual(transient_error_global_counter, 2)

            try:
                transient_error_global_counter = 0
                self.test_api.transient_error_rerunner(test_callable, max_retries=0)
            except TransientException:
                # Ran once
                self.assertEqual(transient_error_global_counter, 1)

            try:
                transient_error_global_counter = 0
                self.test_api.transient_error_rerunner(test_callable, max_retries=2)
            except TransientException:
                # Ran three times
                self.assertEqual(transient_error_global_counter, 3)

    def _test_unit_test_query_options(self):
        self.assertIsInstance(self.test_api.unit_test_query_options(), dict)

    def _run_all_tests(self):
        self._test_create_table_as_select()
        self._test_gl_type_mapping_generated_table_col_specs()
        self._test_host_compare_sql_projection()
        self._test_transient_error_rerunner()
        self._test_unit_test_query_options()


class TestHiveBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_HIVE
        self.config = OrchestrationConfig.from_dict({"verbose": False})
        super(TestHiveBackendTestingApi, self).setUp()

    def test_all_non_connecting_hive_tests(self):
        self._run_all_tests()


class TestImpalaBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_IMPALA
        self.config = OrchestrationConfig.from_dict({"verbose": False})
        super(TestImpalaBackendTestingApi, self).setUp()

    def test_all_non_connecting_impala_tests(self):
        self._run_all_tests()


class TestBigQueryBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_BIGQUERY
        self.config = OrchestrationConfig.from_dict({"verbose": False})
        super(TestBigQueryBackendTestingApi, self).setUp()

    def test_all_non_connecting_bigquery_tests(self):
        self._run_all_tests()


class TestSnowflakeBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_SNOWFLAKE
        self.config = OrchestrationConfig.from_dict({"verbose": False})
        super(TestSnowflakeBackendTestingApi, self).setUp()

    def test_all_non_connecting_snowflake_tests(self):
        self._run_all_tests()


class TestSynapseBackendTestingApi(TestBackendTestingApi):
    def setUp(self):
        self.target = DBTYPE_SYNAPSE
        self.config = OrchestrationConfig.from_dict({"verbose": False})
        if self.config.synapse_database is None:
            self.config.synapse_database = "any-db"
        super(TestSynapseBackendTestingApi, self).setUp()

    def test_all_non_connecting_synapse_tests(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
