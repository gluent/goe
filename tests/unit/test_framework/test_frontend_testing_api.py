""" TestFrontendTestingApi: Unit test library to test the testing API for all supported frontends
    This is split into two categories
    1) For all possible frontends test API calls that do not need to connect to the system
       Because there is no connection we can fake any frontend and test functionality
       These classes have the system in the name: TestHiveFrontendApi, TestImpalaFrontendApi, etc
    2) For the current frontend test API calls that need to connect to the system
       This class has Current in the name: TestCurrentFrontendApi
"""

from unittest import TestCase, main

from goe.offload.column_metadata import ALL_CANONICAL_TYPES
from goe.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_MSSQL, DBTYPE_TERADATA
from goe.offload.offload_messages import OffloadMessages
from tests.unit.test_functions import (
    build_mock_options,
    FAKE_MSSQL_ENV,
    FAKE_ORACLE_ENV,
    FAKE_TERADATA_ENV,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)


transient_error_global_counter = 0


class TestFrontendTestingApi(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFrontendTestingApi, self).__init__(*args, **kwargs)
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.connect_to_frontend = False
        self.db_type = None

    def setUp(self):
        messages = OffloadMessages()
        self.test_api = frontend_testing_api_factory(
            self.db_type,
            self.config,
            messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_frontend),
        )
        self.db = "any_db"
        self.table = "some_table"

    def _test_drop_table(self):
        self.assertIsInstance(self.test_api.drop_table(self.db, "some-table"), list)

    def _test_expected_sales_offload_predicates(self):
        try:
            self.assertIsInstance(
                self.test_api.expected_sales_offload_predicates(), list
            )
        except NotImplementedError:
            pass

    def _test_goe_type_mapping_generated_table_col_specs(self):
        if self.db_type != DBTYPE_ORACLE:
            return
        max_backend_precision = 29
        max_backend_scale = 3
        max_decimal_integral_magnitude = 20
        supported_canonical_types = ALL_CANONICAL_TYPES
        return_value = self.test_api.goe_type_mapping_generated_table_col_specs(
            max_backend_precision,
            max_backend_scale,
            max_decimal_integral_magnitude,
            supported_canonical_types,
        )
        self.assertIsInstance(return_value, tuple)
        self.assertIsInstance(return_value[0], list)
        self.assertIsInstance(return_value[1], list)

    def _test_host_compare_sql_projection(self):
        cols = [
            self.test_api.gen_column_object(
                "col1", data_type=self.test_api.test_type_canonical_date()
            ),
            self.test_api.gen_column_object(
                "col2", data_type=self.test_api.test_type_canonical_int_8()
            ),
        ]
        self.assertIsInstance(self.test_api.host_compare_sql_projection(cols), str)

    def _test_test_type_canonical_date(self):
        self.assertIsInstance(self.test_api.test_type_canonical_date(), str)

    def _test_test_type_canonical_decimal(self):
        self.assertIsInstance(self.test_api.test_type_canonical_decimal(), str)

    def _test_test_type_canonical_int_8(self):
        self.assertIsInstance(self.test_api.test_type_canonical_int_8(), str)

    def _test_test_type_canonical_timestamp(self):
        self.assertIsInstance(self.test_api.test_type_canonical_timestamp(), str)

    def _run_all_tests(self):
        self._test_drop_table()
        self._test_expected_sales_offload_predicates()
        self._test_goe_type_mapping_generated_table_col_specs()
        self._test_host_compare_sql_projection()
        self._test_test_type_canonical_date()
        self._test_test_type_canonical_decimal()
        self._test_test_type_canonical_int_8()
        self._test_test_type_canonical_timestamp()


class TestMSSQLFrontendTestingApi(TestFrontendTestingApi):
    def setUp(self):
        self.db_type = DBTYPE_MSSQL
        self.config = build_mock_options(FAKE_MSSQL_ENV)
        super().setUp()

    def test_all_non_connecting_mssql_tests(self):
        self._run_all_tests()


class TestOracleFrontendTestingApi(TestFrontendTestingApi):
    def setUp(self):
        self.db_type = DBTYPE_ORACLE
        self.config = build_mock_options(FAKE_ORACLE_ENV)
        super().setUp()

    def test_all_non_connecting_oracle_tests(self):
        self._run_all_tests()


class TestTeradataFrontendTestingApi(TestFrontendTestingApi):
    def setUp(self):
        self.db_type = DBTYPE_TERADATA
        self.config = build_mock_options(FAKE_TERADATA_ENV)
        super().setUp()

    def test_all_non_connecting_teradata_tests(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
