""" TestFrontendTestingApi: Unit test library to test the testing API for all supported frontends
    This is split into two categories
    1) For all possible frontends test API calls that do not need to connect to the system
       Because there is no connection we can fake any frontend and test functionality
       These classes have the system in the name: TestHiveFrontendApi, TestImpalaFrontendApi, etc
    2) For the current frontend test API calls that need to connect to the system
       This class has Current in the name: TestCurrentFrontendApi
"""

from unittest import TestCase, main

from tests.offload.unittest_functions import build_current_options, build_non_connecting_options,\
    get_real_frontend_schema_and_table

from goe.offload.column_metadata import ALL_CANONICAL_TYPES
from goe.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_MSSQL, DBTYPE_TERADATA
from goe.offload.offload_messages import OffloadMessages
from testlib.test_framework.factory.frontend_testing_api_factory import frontend_testing_api_factory


transient_error_global_counter = 0


class TestFrontendTestingApi(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestFrontendTestingApi, self).__init__(*args, **kwargs)
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.connect_to_frontend = None
        self.db_type = None

    def setUp(self):
        messages = OffloadMessages()
        self.test_api = frontend_testing_api_factory(self.db_type, self.config, messages, dry_run=True,
                                                     do_not_connect=bool(not self.connect_to_frontend))
        if self.connect_to_frontend:
            self.db, self.table = get_real_frontend_schema_and_table('GL_TYPES', self.config, messages=messages)
            if not self.table:
                print('Falling back to connect_to_frontend=False because there are no test tables')
                self.connect_to_frontend = False
                self.db = 'any_db'
                self.table = 'some_table'
        else:
            self.db = 'any_db'
            self.table = 'some_table'

    def _test_drop_table(self):
        self.assertIsInstance(self.test_api.drop_table(self.db, 'some-table'), list)

    def _test_expected_channels_offload_predicates(self):
        self.assertIsInstance(self.test_api.expected_channels_offload_predicates(), list)

    def _test_expected_sales_offload_predicates(self):
        try:
            self.assertIsInstance(self.test_api.expected_sales_offload_predicates(), list)
        except NotImplementedError:
            pass

    def _test_gl_type_mapping_generated_table_col_specs(self):
        if self.db_type != DBTYPE_ORACLE:
            return
        max_backend_precision = 29
        max_backend_scale = 3
        max_decimal_integral_magnitude = 20
        supported_canonical_types = ALL_CANONICAL_TYPES
        return_value = self.test_api.gl_type_mapping_generated_table_col_specs(
            max_backend_precision, max_backend_scale, max_decimal_integral_magnitude, supported_canonical_types)
        self.assertIsInstance(return_value, tuple)
        self.assertIsInstance(return_value[0], list)
        self.assertIsInstance(return_value[1], list)

    def _test_host_compare_sql_projection(self):
        cols = [self.test_api.gen_column_object('col1', data_type=self.test_api.test_type_canonical_date()),
                self.test_api.gen_column_object('col2', data_type=self.test_api.test_type_canonical_int_8())]
        self.assertIsInstance(self.test_api.host_compare_sql_projection(cols), str)

    def _test_test_type_canonical_date(self):
        self.assertIsInstance(self.test_api.test_type_canonical_date(), str)

    def _test_test_type_canonical_decimal(self):
        self.assertIsInstance(self.test_api.test_type_canonical_decimal(), str)

    def _test_test_type_canonical_int_8(self):
        self.assertIsInstance(self.test_api.test_type_canonical_int_8(), str)

    def _test_test_type_canonical_timestamp(self):
        self.assertIsInstance(self.test_api.test_type_canonical_timestamp(), str)

    def _test_view_is_valid(self):
        if self.connect_to_frontend:
            self.assertIsInstance(self.test_api.view_is_valid(self.db, 'some-view'), bool)

    def _run_all_tests(self):
        self._test_drop_table()
        self._test_expected_channels_offload_predicates()
        self._test_expected_sales_offload_predicates()
        self._test_gl_type_mapping_generated_table_col_specs()
        self._test_host_compare_sql_projection()
        self._test_test_type_canonical_date()
        self._test_test_type_canonical_decimal()
        self._test_test_type_canonical_int_8()
        self._test_test_type_canonical_timestamp()
        self._test_view_is_valid()


class TestMSSQLFrontendTestingApi(TestFrontendTestingApi):

    def setUp(self):
        self.connect_to_frontend = False
        self.db_type = DBTYPE_MSSQL
        self.config = build_non_connecting_options(DBTYPE_MSSQL)
        super().setUp()

    def test_all_non_connecting_mssql_tests(self):
        self._run_all_tests()


class TestOracleFrontendTestingApi(TestFrontendTestingApi):

    def setUp(self):
        self.connect_to_frontend = False
        self.db_type = DBTYPE_ORACLE
        self.config = build_non_connecting_options(DBTYPE_ORACLE)
        super().setUp()

    def test_all_non_connecting_oracle_tests(self):
        self._run_all_tests()


class TestTeradataFrontendTestingApi(TestFrontendTestingApi):

    def setUp(self):
        self.connect_to_frontend = False
        self.db_type = DBTYPE_TERADATA
        self.config = build_non_connecting_options(DBTYPE_TERADATA)
        super().setUp()

    def test_all_non_connecting_teradata_tests(self):
        self._run_all_tests()


class TestCurrentFrontendTestingApi(TestFrontendTestingApi):

    def setUp(self):
        self.connect_to_frontend = True
        self.config = self._build_current_options()
        super(TestCurrentFrontendTestingApi, self).setUp()

    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.db_type = orchestration_options.db_type
        return orchestration_options

    def test_full_api_on_current_frontend(self):
        self._run_all_tests()


if __name__ == '__main__':
    main()
