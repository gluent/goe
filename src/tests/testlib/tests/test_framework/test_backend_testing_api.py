""" TestBackendTestingApi: Unit test library to test the testing API for all supported backends
    This is split into two categories
    1) For all possible backends test API calls that do not need to connect to the system
       Because there is no connection we can fake any backend and test functionality
       These classes have the system in the name: TestHiveBackendApi, TestImpalaBackendApi, etc
    2) For the current backend test API calls that need to connect to the system
       This class has Current in the name: TestCurrentBackendApi
"""

from unittest import TestCase, main

from tests.offload.unittest_functions import build_current_options, get_default_test_user

from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_HIVE, DBTYPE_IMPALA,\
    DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE
from goe.offload.offload_messages import OffloadMessages
from goe.persistence.orchestration_metadata import OrchestrationMetadata
from tests.testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory


transient_error_global_counter = 0


class TestBackendTestingApi(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestBackendTestingApi, self).__init__(*args, **kwargs)
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.connect_to_backend = None
        self.target = None

    def setUp(self):
        messages = OffloadMessages()
        self.test_api = backend_testing_api_factory(self.target, self.config, messages, dry_run=True,
                                                    do_not_connect=bool(not self.connect_to_backend))
        if self.connect_to_backend:
            self.db, self.table = self._get_real_db_table()
            if not self.table:
                print('Falling back to connect_to_backend=False because there are no test tables')
                self.connect_to_backend = False
                self.db = 'any_db'
                self.table = 'some_table'
        else:
            self.db = 'any_db'
            self.table = 'some_table'

    def _get_real_db_table(self):
        # Try to find a GL_TYPES table via metadata
        messages = OffloadMessages()
        hybrid_schema = get_default_test_user(hybrid=True)
        for hybrid_schema, hybrid_view in [(hybrid_schema, 'GL_TYPES'), ('SH_H', 'GL_TYPES')]:
            metadata = OrchestrationMetadata.from_name(hybrid_schema, hybrid_view, connection_options=self.config,
                                                       messages=messages)
            if metadata:
                return metadata.backend_owner, metadata.backend_table
        # We shouldn't get to here in a correctly configured environment
        raise Exception('GL_TYPES test table is missing, please configure your environment')

    def _test_create_partitioned_test_table(self):
        if self.connect_to_backend:
            try:
                self.assertIsInstance(self.test_api.create_partitioned_test_table(self.db, self.table,
                                                                                  self.table, 'FORMAT',
                                                                                  filter_clauses=['1 = 2', '3 = 4']),
                                      list)
            except NotImplementedError:
                pass

    def _test_create_table_as_select(self):
        # CTAS with no source table
        self.assertIsInstance(self.test_api.create_table_as_select(self.db, 'new_table', 'FORMAT',
                                                                   [('123', 'COL1'), ("'abc'", 'COL2')]), list)
        # CTAS from table
        self.assertIsInstance(self.test_api.create_table_as_select(self.db, 'new_table', 'FORMAT',
                                                                   [('COLUMN_1', 'COL1'), ('COLUMN_2', 'COL2')],
                                                                   from_db_name=self.db, from_table_name=self.table,
                                                                   row_limit=10), list)

    def _test_drop_column(self):
        if self.connect_to_backend:
            self.test_api.drop_column(self.db, self.table, self.test_api.get_column_names(self.db, self.table)[0])

    def _test_drop_database(self):
        if self.connect_to_backend:
            self.assertIsInstance(self.test_api.drop_database(self.db, cascade=False), list)
            self.assertIsInstance(self.test_api.drop_database(self.db, cascade=True), list)

    def _test_gl_type_mapping_generated_table_col_specs(self):
        self.assertIsInstance(self.test_api.gl_type_mapping_generated_table_col_specs(), tuple)
        self.assertIsInstance(self.test_api.gl_type_mapping_generated_table_col_specs()[0], list)
        self.assertIsInstance(self.test_api.gl_type_mapping_generated_table_col_specs()[1], list)

    def _test_host_compare_sql_projection(self):
        cols = [self.test_api.gen_column_object('col1', data_type=self.test_api.backend_test_type_canonical_date()),
                self.test_api.gen_column_object('col2', data_type=self.test_api.backend_test_type_canonical_int_8())]
        self.assertIsInstance(self.test_api.host_compare_sql_projection(cols), str)

    def _test_rename_column(self):
        if self.connect_to_backend:
            column_name = self.test_api.get_column_names(self.db, self.table)[0]
            self.assertIsInstance(
                self.test_api.rename_column(self.db, self.table, column_name, 'a_new_name', sync=True), list)

    def _test_select_single_non_null_value(self):
        if self.connect_to_backend:
            col_name = self.test_api.get_column_names(self.db, self.table)[0]
            self.assertIsNotNone(self.test_api.select_single_non_null_value(self.db, self.table, col_name, col_name))

    def _test_sql_median_expression(self):
        if self.connect_to_backend:
            # Must be a connected test because some backends need more than a column name
            for column_name in self.test_api.get_column_names(self.db, self.table):
                expr = self.test_api.sql_median_expression(self.db, self.table, column_name)
                self.assertIsNotNone(expr)
                sql = 'SELECT %s FROM %s.%s' % (expr, self.db, self.table)
                self.assertIsNotNone(self.test_api.execute_query_fetch_one(sql))

    def _test_story_test_offload_nums_expected_backend_types(self):
        self.assertIsInstance(self.test_api.story_test_offload_nums_expected_backend_types(), dict)

    def _test_story_test_table_extra_col_info(self):
        self.assertIsInstance(self.test_api.story_test_table_extra_col_info(), dict)

    def _test_table_distribution(self):
        if self.connect_to_backend:
            self.assertIsInstance(self.test_api.table_distribution(self.db, self.table), (type(None), str))

    def _test_transient_error_rerunner(self):
        global transient_error_global_counter
        if self.test_api.transient_query_error_identification_strings():
            class TransientException(Exception):
                pass

            def test_callable():
                global transient_error_global_counter
                transient_error_global_counter += 1
                raise TransientException('Pretend exception: {}'.format(
                    self.test_api.transient_query_error_identification_strings()[0]))

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
        self._test_create_partitioned_test_table()
        self._test_create_table_as_select()
        self._test_drop_column()
        self._test_drop_database()
        self._test_gl_type_mapping_generated_table_col_specs()
        self._test_host_compare_sql_projection()
        self._test_rename_column()
        self._test_select_single_non_null_value()
        self._test_sql_median_expression()
        self._test_story_test_offload_nums_expected_backend_types()
        self._test_story_test_table_extra_col_info()
        self._test_table_distribution()
        self._test_transient_error_rerunner()
        self._test_unit_test_query_options()


class TestHiveBackendTestingApi(TestBackendTestingApi):

    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_HIVE
        self.config = OrchestrationConfig.from_dict({'verbose': False})
        super(TestHiveBackendTestingApi, self).setUp()

    def test_all_non_connecting_hive_tests(self):
        self._run_all_tests()


class TestImpalaBackendTestingApi(TestBackendTestingApi):

    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_IMPALA
        self.config = OrchestrationConfig.from_dict({'verbose': False})
        super(TestImpalaBackendTestingApi, self).setUp()

    def test_all_non_connecting_impala_tests(self):
        self._run_all_tests()


class TestBigQueryBackendTestingApi(TestBackendTestingApi):

    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_BIGQUERY
        self.config = OrchestrationConfig.from_dict({'verbose': False})
        super(TestBigQueryBackendTestingApi, self).setUp()

    def test_all_non_connecting_bigquery_tests(self):
        self._run_all_tests()


class TestSnowflakeBackendTestingApi(TestBackendTestingApi):

    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_SNOWFLAKE
        self.config = OrchestrationConfig.from_dict({'verbose': False})
        super(TestSnowflakeBackendTestingApi, self).setUp()

    def test_all_non_connecting_snowflake_tests(self):
        self._run_all_tests()


class TestSynapseBackendTestingApi(TestBackendTestingApi):

    def setUp(self):
        self.connect_to_backend = False
        self.target = DBTYPE_SYNAPSE
        self.config = OrchestrationConfig.from_dict({'verbose': False})
        if self.config.synapse_database is None:
            self.config.synapse_database = 'any-db'
        super(TestSynapseBackendTestingApi, self).setUp()

    def test_all_non_connecting_synapse_tests(self):
        self._run_all_tests()


class TestCurrentBackendTestingApi(TestBackendTestingApi):

    def setUp(self):
        self.connect_to_backend = True
        self.config = self._build_current_options()
        super(TestCurrentBackendTestingApi, self).setUp()

    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.target = orchestration_options.target
        return orchestration_options

    def test_full_api_on_current_backend(self):
        self._run_all_tests()


if __name__ == '__main__':
    main()
