""" TestOffloadSourceTable: Unit test library to test API for all supported frontend RDBMSs
    This is split into two categories:
    1) For all possible frontends test API calls that do not need to connect to the system.
       Because there is no connection we can fake any backend and test functionality. These classes
       have the system in the name: TestOracleOffloadSourceTable, TestMSSQLOffloadSourceTable, etc
    2) For the current backend test API calls that need to connect to the system.
       This class has Current in the name: TestCurrentOffloadSourceTable
"""
from unittest import TestCase, main

from numpy import datetime64

from tests.offload.unittest_functions import (
    build_current_options,
    build_non_connecting_options,
    get_default_test_user,
)

from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import (
    DBTYPE_ORACLE,
    DBTYPE_MSSQL,
    DBTYPE_NETEZZA,
    DBTYPE_TERADATA,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_source_table import OffloadSourceTableException
from goe.offload.microsoft.mssql_column import (
    MSSQL_TYPE_BIGINT,
    MSSQL_TYPE_DATETIME,
    MSSQL_TYPE_VARCHAR,
)
from goe.offload.netezza.netezza_column import (
    NETEZZA_TYPE_BIGINT,
    NETEZZA_TYPE_TIMESTAMP,
)
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_VARCHAR2,
)
from goe.util.ora_query import get_oracle_connection
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)


class TestOffloadSourceTable(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestOffloadSourceTable, self).__init__(*args, **kwargs)
        self.api = None
        self.test_api = None
        self.config = None
        self.db = None
        self.table = None
        self.connect_to_db = None

    def setUp(self):
        self.api = None
        self.messages = OffloadMessages()
        if self.connect_to_db:
            self.test_api = frontend_testing_api_factory(
                self.config.db_type, self.config, self.messages, dry_run=True
            )
            # Ideally this is a partitioned table in order to test partition calls
            self.db, self.table = self._get_real_db_table()
            if not self.db or not self.table:
                raise NotImplementedError(
                    "Unable to test current config when no test table available"
                )
            self.api = OffloadSourceTable.create(
                self.db,
                self.table,
                self.config,
                self.messages,
                dry_run=True,
                do_not_connect=bool(not self.connect_to_db),
            )
            if not self.api.exists():
                raise NotImplementedError(
                    "Unable to test current config because test table is missing: %s.%s"
                    % (self.db, self.table)
                )

        if not self.api:
            self.db = "any_db"
            self.table = "some_table"
            self.api = OffloadSourceTable.create(
                self.db,
                self.table,
                self.config,
                self.messages,
                dry_run=True,
                do_not_connect=bool(not self.connect_to_db),
            )

    def _get_real_db_table(self):
        """In order to run the connected tests we just need any RDBMS table, ideally
        a partitioned one as that will exercise more code.
        We can't get any context from integration test config so this is a hacky
        method to pick a nice candidate table from the DB.
        """
        table_name = "SALES"
        try:
            test_user = get_default_test_user().upper()
            return self.test_api.get_test_table_owner(test_user, table_name), table_name
        except Exception:
            print("Unable to test current config: %s" % self.config.db_type)
            raise

    def _test__get_column_low_high_values(self):
        if self.connect_to_db and self.config.db_type == DBTYPE_ORACLE:
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
        if self.connect_to_db:
            # 6 below is for milliseconds, False is for backend NaN support. Actual values unimportant
            self.assertIn(self.api.check_data_types_supported(6, False), [True, False])

    def _test_columns(self):
        if self.connect_to_db:
            # Referencing the property will load the column from backend
            self.api.columns
            try:
                # Test that setter property works
                self.api.columns = self.api.columns
            except OffloadSourceTableException:
                # We expect "Set of columns is only supported when the table does NOT exist"
                pass

    def _test_data_types_in_use(self):
        if self.connect_to_db:
            self.api.data_types_in_use()

    def _test_enclose_identifier(self):
        self.api.enclose_identifier(self.db)

    def _test_exists(self):
        if self.connect_to_db:
            self.api.exists()

    def _test_gen_default_date_column(self):
        self.api.gen_default_date_column("some_column")

    def _test_gen_default_numeric_column(self):
        self.api.gen_default_numeric_column("some_column")

    def _test_get_column(self):
        if self.connect_to_db:
            self.api.get_column("some_column")

    def _test_get_current_scn(self):
        if self.connect_to_db:
            try:
                self.api.get_current_scn()
            except NotImplementedError:
                pass

    def _test_get_hash_bucket_candidate(self):
        if self.connect_to_db:
            try:
                self.api.get_hash_bucket_candidate()
            except NotImplementedError:
                pass

    def _test_get_hash_bucket_last_resort(self):
        if self.connect_to_db:
            self.api.get_hash_bucket_last_resort()

    def _test_get_max_partition_size(self):
        if self.connect_to_db:
            self.api.get_max_partition_size()

    def _test_get_minimum_partition_key_data(self):
        if self.connect_to_db and self.api.is_partitioned():
            self.api.get_minimum_partition_key_data()

    def _test_get_partitions(self):
        if self.connect_to_db:
            self.api.get_partitions()
            self.api.get_partitions(populate_hvs=False)

    def _test_get_primary_index_columns(self):
        if self.connect_to_db:
            self.api.get_primary_index_columns()

    def _test_get_primary_key_columns(self):
        if self.connect_to_db:
            self.api.get_primary_key_columns()

    def _test_get_referenced_dependencies(self):
        if self.connect_to_db and self.config == DBTYPE_ORACLE:
            self.api.get_referenced_dependencies()

    def _test_get_session_option(self):
        if self.connect_to_db and self.test_api:
            test_options = self.test_api.unit_test_query_options()
            if test_options:
                test_option = list(test_options.keys()).pop()
                self.assertIsNotNone(self.api.get_session_option(test_option))

    def _test_get_stored_object_dependent_schemas(self):
        if self.connect_to_db and self.config == DBTYPE_ORACLE:
            self.api.get_stored_object_dependent_schemas()

    def _test_get_suitable_sample_size(self):
        if self.connect_to_db:
            self.api.get_suitable_sample_size()

    def _test_hash_bucket_unsuitable_data_types(self):
        self.api.hash_bucket_unsuitable_data_types()

    def _test_has_rowdependencies(self):
        if self.connect_to_db and self.config == DBTYPE_ORACLE:
            self.api.has_rowdependencies()

    def _test_max_column_name_length(self):
        if self.connect_to_db:
            self.assertIsInstance(self.api.max_column_name_length(), int)
            self.assertGreater(self.api.max_column_name_length(), 0)

    def _test_max_schema_name_length(self):
        if self.connect_to_db:
            self.assertIsInstance(self.api.max_schema_name_length(), int)
            self.assertGreater(self.api.max_schema_name_length(), 0)

    def _test_max_datetime_scale(self):
        self.assertIsInstance(self.api.max_datetime_scale(), int)

    def _test_max_table_name_length(self):
        if self.connect_to_db:
            self.assertIsInstance(self.api.max_table_name_length(), int)
            self.assertGreater(self.api.max_table_name_length(), 0)

    def _test_max_datetime_value(self):
        self.api.max_datetime_value()

    def _test_min_datetime_value(self):
        self.api.min_datetime_value()

    def _test_nan_capable_data_types(self):
        self.api.nan_capable_data_types()

    def _test_numeric_literal_to_python(self):
        try:
            self.api.numeric_literal_to_python("123")
        except NotImplementedError:
            pass

    def _test_parallel_query_hint(self):
        if self.config == DBTYPE_ORACLE:
            self.assertFalse(bool(self.api.parallel_query_hint(None)))
            self.assertIsInstance(self.api.parallel_query_hint(0), str)
            self.assertIsInstance(self.api.parallel_query_hint(1), str)
            self.assertIsInstance(self.api.parallel_query_hint(2), str)

    def _test_partition_columns(self):
        if self.connect_to_db:
            self.api.partition_columns

    def _test_partition_has_rows(self):
        if self.connect_to_db and self.api.is_partitioned():
            partitions = self.api.get_partitions()
            if partitions:
                self.api.partition_has_rows(partitions[0].partition_name)

    def _test_rdbms_literal_to_python(self):
        try:
            self.api.rdbms_literal_to_python(
                self.api.gen_default_numeric_column("some_col"),
                "123",
                None,
                strict=False,
            )
            self.api.rdbms_literal_to_python(
                self.api.gen_default_date_column("some_col"), "123", None, strict=False
            )
        except NotImplementedError:
            pass

    def _test_sample_rdbms_data_types(self):
        if self.connect_to_db:
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

    def _test_supported_data_types(self):
        self.api.supported_data_types()

    def _test_supported_list_partition_data_type(self):
        try:
            self.api.supported_list_partition_data_type(
                self.api.gen_default_numeric_column("some_col").data_type
            )
        except NotImplementedError:
            pass

    def _test_supported_range_partition_data_type(self):
        try:
            self.api.supported_range_partition_data_type(
                self.api.gen_default_numeric_column("some_col").data_type
            )
        except NotImplementedError:
            pass

    def _test_to_rdbms_literal_with_sql_conv_fn(self):
        if self.config.db_type == DBTYPE_ORACLE:
            self.api.to_rdbms_literal_with_sql_conv_fn(123, ORACLE_TYPE_NUMBER)
            self.api.to_rdbms_literal_with_sql_conv_fn(
                self.api.min_datetime_value(), ORACLE_TYPE_DATE
            )
            self.api.to_rdbms_literal_with_sql_conv_fn(
                datetime64(self.api.min_datetime_value()), ORACLE_TYPE_DATE
            )
            self.api.to_rdbms_literal_with_sql_conv_fn(
                self.api.min_datetime_value(), ORACLE_TYPE_TIMESTAMP
            )
            self.api.to_rdbms_literal_with_sql_conv_fn(
                datetime64(self.api.min_datetime_value()), ORACLE_TYPE_TIMESTAMP
            )
            self.api.to_rdbms_literal_with_sql_conv_fn("Hello", ORACLE_TYPE_VARCHAR2)
        elif self.config.db_type == DBTYPE_MSSQL:
            try:
                self.api.to_rdbms_literal_with_sql_conv_fn(123, MSSQL_TYPE_BIGINT)
                self.api.to_rdbms_literal_with_sql_conv_fn(
                    self.api.min_datetime_value(), MSSQL_TYPE_DATETIME
                )
                self.api.to_rdbms_literal_with_sql_conv_fn(
                    datetime64(self.api.min_datetime_value()), MSSQL_TYPE_DATETIME
                )
                self.api.to_rdbms_literal_with_sql_conv_fn("Hello", MSSQL_TYPE_VARCHAR)
            except NotImplementedError:
                pass
        elif self.config.db_type == DBTYPE_NETEZZA:
            try:
                self.api.to_rdbms_literal_with_sql_conv_fn(123, NETEZZA_TYPE_BIGINT)
                self.api.to_rdbms_literal_with_sql_conv_fn(
                    self.api.min_datetime_value(), NETEZZA_TYPE_TIMESTAMP
                )
                self.api.to_rdbms_literal_with_sql_conv_fn(
                    datetime64(self.api.min_datetime_value()), NETEZZA_TYPE_TIMESTAMP
                )
                self.api.to_rdbms_literal_with_sql_conv_fn("Hello", MSSQL_TYPE_VARCHAR)
            except NotImplementedError:
                pass

    def _run_all_tests(self):
        self._test__get_column_low_high_values()
        self._test_check_data_types_supported()
        self._test_columns()
        self._test_data_types_in_use()
        self._test_enclose_identifier()
        self._test_exists()
        self._test_gen_default_numeric_column()
        self._test_gen_default_date_column()
        self._test_get_column()
        self._test_get_hash_bucket_candidate()
        self._test_get_hash_bucket_last_resort()
        self._test_get_max_partition_size()
        self._test_get_partitions()
        self._test_get_minimum_partition_key_data()
        self._test_get_primary_key_columns()
        self._test_get_referenced_dependencies()
        self._test_get_session_option()
        self._test_get_stored_object_dependent_schemas()
        self._test_get_suitable_sample_size()
        self._test_get_suitable_sample_size()
        self._test_hash_bucket_unsuitable_data_types()
        self._test_has_rowdependencies()
        self._test_max_column_name_length()
        self._test_max_datetime_scale()
        self._test_max_datetime_value()
        self._test_max_table_name_length()
        self._test_min_datetime_value()
        self._test_nan_capable_data_types()
        self._test_numeric_literal_to_python()
        self._test_parallel_query_hint()
        self._test_partition_columns()
        self._test_partition_has_rows()
        self._test_rdbms_literal_to_python()
        self._test_sample_rdbms_data_types()
        self._test_supported_data_types()
        self._test_supported_list_partition_data_type()
        self._test_supported_range_partition_data_type()
        self._test_to_rdbms_literal_with_sql_conv_fn()


class TestOracleOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.connect_to_db = False
        self.config = build_non_connecting_options(DBTYPE_ORACLE)
        super(TestOracleOffloadSourceTable, self).setUp()

    def test_all_non_connecting_oracle_tests(self):
        self._run_all_tests()


class TestMSSQLOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.connect_to_db = False
        self.config = build_non_connecting_options(DBTYPE_MSSQL)
        super(TestMSSQLOffloadSourceTable, self).setUp()

    def test_all_non_connecting_mssql_tests(self):
        self._run_all_tests()


class TestNetezzaOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.connect_to_db = False
        self.config = build_non_connecting_options(DBTYPE_NETEZZA)
        super(TestNetezzaOffloadSourceTable, self).setUp()

    def test_all_non_connecting_netezza_tests(self):
        self._run_all_tests()


class TestTeradataOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.connect_to_db = False
        self.config = build_non_connecting_options(DBTYPE_TERADATA)
        super(TestTeradataOffloadSourceTable, self).setUp()

    def test_all_non_connecting_teradata_tests(self):
        self._run_all_tests()


class TestCurrentOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.connect_to_db = True
        self.config = self._build_current_options()
        super(TestCurrentOffloadSourceTable, self).setUp()

    def _build_current_options(self):
        return build_current_options()

    def test_full_api_on_current_rdbms(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
