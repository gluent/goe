from unittest import TestCase

from numpy import datetime64

from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_NETEZZA,
    DBTYPE_ORACLE,
)
from goe.offload.offload_messages import OffloadMessages
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
from tests.unit.test_functions import (
    build_mock_options,
    FAKE_MSSQL_ENV,
    FAKE_NETEZZA_ENV,
    FAKE_ORACLE_ENV,
    FAKE_TERADATA_ENV,
)


class TestOffloadSourceTable(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestOffloadSourceTable, self).__init__(*args, **kwargs)
        self.api = None
        self.test_api = None
        self.config = None
        self.db = "any_db"
        self.table = "some_table"
        self.connect_to_db = False

    def setUp(self):
        self.api = None
        self.messages = OffloadMessages()
        self.api = OffloadSourceTable.create(
            self.db,
            self.table,
            self.config,
            self.messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_db),
        )

    def _get_mock_config(self, mock_env: dict):
        return build_mock_options(mock_env)

    def _test_enclose_identifier(self):
        self.api.enclose_identifier(self.db)

    def _test_gen_default_date_column(self):
        self.api.gen_default_date_column("some_column")

    def _test_gen_default_numeric_column(self):
        self.api.gen_default_numeric_column("some_column")

    def _test_hash_bucket_unsuitable_data_types(self):
        self.api.hash_bucket_unsuitable_data_types()

    def _test_max_datetime_scale(self):
        self.assertIsInstance(self.api.max_datetime_scale(), int)

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
        self._test_enclose_identifier()
        self._test_gen_default_numeric_column()
        self._test_gen_default_date_column()
        self._test_hash_bucket_unsuitable_data_types()
        self._test_max_datetime_scale()
        self._test_max_datetime_value()
        self._test_min_datetime_value()
        self._test_nan_capable_data_types()
        self._test_numeric_literal_to_python()
        self._test_parallel_query_hint()
        self._test_rdbms_literal_to_python()
        self._test_supported_data_types()
        self._test_supported_list_partition_data_type()
        self._test_supported_range_partition_data_type()
        self._test_to_rdbms_literal_with_sql_conv_fn()


class TestOracleOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.config = self._get_mock_config(FAKE_ORACLE_ENV)
        super(TestOracleOffloadSourceTable, self).setUp()

    def test_all_non_connecting_oracle_tests(self):
        self._run_all_tests()


class TestMSSQLOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.config = self._get_mock_config(FAKE_MSSQL_ENV)
        super(TestMSSQLOffloadSourceTable, self).setUp()

    def test_all_non_connecting_mssql_tests(self):
        self._run_all_tests()


class TestNetezzaOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.config = self._get_mock_config(FAKE_NETEZZA_ENV)
        super(TestNetezzaOffloadSourceTable, self).setUp()

    def test_all_non_connecting_netezza_tests(self):
        self._run_all_tests()


class TestTeradataOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.config = self._get_mock_config(FAKE_TERADATA_ENV)
        super(TestTeradataOffloadSourceTable, self).setUp()

    def test_all_non_connecting_teradata_tests(self):
        self._run_all_tests()
