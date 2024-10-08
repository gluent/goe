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

from unittest import TestCase

from numpy import datetime64

from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.microsoft.mssql_column import (
    MSSQL_TYPE_BIGINT,
    MSSQL_TYPE_DATETIME,
    MSSQL_TYPE_VARCHAR,
)
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_VARCHAR2,
)
from goe.offload.oracle import oracle_offload_source_table
from tests.unit.test_functions import (
    build_mock_options,
    optional_sql_server_dependency_exception,
    optional_teradata_dependency_exception,
    FAKE_MSSQL_ENV,
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

    def _run_all_tests(self):
        if not self.api:
            return
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
        try:
            super(TestMSSQLOffloadSourceTable, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_sql_server_dependency_exception(e):
                raise

    def test_all_non_connecting_mssql_tests(self):
        self._run_all_tests()


class TestTeradataOffloadSourceTable(TestOffloadSourceTable):
    def setUp(self):
        self.config = self._get_mock_config(FAKE_TERADATA_ENV)
        try:
            super(TestTeradataOffloadSourceTable, self).setUp()
        except ModuleNotFoundError as e:
            if not optional_teradata_dependency_exception(e):
                raise

    def test_all_non_connecting_teradata_tests(self):
        self._run_all_tests()


def test_oracle_version_is_smart_scan_unsafe():
    assert isinstance(
        oracle_offload_source_table.oracle_version_is_smart_scan_unsafe(
            oracle_offload_source_table.ORACLE_VERSION_WITH_CELL_OFFLOAD_PROCESSING
        ),
        bool,
    )
    assert oracle_offload_source_table.oracle_version_is_smart_scan_unsafe(
        oracle_offload_source_table.ORACLE_VERSION_WITH_CELL_OFFLOAD_PROCESSING
    )
    assert not oracle_offload_source_table.oracle_version_is_smart_scan_unsafe(
        oracle_offload_source_table.ORACLE_VERSION_SAFE_FOR_CELL_OFFLOAD_PROCESSING
    )
