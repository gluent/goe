# Copyright 2024 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from goe.offload.bigquery import bigquery_column
from goe.offload.column_metadata import (
    CanonicalColumn,
    GOE_TYPE_INTEGER_1,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.operation import table_structure_checks as module_under_test
from goe.offload.oracle import oracle_column

from tests.unit.test_functions import (
    build_fake_backend_table,
    build_fake_oracle_table,
    build_mock_options,
    FAKE_ORACLE_BQ_ENV,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig


@pytest.fixture(scope="module")
def ora_bq_config() -> "OrchestrationConfig":
    return build_mock_options(FAKE_ORACLE_BQ_ENV)


@pytest.fixture(scope="module")
def messages():
    return OffloadMessages()


@pytest.fixture
def bigquery_table(ora_bq_config, messages):
    return build_fake_backend_table(ora_bq_config, messages)


@pytest.fixture
def oracle_table(ora_bq_config, messages):
    return build_fake_oracle_table(ora_bq_config, messages)


@pytest.mark.parametrize(
    "frontend_columns,backend_columns,expected_extra_frontend_names,expected_missing_frontend_names",
    [
        # Happy path, no expected mismatches.
        (
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("col_2", GOE_TYPE_INTEGER_1),
            ],
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("col_2", GOE_TYPE_INTEGER_1),
            ],
            [],
            [],
        ),
        # Happy path with different case, no expected mismatches.
        (
            [
                CanonicalColumn("Col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("COL_2", GOE_TYPE_INTEGER_1),
            ],
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("cOl_2", GOE_TYPE_INTEGER_1),
            ],
            [],
            [],
        ),
        # Missing backend column.
        (
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("col_2", GOE_TYPE_INTEGER_1),
            ],
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
            ],
            ["COL_2"],
            [],
        ),
        # Missing frontend column.
        (
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
            ],
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("col_2", GOE_TYPE_INTEGER_1),
            ],
            [],
            ["COL_2"],
        ),
        # Missing backend columns.
        (
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
                CanonicalColumn("col_2", GOE_TYPE_INTEGER_1),
                CanonicalColumn("col_3", GOE_TYPE_INTEGER_1),
            ],
            [
                CanonicalColumn("col_1", GOE_TYPE_INTEGER_1),
            ],
            ["COL_2", "COL_3"],
            [],
        ),
    ],
)
def test_check_table_columns_by_name(
    frontend_columns: list,
    backend_columns: list,
    expected_extra_frontend_names: list,
    expected_missing_frontend_names: list,
):
    extra_frontend_names, missing_frontend_names = (
        module_under_test.check_table_columns_by_name(frontend_columns, backend_columns)
    )
    assert extra_frontend_names == expected_extra_frontend_names
    assert missing_frontend_names == expected_missing_frontend_names


@pytest.mark.parametrize(
    "extra_frontend_names,missing_frontend_names",
    [
        (
            [],
            [],
        ),
        # Missing backend column.
        (
            ["COL_2"],
            [],
        ),
        # Missing frontend column.
        (
            [],
            ["COL_2"],
        ),
        # Missing backend columns.
        (
            ["COL_2", "COL_3"],
            [],
        ),
    ],
)
def test_check_table_columns_by_name_logging(
    extra_frontend_names: list, missing_frontend_names: list, messages
):
    fake_table = mock.MagicMock()
    fake_table.frontend_db_name = lambda: "System A"
    fake_table.backend_db_name = lambda: "System B"
    module_under_test.check_table_columns_by_name_logging(
        fake_table, fake_table, extra_frontend_names, missing_frontend_names, messages
    )


@pytest.mark.parametrize(
    "frontend_columns,backend_columns,expected_return_dict",
    [
        # Happy path, no expected mismatches.
        (
            [
                oracle_column.OracleColumn("COL_N1", oracle_column.ORACLE_TYPE_NUMBER),
                oracle_column.OracleColumn("COL_N2", oracle_column.ORACLE_TYPE_NUMBER),
                oracle_column.OracleColumn(
                    "COL_S1", oracle_column.ORACLE_TYPE_VARCHAR2
                ),
                oracle_column.OracleColumn("COL_D1", oracle_column.ORACLE_TYPE_DATE),
                oracle_column.OracleColumn("COL_D2", oracle_column.ORACLE_TYPE_DATE),
                oracle_column.OracleColumn("COL_D3", oracle_column.ORACLE_TYPE_DATE),
                oracle_column.OracleColumn(
                    "COL_T1", oracle_column.ORACLE_TYPE_TIMESTAMP
                ),
                oracle_column.OracleColumn(
                    "COL_T2", oracle_column.ORACLE_TYPE_TIMESTAMP
                ),
                oracle_column.OracleColumn(
                    "COL_T3", oracle_column.ORACLE_TYPE_TIMESTAMP
                ),
            ],
            [
                bigquery_column.BigQueryColumn(
                    "COL_N1", bigquery_column.BIGQUERY_TYPE_NUMERIC
                ),
                bigquery_column.BigQueryColumn(
                    "COL_N2", bigquery_column.BIGQUERY_TYPE_INT64
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S1", bigquery_column.BIGQUERY_TYPE_STRING
                ),
                bigquery_column.BigQueryColumn(
                    "COL_D1", bigquery_column.BIGQUERY_TYPE_DATE
                ),
                bigquery_column.BigQueryColumn(
                    "COL_D2", bigquery_column.BIGQUERY_TYPE_DATETIME
                ),
                bigquery_column.BigQueryColumn(
                    "COL_D3", bigquery_column.BIGQUERY_TYPE_TIMESTAMP
                ),
                bigquery_column.BigQueryColumn(
                    "COL_T1", bigquery_column.BIGQUERY_TYPE_DATE
                ),
                bigquery_column.BigQueryColumn(
                    "COL_T2", bigquery_column.BIGQUERY_TYPE_DATETIME
                ),
                bigquery_column.BigQueryColumn(
                    "COL_T3", bigquery_column.BIGQUERY_TYPE_TIMESTAMP
                ),
            ],
            {},
        ),
        # Dates to strings.
        (
            [
                oracle_column.OracleColumn("COL_D1", oracle_column.ORACLE_TYPE_DATE),
                oracle_column.OracleColumn(
                    "COL_T1", oracle_column.ORACLE_TYPE_TIMESTAMP
                ),
            ],
            [
                bigquery_column.BigQueryColumn(
                    "COL_D1", bigquery_column.BIGQUERY_TYPE_STRING
                ),
                bigquery_column.BigQueryColumn(
                    "COL_T1", bigquery_column.BIGQUERY_TYPE_STRING
                ),
            ],
            {},
        ),
        # Numbers to strings is not currently supported.
        (
            [
                oracle_column.OracleColumn("COL_N1", oracle_column.ORACLE_TYPE_NUMBER),
                oracle_column.OracleColumn("COL_N2", oracle_column.ORACLE_TYPE_NUMBER),
            ],
            [
                bigquery_column.BigQueryColumn(
                    "COL_N1", bigquery_column.BIGQUERY_TYPE_NUMERIC
                ),
                bigquery_column.BigQueryColumn(
                    "COL_N2", bigquery_column.BIGQUERY_TYPE_STRING
                ),
            ],
            {
                "COL_N2": bigquery_column.BIGQUERY_TYPE_STRING,
            },
        ),
        # Strings to numbers is not currently supported.
        (
            [
                oracle_column.OracleColumn(
                    "COL_S1", oracle_column.ORACLE_TYPE_VARCHAR2
                ),
                oracle_column.OracleColumn(
                    "COL_S2", oracle_column.ORACLE_TYPE_VARCHAR2
                ),
                oracle_column.OracleColumn(
                    "COL_S3", oracle_column.ORACLE_TYPE_NVARCHAR2
                ),
                oracle_column.OracleColumn("COL_S4", oracle_column.ORACLE_TYPE_CLOB),
            ],
            [
                bigquery_column.BigQueryColumn(
                    "COL_S1", bigquery_column.BIGQUERY_TYPE_STRING
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S2", bigquery_column.BIGQUERY_TYPE_NUMERIC
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S3", bigquery_column.BIGQUERY_TYPE_BIGNUMERIC
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S4", bigquery_column.BIGQUERY_TYPE_NUMERIC
                ),
            ],
            {
                "COL_S2": bigquery_column.BIGQUERY_TYPE_NUMERIC,
                "COL_S3": bigquery_column.BIGQUERY_TYPE_BIGNUMERIC,
                "COL_S4": bigquery_column.BIGQUERY_TYPE_NUMERIC,
            },
        ),
        # Strings to dates is not currently supported.
        (
            [
                oracle_column.OracleColumn(
                    "COL_S1", oracle_column.ORACLE_TYPE_VARCHAR2
                ),
                oracle_column.OracleColumn(
                    "COL_S2", oracle_column.ORACLE_TYPE_VARCHAR2
                ),
                oracle_column.OracleColumn(
                    "COL_S3", oracle_column.ORACLE_TYPE_NVARCHAR2
                ),
                oracle_column.OracleColumn("COL_S4", oracle_column.ORACLE_TYPE_CLOB),
            ],
            [
                bigquery_column.BigQueryColumn(
                    "COL_S1", bigquery_column.BIGQUERY_TYPE_STRING
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S2", bigquery_column.BIGQUERY_TYPE_DATE
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S3", bigquery_column.BIGQUERY_TYPE_DATETIME
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S4", bigquery_column.BIGQUERY_TYPE_TIMESTAMP
                ),
            ],
            {
                "COL_S2": bigquery_column.BIGQUERY_TYPE_DATE,
                "COL_S3": bigquery_column.BIGQUERY_TYPE_DATETIME,
                "COL_S4": bigquery_column.BIGQUERY_TYPE_TIMESTAMP,
            },
        ),
        # Backend TIME is not supported.
        (
            [
                oracle_column.OracleColumn("COL_N1", oracle_column.ORACLE_TYPE_NUMBER),
                oracle_column.OracleColumn("COL_N2", oracle_column.ORACLE_TYPE_NUMBER),
                oracle_column.OracleColumn(
                    "COL_S1", oracle_column.ORACLE_TYPE_VARCHAR2
                ),
                oracle_column.OracleColumn("COL_D1", oracle_column.ORACLE_TYPE_DATE),
                oracle_column.OracleColumn(
                    "COL_T1", oracle_column.ORACLE_TYPE_TIMESTAMP
                ),
            ],
            [
                bigquery_column.BigQueryColumn(
                    "COL_N1", bigquery_column.BIGQUERY_TYPE_NUMERIC
                ),
                bigquery_column.BigQueryColumn(
                    "COL_N2", bigquery_column.BIGQUERY_TYPE_TIME
                ),
                bigquery_column.BigQueryColumn(
                    "COL_S1", bigquery_column.BIGQUERY_TYPE_TIME
                ),
                bigquery_column.BigQueryColumn(
                    "COL_D1", bigquery_column.BIGQUERY_TYPE_TIME
                ),
                bigquery_column.BigQueryColumn(
                    "COL_T1", bigquery_column.BIGQUERY_TYPE_TIME
                ),
            ],
            {
                "COL_N2": bigquery_column.BIGQUERY_TYPE_TIME,
                "COL_S1": bigquery_column.BIGQUERY_TYPE_TIME,
                "COL_D1": bigquery_column.BIGQUERY_TYPE_TIME,
                "COL_T1": bigquery_column.BIGQUERY_TYPE_TIME,
            },
        ),
    ],
)
def test_check_table_columns_by_type_oracle_to_bigquery(
    frontend_columns: list,
    backend_columns: list,
    expected_return_dict: dict,
    oracle_table,
    bigquery_table,
):
    oracle_table._columns = frontend_columns
    oracle_table._columns_with_partition_info = frontend_columns
    bigquery_table._columns = backend_columns
    result = module_under_test.check_table_columns_by_type(oracle_table, bigquery_table)
    assert result == expected_return_dict


@pytest.mark.parametrize(
    "invalid_combinations",
    [
        {},
        {
            "COL_N2": bigquery_column.BIGQUERY_TYPE_TIME,
            "COL_S1": bigquery_column.BIGQUERY_TYPE_TIME,
            "COL_D1": bigquery_column.BIGQUERY_TYPE_TIME,
            "COL_T1": bigquery_column.BIGQUERY_TYPE_TIME,
        },
    ],
)
def test_check_table_columns_by_type_logging(invalid_combinations: dict, messages):
    fake_table = mock.MagicMock()
    fake_table.frontend_db_name = lambda: "System A"
    fake_table.backend_db_name = lambda: "System B"
    fake_table.get_column = lambda x: CanonicalColumn("COL-NAME", GOE_TYPE_INTEGER_1)
    module_under_test.check_table_columns_by_type_logging(
        fake_table, fake_table, invalid_combinations, messages
    )
