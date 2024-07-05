# Copyright 2024 The GOE Authors. All rights reserved.
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

import pytest
from typing import TYPE_CHECKING

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.operation.partition_controls import (
    MISSING_PARTITION_GRANULARITY_EXCEPTION_TEXT,
    PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    sales_based_fact_assertion,
    synthetic_part_col_name,
)
from tests.integration.scenarios import scenario_constants
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import drop_backend_test_table
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api_ctx,
    get_test_messages_ctx,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from testlib.test_framework.backend_testing_api import BackendTestingApiInterface
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface
    from testlib.test_framework.offload_test_messages import OffloadTestMessages


FACT_DATE_Y = "BPART_FACT_DATE_Y"
FACT_DATE_M = "BPART_FACT_DATE_M"
FACT_DATE_D = "BPART_FACT_DATE_D"
FACT_TSTZ_Y = "BPART_FACT_TSTZ_Y"
FACT_TSTZ_M = "BPART_FACT_TSTZ_M"
FACT_TSTZ_D = "BPART_FACT_TSTZ_D"
FACT_DATE_STR = "BPART_FACT_DATE_STR"


@pytest.fixture
def config():
    return cached_current_options()


@pytest.fixture
def schema():
    return cached_default_test_user()


@pytest.fixture
def data_db(schema, config):
    data_db = data_db_name(schema, config)
    data_db = convert_backend_identifier_case(config, data_db)
    return data_db


def offload_date_assertion(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadTestMessages",
    data_db: str,
    table_name: str,
    part_col: str,
    granularity: str,
    expected_values=None,
    at_least_values=None,
):
    backend_name = convert_backend_identifier_case(config, table_name)
    backend_column = backend_api.get_column(data_db, backend_name, part_col)
    if not backend_column:
        messages.log("offload_date_assertion: Missing column %s" % part_col)
        return False
    if backend_api.partition_column_requires_synthetic_column(
        backend_column, granularity
    ):
        check_part_col = synthetic_part_col_name(granularity, part_col)
        if not backend_column_exists(
            backend_api, data_db, backend_name, check_part_col
        ):
            return False
        synth_column = backend_api.get_column(data_db, backend_name, check_part_col)
        if synth_column.is_string_based():
            # We can check that the synthetic values are of the correct length
            expected_len = {"Y": 4, "M": 7, "D": 10}[granularity]
            max_len = backend_api.get_max_column_length(
                data_db, backend_name, check_part_col
            )
            if max_len != expected_len:
                messages.log("%s != %s" % (max_len, expected_len))
                return False
    if expected_values or at_least_values:
        num_parts = backend_api.get_table_partition_count(data_db, backend_name)
        if expected_values and num_parts != expected_values:
            messages.log(
                "num_parts != expected_values: %s != %s" % (num_parts, expected_values)
            )
            return False
        if at_least_values and num_parts < at_least_values:
            messages.log(
                "num_parts < at_least_values: %s < %s" % (num_parts, at_least_values)
            )
            return False
    return True


def fact_as_date_tests(
    config: "OrchestrationConfig",
    schema: str,
    data_db: str,
    table_name: str,
    frontend_api: "FrontendTestingApiInterface",
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadTestMessages",
    test_id: str,
    test_type: str,
    backend_granularity: str,
):
    assert test_type in ["date", "tstz"]

    if test_type == "date":
        column_type_option = "date_columns_csv"
    else:
        column_type_option = "timestamp_tz_columns_csv"

    expected_backend_partitions = None
    at_least_backend_partitions = None
    if backend_granularity == offload_constants.PART_COL_GRANULARITY_YEAR:
        expected_backend_partitions = 1
    elif backend_granularity == offload_constants.PART_COL_GRANULARITY_MONTH:
        expected_backend_partitions = 2
    else:
        at_least_backend_partitions = 5

    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({test_id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, table_name, simple_partition_names=True
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, table_name
        ),
    )

    # Offload with requested granularity.
    options = {
        "owner_table": schema + "." + table_name,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
        column_type_option: "time_id",
        "offload_partition_columns": "time_id",
        "offload_partition_granularity": backend_granularity,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        test_constants.SALES_BASED_FACT_HV_2,
        check_backend_rowcount=True,
    )
    assert offload_date_assertion(
        config,
        backend_api,
        messages,
        data_db,
        table_name,
        "time_id",
        backend_granularity,
        expected_values=expected_backend_partitions,
        at_least_values=at_least_backend_partitions,
    )


def check_granularity_for_test(
    id: str,
    granularity: str,
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadTestMessages",
):
    if granularity not in backend_api.supported_date_based_partition_granularities():
        messages.log(
            f"Skipping {id} because granulrity {granularity} is not supported in backend"
        )
        pytest.skip(
            f"Skipping {id} because granulrity {granularity} is not supported in backend"
        )


def test_offload_backend_part_fact_as_date_year(config, schema, data_db):
    """Offload a table that is frontend partitioned by a date/time data type. Offload with granularity YEAR in backend."""
    id = "test_offload_backend_part_fact_as_date_year"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        if not backend_api.canonical_date_supported():
            messages.log(f"Skipping {id} because canonical_date_supported() == False")
            pytest.skip(f"Skipping {id} because canonical_date_supported() == False")

        check_granularity_for_test(
            id, offload_constants.PART_COL_GRANULARITY_YEAR, backend_api, messages
        )

        fact_as_date_tests(
            config,
            schema,
            data_db,
            FACT_DATE_Y,
            frontend_api,
            backend_api,
            messages,
            id,
            "date",
            offload_constants.PART_COL_GRANULARITY_YEAR,
        )


def test_offload_backend_part_fact_as_date_month(config, schema, data_db):
    id = "test_offload_backend_part_fact_as_date_month"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        if not backend_api.canonical_date_supported():
            messages.log(f"Skipping {id} because canonical_date_supported() == False")
            pytest.skip(f"Skipping {id} because canonical_date_supported() == False")

        check_granularity_for_test(
            id, offload_constants.PART_COL_GRANULARITY_MONTH, backend_api, messages
        )
        fact_as_date_tests(
            config,
            schema,
            data_db,
            FACT_DATE_M,
            frontend_api,
            backend_api,
            messages,
            id,
            "date",
            offload_constants.PART_COL_GRANULARITY_MONTH,
        )


def test_offload_backend_part_fact_as_date_day(config, schema, data_db):
    id = "test_offload_backend_part_fact_as_date_day"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        if not backend_api.canonical_date_supported():
            messages.log(f"Skipping {id} because canonical_date_supported() == False")
            pytest.skip(f"Skipping {id} because canonical_date_supported() == False")

        check_granularity_for_test(
            id, offload_constants.PART_COL_GRANULARITY_DAY, backend_api, messages
        )
        fact_as_date_tests(
            config,
            schema,
            data_db,
            FACT_DATE_D,
            frontend_api,
            backend_api,
            messages,
            id,
            "date",
            offload_constants.PART_COL_GRANULARITY_DAY,
        )


def test_offload_backend_part_fact_as_tstz_year(config, schema, data_db):
    """Offload a table that is frontend partitioned by a date/time data type. Offload with granularity YEAR in backend."""
    id = "test_offload_backend_part_fact_as_tstz_year"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        if not backend_api.canonical_date_supported():
            messages.log(f"Skipping {id} because canonical_date_supported() == False")
            pytest.skip(f"Skipping {id} because canonical_date_supported() == False")

        check_granularity_for_test(
            id, offload_constants.PART_COL_GRANULARITY_YEAR, backend_api, messages
        )
        fact_as_date_tests(
            config,
            schema,
            data_db,
            FACT_TSTZ_Y,
            frontend_api,
            backend_api,
            messages,
            id,
            "tstz",
            offload_constants.PART_COL_GRANULARITY_YEAR,
        )


def test_offload_backend_part_fact_as_tstz_month(config, schema, data_db):
    id = "test_offload_backend_part_fact_as_tstz_month"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        if not backend_api.canonical_date_supported():
            messages.log(f"Skipping {id} because canonical_date_supported() == False")
            pytest.skip(f"Skipping {id} because canonical_date_supported() == False")

        check_granularity_for_test(
            id, offload_constants.PART_COL_GRANULARITY_MONTH, backend_api, messages
        )
        fact_as_date_tests(
            config,
            schema,
            data_db,
            FACT_TSTZ_M,
            frontend_api,
            backend_api,
            messages,
            id,
            "tstz",
            offload_constants.PART_COL_GRANULARITY_MONTH,
        )


def test_offload_backend_part_fact_as_tstz_day(config, schema, data_db):
    id = "test_offload_backend_part_fact_as_tstz_day"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        if not backend_api.canonical_date_supported():
            messages.log(f"Skipping {id} because canonical_date_supported() == False")
            pytest.skip(f"Skipping {id} because canonical_date_supported() == False")

        check_granularity_for_test(
            id, offload_constants.PART_COL_GRANULARITY_DAY, backend_api, messages
        )
        fact_as_date_tests(
            config,
            schema,
            data_db,
            FACT_TSTZ_D,
            frontend_api,
            backend_api,
            messages,
            id,
            "tstz",
            offload_constants.PART_COL_GRANULARITY_DAY,
        )


def partition_by_string_supported_exception(backend_api, exception_text=None):
    if backend_api.partition_by_string_supported():
        return exception_text
    else:
        return PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT


def test_offload_backend_part_date_as_str(config, schema, data_db):
    id = "test_offload_backend_part_date_as_str"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )
        table_name = FACT_DATE_STR

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_fact_create_ddl(
                schema, table_name, simple_partition_names=True
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
        )
        # Offload 1st partition with TIME_ID as a STRING in backend with date based granularity rather than string based.
        # Expect to fail.
        options = {
            "owner_table": schema + "." + table_name,
            "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
            "variable_string_columns_csv": "time_id",
            "reset_backend_table": True,
            "create_backend_db": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=partition_by_string_supported_exception(
                backend_api, MISSING_PARTITION_GRANULARITY_EXCEPTION_TEXT
            ),
        )

        # Offload 1st partition with TIME_ID as a STRING in backend.
        # We have assertion for backends that support this and an expected exception for those that do not.
        options = {
            "owner_table": schema + "." + table_name,
            "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
            "variable_string_columns_csv": "time_id",
            "offload_partition_granularity": "4",
            "reset_backend_table": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=partition_by_string_supported_exception(
                backend_api
            ),
        )
        if not partition_by_string_supported_exception(backend_api):
            assert sales_based_fact_assertion(
                config,
                backend_api,
                frontend_api,
                messages,
                repo_client,
                schema,
                data_db,
                table_name,
                test_constants.SALES_BASED_FACT_HV_1,
                check_backend_rowcount=True,
                offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
            )
            assert backend_column_exists(
                backend_api,
                data_db,
                table_name,
                "time_id",
                backend_api.backend_test_type_canonical_string(),
            )
            assert backend_column_exists(
                backend_api,
                data_db,
                table_name,
                "gl_part_4_time_id",
                backend_api.backend_test_type_canonical_string(),
            )
