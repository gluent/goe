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

import pytest

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_source_data import (
    TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT,
)
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_VARCHAR2,
)
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    sales_based_fact_assertion,
    synthetic_part_col_name,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
    gen_drop_sales_based_fact_partition_ddls,
    sales_based_fact_partition_exists,
)
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


LPA_FACT_TABLE_NUM = "STORY_LIST_RPA_NUM"
LPA_FACT_TABLE_DATE = "STORY_LIST_RPA_DATE"
LPA_FACT_TABLE_TS = "STORY_LIST_RPA_TS"
LPA_FACT_TABLE_STR = "STORY_LIST_RPA_STR"
LPA_FACT_TABLE_NUM_UDF = "STORY_LIST_RPA_UNUM"
LPA_FACT_TABLE_STR_UDF = "STORY_LIST_RPA_USTR"
LIST_RANGE_TABLE = "STORY_LIST_RPA_RANGE"


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


def offload_list_as_range_ipa_standard_story_tests(
    schema,
    data_db,
    table_name,
    config,
    backend_api,
    frontend_api,
    messages,
    repo_client,
    part_key_type,
    partition_function=None,
):
    if partition_function and not backend_api.goe_partition_functions_supported():
        pytest.skip(
            f"Skipping {table_name} partition function tests due to goe_partition_functions_supported() == False"
        )

    hv_1 = test_constants.SALES_BASED_LIST_HV_1
    hv_2 = test_constants.SALES_BASED_LIST_HV_2
    hv_3 = test_constants.SALES_BASED_LIST_HV_3
    hv_4 = test_constants.SALES_BASED_LIST_HV_4
    hv_5 = test_constants.SALES_BASED_LIST_HV_5
    less_than_option = "less_than_value"
    udf = None
    gl_part_column_check_name = None
    if table_name == LPA_FACT_TABLE_NUM_UDF:
        table_name = LPA_FACT_TABLE_NUM_UDF
        udf = data_db + "." + partition_function
        gl_part_column_check_name = synthetic_part_col_name("U0", "yrmon")
        gl_part_column_check_name = convert_backend_identifier_case(
            config, gl_part_column_check_name
        )
    elif table_name == LPA_FACT_TABLE_STR_UDF:
        table_name = LPA_FACT_TABLE_STR_UDF
        udf = data_db + "." + partition_function
        gl_part_column_check_name = synthetic_part_col_name("U0", "yrmon")
        gl_part_column_check_name = convert_backend_identifier_case(
            config, gl_part_column_check_name
        )
    elif table_name in [LPA_FACT_TABLE_TS, LPA_FACT_TABLE_DATE]:
        hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_2 = test_constants.SALES_BASED_FACT_HV_2
        hv_3 = test_constants.SALES_BASED_FACT_HV_3
        hv_4 = test_constants.SALES_BASED_FACT_HV_4
        hv_5 = test_constants.SALES_BASED_FACT_HV_5
        less_than_option = "older_than_date"
    backend_name = convert_backend_identifier_case(config, table_name)

    # Try to force LIST table to offload as RANGE.
    options = {
        "owner_table": schema + "." + table_name,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        less_than_option: hv_1,
        "offload_partition_functions": udf,
        "reset_backend_table": True,
        "execute": False,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=offload_constants.IPA_PREDICATE_TYPE_EXCEPTION_TEXT,
    )

    # Non-Execute LIST_AS_RANGE offload of 1st partition.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_1,
        "offload_partition_functions": udf,
        "reset_backend_table": True,
        "execute": False,
    }
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # LIST_AS_RANGE offload 1st partition.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_2,
        "offload_partition_functions": udf,
        "offload_partition_lower_value": test_constants.LOWER_YRMON_NUM,
        "offload_partition_upper_value": test_constants.UPPER_YRMON_NUM,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
    )
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_1,
        incremental_key="YRMON",
        ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        incremental_key_type=part_key_type,
        backend_table=backend_name,
        partition_functions=udf,
        synthetic_partition_column_name=gl_part_column_check_name,
    )

    # 2nd partition happens to be empty but we should still move HWMs etc.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_3,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
    )
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_2,
        incremental_key="YRMON",
        ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        synthetic_partition_column_name=gl_part_column_check_name,
    )

    # Attempt to top up LIST_AS_RANGE by LPA - expect exception.
    options = {
        "owner_table": schema + "." + table_name,
        "equal_to_values": hv_4,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=offload_constants.IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
    )

    # LIST_AS_RANGE offload with multiple partition names - expect exception.
    options = {
        "owner_table": schema + "." + table_name,
        "partition_names_csv": test_constants.SALES_BASED_LIST_PNAME_4
        + ","
        + test_constants.SALES_BASED_LIST_PNAME_5,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT,
    )

    # Offload 3rd partition identified by partition name.
    options = {
        "owner_table": schema + "." + table_name,
        "partition_names_csv": test_constants.SALES_BASED_LIST_PNAME_4,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
    )
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_3,
        incremental_key="YRMON",
        ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        incremental_key_type=part_key_type,
        partition_functions=udf,
    )

    # No-op re-offload of 3rd partition.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_4,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # Setup - drop oldest partition from fact.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_drop_sales_based_fact_partition_ddls(
            schema,
            table_name,
            [hv_1],
            frontend_api,
        ),
    )

    assert not sales_based_fact_partition_exists(
        schema, table_name, [hv_1], frontend_api
    )

    # Offloads next partition from fact table after the oldest partition was dropped. The verification step should still succeed.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_5,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
    )
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_4,
        incremental_key="YRMON",
        ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        incremental_key_type=part_key_type,
        partition_functions=udf,
    )


def test_offload_list_rpa_num(config, schema, data_db):
    id = "test_offload_list_rpa_num"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LPA_FACT_TABLE_NUM,
                part_key_type=ORACLE_TYPE_NUMBER,
                default_partition=True,
                out_of_sequence=True,
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_FACT_TABLE_NUM
            ),
        )

        offload_list_as_range_ipa_standard_story_tests(
            schema,
            data_db,
            LPA_FACT_TABLE_NUM,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            ORACLE_TYPE_NUMBER,
        )


def test_offload_list_rpa_date(config, schema, data_db):
    id = "test_offload_list_rpa_date"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LPA_FACT_TABLE_DATE,
                part_key_type=ORACLE_TYPE_DATE,
                default_partition=True,
                out_of_sequence=True,
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_FACT_TABLE_DATE
            ),
        )

        offload_list_as_range_ipa_standard_story_tests(
            schema,
            data_db,
            LPA_FACT_TABLE_DATE,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            ORACLE_TYPE_DATE,
        )


def test_offload_list_rpa_timestamp(config, schema, data_db):
    id = "test_offload_list_rpa_timestamp"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LPA_FACT_TABLE_TS,
                part_key_type=ORACLE_TYPE_TIMESTAMP,
                default_partition=True,
                out_of_sequence=True,
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_FACT_TABLE_TS
            ),
        )

        offload_list_as_range_ipa_standard_story_tests(
            schema,
            data_db,
            LPA_FACT_TABLE_TS,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            ORACLE_TYPE_TIMESTAMP,
        )


def test_offload_list_rpa_varchar(config, schema, data_db):
    id = "test_offload_list_rpa_varchar"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LPA_FACT_TABLE_STR,
                part_key_type=ORACLE_TYPE_VARCHAR2,
                default_partition=True,
                out_of_sequence=True,
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_FACT_TABLE_STR
            ),
        )

        offload_list_as_range_ipa_standard_story_tests(
            schema,
            data_db,
            LPA_FACT_TABLE_STR,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            ORACLE_TYPE_VARCHAR2,
        )


def test_offload_list_rpa_num_udf(config, schema, data_db):
    id = "test_offload_list_rpa_num_udf"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LPA_FACT_TABLE_NUM_UDF,
                part_key_type=ORACLE_TYPE_NUMBER,
                default_partition=True,
                out_of_sequence=True,
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_FACT_TABLE_NUM_UDF
            ),
        )

        offload_list_as_range_ipa_standard_story_tests(
            schema,
            data_db,
            LPA_FACT_TABLE_NUM_UDF,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            ORACLE_TYPE_NUMBER,
            partition_function=test_constants.PARTITION_FUNCTION_TEST_FROM_INT8,
        )


def test_offload_list_rpa_subpart(config, schema, data_db):
    id = "test_offload_list_rpa_subpart"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_subpartitioned_fact_ddl(
                schema, LIST_RANGE_TABLE
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LIST_RANGE_TABLE
            ),
        )

        # Offloads from a LIST/RANGE subpartitioned fact table with LIST_AS_RANGE proving we can use the IPA options on the top level.
        options = {
            "owner_table": schema + "." + LIST_RANGE_TABLE,
            "reset_backend_table": True,
            "less_than_value": "4",
            "offload_partition_lower_value": 0,
            "offload_partition_upper_value": 10,
            "create_backend_db": True,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
        )
        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            LIST_RANGE_TABLE,
            "3",
            incremental_key="CHANNEL_ID",
            ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
            incremental_key_type=ORACLE_TYPE_NUMBER,
            incremental_range="PARTITION",
        )
