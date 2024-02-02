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
from goe.offload.offload_metadata_functions import (
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
    INCREMENTAL_PREDICATE_TYPE_LIST,
)
from goe.offload.oracle.oracle_column import ORACLE_TYPE_NUMBER
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    sales_based_fact_assertion,
)
from tests.integration.scenarios import scenario_constants
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import drop_backend_test_table
from tests.integration.scenarios.test_offload_lpa import offload_lpa_fact_assertion
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


FACT_RANGE_RANGE = "STORY_SP_RR"
FACT_LIST_RANGE_R = "STORY_SP_LR_R"
FACT_LIST_RANGE_L = "STORY_SP_LR_L"
FACT_HASH_RANGE = "STORY_SP_HR"


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


def test_offload_subpart_lr_range(config, schema, data_db):
    id = "test_offload_subpart_lr_range"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
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
            schema,
            FACT_LIST_RANGE_R,
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, FACT_LIST_RANGE_R
        ),
    )

    # Initial Offload of Range Subpartitioned Fact.
    # Offloads some partitions from a range subpartitioned fact table.
    # offload_fact_assertions() will check that TIME_ID is the incremental key even though that is the subpartition key.
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_R,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "offload_by_subpartition": True,
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
        FACT_LIST_RANGE_R,
        test_constants.SALES_BASED_FACT_HV_1,
        incremental_range="SUBPARTITION",
    )

    # Incremental Offload of Subpartitioned Fact.
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_R,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
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
        FACT_LIST_RANGE_R,
        test_constants.SALES_BASED_FACT_HV_2,
        incremental_range="SUBPARTITION",
    )

    # This test chooses a HWM that is not common across all list partition values and therefore should throw an exception.
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_R,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_3,
    }
    run_offload(options, config, messages, expected_exception_string="common boundary")

    # This test chooses the final HWM in the table and therefore should throw an exception.
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_R,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_6,
    }
    run_offload(
        options, config, messages, expected_exception_string="--offload-type=FULL"
    )

    # Offload type FULL of subpartitioned fact.
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_R,
        "offload_type": OFFLOAD_TYPE_FULL,
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
        FACT_LIST_RANGE_R,
        None,
        incremental_range="SUBPARTITION",
        offload_pattern=scenario_constants.OFFLOAD_PATTERN_100_0,
    )

    # Offload Type FULL->INCREMENTAL of Subpartitioned Fact (Expect to Fail).
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_R,
        "offload_type": OFFLOAD_TYPE_INCREMENTAL,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=offload_constants.OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_subpart_lr_list(config, schema, data_db):
    id = "test_offload_subpart_lr_list"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
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
            schema,
            FACT_LIST_RANGE_L,
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, FACT_LIST_RANGE_L
        ),
    )

    # Initial LPA Offload of Same List/Range Fact.
    # Confirms that an Offload of FACT_LIST_RANGE using LPA options gives us a top level LIST offload and not subpartition RANGE.
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_L,
        "equal_to_values": ["2"],
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 10,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)

    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        FACT_LIST_RANGE_L,
        config,
        backend_api,
        messages,
        repo_client,
        ["2"],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    #
    options = {
        "owner_table": schema + "." + FACT_LIST_RANGE_L,
        "equal_to_values": ["3"],
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 10,
    }
    run_offload(options, config, messages)

    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        FACT_LIST_RANGE_L,
        config,
        backend_api,
        messages,
        repo_client,
        ["2", "3"],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_subpart_range_range(config, schema, data_db):
    id = "test_offload_subpart_range_range"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
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
            schema,
            FACT_RANGE_RANGE,
            top_level="RANGE",
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, FACT_RANGE_RANGE
        ),
    )

    # Offload a RANGE/RANGE NUMBER/DATE table, will offload at top level RANGE.
    options = {
        "owner_table": schema + "." + FACT_RANGE_RANGE,
        "less_than_value": 3,
        "offload_partition_granularity": "1",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 10,
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
        FACT_RANGE_RANGE,
        "3",
        incremental_range="PARTITION",
        incremental_key="CHANNEL_ID",
        incremental_key_type=ORACLE_TYPE_NUMBER,
    )

    # Offload a RANGE/RANGE NUMBER/DATE table at subpartition level RANGE.
    options = {
        "owner_table": schema + "." + FACT_RANGE_RANGE,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "offload_by_subpartition": True,
        "reset_backend_table": True,
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
        FACT_RANGE_RANGE,
        test_constants.SALES_BASED_FACT_HV_1,
        incremental_range="SUBPARTITION",
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_subpart_hash_range(config, schema, data_db):
    id = "test_offload_subpart_hash_range"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
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
            schema,
            FACT_HASH_RANGE,
            top_level="HASH",
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, FACT_HASH_RANGE
        ),
    )

    # Offloads from a HASH/RANGE subpartitioned fact table.
    # Expect subpartition offload purely because we used older_than_date.
    options = {
        "owner_table": schema + "." + FACT_HASH_RANGE,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
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
        FACT_HASH_RANGE,
        test_constants.SALES_BASED_FACT_HV_1,
        incremental_range="SUBPARTITION",
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
