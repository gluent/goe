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
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_metadata_functions import OFFLOAD_TYPE_FULL
from goe.offload.offload_source_data import NO_MAXVALUE_PARTITION_NOTICE_TEXT
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    hint_text_in_log,
    sales_based_fact_assertion,
    text_in_messages,
)
from tests.integration.scenarios import scenario_constants
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
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


PARALLEL_V_DIM = "STORY_PARALLEL_VER_DIM"
MAXVAL_FACT = "STORY_MAXVALUE_FACT"


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


def test_offload_misc_verification_parallel(config, schema, data_db):
    id = "test_offload_misc_verification_parallel"

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        pytest.skip(f"Skipping {id} for system: {config.db_type}")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
                schema, PARALLEL_V_DIM
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, PARALLEL_V_DIM
                ),
            ],
        )

        # Offload with count verification parallelism=3.
        # Disables data sampling to minimize risk of other hints being matched.
        options = {
            "owner_table": schema + "." + PARALLEL_V_DIM,
            "verify_parallelism": 3,
            "data_sample_pct": 0,
            "reset_backend_table": True,
            "execute": False,
        }
        offload_messages = run_offload(options, config, messages)
        assert hint_text_in_log(offload_messages, config, 3)

        # Offload with verification parallelism=1.
        options = {
            "owner_table": schema + "." + PARALLEL_V_DIM,
            "verify_parallelism": 1,
            "data_sample_pct": 0,
            "reset_backend_table": True,
            "execute": False,
        }
        offload_messages = run_offload(options, config, messages)
        assert hint_text_in_log(offload_messages, config, 1)

        # Offload with verification parallelism=0.
        options = {
            "owner_table": schema + "." + PARALLEL_V_DIM,
            "verify_parallelism": 0,
            "data_sample_pct": 0,
            "reset_backend_table": True,
            "execute": False,
        }
        offload_messages = run_offload(options, config, messages)
        assert hint_text_in_log(offload_messages, config, 0)

        # Offload with aggregation verification parallelism=4.
        options = {
            "owner_table": schema + "." + PARALLEL_V_DIM,
            "verify_parallelism": 4,
            "verify_row_count": "aggregate",
            "data_sample_pct": 0,
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)
        assert hint_text_in_log(offload_messages, config, 4)


def test_offload_misc_maxvalue_partition(config, schema, data_db):
    id = "test_offload_misc_maxvalue_partition"

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        pytest.skip(f"Skipping {id} for system: {config.db_type}")

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
            frontend_sqls=frontend_api.sales_based_fact_create_ddl(
                schema, MAXVAL_FACT, maxval_partition=True
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, MAXVAL_FACT
                ),
            ],
        )

        # 90/10 Offload of Fact Ready to Convert.
        # Offloads first partitions from a fact table ready for subsequent tests.
        options = {
            "owner_table": schema + "." + MAXVAL_FACT,
            "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)
        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            MAXVAL_FACT,
            test_constants.SALES_BASED_FACT_HV_2,
            offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
            offload_messages=offload_messages,
        )

        # 90/10 Offload of Fact with MAXVALUE Partition.
        # Offloads all partitions from a MAXVALUE fact table but in 90/10, the MAXVALUE partition should be skipped.
        options = {
            "owner_table": schema + "." + MAXVAL_FACT,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)
        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            MAXVAL_FACT,
            test_constants.SALES_BASED_FACT_HV_6,
            offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
            offload_messages=offload_messages,
        )
        assert text_in_messages(
            offload_messages, NO_MAXVALUE_PARTITION_NOTICE_TEXT, messages
        )

        # Offload 90/10 fact to 100/0.
        # Offloads all partitions from a fact table including MAXVALUE partition.
        options = {
            "owner_table": schema + "." + MAXVAL_FACT,
            "offload_type": OFFLOAD_TYPE_FULL,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)
        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            MAXVAL_FACT,
            None,
            offload_pattern=scenario_constants.OFFLOAD_PATTERN_100_0,
            check_backend_rowcount=True,
            offload_messages=offload_messages,
        )
