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

from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import step_title_to_step_id, FORCED_EXCEPTION_TEXT
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title

from tests.integration.scenarios.assertion_functions import (
    messages_step_executions,
)
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
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api_ctx,
    get_test_messages_ctx,
)


STEP_DIM = "STORY_STEP_DIM"


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


def test_offload_step_dim(config, schema, data_db):
    id = "test_offload_step_dim"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        backend_name = convert_backend_identifier_case(config, STEP_DIM)

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
                schema, STEP_DIM
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, STEP_DIM
                ),
            ],
        )

        # Offload skipping step STEP_VALIDATE_DATA.
        options = {
            "owner_table": schema + "." + STEP_DIM,
            "skip": [
                step_title_to_step_id(step_title(command_steps.STEP_VALIDATE_DATA))
            ],
            "reset_backend_table": True,
            "execute": False,
        }
        offload_messages = run_offload(options, config, messages)
        assert (
            messages_step_executions(
                offload_messages, step_title(command_steps.STEP_VALIDATE_DATA), messages
            )
            == 0
        )

        # Offload skipping step STEP_VALIDATE_CASTS.
        options = {
            "owner_table": schema + "." + STEP_DIM,
            "skip": [
                step_title_to_step_id(step_title(command_steps.STEP_VALIDATE_CASTS))
            ],
            "reset_backend_table": True,
            "execute": False,
        }
        offload_messages = run_offload(options, config, messages)
        assert (
            messages_step_executions(
                offload_messages,
                step_title(command_steps.STEP_VALIDATE_CASTS),
                messages,
            )
            == 0
        )

        # Offload skipping step STEP_VERIFY_EXPORTED_DATA.
        options = {
            "owner_table": schema + "." + STEP_DIM,
            "skip": [
                step_title_to_step_id(
                    step_title(command_steps.STEP_VERIFY_EXPORTED_DATA)
                )
            ],
            "reset_backend_table": True,
            "execute": False,
        }
        offload_messages = run_offload(options, config, messages)
        assert (
            messages_step_executions(
                offload_messages,
                step_title(command_steps.STEP_VERIFY_EXPORTED_DATA),
                messages,
            )
            == 0
        )

        # Offload aborting before step STEP_CREATE_TABLE
        options = {
            "owner_table": schema + "." + STEP_DIM,
            "error_before_step": step_title(command_steps.STEP_CREATE_TABLE),
            "reset_backend_table": True,
            "execute": False,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=FORCED_EXCEPTION_TEXT,
        )

        # Offload aborting before step STEP_FINAL_LOAD
        options = {
            "owner_table": schema + "." + STEP_DIM,
            "error_before_step": step_title(command_steps.STEP_FINAL_LOAD),
            "reset_backend_table": True,
            "execute": True,
        }
        run_offload(
            options, config, messages, expected_exception_string=FORCED_EXCEPTION_TEXT
        )

        # Table exists
        assert backend_api.table_exists(data_db, backend_name)
        # Table is empty
        assert backend_api.get_table_row_count(data_db, backend_name) == 0
