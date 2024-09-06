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

import os

import pytest

from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.orchestration import command_steps
from goe.util.misc_functions import get_temp_path

from tests.integration.scenarios.assertion_functions import (
    backend_table_exists,
    text_in_messages,
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


TEST_TABLE_LFS_1 = "DDL_FILE_DIM_LFS_1"
TEST_TABLE_LFS_2 = "DDL_FILE_DIM_LFS_2"
TEST_TABLE_CS_1 = "DDL_FILE_DIM_CS_1"
TEST_TABLE_CS_2 = "DDL_FILE_DIM_CS_2"


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


def step_assertions(offload_messages):
    """Check that we didn't run Offload steps that come after the DDL file is produced."""
    assert (
        command_steps.step_title(command_steps.STEP_DDL_FILE) in offload_messages.steps
    )
    # After creating the DDL file Offload should stop, therefore
    # we should never see data staged or loaded.
    assert (
        command_steps.step_title(command_steps.STEP_CREATE_TABLE)
        not in offload_messages.steps
    ), f"We ran an offload step that shouldn't be run: {command_steps.step_title(command_steps.STEP_CREATE_TABLE)}"
    assert (
        command_steps.step_title(command_steps.STEP_STAGING_TRANSPORT)
        not in offload_messages.steps
    ), f"We ran an offload step that shouldn't be run: {command_steps.step_title(command_steps.STEP_STAGING_TRANSPORT)}"
    assert (
        command_steps.step_title(command_steps.STEP_FINAL_LOAD)
        not in offload_messages.steps
    ), f"We ran an offload step that shouldn't be run: {command_steps.step_title(command_steps.STEP_FINAL_LOAD)}"


def new_table_ddl_file_tests(
    config,
    schema: str,
    data_db: str,
    test_table: str,
    ddl_file_prefix: str,
    backend_api,
    messages,
    dfs_client=None,
):
    # Offload in execute mode requesting a DDL file.
    ddl_file = ddl_file_prefix + "_1.sql"
    options = {
        "owner_table": schema + "." + test_table,
        "reset_backend_table": True,
        "ddl_file": ddl_file,
        "create_backend_db": True,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)
    # When using DDL file no table should be created, even in execute mode.
    assert not backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), f"Backend table for {schema}.{test_table} should not exist"
    assert text_in_messages(
        offload_messages, offload_constants.DDL_FILE_EXECUTE_MESSAGE_TEXT, messages
    )
    step_assertions(offload_messages)

    if dfs_client:
        assert dfs_client.stat(ddl_file), f"DDL file has not been created: {ddl_file}"
    else:
        assert os.path.isfile(ddl_file), f"DDL file has not been created: {ddl_file}"

    # Offload in non-execute mode asking for ddl_file.
    ddl_file = ddl_file_prefix + "_2.sql"
    options = {
        "owner_table": schema + "." + test_table,
        "reset_backend_table": True,
        "ddl_file": ddl_file,
        "execute": False,
    }
    run_offload(options, config, messages)
    assert not backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), f"Backend table for {schema}.{test_table} should not exist"
    step_assertions(offload_messages)
    # Even in non-execture mode we expect to see a DDL file.
    if dfs_client:
        assert dfs_client.stat(ddl_file), f"DDL file has not been created: {ddl_file}"
    else:
        assert os.path.isfile(ddl_file), f"DDL file has not been created: {ddl_file}"

    # Re-use same file name, should be rejected
    run_offload(options, config, messages, expected_exception_string=ddl_file)


def exsting_table_ddl_file_tests(
    config,
    schema: str,
    data_db: str,
    test_table: str,
    ddl_file_prefix: str,
    backend_api,
    messages,
):
    # First offload the table
    options = {
        "owner_table": schema + "." + test_table,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)

    # Now request a DDL file, in execute mode.
    ddl_file = ddl_file_prefix + "_1.sql"
    options = {
        "owner_table": schema + "." + test_table,
        "ddl_file": ddl_file,
        "reset_backend_table": True,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)
    assert backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), f"Backend table for {schema}.{test_table} should exist"
    assert text_in_messages(
        offload_messages, offload_constants.DDL_FILE_EXECUTE_MESSAGE_TEXT, messages
    )
    step_assertions(offload_messages)
    assert os.path.isfile(ddl_file), f"DDL file has not been created: {ddl_file}"

    # Request a DDL file, in non-execute mode.
    ddl_file = ddl_file_prefix + "_2.sql"
    options = {
        "owner_table": schema + "." + test_table,
        "ddl_file": ddl_file,
        "reset_backend_table": True,
        "execute": False,
    }
    offload_messages = run_offload(options, config, messages)
    assert backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), f"Backend table for {schema}.{test_table} should exist"
    step_assertions(offload_messages)
    assert os.path.isfile(ddl_file), f"DDL file has not been created: {ddl_file}"


def test_ddl_file_new_table_local_fs(config, schema, data_db):
    """Test requesting a DDL file to local FS for a new table."""
    id = "test_ddl_file_new_table_local_fs"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        test_table = TEST_TABLE_LFS_1

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
                schema, test_table
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
        )

        ddl_file_prefix = get_temp_path(prefix=id)
        new_table_ddl_file_tests(
            config, schema, data_db, test_table, ddl_file_prefix, backend_api, messages
        )


def test_ddl_file_existing_table_local_fs(config, schema, data_db):
    """Test requesting a DDL file to local FS for a previously offloaded table."""
    id = "test_ddl_file_existing_table_local_fs"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        test_table = TEST_TABLE_LFS_2

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
                schema, test_table
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
        )

        ddl_file_prefix = get_temp_path(prefix=id)
        exsting_table_ddl_file_tests(
            config, schema, data_db, test_table, ddl_file_prefix, backend_api, messages
        )


def test_ddl_file_new_table_cloud_storage(config, schema, data_db):
    """Test requesting a DDL file to cloud storage for a new table."""
    id = "test_ddl_file_new_table_cloud_storage"
    if not config.offload_fs_container:
        pytest.skip(f"Skipping {id} when OFFLOAD_FS_CONTAINER is empty")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        test_table = TEST_TABLE_CS_1

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
                schema, test_table
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
        )

        dfs_client = get_dfs_from_options(config, messages)
        bucket_path = dfs_client.gen_uri(
            config.offload_fs_scheme,
            config.offload_fs_container,
            config.offload_fs_prefix,
        )

        ddl_file_prefix = get_temp_path(tmp_dir=bucket_path, prefix=id)
        new_table_ddl_file_tests(
            config,
            schema,
            data_db,
            test_table,
            ddl_file_prefix,
            backend_api,
            messages,
            dfs_client=dfs_client,
        )
