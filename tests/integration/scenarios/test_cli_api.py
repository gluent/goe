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

import os

import pytest

from goe.config import orchestration_defaults
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)

from tests.integration.scenarios.assertion_functions import (
    backend_table_exists,
)
from tests.integration.scenarios.scenario_runner import (
    run_setup,
    run_shell_cmd,
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
    get_frontend_testing_api,
    get_test_messages,
)


CLI_DIM = "STORY_CLI_DIM"
CLI_FACT = "STORY_CLI_FACT"


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


def get_bin_path():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return os.path.join(offload_home, "bin")


def get_log_path():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return os.path.join(offload_home, "log")


def get_run_path():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return os.path.join(offload_home, "run")


def command_supports_no_version_check(list_of_args):
    return bool(
        list_of_args[0].endswith("connect") or list_of_args[0].endswith("offload")
    )


def goe_shell_command(list_of_args):
    """Return list_of_args supplemented with other args if relevant"""
    suffix_args = []

    if command_supports_no_version_check(list_of_args):
        # No TeamCity so not worried about version match
        suffix_args.append("--no-version-check")

    return list_of_args + suffix_args


def test_cli_connect(config):
    id = "test_cli_connect"
    messages = get_test_messages(config, id)
    bin_path = get_bin_path()

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, "connect"), "-h"])
    )

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, "connect")])
    )


def test_cli_offload_opts(config):
    id = "test_cli_offload_opts"
    messages = get_test_messages(config, id)
    bin_path = get_bin_path()

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, "offload"), "-h"])
    )

    run_shell_cmd(
        config,
        messages,
        goe_shell_command([os.path.join(bin_path, "offload"), "--version"]),
    )


def test_cli_offload_full(config, schema, data_db):
    id = "test_cli_offload_full"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages)
    bin_path = get_bin_path()

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, CLI_DIM),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, CLI_DIM
        ),
    )

    assert not backend_table_exists(config, backend_api, messages, data_db, CLI_DIM)

    # Non-execute mode
    run_shell_cmd(
        config,
        messages,
        goe_shell_command(
            [
                os.path.join(bin_path, "offload"),
                "-t",
                schema + "." + CLI_DIM,
                "--reset-backend-table",
            ]
        ),
    )

    assert not backend_table_exists(
        config, backend_api, messages, data_db, CLI_DIM
    ), "The backend table should NOT exist"

    # Execute mode
    run_shell_cmd(
        config,
        messages,
        goe_shell_command(
            [
                os.path.join(bin_path, "offload"),
                "-t",
                schema + "." + CLI_DIM,
                "-x",
                "--reset-backend-table",
                "--create-backend-db",
            ]
        ),
    )

    assert backend_table_exists(
        config, backend_api, messages, data_db, CLI_DIM
    ), "The backend table should exist"

    # Execute mode with many options
    run_shell_cmd(
        config,
        messages,
        goe_shell_command(
            [
                os.path.join(bin_path, "offload"),
                "-t",
                schema + "." + CLI_DIM,
                "-x",
                "--force",
                "--reset-backend-table",
                "--reset-backend-table",
                "--skip-steps=xyz",
                "--allow-decimal-scale-rounding",
                "--allow-floating-point-conversions",
                "--allow-nanosecond-timestamp-columns",
                "--compress-load-table",
                "--data-sample-parallelism=2",
                "--max-offload-chunk-count=4",
                "--offload-fs-scheme={}".format(
                    orchestration_defaults.offload_fs_scheme_default()
                ),
                "--no-verify",
            ]
        ),
    )

    assert backend_table_exists(
        config, backend_api, messages, data_db, CLI_DIM
    ), "The backend table should exist"


def test_cli_offload_rpa(config, schema, data_db):
    id = "test_cli_offload_rpa"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages)
    bin_path = get_bin_path()

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, CLI_FACT, simple_partition_names=True
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, CLI_FACT
        ),
    )

    assert not backend_table_exists(
        config, backend_api, messages, data_db, CLI_FACT
    ), "The backend table should NOT exist"

    run_shell_cmd(
        config,
        messages,
        goe_shell_command(
            [
                os.path.join(bin_path, "offload"),
                "-t",
                schema + "." + CLI_FACT,
                "-x",
                f"--older-than-date={test_constants.SALES_BASED_FACT_HV_2}",
                "--reset-backend-table",
                "--create-backend-db",
            ]
        ),
    )

    assert backend_table_exists(
        config, backend_api, messages, data_db, CLI_FACT
    ), "The backend table should exist"

    run_shell_cmd(
        config,
        messages,
        goe_shell_command(
            [
                os.path.join(bin_path, "offload"),
                "-t",
                schema + "." + CLI_FACT,
                "-x",
                "-v",
                f"--older-than-date={test_constants.SALES_BASED_FACT_HV_3}",
                "--reset-backend-table",
            ]
        ),
    )

    assert backend_table_exists(
        config, backend_api, messages, data_db, CLI_FACT
    ), "The backend table should exist"
