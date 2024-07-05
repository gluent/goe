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

import inspect
import time
import traceback
from typing import TYPE_CHECKING

from goe.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from goe.orchestration import orchestration_constants
from goe.orchestration.execution_id import ExecutionId
from goe.orchestration.orchestration_runner import OrchestrationRunner
from goe.util.misc_functions import get_temp_path
from tests.integration.test_functions import (
    run_setup_ddl,
)
from tests.testlib.test_framework.backend_testing_api import subproc_cmd
from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages

if TYPE_CHECKING:
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from testlib.test_framework.backend_testing_api import BackendTestingApiInterface
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface


class ScenarioRunnerException(Exception):
    pass


def get_config_overrides(
    config_dict: dict, orchestration_config: "OrchestrationRepoClientInterface"
):
    """Return config from story enhanced with certain attributes from orchestration_config"""
    base_config = {
        "execute": True,
        "verbose": orchestration_config.verbose,
        "vverbose": orchestration_config.vverbose,
    }
    if config_dict:
        base_config.update(config_dict)
    return base_config


def run_offload(
    option_dict: dict,
    orchestration_config: "OrchestrationRepoClientInterface",
    parent_messages: OffloadMessages,
    expected_status=True,
    expected_exception_string: str = None,
    config_overrides: dict = None,
    no_messages_override: bool = False,
) -> OffloadMessages:
    execution_id = ExecutionId()
    messages = OffloadMessages.from_options(
        orchestration_config,
        log_fh=parent_messages.get_log_fh(),
        execution_id=execution_id,
        command_type=orchestration_constants.COMMAND_OFFLOAD,
    )
    try:
        config_overrides = get_config_overrides(config_overrides, orchestration_config)
        messages_override = None if no_messages_override else messages
        status = OrchestrationRunner(config_overrides=config_overrides).offload(
            option_dict,
            execution_id=execution_id,
            reuse_log=(not no_messages_override),
            messages_override=messages_override,
        )
        if expected_status is not None and status != expected_status:
            raise ScenarioRunnerException(
                "Tested offload() return != %s" % expected_status
            )
        if expected_exception_string:
            # We shouldn't get here if we're expecting an exception
            parent_messages.log(
                "Missing exception containing: %s" % expected_exception_string
            )
            # Can't include exception in error below otherwise we'll end up with a pass
            raise ScenarioRunnerException("offload() did not throw expected exception")
    except Exception as exc:
        if (
            expected_exception_string
            and expected_exception_string.lower() in str(exc).lower()
        ):
            parent_messages.log(
                "Test caught expected exception:%s\n%s" % (type(exc), str(exc))
            )
            parent_messages.log(
                "Ignoring exception containing: %s" % expected_exception_string
            )
        else:
            parent_messages.log(traceback.format_exc())
            raise
    return messages


def run_setup(
    frontend_api: "FrontendTestingApiInterface",
    backend_api: "BackendTestingApiInterface",
    config: "OrchestrationRepoClientInterface",
    test_messages: OffloadTestMessages,
    frontend_sqls=None,
    python_fns=None,
):
    try:
        if frontend_sqls:
            assert frontend_api
            run_setup_ddl(
                config,
                frontend_api,
                test_messages,
                frontend_sqls,
                trace_action="scenarios.run_setup()",
            )

        if backend_api.test_setup_seconds_delay():
            time.sleep(backend_api.test_setup_seconds_delay())

        if python_fns:
            if not isinstance(python_fns, list):
                python_fns = [
                    python_fns,
                ]
            for fn in python_fns:
                if not inspect.isfunction(fn):
                    raise ScenarioRunnerException(
                        "Row in python_fns is not a function: %s %s"
                        % (type(fn), str(fn))
                    )
                try:
                    fn()
                except Exception as exc:
                    if " exist" in str(exc):
                        test_messages.log("Ignoring: " + str(exc), detail=VVERBOSE)
                    else:
                        raise
    except Exception:
        test_messages.log(traceback.format_exc())
        raise


def create_goe_shell_runner(
    orchestration_config: "OrchestrationRepoClientInterface",
    messages: OffloadTestMessages,
    shell_command: list,
    cwd: str = None,
) -> str:
    """Creates a temporary shell script to run a GOE command and returns the name of the script."""
    tmp_file = get_temp_path(suffix=".sh")
    with open(tmp_file, "w") as f:
        f.write("#!/bin/bash\n")
        if cwd:
            f.write(f"cd {cwd}\n")
        f.write(f"{' '.join(_ for _ in shell_command)}\n")
    cmd = ["chmod", "700", tmp_file]
    returncode, _ = subproc_cmd(cmd, orchestration_config, messages)
    if returncode != 0:
        raise ScenarioRunnerException(f"Failed chmod command: {cmd}")
    return tmp_file


def run_shell_cmd(
    orchestration_config: "OrchestrationRepoClientInterface",
    messages: OffloadTestMessages,
    shell_command: list,
    cwd: str = None,
    acceptable_return_codes: list = None,
    expected_exception_string: str = None,
):
    messages.log("Running subproc_cmd: %s" % shell_command, detail=VERBOSE)
    tmp_file = create_goe_shell_runner(
        orchestration_config, messages, shell_command, cwd=cwd
    )
    try:
        returncode, output = subproc_cmd([tmp_file], orchestration_config, messages)
        messages.log("subproc_cmd return code: %s" % returncode, detail=VVERBOSE)
        messages.log("subproc_cmd output: %s" % output, detail=VVERBOSE)
        acceptable_return_codes = acceptable_return_codes or [0]
        if returncode not in acceptable_return_codes:
            raise ScenarioRunnerException(
                "Tested shell_command return %s not in %s: %s"
                % (returncode, acceptable_return_codes, shell_command[0])
            )
    except Exception as exc:
        if (
            expected_exception_string
            and expected_exception_string.lower() in str(exc).lower()
        ):
            messages.log("Test caught expected exception:\n%s" % str(exc))
            messages.log(
                'Ignoring exception containing "%s"' % expected_exception_string
            )
        else:
            messages.log(traceback.format_exc())
            raise
    finally:
        try:
            pass  # os.remove(tmp_file)
        except Exception:
            pass
