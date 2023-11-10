import inspect
import os
import time
import traceback

from gluentlib.offload.offload_constants import DBTYPE_MSSQL, DBTYPE_TERADATA
from gluentlib.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from gluentlib.orchestration import orchestration_constants
from gluentlib.orchestration.execution_id import ExecutionId
from gluentlib.orchestration.orchestration_runner import OrchestrationRunner
from tests.integration.test_functions import (
    get_default_test_user,
    get_default_test_user_pass,
)
from tests.testlib.test_framework.backend_testing_api import subproc_cmd
from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages


class ScenarioRunnerException(Exception):
    pass


def get_config_overrides(config_dict: dict, orchestration_config):
    """Return config from story enhanced with certain attributes from orchestration_config"""
    base_config = {
        "execute": True,
        "verbose": orchestration_config.verbose,
        "vverbose": orchestration_config.vverbose,
    }
    if config_dict:
        base_config.update(config_dict)
    return base_config


def get_conf_path():
    offload_home = os.environ.get('OFFLOAD_HOME')
    assert offload_home, 'OFFLOAD_HOME must be set in order to run tests'
    return os.path.join(offload_home, 'conf')


def run_offload(
    option_dict: dict,
    orchestration_config,
    parent_messages: OffloadMessages,
    expected_status=True,
    expected_exception_string: str = None,
    config_overrides: dict = None,
):
    execution_id = ExecutionId()
    messages = OffloadMessages.from_options(
        orchestration_config,
        log_fh=parent_messages.get_log_fh(),
        execution_id=execution_id,
        command_type=orchestration_constants.COMMAND_OFFLOAD,
    )
    try:
        config_overrides = get_config_overrides(config_overrides, orchestration_config)
        status = OrchestrationRunner(config_overrides=config_overrides).offload(
            option_dict,
            execution_id=execution_id,
            reuse_log=True,
            messages_override=messages,
        )
        if expected_status is not None and status != expected_status:
            raise ScenarioRunnerException(
                "Tested offload() return != %s" % expected_status
            )
        if expected_exception_string:
            # We shouldn't get here if we're expecting an exception
            messages.log("Missing exception containing: %s" % expected_exception_string)
            # Can't include exception in error below otherwise we'll end up with a pass
            raise ScenarioRunnerException("offload() did not throw expected exception")
    except Exception as exc:
        if (
            expected_exception_string
            and expected_exception_string.lower() in str(exc).lower()
        ):
            messages.log(
                "Test caught expected exception:%s\n%s" % (type(exc), str(exc))
            )
            messages.log(
                "Ignoring exception containing: %s" % expected_exception_string
            )
        else:
            messages.log(traceback.format_exc())
            raise


def run_setup(
    frontend_api, backend_api, config, test_messages: OffloadTestMessages, frontend_sqls=None, python_fns=None
):
    try:
        if frontend_sqls:
            test_schema = get_default_test_user()
            test_schema_pass = get_default_test_user_pass()
            with frontend_api.create_new_connection_ctx(
                test_schema,
                test_schema_pass,
                trace_action_override="FrontendTestingApi(StorySetup)",
            ) as sh_test_api:
                if config.db_type == DBTYPE_MSSQL:
                    sh_test_api.execute_ddl("BEGIN TRAN")
                for sql in frontend_sqls:
                    try:
                        sh_test_api.execute_ddl(sql)
                    except Exception as exc:
                        if "does not exist" in str(exc) and sql.upper().startswith(
                            "DROP"
                        ):
                            test_messages.log("Ignoring: " + str(exc), detail=VVERBOSE)
                        else:
                            test_messages.log(str(exc))
                            raise
                if config.db_type != DBTYPE_TERADATA:
                    # We have autocommit enabled on Teradata:
                    #   COMMIT WORK not allowed for a DBC/SQL session. (-3706)
                    sh_test_api.execute_ddl("COMMIT")

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


def run_shell_cmd(
    orchestration_config,
    messages: OffloadTestMessages,
    shell_command: list,
    cwd: str = None,
    acceptable_return_codes: list = None,
    expected_exception_string: str = None,
):
    try:
        messages.log('Running subproc_cmd: %s' % shell_command, detail=VERBOSE)
        conf_dir = get_conf_path()
        conf_file = os.path.join(conf_dir, 'offload.env')
        cmd = ['.', conf_file, '&&'] + shell_command
        print("cmd", cmd)
        returncode, output = subproc_cmd(cmd, orchestration_config, messages, cwd=cwd)
        messages.log('subproc_cmd return code: %s' % returncode, detail=VVERBOSE)
        messages.log('subproc_cmd output: %s' % output, detail=VVERBOSE)
        acceptable_return_codes = acceptable_return_codes or [0]
        if returncode not in acceptable_return_codes:
            raise ScenarioRunnerException('Tested shell_command return %s not in %s: %s'
                                       % (returncode, acceptable_return_codes, shell_command[0]))
    except Exception as exc:
        if expected_exception_string and expected_exception_string.lower() in str(exc).lower():
            messages.log('Test caught expected exception:\n%s' % str(exc))
            messages.log('Ignoring exception containing "%s"' % expected_exception_string)
        else:
            messages.log(traceback.format_exc())
            raise
