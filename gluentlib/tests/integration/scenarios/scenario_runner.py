import inspect
import time
import traceback

from gluent import vverbose
from gluentlib.offload.offload_constants import DBTYPE_MSSQL, DBTYPE_TERADATA
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.orchestration import orchestration_constants
from gluentlib.orchestration.execution_id import ExecutionId
from gluentlib.orchestration.orchestration_runner import OrchestrationRunner
from tests.integration.test_functions import (
    get_default_test_user,
    get_default_test_user_pass,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_orchestration_options_object,
    log,
)


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
    frontend_api, backend_api, config_options, frontend_sqls=None, python_fns=None
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
                if config_options.db_type == DBTYPE_MSSQL:
                    sh_test_api.execute_ddl("BEGIN TRAN")
                for sql in frontend_sqls:
                    try:
                        sh_test_api.execute_ddl(sql)
                    except Exception as exc:
                        if "does not exist" in str(exc) and sql.upper().startswith(
                            "DROP"
                        ):
                            log("Ignoring: " + str(exc), vverbose)
                        else:
                            log(str(exc))
                            raise
                if config_options.db_type != DBTYPE_TERADATA:
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
                        log("Ignoring: " + str(exc), vverbose)
                    else:
                        raise
    except Exception:
        log(traceback.format_exc())
        raise
