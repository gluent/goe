# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OrchestrationRunner: Library providing simple entry point for orchestration commands.
                     Utilized by both CLI and Orchestration Listener.
"""

# Standard Library
import logging
import sys
import traceback
from typing import Optional, TYPE_CHECKING

# GOE
from goe.goe import (
    OffloadOperation,
    get_log_fh,
    get_offload_target_table,
    init,
    init_redis_execution_id,
    init_log,
    offload_table,
)
from goe.config import orchestration_defaults
from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_messages import (
    OffloadMessages,
    NORMAL,
    VVERBOSE,
)
from goe.orchestration import command_steps, orchestration_constants
from goe.orchestration.execution_id import ExecutionId
from goe.orchestration.orchestration_lock import orchestration_lock_for_table
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

if TYPE_CHECKING:
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


class OrchestrationRunnerException(Exception):
    pass


###########################################################################
# CONSTANTS
###########################################################################

COMMAND_ID_CONNECT = "CONNECT"

# TODO NJ@2022-07-08 Shall we start giving commands better log file names?
LOG_FILE_PREFIXES = {
    COMMAND_ID_CONNECT: "connect",
    orchestration_constants.COMMAND_OFFLOAD: "offload",
    orchestration_constants.COMMAND_SCHEMA_SYNC: "schema_sync",
}


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


###########################################################################
# OrchestrationRunner
###########################################################################


class OrchestrationRunner:
    """OrchestrationRunner: Library providing simple entry point for orchestration commands."""

    def __init__(self, config_overrides=None, suppress_stdout=False):
        self._config = self._gen_config(
            config_overrides, suppress_stdout=suppress_stdout
        )
        # State refreshed by each command, not necessarily static.
        self._execution_id: Optional[ExecutionId] = None
        self._messages: Optional[OffloadMessages] = None
        self._max_hybrid_name_length: Optional[int] = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _build_offload_source_table(self, operation):
        return OffloadSourceTable.create(
            operation.owner,
            operation.table_name,
            self._config,
            self._messages,
            dry_run=(not operation.execute),
        )

    def _build_repo_client(
        self, messages, dry_run=False
    ) -> "OrchestrationRepoClientInterface":
        return orchestration_repo_client_factory(
            self._config,
            messages,
            dry_run=dry_run,
            trace_action="repo_client(OrchestrationRunner)",
        )

    def _cleanup_objects(self, repo_client, frontend_table=None, backend_table=None):
        try:
            if frontend_table:
                frontend_table.close()
            if backend_table:
                backend_table.close()
            repo_client.close()
        except Exception as exc:
            self._log_error(
                "Exception cleaning up connections: {}".format(str(exc)),
                detail=VVERBOSE,
            )

    def _command_begin(
        self,
        command_type: str,
        params,
        repo_client: "OrchestrationRepoClientInterface",
        operation=None,
    ) -> int:
        assert command_type
        assert params
        assert repo_client
        assert self._execution_id

        try:
            if isinstance(params, dict):
                command_input = params
            else:
                command_input = " ".join(sys.argv)

            operation_dict = (
                operation
                if isinstance(operation, (dict, type(None)))
                else operation.vars()
            )

            command_id = repo_client.start_command(
                self._execution_id,
                command_type,
                command_input,
                operation_dict,
            )
            self._log(
                f"Command id for execution {self._execution_id}: {command_id}",
                detail=VVERBOSE,
            )
        except Exception as exc:
            self._log_error(
                f"Exception starting command {self._execution_id}: {str(exc)}",
                detail=VVERBOSE,
            )
            self._log(
                "Exception stack: {}".format(traceback.format_exc()), detail=VVERBOSE
            )
            raise
        return command_id

    def _command_end(
        self, command_id: int, repo_client: "OrchestrationRepoClientInterface"
    ):
        assert repo_client
        try:
            repo_client.end_command(command_id, orchestration_constants.COMMAND_SUCCESS)
        except Exception as exc:
            self._log_error(
                f"Exception closing command {command_id}: {str(exc)}", detail=VVERBOSE
            )
            self._log(
                "Exception stack: {}".format(traceback.format_exc()), detail=VVERBOSE
            )
            raise

    def _command_fail(
        self,
        command_id: int,
        external_exc: Exception,
        repo_client: "OrchestrationRepoClientInterface",
    ):
        assert repo_client
        self._log_error(
            f"Failing command due to exception: {str(external_exc)}", detail=VVERBOSE
        )
        self._log(
            "External exception stack: {}".format(
                traceback.format_tb(external_exc.__traceback__)
            ),
            detail=VVERBOSE,
        )
        try:
            repo_client.end_command(command_id, orchestration_constants.COMMAND_ERROR)
        except Exception as local_exc:
            self._log_error(
                f"Exception failing command {command_id}: {str(local_exc)}",
                detail=VVERBOSE,
            )
            self._log(
                "Exception stack: {}".format(traceback.format_exc()), detail=VVERBOSE
            )
            raise

    def _execute_from_params(self, params) -> bool:
        if isinstance(params, dict):
            return params["execute"]
        else:
            return params.execute

    def _gen_config(
        self, config_overrides, suppress_stdout=False
    ) -> OrchestrationConfig:
        overrides = config_overrides or {}
        if suppress_stdout:
            overrides["suppress_stdout"] = suppress_stdout
        return OrchestrationConfig.from_dict(overrides)

    def _gen_messages(self, execution_id, command_type):
        return OffloadMessages.from_options(
            self._config,
            log_fh=get_log_fh(),
            execution_id=execution_id,
            cache_enabled=orchestration_defaults.cache_enabled(),
            command_type=command_type,
        )

    def _gen_offload_operation(
        self,
        params,
        repo_client: "OrchestrationRepoClientInterface",
    ):
        """Return an OffloadOperation object based on either a parameter dict or OptParse object."""
        try:
            max_hybrid_name_length = self._get_max_hybrid_identifier_length(
                dry_run=bool(not self._execute_from_params(params))
            )
            if isinstance(params, dict):
                # Non-CLI APIs are dict driven therefore we construct via "from_dict".
                # Also will be threaded and not-safe to pass in shared repo_client.
                op = OffloadOperation.from_dict(
                    params,
                    self._config,
                    self._messages,
                    repo_client=repo_client,
                    execution_id=self._execution_id,
                    max_hybrid_name_length=max_hybrid_name_length,
                )
            else:
                # CLI has an OptParse object therefore we construct via "from_options".
                # Also will not be threaded and safe to pass in shared repo_client.
                op = OffloadOperation.from_options(
                    params,
                    self._config,
                    self._messages,
                    repo_client=repo_client,
                    execution_id=self._execution_id,
                    max_hybrid_name_length=max_hybrid_name_length,
                )
            return op
        except Exception as exc:
            self._log_error(
                f"Exception generating offload operation {self._execution_id}: {str(exc)}",
                detail=VVERBOSE,
            )
            self._log(
                "Exception stack: {}".format(traceback.format_exc()), detail=VVERBOSE
            )
            raise

    def _get_execution_id(
        self, execution_id: Optional[ExecutionId] = None
    ) -> ExecutionId:
        """Return an ID to uniquely identify an orchestration command."""
        if execution_id is None:
            execution_id = ExecutionId()
        return execution_id

    def _get_max_hybrid_identifier_length(self, dry_run: bool) -> int:
        """Get the max supported hybrid identifier (table/view/column) length for the frontend RDBMS.
        This is not ideal because it is making a frontend connection just to get this information but at the point
        this is called we don't already have a connection we can use.
        By storing self._max_hybrid_name_length in state we should not need to make the connection a second time.
        """
        if self._max_hybrid_name_length is None:
            frontend_api = frontend_api_factory(
                self._config.db_type,
                self._config,
                self._messages,
                dry_run=dry_run,
                trace_action="_get_max_hybrid_identifier_length",
            )
            self._max_hybrid_name_length = frontend_api.max_table_name_length()
            frontend_api.close()
        return self._max_hybrid_name_length

    def _init_command(
        self,
        command: str,
        params,
        execution_id: Optional[ExecutionId] = None,
        reuse_log: bool = False,
        messages_override: Optional[OffloadMessages] = None,
    ) -> "OrchestrationRepoClientInterface":
        """
        Initialize an orchestration command.
        Sets execution_id, messages in state and returns repo_client.
        We do NOT store repo_client in state because of multiprocess issues with cx_Oracle.
        """
        self._init_command_log(command, params, reuse_log=reuse_log)
        self._execution_id = self._get_execution_id(execution_id=execution_id)
        self._messages = messages_override or self._gen_messages(
            self._execution_id, command
        )
        try:
            if messages_override and self._messages.execution_id is None:
                # We are testing and have no execution id.
                self._messages.set_execution_id(self._execution_id)
            self._log(
                "{} ExecutionId: {}".format(
                    "Using overridden" if execution_id else "Generated",
                    self._execution_id,
                ),
                detail=VVERBOSE,
            )
            init_redis_execution_id(self._execution_id)
            return self._build_repo_client(
                self._messages, dry_run=(not self._execute_from_params(params))
            )
        except Exception as exc:
            self._log_error(
                f"Exception initializing command {command}: {str(exc)}", detail=VVERBOSE
            )
            self._log(
                "Exception stack: {}".format(traceback.format_exc()), detail=VVERBOSE
            )
            raise

    def _init_command_log(self, command: str, params, reuse_log=False) -> None:
        """Create a log file for the orchestration command and set the global file handle in goe.py."""

        def get_owner_table_for_command():
            if command == COMMAND_ID_CONNECT:
                return None
            else:
                # We need to create the log before doing anything else, which means haven't rationalised the
                # contents of params yet. It could be a dict or a namespace.
                param_name = "owner_table"
                owner_table = (
                    params.get(param_name)
                    if isinstance(params, dict)
                    else getattr(params, param_name, None)
                )
                if not owner_table:
                    raise OrchestrationRunnerException(
                        f"Missing owner/table parameter for command: {command}"
                    )
                return owner_table

        # Install current config as global goe.py options.
        init(self._config)
        if not reuse_log:
            # Create log file for command and install it as global goe.py logging file handle.
            owner_table_token = get_owner_table_for_command()
            log_prefix = LOG_FILE_PREFIXES[command]
            if owner_table_token:
                init_log(f"{log_prefix}_{owner_table_token}")
            else:
                init_log(f"{log_prefix}")

    def _debug(self, msg):
        logger.debug(msg)
        if self._messages:
            self._messages.debug(msg)

    def _log(self, msg, detail=NORMAL, ansi_code=None):
        """Write to offload log file"""
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)
        if self._messages:
            self._messages.log(msg, detail=detail, ansi_code=ansi_code)

    def _log_error(self, msg, detail=NORMAL):
        logger.error(msg)
        self._messages.log(msg, detail=detail)

    def _log_final_messages(self, command_type, dry_run):
        self._messages.log_step_deltas()
        if self._messages.get_messages():
            self._messages.offload_step(
                command_steps.STEP_MESSAGES,
                self._messages.log_messages,
                command_type=command_type,
                execute=(not dry_run),
            )

    def _offload(self, operation, offload_source_table, offload_target_table):
        with orchestration_lock_for_table(
            offload_source_table.owner,
            offload_source_table.table_name,
            dry_run=(not operation.execute),
        ):
            try:
                return offload_table(
                    self._config,
                    operation,
                    offload_source_table,
                    offload_target_table,
                    self._messages,
                )
            except Exception as exc:
                try:
                    self._log_error(
                        "Unhandled exception in offload_table(): {}".format(str(exc))
                    )
                    self._log_error(traceback.format_exc(), detail=VVERBOSE)
                except:
                    pass
                raise

    def _target_version_string(self, backend_table):
        target_version = backend_table.target_version()
        ver_str = f" ({target_version})" if target_version else ""
        return "%s%s" % (backend_table.backend_db_name(), ver_str)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload(
        self,
        params,
        execution_id: Optional[ExecutionId] = None,
        reuse_log: bool = False,
        messages_override: Optional[OffloadMessages] = None,
    ):
        """
        Run an offload based on incoming params.
        params: Can be a dict or an OptParse object.
        execution_id: A UUID used to uniquely identify the Offload. Can be generted internally or provided as
                      an override.
        reuse_log: True indicates that there is already a log file installed in goe.py for us to integrate with.
        messages_override: Allows us to pass in an existing messages object so a parent can inspect the messages,
                           used for testing.
        """
        dry_run = bool(not self._execute_from_params(params))
        repo_client = self._init_command(
            orchestration_constants.COMMAND_OFFLOAD,
            params,
            execution_id=execution_id,
            reuse_log=reuse_log,
            messages_override=messages_override,
        )

        operation = self._gen_offload_operation(params, repo_client)
        command_id = self._command_begin(
            orchestration_constants.COMMAND_OFFLOAD, params, repo_client, operation
        )
        try:
            offload_source_table = self._build_offload_source_table(operation)
            offload_target_table = get_offload_target_table(
                operation, self._config, self._messages
            )

            self._log(
                "Offloading to {}".format(
                    self._target_version_string(offload_target_table)
                )
            )

            status = self._offload(
                operation, offload_source_table, offload_target_table
            )
            self._log_final_messages(orchestration_constants.COMMAND_OFFLOAD, dry_run)

            self._command_end(command_id, repo_client)
            self._cleanup_objects(
                repo_client,
                frontend_table=offload_source_table,
                backend_table=offload_target_table,
            )
        except Exception as exc:
            self._command_fail(command_id, exc, repo_client)
            repo_client.close(force=True)
            raise
        finally:
            if not reuse_log:
                self._messages.close_log()

        return status

    def schema_sync(
        self,
        params,
        execution_id: Optional[ExecutionId] = None,
        reuse_log: bool = False,
        messages_override: Optional[OffloadMessages] = None,
    ):
        """
        Run Schema Sync using incoming params.
        """
        repo_client = self._init_command(
            orchestration_constants.COMMAND_SCHEMA_SYNC,
            params,
            execution_id=execution_id,
            reuse_log=reuse_log,
            messages_override=messages_override,
        )

        # TODO schema_sync() currently only supports params of type Opt/Argparse, not a dict. When we
        #      add Schema Sync to Listener we'll need to change this.
        command_id = self._command_begin(
            orchestration_constants.COMMAND_SCHEMA_SYNC, params, repo_client
        )
        try:
            raise OrchestrationRunnerException("Schema Sync no longer exists")
        except Exception as exc:
            self._command_fail(command_id, exc, repo_client)
            raise
