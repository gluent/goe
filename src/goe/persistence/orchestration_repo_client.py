#! /usr/bin/env python3

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

""" OrchestrationRepoClientInterface: Base interface of API to interact with orchestration metadata repository.
    Each frontend/metadata system will have its own implementation.
"""

# Standard Library
import datetime
import decimal
import json
import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

# GOE
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_source_data import OffloadSourcePartition
from goe.offload.predicate_offload import GenericPredicate
from goe.orchestration import command_steps, orchestration_constants
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.orchestration_metadata import (
    ALL_METADATA_ATTRIBUTES,
    OrchestrationMetadata,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.frontend_api import FrontendApiInterface
    from goe.offload.offload_messages import OffloadMessages


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def type_safe_json_dumps(d) -> str:
    """JSON dump to str that caters for Decimal and datetime, taken from orjson README and modified to suit."""

    def default(obj):
        if isinstance(obj, (decimal.Decimal, datetime.datetime, ExecutionId)):
            return str(obj)
        elif isinstance(obj, GenericPredicate):
            return obj.dsl
        raise TypeError

    return json.dumps(d, default=default)


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OrchestrationRepoClientInterface
###########################################################################


class OrchestrationRepoClientInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for frontend specific sub-classes for
    get/put of orchestration metadata.
    """

    def __init__(
        self,
        connection_options: "OrchestrationConfig",
        messages: "OffloadMessages",
        dry_run: bool = False,
        trace_action: str = None,
    ):
        """Abstract base class which acts as an interface for frontend specific sub-classes for
        get/put of orchestration metadata.
        dry_run: Read only mode
        """
        assert connection_options
        assert messages
        self._connection_options = connection_options
        self._messages = messages
        self._dry_run = dry_run
        self._frontend_client: "Optional[FrontendApiInterface]" = None
        self._trace_action = trace_action or self.__class__.__name__

    def __del__(self):
        self.close()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _log(self, msg, detail=None, ansi_code=None):
        """Write to offload log file"""
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def _warning(self, msg):
        self._messages.warning(msg)
        logger.warning(msg)

    def _assert_valid_start_command_inputs(
        self, execution_id: ExecutionId, command_type: str
    ) -> None:
        assert isinstance(execution_id, ExecutionId), "{} is not ExecutionId".format(
            type(execution_id)
        )
        assert (
            command_type in orchestration_constants.ALL_COMMAND_CODES
        ), f"{command_type} not in ALL_COMMAND_CODES"

    def _assert_valid_start_step_inputs(
        self, execution_id: ExecutionId, command_type: str, command_step: str
    ) -> None:
        assert isinstance(execution_id, ExecutionId), "{} is not ExecutionId".format(
            type(execution_id)
        )
        assert (
            command_type in orchestration_constants.ALL_COMMAND_CODES
        ), f"{command_type} not in ALL_COMMAND_CODES"
        assert (
            command_step in command_steps.STEP_TITLES
        ), f"{command_step} not in STEP_TITLES"

    def _assert_valid_start_chunk_inputs(
        self,
        execution_id: ExecutionId,
        frontend_schema: str,
        frontend_table_name: str,
        backend_schema: str,
        backend_table_name: str,
        chunk_number: int,
        offload_partitions: list,
        offload_partition_level: int,
    ) -> None:
        assert isinstance(execution_id, ExecutionId), "{} is not ExecutionId".format(
            type(execution_id)
        )
        assert isinstance(frontend_schema, str), "{} is not str".format(
            type(frontend_schema)
        )
        assert isinstance(frontend_table_name, str), "{} is not str".format(
            type(frontend_table_name)
        )
        assert isinstance(backend_schema, str), "{} is not str".format(
            type(backend_schema)
        )
        assert isinstance(backend_table_name, str), "{} is not str".format(
            type(backend_table_name)
        )
        assert (
            isinstance(chunk_number, int) and chunk_number > 0
        ), f"{chunk_number} is not a positive int"
        if offload_partitions:
            assert isinstance(
                offload_partitions, list
            ), f"{type(offload_partitions)} is not list"
            assert isinstance(
                offload_partitions[0], OffloadSourcePartition
            ), f"{type(offload_partitions[0])} is not OffloadSourcePartition"
            assert (
                isinstance(offload_partition_level, int) and offload_partition_level > 0
            ), f"{offload_partition_level} is not a positive int"

    def _assert_valid_end_step_inputs(
        self, command_step_id: int, status: str, step_details: dict
    ) -> None:
        if not self._dry_run:
            assert isinstance(command_step_id, int), "{} is not int".format(
                type(command_step_id)
            )
        self._assert_valid_command_status(status)
        if step_details is not None:
            assert isinstance(step_details, dict), "{} is not dict".format(
                type(step_details)
            )

    def _assert_valid_end_chunk_inputs(self, chunk_id: int, status: str) -> None:
        if not self._dry_run:
            assert isinstance(chunk_id, int), "{} is not int".format(type(chunk_id))
        self._assert_valid_command_status(status)

    def _assert_valid_command_status(self, status):
        assert status in [
            orchestration_constants.COMMAND_SUCCESS,
            orchestration_constants.COMMAND_ERROR,
        ]

    @property
    def _frontend_api(self) -> "FrontendApiInterface":
        """
        Return a frontend api client to make calls to the repo.
        Implemented this way to ensure connection is lazy and not taken when class instantiated.
        """
        if self._frontend_client is None:
            self._frontend_client = frontend_api_factory(
                self._connection_options.db_type,
                self._connection_options,
                self._messages,
                dry_run=self._dry_run,
                trace_action=self._trace_action,
            )
        return self._frontend_client

    @abstractmethod
    def _get_metadata(self, frontend_owner: str, frontend_name: str) -> dict:
        """Return metadata object for owner/name"""

    def _metadata_dict_to_json_string(self, metadata_dict) -> str:
        """Simple wrapper over json.dumps to centralise the encoding args."""
        return json.dumps(metadata_dict, sort_keys=True, ensure_ascii=False)

    def _metadata_keys_with_positions(self) -> dict:
        return {k: i for i, k in enumerate(ALL_METADATA_ATTRIBUTES)}

    def _prepare_command_parameters(self, parameters: Union[str, dict]) -> str:
        """Ensure PARAMETERS column value is str"""
        if isinstance(parameters, dict):
            try:
                param_str = json.dumps(parameters)
            except TypeError as exc:
                if any(
                    _ in str(exc)
                    for _ in ["Decimal", "datetime", "GenericPredicate", "ExecutionId"]
                ):
                    param_str = type_safe_json_dumps(parameters)
                else:
                    raise
        else:
            param_str = parameters
        return param_str

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def close(self, force=False):
        """Free up any resources currently held"""
        if self._frontend_client:
            if force:
                try:
                    self._frontend_client.close()
                except:
                    pass
            else:
                self._frontend_client.close()

    #
    # OFFLOAD METADATA
    #
    def get_offload_metadata(
        self, frontend_owner: str, frontend_name: str
    ) -> OrchestrationMetadata:
        """Return metadata object for owner/name"""
        metadata_dict = self._get_metadata(frontend_owner, frontend_name)
        if metadata_dict:
            return OrchestrationMetadata(
                metadata_dict,
                connection_options=self._connection_options,
                messages=self._messages,
                client=self,
                dry_run=self._dry_run,
            )
        else:
            return None

    @abstractmethod
    def set_offload_metadata(
        self,
        metadata: Union[dict, OrchestrationMetadata],
    ):
        """Persist metadata"""

    @abstractmethod
    def drop_offload_metadata(self, frontend_owner: str, frontend_name: str):
        """Remove metadata"""

    #
    # COMMAND EXECUTION LOGGING METHODS
    #
    @abstractmethod
    def start_command(
        self,
        execution_id: ExecutionId,
        command_type: str,
        command_input: Union[str, dict],
        parameters: Union[dict, None],
    ) -> int:
        """
        Record the start of an orchestration command in the repo.
        Returns the numeric identifier of the history record.
        execution_id: The UUID identifying the command execution.
        command_type: Valid code from GOE_REPO.COMMAND_TYPES.
        command_input: Command line or API JSON. The user provided input.
        parameters: Full set of operational parameters as a dictionary.
        """

    @abstractmethod
    def end_command(self, command_execution_id: int, status: str) -> None:
        """
        Record the end of an orchestration command along with its status.
        command_execution_id: The identifier returned from start_command, not the higher level execution id.
        status: Valid status code from GOE_REPO.STATUS table.
        """

    @abstractmethod
    def start_command_step(
        self, execution_id: ExecutionId, command_type: str, command_step: str
    ) -> int:
        """
        Record the start of a discrete step of an orchestration command in the repo.
        Returns the numeric identifier of the history record for use in end_command_step().
        execution_id: The UUID identifying the command execution.
        command_type: Valid code from GOE_REPO.COMMAND_TYPE.
        command_step: Valid code from GOE_REPO.COMMAND_STEP.
        """

    @abstractmethod
    def end_command_step(
        self, command_step_id: int, status: str, step_details: Union[dict, None] = None
    ) -> None:
        """
        Record the end of an orchestration command step along with its status.
        command_step_id: The identifier returned from start_command_step.
        step_details: A dictionary of key/value pairs used to record step outcomes.
        status: Valid status code from GOE_REPO.STATUS table.
        """

    @abstractmethod
    def start_offload_chunk(
        self,
        execution_id: ExecutionId,
        frontend_schema: str,
        frontend_table_name: str,
        backend_schema: str,
        backend_table_name: str,
        chunk_number: int = 1,
        offload_partitions: Union[list, None] = None,
        offload_partition_level: Union[int, None] = None,
    ) -> int:
        """
        Record the start of offload transport for an offload chunk.
        Returns the numeric identifier of the history record for use in end_offload_chunk().
        execution_id: The UUID identifying the command execution.
        frontend_schema/frontend_table_name: Source owner/table names.
        backend_schema/backend_table_name: Target owner/table names.
        chunk_number: Starting with 1.
        offload_partitions: list of OffloadSourcePartition.
        offload_partition_level: Level at which the offloaded partitions were identified in the frontend.
        """

    @abstractmethod
    def end_offload_chunk(
        self,
        chunk_id: int,
        status: str,
        row_count: Union[int, None] = None,
        frontend_bytes: Union[int, None] = None,
        transport_bytes: Union[int, None] = None,
        backend_bytes: Union[int, None] = None,
    ) -> None:
        """
        Record the completion of offload transport for an offload chunk.
        chunk_id: The identifier returned from start_offload_chunk.
        """

    #
    # OFFLOAD LISTENER API METHODS
    #
    @abstractmethod
    def get_offloadable_schemas(self):
        """Returns a dict of all schemas in the database (excluding GOE-created ones)
        and whether they currently have a hybrid schema created.
        """

    @abstractmethod
    def get_schema_tables(self, schema_name: str):
        """Returns a dict of all tables for a schema"""

    @abstractmethod
    def get_table_columns(self, schema_name: str, table_name: str):
        """Returns a dict of all columns for a schema's table"""

    @abstractmethod
    def get_table_partitions(self, schema_name: str, table_name: str):
        """Returns a dict of all partitions for a schema's table"""

    @abstractmethod
    def get_table_subpartitions(self, schema_name: str, table_name: str):
        """Returns a dict of all subpartitions for a schema's table"""

    # GENERAL REPO INTROSPECTION
    #
    @abstractmethod
    def get_command_step_codes(self) -> list:
        """Return a list of codes from REPO.COMMEND_STEP table"""

    @abstractmethod
    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, Any]]:
        """Return a list of command executions"""

    @abstractmethod
    def get_command_executions(self) -> List[Dict[str, Union[str, Any]]]:
        """Return a list of command executions"""

    @abstractmethod
    def get_command_execution_steps(
        self, execution_id: Optional[ExecutionId]
    ) -> List[Dict[str, Union[str, Any]]]:
        """Return a list of steps for a given execution id"""
