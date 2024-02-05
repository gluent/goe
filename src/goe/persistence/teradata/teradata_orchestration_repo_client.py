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

""" TeradataOrchestrationRepoClient: Teradata implementation of API for get/put of orchestration metadata.
"""

import json
import logging
from textwrap import dedent
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.orchestration_metadata import (
    OrchestrationMetadata,
    ALL_METADATA_ATTRIBUTES,
    INCREMENTAL_PREDICATE_VALUE,
    OFFLOADED_OWNER,
    OFFLOADED_TABLE,
)
from goe.persistence.orchestration_repo_client import OrchestrationRepoClientInterface

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.offload_messages import OffloadMessages


###############################################################################
# CONSTANTS
###############################################################################

METADATA_SOURCE_TYPE_VIEW = "VIEW"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# TeradataOrchestrationRepoClient
###########################################################################


class TeradataOrchestrationRepoClient(OrchestrationRepoClientInterface):
    """TeradataOrchestrationRepoClient: Teradata implementation of API for get/put of orchestration metadata"""

    def __init__(
        self,
        connection_options: "OrchestrationConfig",
        messages: "OffloadMessages",
        dry_run: bool = False,
        trace_action: str = None,
    ):
        super().__init__(
            connection_options, messages, dry_run=dry_run, trace_action=trace_action
        )
        self._repo_user = self._connection_options.teradata_repo_user

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _drop_metadata(self, frontend_owner: str, frontend_name: str):
        logger.debug(f"Dropping metadata: {frontend_owner}, {frontend_name}")
        assert frontend_owner
        assert frontend_name
        sql = dedent(
            f"""\
            DELETE {self._repo_user}.offload_metadata om
            WHERE  om.frontend_object_id = (
                SELECT fo.id
                FROM   {self._repo_user}.frontend_object fo
                WHERE  fo.object_owner = ?
                AND    fo.object_name = ?
                );
            """
        )
        self._frontend_api.execute_dml(
            sql, query_params=[frontend_owner, frontend_name], log_level=VERBOSE
        )

    def _get_metadata(self, frontend_owner: str, frontend_name: str) -> dict:
        logger.debug(f"Fetching metadata: {frontend_owner}, {frontend_name}")
        assert frontend_owner
        assert frontend_name
        # Columns will be in a specific order due to ALL_METADATA_ATTRIBUTES being a list
        projection = ",".join(ALL_METADATA_ATTRIBUTES)
        sql = dedent(
            f"""\
            SELECT {projection}
            FROM   {self._repo_user}.offload_metadata om
            WHERE  om.frontend_object_id = (
                SELECT fo.id
                FROM   {self._repo_user}.frontend_object fo
                WHERE  fo.object_owner = ?
                AND    fo.object_name = ?
                );
            """
        )
        row = self._frontend_api.execute_query_fetch_one(
            sql, query_params=[frontend_owner, frontend_name], log_level=VVERBOSE
        )
        if row:
            return self._metadata_row_to_metadata_dict(row)
        return None

    def _metadata_row_to_metadata_dict(self, row_tuple):
        def format_row_item(k, i):
            if k in (INCREMENTAL_PREDICATE_VALUE) and row_tuple[i]:
                return json.loads(row_tuple[i])
            elif row_tuple[i] == "":
                return None
            else:
                return row_tuple[i]

        assert row_tuple
        # Row items will be in a specific order due to ALL_METADATA_ATTRIBUTES being a list
        metadata_dict = {
            k: format_row_item(k, i) for i, k in enumerate(ALL_METADATA_ATTRIBUTES)
        }
        return metadata_dict

    def _set_metadata(self, metadata: Union[dict, OrchestrationMetadata]):
        def prep_value(k, metadata):
            v = metadata.get(k)
            if v is None:
                return None
            elif isinstance(v, str):
                return f"{v}"
            elif isinstance(v, (dict, list)):
                return self._metadata_dict_to_json_string(v)
            else:
                return v

        assert metadata
        if isinstance(metadata, OrchestrationMetadata):
            metadata = metadata.as_dict()

        logger.debug(
            f"Writing metadata: {metadata[OFFLOADED_OWNER]}, {metadata[OFFLOADED_TABLE]}"
        )
        source_alias = "src"
        target_alias = "tgt"
        parameter_markers = ",".join("?" for _ in ALL_METADATA_ATTRIBUTES)
        unaliased_columns = ",".join(ALL_METADATA_ATTRIBUTES)
        source_columns = ",".join(
            f"{source_alias}.{_}" for _ in ALL_METADATA_ATTRIBUTES
        )
        update_columns = "\n            ,    ".join(
            f"{_} = {source_alias}.{_}"
            for _ in ALL_METADATA_ATTRIBUTES
            if _ not in [OFFLOADED_OWNER, OFFLOADED_TABLE]
        )
        insert_parameters = [prep_value(_, metadata) for _ in ALL_METADATA_ATTRIBUTES]
        sql = dedent(
            f"""\
            MERGE INTO {self._repo_user}.offload_metadata {target_alias}
            USING VALUES ({parameter_markers}) AS src ({unaliased_columns})
            ON ({source_alias}.{OFFLOADED_OWNER} = {target_alias}.{OFFLOADED_OWNER}
                AND {source_alias}.{OFFLOADED_TABLE} = {target_alias}.{OFFLOADED_TABLE})
            WHEN MATCHED THEN
            UPDATE
            SET  {update_columns}
            WHEN NOT MATCHED THEN
            INSERT
            ({unaliased_columns})
            VALUES
            ({source_columns})
            """
        )
        self._frontend_api.execute_dml(
            sql, query_params=insert_parameters, log_level=VERBOSE
        )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def set_offload_metadata(
        self,
        metadata: Union[dict, OrchestrationRepoClientInterface],
    ):
        self._set_metadata(metadata)

    def drop_offload_metadata(self, frontend_owner, frontend_name):
        self._drop_metadata(frontend_owner, frontend_name)

    #
    # COMMAND EXECUTION LOGGING METHODS
    #
    def start_command(
        self,
        execution_id: ExecutionId,
        command_type: str,
        command_input: Union[str, dict, None],
        parameters: Union[dict, None],
    ) -> int:
        self._log(
            f"Recording command start: {execution_id}/{command_type})", detail=VVERBOSE
        )
        self._debug(f"command_input: {command_input}")
        self._assert_valid_start_command_inputs(execution_id, command_type)
        prepared_input = self._prepare_command_parameters(command_input)
        prepared_parameters = self._prepare_command_parameters(parameters)
        # TODO For MVP this method is a pass-thru
        return -1

    def end_command(self, command_execution_id: int, status: str) -> None:
        self._log(
            f"Recording command {command_execution_id} status: {status}",
            detail=VVERBOSE,
        )
        self._assert_valid_command_status(status)
        # TODO For MVP this method is a pass-thru

    def start_command_step(
        self, execution_id: ExecutionId, command_type: str, command_step: str
    ) -> int:
        self._log(
            f"Recording command step start: {execution_id}/{command_step}",
            detail=VVERBOSE,
        )
        self._assert_valid_start_step_inputs(execution_id, command_type, command_step)
        # TODO For MVP this method is a pass-thru
        return -1

    def end_command_step(
        self, command_step_id: int, status: str, step_details: Optional[dict] = None
    ) -> None:
        self._log(
            f"Recording command step {command_step_id} status: {status}",
            detail=VVERBOSE,
        )
        self._assert_valid_end_step_inputs(command_step_id, status, step_details)
        step_details_str = (
            json.dumps(step_details) if step_details is not None else None
        )
        # TODO For MVP this method is a pass-thru

    def start_offload_chunk(
        self,
        execution_id: ExecutionId,
        frontend_schema: str,
        frontend_table_name: str,
        backend_schema: str,
        backend_table_name: str,
        chunk_number: int = 1,
        offload_partitions: Optional[list] = None,
        offload_partition_level: Optional[int] = None,
    ) -> int:
        self._log(
            f"Recording command chunk start: {execution_id}/{chunk_number}",
            detail=VVERBOSE,
        )
        self._debug(f"frontend: {frontend_schema}/{frontend_table_name})")
        self._debug(f"backend: {backend_schema}/{backend_table_name})")
        self._assert_valid_start_chunk_inputs(
            execution_id,
            frontend_schema,
            frontend_table_name,
            backend_schema,
            backend_table_name,
            chunk_number,
            offload_partitions,
            offload_partition_level,
        )
        # TODO For MVP this method is a pass-thru
        return -1

    def end_offload_chunk(
        self,
        chunk_id: int,
        status: str,
        row_count: Optional[int] = None,
        frontend_bytes: Optional[int] = None,
        transport_bytes: Optional[int] = None,
        backend_bytes: Optional[int] = None,
    ) -> None:
        self._log(f"Recording chunk {chunk_id} status: {status}", detail=VVERBOSE)
        self._debug(f"row_count: {row_count})")
        self._debug(f"frontend_bytes: {frontend_bytes})")
        self._debug(f"transport_bytes: {transport_bytes})")
        self._debug(f"backend_bytes: {backend_bytes})")
        self._assert_valid_end_chunk_inputs(chunk_id, status)
        # TODO For MVP this method is a pass-thru

    #
    # ORACLE LISTENER API METHODS
    #

    def get_command_step_codes(self) -> list:
        raise NotImplementedError(
            "Teradata get_command_step_codes pending implementation"
        )
        # sql = f"SELECT code FROM {self._repo_user}.command_step ORDER BY 1"
        # rows = self._frontend_api.execute_query_fetch_all(sql, log_level=VVERBOSE)
        # return [_[0] for _ in rows] if rows else rows

    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, Any]]:
        raise NotImplementedError(
            "Teradata get_command_execution pending implementation"
        )

    def get_command_execution_steps(
        self,
        execution_id: Optional[ExecutionId],
    ) -> List[Dict[str, Union[str, Any]]]:
        raise NotImplementedError(
            "Teradata get_command_execution_steps pending implementation"
        )

    def get_command_executions(
        self,
    ) -> List[Dict[str, Union[str, Any]]]:
        raise NotImplementedError(
            "Teradata get_command_executions pending implementation"
        )

    def get_offloadable_schemas(self):
        raise NotImplementedError(
            "Teradata get_offloadable_schemas pending implementation"
        )

    def get_schema_tables(self, schema_name):
        raise NotImplementedError("Teradata get_schema_tables pending implementation")

    def get_table_columns(self, schema_name, table_name):
        raise NotImplementedError("Teradata get_table_columns pending implementation")

    def get_table_partitions(self, schema_name, table_name):
        raise NotImplementedError(
            "Teradata get_table_partitions pending implementation"
        )

    def get_table_subpartitions(self, schema_name, table_name):
        raise NotImplementedError(
            "Teradata get_table_subpartitions pending implementation"
        )
