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

"""OracleOrchestrationRepoClient: Oracle implementation of API for get/put of orchestration metadata."""

# Standard Library
import json
import logging
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

# Third Party Libraries
import oracledb

# GOE
from goe.offload.offload_messages import QUIET, VERBOSE, VVERBOSE
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.orchestration_metadata import (
    HADOOP_OWNER,
    HADOOP_TABLE,
    INCREMENTAL_HIGH_VALUE,
    INCREMENTAL_KEY,
    INCREMENTAL_PREDICATE_TYPE,
    INCREMENTAL_PREDICATE_VALUE,
    INCREMENTAL_RANGE,
    OFFLOAD_BUCKET_COLUMN,
    OFFLOAD_PARTITION_FUNCTIONS,
    OFFLOAD_SNAPSHOT,
    OFFLOAD_SORT_COLUMNS,
    OFFLOAD_TYPE,
    OFFLOADED_OWNER,
    OFFLOADED_TABLE,
    COMMAND_EXECUTION,
    OrchestrationMetadata,
)
from goe.persistence.orchestration_repo_client import (
    OrchestrationRepoClientInterface,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.offload_messages import OffloadMessages


###############################################################################
# CONSTANTS
###############################################################################

OFFLOAD_METADATA_ORA_TYPE_NAME = "OFFLOAD_METADATA_OT"
OFFLOAD_PARTITION_ORA_TYPE_NAME = "OFFLOAD_PARTITION_OT"
OFFLOAD_PARTITIONS_ORA_TYPE_NAME = "OFFLOAD_PARTITION_NTT"

METADATA_SOURCE_TYPE_VIEW = "VIEW"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OracleOrchestrationRepoClient
###########################################################################


class OracleOrchestrationRepoClient(OrchestrationRepoClientInterface):
    """OracleOrchestrationRepoClient: Oracle implementation of API for get/put of orchestration metadata"""

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
        self._repo_user = self._connection_options.ora_repo_user

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _drop_metadata(self, frontend_owner: str, frontend_name: str):
        logger.debug(f"Dropping metadata: {frontend_owner}, {frontend_name}")
        assert frontend_owner
        assert frontend_name
        # In Oracle we expect the identifying owner/name to be upper case
        frontend_owner = frontend_owner.upper()
        frontend_name = frontend_name.upper()
        self._frontend_api.execute_function(
            "offload_repo.delete_offload_metadata",
            arg_list=[frontend_owner, frontend_name],
            not_when_dry_running=True,
            commit=True,
        )

    def _get_metadata(self, frontend_owner: str, frontend_name: str) -> dict:
        logger.debug(f"Fetching metadata: {frontend_owner}, {frontend_name}")
        assert frontend_owner
        assert frontend_name
        # In Oracle we expect the identifying owner/name to be upper case
        frontend_owner = frontend_owner.upper()
        frontend_name = frontend_name.upper()
        metadata_obj = self._frontend_api.execute_function(
            "offload_repo.get_offload_metadata",
            return_type=oracledb.DB_TYPE_OBJECT,
            return_type_name=self._get_ora_type_object_name(
                OFFLOAD_METADATA_ORA_TYPE_NAME
            ),
            arg_list=[frontend_owner, frontend_name],
            log_level=VVERBOSE,
        )
        if metadata_obj:
            return self._ora_object_to_metadata_dict(metadata_obj)
        return None

    def _get_offload_metadata_ora_type_object(self):
        """This subverts FrontendApi because it has knowledge about oracledb. We are in an Oracle only class
        so this is slightly less terrible but still not ideal.
        """
        return self._get_ora_type_object(OFFLOAD_METADATA_ORA_TYPE_NAME)

    def _get_ora_type_object(self, repo_type_name: str, owner_override: str = None):
        qualified_name = self._get_ora_type_object_name(
            repo_type_name, owner_override=owner_override
        )
        return self._frontend_api.oracle_get_type_object(qualified_name)

    def _get_ora_type_object_name(
        self, repo_type_name: str, owner_override: str = None
    ):
        return '"{}"."{}"'.format(
            (owner_override or self._repo_user).upper(), repo_type_name.upper()
        )

    def _metadata_dict_to_ora_object(self, metadata_dict):
        """
        Used to convert a Python dict of metadata to an Oracle object type ready for saving to the database.
        """
        logger.debug(f"Converting metadata: {metadata_dict}")
        metadata_obj = self._get_offload_metadata_ora_type_object()
        metadata_obj.FRONTEND_OBJECT_OWNER = metadata_dict[OFFLOADED_OWNER]
        metadata_obj.FRONTEND_OBJECT_NAME = metadata_dict[OFFLOADED_TABLE]
        metadata_obj.BACKEND_OBJECT_OWNER = metadata_dict[HADOOP_OWNER]
        metadata_obj.BACKEND_OBJECT_NAME = metadata_dict[HADOOP_TABLE]
        metadata_obj.OFFLOAD_TYPE = metadata_dict[OFFLOAD_TYPE]
        metadata_obj.OFFLOAD_RANGE_TYPE = metadata_dict[INCREMENTAL_RANGE]
        metadata_obj.OFFLOAD_KEY = metadata_dict[INCREMENTAL_KEY]
        metadata_obj.OFFLOAD_HIGH_VALUE = metadata_dict[INCREMENTAL_HIGH_VALUE]
        metadata_obj.OFFLOAD_PREDICATE_TYPE = metadata_dict[INCREMENTAL_PREDICATE_TYPE]
        if metadata_dict[INCREMENTAL_PREDICATE_VALUE] is None:
            metadata_obj.OFFLOAD_PREDICATE_VALUE = metadata_dict[
                INCREMENTAL_PREDICATE_VALUE
            ]
        else:
            metadata_obj.OFFLOAD_PREDICATE_VALUE = self._metadata_dict_to_json_string(
                metadata_dict[INCREMENTAL_PREDICATE_VALUE]
            )
        metadata_obj.OFFLOAD_SNAPSHOT = metadata_dict[OFFLOAD_SNAPSHOT]
        metadata_obj.OFFLOAD_HASH_COLUMN = metadata_dict[OFFLOAD_BUCKET_COLUMN]
        metadata_obj.OFFLOAD_SORT_COLUMNS = metadata_dict[OFFLOAD_SORT_COLUMNS]
        metadata_obj.OFFLOAD_PARTITION_FUNCTIONS = metadata_dict[
            OFFLOAD_PARTITION_FUNCTIONS
        ]
        metadata_obj.COMMAND_EXECUTION = metadata_dict[COMMAND_EXECUTION].as_bytes()
        return metadata_obj

    def _ora_object_to_metadata_dict(self, metadata_obj):
        """Converts the Oracle object type to a Python dict of metadata to be used in orchestration."""
        metadata_dict = {
            HADOOP_OWNER: metadata_obj.BACKEND_OBJECT_OWNER,
            HADOOP_TABLE: metadata_obj.BACKEND_OBJECT_NAME,
            OFFLOAD_TYPE: metadata_obj.OFFLOAD_TYPE,
            OFFLOADED_OWNER: metadata_obj.FRONTEND_OBJECT_OWNER,
            OFFLOADED_TABLE: metadata_obj.FRONTEND_OBJECT_NAME,
            INCREMENTAL_KEY: metadata_obj.OFFLOAD_KEY or None,
            INCREMENTAL_HIGH_VALUE: (
                metadata_obj.OFFLOAD_HIGH_VALUE.read()
                if metadata_obj.OFFLOAD_HIGH_VALUE
                else None
            ),
            INCREMENTAL_RANGE: metadata_obj.OFFLOAD_RANGE_TYPE or None,
            INCREMENTAL_PREDICATE_TYPE: metadata_obj.OFFLOAD_PREDICATE_TYPE or None,
            INCREMENTAL_PREDICATE_VALUE: (
                json.loads(metadata_obj.OFFLOAD_PREDICATE_VALUE.read())
                if metadata_obj.OFFLOAD_PREDICATE_VALUE
                else None
            ),
            OFFLOAD_BUCKET_COLUMN: metadata_obj.OFFLOAD_HASH_COLUMN or None,
            OFFLOAD_SORT_COLUMNS: metadata_obj.OFFLOAD_SORT_COLUMNS or None,
            OFFLOAD_SNAPSHOT: metadata_obj.OFFLOAD_SNAPSHOT or None,
            OFFLOAD_PARTITION_FUNCTIONS: metadata_obj.OFFLOAD_PARTITION_FUNCTIONS
            or None,
            COMMAND_EXECUTION: ExecutionId.from_bytes(metadata_obj.COMMAND_EXECUTION),
        }
        return metadata_dict

    def _offload_partitions_to_ora_object(
        self,
        offload_partitions: list,
        offload_partition_level: int,
        frontend_schema: str,
        frontend_table_name: str,
    ):
        """
        Used to convert a Python dict of metadata to an Oracle object type ready for saving to the database.
        """
        logger.debug("Converting offload_partitions")
        partitions_ntt = self._get_ora_type_object(OFFLOAD_PARTITIONS_ORA_TYPE_NAME)
        if not offload_partitions:
            return partitions_ntt
        partition_obj = self._get_ora_type_object(OFFLOAD_PARTITION_ORA_TYPE_NAME)
        for partition in offload_partitions:
            partition_obj.TABLE_OWNER = frontend_schema
            partition_obj.TABLE_NAME = frontend_table_name
            partition_obj.PARTITION_NAME = partition.partition_name
            partition_obj.PARTITION_LEVEL = offload_partition_level
            partition_obj.PARTITION_BYTES = partition.size_in_bytes
            partition_obj.PARTITION_BOUNDARY = partition.partition_literal
            partitions_ntt.append(partition_obj)
        return partitions_ntt

    def _set_metadata(
        self,
        metadata: Union[dict, OrchestrationMetadata],
    ):
        assert metadata
        # In Oracle we expect the identifying owner/name to be upper case
        frontend_owner = metadata[OFFLOADED_OWNER].upper()
        frontend_name = metadata[OFFLOADED_TABLE].upper()
        logger.debug(f"Writing metadata: {frontend_owner}, {frontend_name}")
        if isinstance(metadata, OrchestrationMetadata):
            metadata = metadata.as_dict()
        ora_metadata = self._metadata_dict_to_ora_object(metadata)
        self._frontend_api.execute_function(
            "offload_repo.save_offload_metadata",
            arg_list=[frontend_owner, frontend_name, ora_metadata],
            not_when_dry_running=True,
            commit=True,
        )
        # FrontendApi logging won't show metadata values due to being in an Oracle type. So we log it here for
        # benefit of support.
        self._log("Saved metadata: {}".format(str(metadata)), detail=VERBOSE)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def set_offload_metadata(
        self,
        metadata: Union[dict, OrchestrationRepoClientInterface],
    ):
        self._set_metadata(metadata)

    def drop_offload_metadata(self, frontend_owner: str, frontend_name: str):
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
        """Call into Oracle API function OFFLOAD_REPO.START_COMMAND_EXECUTION()"""
        self._log(
            f"Recording command start: {execution_id}/{command_type})", detail=VVERBOSE
        )
        self._debug(f"command_input: {command_input}")
        self._assert_valid_start_command_inputs(execution_id, command_type)
        prepared_input = self._prepare_command_parameters(command_input)
        prepared_parameters = self._prepare_command_parameters(parameters)
        conn_obj = self._frontend_api.get_oracle_connection_object()
        command_execution_id = conn_obj.cursor().var(int)
        self._frontend_api.execute_function(
            "offload_repo.start_command_execution",
            arg_list=[
                execution_id.as_bytes(),
                command_type,
                self._messages.get_log_fh_name(),
                prepared_input,
                prepared_parameters,
                command_execution_id,
            ],
            log_level=VVERBOSE,
            not_when_dry_running=True,
        )
        command_execution_id = command_execution_id.getvalue()
        self._debug(f"command_execution_id: {command_execution_id})")
        return command_execution_id

    def end_command(self, command_execution_id: int, status: str) -> None:
        """Call into Oracle API function OFFLOAD_REPO.END_COMMAND_EXECUTION()"""
        self._log(
            f"Recording command {command_execution_id} status: {status}",
            detail=VVERBOSE,
        )
        self._assert_valid_command_status(status)
        self._frontend_api.execute_function(
            "offload_repo.end_command_execution",
            arg_list=[command_execution_id, status],
            log_level=VVERBOSE,
            not_when_dry_running=True,
        )

    def start_command_step(
        self, execution_id: ExecutionId, command_type: str, command_step: str
    ) -> int:
        """Call into Oracle API function OFFLOAD_REPO.START_COMMAND_EXECUTION_STEP()"""
        self._log(
            f"Recording command step start: {execution_id}/{command_step}",
            detail=VVERBOSE,
        )
        self._assert_valid_start_step_inputs(execution_id, command_type, command_step)
        conn_obj = self._frontend_api.get_oracle_connection_object()
        command_step_id = conn_obj.cursor().var(int)
        self._frontend_api.execute_function(
            "offload_repo.start_command_execution_step",
            arg_list=[
                execution_id.as_bytes(),
                command_type,
                command_step,
                command_step_id,
            ],
            log_level=VVERBOSE,
            not_when_dry_running=True,
        )
        command_step_id = command_step_id.getvalue()
        self._debug(f"command_step_id: {command_step_id})")
        return command_step_id

    def end_command_step(
        self, command_step_id: int, status: str, step_details: Union[dict, None] = None
    ) -> None:
        """Call into Oracle API function OFFLOAD_REPO.END_COMMAND_EXECUTION_STEP()"""
        self._log(
            f"Recording command step {command_step_id} status: {status}",
            detail=VVERBOSE,
        )
        self._assert_valid_end_step_inputs(command_step_id, status, step_details)
        step_details_str = (
            json.dumps(step_details) if step_details is not None else None
        )
        self._frontend_api.execute_function(
            "offload_repo.end_command_execution_step",
            arg_list=[command_step_id, step_details_str, status],
            log_level=VVERBOSE,
            not_when_dry_running=True,
        )

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
        """Call into Oracle API function OFFLOAD_REPO.START_OFFLOAD_CHUNK()"""
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
        conn_obj = self._frontend_api.get_oracle_connection_object()
        chunk_id = conn_obj.cursor().var(int)
        offload_partitions_ntt = self._offload_partitions_to_ora_object(
            offload_partitions,
            offload_partition_level,
            frontend_schema,
            frontend_table_name,
        )
        self._frontend_api.execute_function(
            "offload_repo.start_offload_chunk",
            arg_list=[
                execution_id.as_bytes(),
                frontend_schema,
                frontend_table_name,
                backend_schema,
                backend_table_name,
                chunk_number,
                offload_partitions_ntt,
                chunk_id,
            ],
            log_level=VVERBOSE,
            not_when_dry_running=True,
        )
        chunk_id = chunk_id.getvalue()
        self._debug(f"chunk_id: {chunk_id})")
        return chunk_id

    def end_offload_chunk(
        self,
        chunk_id: int,
        status: str,
        row_count: Union[int, None] = None,
        frontend_bytes: Union[int, None] = None,
        transport_bytes: Union[int, None] = None,
        backend_bytes: Union[int, None] = None,
    ) -> None:
        """Call into Oracle API function OFFLOAD_REPO.END_OFFLOAD_CHUNK()"""
        self._log(f"Recording chunk {chunk_id} status: {status}", detail=VVERBOSE)
        self._debug(f"row_count: {row_count})")
        self._debug(f"frontend_bytes: {frontend_bytes})")
        self._debug(f"transport_bytes: {transport_bytes})")
        self._debug(f"backend_bytes: {backend_bytes})")
        self._assert_valid_end_chunk_inputs(chunk_id, status)
        self._frontend_api.execute_function(
            "offload_repo.end_offload_chunk",
            arg_list=[
                chunk_id,
                row_count,
                frontend_bytes,
                transport_bytes,
                backend_bytes,
                status,
            ],
            log_level=VVERBOSE,
            not_when_dry_running=True,
        )

    def get_command_step_codes(self) -> list:
        sql = f"SELECT code FROM {self._repo_user}.command_step ORDER BY 1"
        rows = self._frontend_api.execute_query_fetch_all(sql, log_level=QUIET)
        return [_[0] for _ in rows] if rows else rows

    def get_command_executions(
        self,
    ) -> List[Dict[str, Union[str, Any]]]:
        """Gets command execution stats"""
        sql = f"""
            SELECT  CE.UUID                AS EXECUTION_ID,
                    CT.CODE                AS COMMAND_TYPE_CODE,
                    CT.NAME                AS COMMAND_TYPE,
                    S.CODE                 AS STATUS_CODE,
                    S.NAME                 AS STATUS,
                    CE.START_TIME          AS STARTED_AT,
                    CE.END_TIME            AS COMPLETED_AT,
                    CE.COMMAND_LOG_PATH    AS COMMAND_LOG_PATH,
                    CE.COMMAND_INPUT       AS COMMAND_INPUT,
                    CE.COMMAND_PARAMETERS  AS COMMAND_PARAMETERS,
                    GV.VERSION             AS GOE_VERSION,
                    GV.BUILD               AS GOE_BUILD
            FROM {self._repo_user}.COMMAND_EXECUTION CE
            JOIN {self._repo_user}.STATUS S on S.ID = CE.STATUS_ID
            JOIN {self._repo_user}.COMMAND_TYPE CT on CT.ID = CE.COMMAND_TYPE_ID
            JOIN {self._repo_user}.GOE_VERSION GV on GV.ID = CE.GOE_VERSION_ID
        """  # noqa: W605 W291
        return self._frontend_api.execute_query_fetch_all(
            sql,
            as_dict=True,
            log_level=None,
        )

    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, Any]]:
        """Gets command execution stats"""
        sql = f"""
            SELECT  CE.UUID                AS EXECUTION_ID,
                    CT.CODE                AS COMMAND_TYPE_CODE,
                    CT.NAME                AS COMMAND_TYPE,
                    S.CODE                 AS STATUS_CODE,
                    S.NAME                 AS STATUS,
                    CE.START_TIME          AS STARTED_AT,
                    CE.END_TIME            AS COMPLETED_AT,
                    CE.COMMAND_LOG_PATH    AS COMMAND_LOG_PATH,
                    CE.COMMAND_INPUT       AS COMMAND_INPUT,
                    CE.COMMAND_PARAMETERS  AS COMMAND_PARAMETERS,
                    GV.VERSION             AS GOE_VERSION,
                    GV.BUILD               AS GOE_BUILD
            FROM {self._repo_user}.COMMAND_EXECUTION CE
            JOIN {self._repo_user}.STATUS S on S.ID = CE.STATUS_ID
            JOIN {self._repo_user}.COMMAND_TYPE CT on CT.ID = CE.COMMAND_TYPE_ID
            JOIN {self._repo_user}.GOE_VERSION GV on GV.ID = CE.GOE_VERSION_ID
            WHERE CE.UUID = :execution_id
        """  # noqa: W605 W291
        return self._frontend_api.execute_query_fetch_one(
            sql,
            as_dict=True,
            query_params={"execution_id": execution_id.as_bytes()},
            log_level=None,
        )

    def get_command_execution_steps(
        self,
        execution_id: Optional[ExecutionId],
    ) -> List[Dict[str, Union[str, Any]]]:
        """Gets command execution stats"""
        query_params = {}
        sql = f"""
            SELECT  CE.UUID          AS EXECUTION_ID,
                    CS.ID            AS STEP_ID,
                    CS.CODE          AS STEP_CODE,
                    CS.TITLE         AS STEP_TITLE,
                    CESS.CODE        AS STEP_STATUS_CODE,
                    CESS.NAME        AS STEP_STATUS,
                    CES.START_TIME   AS STARTED_AT,
                    CES.END_TIME     AS COMPLETED_AT,
                    CES.STEP_DETAILS AS STEP_DETAILS
            FROM {self._repo_user}.COMMAND_EXECUTION CE
            JOIN {self._repo_user}.STATUS S on S.ID = CE.STATUS_ID
            JOIN {self._repo_user}.COMMAND_EXECUTION_STEP CES ON CE.ID = CES.COMMAND_EXECUTION_ID
            JOIN {self._repo_user}.STATUS CESS on CESS.ID = CES.STATUS_ID
            JOIN {self._repo_user}.COMMAND_STEP CS ON CS.ID = CES.COMMAND_STEP_ID
            JOIN {self._repo_user}.COMMAND_TYPE CT ON CT.ID = CES.COMMAND_TYPE_ID
        """  # noqa: W605 W291
        if execution_id:
            sql = f"{sql} WHERE CE.UUID = :execution_id"
            query_params = {"execution_id": execution_id.as_bytes()}
        return self._frontend_api.execute_query_fetch_all(
            sql,
            as_dict=True,
            query_params=query_params,
            log_level=None,
        )
