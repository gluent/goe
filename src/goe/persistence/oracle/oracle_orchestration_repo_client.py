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

""" OracleOrchestrationRepoClient: Oracle implementation of API for get/put of orchestration metadata.
"""

# Standard Library
import json
import logging
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

# Third Party Libraries
import cx_Oracle

# GOE
from goe.listener.schemas.system import (
    ColumnDetail,
    PartitionDetail,
    SubPartitionDetail,
)
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import (
    BACKEND_DISTRO_CDH,
    BACKEND_DISTRO_GCP,
    BACKEND_DISTRO_SNOWFLAKE,
    BACKEND_DISTRO_MSAZURE,
)
from goe.offload.offload_messages import QUIET, VERBOSE, VVERBOSE, OffloadMessages
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
            return_type=cx_Oracle.OBJECT,
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
        """This subverts FrontendApi because it has knowledge about cx-Oracle. We are in an Oracle only class
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

    #
    # ORACLE LISTENER API METHODS
    #

    def get_offloadable_schemas(self):
        sql = f"""
        WITH valid_schemas AS (
                SELECT owner AS schema_name
                ,      count(*) table_count
                FROM   dba_tables
                WHERE  1=1
                AND owner NOT IN (
                    SELECT grantee
                    FROM dba_role_privs
                    WHERE granted_role = 'GOE_OFFLOAD_ROLE'
                    AND grantee LIKE '%\_H' escape '\\'
                )
                AND owner NOT IN (
                'ANONYMOUS' , 'APEX_PUBLIC_USER' , 'APPQOSSYS'
                , 'AUDSYS' , 'AURORA$JIS$UTILITY$' , 'CSMIG' , 'CTXSYS'
                , 'DBMS_PRIVILEGE_CAPTURE' , 'DBSFWUSER' , 'DBSNMP' , 'DIP'
                , 'DMSYS' , 'DSSYS' , 'DVF' , 'DVSYS'
                , 'ECCADMIN' , 'EXFSYS' , 'FLOWS_030000' , 'FLOWS_FILES'
                , 'GGSYS' , 'GSMADMIN_INTERNAL' , 'GSMCATUSER' , 'GSMROOTUSER'
                , 'GSMUSER' , 'IGNITE' , 'LBACSYS' , 'MDDATA'
                , 'MDSYS' , 'MGMT_VIEW' , 'MTSSYS' , 'OJVMSYS'
                , 'OLAPSYS' , 'ORACLE_OCM' , 'ORDDATA' , 'ORDPLUGINS'
                , 'ORDSYS' , 'OSE$HTTP$ADMIN' , 'OUTLN' , 'OWBSYS'
                , 'OWBSYS_AUDIT' , 'PERFSTAT' , 'REMOTE_SCHEDULER_AGENT' , 'SI_INFORMTN_SCHEMA'
                , 'SPATIAL_CSW_ADMIN_USR' , 'SPATIAL_WFS_ADMIN_USR' , 'SYS$UMF' , 'SYS'
                , 'SYSBACKUP' , 'SYSDG' , 'SYSKM' , 'SYSMAN'
                , 'SYSRAC' , 'SYSTEM' , 'TRACESVR' , 'TSMSYS'
                , 'WKPROXY' , 'WKSYS' , 'WK_TEST' , 'WMSYS'
                , 'XDB' , 'XS$NULL'
                )
                AND NOT regexp_like( owner, '^APEX_[0-9]*')
                AND NOT regexp_like( owner, '^FLOWS_[0-9]*')
                AND owner NOT IN (upper('{self._connection_options.ora_adm_user}'),upper('{self._connection_options.ora_app_user}'),upper('{self._connection_options.ora_repo_user}'))
                GROUP BY owner
                )
        ,    segments AS (
                SELECT /*+
                            MATERIALIZE
                            LEADING(s vs)
                            NO_MERGE(s)
                            USE_HASH(vs)
                            SWAP_JOIN_INPUTS(vs)
                        */
                    s.owner
                ,      CASE
                        WHEN EXISTS( SELECT 1 FROM dba_role_privs WHERE granted_role = 'GOE_OFFLOAD_ROLE' AND grantee = owner || '_H' ) THEN 'True'
                        ELSE 'False'
                    END hybrid_schema_exists
                ,      vs.table_count
                ,      s.segment_name
                ,      s.partition_name
                ,      s.segment_type
                ,      s.bytes
                FROM   dba_segments         s
                    INNER JOIN
                    valid_schemas        vs
                    ON (vs.schema_name   = s.owner)
                WHERE  1=1
                )
        SELECT
            s.owner  schema_name
            , s.hybrid_schema_exists
            , s.table_count
            , sum(bytes) schema_size_in_bytes
        FROM
            segments s
        GROUP BY
            s.owner
            , s.hybrid_schema_exists
            , s.table_count
        ORDER BY
            s.owner
        """  # noqa: W605 E501

        return self._frontend_api.execute_query_fetch_all(
            sql,
            log_level=None,
            as_dict=True,
        )

    def get_schema_tables(self, schema_name):
        backend = self._connection_options.backend_distribution
        # Relevant unsupported column name pattern number to inject into the SQL...
        if backend == BACKEND_DISTRO_CDH:
            unsupported_column_pattern = "1"
        elif backend == BACKEND_DISTRO_GCP:
            unsupported_column_pattern = "2"
        elif backend == BACKEND_DISTRO_SNOWFLAKE:
            unsupported_column_pattern = "3"
        elif backend == BACKEND_DISTRO_MSAZURE:
            unsupported_column_pattern = "4"
        sql = f"""
        WITH unsupported_columns AS (
                SELECT ct.owner
                ,      ct.table_name
                ,      MIN(ct.unsupported_data_type) KEEP (
                        DENSE_RANK FIRST ORDER BY ct.unsupported_data_type)   AS unsupported_data_type
                ,      COUNT(ct.unsupported_data_type)                        AS unsupported_data_type_count
                ,      MIN(ct.unsupported_col_name_1)  KEEP (
                        DENSE_RANK FIRST ORDER BY ct.unsupported_col_name_1)  AS unsupported_col_name_1
                ,      COUNT(ct.unsupported_col_name_1)                       AS unsupported_col_name_1_count
                ,      MIN(ct.unsupported_col_name_2)  KEEP (
                        DENSE_RANK FIRST ORDER BY ct.unsupported_col_name_2)  AS unsupported_col_name_2
                ,      COUNT(ct.unsupported_col_name_2)                       AS unsupported_col_name_2_count
                ,      MIN(ct.unsupported_col_name_3)  KEEP (
                        DENSE_RANK FIRST ORDER BY ct.unsupported_col_name_3)  AS unsupported_col_name_3
                ,      COUNT(ct.unsupported_col_name_3)                       AS unsupported_col_name_3_count
                ,      MIN(ct.unsupported_col_name_4)  KEEP (
                        DENSE_RANK FIRST ORDER BY ct.unsupported_col_name_4)  AS unsupported_col_name_4
                ,      COUNT(ct.unsupported_col_name_4)                       AS unsupported_col_name_4_count
                FROM  (
                        SELECT /*+
                                    MATERIALIZE
                                    LEADING(tc vs)
                                    NO_MERGE(tc)
                                    USE_HASH(vs)
                                    SWAP_JOIN_INPUTS(vs)
                                */
                            DISTINCT
                            tc.owner
                        ,      tc.table_name
                        ,      CASE
                                WHEN REGEXP_SUBSTR(tc.data_type, '[^\\( ]+') NOT IN ('VARCHAR', 'VARCHAR2', 'NVARCHAR2',
                                                                                    'CHAR', 'NCHAR', 'NUMBER', 'FLOAT',
                                                                                    'BINARY_DOUBLE', 'BINARY_FLOAT',
                                                                                    'DATE', 'TIMESTAMP', 'INTERVAL',
                                                                                    'BLOB', 'CLOB', 'NCLOB', 'RAW')
                                THEN tc.data_type
                            END AS unsupported_data_type
                        ,      CASE
                                WHEN NOT REGEXP_LIKE(tc.column_name, '^[A-Za-z0-9_][A-Za-z0-9_]*$')
                                THEN tc.column_name
                            END AS unsupported_col_name_1
                        ,      CASE
                                WHEN NOT REGEXP_LIKE(tc.column_name, '^[A-Za-z_][A-Za-z0-9_]*$')
                                THEN tc.column_name
                            END AS unsupported_col_name_2
                        ,      CASE
                                WHEN NOT REGEXP_LIKE(tc.column_name, '^[A-Za-z_][A-Za-z0-9_\\$]*$')
                                THEN tc.column_name
                            END AS unsupported_col_name_3
                        ,      CASE
                                WHEN NOT REGEXP_LIKE(tc.column_name, '^[A-Za-z0-9_#@][A-Za-z_0-9#@\\$]*$')
                                THEN tc.column_name
                            END AS unsupported_col_name_4
                        FROM   dba_tab_cols        tc
                        WHERE  1=1
                        AND    tc.owner = UPPER( :schema_name)
                        AND    tc.hidden_column = 'NO'
                        AND   (   NOT REGEXP_LIKE(tc.column_name, '^[A-Za-z_][A-Za-z0-9_]*$')
                            OR REGEXP_SUBSTR(tc.data_type, '[^\\( ]+') NOT IN ('VARCHAR', 'VARCHAR2', 'NVARCHAR2',
                                                                                'CHAR', 'NCHAR', 'NUMBER', 'FLOAT',
                                                                                'BINARY_DOUBLE', 'BINARY_FLOAT',
                                                                                'DATE', 'TIMESTAMP', 'INTERVAL',
                                                                                'BLOB', 'CLOB', 'NCLOB', 'RAW'))
                    ) ct
                GROUP BY
                    ct.owner
                ,      ct.table_name
                )
        ,    part_tables AS (
                SELECT /*+
                            MATERIALIZE
                            LEADING(pt vs)
                            NO_MERGE(pt)
                            USE_HASH(vs pc tc)
                            SWAP_JOIN_INPUTS(vs)
                        */
                    pt.owner
                ,      pt.table_name
                ,      pt.partitioning_type
                ,      pt.subpartitioning_type
                FROM   dba_part_tables            pt
                WHERE  1=1
                AND    pt.owner = UPPER( :schema_name)
                )
        ,    tab_part_subpart AS (
                SELECT
                    /*+
                    materialize
                    no_merge(dt)
                    user_hash(dtp dts)
                    */
                    dt.owner                                       table_owner
                    , dt.table_name                                table_name
                    , dtp.partition_name                           partition_name
                    , dts.subpartition_name                        subpartition_name
                FROM
                    dba_tables dt
                LEFT OUTER JOIN
                    dba_tab_partitions dtp
                    ON (
                    dtp.table_owner = dt.owner
                    AND dtp.table_name = dt.table_name
                    )
                LEFT OUTER JOIN
                    dba_tab_subpartitions dts
                    ON (
                    dts.table_owner = dtp.table_owner
                    AND dts.table_name = dtp.table_name
                    AND dts.partition_name = dtp.partition_name
                    )
                WHERE
                    dt.owner = UPPER( :schema_name)
            )
        ,    segments AS (
                SELECT
                    /*+
                    materialize
                    no_merge(ds)
                    */
                    ds.owner
                    , ds.segment_name
                    , ds.partition_name
                    , ds.bytes
                FROM
                    dba_segments ds
                WHERE
                    ds.owner = UPPER( :schema_name)
            )
        ,    size_data AS (
                SELECT
                    t.table_owner   table_owner
                    , t.table_name  table_name
                    , sum(s.bytes)  table_size
                    , sum(op.bytes) table_offloaded_size
                FROM
                    tab_part_subpart t
                INNER JOIN
                    segments s
                    ON (
                    s.owner = t.table_owner
                    AND s.segment_name = t.table_name
                    AND COALESCE( s.partition_name, s.segment_name) = COALESCE( t.subpartition_name, t.partition_name, t.table_name)
                    )
                LEFT OUTER JOIN
                    {self._repo_user}.frontend_object fo
                    ON (
                    fo.database_name = t.table_owner
                    AND fo.object_name = t.table_name
                    )
                LEFT OUTER JOIN
                    {self._repo_user}.offload_partition op
                    ON (
                    op.frontend_object_id = fo.id
                    AND op.name = COALESCE( t.subpartition_name, t.partition_name, t.table_name)
                    )
                GROUP BY
                    t.table_owner, t.table_name
            )
        ,    reclaim_data AS (
                SELECT
                    /*+
                    materialize
                    */
                    fo.database_name   table_owner
                    , fo.object_name   table_name
                    , sum(op.bytes)    table_reclaimed_size
                FROM
                    {self._repo_user}.frontend_object fo
                INNER JOIN
                    {self._repo_user}.offload_partition op
                    ON (
                    op.frontend_object_id = fo.id
                    )
                WHERE NOT EXISTS(
                    SELECT 1
                        FROM tab_part_subpart t
                        WHERE fo.database_name = t.table_owner
                        AND   fo.object_name = t.table_name
                        AND   op.name = COALESCE( t.subpartition_name, t.partition_name, t.table_name)
                )
                GROUP BY
                    fo.database_name, fo.object_name
            )
        ,    table_data AS (
                SELECT /*+
                            MATERIALIZE
                            LEADING(t vs)
                            NO_MERGE(t)
                            USE_HASH(vs pt uc)
                            SWAP_JOIN_INPUTS(vs)
                        */
                    t.owner
                ,      t.table_name
                ,      sd.table_size
                ,      sd.table_offloaded_size
                ,      rd.table_reclaimed_size
                ,      t.num_rows table_stats_rows
                ,      t.last_analyzed table_stats_date
                ,      CASE
                        WHEN t.partitioned = 'YES' THEN NULL
                        ELSE t.compression
                    END table_compression
                ,      CASE
                        WHEN t.partitioned = 'YES' THEN NULL
                        ELSE t.compress_for
                    END table_compress_for
                ,      pt.partitioning_type
                ,      pt.subpartitioning_type
                ,      CASE
                        WHEN om.offloaded_owner IS NULL then 'False'
                        ELSE 'True'
                    END table_is_offloaded
                ,      CASE
                        WHEN t.cluster_name IS NOT NULL
                        THEN 1
                        WHEN t.iot_type = 'IOT'
                        THEN 2
                        WHEN t.iot_type = 'IOT_OVERFLOW'
                        THEN 3
                        WHEN t.secondary = 'Y'
                        THEN 4
                        WHEN t.nested = 'YES'
                        THEN 5
                        WHEN t.table_name LIKE 'GOE_ADV%'
                        THEN 6
                        WHEN EXISTS (SELECT NULL
                                        FROM   dba_mviews    mv
                                        WHERE  mv.owner      = t.owner
                                        AND    mv.mview_name = t.table_name)
                        THEN 7
                        WHEN EXISTS (SELECT NULL
                                        FROM   dba_mview_logs mvl
                                        WHERE  mvl.log_owner = t.owner
                                        AND    mvl.log_table = t.table_name)
                        THEN 8
                        ELSE 0
                    END AS table_type
                ,      uc.unsupported_data_type
                ,      uc.unsupported_data_type_count
                ,      uc.unsupported_col_name_1
                ,      uc.unsupported_col_name_1_count
                ,      uc.unsupported_col_name_2
                ,      uc.unsupported_col_name_2_count
                ,      uc.unsupported_col_name_3
                ,      uc.unsupported_col_name_3_count
                ,      uc.unsupported_col_name_4
                ,      uc.unsupported_col_name_4_count
                ,      uc.unsupported_col_name_{unsupported_column_pattern}       AS unsupported_col_name
                ,      uc.unsupported_col_name_{unsupported_column_pattern}_count AS unsupported_col_name_count
                FROM   dba_all_tables           t
                    INNER JOIN
                    size_data                sd
                    ON (    sd.table_owner   = t.owner
                        AND sd.table_name    = t.table_name)
                    LEFT OUTER JOIN
                    reclaim_data             rd
                    ON (    rd.table_owner   = t.owner
                        AND rd.table_name    = t.table_name)
                    LEFT OUTER JOIN
                    part_tables              pt
                    ON (    pt.owner         = t.owner
                        AND pt.table_name    = t.table_name)
                    LEFT OUTER JOIN
                    unsupported_columns      uc
                    ON (    uc.owner         = t.owner
                        AND uc.table_name    = t.table_name)
                    LEFT OUTER JOIN
                        {self._repo_user}.offload_metadata om
                    ON (    om.offloaded_owner              = t.owner
                        AND om.offloaded_table              = t.table_name
                        AND om.hybrid_view_type             = 'GOE_OFFLOAD_HYBRID_VIEW')
                WHERE  1=1
                AND    t.owner = UPPER( :schema_name)
                AND   (t.owner, t.table_name) NOT IN (SELECT et.owner, et.table_name FROM dba_external_tables et)
                AND    t.temporary = 'N'
                AND    t.dropped   = 'NO'
                )
        SELECT /*+ MONITOR */
              t.table_name                                                           AS table_name
            , t.table_size                                                           AS table_size_in_bytes
            , t.table_offloaded_size                                                 AS table_offloaded_size_in_bytes
            , t.table_reclaimed_size                                                 AS table_reclaimed_size_in_bytes
            , t.table_stats_rows                                                     AS estimated_row_count
            , t.table_stats_date                                                     AS statistics_last_gathered_on
            , t.partitioning_type                                                    AS partitioning_type
            , t.subpartitioning_type                                                 AS subpartitioning_type
            , t.table_compression                                                    AS table_compression
            , t.table_compress_for                                                   AS table_compress_for
            , t.table_is_offloaded                                                   AS is_offloaded
            , CASE
               WHEN t.partitioning_type IS NOT NULL
               THEN 'True'
               ELSE 'False'
              END                                                                    AS is_partitioned
            , CASE
               WHEN t.subpartitioning_type IS NOT NULL
               THEN decode( t.subpartitioning_type, 'NONE', 'False', 'True')
               ELSE 'False'
              END                                                                    AS is_subpartitioned
            , CASE
               WHEN t.table_compress_for IS NOT NULL
               THEN 'True'
               WHEN nvl(t.table_compression, 'DISABLED') != 'DISABLED'
               THEN 'True'
               ELSE 'False'
              END                                                                    AS is_compressed
            , CASE
               WHEN t.unsupported_data_type_count + t.unsupported_col_name_count > 0
               THEN 'False'
               WHEN t.table_type in (1, 3, 4, 5, 6, 7, 8)
               THEN 'False'
               ELSE 'True'
              END                                                                    AS is_offloadable
            , CASE t.table_type
               WHEN 1
               THEN 'Table In Cluster'
               WHEN 3
               THEN 'IOT Overflow'
               WHEN 4
               THEN 'Secondary Table'
               WHEN 5
               THEN 'Nested Table'
               WHEN 6
               THEN 'GOE Table'
               WHEN 7
               THEN 'Materialized View'
               WHEN 8
               THEN 'Materialized View Log'
               ELSE
                CASE
                 WHEN t.unsupported_data_type IS NOT NULL
                 THEN 'Column Types [' || t.unsupported_data_type ||
                      CASE
                       WHEN t.unsupported_data_type_count > 1
                       THEN ' +' || TO_CHAR(t.unsupported_data_type_count - 1) || ' more'
                      END || ']'
                 WHEN t.unsupported_col_name IS NOT NULL
                 THEN 'Column Names [' || t.unsupported_col_name ||
                      CASE
                       WHEN t.unsupported_col_name_count > 1
                       THEN ' +' || TO_CHAR(t.unsupported_col_name_count - 1) || ' more'
                      END || ']'
                END
              END                                                                    AS reason_not_offloadable
        FROM table_data t
        ORDER BY t.table_name
        """  # noqa: E501 W291

        return self._frontend_api.execute_query_fetch_all(
            sql,
            query_params={"schema_name": schema_name},
            as_dict=True,
            log_level=None,
        )

    def get_table_columns(self, schema_name, table_name):
        cols = self._frontend_api.get_columns(schema_name.upper(), table_name.upper())
        partition_columns = self._frontend_api.get_partition_columns(
            schema_name.upper(), table_name.upper()
        )
        subpartition_columns = self._frontend_api.get_subpartition_columns(
            schema_name.upper(), table_name.upper()
        )
        return_columns = [
            ColumnDetail(
                column_name=one_col.name,
                data_type=one_col.data_type,
                data_scale=one_col.data_precision,
                is_nullable=one_col.nullable,
                partition_position=None,
            )
            for one_col in cols
        ]
        part_pos = 1
        for part_col in partition_columns:
            for tab_col in return_columns:
                if tab_col.column_name == part_col.name:
                    tab_col.partition_position = part_pos
            part_pos += 1
        subpart_pos = 1
        for subpart_col in subpartition_columns:
            for tab_col in return_columns:
                if tab_col.column_name == subpart_col.name:
                    tab_col.subpartition_position = subpart_pos
            subpart_pos += 1
        return return_columns

    def get_table_partitions(self, schema_name, table_name):
        frontend_table = OffloadSourceTable.create(
            schema_name.upper(),
            table_name.upper(),
            self._connection_options,
            OffloadMessages(detail=QUIET),
            offload_by_subpartition=False,
        )
        try:
            table_partitions = frontend_table.get_partitions()
        except Exception as exc:
            logger.error(
                f"Table partition type is not supported: {exc.__class__.__qualname__}{exc.args}"
            )
        else:
            if table_partitions:
                return [
                    PartitionDetail.from_orm(table_partition).dict()
                    for table_partition in table_partitions
                ]

        return []

    def get_table_subpartitions(self, schema_name, table_name):
        frontend_table = OffloadSourceTable.create(
            schema_name.upper(),
            table_name.upper(),
            self._connection_options,
            OffloadMessages(detail=QUIET),
            offload_by_subpartition=False,
        )
        try:
            table_subpartitions = frontend_table.get_subpartitions()
        except Exception as exc:
            logger.error(
                f"Table subpartition type is not supported: {exc.__class__.__qualname__}{exc.args}"
            )
        else:
            if table_subpartitions:
                return [
                    SubPartitionDetail.from_orm(table_subpartition)
                    for table_subpartition in table_subpartitions
                ]
        return []

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
