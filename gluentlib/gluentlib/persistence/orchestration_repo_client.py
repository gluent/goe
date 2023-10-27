#! /usr/bin/env python3
""" OrchestrationRepoClientInterface: Base interface of API to interact with orchestration metadata repository.
    Each frontend/metadata system will have its own implementation.
    LICENSE_TEXT
"""

# Standard Library
import datetime
import decimal
import json
import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

# Gluent
from gluentlib.offload.factory.frontend_api_factory import frontend_api_factory
from gluentlib.offload.offload_messages import VVERBOSE
from gluentlib.offload.offload_metadata_functions import column_name_list_to_csv
from gluentlib.offload.offload_source_data import OffloadSourcePartition
from gluentlib.offload.predicate_offload import GenericPredicate
from gluentlib.orchestration import command_steps, orchestration_constants
from gluentlib.orchestration.execution_id import ExecutionId
from gluentlib.persistence.orchestration_metadata import (
    ALL_METADATA_ATTRIBUTES,
    CHANGELOG_SEQUENCE,
    CHANGELOG_TABLE,
    CHANGELOG_TRIGGER,
    EXTERNAL_TABLE,
    IU_EXTRACTION_METHOD,
    IU_EXTRACTION_SCN,
    IU_EXTRACTION_TIME,
    IU_KEY_COLUMNS,
    OBJECT_TYPE,
    UPDATABLE_TRIGGER,
    UPDATABLE_VIEW,
    OrchestrationMetadata,
)
from gluentlib.util.misc_functions import csv_split

if TYPE_CHECKING:
    from gluentlib.config.orchestration_config import OrchestrationConfig
    from gluentlib.offload.frontend_api import FrontendApiInterface
    from gluentlib.offload.offload_messages import OffloadMessages


###############################################################################
# CONSTANTS
###############################################################################

# Constants for table_name columns which do not match metadata keys
HYBRID_VIEW_TYPE = "HYBRID_VIEW_TYPE"
HYBRID_EXTERNAL_TABLE = "HYBRID_EXTERNAL_TABLE"
# And a dictionary to map the names
METADATA_KEY_TO_TABLE_COLUMN_MAPPING = {
    OBJECT_TYPE: HYBRID_VIEW_TYPE,
    EXTERNAL_TABLE: HYBRID_EXTERNAL_TABLE,
}

ALL_METADATA_TABLE_ATTRIBUTES = [
    METADATA_KEY_TO_TABLE_COLUMN_MAPPING.get(_, _) for _ in ALL_METADATA_ATTRIBUTES
]

INCREMENTAL_UPDATE_METADATA_ATTRIBUTES = [
    IU_KEY_COLUMNS,
    IU_EXTRACTION_METHOD,
    IU_EXTRACTION_SCN,
    IU_EXTRACTION_TIME,
    CHANGELOG_TABLE,
    CHANGELOG_TRIGGER,
    CHANGELOG_SEQUENCE,
    UPDATABLE_VIEW,
    UPDATABLE_TRIGGER,
]


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

    def __init__(self, connection_options: "OrchestrationConfig", messages: "OffloadMessages", dry_run: bool=False):
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
                trace_action=self.__class__.__name__,
            )
        return self._frontend_client

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

    def close(self):
        """Free up any resources currently held"""
        if self._frontend_client:
            self._frontend_client.close()

    #
    # OFFLOAD METADATA
    #
    @abstractmethod
    def get_offload_metadata(
        self, hybrid_owner: str, hybrid_name: str
    ) -> OrchestrationMetadata:
        """Return metadata object for owner/name"""

    @abstractmethod
    def set_offload_metadata(
        self, metadata: Union[dict, OrchestrationMetadata], execution_id: ExecutionId
    ):
        """Persist metadata"""

    @abstractmethod
    def drop_offload_metadata(self, hybrid_owner: str, hybrid_name: str):
        """Remove metadata"""

    #
    # INCREMENTAL UPDATE CONFIGURATION
    #
    def get_incremental_update_metadata(
        self, hybrid_owner: str, hybrid_name: str
    ) -> dict:
        """Return a dict of Incremental Update metadata for a hybrid view"""
        offload_metadata = self.get_offload_metadata(hybrid_owner, hybrid_name)
        iu_key_columns = (
            csv_split(offload_metadata.iu_key_columns)
            if offload_metadata.iu_key_columns
            else None
        )
        return {
            "gl_inc_lite_id_columns": iu_key_columns,
            "gl_inc_lite_extractor_flavor": offload_metadata.iu_extraction_method,
            "gl_inc_lite_source_table_owner": offload_metadata.offloaded_owner,
            "gl_inc_lite_source_table_name": offload_metadata.offloaded_table,
            "gl_inc_lite_base_table_owner": offload_metadata.backend_owner,
            "gl_inc_lite_current_base_table_name": offload_metadata.backend_table,
            "gl_inc_lite_changelog_owner": offload_metadata.hybrid_owner,
            "gl_inc_lite_changelog_table": offload_metadata.changelog_table,
            "gl_inc_lite_changelog_trigger": offload_metadata.changelog_trigger,
            "gl_inc_lite_changelog_sequence": offload_metadata.changelog_sequence,
            "gl_inc_lite_updatable_view": offload_metadata.updatable_view,
            "gl_inc_lite_updatable_trigger": offload_metadata.updatable_trigger,
        }

    def save_incremental_update_metadata(
        self,
        hybrid_owner: str,
        hybrid_name: str,
        incremental_update_metadata: dict,
        execution_id: ExecutionId,
    ):
        """Save Incremental Update metadata for a hybrid view"""
        assert isinstance(incremental_update_metadata, dict)
        assert isinstance(execution_id, ExecutionId)
        if incremental_update_metadata.get("gl_inc_lite_id_columns"):
            if isinstance(
                incremental_update_metadata.get("gl_inc_lite_id_columns"), list
            ):
                iu_key_columns_csv = column_name_list_to_csv(
                    incremental_update_metadata.get("gl_inc_lite_id_columns")
                )
            elif isinstance(
                incremental_update_metadata.get("gl_inc_lite_id_columns"), str
            ):
                iu_key_columns_csv = incremental_update_metadata.get(
                    "gl_inc_lite_id_columns"
                )
        else:
            iu_key_columns_csv = None

        offload_metadata = self.get_offload_metadata(hybrid_owner, hybrid_name)
        offload_metadata.iu_key_columns = iu_key_columns_csv
        offload_metadata.iu_extraction_method = incremental_update_metadata.get(
            "gl_inc_lite_extractor_flavor"
        )
        offload_metadata.iu_extraction_scn = None
        offload_metadata.iu_extraction_time = None
        offload_metadata.changelog_table = incremental_update_metadata.get(
            "gl_inc_lite_changelog_table"
        )
        offload_metadata.changelog_trigger = incremental_update_metadata.get(
            "gl_inc_lite_changelog_trigger"
        )
        offload_metadata.changelog_sequence = incremental_update_metadata.get(
            "gl_inc_lite_changelog_sequence"
        )
        offload_metadata.updatable_view = incremental_update_metadata.get(
            "gl_inc_lite_updatable_view"
        )
        offload_metadata.updatable_trigger = incremental_update_metadata.get(
            "gl_inc_lite_updatable_trigger"
        )
        offload_metadata.save(execution_id)

    #
    # INCREMENTAL UPDATE EXTRACTION METADATA
    #
    def get_incremental_update_extraction_metadata(
        self, hybrid_owner: str, hybrid_name: str
    ) -> dict:
        """Get Incremental Update extraction metadata for a hybrid view"""
        offload_metadata = self.get_offload_metadata(hybrid_owner, hybrid_name)
        return {
            "last_scn": offload_metadata.iu_extraction_scn,
            "ts": offload_metadata.iu_extraction_time,
        }

    def save_incremental_update_extraction_metadata(
        self,
        hybrid_owner: str,
        hybrid_name: str,
        extraction_metadata: dict,
        execution_id: ExecutionId,
    ):
        """Save Incremental Update extraction metadata for a hybrid view"""
        assert isinstance(extraction_metadata, dict)
        assert isinstance(execution_id, ExecutionId)
        offload_metadata = self.get_offload_metadata(hybrid_owner, hybrid_name)
        offload_metadata.iu_extraction_scn = int(extraction_metadata["last_scn"])
        offload_metadata.iu_extraction_time = int(extraction_metadata["ts"])
        offload_metadata.save(execution_id)

    def delete_incremental_update_metadata(
        self, hybrid_owner: str, hybrid_name: str, execution_id: ExecutionId
    ):
        """Save Incremental Update metadata for a hybrid view"""
        incremental_update_metadata = {
            k: None for k in INCREMENTAL_UPDATE_METADATA_ATTRIBUTES
        }
        self.save_incremental_update_metadata(
            hybrid_owner, hybrid_name, incremental_update_metadata, execution_id
        )

    #
    # COMMAND EXECUTION LOGGING METHODS
    #
    @abstractmethod
    def start_command(
        self,
        execution_id: ExecutionId,
        command_type: str,
        log_path: str,
        command_input: Union[str, dict],
        parameters: Union[dict, None],
    ) -> int:
        """
        Record the start of an orchestration command in the repo.
        Returns the numeric identifier of the history record.
        execution_id: The UUID identifying the command execution.
        command_type: Valid code from GLUENT_REPO.COMMAND_TYPES.
        command_input: Command line or API JSON. The user provided input.
        parameters: Full set of operational parameters as a dictionary.
        """

    @abstractmethod
    def end_command(self, command_execution_id: int, status: str) -> None:
        """
        Record the end of an orchestration command along with its status.
        command_execution_id: The identifier returned from start_command, not the higher level execution id.
        status: Valid status code from GLUENT_REPO.STATUS table.
        """

    @abstractmethod
    def start_command_step(
        self, execution_id: ExecutionId, command_type: str, command_step: str
    ) -> int:
        """
        Record the start of a discrete step of an orchestration command in the repo.
        Returns the numeric identifier of the history record for use in end_command_step().
        execution_id: The UUID identifying the command execution.
        command_type: Valid code from GLUENT_REPO.COMMAND_TYPE.
        command_step: Valid code from GLUENT_REPO.COMMAND_STEP.
        """

    @abstractmethod
    def end_command_step(
        self, command_step_id: int, status: str, step_details: Union[dict, None] = None
    ) -> None:
        """
        Record the end of an orchestration command step along with its status.
        command_step_id: The identifier returned from start_command_step.
        step_details: A dictionary of key/value pairs used to record step outcomes.
        status: Valid status code from GLUENT_REPO.STATUS table.
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
        """Returns a dict of all schemas in the database (excluding Gluent-created ones)
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