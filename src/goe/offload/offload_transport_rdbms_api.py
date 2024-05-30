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

""" OffloadTransportRdbmsApi: Library for logic/interaction with source RDBMS during offload transport
"""

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING

from goe.offload.offload_constants import OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED
from goe.offload.offload_transport_functions import ssh_cmd_prefix
from goe.util.misc_functions import ansi_c_string_safe

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.column_metadata import ColumnMetadataInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.offload.offload_source_data import OffloadSourcePartitions
    from goe.offload.offload_source_table import OffloadSourceTableInterface


class OffloadTransportRdbmsApiException(Exception):
    pass


###########################################################################
# CONSTANTS
###########################################################################

OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE = "Transport SQL Statistics"

TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION = "partition"
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION = "subpartition"
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT = "extent"
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD = "mod"
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE = "id_range"
# e.g. Spark native fetch partitioning.
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE = "native_range"
# TODO Should this have a generic name, e.g. _SYSTEM_SHARD?
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_AMP = "amp"
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP = "hashamp"
TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN = "goe_offload_batch"

TRANSPORT_ROW_SOURCE_QUERY_SPLIT_TYPE_TEXT = "Transport rowsource split type: "


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# INTERFACE
###########################################################################


class OffloadTransportRdbmsApiInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for DB specific sub-classes"""

    def __init__(
        self,
        rdbms_owner: str,
        rdbms_table_name: str,
        offload_options: "OrchestrationConfig",
        messages: "OffloadMessages",
        dry_run=False,
    ):
        self._rdbms_owner = rdbms_owner
        self._rdbms_table_name = rdbms_table_name
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = dry_run
        self._offload_transport_dsn = offload_options.offload_transport_dsn
        self._rdbms_adm_dsn = offload_options.rdbms_dsn
        self._offload_transport_rdbms_session_parameters = (
            offload_options.offload_transport_rdbms_session_parameters
        )
        self._rdbms_adm_conn = None
        self._rdbms_app_conn = None
        self._fixed_goe_parameters = None
        self._app_user = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _row_source_query_union_all_clause(self, pad):
        if pad is None:
            # All on one line, Sqoop needs this
            return " UNION ALL "
        else:
            crlf = "\n" + (" " * (pad or 0))
            return crlf + "UNION ALL" + crlf

    def _ssh_cli_safe_value(self, cmd_option_string):
        """If we are invoking Sqoop via SSH then we need extra protection for special characters on the command line
        Without SSH the protection added by subprocess module is sufficient
        """
        if self._ssh_cmd_prefix():
            return ansi_c_string_safe(cmd_option_string)
        else:
            return cmd_option_string

    def _ssh_cmd_prefix(self, host=None):
        """Many calls to ssh_cmd_prefix() in this class use std inputs therefore abstract in this method"""
        host = host or self._offload_options.offload_transport_cmd_host
        return ssh_cmd_prefix(self._offload_options.offload_transport_user, host)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def log(self, msg, detail=None):
        self._messages.log(msg, detail=detail)

    def debug(self, msg):
        self._messages.debug(msg)

    def convert_query_import_expressions_on_rdbms_side(self):
        """By default we TO_CHAR columns on the RDBMS side before extracting to Python, where things
        get much trickier to control.
        Oracle implementation has an override.
        """
        return True

    @abstractmethod
    def generate_transport_action(self):
        """Generate a unique(ish) string that can be used to track an RDBMS session"""

    @abstractmethod
    def get_id_column_for_range_splitting(
        self, rdbms_table: "OffloadSourceTableInterface"
    ) -> "ColumnMetadataInterface":
        pass

    @abstractmethod
    def get_id_range(
        self, rdbms_col_name: str, predicate_offload_clause: str, partition_chunk=None
    ) -> tuple:
        """Function to get the MIN and MAX values for an id column"""

    @abstractmethod
    def get_rdbms_query_cast(
        self,
        column_expression,
        rdbms_column,
        staging_column,
        max_ts_scale,
        convert_expressions_on_rdbms_side=False,
        for_spark=False,
        nan_values_as_null=False,
    ):
        """Return an expression suitable for reading a specific column from the RDBMS table.
        Takes column_expression as an input rather than a column name because a transformation may already have
        taken place.
        for_spark: Convert any data types that Spark cannot process in the DB before extracting.
        convert_expressions_on_rdbms_side: Used to convert DATEs, TIMESTAMPs and NUMBERs to character in
                                           the DB, not in the extracting language.
        """

    @abstractmethod
    def get_rdbms_session_setup_commands(
        self,
        module,
        action,
        custom_session_parameters,
        include_fixed_sqoop=True,
        for_plsql=False,
        include_semi_colons=False,
        escape_semi_colons=False,
        max_ts_scale=None,
    ) -> list:
        pass

    @abstractmethod
    def get_rdbms_session_setup_hint(self, custom_session_parameters, max_ts_scale):
        pass

    @abstractmethod
    def get_rdbms_scn(self):
        pass

    @abstractmethod
    def get_transport_split_type(
        self,
        partition_chunk: "OffloadSourcePartitions",
        rdbms_table: "OffloadSourceTableInterface",
        parallelism: int,
        rdbms_partition_type: str,
        offload_by_subpartition: bool,
        predicate_offload_clause: str,
        native_range_split_available: bool = False,
    ) -> tuple:
        """Return a TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_* constant stating the best splitter method for the source."""

    @abstractmethod
    def get_transport_row_source_query(
        self,
        partition_by_prm: str,
        rdbms_table: "OffloadSourceTableInterface",
        consistent_read,
        parallelism,
        offload_by_subpartition,
        mod_column,
        predicate_offload_clause: str,
        partition_chunk: "OffloadSourcePartitions" = None,
        pad: int = None,
        id_col_min=None,
        id_col_max=None,
    ) -> str:
        """Define a frontend query that will retrieve data from a table dividing it by a split method
        such as partition or rowid range.
        """

    @abstractmethod
    def get_transport_row_source_query_hint_block(self) -> str:
        """Return a hint block for the row source query"""

    @abstractmethod
    def jdbc_driver_name(self) -> str:
        """Return the name of the JDBC driver name to be used for extraction"""

    @abstractmethod
    def jdbc_url(self) -> str:
        pass

    @abstractmethod
    def log_sql_stats(
        self,
        module,
        action,
        payload,
        validation_polling_interval=OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
    ):
        pass

    @abstractmethod
    def get_rdbms_canary_query(self):
        pass

    @contextmanager
    @abstractmethod
    def query_import_extraction(
        self,
        staging_columns,
        source_query,
        source_binds,
        fetch_size,
        compress,
        rdbms_session_setup_commands,
    ):
        """Context manager that logs into a frontend system, executes an extraction query and then returns
        a cursor for Query Import to fetch from and write data to a staging file.
        """

    def sqoop_by_query_boundary_query(self, offload_transport_parallelism):
        """Return a query providing a value range for Sqoop --boundary-query option.
        Oracle has it's own override.
        """
        return '"SELECT 0, {}"'.format(offload_transport_parallelism - 1)

    @abstractmethod
    def sqoop_rdbms_specific_jvm_overrides(self, rdbms_session_setup_commands) -> list:
        """Return a list of Sqoop -D JVM overrides specific to the frontend RDBMS"""

    @abstractmethod
    def sqoop_rdbms_specific_jvm_table_options(
        self, partition_chunk, partition_type=None, is_iot=False, consistent_read=False
    ) -> list:
        """Return a list of Sqoop -D JVM overrides specific to the frontend table"""

    @abstractmethod
    def sqoop_rdbms_specific_options(self) -> list:
        """Return a list of Sqoop options specific to the frontend RDBMS"""

    @abstractmethod
    def sqoop_rdbms_specific_table_options(self, rdbms_owner, table_name) -> list:
        """Return a list of Sqoop options specific to the frontend table"""
