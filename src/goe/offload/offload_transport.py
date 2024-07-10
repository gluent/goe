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

""" OffloadTransport: Library for offloading data from an RDBMS frontend to a cloud backend.
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime
import json
import logging
import os
import re
from socket import gethostname
from textwrap import dedent
import traceback
from typing import Optional, Union, TYPE_CHECKING

from goe.config import orchestration_defaults
from goe.offload.column_metadata import match_table_column
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.factory.query_import_factory import query_import_factory
from goe.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)
from goe.offload.factory.staging_file_factory import staging_file_factory
from goe.offload.frontend_api import FRONTEND_TRACE_MODULE
from goe.offload.offload_constants import (
    BACKEND_DISTRO_GCP,
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_SPARK,
    DBTYPE_SYNAPSE,
    DBTYPE_TERADATA,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
    OFFLOAD_TRANSPORT_AUTO,
    OFFLOAD_TRANSPORT_GOE,
    OFFLOAD_TRANSPORT_GCP,
    OFFLOAD_TRANSPORT_SQOOP,
    OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_TIMESTAMP_TZ,
    ORACLE_TYPE_INTERVAL_YM,
    ORACLE_TYPE_XMLTYPE,
)
from goe.offload.offload_transport_functions import (
    credential_provider_path_jvm_override,
    hs2_connection_log_message,
    run_os_cmd,
    running_as_same_user_and_host,
    scp_to_cmd,
    ssh_cmd_prefix,
    get_local_staging_path,
)
from goe.offload.offload_transport_rdbms_api import (
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
)
from goe.offload.offload_xform_functions import apply_transformation
from goe.offload.operation.data_type_controls import char_semantics_override_map
from goe.offload.spark.pyspark_literal import PysparkLiteral
from goe.orchestration import command_steps

from goe.filesystem.goe_dfs import DFS_TYPE_FILE

from goe.util.misc_functions import (
    ansi_c_string_safe,
    bytes_to_human_size,
    get_os_username,
    get_temp_path,
    id_generator,
    write_temp_file,
)
from goe.util.polling_thread import PollingThread

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.backend_table import BackendTableInterface
    from goe.offload.column_metadata import ColumnMetadataInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.offload.offload_source_data import OffloadSourcePartitions
    from goe.offload.offload_source_table import OffloadSourceTableInterface


class OffloadTransportException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# The specific methods used to transport data
OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT = "QUERY_IMPORT"
OFFLOAD_TRANSPORT_METHOD_SQOOP = "SQOOP"
OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY = "SQOOP_BY_QUERY"
OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT = "SPARK_THRIFT"
OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT = "SPARK_SUBMIT"
OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY = "SPARK_LIVY"
OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD = "SPARK_DATAPROC_GCLOUD"
OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD = "SPARK_BATCHES_GCLOUD"
VALID_OFFLOAD_TRANSPORT_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
]
OFFLOAD_TRANSPORT_SPARK_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
]

# The rules that contain specific transport methods

VALID_OFFLOAD_TRANSPORTS = [
    OFFLOAD_TRANSPORT_AUTO,
    OFFLOAD_TRANSPORT_GOE,
    OFFLOAD_TRANSPORT_GCP,
    OFFLOAD_TRANSPORT_SQOOP,
]
# Transport methods for the GOE rule set
OFFLOAD_TRANSPORT_GOE_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
]
# Transport methods for the Sqoop rule set
OFFLOAD_TRANSPORT_SQOOP_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
]
# Transport methods for the GCP rule set
OFFLOAD_TRANSPORT_GCP_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
]

YARN_TRANSPORT_NAME = "GOE"

MISSING_ROWS_IMPORTED_WARNING = "Unable to identify import record count"

# Spark constants
SPARK_OPTIONS_FILE_PREFIX = "goe-spark-options-"

SPARK_THRIFT_SERVER_PROPERTY_EXCLUDE_LIST = [
    "spark.extraListeners",
    "spark.driver.memory",
    "spark.executor.memory",
]

SPARK_SUBMIT_SPARK_YARN_MASTER = "yarn"
VALID_SPARK_SUBMIT_EXECUTABLES = ["spark-submit", "spark2-submit"]
MISSING_ROWS_SPARK_WARNING = "Import record count not identified from Spark"
OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE = "gcloud"

# Used for test assertions
POLLING_VALIDATION_TEXT = (
    "Calculating offload transport source row counts from %s snapshots"
)

# Used for scraping Spark log messages to find transport row count
# Example logger messages:
# 21/08/16 14:07:25 INFO GOETaskListener: {"taskInfo.id":"1.0","taskInfo.taskId":1,"taskInfo.launchTime":1629122840262,"taskInfo.finishTime":1629122845656,"duration":5394,"recordsWritten":0,"executorRunTime":3952}
# 21/08/16 14:07:27 INFO GOETaskListener: {"taskInfo.id":"0.0","taskInfo.taskId":0,"taskInfo.launchTime":1629122840224,"taskInfo.finishTime":1629122847797,"duration":7573,"recordsWritten":145,"executorRunTime":6191}
SPARK_LOG_ROW_COUNT_PATTERN = r'^.* GOETaskListener: .*"recordsWritten":(\d+).*}\r?$'

GOE_LISTENER_NAME = "GOETaskListener"
GOE_LISTENER_JAR = "goe-spark-listener.jar"

TRANSPORT_CXT_BYTES = "staged_bytes"
TRANSPORT_CXT_ROWS = "staged_rows"

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def spark_submit_executable_exists(
    config: "OrchestrationConfig", messages: "OffloadMessages", executable_override=None
):
    spark_submit_executable = (
        executable_override or config.offload_transport_spark_submit_executable
    )
    cmd = ["which", spark_submit_executable]
    cmd = (
        ssh_cmd_prefix(
            config.offload_transport_user,
            config.offload_transport_cmd_host,
        )
        + cmd
    )
    rc, _ = run_os_cmd(cmd, messages, config, optional=True, force=True, silent=True)
    return bool(rc == 0)


def is_spark_thrift_available(
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    messages: "OffloadMessages" = None,
):
    """If messages is passed in then we'll log any reason for a False return"""
    if (
        not config.offload_transport_spark_thrift_host
        or not config.offload_transport_spark_thrift_port
    ):
        if messages:
            messages.log(
                "OFFLOAD_TRANSPORT_SPARK_THRIFT_HOST and OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
                detail=VVERBOSE,
            )
        return False
    else:
        return True


def is_livy_available(
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    messages: "OffloadMessages" = None,
):
    """If messages is passed in then we'll log any reason for a False return"""
    if not config.offload_transport_livy_api_url:
        (
            messages.log(
                "OFFLOAD_TRANSPORT_LIVY_API_URL required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    else:
        return True


def is_spark_submit_available(
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    messages: "OffloadMessages" = None,
    executable_override=None,
):
    """If messages is passed in then we'll log any reason for a False return
    If messages is not passed in then we can't yet run OS cmds, hence no spark_submit_executable_exists()
    """
    if not config.offload_transport_cmd_host:
        (
            messages.log(
                "OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    elif not config.offload_transport_spark_submit_executable:
        (
            messages.log(
                "OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    elif messages and not spark_submit_executable_exists(
        config, messages, executable_override=executable_override
    ):
        messages.log(
            "Spark submit executable required for transport method: %s"
            % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
            detail=VVERBOSE,
        )
        return False
    else:
        return True


def is_spark_gcloud_available(
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    messages: "OffloadMessages" = None,
):
    """If messages is passed in then we'll log any reason for a False return
    If messages is not passed in then we can't yet run OS cmds, hence no spark_submit_executable_exists()
    """
    if not config.offload_transport_cmd_host:
        (
            messages.log(
                "OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    if config.backend_distribution != BACKEND_DISTRO_GCP:
        (
            messages.log(
                "BACKEND_DISTRIBUTION not valid for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    elif messages and not spark_submit_executable_exists(
        config,
        messages,
        executable_override=OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
    ):
        messages.log(
            "gcloud executable required for transport method: %s"
            % OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
            detail=VVERBOSE,
        )
        return False
    else:
        return True


def is_spark_gcloud_dataproc_available(
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    messages: "OffloadMessages" = None,
):
    return bool(
        config.google_dataproc_cluster
        and is_spark_gcloud_available(config, offload_source_table, messages=messages)
    )


def is_spark_gcloud_batches_available(
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    messages: "OffloadMessages" = None,
):
    return bool(
        config.google_dataproc_batches_version
        and is_spark_gcloud_available(config, offload_source_table, messages=messages)
    )


def is_query_import_available(
    offload_operation,
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface" = None,
    messages: "OffloadMessages" = None,
):
    """If messages is passed in then we'll log any reason for a False return
    Query Import can only be validated if operational config is passed in. Without that we just assume True
    """

    def log(msg, messages):
        if messages:
            messages.log(msg, detail=VVERBOSE)

    if config.offload_staging_format not in [
        FILE_STORAGE_FORMAT_AVRO,
        FILE_STORAGE_FORMAT_PARQUET,
    ]:
        log(
            "Staging file format %s not supported by transport method: %s"
            % (
                config.offload_staging_format,
                OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
            ),
            messages,
        )
        return False

    if offload_source_table:
        if (
            offload_operation
            and (offload_source_table.size_in_bytes or 0)
            > offload_operation.offload_transport_small_table_threshold
        ):
            log(
                "Table size (%s) > %s (OFFLOAD_TRANSPORT_SMALL_TABLE_THRESHOLD) is not valid with transport method: %s"
                % (
                    offload_source_table.size_in_bytes or 0,
                    offload_operation.offload_transport_small_table_threshold,
                    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
                ),
                messages,
            )
            return False

    return True


def is_sqoop_available(
    offload_operation,
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface" = None,
    messages: "OffloadMessages" = None,
):
    """If messages is passed in then we'll log any reason for a False return
    Sqoop can only be validated if operational config is passed in. Without that we just assume True
    """
    if (
        config.backend_distribution
        and config.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS
    ):
        if messages:
            messages.log(
                "Transport method only valid on Hadoop systems: %s/%s"
                % (
                    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                    config.backend_distribution,
                ),
                detail=VVERBOSE,
            )
        return False
    elif not config.offload_transport_cmd_host:
        if messages:
            messages.log(
                "OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                detail=VVERBOSE,
            )
        return False
    elif offload_operation and offload_operation.offload_by_subpartition:
        if messages:
            messages.log(
                "Subpartition level offloads are not valid with transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                detail=VVERBOSE,
            )
        return False
    elif offload_operation and offload_operation.column_transformations:
        if messages:
            messages.log(
                "Column transformations are not valid with transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                detail=VVERBOSE,
            )
        return False
    elif offload_operation and offload_operation.offload_predicate:
        if messages:
            messages.log(
                "Predicate offloads are not valid with transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                detail=VVERBOSE,
            )
        return False
    elif (
        offload_source_table
        and ORACLE_TYPE_TIMESTAMP_TZ in offload_source_table.data_types_in_use()
    ):
        if messages:
            messages.log(
                "TIMESTAMP WITH TIME ZONE offloads are not valid with transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                detail=VVERBOSE,
            )
        return False
    elif (
        offload_source_table
        and ORACLE_TYPE_XMLTYPE in offload_source_table.data_types_in_use()
    ):
        if messages:
            messages.log(
                "XMLTYPE offloads are not valid with transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                detail=VVERBOSE,
            )
        return False
    elif not offload_operation:
        return True
    else:
        return True


def is_sqoop_by_query_available(
    config: "OrchestrationConfig", messages: "OffloadMessages" = None
):
    """If messages is passed in then we'll log any reason for a False return"""
    if (
        config.backend_distribution
        and config.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS
    ):
        (
            messages.log(
                "Transport method only valid on Hadoop systems: %s/%s"
                % (
                    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                    config.backend_distribution,
                ),
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    elif not config.offload_transport_cmd_host:
        (
            messages.log(
                "OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s"
                % OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                detail=VVERBOSE,
            )
            if messages
            else None
        )
        return False
    else:
        return True


def get_offload_transport_method_validation_fns(
    config: "OrchestrationConfig",
    offload_operation=None,
    offload_source_table: "OffloadSourceTableInterface" = None,
    messages: "OffloadMessages" = None,
):
    return {
        OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT: lambda: is_spark_thrift_available(
            config, offload_source_table, messages=messages
        ),
        OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY: lambda: is_livy_available(
            config, offload_source_table, messages=messages
        ),
        OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT: lambda: is_spark_submit_available(
            config, offload_source_table, messages=messages
        ),
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD: lambda: is_spark_gcloud_batches_available(
            config, offload_source_table, messages=messages
        ),
        OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD: lambda: is_spark_gcloud_dataproc_available(
            config, offload_source_table, messages=messages
        ),
        OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT: lambda: is_query_import_available(
            offload_operation,
            config,
            offload_source_table=offload_source_table,
            messages=messages,
        ),
        OFFLOAD_TRANSPORT_METHOD_SQOOP: lambda: is_sqoop_available(
            offload_operation,
            config,
            offload_source_table=offload_source_table,
            messages=messages,
        ),
        OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY: lambda: is_sqoop_by_query_available(
            config, messages=messages
        ),
    }


def validate_offload_transport_method(
    offload_transport_method: str,
    config: "OrchestrationConfig",
    offload_operation=None,
    offload_source_table: "OffloadSourceTableInterface" = None,
    messages: "OffloadMessages" = None,
    exception_class=OffloadTransportException,
):
    """If a specific method is requested then we can do certain config checks.
    This is not a health check, just confirming we have the config we need.
    Query Import and Sqoop methods can only be validated if optional operation config is passed in.
    """
    assert offload_transport_method
    assert exception_class

    if offload_transport_method not in VALID_OFFLOAD_TRANSPORT_METHODS:
        raise exception_class(
            "Unsupported transport method: %s" % offload_transport_method
        )

    validation_fn = get_offload_transport_method_validation_fns(
        config, offload_operation, offload_source_table, messages
    )[offload_transport_method]

    if not validation_fn():
        raise exception_class(
            "Transport method is incompatible with this offload: %s"
            % offload_transport_method
        )


def choose_offload_transport_method(
    offload_operation,
    offload_source_table: "OffloadSourceTableInterface",
    config: "OrchestrationConfig",
    messages: "OffloadMessages",
):
    """Select the data transport method based on a series of rules:
    AUTO:
        Work through GOE rules and then fall back to SQOOP rules
    GOE:
        Pick a method using the following priorities: SPARK_THRIFT, QUERY_IMPORT, SPARK_LIVY, SPARK_SUBMIT
    GCP:
        Pick a method using the following priorities: SPARK_DATAPROC_GCLOUD, SPARK_BATCHES_GCLOUD
    SQOOP:
        Pick a method using the following priorities: SQOOP, SQOOP_BY_QUERY
    """
    assert config.offload_transport

    validation_fns = get_offload_transport_method_validation_fns(
        config, offload_operation, offload_source_table, messages
    )

    if config.db_type in [DBTYPE_ORACLE, DBTYPE_TERADATA]:
        if offload_operation.offload_transport_method:
            # An override was supplied
            rules = [offload_operation.offload_transport_method]
        elif config.offload_transport == OFFLOAD_TRANSPORT_AUTO:
            rules = (
                OFFLOAD_TRANSPORT_GOE_METHODS
                + OFFLOAD_TRANSPORT_SQOOP_METHODS
                + OFFLOAD_TRANSPORT_GCP_METHODS
            )
        elif config.offload_transport == OFFLOAD_TRANSPORT_GOE:
            rules = OFFLOAD_TRANSPORT_GOE_METHODS
        elif config.offload_transport == OFFLOAD_TRANSPORT_SQOOP:
            rules = OFFLOAD_TRANSPORT_SQOOP_METHODS
        elif config.offload_transport == OFFLOAD_TRANSPORT_GCP:
            rules = OFFLOAD_TRANSPORT_GCP_METHODS

        for rule in rules:
            if validation_fns[rule]():
                return rule

        # if we get to here then no rule was accepted
        raise OffloadTransportException(
            "No valid transport methods found for offload transport: %s"
            % config.offload_transport
        )
    elif config.db_type == DBTYPE_MSSQL:
        return OFFLOAD_TRANSPORT_METHOD_SQOOP
    else:
        raise OffloadTransportException("Unsupported DB type: %s" % config.db_type)


def derive_rest_api_verify_value_from_url(url):
    """Define whether to verify SSL certificates:
    False: Use SSL but no verification
    None: No SSL
    """
    if url and url[:6] == "https:":
        return False
    else:
        return None


def convert_nans_to_nulls(offload_target_table, offload_operation):
    """If the backend does not support NaN values and the --allow-floating-point-conversions parameter is set,
    we must convert any 'NaN','Infinity','-Infinity' values in RDBMS nan capable columns to NULL
    """
    return bool(
        not offload_target_table.nan_supported()
        and offload_operation.allow_floating_point_conversions
    )


def get_offload_transport_value(
    offload_transport_method: str,
    dedicated_value,
    sqoop_specific_value,
    spark_specific_value,
):
    if dedicated_value:
        return dedicated_value
    elif (
        offload_transport_method in OFFLOAD_TRANSPORT_SQOOP_METHODS
        and sqoop_specific_value
    ):
        return sqoop_specific_value
    elif (
        offload_transport_method in OFFLOAD_TRANSPORT_SPARK_METHODS
        and spark_specific_value
    ):
        return spark_specific_value
    else:
        return None


class OffloadTransport(object, metaclass=ABCMeta):
    """Interface for classes transporting data from an RDBMS frontend to storage that can be accessed by a backend.
    Overloads by different transport methods, such a Spark, Sqoop, etc
    (abstract parent class)
    """

    def __init__(
        self,
        offload_source_table: "OffloadSourceTableInterface",
        offload_target_table: "BackendTableInterface",
        offload_operation,
        offload_options,
        messages: "OffloadMessages",
        dfs_client,
        rdbms_columns_override=None,
    ):
        # not decided how to deal with offload_options, below is a temporary measure
        self._offload_options = offload_options
        self._messages = messages
        self._dfs_client = dfs_client
        self._backend_dfs = self._dfs_client.backend_dfs
        self._dry_run = bool(not offload_operation.execute)
        # Details of the source of the offload
        self._rdbms_owner = offload_source_table.owner
        self._rdbms_table_name = offload_source_table.table_name
        self._rdbms_columns = rdbms_columns_override or offload_source_table.columns
        self._rdbms_is_iot = offload_source_table.is_iot()
        self._rdbms_partition_type = offload_source_table.partition_type
        self._rdbms_pk_cols = offload_source_table.get_primary_key_columns()
        self._fetchmany_takes_fetch_size = (
            offload_source_table.fetchmany_takes_fetch_size()
        )
        self._offload_by_subpartition = offload_operation.offload_by_subpartition
        self._rdbms_table = offload_source_table
        self._rdbms_offload_predicate = offload_operation.inflight_offload_predicate
        self._canonical_columns = None
        # Details of the target of the offload
        self._target = offload_options.target
        self._target_owner = offload_operation.target_owner
        self._target_db_name = offload_target_table.db_name
        self._target_table_name = offload_target_table.table_name
        self._target_table = offload_target_table
        self._staging_table_location = self._target_table.get_staging_table_location()
        # Config relating to the transport method
        self._offload_transport_consistent_read = (
            offload_operation.offload_transport_consistent_read
        )
        self._offload_transport_fetch_size = (
            offload_operation.offload_transport_fetch_size
        )
        self._offload_transport_queue_name = get_offload_transport_value(
            self._offload_transport_method,
            offload_operation.offload_transport_queue_name,
            offload_options.sqoop_queue_name,
            offload_options.offload_transport_spark_queue_name,
        )
        self._offload_transport_jvm_overrides = get_offload_transport_value(
            self._offload_transport_method,
            offload_operation.offload_transport_jvm_overrides,
            offload_options.sqoop_overrides,
            offload_options.offload_transport_spark_overrides,
        )
        self._offload_transport_parallelism = int(
            offload_operation.offload_transport_parallelism
        )
        self._validation_polling_interval = (
            offload_operation.offload_transport_validation_polling_interval
        )
        self._create_basic_connectivity_attributes(offload_options)
        self._transport_context = {}
        # For a full list of properties see:
        #   http://spark.apache.org/docs/latest/configuration.html#available-properties
        self._spark_config_properties = self._prepare_spark_config_properties(
            offload_operation.offload_transport_spark_properties
        )
        self._bucket_hash_col = offload_operation.bucket_hash_col
        self._column_transformations = {}
        self._unicode_string_columns_csv = offload_operation.unicode_string_columns_csv
        self._convert_nans_to_nulls = False
        # Details relating to a load table
        self._compress_load_table = offload_operation.compress_load_table
        self._preserve_load_table = offload_operation.preserve_load_table
        self._compute_load_table_stats = offload_operation.compute_load_table_stats
        self._load_db_name = self._target_table.get_load_db_name()
        self._load_table_name = self._target_table.table_name
        self._staging_format = offload_options.offload_staging_format
        self._staging_file = staging_file_factory(
            self._load_db_name,
            self._load_table_name,
            self._staging_format,
            self._get_canonical_columns(),
            offload_options,
            bool(
                self._target_table
                and self._target_table.transport_binary_data_in_base64()
            ),
            messages,
            dry_run=self._dry_run,
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(
            self._rdbms_owner,
            self._rdbms_table_name,
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _base64_staged_columns(self):
        """Return a list of columns that should be staged as Base64.
        We drive this from the canonical type being binary because the staging schema will be string if
        we are staging in base64, which is exactly what we are doing here.
        """
        if self._target_table and self._target_table.transport_binary_data_in_base64():
            base64_columns = []
            staging_columns = self._staging_file.get_staging_columns()
            for column in [_ for _ in self._get_canonical_columns() if _.is_binary()]:
                staging_column = match_table_column(column.name, staging_columns)
                base64_columns.append(staging_column)
            return base64_columns
        else:
            return []

    def _get_canonical_columns(self):
        """Map source RDBMS columns to canonical columns so they can then be converted to staging columns"""
        if not self._canonical_columns:
            assume_all_staging_columns_nullable = bool(
                self._offload_transport_method in OFFLOAD_TRANSPORT_SQOOP_METHODS
            )
            char_semantics_map = char_semantics_override_map(
                self._unicode_string_columns_csv, self._rdbms_columns
            )
            canonical_columns = []
            for tab_col in self._rdbms_columns:
                canonical_column = self._rdbms_table.to_canonical_column(tab_col)
                if assume_all_staging_columns_nullable:
                    canonical_column.nullable = True
                if tab_col.name in char_semantics_map:
                    canonical_column.char_semantics = char_semantics_map[tab_col.name]
                canonical_columns.append(canonical_column)
            self._canonical_columns = canonical_columns
        return self._canonical_columns

    def _create_basic_connectivity_attributes(self, offload_options):
        self._offload_transport_auth_using_oracle_wallet = (
            offload_options.offload_transport_auth_using_oracle_wallet
        )
        self._offload_transport_cmd_host = offload_options.offload_transport_cmd_host
        self._offload_transport_credential_provider_path = (
            offload_options.offload_transport_credential_provider_path
        )
        self._offload_transport_dsn = offload_options.offload_transport_dsn
        self._offload_transport_password_alias = (
            offload_options.offload_transport_password_alias
        )
        self._offload_transport_rdbms_session_parameters = (
            json.loads(offload_options.offload_transport_rdbms_session_parameters)
            if offload_options.offload_transport_rdbms_session_parameters
            else {}
        )
        self._spark_thrift_host = offload_options.offload_transport_spark_thrift_host
        self._spark_thrift_port = offload_options.offload_transport_spark_thrift_port

    def _get_transport_app_name(self, sep=".", ts=False, name_override=None) -> str:
        """Get a name suitable for the transport method in use.
        For most transports this will include the table name,  but some may choose to overload this
        If adding any punctuation, stick to underscores and dots below due to Java name rules
        Using target info because it is possible to offload RDBMS to different name which would then
        have an inaccurate name if we used RDBMS names
        """
        tokens = [YARN_TRANSPORT_NAME]
        if name_override:
            tokens.append(name_override)
        else:
            tokens.extend([self._target_db_name.lower(), self._load_table_name.lower()])
        if ts:
            tokens.append(self._str_time(name_safe=True))
        return sep.join(tokens)

    def _nothing_to_do(self, partition_chunk):
        """If partition_chunk is None then we are offloading everything
        If partition_chunk is of type OffloadSourcePartitions but with no partitions then there's nothing to do
        """
        if partition_chunk and partition_chunk.count() == 0:
            self.log("No partitions to export to Hadoop")
            return True
        return False

    def _prepare_spark_config_properties(self, offload_transport_spark_properties):
        spark_config_properties = {}
        if offload_transport_spark_properties:
            spark_config_properties = json.loads(offload_transport_spark_properties)
        if not self._offload_transport_password_alias:
            spark_config_properties.update(
                {"spark.jdbc.password": self._offload_options.rdbms_app_pass}
            )
        return spark_config_properties

    def _reset_transport_context(self):
        self._transport_context = {
            TRANSPORT_CXT_BYTES: None,
            TRANSPORT_CXT_ROWS: None,
        }

    def _ssh_cmd_prefix(self, host=None):
        """many calls to ssh_cmd_prefix() in this class use std inputs therefore abstract in this method"""
        host = host or self._offload_transport_cmd_host
        return ssh_cmd_prefix(self._offload_options.offload_transport_user, host)

    def _ssh_cli_safe_value(self, cmd_option_string):
        """If we are invoking Sqoop via SSH then we need extra protection for special characters on the command line
        Without SSH the protection added by subprocess module is sufficient
        """
        if self._ssh_cmd_prefix():
            return ansi_c_string_safe(cmd_option_string)
        else:
            return cmd_option_string

    def _spark_sql_option_safe(self, opt_val):
        # options in spark sql are wrapped in single quotes and need protecting
        return opt_val.replace("'", "\\'") if opt_val else opt_val

    def _str_time(self, name_safe=False):
        if name_safe:
            return datetime.now().strftime("%Y%m%d%H%M%S")
        else:
            return str(datetime.now().replace(microsecond=0))

    def _run_os_cmd(self, cmd, optional=False, no_log_items=[]):
        """Run an os command and:
          - Throw an exception if the return code is non-zero (and optional=False)
          - Put a dot on stdout for each line logged to give the impression of progress
          - Logs all command output at VVERBOSE level
        silent: No stdout unless vverbose is requested
        """
        return run_os_cmd(
            cmd,
            self._messages,
            self._offload_options,
            dry_run=self._dry_run,
            optional=optional,
            silent=bool(self._offload_options.suppress_stdout),
            no_log_items=no_log_items,
        )

    def _refresh_rdbms_action(self):
        self._rdbms_action = self._rdbms_api.generate_transport_action()

    def _get_max_ts_scale(self):
        if self._target_table and self._target_table:
            return min(
                self._target_table.max_datetime_scale(),
                self._rdbms_table.max_datetime_scale(),
            )
        else:
            return None

    def _get_rdbms_session_setup_commands(
        self,
        include_fixed_sqoop=True,
        for_plsql=False,
        include_semi_colons=False,
        escape_semi_colons=False,
    ):
        max_ts_scale = self._get_max_ts_scale()
        return self._rdbms_api.get_rdbms_session_setup_commands(
            self._rdbms_module,
            self._rdbms_action,
            self._offload_transport_rdbms_session_parameters,
            include_fixed_sqoop=include_fixed_sqoop,
            for_plsql=for_plsql,
            include_semi_colons=include_semi_colons,
            escape_semi_colons=escape_semi_colons,
            max_ts_scale=max_ts_scale,
        )

    def _check_rows_imported(self, rows_imported):
        """Log a warning if we weren't able to identify the number of rows imported
        the main benefit to this is being able to check for the warning during automated tests
        """
        if rows_imported is None and not self._dry_run:
            self._messages.warning(MISSING_ROWS_IMPORTED_WARNING)

    def _remote_copy_transport_control_file(
        self, options_file_local_path, target_host, prefix="", suffix=""
    ):
        """Copies a control file to Offload Transport host which will be used to run a command.
        If SSH short-circuit is in play then no copy is made, we just return the name of the local file.
        Returns commands to be used to remove the files, we do this because the control files can contain
        sensitive information so we want to clean them up even though they are in /tmp.
        """
        ssh_user = self._offload_options.offload_transport_user
        self.debug(
            "_remote_copy_transport_control_file(%s, %s, %s)"
            % (options_file_local_path, ssh_user, target_host)
        )
        rm_commands = [["rm", "-f", options_file_local_path]]
        if running_as_same_user_and_host(ssh_user, target_host):
            self.log(
                "Local host (%s@%s) and remote host (%s@%s) are the same therefore no file copy required"
                % (get_os_username(), gethostname(), ssh_user, target_host),
                detail=VVERBOSE,
            )
            options_file_remote_path = options_file_local_path
        else:
            # Copy the control file to the target host
            options_file_remote_path = get_temp_path(prefix=prefix, suffix=suffix)
            self.log(
                "Copying local file (%s) to remote (%s)"
                % (options_file_local_path, options_file_remote_path),
                detail=VVERBOSE,
            )
            scp_cmd = scp_to_cmd(
                ssh_user, target_host, options_file_local_path, options_file_remote_path
            )
            self._run_os_cmd(scp_cmd)
            rm_commands.append(
                ssh_cmd_prefix(ssh_user, target_host)
                + ["rm", "-f", options_file_remote_path]
            )
        return rm_commands, options_file_remote_path

    def _remote_copy_transport_file(self, local_path: str, target_host: str):
        """Copies a file required by Offload Transport to a remote host.
        If SSH short-circuit is in play then no copy is made, we just return the name of the local file.
        No effort is made to remove the remote file, this is because we put the file in /tmp and we could have a
        situation where concurrent offloads want to use the same file, removing it could cause problems. Copying
        the file to a unique name just so we can remove it seems overkill when the file is in /tmp anyway.
        TODO NJ@2021-08-23 This is very similar to _remote_copy_transport_control_file() but different enough
             to not want to destabilise _remote_copy_transport_control_file() for a patch release. We probably
             want to revisit this to try and merge the methods.
        """
        ssh_user = self._offload_options.offload_transport_user
        self.debug(
            "_remote_copy_transport_file(%s, %s, %s)"
            % (local_path, ssh_user, target_host)
        )
        if running_as_same_user_and_host(ssh_user, target_host):
            self.log(
                "Local host (%s@%s) and remote host (%s@%s) are the same therefore no file copy required"
                % (get_os_username(), gethostname(), ssh_user, target_host),
                detail=VVERBOSE,
            )
            remote_path = local_path
        else:
            # Copy the file to the target host
            remote_name = os.path.basename(local_path)
            remote_path = os.path.join("/tmp", remote_name)
            self.log(
                "Copying local file (%s) to remote (%s)" % (local_path, remote_path),
                detail=VVERBOSE,
            )
            scp_cmd = scp_to_cmd(ssh_user, target_host, local_path, remote_path)
            self._run_os_cmd(scp_cmd)
        return remote_path

    def _remote_copy_transport_files(self, local_paths: list, target_host: str) -> list:
        """Calls _remote_copy_transport_file() for all files in a list and returns new list of paths."""
        remote_paths = []
        for local_path in local_paths:
            if local_path:
                remote_paths.append(
                    self._remote_copy_transport_file(local_path, target_host)
                )
        return remote_paths

    def _remote_copy_transport_file_csv(
        self, local_path_csv: str, target_host: str
    ) -> str:
        """Calls _remote_copy_transport_files() for all file paths in a CSV and returns new list of paths."""
        if not local_path_csv:
            return local_path_csv
        remote_files = self._remote_copy_transport_files(
            local_path_csv.split(","), self._offload_transport_cmd_host
        )
        return ",".join(remote_files)

    def _spark_listener_included_in_config(self):
        """Used to get truthy True when extra Spark listeners are configured.
        Search is case-insensitive but returns the config name in correct case.
        """
        for k, v in self._spark_config_properties.items():
            if (
                k.lower() == "spark.extralisteners"
                and isinstance(v, str)
                and GOE_LISTENER_NAME in v
            ):
                self.log(
                    "%s configured: %s: %s" % (GOE_LISTENER_NAME, k, v), detail=VVERBOSE
                )
                return k
        return None

    def _credential_provider_path_jvm_override(self):
        return credential_provider_path_jvm_override(
            self._offload_transport_credential_provider_path
        )

    def _get_mod_column(self) -> str:
        """Pick a column suitable for MOD splitting"""
        if self._bucket_hash_col:
            return self._bucket_hash_col
        elif self._rdbms_pk_cols:
            return self._rdbms_pk_cols[0]
        else:
            # Return any column
            return self._rdbms_columns[0].name

    def _get_transport_row_source_query(
        self,
        partition_by: str,
        partition_chunk=None,
        pad=None,
        id_col_min=None,
        id_col_max=None,
    ):
        if self._rdbms_offload_predicate:
            self.log(
                "Offloading with offload predicate:\n%s"
                % str(self._rdbms_offload_predicate),
                detail=VVERBOSE,
            )
        predicate_offload_clause = self._rdbms_table.predicate_to_where_clause(
            self._rdbms_offload_predicate
        )
        mod_column = self._get_mod_column()
        return self._rdbms_api.get_transport_row_source_query(
            partition_by,
            self._rdbms_table,
            self._offload_transport_consistent_read,
            self._offload_transport_parallelism,
            self._offload_by_subpartition,
            mod_column,
            predicate_offload_clause,
            partition_chunk=partition_chunk,
            pad=pad,
            id_col_min=id_col_min,
            id_col_max=id_col_max,
        )

    def _get_spark_jdbc_query_text(
        self,
        split_type: str,
        partition_chunk,
        sql_comment=None,
        id_col_min=None,
        id_col_max=None,
    ) -> str:
        """Adds an outer query around the row source query produced by get_transport_row_source_query."""
        self.debug(
            "_get_spark_jdbc_query_text(%s, %s, %s)"
            % (TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN, split_type, sql_comment)
        )
        colexpressions, colnames = self._build_offload_query_lists(for_spark=True)
        rdbms_sql_projection = self._sql_projection_from_offload_query_expression_list(
            colexpressions,
            colnames,
            extra_cols=[TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN],
        )
        row_source_subquery = self._get_transport_row_source_query(
            split_type,
            partition_chunk=partition_chunk,
            pad=6,
            id_col_min=id_col_min,
            id_col_max=id_col_max,
        )
        rdbms_source_query = """(SELECT %(row_source_hint)s%(sql_comment)s %(rdbms_projection)s
FROM (%(row_source_subquery)s) %(table_alias)s) v2""" % {
            "row_source_subquery": row_source_subquery,
            "rdbms_projection": rdbms_sql_projection,
            "row_source_hint": self._rdbms_api.get_transport_row_source_query_hint_block(),
            "sql_comment": f" /* {sql_comment} */" if sql_comment else "",
            "table_alias": self._rdbms_table.enclose_identifier(self._rdbms_table_name),
        }
        return rdbms_source_query

    def _get_transport_split_type(
        self, partition_chunk, native_range_split_available: bool = False
    ) -> str:
        predicate_offload_clause = self._rdbms_table.predicate_to_where_clause(
            self._rdbms_offload_predicate
        )
        (
            split_row_source_by,
            tuned_parallelism,
        ) = self._rdbms_api.get_transport_split_type(
            partition_chunk,
            self._rdbms_table,
            self._offload_transport_parallelism,
            self._rdbms_partition_type,
            self._offload_by_subpartition,
            predicate_offload_clause,
            native_range_split_available=native_range_split_available,
        )
        if (
            tuned_parallelism
            and tuned_parallelism != self._offload_transport_parallelism
        ):
            self.notice(
                f"Overriding transport parallelism: {self._offload_transport_parallelism} -> {tuned_parallelism}"
            )
            self._offload_transport_parallelism = tuned_parallelism
        return split_row_source_by

    def _build_offload_query_lists(
        self, convert_expressions_on_rdbms_side=None, for_spark=False, for_qi=False
    ):
        """Returns a tuple containing:
        A list of column expressions to build an RDBMS projection.
        A list of column names for the projection (useful for aliases if above has anonymous CAST()s),
          this list is the names of the staging columns rather than the original source RDBMS columns.
          Nearly always the same but sometimes not.
        """
        self.debug("_build_offload_query_lists()")
        colnames, colexpressions = [], []
        staging_columns = self._staging_file.get_staging_columns()
        if convert_expressions_on_rdbms_side is None:
            convert_expressions_on_rdbms_side = bool(
                self._target_table.backend_type() == DBTYPE_SYNAPSE
            )
        for rdbms_col in self._rdbms_columns:
            cast_column = apply_transformation(
                self._column_transformations,
                rdbms_col,
                self._rdbms_table,
                self._messages,
            )
            staging_column = match_table_column(rdbms_col.name, staging_columns)

            colnames.append(staging_column.staging_file_column_name)

            cast_column = cast_column or self._rdbms_table.enclose_identifier(
                rdbms_col.name
            )

            cast_column = self._rdbms_api.get_rdbms_query_cast(
                cast_column,
                rdbms_col,
                staging_column,
                self._get_max_ts_scale(),
                convert_expressions_on_rdbms_side=convert_expressions_on_rdbms_side,
                for_spark=for_spark,
                for_qi=for_qi,
                nan_values_as_null=self._convert_nans_to_nulls,
            )
            colexpressions.append(cast_column)

        return colexpressions, colnames

    def _sql_projection_from_offload_query_expression_list(
        self, colexpressions, colnames, no_newlines=False, extra_cols=[]
    ):
        """Function for formatted results of _build_offload_query_lists() into a SQL projection"""
        if extra_cols:
            colexpressions = colexpressions + extra_cols
            colnames = colnames + extra_cols
        if no_newlines:
            sql_projection = ",".join(
                [
                    '%s "%s"' % (expr, cname)
                    for expr, cname in zip(colexpressions, colnames)
                ]
            )
        else:
            max_expr_len = max(len(_) for _ in colexpressions)
            project_pattern = "%-" + str(max_expr_len or 32) + 's AS "%s"'
            sql_projection = "\n,      ".join(
                [
                    project_pattern % (expr, cname)
                    for expr, cname in zip(colexpressions, colnames)
                ]
            )
        return sql_projection

    def _offload_transport_type_remappings(self, return_as_list=True, remap_sep=" "):
        """Returns a list or string containing a parameter and values, or an empty list/string:
        Sqoop:
            ['--map-column-java', 'a value for --map-column-java that maps columns appropriately']
        Spark:
            ['customSchema', 'a value for customSchema that maps columns appropriately']
        """
        assert self._offload_transport_method in (
            OFFLOAD_TRANSPORT_METHOD_SQOOP,
            OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
            OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
            OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
            OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
            OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
        ), f"Transport method {self._offload_transport_method} should not reach this code"

        remap = []
        for col in self._staging_file.get_staging_columns():
            java_primitive = self._staging_file.get_java_primitive(col)
            if java_primitive:
                remap.append((col.staging_file_column_name, java_primitive))

        if remap:
            if self._offload_transport_method in OFFLOAD_TRANSPORT_SQOOP_METHODS:
                remap_strings = ",".join(
                    "%s=%s" % (col_name, remap_type) for col_name, remap_type in remap
                )
                remap_prefix = "--map-column-java"
            elif self._offload_transport_method in OFFLOAD_TRANSPORT_SPARK_METHODS:
                remap_strings = "'%s'" % ",".join(
                    "%s %s" % (col_name, remap_type) for col_name, remap_type in remap
                )
                remap_prefix = "customSchema"
            else:
                raise NotImplementedError(
                    "_offload_transport_type_remappings: unknown remap type %s"
                    % self._offload_transport_method
                )

            self.log(
                "Mapped staging columns to Java primitives: %s" % remap_strings,
                detail=VVERBOSE,
            )

            if return_as_list:
                return [remap_prefix, remap_strings]
            else:
                return remap_prefix + remap_sep + remap_strings
        else:
            return [] if return_as_list else ""

    def _check_and_log_transported_files(self, row_count) -> Union[int, None]:
        """Check and list contents of target location.
        This interface level method assumes we can list contents via GOEDfs and is particularly useful when
        writing to cloud storage by ensuring filesystems with eventual consistency have made the files visible.
        target_uri: a file or a directory path, this method will only go one level down, we do not recurse.
        row_count: If this is 0 then we don't expect to find any files.
        Individual transport implementations may override this if it is not suitable.
        Returns the collective size in bytes of all files found.
        """

        def log_file_size(path, uri_attribs) -> int:
            self.log("Staged path: %s" % path, detail=VVERBOSE)
            if uri_attribs.get("length") is None:
                size_str = "Unknown"
            else:
                size_str = bytes_to_human_size(uri_attribs["length"])
            self.log("Staged size: %s" % size_str, detail=VVERBOSE)
            return uri_attribs.get("length")

        if not row_count:
            self.debug("Not logging contents of URI for row count: %s" % row_count)
            return None

        self.debug("Logging contents of URI: %s" % self._staging_table_location)
        if self._dry_run:
            return None

        try:
            total_size = 0
            for file_path in self._dfs_client.list_dir_and_wait_for_contents(
                self._staging_table_location
            ):
                uri_attribs = self._dfs_client.stat(file_path)
                self.debug("%s attributes: %s" % (file_path, uri_attribs))
                file_bytes = None
                if uri_attribs["type"] == DFS_TYPE_FILE:
                    file_bytes = log_file_size(file_path, uri_attribs)
                else:
                    for sub_path in self._dfs_client.list_dir(file_path):
                        uri_attribs = self._dfs_client.stat(sub_path)
                        self.debug("%s attributes: %s" % (sub_path, uri_attribs))
                        if uri_attribs["type"] == DFS_TYPE_FILE:
                            file_bytes = log_file_size(sub_path, uri_attribs)
                if file_bytes:
                    total_size += file_bytes
            return total_size
        except Exception as exc:
            # This logging is totally a nice-to-have and therefore we move on if it fails
            self.log(traceback.format_exc(), detail=VVERBOSE)
            self.log(str(exc), detail=VVERBOSE)
            return None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def debug(self, msg):
        self._messages.debug(msg)

    def log(self, msg, detail=None):
        self._messages.log(msg, detail=detail)

    def log_dfs_cmd(self, msg, detail=VERBOSE):
        self._messages.log("%s cmd: %s" % (self._backend_dfs, msg), detail=detail)

    def notice(self, msg):
        self._messages.notice(msg)

    def warning(self, msg):
        self._messages.warning(msg)

    def get_transport_bytes(self) -> Union[int, None]:
        """Return a dict used to pass contextual information back from transport()"""
        return self._transport_context.get(TRANSPORT_CXT_BYTES)

    def get_staging_file(self):
        return self._staging_file

    def transport(
        self, partition_chunk: Optional["OffloadSourcePartitions"] = None
    ) -> Union[int, None]:
        """Run data transport for the implemented transport method and return the number of rows transported."""

    @abstractmethod
    def ping_source_rdbms(self):
        """Test connectivity from the Offload Transport tool back to the source RDBMS"""


class OffloadTransportSpark(OffloadTransport, metaclass=ABCMeta):
    """Parent class for Spark methods"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        super(OffloadTransportSpark, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )
        self._OffloadTransportSqlStatsThread = None
        self._convert_nans_to_nulls = convert_nans_to_nulls(
            offload_target_table, offload_operation
        )
        self._spark_files_csv = offload_options.offload_transport_spark_files
        self._spark_jars_csv = offload_options.offload_transport_spark_jars

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=False)

    def _standalone_spark(self) -> bool:
        return bool(
            self._offload_options.backend_distribution
            not in HADOOP_BASED_BACKEND_DISTRIBUTIONS
        )

    def _option_from_properties(self, option_name, property_name) -> list:
        """Small helper function to pluck a property value from self._spark_config_properties"""
        if property_name in self._spark_config_properties:
            return [option_name, self._spark_config_properties[property_name]]
        else:
            return []

    def _jdbc_option_clauses(self) -> str:
        jdbc_option_clauses = ""
        if self._offload_options.db_type == DBTYPE_ORACLE:
            jdbc_option_clauses = ".option('oracle.jdbc.timezoneAsRegion', 'false')"
        return jdbc_option_clauses

    def _get_id_range(
        self, split_row_source_by, id_range_column, partition_chunk
    ) -> tuple:
        col_name = (
            id_range_column
            if isinstance(id_range_column, str)
            else id_range_column.name
        )
        predicate_offload_clause = self._rdbms_table.predicate_to_where_clause(
            self._rdbms_offload_predicate
        )
        id_col_min, id_col_max = self._rdbms_api.get_id_range(
            col_name, predicate_offload_clause, partition_chunk=partition_chunk
        )
        if id_col_min is None or id_col_max is None:
            self.log(
                f"Switching from range to mod data split due to blank values: {id_col_min} -> {id_col_max}",
                detail=VVERBOSE,
            )
            split_row_source_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD
            id_col_min = None
            id_col_max = None
        return split_row_source_by, id_col_min, id_col_max

    def _get_id_column_for_range_splitting(self) -> "ColumnMetadataInterface":
        return self._rdbms_api.get_id_column_for_range_splitting(self._rdbms_table)

    def _get_pyspark_body(
        self, partition_chunk=None, create_spark_context=True, canary_query=None
    ) -> str:
        """Return pyspark code to copy data to the load table.
        Shared by multiple sub-classes.
        When defining the parallel chunks we set the upperBound (batch_col_max) one higher than the actual value.
        This is because Spark SQL defines the number of partitions using logic below and we always end up in the
        "else" if we tell the truth:
            val numPartitions =
                if ((upperBound - lowerBound) >= requestedPartitions) {
                    requestedPartitions
                } else {
                    upperBound - lowerBound
                }
        canary_query allows us to pass in a specific test query and not use one based on an offload table
        """

        def get_password_snippet():
            password_python = []
            if self._offload_transport_password_alias:
                password_python.append(
                    "hconf = spark.sparkContext._jsc.hadoopConfiguration()"
                )
                if self._offload_transport_credential_provider_path:
                    # provider_path is optional because a customer may define it in their Hadoop config rather than GOE config
                    password_python.append(
                        "hconf.set('hadoop.security.credential.provider.path', '%s')"
                        % self._offload_transport_credential_provider_path
                    )
                password_python.append(
                    "pw = hconf.getPassword('%s')"
                    % self._offload_transport_password_alias
                )
                password_python.append(
                    "assert pw, 'Unable to retrieve password for alias: %s'"
                    % self._offload_transport_password_alias
                )
                password_python.append(
                    "jdbc_password = ''.join(str(pw.__getitem__(i)) for i in range(pw.__len__()))"
                )
            else:
                password_python.append(
                    "jdbc_password = spark.conf.get('spark.jdbc.password')"
                )
            return "\n".join(password_python)

        batch_col_min = 0
        batch_col_max = self._offload_transport_parallelism
        id_col_min = None
        id_col_max = None
        if canary_query:
            custom_schema_clause = ""
            rdbms_source_query = canary_query
            transport_app_name = "GOE Connect"
            load_db_name, load_table_name = "", ""
        else:
            split_row_source_by = self._get_transport_split_type(
                partition_chunk, native_range_split_available=True
            )
            if split_row_source_by in (
                TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
                TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
            ):
                id_range_column = self._get_id_column_for_range_splitting()
                split_row_source_by, id_col_min, id_col_max = self._get_id_range(
                    split_row_source_by, id_range_column, partition_chunk
                )

                if (
                    split_row_source_by
                    == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE
                ):
                    batch_col_min = id_col_min
                    batch_col_max = id_col_max

            custom_schema_clause = self._column_type_read_remappings()
            rdbms_source_query = self._get_spark_jdbc_query_text(
                split_row_source_by,
                partition_chunk,
                sql_comment=self._rdbms_action,
                id_col_min=id_col_min,
                id_col_max=id_col_max,
            )
            transport_app_name = self._get_transport_app_name()
            load_db_name, load_table_name = self._load_db_name, self._load_table_name

        session_init_statements = "".join(
            self._get_rdbms_session_setup_commands(for_plsql=True)
        )
        session_init_statement_opt = ""
        if session_init_statements:
            session_init_statement_opt = (
                "\n    sessionInitStatement='BEGIN %s END;',"
                % self._spark_sql_option_safe(session_init_statements)
            )

        jdbc_option_clauses = self._jdbc_option_clauses()

        if self._offload_transport_auth_using_oracle_wallet:
            rdbms_app_user_opt = rdbms_app_pass_opt = password_snippet = ""
        else:
            rdbms_app_user_opt = (
                "\n    user='%s'," % self._offload_options.rdbms_app_user
            )
            rdbms_app_pass_opt = "\n    password=jdbc_password,"
            password_snippet = get_password_snippet()

        # Certain config should not be logged
        config_blacklist = [
            "spark.jdbc.password",
            "spark.authenticate.secret",
            "spark.hadoop.fs.gs.auth.service.account.private.key",
            "spark.hadoop.fs.gs.auth.service.account.private.key.id",
            "spark.hadoop.fs.s3a.access.key",
            "spark.hadoop.fs.s3a.secret.key",
            "spark.ssl.keyPassword",
            "spark.ssl.keyStorePassword",
            "spark.ssl.trustStorePassword",
            "spark.com.goe.SparkBasicAuth.params",
        ]
        debug_conf_snippet = dedent(
            """\
                                    config_blacklist = %s
                                    for kv in sorted(spark.sparkContext.getConf().getAll()):
                                        if kv[0] in config_blacklist:
                                            print(kv[0], '?')
                                        else:
                                            print(kv)"""
            % repr(config_blacklist)
        )

        params = {
            "load_db": load_db_name,
            "table_name": load_table_name,
            "rdbms_jdbc_url": self._rdbms_api.jdbc_url(),
            "rdbms_jdbc_driver_name": self._rdbms_api.jdbc_driver_name(),
            "rdbms_app_user_opt": rdbms_app_user_opt,
            "rdbms_app_pass_opt": rdbms_app_pass_opt,
            "get_password_snippet": password_snippet,
            "rdbms_source_query": self._spark_sql_option_safe(rdbms_source_query),
            "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
            "batch_col_min": PysparkLiteral.format_literal(batch_col_min),
            # batch_col_max is one higher than reality, see header comment
            "batch_col_max": PysparkLiteral.format_literal(batch_col_max),
            "parallelism": self._offload_transport_parallelism,
            "custom_schema_clause": (
                (",\n    " + custom_schema_clause) if custom_schema_clause else ""
            ),
            "write_format": self._staging_format,
            "uri": (
                "" if canary_query else self._target_table.get_staging_table_location()
            ),
            "fetch_size": self._offload_transport_fetch_size,
            "session_init_statement_opt": session_init_statement_opt,
            "debug_conf_snippet": debug_conf_snippet,
            "jdbc_option_clauses": jdbc_option_clauses,
        }

        pyspark_body = ""

        if self._offload_transport_method != OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY:
            # On HDP2 Livy the UTF coding is causing errors submitting the script:
            # SyntaxError: encoding declaration in Unicode string
            # We want UTF coding just in case we have PBO with a unicode predicate but decided to exclude line below
            # for Livy only. In reality no-one is using Livy with GOE in the near future. We might have to revisit.
            pyspark_body += dedent(
                """\
                   # -*- coding: UTF-8 -*-
                   """
            )

        pyspark_body += dedent(
            """\
            # GOE Spark Transport
            """
        )

        if self._base64_staged_columns():
            pyspark_body += dedent(
                """\
                from pyspark.sql.functions import base64
                """
            )

        if create_spark_context:
            pyspark_body += (
                dedent(
                    """\
                                   from pyspark.sql import SparkSession
                                   spark = SparkSession.builder.appName('%(app_name)s')%(hive_support)s.getOrCreate()
                                   """
                )
                % {
                    "app_name": transport_app_name,
                    "hive_support": (
                        "" if self._standalone_spark() else ".enableHiveSupport()"
                    ),
                }
            )

        if canary_query:
            # Just testing connectivity and will drop out with no write of data
            pyspark_body += (
                dedent(
                    """\
                %(get_password_snippet)s
                df = spark.read.format('jdbc').options(
                    driver='%(rdbms_jdbc_driver_name)s',
                    url='%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s%(session_init_statement_opt)s
                    dbtable=\"\"\"%(rdbms_source_query)s\"\"\",
                )%(jdbc_option_clauses)s.load().show()
                """
                )
                % params
            )
            return pyspark_body

        # Using saveAsTable because, unlike insertInto, it will use the column names to find the correct column
        # positions which then allows it to skip TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN
        # In the code below, after loading the data from the RDBMS we use select() to only project
        # table columns and not the split column.
        pyspark_body += (
            dedent(
                """\
            %(debug_conf_snippet)s
            %(get_password_snippet)s
            df = spark.read.format('jdbc').options(
                driver='%(rdbms_jdbc_driver_name)s',
                url='%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s%(session_init_statement_opt)s
                dbtable=\"\"\"%(rdbms_source_query)s\"\"\",
                fetchSize=%(fetch_size)s,
                partitionColumn='%(batch_col)s',
                lowerBound=%(batch_col_min)s,
                upperBound=%(batch_col_max)s,
                numPartitions=%(parallelism)s%(custom_schema_clause)s
            )%(jdbc_option_clauses)s.load()
            df.printSchema()
            """
            )
            % params
        )

        if self._base64_staged_columns():

            def proj_col(col_name):
                if match_table_column(col_name, self._base64_staged_columns()):
                    return "base64(df.%(col)s).alias('%(col)s')" % {"col": col_name}
                else:
                    return "'%s'" % col_name

            pyspark_body += (
                dedent(
                    """\
            projection = [%s]
            """
                )
                % ",".join(proj_col(_.name) for _ in self._rdbms_columns)
            )
        else:
            # Remove any synthetic split/partition column from the projection.
            pyspark_body += (
                dedent(
                    """\
            projection = [_ for _ in df.columns if _.upper() != '%(batch_col)s'.upper()]
            """
                )
                % params
            )

        if self._standalone_spark():
            pyspark_body += (
                dedent(
                    """\
            df.select(projection).write.format('%(write_format)s').save('%(uri)s')
            """
                )
                % params
            )
        else:
            pyspark_body += (
                dedent(
                    """\
            df.select(projection).write.mode('append').format('hive').saveAsTable('`%(load_db)s`.`%(table_name)s`')
            """
                )
                % params
            )
        return pyspark_body

    def _get_rows_imported_from_spark_log(self, spark_log_text):
        """Scrape spark_log_text searching for rows imported information"""
        self.debug("_get_rows_imported_from_spark_log()")
        if not spark_log_text:
            return None
        assert isinstance(spark_log_text, str), "{} is not str".format(
            type(spark_log_text)
        )
        # Now look for the row count marker
        m = re.findall(
            SPARK_LOG_ROW_COUNT_PATTERN, spark_log_text, re.IGNORECASE | re.MULTILINE
        )
        if m:
            self.log(
                "Found {} recordsWritten matches in Spark log: {}".format(
                    len(m), str(m)
                ),
                detail=VVERBOSE,
            )
            records_written = sum([int(_) for _ in m if _])
            self.log(
                "Spark recordsWritten total: {}".format(str(records_written)),
                detail=VVERBOSE,
            )
            return records_written
        else:
            return None

    def _load_table_compression_pyspark_settings(self):
        if self._compress_load_table:
            codec = "snappy"
            true_or_false = "true"
        else:
            codec = "uncompressed"
            true_or_false = "false"
        prm_format = (
            "avro" if self._staging_format == FILE_STORAGE_FORMAT_AVRO else "parquet"
        )
        if self._standalone_spark():
            self._spark_config_properties.update(
                {"spark.sql.%s.compression.codec" % prm_format: codec}
            )
        else:
            # Spark is part of Hadoop
            self._spark_config_properties.update(
                {
                    "spark.hadoop.%s.output.codec" % prm_format: codec,
                    "spark.hadoop.hive.exec.compress.output": true_or_false,
                }
            )

    def _load_table_compression_spark_sql_settings(self):
        if self._compress_load_table:
            codec = "snappy"
            true_or_false = "true"
        else:
            codec = "uncompressed"
            true_or_false = "false"
        prm_format = (
            "avro" if self._staging_format == FILE_STORAGE_FORMAT_AVRO else "parquet"
        )
        # These settings work whether Spark is stand-alone or part of Hadoop
        self._spark_config_properties.update(
            {
                "%s.output.codec" % prm_format: codec,
                "hive.exec.compress.output": true_or_false,
            }
        )

    def _local_goe_listener_jar(self):
        """Returns path to GOETaskListener jar file in $OFFLOAD_HOME/lib directory"""
        offload_home = os.environ.get("OFFLOAD_HOME")
        assert offload_home, "OFFLOAD_HOME is not set, environment is not correct"
        jar_path = os.path.join(offload_home, "lib", GOE_LISTENER_JAR)
        # We should never be missing the JAR file as it is bundled with the code that we are part of.
        assert os.path.exists(
            jar_path
        ), f"{jar_path} cannot be found, environment is not correct"
        return jar_path

    def _start_validation_polling_thread(self):
        if (
            self._validation_polling_interval
            and self._validation_polling_interval
            != OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED
        ):
            self._OffloadTransportSqlStatsThread = PollingThread(
                run_function=self._rdbms_api.get_offload_transport_sql_stats_function(
                    self._rdbms_module,
                    self._rdbms_action,
                    "OffloadTransportSqlStatsThread",
                ),
                name="OffloadTransportSqlStatsThread",
                interval=self._validation_polling_interval,
            )
            self._OffloadTransportSqlStatsThread.start()

    def _stop_validation_polling_thread(self):
        if (
            self._OffloadTransportSqlStatsThread
            and self._OffloadTransportSqlStatsThread.is_alive()
        ):
            self._OffloadTransportSqlStatsThread.stop()

    def _drain_validation_polling_thread_queue(self):
        if self._OffloadTransportSqlStatsThread:
            if self._OffloadTransportSqlStatsThread.is_alive():
                self._stop_validation_polling_thread()
            if self._OffloadTransportSqlStatsThread.exception:
                self.log(
                    "Exception in %s polling method:\n%s"
                    % (
                        self._OffloadTransportSqlStatsThread.name,
                        self._OffloadTransportSqlStatsThread.exception,
                    ),
                    VVERBOSE,
                )
                return None
            elif self._OffloadTransportSqlStatsThread.get_queue_length() == 0:
                self.log(
                    "Empty results queue from %s polling method"
                    % self._OffloadTransportSqlStatsThread.name,
                    VVERBOSE,
                )
                return None
            else:
                self.log(
                    POLLING_VALIDATION_TEXT % self._OffloadTransportSqlStatsThread.name,
                    VVERBOSE,
                )
                self.debug(
                    "Parsing %s snapshots taken at %s second intervals"
                    % (
                        self._OffloadTransportSqlStatsThread.get_queue_length(),
                        self._OffloadTransportSqlStatsThread.interval,
                    )
                )
                return self._OffloadTransportSqlStatsThread.drain_queue()


class OffloadTransportSparkThrift(OffloadTransportSpark):
    """Use Spark Thrift Server to transport data"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT
        super(OffloadTransportSparkThrift, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )
        assert (
            offload_options.offload_transport_spark_thrift_host
            and offload_options.offload_transport_spark_thrift_port
        )

        self._load_table_compression_spark_sql_settings()

        # spark.jdbc.password is not retrievable in pure Spark SQL so remove from properties
        if "spark.jdbc.password" in self._spark_config_properties:
            del self._spark_config_properties["spark.jdbc.password"]

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _log_connection_details(self):
        self.log(
            hs2_connection_log_message(
                self._spark_thrift_host,
                self._spark_thrift_port,
                self._offload_options,
                service_name="Spark Thrift Server",
            ),
            detail=VVERBOSE,
        )

    def _get_create_temp_view_sql(
        self,
        temp_vw_name,
        rdbms_source_query,
        spark_api,
        custom_schema_clause=None,
        connectivity_test_only=False,
    ):
        """Generate text for a Spark session temporary view which will contain details of where to get data from and
        how to partition the workload
        When defining the parallel chunks we set the upperBound (batch_col_max) one higher than the actual value.
        This is because Spark SQL defines the number of partitions using logic below and we always end up in the
        "else" if we tell the truth:
            val numPartitions =
                if ((upperBound - lowerBound) >= requestedPartitions) {
                    requestedPartitions
                } else {
                    upperBound - lowerBound
                }
        Unable to find a way to hook into credential API when using the thrift server
        """
        self.debug("_get_create_temp_view_sql()")

        session_init_statements = "".join(
            self._get_rdbms_session_setup_commands(
                for_plsql=True, escape_semi_colons=True
            )
        )

        if self._offload_transport_auth_using_oracle_wallet:
            rdbms_app_user_opt = ""
            rdbms_app_pass_opt = ""
        else:
            rdbms_app_user_opt = "\n  user='%s'," % self._offload_options.rdbms_app_user
            rdbms_app_pass_opt = "\n  password '%s'," % self._spark_sql_option_safe(
                self._offload_options.rdbms_app_pass
            )
        quoted_name = spark_api.enclose_identifier(temp_vw_name)
        params = {
            "temp_vw_name": quoted_name,
            "rdbms_app_user_opt": rdbms_app_user_opt,
            "rdbms_app_pass_opt": rdbms_app_pass_opt,
            "rdbms_jdbc_url": self._rdbms_api.jdbc_url(),
            "rdbms_jdbc_driver_name": self._rdbms_api.jdbc_driver_name(),
            "rdbms_source_query": self._spark_sql_option_safe(rdbms_source_query),
            "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
            # batch_col_max is one higher than reality, see header comment
            "batch_col_max": self._offload_transport_parallelism,
            "parallelism": self._offload_transport_parallelism,
            "custom_schema_clause": (
                (",\n " + custom_schema_clause) if custom_schema_clause else ""
            ),
            "fetch_size": self._offload_transport_fetch_size,
            "session_init_statements": self._spark_sql_option_safe(
                session_init_statements
            ),
        }
        if connectivity_test_only:
            temp_vw_sql = (
                """CREATE TEMPORARY VIEW %(temp_vw_name)s
    USING org.apache.spark.sql.jdbc
    OPTIONS (
      driver '%(rdbms_jdbc_driver_name)s',
      url '%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s
      sessionInitStatement 'BEGIN %(session_init_statements)s END\\;',
      dbtable '%(rdbms_source_query)s'
    )"""
                % params
            )
        else:
            temp_vw_sql = (
                """CREATE TEMPORARY VIEW %(temp_vw_name)s
    USING org.apache.spark.sql.jdbc
    OPTIONS (
      driver '%(rdbms_jdbc_driver_name)s',
      url '%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s
      sessionInitStatement 'BEGIN %(session_init_statements)s END\\;',
      dbtable '%(rdbms_source_query)s',
      fetchSize '%(fetch_size)s',
      partitionColumn '%(batch_col)s',
      lowerBound 0,
      upperBound %(batch_col_max)s,
      numPartitions %(parallelism)s%(custom_schema_clause)s
    )"""
                % params
            )
        return temp_vw_sql

    def _spark_thrift_import(self, partition_chunk=None):
        """Transport data using the Spark2 Thrift Server"""
        self.debug(
            "_spark_thrift_import(%s, %s)"
            % (self._spark_thrift_host, self._spark_thrift_port)
        )
        self._log_connection_details()

        if self._nothing_to_do(partition_chunk):
            return 0

        self._refresh_rdbms_action()
        id_str = id_generator()
        rows_imported = None

        temp_vw_name = "goe_%s_%s_%s_jdbc_vw" % (
            self._rdbms_owner.lower(),
            self._rdbms_table_name.lower(),
            id_str,
        )
        hive_sql_projection = "\n,      ".join(_.name for _ in self._rdbms_columns)
        split_row_source_by = self._get_transport_split_type(partition_chunk)
        id_range_column = self._get_id_column_for_range_splitting()
        split_row_source_by, id_col_min, id_col_max = self._get_id_range(
            split_row_source_by, id_range_column, partition_chunk
        )
        rdbms_source_query = self._get_spark_jdbc_query_text(
            split_row_source_by,
            partition_chunk,
            sql_comment=self._rdbms_action,
            id_col_min=id_col_min,
            id_col_max=id_col_max,
        )
        custom_schema_clause = self._column_type_read_remappings()

        spark_api = backend_api_factory(
            DBTYPE_SPARK, self._offload_options, self._messages, dry_run=self._dry_run
        )

        temp_vw_sql = self._get_create_temp_view_sql(
            temp_vw_name,
            rdbms_source_query,
            spark_api,
            custom_schema_clause=custom_schema_clause,
        )

        ins_sql = """INSERT INTO %(load_db)s.%(table_name)s
SELECT %(hive_projection)s
FROM   %(temp_vw_name)s""" % {
            "temp_vw_name": spark_api.enclose_identifier(temp_vw_name),
            "load_db": spark_api.enclose_identifier(self._load_db_name),
            "table_name": spark_api.enclose_identifier(self._load_table_name),
            "hive_projection": hive_sql_projection,
        }

        if self._standalone_spark():
            # For stand-alone Spark we need a load db and table in the Derby DB in order to stage to cloud storage
            self.log(
                "%s: START Prepare Spark load table" % self._str_time(), detail=VVERBOSE
            )
            if not spark_api.database_exists(self._load_db_name):
                spark_api.create_database(self._load_db_name)
            if spark_api.table_exists(self._load_db_name, self._load_table_name):
                spark_api.drop_table(self._load_db_name, self._load_table_name)
            spark_columns = [
                spark_api.from_canonical_column(_)
                for _ in self._staging_file.get_canonical_staging_columns()
            ]
            spark_api.create_table(
                self._load_db_name,
                self._load_table_name,
                spark_columns,
                None,
                storage_format=self._staging_format,
                location=self._target_table.get_staging_table_location(),
                external=True,
            )
            self.log(
                "%s: END Prepare Spark load table" % self._str_time(), detail=VVERBOSE
            )

        query_options = self._spark_properties_thrift_server_exclusions()

        # We don't want passwords on screen so request API to sub the password for ?
        hive_jdbc_pass_clause = "password '%s'" % self._spark_sql_option_safe(
            self._offload_options.rdbms_app_pass
        )

        self._start_validation_polling_thread()

        # These need to be executed together to ensure they are on the same cursor
        # otherwise the temporary view disappears.
        transport_sqls = [temp_vw_sql, ins_sql]
        spark_api.execute_ddl(
            transport_sqls,
            query_options=query_options,
            log_level=VERBOSE,
            no_log_items={"item": hive_jdbc_pass_clause, "sub": "password '?'"},
        )

        self._stop_validation_polling_thread()

        if not self._dry_run:
            rows_imported = self._rdbms_api.log_sql_stats(
                self._rdbms_module,
                self._rdbms_action,
                self._drain_validation_polling_thread_queue(),
                validation_polling_interval=self._validation_polling_interval,
            )

        if self._standalone_spark() and not self._preserve_load_table:
            # Drop the Spark stand alone load table
            self.log(
                "%s: START Remove Spark load table" % self._str_time(), detail=VVERBOSE
            )
            spark_api.drop_table(self._load_db_name, self._load_table_name)
            self.log(
                "%s: END Remove Spark load table" % self._str_time(), detail=VVERBOSE
            )

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _spark_properties_thrift_server_exclusions(self, with_warnings=True):
        query_options = {}
        if self._spark_config_properties:
            if (
                bool(
                    [_ for _ in self._spark_config_properties if "memory" in _.lower()]
                )
                and with_warnings
            ):
                self._messages.warning(
                    "Memory configuration overrides are ignored when using Thrift Server"
                )
                # In truth many settings are ignored but we'll still go through the motions as some,
                # compression for example, do work
            # Remove any properties we know to be incompatible with the Thrift Server
            for property_name in [
                _
                for _ in self._spark_config_properties
                if _ not in SPARK_THRIFT_SERVER_PROPERTY_EXCLUDE_LIST
            ]:
                query_options[property_name] = self._spark_config_properties[
                    property_name
                ]
        return query_options

    def _verify_rdbms_connectivity(self):
        """Use a simple canary query for verification test"""
        rdbms_source_query = "(%s) v" % self._rdbms_api.get_rdbms_canary_query()
        temp_vw_name = "goe_canary_jdbc_vw"
        spark_api = backend_api_factory(
            DBTYPE_SPARK, self._offload_options, self._messages, dry_run=self._dry_run
        )
        temp_vw_sql = self._get_create_temp_view_sql(
            temp_vw_name, rdbms_source_query, spark_api, connectivity_test_only=True
        )
        hive_jdbc_pass_clause = "password '%s'" % self._spark_sql_option_safe(
            self._offload_options.rdbms_app_pass
        )
        test_sql = "SELECT COUNT(*) FROM %s" % temp_vw_name
        query_options = self._spark_properties_thrift_server_exclusions(
            with_warnings=False
        )
        # Do not log temp_vw_sql - it may contain a password
        spark_api.execute_ddl(
            temp_vw_sql,
            query_options=query_options,
            log_level=VVERBOSE,
            no_log_items={"item": hive_jdbc_pass_clause, "sub": "password '?'"},
        )
        row = spark_api.execute_query_fetch_one(test_sql, log_level=VERBOSE)
        return True if row[0] == 1 else False

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(
        self, partition_chunk: Optional["OffloadSourcePartitions"] = None
    ) -> Union[int, None]:
        """Spark Thriftserver transport"""
        self._reset_transport_context()

        def step_fn():
            row_count = self._spark_thrift_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count

        return self._messages.offload_step(
            command_steps.STEP_STAGING_TRANSPORT, step_fn, execute=(not self._dry_run)
        )

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkThriftCanary(OffloadTransportSparkThrift):
    """Validate Spark Thrift Server connectivity"""

    def __init__(self, offload_options, messages):
        """CONSTRUCTOR
        This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        assert offload_options.offload_transport_spark_thrift_host
        assert offload_options.offload_transport_spark_thrift_port
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = (
            orchestration_defaults.bool_option_from_string(
                "OFFLOAD_TRANSPORT_CONSISTENT_READ",
                orchestration_defaults.offload_transport_consistent_read_default(),
            )
        )
        self._offload_transport_fetch_size = (
            orchestration_defaults.offload_transport_fetch_size_default()
        )
        self._offload_transport_jvm_overrides = (
            orchestration_defaults.offload_transport_spark_overrides_default()
        )
        self._offload_transport_queue_name = (
            orchestration_defaults.offload_transport_spark_queue_name_default()
        )
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = (
            orchestration_defaults.offload_transport_validation_polling_interval_default()
        )
        self._spark_config_properties = self._prepare_spark_config_properties(
            orchestration_defaults.offload_transport_spark_properties_default()
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(
            "dummy_owner",
            "dummy_table",
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkSubmit(OffloadTransportSpark):
    """Use Python via spark-submit to transport data"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT
        super(OffloadTransportSparkSubmit, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )
        # For spark-submit we need to pass compression in as a config to the driver program
        self._load_table_compression_pyspark_settings()
        self._spark_submit_executable = (
            offload_options.offload_transport_spark_submit_executable
        )
        self._offload_transport_spark_submit_master_url = (
            offload_options.offload_transport_spark_submit_master_url
        )
        assert self._spark_submit_executable

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _spark_cli_safe_value(self, cmd_option_string):
        return self._ssh_cli_safe_value(cmd_option_string)

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(
            return_as_list=False, remap_sep="="
        )

    def _remote_copy_spark_control_file(self, options_file_local_path, suffix=""):
        return self._remote_copy_transport_control_file(
            options_file_local_path,
            self._offload_transport_cmd_host,
            prefix=SPARK_OPTIONS_FILE_PREFIX,
            suffix=suffix,
        )

    def _derive_spark_submit_cmd_arg(self):
        """Look to see if spark2-submit is available, if so then use it
        Otherwise fall back to normal executable but trying to influence Spark version via env var
        Retruns a list suitable for building a command for _run_os_cmd()
        """
        if (
            self._spark_submit_executable == "spark-submit"
            and not self._standalone_spark()
        ):
            return [
                "export",
                "SPARK_MAJOR_VERSION=2",
                ";",
                self._spark_submit_executable,
            ]
        else:
            return [self._spark_submit_executable]

    def _get_spark_submit_command(self, pyspark_body):
        """Submit PySpark code via spark-submit
        spark-submit --deploy-mode:
          The default for --deploy-mode is client. This appears to be the most sensible value for us anyway:
            1) When the client goes away the job aborts rather than continuing
            2) More detailed logging is reported because the driver is local
          If a customer does require "cluster" mode then it can be achieved via config:
            spark.submit.deployMode=cluster
        spark-submit --principal/--keytab:
          I have not implemented these. The default code will renew tickets anyway. These would be required to allow
          spark-submit to re-authenticate if the renewal lifetime expired. If that happened then the remained of the
          offload command would also fail so there's no point in implementing right now. If we do then we'll need a
          mechanism to get the keytab onto the edge node.
        """

        def cli_safe_the_password(k, v):
            if k == "spark.jdbc.password":
                return self._spark_cli_safe_value(v)
            else:
                return v

        self.log("PySpark: " + pyspark_body, detail=VVERBOSE)
        options_file_local_path = write_temp_file(
            pyspark_body, prefix=SPARK_OPTIONS_FILE_PREFIX, suffix="py"
        )
        py_rm_commands, options_file_remote_path = self._remote_copy_spark_control_file(
            options_file_local_path, suffix="py"
        )
        if self._spark_listener_included_in_config():
            spark_listener_jar_local_path = self._local_goe_listener_jar()
            spark_listener_jar_remote_path = self._remote_copy_transport_file(
                spark_listener_jar_local_path, self._offload_transport_cmd_host
            )
        else:
            spark_listener_jar_remote_path = None
        self.log("Written PySpark file: %s" % options_file_remote_path, detail=VVERBOSE)

        remote_spark_files_csv = self._remote_copy_transport_file_csv(
            self._spark_files_csv, self._offload_transport_cmd_host
        )
        remote_spark_jars_csv = self._remote_copy_transport_file_csv(
            self._spark_jars_csv, self._offload_transport_cmd_host
        )

        if spark_listener_jar_remote_path:
            if remote_spark_jars_csv:
                remote_spark_jars_csv = (
                    f"{spark_listener_jar_remote_path},{remote_spark_jars_csv}"
                )
            else:
                remote_spark_jars_csv = spark_listener_jar_remote_path

        spark_submit_binary = self._derive_spark_submit_cmd_arg()
        spark_master = (
            self._offload_transport_spark_submit_master_url
            if self._standalone_spark()
            else SPARK_SUBMIT_SPARK_YARN_MASTER
        )
        spark_master_opt = ["--master", spark_master] if spark_master else []
        spark_config_props, no_log_password = [], []
        [
            spark_config_props.extend(
                ["--conf", "%s=%s" % (k, cli_safe_the_password(k, v))]
            )
            for k, v in self._spark_config_properties.items()
        ]
        if (
            "spark.jdbc.password" in self._spark_config_properties
            and not self._offload_transport_password_alias
        ):
            # If the rdbms app password is visible in the CLI then obscure it from any logging
            password_config_to_obscure = (
                "spark.jdbc.password=%s"
                % cli_safe_the_password(
                    "spark.jdbc.password",
                    self._spark_config_properties["spark.jdbc.password"],
                )
            )
            no_log_password = [{"item": password_config_to_obscure, "prior": "--conf"}]
        if (
            "spark.cores.max" not in self._spark_config_properties
            and self._offload_transport_parallelism
            and spark_master != SPARK_SUBMIT_SPARK_YARN_MASTER
        ):
            # If the user has not configured spark.cores.max then default to offload_transport_parallelism.
            # This only applies to spark-submit and therefore is not injected into self._spark_config_properties.
            spark_config_props.extend(
                [
                    "--conf",
                    "spark.cores.max={}".format(
                        str(self._offload_transport_parallelism)
                    ),
                ]
            )

        driver_memory_opt = self._option_from_properties(
            "--driver-memory", "spark.driver.memory"
        )
        executor_memory_opt = self._option_from_properties(
            "--executor-memory", "spark.executor.memory"
        )

        jvm_opts = (
            ["--driver-java-options", """%s""" % self._offload_transport_jvm_overrides]
            if self._offload_transport_jvm_overrides
            else []
        )
        jars_opt = ["--jars", remote_spark_jars_csv] if remote_spark_jars_csv else []
        files_opt = (
            ["--files", remote_spark_files_csv] if remote_spark_files_csv else []
        )
        spark_submit_cmd = (
            spark_submit_binary
            + jvm_opts
            + spark_master_opt
            + driver_memory_opt
            + executor_memory_opt
            + jars_opt
            + files_opt
            + spark_config_props
        )
        if self._offload_options.vverbose:
            spark_submit_cmd.append("--verbose")
        if self._offload_transport_queue_name:
            spark_submit_cmd.append("--queue=%s" % self._offload_transport_queue_name)

        spark_submit_cmd.append(options_file_remote_path)
        return spark_submit_cmd, no_log_password, py_rm_commands

    def _spark_submit_import(self, partition_chunk=None):
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        rows_imported = None
        pyspark_body = self._get_pyspark_body(partition_chunk)
        (
            spark_submit_cmd,
            no_log_password,
            py_rm_commands,
        ) = self._get_spark_submit_command(pyspark_body)

        self._start_validation_polling_thread()
        rc, cmd_out = self._run_os_cmd(
            self._ssh_cmd_prefix() + spark_submit_cmd, no_log_items=no_log_password
        )
        self._stop_validation_polling_thread()

        if not self._dry_run:
            rows_imported = self._get_rows_imported_from_spark_log(cmd_out)
            rows_imported_from_sql_stats = self._rdbms_api.log_sql_stats(
                self._rdbms_module,
                self._rdbms_action,
                self._drain_validation_polling_thread_queue(),
                validation_polling_interval=self._validation_polling_interval,
            )
            if rows_imported is None:
                if rows_imported_from_sql_stats is None:
                    self.warning(MISSING_ROWS_SPARK_WARNING)
                else:
                    self.warning(
                        f"{MISSING_ROWS_SPARK_WARNING}, falling back on RDBMS SQL statistics"
                    )
                    rows_imported = rows_imported_from_sql_stats

        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _verify_rdbms_connectivity(self):
        """Use a simple canary query for verification test"""
        rdbms_source_query = "(%s) v" % self._rdbms_api.get_rdbms_canary_query()
        pyspark_body = self._get_pyspark_body(canary_query=rdbms_source_query)
        self.log("PySpark: " + pyspark_body, detail=VVERBOSE)
        (
            spark_submit_cmd,
            no_log_password,
            py_rm_commands,
        ) = self._get_spark_submit_command(pyspark_body)
        rc, cmd_out = self._run_os_cmd(
            self._ssh_cmd_prefix() + spark_submit_cmd, no_log_items=no_log_password
        )
        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]
        # if we got this far then we're in good shape
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(
        self, partition_chunk: Optional["OffloadSourcePartitions"] = None
    ) -> Union[int, None]:
        """Spark by spark-submit transport"""
        self._reset_transport_context()

        def step_fn():
            row_count = self._spark_submit_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count

        return self._messages.offload_step(
            command_steps.STEP_STAGING_TRANSPORT, step_fn, execute=(not self._dry_run)
        )

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkSubmitCanary(OffloadTransportSparkSubmit):
    """Validate Spark Submit connectivity"""

    def __init__(self, offload_options, messages):
        """CONSTRUCTOR
        This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        self._spark_submit_executable = (
            offload_options.offload_transport_spark_submit_executable
        )
        assert self._spark_submit_executable
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = (
            orchestration_defaults.bool_option_from_string(
                "OFFLOAD_TRANSPORT_CONSISTENT_READ",
                orchestration_defaults.offload_transport_consistent_read_default(),
            )
        )
        self._offload_transport_fetch_size = (
            orchestration_defaults.offload_transport_fetch_size_default()
        )
        self._offload_transport_jvm_overrides = (
            orchestration_defaults.offload_transport_spark_overrides_default()
        )
        self._offload_transport_queue_name = (
            orchestration_defaults.offload_transport_spark_queue_name_default()
        )
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = (
            orchestration_defaults.offload_transport_validation_polling_interval_default()
        )
        self._spark_config_properties = self._prepare_spark_config_properties(
            orchestration_defaults.offload_transport_spark_properties_default()
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(
            "dummy_owner",
            "dummy_table",
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        self._offload_transport_spark_submit_master_url = (
            offload_options.offload_transport_spark_submit_master_url
        )
        self._spark_files_csv = offload_options.offload_transport_spark_files
        self._spark_jars_csv = offload_options.offload_transport_spark_jars

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportQueryImport(OffloadTransport):
    """Use Python to transport data, only suitable for small chunks"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
        super(OffloadTransportQueryImport, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )
        self._offload_transport_parallelism = 1
        # Cap fetch size at 1000
        self._offload_transport_fetch_size = min(
            int(self._offload_transport_fetch_size), 1000
        )
        self._convert_nans_to_nulls = convert_nans_to_nulls(
            offload_target_table, offload_operation
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _column_type_read_remappings(self):
        # Not applicable to Query Import
        return None

    def _query_import_to_local_fs(self, partition_chunk=None) -> tuple:
        """Execute Query Import transport.

        Query Import is not partition aware therefore partition_chunk is ignored"""

        if self._nothing_to_do(partition_chunk):
            return 0

        local_staging_path = get_local_staging_path(
            self._target_owner,
            self._target_table_name,
            self._offload_options,
            "." + self._staging_format.lower(),
        )
        dfs_load_path = os.path.join(
            self._staging_table_location, "part-m-00000." + self._staging_format.lower()
        )
        qi_fetch_size = (
            self._offload_transport_fetch_size
            if self._fetchmany_takes_fetch_size
            else None
        )

        staging_columns = self._staging_file.get_staging_columns()

        if self._offload_transport_consistent_read:
            self.log(
                "Ignoring --offload-transport-consistent-read for serial transport task",
                detail=VVERBOSE,
            )

        colexpressions, colnames = self._build_offload_query_lists(
            convert_expressions_on_rdbms_side=self._rdbms_api.convert_query_import_expressions_on_rdbms_side(),
            for_qi=True,
        )

        sql_projection = self._sql_projection_from_offload_query_expression_list(
            colexpressions, colnames
        )
        table_name = ('"%s"."%s"' % (self._rdbms_owner, self._rdbms_table_name)).upper()
        if partition_chunk:
            split_row_source_by = self._get_transport_split_type(partition_chunk)
            row_source = self._get_transport_row_source_query(
                split_row_source_by, partition_chunk
            )
            source_query = "SELECT %s\nFROM (%s)" % (sql_projection, row_source)
        else:
            source_query = "SELECT %s\nFROM   %s" % (sql_projection, table_name)
        if self._rdbms_offload_predicate:
            source_query += (
                "\nWHERE (%s)"
                % self._rdbms_table.predicate_to_where_clause(
                    self._rdbms_offload_predicate
                )
            )
        source_binds = None

        self._refresh_rdbms_action()
        rows_imported = None
        if self._dry_run:
            self.log("Extraction sql: %s" % source_query, detail=VERBOSE)
        else:
            encoder = query_import_factory(
                self._staging_file,
                self._messages,
                compression=self._compress_load_table,
                base64_columns=self._base64_staged_columns(),
            )
            with self._rdbms_api.query_import_extraction(
                staging_columns,
                source_query,
                source_binds,
                self._offload_transport_fetch_size,
                self._compress_load_table,
                self._get_rdbms_session_setup_commands(),
            ) as rdbms_cursor:
                rows_imported = encoder.write_from_cursor(
                    local_staging_path, rdbms_cursor, self._rdbms_columns, qi_fetch_size
                )

        self._check_rows_imported(rows_imported)
        return rows_imported, local_staging_path, dfs_load_path

    def _query_import_copy_to_dfs(self, local_staging_path, dfs_load_path):
        rm_local_file = ["rm", "-f", local_staging_path]
        # Simulate Sqoop's use of recreate load dir
        self.log_dfs_cmd(
            'copy_from_local("%s", "%s")' % (local_staging_path, dfs_load_path)
        )
        if not self._dry_run:
            self._dfs_client.copy_from_local(
                local_staging_path, dfs_load_path, overwrite=True
            )
            self._run_os_cmd(rm_local_file)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(
        self, partition_chunk: Optional["OffloadSourcePartitions"] = None
    ) -> Union[int, None]:
        """Run the data transport"""
        self._reset_transport_context()

        def step_fn():
            return_values = self._query_import_to_local_fs(partition_chunk)
            if return_values:
                rows_imported, local_staging_path, dfs_load_path = return_values
                self._query_import_copy_to_dfs(local_staging_path, dfs_load_path)
                staged_bytes = self._check_and_log_transported_files(rows_imported)
                self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
                self._transport_context[TRANSPORT_CXT_ROWS] = rows_imported
                self._target_table.post_transport_tasks(self._staging_file)
                return rows_imported

        rows_imported = self._messages.offload_step(
            command_steps.STEP_STAGING_TRANSPORT, step_fn, execute=(not self._dry_run)
        )
        return rows_imported

    def ping_source_rdbms(self):
        raise NotImplementedError("ping_source_rdbms not implemented for QueryImport")
