#! /usr/bin/env python3
""" OffloadTransport: Library for offloading data from an RDBMS frontend to a Hadoop backend
    LICENSE_TEXT
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime
import json
import logging
import os
import re
from socket import gethostname
from textwrap import dedent
import time
import traceback
from typing import Union

from goe.config import orchestration_defaults
from goe.offload.column_metadata import match_table_column
from goe.offload.offload_constants import LIVY_MAX_SESSIONS
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.factory.query_import_factory import query_import_factory
from goe.offload.factory.offload_transport_rdbms_api_factory import offload_transport_rdbms_api_factory
from goe.offload.factory.staging_file_factory import staging_file_factory
from goe.offload.frontend_api import FRONTEND_TRACE_MODULE
from goe.offload.offload_constants import (
    BACKEND_DISTRO_GCP,
    DBTYPE_MSSQL, DBTYPE_ORACLE, DBTYPE_NETEZZA, DBTYPE_SPARK,
    DBTYPE_SYNAPSE, DBTYPE_TERADATA,
    FILE_STORAGE_FORMAT_AVRO, FILE_STORAGE_FORMAT_PARQUET,
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
    OFFLOAD_TRANSPORT_AUTO,
    OFFLOAD_TRANSPORT_GLUENT,
    OFFLOAD_TRANSPORT_GCP,
    OFFLOAD_TRANSPORT_SQOOP,
    OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_TIMESTAMP_TZ,
    ORACLE_TYPE_INTERVAL_YM,
    ORACLE_TYPE_XMLTYPE
)
from goe.offload.offload_transport_functions import credential_provider_path_jvm_override, \
    finish_progress_on_stdout, hs2_connection_log_message, \
    run_os_cmd, running_as_same_user_and_host, scp_to_cmd, ssh_cmd_prefix, schema_paths, write_progress_to_stdout
from goe.offload.offload_transport_livy_requests import OffloadTransportLivyRequests
from goe.offload.offload_transport_rdbms_api import TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN
from goe.offload.offload_xform_functions import apply_transformation
from goe.offload.operation.data_type_controls import char_semantics_override_map
from goe.orchestration import command_steps

from goe.filesystem.gluent_dfs import DFS_TYPE_FILE
from goe.filesystem.gluent_dfs_factory import get_dfs_from_options

from goe.util.misc_functions import ansi_c_string_safe, bytes_to_human_size, get_os_username, get_temp_path, \
    id_generator, split_not_in_quotes, write_temp_file
from goe.util.polling_thread import PollingThread


class OffloadTransportException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# The specific methods used to transport data
OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT = 'QUERY_IMPORT'
OFFLOAD_TRANSPORT_METHOD_SQOOP = 'SQOOP'
OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY = 'SQOOP_BY_QUERY'
OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT = 'SPARK_THRIFT'
OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT = 'SPARK_SUBMIT'
OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY = 'SPARK_LIVY'
OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD = 'SPARK_DATAPROC_GCLOUD'
OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD = 'SPARK_BATCHES_GCLOUD'
VALID_OFFLOAD_TRANSPORT_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SQOOP, OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT, OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY, OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
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
    OFFLOAD_TRANSPORT_GLUENT,
    OFFLOAD_TRANSPORT_GCP,
    OFFLOAD_TRANSPORT_SQOOP
]
# Transport methods for the Gluent rule set
OFFLOAD_TRANSPORT_GLUENT_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
]
# Transport methods for the Sqoop rule set
OFFLOAD_TRANSPORT_SQOOP_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY
]
# Transport methods for the GCP rule set
OFFLOAD_TRANSPORT_GCP_METHODS = [
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD
]

YARN_TRANSPORT_NAME = 'GOE'

MISSING_ROWS_IMPORTED_WARNING = 'Unable to identify import record count'

# Sqoop constants
SQOOP_OPTIONS_FILE_PREFIX = 'gl-sqoop-options-'
SQOOP_LOG_ROW_COUNT_PATTERN = r'^.*mapreduce\.ImportJobBase: Retrieved (\d+) records\.\r?$'
SQOOP_LOG_MODULE_PATTERN = r"^.*dbms_application_info.set_module\(module_name => '(.+)', action_name => '(.+)'\)"

# Spark constants
SPARK_OPTIONS_FILE_PREFIX = 'gl-spark-options-'

SPARK_THRIFT_SERVER_PROPERTY_EXCLUDE_LIST = ['spark.extraListeners', 'spark.driver.memory', 'spark.executor.memory']

SPARK_SUBMIT_SPARK_YARN_MASTER = 'yarn'
VALID_SPARK_SUBMIT_EXECUTABLES = ['spark-submit', 'spark2-submit']
MISSING_ROWS_SPARK_WARNING = 'Import record count not identified from Spark'
OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE = 'gcloud'
GCLOUD_PROPERTY_SEPARATOR = ',GSEP,'

URL_SEP = '/'
LIVY_SESSIONS_SUBURL = 'sessions'
LIVY_LOG_QUEUE_PATTERN = r'^\s*queue:\s*%s$'
# Seconds between looped checks when creating a session
LIVY_CONNECT_POLL_DELAY = 2
# Number of polls when creating a session.
# i.e. we'll wait LIVY_CONNECT_MAX_POLLS * LIVY_CONNECT_POLL_DELAY seconds
LIVY_CONNECT_MAX_POLLS = 30
# Seconds to allow between looped checks when running a Spark job
LIVY_STATEMENT_POLL_DELAY = 5

# Used for test assertions
POLLING_VALIDATION_TEXT = 'Calculating offload transport source row counts from %s snapshots'

# Used for scraping Spark log messages to find transport row count
# Example logger messages:
# 21/08/16 14:07:25 INFO GOETaskListener: {"taskInfo.id":"1.0","taskInfo.taskId":1,"taskInfo.launchTime":1629122840262,"taskInfo.finishTime":1629122845656,"duration":5394,"recordsWritten":0,"executorRunTime":3952}
# 21/08/16 14:07:27 INFO GOETaskListener: {"taskInfo.id":"0.0","taskInfo.taskId":0,"taskInfo.launchTime":1629122840224,"taskInfo.finishTime":1629122847797,"duration":7573,"recordsWritten":145,"executorRunTime":6191}
SPARK_LOG_ROW_COUNT_PATTERN = r'^.* GOETaskListener: .*"recordsWritten":(\d+).*}\r?$'

GOE_LISTENER_NAME = 'GOETaskListener'
GOE_LISTENER_JAR = 'goe-spark-listener.jar'

TRANSPORT_CXT_BYTES = 'staged_bytes'
TRANSPORT_CXT_ROWS = 'staged_rows'

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################

def spark_submit_executable_exists(offload_options, messages, executable_override=None):
    spark_submit_executable = executable_override or offload_options.offload_transport_spark_submit_executable
    cmd = ['which', spark_submit_executable]
    cmd = ssh_cmd_prefix(offload_options.offload_transport_user, offload_options.offload_transport_cmd_host) + cmd
    rc, _ = run_os_cmd(cmd, messages, offload_options, optional=True, force=True, silent=True)
    return bool(rc == 0)


def is_spark_thrift_available(offload_options, offload_source_table, messages=None):
    """ If messages is passed in then we'll log any reason for a False return
    """
    if not offload_options.offload_transport_spark_thrift_host or not offload_options.offload_transport_spark_thrift_port:
        if messages:
            messages.log('OFFLOAD_TRANSPORT_SPARK_THRIFT_HOST and OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT required for transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT, detail=VVERBOSE)
        return False
    else:
        return True


def is_livy_available(offload_options, offload_source_table, messages=None):
    """ If messages is passed in then we'll log any reason for a False return
    """
    if not offload_options.offload_transport_livy_api_url:
        messages.log('OFFLOAD_TRANSPORT_LIVY_API_URL required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY, detail=VVERBOSE) if messages else None
        return False
    else:
        return True


def is_spark_submit_available(offload_options, offload_source_table, messages=None, executable_override=None):
    """ If messages is passed in then we'll log any reason for a False return
        If messages is not passed in then we can't yet run OS cmds, hence no spark_submit_executable_exists()
    """
    if not offload_options.offload_transport_cmd_host:
        messages.log('OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT, detail=VVERBOSE) if messages else None
        return False
    elif not offload_options.offload_transport_spark_submit_executable:
        messages.log('OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT, detail=VVERBOSE) if messages else None
        return False
    elif messages and not spark_submit_executable_exists(offload_options, messages, executable_override=executable_override):
        messages.log('Spark submit executable required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT, detail=VVERBOSE)
        return False
    else:
        return True


def is_spark_gcloud_available(offload_options, offload_source_table, messages=None):
    """ If messages is passed in then we'll log any reason for a False return
        If messages is not passed in then we can't yet run OS cmds, hence no spark_submit_executable_exists()
    """
    if not offload_options.offload_transport_cmd_host:
        messages.log('OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD, detail=VVERBOSE) if messages else None
        return False
    if offload_options.backend_distribution != BACKEND_DISTRO_GCP:
        messages.log('BACKEND_DISTRIBUTION not valid for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD, detail=VVERBOSE) if messages else None
        return False
    elif messages and not spark_submit_executable_exists(offload_options, messages,
                                                         executable_override=OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE):
        messages.log('gcloud executable required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD, detail=VVERBOSE)
        return False
    else:
        return True


def is_spark_gcloud_dataproc_available(offload_options, offload_source_table, messages=None):
    return bool(
        offload_options.google_dataproc_cluster and
        is_spark_gcloud_available(offload_options, offload_source_table, messages=messages)
    )


def is_spark_gcloud_batches_available(offload_options, offload_source_table, messages=None):
    return bool(
        offload_options.google_dataproc_batches_version and
        is_spark_gcloud_available(offload_options, offload_source_table, messages=messages)
    )


def is_query_import_available(offload_operation, offload_options, offload_source_table=None, messages=None):
    """ If messages is passed in then we'll log any reason for a False return
        Query Import can only be validated if operational config is passed in. Without that we just assume True
    """
    def log(msg, messages):
        if messages:
            messages.log(msg, detail=VVERBOSE)

    if offload_options.offload_staging_format not in [FILE_STORAGE_FORMAT_AVRO, FILE_STORAGE_FORMAT_PARQUET]:
        log('Staging file format %s not supported by transport method: %s'
            % (offload_options.offload_staging_format, OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT),
            messages)
        return False

    if offload_source_table:
        if ORACLE_TYPE_INTERVAL_YM in offload_source_table.data_types_in_use():
            log('INTERVAL YEAR data type is not valid for transport method: %s'
                % OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
                messages)
            return False
        if offload_operation and (offload_source_table.size_in_bytes or 0) > offload_operation.offload_transport_small_table_threshold:
            log('Table size (%s) > %s (OFFLOAD_TRANSPORT_SMALL_TABLE_THRESHOLD) is not valid with transport method: %s'
                % (offload_source_table.size_in_bytes or 0, offload_operation.offload_transport_small_table_threshold, OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT),
                messages)
            return False

    return True


def is_sqoop_available(offload_operation, offload_options, offload_source_table=None, messages=None):
    """ If messages is passed in then we'll log any reason for a False return
        Sqoop can only be validated if operational config is passed in. Without that we just assume True
    """
    if offload_options.backend_distribution and offload_options.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        if messages:
            messages.log('Transport method only valid on Hadoop systems: %s/%s'
                         % (OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY, offload_options.backend_distribution),
                         detail=VVERBOSE)
        return False
    elif not offload_options.offload_transport_cmd_host:
        if messages:
            messages.log('OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SQOOP, detail=VVERBOSE)
        return False
    elif offload_operation and offload_operation.offload_by_subpartition:
        if messages:
            messages.log('Subpartition level offloads are not valid with transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SQOOP, detail=VVERBOSE)
        return False
    elif offload_operation and offload_operation.column_transformations:
        if messages:
            messages.log('Column transformations are not valid with transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SQOOP, detail=VVERBOSE)
        return False
    elif offload_operation and offload_operation.offload_predicate:
        if messages:
            messages.log('Predicate offloads are not valid with transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SQOOP, detail=VVERBOSE)
        return False
    elif offload_source_table and ORACLE_TYPE_TIMESTAMP_TZ in offload_source_table.data_types_in_use():
        if messages:
            messages.log('TIMESTAMP WITH TIME ZONE offloads are not valid with transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SQOOP, detail=VVERBOSE)
        return False
    elif offload_source_table and ORACLE_TYPE_XMLTYPE in offload_source_table.data_types_in_use():
        if messages:
            messages.log('XMLTYPE offloads are not valid with transport method: %s'
                         % OFFLOAD_TRANSPORT_METHOD_SQOOP, detail=VVERBOSE)
        return False
    elif not offload_operation:
        return True
    else:
        return True


def is_sqoop_by_query_available(offload_options, messages=None):
    """ If messages is passed in then we'll log any reason for a False return
    """
    if offload_options.backend_distribution and offload_options.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        messages.log('Transport method only valid on Hadoop systems: %s/%s'
                     % (OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY, offload_options.backend_distribution),
                     detail=VVERBOSE) if messages else None
        return False
    elif not offload_options.offload_transport_cmd_host:
        messages.log('OFFLOAD_TRANSPORT_CMD_HOST required for transport method: %s'
                     % OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY, detail=VVERBOSE) if messages else None
        return False
    else:
        return True


def get_offload_transport_method_validation_fns(offload_options, offload_operation=None, offload_source_table=None,
                                                messages=None):
    return {OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT:
                lambda: is_spark_thrift_available(offload_options, offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY:
                lambda: is_livy_available(offload_options, offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT:
                lambda: is_spark_submit_available(offload_options, offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD:
                lambda: is_spark_gcloud_batches_available(offload_options, offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD:
                lambda: is_spark_gcloud_dataproc_available(offload_options, offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT:
                lambda: is_query_import_available(offload_operation, offload_options,
                                                  offload_source_table=offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_SQOOP:
                lambda: is_sqoop_available(offload_operation, offload_options,
                                           offload_source_table=offload_source_table, messages=messages),
            OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY:
                lambda: is_sqoop_by_query_available(offload_options, messages=messages)}


def validate_offload_transport_method(offload_transport_method, offload_options, offload_operation=None,
                                      offload_source_table=None, messages=None,
                                      exception_class=OffloadTransportException):
    """ If a specific method is requested then we can do certain config checks.
        This is not a health check, just confirming we have the config we need.
        Query Import and Sqoop methods can only be validated if optional operation config is passed in.
    """
    assert offload_transport_method
    assert exception_class

    if offload_transport_method not in VALID_OFFLOAD_TRANSPORT_METHODS:
        raise exception_class('Unsupported transport method: %s' % offload_transport_method)

    validation_fn = get_offload_transport_method_validation_fns(offload_options, offload_operation,
                                                                offload_source_table, messages)[offload_transport_method]

    if not validation_fn():
        raise exception_class('Transport method is incompatible with this offload: %s' % offload_transport_method)


def choose_offload_transport_method(offload_operation, offload_source_table, offload_options, messages):
    """ Select the data transport method based on a series of rules:
            AUTO:
                Work through GLUENT rules and then fall back to SQOOP rules
            GLUENT:
                Pick a method using the following priorities: SPARK_THRIFT, QUERY_IMPORT, SPARK_LIVY, SPARK_SUBMIT
            GCP:
                Pick a method using the following priorities: SPARK_DATAPROC_GCLOUD, SPARK_BATCHES_GCLOUD
            SQOOP:
                Pick a method using the following priorities: SQOOP, SQOOP_BY_QUERY
    """
    assert offload_options.offload_transport

    validation_fns = get_offload_transport_method_validation_fns(offload_options, offload_operation,
                                                                 offload_source_table, messages)

    if offload_options.db_type in [DBTYPE_ORACLE, DBTYPE_TERADATA]:
        if offload_operation.offload_transport_method:
            # An override was supplied
            rules = [offload_operation.offload_transport_method]
        elif offload_options.offload_transport == OFFLOAD_TRANSPORT_AUTO:
            rules = (OFFLOAD_TRANSPORT_GLUENT_METHODS +
                     OFFLOAD_TRANSPORT_SQOOP_METHODS +
                     OFFLOAD_TRANSPORT_GCP_METHODS)
        elif offload_options.offload_transport == OFFLOAD_TRANSPORT_GLUENT:
            rules = OFFLOAD_TRANSPORT_GLUENT_METHODS
        elif offload_options.offload_transport == OFFLOAD_TRANSPORT_SQOOP:
            rules = OFFLOAD_TRANSPORT_SQOOP_METHODS
        elif offload_options.offload_transport == OFFLOAD_TRANSPORT_GCP:
            rules = OFFLOAD_TRANSPORT_GCP_METHODS

        for rule in rules:
            if validation_fns[rule]():
                return rule

        # if we get to here then no rule was accepted
        raise OffloadTransportException('No valid transport methods found for offload transport: %s'
                                        % offload_options.offload_transport)
    elif offload_options.db_type == DBTYPE_MSSQL:
        return OFFLOAD_TRANSPORT_METHOD_SQOOP
    elif offload_options.db_type == DBTYPE_NETEZZA:
        return OFFLOAD_TRANSPORT_METHOD_SQOOP
    else:
        raise OffloadTransportException('Unsupported DB type: %s' % offload_options.db_type)


def offload_transport_factory(offload_transport_method, offload_source_table, offload_target_table, offload_operation,
                              offload_options, messages, dfs_client, incremental_update_extractor=None,
                              rdbms_columns_override=None):
    """ Constructs and returns an appropriate data transport object based on user inputs and RDBMS table
    """
    if offload_transport_method == OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT:
        messages.log('Data transport method: OffloadTransportQueryImport', detail=VVERBOSE)
        return OffloadTransportQueryImport(offload_source_table, offload_target_table, offload_operation,
                                           offload_options, messages, dfs_client, incremental_update_extractor,
                                           rdbms_columns_override=rdbms_columns_override)
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SQOOP:
        messages.log('Data transport method: OffloadTransportStandardSqoop', detail=VVERBOSE)
        return OffloadTransportStandardSqoop(offload_source_table, offload_target_table, offload_operation,
                                             offload_options, messages, dfs_client, incremental_update_extractor,
                                             rdbms_columns_override=rdbms_columns_override)
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY:
        messages.log('Data transport method: OffloadTransportSqoopByQuery', detail=VVERBOSE)
        return OffloadTransportSqoopByQuery(offload_source_table, offload_target_table, offload_operation,
                                            offload_options, messages, dfs_client, incremental_update_extractor,
                                            rdbms_columns_override=rdbms_columns_override)
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT:
        messages.log('Data transport method: OffloadTransportSparkThrift', detail=VVERBOSE)
        return OffloadTransportSparkThrift(offload_source_table, offload_target_table, offload_operation,
                                           offload_options, messages, dfs_client, incremental_update_extractor,
                                           rdbms_columns_override=rdbms_columns_override)
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT:
        messages.log('Data transport method: OffloadTransportSparkSubmit', detail=VVERBOSE)
        return OffloadTransportSparkSubmit(
            offload_source_table, offload_target_table, offload_operation,
            offload_options, messages, dfs_client, incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD:
        messages.log('Data transport method: OffloadTransportSparkDataprocGcloud', detail=VVERBOSE)
        return OffloadTransportSparkDataprocGcloud(
            offload_source_table, offload_target_table, offload_operation,
            offload_options, messages, dfs_client, incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD:
        messages.log('Data transport method: OffloadTransportSparkBatchesGcloud', detail=VVERBOSE)
        return OffloadTransportSparkBatchesGcloud(
            offload_source_table, offload_target_table, offload_operation,
            offload_options, messages, dfs_client, incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY:
        messages.log('Data transport method: OffloadTransportSparkLivy', detail=VVERBOSE)
        return OffloadTransportSparkLivy(offload_source_table, offload_target_table, offload_operation,
                                         offload_options, messages, dfs_client, incremental_update_extractor,
                                         rdbms_columns_override=rdbms_columns_override)
    else:
        raise NotImplementedError('Offload transport method not implemented: %s' % offload_transport_method)


def spark_thrift_jdbc_connectivity_checker(offload_options, messages):
    """ Connect needs a cut down client to simply check RDBMS connectivity from Spark
        back to the source RDBMS is correctly configured
    """
    messages.log('Invoking OffloadTransportSparkThriftCanary', detail=VVERBOSE)
    return OffloadTransportSparkThriftCanary(offload_options, messages)


def spark_submit_jdbc_connectivity_checker(offload_options, messages):
    """ Connect needs a cut down client to simply check RDBMS connectivity from Spark
        back to the source RDBMS is correctly configured
    """
    messages.log('Invoking OffloadTransportSparkSubmitCanary', detail=VVERBOSE)
    return OffloadTransportSparkSubmitCanary(offload_options, messages)


def spark_livy_jdbc_connectivity_checker(offload_options, messages):
    """ Connect needs a cut down client to simply check RDBMS connectivity from Spark
        back to the source RDBMS is correctly configured
    """
    messages.log('Invoking OffloadTransportSparkLivyCanary', detail=VVERBOSE)
    return OffloadTransportSparkLivyCanary(offload_options, messages)


def spark_dataproc_jdbc_connectivity_checker(offload_options, messages):
    """ Connect needs a cut down client to simply check RDBMS connectivity from Dataproc
        back to the source RDBMS is correctly configured
    """
    messages.log('Invoking OffloadTransportSparkDataprocGcloudCanary', detail=VVERBOSE)
    return OffloadTransportSparkDataprocGcloudCanary(offload_options, messages)


def spark_dataproc_batches_jdbc_connectivity_checker(offload_options, messages):
    """ Connect needs a cut down client to simply check RDBMS connectivity from Dataproc Batches
        back to the source RDBMS is correctly configured
    """
    messages.log('Invoking OffloadTransportSparkBatchesGcloudCanary', detail=VVERBOSE)
    return OffloadTransportSparkBatchesGcloudCanary(offload_options, messages)


def sqoop_jdbc_connectivity_checker(offload_options, messages):
    """ Connect needs a cut down client to simply check RDBMS connectivity from Sqoop
        back to the source RDBMS is correctly configured
    """
    messages.log('Invoking OffloadTransportSqoopCanary', detail=VVERBOSE)
    return OffloadTransportSqoopCanary(offload_options, messages)


def derive_rest_api_verify_value_from_url(url):
    """ Define whether to verify SSL certificates:
            False: Use SSL but no verification
            None: No SSL
    """
    if url and url[:6] == 'https:':
        return False
    else:
        return None


def convert_nans_to_nulls(offload_target_table, offload_operation):
    """ If the backend does not support NaN values and the --allow-floating-point-conversions parameter is set,
        we must convert any 'NaN','Infinity','-Infinity' values in RDBMS nan capable columns to NULL
    """
    return bool(not offload_target_table.nan_supported() and offload_operation.allow_floating_point_conversions)


def get_offload_transport_value(offload_transport_method, dedicated_value, sqoop_specific_value, spark_specific_value):
    if dedicated_value:
        return dedicated_value
    elif offload_transport_method in OFFLOAD_TRANSPORT_SQOOP_METHODS and sqoop_specific_value:
        return sqoop_specific_value
    elif offload_transport_method in OFFLOAD_TRANSPORT_SPARK_METHODS and spark_specific_value:
        return spark_specific_value
    else:
        return None


class OffloadTransport(object, metaclass=ABCMeta):
    """ Interface for classes transporting data from an RDBMS frontend to storage that can be accessed by a backend.
        Overloads by different transport methods, such a Spark, Sqoop, etc
        (abstract parent class)
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation, offload_options,
                 messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        # not decided how to deal with offload_options, below is a temporary measure
        self._offload_options = offload_options
        self._messages = messages
        self._dfs_client = dfs_client
        self._incremental_update_extractor = incremental_update_extractor
        self._backend_dfs = self._dfs_client.backend_dfs
        self._dry_run = bool(not offload_options.execute)
        # Details of the source of the offload
        self._rdbms_owner = offload_source_table.owner
        self._rdbms_table_name = offload_source_table.table_name
        self._rdbms_columns = rdbms_columns_override or offload_source_table.columns
        self._rdbms_is_iot = offload_source_table.is_iot()
        self._rdbms_partition_type = offload_source_table.partition_type
        self._rdbms_pk_cols = offload_source_table.get_primary_key_columns()
        self._fetchmany_takes_fetch_size = offload_source_table.fetchmany_takes_fetch_size()
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
        self._offload_transport_consistent_read = offload_operation.offload_transport_consistent_read
        self._offload_transport_fetch_size = offload_operation.offload_transport_fetch_size
        self._offload_transport_queue_name = get_offload_transport_value(
            self._offload_transport_method, offload_operation.offload_transport_queue_name,
            offload_options.sqoop_queue_name, offload_options.offload_transport_spark_queue_name
        )
        self._offload_transport_jvm_overrides = get_offload_transport_value(
            self._offload_transport_method, offload_operation.offload_transport_jvm_overrides,
            offload_options.sqoop_overrides, offload_options.offload_transport_spark_overrides
        )
        self._offload_transport_parallelism = offload_operation.offload_transport_parallelism
        self._validation_polling_interval = offload_operation.offload_transport_validation_polling_interval
        self._create_basic_connectivity_attributes(offload_options)
        self._transport_context = {}
        # For a full list of properties see:
        #   http://spark.apache.org/docs/latest/configuration.html#available-properties
        self._spark_config_properties = self._prepare_spark_config_properties(offload_operation.offload_transport_spark_properties)
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
            self._load_db_name, self._load_table_name, self._staging_format, self._get_canonical_columns(),
            offload_options, bool(self._target_table and self._target_table.transport_binary_data_in_base64()),
            messages, staging_incremental_update=self._staging_incremental_update(), dry_run=self._dry_run
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(self._rdbms_owner, self._rdbms_table_name,
                                                              self._offload_options, self._messages,
                                                              self._incremental_update_extractor,
                                                              dry_run=self._dry_run)

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _base64_staged_columns(self):
        """ Return a list of columns that should be staged as Base64.
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
        """ Map source RDBMS columns to canonical columns so they can then be converted to staging columns
        """
        if not self._canonical_columns:
            assume_all_staging_columns_nullable = bool(self._offload_transport_method in OFFLOAD_TRANSPORT_SQOOP_METHODS)
            char_semantics_map = char_semantics_override_map(self._unicode_string_columns_csv,
                                                             self._rdbms_columns)
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
        self._offload_transport_auth_using_oracle_wallet = offload_options.offload_transport_auth_using_oracle_wallet
        self._offload_transport_cmd_host = offload_options.offload_transport_cmd_host
        self._offload_transport_credential_provider_path = offload_options.offload_transport_credential_provider_path
        self._offload_transport_dsn = offload_options.offload_transport_dsn
        self._offload_transport_password_alias = offload_options.offload_transport_password_alias
        self._offload_transport_rdbms_session_parameters = json.loads(offload_options.offload_transport_rdbms_session_parameters) \
            if offload_options.offload_transport_rdbms_session_parameters else {}
        self._spark_thrift_host = offload_options.offload_transport_spark_thrift_host
        self._spark_thrift_port = offload_options.offload_transport_spark_thrift_port

    def _get_transport_app_name(self, sep='.', ts=False, name_override=None) -> str:
        """ Get a name suitable for the transport method in use.
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
        """ If partition_chunk is None then we are offloading everything
            If partition_chunk is of type OffloadSourcePartitions but with no partitions then there's nothing to do
        """
        if partition_chunk and partition_chunk.count() == 0:
            self.log('No partitions to export to Hadoop')
            return True
        return False

    def _prepare_spark_config_properties(self, offload_transport_spark_properties):
        spark_config_properties = {}
        if offload_transport_spark_properties:
            spark_config_properties = json.loads(offload_transport_spark_properties)
        if not self._offload_transport_password_alias:
            spark_config_properties.update({'spark.jdbc.password': self._offload_options.rdbms_app_pass})
        return spark_config_properties

    def _reset_transport_context(self):
        self._transport_context = {
            TRANSPORT_CXT_BYTES: None,
            TRANSPORT_CXT_ROWS: None,
        }

    def _ssh_cmd_prefix(self, host=None):
        """ many calls to ssh_cmd_prefix() in this class use std inputs therefore abstract in this method
        """
        host = host or self._offload_transport_cmd_host
        return ssh_cmd_prefix(self._offload_options.offload_transport_user, host)

    def _ssh_cli_safe_value(self, cmd_option_string):
        """ If we are invoking Sqoop via SSH then we need extra protection for special characters on the command line
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
            return datetime.now().strftime('%Y%m%d%H%M%S')
        else:
            return str(datetime.now().replace(microsecond=0))

    def _run_os_cmd(self, cmd, optional=False, no_log_items=[]):
        """ Run an os command and:
              - Throw an exception if the return code is non-zero (and optional=False)
              - Put a dot on stdout for each line logged to give the impression of progress
              - Logs all command output at VVERBOSE level
            silent: No stdout unless vverbose is requested
        """
        return run_os_cmd(cmd, self._messages, self._offload_options, dry_run=self._dry_run, optional=optional,
                          silent=bool(self._offload_options.suppress_stdout), no_log_items=no_log_items)

    def _staging_incremental_update(self):
        return bool(self._incremental_update_extractor)

    def _refresh_rdbms_action(self):
        self._rdbms_action = self._rdbms_api.generate_transport_action()

    def _get_max_ts_scale(self):
        if self._target_table and self._target_table:
            return min(self._target_table.max_datetime_scale(), self._rdbms_table.max_datetime_scale())
        else:
            return None

    def _get_rdbms_session_setup_commands(self, include_fixed_sqoop=True, for_plsql=False,
                                          include_semi_colons=False, escape_semi_colons=False):
        max_ts_scale = self._get_max_ts_scale()
        return self._rdbms_api.get_rdbms_session_setup_commands(
            self._rdbms_module, self._rdbms_action, self._offload_transport_rdbms_session_parameters,
            include_fixed_sqoop=include_fixed_sqoop, for_plsql=for_plsql, include_semi_colons=include_semi_colons,
            escape_semi_colons=escape_semi_colons, max_ts_scale=max_ts_scale
        )

    def _check_rows_imported(self, rows_imported):
        """ Log a warning if we weren't able to identify the number of rows imported
            the main benefit to this is being able to check for the warning during automated tests
        """
        if rows_imported is None and not self._dry_run:
            self._messages.warning(MISSING_ROWS_IMPORTED_WARNING)

    def _remote_copy_transport_control_file(self, options_file_local_path, target_host, prefix='', suffix=''):
        """ Copies a control file to Offload Transport host which will be used to run a command.
            If SSH short-circuit is in play then no copy is made, we just return the name of the local file.
            Returns commands to be used to remove the files, we do this because the control files can contain
            sensitive information so we want to clean them up even though they are in /tmp.
        """
        ssh_user = self._offload_options.offload_transport_user
        self.debug('_remote_copy_transport_control_file(%s, %s, %s)' % (options_file_local_path, ssh_user, target_host))
        rm_commands = [['rm', '-f', options_file_local_path]]
        if running_as_same_user_and_host(ssh_user, target_host):
            self.log('Local host (%s@%s) and remote host (%s@%s) are the same therefore no file copy required'
                     % (get_os_username(), gethostname(), ssh_user, target_host), detail=VVERBOSE)
            options_file_remote_path = options_file_local_path
        else:
            # Copy the control file to the target host
            options_file_remote_path = get_temp_path(prefix=prefix, suffix=suffix)
            self.log('Copying local file (%s) to remote (%s)'
                     % (options_file_local_path, options_file_remote_path), detail=VVERBOSE)
            scp_cmd = scp_to_cmd(ssh_user, target_host, options_file_local_path, options_file_remote_path)
            self._run_os_cmd(scp_cmd)
            rm_commands.append(ssh_cmd_prefix(ssh_user, target_host) + ['rm', '-f', options_file_remote_path])
        return rm_commands, options_file_remote_path

    def _remote_copy_transport_file(self, local_path: str, target_host: str):
        """ Copies a file required by Offload Transport to a remote host.
            If SSH short-circuit is in play then no copy is made, we just return the name of the local file.
            No effort is made to remove the remote file, this is because we put the file in /tmp and we could have a
            situation where concurrent offloads want to use the same file, removing it could cause problems. Copying
            the file to a unique name just so we can remove it seems overkill when the file is in /tmp anyway.
            TODO NJ@2021-08-23 This is very similar to _remote_copy_transport_control_file() but different enough
                 to not want to destabilise _remote_copy_transport_control_file() for a patch release. We probably
                 want to revisit this to try and merge the methods.
        """
        ssh_user = self._offload_options.offload_transport_user
        self.debug('_remote_copy_transport_file(%s, %s, %s)' % (local_path, ssh_user, target_host))
        if running_as_same_user_and_host(ssh_user, target_host):
            self.log('Local host (%s@%s) and remote host (%s@%s) are the same therefore no file copy required'
                     % (get_os_username(), gethostname(), ssh_user, target_host), detail=VVERBOSE)
            remote_path = local_path
        else:
            # Copy the file to the target host
            remote_name = os.path.basename(local_path)
            remote_path = os.path.join('/tmp', remote_name)
            self.log('Copying local file (%s) to remote (%s)'
                     % (local_path, remote_path), detail=VVERBOSE)
            scp_cmd = scp_to_cmd(ssh_user, target_host, local_path, remote_path)
            self._run_os_cmd(scp_cmd)
        return remote_path

    def _remote_copy_transport_files(self, local_paths: list, target_host: str) -> list:
        """Calls _remote_copy_transport_file() for all files in a list and returns new list of paths."""
        remote_paths = []
        for local_path in local_paths:
            if local_path:
                remote_paths.append(self._remote_copy_transport_file(local_path, target_host))
        return remote_paths

    def _remote_copy_transport_file_csv(self, local_path_csv: str, target_host: str) -> str:
        """Calls _remote_copy_transport_files() for all file paths in a CSV and returns new list of paths."""
        if not local_path_csv:
            return local_path_csv
        remote_files = self._remote_copy_transport_files(local_path_csv.split(','), self._offload_transport_cmd_host)
        return ','.join(remote_files)

    def _spark_listener_included_in_config(self):
        """ Used to get truthy True when extra Spark listeners are configured.
            Search is case-insensitive but returns the config name in correct case.
        """
        for k, v in self._spark_config_properties.items():
            if k.lower() == 'spark.extralisteners' and isinstance(v, str) and GOE_LISTENER_NAME in v:
                self.log('%s configured: %s: %s' % (GOE_LISTENER_NAME, k, v), detail=VVERBOSE)
                return k
        return None

    def _credential_provider_path_jvm_override(self):
        return credential_provider_path_jvm_override(self._offload_transport_credential_provider_path)

    def _get_mod_column(self) -> str:
        """ Pick a column suitable for MOD splitting """
        if self._bucket_hash_col:
            return self._bucket_hash_col
        elif self._rdbms_pk_cols:
            return self._rdbms_pk_cols[0]
        else:
            # Return any column
            return self._rdbms_columns[0].name

    def _get_transport_row_source_query(self, partition_by, partition_chunk=None, pad=None):
        # Render predicate into SQL here, because otherwise we'd have to pass in rdbms table as
        # another argument to get_transport_row_source_query.
        if self._rdbms_offload_predicate:
            self.log('Offloading with offload predicate:\n%s' % str(self._rdbms_offload_predicate), detail=VVERBOSE)
        predicate_offload_clause = self._rdbms_table.predicate_to_where_clause(self._rdbms_offload_predicate)
        mod_column = self._get_mod_column()
        return self._rdbms_api.get_transport_row_source_query(partition_by, self._rdbms_table,
                                                              self._offload_transport_consistent_read,
                                                              self._offload_transport_parallelism,
                                                              self._offload_by_subpartition, mod_column,
                                                              predicate_offload_clause,
                                                              partition_chunk=partition_chunk, pad=pad)

    def _get_spark_jdbc_query_text(self, split_column, split_type, partition_chunk, sql_comment=None) -> str:
        """ Adds an outer query around the row source query produced by get_transport_row_source_query.
        """
        self.debug('_get_spark_jdbc_query_text(%s, %s, %s)' % (split_column, split_type, sql_comment))
        colexpressions, colnames = self._build_offload_query_lists(for_spark=True)
        rdbms_sql_projection = self._sql_projection_from_offload_query_expression_list(colexpressions, colnames,
                                                                                       extra_cols=[split_column.upper()])
        row_source_subquery = self._get_transport_row_source_query(split_type, partition_chunk=partition_chunk, pad=6)
        rdbms_source_query = """(SELECT %(row_source_hint)s%(sql_comment)s %(rdbms_projection)s
FROM (%(row_source_subquery)s) %(table_alias)s) v2""" % {
            'row_source_subquery': row_source_subquery,
            'rdbms_projection': rdbms_sql_projection,
            'row_source_hint': self._rdbms_api.get_transport_row_source_query_hint_block(),
            'sql_comment': f' /* {sql_comment} */' if sql_comment else '',
            'table_alias': self._rdbms_table.enclose_identifier(self._rdbms_table_name)
        }
        return rdbms_source_query

    def _get_transport_split_type(self, partition_chunk) -> str:
        split_row_source_by, tuned_parallelism = self._rdbms_api.get_transport_split_type(
            partition_chunk, self._rdbms_table, self._offload_transport_parallelism, self._rdbms_partition_type,
            self._rdbms_columns, self._offload_by_subpartition, self._rdbms_offload_predicate
        )
        if tuned_parallelism and tuned_parallelism != self._offload_transport_parallelism:
            self.notice(
                f'Overriding transport parallelism: {self._offload_transport_parallelism} -> {tuned_parallelism}')
            self._offload_transport_parallelism = tuned_parallelism
        return split_row_source_by

    def _build_offload_query_lists(self, convert_expressions_on_rdbms_side=None, for_spark=False):
        """ Returns a tuple containing:
              A list of column expressions to build an RDBMS projection.
              A list of column names for the projection (useful for aliases if above has anonymous CAST()s),
                this list is the names of the staging columns rather than the original source RDBMS columns.
                Nearly always the same but sometimes not.
        """
        self.debug('_build_offload_query_lists()')
        colnames, colexpressions = [], []
        staging_columns = self._staging_file.get_staging_columns()
        if convert_expressions_on_rdbms_side is None:
            convert_expressions_on_rdbms_side = bool(self._target_table.backend_type() == DBTYPE_SYNAPSE)
        for rdbms_col in self._rdbms_columns:
            cast_column = apply_transformation(self._column_transformations, rdbms_col, self._rdbms_table,
                                               self._messages)
            staging_column = match_table_column(rdbms_col.name, staging_columns)

            colnames.append(staging_column.staging_file_column_name)

            cast_column = cast_column or self._rdbms_table.enclose_identifier(rdbms_col.name)

            cast_column = self._rdbms_api.get_rdbms_query_cast(
                cast_column, rdbms_col, staging_column, self._get_max_ts_scale(),
                convert_expressions_on_rdbms_side=convert_expressions_on_rdbms_side, for_spark=for_spark,
                nan_values_as_null=self._convert_nans_to_nulls
            )
            colexpressions.append(cast_column)

        return colexpressions, colnames

    def _sql_projection_from_offload_query_expression_list(self, colexpressions, colnames, no_newlines=False,
                                                           extra_cols=[]):
        """ Function for formatted results of _build_offload_query_lists() into a SQL projection
        """
        if extra_cols:
            colexpressions = colexpressions + extra_cols
            colnames = colnames + extra_cols
        if no_newlines:
            sql_projection = ','.join(['%s "%s"' % (expr, cname) for expr, cname in zip(colexpressions, colnames)])
        else:
            max_expr_len = max(len(_) for _ in colexpressions)
            project_pattern = '%-' + str(max_expr_len or 32) + 's AS "%s"'
            sql_projection = '\n,      '.join([project_pattern % (expr, cname)
                                               for expr, cname in zip(colexpressions, colnames)])
        return sql_projection

    def _offload_transport_type_remappings(self, return_as_list=True, remap_sep=' '):
        """ Returns a list or string containing a parameter and values, or an empty list/string:
              Sqoop:
                  ['--map-column-java', 'a value for --map-column-java that maps columns appropriately']
              Spark:
                  ['customSchema', 'a value for customSchema that maps columns appropriately']
        """
        assert self._offload_transport_method in (OFFLOAD_TRANSPORT_METHOD_SQOOP,
                                                  OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                                                  OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
                                                  OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                                                  OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
                                                  OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY),\
            f'Transport method {self._offload_transport_method} should not reach this code'

        remap = []
        for col in self._staging_file.get_staging_columns():
            java_primitive = self._staging_file.get_java_primitive(col)
            if java_primitive:
                remap.append((col.staging_file_column_name, java_primitive))

        if remap:
            if self._offload_transport_method in OFFLOAD_TRANSPORT_SQOOP_METHODS:
                remap_strings = ','.join('%s=%s' % (col_name, remap_type) for col_name, remap_type in remap)
                remap_prefix = '--map-column-java'
            elif self._offload_transport_method in OFFLOAD_TRANSPORT_SPARK_METHODS:
                remap_strings = "'%s'" % ','.join('%s %s' % (col_name, remap_type) for col_name, remap_type in remap)
                remap_prefix = 'customSchema'
            else:
                raise NotImplementedError('_offload_transport_type_remappings: unknown remap type %s'
                                          % self._offload_transport_method)

            self.log('Mapped staging columns to Java primitives: %s' % remap_strings, detail=VVERBOSE)

            if return_as_list:
                return [remap_prefix, remap_strings]
            else:
                return remap_prefix + remap_sep + remap_strings
        else:
            return [] if return_as_list else ''

    def _check_and_log_transported_files(self, row_count) -> Union[int, None]:
        """ Check and list contents of target location.
            This interface level method assumes we can list contents via GluentDfs and is particularly useful when
            writing to cloud storage by ensuring filesystems with eventual consistency have made the files visible.
            target_uri: a file or a directory path, this method will only go one level down, we do not recurse.
            row_count: If this is 0 then we don't expect to find any files.
            Individual transport implementations may override this if it is not suitable.
            Returns the collective size in bytes of all files found.
        """
        def log_file_size(path, uri_attribs) -> int:
            self.log('Staged path: %s' % path, detail=VVERBOSE)
            if uri_attribs.get('length') is None:
                size_str = 'Unknown'
            else:
                size_str = bytes_to_human_size(uri_attribs['length'])
            self.log('Staged size: %s' % size_str, detail=VVERBOSE)
            return uri_attribs.get('length')

        if not row_count:
            self.debug('Not logging contents of URI for row count: %s' % row_count)
            return None

        self.debug('Logging contents of URI: %s' % self._staging_table_location)
        if not self._offload_options.execute:
            return None

        try:
            total_size = 0
            for file_path in self._dfs_client.list_dir_and_wait_for_contents(self._staging_table_location):
                uri_attribs = self._dfs_client.stat(file_path)
                self.debug('%s attributes: %s' % (file_path, uri_attribs))
                file_bytes = None
                if uri_attribs['type'] == DFS_TYPE_FILE:
                    file_bytes = log_file_size(file_path, uri_attribs)
                else:
                    for sub_path in self._dfs_client.list_dir(file_path):
                        uri_attribs = self._dfs_client.stat(sub_path)
                        self.debug('%s attributes: %s' % (sub_path, uri_attribs))
                        if uri_attribs['type'] == DFS_TYPE_FILE:
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
        self._messages.log('%s cmd: %s' % (self._backend_dfs, msg), detail=detail)

    def notice(self, msg):
        self._messages.notice(msg)

    def warning(self, msg):
        self._messages.warning(msg)

    def get_transport_bytes(self) -> Union[int, None]:
        """Return a dict used to pass contextual information back from transport()"""
        return self._transport_context.get(TRANSPORT_CXT_BYTES)

    def get_staging_file(self):
        return self._staging_file

    @abstractmethod
    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Run data transport for the implemented transport method and return the number of rows transported."""

    @abstractmethod
    def ping_source_rdbms(self):
        """Test connectivity from the Offload Transport tool back to the source RDBMS"""


class OffloadTransportStandardSqoop(OffloadTransport):
    """ Use standard Sqoop options to transport data
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation, offload_options, messages,
                 dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP

        super(OffloadTransportStandardSqoop, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )

        if offload_options.db_type in (DBTYPE_MSSQL, DBTYPE_NETEZZA):
            if len(offload_source_table.get_primary_key_columns()) == 1:
                messages.warning('Downgrading Sqoop parallelism for %s table without a singleton primary key'
                                 % offload_options.db_type)
            self._offload_transport_parallelism = 1

        self._sqoop_additional_options = offload_operation.sqoop_additional_options
        self._sqoop_mapreduce_map_memory_mb = offload_operation.sqoop_mapreduce_map_memory_mb
        self._sqoop_mapreduce_map_java_opts = offload_operation.sqoop_mapreduce_map_java_opts

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _sqoop_cli_safe_value(self, cmd_option_string):
        return self._ssh_cli_safe_value(cmd_option_string)

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=True)

    def _remote_copy_sqoop_control_file(self, options_file_local_path, suffix=''):
        return self._remote_copy_transport_control_file(options_file_local_path, self._offload_transport_cmd_host,
                                                        prefix=SQOOP_OPTIONS_FILE_PREFIX, suffix=suffix)

    def _sqoop_by_table_options(self, partition_chunk):
        """ Options dictating that Sqoop should process a table/partition list and split data between mappers as it sees fit
        """
        sqoop_options = self._rdbms_api.sqoop_rdbms_specific_table_options(
            self._rdbms_owner, self._rdbms_table_name
        )
        jvm_options = self._rdbms_api.sqoop_rdbms_specific_jvm_table_options(
            partition_chunk, self._rdbms_partition_type, self._rdbms_is_iot, self._offload_transport_consistent_read
        )
        return jvm_options, sqoop_options

    def _sqoop_source_options(self, partition_chunk):
        """ Matches returns for same function in SqoopByQuery class
        """
        options_file_local_path = None
        jvm_options, sqoop_options = self._sqoop_by_table_options(partition_chunk)
        return jvm_options, sqoop_options, options_file_local_path

    def _sqoop_memory_options(self):
        memory_flags = []
        if self._sqoop_mapreduce_map_memory_mb:
            memory_flags.append("-Dmapreduce.map.memory.mb=%s" % self._sqoop_mapreduce_map_memory_mb)
        if self._sqoop_mapreduce_map_java_opts:
            memory_flags.append("-Dmapreduce.map.java.opts=%s" % self._sqoop_mapreduce_map_java_opts)
        return memory_flags

    def _get_rows_imported_from_sqoop_log(self, log_string):
        """ Search through the Sqoop log file and extract num rows imported
        """
        self.debug('_get_rows_imported_from_sqoop_log()')
        if not log_string:
            return None
        assert isinstance(log_string, str)
        m = re.search(SQOOP_LOG_ROW_COUNT_PATTERN, log_string, re.IGNORECASE | re.MULTILINE)
        if m:
            return int(m.group(1))

    def _get_oracle_module_action_from_sqoop_log(self, log_string):
        """ Search through the Sqoop log file and extract Oracle session module/action.
            Only works when OraOop is active, in some cases these will be inaccessible
            Example OraOop command:
                dbms_application_info.set_module(module_name => 'Data Connector for Oracle and Hadoop', action_name => 'import 20190122162424UTC')
        """
        self.debug('_get_oracle_module_action_from_sqoop_log()')
        if not log_string:
            return (None, None)
        assert isinstance(log_string, str)

        m = re.search(SQOOP_LOG_MODULE_PATTERN, log_string, re.IGNORECASE | re.MULTILINE)
        if m and len(m.groups()) == 2:
            return m.groups()
        else:
            self._messages.log('Unable to identify Sqoop Oracle module/action', detail=VVERBOSE)
            return (None, None)

    def _get_sqoop_cli_auth_vars(self):
        extra_jvm_options = []
        no_log_password = []
        app_user_option = ['--username', self._offload_options.rdbms_app_user]
        if self._offload_transport_auth_using_oracle_wallet:
            app_user_option = []
            app_pass_option = []
        elif self._offload_transport_password_alias:
            app_pass_option = ['--password-alias', self._offload_transport_password_alias]
            if self._offload_transport_credential_provider_path:
                extra_jvm_options = [self._credential_provider_path_jvm_override()]
        elif self._offload_options.sqoop_password_file:
            app_pass_option = ['--password-file', self._offload_options.sqoop_password_file]
        else:
            sqoop_cli_safe_pass = self._sqoop_cli_safe_value(self._offload_options.rdbms_app_pass)
            no_log_password = [{'item': sqoop_cli_safe_pass, 'prior': '--password'}]
            app_pass_option = ['--password', sqoop_cli_safe_pass]
        return app_user_option, app_pass_option, extra_jvm_options, no_log_password

    def _check_for_ora_errors(self, cmd_out):
        # Need to think about checks for non-Oracle rdbms
        ora_errors = [line for line in (cmd_out or '').splitlines() if 'ORA-' in line]
        if ora_errors:
            self.log('Sqoop exited cleanly, but the following errors were found in the output:')
            self.log('\t' + '\n\t'.join(ora_errors))
            self.log('This is a questionable Sqoop execution')
            raise OffloadTransportException('Errors in Sqoop output')

    def _get_common_jvm_overrides(self):
        std_d_options = []
        if self._offload_transport_jvm_overrides:
            std_d_options += split_not_in_quotes(self._offload_transport_jvm_overrides, exclude_empty_tokens=True)

        std_d_options += self._sqoop_memory_options()

        if self._offload_transport_queue_name:
            std_d_options += ['-Dmapreduce.job.queuename=%s' % self._offload_transport_queue_name]
        return std_d_options

    def _sqoop_import(self, partition_chunk=None):
        self.debug('_sqoop_import()')
        sqoop_rm_commands = None
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        if partition_chunk and partition_chunk.count() > 0:
            if self._offload_options.sqoop_disable_direct or self._offload_transport_parallelism < 2:
                self.log('\nWARNING: Hybrid/partition offload without OraOop direct mode enabled may offload all partitions')

        if not self._offload_transport_consistent_read:
            self.log('\nPerforming Sqoop import without consistent read.', detail=VVERBOSE)

        oraoop_d_options, offload_source_options, options_file_local_path = self._sqoop_source_options(partition_chunk)

        app_user_option, app_pass_option, extra_jvm_options, no_log_password = self._get_sqoop_cli_auth_vars()
        oraoop_d_options = extra_jvm_options + oraoop_d_options

        if options_file_local_path:
            sqoop_rm_commands, options_file_remote_path = self._remote_copy_sqoop_control_file(options_file_local_path)
            # Ignore the actual table options and redirect to the options file instead
            offload_source_options = ['--options-file', options_file_remote_path]

        self._run_os_cmd(self._ssh_cmd_prefix() + ['mkdir', '-p', self._offload_options.sqoop_outdir])

        sqoop_cmd = ['sqoop', 'import']
        sqoop_cmd += self._get_common_jvm_overrides() + self._sqoop_rdbms_specific_jvm_overrides() + oraoop_d_options

        connect_string = self._sqoop_cli_safe_value(self._rdbms_api.jdbc_url())
        sqoop_parallel = ['-m' + str(self._offload_transport_parallelism)]
        rdbms_specific_opts = self._rdbms_api.sqoop_rdbms_specific_options()

        sqoop_cmd += ['--connect', connect_string] + app_user_option + app_pass_option + [
                      '--null-string', '\'\'',
                      '--null-non-string', '\'\''] + offload_source_options + [
                      '--target-dir=' + self._staging_table_location,
                      '--delete-target-dir'] + rdbms_specific_opts + sqoop_parallel + [
                      '--fetch-size=%d' % int(self._offload_transport_fetch_size)] + self._column_type_read_remappings() + [
                      '--as-avrodatafile' if self._staging_format == FILE_STORAGE_FORMAT_AVRO else '--as-parquetfile',
                      '--outdir=' + self._offload_options.sqoop_outdir]

        if self._offload_options.vverbose:
            sqoop_cmd.append('--verbose')

        if self._compress_load_table:
            sqoop_cmd += ['--compression-codec', 'org.apache.hadoop.io.compress.SnappyCodec']

        sqoop_cmd += ['--class-name', self._get_transport_app_name()]

        if self._sqoop_additional_options:
            sqoop_cmd += split_not_in_quotes(self._sqoop_additional_options, exclude_empty_tokens=True)

        try:
            rc, cmd_out = self._run_os_cmd(self._ssh_cmd_prefix() + sqoop_cmd, no_log_items=no_log_password)
            self._check_for_ora_errors(cmd_out)

            rows_imported = None
            if not self._dry_run:
                rows_imported = self._get_rows_imported_from_sqoop_log(cmd_out)
                if self._offload_options.db_type == DBTYPE_ORACLE:
                    module, action = self._get_oracle_module_action_from_sqoop_log(cmd_out)
                    self._rdbms_api.log_sql_stats(module, action, payload=None,
                                                  validation_polling_interval=self._validation_polling_interval)
            # In order to let Impala/Hive drop the load table in the future we need g+w
            self.log_dfs_cmd('chmod(%s, "g+w")' % self._staging_table_location)
            if self._offload_options.execute:
                self._dfs_client.chmod(self._staging_table_location, mode='g+w')
        except:
            # Even in a sqoop failure we still want to chmod the load directory - if it exists
            self.log_dfs_cmd('(exception resilience) chmod(%s, "g+w")' % self._staging_table_location, detail=VVERBOSE)
            if self._offload_options.execute:
                try:
                    self._dfs_client.chmod(self._staging_table_location, mode='g+w')
                except Exception as exc:
                    # if we fail while coping with a Sqoop failure then do nothing except log it
                    self.log('Unable to chmod(%s): %s' % (self._staging_table_location, str(exc)), detail=VVERBOSE)
            raise

        # if we created a Sqoop options file, let's remove it
        if sqoop_rm_commands:
            [self._run_os_cmd(_) for _ in sqoop_rm_commands]

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _sqoop_rdbms_specific_jvm_overrides(self):
        return self._rdbms_api.sqoop_rdbms_specific_jvm_overrides(
            self._get_rdbms_session_setup_commands(include_fixed_sqoop=False, include_semi_colons=True))

    def _verify_rdbms_connectivity(self):
        """ Use a simple canary query for verification test """
        rdbms_source_query = self._rdbms_api.get_rdbms_canary_query()

        app_user_option, app_pass_option, oraoop_d_options, no_log_password = self._get_sqoop_cli_auth_vars()

        oraoop_d_options = self._sqoop_rdbms_specific_jvm_overrides() + oraoop_d_options
        std_d_options = self._get_common_jvm_overrides()

        sqoop_cmd = ['sqoop', 'eval'] + std_d_options + oraoop_d_options

        connect_string = self._sqoop_cli_safe_value(self._rdbms_api.jdbc_url())

        sqoop_cmd += ['--connect', connect_string] + app_user_option + app_pass_option + [
                      '--query', self._sqoop_cli_safe_value(rdbms_source_query)]

        sqoop_cmd += self._rdbms_api.sqoop_rdbms_specific_options()

        if self._sqoop_additional_options:
            sqoop_cmd += split_not_in_quotes(self._sqoop_additional_options, exclude_empty_tokens=True)

        rc, cmd_out = self._run_os_cmd(self._ssh_cmd_prefix() + sqoop_cmd, no_log_items=no_log_password)
        self._check_for_ora_errors(cmd_out)
        # if we got this far then we're in good shape
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Table centric Sqoop transport"""
        self._reset_transport_context()
        def step_fn():
            row_count = self._sqoop_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count
        return self._messages.offload_step(command_steps.STEP_STAGING_TRANSPORT, step_fn,
                                           execute=self._offload_options.execute)

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSqoopByQuery(OffloadTransportStandardSqoop):
    """ Use Sqoop with a custom query as the row source to transport data
        At the moment this is identical to the parent class, I'm hoping to move some specific
        logic to overloads in this child class
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation,
                 offload_options, messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY
        super(OffloadTransportSqoopByQuery, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _sqoop_by_query_options(self, partition_chunk, write_options_to_file=False):
        """ Options dictating that Sqoop should process a GOE defined query which splits data into groups ready for
            distributing amongst mappers
        """
        # No double quotes below - they break Sqoop
        sqoop_d_options = ['-Dmapred.child.java.opts=-Duser.timezone=UTC']
        colexpressions, colnames = self._build_offload_query_lists()
        # Column aliases included for correct AVRO identification
        sql_projection = self._sql_projection_from_offload_query_expression_list(colexpressions, colnames,
                                                                                 no_newlines=True)
        split_by = ['--split-by', TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN]
        split_row_source_by = self._get_transport_split_type(partition_chunk)

        boundary_query = ['--boundary-query',
                          self._rdbms_api.sqoop_by_query_boundary_query(self._offload_transport_parallelism)]
        row_source = self._get_transport_row_source_query(split_row_source_by, partition_chunk)
        outer_hint = self._rdbms_api.get_rdbms_session_setup_hint(self._offload_transport_rdbms_session_parameters,
                                                                  self._get_max_ts_scale())

        q = 'SELECT %s %s FROM (%s) %s WHERE $CONDITIONS' % (
            outer_hint,
            sql_projection,
            row_source,
            self._rdbms_table.enclose_identifier(self._rdbms_table_name)
        )
        self.log('Sqoop custom query:\n%s' % q, detail=VVERBOSE)

        if not write_options_to_file:
            q = self._sqoop_cli_safe_value(q)

        offload_source_options = ['--query', q] + split_by + boundary_query

        options_file_local_path = None
        if write_options_to_file:
            options_file_local_path = write_temp_file('\n'.join(offload_source_options),
                                                      prefix=SQOOP_OPTIONS_FILE_PREFIX)
            self.log('Written query options to parameter file: %s' % options_file_local_path, detail=VVERBOSE)

        return sqoop_d_options, offload_source_options, options_file_local_path

    def _sqoop_source_options(self, partition_chunk):
        """ Matches returns for same function in StandardSqoop class
        """
        oraoop_d_options, offload_source_options, options_file_local_path = self._sqoop_by_query_options(partition_chunk, write_options_to_file=True)
        if not options_file_local_path:
            raise OffloadTransportException('Sqoop options file not created as expected')
        return oraoop_d_options, offload_source_options, options_file_local_path


class OffloadTransportSqoopCanary(OffloadTransportStandardSqoop):
    """ Validate Sqoop connectivity
    """
    def __init__(self, offload_options, messages):
        """ CONSTRUCTOR
            This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = orchestration_defaults.bool_option_from_string(
            'OFFLOAD_TRANSPORT_CONSISTENT_READ', orchestration_defaults.offload_transport_consistent_read_default()
        )
        self._offload_transport_fetch_size = orchestration_defaults.offload_transport_fetch_size_default()
        self._offload_transport_jvm_overrides = orchestration_defaults.sqoop_overrides_default()
        self._offload_transport_queue_name = orchestration_defaults.sqoop_queue_name_default()
        self._offload_transport_parallelism = 2
        self._validation_polling_interval = orchestration_defaults.offload_transport_validation_polling_interval_default()
        self._spark_config_properties = self._prepare_spark_config_properties(orchestration_defaults.offload_transport_spark_properties_default())
        self._sqoop_additional_options = orchestration_defaults.sqoop_additional_options_default()
        self._sqoop_mapreduce_map_memory_mb = None
        self._sqoop_mapreduce_map_java_opts = None
        # No OraOop for canary queries
        self._offload_options.sqoop_disable_direct = True

        self._rdbms_api = offload_transport_rdbms_api_factory('dummy_owner', 'dummy_table',
                                                              self._offload_options, self._messages,
                                                              dry_run=self._dry_run)

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


class OffloadTransportSpark(OffloadTransport, metaclass=ABCMeta):
    """ Parent class for Spark methods
    """
    def __init__(self, offload_source_table, offload_target_table, offload_operation,
                 offload_options, messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        super(OffloadTransportSpark, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )
        self._OffloadTransportSqlStatsThread = None
        self._convert_nans_to_nulls = convert_nans_to_nulls(offload_target_table, offload_operation)
        self._spark_files_csv = offload_options.offload_transport_spark_files
        self._spark_jars_csv = offload_options.offload_transport_spark_jars

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=False)

    def _standalone_spark(self):
        return bool(self._offload_options.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS)

    def _option_from_properties(self, option_name, property_name):
        """ Small helper function to pluck a property value from self._spark_config_properties
        """
        if property_name in self._spark_config_properties:
            return [option_name, self._spark_config_properties[property_name]]
        else:
            return []

    def _jdbc_option_clauses(self) -> str:
        jdbc_option_clauses = ''
        if self._offload_options.db_type == DBTYPE_ORACLE:
            jdbc_option_clauses = ".option('oracle.jdbc.timezoneAsRegion', 'false')"
        return jdbc_option_clauses

    def _get_pyspark_body(self, partition_chunk=None, create_spark_context=True, canary_query=None):
        """ Return pyspark code to copy data to the load table.
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
                password_python.append('hconf = spark.sparkContext._jsc.hadoopConfiguration()')
                if self._offload_transport_credential_provider_path:
                    # provider_path is optional because a customer may define it in their Hadoop config rather than GOE config
                    password_python.append("hconf.set('hadoop.security.credential.provider.path', '%s')"
                                           % self._offload_transport_credential_provider_path)
                password_python.append("pw = hconf.getPassword('%s')" % self._offload_transport_password_alias)
                password_python.append("assert pw, 'Unable to retrieve password for alias: %s'"
                                       % self._offload_transport_password_alias)
                password_python.append("jdbc_password = ''.join(str(pw.__getitem__(i)) for i in range(pw.__len__()))")
            else:
                password_python.append("jdbc_password = spark.conf.get('spark.jdbc.password')")
            return '\n'.join(password_python)

        if canary_query:
            custom_schema_clause = ''
            rdbms_source_query = canary_query
            transport_app_name = 'GOE Connect'
            load_db_name, load_table_name = '', ''
        else:
            split_row_source_by = self._get_transport_split_type(partition_chunk)
            custom_schema_clause = self._column_type_read_remappings()
            rdbms_source_query = self._get_spark_jdbc_query_text(TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                                                                 split_row_source_by, partition_chunk,
                                                                 sql_comment=self._rdbms_action)
            transport_app_name = self._get_transport_app_name()
            load_db_name, load_table_name = self._load_db_name, self._load_table_name

        session_init_statements = ''.join(self._get_rdbms_session_setup_commands(for_plsql=True))
        session_init_statement_opt = ''
        if session_init_statements:
            session_init_statement_opt = "\n    sessionInitStatement='BEGIN %s END;',"\
                % self._spark_sql_option_safe(session_init_statements)

        jdbc_option_clauses = self._jdbc_option_clauses()

        if self._offload_transport_auth_using_oracle_wallet:
            rdbms_app_user_opt = rdbms_app_pass_opt = password_snippet = ''
        else:
            rdbms_app_user_opt = "\n    user='%s'," % self._offload_options.rdbms_app_user
            rdbms_app_pass_opt = '\n    password=jdbc_password,'
            password_snippet = get_password_snippet()

        # Certain config should not be logged
        config_blacklist = ['spark.jdbc.password',
                            'spark.authenticate.secret',
                            'spark.hadoop.fs.gs.auth.service.account.private.key',
                            'spark.hadoop.fs.gs.auth.service.account.private.key.id',
                            'spark.hadoop.fs.s3a.access.key',
                            'spark.hadoop.fs.s3a.secret.key',
                            'spark.ssl.keyPassword',
                            'spark.ssl.keyStorePassword',
                            'spark.ssl.trustStorePassword',
                            'spark.com.gluent.SparkBasicAuth.params']
        debug_conf_snippet = dedent("""\
                                    config_blacklist = %s
                                    for kv in sorted(spark.sparkContext.getConf().getAll()):
                                        if kv[0] in config_blacklist:
                                            print(kv[0], '?')
                                        else:
                                            print(kv)""" % repr(config_blacklist))

        params = {
            'load_db': load_db_name,
            'table_name': load_table_name,
            'rdbms_jdbc_url': self._rdbms_api.jdbc_url(),
            'rdbms_jdbc_driver_name': self._rdbms_api.jdbc_driver_name(),
            'rdbms_app_user_opt': rdbms_app_user_opt,
            'rdbms_app_pass_opt': rdbms_app_pass_opt,
            'get_password_snippet': password_snippet,
            'rdbms_source_query': self._spark_sql_option_safe(rdbms_source_query),
            'batch_col': TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
            # batch_col_max is one higher than reality, see header comment
            'batch_col_max': self._offload_transport_parallelism,
            'parallelism': self._offload_transport_parallelism,
            'custom_schema_clause': ((',\n    ' + custom_schema_clause) if custom_schema_clause else ''),
            'write_format': self._staging_format,
            'uri': '' if canary_query else self._target_table.get_staging_table_location(),
            'fetch_size': self._offload_transport_fetch_size,
            'session_init_statement_opt': session_init_statement_opt,
            'debug_conf_snippet': debug_conf_snippet,
            'jdbc_option_clauses': jdbc_option_clauses
        }

        pyspark_body = ''

        if self._offload_transport_method != OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY:
            # On HDP2 Livy the UTF coding is causing errors submitting the script:
            # SyntaxError: encoding declaration in Unicode string
            # We want UTF coding just in case we have PBO with a unicode predicate but decided to exclude line below
            # for Livy only. In reality no-one is using Livy with GOE in the near future. We might have to revisit.
            pyspark_body += dedent("""\
                   # -*- coding: UTF-8 -*-
                   """)

        pyspark_body += dedent("""\
            # GOE Spark Transport
            """)

        if self._base64_staged_columns():
            pyspark_body += dedent("""\
                from pyspark.sql.functions import base64
                """)

        if create_spark_context:
            pyspark_body += dedent("""\
                                   from pyspark.sql import SparkSession
                                   spark = SparkSession.builder.appName('%(app_name)s')%(hive_support)s.getOrCreate()
                                   """) % {'app_name': transport_app_name,
                                           'hive_support': '' if self._standalone_spark() else '.enableHiveSupport()'}

        if canary_query:
            # Just testing connectivity and will drop out with no write of data
            pyspark_body += dedent("""\
                %(get_password_snippet)s
                df = spark.read.format('jdbc').options(
                    driver='%(rdbms_jdbc_driver_name)s',
                    url='%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s%(session_init_statement_opt)s
                    dbtable=\"\"\"%(rdbms_source_query)s\"\"\",
                )%(jdbc_option_clauses)s.load().show()
                """) % params
            return pyspark_body

        # Using saveAsTable because, unlike insertInto, it will use the column names to find the correct column
        # positions which then allows it to skip TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN
        # In the code below, after loading the data from the RDBMS we use select() to only project
        # table columns and not the split column.
        pyspark_body += dedent("""\
            %(debug_conf_snippet)s
            %(get_password_snippet)s
            df = spark.read.format('jdbc').options(
                driver='%(rdbms_jdbc_driver_name)s',
                url='%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s%(session_init_statement_opt)s
                dbtable=\"\"\"%(rdbms_source_query)s\"\"\",
                fetchSize=%(fetch_size)s,
                partitionColumn='%(batch_col)s',
                lowerBound=0,
                upperBound=%(batch_col_max)s,
                numPartitions=%(parallelism)s%(custom_schema_clause)s
            )%(jdbc_option_clauses)s.load()
            df.printSchema()
            """) % params

        if self._base64_staged_columns():
            def proj_col(col_name):
                if match_table_column(col_name, self._base64_staged_columns()):
                    return "base64(df.%(col)s).alias('%(col)s')" % {'col': col_name}
                else:
                    return "'%s'" % col_name
            pyspark_body += dedent("""\
            projection = [%s]
            """) % ','.join(proj_col(_.name) for _ in self._rdbms_columns)
        else:
            pyspark_body += dedent("""\
            projection = [_ for _ in df.columns if _.upper() != '%(batch_col)s'.upper()]
            """) % params

        if self._standalone_spark():
            pyspark_body += dedent("""\
            df.select(projection).write.format('%(write_format)s').save('%(uri)s')
            """) % params
        else:
            pyspark_body += dedent("""\
            df.select(projection).write.mode('append').format('hive').saveAsTable('`%(load_db)s`.`%(table_name)s`')
            """) % params
        return pyspark_body

    def _get_rows_imported_from_spark_log(self, spark_log_text):
        """ Scrape spark_log_text searching for rows imported information """
        self.debug('_get_rows_imported_from_spark_log()')
        if not spark_log_text:
            return None
        assert isinstance(spark_log_text, str), '{} is not str'.format(type(spark_log_text))
        # Now look for the row count marker
        m = re.findall(SPARK_LOG_ROW_COUNT_PATTERN, spark_log_text, re.IGNORECASE | re.MULTILINE)
        if m:
            self.log('Found {} recordsWritten matches in Spark log: {}'.format(len(m), str(m)), detail=VVERBOSE)
            records_written = sum([int(_) for _ in m if _])
            self.log('Spark recordsWritten total: {}'.format(str(records_written)), detail=VVERBOSE)
            return records_written
        else:
            return None

    def _load_table_compression_pyspark_settings(self):
        if self._compress_load_table:
            codec = 'snappy'
            true_or_false = 'true'
        else:
            codec = 'uncompressed'
            true_or_false = 'false'
        prm_format = 'avro' if self._staging_format == FILE_STORAGE_FORMAT_AVRO else 'parquet'
        if self._standalone_spark():
            self._spark_config_properties.update({'spark.sql.%s.compression.codec' % prm_format: codec})
        else:
            # Spark is part of Hadoop
            self._spark_config_properties.update({'spark.hadoop.%s.output.codec' % prm_format: codec,
                                                  'spark.hadoop.hive.exec.compress.output': true_or_false})

    def _load_table_compression_spark_sql_settings(self):
        if self._compress_load_table:
            codec = 'snappy'
            true_or_false = 'true'
        else:
            codec = 'uncompressed'
            true_or_false = 'false'
        prm_format = 'avro' if self._staging_format == FILE_STORAGE_FORMAT_AVRO else 'parquet'
        # These settings work whether Spark is stand-alone or part of Hadoop
        self._spark_config_properties.update({'%s.output.codec' % prm_format: codec,
                                              'hive.exec.compress.output': true_or_false})

    def _local_gluent_listener_jar(self):
        """ Returns path to GOETaskListener jar file in $OFFLOAD_HOME/lib directory """
        offload_home = os.environ.get('OFFLOAD_HOME')
        assert offload_home, 'OFFLOAD_HOME is not set, environment is not correct'
        jar_path = os.path.join(offload_home, 'lib', GOE_LISTENER_JAR)
        # We should never be missing the JAR file as it is bundled with the code that we are part of.
        assert os.path.exists(jar_path), f'{jar_path} cannot be found, environment is not correct'
        return jar_path

    def _start_validation_polling_thread(self):
        if (self._validation_polling_interval
                and self._validation_polling_interval != OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED):
            self._OffloadTransportSqlStatsThread = PollingThread(
                run_function=self._rdbms_api.get_offload_transport_sql_stats_function(self._rdbms_module,
                                                                                      self._rdbms_action,
                                                                                      'OffloadTransportSqlStatsThread'),
                name='OffloadTransportSqlStatsThread',
                interval=self._validation_polling_interval)
            self._OffloadTransportSqlStatsThread.start()

    def _stop_validation_polling_thread(self):
        if self._OffloadTransportSqlStatsThread and self._OffloadTransportSqlStatsThread.is_alive():
            self._OffloadTransportSqlStatsThread.stop()

    def _drain_validation_polling_thread_queue(self):
        if self._OffloadTransportSqlStatsThread:
            if self._OffloadTransportSqlStatsThread.is_alive():
                self._stop_validation_polling_thread()
            if self._OffloadTransportSqlStatsThread.exception:
                self.log(
                    'Exception in %s polling method:\n%s' % (
                        self._OffloadTransportSqlStatsThread.name, self._OffloadTransportSqlStatsThread.exception),
                    VVERBOSE)
                return None
            elif self._OffloadTransportSqlStatsThread.get_queue_length() == 0:
                self.log(
                    'Empty results queue from %s polling method' %
                    self._OffloadTransportSqlStatsThread.name, VVERBOSE)
                return None
            else:
                self.log(POLLING_VALIDATION_TEXT % self._OffloadTransportSqlStatsThread.name, VVERBOSE)
                self.debug('Parsing %s snapshots taken at %s second intervals' % (
                    self._OffloadTransportSqlStatsThread.get_queue_length(),
                    self._OffloadTransportSqlStatsThread.interval))
                return self._OffloadTransportSqlStatsThread.drain_queue()


class OffloadTransportSparkThrift(OffloadTransportSpark):
    """ Use Spark Thrift Server to transport data
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation,
                 offload_options, messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT
        super(OffloadTransportSparkThrift, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )
        assert offload_options.offload_transport_spark_thrift_host and offload_options.offload_transport_spark_thrift_port

        self._load_table_compression_spark_sql_settings()

        # spark.jdbc.password is not retrievable in pure Spark SQL so remove from properties
        if 'spark.jdbc.password' in self._spark_config_properties:
            del self._spark_config_properties['spark.jdbc.password']

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _log_connection_details(self):
        self.log(hs2_connection_log_message(self._spark_thrift_host, self._spark_thrift_port,
                                            self._offload_options, service_name='Spark Thrift Server'), detail=VVERBOSE)

    def _get_create_temp_view_sql(self, temp_vw_name, rdbms_source_query, spark_api, custom_schema_clause=None,
                                  connectivity_test_only=False):
        """ Generate text for a Spark session temporary view which will contain details of where to get data from and
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
        self.debug('_get_create_temp_view_sql()')

        session_init_statements = ''.join(self._get_rdbms_session_setup_commands(for_plsql=True,
                                                                                 escape_semi_colons=True))

        if self._offload_transport_auth_using_oracle_wallet:
            rdbms_app_user_opt = ''
            rdbms_app_pass_opt = ''
        else:
            rdbms_app_user_opt = "\n  user='%s'," % self._offload_options.rdbms_app_user
            rdbms_app_pass_opt = "\n  password '%s'," % self._spark_sql_option_safe(self._offload_options.rdbms_app_pass)
        quoted_name = spark_api.enclose_identifier(temp_vw_name)
        params = {'temp_vw_name': quoted_name,
                  'rdbms_app_user_opt': rdbms_app_user_opt,
                  'rdbms_app_pass_opt': rdbms_app_pass_opt,
                  'rdbms_jdbc_url': self._rdbms_api.jdbc_url(),
                  'rdbms_jdbc_driver_name': self._rdbms_api.jdbc_driver_name(),
                  'rdbms_source_query': self._spark_sql_option_safe(rdbms_source_query),
                  'batch_col': TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                  # batch_col_max is one higher than reality, see header comment
                  'batch_col_max': self._offload_transport_parallelism,
                  'parallelism': self._offload_transport_parallelism,
                  'custom_schema_clause': ((',\n ' + custom_schema_clause) if custom_schema_clause else ''),
                  'fetch_size': self._offload_transport_fetch_size,
                  'session_init_statements': self._spark_sql_option_safe(session_init_statements)}
        if connectivity_test_only:
            temp_vw_sql = """CREATE TEMPORARY VIEW %(temp_vw_name)s
    USING org.apache.spark.sql.jdbc
    OPTIONS (
      driver '%(rdbms_jdbc_driver_name)s',
      url '%(rdbms_jdbc_url)s',%(rdbms_app_user_opt)s%(rdbms_app_pass_opt)s
      sessionInitStatement 'BEGIN %(session_init_statements)s END\\;',
      dbtable '%(rdbms_source_query)s'
    )""" % params
        else:
            temp_vw_sql = """CREATE TEMPORARY VIEW %(temp_vw_name)s
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
    )""" % params
        return temp_vw_sql

    def _spark_thrift_import(self, partition_chunk=None):
        """ Transport data using the Spark2 Thrift Server
        """
        self.debug('_spark_thrift_import(%s, %s)' % (self._spark_thrift_host, self._spark_thrift_port))
        self._log_connection_details()

        if self._nothing_to_do(partition_chunk):
            return 0

        self._refresh_rdbms_action()
        id_str = id_generator()
        rows_imported = None

        temp_vw_name = 'gluent_%s_%s_%s_jdbc_vw' % (self._rdbms_owner.lower(), self._rdbms_table_name.lower(), id_str)
        hive_sql_projection = '\n,      '.join(_.name for _ in self._rdbms_columns)
        split_row_source_by = self._get_transport_split_type(partition_chunk)
        rdbms_source_query = self._get_spark_jdbc_query_text(TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                                                             split_row_source_by, partition_chunk,
                                                             sql_comment=self._rdbms_action)
        custom_schema_clause = self._column_type_read_remappings()

        spark_api = backend_api_factory(DBTYPE_SPARK, self._offload_options, self._messages, dry_run=self._dry_run)

        temp_vw_sql = self._get_create_temp_view_sql(temp_vw_name, rdbms_source_query, spark_api,
                                                     custom_schema_clause=custom_schema_clause)

        ins_sql = """INSERT INTO %(load_db)s.%(table_name)s
SELECT %(hive_projection)s
FROM   %(temp_vw_name)s""" % {'temp_vw_name': spark_api.enclose_identifier(temp_vw_name),
                              'load_db': spark_api.enclose_identifier(self._load_db_name),
                              'table_name': spark_api.enclose_identifier(self._load_table_name),
                              'hive_projection': hive_sql_projection}

        if self._standalone_spark():
            # For stand-alone Spark we need a load db and table in the Derby DB in order to stage to cloud storage
            self.log('%s: START Prepare Spark load table' % self._str_time(), detail=VVERBOSE)
            if not spark_api.database_exists(self._load_db_name):
                spark_api.create_database(self._load_db_name)
            if spark_api.table_exists(self._load_db_name, self._load_table_name):
                spark_api.drop_table(self._load_db_name, self._load_table_name)
            spark_columns = [spark_api.from_canonical_column(_)
                             for _ in self._staging_file.get_canonical_staging_columns()]
            spark_api.create_table(self._load_db_name, self._load_table_name, spark_columns,
                                   None, storage_format=self._staging_format,
                                   location=self._target_table.get_staging_table_location(), external=True)
            self.log('%s: END Prepare Spark load table' % self._str_time(), detail=VVERBOSE)

        query_options = self._spark_properties_thrift_server_exclusions()

        # We don't want passwords on screen so request API to sub the password for ?
        hive_jdbc_pass_clause = "password '%s'" % self._spark_sql_option_safe(self._offload_options.rdbms_app_pass)

        self._start_validation_polling_thread()

        # These need to be executed together to ensure they are on the same cursor
        # otherwise the temporary view disappears.
        transport_sqls = [temp_vw_sql, ins_sql]
        spark_api.execute_ddl(transport_sqls, query_options=query_options, log_level=VERBOSE,
                              no_log_items={'item': hive_jdbc_pass_clause, 'sub': "password '?'"})

        self._stop_validation_polling_thread()

        if not self._dry_run:
            rows_imported = self._rdbms_api.log_sql_stats(self._rdbms_module, self._rdbms_action,
                                                          self._drain_validation_polling_thread_queue(),
                                                          validation_polling_interval=self._validation_polling_interval)

        if self._standalone_spark() and not self._preserve_load_table:
            # Drop the Spark stand alone load table
            self.log('%s: START Remove Spark load table' % self._str_time(), detail=VVERBOSE)
            spark_api.drop_table(self._load_db_name, self._load_table_name)
            self.log('%s: END Remove Spark load table' % self._str_time(), detail=VVERBOSE)

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _spark_properties_thrift_server_exclusions(self, with_warnings=True):
        query_options = {}
        if self._spark_config_properties:
            if bool([_ for _ in self._spark_config_properties if 'memory' in _.lower()]) and with_warnings:
                self._messages.warning('Memory configuration overrides are ignored when using Thrift Server')
                # In truth many settings are ignored but we'll still go through the motions as some,
                # compression for example, do work
            # Remove any properties we know to be incompatible with the Thrift Server
            for property_name in [_ for _ in self._spark_config_properties
                                  if _ not in SPARK_THRIFT_SERVER_PROPERTY_EXCLUDE_LIST]:
                query_options[property_name] = self._spark_config_properties[property_name]
        return query_options

    def _verify_rdbms_connectivity(self):
        """ Use a simple canary query for verification test """
        rdbms_source_query = '(%s) v' % self._rdbms_api.get_rdbms_canary_query()
        temp_vw_name = 'gluent_canary_jdbc_vw'
        spark_api = backend_api_factory(DBTYPE_SPARK, self._offload_options, self._messages, dry_run=self._dry_run)
        temp_vw_sql = self._get_create_temp_view_sql(temp_vw_name, rdbms_source_query, spark_api,
                                                     connectivity_test_only=True)
        hive_jdbc_pass_clause = "password '%s'" % self._spark_sql_option_safe(self._offload_options.rdbms_app_pass)
        test_sql = 'SELECT COUNT(*) FROM %s' % temp_vw_name
        query_options = self._spark_properties_thrift_server_exclusions(with_warnings=False)
        # Do not log temp_vw_sql - it may contain a password
        spark_api.execute_ddl(temp_vw_sql, query_options=query_options, log_level=VVERBOSE,
                              no_log_items={'item': hive_jdbc_pass_clause, 'sub': "password '?'"})
        row = spark_api.execute_query_fetch_one(test_sql, log_level=VERBOSE)
        return True if row[0] == 1 else False

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Spark Thriftserver transport"""
        self._reset_transport_context()
        def step_fn():
            row_count = self._spark_thrift_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count
        return self._messages.offload_step(command_steps.STEP_STAGING_TRANSPORT, step_fn,
                                           execute=self._offload_options.execute)

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkThriftCanary(OffloadTransportSparkThrift):
    """ Validate Spark Thrift Server connectivity
    """
    def __init__(self, offload_options, messages):
        """ CONSTRUCTOR
            This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        assert offload_options.offload_transport_spark_thrift_host
        assert offload_options.offload_transport_spark_thrift_port
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = orchestration_defaults.bool_option_from_string(
            'OFFLOAD_TRANSPORT_CONSISTENT_READ', orchestration_defaults.offload_transport_consistent_read_default()
        )
        self._offload_transport_fetch_size = orchestration_defaults.offload_transport_fetch_size_default()
        self._offload_transport_jvm_overrides = orchestration_defaults.offload_transport_spark_overrides_default()
        self._offload_transport_queue_name = orchestration_defaults.offload_transport_spark_queue_name_default()
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = orchestration_defaults.offload_transport_validation_polling_interval_default()
        self._spark_config_properties = self._prepare_spark_config_properties(orchestration_defaults.offload_transport_spark_properties_default())

        self._rdbms_api = offload_transport_rdbms_api_factory('dummy_owner', 'dummy_table', self._offload_options,
                                                              self._messages, dry_run=self._dry_run)

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
    """ Use Python via spark-submit to transport data
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation,
                 offload_options, messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT
        super(OffloadTransportSparkSubmit, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )
        # For spark-submit we need to pass compression in as a config to the driver program
        self._load_table_compression_pyspark_settings()
        self._spark_submit_executable = offload_options.offload_transport_spark_submit_executable
        self._offload_transport_spark_submit_master_url = offload_options.offload_transport_spark_submit_master_url
        assert self._spark_submit_executable

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _spark_cli_safe_value(self, cmd_option_string):
        return self._ssh_cli_safe_value(cmd_option_string)

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=False, remap_sep='=')

    def _remote_copy_spark_control_file(self, options_file_local_path, suffix=''):
        return self._remote_copy_transport_control_file(options_file_local_path, self._offload_transport_cmd_host,
                                                        prefix=SPARK_OPTIONS_FILE_PREFIX, suffix=suffix)

    def _derive_spark_submit_cmd_arg(self):
        """ Look to see if spark2-submit is available, if so then use it
            Otherwise fall back to normal executable but trying to influence Spark version via env var
            Retruns a list suitable for building a command for _run_os_cmd()
        """
        if self._spark_submit_executable == 'spark-submit' and not self._standalone_spark():
            return ['export', 'SPARK_MAJOR_VERSION=2', ';', self._spark_submit_executable]
        else:
            return [self._spark_submit_executable]

    def _get_spark_submit_command(self, pyspark_body):
        """ Submit PySpark code via spark-submit
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
            if k == 'spark.jdbc.password':
                return self._spark_cli_safe_value(v)
            else:
                return v

        self.log('PySpark: ' + pyspark_body, detail=VVERBOSE)
        options_file_local_path = write_temp_file(pyspark_body, prefix=SPARK_OPTIONS_FILE_PREFIX, suffix='py')
        py_rm_commands, options_file_remote_path = self._remote_copy_spark_control_file(options_file_local_path,
                                                                                        suffix='py')
        if self._spark_listener_included_in_config():
            spark_listener_jar_local_path = self._local_gluent_listener_jar()
            spark_listener_jar_remote_path = self._remote_copy_transport_file(spark_listener_jar_local_path,
                                                                              self._offload_transport_cmd_host)
        else:
            spark_listener_jar_remote_path = None
        self.log('Written PySpark file: %s' % options_file_remote_path, detail=VVERBOSE)

        remote_spark_files_csv = self._remote_copy_transport_file_csv(
            self._spark_files_csv,
            self._offload_transport_cmd_host
        )
        remote_spark_jars_csv = self._remote_copy_transport_file_csv(
            self._spark_jars_csv,
            self._offload_transport_cmd_host
        )

        if spark_listener_jar_remote_path:
            if remote_spark_jars_csv:
                remote_spark_jars_csv = f'{spark_listener_jar_remote_path},{remote_spark_jars_csv}'
            else:
                remote_spark_jars_csv = spark_listener_jar_remote_path

        spark_submit_binary = self._derive_spark_submit_cmd_arg()
        spark_master = self._offload_transport_spark_submit_master_url if self._standalone_spark() else SPARK_SUBMIT_SPARK_YARN_MASTER
        spark_master_opt = ['--master', spark_master] if spark_master else []
        spark_config_props, no_log_password = [], []
        [spark_config_props.extend(['--conf', '%s=%s' % (k, cli_safe_the_password(k, v))])
         for k, v in self._spark_config_properties.items()]
        if 'spark.jdbc.password' in self._spark_config_properties and not self._offload_transport_password_alias:
            # If the rdbms app password is visible in the CLI then obscure it from any logging
            password_config_to_obscure = 'spark.jdbc.password=%s' % \
                cli_safe_the_password('spark.jdbc.password', self._spark_config_properties['spark.jdbc.password'])
            no_log_password = [{'item': password_config_to_obscure, 'prior': '--conf'}]
        if ('spark.cores.max' not in self._spark_config_properties
                and self._offload_transport_parallelism
                and spark_master != SPARK_SUBMIT_SPARK_YARN_MASTER):
            # If the user has not configured spark.cores.max then default to offload_transport_parallelism.
            # This only applies to spark-submit and therefore is not injected into self._spark_config_properties.
            spark_config_props.extend(['--conf', 'spark.cores.max={}'.format(str(self._offload_transport_parallelism))])

        driver_memory_opt = self._option_from_properties('--driver-memory', 'spark.driver.memory')
        executor_memory_opt = self._option_from_properties('--executor-memory', 'spark.executor.memory')

        jvm_opts = ['--driver-java-options', """%s""" % self._offload_transport_jvm_overrides] if self._offload_transport_jvm_overrides else []
        jars_opt = ['--jars', remote_spark_jars_csv] if remote_spark_jars_csv else []
        files_opt = ['--files', remote_spark_files_csv] if remote_spark_files_csv else []
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
            spark_submit_cmd.append('--verbose')
        if self._offload_transport_queue_name:
            spark_submit_cmd.append('--queue=%s' % self._offload_transport_queue_name)

        spark_submit_cmd.append(options_file_remote_path)
        return spark_submit_cmd, no_log_password, py_rm_commands

    def _spark_submit_import(self, partition_chunk=None):
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        rows_imported = None
        pyspark_body = self._get_pyspark_body(partition_chunk)
        spark_submit_cmd, no_log_password, py_rm_commands = self._get_spark_submit_command(pyspark_body)

        self._start_validation_polling_thread()
        rc, cmd_out = self._run_os_cmd(self._ssh_cmd_prefix() + spark_submit_cmd, no_log_items=no_log_password)
        self._stop_validation_polling_thread()

        if not self._dry_run:
            rows_imported = self._get_rows_imported_from_spark_log(cmd_out)
            rows_imported_from_sql_stats = self._rdbms_api.log_sql_stats(self._rdbms_module, self._rdbms_action,
                                                                         self._drain_validation_polling_thread_queue(),
                                                                         validation_polling_interval=self._validation_polling_interval)
            if rows_imported is None:
                if rows_imported_from_sql_stats is None:
                    self.warning(MISSING_ROWS_SPARK_WARNING)
                else:
                    self.warning(
                        f'{MISSING_ROWS_SPARK_WARNING}, falling back on RDBMS SQL statistics')
                    rows_imported = rows_imported_from_sql_stats

        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _verify_rdbms_connectivity(self):
        """ Use a simple canary query for verification test """
        rdbms_source_query = '(%s) v' % self._rdbms_api.get_rdbms_canary_query()
        pyspark_body = self._get_pyspark_body(canary_query=rdbms_source_query)
        self.log('PySpark: ' + pyspark_body, detail=VVERBOSE)
        spark_submit_cmd, no_log_password, py_rm_commands = self._get_spark_submit_command(pyspark_body)
        rc, cmd_out = self._run_os_cmd(self._ssh_cmd_prefix() + spark_submit_cmd, no_log_items=no_log_password)
        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]
        # if we got this far then we're in good shape
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Spark by spark-submit transport"""
        self._reset_transport_context()
        def step_fn():
            row_count = self._spark_submit_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count
        return self._messages.offload_step(command_steps.STEP_STAGING_TRANSPORT, step_fn,
                                           execute=self._offload_options.execute)

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkSubmitCanary(OffloadTransportSparkSubmit):
    """ Validate Spark Submit connectivity
    """
    def __init__(self, offload_options, messages):
        """ CONSTRUCTOR
            This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        self._spark_submit_executable = offload_options.offload_transport_spark_submit_executable
        assert self._spark_submit_executable
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = orchestration_defaults.bool_option_from_string(
            'OFFLOAD_TRANSPORT_CONSISTENT_READ', orchestration_defaults.offload_transport_consistent_read_default()
        )
        self._offload_transport_fetch_size = orchestration_defaults.offload_transport_fetch_size_default()
        self._offload_transport_jvm_overrides = orchestration_defaults.offload_transport_spark_overrides_default()
        self._offload_transport_queue_name = orchestration_defaults.offload_transport_spark_queue_name_default()
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = orchestration_defaults.offload_transport_validation_polling_interval_default()
        self._spark_config_properties = self._prepare_spark_config_properties(orchestration_defaults.offload_transport_spark_properties_default())

        self._rdbms_api = offload_transport_rdbms_api_factory('dummy_owner', 'dummy_table', self._offload_options,
                                                              self._messages, dry_run=self._dry_run)

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        self._offload_transport_spark_submit_master_url = offload_options.offload_transport_spark_submit_master_url
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


class OffloadTransportSparkLivy(OffloadTransportSpark):
    """ Use PySpark via Livy REST interface to transport data
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation, offload_options, messages,
                 dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY
        super(OffloadTransportSparkLivy, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )
        assert offload_options.offload_transport_livy_api_url, 'REST API URL has not been defined'

        self._api_url = offload_options.offload_transport_livy_api_url
        self._livy_requests = OffloadTransportLivyRequests(offload_options, messages)
        # cap the number of Livy sessions we will create before queuing
        self._livy_max_sessions = int(offload_options.offload_transport_livy_max_sessions) if offload_options.offload_transport_livy_max_sessions else LIVY_MAX_SESSIONS
        # timeout and close Livy sessions when idle
        self._idle_session_timeout = int(offload_options.offload_transport_livy_idle_session_timeout)
        # For Livy we need to pass compression in as a config to the driving session
        self._load_table_compression_pyspark_settings()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_transport_app_name(self, sep='.') -> str:
        """ Overload without table names
        """
        return YARN_TRANSPORT_NAME

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=False, remap_sep='=')

    def _close_livy_session(self, session_url):
        try:
            self.log('Closing session: %s' % session_url, detail=VVERBOSE)
            resp = self._livy_requests.delete(session_url)
        except Exception:
            self.log('Unable to close session: %s' % session_url, detail=VVERBOSE)

    def _log_app_info(self, resp_json, last_log_msg):
        if resp_json.get('log'):
            log_msg = resp_json['log']
            if log_msg != last_log_msg:
                self._messages.log_timestamp(detail=VVERBOSE)
                self.log('\n'.join(_ for _ in log_msg) if isinstance(log_msg, list) else str(log_msg), detail=VVERBOSE)
            return log_msg

    def _get_api_sessions_url(self):
        return URL_SEP.join([self._api_url, LIVY_SESSIONS_SUBURL])

    def _copy_gluent_listener_jar_to_dfs(self):
        """ Copies the jar file to HDFS and returns the remote DFS location """
        self.debug('_copy_gluent_listener_jar_to_dfs()')
        spark_listener_jar_local_path = self._local_gluent_listener_jar()
        if self._standalone_spark():
            # Ensure jar file is on transport host local filesystem
            spark_listener_jar_remote_path = self._remote_copy_transport_file(spark_listener_jar_local_path,
                                                                              self._offload_transport_cmd_host)
        else:
            # Ensure jar file is copied to DFS
            spark_listener_jar_remote_path = os.path.join(self._offload_options.hdfs_home, GOE_LISTENER_JAR)
            self.log_dfs_cmd('copy_from_local("%s", "%s")'
                             % (spark_listener_jar_local_path, spark_listener_jar_remote_path))
            if not self._dry_run:
                self._dfs_client.copy_from_local(spark_listener_jar_local_path, spark_listener_jar_remote_path,
                                                 overwrite=True)
        return spark_listener_jar_remote_path

    def _create_livy_session(self):
        """ Create a Livy session via REST API post
            Valid attributes for the payload are:
                kind           Session kind
                proxyUser      User ID to impersonate
                jars           Jar files to be used
                pyFiles        Python files to be used
                files          Other files to be used
                driverMemory   memory to use for the driver
                driverCores    Number of cores to use for the driver
                executorMemory Amount of memory to use for each executor process
                executorCores  Number of cores to use for each executor process
                numExecutors   Number of executors to launch for this session
                archives       Archives to be used in this session
                queue          The name of the YARN queue to which the job should be submitted
                name           Name of this session
                conf           Spark configuration properties
                heartbeatTimeoutInSecond    Timeout in second to which session be orphaned
        """
        def add_payload_option_from_spark_properties(payload, key_name, property_name):
            kv = self._option_from_properties(key_name, property_name)
            if kv:
                self.log('Adding %s = %s to session payload' % (kv[0], kv[1]), detail=VVERBOSE)
                payload[kv[0]] = kv[1]

        last_log_msg, session_url = None, None

        if self._spark_listener_included_in_config():
            remote_jar_file_path = self._copy_gluent_listener_jar_to_dfs()
        else:
            remote_jar_file_path = None

        data = {'kind': 'pyspark',
                'heartbeatTimeoutInSecond': self._idle_session_timeout,
                'name': self._get_transport_app_name(),
                'conf': self._spark_config_properties,
                'jars': [remote_jar_file_path] if remote_jar_file_path else None}
        if self._offload_transport_queue_name:
            data['queue'] = self._offload_transport_queue_name
        add_payload_option_from_spark_properties(data, 'driverMemory', 'spark.driver.memory')
        add_payload_option_from_spark_properties(data, 'executorMemory', 'spark.executor.memory')
        self.log('%s: Requesting Livy session: %s'
                 % (self._str_time(), str({k: data[k] for k in ['kind', 'heartbeatTimeoutInSecond', 'name']})),
                 detail=VVERBOSE)

        resp = self._livy_requests.post(self._get_api_sessions_url(), data=json.dumps(data))
        if resp.ok:
            self.log('Livy session id: %s' % resp.json().get('id'), detail=VVERBOSE)
            session_url = (self._api_url + resp.headers['location']) if resp.headers.get('location') else None
            session_state = None
            polls = 0
            # Wait until the state of the session is "idle" - not "starting"
            while polls <= LIVY_CONNECT_MAX_POLLS and session_url:
                session_state = resp.json()['state']
                if session_state == 'idle' and session_url:
                    if not self._offload_options.vverbose:
                        finish_progress_on_stdout()
                    self.log('REST session started: %s' % session_url, detail=VVERBOSE)
                    return session_url
                elif session_state == 'starting':
                    last_log_msg = self._log_app_info(resp.json(), last_log_msg)
                    time.sleep(LIVY_CONNECT_POLL_DELAY)
                    polls += 1
                    self.log('%s: Polling session with state: %s' % (self._str_time(), session_state), detail=VVERBOSE)
                    resp = self._livy_requests.get(session_url)
                    if not resp.ok:
                        self.log('Response code: %s' % resp.status_code, detail=VERBOSE)
                        self.log('Response text: %s' % resp.text, detail=VERBOSE)
                        self._close_livy_session(session_url)
                        resp.raise_for_status()
                    elif not self._offload_options.vverbose:
                        # Simulate the same dot progress we have with Sqoop offloads
                        write_progress_to_stdout()
                else:
                    # Something went wrong
                    self.log('Response text: %s' % resp.text, detail=VVERBOSE)
                    self._close_livy_session(session_url)
                    raise OffloadTransportException('REST session was not created: state=%s' % session_state)
            # If we got here then we're timing out
            self.log('Response text: %s' % resp.text, detail=VVERBOSE)
            self._close_livy_session(session_url)
            raise OffloadTransportException('REST session was not created, timing out: state=%s' % session_state)
        else:
            self.log('Response code: %s' % resp.status_code, detail=VERBOSE)
            self.log('Response text: %s' % resp.text, detail=VERBOSE)
            resp.raise_for_status()

    def _attach_livy_session(self):
        def is_gluent_usable_session(job_dict, ignore_session_state=False):
            if job_dict.get('kind') != 'pyspark' or (job_dict.get('state') != 'idle' and not ignore_session_state):
                return False
            if self._offload_transport_queue_name:
                pattern = re.compile(LIVY_LOG_QUEUE_PATTERN % self._offload_transport_queue_name, re.IGNORECASE)
                if [_ for _ in job_dict.get('log') if pattern.search(_)]:
                    # Found a message in the log stating this is on the right queue for us
                    return True
                return False
            return True

        resp = self._livy_requests.get(self._get_api_sessions_url())
        if resp.ok:
            sessions = resp.json()['sessions'] or []
            gluent_usable_sessions = [_ for _ in sessions if is_gluent_usable_session(_)]
            self.log('Found %s usable Livy sessions' % len(gluent_usable_sessions), detail=VVERBOSE)
            if len(gluent_usable_sessions) > 0:
                use_session = gluent_usable_sessions.pop()
                self.log('Re-using Livy session: %s/%s' % (str(use_session['id']), use_session.get('appId')),
                         detail=VERBOSE)
                return URL_SEP.join([self._get_api_sessions_url(), str(use_session['id'])])
            else:
                all_gluent_sessions = [_ for _ in sessions if is_gluent_usable_session(_, ignore_session_state=True)]
                self.log('No usable Livy sessions out of total: %s' % len(all_gluent_sessions), detail=VVERBOSE)
                if len(all_gluent_sessions) < self._livy_max_sessions:
                    # create and use a new session
                    return self._create_livy_session()
                else:
                    self.log('All gluent capable Livy sessions: %s' % str(all_gluent_sessions), detail=VVERBOSE)
                    raise OffloadTransportException('Exceeded maximum Livy sessions for offload transport: %s'
                                                    % str(self._livy_max_sessions))
        else:
            self.log('Response code: %s' % resp.status_code, detail=VERBOSE)
            self.log('Response text: %s' % resp.text, detail=VERBOSE)
            resp.raise_for_status()

    def _submit_pyspark_to_session(self, session_url, payload_data):
        """ Submit a pyspark job to Livy and poll until the job is complete. Returns log output. """
        last_log_msg = None
        statements_url = URL_SEP.join([session_url, 'statements'])
        payload = {'code': payload_data}
        resp = self._livy_requests.post(statements_url, data=json.dumps(payload))
        if resp.ok:
            statement_url = (self._api_url + resp.headers['location']) if resp.headers.get('location') else None
            self.log('Submitted REST statement: %s' % statement_url, detail=VERBOSE)
            # Wait until the state of the statement is "available" - not "running" or "waiting"
            polls = 0
            while True:
                statement_state = resp.json()['state']
                if statement_state == 'available':
                    self.log('REST statement complete: %s' % statement_url, detail=VVERBOSE)
                    statement_output = resp.json().get('output')
                    self.log('Output: %s' % statement_output, detail=VVERBOSE)
                    status = statement_output.get('status') if statement_output else None
                    if status != 'ok':
                        raise OffloadTransportException('Spark statement has failed with status: %s' % status)
                    # The REST response has CRs converted to plain text, change them back before return the value
                    statement_log = str(statement_output.get('data')).replace('\\n', '\n')
                    return statement_log
                elif statement_state in ('running', 'waiting'):
                    last_log_msg = self._log_app_info(resp.json(), last_log_msg)
                    time.sleep(LIVY_STATEMENT_POLL_DELAY)
                    polls += 1
                    self.log('%s: Polling statement with state: %s' % (self._str_time(), statement_state),
                             detail=VVERBOSE)
                    resp = self._livy_requests.get(statement_url)
                    if not resp.ok:
                        self.log('Response code: %s' % resp.status_code, detail=VERBOSE)
                        self.log('Response text: %s' % resp.text, detail=VERBOSE)
                        resp.raise_for_status()
                else:
                    # Something went wrong
                    self.log('Response text: %s' % resp.text, detail=VVERBOSE)
                    raise OffloadTransportException('Spark statement has unexpected state: %s' % statement_state)
        else:
            self.log('Response code: %s' % resp.status_code, detail=VERBOSE)
            self.log('Response text: %s' % resp.text, detail=VERBOSE)
            resp.raise_for_status()

        return None

    def _livy_import(self, partition_chunk=None) -> Union[int, None]:
        """ Submit PySpark code via Livy REST interface
        """
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        pyspark_body = self._get_pyspark_body(partition_chunk, create_spark_context=False)
        self.log('PySpark: ' + pyspark_body, detail=VVERBOSE)
        session_url = self._attach_livy_session()
        if not self._dry_run:
            self._start_validation_polling_thread()
            job_output = self._submit_pyspark_to_session(session_url, pyspark_body)
            self._stop_validation_polling_thread()
            # In theory we should be able to get rows_imported from Livy logging here but we can't get at
            # the executor logger messages even if we put our Spark Listener in place, therefore we continue
            # to use RDBMS SQL stats.
            rows_imported = self._rdbms_api.log_sql_stats(self._rdbms_module, self._rdbms_action,
                                                          self._drain_validation_polling_thread_queue(),
                                                          validation_polling_interval=self._validation_polling_interval)
            self._check_rows_imported(rows_imported)
            return rows_imported

    def _verify_rdbms_connectivity(self):
        """ Use a simple canary query for verification test """
        rdbms_source_query = '(%s) v' % self._rdbms_api.get_rdbms_canary_query()
        pyspark_body = self._get_pyspark_body(canary_query=rdbms_source_query)
        self.log('PySpark: ' + pyspark_body, detail=VVERBOSE)
        session_url = self._attach_livy_session()
        self._submit_pyspark_to_session(session_url, pyspark_body)
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """ Run the data transport """
        self._reset_transport_context()
        def step_fn():
            row_count = self._livy_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count
        return self._messages.offload_step(command_steps.STEP_STAGING_TRANSPORT, step_fn,
                                           execute=self._offload_options.execute)

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkLivyCanary(OffloadTransportSparkLivy):
    """ Validate Spark Livy connectivity
    """
    def __init__(self, offload_options, messages):
        """ CONSTRUCTOR
            This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        assert offload_options.offload_transport_livy_api_url, 'REST API URL has not been defined'

        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._api_url = offload_options.offload_transport_livy_api_url
        self._livy_requests = OffloadTransportLivyRequests(offload_options, messages)
        # cap the number of Livy sessions we will create before queuing
        self._livy_max_sessions = int(offload_options.offload_transport_livy_max_sessions)\
            if offload_options.offload_transport_livy_max_sessions else LIVY_MAX_SESSIONS
        # We don't want to leave this canary session hanging around but need it there for long enough to use it
        self._idle_session_timeout = 30

        self._dfs_client = get_dfs_from_options(self._offload_options, messages=self._messages)
        self._backend_dfs = self._dfs_client.backend_dfs

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = orchestration_defaults.bool_option_from_string(
            'OFFLOAD_TRANSPORT_CONSISTENT_READ', orchestration_defaults.offload_transport_consistent_read_default()
        )
        self._offload_transport_fetch_size = orchestration_defaults.offload_transport_fetch_size_default()
        self._offload_transport_jvm_overrides = orchestration_defaults.offload_transport_spark_overrides_default()
        self._offload_transport_queue_name = orchestration_defaults.offload_transport_spark_queue_name_default()
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = orchestration_defaults.offload_transport_validation_polling_interval_default()
        self._spark_config_properties = self._prepare_spark_config_properties(orchestration_defaults.offload_transport_spark_properties_default())

        self._rdbms_api = offload_transport_rdbms_api_factory('dummy_owner', 'dummy_table', self._offload_options,
                                                              self._messages, dry_run=self._dry_run)

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


class OffloadTransportSparkBatchesGcloud(OffloadTransportSpark):
    """ Submit PySpark to Dataproc via gcloud to transport data.
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation,
                 offload_options, messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD
        super().__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )
        # For spark-submit we need to pass compression in as a config to the driver program
        self._load_table_compression_pyspark_settings()
        self._offload_fs_container = offload_options.offload_fs_container
        self._dataproc_cluster = offload_options.google_dataproc_cluster
        self._dataproc_project = offload_options.google_dataproc_project
        self._dataproc_region = offload_options.google_dataproc_region
        self._dataproc_service_account = offload_options.google_dataproc_service_account
        self._google_dataproc_batches_subnet = offload_options.google_dataproc_batches_subnet
        self._google_dataproc_batches_version = offload_options.google_dataproc_batches_version

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _spark_cli_safe_value(self, cmd_option_string):
        return self._ssh_cli_safe_value(cmd_option_string)

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=False, remap_sep='=')

    def _remote_copy_spark_control_file(self, options_file_local_path, suffix=''):
        return self._remote_copy_transport_control_file(options_file_local_path, self._offload_transport_cmd_host,
                                                        prefix=SPARK_OPTIONS_FILE_PREFIX, suffix=suffix)

    def _gcloud_dataproc_command(self) -> list:
        gcloud_cmd = [OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE, 'dataproc', 'batches', 'submit', 'pyspark']
        if self._dataproc_project:
            gcloud_cmd.append(f'--project={self._dataproc_project}')
        if self._dataproc_region:
            gcloud_cmd.append(f'--region={self._dataproc_region}')
        if self._dataproc_service_account:
            gcloud_cmd.append(f'--service-account={self._dataproc_service_account}')
        if self._google_dataproc_batches_version:
            gcloud_cmd.append(f'--version={self._google_dataproc_batches_version}')
        if self._google_dataproc_batches_subnet:
            if not self._dataproc_project or not self._dataproc_region:
                raise OffloadTransportException("GOOGLE_DATAPROC_PROJECT and GOOGLE_DATAPROC_REGION are required when using GOOGLE_DATAPROC_BATCHES_SUBNET")
            subnet = "/".join(
                ["projects", self._dataproc_project,
                 "regions", self._dataproc_region,
                 "subnetworks", self._google_dataproc_batches_subnet]
            )
            gcloud_cmd.append(f'--subnet={subnet}')
        gcloud_cmd.append(f'--deps-bucket={self._offload_fs_container}')
        return gcloud_cmd

    def _get_batch_name_option(self) -> list:
        # Dataproc batch names only accept a simple set of characters and 4-63 characters in length
        batch_name_root = re.sub(r'[^a-zA-Z0-9\-]+', '', self._target_db_name + '-' + self._load_table_name)
        batch_name_opt = ['--batch={}'.format(
            self._get_transport_app_name(sep='-', ts=True, name_override=batch_name_root).lower()[:64]
        )]
        return batch_name_opt

    def _get_spark_gcloud_command(self, pyspark_body):
        """ Submit PySpark code via gcloud
        """
        def cli_safe_the_password(k, v):
            if k == 'spark.jdbc.password':
                return self._spark_cli_safe_value(v)
            else:
                return v

        self.log('PySpark: ' + pyspark_body, detail=VVERBOSE)
        options_file_local_path = write_temp_file(pyspark_body, prefix=SPARK_OPTIONS_FILE_PREFIX, suffix='py')
        py_rm_commands, options_file_remote_path = self._remote_copy_spark_control_file(options_file_local_path,
                                                                                        suffix='py')
        if self._spark_listener_included_in_config():
            spark_listener_jar_local_path = self._local_gluent_listener_jar()
            spark_listener_jar_remote_path = self._remote_copy_transport_file(spark_listener_jar_local_path,
                                                                              self._offload_transport_cmd_host)
        else:
            spark_listener_jar_remote_path = None
        self.log('Written PySpark file: %s' % options_file_remote_path, detail=VVERBOSE)

        remote_spark_files_csv = self._remote_copy_transport_file_csv(
            self._spark_files_csv,
            self._offload_transport_cmd_host
        )
        remote_spark_jars_csv = self._remote_copy_transport_file_csv(
            self._spark_jars_csv,
            self._offload_transport_cmd_host
        )

        if spark_listener_jar_remote_path:
            if remote_spark_jars_csv:
                remote_spark_jars_csv = f'{spark_listener_jar_remote_path},{remote_spark_jars_csv}'
            else:
                remote_spark_jars_csv = spark_listener_jar_remote_path

        gcloud_cmd = self._gcloud_dataproc_command()

        spark_config_props, no_log_password = [], []
        [spark_config_props.extend(['%s=%s' % (k, cli_safe_the_password(k, v))])
         for k, v in self._spark_config_properties.items()]
        if 'spark.jdbc.password' in self._spark_config_properties and not self._offload_transport_password_alias:
            # If the rdbms app password is visible in the CLI then obscure it from any logging
            password_config_to_obscure = 'spark.jdbc.password=%s' % \
                cli_safe_the_password('spark.jdbc.password', self._spark_config_properties['spark.jdbc.password'])
            no_log_password = [{'item': password_config_to_obscure, 'prior': '--conf'}]

        if 'spark.cores.min' not in self._spark_config_properties and self._offload_transport_parallelism:
            # If the user has not configured spark.cores.min then default to offload_transport_parallelism + 1
            # This only applies to Dataproc Serverless and therefore is not injected
            # into self._spark_config_properties.
            spark_config_props.extend(['spark.cores.min={}'.format(str(self._offload_transport_parallelism + 1))])

        if self._offload_transport_jvm_overrides:
            spark_config_props.extend([
                f'spark.driver.extraJavaOptions={self._offload_transport_jvm_overrides}',
                f'spark.executor.extraJavaOptions={self._offload_transport_jvm_overrides}'
            ])

        if spark_config_props:
            properties_clause = [f'--properties=^{GCLOUD_PROPERTY_SEPARATOR}^'
                                 + GCLOUD_PROPERTY_SEPARATOR.join(spark_config_props)]
        else:
            properties_clause = []

        jars_opt = [f'--jars={remote_spark_jars_csv}'] if remote_spark_jars_csv else []
        files_opt = [f'--files={remote_spark_files_csv}'] if remote_spark_files_csv else []

        batch_name_opt = self._get_batch_name_option()
        cmd = (
            gcloud_cmd
            + [options_file_remote_path]
            + batch_name_opt
            + jars_opt
            + files_opt
            + properties_clause
        )

        return cmd, no_log_password, py_rm_commands

    def _spark_gcloud_import(self, partition_chunk=None):
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        rows_imported = None
        pyspark_body = self._get_pyspark_body(partition_chunk)
        spark_gcloud_cmd, no_log_password, py_rm_commands = self._get_spark_gcloud_command(pyspark_body)

        self._start_validation_polling_thread()
        rc, cmd_out = self._run_os_cmd(self._ssh_cmd_prefix() + spark_gcloud_cmd, no_log_items=no_log_password)
        self._stop_validation_polling_thread()

        if not self._dry_run:
            rows_imported = self._get_rows_imported_from_spark_log(cmd_out)
            rows_imported_from_sql_stats = self._rdbms_api.log_sql_stats(self._rdbms_module, self._rdbms_action,
                                                                         self._drain_validation_polling_thread_queue(),
                                                                         validation_polling_interval=self._validation_polling_interval)
            if rows_imported is None:
                if rows_imported_from_sql_stats is None:
                    self.warning(MISSING_ROWS_SPARK_WARNING)
                else:
                    self.warning(
                        f'{MISSING_ROWS_SPARK_WARNING}, falling back on RDBMS SQL statistics')
                    rows_imported = rows_imported_from_sql_stats

        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _verify_rdbms_connectivity(self):
        """ Use a simple canary query for verification test """
        rdbms_source_query = '(%s) v' % self._rdbms_api.get_rdbms_canary_query()
        pyspark_body = self._get_pyspark_body(canary_query=rdbms_source_query)
        self.log('PySpark: ' + pyspark_body, detail=VVERBOSE)
        spark_gcloud_cmd, no_log_password, py_rm_commands = self._get_spark_gcloud_command(pyspark_body)
        rc, cmd_out = self._run_os_cmd(self._ssh_cmd_prefix() + spark_gcloud_cmd, no_log_items=no_log_password)
        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]
        # If we got this far then we're in good shape
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Spark by gcloud batches transport"""
        self._reset_transport_context()
        def step_fn():
            row_count = self._spark_gcloud_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count
        return self._messages.offload_step(command_steps.STEP_STAGING_TRANSPORT, step_fn,
                                           execute=self._offload_options.execute)

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkBatchesGcloudCanary(OffloadTransportSparkBatchesGcloud):
    """ Validate Spark Dataproc Serverless connectivity
    """
    def __init__(self, offload_options, messages):
        """ CONSTRUCTOR
            This does not call up the stack to parent constructor because we only want a subset of functionality.
        """
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = orchestration_defaults.bool_option_from_string(
            'OFFLOAD_TRANSPORT_CONSISTENT_READ', orchestration_defaults.offload_transport_consistent_read_default()
        )
        self._offload_transport_fetch_size = orchestration_defaults.offload_transport_fetch_size_default()
        self._offload_transport_jvm_overrides = orchestration_defaults.offload_transport_spark_overrides_default()
        self._offload_transport_queue_name = orchestration_defaults.offload_transport_spark_queue_name_default()
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = orchestration_defaults.offload_transport_validation_polling_interval_default()
        self._spark_config_properties = self._prepare_spark_config_properties(orchestration_defaults.offload_transport_spark_properties_default())

        self._rdbms_api = offload_transport_rdbms_api_factory('dummy_owner', 'dummy_table', self._offload_options,
                                                              self._messages, dry_run=self._dry_run)

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        self._offload_fs_container = offload_options.offload_fs_container
        self._dataproc_cluster = offload_options.google_dataproc_cluster
        self._dataproc_project = offload_options.google_dataproc_project
        self._dataproc_region = offload_options.google_dataproc_region
        self._dataproc_service_account = offload_options.google_dataproc_service_account
        self._google_dataproc_batches_subnet = offload_options.google_dataproc_batches_subnet
        self._google_dataproc_batches_version = offload_options.google_dataproc_batches_version
        self._spark_files_csv = offload_options.offload_transport_spark_files
        self._spark_jars_csv = offload_options.offload_transport_spark_jars

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    def _get_batch_name_option(self) -> list:
        # Dataproc batch names only accept a simple set of characters and 4-63 characters in length
        batch_name_opt = ['--batch={}'.format(
            self._get_transport_app_name(sep='-', ts=True, name_override='canary').lower()[:64]
        )]
        return batch_name_opt

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkDataprocGcloud(OffloadTransportSparkBatchesGcloud):
    """ Submit PySpark to Dataproc via gcloud to transport data.
    """

    def _gcloud_dataproc_command(self) -> list:
        gcloud_cmd = [OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE, 'dataproc', 'jobs', 'submit', 'pyspark']
        if not self._dataproc_cluster:
            raise OffloadTransportException('Missing mandatory configuration: GOOGLE_DATAPROC_CLUSTER')
        gcloud_cmd.append(f'--cluster={self._dataproc_cluster}')
        if self._dataproc_project:
            gcloud_cmd.append(f'--project={self._dataproc_project}')
        if self._dataproc_region:
            gcloud_cmd.append(f'--region={self._dataproc_region}')
        if self._dataproc_service_account:
            gcloud_cmd.append(f'--impersonate-service-account={self._dataproc_service_account}')
        return gcloud_cmd

    def _get_batch_name_option(self) -> list:
        return []


class OffloadTransportSparkDataprocGcloudCanary(OffloadTransportSparkDataprocGcloud):
    """ Validate Dataproc connectivity
    """
    def __init__(self, offload_options, messages):
        """ CONSTRUCTOR
            This does not call up the stack to parent constructor because we only want a subset of functionality.
        """
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = orchestration_defaults.bool_option_from_string(
            'OFFLOAD_TRANSPORT_CONSISTENT_READ', orchestration_defaults.offload_transport_consistent_read_default()
        )
        self._offload_transport_fetch_size = orchestration_defaults.offload_transport_fetch_size_default()
        self._offload_transport_jvm_overrides = orchestration_defaults.offload_transport_spark_overrides_default()
        self._offload_transport_queue_name = orchestration_defaults.offload_transport_spark_queue_name_default()
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = orchestration_defaults.offload_transport_validation_polling_interval_default()
        self._spark_config_properties = self._prepare_spark_config_properties(orchestration_defaults.offload_transport_spark_properties_default())

        self._rdbms_api = offload_transport_rdbms_api_factory('dummy_owner', 'dummy_table', self._offload_options,
                                                              self._messages, dry_run=self._dry_run)

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        self._dataproc_cluster = offload_options.google_dataproc_cluster
        self._dataproc_project = offload_options.google_dataproc_project
        self._dataproc_region = offload_options.google_dataproc_region
        self._dataproc_service_account = offload_options.google_dataproc_service_account
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
    """ Use Python to transport data, only suitable for small chunks
    """

    def __init__(self, offload_source_table, offload_target_table, offload_operation,
                 offload_options, messages, dfs_client, incremental_update_extractor, rdbms_columns_override=None):
        """ CONSTRUCTOR
        """
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
        super(OffloadTransportQueryImport, self).__init__(
            offload_source_table, offload_target_table, offload_operation, offload_options,
            messages, dfs_client, incremental_update_extractor, rdbms_columns_override=rdbms_columns_override
        )
        self._offload_transport_parallelism = 1
        # Cap fetch size at 1000
        self._offload_transport_fetch_size = min(int(self._offload_transport_fetch_size), 1000)
        self._convert_nans_to_nulls = convert_nans_to_nulls(offload_target_table, offload_operation)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _column_type_read_remappings(self):
        # Not applicable to Query Import
        return None

    def _query_import_to_local_fs(self, partition_chunk=None):
        """ Query Import is not partition aware therefore partition_chunk is ignored
        """

        if self._nothing_to_do(partition_chunk):
            return 0

        local_staging_path, _ = schema_paths(self._target_owner, self._target_table_name,
                                             self._load_db_name, self._offload_options,
                                             local_extension_override='.' + self._staging_format.lower())
        dfs_load_path = os.path.join(self._staging_table_location, 'part-m-00000.' + self._staging_format.lower())
        qi_fetch_size = self._offload_transport_fetch_size if self._fetchmany_takes_fetch_size else None

        staging_columns = self._staging_file.get_staging_columns()

        if self._offload_transport_consistent_read:
            self.log('Ignoring --offload-transport-consistent-read for serial transport task', detail=VVERBOSE)

        colexpressions, colnames = self._build_offload_query_lists(
            convert_expressions_on_rdbms_side=self._rdbms_api.convert_query_import_expressions_on_rdbms_side()
        )

        if self._incremental_update_extractor:
            # The source query needs to be an incremental update extraction query
            source_query, source_binds = self._incremental_update_extractor.extract_sql_and_binds(including=False)
        else:
            sql_projection = self._sql_projection_from_offload_query_expression_list(colexpressions, colnames)
            table_name = ('"%s"."%s"' % (self._rdbms_owner, self._rdbms_table_name)).upper()
            if partition_chunk:
                split_row_source_by = self._get_transport_split_type(partition_chunk)
                row_source = self._get_transport_row_source_query(split_row_source_by, partition_chunk)
                source_query = 'SELECT %s\nFROM (%s)' % (sql_projection, row_source)
            else:
                source_query = 'SELECT %s\nFROM   %s' % (sql_projection, table_name)
            if self._rdbms_offload_predicate:
                source_query += '\nWHERE (%s)' % self._rdbms_table.predicate_to_where_clause(self._rdbms_offload_predicate)
            source_binds = None

        self._refresh_rdbms_action()
        rows_imported = None
        if self._dry_run:
            self.log('Extraction sql: %s' % source_query, detail=VERBOSE)
        else:
            encoder = query_import_factory(self._staging_file, self._messages,
                                           compression=self._compress_load_table,
                                           base64_columns=self._base64_staged_columns())
            with self._rdbms_api.query_import_extraction(
                staging_columns, source_query, source_binds,
                self._offload_transport_fetch_size, self._compress_load_table,
                self._get_rdbms_session_setup_commands()
            ) as rdbms_cursor:
                rows_imported = encoder.write_from_cursor(local_staging_path, rdbms_cursor, self._rdbms_columns,
                                                          qi_fetch_size)

        self._check_rows_imported(rows_imported)
        return rows_imported, local_staging_path, dfs_load_path

    def _query_import_copy_to_dfs(self, local_staging_path, dfs_load_path):
        rm_local_file = ['rm', '-f', local_staging_path]
        # Simulate Sqoop's use of recreate load dir
        self.log_dfs_cmd('copy_from_local("%s", "%s")' % (local_staging_path, dfs_load_path))
        if not self._dry_run:
            self._dfs_client.copy_from_local(local_staging_path, dfs_load_path, overwrite=True)
            self._run_os_cmd(rm_local_file)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """ Run the data transport """
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
        rows_imported = self._messages.offload_step(command_steps.STEP_STAGING_TRANSPORT, step_fn,
                                                    execute=self._offload_options.execute)
        return rows_imported

    def ping_source_rdbms(self):
        raise NotImplementedError('ping_source_rdbms not implemented for QueryImport')
