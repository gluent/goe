#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

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

""" BackendHadoopApi: BackendApi implementation for a Hadoop backend, Hive and Impala are subclasses of this.

    Comment from nj@2019-11-06:
      The attraction in creating this module is that, at the time of writing, better_impyla
      has a rich and robust set of methods but I don't know what needs an equivalent in each new
      backend we introduce.
      Via this interface we can get a clear picture of the top level methods required and ensure,
      by using abstractmethod, that all backends offer the same functionality.

      Justification for not dismantling better_impyla from PR:

        better_impyla is not only used by our main orchestration tools. It is also used by other
        tools we have in the repo, such as cloud_sync. Retiring better_impyla completely would
        extend the scope to these other scripts. That was only a secondary consideration though.
        When I decided to use better_impyla as a lower level API rather than try and eliminate it
        the decision was based on there being zero contribution towards the goal of easing
        implementation of other backends. It was all risk and no reward. So I decided to treat that
        as a lower level API, just like Google's big query python module.

        Same for HiveStats & HiveTableStats which are based on better_impyla. There's a lot of
        mature/reliable code in there that contains branches between Hive and Impala but I’ve used
        those as valid building blocks rather than try to move logic away from them.
"""

from datetime import datetime
import logging
import os
import re
import socket
import traceback

# Importing TTransportException from impala.hiveserver2 because Cloudera have switched the underlying
# thrift module in the past preventing the exception from being caught.
from impala.hiveserver2 import TTransportException

from goe.connect.connect_constants import CONNECT_DETAIL, CONNECT_STATUS, CONNECT_TEST
from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.filesystem.goe_dfs import (
    get_scheme_from_location_uri,
    OFFLOAD_FS_SCHEME_S3A,
    OFFLOAD_NON_HDFS_FS_SCHEMES,
)
from goe.offload.backend_api import (
    BackendApiInterface,
    BackendApiException,
    BackendApiConnectionException,
    FETCH_ACTION_ALL,
    FETCH_ACTION_CURSOR,
    FETCH_ACTION_ONE,
    SORT_COLUMNS_UNLIMITED,
    REPORT_ATTR_BACKEND_CLASS,
    REPORT_ATTR_BACKEND_TYPE,
    REPORT_ATTR_BACKEND_DISPLAY_NAME,
    REPORT_ATTR_BACKEND_HOST_INFO_TYPE,
    REPORT_ATTR_BACKEND_HOST_INFO,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    is_safe_mapping,
    match_table_column,
    valid_column_list,
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_VARIABLE_STRING,
    GOE_TYPE_BINARY,
    GOE_TYPE_LARGE_BINARY,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_FLOAT,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_DATE,
    GOE_TYPE_TIME,
    GOE_TYPE_TIMESTAMP,
    GOE_TYPE_TIMESTAMP_TZ,
    GOE_TYPE_INTERVAL_DS,
    GOE_TYPE_INTERVAL_YM,
    GOE_TYPE_BOOLEAN,
    ALL_CANONICAL_TYPES,
    DATE_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_constants import (
    DBTYPE_IMPALA,
    DBTYPE_HIVE,
    FILE_STORAGE_COMPRESSION_CODEC_GZIP,
    FILE_STORAGE_COMPRESSION_CODEC_SNAPPY,
    FILE_STORAGE_COMPRESSION_CODEC_ZLIB,
    EMPTY_BACKEND_TABLE_STATS_LIST,
    EMPTY_BACKEND_TABLE_STATS_DICT,
    EMPTY_BACKEND_COLUMN_STATS_LIST,
    EMPTY_BACKEND_COLUMN_STATS_DICT,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_ORC,
    FILE_STORAGE_FORMAT_PARQUET,
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
    PART_COL_GRANULARITY_YEAR,
)
from goe.offload.hadoop.hadoop_column import (
    HadoopColumn,
    HADOOP_TYPE_CHAR,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_VARCHAR,
    HADOOP_TYPE_BINARY,
    HADOOP_TYPE_TINYINT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_DOUBLE_PRECISION,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_TIMESTAMP,
    HADOOP_TYPE_INTERVAL_DS,
    HADOOP_TYPE_INTERVAL_YM,
    HADOOP_TYPE_BOOLEAN,
)
from goe.util.better_impyla import (
    HiveConnection,
    HiveTable,
    HiveServer2Error,
    BetterImpylaException,
)
from goe.util.hive_table_stats import HiveTableStats
from goe.util.hs2_connection import hs2_connection, HS2_OPTIONS
from goe.util.misc_functions import backtick_sandwich

###############################################################################
# CONSTANTS
###############################################################################

HADOOP_DECIMAL_MAX_PRECISION = 38

# This re is looking for:
#   A column name group        : ^([a-z0-9_]+)
#   An optional opening bracket: [\(]?
#   An optional precision group: ([0-9]*)?
#   An optional comma          : [\,]?
#   An optional scale group    : ([0-9]*)?
#   An optional closing bracket: [\)]?
# It could be more precise using lookahead/lookbehinds but was getting much harder to read
HADOOP_DATA_TYPE_DECODE_RE = re.compile(
    r"^([a-z0-9_]+)[\(]?([0-9]*)?[\,]?([0-9]*)?[\)]?$", re.I
)

# Regular expression matching invalid identifier characters, constant to ensure compiled only once
HADOOP_INVALID_IDENTIFIER_CHARS_RE = re.compile(r"[^A-Z0-9_]", re.I)

HIVE_UDF_LIB = "goe_hive_udf.jar"
IMPALA_UDF_LIB = "to_internal.so"

IMPALA_PROFILE_LOG_LENGTH = 1024 * 32


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def format_hive_set_command(attribute, value):
    return "SET %s=%s" % (attribute, value)


def hive_enable_dynamic_partitions_for_insert_sqls(
    max_dynamic_partitions=None, max_dynamic_partitions_pernode=None, as_dict=False
):
    """Hive session options for use when inserting into a partitioned table."""
    hive_settings = {
        "hive.exec.dynamic.partition": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
    }

    if max_dynamic_partitions:
        hive_settings["hive.exec.max.dynamic.partitions"] = int(max_dynamic_partitions)
    if max_dynamic_partitions_pernode:
        hive_settings["hive.exec.max.dynamic.partitions.pernode"] = int(
            max_dynamic_partitions_pernode
        )

    if as_dict:
        return hive_settings
    else:
        sqls = [format_hive_set_command(k, v) for k, v in hive_settings.items()]
        return sqls


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendHadoopApi
###########################################################################


class BackendHadoopApi(BackendApiInterface):
    """Common Hadoop backend implementation
    Impala and Hive share a lot of functionality, this class holds those
    common methods. Also Hadoop specifics, such as HDFS, should be found
    in this Hadoop class.
    """

    def __init__(
        self,
        connection_options,
        backend_type,
        messages,
        dry_run=False,
        no_caching=False,
        do_not_connect=False,
    ):
        """CONSTRUCTOR"""
        super(BackendHadoopApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        self._sql_engine_name = "Hadoop"

        self._target_version = None

        if do_not_connect:
            self._hive_conn = None
        else:
            self._validate_connection_options(HS2_OPTIONS)
            self._hive_conn = self._connect_to_backend()
            # Whenever we make a new connection we should set global session parameters
            self._execute_global_session_parameters()

        self._cached_hive_tables = {}
        self._cached_hive_table_stats = {}

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _align_decimal_scale_to_udfs(self, column_name, data_precision, data_scale):
        """Pad/round up decimal precision and scale in a number of ways:
        1) If precision is unset then assume it is the max possible for Hadoop
        2) UDFs are on even numbered scales only, increase if applicable and if precision has headroom
        3) Increase precision to accommodate any scale increases in 2
        4) The precision we end up with is finally increased to either 18 or 38 as appropriate
        Returns a tuple of the adjusted precision & scale
        """
        self._debug(
            "Aligning decimal scale to UDFs: %s (%s,%s)"
            % (column_name, data_precision, data_scale)
        )

        if not data_precision and not data_scale:
            return data_precision, data_scale

        private_precision = (
            data_precision if data_precision else self.max_decimal_precision()
        )
        new_precision = data_precision
        new_scale = data_scale

        # Round precision and scale up to even numbers
        if private_precision < self.max_decimal_precision():
            # Change scale after precision
            if new_precision:
                new_precision += new_scale % 2
            new_scale += new_scale % 2
        return new_precision, new_scale

    def _align_decimal_precision_to_udfs(self, column_name, data_precision, data_scale):
        self._debug(
            "Aligning decimal precision to UDFs: %s (%s,%s)"
            % (column_name, data_precision, data_scale)
        )
        new_precision = (
            data_precision if data_precision else self.max_decimal_precision()
        )
        # Precision should be 18 or self.max_decimal_precision()
        new_precision = (
            max(new_precision, 18)
            if new_precision < 19
            else max(new_precision, self.max_decimal_precision())
        )
        return new_precision, data_scale

    def _alter_table_external(self, db_name, table_name, external=True):
        """Execute Hive SQL to switch a table to external or managed"""
        assert db_name and table_name
        alter_sql = "ALTER TABLE {}.{} SET TBLPROPERTIES ('EXTERNAL'='{}')".format(
            self.enclose_identifier(db_name),
            self.enclose_identifier(table_name),
            "TRUE" if external else "FALSE",
        )
        return self.execute_ddl(alter_sql)

    def _backend_capabilities(self):
        raise NotImplementedError(
            "_backend_capabilities() is not implemented for common Hadoop class"
        )

    def _connect_to_backend(self, host_override=None, port_override=None):
        # Create a connection to backend HiveServer
        host = host_override or self._connection_options.hadoop_host
        port = port_override or self._connection_options.hadoop_port
        try:
            return HiveConnection.fromconnection(
                hs2_connection(
                    self._connection_options, host_override=host, port_override=port
                ),
                db_type=self._backend_type,
            )
        except (socket.error, TTransportException) as exc:
            self._log(traceback.format_exc(), detail=VERBOSE)
            if "TSocket read 0 bytes" in str(
                exc
            ) or "Bad status: 3 (Error validating the login)" in str(exc):
                encryption_text = (
                    "ly encrypted" if self._connection_options.password_key_file else ""
                )
                raise BackendApiConnectionException(
                    self._get_hadoop_connection_exception_message_template()
                    % {"host": host, "port": port, "enc_text": encryption_text}
                )
            if "Credentials cache file" in str(exc):
                raise BackendApiConnectionException(
                    "Check klist output - orchestration tools require a kerberos authenticated session"
                )
            raise

    def _decode_hadoop_data_type_spec(self, data_type_spec):
        """Takes a data type spec as output by DESCRIBE, e.g. DECIMAL(10,2), and
        returns 4 components:
            (data type, data_length, data_precision, data_scale)
        """
        m = HADOOP_DATA_TYPE_DECODE_RE.match(data_type_spec.upper())
        if not m:
            raise NotImplementedError(
                "Unsupported backend datatype: %s" % data_type_spec.upper()
            )
        data_type, precision, scale = m.groups()
        precision = None if precision == "" else precision
        scale = None if scale == "" else scale
        if not self.is_supported_data_type(data_type):
            raise NotImplementedError("Unsupported backend datatype: %s" % data_type)
        if data_type == HADOOP_TYPE_DECIMAL:
            return (data_type, None, int(precision), int(scale))
        elif data_type in (HADOOP_TYPE_VARCHAR, HADOOP_TYPE_CHAR):
            return (data_type, int(precision), None, None)
        else:
            return (data_type, None, None, None)

    def _drop_udf(
        self, function_name, udf_db=None, function_spec=None, sync=None, if_exists=True
    ):
        """Drop a Hadoop UDF, used for both Hive and Impala"""
        assert function_name
        log_level = VVERBOSE if self._dry_run else VERBOSE
        db_clause = (self.enclose_identifier(udf_db) + ".") if udf_db else ""
        spec_clause = "" if function_spec is None else "({})".format(function_spec)
        exists_clause = "" if not if_exists else "IF EXISTS "
        drop_sql = "DROP FUNCTION %s%s%s%s" % (
            exists_clause,
            db_clause,
            self.enclose_identifier(function_name),
            spec_clause,
        )
        return self.execute_ddl(drop_sql, sync=sync, log_level=log_level)

    def _execute_query_fetch_x(
        self,
        sql,
        fetch_action=FETCH_ACTION_ALL,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
        as_dict=False,
    ):
        """Run a query and fetch results
        query_options: key/value pairs for session settings
        query_params: Not applicable on Hadoop
        log_level: None = no logging. Otherwise VERBOSE/VVERBOSE etc
        time_sql: If logging then log SQL elapsed time
        not_when_dry_running: Some SQL will only work in execute mode, such as queries on a load table
        as_dict: When True each row is returned as a dict keyed by column name rather than a tuple/list.
        """
        assert sql

        self._execute_session_options(query_options, log_level=log_level)

        self._log_or_not(
            "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
        )

        if self._dry_run and not_when_dry_running:
            return None

        t1 = datetime.now().replace(microsecond=0)
        if fetch_action == FETCH_ACTION_ALL:
            cursor_fn = lambda c: c.fetchall()
        elif fetch_action == FETCH_ACTION_ONE:
            cursor_fn = lambda c: c.fetchone()
        else:
            cursor_fn = lambda c: c

        rows = self._hive_conn.execute(sql, cursor_fn=cursor_fn)

        if as_dict:
            columns = self._cursor_projection(self._hive_conn.cursor)
            if fetch_action == FETCH_ACTION_ALL:
                rows = [self._cursor_row_to_dict(columns, _) for _ in rows]
            else:
                rows = self._cursor_row_to_dict(columns, rows)

        if time_sql:
            t2 = datetime.now().replace(microsecond=0)
            self._log("Elapsed time: %s" % (t2 - t1), detail=VVERBOSE)

        if profile:
            sql_profile = self._get_query_profile()
            if sql_profile:
                self._log(sql_profile[:IMPALA_PROFILE_LOG_LENGTH], detail=VVERBOSE)
        return rows

    def _execute_global_session_parameters(self, log_level=VVERBOSE):
        """Execute global backend session parameters.
        We do this outside of _execute_session_options() so that global settings can be applied before any
        localised requirements. If we merged the dicts we would lose the order, I (NJ) made the call that localised
        settings should take precedence over global settings, this may prove to be the wrong way around, only
        time will tell.
        """
        if self._global_session_parameters:
            if log_level is not None:
                self._log("Setting global session options:", detail=log_level)
            return self._execute_session_options(
                self._global_session_parameters, log_level=log_level
            )
        else:
            return []

    def _execute_session_options(self, query_options, log_level):
        """Sharing code between Hive and Impala"""
        return_list = []
        if query_options:
            for prep_sql in self._format_query_options(query_options):
                self._log_or_not(
                    "%s SQL: %s" % (self._sql_engine_name, prep_sql),
                    log_level=log_level,
                )
                if self._hive_conn:
                    self._hive_conn.execute(prep_sql)
                return_list.append(prep_sql)
            if self._hive_conn:
                self._hive_conn._cursor.execute("set")
        return return_list

    def _execute_sqls(self, sql, log_level=VERBOSE, profile=None, no_log_items=None):
        """Sharing code between Hive and Impala"""
        return_list = []
        sqls = [sql] if isinstance(sql, str) else sql
        for i, run_sql in enumerate(sqls):
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, run_sql),
                log_level=log_level,
                no_log_items=no_log_items,
            )
            return_list.append(run_sql)
            if not self._dry_run:
                self._hive_conn.execute(run_sql)
                if profile and i == 0:
                    # Log a backend profile
                    sql_profile = self._get_query_profile()
                    if sql_profile:
                        self._log(
                            sql_profile[:IMPALA_PROFILE_LOG_LENGTH], detail=VVERBOSE
                        )
                    return_list.append("PROFILE")
        return return_list

    def _format_partition_clause(self, partition_tuples, sep_char="/"):
        """Formats a partition clause as a HDFS style string"""

        def str_value_fn(val):
            return self._partition_clause_null_constant() if val is None else val

        if not partition_tuples:
            return None
        return sep_char.join(
            "{part_col}={part_value}".format(
                part_col=_[0], part_value=str_value_fn(_[1])
            )
            for _ in partition_tuples
        )

    def _format_partition_clause_for_sql(self, db_name, table_name, partition_tuples):
        """Formats a partition clause as a SQL style string
        Requires db/table info in order to get the data types and format the literal correctly
        """
        assert db_name and table_name
        if not partition_tuples:
            return None
        assert isinstance(partition_tuples, list)
        assert isinstance(partition_tuples[0], tuple), "%s is not tuple" % type(
            partition_tuples[0]
        )

        part_cols = self.get_partition_columns(db_name, table_name)
        formatted_partition_tuples = []
        for part_name, part_val in partition_tuples:
            part_col = match_table_column(part_name, part_cols)
            part_val = (
                self.to_backend_literal(part_val)
                if part_col.is_string_based()
                else part_val
            )
            formatted_partition_tuples.append((backtick_sandwich(part_name), part_val))
        return self._format_partition_clause(formatted_partition_tuples, sep_char=",")

    def _format_query_options(self, query_options=None):
        """Format options for Hive & Impala
        query_options: key/value pairs for session settings
        """
        if not query_options:
            return []
        assert isinstance(query_options, dict)
        return [format_hive_set_command(k, v) for k, v in query_options.items()]

    def _gen_sample_stats_sql_sample_clause(
        self, db_name, table_name, sample_perc=None
    ):
        """No Query SAMPLE clause on Hadoop"""
        return None

    def _get_hive_stats_table(self, db_name, table_name):
        """Instantiates HiveTableStats if required or retrieves from cache"""
        ht_id = (db_name + "_" + table_name).lower()
        if ht_id not in self._cached_hive_table_stats:
            self._hive_conn.refresh_cursor()
            hive_stats = HiveTableStats.construct(db_name, table_name, self._hive_conn)
            if self._no_caching:
                return hive_stats
            self._cached_hive_table_stats[ht_id] = hive_stats
        return self._cached_hive_table_stats[ht_id]

    def _get_hadoop_connection_exception_message_template(self):
        raise NotImplementedError(
            "_get_hadoop_connection_exception_message_template() is not implemented for common Hadoop class"
        )

    def _get_hive_table(self, db_name, table_name, no_cache=False):
        """Instantiates HiveTable if required or retrieves from cache"""
        ht_id = (db_name + "_" + table_name).lower()
        if ht_id not in self._cached_hive_tables or no_cache:
            hive_table = HiveTable(db_name, table_name, self._hive_conn)
            if self._no_caching:
                return hive_table
            self._cached_hive_tables[ht_id] = hive_table
        return self._cached_hive_tables[ht_id]

    def _get_query_profile(self, query_identifier=None):
        raise NotImplementedError(
            "_get_query_profile() is not implemented for common Hadoop class"
        )

    def _get_table_stats(self, hive_stats, as_dict=False, part_stats=False):
        raise NotImplementedError(
            "_get_table_stats() is not implemented for common Hadoop class"
        )

    def _insert_literal_values_format_sql(
        self, db_name, table_name, column_names, literal_csv_list, split_by_cr=True
    ):
        raise NotImplementedError(
            "_insert_literal_values_format_sql() is not implemented for common Hadoop class"
        )

    def _invalid_identifier_character_re(self):
        return HADOOP_INVALID_IDENTIFIER_CHARS_RE

    def _legacy_column_list_to_type(self, legacy_columns):
        backend_columns = []
        if not legacy_columns:
            return backend_columns
        for col_name, data_type_spec, _ in legacy_columns:
            (
                data_type,
                data_length,
                precision,
                scale,
            ) = self._decode_hadoop_data_type_spec(data_type_spec)
            backend_columns.append(
                HadoopColumn(
                    col_name,
                    data_type=data_type,
                    data_length=data_length,
                    data_precision=precision,
                    data_scale=scale,
                    nullable=None,
                    data_default=None,
                )
            )
        return backend_columns

    def _partition_clause_null_constant(self):
        raise NotImplementedError(
            "_partition_clause_null_constant() is not implemented for common Hadoop class"
        )

    def _udf_installation_sql(self, udf_db=None):
        raise NotImplementedError(
            "_udf_installation_sql() is not implemented for common Hadoop class"
        )

    def _udf_installation_copy_library_to_hdfs(
        self, udf_lib_source, udf_lib_destination
    ):
        """Copies the UDF library from OFFLOAD_HOME/bin (CWD) to the home directory of hadoop_ssh_user
        on the edge node and then copies from there into HDFS (local or Cloud Storage)
        Returns a list of commands executed
        This is shared code for Hive and Impala
        """

        self._log("UDF copy source: %s" % udf_lib_source, detail=VVERBOSE)
        self._log("UDF copy target: %s" % udf_lib_destination, detail=VVERBOSE)

        cmds = []
        hdfs_client = get_dfs_from_options(
            self._connection_options, messages=self._messages, dry_run=self._dry_run
        )
        if self._connection_options.offload_fs_scheme in OFFLOAD_NON_HDFS_FS_SCHEMES:
            target_uri = hdfs_client.gen_uri(
                self._connection_options.offload_fs_scheme,
                self._connection_options.offload_fs_container,
                self._connection_options.offload_fs_prefix,
            )
        else:
            target_uri = self._connection_options.hdfs_home
        # Copy the file to the edge node
        # UDF library is in OFFLOAD_HOME
        local_file = os.path.join(os.environ.get("OFFLOAD_HOME"), "bin", udf_lib_source)
        target_file = os.path.join(target_uri, udf_lib_destination)
        log_cmd = 'HDFS cmd: copy_from_local("%s", "%s")' % (local_file, target_file)
        self._log(log_cmd, detail=VERBOSE)
        hdfs_client.copy_from_local(local_file, target_file, overwrite=True)
        cmds.append(log_cmd)
        return cmds

    def _udf_test_sql(self, udf_db=None):
        raise NotImplementedError(
            "_udf_test_sql() is not implemented for common Hadoop class"
        )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def add_columns(self, db_name, table_name, column_tuples, sync=None):
        """See interface spec for description"""
        assert db_name and table_name
        assert column_tuples
        assert isinstance(column_tuples, list), "%s is not list" % type(column_tuples)
        assert isinstance(column_tuples[0], (tuple, list))
        column_clause = ", ".join(
            "{} {}".format(self.enclose_identifier(col_name.lower()), col_type.lower())
            for col_name, col_type in column_tuples
        )
        sql = "ALTER TABLE %s.%s ADD COLUMNS (%s)" % (
            self.enclose_identifier(db_name),
            self.enclose_identifier(table_name),
            column_clause,
        )
        return self.execute_ddl(sql, sync=sync)

    def backend_report_info(self):
        """Reporting information about the type of backend and how to display
        its configuration details (e.g. Hadoop host:port or BigQuery project).
        """
        return {
            REPORT_ATTR_BACKEND_CLASS: "Hadoop",
            REPORT_ATTR_BACKEND_TYPE: self._backend_type,
            REPORT_ATTR_BACKEND_DISPLAY_NAME: self.backend_db_name(),
            REPORT_ATTR_BACKEND_HOST_INFO_TYPE: "Host:port",
            REPORT_ATTR_BACKEND_HOST_INFO: "%s:%s"
            % (
                self._connection_options.hadoop_host,
                self._connection_options.hadoop_port,
            ),
        }

    def check_backend_supporting_objects(self, orchestration_options):
        results = []

        try:
            user = self.get_user_name()
            results.append(
                {
                    CONNECT_TEST: "Test backend user",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "Backend User: %s" % user,
                }
            )
        except Exception as exc:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            results.append(
                {
                    CONNECT_TEST: "Test backend user",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Unable to obtain backend user: %s" % str(exc),
                }
            )

        try:
            v = self.backend_version()
            if v:
                results.append(
                    {
                        CONNECT_TEST: "Backend version",
                        CONNECT_STATUS: True,
                        CONNECT_DETAIL: v,
                    }
                )
            else:
                results.append(
                    {
                        CONNECT_TEST: "Backend version",
                        CONNECT_STATUS: False,
                        CONNECT_DETAIL: None,
                    }
                )
        except Exception as exc:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            results.append(
                {
                    CONNECT_TEST: "Backend version",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Unable to query version: %s" % str(exc),
                }
            )

        return results

    def close(self):
        if self._hive_conn:
            # We don't properly close better_impyla connections because there is connection sharing occurring
            # which I (NJ) cannot fully track down. Instead I drop state so we'll start afresh if required.
            self.drop_state()

    def create_database(
        self, db_name, comment=None, properties=None, with_terminator=False
    ):
        """Create a Hadoop database.
        properties: Allows properties["location"] to specify a DFS location
        """
        assert db_name
        if properties:
            assert isinstance(properties, dict)
        if comment:
            assert isinstance(comment, str)

        sql = "CREATE DATABASE IF NOT EXISTS %s" % self.enclose_identifier(db_name)
        if comment:
            sql += " COMMENT '%s'" % comment
        if properties and properties.get("location"):
            sql += " LOCATION '%s'" % properties["location"]
        if with_terminator:
            sql += ";"

        return self.execute_ddl(sql)

    def create_view(
        self,
        db_name,
        view_name,
        column_tuples,
        ansi_joined_tables,
        filter_clauses=None,
        sync=None,
    ):
        """Create a Hadoop view.
        See create_view() description for parameter descriptions.
        """
        projection = self._format_select_projection(column_tuples)
        where_clause = (
            "\nWHERE  " + "\nAND    ".join(filter_clauses) if filter_clauses else ""
        )
        sql = """CREATE VIEW %(db)s.%(view)s AS
SELECT %(projection)s
FROM   %(from_tables)s%(where_clause)s""" % {
            "db": self.enclose_identifier(db_name),
            "view": self.enclose_identifier(view_name),
            "projection": projection,
            "from_tables": ansi_joined_tables,
            "where_clause": where_clause,
        }
        return self.execute_ddl(sql, sync=sync)

    def data_type_accepts_length(self, data_type):
        return bool(
            data_type in [HADOOP_TYPE_CHAR, HADOOP_TYPE_DECIMAL, HADOOP_TYPE_VARCHAR]
        )

    def database_exists(self, db_name):
        assert db_name
        return self._hive_conn.database_exists(db_name.lower())

    def db_name_label(self, initcap=False):
        label = "database"
        return label.capitalize() if initcap else label

    def default_date_based_partition_granularity(self):
        return PART_COL_GRANULARITY_MONTH

    def default_storage_compression(self, user_requested_codec, user_requested_format):
        codec = user_requested_codec or FILE_STORAGE_COMPRESSION_CODEC_SNAPPY
        if codec == "HIGH" and user_requested_format == FILE_STORAGE_FORMAT_ORC:
            codec = FILE_STORAGE_COMPRESSION_CODEC_ZLIB
        elif codec == "HIGH" and user_requested_format == FILE_STORAGE_FORMAT_PARQUET:
            codec = FILE_STORAGE_COMPRESSION_CODEC_GZIP
        elif codec == "MED":
            codec = FILE_STORAGE_COMPRESSION_CODEC_SNAPPY
        return codec

    def derive_native_partition_info(self, db_name, table_name, column, position):
        """Return a ColumnPartitionInfo object (or None) based on native partition settings.
        No such thing on Hadoop.
        """
        return None

    def detect_column_has_fractional_seconds(self, db_name, table_name, column):
        assert db_name and table_name
        assert isinstance(column, HadoopColumn)
        # we can rule out anything other than timestamp in Hadoop
        if column.data_type != HADOOP_TYPE_TIMESTAMP:
            return False
        sql = """SELECT %(col)s
FROM %(db_table)s
WHERE UNIX_TIMESTAMP(%(col)s) >= 0 AND %(col)s != FROM_UNIXTIME(UNIX_TIMESTAMP(%(col)s))
LIMIT 1""" % {
            "db_table": self.enclose_object_reference(db_name, table_name),
            "col": self.enclose_identifier(column.name),
        }
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return True if row else False

    def drop_state(self):
        """Drop any cached state so values are refreshed on next request."""
        self._cached_hive_tables = {}

    def drop_table(self, db_name, table_name, purge=False, if_exists=True, sync=None):
        """Drop a table and cater for backend specific details
        See abstract method for more description
        """
        assert db_name and table_name
        if_exists_clause = "IF EXISTS " if if_exists else ""
        drop_sql = "DROP TABLE %s%s" % (
            if_exists_clause,
            self.enclose_object_reference(db_name, table_name),
        )
        if purge:
            drop_sql += " PURGE"
        return self.execute_ddl(drop_sql, sync=sync)

    def drop_view(self, db_name, view_name, if_exists=True, sync=None):
        assert db_name and view_name
        if_exists_clause = "IF EXISTS " if if_exists else ""
        drop_sql = "DROP VIEW %s%s" % (
            if_exists_clause,
            self.enclose_object_reference(db_name, view_name),
        )
        return self.execute_ddl(drop_sql, sync=sync)

    def enclose_identifier(self, identifier):
        """Backtick identifiers on Hadoop, also lower case them for historical reasons."""
        if identifier is None:
            return None
        return backtick_sandwich(identifier.lower(), ch=self.enclosure_character())

    def enclose_object_reference(self, db_name, object_name):
        """Backtick identifiers on Hadoop, also lower case them for historical reasons."""
        assert db_name and object_name
        return (
            self.enclose_identifier(db_name.lower())
            + "."
            + self.enclose_identifier(object_name.lower())
        )

    def enclosure_character(self):
        return "`"

    def execute_query_fetch_all(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
        as_dict=False,
    ):
        """For parameter descriptions see: _execute_query_fetch_x"""
        return self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_ALL,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            profile=profile,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            as_dict=as_dict,
        )

    def execute_query_fetch_one(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
        as_dict=False,
    ):
        """For parameter descriptions see: _execute_query_fetch_x"""
        return self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_ONE,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            profile=profile,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            as_dict=as_dict,
        )

    def execute_query_get_cursor(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
    ):
        """Run a query and return the cursor to allow the calling program to handle fetching"""
        return self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_CURSOR,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
        )

    def exists(self, db_name, object_name):
        """Does the table/view exist.
        When using impyla module table_exists() is for views too.
        """
        assert db_name
        assert object_name
        return self._hive_conn.table_exists(db_name.lower(), object_name.lower())

    def format_hadoop_partition_clause(self, partition_tuples, sep_char="/"):
        """Hadoop specific function call for use from Hadoop specific BackendTable function
        of Hadoop specific BackendTestingApi.
        There is no abstractmethod for this function
        """
        assert self._backend_type in [DBTYPE_HIVE, DBTYPE_IMPALA]
        return self._format_partition_clause(partition_tuples, sep_char=sep_char)

    def format_query_parameter(self, param_name):
        return param_name

    def gen_column_object(self, column_name, **kwargs):
        return HadoopColumn(column_name, **kwargs)

    def gen_default_numeric_column(self, column_name, data_scale=18):
        """Return a default best-fit numeric column for generic usage.
        Used when we can't really know or influence a type, e.g. AVG() output in AAPD.
        data_scale=0 can be used if the outcome is known to be integral.
        """
        return HadoopColumn(
            column_name,
            data_type=HADOOP_TYPE_DECIMAL,
            data_precision=self.max_decimal_precision(),
            data_scale=data_scale,
            nullable=True,
            safe_mapping=False,
        )

    def gen_ctas_sql_text(
        self,
        db_name,
        table_name,
        storage_format,
        column_tuples,
        from_db_name=None,
        from_table_name=None,
        row_limit=None,
        external=False,
        table_properties=None,
    ):
        """See interface description for parameter descriptions"""
        assert db_name and table_name
        assert storage_format
        assert column_tuples
        assert isinstance(column_tuples, list), "%s is not list" % type(column_tuples)
        assert isinstance(column_tuples[0], (tuple, list))

        external_clause = " EXTERNAL" if external else ""
        projection = self._format_select_projection(column_tuples)
        from_clause = (
            "\nFROM   {}".format(
                self.enclose_object_reference(from_db_name, from_table_name)
            )
            if from_db_name and from_table_name
            else ""
        )
        limit_clause = "\nLIMIT  {}".format(row_limit) if row_limit is not None else ""
        if table_properties:
            table_prop_clause = "\nTBLPROPERTIES (%s)" % ", ".join(
                "%s=%s" % (self.to_backend_literal(k), self.to_backend_literal(v))
                for k, v in table_properties.items()
            )
        else:
            table_prop_clause = ""
        sql = """CREATE%(external_clause)s TABLE %(db_table)s STORED AS %(storage)s%(table_prop_clause)s AS
SELECT %(projection)s%(from_clause)s%(limit_clause)s""" % {
            "external_clause": external_clause,
            "db_table": self.enclose_object_reference(db_name, table_name),
            "storage": storage_format,
            "table_prop_clause": table_prop_clause,
            "projection": projection,
            "from_clause": from_clause,
            "limit_clause": limit_clause,
        }
        return sql

    def gen_sql_text(
        self,
        db_name,
        table_name,
        column_names=None,
        filter_clauses=None,
        measures=None,
        agg_fns=None,
    ):
        return self._gen_sql_text_common(
            db_name,
            table_name,
            column_names=column_names,
            filter_clauses=filter_clauses,
            measures=measures,
            agg_fns=agg_fns,
        )

    def generic_string_data_type(self):
        return HADOOP_TYPE_STRING

    def get_columns(self, db_name, table_name):
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name)
        legacy_columns = hive_table.all_columns()

        return self._legacy_column_list_to_type(legacy_columns)

    def gen_copy_into_sql_text(
        self,
        db_name,
        table_name,
        from_object_clause,
        select_expr_tuples,
        filter_clauses=None,
    ):
        raise NotImplementedError("COPY INTO does not exist in Hadoop SQL")

    def get_distinct_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        order_results=False,
        not_when_dry_running=False,
    ):
        """Run SQL to get distinct values for a list of columns
        The columns_to_cast_to_string parameter is because our HiveServer2 DBAPI does not support nanoseconds
        therefore we bring certain data types back as strings to avoid truncating to milliseconds
        """

        def add_sql_cast(col):
            return (
                ("CAST(%s as STRING)" % col)
                if col in (columns_to_cast_to_string or [])
                else col
            )

        assert column_name_list and isinstance(column_name_list, list)
        assert isinstance(column_name_list[0], str)
        expression_list = [
            add_sql_cast(self.enclose_identifier(_)) for _ in column_name_list
        ]
        return self.get_distinct_expressions(
            db_name,
            table_name,
            expression_list,
            order_results=order_results,
            not_when_dry_running=not_when_dry_running,
        )

    def gen_native_range_partition_key_cast(self, partition_column):
        """No native range partitioning on Hadoop"""
        raise NotImplementedError("Native range partitioning unsupported on Snowflake")

    def get_hive_conn(self):
        return self._hive_conn

    def get_partition_columns(self, db_name, table_name):
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name)
        part_cols_as_list = hive_table.partition_columns()
        return self._legacy_column_list_to_type(part_cols_as_list)

    def get_session_option(self, option_name):
        raise NotImplementedError(
            "get_session_option() is not implemented for common Hadoop class"
        )

    def get_table_ddl(
        self, db_name, table_name, as_list=False, terminate_sql=False, for_replace=False
    ):
        """Return table DDL as a string (or a list of strings split on CR if as_list=True)
        for_replace ignored on Hadoop
        """
        assert db_name and table_name
        self._log("Fetching table DDL: %s.%s" % (db_name, table_name), detail=VVERBOSE)
        try:
            hive_table = self._get_hive_table(db_name, table_name)
        except BetterImpylaException as exc:
            # Convert BetterImpylaExceptions to BackendApi
            raise BackendApiException(str(exc)) from exc
        ddl_str = hive_table.table_ddl(terminate_sql=terminate_sql)
        if not ddl_str:
            raise BackendApiException(
                "Table does not exist for DDL retrieval: %s.%s" % (db_name, table_name)
            )
        self._debug("Table DDL: %s" % ddl_str)
        if as_list:
            return ddl_str.split("\n")
        else:
            return ddl_str

    def get_table_location(self, db_name, table_name):
        if self.table_exists(db_name, table_name):
            hive_table = self._get_hive_table(db_name, table_name)
            return hive_table.table_location()
        else:
            return None

    def get_table_partition_count(self, db_name, table_name):
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name)
        return len(hive_table.table_partitions())

    def get_table_partitions(self, db_name, table_name):
        raise NotImplementedError(
            "get_table_partitions() is not implemented for common Hadoop class"
        )

    def get_table_row_count_from_metadata(self, db_name, table_name):
        hive_table = self._get_hive_table(db_name, table_name)
        return hive_table.num_rows()

    def get_table_size_and_row_count(self, db_name, table_name):
        return (
            self.get_table_size(db_name, table_name),
            self.get_table_row_count_from_metadata(db_name, table_name),
        )

    def get_table_stats(self, db_name, table_name, as_dict=False):
        if self.table_exists(db_name, table_name) and self.table_stats_get_supported():
            hive_stats = self._get_hive_stats_table(db_name, table_name)
            tab_stats, _, col_stats = self._get_table_stats(hive_stats, as_dict=as_dict)
            return tab_stats, col_stats
        else:
            if as_dict:
                return EMPTY_BACKEND_TABLE_STATS_DICT, EMPTY_BACKEND_COLUMN_STATS_DICT
            else:
                return EMPTY_BACKEND_TABLE_STATS_LIST, EMPTY_BACKEND_COLUMN_STATS_LIST

    def get_table_and_partition_stats(self, db_name, table_name, as_dict=False):
        """This has been broken out from get_table_stats() because the output looks pretty Hadoop specific
        If we decide we need this functionality for another backend then we should look at the format
        and decide what to do
        """
        if self.table_exists(db_name, table_name) and self.table_stats_get_supported():
            hive_stats = self._get_hive_stats_table(db_name, table_name)
            return self._get_table_stats(hive_stats, as_dict=as_dict, part_stats=True)
        else:
            if as_dict:
                return (
                    EMPTY_BACKEND_TABLE_STATS_DICT,
                    {},
                    EMPTY_BACKEND_COLUMN_STATS_DICT,
                )
            else:
                return (
                    EMPTY_BACKEND_TABLE_STATS_LIST,
                    [],
                    EMPTY_BACKEND_COLUMN_STATS_LIST,
                )

    def get_table_stats_partitions(self, db_name, table_name):
        if not self.table_stats_get_supported():
            return []
        hive_stats = self._get_hive_stats_table(db_name, table_name)
        return hive_stats.table_partitions()

    def insert_literal_values(
        self,
        db_name,
        table_name,
        literal_list,
        column_list=None,
        max_rows_per_insert=250,
        split_by_cr=True,
    ):
        """Used to insert specific data into a table. The table should already exist.
        literal_list: A list of rows to insert (a list of lists).
                      The row level lists should contain the exact right number of columns
        column_list: The columns to be inserted, if left blank this will default to all columns in the table
        Disclaimer: This code is used in testing and generating the sequence table. It is not robust enough to
                    be a part of any Offload Transport
        """

        def gen_literal(py_val):
            return str(self.to_backend_literal(py_val))

        def add_cast(literal, formatted_data_type):
            if formatted_data_type.upper() in [
                HADOOP_TYPE_STRING,
                HADOOP_TYPE_TIMESTAMP,
            ]:
                return literal
            elif (
                self._backend_type == DBTYPE_HIVE
                and HADOOP_TYPE_VARCHAR in formatted_data_type.upper()
            ):
                # No CAST if Hive and VARCHAR2 because UNION ALL failing with NullPointerException.
                # This is a fudge really but only actually used in testing so not a big issue.
                return literal
            return "CAST(%s AS %s)" % (literal, formatted_data_type)

        assert db_name
        assert table_name
        assert literal_list and isinstance(literal_list, list)
        assert isinstance(literal_list[0], list)

        if column_list:
            assert valid_column_list(column_list), (
                "Incorrectly formed column_list: %s" % column_list
            )

        column_list = column_list or self.get_columns(db_name, table_name)
        column_names = [_.name for _ in column_list]
        data_type_strs = [_.format_data_type() for _ in column_list]

        cmds = []
        remaining_rows = literal_list[:]
        while remaining_rows:
            this_chunk = remaining_rows[:max_rows_per_insert]
            remaining_rows = remaining_rows[max_rows_per_insert:]
            formatted_rows = []
            for row in this_chunk:
                formatted_rows.append(
                    ",".join(
                        add_cast(gen_literal(py_val), data_type)
                        for py_val, data_type in zip(row, data_type_strs)
                    )
                )
            sql = self._insert_literal_values_format_sql(
                db_name,
                table_name,
                column_names,
                formatted_rows,
                split_by_cr=split_by_cr,
            )
            cmds.extend(self.execute_dml(sql, log_level=VVERBOSE))
        return cmds

    def is_valid_partitioning_data_type(self, data_type):
        if not data_type:
            return False
        # TODO Add HADOOP_TYPE_DATE to list below when GOE-1458 is complete
        return bool(
            data_type.upper()
            in [
                HADOOP_TYPE_BIGINT,
                HADOOP_TYPE_CHAR,
                HADOOP_TYPE_DECIMAL,
                HADOOP_TYPE_DOUBLE,
                HADOOP_TYPE_FLOAT,
                HADOOP_TYPE_INT,
                HADOOP_TYPE_REAL,
                HADOOP_TYPE_STRING,
                HADOOP_TYPE_SMALLINT,
                HADOOP_TYPE_TIMESTAMP,
                HADOOP_TYPE_TINYINT,
                HADOOP_TYPE_VARCHAR,
            ]
        )

    def is_valid_sort_data_type(self, data_type):
        """No known data type restrictions for sorting in Hadoop."""
        return True

    def is_valid_storage_compression(self, user_requested_codec, user_requested_format):
        if user_requested_codec == "NONE":
            return True
        elif (
            user_requested_format == FILE_STORAGE_FORMAT_ORC
            and user_requested_codec
            in [
                FILE_STORAGE_COMPRESSION_CODEC_SNAPPY,
                FILE_STORAGE_COMPRESSION_CODEC_ZLIB,
            ]
        ):
            return True
        elif (
            user_requested_format == FILE_STORAGE_FORMAT_PARQUET
            and user_requested_codec
            in [
                FILE_STORAGE_COMPRESSION_CODEC_SNAPPY,
                FILE_STORAGE_COMPRESSION_CODEC_GZIP,
            ]
        ):
            return True
        else:
            return False

    def is_view(self, db_name, object_name):
        try:
            hive_table = self._get_hive_table(db_name, object_name)
            return hive_table.is_view()
        except BetterImpylaException as exc:
            # view does not exist
            return False

    def length_sql_expression(self, column_expression):
        assert column_expression
        return "LENGTH(%s)" % column_expression

    def list_databases(self, db_name_filter=None, case_sensitive=True):
        """In Impala all object names are lower case so case_sensitive does not apply."""
        self._debug("list_databases(%s)" % db_name_filter)
        if db_name_filter:
            sql = "SHOW DATABASES LIKE '%s'" % db_name_filter.lower()
        else:
            sql = "SHOW DATABASES"
        rows = self.execute_query_fetch_all(sql, log_level=VVERBOSE)
        if rows:
            return [_[0] for _ in rows]
        else:
            return []

    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        """In Hadoop all object names are lower case so case_sensitive does not apply."""
        assert db_name
        if table_name_filter:
            sql = "SHOW TABLES IN %s LIKE '%s'" % (db_name, table_name_filter.lower())
        else:
            sql = "SHOW TABLES IN %s" % db_name
        rows = self.execute_query_fetch_all(sql, log_level=VVERBOSE)
        if rows:
            return [_[0] for _ in rows]
        else:
            return []

    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        raise NotImplementedError(
            "list_udfs() is not implemented for common Hadoop class"
        )

    def list_views(self, db_name, view_name_filter=None, case_sensitive=True):
        """In Hadoop list tables/views is the same command.
        Filtering afterwards is too inefficient therefore NotImplementedError
        """
        raise NotImplementedError("list_views() is not implemented for Hadoop")

    def max_decimal_integral_magnitude(self):
        return HADOOP_DECIMAL_MAX_PRECISION

    def max_decimal_precision(self):
        return HADOOP_DECIMAL_MAX_PRECISION

    def max_decimal_scale(self, data_type=None):
        return 38

    def max_datetime_scale(self):
        """Hive & Impala support nanoseconds"""
        return 9

    def max_partition_columns(self):
        """Returning 100 as an extremely unlikely to be a factor limit.
        Previously we had no check for a limit but needed to introduce something
        because on BigQuery
        """
        return 100

    def max_sort_columns(self):
        """Returning SORT_COLUMNS_UNLIMITED as an extremely unlikely to be a factor limit.
        Previously we had no check for a limit but needed to introduce something because of BigQuery.
        """
        return SORT_COLUMNS_UNLIMITED

    def native_integer_types(self):
        return [
            HADOOP_TYPE_BIGINT,
            HADOOP_TYPE_INT,
            HADOOP_TYPE_SMALLINT,
            HADOOP_TYPE_TINYINT,
        ]

    def partition_column_requires_synthetic_column(self, backend_column, granularity):
        """On Hadoop all partition columns currently require a synthetic column."""
        return True

    def regexp_extract_sql_expression(self, subject, pattern):
        return "REGEXP_EXTRACT(%s, '%s', 1)" % (subject, pattern)

    def rename_table(
        self, from_db_name, from_table_name, to_db_name, to_table_name, sync=None
    ):
        """Rename a Hive/Impala table
        If the table is stored in S3 then we need to switch the table to be an external table before renaming
        """
        assert from_db_name and from_table_name
        assert to_db_name and to_table_name

        if not self._dry_run and not self.table_exists(from_db_name, from_table_name):
            raise BackendApiException(
                "Source table does not exist, cannot rename table: %s.%s"
                % (from_db_name, from_table_name)
            )

        if not self._dry_run and self.exists(to_db_name, to_table_name):
            raise BackendApiException(
                "Target table already exists, cannot rename table to: %s.%s"
                % (to_db_name, to_table_name)
            )

        rename_sql = "ALTER TABLE {}.{} RENAME TO {}.{}".format(
            self.enclose_identifier(from_db_name),
            self.enclose_identifier(from_table_name),
            self.enclose_identifier(to_db_name),
            self.enclose_identifier(to_table_name),
        )

        table_location = self.get_table_location(from_db_name, from_table_name)
        fs_scheme = (
            get_scheme_from_location_uri(table_location) if table_location else None
        )
        if (
            fs_scheme == OFFLOAD_FS_SCHEME_S3A
            and not self.transactional_tables_default()
        ):
            self._log(
                "Renaming S3 based table using external table management: %s.%s"
                % (from_db_name, from_table_name),
                detail=VERBOSE,
            )
            self._alter_table_external(from_db_name, from_table_name, external=True)

        executed_sqls = self.execute_ddl(rename_sql, sync=sync)

        if (
            fs_scheme == OFFLOAD_FS_SCHEME_S3A
            and not self.transactional_tables_default()
        ):
            self._alter_table_external(to_db_name, to_table_name, external=False)

        return executed_sqls

    def role_exists(self, role_name):
        """No roles in Hadoop"""
        pass

    def set_column_stats(
        self, db_name, table_name, new_column_stats, ndv_cap, num_null_factor
    ):
        if not self.table_stats_get_supported():
            return
        hive_stats = self._get_hive_stats_table(db_name, table_name)
        hive_stats.set_column_stats(
            new_column_stats,
            self._backend_type,
            ndv_cap=ndv_cap,
            num_null_factor=num_null_factor,
            dry_run=self._dry_run,
            messages=self._messages,
        )

    def set_partition_stats(
        self, db_name, table_name, new_partition_stats, additive_stats
    ):
        if not self.table_stats_get_supported():
            return
        hive_stats = self._get_hive_stats_table(db_name, table_name)
        hive_stats.set_partition_stats(
            new_partition_stats,
            self._backend_type,
            additive=additive_stats,
            dry_run=self._dry_run,
            messages=self._messages,
        )

    def set_session_db(self, db_name, log_level=VERBOSE):
        assert db_name
        return self.execute_ddl("USE %s" % db_name, log_level=log_level)

    def set_table_stats(self, db_name, table_name, new_table_stats, additive_stats):
        if not self.table_stats_get_supported():
            return
        hive_stats = self._get_hive_stats_table(db_name, table_name)
        hive_stats.set_table_stats(
            new_table_stats,
            self._backend_type,
            additive=additive_stats,
            dry_run=self._dry_run,
            messages=self._messages,
        )

    def supported_date_based_partition_granularities(self):
        return [
            PART_COL_GRANULARITY_YEAR,
            PART_COL_GRANULARITY_MONTH,
            PART_COL_GRANULARITY_DAY,
        ]

    def supported_partition_function_parameter_data_types(self):
        return [HADOOP_TYPE_DECIMAL, HADOOP_TYPE_BIGINT, HADOOP_TYPE_STRING]

    def supported_partition_function_return_data_types(self):
        return [
            HADOOP_TYPE_BIGINT,
            HADOOP_TYPE_INT,
            HADOOP_TYPE_SMALLINT,
            HADOOP_TYPE_TINYINT,
        ]

    def synthetic_partition_numbers_are_string(self):
        return True

    def table_distribution(self, db_name, table_name):
        return None

    def table_exists(self, db_name: str, table_name: str) -> bool:
        return self.exists(db_name, table_name)

    def table_has_rows(self, db_name: str, table_name: str) -> bool:
        """Return bool depending whether the table has rows or not."""
        sql = f"SELECT 1 FROM {self.enclose_object_reference(db_name, table_name)} LIMIT 1"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return bool(row)

    def target_version(self):
        if self._target_version is None:
            if self._hive_conn is None:
                # Only get sql_engine_version if _hive_conn is set, won't be in unit tests
                self._target_version = "0.0.0"
            else:
                self._target_version = self._hive_conn.sql_engine_version()
        return self._target_version

    def transactional_tables_default(self):
        """Whether a CREATE TABLE statement would create a Hive3 based transactional table by default"""
        return False

    def transform_encrypt_data_type(self):
        return HADOOP_TYPE_STRING

    def transform_null_cast(self, backend_column):
        assert isinstance(backend_column, HadoopColumn)
        return "CAST(NULL AS %s)" % (backend_column.format_data_type())

    def transform_tokenize_data_type(self):
        return HADOOP_TYPE_STRING

    def transform_regexp_replace_expression(
        self, backend_column, regexp_replace_pattern, regexp_replace_string
    ):
        return "REGEXP_REPLACE(%s, %s, %s)" % (
            self.enclose_identifier(backend_column.name),
            regexp_replace_pattern,
            regexp_replace_string,
        )

    def transform_translate_expression(self, backend_column, from_string, to_string):
        return "TRANSLATE(%s, %s, %s)" % (
            self.enclose_identifier(backend_column.name),
            from_string,
            to_string,
        )

    def udf_exists(self, db_name, udf_name):
        try:
            return bool(self.list_udfs(db_name, udf_name))
        except BetterImpylaException as exc:
            if "does not exist" in str(exc):
                return False
            else:
                raise

    def udf_installation_sql(self, create_udf_db, udf_db=None):
        if not self.goe_udfs_supported():
            self._messages.log(
                "Skipping installation of UDFs due to backend: %s" % self._backend_type
            )
            return None

        cmds = []
        if udf_db and not self.database_exists(udf_db):
            if create_udf_db:
                cmds.extend(self.create_database(udf_db))
            else:
                raise BackendApiException(
                    "Database: %s does not exist. Specify --create-backend-db flag to create"
                    % udf_db
                )

        cmds.extend(self._udf_installation_sql(udf_db=udf_db))
        return cmds

    def udf_installation_test(self, udf_db=None):
        if not self.goe_udfs_supported():
            self._messages.log(
                "Skipping test of UDFs due to backend: %s" % self._backend_type
            )
            return None

        cmds = []
        try:
            for sql in self._udf_test_sql(udf_db=udf_db):
                cmds.append(sql)
                row = self.execute_query_fetch_one(
                    sql, log_level=VVERBOSE, not_when_dry_running=True
                )
                if row:
                    self._log(str(row), detail=VVERBOSE)
        except HiveServer2Error as exc:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            raise BackendApiException(str(exc))
        return cmds

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, HadoopColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.is_number_based():
            if column.data_type in [HADOOP_TYPE_DOUBLE, HADOOP_TYPE_FLOAT]:
                return bool(
                    target_type in [GOE_TYPE_DECIMAL, GOE_TYPE_DOUBLE, GOE_TYPE_FLOAT]
                )
            else:
                return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based():
            return bool(target_type in DATE_CANONICAL_TYPES)
        elif column.is_string_based():
            if column.data_type == HADOOP_TYPE_CHAR:
                return bool(target_type == GOE_TYPE_FIXED_STRING)
            else:
                return bool(
                    target_type in STRING_CANONICAL_TYPES
                    or target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY]
                    or target_type in [GOE_TYPE_INTERVAL_DS, GOE_TYPE_INTERVAL_YM]
                )
        elif target_type not in ALL_CANONICAL_TYPES:
            self._log(
                "Unknown canonical type in mapping: %s" % target_type, detail=VVERBOSE
            )
            return False
        elif column.data_type not in self.supported_backend_data_types():
            return False
        elif column.data_type == HADOOP_TYPE_BOOLEAN:
            return bool(target_type == GOE_TYPE_BOOLEAN)
        elif column.data_type == HADOOP_TYPE_BINARY:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        return False

    def valid_staging_formats(self):
        return [FILE_STORAGE_FORMAT_AVRO]

    def view_exists(self, db_name, view_name):
        return self.exists(db_name, view_name)

    def to_canonical_column(self, column):
        """Translate a Hive/Impala column to an internal GOE column."""

        def new_column(
            col, data_type, data_precision=None, data_scale=None, safe_mapping=None
        ):
            """Wrapper that carries name forward to the canonical column"""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return CanonicalColumn(
                name=col.name,
                data_type=data_type,
                data_length=col.data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                char_length=col.char_length,
            )

        assert column
        assert isinstance(column, HadoopColumn)

        if column.data_type == HADOOP_TYPE_CHAR:
            return new_column(column, GOE_TYPE_FIXED_STRING)
        elif column.data_type in (HADOOP_TYPE_STRING, HADOOP_TYPE_VARCHAR):
            return new_column(column, GOE_TYPE_VARIABLE_STRING)
        elif column.data_type == HADOOP_TYPE_BINARY:
            return new_column(column, GOE_TYPE_BINARY)
        elif column.data_type in (
            HADOOP_TYPE_TINYINT,
            HADOOP_TYPE_SMALLINT,
            HADOOP_TYPE_INT,
            HADOOP_TYPE_BIGINT,
        ) or (column.data_type == HADOOP_TYPE_DECIMAL and column.data_scale == 0):
            if column.data_type == HADOOP_TYPE_TINYINT or (
                1 <= (column.data_precision or 0) <= 2
            ):
                integral_type = GOE_TYPE_INTEGER_1
            elif column.data_type == HADOOP_TYPE_SMALLINT or (
                3 <= (column.data_precision or 0) <= 4
            ):
                integral_type = GOE_TYPE_INTEGER_2
            elif column.data_type == HADOOP_TYPE_INT or (
                5 <= (column.data_precision or 0) <= 9
            ):
                integral_type = GOE_TYPE_INTEGER_4
            elif column.data_type == HADOOP_TYPE_BIGINT or (
                10 <= (column.data_precision or 0) <= 18
            ):
                integral_type = GOE_TYPE_INTEGER_8
            elif column.data_type == HADOOP_TYPE_BIGINT or (
                19 <= (column.data_precision or 0) <= 38
            ):
                integral_type = GOE_TYPE_INTEGER_38
            else:
                return new_column(
                    column,
                    GOE_TYPE_INTEGER_38,
                    data_precision=column.data_precision,
                    data_scale=0,
                )
            return new_column(column, integral_type)
        elif column.data_type == HADOOP_TYPE_DECIMAL:
            return new_column(
                column,
                GOE_TYPE_DECIMAL,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        elif column.data_type == HADOOP_TYPE_FLOAT:
            return new_column(column, GOE_TYPE_FLOAT)
        elif column.data_type in (
            HADOOP_TYPE_DOUBLE,
            HADOOP_TYPE_DOUBLE_PRECISION,
            HADOOP_TYPE_REAL,
        ):
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == HADOOP_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE)
        elif column.data_type == HADOOP_TYPE_TIMESTAMP:
            data_scale = (
                column.data_scale
                if column.data_scale is not None
                else self.max_datetime_scale()
            )
            return new_column(column, GOE_TYPE_TIMESTAMP, data_scale=data_scale)
        elif column.data_type == HADOOP_TYPE_INTERVAL_DS:
            return new_column(column, GOE_TYPE_INTERVAL_DS)
        elif column.data_type == HADOOP_TYPE_INTERVAL_YM:
            return new_column(column, GOE_TYPE_INTERVAL_YM)
        elif column.data_type == HADOOP_TYPE_BOOLEAN:
            return new_column(column, GOE_TYPE_BOOLEAN)
        else:
            raise NotImplementedError(
                "Unsupported backend data type: %s" % column.data_type
            )

    def from_canonical_column(self, column, decimal_padding_digits=0):
        """Translate an internal GOE column to a Hadoop column.
        Note that Impala has its own override.
        """

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable & data_default forward from RDBMS"""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return HadoopColumn(
                col.name,
                data_type=data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
            )

        assert column
        assert isinstance(
            column, CanonicalColumn
        ), "%s is not instance of CanonicalColumn" % type(column)

        if column.data_type == GOE_TYPE_FIXED_STRING:
            return new_column(column, HADOOP_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_LARGE_STRING:
            return new_column(column, HADOOP_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_VARIABLE_STRING:
            return new_column(column, HADOOP_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_BINARY:
            return new_column(column, HADOOP_TYPE_BINARY, safe_mapping=True)
        elif column.data_type == GOE_TYPE_LARGE_BINARY:
            return new_column(column, HADOOP_TYPE_BINARY, safe_mapping=True)
        elif column.data_type in (
            GOE_TYPE_INTEGER_1,
            GOE_TYPE_INTEGER_2,
            GOE_TYPE_INTEGER_4,
            GOE_TYPE_INTEGER_8,
        ):
            if column.from_override:
                # Honour the canonical type because it is from a user override or a staging file
                if column.data_type == GOE_TYPE_INTEGER_1:
                    return new_column(column, HADOOP_TYPE_TINYINT, safe_mapping=True)
                elif column.data_type == GOE_TYPE_INTEGER_2:
                    return new_column(column, HADOOP_TYPE_SMALLINT, safe_mapping=True)
                elif column.data_type == GOE_TYPE_INTEGER_4:
                    return new_column(column, HADOOP_TYPE_INT, safe_mapping=True)
                else:
                    return new_column(column, HADOOP_TYPE_BIGINT, safe_mapping=True)
            else:
                # On Hadoop all 4 native integer types map to BIGINT
                return new_column(column, HADOOP_TYPE_BIGINT, safe_mapping=True)
        elif column.data_type == GOE_TYPE_INTEGER_38:
            return new_column(
                column,
                HADOOP_TYPE_DECIMAL,
                data_precision=self.max_decimal_precision(),
                data_scale=0,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_DECIMAL:
            if column.data_precision is None and column.data_scale is None:
                new_col = self.gen_default_numeric_column(column.name)
            else:
                # On Hadoop we have UDFs for each even scale but need to mix decimal_padding_digits with UDF alignment
                new_precision, new_scale = self._align_decimal_scale_to_udfs(
                    column.name, column.data_precision, column.data_scale
                )
                new_precision, new_scale = self._apply_decimal_padding_digits(
                    column.name, new_precision, new_scale, decimal_padding_digits
                )
                new_precision, new_scale = self._align_decimal_precision_to_udfs(
                    column.name, new_precision, new_scale
                )
                if self._unsupported_decimal_precision_scale(new_precision, new_scale):
                    raise NotImplementedError(
                        "Unsupported precision/scale for %s column %s: %s/%s"
                        % (column.data_type, column.name, new_precision, new_scale)
                    )
                if (
                    new_precision != column.data_precision
                    or new_scale != column.data_scale
                ):
                    self._log(
                        "UDF aligned/padded precision/scale for %s: %s,%s -> %s,%s"
                        % (
                            column.name,
                            column.data_precision,
                            column.data_scale,
                            new_precision,
                            new_scale,
                        ),
                        detail=VERBOSE,
                    )
                new_col = new_column(
                    column,
                    HADOOP_TYPE_DECIMAL,
                    data_precision=new_precision,
                    data_scale=new_scale,
                    safe_mapping=False,
                )
            return new_col
        elif column.data_type == GOE_TYPE_FLOAT:
            return new_column(column, HADOOP_TYPE_FLOAT, safe_mapping=True)
        elif column.data_type == GOE_TYPE_DOUBLE:
            return new_column(column, HADOOP_TYPE_DOUBLE, safe_mapping=True)
        elif column.data_type == GOE_TYPE_DATE and not self.canonical_date_supported():
            return new_column(column, HADOOP_TYPE_TIMESTAMP)
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, HADOOP_TYPE_DATE)
        elif column.data_type == GOE_TYPE_TIME:
            return new_column(column, HADOOP_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            return new_column(column, HADOOP_TYPE_TIMESTAMP, safe_mapping=False)
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            return new_column(column, HADOOP_TYPE_TIMESTAMP, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_DS:
            return new_column(column, HADOOP_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_YM:
            return new_column(column, HADOOP_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_BOOLEAN:
            return new_column(column, HADOOP_TYPE_BOOLEAN, safe_mapping=True)
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )
