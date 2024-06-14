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

""" BackendSnowflakeApi: Library for logic/interaction with a remote Snowflake backend.
    This module enforces an interface with common, high level, methods and an implementation
    for each supported remote system, e.g. Impala, Hive, Google BigQuery.
"""

from datetime import datetime
import logging
import re
from textwrap import dedent
import time
import traceback

from numpy import datetime64
import snowflake.connector

from goe.connect.connect_constants import CONNECT_DETAIL, CONNECT_STATUS, CONNECT_TEST
from goe.offload.backend_api import (
    BackendApiInterface,
    BackendApiException,
    UdfDetails,
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
    is_safe_mapping,
    valid_column_list,
    CanonicalColumn,
    GOE_TYPE_BINARY,
    GOE_TYPE_BOOLEAN,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_DATE,
    GOE_TYPE_FLOAT,
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_INTERVAL_DS,
    GOE_TYPE_INTERVAL_YM,
    GOE_TYPE_LARGE_BINARY,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_TIMESTAMP,
    GOE_TYPE_TIME,
    GOE_TYPE_TIMESTAMP_TZ,
    GOE_TYPE_VARIABLE_STRING,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    ALL_CANONICAL_TYPES,
    DATE_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
)
from goe.offload.offload_constants import (
    DBTYPE_SNOWFLAKE,
    SNOWFLAKE_BACKEND_CAPABILITIES,
    EMPTY_BACKEND_TABLE_STATS_DICT,
    EMPTY_BACKEND_COLUMN_STATS_DICT,
    EMPTY_BACKEND_COLUMN_STATS_LIST,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.snowflake.snowflake_column import (
    SnowflakeColumn,
    SNOWFLAKE_TYPE_BOOLEAN,
    SNOWFLAKE_TYPE_BINARY,
    SNOWFLAKE_TYPE_DATE,
    SNOWFLAKE_TYPE_FLOAT,
    SNOWFLAKE_TYPE_INTEGER,
    SNOWFLAKE_TYPE_NUMBER,
    SNOWFLAKE_TYPE_TEXT,
    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
    SNOWFLAKE_TYPE_TIME,
    SNOWFLAKE_TYPE_TIMESTAMP_TZ,
)
from goe.offload.snowflake.snowflake_literal import SnowflakeLiteral

from goe.util.misc_functions import (
    backtick_sandwich,
    format_list_for_logging,
    unsurround,
)
from goe.util.password_tools import PasswordTools


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

# Regular expression matching invalid identifier characters, as a constant to ensure compiled only once.
# According to https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#double-quoted-identifiers,
# double quoted identifiers can have any character.
SNOWFLAKE_INVALID_IDENTIFIER_CHARS_RE = re.compile(r"[\"]", re.I)

# Identifier used when making a connection to Snowflake in order for Snowflake to:
#   "better understand the usage patterns associated with specific partner integrations"
# We should not change this without also changing the identifier in the partner portal.
SNOWFLAKE_CONNECTION_IDENTIFIER = "GOE"


###########################################################################
# BackendSnowflakeApi
###########################################################################


class BackendSnowflakeApi(BackendApiInterface):
    def __init__(
        self,
        connection_options,
        backend_type,
        messages,
        dry_run=False,
        no_caching=False,
        do_not_connect=False,
    ):
        super(BackendSnowflakeApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        logger.info("BackendSnowflakeApi")
        if dry_run:
            logger.info("* Dry run *")

        self._client = None
        if not do_not_connect:
            # Allow true bind variables
            snowflake.connector.paramstyle = "qmark"

            if connection_options.snowflake_pem_file:
                if connection_options.snowflake_pem_passphrase:
                    self._log(
                        "Establishing Snowflake connection with PEM file and passphrase",
                        detail=VVERBOSE,
                    )
                else:
                    self._log(
                        "Establishing Snowflake connection with PEM file",
                        detail=VVERBOSE,
                    )
                snowflake_pass = None
                pem_passphrase = None
                if connection_options.snowflake_pem_passphrase is not None:
                    pem_passphrase = self._decrypt_password(
                        connection_options.snowflake_pem_passphrase
                    )
                private_key = self._get_private_key_from_pemfile(
                    connection_options.snowflake_pem_file, pem_passphrase
                )
            else:
                self._log(
                    "Establishing Snowflake connection with password authentication",
                    detail=VVERBOSE,
                )
                snowflake_pass = self._decrypt_password(
                    connection_options.snowflake_pass
                )
                private_key = None

            self._client = snowflake.connector.connect(
                user=connection_options.snowflake_user,
                password=snowflake_pass,
                private_key=private_key,
                account=connection_options.snowflake_account,
                database=connection_options.snowflake_database,
                role=connection_options.snowflake_role,
                warehouse=connection_options.snowflake_warehouse,
                session_parameters=self._execute_global_session_parameters(),
                application=SNOWFLAKE_CONNECTION_IDENTIFIER,
            )
            # An incorrect snowflake_warehouse does not cause an exception above (2020-12-01), check below with USE
            self._use_warehouse(
                connection_options.snowflake_warehouse, log_level=VVERBOSE
            )
        self._cursor = None

        self._sql_engine_name = "Snowflake"
        self._target_version = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _backend_capabilities(self):
        return SNOWFLAKE_BACKEND_CAPABILITIES

    def _catalog_name(self):
        """Using "catalog" because used in INFORMATION_SCHEMA to avoid confusion of using term "database" """
        if self._client:
            return self._client.database
        else:
            return self._connection_options.snowflake_database

    def _close_cursor(self):
        if self._client and self._cursor:
            try:
                self._cursor.close()
            except Exception as exc:
                self._log("Exception closing cursor:\n%s" % str(exc), detail=VVERBOSE)

    def _execute_ddl_or_dml(
        self,
        sql,
        sync=None,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        """See interface for parameter descriptions.
        sync is ignored on Snowflake.
        """
        assert sql
        assert isinstance(sql, (str, list))

        self._open_cursor()
        run_opts = self._execute_session_options(query_options, log_level=log_level)
        run_sqls = self._execute_sqls(
            sql, log_level=log_level, profile=profile, no_log_items=no_log_items
        )
        self._close_cursor()
        return run_opts + run_sqls

    def _execute_global_session_parameters(self, log_level=VVERBOSE):
        """On Snowflake we do not need to execute these, just return them to be used during connection."""
        return self._global_session_parameters

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
        query_params: list of bind values for 1..n params in sql as described in:
                      https://docs.snowflake.com/en/user-guide/python-connector-example.html#binding-data
        log_level: None = no logging. Otherwise VERBOSE/VVERBOSE etc
        time_sql: If logging then log SQL elapsed time
        not_when_dry_running: Some SQL will only work in execute mode, such as queries on a load table
        as_dict: When True each row is returned as a dict keyed by column name rather than a tuple/list.
        """
        assert sql

        if self._dry_run and not_when_dry_running:
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
            )
            return None

        t1 = datetime.now().replace(microsecond=0)

        self._open_cursor()
        try:
            self._execute_session_options(query_options, log_level=log_level)
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
            )
            self._cursor.execute(sql, query_params)
            if fetch_action == FETCH_ACTION_ALL:
                rows = self._cursor.fetchall()
                if as_dict:
                    columns = self._cursor_projection(self._cursor)
                    rows = [self._cursor_row_to_dict(columns, _) for _ in rows]
            elif fetch_action == FETCH_ACTION_ONE:
                rows = self._cursor.fetchone()
                if as_dict:
                    columns = self._cursor_projection(self._cursor)
                    rows = self._cursor_row_to_dict(columns, rows)
            else:
                rows = None

            if profile:
                self._log(self._get_query_profile(), detail=VVERBOSE)
        finally:
            if fetch_action in (FETCH_ACTION_ALL, FETCH_ACTION_ONE):
                self._close_cursor()

        if time_sql:
            t2 = datetime.now().replace(microsecond=0)
            self._log("Elapsed time: %s" % (t2 - t1), detail=VVERBOSE)

        if fetch_action == FETCH_ACTION_CURSOR:
            return self._cursor
        else:
            return rows

    def _execute_session_options(self, query_options, log_level):
        return_list = []
        if query_options:
            for prep_sql in self._format_query_options(query_options):
                self._log_or_not(
                    "%s SQL: %s" % (self._sql_engine_name, prep_sql),
                    log_level=log_level,
                )
                if self._client:
                    self._cursor.execute(prep_sql)
                return_list.append(prep_sql)
        return return_list

    def _execute_sqls(self, sql, log_level=VERBOSE, profile=None, no_log_items=None):
        return_list = []
        sqls = [sql] if isinstance(sql, str) else sql
        self._open_cursor()
        try:
            for i, run_sql in enumerate(sqls):
                self._log_or_not(
                    "%s SQL: %s" % (self._sql_engine_name, run_sql),
                    log_level=log_level,
                    no_log_items=no_log_items,
                )
                return_list.append(run_sql)
                if not self._dry_run:
                    self._cursor.execute(run_sql)
                    if profile:
                        self._log(self._get_query_profile(), detail=VVERBOSE)
        finally:
            self._close_cursor()
        return return_list

    @staticmethod
    def _fixed_session_parameters():
        """Dictionary of fixed session parameters for Snowflake"""
        return {
            "AUTOCOMMIT": "TRUE",
            "QUERY_TAG": "GOE",
            "TIMESTAMP_NTZ_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF9",
            "TIMESTAMP_TZ_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM",
            "TIME_OUTPUT_FORMAT": "HH24:MI:SS.FF9",
        }

    @staticmethod
    def _format_query_options(query_options=None):
        """Format SET commands for Snowflake.
        query_options: key/value pairs for session settings.
        """

        def prep_param_value(v):
            if isinstance(v, str):
                return backtick_sandwich(v, ch="'")
            elif isinstance(v, float):
                return repr(v)
            else:
                return str(v)

        if not query_options:
            return []
        assert isinstance(query_options, dict)
        return [
            "ALTER SESSION SET {}={}".format(k, prep_param_value(v))
            for k, v in query_options.items()
        ]

    def _get_private_key_from_pemfile(self, pem_file, pem_passphrase):
        pass_tool = PasswordTools()
        return pass_tool.get_private_key_from_pkcs8_pem_file(
            pem_file, passphrase=pem_passphrase
        )

    def _gen_sample_stats_sql_sample_clause(
        self, db_name, table_name, sample_perc=None
    ):
        assert db_name and table_name
        if (
            sample_perc is not None
            and sample_perc >= 0
            and self.query_sample_clause_supported()
        ):
            if self.is_view(db_name, table_name):
                return "SAMPLE ROW (%s)" % sample_perc
            else:
                return "SAMPLE BLOCK (%s)" % sample_perc
        else:
            return ""

    def _get_query_profile(self, query_identifier=None):
        """On Snowflake query_identifier is not used.
        We look for a query using the existing cursor.
        From Snowflake docs re QUERY_HISTORY view:
            Latency for the view may be up to 45 minutes.
        We don't see the same comment for QUERY_HISTORY table function.
        There's no query id parameter to functions so we filter on result set.
        This function has a built in retry because, in testing, we've frequently seen a delay of 1s in seeing data.
        """

        def get_query_history_after_delay(wait_seconds):
            time.sleep(wait_seconds)
            sql = dedent(
                """\
                 SELECT *
                 FROM TABLE(
                    %s.information_schema.query_history_by_session(end_time_range_end => CURRENT_TIMESTAMP,
                                                                   result_limit => 1)
                 )
                 WHERE query_id = ?"""
                % self.enclose_identifier(self._catalog_name())
            )
            self._cursor.execute(sql, [self._cursor.sfqid])
            return self._cursor.fetchone()

        def str_fn(x):
            return x if isinstance(x, str) else str(x)

        assert self._cursor
        assert not self._cursor.is_closed()
        self._log(
            "Fetching query history for id: %s" % self._cursor.sfqid, detail=VVERBOSE
        )
        # Found the data was not always there instantly, so added a brief pause
        row = get_query_history_after_delay(0.5)
        if not row:
            self._log("Query history not found (after attempt 1)", detail=VVERBOSE)
            row = get_query_history_after_delay(2)
        if not row:
            self._log("Query history not found (after attempt 2)", detail=VVERBOSE)
            return ""
        else:
            profile_keys = [_[0] for _ in self._cursor.description]
            stats = [("Statistic", "Value")]
            stats.extend(sorted(zip(profile_keys, [str_fn(_) for _ in row])))
            return format_list_for_logging(stats)

    def _get_snowflake_ddl(
        self,
        db_name,
        object_name,
        object_type,
        as_list=False,
        terminate_sql=False,
        for_replace=False,
    ):
        assert db_name and object_name
        self._log("Fetching DDL: %s.%s" % (db_name, object_name), detail=VVERBOSE)
        sql = "SELECT get_ddl('%s', '%s')" % (
            object_type,
            self.enclose_object_reference(db_name, object_name),
        )
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        if not row:
            raise BackendApiException(
                "Object does not exist for DDL retrieval: %s.%s"
                % (db_name, object_name)
            )
        ddl_str = row[0]
        self._debug("Object DDL: %s" % ddl_str)
        if not terminate_sql:
            ddl_str = ddl_str.rstrip(";")
        if as_list:
            return ddl_str.split("\n")
        else:
            return ddl_str

    def _invalid_identifier_character_re(self):
        return SNOWFLAKE_INVALID_IDENTIFIER_CHARS_RE

    def _open_cursor(self):
        self._cursor = self._client.cursor() if self._client else None

    def _role_granted_to_user(self, role_name, user_name):
        """Check if the role is granted to user, returns True/False"""
        assert role_name
        assert user_name
        sql = (
            "SELECT role_name FROM %s.information_schema.applicable_roles WHERE role_name = ? AND grantee = ?"
            % self.enclose_identifier(self._catalog_name())
        )
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=[role_name, user_name]
        )
        return bool(row)

    def _use_warehouse(self, warehouse_name, log_level=VERBOSE):
        assert warehouse_name
        try:
            return self.execute_ddl(
                "USE WAREHOUSE %s" % warehouse_name, log_level=log_level
            )
        except snowflake.connector.ProgrammingError as exc:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            raise BackendApiException(
                "Snowflake warehouse %s is not usable: %s" % (warehouse_name, str(exc))
            )

    def _warehouse_exists(self, warehouse_name):
        """Check the Warehouse exists, returns True/False"""
        sql = "SHOW WAREHOUSES LIKE '%s'" % warehouse_name
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return bool(row)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def add_columns(self, db_name, table_name, column_tuples, sync=None):
        """See interface spec for description"""
        assert db_name and table_name
        assert column_tuples
        assert isinstance(column_tuples, list), "%s is not list" % type(column_tuples)
        assert isinstance(column_tuples[0], (tuple, list))

        ddls = []
        for col_name, col_type in column_tuples:
            sql = "ALTER TABLE %s.%s ADD COLUMN %s %s" % (
                self.enclose_identifier(db_name),
                self.enclose_identifier(table_name),
                self.enclose_identifier(col_name),
                col_type,
            )
            ddls += self.execute_ddl(sql)
        return ddls

    def alter_sort_columns(self, db_name, table_name, sort_column_names, sync=None):
        assert db_name and table_name
        assert isinstance(sort_column_names, list)
        if sort_column_names:
            sort_csv = ",".join([self.enclose_identifier(_) for _ in sort_column_names])
            sql = "ALTER TABLE %s CLUSTER BY (%s)" % (
                self.enclose_object_reference(db_name, table_name),
                sort_csv,
            )
        else:
            sql = "ALTER TABLE %s DROP CLUSTERING KEY" % self.enclose_object_reference(
                db_name, table_name
            )
        return self.execute_ddl(sql, log_level=VERBOSE)

    def backend_report_info(self):
        """Reporting information about the type of backend and how to display
        its configuration details (e.g. Hadoop host:port or BigQuery project).
        """
        return {
            REPORT_ATTR_BACKEND_CLASS: "Cloud Warehouse",
            REPORT_ATTR_BACKEND_TYPE: self._backend_type,
            REPORT_ATTR_BACKEND_DISPLAY_NAME: self.backend_db_name(),
            REPORT_ATTR_BACKEND_HOST_INFO_TYPE: "Snowflake Database",
            REPORT_ATTR_BACKEND_HOST_INFO: self._catalog_name(),
        }

    def backend_version(self):
        """Output some descriptive information about the backend
        This is different to target_version() even though it appears similar in function.
        """
        return self.target_version()

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
        except Exception:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            results.append(
                {
                    CONNECT_TEST: "Test backend user",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Unable to obtain backend user",
                }
            )

        if self.role_exists(orchestration_options.snowflake_role):
            if self._role_granted_to_user(
                orchestration_options.snowflake_role,
                orchestration_options.snowflake_user,
            ):
                results.append(
                    {
                        CONNECT_TEST: "Role",
                        CONNECT_STATUS: True,
                        CONNECT_DETAIL: "Role exists and is granted: %s"
                        % orchestration_options.snowflake_role,
                    }
                )
            else:
                results.append(
                    {
                        CONNECT_TEST: "Role",
                        CONNECT_STATUS: False,
                        CONNECT_DETAIL: "Role is not granted to user: %s"
                        % orchestration_options.snowflake_role,
                    }
                )
        else:
            results.append(
                {
                    CONNECT_TEST: "Role",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Role does not exist: %s"
                    % orchestration_options.snowflake_role,
                }
            )

        if self._warehouse_exists(orchestration_options.snowflake_warehouse):
            results.append(
                {
                    CONNECT_TEST: "Warehouse",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "Warehouse exists: %s"
                    % orchestration_options.snowflake_warehouse,
                }
            )
        else:
            results.append(
                {
                    CONNECT_TEST: "Warehouse",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Warehouse does not exist: %s"
                    % orchestration_options.snowflake_warehouse,
                }
            )

        if self.snowflake_integration_exists(
            orchestration_options.snowflake_integration
        ):
            results.append(
                {
                    CONNECT_TEST: "Storage integration",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "Integration exists: %s"
                    % orchestration_options.snowflake_integration,
                }
            )
        else:
            results.append(
                {
                    CONNECT_TEST: "Storage integration",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Integration does not exist: %s"
                    % orchestration_options.snowflake_integration,
                }
            )

        return results

    def close(self):
        if self._client:
            self._client.close()

    def compute_stats(
        self,
        db_name,
        table_name,
        incremental=None,
        for_columns=False,
        partition_tuples=None,
        sync=None,
    ):
        """No manual controls of stats on Snowflake"""
        raise NotImplementedError("Compute statistics does not apply for Snowflake")

    def create_database(
        self, db_name, comment=None, properties=None, with_terminator=False
    ):
        """Create a Snowflake schema which is a database in GOE terminology.
        properties["transient"]: Can be used to create a transient schema if value is truthy.
        """
        assert db_name
        if properties:
            assert isinstance(properties, dict)
        if comment:
            assert isinstance(comment, str)

        if properties and properties.get("transient"):
            transient_clause = " TRANSIENT"
            retention_clause = " DATA_RETENTION_TIME_IN_DAYS = 0"
        else:
            transient_clause = retention_clause = ""

        if comment:
            comment_clause = " COMMENT = '%s'" % comment
        else:
            comment_clause = ""

        sql = "CREATE%s SCHEMA IF NOT EXISTS %s%s%s" % (
            transient_clause,
            self.enclose_identifier(db_name),
            retention_clause,
            comment_clause,
        )
        if with_terminator:
            sql += ";"
        return self.execute_ddl(sql)

    def create_table(
        self,
        db_name,
        table_name,
        column_list,
        partition_column_names,
        storage_format=None,
        location=None,
        external=False,
        table_properties=None,
        sort_column_names=None,
        without_db_name=False,
        sync=None,
        with_terminator=False,
    ):
        """Create a Snowflake table
        partition_column_names: Not supported on Snowflake
        location: Ignored for Snowflake
        table_properties: Ignored for Snowflake
        without_db_name: Ignored for Snowflake
        See abstract method for more description
        """
        assert db_name
        assert table_name
        assert column_list
        assert valid_column_list(column_list), (
            "Incorrectly formed column_list: %s" % column_list
        )
        if table_properties:
            assert isinstance(table_properties, dict)
        if sort_column_names:
            assert isinstance(sort_column_names, list)

        if partition_column_names:
            raise NotImplementedError(
                "Partitioning by column is not supported in Snowflake"
            )
        if external:
            raise NotImplementedError(
                "Offload by external table is not supported on Snowflake"
            )

        col_projection = self._create_table_columns_clause_common(
            column_list, external=external
        )

        if sort_column_names:
            sort_csv = ",".join([self.enclose_identifier(_) for _ in sort_column_names])
            sort_by_clause = "\nCLUSTER BY (%s)" % sort_csv
        else:
            sort_by_clause = ""

        sql = (
            dedent(
                """\
        CREATE TABLE %(db_table)s (
        %(col_projection)s
        )%(sort_by_clause)s"""
            )
            % {
                "db_table": self.enclose_object_reference(db_name, table_name),
                "col_projection": col_projection,
                "sort_by_clause": sort_by_clause,
            }
        )
        if with_terminator:
            sql += ";"
        return self.execute_ddl(sql, sync=sync)

    def create_view(
        self,
        db_name,
        view_name,
        column_tuples,
        ansi_joined_tables,
        filter_clauses=None,
        sync=None,
    ):
        """Create a Snowflake view
        See create_view() description on interface for parameter descriptions.
        """
        projection = self._format_select_projection(column_tuples)
        where_clause = (
            "\nWHERE  " + "\nAND    ".join(filter_clauses) if filter_clauses else ""
        )
        sql = (
            dedent(
                """\
        CREATE VIEW %(db_view)s AS
        SELECT %(projection)s
        FROM   %(from_tables)s%(where_clause)s"""
            )
            % {
                "db_view": self.enclose_object_reference(db_name, view_name),
                "projection": projection,
                "from_tables": ansi_joined_tables,
                "where_clause": where_clause,
            }
        )
        return self.execute_ddl(sql, sync=sync)

    def create_udf(
        self,
        db_name,
        udf_name,
        return_data_type,
        parameter_tuples,
        udf_body,
        or_replace=False,
        spec_as_string=None,
        sync=None,
        log_level=VERBOSE,
    ):
        """Pending implementation due to no current requirement for Snowflake UDF Support in GOE"""
        raise NotImplementedError(
            "create_udf() is not implemented for Snowflake backend"
        )

    def current_date_sql_expression(self):
        return "CURRENT_DATE()"

    def data_type_accepts_length(self, data_type):
        return bool(
            data_type
            in [SNOWFLAKE_TYPE_NUMBER, SNOWFLAKE_TYPE_TEXT, SNOWFLAKE_TYPE_BINARY]
        )

    def database_exists(self, db_name):
        return bool(self.list_databases(db_name_filter=db_name))

    def db_name_label(self, initcap=False):
        label = "schema"
        return label.capitalize() if initcap else label

    def default_date_based_partition_granularity(self):
        return None

    def default_storage_compression(self, user_requested_codec, user_requested_format):
        if user_requested_codec in ["HIGH", "MED", "NONE", None]:
            return None
        return user_requested_codec

    @staticmethod
    def default_storage_format():
        return None

    def derive_native_partition_info(self, db_name, table_name, column, position):
        return None

    def detect_column_has_fractional_seconds(self, db_name, table_name, column):
        assert db_name and table_name
        assert isinstance(column, SnowflakeColumn)
        if column.data_type not in [
            SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
            SNOWFLAKE_TYPE_TIMESTAMP_TZ,
        ]:
            return False
        sql = (
            dedent(
                """\
        SELECT %(col)s
        FROM %(db_table)s
        WHERE EXTRACT(NANOSECOND FROM %(col)s) != 0
        LIMIT 1"""
            )
            % {
                "db_table": self.enclose_object_reference(db_name, table_name),
                "col": self.enclose_identifier(column.name),
            }
        )
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return True if row else False

    def drop_state(self):
        pass

    def drop_table(self, db_name, table_name, purge=False, if_exists=True, sync=None):
        """Drop a table, purge parameter is ignored on Snowflake.
        See abstract method for more description
        """
        assert db_name and table_name
        if_exists_clause = "IF EXISTS " if if_exists else ""
        drop_sql = "DROP TABLE %s%s" % (
            if_exists_clause,
            self.enclose_object_reference(db_name, table_name),
        )
        return self.execute_ddl(drop_sql, sync=sync)

    def drop_view(self, db_name, view_name, if_exists=True, sync=None):
        """Drop a view, purge parameter is ignored on Snowflake.
        See abstract method for more description
        """
        assert db_name and view_name
        if_exists_clause = "IF EXISTS " if if_exists else ""
        drop_sql = "DROP VIEW %s%s" % (
            if_exists_clause,
            self.enclose_object_reference(db_name, view_name),
        )
        return self.execute_ddl(drop_sql, sync=sync)

    def enclose_identifier(self, identifier):
        if identifier is None:
            return None
        return backtick_sandwich(identifier, ch=self.enclosure_character())

    def enclose_object_reference(self, db_name, object_name):
        assert db_name and object_name
        snowflake_db = self._catalog_name()
        if snowflake_db is None:
            # This only happens during unit tests
            return ".".join(
                [self.enclose_identifier(db_name), self.enclose_identifier(object_name)]
            )
        else:
            return ".".join(
                [
                    self.enclose_identifier(snowflake_db),
                    self.enclose_identifier(db_name),
                    self.enclose_identifier(object_name),
                ]
            )

    def enclosure_character(self):
        return '"'

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
        return bool(
            self.table_exists(db_name, object_name)
            or self.view_exists(db_name, object_name)
        )

    def format_query_parameter(self, param_name):
        return "?"

    def gen_column_object(self, column_name, **kwargs):
        return SnowflakeColumn(column_name, **kwargs)

    def gen_copy_into_sql_text(
        self,
        db_name,
        table_name,
        from_object_clause,
        select_expr_tuples,
        filter_clauses=None,
    ):
        """Format COPY INTO from SELECT statement for Snowflake"""
        self._gen_insert_select_sql_assertions(
            db_name,
            table_name,
            None,
            None,
            select_expr_tuples,
            None,
            filter_clauses,
            from_object_clause,
        )
        projection = self._format_select_projection(select_expr_tuples)
        where_clause = ""
        if filter_clauses:
            where_clause = "\nWHERE  " + "\nAND    ".join(filter_clauses)

        insert_sql = (
            dedent(
                """\
        COPY INTO %(db_table)s
        FROM
        (
        SELECT %(proj)s
        FROM   %(from_object_clause)s%(where)s
        )
        """
            )
            % {
                "db_table": self.enclose_object_reference(db_name, table_name),
                "proj": projection,
                "from_object_clause": from_object_clause,
                "where": where_clause,
            }
        )
        return insert_sql

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
        """See interface description for parameter descriptions.
        Ignoring storage_format on Snowflake.
        """
        assert db_name and table_name
        assert column_tuples
        assert isinstance(column_tuples, list), "%s is not list" % type(column_tuples)
        assert isinstance(column_tuples[0], (tuple, list))
        projection = self._format_select_projection(column_tuples)
        from_clause = (
            "\nFROM   {}".format(
                self.enclose_object_reference(from_db_name, from_table_name)
            )
            if from_db_name and from_table_name
            else ""
        )
        limit_clause = "\nLIMIT  {}".format(row_limit) if row_limit is not None else ""
        sql = (
            dedent(
                """\
            CREATE TABLE %(db_table)s
            AS
            SELECT %(projection)s%(from_clause)s%(limit_clause)s"""
            )
            % {
                "db_table": self.enclose_object_reference(db_name, table_name),
                "projection": projection,
                "from_clause": from_clause,
                "limit_clause": limit_clause,
            }
        )
        return sql

    def gen_default_numeric_column(self, column_name, data_scale=18):
        """Return a default best-fit numeric column for generic usage.
        Used when we can't really know or influence a type, e.g. AVG() output in AAPD.
        data_scale=0 can be used if the outcome is known to be integral.
        """
        return SnowflakeColumn(
            column_name,
            data_type=SNOWFLAKE_TYPE_NUMBER,
            data_precision=self.max_decimal_precision(),
            data_scale=data_scale,
            nullable=True,
            safe_mapping=False,
        )

    def gen_insert_select_sql_text(
        self,
        db_name,
        table_name,
        from_db_name,
        from_table_name,
        select_expr_tuples,
        partition_expr_tuples=None,
        filter_clauses=None,
        sort_expr_list=None,
        distribute_columns=None,
        insert_hint=None,
        from_object_override=None,
    ):
        """Format INSERT from SELECT statement for Snowflake
        Ignores insert_hint, sort_expr_list and distribute_columns
        See abstractmethod spec for parameter descriptions
        """
        self._gen_insert_select_sql_assertions(
            db_name,
            table_name,
            from_db_name,
            from_table_name,
            select_expr_tuples,
            partition_expr_tuples,
            filter_clauses,
            from_object_override,
        )

        projection = self._format_select_projection(select_expr_tuples)
        from_db_table = from_object_override or self.enclose_object_reference(
            from_db_name, from_table_name
        )

        where_clause = ""
        if filter_clauses:
            where_clause = "\nWHERE  " + "\nAND    ".join(filter_clauses)

        insert_sql = (
            dedent(
                """\
        INSERT INTO %(db_table)s
        SELECT %(proj)s
        FROM   %(from_db_table)s%(where)s"""
            )
            % {
                "db_table": self.enclose_object_reference(db_name, table_name),
                "proj": projection,
                "from_db_table": from_db_table,
                "where": where_clause,
            }
        )
        return insert_sql

    def gen_native_range_partition_key_cast(self, partition_column):
        """No native range partitioning on Snowflake"""
        raise NotImplementedError("Native range partitioning unsupported on Snowflake")

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
        return SNOWFLAKE_TYPE_TEXT

    def get_columns(self, db_name, table_name):
        def get_binary_lengths_from_desc(db_name, table_name):
            """Information schema does not expose lengths of BINARY columns, this function gets the correct information
            from DESCRIBE and stores it in a dictionary.
            """
            if self.is_view(db_name, table_name):
                sql = "DESCRIBE VIEW %s" % self.enclose_object_reference(
                    db_name, table_name
                )
            else:
                sql = "DESCRIBE TABLE %s" % self.enclose_object_reference(
                    db_name, table_name
                )
            data_type_decode_re = re.compile(r"^([a-z0-9_]+)[\(]?([0-9]*)?[\)]?$", re.I)
            byte_lengths = {}
            for row in [
                _
                for _ in self.execute_query_fetch_all(sql, log_level=VVERBOSE)
                if SNOWFLAKE_TYPE_BINARY in _[1]
            ]:
                m = data_type_decode_re.match(row[1])
                if m:
                    byte_length = int(m.group(2))
                    byte_lengths[row[0]] = byte_length
            return byte_lengths

        assert db_name and table_name

        backend_columns = []
        binary_byte_lengths = {}

        # Snowflake information_schema queries are faster if database is in FROM clause than when via column predicate
        sql = dedent(
            """\
            SELECT column_name, data_type, is_nullable, character_maximum_length, character_octet_length
            ,      numeric_precision, numeric_scale, datetime_precision
            FROM   %s.information_schema.columns
            WHERE  table_catalog = ? AND table_schema = ? AND table_name = ?
            ORDER BY ordinal_position"""
            % self.enclose_identifier(self._catalog_name())
        )
        col_rows = self.execute_query_fetch_all(
            sql,
            log_level=VVERBOSE,
            query_params=[self._client.database, db_name, table_name],
        )
        if any(_[1] == SNOWFLAKE_TYPE_BINARY for _ in col_rows):
            # Information schema does not expose lengths of BINARY columns, need supplementary information from DESCRIBE
            binary_byte_lengths = get_binary_lengths_from_desc(db_name, table_name)
        for col in col_rows:
            (
                col_name,
                data_type,
                is_nullable,
                char_length,
                byte_length,
                num_precision,
                num_scale,
                time_scale,
            ) = col
            if time_scale is not None:
                data_scale = time_scale
            else:
                data_scale = num_scale
            if data_type == SNOWFLAKE_TYPE_BINARY:
                self._debug(
                    "Overriding byte_length for column %s with: %s"
                    % (col_name, binary_byte_lengths[col_name])
                )
                char_length = byte_length = binary_byte_lengths[col_name]
            backend_columns.append(
                SnowflakeColumn(
                    col_name,
                    data_type,
                    data_length=byte_length,
                    char_length=char_length,
                    data_precision=num_precision,
                    data_scale=data_scale,
                    nullable=is_nullable,
                )
            )
        return backend_columns

    def get_distinct_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        order_results=False,
        not_when_dry_running=False,
    ):
        """Run SQL to get distinct values for a list of columns.
        See interface description for parameter details.
        """

        def add_sql_cast(col):
            return (
                ("CAST(%s as TEXT)" % col)
                if col in (columns_to_cast_to_string or [])
                else col
            )

        assert column_name_list and isinstance(column_name_list, list)
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

    def get_max_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
        not_when_dry_running=False,
    ):
        """Run SQL to get max value for a set of columns.
        See interface description for parameter details.
        """
        return self._get_max_column_values_common(
            db_name,
            table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            optimistic_prune_clause=optimistic_prune_clause,
            not_when_dry_running=not_when_dry_running,
        )

    def get_partition_columns(self, db_name, table_name):
        """Table partitioning is not supported on Snowflake"""
        return []

    def get_query_plan(self, query_id):
        assert query_id
        sql = dedent(
            """\
             SELECT SYSTEM$EXPLAIN_JSON_TO_TEXT(
                SYSTEM$EXPLAIN_PLAN_JSON('%s')
             )
             """
            % query_id
        )
        try:
            rows = self.execute_query_fetch_all(
                sql, log_level=VVERBOSE, query_params=[query_id]
            )
            if not rows:
                raise BackendApiException(
                    "Query plan not found for query id: %s" % query_id
                )
            else:
                return "\n".join([row[0] for row in rows])
        except snowflake.connector.errors.ProgrammingError:
            raise BackendApiException(
                "Query plan not found for query id: %s" % query_id
            )
        finally:
            if self._cursor:
                self._close_cursor()

    def get_session_query_profile(self, session_id, query_id):
        assert session_id and query_id
        sql = dedent(
            """\
             SELECT *
             FROM TABLE(
                %s.information_schema.query_history_by_session(end_time_range_end => CURRENT_TIMESTAMP,
                                                               session_id => %s,
                                                               result_limit => 100)
             )
             WHERE query_id = ?"""
            % (self.enclose_identifier(self._catalog_name()), session_id)
        )
        try:
            self.execute_query_get_cursor(
                sql, log_level=VVERBOSE, query_params=[query_id]
            )
            row = self._cursor.fetchone()
            if not row:
                raise BackendApiException(
                    "Query plan not found for session id: %s, query id: %s"
                    % (session_id, query_id)
                )
            else:
                profile_keys = [_[0] for _ in self._cursor.description]
                stats = sorted(zip(profile_keys, list(row)))
                longest_key = max(len(_) for _ in profile_keys)
                stats = [
                    "{k: <{pad}}: {v}".format(k=k, v=v, pad=longest_key)
                    for k, v in stats
                ]
                return "\n".join(stats)
        except snowflake.connector.errors.ProgrammingError:
            raise BackendApiException(
                "Query plan not found for session id: %s, query id: %s"
                % (session_id, query_id)
            )
        finally:
            if self._cursor:
                self._close_cursor()

    def get_session_option(self, option_name):
        if not option_name:
            return None
        # LIKE is case insensitive
        sql = "SHOW PARAMETERS LIKE '%s'" % option_name
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        # Second field in SHOW PARAMETERS is value
        return row[1] if row else None

    def get_table_ddl(
        self, db_name, table_name, as_list=False, terminate_sql=False, for_replace=False
    ):
        return self._get_snowflake_ddl(
            db_name,
            table_name,
            "TABLE",
            as_list=as_list,
            terminate_sql=terminate_sql,
            for_replace=for_replace,
        )

    def get_table_location(self, db_name, table_name):
        """There is no load table on Snowflake"""
        return None

    def get_table_partition_count(self, db_name, table_name):
        """Table partitioning is not supported on Snowflake"""
        return 0

    def get_table_partitions(self, db_name, table_name):
        """Table partitioning is not supported on Snowflake"""
        return []

    def get_table_row_count(
        self,
        db_name,
        table_name,
        filter_clause=None,
        not_when_dry_running=False,
        log_level=VVERBOSE,
    ):
        sql = self._gen_select_count_sql_text_common(
            db_name, table_name, filter_clause=filter_clause
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=log_level,
            time_sql=True,
            not_when_dry_running=not_when_dry_running,
        )
        return row[0] if row else None

    def get_table_row_count_from_metadata(self, db_name, table_name):
        _, row_count = self.get_table_size_and_row_count(db_name, table_name)
        return row_count

    def get_table_size(self, db_name, table_name, no_cache=False):
        # no_cache is ignored on Snowflake because we use SQL to get the size.
        assert db_name and table_name
        size_bytes, _ = self.get_table_size_and_row_count(db_name, table_name)
        return size_bytes

    def get_table_size_and_row_count(self, db_name, table_name):
        assert db_name and table_name
        # Snowflake information_schema queries are faster if database is in FROM clause than when via column predicate
        sql = dedent(
            """\
                SELECT bytes, row_count
                FROM   %s.information_schema.tables
                WHERE  table_catalog = ? AND table_schema = ? AND table_name = ?"""
            % self.enclose_identifier(self._catalog_name())
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=VVERBOSE,
            query_params=[self._client.database, db_name, table_name],
        )
        return row if row else (None, None)

    def get_table_sort_columns(self, db_name, table_name, as_csv=True):
        """Extract cluster key columns from information_schema.
        The regexp_replace was taken from Snowflake documentation:
            https://community.snowflake.com/s/article/LINEAR-keyword-missing-in-clustering-key-column-in-information-schema-table-table
        In Snowflake, cluster keys can be expressions, Offload does not support that therefore we
        filter the results by real table columns.
        """
        assert db_name and table_name
        # Snowflake information_schema queries are faster if database is in FROM clause than when via column predicate
        sql = r"""SELECT REGEXP_REPLACE(clustering_key, '(LINEAR)?\\((.*)\\)','\\2')
                  FROM   %s.information_schema.tables
                  WHERE  table_catalog = ? AND table_schema = ? AND table_name = ?""" % self.enclose_identifier(
            self._catalog_name()
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=VVERBOSE,
            query_params=[self._client.database, db_name, table_name],
        )
        if not row or not row[0]:
            return []
        cluster_key_expressions = [
            unsurround(_.upper().strip(), self.enclosure_character())
            for _ in row[0].split(",")
        ]
        column_names = set(
            self.get_column_names(db_name, table_name, conv_fn=lambda x: x.upper())
        )
        # Drive by cluster key to maintain column order
        cluster_columns = [_ for _ in cluster_key_expressions if _ in column_names]
        return ",".join(cluster_columns) if as_csv else cluster_columns

    def get_table_stats(self, db_name, table_name, as_dict=False):
        assert db_name and table_name

        if self.table_exists(db_name, table_name):
            num_bytes, num_rows = self.get_table_size_and_row_count(db_name, table_name)
            tab_stats = {"num_rows": num_rows, "num_bytes": num_bytes, "avg_row_len": 0}
        else:
            tab_stats = EMPTY_BACKEND_TABLE_STATS_DICT

        if as_dict:
            return tab_stats, EMPTY_BACKEND_COLUMN_STATS_DICT
        else:
            stats_tuple = (
                tab_stats["num_rows"],
                tab_stats["num_bytes"],
                tab_stats["avg_row_len"],
            )
            return stats_tuple, EMPTY_BACKEND_COLUMN_STATS_LIST

    def get_table_and_partition_stats(self, db_name, table_name, as_dict=False):
        tab_stats, _ = self.get_table_stats(db_name, table_name, as_dict=as_dict)
        if as_dict:
            return tab_stats, {}, EMPTY_BACKEND_COLUMN_STATS_DICT
        else:
            return tab_stats, [], EMPTY_BACKEND_COLUMN_STATS_LIST

    def get_table_stats_partitions(self, db_name, table_name):
        raise NotImplementedError("Get statistics does not apply for Snowflake")

    def get_user_name(self):
        sql = "SELECT current_user()"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return row[0] if row else None

    def insert_literal_values(
        self,
        db_name,
        table_name,
        literal_list,
        column_list=None,
        max_rows_per_insert=250,
        split_by_cr=True,
    ):
        def gen_literal(py_val, data_type):
            return str(self.to_backend_literal(py_val, data_type))

        def add_cast(literal, formatted_data_type):
            if any(
                _ in formatted_data_type
                for _ in [
                    SNOWFLAKE_TYPE_TEXT,
                    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                    SNOWFLAKE_TYPE_TIMESTAMP_TZ,
                ]
            ):
                return literal
            elif SNOWFLAKE_TYPE_BINARY in formatted_data_type:
                return "TO_BINARY(%s, 'UTF-8')" % literal
            else:
                return "CAST(%s AS %s)" % (literal, formatted_data_type)

        assert db_name
        assert table_name
        assert literal_list and isinstance(literal_list, list)
        assert isinstance(literal_list[0], list)

        if column_list:
            assert valid_column_list(column_list), (
                "Incorrectly formed column_list: %s" % column_list
            )

        column_names = [_.name for _ in column_list]
        cmds = []
        remaining_rows = literal_list[:]
        while remaining_rows:
            this_chunk = remaining_rows[:max_rows_per_insert]
            remaining_rows = remaining_rows[max_rows_per_insert:]
            formatted_rows = []
            for row in this_chunk:
                formatted_rows.append(
                    ",".join(
                        add_cast(
                            gen_literal(py_val, col.data_type), col.format_data_type()
                        )
                        for py_val, col in zip(row, column_list)
                    )
                )
            sql = self._insert_literals_using_insert_values_sql_text(
                db_name,
                table_name,
                column_names,
                formatted_rows,
                split_by_cr=split_by_cr,
            )
            cmds.extend(self.execute_dml(sql, log_level=VVERBOSE))
        return cmds

    def set_session_db(self, db_name, log_level=VERBOSE):
        assert db_name
        return self.execute_ddl("USE SCHEMA %s" % db_name, log_level=log_level)

    def snowflake_file_format_exists(self, db_name, file_format_name, format_type=None):
        """Check a Snowflake File Format exists. This is Snowflake only, hence snowflake in the method name.
        format_type can optionally be passed in to verify any existing file_format is for the correct file type.
        It is public so that BackendSnowflakeTable can call it.
        """
        assert self._backend_type == DBTYPE_SNOWFLAKE
        sql = dedent(
            """\
            SELECT file_format_type
            FROM   %s.information_schema.file_formats
            WHERE  file_format_catalog = ? AND file_format_schema = ? AND file_format_name = ?"""
            % self.enclose_identifier(self._catalog_name())
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=VVERBOSE,
            query_params=[self._client.database, db_name, file_format_name],
        )
        if row and format_type:
            # We've additionally asked for a check that the file_format_type matches a specific value
            if format_type.upper() != row[0].upper():
                raise BackendApiException(
                    "File format %s has file type mismatch: %s != %s"
                    % (
                        self.enclose_object_reference(db_name, file_format_name),
                        format_type.upper(),
                        row[0].upper(),
                    )
                )
        return bool(row)

    def snowflake_integration_exists(self, integration_name):
        """Check a Snowflake Integration exists. This is Snowflake only, hence snowflake in the method name.
        It is public so that BackendTable can call it.
        """
        assert self._backend_type == DBTYPE_SNOWFLAKE
        # Docs cover IN DATABASE but it is not supported yet (also mentioned in docs):
        # https://docs.snowflake.com/en/sql-reference/sql/show-integrations.html
        # sql = "SHOW STORAGE INTEGRATIONS LIKE '%s' IN DATABASE %s" % (integration_name, self._client.database)
        sql = "SHOW STORAGE INTEGRATIONS LIKE '%s'" % integration_name
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return bool(row)

    def snowflake_stage_exists(self, db_name, stage_name, url_prefix=None):
        """Check a Snowflake Stage exists. This is Snowflake only, hence snowflake in the method name.
        It is public so that BackendTable can call it.
        """
        assert self._backend_type == DBTYPE_SNOWFLAKE
        sql = dedent(
            """\
            SELECT stage_url
            FROM   %s.information_schema.stages
            WHERE  stage_catalog = ? AND stage_schema = ? AND stage_name = ?"""
            % self.enclose_identifier(self._catalog_name())
        )
        query_params = [self._client.database, db_name, stage_name]
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        if row and url_prefix:
            # We've additionally asked for a check that the stage_url matches a specific value
            if not row[0].lower().startswith(url_prefix.lower()):
                raise BackendApiException(
                    "Snowflake stage %s has URL mismatch: %s does not start with %s"
                    % (
                        self.enclose_object_reference(db_name, stage_name),
                        row[0].lower(),
                        url_prefix.lower(),
                    )
                )
        return bool(row)

    def is_nan_sql_expression(self, column_expr):
        return "%s = 'NaN'" % column_expr

    def is_valid_partitioning_data_type(self, data_type):
        """On Snowflake there are no partition columns therefore this always returns False"""
        return False

    def is_valid_sort_data_type(self, data_type):
        """Always True on Snowflake.
        From Snowflake docs:
            Where each clustering key consists of one or more table columns/expressions, which can be of any
            data type, except VARIANT, OBJECT, or ARRAY
        """
        return True

    def is_valid_storage_compression(self, user_requested_codec, user_requested_format):
        return bool(user_requested_codec in ["NONE", None])

    def is_valid_storage_format(self, storage_format):
        return bool(storage_format is None)

    def is_view(self, db_name, object_name):
        return self.view_exists(db_name, object_name)

    def length_sql_expression(self, column_expression):
        assert column_expression
        return "LENGTH(%s)" % column_expression

    def list_databases(self, db_name_filter=None, case_sensitive=True):
        sql = (
            """SELECT schema_name FROM %s.information_schema.schemata WHERE catalog_name = ?"""
            % self.enclose_identifier(self._catalog_name())
        )
        query_params = [self._client.database]
        if db_name_filter:
            sql += " AND schema_name %s ?" % ("LIKE" if case_sensitive else "ILIKE")
            query_params.append(db_name_filter.replace("*", "%"))
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        return [_[0] for _ in rows]

    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        sql = (
            """SELECT table_name FROM %s.information_schema.tables WHERE table_catalog = ? AND table_schema = ?"""
            % self.enclose_identifier(self._catalog_name())
        )
        query_params = [self._client.database, db_name]
        if table_name_filter:
            sql += " AND table_name %s ?" % ("LIKE" if case_sensitive else "ILIKE")
            query_params.append(table_name_filter.replace("*", "%"))
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        return [_[0] for _ in rows]

    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        sql = (
            dedent(
                """\
                SELECT procedure_name, data_type
                FROM  %s.information_schema.procedures
                WHERE procedure_catalog = ?
                AND   procedure_schema = ?"""
            )
            % self.enclose_identifier(self._catalog_name())
        )
        query_params = [self._client.database, db_name]
        if udf_name_filter:
            sql += "\nAND   procedure_name %s ?" % (
                "LIKE" if case_sensitive else "ILIKE"
            )
            query_params.append(udf_name_filter.replace("*", "%"))
        return self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )

    def list_views(self, db_name, view_name_filter=None, case_sensitive=True):
        sql = (
            """SELECT table_name FROM %s.information_schema.views WHERE table_catalog = ? AND table_schema = ?"""
            % self.enclose_identifier(self._catalog_name())
        )
        query_params = [self._client.database, db_name]
        if view_name_filter:
            sql += " AND table_name %s ?" % ("LIKE" if case_sensitive else "ILIKE")
            query_params.append(view_name_filter.replace("*", "%"))
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        return [_[0] for _ in rows]

    def max_decimal_integral_magnitude(self):
        return 38

    def max_decimal_precision(self):
        return 38

    def max_decimal_scale(self, data_type=None):
        return 37

    def max_datetime_value(self):
        return datetime64("99999-12-31T23:59:59.999999")

    def max_datetime_scale(self):
        return 9

    def max_partition_columns(self):
        return 0

    def max_sort_columns(self):
        """Cannot find a documented maximum but tested up to 15 columns and worked. Did find this in docs:
        https://docs.snowflake.com/en/user-guide/tables-clustering-keys.html#strategies-for-selecting-clustering-keys
            For most tables, Snowflake recommends a maximum of 3 or 4 columns (or expressions) per key.
            Adding more than 3-4 columns tends to increase costs more than benefits.
        """
        return SORT_COLUMNS_UNLIMITED

    def max_table_name_length(self):
        return 255

    def min_datetime_value(self):
        return datetime64("0000-01-01")

    def native_integer_types(self):
        return [SNOWFLAKE_TYPE_INTEGER]

    def partition_column_requires_synthetic_column(self, backend_column, granularity):
        return False

    def populate_sequence_table(
        self, db_name, table_name, starting_seq, target_seq, split_by_cr=False
    ):
        raise NotImplementedError("Sequence table does not apply for Snowflake")

    def refresh_table_files(self, db_name, table_name, sync=None):
        """No requirement to re-scan files for a table on Snowflake but drop cache because that will be stale"""
        self.drop_state()

    def regexp_extract_sql_expression(self, subject, pattern):
        return "REGEXP_SUBSTR(%s, '%s', 1, 1, 'e')" % (subject, pattern)

    def rename_table(
        self, from_db_name, from_table_name, to_db_name, to_table_name, sync=None
    ):
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

        executed_sqls = self.execute_ddl(rename_sql, sync=sync)
        return executed_sqls

    def role_exists(self, role_name):
        """Check the role exists, returns True/False"""
        sql = "SHOW ROLES LIKE '%s'" % role_name
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return bool(row)

    def sequence_table_max(self, db_name, table_name):
        raise NotImplementedError("Sequence table does not apply for Snowflake")

    def set_column_stats(
        self, db_name, table_name, new_column_stats, ndv_cap, num_null_factor
    ):
        raise NotImplementedError(
            "Set of table statistics does not apply for Snowflake"
        )

    def set_partition_stats(
        self, db_name, table_name, new_partition_stats, additive_stats
    ):
        raise NotImplementedError(
            "Set of table statistics does not apply for Snowflake"
        )

    def set_table_stats(self, db_name, table_name, new_table_stats, additive_stats):
        raise NotImplementedError(
            "Set of table statistics does not apply for Snowflake"
        )

    def supported_backend_data_types(self):
        return [
            SNOWFLAKE_TYPE_BINARY,
            SNOWFLAKE_TYPE_DATE,
            SNOWFLAKE_TYPE_FLOAT,
            SNOWFLAKE_TYPE_NUMBER,
            SNOWFLAKE_TYPE_TEXT,
            SNOWFLAKE_TYPE_TIME,
            SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
            SNOWFLAKE_TYPE_TIMESTAMP_TZ,
        ]

    def supported_date_based_partition_granularities(self):
        """No partitioning on Snowflake"""
        return None

    def supported_partition_function_parameter_data_types(self):
        """No partitioning on Snowflake"""
        return None

    def supported_partition_function_return_data_types(self):
        """No partitioning on Snowflake"""
        return None

    def synthetic_partition_numbers_are_string(self):
        """No partitioning on Snowflake"""
        return None

    def table_distribution(self, db_name, table_name):
        return None

    def table_exists(self, db_name: str, table_name: str) -> bool:
        """Return True/False if a table exists using SHOW TABLES.
        DESCRIBE TABLE/SHOW TABLES are much faster than using INFORMATION_SCHEMA but both have problems:
        SHOW TABLES is has case-insensitive LIKE and IN clauses plus throws exceptions if the schema doesn't exist.
        DESCRIBE TABLE has no case-sensitivity problems but is interchangeable with DESCRIBE VIEW, also
                       throws exceptions when schema or object do not exist.
        INFORMATION_SCHEMA is much nicer to use but slow and costs money. Offload Status Report is really slow
        when using INFORMATION_SCHEMA for table/view_exists(). So it is a case of choosing the least worst option.
        Chosen below and will recheck data in Python to avoid case-sensitivity issues:
            SHOW TERSE TABLES LIKE '%s' IN SCHEMA %s STARTS WITH '%s'
        I can't find a documented list of err codes which is why we're using a primitive check on the msg attribute.
        Because we're relying on the order of the output columns from SHOW we use cursor.description to verify
        things are where they should be.
        """
        sql = "SHOW TERSE TABLES LIKE '%s' IN SCHEMA %s.%s STARTS WITH '%s'" % (
            table_name,
            self.enclose_identifier(self._catalog_name()),
            self.enclose_identifier(db_name),
            table_name,
        )
        try:
            cursor = self.execute_query_get_cursor(sql, log_level=None)
            headers = [_[0] for _ in cursor.description]
            assert headers[1] == "name"
            assert headers[3] == "database_name"
            assert headers[4] == "schema_name"
            rows = cursor.fetchall()
            if any(
                (_[3], _[4], _[1]) == (self._catalog_name(), db_name, table_name)
                for _ in rows
            ):
                return True
        except snowflake.connector.ProgrammingError as exc:
            if "not exist" in exc.msg:
                return False
            else:
                raise
        return False

    def table_has_rows(self, db_name: str, table_name: str) -> bool:
        """Return bool depending whether the table has rows or not."""
        sql = f"SELECT 1 FROM {self.enclose_object_reference(db_name, table_name)} LIMIT 1"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return bool(row)

    def target_version(self):
        """Return version of the backend SQL engine in x.y.z format that can be used by GOEVersion().
        This is different to backend_version() even though it appears similar in function.
        """
        if self._target_version is None:
            sql = "SELECT current_version()"
            row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
            self._target_version = row[0] if row else None
        return self._target_version

    def to_backend_literal(self, py_val, data_type=None):
        """Translate a Python value to a Snowflake literal"""
        self._debug("Formatting %s literal: %s" % (type(py_val), repr(py_val)))
        self._debug("For backend datatype: %s" % data_type)
        new_py_val = SnowflakeLiteral.format_literal(py_val, data_type=data_type)
        self._debug("Final %s literal: %s" % (type(py_val), str(new_py_val)))
        return new_py_val

    def transform_encrypt_data_type(self):
        return SNOWFLAKE_TYPE_TEXT

    def transform_null_cast(self, backend_column):
        assert isinstance(backend_column, SnowflakeColumn)
        return "CAST(NULL AS %s)" % (backend_column.format_data_type())

    def transform_tokenize_data_type(self):
        return SNOWFLAKE_TYPE_TEXT

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

    def udf_details(self, db_name, udf_name):
        """Get details of a Snowflake UDF"""
        sql = (
            dedent(
                """\
                SELECT data_type, argument_signature
                FROM  %s.information_schema.procedures
                WHERE procedure_catalog = ?
                AND   procedure_schema = ?
                AND   procedure_name = ?"""
            )
            % self.enclose_identifier(self._catalog_name())
        )
        query_params = [self._client.database, db_name, udf_name]
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        udfs = []
        for row in rows:
            return_type = row[0] if row else None
            parameters = []
            if row and row[1]:
                # UDF parameters are embedded in a string, e.g.:
                # (ARG1 FLOAT, ARG2 STRING)
                arg_strings = re.findall(r"[A-Z0-9_]+ [A-Z0-9_]+", row[1])
                if arg_strings:
                    parameters = [tuple(_.split()) for _ in arg_strings]
            udfs.append(UdfDetails(db_name, udf_name, return_type, parameters))
        return udfs

    def udf_installation_os(self, user_udf_version):
        raise NotImplementedError("GOE UDFs are not supported on Snowflake")

    def udf_installation_sql(self, create_udf_db, udf_db=None):
        raise NotImplementedError("GOE UDFs are not supported on Snowflake")

    def udf_installation_test(self, udf_db=None):
        raise NotImplementedError("GOE UDFs are not supported on Snowflake")

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, SnowflakeColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.is_number_based():
            if column.data_type == SNOWFLAKE_TYPE_FLOAT:
                # Snowflake FLOAT is always stored as DOUBLE therefore no canonical FLOAT below
                return bool(target_type in [GOE_TYPE_DECIMAL, GOE_TYPE_DOUBLE])
            else:
                return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based():
            return bool(target_type in DATE_CANONICAL_TYPES)
        elif column.is_string_based():
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
        elif column.data_type == SNOWFLAKE_TYPE_BINARY:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        elif column.data_type == SNOWFLAKE_TYPE_TIME:
            return bool(target_type == GOE_TYPE_TIME)
        return False

    def valid_staging_formats(self):
        return [FILE_STORAGE_FORMAT_AVRO, FILE_STORAGE_FORMAT_PARQUET]

    def view_exists(self, db_name, view_name):
        """Return True/False if a view exists
        See header of table_exists() for rationale for using SHOW VIEWS
        """
        sql = "SHOW TERSE VIEWS LIKE '%s' IN SCHEMA %s.%s STARTS WITH '%s'" % (
            view_name,
            self.enclose_identifier(self._catalog_name()),
            self.enclose_identifier(db_name),
            view_name,
        )
        try:
            cursor = self.execute_query_get_cursor(sql, log_level=None)
            headers = [_[0] for _ in cursor.description]
            assert headers[1] == "name"
            assert headers[3] == "database_name"
            assert headers[4] == "schema_name"
            rows = cursor.fetchall()
            if any(
                (_[3], _[4], _[1]) == (self._catalog_name(), db_name, view_name)
                for _ in rows
            ):
                return True
        except snowflake.connector.ProgrammingError as exc:
            if "not exist" in exc.msg:
                return False
            else:
                raise
        return False

    def to_canonical_column(self, column):
        """Translate a Snowflake column to an internal GOE column"""

        def new_column(
            col, data_type, data_precision=None, data_scale=None, safe_mapping=None
        ):
            """Wrapper that carries name forward but applies other attributes as specified"""
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
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            )

        assert column
        assert isinstance(column, SnowflakeColumn)

        if column.data_type == SNOWFLAKE_TYPE_BOOLEAN:
            return new_column(column, GOE_TYPE_BOOLEAN)
        elif column.data_type == SNOWFLAKE_TYPE_BINARY:
            return new_column(column, GOE_TYPE_BINARY)
        elif column.data_type == SNOWFLAKE_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE)
        elif column.data_type == SNOWFLAKE_TYPE_FLOAT:
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == SNOWFLAKE_TYPE_INTEGER:
            return new_column(column, GOE_TYPE_INTEGER_38)
        elif column.data_type == SNOWFLAKE_TYPE_NUMBER:
            if column.data_scale == 0:
                if 1 <= column.data_precision <= 2:
                    integral_type = GOE_TYPE_INTEGER_1
                elif 3 <= column.data_precision <= 4:
                    integral_type = GOE_TYPE_INTEGER_2
                elif 5 <= column.data_precision <= 9:
                    integral_type = GOE_TYPE_INTEGER_4
                elif 10 <= column.data_precision <= 18:
                    integral_type = GOE_TYPE_INTEGER_8
                else:
                    integral_type = GOE_TYPE_INTEGER_38
                return new_column(column, integral_type)
            else:
                return new_column(
                    column,
                    GOE_TYPE_DECIMAL,
                    data_precision=column.data_precision,
                    data_scale=column.data_scale,
                )
        elif column.data_type == SNOWFLAKE_TYPE_TEXT:
            return new_column(column, GOE_TYPE_VARIABLE_STRING)
        elif column.data_type == SNOWFLAKE_TYPE_TIME:
            return new_column(column, GOE_TYPE_TIME)
        elif column.data_type == SNOWFLAKE_TYPE_TIMESTAMP_NTZ:
            return new_column(
                column, GOE_TYPE_TIMESTAMP, data_scale=self.max_datetime_scale()
            )
        elif column.data_type == SNOWFLAKE_TYPE_TIMESTAMP_TZ:
            return new_column(
                column, GOE_TYPE_TIMESTAMP_TZ, data_scale=self.max_datetime_scale()
            )
        else:
            raise NotImplementedError(
                "Unsupported backend data type: %s" % column.data_type
            )

    def from_canonical_column(self, column, decimal_padding_digits=0):
        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            char_length=None,
            safe_mapping=None,
        ):
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return SnowflakeColumn(
                col.name,
                data_type=data_type,
                data_length=data_length,
                char_length=char_length,
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

        if column.data_type == GOE_TYPE_BOOLEAN:
            return new_column(column, SNOWFLAKE_TYPE_BOOLEAN, safe_mapping=True)
        elif column.data_type in (
            GOE_TYPE_FIXED_STRING,
            GOE_TYPE_LARGE_STRING,
            GOE_TYPE_VARIABLE_STRING,
        ):
            return new_column(
                column,
                SNOWFLAKE_TYPE_TEXT,
                char_length=column.char_length or column.data_length,
                safe_mapping=True,
            )
        elif column.data_type in (GOE_TYPE_LARGE_BINARY, GOE_TYPE_BINARY):
            return new_column(
                column, SNOWFLAKE_TYPE_BINARY, data_length=column.data_length
            )
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, SNOWFLAKE_TYPE_DATE, safe_mapping=True)
        elif column.data_type in (GOE_TYPE_FLOAT, GOE_TYPE_DOUBLE):
            return new_column(column, SNOWFLAKE_TYPE_FLOAT, safe_mapping=True)
        elif column.data_type == GOE_TYPE_INTEGER_1:
            return new_column(
                column,
                SNOWFLAKE_TYPE_NUMBER,
                data_precision=3,
                data_scale=0,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_INTEGER_2:
            return new_column(
                column,
                SNOWFLAKE_TYPE_NUMBER,
                data_precision=5,
                data_scale=0,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_INTEGER_4:
            return new_column(
                column,
                SNOWFLAKE_TYPE_NUMBER,
                data_precision=10,
                data_scale=0,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_INTEGER_8:
            return new_column(
                column,
                SNOWFLAKE_TYPE_NUMBER,
                data_precision=19,
                data_scale=0,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_INTEGER_38:
            return new_column(
                column,
                SNOWFLAKE_TYPE_NUMBER,
                data_precision=38,
                data_scale=0,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_DECIMAL:
            if column.data_precision is None and column.data_scale is None:
                return self.gen_default_numeric_column(column.name)
            else:
                data_precision = (
                    column.data_precision
                    if column.data_precision
                    else self.max_decimal_precision()
                )
                return new_column(
                    column,
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=data_precision,
                    data_scale=column.data_scale,
                    safe_mapping=True,
                )
        elif column.data_type == GOE_TYPE_TIME:
            return new_column(
                column,
                SNOWFLAKE_TYPE_TIME,
                data_scale=column.data_scale,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            return new_column(
                column, SNOWFLAKE_TYPE_TIMESTAMP_NTZ, data_scale=column.data_scale
            )
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            return new_column(
                column, SNOWFLAKE_TYPE_TIMESTAMP_TZ, data_scale=column.data_scale
            )
        elif column.data_type == GOE_TYPE_INTERVAL_DS:
            return new_column(column, SNOWFLAKE_TYPE_TEXT, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_YM:
            return new_column(column, SNOWFLAKE_TYPE_TEXT, safe_mapping=False)
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )
