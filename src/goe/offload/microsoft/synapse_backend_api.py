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

""" BackendSynapseApi: Library for logic/interaction with an Azure Synapse Sql Server backend.
"""

from datetime import datetime, timedelta, timezone
from enum import Enum
import logging
import re
import struct
from textwrap import dedent
import traceback

from numpy import datetime64
import pyodbc

from goe.connect.connect_constants import CONNECT_DETAIL, CONNECT_STATUS, CONNECT_TEST
from goe.offload.backend_api import (
    BackendApiInterface,
    BackendApiException,
    FETCH_ACTION_ALL,
    FETCH_ACTION_CURSOR,
    FETCH_ACTION_ONE,
    REPORT_ATTR_BACKEND_CLASS,
    REPORT_ATTR_BACKEND_TYPE,
    REPORT_ATTR_BACKEND_DISPLAY_NAME,
    REPORT_ATTR_BACKEND_HOST_INFO_TYPE,
    REPORT_ATTR_BACKEND_HOST_INFO,
    SORT_COLUMNS_UNLIMITED,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    is_safe_mapping,
    valid_column_list,
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
    ALL_CANONICAL_TYPES,
    DATE_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
    CANONICAL_CHAR_SEMANTICS_BYTE,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
)
from goe.offload.offload_constants import (
    SYNAPSE_BACKEND_CAPABILITIES,
    FILE_STORAGE_FORMAT_PARQUET,
    EMPTY_BACKEND_TABLE_STATS_DICT,
    EMPTY_BACKEND_TABLE_STATS_LIST,
    EMPTY_BACKEND_COLUMN_STATS_DICT,
    EMPTY_BACKEND_COLUMN_STATS_LIST,
    DBTYPE_SYNAPSE,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.microsoft.synapse_column import (
    SynapseColumn,
    SYNAPSE_TYPE_BIGINT,
    SYNAPSE_TYPE_BINARY,
    SYNAPSE_TYPE_BIT,
    SYNAPSE_TYPE_CHAR,
    SYNAPSE_TYPE_DATE,
    SYNAPSE_TYPE_DATETIME,
    SYNAPSE_TYPE_DATETIME2,
    SYNAPSE_TYPE_DATETIMEOFFSET,
    SYNAPSE_TYPE_DECIMAL,
    SYNAPSE_TYPE_FLOAT,
    SYNAPSE_TYPE_INT,
    SYNAPSE_TYPE_MONEY,
    SYNAPSE_TYPE_NCHAR,
    SYNAPSE_TYPE_NUMERIC,
    SYNAPSE_TYPE_NVARCHAR,
    SYNAPSE_TYPE_REAL,
    SYNAPSE_TYPE_SMALLDATETIME,
    SYNAPSE_TYPE_SMALLINT,
    SYNAPSE_TYPE_SMALLMONEY,
    SYNAPSE_TYPE_TIME,
    SYNAPSE_TYPE_TINYINT,
    SYNAPSE_TYPE_UNIQUEIDENTIFIER,
    SYNAPSE_TYPE_VARBINARY,
    SYNAPSE_TYPE_VARCHAR,
    SYNAPSE_TYPE_MAX_TOKEN,
)
from goe.offload.microsoft.synapse_constants import (
    SYNAPSE_AUTH_MECHANISM_AD_MSI,
    SYNAPSE_AUTH_MECHANISM_AD_SERVICE_PRINCIPAL,
    SYNAPSE_USER_PASS_AUTH_MECHANISMS,
)
from goe.offload.microsoft.synapse_literal import SynapseLiteral
from goe.util.misc_functions import (
    add_prefix_in_same_case,
    format_list_for_logging,
    human_size_to_bytes,
    id_generator,
)
from goe.util.hive_table_stats import (
    parse_stats_into_tab_col,
    transform_stats_as_tuples,
)

###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

# Regular expression matching invalid identifier characters, as a constant to ensure compiled only once.
SYNAPSE_INVALID_IDENTIFIER_CHARS_RE = re.compile(r'[\[\]"]', re.I)

# Column statistics object prefix
SYNAPSE_COLUMN_STAT_PREFIX = "goe_statistics_"


# Enums for tuple return values
class sp_spaceused(Enum):
    name = 0
    rows = 1
    reserved = 2
    data = 3
    index_size = 4
    unused = 5


class dbcc_showstatistics_stat_header(Enum):
    name = 0
    updated = 1
    rows = 2
    rows_sampled = 3
    steps = 4
    density = 5
    average_key_length = 6
    string_index = 7
    filter_expression = 8
    unfiltered_rows = 9
    persisted_sample_percent = 10


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def synapse_collation_clause(column_or_collation):
    assert isinstance(column_or_collation, (SynapseColumn, str))
    if isinstance(column_or_collation, SynapseColumn):
        return (
            " COLLATE {}".format(column_or_collation.collation)
            if column_or_collation.collation
            else ""
        )
    else:
        return " COLLATE {}".format(column_or_collation) if column_or_collation else ""


###########################################################################
# BackendSynapseApi
###########################################################################


class BackendSynapseApi(BackendApiInterface):
    def __init__(
        self,
        connection_options,
        backend_type,
        messages,
        dry_run=False,
        no_caching=False,
        do_not_connect=False,
    ):
        super().__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        def handle_datetimeoffset(dto_value):
            # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
            tup = struct.unpack(
                "<6hI2h", dto_value
            )  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
            return datetime(
                tup[0],
                tup[1],
                tup[2],
                tup[3],
                tup[4],
                tup[5],
                tup[6] // 1000,
                timezone(timedelta(hours=tup[7], minutes=tup[8])),
            )

        logger.info("BackendSynapseApi")
        if dry_run:
            logger.info("* Dry run *")

        self._client = None
        # Some unit tests don't have all options hence the hasattr below
        if (
            hasattr(connection_options, "synapse_database")
            and connection_options.synapse_database
        ):
            self._synapse_database = connection_options.synapse_database
        else:
            self._synapse_database = None

        if not do_not_connect:
            url = [
                "Driver=" + "{%s}" % connection_options.backend_odbc_driver_name,
                "Server=tcp:%s,%s"
                % (connection_options.synapse_server, connection_options.synapse_port),
                "Database=" + self._synapse_database,
            ]
            # Uid and Pwd can come from different sources depending on the auth mechanism
            if (
                connection_options.synapse_auth_mechanism
                in SYNAPSE_USER_PASS_AUTH_MECHANISMS
            ):
                if connection_options.synapse_user:
                    url.append("Uid=" + connection_options.synapse_user)
                if connection_options.synapse_pass:
                    synapse_pass = self._decrypt_password(
                        connection_options.synapse_pass
                    )
                    url.append("Pwd=" + synapse_pass)
            elif (
                connection_options.synapse_auth_mechanism
                == SYNAPSE_AUTH_MECHANISM_AD_SERVICE_PRINCIPAL
            ):
                if connection_options.synapse_service_principal_id:
                    url.append("Uid=" + connection_options.synapse_service_principal_id)
                if connection_options.synapse_service_principal_secret:
                    url.append(
                        "Pwd=" + connection_options.synapse_service_principal_secret
                    )
            elif (
                connection_options.synapse_auth_mechanism
                == SYNAPSE_AUTH_MECHANISM_AD_MSI
            ):
                if connection_options.synapse_msi_client_id:
                    url.append("Uid=" + connection_options.synapse_msi_client_id)
            url.extend(
                [
                    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30",
                    "Authentication=" + connection_options.synapse_auth_mechanism,
                ]
            )
            odbc_url = ";".join(url)
            self._client = pyodbc.connect(odbc_url)
            # Without autocommit DDL fails with: "Operation cannot be performed within a transaction"
            self._client.autocommit = True
            # Add output converter function for DATETIMEOFFSET
            # https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
            self._client.add_output_converter(-155, handle_datetimeoffset)

        self._cursor = None
        self._sql_engine_name = "Synapse"
        self._target_version = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _backend_capabilities(self):
        return SYNAPSE_BACKEND_CAPABILITIES

    def _close_cursor(self):
        """After a DDL/DML operation this is raising and logging:
            "Exception closing cursor: Attempt to use a closed cursor."
        Those operations have None in the description property on the cursor so we
        check for it before trying to close.
        """
        if self._client and self._cursor and self._cursor.description is not None:
            try:
                self._cursor.close()
            except Exception as exc:
                self._log("Exception closing cursor:\n%s" % str(exc), detail=VVERBOSE)

    def _collation_clause(self, column_or_collation):
        return synapse_collation_clause(column_or_collation)

    def _create_statistics_sql(self, db_name, table_name, column_name, sample_pct=5):
        """Return SQL to create statistics object on a column"""
        return "CREATE STATISTICS %s ON %s (%s) WITH SAMPLE %s PERCENT" % (
            self.enclose_identifier(
                add_prefix_in_same_case(id_generator(16), SYNAPSE_COLUMN_STAT_PREFIX)
            ),
            self.enclose_object_reference(db_name, table_name),
            self.enclose_identifier(column_name),
            sample_pct,
        )

    def _create_table_columns_clause(self, column_list, external=False):
        """Trivial helper for SQL text consistency.
        Not using BackendApi common method due to Synapse requiring COLLATE support.
        """
        sql_cols = [
            (
                self.enclose_identifier(_.name),
                _.format_data_type(),
                self._collation_clause(_),
                self._create_table_column_nn_clause_common(_, external=external),
            )
            for _ in column_list
        ]
        max_name = max(len(_[0]) for _ in sql_cols)
        max_type = max(len(_[1]) for _ in sql_cols)
        max_collate = max(len(_[2]) for _ in sql_cols)
        col_template = f"%-{max_name}s %-{max_type}s %-{max_collate}s %s"
        return "    " + "\n,   ".join(
            [col_template % (_[0], _[1], _[2], _[3]) for _ in sql_cols]
        )

    def _create_table_sql_text(
        self,
        db_name,
        table_name,
        column_list,
        partition_column_names,
        location=None,
        external=False,
        table_properties=None,
        sort_column_names=None,
    ):
        """Return SQL text to create a Synapse table.
        See abstract method create_table() for more description.
        """

        def cci_incompatible(column_list):
            # Check if any columns cannot be part of a clustered columnstore index
            return bool(
                any(
                    SYNAPSE_TYPE_MAX_TOKEN in _
                    for _ in [
                        col.format_data_type()
                        for col in column_list
                        if col.data_type
                        in (
                            SYNAPSE_TYPE_VARCHAR,
                            SYNAPSE_TYPE_NVARCHAR,
                            SYNAPSE_TYPE_VARBINARY,
                        )
                    ]
                )
            )

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
                "Partitioning by column is not supported for Synapse"
            )
        if external:
            assert location
            assert table_properties

        col_projection = self._create_table_columns_clause(
            column_list, external=external
        )

        with_clauses = []
        if sort_column_names:
            if cci_incompatible(column_list):
                self._messages.notice(
                    "Ignoring sort columns due to the presence of datatype(s) with %s precision"
                    % SYNAPSE_TYPE_MAX_TOKEN
                )
            else:
                sort_csv = ",".join(
                    [self.enclose_identifier(_) for _ in sort_column_names]
                )
                if sort_csv:
                    with_clauses.append(
                        "CLUSTERED COLUMNSTORE INDEX ORDER (%s)" % sort_csv
                    )

        if external:
            external_clause = " EXTERNAL"
            table_properties["LOCATION"] = self.to_backend_literal(location)
        else:
            external_clause = ""
            if cci_incompatible(column_list):
                with_clauses.append("HEAP")
                if table_properties:
                    table_properties.pop("DISTRIBUTION", None)
                self._messages.notice(
                    "Creating backend table as HEAP due to the presence of datatype(s) with %s precision"
                    % SYNAPSE_TYPE_MAX_TOKEN
                )

        if table_properties:
            with_clauses.extend(["%s=%s" % (k, v) for k, v in table_properties.items()])
        with_clause = ""
        if with_clauses:
            with_clause = "\nWITH (\n    %(with_statement)s\n)" % {
                "with_statement": "\n,   ".join(with_clauses)
            }

        sql = (
            dedent(
                """\
                    CREATE%(external)s TABLE %(db_table)s (
                    %(col_projection)s
                    )%(with_clause)s"""
            )
            % {
                "external": external_clause,
                "db_table": self.enclose_object_reference(db_name, table_name),
                "col_projection": col_projection,
                "with_clause": with_clause,
            }
        )
        return sql

    def _execute_ddl_or_dml(
        self,
        sql,
        sync=None,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        """Run SQL that has no result set, e.g. DDL, sql can be a str or a list of SQLs.
        This function returns the SQL statements that were executed.
        Exception will be raised upon failure.
        Parameters:
            sync: None means do nothing. True/False mean act as appropriate.
            query_options: key/value pairs for session settings.
            profile: if True we will log a profile (if possible) *after the first* SQL statement.
        """
        assert sql
        assert isinstance(sql, (str, list))

        # _execute_sqls opens its own cursor so pass through query_options
        run_sqls = self._execute_sqls(
            sql,
            query_options,
            log_level=log_level,
            profile=profile,
            no_log_items=no_log_items,
        )
        return run_sqls

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

    def _execute_global_session_parameters(self, log_level=VVERBOSE):
        """Execute global backend session parameters.
        self._global_session_parameters is set in the super interface class and merges backend session parameters
        passed in from the command line and any _fixed_session_parameters set in this class.
        """
        if self._global_session_parameters:
            if log_level is not None:
                self._log("Setting global session options:", detail=log_level)
            return self._execute_session_options(
                self._global_session_parameters, log_level=log_level
            )
        else:
            return []

    def _add_option_clause_to_sql_text(self, sql_text: str, option_dict: dict):
        assert isinstance(option_dict, dict)
        assert option_dict
        option_string = ",".join(f"{k} = '{v}'" for k, v in option_dict.items())
        option_clause = f"\nOPTION ({option_string})"
        if not self._option_clause_valid_for_sql_text(sql_text):
            # We cannot add OPTION clause to all statements. Ideally this is covered by the developer not asking
            # for a profile when it doesn't make sense. But in some cases we may pass a list of SQLs where some are
            # compatible and some are not.
            self._log(
                f"Cannot add OPTION clause to incompatible SQL statement: {option_clause}",
                detail=VVERBOSE,
            )
            return sql_text
        if sql_text.rstrip().endswith(";"):
            # We need to inject the OPTION clause before the semi-colon
            sql = sql_text.rstrip()[:-1] + option_clause + ";"
        else:
            sql = sql_text.rstrip() + option_clause
        return sql

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
        query_params: list of bind values for 1..n params in sql
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
        query_option_label = self._get_query_option_label_identifier()
        if profile and self._option_clause_valid_for_sql_text(sql):
            sql = self._add_option_clause_to_sql_text(
                sql, {"LABEL": query_option_label}
            )

        self._open_cursor()
        try:
            self._execute_session_options(query_options, log_level=log_level)
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
            )
            if query_params:
                self._log_or_not(
                    "%s SQL parameters: %s" % (self._sql_engine_name, query_params),
                    log_level=log_level,
                )
                self._cursor.execute(sql, *query_params)
            else:
                self._cursor.execute(sql)
            if fetch_action == FETCH_ACTION_ALL:
                if as_dict:
                    columns = self._cursor_projection(self._cursor)
                    rows = [
                        self._cursor_row_to_dict(columns, _)
                        for _ in self._cursor.fetchall()
                    ]
                else:
                    # pyodbc gives us back a list of pyodbc.Row and not tuple, in most cases no-big-deal, but there is
                    # a possibility for code to be using isinstance(, (tuple, list)) so we stay safe and convert to tuple.
                    rows = [tuple(_) for _ in self._cursor.fetchall()]
            elif fetch_action == FETCH_ACTION_ONE:
                row = self._cursor.fetchone()
                if as_dict:
                    rows = (
                        self._cursor_row_to_dict(
                            self._cursor_projection(self._cursor), row
                        )
                        if row
                        else row
                    )
                else:
                    # pyodbc gives us back a pyodbc.Row and not a tuple, in practice no-big-deal but
                    # upsets a number of unit tests.
                    rows = tuple(row) if row else row
            else:
                rows = None

            if profile and self._option_clause_valid_for_sql_text(sql):
                self._log(
                    self._get_query_profile(query_identifier=query_option_label),
                    detail=VVERBOSE,
                )
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

    def _execute_sqls(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        return_list = []
        sqls = [sql] if isinstance(sql, str) else sql
        self._open_cursor()
        session_options = self._execute_session_options(
            query_options, log_level=log_level
        )
        if session_options:
            return_list.append(session_options)
        try:
            for i, run_sql in enumerate(sqls):
                query_option_label = self._get_query_option_label_identifier()
                if profile and self._option_clause_valid_for_sql_text(run_sql):
                    run_sql = self._add_option_clause_to_sql_text(
                        run_sql, {"LABEL": query_option_label}
                    )
                self._log_or_not(
                    "%s SQL: %s" % (self._sql_engine_name, run_sql),
                    log_level=log_level,
                    no_log_items=no_log_items,
                )
                return_list.append(run_sql)
                if not self._dry_run:
                    self._cursor.execute(run_sql)
                    if profile and self._option_clause_valid_for_sql_text(run_sql):
                        self._log(
                            self._get_query_profile(
                                query_identifier=query_option_label
                            ),
                            detail=VVERBOSE,
                        )
        finally:
            self._close_cursor()
        return return_list

    @staticmethod
    def _fixed_session_parameters():
        """No fixed session parameters currently required for Synapse"""
        return {}

    @staticmethod
    def _format_query_options(query_options=None):
        """Format SET commands for Synapse.
        query_options: key/value pairs for session settings. E.g.
        """
        if not query_options:
            return []
        assert isinstance(query_options, dict)
        return ["SET {} {}".format(k, v) for k, v in query_options.items()]

    def _gen_max_column_values_sql(
        self,
        db_name,
        table_name,
        column_name_list,
        filter_clause=None,
        columns_to_cast_to_string=None,
    ):
        """Produce a SQL statement that selects a MAX row from the underlying table.
        See docstring on BackendApi for more details.
        """

        def add_sql_cast(col_name, col_expr=None, add_alias=False):
            col_expr = col_expr or col_name
            if col_name in (columns_to_cast_to_string or []):
                cast_str = "CAST(%s AS %s)" % (
                    col_expr,
                    self.generic_string_data_type(),
                )
            else:
                cast_str = col_expr
            if add_alias:
                cast_str += " AS %s" % col_name
            return cast_str

        assert column_name_list and isinstance(column_name_list, list)

        self._debug("Generate max SQL for columns: %s" % column_name_list)

        if len(column_name_list) == 1:
            # For a single column nothing out performs max()
            proj_col = add_sql_cast(
                column_name_list[0],
                col_expr="MAX(%s)" % self.enclose_identifier(column_name_list[0]),
            )
            sql = "SELECT %s FROM %s" % (
                proj_col,
                self.enclose_object_reference(db_name, table_name),
            )
            if filter_clause:
                sql += " WHERE " + filter_clause
        else:
            # Analytic row_number() function tests very slow in Impala, group by method below much faster.
            # max() fastest but not multi column on supported backends at time of implementation.
            cols = ",".join(column_name_list)
            proj_cols = ",".join(
                [add_sql_cast(_, add_alias=True) for _ in column_name_list]
            )
            order_by = ",".join([_ + " DESC" for _ in column_name_list])
            where = (" WHERE " + filter_clause) if filter_clause else ""
            sql = (
                "SELECT %s FROM (SELECT TOP(1) %s FROM %s%s GROUP BY %s ORDER BY %s) v"
                % (
                    proj_cols,
                    cols,
                    self.enclose_object_reference(db_name, table_name),
                    where,
                    cols,
                    order_by,
                )
            )
        return sql

    def _gen_sample_stats_sql_sample_clause(
        self, db_name, table_name, sample_perc=None
    ):
        assert db_name and table_name
        if (
            sample_perc is not None
            and sample_perc >= 0
            and self.query_sample_clause_supported()
            and not self.is_view(db_name, table_name)
        ):
            return "TABLESAMPLE (%s)" % sample_perc
        else:
            return ""

    def _get_query_option_label_identifier(self):
        """Return a unique identifier for identifying a SQL statement"""
        return "GOE (%s)" % id_generator(16)

    def _get_query_profile(self, query_identifier=None):
        """Build and return a str payload from _get_query_profile_tuples()"""
        profile = []
        for label, payload in self._get_query_profile_tuples(
            query_label=query_identifier
        ):
            if payload:
                profile.extend([label, payload])
            else:
                profile.append(label)
        return "\n".join(profile)

    def _get_query_profile_tuples(self, query_label=None, request_id=None):
        """We build up the profile information using separate calls to DMVs.
        Need one of either query_label or request_id.
        """

        def get_requests_information_by_label(label):
            sql = dedent(
                """\
                        SELECT TOP(1) *
                        FROM   sys.dm_pdw_exec_requests
                        WHERE  "label" = ?"""
            )
            self._cursor.execute(sql, [label])
            return self._cursor.fetchone()

        def get_requests_information_by_request_id(request_id):
            sql = dedent(
                """\
                        SELECT TOP(1) *
                        FROM   sys.dm_pdw_exec_requests
                        WHERE  request_id = ?"""
            )
            self._cursor.execute(sql, [request_id])
            return self._cursor.fetchone()

        def get_request_steps_information(request_id):
            sql = dedent(
                """\
                        SELECT operation_type
                        ,      "status"
                        ,      error_id
                        ,      MIN(start_time)          AS min_start_time
                        ,      MAX(end_time)            AS max_start_time
                        ,      SUM(total_elapsed_time)  AS sum_total_elapsed_time
                        ,      MIN(row_count)           AS min_row_count
                        ,      MAX(row_count)           AS max_row_count
                        FROM   sys.dm_pdw_request_steps
                        WHERE  request_id = ?
                        GROUP  BY
                               operation_type
                        ,      "status"
                        ,      error_id
                        ORDER  BY
                               min(start_time)"""
            )
            self._cursor.execute(sql, [request_id])
            return self._cursor.fetchall()

        def get_dms_information(request_id):
            sql = dedent(
                """\
                        SELECT type
                        ,      "status"
                        ,      error_id
                        ,      min(start_time)          AS min_start_time
                        ,      max(end_time)            AS max_start_time
                        ,      SUM(bytes_processed)     AS sum_bytes_processed
                        ,      SUM(rows_processed)      AS sum_rows_processed
                        ,      SUM(total_elapsed_time)  AS sum_total_elapsed_time
                        ,      SUM(cpu_time)            AS sum_cpu_time
                        FROM   sys.dm_pdw_dms_workers
                        WHERE  request_id = ?
                        GROUP  BY
                               type
                        ,      "status"
                        ,      error_id
                        ORDER  BY
                               min(start_time)"""
            )
            self._cursor.execute(sql, [request_id])
            return self._cursor.fetchall()

        def str_fn(x):
            return x if isinstance(x, str) else str(x)

        def list_for_logging(rows):
            assert self._cursor
            stats = []
            profile_keys = [_[0] for _ in self._cursor.description]
            for row in rows:
                stats.extend(zip(profile_keys, [str_fn(_) for _ in row]))
            if stats:
                stats.insert(0, ("Statistic", "Value"))
                return format_list_for_logging(stats)
            else:
                return ""

        assert query_label or request_id

        profile = []

        self._open_cursor()
        if query_label:
            self._log(
                "Fetching query profile for label: %s" % query_label, detail=VVERBOSE
            )
            requests_row = get_requests_information_by_label(query_label)
        else:
            self._log(
                "Fetching query profile for request: %s" % request_id, detail=VVERBOSE
            )
            requests_row = get_requests_information_by_request_id(request_id)

        if not requests_row:
            self._log("Request information not found", detail=VVERBOSE)
            return []
        else:
            request_id = requests_row.request_id

            profile.append(("Request Information", list_for_logging([requests_row])))

            request_steps_rows = get_request_steps_information(request_id)
            if not request_steps_rows:
                self._log("Request Steps information not found", detail=VVERBOSE)
                profile.append(("Request Steps information not found", None))
            else:
                profile.append(
                    ("Request Steps Information", list_for_logging(request_steps_rows))
                )

            # We're finding that the DMS queries take longer than most DML/DDL statements and therefore
            # have disabled them for the time being. This was done as part of PR for Incremental Update (GOE-2194).
            # dms_rows = get_dms_information(request_id)
            # if not dms_rows:
            #     self._log('DMS information not found', detail=VVERBOSE)
            #     profile.append(('DMS information not found', None))
            # else:
            #     profile.append(('DMS Information', list_for_logging(dms_rows)))

        return profile

    def _invalid_identifier_character_re(self):
        return SYNAPSE_INVALID_IDENTIFIER_CHARS_RE

    def _max_identifier_length(self):
        return 128

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
            "{} {}".format(self.enclose_identifier(col_name), col_type)
            for col_name, col_type in column_tuples
        )
        sqls = [
            "ALTER TABLE %s ADD %s"
            % (self.enclose_object_reference(db_name, table_name), column_clause)
        ]
        sqls.extend(
            [
                self._create_statistics_sql(db_name, table_name, col_name)
                for col_name, _ in column_tuples
            ]
        )
        return self.execute_ddl(sqls)

    def backend_report_info(self):
        """Reporting information about the type of backend and how to display
        its configuration details (e.g. Synapse host:port).
        """
        return {
            REPORT_ATTR_BACKEND_CLASS: "Azure",
            REPORT_ATTR_BACKEND_TYPE: self._backend_type,
            REPORT_ATTR_BACKEND_DISPLAY_NAME: self.backend_db_name(),
            REPORT_ATTR_BACKEND_HOST_INFO_TYPE: "Database",
            REPORT_ATTR_BACKEND_HOST_INFO: self._synapse_database,
        }

    def backend_version(self):
        sql = "SELECT @@VERSION"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return row[0] if row else None

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

        if self.role_exists(orchestration_options.synapse_role):
            results.append(
                {
                    CONNECT_TEST: "Role",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "Role exists: %s"
                    % orchestration_options.synapse_role,
                }
            )
        else:
            results.append(
                {
                    CONNECT_TEST: "Role",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Role does not exist: %s"
                    % orchestration_options.synapse_role,
                }
            )

        if self.synapse_external_data_source_exists(
            orchestration_options.synapse_data_source
        ):
            results.append(
                {
                    CONNECT_TEST: "Data source",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "Data source exists: %s"
                    % orchestration_options.synapse_data_source,
                }
            )
        else:
            results.append(
                {
                    CONNECT_TEST: "Data source",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Data source does not exist: %s"
                    % orchestration_options.synapse_data_source,
                }
            )

        if self.synapse_file_format_exists(orchestration_options.synapse_file_format):
            results.append(
                {
                    CONNECT_TEST: "File format",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "File format exists: %s"
                    % orchestration_options.synapse_file_format,
                }
            )
        else:
            results.append(
                {
                    CONNECT_TEST: "File format",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "File format does not exist or is not a supported format type: %s"
                    % orchestration_options.synapse_file_format,
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
        """Synapse has statistics objects on each column that we must maintain.
        If for_columns is True we will create the statistics objects on each column.
        If for_columns is None/False we will update the statistics on the entire table.
        We always use a fixed 5% sample for now. We may turn --sample-stats into an offload/present
        option in the future to allow the sample size to be parameterised (GOE-2133).
        """
        if not self.table_stats_compute_supported():
            return None
        _, col_stats = self.get_table_stats(db_name, table_name)

        sqls = []
        sample_pct = 5
        if for_columns:
            # get_columns returns columns ordered by column position to we can enumerate this
            for col in self.get_columns(db_name, table_name):
                sqls.append(self._create_statistics_sql(db_name, table_name, col.name))
        else:
            sqls.append(
                "UPDATE STATISTICS %s WITH SAMPLE %s PERCENT"
                % (self.enclose_object_reference(db_name, table_name), sample_pct)
            )
        return self.execute_ddl(sqls) if sqls else sqls

    def create_database(
        self, db_name, comment=None, properties=None, with_terminator=False
    ):
        """Create a Synapse schema which is a database in GOE terminology.
        properties: not applicable
        comment: not applicable
        """
        assert db_name
        if properties:
            assert isinstance(properties, dict)
        if comment:
            assert isinstance(comment, str)
        if self.database_exists(db_name):
            self._log(
                "Schema already exists, not attempting to create: %s" % db_name,
                detail=VVERBOSE,
            )
        sql = "CREATE SCHEMA %s" % self.enclose_identifier(db_name)
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
        """Create an Azure Synapse SQL table
        See abstract method for more description
        """

        def add_colation(column):
            if (
                column.is_string_based()
                and not external
                and self._connection_options.synapse_collation
            ):
                new_column = column.clone()
                new_column.collation = self._connection_options.synapse_collation
                return new_column
            else:
                return column

        column_list_with_collations = [add_colation(_) for _ in column_list]
        sql = self._create_table_sql_text(
            db_name,
            table_name,
            column_list_with_collations,
            partition_column_names,
            location=location,
            external=external,
            table_properties=table_properties,
            sort_column_names=sort_column_names,
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
        """Create a Synapse view
        See create_view() description on interface for parameter descriptions.
        SQL below does not use enclose_object_reference() due to:
            Parse error at line: 1, column: 13:
            VIEW does not allow a database name to be specified. (103021) (SQLExecDirectW)')
        """
        projection = self._format_select_projection(column_tuples)
        where_clause = (
            "\nWHERE  " + "\nAND    ".join(filter_clauses) if filter_clauses else ""
        )
        sql = (
            dedent(
                """\
                CREATE VIEW %(db)s.%(view)s AS
                SELECT %(projection)s
                FROM   %(from_tables)s%(where_clause)s"""
            )
            % {
                "db": self.enclose_identifier(db_name),
                "view": self.enclose_identifier(view_name),
                "projection": projection,
                "from_tables": ansi_joined_tables,
                "where_clause": where_clause,
            }
        )
        return self.execute_ddl(sql, sync=sync)

    def current_date_sql_expression(self):
        return "CAST(GETDATE() AS DATE)"

    def data_type_accepts_length(self, data_type):
        return bool(
            data_type
            in [
                SYNAPSE_TYPE_DECIMAL,
                SYNAPSE_TYPE_NUMERIC,
                SYNAPSE_TYPE_CHAR,
                SYNAPSE_TYPE_NCHAR,
                SYNAPSE_TYPE_VARCHAR,
                SYNAPSE_TYPE_NVARCHAR,
                SYNAPSE_TYPE_BINARY,
                SYNAPSE_TYPE_VARBINARY,
            ]
        )

    def database_exists(self, db_name):
        return bool(self.list_databases(db_name_filter=db_name))

    def default_date_based_partition_granularity(self):
        return None

    def default_storage_compression(self, user_requested_codec, user_requested_format):
        if user_requested_codec in ["HIGH", "MED", "NONE", None]:
            return None
        return user_requested_codec

    @staticmethod
    def default_storage_format():
        """Storage format out of our control"""
        return None

    def detect_column_has_fractional_seconds(self, db_name, table_name, column):
        assert db_name and table_name
        assert isinstance(column, SynapseColumn)
        if not column.has_time_element():
            return False
        sql = (
            dedent(
                """\
                SELECT TOP(1) %(col)s
                FROM %(db_table)s
                WHERE DATEPART(ns, %(col)s) != 0"""
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
        """Drop a table, purge parameter is ignored on Synapse.
        IF EXISTS clause is not valid on Azure Synapse
        See abstract method for more description
        """
        assert db_name and table_name
        if if_exists and not self.table_exists(db_name, table_name):
            # Nothing to do
            return []
        drop_sql = "DROP TABLE %s" % self.enclose_object_reference(db_name, table_name)
        return self.execute_ddl(drop_sql, sync=sync)

    def drop_view(self, db_name, view_name, if_exists=True, sync=None):
        """Drop a view, purge parameter is ignored on Synapse.
        See abstract method for more description
        SQL below does not use enclose_object_reference() due to:
            Parse error at line: 1, column: 21:
            DROP does not allow a database name to be specified. (103021) (SQLExecDirectW)')
        """
        assert db_name and view_name
        if_exists_clause = "IF EXISTS " if if_exists else ""
        drop_sql = "DROP VIEW %s%s.%s" % (
            if_exists_clause,
            self.enclose_identifier(db_name),
            self.enclose_identifier(view_name),
        )
        return self.execute_ddl(drop_sql, sync=sync)

    def enclose_identifier(self, identifier):
        if identifier is None:
            return None
        return f"[{identifier}]"

    def enclose_object_reference(self, db_name, object_name):
        assert db_name and object_name
        return ".".join(
            [
                self.enclose_identifier(self._synapse_database),
                self.enclose_identifier(db_name),
                self.enclose_identifier(object_name),
            ]
        )

    def enclosure_character(self):
        """Can be enclosed in double quotation marks (") or brackets ([ ]).
        Using a single enclosure character (") for this method but hopefully it is seldom used.
        """
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

    def extract_date_part_sql_expression(self, date_part, column):
        """Return a SQL expression that can be used to extract DAY, MONTH or YEAR from a column value.
        column: Can be a column object or a column name.
        """
        assert isinstance(date_part, str)
        assert date_part.upper() in ("DAY", "MONTH", "YEAR")
        assert isinstance(column, (str, SynapseColumn))
        column_name = column if isinstance(column, str) else column.name
        return f"{date_part.upper()}({column_name})"

    def format_column_comparison(
        self, left_col, operator, right_col, left_alias=None, right_alias=None
    ):
        """Format a simple 'column operator column' string for Synapse"""
        assert isinstance(left_col, SynapseColumn)
        assert isinstance(right_col, SynapseColumn)
        left_identifier = self.enclose_identifier(left_col.name)
        if left_alias:
            left_identifier = "{}.{}".format(
                self.enclose_identifier(left_alias), left_identifier
            )
        right_identifier = self.enclose_identifier(right_col.name)
        if right_alias:
            right_identifier = "{}.{}".format(
                self.enclose_identifier(right_alias), right_identifier
            )
        if left_col.collation or right_col.collation:
            # The columns are collation sensitive so ensure all comparisons are in SYNAPSE_COLLATION.
            return "{}{} {} {}{}".format(
                left_identifier,
                self._collation_clause(self._connection_options.synapse_collation),
                operator,
                right_identifier,
                self._collation_clause(self._connection_options.synapse_collation),
            )
        else:
            return "{} {} {}".format(left_identifier, operator, right_identifier)

    def format_query_parameter(self, param_name):
        """No named parameters so always return "?" """
        return "?"

    def gen_column_object(self, column_name, **kwargs):
        return SynapseColumn(column_name, **kwargs)

    def gen_copy_into_sql_text(
        self,
        db_name,
        table_name,
        from_object_clause,
        select_expr_tuples,
        filter_clauses=None,
    ):
        raise NotImplementedError("Not implemented for prototype")

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
        Ignoring storage_format on Synapse.
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
        limit_clause = "TOP({}) ".format(row_limit) if row_limit is not None else ""
        sql = (
            dedent(
                """\
                    CREATE TABLE %(db_table)s
                    WITH (DISTRIBUTION = ROUND_ROBIN)
                    AS
                    SELECT %(limit_clause)s%(projection)s%(from_clause)s"""
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
        return SynapseColumn(
            column_name,
            data_type=SYNAPSE_TYPE_NUMERIC,
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
        """Format INSERT from SELECT statement for Synapse
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

        projected_expressions = select_expr_tuples + (partition_expr_tuples or [])
        projection = self._format_select_projection(projected_expressions)
        from_db_table = from_object_override or self.enclose_object_reference(
            from_db_name, from_table_name
        )

        where_clause = ""
        if filter_clauses:
            where_clause = "\nWHERE  " + "\nAND    ".join(filter_clauses)

        insert_sql = """INSERT INTO %(db_table)s
SELECT %(proj)s
FROM   %(from_db_table)s%(where)s""" % {
            "db_table": self.enclose_object_reference(db_name, table_name),
            "proj": projection,
            "from_db_table": from_db_table,
            "where": where_clause,
        }
        return insert_sql

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
        return SYNAPSE_TYPE_VARCHAR

    def get_columns(self, db_name, table_name):
        assert db_name and table_name

        backend_columns = []

        sql = dedent(
            """\
                    SELECT column_name, data_type, is_nullable, character_maximum_length, character_octet_length
                    ,      numeric_precision, numeric_scale, datetime_precision, collation_name
                    FROM   INFORMATION_SCHEMA.COLUMNS
                    WHERE  table_catalog = ? AND table_schema = ? AND table_name = ?
                    ORDER BY ordinal_position"""
        )
        col_rows = self.execute_query_fetch_all(
            sql,
            log_level=VVERBOSE,
            query_params=[self._synapse_database, db_name, table_name],
        )
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
                collation,
            ) = col
            # Date is stored as 0
            if time_scale is not None and time_scale > 0:
                data_scale = time_scale
            else:
                data_scale = num_scale
            # max is stored as -1
            if byte_length == -1:
                byte_length = None
            backend_columns.append(
                SynapseColumn(
                    col_name,
                    data_type,
                    data_length=byte_length,
                    char_length=char_length,
                    data_precision=num_precision,
                    data_scale=data_scale,
                    nullable=is_nullable,
                    collation=collation,
                )
            )
        return backend_columns

    def get_column_statistics(self, db_name, table_name):
        assert db_name and table_name
        # get a list of column name, statistics name and max length for a table
        sql = dedent(
            """
                    SELECT  co.name,
                            sc.name,
                            co.max_length
                    FROM    sys.columns co
                            LEFT OUTER JOIN
                                (
                                SELECT  st.name,
                                        st.object_id,
                                        sc.column_id
                                FROM    sys.stats st,
                                        sys.stats_columns sc
                                WHERE   st.object_id = sc.object_id
                                AND     st.stats_id = sc.stats_id
                                AND     st.user_created = 1
                                AND     st.object_id = OBJECT_ID(?)
                                ) sc
                            ON  co.column_id = sc.column_id
                            AND co.object_id = sc.object_id
                    WHERE   co.object_id = OBJECT_ID(?)
                    ORDER BY
                            co.column_id"""
        )
        object_id = "%s.%s" % (db_name, table_name)
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=[object_id, object_id]
        )
        return rows

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
                ("CONVERT(varchar, %s)" % col)
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
        """Table partitioning not implemented at this stage on Synapse"""
        return []

    def get_session_option(self, option_name):
        """Not implemented on Synapse
        SESSIONPROPERTY() looks like the right function to achieve this but on Synapse:
            'SESSION_PROPERTY' is not a recognized built-in function name.
        """
        raise NotImplementedError("get_session_option not supported for Synapse")

    def get_table_ddl(
        self, db_name, table_name, as_list=False, terminate_sql=False, for_replace=False
    ):
        """Mock up Synapse table DDL based on how GOE code would create a table.
        This is not ideal but Synapse doesn;t have a readily available DDL retrieval method. What this code
        does is derive the inputs as they should have been at table creation time and feeds them back through
        the same code. The flaw is, if we change our code generator we'll get a different output from this code.
        This is not widely used (at the time of writing) so should be sufficient.
        """

        def check_external():
            sql = dedent(
                """\
                    SELECT  t.is_external
                    FROM    sys.tables t
                    WHERE   t.object_id = OBJECT_ID(?)"""
            )
            row = self.execute_query_fetch_one(
                sql, log_level=VVERBOSE, query_params=[f"{db_name}.{table_name}"]
            )
            return bool(row and row[0])

        def get_external_table_options():
            external_table_options = None
            sql = dedent(
                """\
                    SELECT  ds.name, ff.name
                    FROM    sys.external_tables t,
                            sys.external_data_sources ds,
                            sys.external_file_formats ff
                    WHERE   t.data_source_id = ds.data_source_id
                    AND     t.file_format_id = ff.file_format_id
                    AND     t.object_id = OBJECT_ID(?)"""
            )
            row = self.execute_query_fetch_one(
                sql, log_level=VVERBOSE, query_params=[f"{db_name}.{table_name}"]
            )
            if row:
                external_table_options = {
                    "DATA_SOURCE": self.enclose_identifier(row[0]),
                    "FILE_FORMAT": self.enclose_identifier(row[1]),
                }
            return external_table_options

        assert db_name and table_name
        self._log("Fetching table DDL: %s.%s" % (db_name, table_name), detail=VVERBOSE)
        column_list = self.get_columns(db_name, table_name)
        is_external = check_external()
        if is_external:
            external_table_location = self.get_table_location(db_name, table_name)
            external_table_properties = get_external_table_options()
        else:
            external_table_location = None
            external_table_properties = None

        ddl_str = self._create_table_sql_text(
            db_name,
            table_name,
            column_list,
            None,
            location=external_table_location,
            external=is_external,
            table_properties=external_table_properties,
            sort_column_names=self.get_table_sort_columns(db_name, table_name),
        )
        if not ddl_str:
            raise BackendApiException(
                "Table does not exist for DDL retrieval: %s.%s" % (db_name, table_name)
            )
        self._debug("Table DDL: %s" % ddl_str)
        if terminate_sql:
            ddl_str += ";"
        if as_list:
            return ddl_str.split("\n")
        else:
            return ddl_str

    def get_table_location(self, db_name, table_name):
        sql = dedent(
            """\
                    SELECT  CONCAT(ds.location, '/', t.location)
                    FROM    sys.schemas s,
                            sys.external_tables t,
                            sys.external_data_sources ds
                    WHERE   s.schema_id = t.schema_id
                    AND     t.data_source_id = ds.data_source_id
                    AND     s.name = ?
                    AND     t.name = ?"""
        )
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=[db_name, table_name]
        )
        return row[0] if row else None

    def get_table_partition_count(self, db_name, table_name):
        """Table partitioning not implemented at this stage on Synapse"""
        return 0

    def get_table_partitions(self, db_name, table_name):
        """Table partitioning not implemented at this stage on Synapse"""
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
            log_level=VVERBOSE,
            time_sql=True,
            not_when_dry_running=not_when_dry_running,
        )
        return row[0] if row else None

    def get_table_row_count_from_metadata(self, db_name, table_name):
        assert db_name and table_name
        _, row_count = self.get_table_size_and_row_count(db_name, table_name)
        return row_count

    def get_table_size(self, db_name, table_name, no_cache=False):
        # no_cache is ignored on Synapse because we use SQL to get the size.
        assert db_name and table_name
        size_bytes, _ = self.get_table_size_and_row_count(db_name, table_name)
        return size_bytes

    def get_table_size_and_row_count(self, db_name, table_name):
        assert db_name and table_name
        sql = "EXEC sp_spaceused ?"
        row = self.execute_query_fetch_one(
            sql,
            query_params=[self.enclose_object_reference(db_name, table_name)],
            log_level=VVERBOSE,
        )
        if row:
            row_count = row[sp_spaceused.rows.value].strip()
            table_size = human_size_to_bytes(
                row[sp_spaceused.reserved.value].strip().replace(" ", "")
            )
            return (int(table_size), int(row_count))
        else:
            return (None, None)

    def get_table_sort_columns(self, db_name, table_name, as_csv=True):
        """Synapse can have
            a) CLUSTERED COLUMNSTORE INDEX ORDER (column [,...n])
            b) CLUSTERED INDEX ( { index_column_name [ ASC | DESC ] } [ ,...n ] ) -- default is ASC
        This should cater for them both
        """
        assert db_name and table_name
        sql = dedent(
            """\
                    SELECT  c.name
                    FROM    sys.columns c,
                            sys.index_columns ic,
                            sys.schemas s,
                            sys.tables t
                    WHERE   s.schema_id = t.schema_id
                    AND     t.object_id = c.object_id
                    AND     c.object_id = ic.object_id
                    AND     c.column_id = ic.column_id
                    AND     (ic.column_store_order_ordinal > 0
                    OR      ic.key_ordinal > 0)
                    AND     s.name = ?
                    AND     t.name = ?
                    ORDER BY CASE WHEN ic.column_store_order_ordinal > 0 THEN ic.column_store_order_ordinal ELSE ic.key_ordinal END"""
        )
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=[db_name, table_name]
        )
        if not rows:
            return []
        cluster_cols = [row[0] for row in rows]
        column_names = set(self.get_column_names(db_name, table_name))
        cluster_columns = [_ for _ in cluster_cols if _ in column_names]
        return ",".join(cluster_columns) if as_csv else cluster_columns

    def get_table_stats(self, db_name, table_name, as_dict=False):
        """For historic reasons this is expected to return two objects:
            (table_stats, column_stats)
            table_stats: num_rows, num_bytes, avg_row_len
            column_stats: col_name, ndv, num_nulls, avg_col_len, low_value, high_value, max_col_len
        See HiveTableStats class for more details

        Note SS@2021-09-20: It's possible to get the num_nulls, low_value and high_value with a call
            to DBCC SHOW_STATISTICS (?, ?) WITH HISTOGRAM. However this has not been implemented due
            to the fact that the result set would be equal to the number of rows for a unique column.
            We only need the min and the max from this result set but there is no way to do this
            in the backend and we don't want to return potentially millions of rows per column.
        """
        stats = []
        column_stats = self.get_column_statistics(db_name, table_name)

        if all([_[1] for _ in column_stats]):
            # Every column has a statistics object that we can query
            for x, col_stat_name in enumerate(column_stats):
                stats_row = self.execute_query_fetch_one(
                    sql="DBCC SHOW_STATISTICS (?, ?) WITH STAT_HEADER",
                    query_params=["%s.%s" % (db_name, table_name), col_stat_name[1]],
                    log_level=VVERBOSE,
                )
                if x == 0:
                    # The statistics for each column should have same num_rows so store the first only
                    stats.append(
                        stats_row[dbcc_showstatistics_stat_header.rows.value]
                        if stats_row[dbcc_showstatistics_stat_header.rows.value]
                        else 0
                    )
                if stats_row:
                    stats.extend(
                        [
                            col_stat_name[0],  # name
                            (
                                int(
                                    1
                                    / stats_row[
                                        dbcc_showstatistics_stat_header.density.value
                                    ]
                                )
                                if stats_row[
                                    dbcc_showstatistics_stat_header.density.value
                                ]
                                else 0
                            ),  # ndv
                            None,  # num_nulls
                            (
                                int(
                                    stats_row[
                                        dbcc_showstatistics_stat_header.average_key_length.value
                                    ]
                                )
                                if stats_row[
                                    dbcc_showstatistics_stat_header.average_key_length.value
                                ]
                                else 0
                            ),  # avg_col_len
                            None,  # low_value
                            None,  # high_value
                            col_stat_name[2],  # max_col_len
                        ]
                    )
                else:
                    stats.extend(col_stat_name[0] + [0] * 6)
        else:
            stats.append(-1)
            for c in column_stats:
                stats.append(c[0])
                stats.extend([0] * 6)

        if stats:
            tab_stats, col_stats = parse_stats_into_tab_col(stats)
            if not as_dict:
                tab_stats, col_stats = transform_stats_as_tuples(
                    tab_stats, col_stats, self.get_column_names(db_name, table_name)
                )
        else:
            tab_stats = (
                EMPTY_BACKEND_TABLE_STATS_DICT
                if as_dict
                else EMPTY_BACKEND_TABLE_STATS_LIST
            )
            col_stats = (
                EMPTY_BACKEND_COLUMN_STATS_DICT
                if as_dict
                else EMPTY_BACKEND_COLUMN_STATS_LIST
            )
        return tab_stats, col_stats

    def get_table_and_partition_stats(self, db_name, table_name, as_dict=False):
        tab_stats, _ = self.get_table_stats(db_name, table_name, as_dict=as_dict)
        if as_dict:
            return tab_stats, {}, EMPTY_BACKEND_COLUMN_STATS_DICT
        else:
            return tab_stats, [], EMPTY_BACKEND_COLUMN_STATS_LIST

    def get_table_stats_partitions(self, db_name, table_name):
        raise NotImplementedError(
            "get_table_stats_partitions not supported for Synapse"
        )

    def get_user_name(self):
        sql = "SELECT SYSTEM_USER"
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
        """Insert rows into Synapse table using INSERT ... VALUES. Only suitable for low volumes.
        Cannot add any CASTs when using INSERT ... VALUES. Statements fail with:
            Msg 104334, Level 16, State 1, Line 1
            Insert values statement can contain only constant literal values or variable references.
        Also, even though MSSQL supports multi-row INSERT ... VALUES statements, Synapse does not:
            https://docs.microsoft.com/en-us/sql/t-sql/statements/insert-transact-sql?view=sql-server-ver15#b-inserting-multiple-rows-of-data
            Note: The table value constructor is not supported in Azure Synapse Analytics.
        For this reason we insert a UNION ALL select statement, like we do on Hive.
        """

        def gen_literal(py_val, data_type):
            return str(self.to_backend_literal(py_val, data_type))

        def insert_from_union_all_select(literal_csv_list):
            join_str = "\nUNION ALL\n" if split_by_cr else " UNION ALL "
            insert_template = "INSERT INTO %s " % (
                self.enclose_object_reference(db_name, table_name)
            )
            select_statements = ["SELECT {}".format(_) for _ in literal_csv_list]
            return insert_template + "\n" + join_str.join(select_statements)

        assert db_name
        assert table_name
        assert literal_list and isinstance(literal_list, list)
        assert isinstance(literal_list[0], list)

        if column_list:
            assert valid_column_list(column_list), (
                "Incorrectly formed column_list: %s" % column_list
            )

        cmds = []
        remaining_rows = literal_list[:]
        while remaining_rows:
            this_chunk = remaining_rows[:max_rows_per_insert]
            remaining_rows = remaining_rows[max_rows_per_insert:]
            formatted_rows = []
            for row in this_chunk:
                formatted_rows.append(
                    ",".join(
                        gen_literal(py_val, col.data_type)
                        for py_val, col in zip(row, column_list)
                    )
                )
            sql = insert_from_union_all_select(formatted_rows)
            cmds.extend(self.execute_dml(sql, log_level=VVERBOSE))
        return cmds

    def is_nan_sql_expression(self, column_expr):
        """If we reach here something has gone wrong so raise an exception"""
        raise NotImplementedError("is_nan_sql_expression not supported for Synapse")

    def is_valid_partitioning_data_type(self, data_type):
        """Table partitioning not implemented at this stage on Synapse"""
        return False

    def is_valid_sort_data_type(self, data_type):
        return bool(
            data_type
            not in [
                SYNAPSE_TYPE_CHAR,
                SYNAPSE_TYPE_VARCHAR,
                SYNAPSE_TYPE_NCHAR,
                SYNAPSE_TYPE_NVARCHAR,
            ]
        )

    def is_valid_storage_compression(self, user_requested_codec, user_requested_format):
        return bool(user_requested_codec in ["NONE", None])

    def is_valid_storage_format(self, storage_format):
        return bool(storage_format is None)

    def is_view(self, db_name, object_name):
        return self.view_exists(db_name, object_name)

    def length_sql_expression(self, column_expression):
        assert column_expression
        return "LEN(%s)" % column_expression

    def list_databases(self, db_name_filter=None, case_sensitive=True):
        """Case-sensitivity of the LIKE function in Synapse is determined by the case sensitivity of
        the collation of the database. So for a case-insensitive database there can only be one
        database name and it will either be stored in lower or upper case, and LIKE will return it
        regardless of the case of the literal passed in to the LIKE function. For a case-sensitive
        database there can be two databases with the same name and different case. The LIKE function
        will only match the case of the literal passed in.
        """
        query_params = [self._synapse_database]
        sql = dedent(
            """\
                    SELECT schema_name
                    FROM   INFORMATION_SCHEMA.SCHEMATA
                    WHERE  catalog_name = ?"""
        )
        if db_name_filter:
            sql += "\nAND    schema_name LIKE ?"
            query_params.append(db_name_filter.replace("*", "%"))
        sql += "\nORDER BY schema_name"
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        return [_[0] for _ in rows]

    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        """case_sensitive is handled by the database collation. See list_databases comment for why"""
        sql = dedent(
            """\
                    SELECT table_name
                    FROM   INFORMATION_SCHEMA.TABLES
                    WHERE  table_type = 'BASE TABLE'
                    AND    table_catalog = ?
                    AND    table_schema = ?"""
        )
        query_params = [self._synapse_database, db_name]
        if table_name_filter:
            sql += "\nAND table_name LIKE ?"
            query_params.append(table_name_filter.replace("*", "%"))
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        return [_[0] for _ in rows]

    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        raise NotImplementedError("list_udfs not supported for Synapse")

    def list_views(self, db_name, view_name_filter=None, case_sensitive=True):
        """case_sensitive is handled by the database collation. See list_databases comment for why"""
        sql = dedent(
            """\
                    SELECT table_name
                    FROM   INFORMATION_SCHEMA.TABLES
                    WHERE  table_type = 'View'
                    AND    table_catalog = ?
                    AND    table_schema = ?"""
        )
        query_params = [self._synapse_database, db_name]
        if view_name_filter:
            sql += "\nAND table_name LIKE ?"
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
        return 38

    def max_datetime_value(self):
        return datetime64("9999-12-31")

    def max_datetime_scale(self):
        """Synapse SQL supports accuracy of 100ns
        https://docs.microsoft.com/en-us/sql/t-sql/functions/date-and-time-data-types-and-functions-transact-sql?view=azure-sqldw-latest
        """
        return 7

    def max_partition_columns(self):
        """Table partitioning not implemented at this stage on Synapse"""
        return 0

    def max_sort_columns(self):
        """Returning SORT_COLUMNS_UNLIMITED as clustered columnstore indexes include all columns"""
        return SORT_COLUMNS_UNLIMITED

    def max_table_name_length(self):
        return 128

    def min_datetime_value(self):
        """This is used to understand whether the minimum data we might offload can cause issues in Synapse.
        We offload canonical DATE/TIMESTAMP to Synapse DATE/DATETIME2 so the value returned below is the minimum
        for those types and not Synapse DATETIME which has minimum 1753-01-01. Offload will never use Synapse
        DATETIME therefore this is fine.
        """
        return datetime64("0001-01-01")

    def native_integer_types(self):
        return [
            SYNAPSE_TYPE_BIGINT,
            SYNAPSE_TYPE_INT,
            SYNAPSE_TYPE_SMALLINT,
            SYNAPSE_TYPE_TINYINT,
        ]

    def _open_cursor(self):
        self._cursor = self._client.cursor() if self._client else None
        self._execute_global_session_parameters()

    def _option_clause_valid_for_sql_text(self, sql_text):
        """Return True if we support adding OPTION clause to sql_text.
        Ideally this is covered by the developer not asking for a profile when it doesn't make sense.
        But, in some cases, we may pass a list of SQLs where some are compatible and some are not.
        """
        if not sql_text:
            return False
        valid_sql_start_tokens = [
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE TABLE",
        ]
        return any(
            sql_text.lstrip().upper().startswith(_) for _ in valid_sql_start_tokens
        )

    def partition_column_requires_synthetic_column(self, backend_column, granularity):
        """Table partitioning not implemented at this stage on Synapse"""
        return False

    def populate_sequence_table(
        self, db_name, table_name, starting_seq, target_seq, split_by_cr=False
    ):
        raise NotImplementedError("Sequence table does not apply for Synapse")

    def refresh_table_files(self, db_name, table_name, sync=None):
        """No requirement to re-scan files for a table on Synapse but drop cache because that will be stale"""
        self.drop_state()

    def decimal_scale_validation_expression(self, column_name, column_scale):
        """Return a SQL expression to validate the scale length of string based staging columns holding decimal data.
        The expression ignores data in scientific notation because the scale in those literals bears no relation
        to the actual scale of the value.
        Uses instr/split type functions because Synapse does not support regular expressions!
        """
        return " AND ".join(
            [
                f"CHARINDEX('.',{column_name}) > 0",
                f"CHARINDEX('E',{column_name}) = 0",
                f"LEN(PARSENAME({column_name}, 1)) > {column_scale}",
            ]
        )

    def regexp_extract_decimal_scale_pattern(self):
        """Traditional regular expressions are not natively supported in Synapse SQL"""
        raise NotImplementedError(
            "regexp_extract_decimal_scale_pattern not supported for Synapse"
        )

    def regexp_extract_sql_expression(self, subject, pattern):
        """Traditional regular expressions are not natively supported in Synapse SQL"""
        raise NotImplementedError(
            "regexp_extract_sql_expression not supported for Synapse"
        )

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

        rename_sql = "RENAME OBJECT {} TO {}".format(
            self.enclose_object_reference(from_db_name, from_table_name),
            self.enclose_identifier(to_table_name),
        )

        executed_sqls = self.execute_ddl(rename_sql, sync=sync)
        return executed_sqls

    def role_exists(self, role_name):
        """Check the role exists, returns True/False"""
        assert role_name
        sql = dedent(
            """\
                    SELECT 1
                    FROM   sys.database_principals
                    WHERE  type = 'R'
                    AND    name = ?"""
        )
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=[role_name]
        )
        return bool(row)

    def sequence_table_max(self, db_name, table_name):
        raise NotImplementedError("Sequence table does not apply for Synapse")

    def set_column_stats(
        self, db_name, table_name, new_column_stats, ndv_cap, num_null_factor
    ):
        raise NotImplementedError("set_column_stats not supported for Synapse")

    def set_partition_stats(
        self, db_name, table_name, new_partition_stats, additive_stats
    ):
        raise NotImplementedError("set_partition_stats not supported for Synapse")

    def set_session_db(self, db_name, log_level=VERBOSE):
        # USE statement does not work on Synapse
        raise NotImplementedError("set_session_db not supported for Synapse")

    def set_table_stats(self, db_name, table_name, new_table_stats, additive_stats):
        raise NotImplementedError("set_table_stats not supported for Synapse")

    def supported_backend_data_types(self):
        return [
            SYNAPSE_TYPE_BIGINT,
            SYNAPSE_TYPE_DECIMAL,
            SYNAPSE_TYPE_FLOAT,
            SYNAPSE_TYPE_INT,
            SYNAPSE_TYPE_MONEY,
            SYNAPSE_TYPE_NUMERIC,
            SYNAPSE_TYPE_REAL,
            SYNAPSE_TYPE_SMALLINT,
            SYNAPSE_TYPE_SMALLMONEY,
            SYNAPSE_TYPE_TINYINT,
            SYNAPSE_TYPE_DATE,
            SYNAPSE_TYPE_DATETIME,
            SYNAPSE_TYPE_DATETIME2,
            SYNAPSE_TYPE_SMALLDATETIME,
            SYNAPSE_TYPE_DATETIMEOFFSET,
            SYNAPSE_TYPE_TIME,
            SYNAPSE_TYPE_CHAR,
            SYNAPSE_TYPE_VARCHAR,
            SYNAPSE_TYPE_NCHAR,
            SYNAPSE_TYPE_NVARCHAR,
            SYNAPSE_TYPE_BINARY,
            SYNAPSE_TYPE_UNIQUEIDENTIFIER,
            SYNAPSE_TYPE_VARBINARY,
        ]

    def supported_date_based_partition_granularities(self):
        """Table partitioning not implemented at this stage on Synapse"""
        return None

    def synapse_external_data_source_exists(self, external_data_source_name):
        """Check a Synapse external data source exists. This is Synapse only, hence synapse in the method name.
        It is public so that BackendTable can call it.
        """
        assert self._backend_type == DBTYPE_SYNAPSE
        sql = dedent(
            """\
                    SELECT 1
                    FROM   sys.external_data_sources
                    WHERE  name = ?"""
        )
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=[external_data_source_name]
        )
        return bool(row)

    def synapse_file_format_exists(self, file_format_name):
        """Check a Synapse file format of the correct staging file type exists. This is Synapse only, hence
        synapse in the method name. It is public so that BackendTable can call it.
        """
        assert self._backend_type == DBTYPE_SYNAPSE
        sql = dedent(
            """\
                    SELECT format_type
                    FROM   sys.external_file_formats
                    WHERE  name = ?"""
        )
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=[file_format_name]
        )
        return bool((row and row[0] in self.valid_staging_formats()))

    def table_distribution(self, db_name, table_name):
        sql = dedent(
            """\
                    SELECT distribution_policy_desc
                    FROM sys.pdw_table_distribution_properties
                    WHERE object_id = OBJECT_ID(?)"""
        )
        row = self.execute_query_fetch_one(
            sql, log_level=VVERBOSE, query_params=[f"{db_name}.{table_name}"]
        )
        return row[0] if row else row

    def table_exists(self, db_name: str, table_name: str) -> bool:
        sql = dedent(
            """\
                    SELECT table_name
                    FROM   INFORMATION_SCHEMA.TABLES
                    WHERE  table_type = 'BASE TABLE'
                    AND    table_catalog = ?
                    AND    table_schema = ?
                    AND    table_name = ?"""
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=VVERBOSE,
            query_params=[self._synapse_database, db_name, table_name],
        )
        return bool(row)

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
            sql = "SELECT CONVERT(VARCHAR(128), SERVERPROPERTY('productversion'))"
            row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
            self._target_version = row[0] if row else None
        return self._target_version

    def to_backend_literal(self, py_val, data_type=None):
        """Translate a Python value to a Synapse literal"""
        self._debug("Formatting %s literal: %s" % (type(py_val), repr(py_val)))
        self._debug("For backend datatype: %s" % data_type)
        new_py_val = SynapseLiteral.format_literal(py_val, data_type=data_type)
        self._debug("Final %s literal: %s" % (type(py_val), str(new_py_val)))
        return new_py_val

    def transform_encrypt_data_type(self):
        return SYNAPSE_TYPE_VARCHAR

    def transform_null_cast(self, backend_column):
        assert isinstance(backend_column, SynapseColumn)
        return "CONVERT(%s, NULL)" % (backend_column.format_data_type())

    def transform_tokenize_data_type(self):
        return SYNAPSE_TYPE_VARCHAR

    def transform_regexp_replace_expression(
        self, backend_column, regexp_replace_pattern, regexp_replace_string
    ):
        """Traditional regular expressions are not natively supported in Synapse SQL"""
        raise NotImplementedError("Translation function is not supported on Synapse")

    def transform_translate_expression(self, backend_column, from_string, to_string):
        return "TRANSLATE(%s, %s, %s)" % (
            self.enclose_identifier(backend_column.name),
            from_string,
            to_string,
        )

    def udf_installation_os(self, user_udf_version):
        raise NotImplementedError("UDFs are not supported for Synapse")

    def udf_installation_sql(self, create_udf_db, udf_db=None):
        raise NotImplementedError("UDFs are not supported for Synapse")

    def udf_installation_test(self, udf_db=None):
        raise NotImplementedError("UDFs are not supported for Synapse")

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, SynapseColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.is_number_based():
            if column.data_type in [SYNAPSE_TYPE_REAL, SYNAPSE_TYPE_FLOAT]:
                return bool(
                    target_type in [GOE_TYPE_DECIMAL, GOE_TYPE_DOUBLE, GOE_TYPE_FLOAT]
                )
            else:
                return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based():
            return bool(target_type in DATE_CANONICAL_TYPES)
        elif column.is_string_based():
            if column.data_type in [SYNAPSE_TYPE_CHAR, SYNAPSE_TYPE_NCHAR]:
                return bool(
                    target_type in STRING_CANONICAL_TYPES
                    or target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY]
                )
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
        elif column.data_type in [SYNAPSE_TYPE_BINARY, SYNAPSE_TYPE_VARBINARY]:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        elif column.data_type == SYNAPSE_TYPE_TIME:
            return bool(target_type == GOE_TYPE_TIME)
        return False

    def valid_staging_formats(self):
        return [FILE_STORAGE_FORMAT_PARQUET]

    def view_exists(self, db_name, view_name):
        sql = dedent(
            """\
                    SELECT table_name
                    FROM   INFORMATION_SCHEMA.TABLES
                    WHERE  table_type = 'VIEW'
                    AND    table_catalog = ?
                    AND    table_schema = ?
                    AND    table_name = ?"""
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=VVERBOSE,
            query_params=[self._synapse_database, db_name, view_name],
        )
        return bool(row)

    def to_canonical_column(self, column):
        """Translate a Synapse SQL column to an internal GOE column"""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
            char_length=None,
            char_semantics=None,
        ):
            """Wrapper that carries name, nullable & data_default forward from RDBMS
            Not carrying partition information forward to the canonical column because that will
            be defined by operational logic.
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return CanonicalColumn(
                col.name,
                data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                char_length=char_length,
                partition_info=None,
                char_semantics=char_semantics,
            )

        assert column
        assert isinstance(column, SynapseColumn)

        if column.data_type == SYNAPSE_TYPE_CHAR:
            return new_column(
                column,
                GOE_TYPE_FIXED_STRING,
                data_length=column.data_length,
                char_length=column.char_length,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
                safe_mapping=True,
            )
        elif column.data_type == SYNAPSE_TYPE_NCHAR:
            return new_column(
                column,
                GOE_TYPE_FIXED_STRING,
                data_length=column.data_length,
                char_length=column.char_length,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
                safe_mapping=True,
            )
        elif column.data_type == SYNAPSE_TYPE_VARCHAR:
            return new_column(
                column,
                GOE_TYPE_VARIABLE_STRING,
                data_length=column.data_length,
                char_length=column.char_length,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            )
        elif column.data_type == SYNAPSE_TYPE_NVARCHAR:
            return new_column(
                column,
                GOE_TYPE_VARIABLE_STRING,
                data_length=column.data_length,
                char_length=column.char_length,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            )
        elif column.data_type in (SYNAPSE_TYPE_BINARY, SYNAPSE_TYPE_VARBINARY):
            return new_column(column, GOE_TYPE_BINARY, data_length=column.data_length)
        elif column.data_type == SYNAPSE_TYPE_TINYINT:
            # GOE_TYPE_INTEGER_1 does not fit in TINYINT but TINYINT *does* fit in GOE_TYPE_INTEGER_1
            return new_column(column, GOE_TYPE_INTEGER_1)
        elif column.data_type == SYNAPSE_TYPE_SMALLINT:
            return new_column(column, GOE_TYPE_INTEGER_2)
        elif column.data_type == SYNAPSE_TYPE_INT:
            return new_column(column, GOE_TYPE_INTEGER_4)
        elif column.data_type == SYNAPSE_TYPE_BIGINT:
            return new_column(column, GOE_TYPE_INTEGER_8)
        elif column.data_type == SYNAPSE_TYPE_FLOAT:
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == SYNAPSE_TYPE_REAL:
            return new_column(column, GOE_TYPE_FLOAT)
        elif column.data_type == SYNAPSE_TYPE_MONEY:
            return new_column(
                column,
                GOE_TYPE_DECIMAL,
                data_precision=column.data_precision or 19,
                data_scale=column.data_scale or 4,
            )
        elif column.data_type == SYNAPSE_TYPE_SMALLMONEY:
            return new_column(
                column,
                GOE_TYPE_DECIMAL,
                data_precision=column.data_precision or 10,
                data_scale=column.data_scale or 4,
            )
        elif column.data_type in (SYNAPSE_TYPE_DECIMAL, SYNAPSE_TYPE_NUMERIC):
            data_precision = column.data_precision
            data_scale = column.data_scale
            if data_precision is not None and data_scale is not None:
                # Process a couple of edge cases
                if data_scale > data_precision:
                    # e.g. NUMBER(3,5) scale > precision
                    data_precision = data_scale
                elif data_scale < 0:
                    # e.g. NUMBER(10,-5)
                    data_scale = 0
            if data_scale == 0:
                # Integral numbers
                if data_precision >= 1 and data_precision <= 2:
                    integral_type = GOE_TYPE_INTEGER_1
                elif data_precision >= 3 and data_precision <= 4:
                    integral_type = GOE_TYPE_INTEGER_2
                elif data_precision >= 5 and data_precision <= 9:
                    integral_type = GOE_TYPE_INTEGER_4
                elif data_precision >= 10 and data_precision <= 18:
                    integral_type = GOE_TYPE_INTEGER_8
                elif data_precision >= 19 and data_precision <= 38:
                    integral_type = GOE_TYPE_INTEGER_38
                else:
                    # The precision overflows our canonical integral types so store as a decimal.
                    integral_type = GOE_TYPE_DECIMAL
                return new_column(
                    column, integral_type, data_precision=data_precision, data_scale=0
                )
            else:
                # If precision & scale are None then this is unsafe, otherwise leave it None to let
                # new_column() logic take over.
                safe_mapping = (
                    False if data_precision is None and data_scale is None else None
                )
                return new_column(
                    column,
                    GOE_TYPE_DECIMAL,
                    data_precision=data_precision,
                    data_scale=data_scale,
                    safe_mapping=safe_mapping,
                )
        elif column.data_type == SYNAPSE_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE)
        elif column.data_type == SYNAPSE_TYPE_TIME:
            return new_column(column, GOE_TYPE_TIME, data_scale=column.data_scale)
        elif column.data_type in (SYNAPSE_TYPE_DATETIME, SYNAPSE_TYPE_DATETIME2):
            return new_column(column, GOE_TYPE_TIMESTAMP, data_scale=column.data_scale)
        elif column.data_type == SYNAPSE_TYPE_SMALLDATETIME:
            return new_column(column, GOE_TYPE_TIMESTAMP, data_scale=0)
        elif column.data_type == SYNAPSE_TYPE_DATETIMEOFFSET:
            return new_column(
                column, GOE_TYPE_TIMESTAMP_TZ, data_scale=column.data_scale
            )
        elif column.data_type == SYNAPSE_TYPE_UNIQUEIDENTIFIER:
            # This mapping was initially BINARY(16) but Hybrid Query required switch to CHAR(36) because:
            #     "JDBC driver gives us a string for a uniqueidentifier and not the underlying bytes"
            return new_column(column, GOE_TYPE_FIXED_STRING, data_length=36)
        else:
            raise NotImplementedError(
                "Unsupported Synapse SQL data type: %s" % column.data_type
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
            return SynapseColumn(
                col.name,
                data_type=data_type,
                data_length=data_length,
                char_length=char_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                char_semantics=col.char_semantics,
                safe_mapping=safe_mapping,
            )

        def nchar_or_char(data_type, char_semantics):
            if (
                data_type == SYNAPSE_TYPE_CHAR
                and char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
            ):
                return SYNAPSE_TYPE_NCHAR
            elif (
                data_type == SYNAPSE_TYPE_VARCHAR
                and char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
            ):
                return SYNAPSE_TYPE_NVARCHAR
            return data_type

        assert column
        assert isinstance(
            column, CanonicalColumn
        ), "%s is not instance of CanonicalColumn" % type(column)

        if column.data_type == GOE_TYPE_FIXED_STRING:
            return new_column(
                column,
                nchar_or_char(SYNAPSE_TYPE_CHAR, column.char_semantics),
                data_length=column.data_length,
                char_length=column.char_length,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_LARGE_STRING:
            return new_column(
                column,
                nchar_or_char(SYNAPSE_TYPE_VARCHAR, column.char_semantics),
                data_length=None,
            )
        elif column.data_type == GOE_TYPE_VARIABLE_STRING:
            return new_column(
                column,
                nchar_or_char(SYNAPSE_TYPE_VARCHAR, column.char_semantics),
                data_length=column.data_length,
                char_length=column.char_length,
                safe_mapping=True,
            )
        elif column.data_type == GOE_TYPE_BINARY:
            return new_column(
                column, SYNAPSE_TYPE_VARBINARY, data_length=column.data_length
            )
        elif column.data_type == GOE_TYPE_LARGE_BINARY:
            return new_column(column, SYNAPSE_TYPE_VARBINARY, data_length=None)
        # Synapse tinyint is unsigned, so cannot cater for all INTEGER_1 values; map to smallint
        elif column.data_type in (GOE_TYPE_INTEGER_1, GOE_TYPE_INTEGER_2):
            return new_column(column, SYNAPSE_TYPE_SMALLINT, safe_mapping=True)
        elif column.data_type == GOE_TYPE_INTEGER_4:
            return new_column(column, SYNAPSE_TYPE_INT, safe_mapping=True)
        elif column.data_type == GOE_TYPE_INTEGER_8:
            return new_column(column, SYNAPSE_TYPE_BIGINT, safe_mapping=True)
        elif column.data_type == GOE_TYPE_INTEGER_38:
            return new_column(
                column,
                SYNAPSE_TYPE_NUMERIC,
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
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=data_precision,
                    data_scale=column.data_scale,
                    safe_mapping=True,
                )
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, SYNAPSE_TYPE_DATE, safe_mapping=True)
        elif column.data_type == GOE_TYPE_FLOAT:
            return new_column(column, SYNAPSE_TYPE_REAL)
        elif column.data_type == GOE_TYPE_DOUBLE:
            return new_column(column, SYNAPSE_TYPE_FLOAT)
        elif column.data_type == GOE_TYPE_TIME:
            safe_mapping = bool(
                column.data_scale is None
                or column.data_scale <= self.max_datetime_scale()
            )
            data_scale = (
                column.data_scale
                if (column.data_scale or 0) < self.max_datetime_scale()
                else self.max_datetime_scale()
            )
            return new_column(
                column,
                SYNAPSE_TYPE_TIME,
                data_scale=data_scale,
                safe_mapping=safe_mapping,
            )
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            safe_mapping = bool(
                column.data_scale is None
                or column.data_scale <= self.max_datetime_scale()
            )
            data_scale = (
                column.data_scale
                if (column.data_scale or 0) < self.max_datetime_scale()
                else self.max_datetime_scale()
            )
            return new_column(
                column,
                SYNAPSE_TYPE_DATETIME2,
                data_scale=data_scale,
                safe_mapping=safe_mapping,
            )
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            safe_mapping = bool(
                column.data_scale is None
                or column.data_scale <= self.max_datetime_scale()
            )
            data_scale = (
                column.data_scale
                if (column.data_scale or 0) < self.max_datetime_scale()
                else self.max_datetime_scale()
            )
            return new_column(
                column,
                SYNAPSE_TYPE_DATETIMEOFFSET,
                data_scale=data_scale,
                safe_mapping=safe_mapping,
            )
        elif column.data_type == GOE_TYPE_INTERVAL_DS:
            return new_column(column, SYNAPSE_TYPE_VARCHAR, data_length=100)
        elif column.data_type == GOE_TYPE_INTERVAL_YM:
            return new_column(column, SYNAPSE_TYPE_VARCHAR, data_length=100)
        elif column.data_type == GOE_TYPE_BOOLEAN:
            return new_column(column, SYNAPSE_TYPE_BIT)
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )

    def alter_sort_columns(self, db_name, table_name, sort_column_names, sync=None):
        raise NotImplementedError("alter_sort_columns not supported for Synapse")

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
        raise NotImplementedError("UDFs are not supported for Synapse")

    def db_name_label(self, initcap=False):
        label = "schema"
        return label.capitalize() if initcap else label

    def derive_native_partition_info(self, db_name, table_name, column, position):
        """Table partitioning not implemented at this stage on Synapse"""
        return None

    def gen_native_range_partition_key_cast(self, partition_column):
        """Table partitioning not implemented at this stage on Synapse"""
        raise NotImplementedError(
            "gen_native_range_partition_key_cast not supported for Synapse"
        )

    def supported_partition_function_parameter_data_types(self):
        """Table partitioning not implemented at this stage on Synapse"""
        return None

    def supported_partition_function_return_data_types(self):
        """Table partitioning not implemented at this stage on Synapse"""
        return None

    def synthetic_partition_numbers_are_string(self):
        """Table partitioning not implemented at this stage on Synapse"""
        return False

    def udf_details(self, db_name, udf_name):
        raise NotImplementedError("UDFs are not supported for Synapse")
