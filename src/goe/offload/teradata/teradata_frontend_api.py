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

""" TeradataFrontendApi: Library for logic/interaction with an Oracle frontend.
    Implements abstract methods from FrontendApiInterface.
"""

# Standard Library
import logging
from datetime import datetime
from sys import getsizeof
from textwrap import dedent
from typing import Dict, List, Optional, Union

# Third Party Libraries
import pyodbc
from numpy import datetime64
from pydantic import UUID4

# GOE
from goe.offload.column_metadata import match_table_column
from goe.offload.frontend_api import (
    FETCH_ACTION_ALL,
    FETCH_ACTION_CURSOR,
    FETCH_ACTION_ONE,
    GET_DDL_TYPE_TABLE,
    GET_DDL_TYPE_VIEW,
    FrontendApiInterface,
)
from goe.offload.offload_constants import TERADATA_FRONTEND_CAPABILITIES
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.teradata.teradata_column import (
    TERADATA_TYPE_VARCHAR,
    TeradataColumn,
)
from goe.offload.teradata.teradata_literal import TeradataLiteral
from goe.offload.teradata.teradata_partition_expression import (
    PARTITION_TYPE_COLUMNAR,
    TeradataPartitionExpression,
    UnsupportedCaseNPartitionExpression,
    UnsupportedPartitionExpression,
)
from goe.orchestration.execution_id import ExecutionId
from goe.util.misc_functions import double_quote_sandwich, split_not_in_quotes

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# CONSTANTS
###########################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def teradata_get_primary_partition_expression(
    owner: str, table_name: str, query_runner: FrontendApiInterface
) -> Optional[TeradataPartitionExpression]:
    """Return a TeradataPartitionExpression object describing the first row partition expression we'll use
    to drive offloads. Only RANGE_N is supported.
    The result is cached in state because we'll need to decode this data for multiple reasons.
    """
    sql = dedent(
        """\
        SELECT ConstraintText
        FROM   DBC.PartitioningConstraintsV
        WHERE  ConstraintType = 'Q'
        AND    DatabaseName = ?
        AND    TableName = ?"""
    )
    row = query_runner.execute_query_fetch_one(sql, query_params=[owner, table_name])
    if not row:
        return None
    expr_str = row[0]
    part_expr = TeradataPartitionExpression(expr_str)
    if part_expr.partition_type == PARTITION_TYPE_COLUMNAR:
        # If a table is columnar alone then treat as non-partitioned
        return None
    else:
        return part_expr


###########################################################################
# TeradataFrontendApi
###########################################################################


class TeradataFrontendApi(FrontendApiInterface):
    """Teradata FrontendApi implementation"""

    def __init__(
        self,
        connection_options,
        frontend_type,
        messages,
        conn_user_override=None,
        existing_connection=None,
        dry_run=False,
        do_not_connect=False,
        trace_action=None,
    ):
        """Standard CONSTRUCTOR"""
        super().__init__(
            connection_options,
            frontend_type,
            messages,
            conn_user_override=conn_user_override,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )

        logger.info("TeradataFrontendApi")
        if dry_run:
            logger.info("* Dry run *")

        self._client = None
        if not do_not_connect:
            self._connect()

        self._cursor = None
        self._frontend_version = None

    def __del__(self):
        """Clean up connection"""
        try:
            self._disconnect()
        except Exception:
            pass

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _close_cursor(self):
        """After a DDL/DML operation this is raising and logging:
            "Exception closing cursor: Attempt to use a closed cursor."
        Those operations have None in the description property on the cursor so we
        check for it before trying to close.
        """
        if (
            self._client
            and self._cursor
            and getattr(self._cursor, "description", None) is not None
        ):
            try:
                self._cursor.close()
            except Exception as exc:
                self._log("Exception closing cursor:\n%s" % str(exc), detail=VVERBOSE)

    def _conn_user_and_pass_for_override(self):
        """Return a user and password from connection_options object for the override user"""
        assert self._conn_user_override
        if self._conn_user_override.upper() == self._upper_or_empty(
            self._connection_options.teradata_adm_user
        ):
            return (
                self._connection_options.teradata_adm_user,
                self._connection_options.teradata_adm_pass,
            )
        elif self._conn_user_override.upper() == self._upper_or_empty(
            self._connection_options.teradata_app_user
        ):
            return (
                self._connection_options.teradata_app_user,
                self._connection_options.teradata_app_pass,
            )
        else:
            raise NotImplementedError(
                f"User details are unknown for Teradata connection: {self._conn_user_override}"
            )

    def _connect(self):
        made_new_connection = False
        if self._existing_connection:
            self._client = self._existing_connection
        if not self._client:
            if self._conn_user_override:
                conn_user, conn_pass = self._conn_user_and_pass_for_override()
            else:
                conn_user = self._connection_options.teradata_adm_user
                conn_pass = self._connection_options.teradata_adm_pass
            odbc_url = self._connect_url(conn_user, conn_pass)
            self._client = pyodbc.connect(odbc_url)
            self._client.autocommit = True
            made_new_connection = True
        return made_new_connection

    def _connect_url(self, conn_user, conn_pass):
        url = [
            "Driver={%s}" % self._connection_options.frontend_odbc_driver_name,
            "Dbcname=%s" % self._connection_options.teradata_server,
            "Uid=%s" % conn_user,
            "Pwd=%s" % conn_pass,
            "MaxSingleLOBBytes=0;MaxTotalLOBBytesPerRow=0",
            "UseDataEncryption=Yes;LoginTimeout=30",
        ]
        odbc_url = ";".join(url)
        return odbc_url

    def _create_table_sql_text(
        self,
        schema,
        table_name,
        column_list,
        partition_column_names,
        table_properties=None,
    ) -> str:
        if table_properties:
            raise NotImplementedError(
                f"Create table properties pending implementation: {table_properties}"
            )

        col_projection = self._create_table_columns_clause_common(column_list)

        partition_clause = ""
        if partition_column_names:
            raise NotImplementedError(
                "Create table partitioning pending implementation"
            )

        sql = (
            dedent(
                """\
            CREATE TABLE %(owner_table)s (
                %(col_projection)s
            )%(partition_clause)s"""
            )
            % {
                "owner_table": self.enclose_object_reference(schema, table_name),
                "col_projection": col_projection,
                "partition_clause": partition_clause,
            }
        )
        return sql

    def _disconnect(self, force=False):
        self._debug("Disconnecting from DB")
        if not self._existing_connection or force:
            # Only close a connection we created
            if self._cursor:
                self._close_cursor()
            try:
                if self._client:
                    self._client.close()
                    self._client = None
            except Exception as exc:
                self._log(
                    "Exception closing connection:\n%s" % str(exc), detail=VVERBOSE
                )

    def _execute_ddl_or_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        executemany=False,
    ):
        """Run SQL that has no result set, e.g. DDL, sql can be a str or a list of SQLs.
        This function returns the SQL statements that were executed.
        Exception will be raised upon failure.
        """
        assert sql
        assert isinstance(sql, (str, list))

        self._open_cursor()
        # TODO if trace_action:
        # TODO      self._db_conn.action = trace_action
        run_opts = self._execute_session_options(query_options, log_level=log_level)
        run_sqls = []
        sqls = [sql] if isinstance(sql, str) else sql
        try:
            for i, run_sql in enumerate(sqls):
                self._log_or_not(
                    "%s SQL: %s" % (self._sql_engine_name, run_sql), log_level=log_level
                )
                run_sqls.append(run_sql)
                if not self._dry_run:
                    if query_params:
                        self._log_or_not(
                            "%s SQL parameters: %s"
                            % (
                                self._sql_engine_name,
                                self._to_native_query_params(query_params),
                            ),
                            log_level=log_level,
                        )
                        self._cursor.execute(
                            run_sql, *self._to_native_query_params(query_params)
                        )
                    else:
                        self._cursor.execute(run_sql)
        finally:
            self._close_cursor()
        # TODO if trace_action:
        # TODO     self._db_conn.action = self._trace_action
        return run_opts + run_sqls

    def _execute_query_fetch_x(
        self,
        sql,
        fetch_action=FETCH_ACTION_ALL,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=False,
        trace_action=None,
        as_dict=False,
    ):
        """Run a query, fetch and return the results.
        Always opens/closes a fresh cursor to ensure we have a clean session environment with no settings
        carried forwards from a previous action.
        """

        def log_query():
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
            )
            if query_params:
                self._log_or_not(
                    "%s SQL parameters: %s"
                    % (
                        self._sql_engine_name,
                        self._to_native_query_params(query_params),
                    ),
                    log_level=log_level,
                )

        assert sql

        if self._dry_run and not_when_dry_running:
            log_query()
            return None

        t1 = datetime.now().replace(microsecond=0)

        self._open_cursor()
        try:
            self._execute_session_options(query_options, log_level=log_level)
            log_query()
            if query_params:
                native_params = self._to_native_query_params(query_params)
                self._cursor.execute(sql, *native_params)
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
                    # pyodbc gives us back a list of pyodbc.Row and not tuple, in most cases no-big-deal, but
                    # there is a possibility for code to be using isinstance(, (tuple, list)) so we stay
                    # safe and convert to tuple.
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

            # TODO if profile and self._option_clause_valid_for_sql_text(sql):
            # TODO     self._log(self._get_query_profile(query_identifier=query_option_label), detail=VVERBOSE)
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

    def _execute_session_options(self, query_options, log_level=VVERBOSE):
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

    def _executemany_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        batch_size=None,
    ):
        assert sql
        self._open_cursor()
        # TODO if trace_action:
        # TODO      self._db_conn.action = trace_action
        run_opts = self._execute_session_options(query_options, log_level=log_level)
        native_params = [self._to_native_query_params(_) for _ in query_params]
        batch_size = batch_size or len(query_params)
        try:
            for i, param_batch in enumerate(
                [
                    native_params[_ * batch_size : (_ + 1) * batch_size]
                    for _ in range(int(len(native_params) / batch_size) + 1)
                ]
            ):
                if param_batch == []:
                    continue
                if i == 0:
                    self._log_or_not(
                        "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
                    )
                self._log_or_not(
                    "Binds(%s): %s rows" % (i, len(param_batch)), log_level=log_level
                )
                if not self._dry_run:
                    self._cursor.executemany(sql, param_batch)
        finally:
            self._close_cursor()
        # TODO if trace_action:
        # TODO     self._db_conn.action = self._trace_action
        return run_opts + [sql]

    def _fast_executemany_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        batch_size=None,
        param_inputsizes=None,
    ):
        assert sql
        self._open_cursor()
        # TODO if trace_action:
        # TODO      self._db_conn.action = trace_action
        run_opts = self._execute_session_options(query_options, log_level=log_level)
        native_params = [self._to_native_query_params(_) for _ in query_params]
        """ Teradata has a SQL request maximum allowed length of 7 MB.
            With fast_executemany the sql text and all of the binds are sent in a single statement.
            Try and fit the biggest batch we can inside this limit.
        """
        LIMIT = 7 * pow(1024, 2)
        total_binds_len = sum(
            [sum([getsizeof(_) for _ in row]) for row in native_params]
        )
        if not batch_size:
            if (total_binds_len + getsizeof(sql)) < LIMIT:
                batch_size = len(query_params)
            else:
                avg_binds_len = int(total_binds_len / len(native_params))
                batch_size = int(
                    (LIMIT / avg_binds_len) - ((LIMIT / avg_binds_len) % 10)
                )
        try:
            for i, param_batch in enumerate(
                [
                    native_params[_ * batch_size : (_ + 1) * batch_size]
                    for _ in range(int(len(native_params) / batch_size) + 1)
                ]
            ):
                if param_batch == []:
                    continue
                if i == 0:
                    self._log_or_not(
                        "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
                    )
                self._log_or_not(
                    "Binds(%s): %s rows" % (i, len(param_batch)), log_level=log_level
                )
                if not self._dry_run:
                    self._cursor.fast_executemany = True
                    if param_inputsizes:
                        self._cursor.setinputsizes(param_inputsizes)
                    self._cursor.executemany(sql, param_batch)
        finally:
            self._close_cursor()
        # TODO if trace_action:
        # TODO     self._db_conn.action = self._trace_action
        return run_opts + [sql]

    def _fixed_session_parameters(self) -> dict:
        return {"time zone": "'+00:00'"}

    def _format_query_options(self, query_options: Optional[dict] = None) -> list:
        """
        Format options for Teradata
        query_options: key/value pairs for session settings
        """
        if not query_options:
            return []
        assert isinstance(query_options, dict)
        return [f"""SET {k} {v}""" for k, v in query_options.items()]

    def _frontend_capabilities(self):
        return TERADATA_FRONTEND_CAPABILITIES

    def _goe_db_component_version(self):
        # We don't have an installed objects on Teradata so this method should never be called
        raise NotImplementedError(
            "_goe_db_component_version() is not implemented for Teradata"
        )

    def _open_cursor(self):
        self._cursor = self._client.cursor() if self._client else None
        self._execute_session_options(self._fixed_session_parameters(), log_level=None)

    def _to_native_query_params(self, query_params):
        return self._odbc_to_native_query_params(query_params)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def close(self, force=False):
        self._disconnect(force=force)

    def agg_validate_sample_column_names(
        self, schema, table_name, num_required: int = 5
    ) -> list:
        sql = dedent(
            """\
        SELECT ColumnName
        FROM  (
               SELECT c.ColumnName
               ,      c.ColumnId
               ,      MIN(c.ColumnId) OVER () AS first_column_id
               ,      MAX(c.ColumnId) OVER () AS last_column_id
               ,      ROW_NUMBER() OVER (ORDER BY s.UniqueValueCount DESC) AS ndv_rank
               FROM   DBC.ColumnsV AS c
               LEFT OUTER JOIN DBC.StatsV AS s ON (c.DatabaseName = s.DatabaseName
                                                   AND c.TableName = s.TableName
                                                   AND c.ColumnName = s.ColumnName)
               WHERE  c.DatabaseName = ?
               AND    c.TableName = ?
              ) AS v
        WHERE  ColumnId IN (first_column_id, last_column_id)
        OR     ndv_rank <= ?"""
        )
        rows = self.execute_query_fetch_all(
            sql, query_params=[schema, table_name, num_required], log_level=VVERBOSE
        )
        return [_[0] for _ in rows] if rows else []

    def create_new_connection(
        self, user_name, user_password, trace_action_override=None
    ):
        self._debug("Making new connection with user %s" % user_name)
        odbc_url = self._connect_url(user_name, user_password)
        client = pyodbc.connect(odbc_url)
        client.autocommit = True
        return client

    def enclose_identifier(self, identifier):
        if identifier is None:
            return None
        return double_quote_sandwich(identifier)

    def enclose_query_hints(self, hint_contents):
        # Teradata does not support query hints. We will only implement this method if a requirement is found.
        return ""

    def enclosure_character(self):
        return '"'

    def execute_function(
        self,
        sql_fn,
        return_type=None,
        return_type_name=None,
        arg_list=None,
        log_level=VERBOSE,
        not_when_dry_running=False,
        commit=False,
    ):
        raise NotImplementedError("execute_function() is not implemented for Teradata")

    def format_query_parameter(self, param_name):
        """No named parameters so always return "?" """
        return "?"

    def frontend_version(self):
        if self._frontend_version is None:
            sql = "SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
            row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
            self._frontend_version = row[0] if row else None
        return self._frontend_version

    def gen_column_object(self, column_name, **kwargs):
        return TeradataColumn(column_name, **kwargs)

    def generic_string_data_type(self) -> str:
        return TERADATA_TYPE_VARCHAR

    def get_columns(self, schema, table_name):
        q = dedent(
            """\
            SELECT ColumnId
            ,      ColumnName
            ,      Nullable
            ,      ColumnType
            ,      DecimalTotalDigits
            ,      DecimalFractionalDigits
            ,      ColumnLength
            ,      DefaultValue
            FROM   DBC.ColumnsV
            WHERE  DatabaseName = ?
            AND    TableName = ?
            ORDER BY ColumnId ASC"""
        )

        cols = []
        for row in self.execute_query_fetch_all(q, query_params=[schema, table_name]):
            col = TeradataColumn.from_teradata(row)
            cols.append(col)
        return cols

    def get_db_unique_name(self) -> str:
        raise NotImplementedError(
            "get_db_unique_name() is not implemented for Teradata"
        )

    def get_distinct_column_values(
        self, schema, table_name, column_names, partition_name=None, order_results=False
    ):
        """Run SQL to get distinct values for a list of columns.
        partition_name is ignored on Teradata.
        """
        assert schema and table_name
        assert column_names
        if isinstance(column_names, str):
            column_names = [column_names]
        projection = ",".join([self.enclose_identifier(_) for _ in column_names])
        order_clause = " ORDER BY {}".format(projection) if order_results else ""
        sql = "SELECT DISTINCT %(col)s FROM %(own)s.%(tab)s%(order_clause)s" % {
            "col": projection,
            "own": self.enclose_identifier(schema.upper()),
            "tab": self.enclose_identifier(table_name.upper()),
            "order_clause": order_clause,
        }
        self._messages.log("Distinct column SQL: %s" % sql, detail=VVERBOSE)
        return self.execute_query_fetch_all(sql)

    def get_object_ddl(
        self,
        schema,
        object_name,
        object_type,
        as_list=False,
        terminate_sql=False,
        remap_schema=None,
    ):
        """Return CREATE DDL as a string (or a list of strings split on CR if as_list=True)
        terminate_sql: Adds any executing character to the end of the SQL (e.g. a semi-colon)
        remap_schema: Replaces the actual schema (schema) with a different name (remap_schema)
        """

        def simple_line_breaks(s):
            return s.replace("\r\n", "\n").replace("\r", "\n")

        assert object_type in [GET_DDL_TYPE_TABLE, GET_DDL_TYPE_VIEW]
        sql = f"SHOW {object_type} {schema}.{object_name}"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        if row:
            ddl_str = row[0]
            # Protect ourselves from Teradata using \r instead of \n
            ddl_str = simple_line_breaks(ddl_str)
            if not terminate_sql:
                ddl_str = ddl_str.strip().rstrip(";")
            if as_list:
                return ddl_str.split("\n")
            else:
                return ddl_str
        else:
            return None

    def get_offloadable_schemas(self):
        raise NotImplementedError(
            "Teradata get_offloadable_schemas is not implemented."
        )

    def get_command_step_codes(self) -> list:
        raise NotImplementedError("Teradata get_command_step_codes is not implemented.")

    def get_command_executions(self) -> List[Dict[str, Union[str, UUID4]]]:
        raise NotImplementedError("Teradata get_command_executions is not implemented.")

    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, UUID4]]:
        raise NotImplementedError(
            "Teradata get_command_execution_status is not implemented."
        )

    def get_command_execution_steps(
        self, execution_id: ExecutionId
    ) -> List[Dict[str, Union[str, UUID4]]]:
        raise NotImplementedError(
            "Teradata get_command_execution_steps is not implemented."
        )

    def get_partition_columns(self, schema, table_name):
        """Return columns in the first row partition expression, i.e. we skip columnar partitioning."""
        try:
            partition_expression = teradata_get_primary_partition_expression(
                schema, table_name, self
            )
        except (
            UnsupportedCaseNPartitionExpression,
            UnsupportedPartitionExpression,
        ) as exc:
            # Treat tables with unsupported partition expressions as non-partitioned.
            self._log(
                "Cannot retrieve partition column from partition expression: {}".format(
                    str(exc)
                ),
                detail=VVERBOSE,
            )
            return []
        if partition_expression:
            table_columns = self.get_columns(schema, table_name)
            return [match_table_column(partition_expression.column, table_columns)]
        else:
            return []

    def get_primary_key_column_names(self, schema, table_name):
        q = dedent(
            """\
            SELECT ColumnName
            FROM   DBC.IndicesV
            WHERE  DatabaseName = ?
            AND    TableName = ?
            AND    IndexType = 'K'
            ORDER BY ColumnPosition"""
        )

        rows = self.execute_query_fetch_all(q, query_params=[schema, table_name])
        return [_[0] for _ in rows] if rows else []

    def get_schema_tables(self, schema_name):
        raise NotImplementedError("Teradata get_schema_tables is not implemented.")

    def get_session_option(self, option_name):
        raise NotImplementedError(
            "get_session_option() is not implemented for Teradata"
        )

    def get_session_user(self) -> str:
        sql = "SELECT CURRENT_USER"
        row = self.execute_query_fetch_one(sql)
        return row[0] if row else row

    def get_subpartition_columns(self, schema, table_name):
        raise NotImplementedError(
            "get_subpartition_columns() is not implemented for Teradata"
        )

    def get_table_row_count(
        self,
        schema,
        table_name,
        filter_clause=None,
        filter_clause_params=None,
        hint_block=None,
        not_when_dry_running=False,
        log_level=VVERBOSE,
    ):
        """Note that this is interrogating the table in order to get the actual row count
        not getting the value from stats or some potentially stale location.
        hint_block: Ignored on Teradata
        """
        where_clause = f" WHERE {filter_clause}" if filter_clause else ""
        sql = "SELECT COUNT(*) FROM %s%s" % (
            self.enclose_object_reference(schema, table_name),
            where_clause,
        )
        row = self.execute_query_fetch_one(
            sql,
            query_params=filter_clause_params,
            log_level=log_level,
            not_when_dry_running=not_when_dry_running,
        )
        return row[0] if row else None

    def get_table_size(self, schema, table_name):
        """Return the size of the table in bytes"""
        sql = dedent(
            """\
            SELECT SUM(CurrentPerm)
            FROM   DBC.TablesizeV
            WHERE  DatabaseName = ?
            AND    TableName = ?
        """
        )
        row = self.execute_query_fetch_one(sql, query_params=[schema, table_name])
        return row[0] if row else row

    def is_view(self, schema, object_name) -> bool:
        """Is the underlying RDBMS object a view"""
        return self.view_exists(schema, object_name)

    def max_datetime_scale(self) -> int:
        """Teradata supports up to microseconds"""
        return 6

    def max_datetime_value(self):
        return datetime64("9999-12-31")

    def max_table_name_length(self):
        return 128

    def min_datetime_value(self):
        return datetime64("0001-01-01")

    def parallel_query_hint(self, query_parallelism):
        # Teradata does not support query hints. We will only implement this method if a requirement is found.
        return None

    def schema_exists(self, schema) -> bool:
        sql = dedent(
            """\
            SELECT DatabaseName
            FROM   DBC.Databases2V
            WHERE  DatabaseName = ?
        """
        )
        return bool(self.execute_query_fetch_one(sql, query_params=[schema]))

    def split_partition_high_value_string(self, hv_string):
        if not hv_string:
            return []
        tokens = split_not_in_quotes(hv_string, sep=",")
        return tokens

    def table_exists(self, schema, table_name) -> bool:
        sql = dedent(
            """\
            SELECT TableName
            FROM   DBC.TablesV
            WHERE  DatabaseName = ?
            AND    TableName = ?
            AND    TableKind = 'T'
        """
        )
        return bool(
            self.execute_query_fetch_one(sql, query_params=[schema, table_name])
        )

    def to_frontend_literal(self, py_val, data_type=None) -> str:
        self._debug("Formatting %s literal: %s" % (type(py_val), repr(py_val)))
        self._debug("For data type: %s" % data_type)
        new_py_val = TeradataLiteral.format_literal(py_val, data_type=data_type)
        self._debug("Final %s literal: %s" % (type(py_val), str(new_py_val)))
        return new_py_val

    def view_exists(self, schema, view_name) -> bool:
        sql = dedent(
            """\
            SELECT TableName
            FROM   DBC.TablesV
            WHERE  DatabaseName = ?
            AND    TableName = ?
            AND    TableKind = 'V'
        """
        )
        return bool(self.execute_query_fetch_one(sql, query_params=[schema, view_name]))
