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

""" MSSQLFrontendApi: Library for logic/interaction with an MSSQL frontend.
    Implements abstract methods from FrontendApiInterface.
"""

# Standard Library
import logging
from datetime import datetime
from textwrap import dedent
from typing import Any, Dict, List, Union

# Third Party Libraries
import pymssql
from numpy import datetime64

# GOE
from goe.offload.frontend_api import (
    FETCH_ACTION_ALL,
    FETCH_ACTION_CURSOR,
    FETCH_ACTION_ONE,
    FRONTEND_TRACE_MODULE,
    FrontendApiInterface,
    QueryParameter,
    extract_connection_details_from_dsn,
)
from goe.offload.microsoft.mssql_column import MSSQL_TYPE_VARCHAR, MSSQLColumn
from goe.offload.microsoft.synapse_literal import SynapseLiteral
from goe.offload.offload_constants import MSSQL_FRONTEND_CAPABILITIES
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.orchestration.execution_id import ExecutionId

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

MSSQL_MAX_TABLE_NAME_LENGTH = 128


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def setup_mssql_session(
    server, port, database, username, password, appname=FRONTEND_TRACE_MODULE
):
    """Take a set of connection properties and return a SQL Server connection"""
    assert server
    assert port
    assert database
    assert username
    assert password
    # Must always use fetchall() - http://pymssql.org/en/latest/pymssql_examples.html#important-note-about-cursors
    logger.debug('Connecting to DB "%s" as %s' % (database, username))
    # Setting conn_properties='' in order to connect to Azure. Left in as MSSQL is not production and it took me ages
    # to find the SO note:
    #   https://stackoverflow.com/questions/52418707/connect-to-sybase-data-base-using-python-3-6-and-pymssql
    db_conn = pymssql.connect(
        server=server,
        user=username,
        password=password,
        database=database,
        port=port,
        appname=appname,
        conn_properties="",
    )
    return db_conn


###########################################################################
# MSSQLFrontendApi
###########################################################################


class MSSQLFrontendApi(FrontendApiInterface):
    """MSSQL FrontendApi implementation"""

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
        super(MSSQLFrontendApi, self).__init__(
            connection_options,
            frontend_type,
            messages,
            conn_user_override=conn_user_override,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )

        logger.info("MSSQLFrontendApi")
        if dry_run:
            logger.info("* Dry run *")

        self._db_conn = None
        self._db_curs = None
        if not do_not_connect:
            # Unit testing requires no db connection
            self._connect()

        # Be careful caching in state, only cache values that cannot change for a different set of inputs
        # such as for a different db_user or table_name.
        self._instance_compatible = None
        self._max_table_name_length = None

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
        if self._db_conn and self._db_curs:
            try:
                self._db_curs.close()
                self._db_curs = None
            except Exception as exc:
                self._log("Exception closing cursor:\n%s" % str(exc), detail=VVERBOSE)

    def _connect(self):
        server, port, database = extract_connection_details_from_dsn(
            self._connection_options.rdbms_dsn
        )
        self._db_conn = setup_mssql_session(
            server,
            port,
            database,
            self._connection_options.rdbms_app_user,
            self._connection_options.rdbms_app_pass,
        )

    def _create_table_sql_text(
        self,
        schema,
        table_name,
        column_list,
        partition_column_names,
        table_properties=None,
    ) -> str:
        """This is missing a lot compared to the backend Synapse equivalent, lots of scope for improvement."""
        if table_properties:
            raise NotImplementedError(
                f"Create table properties pending implementation: {table_properties}"
            )

        col_projection = self._create_table_columns_clause_common(column_list)

        if partition_column_names:
            raise NotImplementedError(
                "Create table partitioning pending implementation"
            )

        sql = (
            dedent(
                """\
            CREATE TABLE %(owner_table)s (
            %(col_projection)s
            )"""
            )
            % {
                "owner_table": self.enclose_object_reference(schema, table_name),
                "col_projection": col_projection,
            }
        )
        return sql

    def _disconnect(self, force=False):
        self._debug("Disconnecting from DB") if logger else None
        if not self._existing_connection or force:
            # Only close a connection we created
            if self._db_curs:
                self._close_cursor()
            try:
                if self._db_conn:
                    self._db_conn.close()
                    self._db_conn = None
            except Exception:
                pass

    def _execute_ddl_or_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
    ):
        """Execute one or more SQL statements.
        Always opens/closes a fresh cursor to ensure we have a clean session environment with no settings
        carried forwards from a previous action. All SQLs in sql are executed in the same query environment.
        """
        assert sql
        assert isinstance(sql, (str, list))
        self._open_cursor()
        run_opts = (
            []
        )  # self._execute_session_options(query_options, log_level=log_level)
        run_sqls = []
        sqls = [sql] if isinstance(sql, str) else sql
        for i, run_sql in enumerate(sqls):
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, run_sql), log_level=log_level
            )
            run_sqls.append(run_sql)
            if not self._dry_run:
                if query_params:
                    self._db_curs.execute(
                        run_sql, self._to_native_query_params(query_params)
                    )
                else:
                    self._db_curs.execute(run_sql)
        self._close_cursor()
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
    ):
        """Run a query, fetch and return the results.
        Always opens/closes a fresh cursor to ensure we have a clean session environment with no settings
        carried forwards from a previous action.
        """
        assert sql

        if self._dry_run and not_when_dry_running:
            return None

        if fetch_action == FETCH_ACTION_CURSOR:
            raise NotImplementedError(
                "_execute_query_fetch_x() by FETCH_ACTION_CURSOR is not supported on MSSQL"
            )

        t1 = datetime.now().replace(microsecond=0)

        self._open_cursor()
        try:
            # TODO Do we have session options on MSSQL? Need to implement here if so.
            # self._execute_session_options(query_options, log_level=log_level)
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
            )
            if query_params:
                self._db_curs.execute(sql, self._to_native_query_params(query_params))
            else:
                self._db_curs.execute(sql)
            if fetch_action == FETCH_ACTION_ALL:
                rows = self._db_curs.fetchall()
            elif fetch_action == FETCH_ACTION_ONE:
                # Must always use fetchall() -
                #   http://pymssql.org/en/latest/pymssql_examples.html#important-note-about-cursors
                rows = self._db_curs.fetchall()
                rows = rows[0] if rows else rows
            else:
                rows = None

            if time_sql:
                t2 = datetime.now().replace(microsecond=0)
                self._log("Elapsed time: %s" % (t2 - t1), detail=VVERBOSE)
        finally:
            self._close_cursor()

        return rows

    def _executemany_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        batch_size=None,
    ):
        raise NotImplementedError("_executemany_dml() is not implemented for MSSQL")

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
        raise NotImplementedError(
            "_fast_executemany_dml() is not implemented for MSSQL"
        )

    def _frontend_capabilities(self):
        return MSSQL_FRONTEND_CAPABILITIES

    def _goe_db_component_version(self):
        return None

    def _open_cursor(self):
        if self._db_conn:
            self._db_curs = self._db_conn.cursor()
        else:
            self._db_curs = None

    def _to_native_query_params(self, query_params):
        """Using a tuple for pymssql binds.
        For convenience, if it is already a tuple then turning a blind eye and returning it, this allows code
        migrated here from OffloadSourceTable to remain unchanged.
        """
        if isinstance(query_params, tuple):
            return query_params
        # If not a tuple then must be a list of QueryParameter objects
        assert isinstance(query_params, list)
        if query_params:
            assert isinstance(query_params[0], QueryParameter)
        param_values = []
        for qp in query_params:
            param_values.append(qp.param_value)
        return tuple(param_values)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def close(self, force=False):
        self._disconnect(force=force)

    def agg_validate_sample_column_names(
        self, schema, table_name, num_required: int = 5
    ) -> list:
        raise NotImplementedError(
            "MSSQL agg_validate_sample_column_names() not implemented."
        )

    def create_new_connection(
        self, user_name, user_password, trace_action_override=None
    ):
        self._debug("Making new connection with user %s" % user_name)
        server, port, database = extract_connection_details_from_dsn(
            self._connection_options.rdbms_dsn
        )
        client = setup_mssql_session(server, port, database, user_name, user_password)
        # client.autocommit = True
        return client

    def enclose_identifier(self, identifier):
        if identifier is None:
            return None
        return "[%s]" % identifier

    def enclose_query_hints(self, hint_contents):
        if not hint_contents:
            return ""
        if isinstance(hint_contents, str):
            hint_contents = [hint_contents]
        return "OPTION({})".format(", ".join(str(_) for _ in hint_contents if _))

    def enclosure_character(self):
        return None

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
        raise NotImplementedError("MSSQL execute_function not implemented.")

    def format_query_parameter(self, param_name):
        """No named parameters so always return "%s" """
        return "%s"

    def frontend_db_name(self):
        return self._frontend_type.upper()

    def frontend_version(self):
        sql = "SELECT @@VERSION"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return row[0] if row else None

    def gen_column_object(self, column_name, **kwargs):
        return MSSQLColumn(column_name, **kwargs)

    def generic_string_data_type(self) -> str:
        return MSSQL_TYPE_VARCHAR

    def get_columns(self, schema, table_name):
        # TODO: byte_length - is this the same as Oracle query: CHARACTER_MAXIMUM_LENGTH / CHARACTER_OCTET_LENGTH
        # TODO: hidden_column - do we have the concept of hidden_columns in MSSQL? If not remove from MSSQLColumn.from_mssql # noqa: E501
        q = """
                    SELECT column_name
                    ,      ordinal_position                                 AS column_id
                    ,      is_nullable                                      AS nullable
                    ,      data_type
                    ,      COALESCE(numeric_precision, datetime_precision)  AS data_precision
                    ,      numeric_scale                                    AS data_scale
                    ,      character_octet_length                           AS byte_length
                    ,      column_default                                   AS data_default
                    ,      NULL                                             AS hidden_columns
                    ,      character_maximum_length                         AS char_length
                    FROM   information_schema.columns
                    WHERE  table_schema = %s
                    AND    table_name   = %s
                    ORDER BY
                           ordinal_position"""

        cols = []
        for row in self.execute_query_fetch_all(q, query_params=(schema, table_name)):
            col = MSSQLColumn.from_mssql(row)
            cols.append(col)
        return cols

    def get_db_unique_name(self) -> str:
        return self._connection_options.rdbms_dsn.split("=")[1]

    def get_distinct_column_values(
        self, schema, table_name, column_names, partition_name=None, order_results=False
    ):
        assert schema and table_name
        assert column_names
        if isinstance(column_names, str):
            column_names = [column_names]
        projection = ",".join([self.enclose_identifier(_) for _ in column_names])
        order_clause = (  # noqa: F841
            " ORDER BY {}".format(projection) if order_results else ""
        )

        raise NotImplementedError("MSSQL get_distinct_column_values not implemented.")

    def get_object_ddl(
        self,
        schema,
        object_name,
        object_type,
        as_list=False,
        terminate_sql=False,
        remap_schema=None,
    ):
        raise NotImplementedError("MSSQL get_object_ddl not implemented.")

    def get_offloadable_schemas(self):
        raise NotImplementedError("MSSQL get_offloadable_schemas is not implemented.")

    def get_command_step_codes(self) -> list:
        raise NotImplementedError("MSSQL get_command_step_codes is not implemented.")

    def get_command_executions(self) -> List[Dict[str, Union[str, Any]]]:
        raise NotImplementedError("MSSQL get_command_executions is not implemented.")

    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, Any]]:
        raise NotImplementedError(
            "MSSQL get_command_execution_status is not implemented."
        )

    def get_command_execution_steps(
        self, execution_id: ExecutionId
    ) -> List[Dict[str, Union[str, Any]]]:
        raise NotImplementedError(
            "MSSQL get_command_execution_steps is not implemented."
        )

    def get_partition_column_names(self, schema, table_name, conv_fn=None):
        raise NotImplementedError("MSSQL get_partition_column_names not implemented.")

    def get_partition_columns(self, schema, table_name):
        raise NotImplementedError("MSSQL get_partition_column_names not implemented.")

    def get_primary_key_column_names(self, schema, table_name):
        q = """
            SELECT COL_NAME(ic.object_id, ic.column_id) AS column_name
            FROM   sys.tables           t
                   INNER JOIN
                   sys.indexes          i
                   ON (t.object_id = i.object_id)
                   INNER JOIN
                   sys.index_columns    ic
                   ON (     i.object_id = ic.object_id
                       AND  i.index_id  = ic.index_id)
                   INNER JOIN
                   sys.schemas          s
                   ON (t.schema_id    = s.schema_id)
            WHERE  s.name = %s
            AND    t.name = %s
            AND    i.is_primary_key = 1
            ORDER BY
                   ic.column_id"""

        rows = self.execute_query_fetch_all(q, query_params=(schema, table_name))
        return [_[0] for _ in rows] if rows else []

    def get_schema_tables(self, schema_name):
        raise NotImplementedError("MSSQL get_schema_tables is not implemented.")

    def get_session_option(self, option_name):
        raise NotImplementedError("MSSQL get_session_option not implemented.")

    def get_session_user(self) -> str:
        raise NotImplementedError("MSSQL get_session_user not implemented.")

    def get_subpartition_column_names(self, schema, table_name, conv_fn=None):
        raise NotImplementedError(
            "MSSQL get_subpartition_column_names not implemented."
        )

    def get_subpartition_columns(self, schema, table_name):
        raise NotImplementedError("MSSQL get_subpartition_columns not implemented.")

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
        raise NotImplementedError("MSSQL get_table_row_count not implemented.")

    def get_table_size(self, schema, table_name):
        """Not actually used since we always choose Sqoop in OffloadOperation.set_offload_method
        The appropriateness of the SP_SPACEUSED procedure for calculating table size must be validated properly
        If it is appropriate then we currently use the "data" size column as this is what Sqoop would extract
        Other columns are reserved_size, index_size and unused
        Assumption here on limited testing is that output is always in KB and suffixed as such. This might not be true
        SP_SPACEUSED is not valid for Azure Serverless SQL Pools
        """
        q = 'SP_SPACEUSED "{schema}.{table}"'.format(schema=schema, table=table_name)
        rows = self.execute_query_fetch_all(q, query_params=(schema, table_name))
        return (int(rows[0][3].replace("KB", "")) * 1024) if rows else None

    def is_view(self, schema, object_name):
        return self.view_exists(schema, object_name)

    def max_datetime_scale(self) -> int:
        """MSSQL supports up to milliseconds"""
        return 3

    def max_datetime_value(self):
        return datetime64("9999-12-31")

    def max_table_name_length(self):
        return MSSQL_MAX_TABLE_NAME_LENGTH

    def min_datetime_value(self):
        return datetime64("1753-01-01")

    def parallel_query_hint(self, query_parallelism):
        if query_parallelism:
            assert isinstance(query_parallelism, int)
            return "MAXDOP %s" % query_parallelism
        else:
            return ""

    def split_partition_high_value_string(self, hv_string):
        raise NotImplementedError(
            "MSSQL split_partition_high_value_string not implemented."
        )

    def schema_exists(self, schema) -> bool:
        sql = dedent(
            """\
                     SELECT schema_name
                     FROM information_schema.schemata
                     WHERE schema_name = '%s'"""
        )
        row = self.execute_query_fetch_one(
            sql, query_params=(schema,), log_level=VVERBOSE
        )
        return bool(row)

    def table_exists(self, schema, table_name) -> bool:
        sql = dedent(
            """\
                     SELECT table_name
                     FROM information_schema.tables
                     WHERE table_type = 'BASE TABLE'
                     AND table_schema = '%s'
                     AND table_name = '%s'"""
        )
        row = self.execute_query_fetch_one(
            sql, query_params=(schema, table_name), log_level=VVERBOSE
        )
        return bool(row)

    def to_frontend_literal(self, py_val, data_type=None) -> str:
        self._debug("Formatting %s literal: %s" % (type(py_val), repr(py_val)))
        self._debug("For data type: %s" % data_type)
        new_py_val = SynapseLiteral.format_literal(py_val, data_type=data_type)
        self._debug("Final %s literal: %s" % (type(py_val), str(new_py_val)))
        return new_py_val

    def view_exists(self, schema, view_name) -> bool:
        sql = dedent(
            """\
                     SELECT table_name
                     FROM information_schema.tables
                     WHERE table_type = 'VIEW'
                     AND table_schema = '%s'
                     AND table_name = '%s'"""
        )
        row = self.execute_query_fetch_one(
            sql, query_params=(schema, view_name), log_level=VVERBOSE
        )
        return bool(row)
