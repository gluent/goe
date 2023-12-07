#! /usr/bin/env python3
""" NetezzaFrontendApi: Library for logic/interaction with an Netezza frontend.
    Implements abstract methods from FrontendApiInterface.
    LICENSE_TEXT
"""

# Standard Library
import logging
from datetime import datetime
from typing import Any, Dict, List, Union

# Third Party Libraries
import pyodbc
from numpy import datetime64

# Gluent
from goe.offload.frontend_api import (
    FETCH_ACTION_ALL,
    FETCH_ACTION_CURSOR,
    FETCH_ACTION_ONE,
    FrontendApiInterface,
    extract_connection_details_from_dsn,
)
from goe.offload.netezza.netezza_column import (
    NETEZZA_TYPE_CHARACTER_VARYING,
    NetezzaColumn,
)
from goe.offload.offload_constants import NETEZZA_FRONTEND_CAPABILITIES
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_source_table import HYBRID_ALL_OBJECTS
from goe.orchestration.execution_id import ExecutionId
from goe.util.misc_functions import double_quote_sandwich

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

NETEZZA_MAX_TABLE_NAME_LENGTH = 128


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def setup_netezza_session(server, port, database, username, password):
    """Take a set of connection properties and return a Netezza connection"""
    assert server
    assert port
    assert database
    assert username
    assert password
    # TODO NetezzaSQL below should be moved to a FRONTEND_ODBC_DRIVER_NAME config attribute.
    logger.debug(
        "DRIVER={NetezzaSQL};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s"
        % (server, port, database, username)
    )
    db_conn = pyodbc.connect(
        "DRIVER={NetezzaSQL};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;"
        % (server, port, database, username, password)
    )
    db_conn.setencoding(str, "utf-8")
    db_conn.setencoding(str, "utf-8")
    db_conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    db_conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
    return db_conn


###########################################################################
# NetezzaFrontendApi
###########################################################################


class NetezzaFrontendApi(FrontendApiInterface):
    """Netezza FrontendApi implementation"""

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
        super(NetezzaFrontendApi, self).__init__(
            connection_options,
            frontend_type,
            messages,
            conn_user_override=conn_user_override,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )

        logger.info("NetezzaFrontendApi")
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
        self._db_conn = setup_netezza_session(
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
        raise NotImplementedError("Netezza _create_table_sql_text not implemented.")

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
                        run_sql, *self._to_native_query_params(query_params)
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
        TODO Do we have a profile on Netezza? If yes we need to implement based on profile argument.
        TODO Do we have an equivalent to cx_Oracle.connection.action on Netezza?
        """
        assert sql

        if self._dry_run and not_when_dry_running:
            return None

        t1 = datetime.now().replace(microsecond=0)

        self._open_cursor()
        try:
            # TODO Do we have session options on Netezza? Need to implement here if so.
            # self._execute_session_options(query_options, log_level=log_level)
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
            )
            if query_params:
                self._db_curs.execute(sql, *self._to_native_query_params(query_params))
            else:
                self._db_curs.execute(sql)
            if fetch_action == FETCH_ACTION_ALL:
                rows = self._db_curs.fetchall()
            elif fetch_action == FETCH_ACTION_ONE:
                rows = self._db_curs.fetchone()
            else:
                rows = None

            if time_sql:
                t2 = datetime.now().replace(microsecond=0)
                self._log("Elapsed time: %s" % (t2 - t1), detail=VVERBOSE)
        finally:
            if fetch_action in (FETCH_ACTION_ALL, FETCH_ACTION_ONE):
                self._close_cursor()

        if fetch_action == FETCH_ACTION_CURSOR:
            return self._db_curs
        else:
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
        raise NotImplementedError("_executemany_dml() is not implemented for Netezza")

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
            "_fast_executemany_dml() is not implemented for Netezza"
        )

    def _frontend_capabilities(self):
        return NETEZZA_FRONTEND_CAPABILITIES

    def _gdp_db_component_version(self):
        return None

    def _open_cursor(self):
        if self._db_conn:
            self._db_curs = self._db_conn.cursor()
        else:
            self._db_curs = None

    def _to_native_query_params(self, query_params):
        return self._odbc_to_native_query_params(query_params)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def close(self, force=False):
        self._disconnect(force=force)

    def agg_validate_sample_column_names(self, schema, table_name, num_required: int=5) -> list:
        raise NotImplementedError('Netezza agg_validate_sample_column_names() not implemented.')

    def create_new_connection(self, user_name, user_password, trace_action_override=None):
        self._debug("Making new connection with user %s" % user_name)
        server, port, database = extract_connection_details_from_dsn(
            self._connection_options.rdbms_dsn
        )
        client = setup_netezza_session(server, port, database, user_name, user_password)
        # client.autocommit = True
        return client

    def enclose_identifier(self, identifier):
        if identifier is None:
            return None
        return double_quote_sandwich(identifier)

    def enclose_query_hints(self, hint_contents):
        raise NotImplementedError("Netezza enclose_query_hints() not implemented.")

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
    ):
        raise NotImplementedError("Netezza execute_function not implemented.")

    def format_query_parameter(self, param_name):
        """No named parameters so always return "?" """
        return "?"

    def generic_string_data_type(self) -> str:
        return NETEZZA_TYPE_CHARACTER_VARYING

    def frontend_version(self):
        raise NotImplementedError("Netezza frontend_version() not implemented.")

    def gen_column_object(self, column_name, **kwargs):
        return NetezzaColumn(column_name, **kwargs)

    def get_columns(self, schema, table_name):
        # TODO SS@2017-09-04: hidden_column - do we have the concept of hidden_columns in Netezza? If not remove from NetezzaColumn.from_netezza # noqa: E501
        logger.debug("_get_column_details: %s, %s" % (schema, table_name))
        q = """
                    SELECT  column_name
                    ,       ordinal_position                                    AS column_id
                    ,       is_nullable                                         AS nullable
                    ,       CASE WHEN instr(data_type,'(') > 0
                              THEN substr(data_type,1,instr(data_type,'(')-1)
                              ELSE data_type
                            END                                                 AS data_type
                    ,       numeric_precision                                   AS data_precision
                    ,       numeric_scale                                       AS data_scale
                    ,       NULL                                                AS byte_length
                    ,       coldefault                                          AS data_default
                    ,       NULL                                                AS hidden_column
                    ,       character_maximum_length                            AS char_length
                    FROM    information_schema.columns
                            INNER JOIN
                            _v_relation_column
                            ON (    information_schema.columns.table_catalog = _v_relation_column.database
                                AND information_schema.columns.table_name    = _v_relation_column.name
                                AND information_schema.columns.column_name   = _v_relation_column.attname)
                    WHERE   _v_relation_column.owner = UPPER(?)
                    AND     _v_relation_column.name = UPPER(?)
                    ORDER BY
                            ordinal_position ASC"""

        cols = []
        self._db_curs.execute(q, schema, table_name)
        for row in self._db_curs.fetchall():
            col = NetezzaColumn.from_netezza(row)
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
        raise NotImplementedError("Netezza get_distinct_column_values not implemented.")

    def get_object_ddl(
        self,
        schema,
        object_name,
        object_type,
        as_list=False,
        terminate_sql=False,
        remap_schema=None,
    ):
        raise NotImplementedError("Netezza get_object_ddl not implemented.")

    def get_offloadable_schemas(self):
        raise NotImplementedError("Netezza get_offloadable_schemas is not implemented.")

    def get_command_step_codes(self) -> list:
        raise NotImplementedError("Netezza get_command_step_codes is not implemented.")

    def get_command_executions(self) -> List[Dict[str, Union[str, Any]]]:
        raise NotImplementedError("Netezza get_command_executions is not implemented.")

    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, Any]]:
        raise NotImplementedError(
            "Netezza get_command_execution_status is not implemented."
        )

    def get_command_execution_steps(
        self, execution_id: ExecutionId
    ) -> List[Dict[str, Union[str, Any]]]:
        raise NotImplementedError(
            "Netezza get_command_execution_steps is not implemented."
        )

    def get_partition_column_names(self, schema, table_name, conv_fn=None):
        raise NotImplementedError("Netezza get_partition_column_names not implemented.")

    def get_partition_columns(self, schema, table_name):
        raise NotImplementedError("Netezza get_partition_column_names not implemented.")

    def get_primary_key_column_names(self, schema, table_name):
        q = """
            SELECT ATTNAME
            FROM   _v_relation_keydata
            WHERE  owner = UPPER(?)
            AND    relation = UPPER(?)
            AND    contype = ?
            ORDER BY conseq"""

        rows = self.execute_query_fetch_all(q, query_params=[schema, table_name, "p"])
        return [_[0] for _ in rows] if rows else []

    def get_schema_tables(self, schema_name):
        raise NotImplementedError("Netezza get_schema_tables is not implemented.")

    def get_session_option(self, option_name):
        raise NotImplementedError("Netezza get_session_option not implemented.")

    def get_session_user(self) -> str:
        raise NotImplementedError("Netezza get_session_user not implemented.")

    def get_subpartition_column_names(self, schema, table_name, conv_fn=None):
        raise NotImplementedError(
            "Netezza get_subpartition_column_names not implemented."
        )

    def get_subpartition_columns(self, schema, table_name):
        raise NotImplementedError("Netezza get_subpartition_columns not implemented.")

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
        raise NotImplementedError("Netezza get_table_row_count not implemented.")

    def get_table_size(self, schema, table_name):
        q = """SELECT used_bytes/POW(1024,3)
               FROM   _v_table_storage_stat
               WHERE  owner = UPPER(?)
               AND    tablename = UPPER(?)"""
        self._db_curs.execute(q, schema, table_name)
        rows = self._db_curs.fetchall()
        return rows[0][0] if rows else None

    def is_view(self, schema, object_name):
        return self.view_exists(schema, object_name)

    def max_datetime_scale(self) -> int:
        """Netezza supports up to microseconds"""
        return 6

    def max_datetime_value(self):
        return datetime64("9999-12-31")

    def max_table_name_length(self):
        return NETEZZA_MAX_TABLE_NAME_LENGTH

    def min_datetime_value(self):
        return datetime64("0001-01-01")

    def parallel_query_hint(self, query_parallelism):
        raise NotImplementedError("Netezza parallel_query_hint() not implemented.")

    def split_partition_high_value_string(self, hv_string):
        raise NotImplementedError(
            "Netezza split_partition_high_value_string not implemented."
        )

    def schema_exists(self, schema) -> bool:
        raise NotImplementedError("Netezza schema_exists() not implemented.")

    def table_exists(self, schema, table_name) -> bool:
        q = "SELECT tablename FROM _v_table WHERE owner = UPPER(?) AND tablename = UPPER(?)"
        self._db_curs.execute(q, schema, table_name)
        rows = self._db_curs.fetchall()
        return bool(rows)

    def to_frontend_literal(self, py_val, data_type=None) -> str:
        raise NotImplementedError("Netezza to_frontend_literal() not implemented.")

    def view_exists(self, schema, view_name) -> bool:
        q = "SELECT tablename FROM _v_view WHERE owner = UPPER(?) AND viewname = UPPER(?)"
        self._db_curs.execute(q, schema, view_name)
        rows = self._db_curs.fetchall()
        return bool(rows)
