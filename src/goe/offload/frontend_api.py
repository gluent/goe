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

""" FrontendApiInterface: Library for logic/interaction with an RDBMS frontend.
    This module enforces an interface with common, high level, methods and an implementation
    for each supported RDBMS, e.g. Oracle, MSSQL
"""

import logging
from abc import ABCMeta, abstractmethod

from goe.offload.column_metadata import (
    get_column_names,
    match_table_column,
    valid_column_list,
)
from goe.offload.offload_constants import (
    CAPABILITY_CANONICAL_DATE,
    CAPABILITY_CANONICAL_FLOAT,
    CAPABILITY_CANONICAL_TIME,
    CAPABILITY_CASE_SENSITIVE,
    CAPABILITY_GOE_HAS_DB_CODE_COMPONENT,
    CAPABILITY_GOE_JOIN_PUSHDOWN,
    CAPABILITY_GOE_LIST_PARTITION_APPEND,
    CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY,
    CAPABILITY_GOE_OFFLOAD_STATUS_REPORT,
    CAPABILITY_GOE_SCHEMA_SYNC,
    CAPABILITY_HYBRID_SCHEMA,
    CAPABILITY_LOW_HIGH_VALUE_FROM_STATS,
    CAPABILITY_NAN,
    CAPABILITY_PARAMETERIZED_QUERIES,
    CAPABILITY_SCHEMA_EVOLUTION,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


class FrontendApiException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

FETCH_ACTION_ALL = "all"
FETCH_ACTION_ONE = "one"
FETCH_ACTION_CURSOR = "cursor"

FRONTEND_TRACE_ID = "GOE"
FRONTEND_TRACE_MODULE = "GOE"
FRONTEND_TRACE_ACTION = "Main"

GET_DDL_TYPE_TABLE = "TABLE"
GET_DDL_TYPE_VIEW = "VIEW"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def extract_connection_details_from_dsn(rdbms_dsn):
    """Take a rdbms_dsn of the form: <server>:<port>;database=<database> and return server, port and database"""
    assert rdbms_dsn
    assert 0 not in [
        _ in rdbms_dsn for _ in [":", ";", "="]
    ], "rdbms_dsn must be of the form: <server>:<port>;database=<database>"
    server, port = rdbms_dsn.split(";")[0].split(":")
    database = rdbms_dsn.split(";")[1].split("=")[1]
    return server, port, database


###########################################################################
# QueryParameter
###########################################################################


class QueryParameter:
    def __init__(self, param_name, param_value):
        self.param_name = param_name
        self.param_value = param_value


###########################################################################
# BackendApiInterface
###########################################################################


class FrontendApiInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for RDBMS specific sub-classes"""

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
        """Abstract base class which acts as an interface for RDBMS specific sub-classes
        conn_user_name: An override for the user to connect as. connection_options may contain details for more
                        than one account because GOE may have multiple RDBMS accounts, for example the admin user
                        vs the hybrid schema. conn_user_name can be used if the non-default user should be active.
        dry_run: Read only mode
        do_not_connect: Prevents the object from automatically connecting, required for unit tests
        existing_connection: Allows re-use of an existing db connection. The onus is on the developer to
                             pass a connection that matches config in connection_options.
        """
        assert connection_options
        assert messages
        assert frontend_type
        self._connection_options = connection_options
        self._frontend_type = frontend_type
        self._messages = messages
        self._conn_user_override = conn_user_override
        self._existing_connection = existing_connection
        self._dry_run = dry_run
        self._trace_action = trace_action or FRONTEND_TRACE_ACTION
        self._sql_engine_name = frontend_type.title()

    ###########################################################################
    # ENFORCED PRIVATE METHODS
    ###########################################################################

    @abstractmethod
    def _create_table_sql_text(
        self,
        schema,
        table_name,
        column_list,
        partition_column_names,
        table_properties=None,
    ) -> str:
        """Return str DDL to create a table and cater for frontend specific details.
        column_list: A list of column objects for the table.
        partition_column_names: A list of column names for table partitioning, the columns must also
                                be in column_list.
        table_properties: a dict of frontend specific table options/attributes.
        """

    @abstractmethod
    def _frontend_capabilities(self):
        pass

    @abstractmethod
    def _execute_ddl_or_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
    ):
        """Run SQL that has no result set, e.g. DDL, sql can be a str or a list of SQLs.
        This function returns the SQL statements that were executed.
        Exception will be raised upon failure.
        query_options: key/value pairs for session settings.
        trace_action: Token to use in tracing information to override self._trace_action
        """

    @abstractmethod
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
        query_options: key/value pairs for session settings
        query_params: a list of QueryParameter objects
        log_level: None = no logging. Otherwise VERBOSE/VVERBOSE etc
        profile: If logging then log profile/statistics from the RDBMS
        time_sql: If logging then log SQL elapsed time
        not_when_dry_running: Some SQL will only work in execute mode, such as queries on a load table
        trace_action: Token to use in tracing information to override self._trace_action
        as_dict: When True each row is returned as a dict keyed by column name rather than a tuple/list.
        """

    @abstractmethod
    def _executemany_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        batch_size=None,
    ):
        """Run SQL that has no result set with multiple input records.
        This function returns the SQL statements that were executed.
        Exception will be raised upon failure.
        query_options: key/value pairs for session settings.
        query_params: Must be populated and a list of lists in order to drive the execute many operation.
        trace_action: Token to use in tracing information to override self._trace_action.
        batch_size: Whether to split query_params into batches when inserting. None means no batching.
        """

    @abstractmethod
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
        """Run SQL that has no result set with multiple input records using the fast_executemany option.
        This function returns the SQL statements that were executed.
        Exception will be raised upon failure.
        query_options: key/value pairs for session settings.
        query_params: Must be populated and a list of lists in order to drive the execute many operation.
        trace_action: Token to use in tracing information to override self._trace_action.
        batch_size: Whether to split query_params into batches when inserting. None means no batching.
        param_inputsizes: Certain requests will require the use of inputsizes to prevent exceptions.
        """

    @abstractmethod
    def _goe_db_component_version(self):
        """Return version of any in-database GOE code"""

    @abstractmethod
    def _to_native_query_params(self, query_params):
        """Convert list of QueryParameter objects to a structure compatible with
        the driver used for the specific frontend.
        """

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _log(self, msg, detail=None, ansi_code=None):
        """Write to offload log file"""
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail in (VERBOSE, VVERBOSE):
            logger.debug(msg)
        else:
            logger.info(msg)

    def _log_or_not(self, msg, log_level=VERBOSE):
        """Wrapper over _log() to cater for suppressing logging of SQLs"""
        if log_level is not None:
            self._log(msg, detail=log_level)

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def _create_table_columns_clause_common(self, column_list):
        """Trivial helper for SQL text consistency"""
        return "    \n,   ".join(
            [
                "%-32s %-20s" % (self.enclose_identifier(_.name), _.format_data_type())
                for _ in column_list
            ]
        )

    def _cursor_projection(self, cursor) -> list:
        """
        Returns a list of strings describing the projection of a cursor.
        Names are coerced to lower case to give a standard output across different frontend systems.
        This should be called after the query has been executed.
        """
        return [_[0].lower() for _ in cursor.description]

    def _cursor_row_to_dict(self, cursor_projection, row) -> dict:
        """
        Trivial function combining typical tuple/list row format with results of _cursor_projection() to
        generate a row dict.
        """
        return dict(zip(cursor_projection, row))

    def _fixed_session_parameters(self) -> dict:
        """Dictionary of session parameters for a given frontend. This top level method gives us an empty base"""
        return {}

    def _odbc_to_native_query_params(self, query_params):
        """Using a list for pyodbc binds which are then bound as execute() arguments.
        For convenience, if it is already a list then turning a blind eye and returning it, this allows code
        migrated here from OffloadSourceTable to remain unchanged.
        """
        assert isinstance(query_params, list)
        if query_params and isinstance(query_params[0], QueryParameter):
            param_values = []
            for qp in query_params:
                param_values.append(qp.param_value)
            return param_values
        else:
            return query_params

    def _upper_or_empty(self, s):
        return (s or "").upper()

    ###########################################################################
    # ENFORCED PUBLIC METHODS
    ###########################################################################

    @abstractmethod
    def close(self, force=False):
        """Free up any resources currently held"""

    @abstractmethod
    def agg_validate_sample_column_names(self, num_required: int = 5) -> list:
        """
        Return a list of column names suitable for aggreage validation.
        Mandatory first, last columns + top (num_required) "high cardinality" columns.
        """

    @abstractmethod
    def create_new_connection(
        self, user_name, user_password, trace_action_override=None
    ):
        """Creates a new low level client connection with an alternative username and password.
        This is primarily used when testing but is included in this class in order to keep connection
        specifics in a single place.
        We only support a simple username/password for this entry point.
        """

    @abstractmethod
    def enclose_identifier(self, identifier):
        """Surround an identifier in the correct character for the RDBMS"""

    @abstractmethod
    def enclose_query_hints(self, hint_contents):
        """Surround query hints with correct hint block start/end for the RDBMS.
        hint_contents is a string containing any number of hints or a list of hint strings.
        """

    @abstractmethod
    def enclosure_character(self):
        """Correct character for the RDBMS for enclose_identifier().

        Superceded by enclose_identifier(), ideally we shouldn't use enclosure_character() any longer.
        """

    @abstractmethod
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
        """Execute a function in the frontend system and return the result.

        Parameters are based on Oracle functionality so may need to change as new frontends are implemented.
        not_when_dry_running: Because we don't know if a frontend function is read-only or read/write we assume
                              we should run the function even in read-only mode. If the function is making changes
                              then the calling code should include not_when_dry_running=True.
        """

    @abstractmethod
    def format_query_parameter(self, param_name):
        """For engines that support query parameters this method prefixes or suffixes a parameter name.

        For example on Oracle it prefixes a ":" symbol.
        """

    @abstractmethod
    def frontend_version(self) -> str:
        """Return version of the frontend SQL engine in x.y.z format that can be used by GOEVersion()"""

    @abstractmethod
    def gen_column_object(self, column_name, **kwargs):
        """Returns a column object of the correct type for the frontend system"""

    @abstractmethod
    def generic_string_data_type(self) -> str:
        """Return data type to be used for generaic string based columns"""

    @abstractmethod
    def get_columns(self, schema, table_name):
        """Get a list of frontend column objects in the order they are stored by the frontend.
        Example might be:
            [OracleColumn(...), OracleColumn(...)]
        """

    @abstractmethod
    def get_db_unique_name(self) -> str:
        """Return the unique name for the frontend database"""

    @abstractmethod
    def get_distinct_column_values(
        self, schema, table_name, column_names, partition_name=None, order_results=False
    ):
        """Run SQL to get distinct values for a list of columns.
        column_names accepts a list of names or a single name.
        """

    @abstractmethod
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
        object_type: One of constants defined at the top of this module, e.g.: GET_DDL_TYPE_TABLE
        terminate_sql: Adds any executing character to the end of the SQL (e.g. a semi-colon)
        remap_schema: Replaces the actual schema (schema) with a different name (remap_schema)
        """

    @abstractmethod
    def get_partition_columns(self, schema, table_name):
        """Returns a list of column objects for partition columns"""

    @abstractmethod
    def get_primary_key_column_names(self, schema, table_name):
        """Get a list of frontend column names in the order they are stored by the frontend."""

    @abstractmethod
    def get_session_option(self, option_name):
        pass

    @abstractmethod
    def get_session_user(self) -> str:
        pass

    @abstractmethod
    def get_subpartition_columns(self, schema, table_name):
        """Returns a list of column objects for subpartition columns"""

    @abstractmethod
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
        filter_clause: Does NOT include "WHERE" because that could be backend specific
        not_when_dry_running: Gives us the option of not running an expensive COUNT in preview mode.
        """

    @abstractmethod
    def get_table_size(self, schema, table_name):
        """Return the size of the table in bytes"""

    @abstractmethod
    def is_view(self, schema, object_name) -> bool:
        """Is the underlying RDBMS object a view"""

    @abstractmethod
    def max_datetime_scale(self) -> int:
        """Return the maximum scale (number of decimal places for seconds) that can be stored by this RDBMS"""

    @abstractmethod
    def max_datetime_value(self):
        """Return the maximum datetime value that can be stored by this RDBMS as datetime64()"""

    @abstractmethod
    def max_table_name_length(self):
        pass

    @abstractmethod
    def min_datetime_value(self):
        """Return the minimum datetime value that can be stored by this RDBMS as datetime64()"""

    @abstractmethod
    def parallel_query_hint(self, query_parallelism):
        """Return an RDBMS specific hint to enable or disable parallelism as appropriate.
        Expect query_parallelism behaviour as below:
            None: No hint
            0, 1: Request no parallelism
            > 1: Request parallelism of query_parallelism
        """

    @abstractmethod
    def schema_exists(self, schema) -> bool:
        """Return True/False if a schema/dataset/database exists
        The equivalent BackendApi method is called database_exists(), database is a misleading term and ideally
        we would not have used it.
        """

    @abstractmethod
    def split_partition_high_value_string(self, hv_string):
        """Break up a partition high value string into a list of individual parts.
        May only ever apply to Oracle but this gives us the opportunity to implement for other frontends.
        """

    @abstractmethod
    def table_exists(self, schema, table_name) -> bool:
        pass

    @abstractmethod
    def to_frontend_literal(self, py_val, data_type=None) -> str:
        """Translate a Python value to a frontend literal.
        Examples where this may act are dates (which may need specific formatting) and
        string (need quoting with a specific character).
        Other types are likely just a pass through.
        """

    @abstractmethod
    def view_exists(self, schema, view_name) -> bool:
        pass

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def create_table(
        self,
        schema,
        table_name,
        column_list,
        partition_column_names=None,
        table_properties=None,
        log_level=VERBOSE,
    ) -> list:
        """Create a table and cater for frontend specific details, we need to pass all parameters even if they
        don't apply to certain backends but some may be ignored depending on the system involved:
            column_list: A list of column objects for the table.
            partition_column_names: A list of column names for table partitioning, the columns must also
                                    be in column_list.
            table_properties: A dict of frontend specific table options/attributes.
                              This parameter is a bit free-form. Each frontend implementation will need to
                              control that key/value pairs are valid.
        Returns a list of any SQL statements executed.
        """
        assert schema
        assert table_name
        assert column_list
        assert valid_column_list(column_list), (
            "Incorrectly formed column_list: %s" % column_list
        )
        if partition_column_names:
            assert isinstance(partition_column_names, list)
            assert isinstance(partition_column_names[0], str)
        if table_properties:
            assert isinstance(table_properties, dict)

        sql = self._create_table_sql_text(
            schema,
            table_name,
            column_list,
            partition_column_names,
            table_properties=table_properties,
        )

        return self.execute_ddl(sql, log_level=log_level)

    def enclose_object_reference(self, schema, object_name):
        """Correctly use enclose_identifier() on a composite reference to an object.
        For example:
            Oracle: "schema"."object"
        This is assuming all frontends follow a pattern of envlosing each token independently, if we come across
        a frontend that uses something like BigQuery, e.g. `ownerobject`, then the frontend implementation will
        need to override this method.
        """
        return ".".join(
            [self.enclose_identifier(schema), self.enclose_identifier(object_name)]
        )

    def execute_ddl(
        self, sql, query_options=None, log_level=VERBOSE, trace_action=None
    ) -> list:
        """Simple wrapper over _execute_ddl_or_dml() to ensure we always get a return value"""
        return_sqls = self._execute_ddl_or_dml(
            sql,
            query_options=query_options,
            log_level=log_level,
            trace_action=trace_action,
        )
        assert return_sqls is not None
        return return_sqls

    def execute_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
    ) -> list:
        return_sqls = self._execute_ddl_or_dml(
            sql,
            query_options=query_options,
            log_level=log_level,
            trace_action=trace_action,
            query_params=query_params,
        )
        assert return_sqls is not None
        return return_sqls

    def execute_query_fetch_all(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=False,
        trace_action=None,
        as_dict=False,
    ) -> list:
        """For parameter descriptions see: _execute_query_fetch_x"""
        rows = self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_ALL,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            profile=profile,
            trace_action=trace_action,
            as_dict=as_dict,
        )
        return rows

    def execute_query_fetch_one(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=False,
        trace_action=None,
        as_dict=False,
    ):
        """For parameter descriptions see: _execute_query_fetch_x"""
        row = self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_ONE,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            profile=profile,
            trace_action=trace_action,
            as_dict=as_dict,
        )
        return row

    def execute_query_get_cursor(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        trace_action=None,
    ):
        """Run a query and return the cursor to allow the calling program to handle fetching.
        execute_query_get_cursor() exposes a low level of detail to higher level code so is seldom used.
        Advise not to use this method unless there are no alternatives.
        """
        return self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_CURSOR,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            trace_action=trace_action,
        )

    def executemany_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        batch_size=None,
    ) -> list:
        """Run SQL that has no result set with multiple input records.
        For executemany we need query_params to be populated and a list of lists.
        sql is a single SQL statement. We trust the developer to add parameter markers matching query_params.
        """
        assert sql
        assert isinstance(sql, str)
        assert query_params
        assert isinstance(query_params, list)
        assert isinstance(query_params[0], list)
        return_sqls = self._executemany_dml(
            sql,
            query_options=query_options,
            log_level=log_level,
            trace_action=trace_action,
            query_params=query_params,
            batch_size=batch_size,
        )
        assert return_sqls is not None
        return return_sqls

    def fast_executemany_dml(
        self,
        sql,
        query_options=None,
        log_level=VERBOSE,
        trace_action=None,
        query_params=None,
        batch_size=None,
        param_inputsizes=None,
    ) -> list:
        """Run SQL that has no result set with multiple input records using the fast_executemany option.
        For executemany we need query_params to be populated and a list of lists.
        sql is a single SQL statement. We trust the developer to add parameter markers matching query_params.
        Certain requests will require the use of inputsizes to prevent exceptions.
        """
        assert sql
        assert isinstance(sql, str)
        assert query_params
        assert isinstance(query_params, list)
        assert isinstance(query_params[0], list)
        assert isinstance(param_inputsizes, list)
        return_sqls = self._fast_executemany_dml(
            sql,
            query_options=query_options,
            log_level=log_level,
            trace_action=trace_action,
            query_params=query_params,
            batch_size=batch_size,
            param_inputsizes=param_inputsizes,
        )
        assert return_sqls is not None
        return return_sqls

    def fetchmany_takes_fetch_size(self):
        """DBAPI states fetchmany should take a fetch size but cx-Oracle doesn't do that, hence this method."""
        return True

    def frontend_db_name(self):
        """Some frontends have overrides due to stylistic formatting, e.g. MSSQL"""
        return self._frontend_type.capitalize()

    def get_table_default_parallelism(self, schema, table_name):
        """Return the table level default parallelism. Oracle has an override for this."""
        # Assume no table level default unless an implementation overrides.
        return None

    def goe_db_component_version(self):
        """Return version of any in-database GOE code"""
        if self.goe_has_db_code_component():
            return self._goe_db_component_version()
        else:
            return None

    def get_column(self, schema, table_name, column_name):
        """Get a single column from get_columns()"""
        return match_table_column(column_name, self.get_columns(schema, table_name))

    def get_column_names(self, schema, table_name, conv_fn=None):
        return get_column_names(self.get_columns(schema, table_name), conv_fn=conv_fn)

    def get_partition_column_names(self, schema, table_name, conv_fn=None):
        """Some backends may override this to avoid double call on get_columns()"""
        return get_column_names(
            self.get_partition_columns(schema, table_name), conv_fn=conv_fn
        )

    def get_subpartition_column_names(self, schema, table_name, conv_fn=None):
        """Some backends may override this to avoid double call on get_columns()"""
        return get_column_names(
            self.get_partition_columns(schema, table_name), conv_fn=conv_fn
        )

    def get_table_ddl(
        self, schema, table_name, as_list=False, terminate_sql=False, remap_schema=None
    ):
        """Return CREATE TABLE DDL as a string (or a list of strings split on CR if as_list=True)
        terminate_sql: Adds any executing character to the end of the SQL (e.g. a semi-colon)
        remap_schema: Replaces the actual schema (schema) with a different name (remap_schema)
        """
        return self.get_object_ddl(
            schema,
            table_name,
            GET_DDL_TYPE_TABLE,
            as_list=as_list,
            terminate_sql=terminate_sql,
            remap_schema=remap_schema,
        )

    ###########################################################################
    # PUBLIC CAPABILITY METHODS
    ###########################################################################

    def is_capability_supported(self, capability_constant) -> bool:
        assert capability_constant in self._frontend_capabilities(), (
            "Unknown capability: %s" % capability_constant
        )
        return self._frontend_capabilities()[capability_constant]

    def canonical_date_supported(self) -> bool:
        """Does the frontend have a pure date data type (no time part).
        Note that there is an Impala override for this.
        """
        return self.is_capability_supported(CAPABILITY_CANONICAL_DATE)

    def canonical_float_supported(self) -> bool:
        """Does the frontend have a pure 4 byte binary floating point data type"""
        return self.is_capability_supported(CAPABILITY_CANONICAL_FLOAT)

    def canonical_time_supported(self) -> bool:
        """Does the frontend have a pure time data type (no date part).
        Note that there is an Impala override for this.
        """
        return self.is_capability_supported(CAPABILITY_CANONICAL_TIME)

    def case_sensitive_identifiers(self) -> bool:
        """Does the frontend have case sensitive db/table name identifiers"""
        return self.is_capability_supported(CAPABILITY_CASE_SENSITIVE)

    def goe_has_db_code_component(self) -> bool:
        """Does the frontend have any in-database GOE code"""
        return self.is_capability_supported(CAPABILITY_GOE_HAS_DB_CODE_COMPONENT)

    def goe_join_pushdown_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_GOE_JOIN_PUSHDOWN)

    def goe_lpa_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_GOE_LIST_PARTITION_APPEND)

    def goe_offload_status_report_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_GOE_OFFLOAD_STATUS_REPORT)

    def goe_schema_sync_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_GOE_SCHEMA_SYNC)

    def low_high_value_from_stats_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_LOW_HIGH_VALUE_FROM_STATS)

    def goe_multi_column_incremental_key_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_GOE_MULTI_COLUMN_INCREMENTAL_KEY)

    def nan_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_NAN)

    def nanoseconds_supported(self) -> bool:
        return bool(self.max_datetime_scale() >= 9)

    def parameterized_queries_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_PARAMETERIZED_QUERIES)

    def schema_evolution_supported(self) -> bool:
        return self.is_capability_supported(CAPABILITY_SCHEMA_EVOLUTION)
