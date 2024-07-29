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

""" BackendApi: Library for logic/interaction with a remote backend.
    This module enforces an interface with common, high level, methods and an implementation
    for each supported remote system, e.g. Impala, Hive, Google BigQuery.
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

from abc import ABCMeta, abstractmethod
import logging

from goe.filesystem.goe_dfs import (
    OFFLOAD_FS_SCHEME_S3A,
    OFFLOAD_FS_SCHEME_WASB,
    OFFLOAD_FS_SCHEME_WASBS,
    OFFLOAD_FS_SCHEME_ADL,
    OFFLOAD_FS_SCHEME_ABFS,
    OFFLOAD_FS_SCHEME_ABFSS,
    OFFLOAD_FS_SCHEME_HDFS,
    OFFLOAD_FS_SCHEME_INHERIT,
    OFFLOAD_FS_SCHEME_GS,
)
from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    get_column_names,
    is_synthetic_partition_column,
    match_table_column,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_constants import (
    CAPABILITY_BUCKET_HASH_COLUMN,
    CAPABILITY_CANONICAL_DATE,
    CAPABILITY_CANONICAL_FLOAT,
    CAPABILITY_CANONICAL_TIME,
    CAPABILITY_CASE_SENSITIVE,
    CAPABILITY_COLUMN_STATS_SET,
    CAPABILITY_CREATE_DB,
    CAPABILITY_DROP_COLUMN,
    CAPABILITY_FS_SCHEME_ABFS,
    CAPABILITY_FS_SCHEME_ADL,
    CAPABILITY_FS_SCHEME_S3A,
    CAPABILITY_FS_SCHEME_GS,
    CAPABILITY_FS_SCHEME_HDFS,
    CAPABILITY_FS_SCHEME_INHERIT,
    CAPABILITY_FS_SCHEME_WASB,
    CAPABILITY_GOE_COLUMN_TRANSFORMATIONS,
    CAPABILITY_GOE_JOIN_PUSHDOWN,
    CAPABILITY_GOE_MATERIALIZED_JOIN,
    CAPABILITY_GOE_PARTITION_FUNCTIONS,
    CAPABILITY_GOE_SEQ_TABLE,
    CAPABILITY_GOE_UDFS,
    CAPABILITY_LOAD_DB_TRANSPORT,
    CAPABILITY_NAN,
    CAPABILITY_NANOSECONDS,
    CAPABILITY_NOT_NULL_COLUMN,
    CAPABILITY_PARAMETERIZED_QUERIES,
    CAPABILITY_PARTITION_BY_COLUMN,
    CAPABILITY_PARTITION_BY_STRING,
    CAPABILITY_QUERY_SAMPLE_CLAUSE,
    CAPABILITY_RANGER,
    CAPABILITY_SCHEMA_EVOLUTION,
    CAPABILITY_SENTRY,
    CAPABILITY_SORTED_TABLE,
    CAPABILITY_SORTED_TABLE_MODIFY,
    CAPABILITY_SQL_MICROSECOND_PREDICATE,
    CAPABILITY_SYNTHETIC_PARTITIONING,
    CAPABILITY_TABLE_STATS_COMPUTE,
    CAPABILITY_TABLE_STATS_GET,
    CAPABILITY_TABLE_STATS_SET,
    DBTYPE_BIGQUERY,
    DBTYPE_IMPALA,
    DBTYPE_HIVE,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SPARK,
    DBTYPE_SYNAPSE,
)
from goe.offload.offload_transport_functions import run_os_cmd
from goe.util.misc_functions import str_summary_of_self
from goe.util.password_tools import PasswordTools


class BackendApiException(Exception):
    pass


class BackendStatsException(Exception):
    pass


class MissingSequenceTableException(Exception):
    pass


class BackendApiConnectionException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

IMPALA_SHUFFLE_HINT = "SHUFFLE"
IMPALA_NOSHUFFLE_HINT = "NOSHUFFLE"

VALID_REMOTE_DB_TYPES = [
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SPARK,
    DBTYPE_SYNAPSE,
]

FETCH_ACTION_ALL = "all"
FETCH_ACTION_ONE = "one"
FETCH_ACTION_CURSOR = "cursor"

SORT_COLUMNS_UNLIMITED = 100

# Reporting info dictionary keys
REPORT_ATTR_BACKEND_CLASS = "backend_class"
REPORT_ATTR_BACKEND_TYPE = "backend_type"
REPORT_ATTR_BACKEND_DISPLAY_NAME = "backend_display_name"
REPORT_ATTR_BACKEND_HOST_INFO_TYPE = "backend_host_info_type"
REPORT_ATTR_BACKEND_HOST_INFO = "backend_host_info"


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# UdfDetails
###########################################################################


class UdfParameter:
    def __init__(self, name, data_type):
        assert data_type
        self.name = name
        self.data_type = data_type

    def __str__(self):
        return str_summary_of_self(self)

    def __repr__(self):
        return str_summary_of_self(self)


class UdfDetails:
    def __init__(self, db_name, udf_name, return_type, parameters):
        assert db_name
        assert udf_name
        if return_type:
            assert isinstance(return_type, str)
        if parameters:
            assert isinstance(parameters, list)
            assert isinstance(parameters[0], UdfParameter)
        self.db_name = db_name
        self.udf_name = udf_name
        self.return_type = return_type
        self.parameters = parameters

    def parameter_spec_string(self):
        """Return the parameters as a str for displaying to an end user"""

        def arg_to_str(arg):
            return " ".join([(arg.name or ""), (arg.data_type or "")])

        return ",".join(arg_to_str(_) for _ in (self.parameters or []))

    def __str__(self):
        return str_summary_of_self(self)

    def __repr__(self):
        return str_summary_of_self(self)


###########################################################################
# BackendApiInterface
###########################################################################


class BackendApiInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for backend specific sub-classes
    no_caching: Prevents state from being cached between calls. When testing we don't want caching
    do_not_connect: Prevents the object from automatically connecting to a backend, required for unit tests
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
        assert connection_options
        assert messages
        self._connection_options = connection_options
        self._backend_type = backend_type
        self._messages = messages
        self._dry_run = dry_run
        self._no_caching = no_caching
        self._sql_engine_name = "Backend"
        self._global_session_parameters = self._gen_global_session_parameters(
            connection_options.backend_session_parameters,
            log_clashes=bool(not do_not_connect),
        )
        self._valid_schemes = None

    def __del__(self):
        self.close()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _log(self, msg, detail=None, ansi_code=None, no_log_items=None):
        """Write to offload log file.
        no_log_items: See self._validate_no_log_items() for description
        """
        no_log_items = self._validate_no_log_items(no_log_items)
        if no_log_items:
            log_msg = msg
            for itm in no_log_items:
                log_msg = log_msg.replace(itm["item"], itm.get("sub", "?"))
        else:
            log_msg = msg
        self._messages.log(log_msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(log_msg)
        else:
            logger.info(log_msg)

    def _log_or_not(self, msg, log_level=VERBOSE, no_log_items=None):
        """Wrapper over _log() to cater for suppressing logging of SQLs"""
        if log_level is not None:
            self._log(msg, detail=log_level, no_log_items=no_log_items)

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def _warning(self, msg):
        self._messages.warning(msg)
        logger.warn(msg)

    def _apply_decimal_padding_digits(
        self, column_name, data_precision, data_scale, decimal_padding_digits
    ):
        """Add decimal_padding_digits to scale if precision has headroom then pad precision if it has headroom for
        that too.
        Currently this has no max for scale, i.e. assumes the max is same as max precision. This may not be true
        for all backends.
        Returns a tuple of the adjusted precision & scale
        """
        if not decimal_padding_digits:
            return data_precision, data_scale

        if data_precision is None or data_scale is None:
            self._log(
                "No padding applied to %s for precision/scale: %s/%s"
                % (column_name, data_precision, data_scale),
                detail=VVERBOSE,
            )
            return data_precision, data_scale

        self._debug("Aligning precision/scale for %s" % column_name)

        new_precision = data_precision
        new_scale = data_scale

        if new_precision <= self.max_decimal_precision() - decimal_padding_digits:
            # Change precision after scale
            new_scale += min(
                decimal_padding_digits, self.max_decimal_precision() - new_precision
            )
            new_precision += min(
                decimal_padding_digits, self.max_decimal_precision() - new_precision
            )

        if new_precision < self.max_decimal_precision():
            new_precision += min(
                decimal_padding_digits, self.max_decimal_precision() - new_precision
            )

        if new_precision != data_precision or new_scale != data_scale:
            self._log(
                "Padded precision/scale for %s in line with DECIMAL_PADDING_DIGITS=%s: %s,%s -> %s,%s"
                % (
                    column_name,
                    decimal_padding_digits,
                    data_precision,
                    data_scale,
                    new_precision,
                    new_scale,
                ),
                detail=VVERBOSE,
            )
        return new_precision, new_scale

    def _create_table_columns_clause_common(self, column_list, external=False):
        """Trivial helper for SQL text consistency"""
        sql_cols = [
            (
                self.enclose_identifier(_.name),
                _.format_data_type(),
                self._create_table_column_nn_clause_common(_, external=external),
            )
            for _ in column_list
        ]
        max_name = max(len(_[0]) for _ in sql_cols)
        col_template = f"%-{max_name}s %s%s"
        return "    " + "\n,   ".join(
            [col_template % (_[0], _[1], f" {_[2]}" if _[2] else "") for _ in sql_cols]
        )

    def _create_table_column_nn_clause_common(
        self, column: ColumnMetadataInterface, external: bool = False
    ) -> str:
        """
        Return clause for a NOT NULL column if the backend supports it and it is not an external table.
        We don't want the constraint on external tables because we want to stage the data and then validate correctness.
        nullable of None is treated as True. i.e. we can't be sure of NOT NULL so go with the safe option.
        """
        if (
            not self.not_null_column_supported()
            or column.nullable
            or column.nullable is None
            or external
        ):
            return ""
        return "NOT NULL"

    def _cursor_projection(self, cursor) -> list:
        """
        Returns a list of strings describing the projection of a cursor.
        Names are coerced to lower case to give a standard output across different backend systems.
        This should be called after the query has been executed.
        """
        return [_[0].lower() for _ in cursor.description]

    def _cursor_row_to_dict(self, cursor_projection, row) -> dict:
        """
        Trivial function combining typical tuple/list row format with results of _cursor_projection() to
        generate a row dict.
        """
        return dict(zip(cursor_projection, row))

    def _decrypt_password(self, password):
        if self._connection_options.password_key_file:
            pass_tool = PasswordTools()
            goe_key = pass_tool.get_password_key_from_key_file(
                self._connection_options.password_key_file
            )
            self._log(
                "Decrypting %s password" % self.backend_db_name(), detail=VVERBOSE
            )
            clear_password = pass_tool.b64decrypt(password, goe_key)
            return clear_password
        else:
            return password

    @staticmethod
    def _fixed_session_parameters():
        """Dictionary of session parameters for a given backend. This top level method gives us an empty base"""
        return {}

    def _format_select_projection(self, select_expr_tuples):
        """Return string of column-expr AS alias pairs formatted consistently across all usages"""
        return "\n,      ".join(
            "{} AS {}".format(e, self.enclose_identifier(n))
            for e, n in select_expr_tuples
        )

    def _gen_max_column_values_sql(
        self,
        db_name,
        table_name,
        column_name_list,
        filter_clause=None,
        columns_to_cast_to_string=None,
    ):
        """Produce a SQL statement that selects a MAX row from the underlying table.
        Returns None if no data selected.
        The columns_to_cast_to_string parameter is because our HiveServer2 DBAPI does not support nanoseconds
        therefore we bring certain data types back as strings to avoid truncating to milliseconds.
        filter_clause: Option WHERE clause

        NOTE: Some backends (e.g. Synapse) may override this if the "common" syntax used here is incompatible.
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
            sql = "SELECT %s FROM %s%s GROUP BY %s ORDER BY %s LIMIT 1" % (
                proj_cols,
                self.enclose_object_reference(db_name, table_name),
                where,
                cols,
                order_by,
            )
        return sql

    def _gen_global_session_parameters(
        self, backend_session_parameters, log_clashes=True
    ):
        """Merge user defined session parameters with our fixed session parameters.
        We remove any fixed parameters from custom list in a case insensitive way to prevent duplicates.
        Apply fixed parameters second so they have precedence.
        """
        global_session_parameters = {}
        if backend_session_parameters:
            global_session_parameters.update(backend_session_parameters)
        if self._fixed_session_parameters():
            fixed_keys = [
                _.lower() for _ in list(self._fixed_session_parameters().keys())
            ]
            for key_to_delete in [
                _
                for _ in list(global_session_parameters.keys())
                if _.lower() in fixed_keys
            ]:
                if log_clashes:
                    self._log(
                        "Removing OFFLOAD_BACKEND_SESSION_PARAMETERS[%s] because of fixed parameter clash"
                        % key_to_delete,
                        detail=VVERBOSE,
                    )
                del global_session_parameters[key_to_delete]
            global_session_parameters.update(self._fixed_session_parameters())
        return global_session_parameters

    def _gen_insert_select_sql_assertions(
        self,
        db_name,
        table_name,
        from_db_name,
        from_table_name,
        select_expr_tuples,
        partition_expr_tuples,
        filter_clauses,
        from_object_override,
    ):
        assert db_name and table_name
        assert (from_db_name and from_table_name) or from_object_override
        assert select_expr_tuples
        assert isinstance(select_expr_tuples, list), "%s is not list" % type(
            select_expr_tuples
        )
        assert isinstance(select_expr_tuples[0], tuple), "%s is not tuple" % type(
            select_expr_tuples[0]
        )
        if partition_expr_tuples:
            assert isinstance(partition_expr_tuples, list), "%s is not list" % type(
                partition_expr_tuples
            )
            assert isinstance(
                partition_expr_tuples[0], tuple
            ), "%s is not tuple" % type(partition_expr_tuples[0])
        if filter_clauses:
            assert isinstance(filter_clauses, list)

    def _gen_sql_text_common(
        self,
        db_name,
        table_name,
        column_names=None,
        filter_clauses=None,
        measures=None,
        agg_fns=None,
    ):
        """Generate a SQL statement appropriate for running in the backend.
        This is common functionality for multiple backends, individual backends may override it.
        See gen_sql_text() description for parameter descriptions.
        """
        assert db_name and table_name
        if column_names:
            assert isinstance(
                column_names, (list, tuple)
            ), "%s is not list/tuple" % type(column_names)
        if measures:
            assert isinstance(measures, (list, tuple)), "%s is not list/tuple" % type(
                measures
            )
            assert agg_fns
            assert isinstance(agg_fns, (list, tuple)), "%s is not list/tuple" % type(
                agg_fns
            )

        projection = []
        if column_names:
            projection += [self.enclose_identifier(_) for _ in column_names]
            self._debug("gen_sql_text base projection: %s" % str(projection))
        if measures and agg_fns:
            projection += [
                "%s(%s)" % (fn.upper(), self.enclose_identifier(m))
                for m in measures
                for fn in agg_fns
            ]
            self._debug("gen_sql_text projection with measures: %s" % str(projection))
        if not projection:
            raise BackendApiException(
                "No columns have been specified for projection: %s.%s"
                % (db_name, table_name)
            )

        projection_clause = "\n,      ".join(projection)
        where_clause = (
            "\nWHERE  " + "\nAND    ".join(filter_clauses) if filter_clauses else ""
        )
        group_by_clause = (
            (
                "\nGROUP BY "
                + "\n,        ".join([self.enclose_identifier(_) for _ in column_names])
            )
            if measures and column_names
            else ""
        )
        order_by_clause = (
            (
                "\nORDER BY "
                + "\n,        ".join([self.enclose_identifier(_) for _ in column_names])
            )
            if agg_fns and column_names
            else ""
        )
        sql = """SELECT %(projection)s
FROM   %(db)s.%(table)s%(where_clause)s%(group_by)s%(order_by)s""" % {
            "db": self.enclose_identifier(db_name),
            "table": self.enclose_identifier(table_name),
            "projection": projection_clause,
            "where_clause": where_clause,
            "group_by": group_by_clause,
            "order_by": order_by_clause,
        }
        return sql

    def _gen_select_count_sql_text_common(
        self, db_name, table_name, filter_clause=None
    ):
        """filter_clause does NOT include "WHERE" because that could be backend specific.
        At the point WHERE is not compatible with a backend we'll need to add an
        override method for this.
        """
        assert db_name and table_name
        where_clause = " WHERE {}".format(filter_clause) if filter_clause else ""
        return "SELECT COUNT(*) FROM %s%s" % (
            self.enclose_object_reference(db_name, table_name),
            where_clause,
        )

    def _get_max_column_values_common(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
        not_when_dry_running=False,
        query_options=None,
    ):
        """Get max value for a set of columns.
        optimistic_prune_clause is used to limit the partitions scanned when looking for existing rows.
        If no data is returned then we fall back to scanning without the clause.
        optimistic_prune_clause is only applicable for single column partition schemes.
        """
        assert db_name and table_name
        assert column_name_list and isinstance(column_name_list, list)

        sql = self._gen_max_column_values_sql(
            db_name,
            table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
        )
        max_row = None
        if len(column_name_list) == 1:
            if optimistic_prune_clause:
                hwm_sql = self._gen_max_column_values_sql(
                    db_name,
                    table_name,
                    column_name_list,
                    filter_clause=optimistic_prune_clause,
                    columns_to_cast_to_string=columns_to_cast_to_string,
                )
                max_row = self.execute_query_fetch_one(
                    hwm_sql,
                    query_options=query_options,
                    not_when_dry_running=not_when_dry_running,
                    log_level=VVERBOSE,
                )

            if not max_row or max_row[0] is None:
                max_row = self.execute_query_fetch_one(
                    sql,
                    query_options=query_options,
                    not_when_dry_running=not_when_dry_running,
                    log_level=VVERBOSE,
                )
                if max_row and max_row[0] is None:
                    # Tuple (None, ) is True, catch that here and use None instead
                    max_row = None
        else:
            max_row = self.execute_query_fetch_one(
                sql,
                query_options=query_options,
                not_when_dry_running=not_when_dry_running,
                log_level=VVERBOSE,
            )
        return max_row

    def _insert_literals_using_insert_values_sql_text(
        self, db_name, table_name, column_names, literal_csv_list, split_by_cr=True
    ):
        """Format a SQL statement for inserting literals using SQL where multiple rows are passed to VALUES."""
        join_str = "\n," if split_by_cr else ","
        insert_template = "INSERT INTO %s (%s) VALUES" % (
            self.enclose_object_reference(db_name, table_name),
            ",".join(self.enclose_identifier(_) for _ in column_names),
        )
        sql = (
            insert_template
            + "\n "
            + join_str.join("({})".format(_) for _ in literal_csv_list)
        )
        return sql

    def _run_os_cmd(self, cmd):
        """Run an os command and:
          - Throw an exception if the return code is non-zero.
          - Put a dot on stdout for each line logged to give the impression of progress.
          - Logs all command output at VVERBOSE level.
        silent: No stdout unless vverbose is requested.
        """
        return run_os_cmd(
            cmd,
            self._messages,
            self._connection_options,
            dry_run=self._dry_run,
            silent=True,
        )

    def _table_partition_info(
        self, partition_id, data_format=None, num_rows=None, size_in_bytes=None
    ):
        """We need a standard value to return from get_table_partitions() so we know what data to expect.
        All inputs, except partition_id, are optional because different backends will have different details.
        This method is to be used on a per partition basis with the intention that combined we'll have a dict
        of all partitions with the partition_id as the key as well as an attribute.
        """
        return {
            "data_format": data_format,
            "num_rows": num_rows,
            "partition_id": partition_id,
            "size_in_bytes": size_in_bytes,
        }

    def _unsupported_decimal_precision_scale(self, data_precision, data_scale):
        return (
            (
                data_scale is not None
                and (data_scale < 0 or data_scale > self.max_decimal_scale())
            )
            or (data_precision and data_precision > self.max_decimal_precision())
            or (data_precision and data_scale and data_scale > data_precision)
        )

    def _validate_no_log_items(self, no_log_items):
        """Validate no_log_items is a valid dict for the purpose of removing sensitive information from logging.
        no_log_items should be either:
            {'item': 'something-sensitive-to-replace', 'sub': 'replace-with-what'}
        or
            [
                {'item': 'something-sensitive-to-replace', 'sub': 'replace-with-what'},
                ... more dicts ...
            ]
        'sub' is optional
        """
        if no_log_items is None:
            return no_log_items
        assert isinstance(no_log_items, (list, dict))
        if isinstance(no_log_items, dict):
            no_log_items = [no_log_items]
        assert all(_ in ["item", "sub"] for _ in list(no_log_items[0].keys()))
        return no_log_items

    def _validate_connection_options(self, option_list):
        missing_opts = [
            _ for _ in option_list if not hasattr(self._connection_options, _)
        ]
        if missing_opts:
            raise BackendApiException(
                "Missing connection options: %s" % ", ".join(missing_opts)
            )

    # enforced methods

    @abstractmethod
    def _backend_capabilities(self):
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def _execute_global_session_parameters(self, log_level=VVERBOSE):
        """Use this to ensure global session parameters requested by the user are honoured in each backend."""
        pass

    @abstractmethod
    def _gen_sample_stats_sql_sample_clause(
        self, db_name, table_name, sample_perc=None
    ):
        """Return a sample clause to slot in after a FROM table clause.
        Some backends may have a completely different structure so this may need refactoring in the future.
        """
        pass

    @abstractmethod
    def _get_query_profile(self, query_identifier=None):
        """Return a string containing query details of last query executed.
        Optional query_id parameter for backends that need more information.
        This is defined as private because backends are inconsistent so we can't have a standard approach.
        """
        pass

    @abstractmethod
    def _invalid_identifier_character_re(self):
        pass

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_db_name(self):
        """Some backends have overrides due to stylistic formatting, e.g. BigQuery."""
        return self._backend_type.capitalize()

    def backend_type(self):
        return self._backend_type

    def decimal_scale_validation_expression(self, column_name, column_scale):
        """Return a SQL expression to validate the scale length of string based staging columns holding decimal data.
        The expression ignores data in scientific notation because the scale in those literals bears no relation
        to the actual scale of the value.
        Some backends have overrides due to lack of regular expression support, e.g. Synapse
        """
        extract_scale_expr = self.regexp_extract_sql_expression(
            column_name, self.regexp_extract_decimal_scale_pattern()
        )
        length_expr = self.length_sql_expression(extract_scale_expr)
        is_invalid_scale_expr = "%s > %s" % (length_expr, column_scale)
        return is_invalid_scale_expr

    def drop(self, db_name, object_name, purge=False, sync=None):
        assert db_name and object_name
        if self.is_view(db_name, object_name):
            self.drop_view(db_name, object_name, sync=sync)
        else:
            self.drop_table(db_name, object_name, purge=purge, sync=sync)

    def execute_ddl(
        self,
        sql,
        sync=None,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        """Simple wrapper over _execute_ddl_or_dml() to ensure we always get a return value.
        See self._validate_no_log_items() for expected use/format.
        """
        no_log_items = self._validate_no_log_items(no_log_items)
        return_sqls = self._execute_ddl_or_dml(
            sql,
            sync=sync,
            query_options=query_options,
            log_level=log_level,
            profile=profile,
            no_log_items=no_log_items,
        )
        assert return_sqls is not None
        return return_sqls

    def execute_dml(
        self,
        sql,
        sync=None,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        no_log_items = self._validate_no_log_items(no_log_items)
        return_sqls = self._execute_ddl_or_dml(
            sql,
            sync=sync,
            query_options=query_options,
            log_level=log_level,
            profile=profile,
            no_log_items=no_log_items,
        )
        assert return_sqls is not None
        return return_sqls

    def extract_date_part_sql_expression(self, date_part, column):
        """Return a SQL expression that can be used to extract DAY, MONTH or YEAR from a column value.
        Most backends have an EXTRACT(x FROM c) function which is what this method returns. Some backends
        may override with their own variation.
        column: Can be a column object or a column name.
        """
        assert isinstance(date_part, str)
        assert date_part.upper() in ("DAY", "MONTH", "YEAR")
        assert isinstance(column, (str, ColumnMetadataInterface))
        column_name = column if isinstance(column, str) else column.name
        column_name = self.enclose_identifier(column_name)
        return f"EXTRACT({date_part.upper()} FROM {column_name})"

    def format_column_comparison(
        self, left_col, operator, right_col, left_alias=None, right_alias=None
    ):
        """Format a simple 'column operator column' string, generic for most backends but some may override.
        left_col and right_col are column objects, not names.
        """
        assert isinstance(left_col, ColumnMetadataInterface)
        assert isinstance(right_col, ColumnMetadataInterface)
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
        return "{} {} {}".format(left_identifier, operator, right_identifier)

    def get_column(self, db_name, table_name, column_name):
        """Get a single column from get_columns()"""
        return match_table_column(column_name, self.get_columns(db_name, table_name))

    def get_column_names(self, db_name, table_name, conv_fn=None):
        return get_column_names(self.get_columns(db_name, table_name), conv_fn=conv_fn)

    def get_distinct_expressions(
        self,
        db_name,
        table_name,
        expression_list,
        order_results=False,
        not_when_dry_running=False,
    ):
        """Run SQL to get distinct expressions from a table.
        Currently common across all backends but could deviate in the future.
        All current backend support this syntax:
            SELECT DISTINCT expr AS alias FROM db.table ORDER BY alias
        """
        assert expression_list and isinstance(expression_list, list)
        sql_template = "SELECT DISTINCT %(projection)s FROM %(db_table)s"
        projection = ",".join(
            ["{} AS c{}".format(_, i) for i, _ in enumerate(expression_list)]
        )
        if order_results:
            aliases = ",".join(["c{}".format(_) for _ in range(len(expression_list))])
            sql_template += " ORDER BY {}".format(aliases)
        sql = sql_template % {
            "projection": projection,
            "db_table": self.enclose_object_reference(db_name, table_name),
        }
        rows = self.execute_query_fetch_all(
            sql, not_when_dry_running=not_when_dry_running, log_level=VVERBOSE
        )
        return rows

    def get_max_column_length(self, db_name, table_name, column_name):
        """Select the length of every value in table_name.column_name and return the max value.
        Useful when trying to infer a suitable column size when presenting to an RDBMS.
        Currently the simple SQL is valid in all backends, this may change in time and need
        to become an abstractmethod.
        """
        assert db_name and table_name
        assert column_name
        sql = "SELECT MAX(%s) FROM %s" % (
            self.length_sql_expression(self.enclose_identifier(column_name)),
            self.enclose_object_reference(db_name, table_name),
        )
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return row[0] if row else None

    def get_non_synthetic_columns(self, db_name, table_name):
        return [
            _
            for _ in self.get_columns(db_name, table_name)
            if not self.is_synthetic_partition_column(_)
        ]

    def identifier_contains_invalid_characters(self, identifier):
        """Checks that characters in identifier are valid for the backend system
        Returns a list of invalid characters which allows truthy use and use of details in logging
        """
        if self._invalid_identifier_character_re().search(identifier):
            return list(
                set(self._invalid_identifier_character_re().findall(identifier))
            )
        else:
            return None

    def is_synthetic_partition_column(self, column):
        """Is a column a synthetic partition column - based on its name.
        column can be a column object or a name.
        """
        return is_synthetic_partition_column(column)

    def is_supported_data_type(self, data_type):
        assert data_type
        assert isinstance(data_type, str)
        return bool(data_type.upper() in self.supported_backend_data_types())

    def is_supported_partition_function_return_data_type(self, data_type):
        assert data_type
        assert isinstance(data_type, str)
        supported_types = self.supported_partition_function_return_data_types()
        return bool(supported_types and data_type in supported_types)

    def is_valid_staging_format(self, staging_format):
        return bool(staging_format in self.valid_staging_formats())

    def max_column_name_length(self):
        """The maximum number of characters permitted in a column name.
        For most engines this is the same but some may require an implementation level override.
        """
        return self.max_table_name_length()

    def max_db_name_length(self):
        """The maximum number of characters permitted in a database/dataset/schema name.
        For most engines this is the same but some may require an implementation level override.
        """
        return self.max_table_name_length()

    def partition_range_max(self):
        """Maximum supported value for a numeric partition key range.
        Defaults to None (meaning not applicable) in this interface but some implementations may override.
        """
        return None

    def partition_range_min(self):
        """Minimum supported value for a numeric partition key range.
        Defaults to None (meaning not applicable) in this interface but some implementations may override.
        """
        return None

    def regexp_extract_decimal_scale_pattern(self):
        """Pattern to extract the scale from a numeric value inside a string.
        Exists as it's own function purely so we can unit test it.
        Searches for:
            ^        Start of line anchor
            -?       Optional sign
            [0-9]*   Any numeric digits (or perhaps no digits in case of numbers like -.0123)
            [.]      Mandatory decimal place, no backslashes because backends are inconsistent inside strings
            ([0-9]+) Mandatory group matching the decimal places
            $        End of line anchor
        """
        return r"^-?[0-9]*[.]([0-9]+)$"

    def udf_exists(self, db_name, udf_name):
        return bool(self.list_udfs(db_name, udf_name))

    def valid_offload_fs_schemes(self):
        if not self._valid_schemes:
            valid_schemes = []
            if self.filesystem_scheme_s3a_supported():
                valid_schemes.append(OFFLOAD_FS_SCHEME_S3A)
            if self.filesystem_scheme_hdfs_supported():
                valid_schemes.append(OFFLOAD_FS_SCHEME_HDFS)
            if self.filesystem_scheme_wasb_supported():
                valid_schemes.extend([OFFLOAD_FS_SCHEME_WASB, OFFLOAD_FS_SCHEME_WASBS])
            if self.filesystem_scheme_adl_supported():
                valid_schemes.append(OFFLOAD_FS_SCHEME_ADL)
            if self.filesystem_scheme_abfs_supported():
                valid_schemes.extend([OFFLOAD_FS_SCHEME_ABFS, OFFLOAD_FS_SCHEME_ABFSS])
            if self.filesystem_scheme_gs_supported():
                valid_schemes.append(OFFLOAD_FS_SCHEME_GS)
            if self.filesystem_scheme_inherit_supported():
                valid_schemes.append(OFFLOAD_FS_SCHEME_INHERIT)
            self._valid_schemes = valid_schemes
        return self._valid_schemes

    # enforced methods/properties

    @abstractmethod
    def add_columns(self, db_name, table_name, column_tuples, sync=None):
        """column_tuples: A list of tuples of the format:
        [
            (col_name1, data_type),
            (col_name2, data_type),
        ]
        """

    @abstractmethod
    def alter_sort_columns(self, db_name, table_name, sort_column_names, sync=None):
        pass

    @abstractmethod
    def backend_report_info(self):
        """Reporting information about the type of backend and how to display
        its configuration details (e.g. Hadoop host:port or BigQuery project).
        """

    @abstractmethod
    def backend_version(self):
        """Output some descriptive information about the backend
        This is different to target_version() even though it appears similar in function.
        """

    @abstractmethod
    def check_backend_supporting_objects(self, orchestration_options):
        """Checks any supporting backend objects are present and correct.
        Used for environment validation (connect).
        Returns a list of dictionaries of this format:
        [
            {
                CONNECT_TEST: 'Test name',
                CONNECT_STATUS: True | False,
                CONNECT_DETAIL: 'Some words to pass on to the consumer if the test failed',
            },
        }
        Example:
        {
            {   CONNECT_TEST: 'Offload role',
                CONNECT_STATUS: False,
                CONNECT_DETAIL: 'Offload role not found: %s' % role_name,
            },
        }
        """

    @abstractmethod
    def close(self):
        """Free up any resources currently held"""

    @abstractmethod
    def compute_stats(
        self,
        db_name,
        table_name,
        incremental=None,
        for_columns=False,
        partition_tuples=None,
        sync=None,
    ):
        """Currently doesn't cater for partition specific commands, this will need to be added in the near future
        for_columns: compute stats at column level rather than table level, ignored on some backends
        partition_tuples: A list of tuples of the format:
            [
                (part_col_name1, col_value),
                (part_col_name2, col_value),
            ]
        """

    @abstractmethod
    def create_database(
        self, db_name, comment=None, properties=None, with_terminator=False
    ):
        """Create a backend database or equivalent container for tables (such as dataset or schema).
        properties: An optional dictionary to pass information to different backends, e.g.:
                    properties={"location": "us-west"}
                    Enables a BackendTable implementation to pass implementation specifics to BackendApi
        """

    @abstractmethod
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
        """Create a table and cater for backend specific details, we need to pass all parameters even if they
        don't apply to certain backends but some may be ignored depending on the system involved:
            column_list: A list of column objects for the table.
            partition_column_names: A list of column names for table partitioning, the columns must also
                                    be in column_list.
            location: The location URI for Hadoop based systems
            table_properties: In Hadoop these are TBLPROPERTIES, in BigQuery table options
            sort_column_names: For systems that can influence data order by defining it when a table is created
            without_db_name: In very special cases we allow a CREATE TABLE to run without the db name
        Passing the various parameters as something false-like will exclude the clause from the SQL
        """

    @abstractmethod
    def create_view(
        self,
        db_name,
        view_name,
        column_tuples,
        ansi_joined_tables,
        filter_clauses=None,
        sync=None,
    ):
        """Create a backend view
        column_tuples: A list of (column expression, alias) tuples
        filter_clauses: A list of 'col=something' strings
        ansi_joined_tables: contents of the from clause which could be
                                db.table
                            or
                                db.t1 inner join db.t2 on (t1.id = t2.id)
                            (there's an assumption here that backend systems understand ANSI
                             SQL - this may need to change)
        Functions should not enclose components of column_tuples/ansi_joined_tables/filter_clauses identifiers
        because they might be expressions with identifiers already enclosed.
        """

    @abstractmethod
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
        pass

    @abstractmethod
    def current_date_sql_expression(self):
        """Return a SQL expression that can be used to get the current date, probably a SQL function name"""
        pass

    @abstractmethod
    def data_type_accepts_length(self, data_type):
        """Returns true when a data type accepts a length in its SQL spec, e.g. VARCHAR2(10) or NUMBER(5)"""
        pass

    @abstractmethod
    def database_exists(self, db_name):
        pass

    @abstractmethod
    def db_name_label(self, initcap=False):
        """Return the correct name for whatever we use db_name for in the backend, e.g. database/schema/etc"""

    @abstractmethod
    def default_date_based_partition_granularity(self):
        """Return default granularity for date based partitioning.
        Should be one of:
          PART_COL_GRANULARITY_YEAR, PART_COL_GRANULARITY_MONTH, PART_COL_GRANULARITY_DAY
        """

    @abstractmethod
    def default_storage_compression(self, user_requested_codec, user_requested_format):
        pass

    @staticmethod
    @abstractmethod
    def default_storage_format():
        """This is a staticmethod in subclasses but Python 2.7 does not have abstractstaticmethod.
        This is correct to be static because it is used before we have a connection.
        """

    @abstractmethod
    def derive_native_partition_info(self, db_name, table_name, column, position):
        """Return a ColumnPartitionInfo object (or None) based on native partition settings"""

    @abstractmethod
    def detect_column_has_fractional_seconds(self, db_name, table_name, column):
        """Detect whether a backend date based column contains fractional seconds.
        Returns True/False.
        """

    @abstractmethod
    def drop_state(self):
        """Drop any cached state so values are refreshed on next request."""

    @abstractmethod
    def drop_table(self, db_name, table_name, purge=False, if_exists=True, sync=None):
        """Drop a table and cater for backend specific details:
        purge: some backends have external tables where we also need to clean up the filesystem.
        sync: some backends are able to run DDL sync or async.
        """

    @abstractmethod
    def drop_view(self, db_name, view_name, if_exists=True, sync=None):
        pass

    @abstractmethod
    def enclose_identifier(self, identifier):
        """Surround an identifier in the correct character for the backend system."""

    @abstractmethod
    def enclose_object_reference(self, db_name, object_name):
        """Correctly use enclose_identifier() on a composite reference to an object.
        For example:
            Hadoop: `db`.`object`
            BigQuery: `ds.object`
        """

    @abstractmethod
    def enclosure_character(self):
        """The character to use in enclose_identifier/enclose_object_reference.
        Superceded by enclose_identifier(), ideally we shouldn't use enclosure_character() any longer.
        """

    @abstractmethod
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
        """Run a query and fetch all rows"""

    @abstractmethod
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
        """Run a query and fetch one row"""

    @abstractmethod
    def execute_query_get_cursor(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
    ):
        """Run a query and return the cursor to allow the calling program to handle fetching.
        execute_query_get_cursor() exposes too lower a level of detail to higher level code.
        Strongly advise NOT to implement/use this method.
        For parameter descriptions see: _execute_query_fetch_x.
        """

    @abstractmethod
    def exists(self, db_name, object_name):
        """Does the table/view exist"""

    @abstractmethod
    def format_query_parameter(self, param_name):
        """For backends that support query parameters this method prefixes or suffixes a parameter name.
        For example on BigQuery it prefixes an @ symbol.
        """

    @abstractmethod
    def gen_column_object(self, column_name, **kwargs):
        """Returns a column object of the correct type for the backend system"""

    @abstractmethod
    def gen_copy_into_sql_text(
        self,
        db_name,
        table_name,
        from_object_clause,
        select_expr_tuples,
        filter_clauses=None,
    ):
        """Build an INSERT/SELECT SQL statement and return the resulting string.
        Most backends will only use gen_insert_select_sql_text() but some have a COPY INTO SQL command.
        select_expr_tuples: A list of tuples containing: (the-SQL-expression-to-select, the-column-name).
        from_object_clause: The name/expression to select from, could be db.table_name or something more complex.
        """

    @abstractmethod
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
        """Generate and return SQL text used to create a table via CTAS
        column_tuples: A list of (column expression, alias) tuples
        from_db_name/table_name: db and table to select from, otherwise select a single row in the standard
                                 way for the backend
        row_limit: Apply a row limit to the select part of the statement
        external: create an external table or not
        table_properties: table properties as a dictionary
        """

    @abstractmethod
    def gen_default_numeric_column(self, column_name, data_scale=None):
        """Return a default best-fit numeric column for generic usage
        Used when we can't really know or influence a type, e.g. AVG() output in AAPD
        data_scale should have an appropriate default for each backend (None used above for illustration)
        data_scale=0 can be used if the outcome is known to be integral
        """

    @abstractmethod
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
        """Build an INSERT/SELECT SQL statement and return the resulting string.
        select_expr_tuples: A list of tuples containing: (the-SQL-expression-to-select, the-column-name).
        partition_expr_tuples: (the-SQL-expression-to-select, the-partition-column-name).
        Note: partition_expr_tuples is only needed when the backend must process them independently, for example
              in Hadoop they are virtual and do not really exist. Most other systems will not need this parameter.
        sort_expr_list: A list of expressions by which to sort incoming data.
        distribute_columns: A list of column names to include in any distribute clause.
        from_object_override: An override for from_db/table_name, in some cases we need a more complex row source.
        """

    @abstractmethod
    def gen_native_range_partition_key_cast(self, partition_column):
        """Generates the CAST to be used to appropriately range partition a table"""

    @abstractmethod
    def gen_sql_text(
        self,
        db_name,
        table_name,
        column_names=None,
        filter_clauses=None,
        measures=None,
        agg_fns=None,
    ):
        """Generate a SQL statement appropriate for running in the backend.
        column_names: A list of column names to select, these are dimensions when aggregating.
                      The values have enclose_identifier() applied so you cannot pass expressions or literals.
        filter_clauses: A list of 'col=something' strings.
        measures: A list of column names to have agg_fns applied to.
        agg_fns: A list of agg fns, e.g. ['sum', 'max']
        """

    @abstractmethod
    def generic_string_data_type(self):
        pass

    @abstractmethod
    def get_columns(self, db_name, table_name):
        pass

    @abstractmethod
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
        The columns_to_cast_to_string parameter is because our HiveServer2 DBAPI does not support nanoseconds
        therefore we bring certain data types back as strings to avoid truncating to milliseconds.
        """
        pass

    @abstractmethod
    def get_max_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
        not_when_dry_running=False,
    ):
        """Get max value for a set of columns.
        optimistic_prune_clause is used to limit the partitions scanned when looking for existing rows.
        If no data is returned then we fall back to scanning without the clause optimistic_prune_clause
        is only applicable for single column partition schemes.
        The columns_to_cast_to_string parameter is because our HiveServer2 DBAPI does not support nanoseconds
        therefore we bring certain data types back as strings to avoid truncating to milliseconds.
        """
        pass

    @abstractmethod
    def get_partition_columns(self, db_name, table_name):
        """Returns a list of column objects for partition columns"""
        pass

    @abstractmethod
    def get_session_option(self, option_name):
        pass

    @abstractmethod
    def get_table_ddl(
        self, db_name, table_name, as_list=False, terminate_sql=False, for_replace=False
    ):
        """Return CREATE TABLE DDL as a string (or a list of strings split on CR if as_list=True)
        terminate_sql: Adds any executing character to the end of the SQL (e.g. a semi-colon)
        for_replace: If the backend supports it this includes the equivalent of:
            CREATE OR REPLACE TABLE
        """
        pass

    @abstractmethod
    def get_table_location(self, db_name, table_name):
        pass

    @abstractmethod
    def get_table_partition_count(self, db_name, table_name):
        pass

    @abstractmethod
    def get_table_partitions(self, db_name, table_name):
        """The return of this is currently matching whatever HiveTable() does
        In order to implement for any new backend we're going to need to dig
        into this and decide on the most appropriate structure for the future
        """

    @abstractmethod
    def get_table_row_count(
        self,
        db_name,
        table_name,
        filter_clause=None,
        not_when_dry_running=False,
        log_level=VVERBOSE,
    ):
        """Note that this is interrogating the table in order to get the actual row count
        not getting the value from stats or some potentially stale location
        filter_clause does NOT include "WHERE" because that could be backend specific
        """

    @abstractmethod
    def get_table_row_count_from_metadata(self, db_name, table_name):
        """Note that this is getting the row count from table metadata, such as stats
        It may or may not be stale depending on the backend system
        """

    @abstractmethod
    def get_table_size(self, db_name, table_name, no_cache=False):
        """Return the size of the table in bytes.
        no_cache: Allows us to bypass any backend caching for systems that automatically do that.
        """

    @abstractmethod
    def get_table_size_and_row_count(self, db_name, table_name):
        """Return a tuple of size in bytes and number of rows for a table.
        Some backends can avoid a round trip if the developer knows both values are required.
        """

    @abstractmethod
    def get_table_sort_columns(self, db_name, table_name, as_csv=True):
        """Get sort/cluster column names for a table, either as a list of names or a CSV."""

    @abstractmethod
    def get_table_stats(self, db_name, table_name, as_dict=False):
        """For historic reasons this is expected to return two objects:
            (table_stats, column_stats)
            table_stats: num_rows, num_bytes, avg_row_len
            column_stats: col_name, ndv, num_nulls, avg_col_len, low_value, high_value, max_col_len
        See HiveTableStats class for more details
        """

    @abstractmethod
    def get_table_and_partition_stats(self, db_name, table_name, as_dict=False):
        """This has been broken out from get_table_stats() because the output looks pretty Hadoop specific
        If we decide we need this functionality for another backend then we should look at the format
        and decide what to do
        """

    @abstractmethod
    def get_table_stats_partitions(self, db_name, table_name):
        pass

    @abstractmethod
    def get_user_name(self):
        pass

    @abstractmethod
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
                      The row level lists should contain the exact right number of columns.
        column_list: The column specs for the columns to be inserted, if left blank this will default to all
                     columns in the table
        Disclaimer: This code is used in testing and generating the sequence table. It is not robust enough to
                    be a part of any Offload Transport
        """

    @abstractmethod
    def is_nan_sql_expression(self, column_expr):
        """Return a SQL expression testing if an expression is NaN (not-a-number)."""

    @abstractmethod
    def is_valid_partitioning_data_type(self, data_type):
        """Checks if a data type is valid for use as a backend partition column.
        This is not saying what the backend supports, it is what GOE support, some
        data types may end up being converted to something else via a GOE_PART column.
        """

    @abstractmethod
    def is_valid_sort_data_type(self, data_type):
        """Checks if a data type is valid for use as a backend sort/cluster column."""

    @abstractmethod
    def is_valid_storage_compression(self, user_requested_codec, user_requested_format):
        """Checks if requested --storage-compression is valid for use by the backend."""

    @abstractmethod
    def is_valid_storage_format(self, storage_format):
        """Checks if requested --storage-format is valid for use by the backend."""

    @abstractmethod
    def is_view(self, db_name, object_name):
        pass

    @abstractmethod
    def length_sql_expression(self, column_expression):
        """Return a SQL expression to get the length of column_expression, e.g.:
        LENGTH(column_name)
        """
        pass

    @abstractmethod
    def list_databases(self, db_name_filter=None, case_sensitive=True):
        """Return a list of database names, optionally filtered.
        db_name_filter: A string used to filter db names. May contain wildcard character * but no other wildcards.
        case_sensitive: Defines case sensitivity of db_name_filter.
        """
        pass

    @abstractmethod
    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        """Return a list of tables names within a specific db, optionally filtered by table name.
        table_name_filter: A string used to filter names. May contain wildcard character * but no other wildcards.
        case_sensitive: Defines case sensitivity of table_name_filter.
        """
        pass

    @abstractmethod
    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        """Return a list of UDFs within a specific db, optionally filtered by UDF name.
            [ [ udf_name, return_type ], ]
        udf_name_filter: A string used to filter names. May contain wildcard character * but no other wildcards.
        case_sensitive: Defines case sensitivity of table_name_filter.
        """
        pass

    @abstractmethod
    def list_views(self, db_name, view_name_filter=None, case_sensitive=True):
        """Return a list of views names within a specific db, optionally filtered by name.
        view_name_filter: A string used to filter names. May contain wildcard character * but no other wildcards.
        case_sensitive: Defines case sensitivity of table_name_filter.
        """
        pass

    @abstractmethod
    def max_decimal_integral_magnitude(self):
        """The maximum number of digits permitted to the left of the decimal place for the DECIMAL based data type."""
        pass

    @abstractmethod
    def max_decimal_precision(self):
        """The maximum value of precision permitted for the relevant DECIMAL/NUMBER based data type."""
        pass

    @abstractmethod
    def max_decimal_scale(self, data_type=None):
        """The maximum value of data_scale permitted for the relevant DECIMAL/NUMBER based data type.
        Optional data type parameter for backends with multiple DECIMAL data types.
        """
        pass

    @abstractmethod
    def max_datetime_value(self):
        """Return the maximum datetime value that can be stored by this backend as datetime64()."""
        pass

    @abstractmethod
    def max_datetime_scale(self):
        """Return the maximum scale (number of decimal places for seconds) that can be stored by this backend."""
        pass

    @abstractmethod
    def max_partition_columns(self):
        """Return the maximum number of partition columns for the backend system."""
        pass

    @abstractmethod
    def max_sort_columns(self):
        """Return the maximum number of sort columns for the backend system"""
        pass

    @abstractmethod
    def max_table_name_length(self):
        """The maximum number of characters permitted in a table name"""
        pass

    @abstractmethod
    def min_datetime_value(self):
        """Return the minimum datetime value that can be stored by this backend as datetime64()."""
        pass

    @abstractmethod
    def native_integer_types(self):
        """Return a list of native integer data types for the backend."""
        pass

    @abstractmethod
    def partition_column_requires_synthetic_column(self, backend_column, granularity):
        """Returns True if the backend column requires a synthetic column if it is used for partitioning."""
        pass

    @abstractmethod
    def populate_sequence_table(
        self, db_name, table_name, starting_seq, target_seq, split_by_cr=False
    ):
        """Populate options.sequence_table_name up to target_seq.
        split_by_cr: This option is a bit of a fudge but allows a calling program to parse the output.
                     The idea being that, on some backends, this method may produce lengthy output
                     which we may not want on screen. With split_by_cr the calling program has the
                     opportunity to split the executed text by cr and only log some lines.
        """
        pass

    @abstractmethod
    def refresh_table_files(self, db_name, table_name, sync=None):
        pass

    @abstractmethod
    def regexp_extract_sql_expression(self, subject, pattern):
        """Return a SQL expression returning a REGEXP_EXTRACT SQL expression."""
        pass

    @abstractmethod
    def rename_table(
        self, from_db_name, from_table_name, to_db_name, to_table_name, sync=None
    ):
        pass

    @abstractmethod
    def role_exists(self, role_name):
        """Check the role exists, returns True/False"""
        pass

    @abstractmethod
    def sequence_table_max(self, db_name, table_name):
        pass

    @abstractmethod
    def set_column_stats(
        self, db_name, table_name, new_column_stats, ndv_cap, num_null_factor
    ):
        pass

    @abstractmethod
    def set_partition_stats(
        self, db_name, table_name, new_partition_stats, additive_stats
    ):
        pass

    @abstractmethod
    def set_session_db(self, db_name, log_level=VERBOSE):
        """Switch the default db for a session to db_name.
        Code should always explicitly specify object prefixes therfore usage of this method ought to be rare.
        Typical SQL for this would be "USE ..."
        """
        pass

    @abstractmethod
    def set_table_stats(self, db_name, table_name, new_table_stats, additive_stats):
        pass

    @abstractmethod
    def supported_backend_data_types(self):
        pass

    @abstractmethod
    def supported_date_based_partition_granularities(self):
        """Return a list of supported granularities for date based partitioning.
        The list should be a subset of:
          [PART_COL_GRANULARITY_YEAR, PART_COL_GRANULARITY_MONTH, PART_COL_GRANULARITY_DAY]
        """
        pass

    @abstractmethod
    def supported_partition_function_parameter_data_types(self):
        """Return a list of backend data types supported as partition function parameters"""
        pass

    @abstractmethod
    def supported_partition_function_return_data_types(self):
        """Return a list of backend data types supported as the result of a partition function"""
        pass

    @abstractmethod
    def synthetic_partition_numbers_are_string(self):
        """Are synthetic partition column values for numeric source columns stored as strings, True/False"""
        pass

    @abstractmethod
    def table_distribution(self, db_name, table_name):
        """Get the distribution setting for a table. This may not apply to some backends, it was added for Synapse"""
        pass

    @abstractmethod
    def table_exists(self, db_name: str, table_name: str) -> bool:
        """Return bool depending whether the table exists or not."""

    @abstractmethod
    def table_has_rows(self, db_name: str, table_name: str) -> bool:
        """Return bool depending whether the table has rows or not."""

    @abstractmethod
    def target_version(self):
        """Return version of the backend SQL engine in x.y.z format that can be used by GOEVersion().
        This is different to backend_version() even though it appears similar in function.
        """
        pass

    @abstractmethod
    def to_backend_literal(self, py_val, data_type=None):
        """Translate a Python value to a backend literal.
        Examples where this may act are dates (which may need specific formatting) and
        string (need quoting with a specific character).
        Other types are likely just a pass through.
        """
        pass

    @abstractmethod
    def transform_encrypt_data_type(self):
        """Return a backend data type matching the output of an encryption expression."""
        pass

    @abstractmethod
    def transform_null_cast(self, backend_column):
        """Return a SQL expression to cast a NULL value in place of a column."""
        pass

    @abstractmethod
    def transform_tokenize_data_type(self):
        """Return a backend data type matching the output of a tokenization expression."""
        pass

    @abstractmethod
    def transform_regexp_replace_expression(
        self, backend_column, regexp_replace_pattern, regexp_replace_string
    ):
        pass

    @abstractmethod
    def transform_translate_expression(self, backend_column, from_string, to_string):
        pass

    @abstractmethod
    def udf_details(self, db_name, udf_name):
        """Returns a list of UdfDetails for requested UDF name.
        We return a list because some backends can overload UDFs.
        """
        pass

    @abstractmethod
    def udf_installation_os(self, user_udf_version):
        """Copies any libraries required to support UDFs from our software package to wherever they should be.
        Returns a list of commands executed.
        """
        pass

    @abstractmethod
    def udf_installation_sql(self, create_udf_db, udf_db=None):
        """Executes any SQL commands required for UDF installation.
        Returns a list of commands executed, in dry_run mode that's all it does.
        udf_db can be empty which means we won't specify one and pickup a default.
        """
        pass

    @abstractmethod
    def udf_installation_test(self, udf_db=None):
        """Executes any SQL commands to test each UDF.
        udf_db can be empty which means we won't specify one and pickup a default.
        """
        pass

    @abstractmethod
    def valid_canonical_override(self, column, canonical_override):
        """Present has a number of options for overriding the default canonical mapping in to_canonical_column().
        This method validates the override.
        column: the source backend column object.
        canonical_override: either a canonical column object or a GOE_TYPE_... data type.
        """
        pass

    @abstractmethod
    def valid_staging_formats(self):
        pass

    @abstractmethod
    def view_exists(self, db_name, view_name):
        pass

    @abstractmethod
    def to_canonical_column(self, column):
        """Translate a backend column to an internal GOE column."""
        pass

    @abstractmethod
    def from_canonical_column(self, column, decimal_padding_digits=0):
        """Translate an internal GOE column to a backend column."""
        pass

    # capability config

    def is_capability_supported(self, capability_constant):
        assert capability_constant in self._backend_capabilities(), (
            "Unknown capability: %s" % capability_constant
        )
        return self._backend_capabilities()[capability_constant]

    def bucket_hash_column_supported(self):
        return self.is_capability_supported(CAPABILITY_BUCKET_HASH_COLUMN)

    def canonical_date_supported(self):
        """Does the backend have a pure date data type (no time part).
        Note that there is an Impala override for this.
        """
        return self.is_capability_supported(CAPABILITY_CANONICAL_DATE)

    def canonical_float_supported(self):
        """Does the backend have a pure 4 byte binary floating point data type"""
        return self.is_capability_supported(CAPABILITY_CANONICAL_FLOAT)

    def canonical_time_supported(self):
        """Does the backend have a pure time data type (no date part).
        Note that there is an Impala override for this.
        """
        return self.is_capability_supported(CAPABILITY_CANONICAL_TIME)

    def case_sensitive_identifiers(self):
        """Does the backend have case sensitive db/table name identifiers"""
        return self.is_capability_supported(CAPABILITY_CASE_SENSITIVE)

    def column_stats_set_supported(self):
        """Note that there is an Impala override for this"""
        return self.is_capability_supported(CAPABILITY_COLUMN_STATS_SET)

    def create_database_supported(self):
        return self.is_capability_supported(CAPABILITY_CREATE_DB)

    def default_sort_columns_to_primary_key(self) -> bool:
        """Does this backend suit clustering on primary key if there's no user specified alternative."""
        return False

    def drop_column_supported(self):
        """Note that there is an Impala override for this"""
        return self.is_capability_supported(CAPABILITY_DROP_COLUMN)

    def filesystem_scheme_abfs_supported(self):
        """Note that there is an Impala override for this"""
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_ABFS)

    def filesystem_scheme_adl_supported(self):
        """Note that there is an Impala override for this"""
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_ADL)

    def filesystem_scheme_gs_supported(self):
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_GS)

    def filesystem_scheme_hdfs_supported(self):
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_HDFS)

    def filesystem_scheme_inherit_supported(self):
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_INHERIT)

    def filesystem_scheme_s3a_supported(self):
        """Note that there is an Impala override for this"""
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_S3A)

    def filesystem_scheme_wasb_supported(self):
        return self.is_capability_supported(CAPABILITY_FS_SCHEME_WASB)

    def goe_column_transformations_supported(self):
        return self.is_capability_supported(CAPABILITY_GOE_COLUMN_TRANSFORMATIONS)

    def goe_join_pushdown_supported(self):
        return self.is_capability_supported(CAPABILITY_GOE_JOIN_PUSHDOWN)

    def goe_materialized_join_supported(self):
        return self.is_capability_supported(CAPABILITY_GOE_MATERIALIZED_JOIN)

    def goe_partition_functions_supported(self):
        return self.is_capability_supported(CAPABILITY_GOE_PARTITION_FUNCTIONS)

    def goe_sequence_table_supported(self):
        return self.is_capability_supported(CAPABILITY_GOE_SEQ_TABLE)

    def goe_udfs_supported(self):
        return self.is_capability_supported(CAPABILITY_GOE_UDFS)

    def load_db_transport_supported(self):
        return self.is_capability_supported(CAPABILITY_LOAD_DB_TRANSPORT)

    def nan_supported(self):
        return self.is_capability_supported(CAPABILITY_NAN)

    def nanoseconds_supported(self):
        return self.is_capability_supported(CAPABILITY_NANOSECONDS)

    def not_null_column_supported(self):
        return self.is_capability_supported(CAPABILITY_NOT_NULL_COLUMN)

    def parameterized_queries_supported(self):
        return self.is_capability_supported(CAPABILITY_PARAMETERIZED_QUERIES)

    def partition_by_column_supported(self):
        return self.is_capability_supported(CAPABILITY_PARTITION_BY_COLUMN)

    def partition_by_string_supported(self):
        return self.is_capability_supported(CAPABILITY_PARTITION_BY_STRING)

    def query_sample_clause_supported(self):
        return self.is_capability_supported(CAPABILITY_QUERY_SAMPLE_CLAUSE)

    def ranger_supported(self):
        """Note that there is an Impala version specific override for this"""
        return self.is_capability_supported(CAPABILITY_RANGER)

    def schema_evolution_supported(self):
        return self.is_capability_supported(CAPABILITY_SCHEMA_EVOLUTION)

    def sentry_supported(self):
        """Note that there is an Impala version specific override for this"""
        return self.is_capability_supported(CAPABILITY_SENTRY)

    def sorted_table_supported(self):
        """Note that there is an Impala override for this"""
        return self.is_capability_supported(CAPABILITY_SORTED_TABLE)

    def sorted_table_modify_supported(self):
        return bool(
            self.sorted_table_supported()
            and self.is_capability_supported(CAPABILITY_SORTED_TABLE_MODIFY)
        )

    def sql_microsecond_predicate_supported(self):
        """Can the orchestration SQL engine cope with literal predicates with microsecond precision"""
        return self.is_capability_supported(CAPABILITY_SQL_MICROSECOND_PREDICATE)

    def synthetic_partitioning_supported(self):
        return self.is_capability_supported(CAPABILITY_SYNTHETIC_PARTITIONING)

    def table_stats_compute_supported(self):
        return self.is_capability_supported(CAPABILITY_TABLE_STATS_COMPUTE)

    def table_stats_get_supported(self):
        return self.is_capability_supported(CAPABILITY_TABLE_STATS_GET)

    def table_stats_set_supported(self):
        return self.is_capability_supported(CAPABILITY_TABLE_STATS_SET)
