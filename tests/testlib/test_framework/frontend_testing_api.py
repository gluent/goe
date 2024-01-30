#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" FrontendTestingApi: An extension of (not yet created) FrontendApi used purely for code relating to the setup,
    processing and verification of integration tests.
    LICENSE_TEXT
"""

import logging
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import Optional, Union

from goe.offload.column_metadata import (
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
)
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.frontend_api import GET_DDL_TYPE_VIEW
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.util.simple_timer import SimpleTimer
from tests.testlib.test_framework import test_constants


class FrontendTestingApiException(Exception):
    pass


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# FrontendTestingApiInterface
###########################################################################


class FrontendTestingApiInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for implementation specific sub-classes"""

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def __init__(
        self,
        frontend_type,
        connection_options,
        messages,
        existing_connection=None,
        dry_run=False,
        do_not_connect=False,
        trace_action=None,
    ):
        assert connection_options
        assert messages
        self._connection_options = connection_options
        self._frontend_type = frontend_type
        self._messages = messages
        self._dry_run = dry_run
        self._db_api = frontend_api_factory(
            frontend_type,
            self._connection_options,
            self._messages,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action or "FrontendTestingApi",
        )

    def _log(self, msg, detail=None, ansi_code=None):
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def _canonical_integer_precision(self, data_type):
        assert data_type
        precision_map = {
            GOE_TYPE_INTEGER_1: 2,
            GOE_TYPE_INTEGER_2: 4,
            GOE_TYPE_INTEGER_4: 9,
            GOE_TYPE_INTEGER_8: 18,
            GOE_TYPE_INTEGER_38: 38,
        }
        return precision_map.get(data_type)

    def _create_generated_test_table(self, schema, table_name, columns):
        """This method creates frontend objects. It is expected to be called from an SH_TEST connection
        and not an ADM or APP one.
        columns: List of dicts of the format described in _goe_type_mapping_column_definitions() docstring.
        """
        column_list = [_["column"] for _ in columns]
        self._db_api.create_table(schema, table_name, column_list)

    def _generated_table_column_definitions(
        self,
        table_name,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        backend_max_test_column_count=None,
    ) -> list:
        """Returns column definitions for the format described in _goe_type_mapping_column_definitions() docstring."""
        if table_name == test_constants.GOE_CHARS:
            return self._goe_chars_column_definitions(
                ascii_only=ascii_only, all_chars_notnull=all_chars_notnull
            )
        elif table_name == test_constants.GOE_TYPES:
            return self._goe_types_column_definitions(
                ascii_only=ascii_only,
                all_chars_notnull=all_chars_notnull,
                supported_canonical_types=supported_canonical_types,
                include_interval_columns=True,
            )
        elif table_name == test_constants.GOE_TYPES_QI:
            return self._goe_types_column_definitions(
                ascii_only=ascii_only,
                all_chars_notnull=all_chars_notnull,
                supported_canonical_types=supported_canonical_types,
                include_interval_columns=False,
            )
        elif table_name == test_constants.GOE_WIDE:
            return self._goe_wide_column_definitions(
                ascii_only=ascii_only,
                all_chars_notnull=all_chars_notnull,
                backend_max_test_column_count=backend_max_test_column_count,
            )
        else:
            raise NotImplementedError(
                f"Missing _generated_table_column_definitions entry for: {table_name}"
            )

    def _generated_table_row_count(self, table_name) -> int:
        """Allows fluctuations in desired row count by frontend. Individual frontends can override."""
        if table_name == test_constants.GOE_CHARS:
            return 512
        elif table_name == test_constants.GOE_TYPE_MAPPING:
            return 100
        elif table_name == test_constants.GOE_TYPES:
            return 10000
        elif table_name == test_constants.GOE_TYPES_QI:
            return 100
        elif table_name == test_constants.GOE_WIDE:
            return 100
        else:
            raise NotImplementedError(
                f"Missing _generated_table_row_count entry for: {table_name}"
            )

    # Enforced methods/properties

    @abstractmethod
    def _create_new_testing_client(
        self, existing_connection, trace_action_override=None
    ) -> "FrontendTestingApiInterface":
        """Return a new FrontendTestingApi object based on a new connection but with other config reused."""

    @abstractmethod
    def _data_type_supports_precision_and_scale(self, column_or_type):
        pass

    @abstractmethod
    def _goe_type_mapping_column_definitions(
        self,
        max_backend_precision,
        max_backend_scale,
        max_decimal_integral_magnitude,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        filter_column=None,
    ):
        """Returns a dict of dicts defining columns for GOE_TYPE_MAPPING test table.
        Supported dictionary keys:
            {'column': ColumnObject(...),                        # Column object used to create the frontend table
             'expected_canonical_column': CanonicalColumn(...),  # Expected canonical column
             'offload_options': {...},                           # dict of offload options to force a mapping
             'ascii_only': bool,                                 # Ascii 7 only (Synapse forced this on us)
             'notnull': bool,                                    # Don't insert NULLs (Synapse forced this on us)
             'ordered': bool,                                    # Insert ascending data into the column (IDs)
             'literals': [...],                                  # Data gen. literals, used when data types change
            },
        filter_column can be used to fetch just a single column dict.
        """

    @abstractmethod
    def _goe_chars_column_definitions(
        self, ascii_only=False, all_chars_notnull=False, supported_canonical_types=None
    ) -> list:
        pass

    @abstractmethod
    def _goe_types_column_definitions(
        self,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        include_interval_columns=True,
    ) -> list:
        pass

    @abstractmethod
    def _goe_wide_column_definitions(
        self, ascii_only=False, all_chars_notnull=False, supported_canonical_types=None
    ) -> list:
        pass

    @abstractmethod
    def _populate_generated_test_table(
        self, schema, table_name, columns, rows, fastexecute
    ):
        """This method populates a frontend object. It is expected to be called from an SH_TEST connection
        and not an ADM or APP one.
        columns is a list of dicts of the format:
          [
            {'column': ColumnObject(...), 'ascii_only': bool, 'notnull': bool},
          ]
        """

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def close(self, force=False):
        if self._db_api:
            self._db_api.close(force=force)

    def collect_table_stats(self, schema, table_name) -> list:
        sql = self.collect_table_stats_sql_text(schema, table_name)
        return self._db_api.execute_ddl(sql)

    def create_generated_table(
        self,
        schema: str,
        table_name: str,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        backend_max_test_column_count=None,
    ):
        """Create and populate a test auto-generated table"""
        self._log(f"Dropping {schema}.{table_name}")
        self.drop_table(schema, table_name)
        self._log(f"Creating {schema}.{table_name}")
        column_specs = self._generated_table_column_definitions(
            table_name,
            ascii_only=ascii_only,
            all_chars_notnull=all_chars_notnull,
            supported_canonical_types=supported_canonical_types,
            backend_max_test_column_count=backend_max_test_column_count,
        )
        self._log(
            "Columns: %s" % str([_["column"].name for _ in column_specs]),
            detail=VERBOSE,
        )
        self._log("Column specs: %s" % str(column_specs), detail=VVERBOSE)
        self._create_generated_test_table(schema, table_name, column_specs)
        self._log(f"Populating {schema}.{table_name}")
        rows = self._generated_table_row_count(table_name)
        self._log(f"Rows: {rows}", detail=VERBOSE)
        t = SimpleTimer(f"Populate: {schema}.{table_name}")
        self._populate_generated_test_table(schema, table_name, column_specs, rows)
        self._debug(t.show())
        self.collect_table_stats(schema, table_name)

    def create_goe_type_mapping(
        self,
        schema: str,
        ascii_only=False,
        all_chars_notnull=False,
        max_backend_precision=None,
        max_backend_scale=None,
        max_decimal_integral_magnitude=None,
        supported_canonical_types=None,
    ):
        """Create and populate GOE_TYPE_MAPPING"""
        table_name = test_constants.GOE_TYPE_MAPPING
        self._log(f"Dropping {schema}.{table_name}")
        self.drop_table(schema, table_name)
        self._log(f"Creating {schema}.{table_name}")
        column_spec_dict = self._goe_type_mapping_column_definitions(
            max_backend_precision=max_backend_precision,
            max_backend_scale=max_backend_scale,
            max_decimal_integral_magnitude=max_decimal_integral_magnitude,
            ascii_only=ascii_only,
            all_chars_notnull=all_chars_notnull,
            supported_canonical_types=supported_canonical_types,
        )
        self._log("Columns: %s" % str(sorted(column_spec_dict.keys())), detail=VERBOSE)
        # Convert column_spec_dict to a list of dicts to match other generated tables
        column_specs = [column_spec_dict[_] for _ in sorted(column_spec_dict.keys())]
        self._log("Column specs: %s" % str(column_specs), detail=VVERBOSE)
        self._create_generated_test_table(schema, table_name, column_specs)
        self._log(f"Populating {schema}.{table_name}")
        rows = self._generated_table_row_count(table_name)
        self._log(f"Rows: {rows}", detail=VERBOSE)
        t = SimpleTimer(f"Populate: {schema}.{table_name}")
        self._populate_generated_test_table(
            schema, table_name, column_specs, rows, fastexecute=False
        )
        self._debug(t.show())
        self.collect_table_stats(schema, table_name)

    def create_new_connection(
        self, user_name, user_password, trace_action_override=None
    ) -> "FrontendTestingApiInterface":
        """Creates a new FrontendTestingApi object for the already configured backend but with an alternative
        username and password.
        We only support a simple username/password for this entry point. It is not a true constructor
        because it re-uses orchestration config and messages objects.
        """
        connection = self._db_api.create_new_connection(
            user_name, user_password, trace_action_override=trace_action_override
        )
        return self._create_new_testing_client(
            connection, trace_action_override=trace_action_override
        )

    @contextmanager
    def create_new_connection_ctx(
        self, user_name, user_password, trace_action_override=None
    ):
        new_api = self.create_new_connection(
            user_name, user_password, trace_action_override=trace_action_override
        )
        yield new_api
        try:
            new_api.close(force=True)
        except:
            pass

    def enclose_object_reference(self, schema, object_name):
        return self._db_api.enclose_object_reference(schema, object_name)

    def execute_ddl(self, sql, query_options=None, log_level=VERBOSE) -> list:
        return self._db_api.execute_ddl(
            sql, query_options=query_options, log_level=log_level
        )

    def execute_query_fetch_all(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        query_params=None,
        profile=False,
        trace_action=None,
    ):
        return self._db_api.execute_query_fetch_all(
            sql,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            query_params=query_params,
            profile=profile,
            trace_action=trace_action,
        )

    def execute_query_fetch_one(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        query_params=None,
        profile=False,
        trace_action=None,
    ):
        return self._db_api.execute_query_fetch_one(
            sql,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            query_params=query_params,
            profile=profile,
            trace_action=trace_action,
        )

    def frontend_table_partition_list(
        self, schema: str, table_name: str, hv_string_list: Optional[list] = None
    ) -> list:
        frontend_table = OffloadSourceTable.create(
            schema, table_name, self._connection_options, self._messages
        )
        partitions = frontend_table.get_partitions()
        if hv_string_list:
            partitions = [
                p
                for p in partitions
                if any(_ in p.high_values_csv for _ in hv_string_list)
            ]
        return partitions

    def frontend_version(self):
        return self._db_api.frontend_version()

    def gen_column_object(self, column_name, **kwargs):
        return self._db_api.gen_column_object(column_name, **kwargs)

    def get_columns(self, schema, table_name):
        return self._db_api.get_columns(schema, table_name)

    def get_max_range_partition_name_and_hv(
        self, schema: str, table_name: str
    ) -> tuple:
        """Return a tuple of name, high value literal for last partition in a RANGE table."""
        partitions = self.frontend_table_partition_list(schema, table_name)
        # partitions are newest to oldest so we can pluck first row to satisfy this function.
        return partitions[0].partition_name, partitions[0].high_values_csv

    def get_table_default_parallelism(self, schema, table_name):
        return self._db_api.get_table_default_parallelism(schema, table_name)

    def get_session_option(self, option_name):
        return self._db_api.get_session_option(option_name)

    def get_view_ddl(self, schema, view_name):
        return self._db_api.get_object_ddl(schema, view_name, GET_DDL_TYPE_VIEW)

    def goe_type_mapping_expected_canonical_cols(
        self, max_backend_precision, max_backend_scale, max_decimal_integral_magnitude
    ):
        """Return a list of tuples containing expected column/type mappings and any user override, e.g.:
        [(column name, CanonicalColumn(...), {'integer_1_columns_csv': 'column name'}), ... ]
        """
        definitions = self._goe_type_mapping_column_definitions(
            max_backend_precision, max_backend_scale, max_decimal_integral_magnitude
        )
        canonical_type_mappings = []
        for col_name in sorted(definitions.keys()):
            col_dict = definitions[col_name]
            canonical_type_mappings.append(
                (
                    col_dict["column"].name,
                    col_dict["expected_canonical_column"],
                    col_dict.get("offload_options"),
                )
            )
        return canonical_type_mappings

    def goe_type_mapping_offload_options(
        self, max_backend_precision, max_backend_scale, max_decimal_integral_magnitude
    ) -> dict:
        """Return a dictionary of offload datatype control options/values for testing purposes."""
        definitions = self._goe_type_mapping_column_definitions(
            max_backend_precision, max_backend_scale, max_decimal_integral_magnitude
        )
        offload_options = {"data_sample_pct": 0}
        for col_dict in definitions.values():
            if col_dict.get("offload_options"):
                opts = col_dict["offload_options"]
                for opt in opts:
                    if opt in offload_options and isinstance(
                        offload_options[opt], list
                    ):
                        offload_options[opt] += opts[opt]
                    elif opt in offload_options:
                        offload_options[opt] += "," + opts[opt]
                    else:
                        offload_options[opt] = opts[opt]
        return offload_options

    def goe_wide_max_test_column_count(self):
        return 800

    def max_table_name_length(self):
        return self._db_api.max_table_name_length()

    def min_datetime_value(self):
        return self._db_api.min_datetime_value()

    def get_table_row_count(
        self, schema, table_name, filter_clause=None, filter_clause_params=None
    ) -> int:
        return self._db_api.get_table_row_count(
            schema,
            table_name,
            filter_clause=filter_clause,
            filter_clause_params=filter_clause_params,
        )

    def split_partition_high_value_string(self, hv_string):
        return self._db_api.split_partition_high_value_string(hv_string)

    def canonical_date_supported(self):
        return self._db_api.canonical_date_supported()

    def goe_join_pushdown_supported(self):
        return self._db_api.goe_join_pushdown_supported()

    def goe_lpa_supported(self):
        return self._db_api.goe_lpa_supported()

    def goe_multi_column_incremental_key_supported(self) -> bool:
        return self._db_api.goe_multi_column_incremental_key_supported()

    def goe_offload_status_report_supported(self):
        return self._db_api.goe_offload_status_report_supported()

    def goe_schema_sync_supported(self) -> bool:
        return self._db_api.goe_schema_sync_supported()

    def lobs_support_minus_operator(self):
        """Oracle has an override for this."""
        return True

    def lob_safe_table_projection(self, schema, table_name) -> str:
        """Returns a CSV of columns names for use in SQL with LOB columns protected if required for the frontend.
        This basic version returns all columns unmodified, some frontends may override.
        """
        return ",".join(self._db_api.get_column_names(schema, table_name))

    def nan_supported(self) -> bool:
        return self._db_api.nan_supported()

    def nanoseconds_supported(self) -> bool:
        return self._db_api.nanoseconds_supported()

    def schema_evolution_supported(self) -> bool:
        return self._db_api.schema_evolution_supported()

    def table_exists(self, schema, table_name) -> bool:
        return self._db_api.table_exists(schema, table_name)

    def table_minus_row_count(
        self,
        schema1: str,
        table_name1: str,
        schema2: str,
        table_name2: Optional[str] = None,
        column: Optional[str] = None,
        where_clause: Optional[str] = None,
    ) -> int:
        """Return row count from a (SELECT * FROM table1 MINUS SELECT * FROM table2) query."""
        projection = (
            column or self.lob_safe_table_projection(schema1, table_name1) or "*"
        )
        where_clause = where_clause or ""
        table_name2 = table_name2 or table_name1
        self._log(
            f"table_minus_row_count: {schema1}.{table_name1} vs {schema2}.{table_name2}",
            detail=VERBOSE,
        )
        q1 = f"SELECT %s FROM %s.%s %s" % (
            projection,
            schema1,
            table_name1,
            where_clause,
        )
        q2 = f"SELECT %s FROM %s.%s %s" % (
            projection,
            schema2,
            table_name2,
            where_clause,
        )
        q = "SELECT COUNT(*) FROM (%s MINUS %s)" % (q1, q2)
        self._log("table_minus_row_count qry: %s" % q, detail=VVERBOSE)
        row = self._db_api.execute_query_fetch_one(q)
        return_count = row[0]
        return return_count

    def view_exists(self, schema, view_name) -> bool:
        return self._db_api.view_exists(schema, view_name)

    def view_is_valid(self, schema, view_name) -> bool:
        """
        Return True/False if a view is valid. Interface method returns True, individual implementations may override.
        """
        return True

    # Enforced methods/properties

    @abstractmethod
    def collect_table_stats_sql_text(self, schema, table_name) -> str:
        pass

    @abstractmethod
    def remove_table_stats_sql_text(self, schema, table_name) -> str:
        pass

    @abstractmethod
    def drop_table(self, schema, table_name):
        """Obviously this is dangerous, that's why it is in this TestingApi only."""

    @abstractmethod
    def expected_std_dim_offload_predicates(self):
        """Return a list of tuples of GOE offload predicates and expected frontend predicate
        tuple format:
            (predicate_dsl, expected_sql)
        """

    @abstractmethod
    def expected_sales_offload_predicates(self):
        """Return a list of tuples of GOE offload predicates and expected frontend predicate.
        tuple format:
            (predicate_dsl, expected_sql, expected_bind_sql, expected_binds)
        or if binds are not relevant:
            (predicate_dsl, expected_sql, None, None)
        """

    @abstractmethod
    def gen_ctas_from_subquery(
        self,
        schema: str,
        table_name: str,
        subquery: str,
        pk_col_name: Optional[str] = None,
        table_parallelism: Optional[str] = None,
        with_drop: bool = True,
        with_stats_collection: bool = False,
    ) -> list:
        """Return a list[str] of SQL required to CTAS a table from a subquery."""

    @abstractmethod
    def get_test_table_owner(self, expected_schema: str, table_name: str) -> str:
        """Return the schema of a test table. Useful because in Teamcity test schemas can have v tag appended."""

    @abstractmethod
    def goe_type_mapping_generated_table_col_specs(
        self,
        max_backend_precision,
        max_backend_scale,
        max_decimal_integral_magnitude,
        supported_canonical_types,
        ascii_only=False,
    ):
        """Return a list of column specs matching how test.generated_tables expects and a list of column names.
        This is not how we want to pass column specs around but it matches existing code in test.
        Returned lists are sorted by column name.
        """

    @abstractmethod
    def host_compare_sql_projection(self, column_list: list) -> str:
        """Return a SQL projection (CSV of column expressions) used to validate offloaded data.
        Timestamps:
            Because some systems do not have canonical dates or have differing scales for time elements all
            date based values must be normalised to UTC in format:
                'YYYY-MM-DD HH24:MI:SS.FFF +00:00'
        Intervals:
            Some clients do not support interval data types so convert them to string in the validation SQL.
        Numbers:
            All numbers need to be strings because:
                >>> -8461.633 == Decimal('-8461.633')
                False
        """

    @abstractmethod
    def run_sql_in_file(self, local_path):
        """Opens the file in local_path and executes each SQL statement it contains.
        Method expects to be called from an SH_TEST connection and not an ADM or APP one.
        """

    @abstractmethod
    def sales_based_fact_create_ddl(
        self,
        schema: str,
        table_name: str,
        maxval_partition: bool = False,
        extra_pred: Optional[str] = None,
        degree: Optional[int] = None,
        subpartitions: int = 0,
        enable_row_movement: bool = False,
        noseg_partition: bool = True,
        part_key_type: Optional[str] = None,
        time_id_column_name: Optional[str] = None,
        extra_col_tuples: Optional[list] = None,
        simple_partition_names: bool = False,
        with_drop: bool = True,
        range_start_literal_override=None,
    ) -> list:
        pass

    @abstractmethod
    def sales_based_fact_add_partition_ddl(self, schema: str, table_name: str) -> list:
        pass

    @abstractmethod
    def sales_based_fact_drop_partition_ddl(
        self,
        schema: str,
        table_name: str,
        hv_string_list: list,
        dropping_oldest: Optional[bool] = None,
    ) -> list:
        pass

    @abstractmethod
    def sales_based_fact_truncate_partition_ddl(
        self, schema: str, table_name: str, hv_string_list: Optional[list] = None
    ) -> list:
        pass

    @abstractmethod
    def sales_based_fact_hwm_literal(self, sales_literal: str, data_type: str) -> tuple:
        """
        Takes a SALES story constant and data type and returns a tuple of 3 strings.
        Tuple format:
            (literal suitable for data type,
             literal suitable for searching within HV metadata,
             literal suitable for a SQL predicate)
        """

    @abstractmethod
    def sales_based_fact_late_arriving_data_sql(
        self,
        schema: str,
        table_name: str,
        time_id_literal: str,
        channel_id_literal: int = 1,
    ) -> list:
        pass

    @abstractmethod
    def sales_based_list_fact_create_ddl(
        self,
        schema: str,
        table_name: str,
        default_partition: bool = False,
        extra_pred: Optional[str] = None,
        part_key_type: Optional[str] = None,
        out_of_sequence: bool = False,
        include_older_partition: bool = False,
        yrmon_column_name: Optional[str] = None,
        extra_col_tuples: Optional[list] = None,
        with_drop: bool = True,
    ) -> list:
        pass

    @abstractmethod
    def sales_based_list_fact_add_partition_ddl(
        self, schema: str, table_name: str, next_ym_override: Optional[tuple] = None
    ) -> list:
        pass

    @abstractmethod
    def sales_based_multi_col_fact_create_ddl(
        self, schema: str, table_name: str, maxval_partition=False
    ) -> list:
        pass

    @abstractmethod
    def sales_based_subpartitioned_fact_ddl(
        self, schema: str, table_name: str, top_level="LIST", rowdependencies=False
    ) -> list:
        pass

    @abstractmethod
    def sales_based_list_fact_late_arriving_data_sql(
        self, schema: str, table_name: str, time_id_literal: str, yrmon_string: str
    ) -> list:
        pass

    @abstractmethod
    def select_grant_exists(
        self,
        schema: str,
        table_name: str,
        to_user: str,
        grantable: Optional[bool] = None,
    ) -> bool:
        """
        Return bool depending on whether to_user has SELECT privileges on source table.
        grantable: None mean don't check. True/False means ensure the privilege is or isn't grantable.
        """

    @abstractmethod
    def table_row_count_from_stats(
        self, schema: str, table_name: str
    ) -> Union[int, None]:
        """Return row count specifically from stats - not from a select on the table."""

    @abstractmethod
    def test_type_canonical_date(self) -> str:
        """A number of tests need a date type for testing, this method avoids hardcoding in multiple places."""

    @abstractmethod
    def test_type_canonical_decimal(self) -> str:
        """A number of tests need a decimal type for testing, this method avoids hardcoding in multiple places."""

    @abstractmethod
    def test_type_canonical_int_8(self) -> str:
        """A number of tests need an integral type for testing, this method avoids hardcoding in multiple places."""

    @abstractmethod
    def test_type_canonical_string(self) -> str:
        """A number of tests need a string type for testing, this method avoids hardcoding in multiple places."""

    @abstractmethod
    def test_type_canonical_timestamp(self) -> str:
        """A number of tests need a datetime type for testing, this method avoids hardcoding in multiple places."""

    @abstractmethod
    def test_time_zone_query_option(self, tz) -> dict:
        """Return a query option dict used to set the time zone in when running frontend query"""

    @abstractmethod
    def unit_test_query_options(self):
        """Return a dict of a valid query/session setting in order to unit test OffloadSourceTable"""
