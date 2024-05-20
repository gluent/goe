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

""" MSSQLSourceTable: Library for logic/interaction with MSSQL source of an offload
"""

import inspect
import logging
from typing import Union

from goe.offload.column_metadata import (
    CanonicalColumn,
    is_safe_mapping,
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
    GOE_TYPE_BOOLEAN,
    ALL_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
)
from goe.offload.microsoft.mssql_column import (
    MSSQLColumn,
    MSSQL_TYPE_BIGINT,
    MSSQL_TYPE_BIT,
    MSSQL_TYPE_DECIMAL,
    MSSQL_TYPE_INT,
    MSSQL_TYPE_MONEY,
    MSSQL_TYPE_NUMERIC,
    MSSQL_TYPE_SMALLINT,
    MSSQL_TYPE_SMALLMONEY,
    MSSQL_TYPE_TINYINT,
    MSSQL_TYPE_FLOAT,
    MSSQL_TYPE_REAL,
    MSSQL_TYPE_DATE,
    MSSQL_TYPE_DATETIME2,
    MSSQL_TYPE_DATETIME,
    MSSQL_TYPE_DATETIMEOFFSET,
    MSSQL_TYPE_SMALLDATETIME,
    MSSQL_TYPE_TIME,
    MSSQL_TYPE_CHAR,
    MSSQL_TYPE_VARCHAR,
    MSSQL_TYPE_NCHAR,
    MSSQL_TYPE_NVARCHAR,
    MSSQL_TYPE_UNIQUEIDENTIFIER,
    MSSQL_TYPE_TEXT,
    MSSQL_TYPE_NTEXT,
    MSSQL_TYPE_BINARY,
    MSSQL_TYPE_VARBINARY,
    MSSQL_TYPE_IMAGE,
)
from goe.offload.offload_source_table import OffloadSourceTableInterface


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# CONSTANTS
###########################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# CLASSES
###########################################################################


class MSSQLSourceTable(OffloadSourceTableInterface):
    """Microsoft SQL Server source table details and methods"""

    def __init__(
        self,
        schema_name,
        table_name,
        connection_options,
        messages,
        dry_run=False,
        do_not_connect=False,
    ):
        assert schema_name and table_name and connection_options
        assert hasattr(connection_options, "rdbms_app_user")
        assert hasattr(connection_options, "rdbms_app_pass")
        assert hasattr(connection_options, "rdbms_dsn")

        super(MSSQLSourceTable, self).__init__(
            schema_name,
            table_name,
            connection_options,
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
        )

        logger.info(
            "MSSQLSourceTable setup: (%s, %s, %s)"
            % (schema_name, table_name, connection_options.rdbms_app_user)
        )
        if dry_run:
            logger.info("* Dry run *")

        # attributes below are specified as private so they can be enforced
        # (where relevant) as properties via base class
        self._iot_type = None
        self._partitioned = None
        self._partition_type = None
        self._subpartition_type = None
        self._partitions = None
        self._dependencies = None
        self._parallelism = None

        if not do_not_connect:
            # Unit testing requires no db connection
            self._get_table_details()
            self._columns_setter(self._get_column_details(), skip_exists_check=True)
            if self._table_exists:
                self._size_in_bytes = self._get_table_size()
                self._hash_bucket_candidate = self._get_hash_bucket_candidate()
                # TODO NJ@2017-02-28 Implement table stats when we pick up MSSQL work again
                self._table_stats = None
            else:
                self._hash_bucket_candidate = None
                self._table_stats = None

    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _extra_data_type_supported_checks(self):
        # Check (n)varchar is not max precision
        if [
            tc.name
            for tc in self._columns
            if "varchar" in tc.data_type and tc.data_length == -1
        ]:
            [
                self._messages.log(
                    "MAX precision not supported for (n)varchar column: %s" % tc.name
                )
                for tc in self._columns
                if "varchar" in tc.data_type and tc.data_length == -1
            ]
            return False

        return True

    def _get_column_low_high_values(self, column_name, from_stats=True, sample_perc=1):
        raise NotImplementedError("MSSQL _get_column_low_high_values not implemented.")

    def _get_table_details(self):
        logger.debug("_get_table_details: %s, %s" % (self.owner, self.table_name))
        # order of columns important as referenced in helper functions
        q = """
            WITH partitions AS (
                SELECT DISTINCT partition_number
                ,      rows
                FROM   sys.tables       t
                       INNER JOIN
                       sys.partitions   p
                       ON (p.object_id = t.object_id)
                       INNER JOIN
                       sys.schemas      s
                       ON (t.schema_id = s.schema_id)
                WHERE  s.name = %s
                AND    t.name = %s
                )
            SELECT NULL             AS iot_type
            ,      SUM(rows)        AS num_rows
            ,      CASE
                     WHEN MAX(partition_number) > 1
                     THEN 'YES'
                     ELSE 'NO'
                   END              AS partitioned
            ,      NULL             AS partitioning_type
            FROM partitions"""

        # TODO: what if we don't have SELECT ON SCHEMA::xyz granted TO [GOE_APP] - below will cause exception
        row = self._db_api.execute_query_fetch_one(
            q, query_params=(self.owner, self.table_name)
        )
        if row:
            (
                self._iot_type,
                self._stats_num_rows,
                self._partitioned,
                self._partition_type,
            ) = row
            self._table_exists = True if self._stats_num_rows is not None else False
        else:
            self._table_exists = False

    def _get_columns_with_partition_info(self, part_col_names_override=None):
        raise NotImplementedError(
            "MSSQL _get_columns_with_partition_info not implemented."
        )

    def _get_hash_bucket_candidate(self):
        """
        Seems that unlike Oracle, column statistics are manually created using for e.g.:
           CREATE STATISTICS STATS_COL1 ON SH_TEST.ALL_SUPPORTED_DATA_TYPES_NOPK (COL1) WITH FULLSCAN
        Unfortuntately it seems access to view statistcs is done through stored procedures rather than viewing system views
        We can list the statistics objects on a table using:
            SP_AUTOSTATS "sh_test.all_supported_data_types_nopk"
        And for each statistic object (called "Index Name") we can get the density using
            DBCC SHOW_STATISTICS ("sh_test.all_supported_data_types_nopk", stats_col4) WITH DENSITY_VECTOR
        The "All density" is the value we need (0 best, 1 worst)
        For now this is the best we have, the column with the "All density" value closest to 0
        In the absence of any column stats we return None and goe.py will use the first column in the table
        SP_AUTOSTATS is not valid for Azure Serverless SQL Pools
        """
        rows = self._db_api.execute_query_fetch_all(
            'SP_AUTOSTATS "{schema}.{table}"'.format(
                schema=self.owner, table=self.table_name
            )
        )
        hash_bucket_density, hash_bucket_column_name = 2, None
        if rows:
            for row in rows:
                stats_rows = self._db_api.execute_query_fetch_all(
                    'DBCC SHOW_STATISTICS ("{schema}.{table}", {stats_col}) WITH DENSITY_VECTOR'.format(
                        schema=self.owner, table=self.table_name, stats_col=row[0]
                    )
                )
                stats_col = [
                    stats_row
                    for stats_row in stats_rows
                    if len(stats_row[2].split(",")) == 1
                ]
                if len(stats_col) > 0:
                    if stats_col[0][0] < hash_bucket_density:
                        hash_bucket_density, hash_bucket_column_name = (
                            stats_col[0][0],
                            stats_col[0][2],
                        )
            return hash_bucket_column_name
        else:
            return None

    def _get_table_stats(self):
        # TODO NJ@2017-02-07 When we revisit MSSQL support we need to decide if having the stats is required
        raise NotImplementedError("MSSQL table_stats not implemented.")
        # return self._table_stats

    def _is_compression_enabled(self) -> bool:
        raise NotImplementedError("MSSQL _is_compression_enabled not implemented.")

    def _sample_data_types_compression_factor(self):
        raise NotImplementedError(
            "MSSQL _sample_data_types_compression_factor not implemented."
        )

    def _sample_data_types_data_sample_parallelism(self, data_sample_parallelism):
        raise NotImplementedError(
            "MSSQL _sample_data_types_compression_factor not implemented."
        )

    def _sample_data_types_data_sample_pct(self, data_sample_pct):
        raise NotImplementedError(
            "MSSQL _sample_data_types_data_sample_pct not implemented."
        )

    def _sample_data_types_date_as_string_column(self, column_name):
        raise NotImplementedError(
            "MSSQL _sample_data_types_date_as_string_column not implemented."
        )

    def _sample_data_types_decimal_column(self, column, data_precision, data_scale):
        return MSSQLColumn(
            column.name,
            MSSQL_TYPE_NUMERIC,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=column.nullable,
            safe_mapping=False,
        )

    def _sample_data_types_max_pct(self):
        raise NotImplementedError("MSSQL _sample_data_types_max_pct not implemented.")

    def _sample_data_types_min_pct(self):
        raise NotImplementedError("MSSQL _sample_data_types_min_pct not implemented.")

    def _sample_data_types_min_gb(self):
        """1GB seems like a good target"""
        return 1

    def _sample_perc_sql_clause(self, data_sample_pct) -> str:
        return ""

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @property
    def partition_type(self):
        return self._partition_type

    @property
    def offload_by_subpartition(self):
        return False

    @property
    def offload_partition_level(self) -> Union[int, None]:
        return None

    @property
    def parallelism(self):
        return self._parallelism

    def decode_partition_high_values(self, hv_csv, strict=True) -> tuple:
        raise NotImplementedError("MSSQL decode_partition_high_values not implemented.")

    def enable_offload_by_subpartition(self, desired_state=True):
        raise NotImplementedError(
            "MSSQL enable_offload_by_subpartition() not implemented."
        )

    def get_current_scn(self, return_none_on_failure=False):
        # TODO: Can get min_active_rowversion() for database which is equivalent to ORA_ROWSCN?
        # https://www.mssqltips.com/sqlservertip/3423/sql-server-rowversion-functions-minactiverowversion-vs-dbts/
        raise NotImplementedError("MSSQL get_current_scn not implemented.")

    def get_hash_bucket_candidate(self):
        return self._hash_bucket_candidate

    def get_partitions(self, strict=True, populate_hvs=True):
        """Fetch list of partitions for table. This can be time consuming therefore
        executed on demand and not during instantiation
        """
        if not self._partitions and self.is_partitioned():
            raise NotImplementedError("MSSQL get_partitions not implemented.")
        return self._partitions

    def get_session_option(self, option_name):
        raise NotImplementedError("MSSQL get_session_option not implemented.")

    def get_max_partition_size(self):
        """Return the size of the largest partition"""
        if self.is_partitioned():
            raise NotImplementedError("MSSQL get_max_partition_size not implemented.")
        return None

    def is_iot(self):
        return self._iot_type == "IOT"

    def is_partitioned(self):
        return self._partitioned == "YES"

    def is_subpartitioned(self):
        return False

    @staticmethod
    def supported_data_types() -> set:
        return set(
            [
                MSSQL_TYPE_BIGINT,
                MSSQL_TYPE_BIT,
                MSSQL_TYPE_DECIMAL,
                MSSQL_TYPE_INT,
                MSSQL_TYPE_MONEY,
                MSSQL_TYPE_NUMERIC,
                MSSQL_TYPE_SMALLINT,
                MSSQL_TYPE_SMALLMONEY,
                MSSQL_TYPE_TINYINT,
                MSSQL_TYPE_FLOAT,
                MSSQL_TYPE_REAL,
                MSSQL_TYPE_DATE,
                MSSQL_TYPE_DATETIME2,
                MSSQL_TYPE_DATETIME,
                MSSQL_TYPE_DATETIMEOFFSET,
                MSSQL_TYPE_SMALLDATETIME,
                MSSQL_TYPE_TIME,
                MSSQL_TYPE_CHAR,
                MSSQL_TYPE_VARCHAR,
                MSSQL_TYPE_NCHAR,
                MSSQL_TYPE_NVARCHAR,
                MSSQL_TYPE_UNIQUEIDENTIFIER,
            ]
        )

    def check_nanosecond_offload_allowed(
        self, backend_max_datetime_scale, allow_nanosecond_timestamp_columns=None
    ):
        raise NotImplementedError(
            "MSSQL check_nanosecond_offload_allowed() not implemented."
        )

    def supported_range_partition_data_type(self, rdbms_data_type):
        raise NotImplementedError(
            "MSSQL supported_range_partition_data_type() not implemented."
        )

    def supported_list_partition_data_type(self, rdbms_data_type):
        raise NotImplementedError(
            "MSSQL supported_list_partition_data_type() not implemented."
        )

    @staticmethod
    def hash_bucket_unsuitable_data_types():
        return set([])

    @staticmethod
    def nan_capable_data_types():
        return set([])

    @staticmethod
    def datetime_literal_to_python(rdbms_literal, strict=True):
        raise NotImplementedError("MSSQL datetime_literal_to_python() not implemented.")

    @staticmethod
    def numeric_literal_to_python(rdbms_literal):
        raise NotImplementedError("MSSQL numeric_literal_to_python() not implemented.")

    def rdbms_literal_to_python(
        self, rdbms_column, rdbms_literal, partition_type, strict=True
    ):
        raise NotImplementedError("MSSQL rdbms_literal_to_python() not implemented.")

    def get_minimum_partition_key_data(self):
        """Returns lowest point of data stored in source MSSQL table"""
        assert self.partition_columns
        return []

    def to_rdbms_literal_with_sql_conv_fn(self, py_val, rdbms_data_type):
        raise NotImplementedError(
            "MSSQL to_rdbms_literal_with_sql_conv_fn() not implemented."
        )

    def get_suitable_sample_size(self, bytes_override=None) -> int:
        # Return something even though sampling is not implemented
        return 0

    def sample_rdbms_data_types(
        self,
        columns_to_sample,
        data_sample_pct,
        data_sample_parallelism,
        backend_min_possible_date,
        backend_max_decimal_integral_magnitude,
        backend_max_decimal_scale,
        allow_decimal_scale_rounding,
    ):
        # Return something even though sampling is not implemented
        return []

    def supported_partition_data_types(self):
        raise NotImplementedError(
            "MSSQL supported_partition_data_types() not implemented."
        )

    def offload_by_subpartition_capable(self, valid_for_auto_enable=False):
        return False

    def partition_has_rows(self, partition_name):
        raise NotImplementedError("MSSQL partition_has_rows() not implemented.")

    def predicate_has_rows(self, predicate):
        raise NotImplementedError(
            self.__class__.__name__ + "." + inspect.currentframe().f_code.co_names
        )

    def predicate_to_where_clause(self, predicate, columns_override=None):
        raise NotImplementedError(
            self.__class__.__name__ + "." + inspect.currentframe().f_code.co_names
        )

    def predicate_to_where_clause_with_binds(self, predicate):
        raise NotImplementedError(
            self.__class__.__name__ + "." + inspect.currentframe().f_code.co_names
        )

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, MSSQLColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.data_type == MSSQL_TYPE_BIT:
            return bool(target_type == GOE_TYPE_BOOLEAN)
        elif column.data_type in [MSSQL_TYPE_CHAR, MSSQL_TYPE_NCHAR]:
            return bool(target_type == GOE_TYPE_FIXED_STRING)
        elif column.data_type in [MSSQL_TYPE_TEXT, MSSQL_TYPE_NTEXT]:
            return bool(target_type == GOE_TYPE_LARGE_STRING)
        elif column.data_type in [
            MSSQL_TYPE_VARCHAR,
            MSSQL_TYPE_NVARCHAR,
            MSSQL_TYPE_UNIQUEIDENTIFIER,
        ]:
            return bool(target_type == GOE_TYPE_VARIABLE_STRING)
        elif column.data_type in [
            MSSQL_TYPE_BINARY,
            MSSQL_TYPE_VARBINARY,
            MSSQL_TYPE_IMAGE,
        ]:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        elif column.data_type == MSSQL_TYPE_FLOAT:
            return bool(target_type == GOE_TYPE_DOUBLE)
        elif column.data_type == MSSQL_TYPE_REAL:
            return bool(target_type == GOE_TYPE_FLOAT)
        elif column.is_number_based():
            return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based() and column.is_time_zone_based():
            return bool(target_type == GOE_TYPE_TIMESTAMP_TZ)
        elif column.is_date_based():
            return bool(
                target_type in [GOE_TYPE_DATE, GOE_TYPE_TIMESTAMP]
                or target_type in STRING_CANONICAL_TYPES
            )
        elif column.data_type == MSSQL_TYPE_TIME:
            return bool(target_type == GOE_TYPE_TIME)
        elif target_type not in ALL_CANONICAL_TYPES:
            # Ideally we would log something here but this class has no messages object
            # self._log('Unknown canonical type in mapping: %s' % target_type, detail=VVERBOSE)
            return False
        return False

    def to_canonical_column(self, column):
        """Translate an MSSQL column to an internal GOE column"""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable & data_default forward from RDBMS
            Not carrying partition information formward to the canonical column because that will
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
                partition_info=None,
            )

        assert column
        assert isinstance(column, MSSQLColumn)

        if column.data_type == MSSQL_TYPE_BIT:
            return new_column(column, GOE_TYPE_BOOLEAN)
        elif column.data_type in (MSSQL_TYPE_CHAR, MSSQL_TYPE_NCHAR):
            return new_column(
                column,
                GOE_TYPE_FIXED_STRING,
                data_length=column.data_length,
                safe_mapping=True,
            )
        elif column.data_type in (MSSQL_TYPE_TEXT, MSSQL_TYPE_NTEXT):
            return new_column(column, GOE_TYPE_LARGE_STRING)
        elif column.data_type in (
            MSSQL_TYPE_VARCHAR,
            MSSQL_TYPE_NVARCHAR,
            MSSQL_TYPE_UNIQUEIDENTIFIER,
        ):
            return new_column(
                column, GOE_TYPE_VARIABLE_STRING, data_length=column.data_length
            )
        elif column.data_type in (
            MSSQL_TYPE_BINARY,
            MSSQL_TYPE_VARBINARY,
            MSSQL_TYPE_IMAGE,
        ):
            return new_column(column, GOE_TYPE_BINARY, data_length=column.data_length)
        elif column.data_type in (MSSQL_TYPE_TINYINT, MSSQL_TYPE_SMALLINT):
            return new_column(column, GOE_TYPE_INTEGER_2)
        elif column.data_type == MSSQL_TYPE_INT:
            return new_column(column, GOE_TYPE_INTEGER_4)
        elif column.data_type == MSSQL_TYPE_BIGINT:
            return new_column(column, GOE_TYPE_INTEGER_8)
        elif column.data_type == MSSQL_TYPE_FLOAT:
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == MSSQL_TYPE_REAL:
            return new_column(column, GOE_TYPE_FLOAT)
        elif column.data_type in (
            MSSQL_TYPE_DECIMAL,
            MSSQL_TYPE_NUMERIC,
            MSSQL_TYPE_MONEY,
            MSSQL_TYPE_SMALLMONEY,
        ):
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
                    # The precision overflows our canonical integral types so store as a decimal
                    integral_type = GOE_TYPE_DECIMAL
                return new_column(
                    column, integral_type, data_precision=data_precision, data_scale=0
                )
            else:
                # If precision & scale are None then this is unsafe, otherwise leave it None to let new_column() logic take over
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
        elif column.data_type == MSSQL_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE)
        elif column.data_type == MSSQL_TYPE_TIME:
            return new_column(column, GOE_TYPE_TIME)
        elif column.data_type in (
            MSSQL_TYPE_SMALLDATETIME,
            MSSQL_TYPE_DATETIME,
            MSSQL_TYPE_DATETIME2,
        ):
            return new_column(column, GOE_TYPE_TIMESTAMP)
        elif column.data_type == MSSQL_TYPE_DATETIMEOFFSET:
            return new_column(column, GOE_TYPE_TIMESTAMP_TZ)
        else:
            raise NotImplementedError(
                "Unsupported MSSQL data type: %s" % column.data_type
            )

    def from_canonical_column(self, column):
        # Present is not yet in scope
        raise NotImplementedError("MSSQL from_canonical_column() not implemented.")

    def gen_column(
        self,
        name,
        data_type,
        data_length=None,
        data_precision=None,
        data_scale=None,
        nullable=None,
        data_default=None,
        hidden=None,
        char_semantics=None,
        char_length=None,
    ):
        return MSSQLColumn(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            hidden=hidden,
            char_semantics=char_semantics,
            char_length=char_length,
        )

    def gen_default_numeric_column(self, column_name):
        return self.gen_column(column_name, MSSQL_TYPE_NUMERIC)

    def gen_default_date_column(self, column_name):
        return self.gen_column(column_name, MSSQL_TYPE_DATE)

    def transform_encrypt_data_type(self):
        return MSSQL_TYPE_VARCHAR

    def transform_null_cast(self, rdbms_column):
        assert isinstance(rdbms_column, MSSQLColumn)
        return "CAST(NULL AS %s)" % (rdbms_column.format_data_type())

    def transform_tokenize_data_type(self):
        return MSSQL_TYPE_VARCHAR

    def transform_regexp_replace_expression(
        self, backend_column, regexp_replace_pattern, regexp_replace_string
    ):
        # SQL Server has no support for regular expressions without writing a function
        raise NotImplementedError(
            "MSSQL transform_regexp_replace_expression() not implemented."
        )

    def transform_translate_expression(self, backend_column, from_string, to_string):
        # SQL Server has support for TRANSLATE() only for versions 2017 and above
        raise NotImplementedError(
            "MSSQL transform_translate_expression() not implemented."
        )
