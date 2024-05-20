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

""" TeradataSourceTable: Library for logic/interaction with Teradata source of an offload
"""

from datetime import date, datetime
import logging
import re
from textwrap import dedent
from typing import Optional, Union

from numpy import datetime64

from goe.offload import offload_constants
from goe.offload.column_metadata import (
    CanonicalColumn,
    is_safe_mapping,
    match_table_column,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
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
    CANONICAL_CHAR_SEMANTICS_CHAR,
    ALL_CANONICAL_TYPES,
    DATE_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_source_table import (
    OffloadSourceTableInterface,
    OffloadSourceTableException,
    RdbmsPartition,
    convert_high_values_to_python,
    OFFLOAD_PARTITION_TYPE_RANGE,
)
from goe.offload.teradata.teradata_column import (
    TeradataColumn,
    TERADATA_TYPE_BIGINT,
    TERADATA_TYPE_BLOB,
    TERADATA_TYPE_BYTE,
    TERADATA_TYPE_BYTEINT,
    TERADATA_TYPE_CHAR,
    TERADATA_TYPE_CLOB,
    TERADATA_TYPE_DATE,
    TERADATA_TYPE_DECIMAL,
    TERADATA_TYPE_DOUBLE,
    TERADATA_TYPE_INTEGER,
    TERADATA_TYPE_INTERVAL_YM,
    TERADATA_TYPE_INTERVAL_DS,
    TERADATA_TYPE_NUMBER,
    TERADATA_TYPE_SMALLINT,
    TERADATA_TYPE_TIME,
    TERADATA_TYPE_TIMESTAMP,
    TERADATA_TYPE_TIME_TZ,
    TERADATA_TYPE_TIMESTAMP_TZ,
    TERADATA_TYPE_VARBYTE,
    TERADATA_TYPE_VARCHAR,
    TERADATA_TIMESTAMP_RE,
)
from goe.offload.teradata.teradata_frontend_api import (
    teradata_get_primary_partition_expression,
)
from goe.offload.teradata.teradata_partition_expression import (
    TeradataPartitionExpression,
    UnsupportedCaseNPartitionExpression,
    UnsupportedPartitionExpression,
)
from goe.offload.teradata import teradata_predicate
from goe.util.misc_functions import chunk_list

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# CONSTANTS
###########################################################################

DATA_SAMPLE_SIZE_MIN_PCT = 0.000001
DATA_SAMPLE_SIZE_MAX_PCT = 0.999999
DATA_SAMPLE_SIZE_MIN_GB = 1

SUPPORTED_RANGE_DATA_TYPES = [
    TERADATA_TYPE_DATE,
    TERADATA_TYPE_TIMESTAMP,
    TERADATA_TYPE_BIGINT,
    TERADATA_TYPE_INTEGER,
    TERADATA_TYPE_SMALLINT,
    TERADATA_TYPE_NUMBER,
]
SUPPORTED_LIST_DATA_TYPES = []

STRTOK_SPLIT_TO_TABLE_MAX_LENGTH = 31000
STRTOK_SPLIT_TO_TABLE_MAX_SUBCSV_ITEMS = 2000


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# CLASSES
###########################################################################


class TeradataSourceTable(OffloadSourceTableInterface):
    """Teradata source table details and methods"""

    def __init__(
        self,
        schema_name,
        table_name,
        connection_options,
        messages,
        dry_run=False,
        conn=None,
        do_not_connect=False,
    ):
        assert schema_name and table_name and connection_options

        super().__init__(
            schema_name,
            table_name,
            connection_options,
            messages,
            dry_run=dry_run,
            conn=conn,
            do_not_connect=do_not_connect,
        )

        logger.info(
            "TeradataSourceTable setup: (%s, %s, %s)"
            % (schema_name, table_name, connection_options.ora_adm_user)
        )
        if dry_run:
            logger.info("* Dry run *")

        self._partition_levels = None
        self._partition_type = None
        self._partitions = None
        self._primary_partition_expression = None
        self._hash_bucket_candidate = None
        if not do_not_connect:
            self._get_table_details()
            self._columns_setter(self._get_column_details(), skip_exists_check=True)
            if self._table_exists:
                self._size_in_bytes = self._get_table_size()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _decode_partition_high_values_string(self, hv, data_type):
        """Convert partition high value to string"""
        if hv is None:
            return None
        else:
            return self._db_api.to_frontend_literal(hv, data_type)

    def _get_column_low_high_values(self, column_name, from_stats=True, sample_perc=1):
        """Return low/high values for a specific column either from stats or from sampling data in the table
        When using optimiser stats only certain data types are supported
        """
        logger.debug(
            "get_column_low_high_values: %s, %s, %s, %s"
            % (self.owner, self.table_name, column_name, from_stats)
        )
        assert column_name
        column_name = column_name.upper()

        if from_stats:
            # TODO Can't find a way to do this on Teradata but we should revisit and try harder than I did for MVP.
            raise NotImplementedError(
                f"Low/high value from statistics is not supported"
            )
        else:
            q = (
                dedent(
                    """\
                SELECT MIN(%(col)s)
                ,      MAX(%(col)s)
                FROM   %(owner_table)s
                SAMPLE %(perc)s"""
                )
                % {
                    "owner_table": self._db_api.enclose_object_reference(
                        self.owner, self.table_name
                    ),
                    "col": column_name,
                    "perc": self._sample_data_types_data_sample_pct(sample_perc or 1),
                }
            )
            row = self._db_api.execute_query_fetch_one(q)

        return row

    def _get_hash_bucket_candidate(self):
        """Return the column name of a selective column suitable for use as a bucket/hash column.
        Techniques may differ by frontend but in general we are looking for a column with high distinct values
        and low number of NULLs.
        """
        logger.debug(
            "_get_hash_bucket_candidate: %s, %s" % (self.owner, self.table_name)
        )
        # TODO we could look for any unique indexes
        # TODO we can use stats like on Oracle but we need to have a proper look into summary vs more detailed stats.
        raise NotImplementedError(
            "_get_hash_bucket_candidate() pending implementation on Teradata"
        )

    def _get_primary_partition_expression(
        self,
    ) -> Optional[TeradataPartitionExpression]:
        """Return a TeradataPartitionExpression object describing the first row partition expression we'll use
        to drive offloads.
        The result is cached in state because we'll need to decode this data for multiple reasons.
        """
        if self._primary_partition_expression is None:
            try:
                self._primary_partition_expression = (
                    teradata_get_primary_partition_expression(
                        self.owner, self.table_name, self._db_api
                    )
                )
            except (
                UnsupportedCaseNPartitionExpression,
                UnsupportedPartitionExpression,
            ) as exc:
                # Treat tables with unsupported partition expressions as non-partitioned.
                self._log(
                    "Cannot retrieve partition expression, treating table as not partitioned: {}".format(
                        str(exc)
                    ),
                    detail=VVERBOSE,
                )
        return self._primary_partition_expression

    def _get_partitions(self, strict=True, populate_hvs=True) -> list:
        """Return a list of RdbmsPartition() objects
        Partitions are in high value descending order
        """
        logger.debug("_get_partitions: %s, %s" % (self.owner, self.table_name))
        pe = self._get_primary_partition_expression()
        partition_column = match_table_column(pe.column, self.partition_columns)
        owner_table = self._db_api.enclose_object_reference(self.owner, self.table_name)

        if pe.partition_type != OFFLOAD_PARTITION_TYPE_RANGE:
            raise NotImplementedError(
                f"Partition type is not supported on Teradata: {pe.partition_type}"
            )

        if partition_column.data_type in (TERADATA_TYPE_DATE, TERADATA_TYPE_TIMESTAMP):
            margin = (
                "INTERVAL '1' DAY"
                if partition_column.data_type == TERADATA_TYPE_TIMESTAMP
                else "1"
            )
            range_n_periods_cte_template = """SELECT END(pd) AS period_end
            FROM   SYS_CALENDAR.CALENDAR
            WHERE  calendar_date = {start}
            EXPAND ON PERIOD( {start}, {end} + {end_margin} ) AS pd BY {interval}"""
            cte_branches = [
                range_n_periods_cte_template.format(
                    start=start, end=end, interval=interval, end_margin=margin
                )
                for start, end, interval in pe.ranges
            ]
            range_n_periods_cte = "\n            UNION ALL\n            ".join(
                cte_branches
            )
        elif partition_column.is_number_based():
            cte_branches = []
            for start, end, interval in pe.ranges:
                # Get Teradata to work out how many periods there are, then we don't need to understand the step literal.
                row = self._db_api.execute_query_fetch_one(
                    f"SELECT ({end}-{start})/{interval} AS n", log_level=VVERBOSE
                )
                interval_count = row[0]
                intervals = [str(_ + 1) for _ in range(interval_count)]
                interval_csv = ",".join(intervals)
                if len(interval_csv) > STRTOK_SPLIT_TO_TABLE_MAX_LENGTH:
                    # Split the CSV into sub-CSVs of no more than STRTOK_SPLIT_TO_TABLE_MAX_SUBCSV_ITEMS elements
                    # keeping each list well below the limit.
                    sub_intervals = chunk_list(
                        intervals, STRTOK_SPLIT_TO_TABLE_MAX_SUBCSV_ITEMS
                    )
                    interval_csvs = [",".join(_) for _ in sub_intervals]
                else:
                    interval_csvs = [interval_csv]

                for interval_csv in interval_csvs:
                    sql = f"""SELECT {start} + ({interval} * CAST(d.token AS INTEGER)) AS period_end
            FROM TABLE (STRTOK_SPLIT_TO_TABLE(1, '{interval_csv}', ',')
                 RETURNS (outkey INTEGER, tokennum INTEGER, token VARCHAR(64) CHARACTER SET UNICODE)
            ) AS d"""
                    cte_branches.append(sql)
            range_n_periods_cte = "\n            UNION ALL\n            ".join(
                cte_branches
            )
        else:
            raise NotImplementedError(
                f'Teradata partition retrieval by "{partition_column.data_type}" has not been implemented'
            )

        # The SQL below appears to retrieve all data from the source table but our testing shows that
        # "SELECT DISTINCT PARTITION#L1" utilises "cylinder index scan" which is much faster than retrieving
        # all rows, example text from EXPLAIN:
        #       3) We do an all-AMPs SUM step in TD_MAP1 to aggregate from
        #          ?.? by way of a cylinder index scan with no residual
        #          conditions, grouping by field1 (?.?.PARTITION#L1).
        sql = dedent(
            f"""\
        WITH partition_range_metadata AS (
            SELECT ROW_NUMBER() OVER (ORDER BY period_end) AS partition_no
            ,      period_end                              AS high_value
            FROM   range_n_periods)
        ,    range_n_periods AS (
            {range_n_periods_cte})
        ,    populated_partitions AS (
                SELECT DISTINCT {pe.pseudo_column} AS partition_no
                FROM   {owner_table})
        SELECT pp.partition_no
        ,      prm.high_value
        ,      CASE
                  WHEN prm.partition_no IS NULL
                  THEN 'Y'
                  ELSE NULL
               END AS out_of_range
        FROM   populated_partitions     pp
               LEFT OUTER JOIN
               partition_range_metadata prm
               ON (prm.partition_no = pp.partition_no)
        ORDER  BY pp.partition_no DESC"""
        )
        rows = self._db_api.execute_query_fetch_all(sql, log_level=VVERBOSE)
        partitions = []
        # TODO Using avergae partition size is obviously flawed but have not found how to get this
        #      information on Teradata so this will suffice for MVP.
        average_partition_size = (
            int((self.size_in_bytes or 0) / len(rows)) if rows else None
        )
        for row in rows:
            partition_id, partition_high_value, out_of_range = row[0], row[1], row[2]
            if out_of_range:
                if pe.partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
                    partition_high_value = offload_constants.PART_OUT_OF_RANGE
                else:
                    partition_high_value = offload_constants.PART_OUT_OF_LIST
            hv_literal = self._decode_partition_high_values_string(
                partition_high_value, partition_column.data_type
            )
            hv_list = convert_high_values_to_python(
                self.partition_columns,
                [partition_high_value],
                self.partition_type,
                self,
                strict=strict,
            )
            partition = RdbmsPartition.by_name(
                partition_count=1,
                partition_name=str(partition_id),
                partition_position=partition_id,
                partition_size=average_partition_size,
                high_values_csv=hv_literal,
                high_values_individual=(hv_literal,),
                high_values_python=hv_list,
            )
            partitions.append(partition)

        return partitions

    def _get_primary_index_columns(self) -> list:
        """Return a list of primary index column objects for applicable frontends."""
        pi_columns = []
        # It is my understanding that only a single index on a table can have type in P, Q but just
        # to be sure we also select the name and ensure all columns are for the same index.
        q = dedent(
            """\
            SELECT IndexName, ColumnName
            FROM   dbc.IndicesV
            WHERE  DatabaseName = ?
            AND    TableName = ?
            AND    IndexType IN ('P', 'Q')"""
        )
        rows = self._db_api.execute_query_fetch_all(
            q, query_params=[self.owner, self.table_name]
        )
        if rows:
            assert (
                len(set([_[0] for _ in rows])) == 1
            ), "Multiple primary indexes unexpected: {}".format(
                [set([_[0] for _ in rows])]
            )
            for row in rows:
                pi_columns.append(match_table_column(row[1], self.columns))
        return pi_columns

    def _get_table_details(self):
        logger.debug("_get_table_details: %s, %s" % (self.owner, self.table_name))
        # num_rows calculation below looks clunky, and probably is clunky.
        # StatsV is by  column and is also a history table of previous stats records.
        # Because of this we find the newest record. Finally use a MAX even though only 1 record to
        # ensure we have NULL if no stats for the table.
        # TODO we need to see how to get partition yes/no. PartitioningLevels is temporary
        q = dedent(
            """\
            WITH s AS
            (   SELECT RowCount
                FROM   dbc.StatsV
                WHERE  DatabaseName = ?
                AND    TableName = ?
                QUALIFY ROW_NUMBER() OVER (ORDER BY LastCollectTimeStamp, RowCount DESC) = 1)
            SELECT PartitioningLevels
            ,      (SELECT MAX(RowCount) FROM s) num_rows
            FROM   DBC.TablesV
            WHERE  DatabaseName = ?
            AND    TableName = ?"""
        )
        row = self._db_api.execute_query_fetch_one(
            q, query_params=[self.owner, self.table_name, self.owner, self.table_name]
        )
        if row:
            self._table_exists = True
            self._partition_levels = row[0]
            self._stats_num_rows = row[1]
            partition_expression = self._get_primary_partition_expression()
            if partition_expression:
                self._partition_type = partition_expression.partition_type
        else:
            self._table_exists = False

    def _get_table_stats(self):
        # TODO pending implementation
        raise NotImplementedError(
            "_get_table_stats() pending implementation on Teradata"
        )
        # return self._table_stats

    def _is_compression_enabled(self) -> bool:
        raise NotImplementedError(
            "_is_compression_enabled() pending implementation on Teradata"
        )

    def _sample_data_types_compression_factor(self):
        return 0.25

    def _sample_data_types_data_sample_parallelism(self, data_sample_parallelism):
        """No sampling parallelism on Teradata"""
        return None

    def _sample_data_types_data_sample_pct(self, data_sample_pct):
        if data_sample_pct is not None:
            if data_sample_pct < 0:
                return self._sample_data_types_min_pct()
            elif data_sample_pct >= 100:
                return self._sample_data_types_max_pct()
            else:
                # Teradata SAMPLE expects a fraction > 0 and < 1
                return data_sample_pct / 100
        else:
            return self._sample_data_types_max_pct()

    def _sample_data_types_date_as_string_column(self, column_name):
        return TeradataColumn(
            column_name, self._db_api.generic_string_data_type(), data_length=128
        )

    def _sample_data_types_min_gb(self):
        """1GB seems like a good target"""
        return 1

    def _sample_data_types_max_pct(self):
        return 0.999999

    def _sample_data_types_min_pct(self):
        return 0.000001

    def _sample_data_types_decimal_column(self, column, data_precision, data_scale):
        return TeradataColumn(
            column.name,
            TERADATA_TYPE_NUMBER,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=column.nullable,
            safe_mapping=False,
        )

    def _sample_perc_sql_clause(self, data_sample_pct) -> str:
        return f"SAMPLE {data_sample_pct}"

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def check_nanosecond_offload_allowed(
        self, backend_max_datetime_scale, allow_nanosecond_timestamp_columns=None
    ):
        """Do not expect to get this far because the frontend does not support nano so the backend is irrelevant"""
        return False

    @staticmethod
    def datetime_literal_to_python(rdbms_literal, strict=True):
        """Return a string based date/timestamp literal we'll generate from partition high value"""
        if rdbms_literal == offload_constants.PART_OUT_OF_RANGE:
            return datetime64(datetime.max)
        elif isinstance(rdbms_literal, date):
            return datetime64(rdbms_literal)
        elif rdbms_literal.startswith("DATE "):
            # if decoding dba_tab_partitions.high_value then need to parse TO_DATE()
            _, dt, _ = rdbms_literal.split("'")
            return datetime64(dt)
        elif rdbms_literal.startswith("TIMESTAMP "):
            _, ts, _ = rdbms_literal.split("'")
            return datetime64(ts)
        elif not strict:
            return None
        else:
            raise NotImplementedError(
                "Teradata date/time literal not implemented: %s" % rdbms_literal
            )

    def decode_partition_high_values(self, hv_csv, strict=True) -> tuple:
        logger.debug("decode_partition_high_values: %s, %s" % (hv_csv, strict))
        if not hv_csv or not self.partition_columns:
            return tuple()

        hv_literal = self._decode_partition_high_values_string(
            hv_csv, self.partition_columns[0].data_type
        )
        hv_list = convert_high_values_to_python(
            self.partition_columns,
            [hv_literal],
            self.partition_type,
            self,
            strict=strict,
        )
        return tuple(hv_list), (hv_literal,)

    def enable_offload_by_subpartition(self, desired_state=True):
        raise NotImplementedError(
            "Subpartition Offloads are not supported for Teradata"
        )

    def from_canonical_column(self, column):
        """Translate an internal GOE column to a Teradata column.
        In practice this method is unlikely to be used because we will no support present, but
        we've fleshed it out anyway.
        """

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            char_length=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable, data_default and char_semantics forward from canonical"""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return TeradataColumn(
                col.name.upper(),
                data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                char_length=char_length,
                char_semantics=col.char_semantics,
            )

        assert column
        assert isinstance(column, CanonicalColumn)

        if column.data_type == GOE_TYPE_FIXED_STRING:
            # TODO Don't know 32000 is just for UNICODE or if CHAR as well
            max_length = (
                32000
                if column.char_semantics
                in [CANONICAL_CHAR_SEMANTICS_CHAR, CANONICAL_CHAR_SEMANTICS_UNICODE]
                else 64000
            )
            if column.data_length or column.char_length:
                data_length = column.data_length
                char_length = column.char_length or data_length
            else:
                data_length = char_length = max_length
            if (
                char_length
                if column.char_semantics
                in [CANONICAL_CHAR_SEMANTICS_CHAR, CANONICAL_CHAR_SEMANTICS_UNICODE]
                else data_length
            ) > max_length:
                return new_column(column, TERADATA_TYPE_CLOB)
            else:
                return new_column(
                    column,
                    TERADATA_TYPE_CHAR,
                    data_length=data_length,
                    char_length=char_length,
                )
        elif column.data_type == GOE_TYPE_LARGE_STRING:
            return new_column(column, TERADATA_TYPE_CLOB)
        elif column.data_type == GOE_TYPE_VARIABLE_STRING:
            # TODO Don't know 32000 is just for UNICODE or if CHAR as well
            max_length = (
                32000
                if column.char_semantics
                in [CANONICAL_CHAR_SEMANTICS_CHAR, CANONICAL_CHAR_SEMANTICS_UNICODE]
                else 64000
            )
            if column.data_length or column.char_length:
                data_length = column.data_length
                char_length = column.char_length or data_length
            else:
                data_length = char_length = max_length
            if (
                char_length
                if column.char_semantics
                in [CANONICAL_CHAR_SEMANTICS_CHAR, CANONICAL_CHAR_SEMANTICS_UNICODE]
                else data_length
            ) > max_length:
                return new_column(column, TERADATA_TYPE_CLOB)
            else:
                return new_column(
                    column,
                    TERADATA_TYPE_VARCHAR,
                    data_length=data_length,
                    char_length=char_length,
                )
        elif column.data_type == GOE_TYPE_BINARY:
            data_length = column.data_length or 64000
            if data_length > 64000:
                return new_column(column, TERADATA_TYPE_BLOB)
            else:
                return new_column(column, TERADATA_TYPE_BYTE, data_length=data_length)
        elif column.data_type == GOE_TYPE_LARGE_BINARY:
            return new_column(column, TERADATA_TYPE_BLOB)
        elif column.data_type == GOE_TYPE_INTEGER_1:
            return new_column(column, TERADATA_TYPE_BYTEINT, data_length=1)
        elif column.data_type == GOE_TYPE_INTEGER_2:
            return new_column(column, TERADATA_TYPE_SMALLINT, data_length=2)
        elif column.data_type == GOE_TYPE_INTEGER_4:
            return new_column(column, TERADATA_TYPE_INTEGER, data_length=4)
        elif column.data_type == GOE_TYPE_INTEGER_8:
            return new_column(column, TERADATA_TYPE_BIGINT, data_length=8)
        elif column.data_type == GOE_TYPE_INTEGER_38:
            return new_column(
                column,
                TERADATA_TYPE_NUMBER,
                data_precision=38,
                data_scale=0,
                data_length=18,
            )
        elif column.data_type == GOE_TYPE_DECIMAL:
            data_length = column.data_length or 18
            data_precision, data_scale = column.data_precision, column.data_scale
            if column.data_precision and column.data_precision > 38:
                data_precision, data_scale = None, None
            return new_column(
                column,
                TERADATA_TYPE_NUMBER,
                data_precision=data_precision,
                data_scale=data_scale,
                data_length=data_length,
            )
        elif column.data_type in [GOE_TYPE_FLOAT, GOE_TYPE_DOUBLE]:
            # Teradata REAL, FLOAT and DOUBLE are all the same thing, a 64 bit double.
            return new_column(column, TERADATA_TYPE_DOUBLE, data_length=8)
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, TERADATA_TYPE_DATE, data_length=4)
        elif column.data_type == GOE_TYPE_TIME:
            return new_column(column, TERADATA_TYPE_TIME, data_length=6)
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            data_scale = (
                column.data_scale
                if column.data_scale is not None
                else self.max_datetime_scale()
            )
            return new_column(
                column, TERADATA_TYPE_TIMESTAMP, data_scale=data_scale, data_length=10
            )
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            data_scale = (
                column.data_scale
                if column.data_scale is not None
                else self.max_datetime_scale()
            )
            return new_column(
                column,
                TERADATA_TYPE_TIMESTAMP_TZ,
                data_scale=data_scale,
                data_length=12,
            )
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )

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
        return TeradataColumn(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            char_semantics=char_semantics,
            char_length=char_length,
        )

    def gen_default_date_column(self, column_name):
        return self.gen_column(column_name, TERADATA_TYPE_DATE)

    def gen_default_numeric_column(self, column_name):
        return self.gen_column(column_name, TERADATA_TYPE_NUMBER)

    def get_current_scn(self, return_none_on_failure=False):
        """Current System Change Number irrelevant for Teradata support"""
        return None

    def get_hash_bucket_candidate(self):
        if not self._hash_bucket_candidate:
            self._hash_bucket_candidate = self._get_hash_bucket_candidate()
        return self._hash_bucket_candidate

    def get_max_partition_size(self):
        """Return the size of the largest partition"""
        if self.is_partitioned():
            # TODO Using avergae partition size is obviously flawed but have not found how to get this
            #      information on Teradata so this will suffice for MVP.
            partitions = self.get_partitions()
            average_partition_size = (
                int((self.size_in_bytes or 0) / len(partitions)) if partitions else None
            )
            return average_partition_size
        return None

    def get_minimum_partition_key_data(self):
        """Returns lowest point of data stored in source table"""
        assert self.partition_columns
        assert (
            len(self.partition_columns) <= 1
        ), "Teradata RANGE partition columns should not be greater than 1"
        min_sql = "SELECT MIN({}) FROM {}".format(
            self.enclose_identifier(self.partition_columns[0].name),
            self._db_api.enclose_object_reference(self.owner, self.table_name),
        )
        row = self._db_api.execute_query_fetch_one(min_sql)
        if row and row[0] is None:
            # Tuple (None, ) is True, catch that here and return None instead
            return None
        return row

    def get_partitions(self, strict=True, populate_hvs=True):
        """Fetch list of partitions for table."""
        if self._partitions is None and self.is_partitioned():
            self._partitions = self._get_partitions()
        return self._partitions

    @staticmethod
    def hash_bucket_unsuitable_data_types():
        return {TERADATA_TYPE_BLOB, TERADATA_TYPE_CLOB}

    def is_iot(self):
        return False

    def is_partitioned(self):
        return bool(self._partition_type)

    def is_subpartitioned(self):
        """Teradata supports multi-level partitioning but for now we are only considering 1st level partition scheme"""
        return False

    @staticmethod
    def nan_capable_data_types():
        return {TERADATA_TYPE_DOUBLE}

    @staticmethod
    def numeric_literal_to_python(rdbms_literal):
        """Return a string based numeric literal as a Python value"""
        if rdbms_literal == offload_constants.PART_OUT_OF_RANGE:
            return float("inf")
        elif isinstance(rdbms_literal, int):
            return rdbms_literal
        elif re.match(r"^-?\d+$", rdbms_literal):
            return int(rdbms_literal)
        else:
            raise NotImplementedError(
                "Teradata numeric literal not implemented: %s" % rdbms_literal
            )

    def offload_by_subpartition_capable(self, valid_for_auto_enable=False):
        """Teradata does support subpartitioning but for now we are only considering level 1 partition schemes"""
        return False

    @property
    def offload_by_subpartition(self):
        """Teradata does support subpartitioning but for now we are only considering level 1 partition schemes"""
        return False

    @property
    def offload_partition_level(self) -> Union[int, None]:
        # TODO we need to get the actual partition level from the partition expression
        return 1 if self.is_partitioned() else None

    def partition_has_rows(self, partition_name):
        """Check if a partition contains a row.
        On Teradata it is assumed that a partition name is also the partition number.
        """
        assert partition_name
        owner_table = self._db_api.enclose_object_reference(self.owner, self.table_name)
        sql = f"SELECT TOP 1  1 FROM {owner_table} WHERE PARTITION = {partition_name}"
        row = self._db_api.execute_query_fetch_one(sql)
        return bool(row)

    @property
    def partition_type(self):
        return self._partition_type

    def predicate_has_rows(self, predicate):
        where_clause = teradata_predicate.predicate_to_where_clause(
            self.columns, predicate
        )
        self._debug(f"Converted predicate: {where_clause}")
        owner_table = self._db_api.enclose_object_reference(self.owner, self.table_name)
        sql = f"SELECT TOP 1 1 FROM {owner_table} WHERE ({where_clause})"
        row = self._db_api.execute_query_fetch_one(sql)
        return bool(row)

    def predicate_to_where_clause(self, predicate, columns_override=None):
        if not predicate:
            return None
        return teradata_predicate.predicate_to_where_clause(
            columns_override or self.columns, predicate
        )

    def predicate_to_where_clause_with_binds(self, predicate):
        # TODO Ideally we should revisit this and implement predicate_to_where_clause_with_binds(), but not for MVP.
        return teradata_predicate.predicate_to_where_clause(self.columns, predicate), []
        # return teradata_predicate.predicate_to_where_clause_with_binds(self.columns, predicate), []

    def rdbms_literal_to_python(
        self, rdbms_column, rdbms_literal, partition_type, strict=True
    ):
        logger.debug(
            "rdbms_literal_to_python: %s, %s, %s"
            % (rdbms_column.name, rdbms_column.data_type, rdbms_literal)
        )
        converted_value = None
        if rdbms_literal == offload_constants.PART_OUT_OF_LIST:
            converted_value = rdbms_literal
        elif rdbms_column.is_date_based():
            converted_value = self.datetime_literal_to_python(
                rdbms_literal, strict=strict
            )
        elif rdbms_column.is_number_based():
            converted_value = self.numeric_literal_to_python(rdbms_literal)
        elif rdbms_column.is_string_based():
            converted_value = self.char_literal_to_python(rdbms_literal)
        else:
            if strict:
                raise OffloadSourceTableException(
                    "Unsupported partition key type for %s: %s"
                    % (rdbms_column.name, rdbms_column.data_type)
                )
            else:
                converted_value = str(rdbms_literal)
        return converted_value

    @staticmethod
    def supported_data_types() -> set:
        return {
            TERADATA_TYPE_BIGINT,
            TERADATA_TYPE_BLOB,
            TERADATA_TYPE_BYTE,
            TERADATA_TYPE_BYTEINT,
            TERADATA_TYPE_CHAR,
            TERADATA_TYPE_CLOB,
            TERADATA_TYPE_DATE,
            TERADATA_TYPE_DECIMAL,
            TERADATA_TYPE_DOUBLE,
            TERADATA_TYPE_INTEGER,
            # TODO Only included INTERVAL DS and YM as they are supported in canonical model. Need to revisit.
            TERADATA_TYPE_INTERVAL_YM,
            TERADATA_TYPE_INTERVAL_DS,
            TERADATA_TYPE_NUMBER,
            TERADATA_TYPE_SMALLINT,
            TERADATA_TYPE_TIME,
            TERADATA_TYPE_TIMESTAMP,
            TERADATA_TYPE_TIME_TZ,
            TERADATA_TYPE_TIMESTAMP_TZ,
            TERADATA_TYPE_VARBYTE,
            TERADATA_TYPE_VARCHAR,
        }

    def supported_partition_data_types(self):
        if self.partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
            return SUPPORTED_RANGE_DATA_TYPES
        else:
            return SUPPORTED_LIST_DATA_TYPES

    def supported_list_partition_data_type(self, data_type):
        return bool(data_type.upper() in SUPPORTED_LIST_DATA_TYPES)

    def supported_range_partition_data_type(self, data_type):
        return bool(data_type.upper() in SUPPORTED_RANGE_DATA_TYPES)

    def teradata_partition_pseudo_column(self) -> str:
        """Teradata only method used in Teradata Offload Transport.
        With more time we should look at understanding partition levels across Oracle and Teradata and have
        a unified approach to choosing which level to offload at.
        For the time being we cheat and use this Teradata only method.
        """
        pe = self._get_primary_partition_expression()
        return pe.pseudo_column

    def to_canonical_column(self, column):
        """Translate a Teradata column to an internal GOE column"""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable, data_default and usually char_semantics forward from RDBMS
            Not carrying partition information forward to the canonical column because that will
            be defined by operational logic.
            safe_mapping means the mapping cannot be lossy and therefore Offload transport could stage in this
            type IF the staging format has an equivalent type. In practice it only really matter for int
            variants, doubles and variable strings.
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            # TODO Do we need to know column/table/database character set here in order to set char_semantics?
            char_semantics = (
                CANONICAL_CHAR_SEMANTICS_UNICODE if (123 == 456) else col.char_semantics
            )
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
                char_length=col.char_length,
                char_semantics=char_semantics,
            )

        assert column
        assert isinstance(column, TeradataColumn)

        if column.data_type == TERADATA_TYPE_BIGINT:
            return new_column(column, GOE_TYPE_INTEGER_8, safe_mapping=True)
        elif column.data_type == TERADATA_TYPE_BLOB:
            # TODO Teradata BLOB/CLOB return 2097088000 (2GB) as the data length which is too large for Snowflake BINARY
            # return new_column(column, GOE_TYPE_LARGE_BINARY, data_length=column.data_length)
            return new_column(column, GOE_TYPE_LARGE_BINARY, data_length=8388608)
        elif column.data_type in [TERADATA_TYPE_BYTE, TERADATA_TYPE_VARBYTE]:
            return new_column(column, GOE_TYPE_BINARY, data_length=column.data_length)
        elif column.data_type == TERADATA_TYPE_BYTEINT:
            return new_column(column, GOE_TYPE_INTEGER_1, safe_mapping=True)
        elif column.data_type == TERADATA_TYPE_CHAR:
            return new_column(
                column,
                GOE_TYPE_FIXED_STRING,
                data_length=column.data_length,
                safe_mapping=True,
            )
        elif column.data_type == TERADATA_TYPE_CLOB:
            # TODO Teradata BLOB/CLOB return 2097088000 (2GB) as the char length which is too large for Snowflake TEXT
            column.char_length = 16777216
            return new_column(
                column, GOE_TYPE_LARGE_STRING, data_length=column.data_length
            )
        elif column.data_type == TERADATA_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE)
        elif column.data_type in (TERADATA_TYPE_DECIMAL, TERADATA_TYPE_NUMBER):
            data_precision = column.data_precision
            data_scale = column.data_scale
            integral_type = self._frontend_decimal_to_integral_type(
                data_precision, data_scale
            )
            if integral_type:
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
        elif column.data_type == TERADATA_TYPE_DOUBLE:
            return new_column(column, GOE_TYPE_DOUBLE, safe_mapping=True)
        elif column.data_type == TERADATA_TYPE_INTEGER:
            return new_column(column, GOE_TYPE_INTEGER_4, safe_mapping=True)
        elif column.data_type == TERADATA_TYPE_INTERVAL_DS:
            return new_column(
                column,
                GOE_TYPE_INTERVAL_DS,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        elif column.data_type == TERADATA_TYPE_INTERVAL_YM:
            return new_column(
                column,
                GOE_TYPE_INTERVAL_YM,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        elif column.data_type == TERADATA_TYPE_SMALLINT:
            return new_column(column, GOE_TYPE_INTEGER_2, safe_mapping=True)
        elif column.data_type == TERADATA_TYPE_TIME:
            return new_column(
                column, GOE_TYPE_TIME, data_scale=column.data_scale, safe_mapping=True
            )
        elif column.data_type == TERADATA_TYPE_TIMESTAMP:
            return new_column(column, GOE_TYPE_TIMESTAMP, data_scale=column.data_scale)
        elif column.data_type == TERADATA_TYPE_TIMESTAMP_TZ:
            return new_column(
                column, GOE_TYPE_TIMESTAMP_TZ, data_scale=column.data_scale
            )
        elif column.data_type == TERADATA_TYPE_VARCHAR:
            return new_column(
                column, GOE_TYPE_VARIABLE_STRING, data_length=column.data_length
            )
        else:
            raise NotImplementedError(
                "Unsupported Teradata data type: %s" % column.data_type
            )

    def to_rdbms_literal_with_sql_conv_fn(self, py_val, rdbms_data_type):
        """Function takes a python variable and returns a variable that pyodbc can cope with along
        with a template for a SQL function that will return a value back to it's original type.
        Currently this is just a case of returning the same variable with no SQL function but with
        datetime64 converted to datetime.
        We may need to go down a similar route to Oracle equivalent if we find types pyodbc cannot
        handle directly.
        Asserting 'py_val is not None' because Unix epoch is False in numpy.datetime64.
        """
        assert py_val is not None
        assert rdbms_data_type

        sql_fn = None
        rdbms_data_type = rdbms_data_type.upper()

        if isinstance(py_val, datetime64):
            py_val = str(py_val).replace("T", " ")
        elif isinstance(py_val, date):
            py_val = datetime.strptime(py_val, "%Y-%m-%d %H:%M:%S.%f")

        if rdbms_data_type == TERADATA_TYPE_DATE:
            py_val = py_val[:19]
            sql_fn = "TO_DATE(%s,'YYYY-MM-DD')"
        elif rdbms_data_type == TERADATA_TYPE_TIMESTAMP:
            m = TERADATA_TIMESTAMP_RE.match(rdbms_data_type)
            ts_ff = int(m.group(1)) if m else self.max_datetime_scale()
            py_val = py_val[: 20 + ts_ff]
            fmt = "YYYY-MM-DD HH24:MI:SS.FF%s" % (str(ts_ff) if ts_ff > 0 else "")
            sql_fn = "TO_TIMESTAMP(%s," + f"'{fmt}')"
        elif rdbms_data_type == TERADATA_TYPE_TIMESTAMP_TZ:
            # Unimplemented as we don't support timezones within the offload/present Python.
            # We can offload the data but not as a partition key or for use with this code.
            raise NotImplementedError(
                "Teradata to_rdbms_literal_with_sql_conv_fn() data type %s not implemented."
                % rdbms_data_type
            )

        return py_val, sql_fn

    def transform_encrypt_data_type(self):
        return TERADATA_TYPE_VARCHAR

    def transform_null_cast(self, rdbms_column):
        assert isinstance(rdbms_column, TeradataColumn)
        return "CAST(NULL AS %s)" % (rdbms_column.format_data_type())

    def transform_tokenize_data_type(self):
        return TERADATA_TYPE_VARCHAR

    def transform_regexp_replace_expression(
        self, rdbms_column, regexp_replace_pattern, regexp_replace_string
    ):
        return "REGEXP_REPLACE(%s, %s, %s)" % (
            self.enclose_identifier(rdbms_column.name),
            regexp_replace_pattern,
            regexp_replace_string,
        )

    def transform_translate_expression(self, rdbms_column, from_string, to_string):
        raise NotImplementedError(
            "Teradata transform_translate_expression() not implemented"
        )

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, TeradataColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.data_type == TERADATA_TYPE_BLOB:
            return bool(target_type == GOE_TYPE_LARGE_BINARY)
        elif column.data_type in [TERADATA_TYPE_BYTE, TERADATA_TYPE_VARBYTE]:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        elif column.data_type == TERADATA_TYPE_CHAR:
            return bool(target_type == GOE_TYPE_FIXED_STRING)
        elif column.data_type == TERADATA_TYPE_VARCHAR:
            return bool(target_type == GOE_TYPE_VARIABLE_STRING)
        elif column.data_type == TERADATA_TYPE_CLOB:
            return bool(target_type == GOE_TYPE_LARGE_STRING)
        elif column.data_type == TERADATA_TYPE_DOUBLE:
            return bool(target_type == GOE_TYPE_DOUBLE)
        elif column.is_number_based():
            return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based() and column.is_time_zone_based():
            return bool(target_type == GOE_TYPE_TIMESTAMP_TZ)
        elif column.is_date_based():
            return bool(
                target_type in DATE_CANONICAL_TYPES
                or target_type in STRING_CANONICAL_TYPES
            )
        elif column.data_type == TERADATA_TYPE_TIME:
            return bool(target_type == GOE_TYPE_TIME)
        elif column.data_type == TERADATA_TYPE_INTERVAL_DS:
            return bool(target_type == GOE_TYPE_INTERVAL_DS)
        elif column.data_type == TERADATA_TYPE_INTERVAL_YM:
            return bool(target_type == GOE_TYPE_INTERVAL_YM)
        elif target_type not in ALL_CANONICAL_TYPES:
            self._log(
                "Unknown canonical type in mapping: %s" % target_type, detail=VVERBOSE
            )
            return False
        return False
