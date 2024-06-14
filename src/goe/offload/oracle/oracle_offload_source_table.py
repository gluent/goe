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

""" OracleSourceTable: Library for logic/interaction with Oracle source of an offload
"""

from datetime import date, datetime
import logging
import re
from typing import Union

from cx_Oracle import DatabaseError
from numpy import datetime64

from goe.offload import offload_constants
from goe.offload.column_metadata import (
    CanonicalColumn,
    get_partition_columns,
    is_safe_mapping,
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
from goe.offload.frontend_api import QueryParameter
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_source_table import (
    OffloadSourceTableInterface,
    OffloadSourceTableException,
    RdbmsPartition,
    convert_high_values_to_python,
    OFFLOAD_PARTITION_TYPE_HASH,
    OFFLOAD_PARTITION_TYPE_LIST,
    OFFLOAD_PARTITION_TYPE_RANGE,
)
from goe.offload.oracle.oracle_column import (
    OracleColumn,
    ORACLE_TYPE_CHAR,
    ORACLE_TYPE_NCHAR,
    ORACLE_TYPE_CLOB,
    ORACLE_TYPE_NCLOB,
    ORACLE_TYPE_LONG,
    ORACLE_TYPE_VARCHAR,
    ORACLE_TYPE_VARCHAR2,
    ORACLE_TYPE_NVARCHAR2,
    ORACLE_TYPE_RAW,
    ORACLE_TYPE_BLOB,
    ORACLE_TYPE_LONG_RAW,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_FLOAT,
    ORACLE_TYPE_BINARY_FLOAT,
    ORACLE_TYPE_BINARY_DOUBLE,
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_TIMESTAMP_TZ,
    ORACLE_TYPE_TIMESTAMP_LOCAL_TZ,
    ORACLE_TYPE_INTERVAL_DS,
    ORACLE_TYPE_INTERVAL_YM,
    ORACLE_TYPE_XMLTYPE,
    ORACLE_TIMESTAMP_RE,
)
from goe.offload.oracle import oracle_predicate
from goe.util.goe_version import GOEVersion


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

MAX_SCN_SCAN_TABLE_SIZE = 1e9  # GB
MAX_UNIONS_IN_MIN_PARTITION_CHECK = 1000

ORACLE_SUPPORTED_RANGE_DATA_TYPES = [
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_VARCHAR2,
    ORACLE_TYPE_NVARCHAR2,
]
ORACLE_SUPPORTED_LIST_DATA_TYPES = [
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_CHAR,
    ORACLE_TYPE_VARCHAR2,
    ORACLE_TYPE_NCHAR,
    ORACLE_TYPE_NVARCHAR2,
]

ORACLE_VERSION_WITH_SUBPART_MIN_MAX_OPTIMIZATION = "12.1.0"
ORACLE_VERSION_WITH_CELL_OFFLOAD_PROCESSING = "11.2.0"
# This is an assumption. We've seen Smart Scan issues on 11.2.x and 12.1.x in the past.
# Assuming that by v18 these issues were resolved. See issue 172 for details.
ORACLE_VERSION_SAFE_FOR_CELL_OFFLOAD_PROCESSING = "18.0.0"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def oracle_datetime_literal_to_python(rdbms_literal, strict=True):
    """Helper function for Offload Status Report"""
    return OracleSourceTable.datetime_literal_to_python(rdbms_literal, strict=strict)


def oracle_number_literal_to_python(rdbms_literal):
    """Helper function for Offload Status Report"""
    return OracleSourceTable.numeric_literal_to_python(rdbms_literal)


def oracle_version_supports_exadata(db_version: str) -> bool:
    return GOEVersion(db_version) >= GOEVersion(
        ORACLE_VERSION_WITH_CELL_OFFLOAD_PROCESSING
    )


def oracle_version_is_smart_scan_unsafe(db_version: str) -> bool:
    return oracle_version_supports_exadata(db_version) and GOEVersion(
        db_version
    ) < GOEVersion(ORACLE_VERSION_SAFE_FOR_CELL_OFFLOAD_PROCESSING)


###########################################################################
# CLASSES
###########################################################################


class OracleSourceTable(OffloadSourceTableInterface):
    """Oracle source table details and methods"""

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
        assert (
            hasattr(connection_options, "ora_adm_user")
            and hasattr(connection_options, "ora_adm_pass")
            and hasattr(connection_options, "rdbms_dsn")
        )

        super(OracleSourceTable, self).__init__(
            schema_name,
            table_name,
            connection_options,
            messages,
            dry_run=dry_run,
            conn=conn,
            do_not_connect=do_not_connect,
        )

        logger.info(
            "OracleSourceTable setup: (%s, %s, %s)"
            % (schema_name, table_name, connection_options.ora_adm_user)
        )
        if dry_run:
            logger.info("* Dry run *")

        # Converting to str() below because Oracle dictionary tables are VARCHAR2 and unicode casts to NVARCHAR2.
        # NVARCHAR2 binds are inefficient when querying the data dictionary.
        # There's a risk this is lossy because str() is more restrictive than VARCHAR2, however, there are many
        # str() conversions throughout the codebase so decided to go with the easy option in this module too.
        self.owner = str(self.owner).upper()
        self.table_name = str(self.table_name).upper()
        self._adm_user = connection_options.ora_adm_user
        self._adm_pass = connection_options.ora_adm_pass
        self._use_oracle_wallet = connection_options.use_oracle_wallet
        self._dsn = connection_options.rdbms_dsn
        self._db_conn = None
        self._db_curs = None
        self._referenced_dependency_tree = None
        self._db_block_size = None
        self._offload_by_subpartition = False
        self._table_stats = None

        # Attributes below are specified as private so they can be enforced
        # (where relevant) as properties via base class.
        self._iot_type = None
        self._partitioned = None
        self._partition_type = None
        self._subpartition_type = None
        self._partitions = None
        self._subpartitions = None
        self._dependencies = None
        self._parallelism = None
        self._compression_enabled = None
        self._columns_with_subpartition_info = []

        self._conn = conn
        if do_not_connect:
            # Unit testing requires no db connection.
            self._db_conn = None
            self._db_curs = None
        else:
            self._db_version = self._get_db_version()
            self._get_db_block_size()
            self._get_table_details()
            self._hash_bucket_candidate = None
            # Get columns regardless of self._table_exists because it might be a view.
            self._columns_setter(self._get_column_details(), skip_exists_check=True)
            if self._table_exists:
                self._is_compression_enabled()
                self._size_in_bytes = self._get_table_size()
            else:
                self._compression_enabled = None

    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _columns_getter(self):
        if self._is_view():
            return self._columns
        elif self._offload_by_subpartition:
            return self._columns_with_subpartition_info
        else:
            return self._columns_with_partition_info

    def _columns_setter(self, new_columns, skip_exists_check=False):
        """When running in verification mode we might need to fake the columns in order to continue processing."""
        if not skip_exists_check and self.exists():
            raise OffloadSourceTableException(
                "Set of columns is only supported when the table does NOT exist"
            )
        self._columns = new_columns
        self._columns_with_partition_info = self._get_columns_with_partition_info()
        self._columns_with_subpartition_info = self._get_columns_with_partition_info(
            part_col_names_override=self._db_api.get_subpartition_column_names(
                self.owner, self.table_name
            )
        )

    def _get_db_block_size(self):
        if not self._db_block_size:
            self._db_block_size = int(self.get_session_option("db_block_size"))
        return self._db_block_size

    def _get_table_details(self):
        logger.debug("_get_table_details: %s, %s" % (self.owner, self.table_name))
        # order of columns important as referenced in helper functions
        q = """SELECT NVL2(t.iot_type,'IOT',NULL) AS iot_type
                ,     t.num_rows
                ,     t.partitioned
                ,     p.partitioning_type
                ,     p.subpartitioning_type
                ,     t.dependencies
                ,     TRIM(t.degree)
                FROM  all_tables t
                ,     all_part_tables p
                WHERE p.owner (+) = t.owner
                AND   p.table_name (+) = t.table_name
                AND   t.owner = :owner
                AND   t.table_name = :table_name"""

        row = self._db_api.execute_query_fetch_one(
            q, query_params={"owner": self.owner, "table_name": self.table_name}
        )
        if row:
            self._table_exists = True
            (
                self._iot_type,
                self._stats_num_rows,
                self._partitioned,
                self._partition_type,
                self._subpartition_type,
                self._dependencies,
                self._parallelism,
            ) = row
        else:
            self._table_exists = False

    def _get_table_stats(self):
        """Get optimizer stats from Oracle catalogue views at table, partition and column level
        Return a dict of the form:
            table_stats = {
                'num_rows', 'num_bytes', 'avg_row_len',
                'column_stats': {name: {ndv, num_nulls, avg_col_len, low_value, high_value},},
                'partition_stats': {name: {num_rows, num_bytes, avg_row_len},}
            }
        """
        if not self._table_stats:
            logger.debug("_get_table_stats: %s, %s" % (self.owner, self.table_name))

            table_stats = {
                "num_rows": -1,
                "num_bytes": 0,
                "avg_row_len": 0,
                "column_stats": {},
                "partition_stats": {},
            }

            logger.debug("fetching table stats")

            q = """SELECT num_rows
                   ,      blocks
                   ,      avg_row_len
                   FROM   all_tab_statistics
                   WHERE  owner = :owner
                   AND    table_name = :table_name
                   AND    partition_name IS NULL"""

            row = self._db_api.execute_query_fetch_one(
                q, query_params={"owner": self.owner, "table_name": self.table_name}
            )
            if not row:
                return None

            table_stats["num_rows"] = row[0]
            table_stats["num_bytes"] = (
                (row[1] * self._db_block_size) if row[1] else row[1]
            )
            table_stats["avg_row_len"] = row[2]

            if self.is_partitioned():
                logger.debug("fetching partition stats")

                q = """SELECT partition_name
                       ,      num_rows
                       ,      blocks
                       ,      avg_row_len
                       FROM   all_tab_statistics
                       WHERE  owner = :owner
                       AND    table_name = :table_name
                       AND    partition_name IS NOT NULL
                       AND    subpartition_name IS NULL
                       ORDER BY partition_position"""

                for row in self._db_api.execute_query_fetch_all(
                    q, query_params={"owner": self.owner, "table_name": self.table_name}
                ):
                    table_stats["partition_stats"][row[0]] = {
                        "num_rows": row[1],
                        "num_bytes": (
                            (row[2] * self._db_block_size) if row[2] else row[2]
                        ),
                        "avg_row_len": row[3],
                    }

            logger.debug("fetching column stats")

            q = """SELECT s.column_name
                   ,      s.num_distinct
                   ,      s.num_nulls
                   ,      s.avg_col_len
                   FROM   all_tab_col_statistics s
                   INNER JOIN all_tab_cols c ON (c.owner = s.owner AND c.table_name = s.table_name AND c.column_name = s.column_name)
                   WHERE  s.owner = :owner
                   AND    s.table_name = :table_name
                   AND    c.hidden_column = 'NO'
                   ORDER BY s.column_name"""

            # There is little point in populating Oracle RAW high/low value therefore using None
            for row in self._db_api.execute_query_fetch_all(
                q, query_params={"owner": self.owner, "table_name": self.table_name}
            ):
                table_stats["column_stats"][row[0]] = {
                    "ndv": row[1],
                    "num_nulls": row[2],
                    "avg_col_len": row[3],
                    "low_value": None,
                    "high_value": None,
                }

            logger.debug(
                "stats: num_rows=%s, num_bytes=%s, avg_row_len=%s, #cols=%s, #parts=%s"
                % (
                    table_stats["num_rows"],
                    table_stats["num_bytes"],
                    table_stats["avg_row_len"],
                    len(table_stats["column_stats"]),
                    len(table_stats["partition_stats"]),
                )
            )
            self._table_stats = table_stats
        return self._table_stats

    def _get_hash_bucket_candidate(self):
        logger.debug(
            "_get_hash_bucket_candidate: %s, %s" % (self.owner, self.table_name)
        )
        q = """SELECT column_name
            FROM (
                SELECT c.column_name
                ,      c.num_distinct * ((GREATEST(t.num_rows,1) - num_nulls) / GREATEST(t.num_rows,1)) AS ndv_null_factor
                FROM   all_tables t
                INNER JOIN all_tab_cols c ON (c.owner = t.owner AND c.table_name = t.table_name)
                WHERE  t.owner = :owner
                AND    t.table_name = :table_name
                AND    c.hidden_column = 'NO'
                AND    c.data_type IN (%s)
                ORDER BY ndv_null_factor DESC
            )
            WHERE ROWNUM = 1""" % ",".join(
            sorted(
                "'%s'" % dtype
                for dtype in self.supported_data_types().difference(
                    self.hash_bucket_unsuitable_data_types()
                )
            )
        )
        rows = self._db_api.execute_query_fetch_one(
            q, query_params={"owner": self.owner, "table_name": self.table_name}
        )
        return rows[0] if rows else None

    def _get_partitions(self, strict=True, populate_hvs=True) -> list:
        """Return a list of RdbmsPartition() objects
        Partitions are in high value descending order
        """

        def hv_python_fn(hv):
            if populate_hvs:
                return tuple(self.decode_partition_high_values(hv, strict=strict))
            else:
                return None

        def hv_individual_fn(hv):
            if populate_hvs:
                return tuple(self._decode_partition_high_values_string(hv))
            else:
                return None

        logger.debug("_get_partitions: %s, %s" % (self.owner, self.table_name))
        q = """
              WITH partition_data AS (
                     SELECT tp.partition_name
                     ,      CASE WHEN pt.partitioning_type IN ('HASH') THEN NULL ELSE tp.high_value END           AS high_value
                     ,      tp.partition_position
                     ,      tp.subpartition_count
                     ,      COALESCE(SUM(s.bytes) OVER (PARTITION BY tp.partition_name), 0)                       AS bytes
                     ,      COALESCE(tp.num_rows, SUM(tsp.num_rows) OVER (PARTITION BY tp.partition_name), 0)     AS num_rows
                     ,      tsp.subpartition_name
                     ,      NVL(tsp.subpartition_position, 0)                                                     AS subpartition_position
                     ,      ROW_NUMBER() OVER (PARTITION BY tp.partition_name ORDER BY tsp.subpartition_position) AS partition_rownum
                     FROM   dba_tables                 t
                            LEFT OUTER JOIN
                            dba_constraints            c
                            ON (    c.owner            = t.owner
                                AND c.table_name       = t.table_name
                                AND c.constraint_type  = 'P')
                            INNER JOIN
                            dba_part_tables            pt
                            ON (    pt.owner           = t.owner
                                AND pt.table_name      = t.table_name)
                            INNER JOIN
                            dba_tab_partitions         tp
                            ON (    tp.table_owner     = t.owner
                                AND tp.table_name      = t.table_name)
                            LEFT OUTER JOIN
                            dba_tab_subpartitions      tsp
                            ON (    tsp.table_owner    = tp.table_owner
                                AND tsp.table_name     = tp.table_name
                                AND tsp.partition_name = tp.partition_name)
                            LEFT OUTER JOIN
                            dba_segments               s
                            ON (    s.owner            = t.owner
                                AND s.segment_name     = DECODE(t.iot_type, 'IOT', c.index_name, t.table_name)
                                AND s.partition_name   = NVL(tsp.subpartition_name, tp.partition_name))
                     WHERE  t.owner       = :owner
                     AND    t.table_name  = :table_name
                     )
              SELECT partition_rownum
              ,      partition_name
              ,      partition_position
              ,      subpartition_count
              ,      subpartition_name
              ,      subpartition_position
              ,      high_value
              ,      bytes
              ,      num_rows
              FROM   partition_data
              ORDER  BY
                     partition_position DESC
              ,      partition_rownum
        """
        # GOE-1381: cursor now fetches all subpartitions rather than a single rollup row per partition.
        # This is because we now keep a list of subpartitions per partition in case they are needed
        # for offload transport. As a result, fetchall() and list comprehension has been replaced with
        # an array fetch loop with a nested loop for generating a subpartitions list. Not only is this necessary
        # for assigning the subpartition names to a nested list, it also prevents fetching lots of redundant
        # denormalised high_values from the partition row to the subpartition rows.
        rows = self._db_api.execute_query_fetch_all(
            q, query_params={"owner": self.owner, "table_name": self.table_name}
        )
        partitions = []
        for row in rows:
            (
                partition_rownum,
                partition_name,
                partition_position,
                subpartition_count,
                subpartition_name,
                subpartition_position,
                partition_high_value,
                partition_bytes,
                num_rows,
            ) = row
            if partition_rownum == 1:
                # Initialise new partition on first occurrence of this partition...
                partition = RdbmsPartition.by_name(
                    partition_count=1,
                    partition_name=partition_name,
                    partition_position=partition_position,
                    subpartition_count=subpartition_count,
                    subpartition_names=(
                        [subpartition_name] if subpartition_name is not None else None
                    ),
                    high_values_csv=partition_high_value,
                    partition_size=partition_bytes,
                    num_rows=num_rows,
                    high_values_individual=hv_individual_fn(partition_high_value),
                    high_values_python=hv_python_fn(partition_high_value),
                )
            else:
                # Append the subpartition name to the list of subpartitions for this partition...
                partition.subpartition_names.append(subpartition_name)
            if subpartition_count == subpartition_position:
                # Reached the last row for this partition. Add this partition to the list of partitions...
                partitions.append(partition)

        return partitions

    def _get_subpartitions(self, strict=True, populate_hvs=True):
        """Return a list of RdbmsPartition() objects at subpartition level
        Partitions are in high value descending order
        """

        def hv_python_fn(hv):
            if populate_hvs:
                return tuple(self.decode_partition_high_values(hv, strict=strict))
            else:
                return None

        def hv_individual_fn(hv):
            if populate_hvs:
                return tuple(self._decode_partition_high_values_string(hv))
            else:
                return None

        logger.debug("_get_subpartitions: %s, %s" % (self.owner, self.table_name))
        q = """
                SELECT pt.partition_count
                ,      tp.partition_name
                ,      tp.partition_position
                ,      tp.subpartition_count
                ,      tsp.subpartition_name
                ,      tsp.subpartition_position
                ,      tsp.high_value
                ,      NVL(s.bytes, 0)      AS subpartition_size
                ,      NVL(tsp.num_rows, 0) AS num_rows
                FROM   dba_tables                 t
                       LEFT OUTER JOIN
                       dba_constraints            c
                       ON (    c.owner            = t.owner
                           AND c.table_name       = t.table_name
                           AND c.constraint_type  = 'P')
                       INNER JOIN
                       dba_part_tables            pt
                       ON (    pt.owner           = t.owner
                           AND pt.table_name      = t.table_name)
                       INNER JOIN
                       dba_tab_partitions         tp
                       ON (    tp.table_owner     = t.owner
                           AND tp.table_name      = t.table_name)
                       LEFT OUTER JOIN
                       dba_tab_subpartitions      tsp
                       ON (    tsp.table_owner    = tp.table_owner
                           AND tsp.table_name     = tp.table_name
                           AND tsp.partition_name = tp.partition_name)
                       LEFT OUTER JOIN                                              -- note the OUTER join
                       dba_segments               s
                       ON (    s.owner            = t.owner
                           AND s.segment_name     = DECODE(t.iot_type, 'IOT', c.index_name, t.table_name)
                           AND s.partition_name   = NVL(tsp.subpartition_name, tp.partition_name))
                WHERE  t.owner       = :owner
                AND    t.table_name  = :table_name
                ORDER  BY
                       tp.partition_position
                ,      tsp.subpartition_position
              """
        rows = self._db_api.execute_query_fetch_all(
            q, query_params={"owner": self.owner, "table_name": self.table_name}
        )
        logger.debug("len(rows): %s" % len(rows))
        partitions = []
        for row in rows:
            (
                partition_count,
                partition_name,
                partition_position,
                subpartition_count,
                subpartition_name,
                subpartition_position,
                subpartition_high_value,
                subpartition_size,
                num_rows,
            ) = row
            partitions.append(
                RdbmsPartition.by_name(
                    partition_count=partition_count,
                    partition_name=partition_name,
                    partition_position=partition_position,
                    subpartition_count=subpartition_count,
                    subpartition_name=subpartition_name,
                    subpartition_position=subpartition_position,
                    high_values_csv=subpartition_high_value,
                    partition_size=subpartition_size,
                    num_rows=num_rows,
                    high_values_individual=hv_individual_fn(subpartition_high_value),
                    high_values_python=hv_python_fn(subpartition_high_value),
                )
            )

        # sort by high value descending to match output from _get_partitions()
        partitions.sort(key=lambda x: x.high_values_python, reverse=True)

        return partitions

    def _is_compression_enabled(self) -> bool:
        """Oracle tables have compression defined at either table/partition/subpartition level
        and for partitioned tables this can be a mixture of compression types.
        For this reason it is not simple enough to say is a table compressed, this function
        is simply defining whether a table has some compressed segments, it is not worrying
        about the ratio of compressed to uncompressed segments.
        This function does not handle the db connection, it expects the calling function to
        have catered for this
        """
        if self._compression_enabled is None:
            if self.is_subpartitioned():
                sql = """SELECT p.compression
                         FROM   dba_tab_subpartitions p
                         WHERE  p.table_owner = :owner
                         AND    p.table_name = :table_name
                         AND    p.compression = 'ENABLED'
                         AND    ROWNUM = 1"""
            elif self.is_partitioned():
                sql = """SELECT p.compression
                         FROM   dba_tab_partitions p
                         WHERE  p.table_owner = :owner
                         AND    p.table_name = :table_name
                         AND    p.compression = 'ENABLED'
                         AND    ROWNUM = 1"""
            else:
                sql = """SELECT t.compression
                         FROM   dba_tables t
                         WHERE  t.owner = :owner
                         AND    t.table_name = :table_name
                         AND    t.compression = 'ENABLED'"""
            rows = self._db_api.execute_query_fetch_one(
                sql, query_params={"owner": self.owner, "table_name": self.table_name}
            )
            self._compression_enabled = bool(rows)
        return self._compression_enabled

    def _decode_partition_high_values_string(self, hv_csv):
        """Break up high value by comma while respecting parentheses in TO_DATE(...,...,...)
        TIMESTAMPs don't contain commas and are therefore not a special case.
        """
        return self._db_api.split_partition_high_value_string(hv_csv)

    def _decode_partition_high_values(self, hv_csv, strict=True):
        """Takes Oracle dba_tab_partitions.high_value and rationalises to Python values
        The values returned are tuples
        """
        logger.debug("_decode_partition_high_values: %s, %s" % (hv_csv, strict))

        if not hv_csv:
            return [], []

        hv_literals = self._decode_partition_high_values_string(hv_csv)
        hv_list = convert_high_values_to_python(
            self.partition_columns,
            hv_literals,
            self.partition_type,
            self,
            strict=strict,
        )

        return tuple(hv_list), tuple(hv_literals)

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
            col = self.get_column(column_name)
            if not col:
                raise OffloadSourceTableException(
                    'Unknown column name "%s"' % column_name
                )

            params = {
                "owner": self.owner,
                "table_name": self.table_name,
                "column_name": str(column_name.upper()),
            }
            if col.data_type in [
                ORACLE_TYPE_NUMBER,
                ORACLE_TYPE_FLOAT,
                ORACLE_TYPE_VARCHAR2,
                ORACLE_TYPE_NVARCHAR2,
                ORACLE_TYPE_BINARY_DOUBLE,
                ORACLE_TYPE_BINARY_FLOAT,
            ]:
                raw_fn = "UTL_RAW.CAST_TO_%s" % (
                    ORACLE_TYPE_NUMBER
                    if col.data_type == ORACLE_TYPE_FLOAT
                    else col.data_type
                )
                q = """SELECT %(fn)s(low_value) AS low_val
                    ,      %(fn)s(high_value) AS high_val
                    FROM   all_tab_cols
                    WHERE  owner = :owner
                    AND    table_name = :table_name
                    AND    column_name = :column_name""" % {
                    "fn": raw_fn
                }
                row = self._db_api.execute_query_fetch_one(q, query_params=params)
            elif col.is_date_based():
                row = self._db_api.oracle_get_column_low_high_dates(
                    self.owner, self.table_name, col.name
                )
            else:
                raise OffloadSourceTableException(
                    'Unable to get values for column "%s" of type "%s" from stats'
                    % (column_name, col.data_type)
                )
        else:
            # TODO nj@2021-07-21 Should this take ORACLE_VERSION_WITH_SUBPART_MIN_MAX_OPTIMIZATION into account?
            q = """SELECT MIN(%(col)s)
                ,     MAX(%(col)s)
                FROM  "%(owner)s"."%(table)s"
                SAMPLE (%(perc)s)""" % {
                "owner": self.owner.upper(),
                "table": self.table_name.upper(),
                "col": column_name,
                "perc": sample_perc or 1,
            }
            row = self._db_api.execute_query_fetch_one(q)

        return row

    def _get_db_version(self):
        """returns the RDBMS version"""
        if not hasattr(self, "_db_version") or not self._db_version:
            self._db_version = self._db_api.frontend_version()
        return self._db_version

    def _partition_has_rows(self, partition_name):
        """Check if a partition contains a row"""
        assert partition_name
        sql = 'SELECT 1 FROM "%s"."%s" PARTITION ("%s") WHERE ROWNUM = 1' % (
            self.owner.upper(),
            self.table_name.upper(),
            partition_name.upper(),
        )
        row = self._db_api.execute_query_fetch_one(sql)
        return bool(row)

    def _sample_data_types_compression_factor(self):
        return 0.25

    def _sample_data_types_data_sample_parallelism(self, data_sample_parallelism):
        """Just a pass through on Oracle"""
        return int(data_sample_parallelism)

    def _sample_data_types_data_sample_pct(self, data_sample_pct):
        if data_sample_pct and data_sample_pct < 0:
            return self._sample_data_types_max_pct()
        elif data_sample_pct and data_sample_pct >= 100:
            return self._sample_data_types_max_pct()
        else:
            return data_sample_pct

    def _sample_data_types_execute_query(self, sample_sql):
        query_options = {}
        if oracle_version_is_smart_scan_unsafe(self._db_version):
            query_options = {"CELL_OFFLOAD_PROCESSING": "FALSE"}
        return self._db_api.execute_query_fetch_one(
            sample_sql, query_options=query_options, log_level=VERBOSE, profile=True
        )

    def _sample_data_types_date_as_string_column(self, column_name):
        return OracleColumn(
            column_name, self._db_api.generic_string_data_type(), data_length=128
        )

    def _sample_data_types_decimal_column(self, column, data_precision, data_scale):
        return OracleColumn(
            column.name,
            ORACLE_TYPE_NUMBER,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=column.nullable,
            safe_mapping=False,
        )

    def _sample_data_types_min_gb(self):
        """1GB seems like a good target"""
        return 1

    def _sample_data_types_max_pct(self):
        return 99.999999

    def _sample_data_types_min_pct(self):
        return 0.000001

    def _sample_perc_sql_clause(self, data_sample_pct) -> str:
        return f"SAMPLE BLOCK ({data_sample_pct})"

    def _sample_data_types_v2_query_hint_block(self):
        return "/*+ NO_MERGE */"

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @property
    def partition_type(self):
        if self._offload_by_subpartition:
            return self._subpartition_type
        else:
            return self._partition_type

    @property
    def subpartition_type(self):
        return self._subpartition_type

    @property
    def offload_by_subpartition(self):
        return self._offload_by_subpartition

    @property
    def offload_partition_level(self) -> Union[int, None]:
        if self.is_partitioned():
            return 2 if self._offload_by_subpartition else 1
        else:
            return None

    @property
    def parallelism(self):
        return self._parallelism

    def check_nanosecond_offload_allowed(
        self, backend_max_datetime_scale, allow_nanosecond_timestamp_columns=None
    ):
        nano_cols = [
            _
            for _ in self.columns
            if _.data_type in [ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ]
            and _.data_scale > backend_max_datetime_scale
        ]
        if nano_cols:
            if allow_nanosecond_timestamp_columns:
                self._messages.warning(
                    "Offloading columns capable of fractional seconds more precise than backend system capability (%s)"
                    % backend_max_datetime_scale
                )
                self._log_cols(nano_cols)
            else:
                self._messages.log(
                    "WARNING: Not offloading columns capable of fractional seconds more precise than backend system capability (%s)"
                    % backend_max_datetime_scale,
                    ansi_code="red",
                )
                self._messages.log(
                    "         Include --allow-nanosecond-timestamp-columns option to proceed with the offload",
                    ansi_code="red",
                )
                self._log_cols(nano_cols)
                return False
        return True

    def decode_partition_high_values(self, hv_csv, strict=True) -> tuple:
        hv_tuple, _ = self._decode_partition_high_values(hv_csv, strict=strict)
        return hv_tuple

    def enable_offload_by_subpartition(self, desired_state=True):
        """Switch the object to return/use sub partition information in place of top level information
        Facilitates incremental partition append offloads by subpartition
        """
        self._offload_by_subpartition = desired_state

    def get_current_scn(self, return_none_on_failure=False):
        """ORA_ROWSCN might not be available when VPD or FGA policies are in play. Therefore
        return_none_on_failure can be used to silently fail.
        return_none_on_failure was introduced rather than expect the caller to handle errors
        because this check is already made optional inside this code by checking the size,
        therefore it felt right to continue the theme.
        """
        logger.debug("get_current_scn: %s, %s" % (self.owner, self.table_name))
        if (self._size_in_bytes or 0) < MAX_SCN_SCAN_TABLE_SIZE:
            try:
                q = 'SELECT MAX(ora_rowscn) FROM "%s"."%s"' % (
                    self.owner.upper(),
                    self.table_name.upper(),
                )
                rows = self._db_api.execute_query_fetch_one(q)
                return rows[0] if rows else rows
            except DatabaseError as e:
                if return_none_on_failure:
                    self._messages.warning(
                        "Unable to retrieve current SCN: %s" % str(e)
                    )
                    logger.warn(str(e))
                else:
                    raise
        else:
            return None

    def get_hash_bucket_candidate(self):
        if not self._hash_bucket_candidate:
            self._hash_bucket_candidate = self._get_hash_bucket_candidate()
        return self._hash_bucket_candidate

    def get_partitions(self, strict=True, populate_hvs=True):
        """Fetch list of partitions for table. This can be time consuming therefore
        executed on demand and not during instantiation
        """
        if not self._partitions and self.is_partitioned():
            self._partitions = self._get_partitions(
                strict=strict, populate_hvs=populate_hvs
            )
        return self._partitions

    def get_subpartitions(self, strict=True, populate_hvs=True):
        """Fetch list of subpartitions for table. This can be time consuming therefore
        executed on demand and not during instantiation
        """
        if not self._subpartitions and self.is_subpartitioned():
            self._subpartitions = self._get_subpartitions(
                strict=strict, populate_hvs=populate_hvs
            )
        return self._subpartitions

    def get_max_partition_size(self):
        """Return the size of the largest partition"""
        if self._offload_by_subpartition:
            partition_fn = self.get_subpartitions
        else:
            partition_fn = self.get_partitions
        if self.is_partitioned() and partition_fn(strict=False):
            return max([p.partition_size for p in partition_fn(strict=False)])
        return None

    def is_iot(self):
        return self._iot_type == "IOT"

    def is_partitioned(self):
        return self._partitioned == "YES"

    def is_subpartitioned(self):
        return bool(self.is_partitioned() and self._subpartition_type != "NONE")

    def has_rowdependencies(self):
        return self._dependencies == "ENABLED"

    @staticmethod
    def hash_bucket_unsuitable_data_types():
        return {ORACLE_TYPE_BLOB, ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB}

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
        return OracleColumn(
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
        return self.gen_column(column_name, ORACLE_TYPE_NUMBER)

    def gen_default_date_column(self, column_name):
        return self.gen_column(column_name, ORACLE_TYPE_DATE)

    def get_minimum_partition_key_data(self):
        """Returns lowest point of data stored in source Oracle table
        Impyla & cx_Oracle both truncate nanoseconds hence TIMESTAMP columns are returned as string
        """

        def to_char_ts_col(col_name, data_type):
            if data_type in (ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ):
                return "TO_CHAR(%s,'YYYY-MM-DD HH24:MI:SS.FF9')" % col_name
            else:
                return col_name

        assert self.partition_columns

        owner_table = '"%s"."%s"' % (self.owner.upper(), self.table_name.upper())
        if len(self.partition_columns) == 1:
            part_col = self.partition_columns[0]
            min_of_col = 'MIN("%s")' % part_col.name
            proj_col = to_char_ts_col(min_of_col, part_col.data_type)
            min_sql = None

            if self._offload_by_subpartition and GOEVersion(
                self._db_version
            ) < GOEVersion(ORACLE_VERSION_WITH_SUBPART_MIN_MAX_OPTIMIZATION):
                # In Oracle 11.2 MIN() on subpartition columns does not take advantage of PARTITION RANGE ALL MIN/MAX
                # here we UNION ALL MIN() queries for each top level partition
                top_level_partitions = self.get_partitions(populate_hvs=False)
                if len(top_level_partitions) <= MAX_UNIONS_IN_MIN_PARTITION_CHECK:
                    min_sql_unions = "\nUNION ALL\n".join(
                        "SELECT %s %s FROM %s PARTITION (%s)"
                        % (min_of_col, part_col.name, owner_table, _.partition_name)
                        for _ in top_level_partitions
                    )
                    min_sql = "SELECT %s\nFROM (\n%s\n)" % (proj_col, min_sql_unions)
                else:
                    self._messages.log(
                        "Top level partition count %s exceeds threshold, falling back to single MIN() component query"
                        % MAX_UNIONS_IN_MIN_PARTITION_CHECK,
                        detail=VVERBOSE,
                    )

            if not min_sql:
                # for single column partition schemes nothing outperforms MIN()
                min_sql = "SELECT %s FROM %s" % (proj_col, owner_table)
        else:
            # MIN() is single column only so for multi-column partition schemes we use GROUP BY/ORDER BY
            cols = ", ".join([_.name for _ in self.partition_columns])
            proj_cols = ", ".join(
                [to_char_ts_col(_.name, _.data_type) for _ in self.partition_columns]
            )
            order_by = cols
            min_sql = (
                "SELECT %s FROM (SELECT %s FROM %s GROUP BY %s ORDER BY %s) WHERE ROWNUM = 1"
                % (proj_cols, cols, owner_table, cols, order_by)
            )

        self._messages.log("Min partition key SQL: %s" % min_sql, detail=VVERBOSE)

        row = self._db_api.execute_query_fetch_one(min_sql)
        if row and row[0] is None:
            # Tuple (None, ) is True, catch that here and return None instead
            return None
        return row

    def partition_has_rows(self, partition_name):
        return self._partition_has_rows(partition_name)

    def predicate_has_rows(self, predicate):
        where_clause, query_params = self.predicate_to_where_clause_with_binds(
            predicate
        )
        self._debug(f"Converted predicate: {where_clause}")
        sql = 'SELECT 1 FROM "%s"."%s" WHERE (%s) AND ROWNUM = 1' % (
            self.owner.upper(),
            self.table_name.upper(),
            where_clause,
        )
        row = self._db_api.execute_query_fetch_one(sql, query_params=query_params)
        return bool(row)

    def predicate_to_where_clause(self, predicate, columns_override=None):
        if not predicate:
            return None
        return oracle_predicate.predicate_to_where_clause(
            columns_override or self.columns, predicate
        )

    def predicate_to_where_clause_with_binds(self, predicate):
        where_clause, bind_dict = oracle_predicate.predicate_to_where_clause_with_binds(
            self.columns, predicate
        )
        query_params = [
            QueryParameter(param_name=k, param_value=v) for k, v in bind_dict.items()
        ]
        return where_clause, query_params

    def rdbms_literal_to_python(
        self, rdbms_column, rdbms_literal, partition_type, strict=True
    ):
        """Takes a single string as stored in dba_tab_partitions.high_value and rationalises to Python value"""
        logger.debug(
            "rdbms_literal_to_python: %s, %s, %s"
            % (rdbms_column.name, rdbms_column.data_type, rdbms_literal)
        )
        converted_value = None
        if rdbms_literal.upper() in ("DEFAULT", offload_constants.PART_OUT_OF_LIST):
            converted_value = offload_constants.PART_OUT_OF_LIST
        elif rdbms_column.is_date_based():
            converted_value = self.datetime_literal_to_python(
                rdbms_literal, strict=strict
            )
        elif rdbms_column.data_type == ORACLE_TYPE_NUMBER:
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

    def supported_partition_data_types(self):
        if self.partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
            return ORACLE_SUPPORTED_RANGE_DATA_TYPES
        else:
            return ORACLE_SUPPORTED_LIST_DATA_TYPES

    def to_rdbms_literal_with_sql_conv_fn(self, py_val, rdbms_data_type):
        """Function takes a python variable and returns a variable that cx-Oracle can cope with along
        with a template for a SQL function that will return a value back to it's original type.
        In most cases this is just a case of returning the same variable with no SQL function,
        however for DATEs and TIMESTAMPs we'll return a string representation of the value along
        with a SQL function. e.g.
          ("2017-10-31 11:15:14", "TO_DATE(%s,'YYYY-MM-DD HH24:MI:SS')")
        This is because we use numpy.datetime64 internally and cx-Oracle can not bind that type.
        Asserting 'py_val is not None' because Unix epoch is False in numpy.datetime64.
        There is overlap here with OracleLiteral class which we may want to look at in the future.
        """
        assert py_val is not None
        assert rdbms_data_type

        sql_fn = None
        rdbms_data_type = rdbms_data_type.upper()

        if isinstance(py_val, datetime64):
            py_val = str(py_val).replace("T", " ")
        elif isinstance(py_val, date):
            py_val = datetime.strptime(py_val, "%Y-%m-%d %H:%M:%S.%f")

        if rdbms_data_type == ORACLE_TYPE_DATE:
            py_val = py_val[:19]
            fmt = "YYYY-MM-DD HH24:MI:SS"
            sql_fn = "TO_DATE(%s," + "'%(fmt)s')" % {"fmt": fmt}
        elif rdbms_data_type == ORACLE_TYPE_TIMESTAMP:
            m = ORACLE_TIMESTAMP_RE.match(rdbms_data_type)
            ts_ff = int(m.group(1)) if m else self.max_datetime_scale()
            py_val = py_val[: 20 + ts_ff]
            fmt = "YYYY-MM-DD HH24:MI:SS.FF%s" % (str(ts_ff) if ts_ff > 0 else "")
            sql_fn = "TO_TIMESTAMP(%s," + "'%(fmt)s')" % {"fmt": fmt}
        elif rdbms_data_type in (
            ORACLE_TYPE_TIMESTAMP_TZ,
            ORACLE_TYPE_TIMESTAMP_LOCAL_TZ,
        ):
            # Unimplemented as we don't support timezones within the offload/present Python
            # We can offload the data but not as a partition key or for use with this code
            raise NotImplementedError(
                'Oracle to_rdbms_literal_with_sql_conv_fn() data type "%s" not implemented.'
                % rdbms_data_type
            )

        return py_val, sql_fn

    def offload_by_subpartition_capable(self, valid_for_auto_enable=False):
        """Is the current table capable of INCREMENTAL offload at subpartition level.
        valid_for_auto_enable is used to change the decisioning to do we trust it to auto enable.
        """

        def partition_key_data_types_are_valid(partition_columns):
            return bool(
                [
                    _
                    for _ in partition_columns
                    if _.data_type in ORACLE_SUPPORTED_RANGE_DATA_TYPES
                ]
            )

        if not self.is_subpartitioned():
            return False
        if not partition_key_data_types_are_valid(
            get_partition_columns(self._columns_with_subpartition_info)
        ):
            return False
        if (self.partition_type, self.subpartition_type) in [
            (OFFLOAD_PARTITION_TYPE_LIST, OFFLOAD_PARTITION_TYPE_RANGE),
            (OFFLOAD_PARTITION_TYPE_HASH, OFFLOAD_PARTITION_TYPE_RANGE),
        ]:
            return True
        if (self.partition_type, self.subpartition_type) == (
            OFFLOAD_PARTITION_TYPE_RANGE,
            OFFLOAD_PARTITION_TYPE_RANGE,
        ):
            if valid_for_auto_enable:
                if not partition_key_data_types_are_valid(
                    get_partition_columns(self._columns_with_partition_info)
                ):
                    # top level RANGE incompatible but subpartition RANGE looks good
                    return True
            else:
                return True
        return False

    @staticmethod
    def nan_capable_data_types():
        return {ORACLE_TYPE_BINARY_FLOAT, ORACLE_TYPE_BINARY_DOUBLE}

    @staticmethod
    def supported_data_types() -> set:
        return {
            ORACLE_TYPE_VARCHAR,
            ORACLE_TYPE_VARCHAR2,
            ORACLE_TYPE_NVARCHAR2,
            ORACLE_TYPE_CHAR,
            ORACLE_TYPE_NCHAR,
            ORACLE_TYPE_CLOB,
            ORACLE_TYPE_NCLOB,
            ORACLE_TYPE_BLOB,
            ORACLE_TYPE_RAW,
            ORACLE_TYPE_NUMBER,
            ORACLE_TYPE_BINARY_DOUBLE,
            ORACLE_TYPE_BINARY_FLOAT,
            ORACLE_TYPE_FLOAT,
            ORACLE_TYPE_DATE,
            ORACLE_TYPE_TIMESTAMP,
            ORACLE_TYPE_TIMESTAMP_TZ,
            ORACLE_TYPE_INTERVAL_DS,
            ORACLE_TYPE_INTERVAL_YM,
            ORACLE_TYPE_XMLTYPE,
        }

    def supported_range_partition_data_type(self, data_type):
        return bool(data_type.upper() in ORACLE_SUPPORTED_RANGE_DATA_TYPES)

    def supported_list_partition_data_type(self, data_type):
        return bool(data_type.upper() in ORACLE_SUPPORTED_LIST_DATA_TYPES)

    @staticmethod
    def datetime_literal_to_python(rdbms_literal, strict=True):
        """Return a string based Oracle date/timestamp literal (as found in HIGH_VALUE) as a Python value"""
        if rdbms_literal in ("MAXVALUE", offload_constants.PART_OUT_OF_RANGE):
            return datetime64(datetime.max)
        elif "TO_DATE(" in rdbms_literal:
            # if decoding dba_tab_partitions.high_value then need to parse TO_DATE()
            td, ds, c1, fmt, c2, cal, br = rdbms_literal.split("'")
            return datetime64(ds)
        elif ORACLE_TYPE_TIMESTAMP in rdbms_literal and len(rdbms_literal) in (31, 41):
            # if decoding dba_tab_partitions.high_value then need to parse TIMESTAMP' ...'
            # len 31 == no FF. len 41 == with FF9
            ts, ds, meh = rdbms_literal.split("'")
            return datetime64(ds)
        elif not strict:
            return None
        else:
            raise NotImplementedError(
                "Oracle date/time literal not implemented: %s" % rdbms_literal
            )

    @staticmethod
    def numeric_literal_to_python(rdbms_literal):
        """Return a string based Oracle NUMBER literal (as found in HIGH_VALUE) as a Python value"""
        if rdbms_literal in ("MAXVALUE", offload_constants.PART_OUT_OF_RANGE):
            return float("inf")
        elif re.match(r"^-?\d+$", rdbms_literal.strip("'").strip()):
            # strip(') because Oracle allows single quoted HVs for NUMERIC partitioning
            return int(rdbms_literal.strip("'").strip())
        else:
            raise NotImplementedError(
                "Oracle NUMBER literal not implemented: %s" % rdbms_literal
            )

    def get_subpartition_boundary_info(self):
        """Return a dictionary of all subpartition high values along with:
        - 'sub_count': how many subpartitions have that HV
        - 'expected_subs': how many subpartitions a common boundary should have
        - 'common': For convenience - True if 'sub_count' == 'expected_subs'
        """
        expected_subs = None
        subs_by_hwm = {}
        # reverse the list order (to high value ascending) in order to calculate expected subpartitions
        for p in reversed(self.get_subpartitions()):
            if expected_subs is None:
                expected_subs = p.partition_count
            # Manage the expected subpartition count. This count is used to recognise when partitions become dormant
            if p.high_values_python in subs_by_hwm:
                subs_by_hwm[p.high_values_python]["sub_count"] += 1
            else:
                subs_by_hwm[p.high_values_python] = {
                    "sub_count": 1,
                    "expected_subs": expected_subs,
                    "common": False,
                }
            # For convenience we store whether the boundary is a "common boundary"
            # This avoids having the logic in multiple places, an alternative would be to expose a method for this
            subs_by_hwm[p.high_values_python]["common"] = bool(
                subs_by_hwm[p.high_values_python]["sub_count"]
                == subs_by_hwm[p.high_values_python]["expected_subs"]
            )
            if p.subpartition_position == p.subpartition_count:
                # Is this subpartition the last in its partition (if it is, remove it from future
                # expected subpartitions-per-partition). This logic is probably going to become a lot
                # more complicated when we need to support new partitions with future-dated initial
                # subpartitions
                expected_subs = expected_subs - 1
        return subs_by_hwm

    def to_canonical_column(self, column):
        """Translate an Oracle column to an internal GOE column"""

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
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            # Always set CANONICAL_CHAR_SEMANTICS_UNICODE for N datatypes when offloading
            char_semantics = (
                CANONICAL_CHAR_SEMANTICS_UNICODE
                if (
                    column.data_type == ORACLE_TYPE_NCHAR
                    or column.data_type == ORACLE_TYPE_NVARCHAR2
                    or column.data_type == ORACLE_TYPE_NCLOB
                )
                else col.char_semantics
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
        assert isinstance(column, OracleColumn)

        if column.data_type in (ORACLE_TYPE_CHAR, ORACLE_TYPE_NCHAR):
            return new_column(
                column,
                GOE_TYPE_FIXED_STRING,
                data_length=column.data_length,
                safe_mapping=True,
            )
        elif column.data_type in (
            ORACLE_TYPE_CLOB,
            ORACLE_TYPE_NCLOB,
            ORACLE_TYPE_LONG,
        ):
            return new_column(column, GOE_TYPE_LARGE_STRING)
        elif column.data_type in (ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2):
            return new_column(
                column, GOE_TYPE_VARIABLE_STRING, data_length=column.data_length
            )
        elif column.data_type == ORACLE_TYPE_RAW:
            return new_column(column, GOE_TYPE_BINARY, data_length=column.data_length)
        elif column.data_type in (ORACLE_TYPE_BLOB, ORACLE_TYPE_LONG_RAW):
            return new_column(column, GOE_TYPE_LARGE_BINARY)
        elif column.data_type == ORACLE_TYPE_FLOAT:
            # FLOAT is an anomaly because precision is specified in bits. We could convert it but scale is unknown
            # and NUMBER(p, *) is not possible. Best thing for now is to wipe out precision matching NUMBER(*,*)
            return new_column(
                column, GOE_TYPE_DECIMAL, data_precision=None, safe_mapping=False
            )
        elif column.data_type == ORACLE_TYPE_NUMBER:
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

            integral_type = self._frontend_decimal_to_integral_type(
                data_precision, data_scale, safe_mapping=column.safe_mapping
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
        elif column.data_type == ORACLE_TYPE_BINARY_FLOAT:
            return new_column(column, GOE_TYPE_FLOAT)
        elif column.data_type == ORACLE_TYPE_BINARY_DOUBLE:
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == ORACLE_TYPE_DATE:
            return new_column(column, GOE_TYPE_TIMESTAMP)
        elif column.data_type == ORACLE_TYPE_TIMESTAMP:
            return new_column(column, GOE_TYPE_TIMESTAMP, data_scale=column.data_scale)
        elif column.data_type == ORACLE_TYPE_TIMESTAMP_TZ:
            return new_column(
                column, GOE_TYPE_TIMESTAMP_TZ, data_scale=column.data_scale
            )
        elif column.data_type == ORACLE_TYPE_TIMESTAMP_LOCAL_TZ:
            return new_column(
                column, GOE_TYPE_TIMESTAMP_TZ, data_scale=column.data_scale
            )
        elif column.data_type == ORACLE_TYPE_INTERVAL_DS:
            return new_column(
                column,
                GOE_TYPE_INTERVAL_DS,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        elif column.data_type == ORACLE_TYPE_INTERVAL_YM:
            return new_column(
                column,
                GOE_TYPE_INTERVAL_YM,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        elif column.data_type == ORACLE_TYPE_XMLTYPE:
            return new_column(
                column, GOE_TYPE_LARGE_STRING, data_scale=column.data_scale
            )
        else:
            raise NotImplementedError(
                "Unsupported Oracle data type: %s" % column.data_type
            )

    def from_canonical_column(self, column):
        """Translate an internal GOE column to an Oracle column"""

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
            return OracleColumn(
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

        def nchar_or_char(data_type, char_semantics):
            if (
                data_type == ORACLE_TYPE_CHAR
                and char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
            ):
                return ORACLE_TYPE_NCHAR
            elif (
                data_type == ORACLE_TYPE_CLOB
                and char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
            ):
                return ORACLE_TYPE_NCLOB
            elif (
                data_type == ORACLE_TYPE_VARCHAR2
                and char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
            ):
                return ORACLE_TYPE_NVARCHAR2
            return data_type

        assert column
        assert isinstance(column, CanonicalColumn)

        if column.data_type == GOE_TYPE_FIXED_STRING:
            max_length = (
                1000
                if column.char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
                else 2000
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
                return new_column(
                    column, nchar_or_char(ORACLE_TYPE_CLOB, column.char_semantics)
                )
            else:
                return new_column(
                    column,
                    nchar_or_char(ORACLE_TYPE_CHAR, column.char_semantics),
                    data_length=data_length,
                    char_length=char_length,
                )
        elif column.data_type == GOE_TYPE_LARGE_STRING:
            return new_column(
                column, nchar_or_char(ORACLE_TYPE_CLOB, column.char_semantics)
            )
        elif column.data_type == GOE_TYPE_VARIABLE_STRING:
            max_length = (
                2000
                if column.char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
                else 4000
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
                return new_column(
                    column, nchar_or_char(ORACLE_TYPE_CLOB, column.char_semantics)
                )
            else:
                return new_column(
                    column,
                    nchar_or_char(ORACLE_TYPE_VARCHAR2, column.char_semantics),
                    data_length=data_length,
                    char_length=char_length,
                )
        elif column.data_type == GOE_TYPE_BINARY:
            data_length = column.data_length or 2000
            if data_length > 2000:
                return new_column(column, ORACLE_TYPE_BLOB)
            else:
                return new_column(column, ORACLE_TYPE_RAW, data_length=data_length)
        elif column.data_type == GOE_TYPE_LARGE_BINARY:
            return new_column(column, ORACLE_TYPE_BLOB)
        elif column.data_type in (
            GOE_TYPE_INTEGER_1,
            GOE_TYPE_INTEGER_2,
            GOE_TYPE_INTEGER_4,
            GOE_TYPE_INTEGER_8,
            GOE_TYPE_INTEGER_38,
        ):
            data_length = column.data_length or 22
            # These are all integrals:
            data_scale = 0
            return new_column(
                column,
                ORACLE_TYPE_NUMBER,
                data_precision=column.data_precision,
                data_scale=data_scale,
                data_length=data_length,
            )
        elif column.data_type == GOE_TYPE_DECIMAL:
            data_length = column.data_length or 22
            data_precision, data_scale = column.data_precision, column.data_scale
            if column.data_precision and column.data_precision > 38:
                # If an incoming GOE DECIMAL has precision > 38 then we need to fall back on the flexibility
                # provided by NUMBER with no precision or scale
                data_precision, data_scale = None, None
            return new_column(
                column,
                ORACLE_TYPE_NUMBER,
                data_precision=data_precision,
                data_scale=data_scale,
                data_length=data_length,
            )
        elif column.data_type == GOE_TYPE_FLOAT:
            return new_column(column, ORACLE_TYPE_BINARY_FLOAT, data_length=4)
        elif column.data_type == GOE_TYPE_DOUBLE:
            return new_column(column, ORACLE_TYPE_BINARY_DOUBLE, data_length=8)
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, ORACLE_TYPE_DATE, data_length=7)
        elif column.data_type == GOE_TYPE_TIME:
            return new_column(column, ORACLE_TYPE_VARCHAR2, data_length=18)
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            data_scale = (
                column.data_scale
                if column.data_scale is not None
                else self.max_datetime_scale()
            )
            if data_scale == 0:
                return new_column(column, ORACLE_TYPE_DATE, data_length=7)
            else:
                return new_column(
                    column, ORACLE_TYPE_TIMESTAMP, data_scale=data_scale, data_length=11
                )
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            data_scale = (
                column.data_scale
                if column.data_scale is not None
                else self.max_datetime_scale()
            )
            return new_column(
                column, ORACLE_TYPE_TIMESTAMP_TZ, data_scale=data_scale, data_length=13
            )
        elif column.data_type == GOE_TYPE_INTERVAL_DS:
            return new_column(
                column,
                ORACLE_TYPE_INTERVAL_DS,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        elif column.data_type == GOE_TYPE_INTERVAL_YM:
            return new_column(
                column,
                ORACLE_TYPE_INTERVAL_YM,
                data_precision=column.data_precision,
                data_scale=column.data_scale,
            )
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, OracleColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.data_type in [ORACLE_TYPE_CHAR, ORACLE_TYPE_NCHAR]:
            return bool(target_type == GOE_TYPE_FIXED_STRING)
        elif column.data_type in [ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB]:
            return bool(target_type == GOE_TYPE_LARGE_STRING)
        elif column.data_type in [
            ORACLE_TYPE_VARCHAR,
            ORACLE_TYPE_VARCHAR2,
            ORACLE_TYPE_NVARCHAR2,
        ]:
            return bool(target_type == GOE_TYPE_VARIABLE_STRING)
        elif column.data_type == ORACLE_TYPE_RAW:
            return bool(target_type == GOE_TYPE_BINARY)
        elif column.data_type == ORACLE_TYPE_BLOB:
            return bool(target_type == GOE_TYPE_LARGE_BINARY)
        elif column.data_type == ORACLE_TYPE_BINARY_FLOAT:
            return bool(target_type in [GOE_TYPE_FLOAT, GOE_TYPE_DOUBLE])
        elif column.data_type == ORACLE_TYPE_BINARY_DOUBLE:
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
        elif column.data_type == ORACLE_TYPE_INTERVAL_DS:
            return bool(target_type == GOE_TYPE_INTERVAL_DS)
        elif column.data_type == ORACLE_TYPE_INTERVAL_YM:
            return bool(target_type == GOE_TYPE_INTERVAL_YM)
        elif target_type not in ALL_CANONICAL_TYPES:
            self._messages.log(
                "Unknown canonical type in mapping: %s" % target_type, detail=VVERBOSE
            )
            return False
        return False

    def transform_encrypt_data_type(self):
        return ORACLE_TYPE_VARCHAR2

    def transform_null_cast(self, rdbms_column):
        assert isinstance(rdbms_column, OracleColumn)
        return "CAST(NULL AS %s)" % (rdbms_column.format_data_type())

    def transform_tokenize_data_type(self):
        return ORACLE_TYPE_VARCHAR2

    def transform_regexp_replace_expression(
        self, rdbms_column, regexp_replace_pattern, regexp_replace_string
    ):
        return "REGEXP_REPLACE(%s, %s, %s)" % (
            self.enclose_identifier(rdbms_column.name),
            regexp_replace_pattern,
            regexp_replace_string,
        )

    def transform_translate_expression(self, rdbms_column, from_string, to_string):
        return "TRANSLATE(%s, %s, %s)" % (
            self.enclose_identifier(rdbms_column.name),
            from_string,
            to_string,
        )
