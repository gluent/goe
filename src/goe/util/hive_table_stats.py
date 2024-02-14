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

""" hive_table_stats: Grab, collect or set hive (mostly impala) table and column statistics
"""

import logging
import random
import re

from collections import defaultdict
from termcolor import colored, cprint

from goe.offload.offload_constants import (
    DBTYPE_IMPALA,
    DBTYPE_HIVE,
    EMPTY_BACKEND_COLUMN_STATS_DICT,
    EMPTY_BACKEND_TABLE_STATS_DICT,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_TINYINT,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_TIMESTAMP,
)
from goe.util.better_impyla import HiveConnection, HiveTable
from goe.util.goe_version import GOEVersion
from goe.util.hive_ddl_transform import DDLTransform
from goe.util.misc_functions import is_number


###############################################################################
# EXCEPTIONS
###############################################################################
class HiveTableStatsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Regex to parse DECIMAL data type
REGEX_DECIMAL = re.compile("(DECIMAL\()([\\d]+)", re.I)

# Column statistics (headers)
COL_STATS = ("ndv", "num_nulls", "avg_col_len", "low_val", "high_val", "max_col_len")

# Percentage of 'distinct values'/'total records', if below column is considered 'low cardinality'
LOW_CARDINALITY_THRESHOLD = 0.25

# Prefix for a temporary 'stats' view
TEMP_VIEW_PREFIX = "goe_tempview_"

# Default 'fudge factor' for num_bytes statistic (issue #263)
FUDGE_NUM_BYTES = 0.25

# Impyla craps out when fetching NULLs, so replacing with a plug
FAKE_HIVE_NULL = -424242

###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def parse_stats_into_tab_col(stats):
    """Parse results of 'stats collection query' and return
    table_stats, col_stats
    Incoming stats is a list of values of the format:
    [table count,
     col1 name,
     col1 ndv,
     col1 num_nulls,
     col1 avg_col_len,
     col1 low_val,
     col1 high_val,
     col1 max_col_len,
     ...
     coln max_col_len]
    """
    # _parse_stats_into_tab_col() begins here
    tab_stats = EMPTY_BACKEND_TABLE_STATS_DICT
    col_stats = EMPTY_BACKEND_COLUMN_STATS_DICT

    tab_stats["num_rows"], stats = int(stats[0]), stats[1:]

    # Re-cast the column stats tuple as a list of tuples (one per column)...
    n = len(COL_STATS) + 1  # no of stats per column (+1 for name)
    while stats:
        # Replace None with 0 for stats
        current_stats, stats = stats[:n], stats[n:]
        col_name, current_col_stats = current_stats[0], current_stats[1:]

        col_dict = dict(list(zip(COL_STATS, current_col_stats)))

        # Replacing fake ndv() data with NULL if Hive
        if FAKE_HIVE_NULL == col_dict["ndv"]:
            col_dict["ndv"] = None

        # Adjusting column types (some of them ints & floats)
        col_dict = {k: adjust_col_type(k, v) for k, v in list(col_dict.items())}
        col_stats[col_name] = col_dict

    # Complete the tab stats by summing up the average column sizes
    # to generate a notional table size and avg row length...
    for col in col_stats:
        avg_col_len = (
            col_stats[col]["avg_col_len"] if col_stats[col]["avg_col_len"] else 0
        )

        tab_stats["num_bytes"] += tab_stats["num_rows"] * avg_col_len
        tab_stats["avg_row_len"] += avg_col_len

    return tab_stats, col_stats


def adjust_col_type(name, value):
    ret = None

    if name in ("ndv", "max_col_len"):
        ret = None if not value else int(value)
    elif name in ("num_nulls"):
        ret = 0 if not value else int(value)
    elif name in ("avg_col_len"):
        ret = 0.0 if not value else float(value)
    elif name in ("high_val", "low_val"):
        ret = ret if not is_number(value) else str(round(float(value), 2))
    else:
        ret = value

    return ret


def transform_stats_as_tuples(tab_stats, col_stats, column_names):
    """Transform tab/col stats from dictionaries to value tuples"""
    tab_stats = (
        tab_stats["num_rows"],
        tab_stats["num_bytes"],
        tab_stats["avg_row_len"],
    )

    xform_col_stats = []
    # Grabbing columns from the table as we need them properly sorted
    for col_name in column_names:
        xform_col_stats.append(
            (
                col_name,
                col_stats[col_name]["ndv"],
                col_stats[col_name]["num_nulls"],
                col_stats[col_name]["avg_col_len"],
                col_stats[col_name]["low_val"],
                col_stats[col_name]["high_val"],
                col_stats[col_name]["max_col_len"],
            )
        )
    col_stats = xform_col_stats

    return tab_stats, col_stats


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class HiveTableStats(object):
    """Grab, collect or set hive/impala table statistics"""

    def __init__(self, hive_table, date_columns=None):
        """CONSTRUCTOR"""

        self._table = hive_table
        self._date_columns = [] if not date_columns else date_columns

        # DDL transformation object
        self._transform = DDLTransform()

        logger.debug(
            "Initialized HiveTableStats() object for: %s.%s"
            % (self._table.db_name, self._table.table_name)
        )

    @classmethod
    def construct(cls, db_name, table_name, hive_conn=None, date_columns=None):
        """Essentially a 2nd constructor when HiveTable object is not available"""
        logger.debug(
            "Constructing HiveTableStats() object for: %s.%s" % (db_name, table_name)
        )

        if not hive_conn:
            hive_conn = HiveConnection.fromdefault()

        hive_table = HiveTable(db_name, table_name, hive_conn)

        return cls(hive_table, date_columns)

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _hive_table(self, db_table=None):
        """Either re-use HiveTable() for original object or create new one if required"""
        if not db_table:
            return self._table
        else:
            db_name, table_name = db_table.split(".")
            logger.debug(
                "Constructing HiveTable(%s, %s) for a temporary object"
                % (db_name, table_name)
            )
            return HiveTable(db_name, table_name, self.hive)

    def _choose_partitions_to_sample(self, percent, hive_table):
        """(randomly) choose partitions to sample based on requested 'sample percent'

        GENERATOR: Yields randomly chosen partition specifications
        """
        partitions = hive_table.table_partitions(as_spec=True)

        if not partitions:
            return

        no_samples = int(round(percent / 100.0 * len(partitions)))
        if 0 == no_samples:
            no_samples = 1
        logger.info(
            "%.2f%% yields: %d partition samples for table: %s"
            % (percent, no_samples, hive_table.db_table)
        )

        for partition_spec in random.sample(partitions, no_samples):
            logger.debug("Randomly choosing partition: %s to sample" % partition_spec)
            yield partition_spec

    def _get_col_oracle_length(self, col_name, col_type, col_fn="avg"):
        """Return 'ORACLE' length for specific column (based on its type)"""
        ret = None

        if "DECIMAL" in col_type:
            m = REGEX_DECIMAL.match(col_type)
            if not m:
                raise HiveTableStatsException(
                    "Unrecognized DECIMAL specification: %s" % col_type
                )
            dec_length = int(m.group(2))
            if dec_length > 18:
                ret = "16"
            elif dec_length > 9:
                ret = "8"
            else:
                ret = "4"
        elif HADOOP_TYPE_TINYINT == col_type:
            ret = "1"
        elif HADOOP_TYPE_SMALLINT == col_type:
            ret = "2"
        elif col_type in [HADOOP_TYPE_FLOAT, HADOOP_TYPE_INT]:
            ret = "4"
        elif col_type in [HADOOP_TYPE_BIGINT, HADOOP_TYPE_DOUBLE, HADOOP_TYPE_REAL]:
            ret = "8"
        elif HADOOP_TYPE_TIMESTAMP == col_type:
            if col_type in self._date_columns:
                ret = "8"
            else:
                ret = "16"
        else:
            ret = "%s(length(cast(`%s` as string)))" % (col_fn, col_name)

        return ret

    def _get_stat_sql(self, hive_table):
        """Construct (impala) SQL to collect (table, column) statistics"""
        col_chunks = ["count(*)"]

        for col in hive_table.all_columns():
            col_name, col_type, _ = col
            col_s = (
                "'%s', %s, count(*)-count(`%s`), %s, cast(min(`%s`) as string), cast(max(`%s`) as string), %s\n"
                % (
                    col_name,
                    (
                        "ndv(`%s`)" % col_name
                        if DBTYPE_IMPALA == hive_table.db_type
                        else FAKE_HIVE_NULL
                    ),
                    col_name,
                    self._get_col_oracle_length(col_name, col_type),
                    col_name,
                    col_name,
                    self._get_col_oracle_length(col_name, col_type, col_fn="max"),
                )
            )
            col_chunks.append(col_s)

        sql = "SELECT %s FROM `%s`.`%s`" % (
            ",".join(col_chunks),
            hive_table.db_name,
            hive_table.table_name,
        )

        logger.debug(
            "Generic stats collection SQL for table: %s is: %s"
            % (hive_table.db_table, sql)
        )
        return sql

    def _get_table_partition_stat_sql(self, partition_spec, hive_table):
        """Construct (impala) SQL to collect partition statistics"""
        sql = "%s\nWHERE %s" % (
            self._get_stat_sql(hive_table),
            hive_table._make_partition_where(partition_spec),
        )

        logger.debug(
            "Stats collection SQL for partition: %s is: %s" % (partition_spec, sql)
        )
        return sql

    def _parse_stats_into_tab_col(self, stats):
        return parse_stats_into_tab_col(stats)

    def _collect_partition_stats(self, partition_spec, hive_table):
        """Sample partition and return table and column stats"""
        logger.info("Collecting statistics for partition: %s" % partition_spec)

        sql = self._get_table_partition_stat_sql(partition_spec, hive_table)
        logger.info("Stats SQL:\n%s" % sql)
        stats = self.hive.execute(sql, lambda x: x.fetchone(), as_dict=False)

        tab_stats, col_stats = self._parse_stats_into_tab_col(stats)

        logger.info("Table stats for partition: %s = %s" % (partition_spec, tab_stats))
        logger.info("Column stats for partition: %s = %s" % (partition_spec, col_stats))
        return tab_stats, col_stats

    def _scan_object_stats(self, hive_table):
        """Scan table or view and return table and column stats"""
        logger.info("Collecting statistics for table: %s" % hive_table.db_table)

        sql = self._get_stat_sql(hive_table)
        logger.info("Stats SQL:\n%s" % sql)
        stats = self.hive.execute(sql, lambda x: x.fetchone(), as_dict=False)

        tab_stats, col_stats = self._parse_stats_into_tab_col(stats)

        logger.info(
            "Table stats for object: %s = %s" % (hive_table.db_table, tab_stats)
        )
        logger.info(
            "Column stats for object: %s = %s" % (hive_table.db_table, col_stats)
        )
        return tab_stats, col_stats

    def _transform_stats_as_tuples(self, tab_stats, col_stats, hive_table):
        column_names = [_[0] for _ in hive_table.all_columns()]
        return transform_stats_as_tuples(tab_stats, col_stats, column_names)

    def _is_partition_column(self, col_name, hive_table):
        """Return True if 'col_name' is a partition column, False otherwise"""
        partition_columns = hive_table.partition_columns()

        if partition_columns:
            for col in partition_columns:
                name, _, _ = col
                if col_name == name:
                    return True

        return False

    def _get_partition_values(self, col_name, hive_table):
        """Return a list of partition values for a specific column"""
        ret = []

        if not self._is_partition_column(col_name, hive_table):
            raise HiveTableStatsException(
                "Column: %s is not a partition column in table: %s"
                % (hive_table.db_table, col_name)
            )

        for partition_spec in hive_table.table_partitions(as_spec=True):
            # partition_spec: [('offload_bucket_id', 0), ('goe_part_m_time_id', '2012-09')]
            for part in partition_spec:
                name, value = part
                if col_name == name:
                    ret.append(value)

        return ret

    def _project_stats_from_samples(
        self,
        sample_tab_stats,
        sample_col_stats,
        as_dict,
        partitioned_table,
        total_partitions,
        scanned_partitions,
        num_bytes_fudge=1,
    ):
        """Project table statistics by analyzing partition statistics

        sample_tab_stats = [{'num_rows': , 'num_bytes': , 'avg_row_len':  '}, ]
        sample_col_stats = [{'col_name': , 'ndv': , 'num_nulls': , 'avg_col_len': , 'low_val': , 'high_val': 'max_col_len': }, ]

        """

        def categorize_by_cardinality(sample_tab_stats, sample_col_stats):
            """Categorize columns by cardinality

            Return: True if column is 'LOW CARDINALITY', False otherwise
            """
            low_cardinality = {}

            for tab_s, col_s in zip(sample_tab_stats, sample_col_stats):
                for col in col_s:
                    col_name = col
                    is_low_cardinality = (
                        True
                        if col_s[col_name]["ndv"]
                        and col_s[col_name]["ndv"]
                        < tab_s["num_rows"] * LOW_CARDINALITY_THRESHOLD
                        else False
                    )
                    logger.debug(
                        "%s: is_low_cardinality: %s %s < %d * %.2f%%"
                        % (
                            col,
                            is_low_cardinality,
                            col_s[col_name]["ndv"],
                            tab_s["num_rows"],
                            round(LOW_CARDINALITY_THRESHOLD * 100.0, 2),
                        )
                    )
                    if col_name not in low_cardinality:
                        low_cardinality[col_name] = []
                    low_cardinality[col_name].append(is_low_cardinality)

            for col in low_cardinality:
                # Column is 'low cardinality' if >50% of partitions is low cardinality
                no_low_cardinality = sum(1 for _ in low_cardinality[col] if _)
                low_cardinality[col] = (
                    True
                    if no_low_cardinality > len(low_cardinality[col]) * 0.5
                    else False
                )
                logger.debug(
                    "Column: %s is %s cardinality"
                    % (col, "LOW" if low_cardinality[col] else "HIGH")
                )

            return low_cardinality

        # _project_stats_from_samples() begins here
        tab_stats = {}
        col_stats = defaultdict(dict)

        # Estimate table stats
        tab_stats["num_rows"] = int(
            round(
                sum(_["num_rows"] for _ in sample_tab_stats)
                * 1.0
                / scanned_partitions
                * total_partitions
            )
        )
        tab_stats["num_bytes"] = num_bytes_fudge * int(
            round(
                sum(_["num_bytes"] for _ in sample_tab_stats)
                * 1.0
                / scanned_partitions
                * total_partitions
            )
        )
        tab_stats["avg_row_len"] = int(
            round(
                sum(_["avg_row_len"] for _ in sample_tab_stats)
                * 1.0
                / len(sample_tab_stats)
            )
        )

        # Categorize columns into high/low cardinality
        low_cardinality = categorize_by_cardinality(sample_tab_stats, sample_col_stats)

        # Estimate column stats (chosing [0] as an example as all 'samples' should have the same 'schema')
        col_names = list(sample_col_stats[0].keys())
        for col in col_names:
            if self._is_partition_column(col, partitioned_table):
                partition_values = self._get_partition_values(col, partitioned_table)
                max_partition_values = max(partition_values)
                col_stats[col]["low_val"] = min(partition_values)
                col_stats[col]["high_val"] = max_partition_values
                if col == "offload_bucket_id":
                    col_stats[col]["ndv"] = (
                        (max_partition_values + 1)
                        if max_partition_values
                        else max_partition_values
                    )
                    col_stats[col]["max_col_len"] = 2
                else:
                    col_stats[col]["ndv"] = total_partitions
                    # Other places in this class assume length 2 for offload_bucket_id so assuming same below
                    len_fn = lambda x: len(x) if isinstance(x, str) else 2
                    col_stats[col]["max_col_len"] = max(
                        list(map(len_fn, partition_values))
                    )
            elif low_cardinality[col]:
                col_stats[col]["ndv"] = int(
                    round(
                        sum(_[col]["ndv"] for _ in sample_col_stats)
                        * 1.0
                        / len(sample_col_stats)
                    )
                )
                col_stats[col]["low_val"] = min(
                    _[col]["low_val"] for _ in sample_col_stats
                )
                col_stats[col]["high_val"] = max(
                    _[col]["high_val"] for _ in sample_col_stats
                )
                col_stats[col]["max_col_len"] = max(
                    _[col]["max_col_len"] for _ in sample_col_stats
                )
            else:
                # "NULL cardinality" (because of unimplemented ndv() in Hive is considered HIGH cardinality)
                if any(_[col]["ndv"] is None for _ in sample_col_stats):
                    col_stats[col]["ndv"] = None
                else:
                    col_stats[col]["ndv"] = int(
                        round(
                            sum(_[col]["ndv"] for _ in sample_col_stats)
                            * 1.0
                            / scanned_partitions
                            * total_partitions
                        )
                    )
                # No simple way to reason about low/high values for high cardinality columns
                col_stats[col]["low_val"] = None
                col_stats[col]["high_val"] = None
                col_stats[col]["max_col_len"] = max(
                    _[col]["max_col_len"] for _ in sample_col_stats
                )

            # low/high values are passed to OFFLOAD package as strings
            col_stats[col]["low_val"] = (
                None
                if col_stats[col]["low_val"] is None
                else str(col_stats[col]["low_val"])
            )
            col_stats[col]["high_val"] = (
                None
                if col_stats[col]["high_val"] is None
                else str(col_stats[col]["high_val"])
            )

            # These stats should be the same between high/low cardinality
            col_stats[col]["num_nulls"] = int(
                round(
                    sum(_[col]["num_nulls"] for _ in sample_col_stats)
                    * 1.0
                    / scanned_partitions
                    * total_partitions
                )
            )
            col_stats[col]["avg_col_len"] = round(
                sum(_[col]["avg_col_len"] for _ in sample_col_stats)
                * 1.0
                / len(sample_col_stats),
                2,
            )

        return tab_stats, col_stats

    def _get_view_sample_where_clause(self, percent):
        """Analyze dependend objects and construct 'partition where clause'
        for 'partition-wise' stats sampling, based on randomly selected list of partitions

        (for the time being, only limited to 1 'dependent' partitioned table)

        Return: 'where clause', partitioned_table, total # of partitions, # of sampled partitions
        """
        where_clause, partitioned_table, total_partitions, partitions_to_sample = (
            None,
            None,
            None,
            None,
        )

        dependent_objects = self._table.dependent_objects()
        if not dependent_objects:
            logger.warn("Unable to find dependent objects for view: %s" % self.db_table)
            return None, None

        for dep_o in dependent_objects:
            db_name, table_name, alias = dep_o
            db_table = "%s.%s" % (db_name, table_name)
            hive_table = self._hive_table(db_table)

            # Is 'db_table' partitioned ? (self._choose_partitions_to_sample() will return [] if it's not)
            sample_partitions = [
                _ for _ in self._choose_partitions_to_sample(percent, hive_table)
            ]
            if sample_partitions:
                logger.info("Identified dependent partitioned table: %s" % db_table)
                where_clause = " OR ".join(
                    [
                        "(%s)" % hive_table._make_partition_where(_, alias)
                        for _ in sample_partitions
                    ]
                )
                partitioned_table = hive_table
                total_partitions = len(hive_table.table_partitions())
                logger.info(
                    "Total number of partitions for: %s is: %d"
                    % (db_table, total_partitions)
                )
                partitions_to_sample = len(sample_partitions)
                logger.info(
                    "Partitions to sample for: %s is: %d"
                    % (db_table, partitions_to_sample)
                )

                # Exit on the 1st partitioned table (current restriction)
                break

        if not where_clause:
            logger.warn(
                "It seems that none of the dependent objects for: %s are partitioned"
                % self.db_table
            )

        return where_clause, partitioned_table, total_partitions, partitions_to_sample

    def _get_temp_view_ddl(self, temp_where_clause):
        """Construct DDL for a temporary 'partition-wise' view for stats collection

        Returns view name, ddl
        """
        temp_view_name = "%s%s" % (TEMP_VIEW_PREFIX, self._table.table_name)

        original_view_ddl = self._table.table_ddl()
        options = {"name": temp_view_name, "where": temp_where_clause}
        temp_view_ddl = self._transform.transform_view(original_view_ddl, options)

        logger.debug(
            "Temp 'stats collection' view\nName: %s\nDDL: %s"
            % (temp_view_name, temp_view_ddl)
        )
        return temp_view_name, temp_view_ddl

    def _create_temp_view(self, temp_db_view, temp_view_ddl):
        """Create temporary 'partition-wise' view for statistics collection"""
        logger.info("Creating temp 'stats collection' view: %s" % temp_db_view)

        logger.info("Temporary Stats view SQL:\n%s" % temp_view_ddl)
        self.hive.execute(temp_view_ddl)

    def _drop_temp_view(self, temp_db_view):
        """Drop temporary 'partition-wise' view for statistics collection"""
        if not temp_db_view:
            logger.debug("'temp_view' was (likely) not created. Nothing to drop")
            return

        logger.info("Dropping temp 'stats collection' view: %s" % temp_db_view)

        sql = "DROP VIEW IF EXISTS %s" % temp_db_view
        self.hive.execute(sql)

    def _nvl_table_stats(self, table_stats, as_dict):
        """Return 'sensible empty' table stats if table_stats is empty"""
        if table_stats:
            return table_stats
        else:
            logger.debug("Replacing table_stats with EMPTY stats")
            if as_dict:
                return {"num_rows": -1, "num_bytes": 0, "avg_row_len": 0}
            else:
                return (-1, 0, 0)

    def _nvl_column_stats(self, col_stats, as_dict):
        """Return 'sensible empty' column stats if col_stats is empty"""
        if col_stats:
            return col_stats
        else:
            logger.debug("Replacing col_stats with EMPTY stats")
            if as_dict:
                return {}
            else:
                return tuple()

    def _compare_stats(self, tab_base, col_base, tab_sample, col_sample):
        """Compare 2 sets of (column/table) statistics

        and return the 'difference': tab_diff, col_diff
        as {'key': {'base': ..., 'sample': ..., 'diff': ...} dictionaries
        """
        tab_diff, col_diff = defaultdict(dict), defaultdict(dict)

        # Table stats
        for stat in tab_base:
            tab_diff[stat]["base"] = tab_base[stat]
            tab_diff[stat]["sample"] = tab_sample[stat]
            tab_diff[stat]["diff"] = tab_sample[stat] - tab_base[stat]
            if 0 != int(tab_base[stat]):
                tab_diff[stat]["pct"] = tab_diff[stat]["diff"] * 100.0 / tab_base[stat]
            else:
                tab_diff[stat]["pct"] = 10000

        # Column stats
        for col in col_base:
            for stat in col_base[col]:
                if col not in col_diff:
                    col_diff[col] = defaultdict(dict)
                col_diff[col][stat] = {"base": col_base[col][stat]}
                col_diff[col][stat]["sample"] = col_sample[col][stat]
                if isinstance(col_base[col][stat], (int, float)):
                    col_diff[col][stat]["diff"] = (
                        col_sample[col][stat] - col_base[col][stat]
                    )
                    if 0 != int(col_base[col][stat]):
                        col_diff[col][stat]["pct"] = round(
                            col_diff[col][stat]["diff"] * 100.0 / col_base[col][stat], 2
                        )
                    else:
                        col_diff[col][stat]["pct"] = 10000
                else:
                    col_diff[col][stat]["diff"] = None

        return tab_diff, col_diff

    def _report_stats(self, tab_comp, col_comp):
        """Print out sample/scan comparison"""

        def f_round(val, n=2):
            if is_number(val):
                return str(round(float(val), n))
            else:
                return val

        def format_diff(diff):
            ret = ""

            if not diff["diff"]:
                pass
            elif abs(diff["pct"]) <= 10:
                ret = "%-12d\t(%s%%)" % (
                    diff["diff"],
                    colored(f_round(diff["pct"]), "green"),
                )
            elif abs(diff["pct"]) <= 50:
                ret = "%-12d\t(%s%%)" % (
                    diff["diff"],
                    colored(f_round(diff["pct"]), "yellow"),
                )
            elif 10000 == diff["pct"]:
                ret = "%-12d\t(%s%%)" % (
                    diff["diff"],
                    colored(f_round(diff["pct"]), "red", attrs=["reverse", "blink"]),
                )
            else:
                ret = "%-12d\t(%s%%)" % (
                    diff["diff"],
                    colored(f_round(diff["pct"]), "red"),
                )

            if ret:
                ret = "Diff: " + ret
            return ret

        cprint("\nTABLE statistics", "blue", attrs=["bold"])

        for stat in tab_comp:
            print(
                "\t%-30sScan: %-10sSample: %-10s%-20s"
                % (
                    stat,
                    f_round(tab_comp[stat]["base"]),
                    f_round(tab_comp[stat]["sample"]),
                    format_diff(tab_comp[stat]),
                )
            )

        cprint("\nCOLUMN statistics", "blue", attrs=["bold"])
        for col in col_comp:
            print("\n%s" % col.upper())
            for stat in col_comp[col]:
                print(
                    "\t%-30sScan: %-10sSample: %-10s%-20s"
                    % (
                        stat,
                        f_round(col_comp[col][stat]["base"]),
                        f_round(col_comp[col][stat]["sample"]),
                        format_diff(col_comp[col][stat]),
                    )
                )

    def _parse_impala_stats_header(self, header):
        cols = {}
        for i in range(len(header)):
            cols[header[i][0]] = i
        return cols

    def _get_impala_table_stats(self, as_dict=False, messages=None, partstats=False):
        """Parses SHOW <TABLE|COLUMN> STATS from Impala"""

        def parse_impala_size_expr(size):
            m = re.match("([\\d\\.]+)([BKMGTP])", str(size))
            multi = {
                "B": 1,
                "K": 1024,
                "M": 1024**2,
                "G": 1024**3,
                "T": 1024**4,
                "P": 1024**5,
            }[m.group(2)]
            return int(float(m.group(1)) * multi)

        owner_table = "`%s`.`%s`" % (self._table.db_name, self._table.table_name)
        logger.info("Fetching Impala table/column stats on %s" % owner_table)

        tab_stats = [-1, 0, 0]  # num_rows, num_bytes, avg_row_len
        col_stats = (
            []
        )  # col_name, ndv, num_nulls, avg_col_len, low_value, high_value, max_col_len

        sqls = [
            "SHOW %s STATS `%s`.`%s`" % (s, self._table.db_name, self._table.table_name)
            for s in ["TABLE", "COLUMN"]
        ]

        try:
            logger.debug("Fetching table stats: %s" % sqls[0])
            c = self.hive.execute(sqls[0], lambda c: c, as_dict=False)
            colpos = self._parse_impala_stats_header(c.description)
            stats = c.fetchall()
            for r in stats:
                if len(stats) == 1 or r[0] == "Total":
                    rows = r[colpos["#Rows"]]
                    size = parse_impala_size_expr(r[colpos["Size"]])
                    tab_stats = [rows, size, 0]

                if tab_stats[0] > -1:
                    # Fetch Impala column stats, clean them up and derive remaining table stats...
                    logger.debug("Fetching column stats: %s" % sqls[0])
                    c = self.hive.execute(sqls[1], lambda c: c, as_dict=False)
                    colpos = self._parse_impala_stats_header(c.description)
                    logger.debug("Column headers: %s" % colpos)
                    stats = c.fetchall()
                    for r in stats:
                        name = r[colpos["Column"]]
                        avg = max(r[colpos["Avg Size"]], 0)
                        tab_stats[2] += avg
                        ndv = max(r[colpos["#Distinct Values"]], 0)
                        nulls = (
                            tab_stats[0] if ndv == 0 else max(r[colpos["#Nulls"]], 0)
                        )
                        max_col_len = max(r[colpos["Max Size"]], 0)
                        col_stats += [(name, ndv, nulls, avg, "", "", max_col_len)]
        except Exception as e:
            if "SHOW TABLE STATS not applicable to a view" not in str(e):
                raise

        part_stats = []
        if partstats and len(self._table.table_partitions()) > 0:
            for k, v in self._table.table_partitions().items():
                rows = v["#Rows"]
                size = parse_impala_size_expr(v["Size"])
                part_stats += [(k, rows, size, 0)]
            if as_dict:
                new_part_stats = {}
                for part in part_stats:
                    new_part_stats[part[0]] = {
                        "num_rows": part[1],
                        "num_bytes": part[2],
                        "avg_row_len": part[3],
                    }
                part_stats = new_part_stats

        if as_dict:
            tab_stats = {
                "num_rows": tab_stats[0],
                "num_bytes": tab_stats[1],
                "avg_row_len": tab_stats[2],
            }
            new_col_stats = {}
            for col in col_stats:
                col_name, single_col_stats = col[0], col[1:]
                col_stat_dict = dict(list(zip(COL_STATS, single_col_stats)))
                new_col_stats[col_name] = col_stat_dict
            col_stats = new_col_stats

        if partstats:
            return tab_stats, col_stats, part_stats
        else:
            return tab_stats, col_stats

    def _get_tbl_props_str(self, prop_val_tuples):
        assert (
            prop_val_tuples
            and type(prop_val_tuples) is list
            and type(prop_val_tuples[0]) is tuple
        )
        prop_strings = [
            "'%s'='%s'" % (prop, val)
            for prop, val in prop_val_tuples
            if val is not None
        ]
        return ", ".join(prop_strings)

    def _set_impala_table_stats(
        self, tab_stats, additive=False, dry_run=False, messages=None
    ):
        """Manually set stats on an Impala table
        tab_stats = {num_rows, num_bytes, avg_row_len}
        tab_stats['avg_row_len'] not valid for Impala
        additive: used to add incoming num_rows to existing values
        """
        owner_table = "`%s`.`%s`" % (self._table.db_name, self._table.table_name)
        logger.info("Manually setting stats on %s" % owner_table)
        assert type(tab_stats) is dict

        if not tab_stats:
            logger.debug("Blank stats - NOOP")
            return

        if additive:
            new_num_rows = (self._table.num_rows() or 0) + (tab_stats["num_rows"] or 0)
        else:
            new_num_rows = tab_stats["num_rows"]

        prop_str = self._get_tbl_props_str([("numRows", new_num_rows)])
        if not prop_str:
            logger.debug("Blank tblproperties - NOOP")
            return

        sql = (
            "ALTER TABLE %(owner_table)s SET TBLPROPERTIES(%(props)s, 'STATS_GENERATED_VIA_STATS_TASK'='true')"
            % {"owner_table": owner_table, "props": prop_str}
        )
        messages.log("Hadoop sql: %s" % sql, VERBOSE) if messages else None
        if not dry_run:
            self.hive.execute(sql)

    def _set_impala_partition_stats(
        self, part_stat_list, additive=False, dry_run=False, messages=None
    ):
        """Manually set partition stats on an Impala table
        part_stat_list = [{partition_spec, num_rows, num_bytes, avg_row_len}, ...]
            partition_spec above is defined in better_impyla
            avg_row_len not valid for Impala
        additive: used to add incoming num_rows to existing values for all affected partitions
        """
        assert not part_stat_list or type(part_stat_list) in (list, tuple)

        owner_table = "`%s`.`%s`" % (self._table.db_name, self._table.table_name)
        logger.info(
            "Manually setting partition stats on %s (additive=%s)"
            % (owner_table, additive)
        )

        if not part_stat_list:
            logger.debug("Blank partition stats - NOOP")
            return

        existing_partitions = self._table.table_partitions()

        for part_stats in part_stat_list:
            if additive:
                current_num_rows = existing_partitions[part_stats["partition_spec"]][
                    "#Rows"
                ]
                new_num_rows = max(current_num_rows, 0) + max(part_stats["num_rows"], 0)
            else:
                new_num_rows = part_stats["num_rows"]

            formal_part_spec = self._table.make_formal_partition_spec(
                self._table.partition_str_to_spec(part_stats["partition_spec"])
            )

            prop_str = self._get_tbl_props_str([("numRows", new_num_rows)])
            sql = (
                "ALTER TABLE %(owner_table)s PARTITION %(part_spec)s SET TBLPROPERTIES(%(props)s, 'STATS_GENERATED_VIA_STATS_TASK'='true')"
                % {
                    "owner_table": owner_table,
                    "part_spec": formal_part_spec,
                    "props": prop_str,
                }
            )
            messages.log("Hadoop sql: %s" % sql, VERBOSE) if messages else None
            if not dry_run:
                self.hive.execute(sql)

    def _set_impala_column_stats(
        self,
        col_stat_dict,
        ndv_cap=None,
        num_null_factor=None,
        dry_run=False,
        messages=None,
    ):
        """Manually set column stats on an Impala table
        col_stats = {name: {ndv, num_nulls, avg_col_len, low_value, high_value}, ...}
        Manually setting columns stats is valid from CDH5.8 onwards
        """

        def is_variable_size_data_type(data_type):
            if (
                data_type
                and data_type.lower() == "string"
                or "char" in data_type.lower()
            ):
                return True
            return False

        assert not col_stat_dict or type(col_stat_dict) is dict

        owner_table = "`%s`.`%s`" % (self._table.db_name, self._table.table_name)
        logger.info("Manually setting column stats on %s" % owner_table)

        if not col_stat_dict:
            logger.debug("Blank column stats - NOOP")
            return

        # we don't cater for maxSize as RDBMS does not store that in stats
        table_columns = self._table.table_columns(as_dict=True)
        for col_name in col_stat_dict:
            col_stats = col_stat_dict[col_name]
            data_type = [
                col["data_type"]
                for col in table_columns
                if col["col_name"] == col_name.lower()
            ]
            if not data_type:
                (
                    messages.log(
                        'Column "%s" not found in target when copying stats' % col_name,
                        VVERBOSE,
                    )
                    if messages
                    else None
                )
                continue
            data_type = data_type[0]
            if not is_variable_size_data_type(data_type):
                col_stats["avg_col_len"] = None
            if ndv_cap:
                col_stats["ndv"] = min(ndv_cap, col_stats["ndv"])
            if num_null_factor and col_stats["num_nulls"] > 0:
                col_stats["num_nulls"] = int(col_stats["num_nulls"] * num_null_factor)

            prop_str = self._get_tbl_props_str(
                [
                    ("numDVs", col_stats["ndv"]),
                    ("numNulls", col_stats["num_nulls"]),
                    ("avgSize", col_stats["avg_col_len"]),
                ]
            )

            sql = (
                "ALTER TABLE %(owner_table)s SET COLUMN STATS `%(col)s` (%(props)s)"
                % {
                    "owner_table": owner_table,
                    "col": col_name.lower(),
                    "props": prop_str,
                }
            )
            messages.log("Hadoop sql: %s" % sql, VERBOSE) if messages else None
            if not dry_run:
                self.hive.execute(sql)

    def _get_hive_table_stats(
        self, as_dict=False, messages=None, missing=False, colstats=False
    ):
        """
        Parses the following from Hive:

            partitions: DESCRIBE EXTENDED <db_name>.<table_name> PARTITION (<partition>)
               columns: DESCRIBE FORMATTED <db_name>.<table_name> PARTITION (<partition>) <column_name>

        Note: Hive does not aggregate partition level (including column) stats into global stats. This means there
              is no single command to get total table rows or column stats. These must be derived from
              partition level stats. We rollup the partition level stats to get table stats in our code.

              For column level stats we don't perform any rollup. We only (for now) need to determine if the columns have stats.
              To prevent having to do <num cols> * <num partitions> calls to Hive to get all of them, we only check for the
              presence of stats on the first column in the table in each partition.
        """

        owner_table = "`%s`.`%s`" % (self._table.db_name, self._table.table_name)
        logger.info("Fetching Hive table/column stats on %s" % owner_table)

        part_stats = []  # part_name, num_rows, num_bytes, avg_row_len
        tab_stats = [-1, 0, None]  # num_rows, num_bytes, avg_row_len
        col_stats = (
            []
        )  # col_name, ndv, num_nulls, avg_col_len, low_value, high_value, max_col_len

        object_type = "VIEW" if self._table.is_view() else "TABLE"
        logger.info("Determined object: %s of type: %s" % (self.db_table, object_type))
        num_partitions = len(self._table.table_partitions())

        if "TABLE" == object_type and num_partitions > 0:
            # Partitioned table
            for pnum, partition in enumerate(self._table.table_partitions()):
                part_keys = partition.split("/")
                part_strings = []
                for part_key in part_keys:
                    # double quote the partition value otherwise Hive throws an error
                    part_strings.append(re.sub(r"(?<=\=)(.*)", r'"\1"', part_key))
                part_string = ",".join(part_strings)
                # Table/Partition Stats
                sql = "DESCRIBE EXTENDED %s.%s PARTITION (%s)" % (
                    self._table.db_name,
                    self._table.table_name,
                    part_string,
                )
                logger.debug("Fetching table/partition stats: %s" % sql)
                if messages and pnum in (0, num_partitions - 1):
                    # there could be millions of partitions so let's not log them all
                    messages.log("Hadoop sql: %s" % sql, VERBOSE)
                    if num_partitions > 2 and pnum == 0:
                        messages.log(
                            "Hadoop sql: DESCRIBE EXTENDED %s.%s PARTITION (<%s more partitions>)"
                            % (
                                self._table.db_name,
                                self._table.table_name,
                                num_partitions - 2,
                            ),
                            VERBOSE,
                        )
                c = self.hive.execute(sql, lambda c: c, as_dict=False)
                stats = c.fetchall()
                s = stats[-1]
                m = re.match(r".*parameters:{(.*)(?=,\sCOLUMN_STATS_ACCURATE)", s[1])
                if m:
                    if tab_stats[0] == -1:
                        tab_stats[0] = 0
                    pstats = dict([d.split("=") for d in m.group(1).split(", ")])
                    num_rows = pstats["numRows"] if "numRows" in pstats else None
                    num_bytes = pstats["totalSize"] if "totalSize" in pstats else None
                    part_stats += [(part_string, num_rows, num_bytes, 0)]
                    if num_rows is not None:
                        tab_stats[0] += max(int(num_rows), 0)
                    if num_bytes is not None:
                        tab_stats[1] += max(int(num_bytes), 0)
                else:
                    part_stats += [(part_string, None, None, 0)]

                # Column Stats
                if colstats:
                    first_column = self._table.table_columns(as_dict=True)[0][
                        "col_name"
                    ]
                    sql_engine_version = self._table.connection.sql_engine_version()
                    if self._table.db_type == DBTYPE_HIVE and (
                        sql_engine_version is None
                        or GOEVersion(sql_engine_version) < GOEVersion("2.0.0")
                    ):
                        # old HiveQL format
                        sql = "DESCRIBE FORMATTED %s.%s %s PARTITION (%s)" % (
                            self._table.db_name,
                            self._table.table_name,
                            first_column,
                            part_string,
                        )
                    else:
                        sql = "DESCRIBE FORMATTED %s.%s PARTITION (%s) %s" % (
                            self._table.db_name,
                            self._table.table_name,
                            part_string,
                            first_column,
                        )

                    logger.debug("Fetching column stats: %s" % sql)
                    c = self.hive.execute(sql, lambda c: c, as_dict=False)
                    stats = c.fetchall()
                    cs = [stat for stat in stats if first_column in stat[0]][0]
                    name = cs[0] if cs[0] and "from deserializer" not in cs[0] else None
                    minv = cs[2] if cs[2] and "from deserializer" not in cs[2] else None
                    maxv = cs[3] if cs[3] and "from deserializer" not in cs[3] else None
                    avg = cs[6] if cs[6] and "from deserializer" not in cs[6] else None
                    ndv = cs[5] if cs[5] and "from deserializer" not in cs[5] else None
                    nulls = (
                        cs[4] if cs[4] and "from deserializer" not in cs[4] else None
                    )
                    max_col_len = (
                        cs[7] if cs[7] and "from deserializer" not in cs[7] else None
                    )
                    col_stats += [
                        (part_string, name, ndv, nulls, avg, minv, maxv, max_col_len)
                    ]
        elif "TABLE" == object_type and num_partitions == 0:
            # Non-Partitioned table
            sql = "DESCRIBE EXTENDED %s.%s" % (
                self._table.db_name,
                self._table.table_name,
            )
            logger.debug("Fetching table stats: %s" % sql)
            messages.log("Hadoop sql: %s" % sql, VERBOSE) if messages else None
            c = self.hive.execute(sql, lambda c: c, as_dict=False)
            stats = c.fetchall()
            s = stats[-1]
            m = re.match(r".*parameters:{(.*)(?=,\sCOLUMN_STATS_ACCURATE)", s[1])
            if m:
                if tab_stats[0] == -1:
                    tab_stats[0] = 0
                f = [d.split("=")[1] for d in m.group(1).split(", ")[:-1]]
                tab_stats[0] += max(int(f[1]), 0)
                tab_stats[1] += max(int(f[0]), 0)

            # Column Stats
            if colstats:
                first_column = self._table.table_columns(as_dict=True)[0]["col_name"]
                sql = "DESCRIBE FORMATTED `%s`.`%s` `%s`" % (
                    self._table.db_name,
                    self._table.table_name,
                    first_column,
                )
                logger.debug("Fetching column stats: %s" % sql)
                c = self.hive.execute(sql, lambda c: c, as_dict=False)
                stats = c.fetchall()
                cs = [stat for stat in stats if first_column in stat[0]][0]
                name = cs[0] if cs[0] and "from deserializer" not in cs[0] else None
                minv = cs[2] if cs[2] and "from deserializer" not in cs[2] else None
                maxv = cs[3] if cs[3] and "from deserializer" not in cs[3] else None
                avg = cs[6] if cs[6] and "from deserializer" not in cs[6] else None
                ndv = cs[5] if cs[5] and "from deserializer" not in cs[5] else None
                nulls = cs[4] if cs[4] and "from deserializer" not in cs[4] else None
                max_col_len = (
                    cs[7] if cs[7] and "from deserializer" not in cs[7] else None
                )
                col_stats += [
                    ("global", name, ndv, nulls, avg, minv, maxv, max_col_len)
                ]

        if missing:
            # only return partitions with None entries for num_rows and columns with None entries for ndv
            part_stats = [part for part in part_stats if part[1] is None]
            col_stats = [col for col in col_stats if col[2] is None]

        if as_dict:
            tab_stats = {
                "num_rows": tab_stats[0],
                "num_bytes": tab_stats[1],
                "avg_row_len": tab_stats[2],
            }
            new_part_stats = {}
            for part in part_stats:
                new_part_stats[part[0]] = {
                    "num_rows": part[1],
                    "num_bytes": part[2],
                    "avg_row_len": part[3],
                }
            part_stats = new_part_stats
            new_col_stats = {}
            for col in col_stats:
                new_col_stats[col[0]] = {
                    "col_name": col[1],
                    "ndv": col[2],
                    "num_nulls": col[3],
                    "avg_col_len": col[4],
                    "low_val": col[5],
                    "high_val": col[6],
                    "max_col_len": col[7],
                }
            col_stats = new_col_stats

        return tab_stats, part_stats, col_stats

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def db_table(self):
        return "%s.%s" % (self._table.db_name, self._table.table_name)

    @property
    def hive(self):
        return self._table.connection

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def sample_partitions(
        self,
        percent=0.1,
        as_dict=False,
        num_bytes_fudge=FUDGE_NUM_BYTES,
        scan_if_sample_fails=False,
    ):
        """Detect object type (table or view) and call appropriate partition sampling routine

        scan_if_sample_fails: Switch to full scan if sample fails for any reason
        """
        percent = float(percent)
        object_type = "VIEW" if self._table.is_view() else "TABLE"
        logger.info("Determined object: %s for be a: %s" % (self.db_table, object_type))
        tab_final, col_final = None, None

        if "VIEW" == object_type:
            tab_final, col_final = self.sample_partitions_view(
                percent, as_dict, num_bytes_fudge
            )
        elif len(self._table.table_partitions()) > 0:
            # Partitioned table
            tab_final, col_final = self.sample_partitions_table(
                percent, as_dict, num_bytes_fudge
            )
        else:
            # Non-Partitioned table
            logger.warn(
                "Object: %s is NOT partitioned. Can't partition-sample" % self.db_table
            )

        if not tab_final and scan_if_sample_fails:
            logger.warn("Switching to full scan for 'stats' as sampling failed")
            tab_final, col_final = self.scan(as_dict)

        return tab_final, col_final

    def sample_partitions_table(
        self, percent=0.1, as_dict=False, num_bytes_fudge=FUDGE_NUM_BYTES
    ):
        """Scan specific percentage of 'partitions' in a table and 'project' statistics from them"""
        percent = float(percent)
        logger.info(
            "Sampling statistics for %.2f%% of partitions for table: %s with: %.2f num_stats fudge factor"
            % (percent, self.db_table, num_bytes_fudge)
        )
        tab_final, col_final = None, None

        if self._table.is_view():
            raise HiveTableStatsException("Object: %s is NOT a table" % self.db_table)

        try:
            tab_results, col_results = [], []
            for partition_spec in self._choose_partitions_to_sample(
                percent, self._table
            ):
                tab_stats, col_stats = self._collect_partition_stats(
                    partition_spec, self._table
                )

                tab_results.append(tab_stats)
                col_results.append(col_stats)

            total_partitions = len(self._table.table_partitions())
            tab_final, col_final = self._project_stats_from_samples(
                tab_results,
                col_results,
                as_dict,
                self._table,
                total_partitions,
                len(tab_stats),
                num_bytes_fudge,
            )
            if not as_dict:
                tab_final, col_final = self._transform_stats_as_tuples(
                    tab_final, col_final, self._table
                )
        except Exception as e:
            logger.warn(
                "Exception: %s detected while sampling stats on a partitioned table: %s"
                % (e, self.db_table),
                exc_info=True,
            )
            raise HiveTableStatsException(e)
        finally:
            # Replace stats with sensible structures if they are empty
            tab_final = self._nvl_table_stats(tab_final, as_dict)
            col_final = self._nvl_column_stats(col_final, as_dict)

        return tab_final, col_final

    def sample_partitions_view(
        self, percent=0.1, as_dict=False, num_bytes_fudge=FUDGE_NUM_BYTES
    ):
        """If the view is based on partitioned tables:

        1. Randomly select 'percent' of partitions to sample
        2. Create temporary view by injecting 'partition sample' where clause
        3. Collect statistics on a temp view and 'project' overall statistics from the numbers
        4. Drop temp view
        """
        percent = float(percent)
        logger.info(
            "Sampling statistics for %.2f%% of partitions for view: %s with: %.2f num_stats fudge factor"
            % (percent, self.db_table, num_bytes_fudge)
        )
        tab_final, col_final = None, None

        if not self._table.is_view():
            raise HiveTableStatsException("Object: %s is NOT a view" % self.db_table)

        temp_db_view = None
        try:
            (
                temp_where_clause,
                partitioned_table,
                total_partitions,
                partitions_to_scan,
            ) = self._get_view_sample_where_clause(percent)
            if not temp_where_clause:
                logger.warn(
                    "Unable to construct partition-wise WHERE clause injection for view: %s"
                    % self.db_table
                )
            else:
                temp_view, temp_view_ddl = self._get_temp_view_ddl(temp_where_clause)
                temp_db_view = "%s.%s" % (self._table.db_name, temp_view)
                self._create_temp_view(temp_db_view, temp_view_ddl)

                # HiveTable() object for a temporary stats view
                temp_view_obj = HiveTable(self._table.db_name, temp_view, self.hive)
                tab_stats, col_stats = self._scan_object_stats(temp_view_obj)
                tab_final, col_final = self._project_stats_from_samples(
                    [tab_stats],
                    [col_stats],
                    as_dict,
                    partitioned_table,
                    total_partitions,
                    partitions_to_scan,
                    num_bytes_fudge,
                )
                if not as_dict:
                    tab_final, col_final = self._transform_stats_as_tuples(
                        tab_final, col_final, temp_view_obj
                    )
        except Exception as e:
            logger.warn(
                "Exception: %s detected while sampling stats on a temporary view: %s"
                % (e, temp_db_view),
                exc_info=True,
            )
            raise HiveTableStatsException(e)
        finally:
            # Drop temporary view whether stat collection was successful or not
            self._drop_temp_view(temp_db_view)
            # Replace stats with sensible structures if they are empty
            tab_final = self._nvl_table_stats(tab_final, as_dict)
            col_final = self._nvl_column_stats(col_final, as_dict)

        return tab_final, col_final

    def scan(self, as_dict=False):
        """(full) Scan table/view and calculate statistics"""
        logger.info("Calculating (exact) statistics for table: %s" % self.db_table)
        tab_final, col_final = None, None

        try:
            tab_final, col_final = self._scan_object_stats(self._table)

            if not as_dict:
                tab_final, col_final = self._transform_stats_as_tuples(
                    tab_final, col_final, self._table
                )
        except Exception as e:
            logger.warn(
                "Exception: %s detected while full scanning object: %s"
                % (e, self._table.db_name),
                exc_info=True,
            )
            raise HiveTableStatsException(e)
        finally:
            # Replace stats with sensible structures if they are empty
            tab_final = self._nvl_table_stats(tab_final, as_dict)
            col_final = self._nvl_column_stats(col_final, as_dict)

        return tab_final, col_final

    def compare(self, percent=0.1, num_bytes_fudge=FUDGE_NUM_BYTES):
        """Partition sample AND full scan for statistics

        and print comparison results
        """
        logger.info(
            "Comparing scan vs sample statistics collection for table: %s"
            % self.db_table
        )

        tab_sample, col_sample = self.sample_partitions(
            percent, as_dict=True, num_bytes_fudge=num_bytes_fudge
        )
        tab_scan, col_scan = self.scan(as_dict=True)

        tab_comp, col_comp = self._compare_stats(
            tab_scan, col_scan, tab_sample, col_sample
        )
        self._report_stats(tab_comp, col_comp)

    def get_table_stats(
        self,
        db_type,
        as_dict=False,
        messages=None,
        missing=False,
        colstats=False,
        partstats=False,
    ):
        if DBTYPE_IMPALA == db_type:
            return self._get_impala_table_stats(
                as_dict=as_dict, messages=messages, partstats=partstats
            )
        elif DBTYPE_HIVE == db_type:
            return self._get_hive_table_stats(
                as_dict=as_dict, messages=messages, missing=missing, colstats=colstats
            )
        else:
            raise NotImplementedError(
                "Fetch of table/column stats not implemented for %s" % db_type
            )

    def set_table_stats(
        self, tab_stats, db_type, additive=False, dry_run=False, messages=None
    ):
        if DBTYPE_IMPALA == db_type:
            self._set_impala_table_stats(
                tab_stats, additive=additive, dry_run=dry_run, messages=messages
            )
        else:
            raise NotImplementedError(
                "Manual setting table column stats not valid for %s" % db_type
            )

    def set_column_stats(
        self,
        col_stats,
        db_type,
        ndv_cap=None,
        num_null_factor=None,
        dry_run=False,
        messages=None,
    ):
        if DBTYPE_IMPALA == db_type:
            self._set_impala_column_stats(
                col_stats,
                ndv_cap=ndv_cap,
                num_null_factor=num_null_factor,
                dry_run=dry_run,
                messages=messages,
            )
        else:
            raise NotImplementedError(
                "Manual setting of column stats not valid for %s" % db_type
            )

    def set_partition_stats(
        self, part_stats, db_type, additive=False, dry_run=False, messages=None
    ):
        if DBTYPE_IMPALA == db_type:
            self._set_impala_partition_stats(
                part_stats, additive=additive, dry_run=dry_run, messages=messages
            )
        else:
            raise NotImplementedError(
                "Manual setting of partition stats not valid for %s" % db_type
            )

    def table_partitions(self):
        partitions = self._table.table_partitions()
        return {
            part_str: {
                "partition_spec": self._table.partition_str_to_spec(part_str),
                "formal_part_spec": self._table.make_formal_partition_spec(
                    self._table.partition_str_to_spec(part_str)
                ),
                "num_rows": partitions[part_str].get("#Rows", -1),
            }
            for part_str in partitions
        }
