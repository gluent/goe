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

import argparse
import collections
import glob
import logging
import os.path
import re
import sys


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

PROG_BANNER = "Trivial SQLMON query efficiency parser"

SQLMON_ORIGINAL_STATS = (
    "connect_write_rows",
    "connect_write_bytes",  # ORACLE received
    "connect_read_rows",
    "connect_write_bytes",  # Connector received
    "hadoop_write_rows",
    "hadoop_write_bytes",  # Impala QC received
    "hadoop_read_rows",
    "hadoop_read_bytes",  # Impala scanned
    "oracle_cpu_secs",
    "connect_cpu_secs",
    "hadoop_cpu_secs",  # CPU
)

SQLMON_FINAL_STATS = SQLMON_ORIGINAL_STATS + (
    "oracle_read_rows",
    "connect_read_bytes",
    "pct_rows",
    "pct_bytes",
    "query",
    "table",
    "table_name",
)

DEFAULT_LOGGING = "WARNING"


# -----------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------
class SqlmonParserException(Exception):
    pass


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("qe-sqlmon")


# -----------------------------------------------------------------------
# SCRIPT ROUTINES
# -----------------------------------------------------------------------


def is_number(s):
    """Return True if string: 's' can be converted to a number, False otherwise"""
    if s is None:
        return False

    # For some strange reason, boolean is 'int' in python
    # i.e. float(True) = 1.0 - we do not want that
    if type(s) in (type(True), type(False)):
        return False

    try:
        float(s)
        return True
    except (ValueError, TypeError) as e:
        return False


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


@static_var("gbytes", 1024**3)
@static_var("mbytes", 1024**2)
@static_var("kbytes", 1024)
@static_var("gs", 1000**3)
@static_var("ns", 1000**2)
@static_var("ks", 1000)
def fmt_number(n, bytes_flag=False):
    if n >= (fmt_number.gbytes if bytes_flag else fmt_number.gs):
        return "%.2fG" % (n / (fmt_number.gbytes if bytes_flag else fmt_number.gs))
    elif n >= (fmt_number.mbytes if bytes_flag else fmt_number.ns):
        return "%.2fM" % (n / (fmt_number.mbytes if bytes_flag else fmt_number.ns))
    elif n >= (fmt_number.kbytes if bytes_flag else fmt_number.ks):
        return "%.2fK" % (n / (fmt_number.kbytes if bytes_flag else fmt_number.ks))
    else:
        return "%.2f" % n


def query_stats_valid(stats):
    for s in SQLMON_ORIGINAL_STATS:
        if s not in stats:
            logger.debug("** STAT: %s is missing" % s)
            return False

    return True


def get_files(pattern):
    for file_pat in pattern:
        logger.debug("Processing file pattern: %s" % file_pat)
        for file_name in glob.glob(file_pat):
            logger.debug("Processing file: %s" % file_name)
            yield file_name


def cat_lines(file_names):
    for fl in file_names:
        with open(fl) as fh:
            for line in fh:
                yield os.path.basename(os.path.splitext(fl)[0]), line


@static_var("re_table_stat", re.compile(r'^d\["(\d+)"\]\["(\S+)"\] = "?(\S+)"?;'))
@static_var("re_total_stat", re.compile(r'^ss\["(\S+)"\] = "?(\S+)"?;'))
def extract_stats(stat_lines):
    def to_number(val):
        if is_number(val):
            return float(val)
        else:
            return val

    Stat = collections.namedtuple("Stat", "query table name value")

    for query, line in stat_lines:
        t_stat = extract_stats.re_table_stat.search(line)
        if t_stat:
            yield Stat(
                query=query,
                table=t_stat.group(1),
                name=t_stat.group(2),
                value=to_number(t_stat.group(3)),
            )
        else:
            c_stat = extract_stats.re_total_stat.search(line)
            if c_stat:
                yield Stat(
                    query=query,
                    table="TOTAL",
                    name=c_stat.group(1),
                    value=to_number(c_stat.group(2)),
                )


def group_stats(stats):
    stats_by_query = {}
    for stat in stats:
        if stat.query not in stats_by_query:
            stats_by_query[stat.query] = {}
        if stat.table not in stats_by_query[stat.query]:
            stats_by_query[stat.query][stat.table] = {}
        stats_by_query[stat.query][stat.table][stat.name] = stat.value

    return stats_by_query


def validate_stats(stats):
    for query in stats:
        for table in stats[query]:
            stat = stats[query][table]
            stat["table_name"] = (
                "TOTAL" if "TOTAL" == table else stat["table_name"].replace('"', "")
            )
            if not query_stats_valid(stat):
                logger.warn(
                    "Invalid stats for query: %s, table: %s"
                    % (query, stat["table_name"])
                )
            else:
                logger.debug(
                    "Stats for query: %s, table: %s validated OK"
                    % (query, stat["table_name"])
                )
                stat["query"] = query
                stat["table"] = table
                yield stat


def derive_stats(stats):
    for stat in stats:
        # Required sqlmon stats mangling
        stat["oracle_read_rows"] = stat["connect_write_rows"]
        stat["connect_read_bytes"] = stat["connect_write_bytes"]

        # Derive new stats
        stat["pct_rows"] = stat["oracle_read_rows"] / stat["hadoop_read_rows"] * 100
        stat["pct_bytes"] = stat["oracle_read_bytes"] / stat["hadoop_read_bytes"] * 100

        yield stat


def filter_stats(stats, filters):
    for stat in stats:
        if all(f(stat) for f in filters):
            logger.debug("All filters successful for stat: %s" % stat)
            yield stat
        else:
            logger.debug("Stat: %s disqualified by filters")


def order_stats(stats, order_by_key):
    return sorted(stats, key=order_by_key)


def make_filters(args):
    def table_filters(owner_table):
        if not owner_table:
            return []
        else:
            return [lambda x: re.search(owner_table, x["table_name"], re.I)]

    def general_filters(filters):
        if filters:
            return [
                lambda x: eval(" and ".join("%s %s" % (x[k], c) for k, c in filters))
            ]
        else:
            return []

    return table_filters(args.table) + general_filters(args.where)


def make_order_by(order_bys):
    if not order_bys:
        order_bys = ("query", "table")
    return lambda x: [x[_] for _ in order_bys]


def row_stats(stats):
    ora = stats["oracle_read_rows"]  # == 'connect_write_rows'
    conn = stats["connect_read_rows"]
    qc = stats["hadoop_write_rows"]
    hdp = stats["hadoop_read_rows"]

    return "Rows: %s (%.2f%%) : %s (%.2f%%) : %s (%.2f%%) : %s" % (
        fmt_number(ora),
        ora / hdp * 100,
        fmt_number(conn),
        conn / hdp * 100,
        fmt_number(qc),
        qc / hdp * 100,
        fmt_number(hdp),
    )


def byte_stats(stats):
    ora = stats["connect_read_bytes"]  # == 'connect_write_bytes'
    conn = stats["connect_write_bytes"]
    qc = stats["hadoop_write_bytes"]
    hdp = stats["hadoop_read_bytes"]

    return "Bytes: %s (%.2f%%) : %s (%.2f%%) : %s (%.2f%%) : %s" % (
        fmt_number(ora, True),
        ora / hdp * 100,
        fmt_number(conn, True),
        conn / hdp * 100,
        fmt_number(qc, True),
        qc / hdp * 100,
        fmt_number(hdp, True),
    )


def cpu_stats(stats):
    ora = stats["oracle_cpu_secs"]
    conn = stats["connect_cpu_secs"]
    hdp = stats["hadoop_cpu_secs"]

    return "CPU: %.2f (%.2f%%) : %.2f (%.2f%%) : %.2f" % (
        ora,
        ora / hdp * 100,
        conn,
        conn / hdp * 100,
        hdp,
    )


def print_stats(stats):
    prev_query = None
    for stat in stats:
        query, table = stat["query"], stat["table"]
        if prev_query != query:
            print("%s:" % query)
        print("\t%s:" % stat["table_name"])
        print("\t\t%s" % row_stats(stat))
        print("\t\t%s" % byte_stats(stat))
        print("\t\t%s" % cpu_stats(stat))
        prev_query = query


def parse_args():
    @static_var("re_filter", re.compile("^(\w+)\s*((>|>=|<|<=|==|!=)+.*)"))
    def parse_filters(filters):
        filter_conditions = []
        for f in filters or []:
            matched = parse_filters.re_filter.match(f.strip())
            if not matched:
                raise SqlmonParserException(
                    "Invalid WHERE condition: %s. Expected: %s"
                    % (f, parse_filters.re_filter.pattern)
                )
            key, condition = matched.group(1), matched.group(2)
            filter_conditions.append((key, condition))

        return filter_conditions

    def validate_keys(keys):
        for k in keys or []:
            if k not in SQLMON_FINAL_STATS:
                raise SqlmonParserException("Key: %s is invalid" % k)

    parser = argparse.ArgumentParser(description=PROG_BANNER)

    parser.add_argument(
        "-t",
        "--table",
        help="FILTER by table-name, eg. OWNER.TABLE. Supports regular expressions for both OWNER and TABLE",
    )
    parser.add_argument(
        "-w",
        "--where",
        nargs="+",
        required=False,
        help="FILTER by stats (aka: ignore tables with non qualifying stats), i.e. oracle_cpu_secs > 1, pct_rows > 10 ",
    )
    parser.add_argument(
        "-s",
        "--order-by",
        nargs="+",
        required=False,
        help="ORDER results by stat, i.e. pct_rows. Default: order by query name",
    )

    parser.add_argument(
        "-l",
        "--dev-log-level",
        required=False,
        default=DEFAULT_LOGGING,
        help="Logging level. Default: %s" % DEFAULT_LOGGING,
    )

    parser.add_argument("pattern", nargs="+", help="List of files to process")

    args = parser.parse_args()

    # Post-process arguments
    validate_keys(args.order_by)
    args.where = parse_filters(args.where)
    validate_keys(_[0] for _ in args.where)

    return args


def set_logging(log_level):
    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    args = parse_args()
    set_logging(args.dev_log_level)

    # Extracting stats from HTMLs
    files = get_files(args.pattern)
    lines = cat_lines(files)
    stats = extract_stats(lines)
    all_stats = group_stats(stats)  # collect()

    # Extracting relevant stats
    stats = validate_stats(all_stats)
    stats = derive_stats(stats)
    stats = filter_stats(stats, make_filters(args))
    relevant_stats = order_stats(stats, make_order_by(args.order_by))  # collect()

    # Reporting
    print_stats(relevant_stats)


main()
