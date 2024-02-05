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

##########################################################################
# cloud_present: Remap partition locations for impala table to
#                       HDFS or S3
##########################################################################

import argparse
import datetime
import logging
import re
import os
import sys
import traceback

from goe.util.config_file import GOERemoteConfig, ConfigException
from goe.util.misc_functions import (
    timedelta_to_str,
    disable_terminal_colors,
    check_offload_env,
    check_remote_offload_env,
    bytes_to_human_size,
)
from goe.util.goe_log import log, init_default_log, get_default_log, close_default_log

from goe.cloud.hive_cloud_present import HiveCloudPresent
from goe.cloud.cloud_sync_tools import (
    get_databases_and_tables,
    make_older_than_jmespath,
)

from goe.offload.offload_messages import OffloadMessages


# -----------------------------------------------------------------------
# EXCEPTIONS
# -----------------------------------------------------------------------
class CloudPresentException(Exception):
    pass


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

PROG_BANNER = "cloud_present: 'Remap' impala table between HDFS and S3 locations"

# "Source" and "target" are sections in offload configuration file:
# $OFFLOAD_CONF/offload.conf
DEFAULT_SRC = "hdfs"  # Default source 'section'
DEFAULT_DST = "s3-backup"  # Default target 'section'

DEFAULT_LOGGING = "CRITICAL"  # Default logging level

# Tool major states
STATUS_EVALUATION = "<evaluation>"
STATUS_NOT_EXECUTED = "<not executed>"


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("cloud_present")


# -----------------------------------------------------------------------
# SCRIPT ROUTINES
# -----------------------------------------------------------------------


def report_status(db_name, table_name, success, elapsed, args):
    """Report tool execution status"""
    elapsed_str = timedelta_to_str(elapsed)

    print("")

    if not success:
        log(
            "CLOUD-PRESENT failed for table: %s.%s in: %s"
            % (db_name, table_name, elapsed_str),
            ansi_code="red",
            options=args,
        )
    elif STATUS_EVALUATION == success:
        log(
            "CLOUD-PRESENT evaluation successful for table: %s.%s in: %s"
            % (db_name, table_name, elapsed_str),
            ansi_code="green",
            options=args,
        )
    elif STATUS_NOT_EXECUTED == success:
        log(
            "CLOUD-PRESENT: no changes detected for table: %s.%s in: %s"
            % (db_name, table_name, elapsed_str),
            ansi_code="yellow",
            options=args,
        )
    else:
        log(
            "CLOUD-PRESENT successful for table: %s.%s in: %s"
            % (db_name, table_name, elapsed_str),
            ansi_code="green",
            options=args,
        )


def report_present_summary(pre, db_name, table_name, args, changes_made):
    """Report summary of current state"""

    def print_section_subreport(report, section):
        log(
            "\n%s part:\n\tPartitions: %d (%.2f%%)\n\tFiles: %d (%.2f%%)\n\tRows: %d (%.2f%%)\n\tTotal size: %s (%.2f%%)"
            % (
                section.upper(),
                report[section]["partitions"],
                (100.0 * report[section]["partitions"] / report["total"]["partitions"]),
                report[section]["files"],
                (100.0 * report[section]["files"] / report["total"]["files"]),
                report[section]["rows"],
                (100.0 * report[section]["rows"] / report["total"]["rows"]),
                bytes_to_human_size(report[section]["size"]),
                (100.0 * report[section]["size"] / report["total"]["size"]),
            ),
            ansi_code="blue",
            options=args,
        )
        log(
            "\tRange: %s - %s" % (report[section]["min_ts"], report[section]["max_ts"]),
            ansi_code="blue",
            options=args,
        )

    message = "\nState for table: %s.%s " % (db_name, table_name)
    message += "after all changes" if changes_made else "before any changes"

    log(message, ansi_code="blue", options=args)
    report = pre.report()

    # Calculate total stats
    report["total"] = {"size": 0, "files": 0, "rows": 0, "partitions": 0}
    for p_type in report:
        if "total" != p_type:
            for k in report["total"]:
                report["total"][k] += report[p_type][k]

    # Display results
    for section in report:
        if "total" != section:
            print_section_subreport(report, section)


def report_projection(pre, section, section_type, filters, args):
    """Report projected changes for either HDFS or S3"""
    changes_detected = False

    projection = pre.project(section, filters)

    if projection and (projection["create"] > 0 or projection["relocate"] > 0):
        log(
            "\nTo be moved to: %s\n\tPartitions: %d\n\tFiles: %d\n\tRows: %d\n\tSize: %s"
            % (
                section_type.upper(),
                projection["partitions"],
                projection["files"],
                projection["rows"],
                bytes_to_human_size(projection["size"]),
            ),
            ansi_code="yellow",
            options=args,
        )
        log(
            "\tRange: %s - %s" % (projection["min_ts"], projection["max_ts"]),
            ansi_code="yellow",
            options=args,
        )
        changes_detected = True

    return changes_detected


def process_all_databases(cfg, args, messages):
    """Process all matching"""
    if args.project:
        messages.warning(
            "Execution mode is OFF. The tool will only project table changes",
            ansi_code="red",
        )

    for db_name, table_name in get_databases_and_tables(cfg, args):
        do_cloud_present(db_name, table_name, cfg, args, messages)

    messages.log_messages()


def do_cloud_present(db_name, table_name, cfg, args, messages):
    """Perform 'cloud present' for individual table/database"""
    success, started = True, datetime.datetime.now()
    executed = False

    log(
        "\nProcessing table: %s.%s" % (db_name, table_name),
        ansi_code="green",
        options=args,
    )

    pre = HiveCloudPresent(
        db_name=db_name,
        table_name=table_name,
        cfg=cfg,
        hive_section=args.impala_config_section,
        messages=messages,
        options=args,
    )

    try:
        if pre.table_exists:
            # Report current state
            report_present_summary(pre, db_name, table_name, args, changes_made=False)

            log("\nPROJECTED CHANGES:", ansi_code="yellow", options=args)
            targets = [
                (
                    args.remote_config_section,
                    cfg.section_type(args.remote_config_section),
                    make_older_than_jmespath(args, "<"),
                ),
                (
                    args.local_config_section,
                    cfg.section_type(args.local_config_section),
                    make_older_than_jmespath(args, ">="),
                ),
            ]

            for item in targets:
                section, typ, filters = item
                execution_needed = report_projection(pre, section, typ, filters, args)

                if execution_needed and args.remap:
                    log(
                        "\nProcessing %s partitions for table: %s.%s"
                        % (typ.upper(), db_name, table_name),
                        ansi_code="blue",
                        options=args,
                    )
                    current_success = pre.present(
                        storage_section=section, filters=filters, parallel=args.parallel
                    )
                    executed = True
                    success = min(success, current_success)

            if executed:
                report_present_summary(
                    pre, db_name, table_name, args, changes_made=True
                )
        else:
            raise CloudPresentException(
                "Table: %s.%s does NOT exist" % (db_name, table_name)
            )
    except Exception as e:
        msg = "Exception: '%s' detected for table: %s.%s" % (e, db_name, table_name)
        traceback.print_exc()
        if args.ignore_individual_db_errors:
            messages.warning(msg)
        else:
            raise CloudPresentException(msg)

    elapsed = datetime.datetime.now() - started

    # Determine statsus (why is it so complicated ?)
    status = success
    if not args.execute or args.project:
        status = STATUS_EVALUATION
    elif not executed:
        status = STATUS_NOT_EXECUTED

    report_status(db_name, table_name, status, elapsed, args)


def set_logging(cfg, args):
    """Set "global" logging parameters"""
    init_default_log("cloud_present", args)

    logging.basicConfig(
        level=logging.getLevelName(cfg.get("logging", "log_level")),
        format=cfg.get("logging", "format"),
        datefmt=cfg.get("logging", "datefmt"),
    )


def print_title():
    """Print utility title"""

    print("%s\n" % (PROG_BANNER))


def parse_args(cfg):
    """
    Parse arguments and return "options" object
    """

    parser = argparse.ArgumentParser(
        description=PROG_BANNER,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "-t",
        "--table",
        help="Required. Owner and table-name, eg. OWNER.TABLE. Supports regular expressions for both OWNER and TABLE",
    )
    source.add_argument(
        "-m", "--manifest-file", help="File with a list of OWNER.TABLE (one per line)"
    )

    parser.add_argument(
        "-S",
        "--local-config-section",
        required=False,
        default=DEFAULT_SRC,
        help="Config file section that represents HDFS location. Default: %s"
        % DEFAULT_SRC,
    )
    parser.add_argument(
        "-D",
        "--remote-config-section",
        required=False,
        default=DEFAULT_DST,
        help="Config file section that represents CLOUD location. Default: %s"
        % DEFAULT_DST,
    )
    parser.add_argument(
        "-I",
        "--impala-config-section",
        required=False,
        default=None,
        help="Config file section with impala connection information. Default: (same as --hdfs-connection)",
    )

    storage_divider = parser.add_mutually_exclusive_group(required=True)
    storage_divider.add_argument(
        "--older-than-days",
        type=int,
        help="Partitions with a boundary older than this number of days will be presented from CLOUD. Exclusive (i.e. boundary partitions are not selected)",
    )
    storage_divider.add_argument(
        "--older-than-date",
        help="Partitions with a boundary older than this date will be presented from CLOUD. Exclusive (i.e. boundary partitions are not selected)",
    )

    parser.add_argument(
        "--ignore-individual-db-errors",
        required=False,
        action="store_true",
        help="Continue if errors are detected while processing an individual table. If not used, any database errors will cause the program to terminate with an exception",
    )

    parser.add_argument(
        "--parallel",
        required=False,
        type=int,
        default=1,
        help="If specified, number of parallel threads to do work. Single threaded by default",
    )

    parser.add_argument(
        "-x",
        "--execute",
        action="store_true",
        required=False,
        help="Execute operations, rather than just logging intended actions",
    )

    display = parser.add_mutually_exclusive_group(required=False)
    display.add_argument(
        "--quiet", action="store_true", help="Minimal output", default=False
    )
    display.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    display.add_argument(
        "-vv", "--vverbose", action="store_true", help="More verbose output"
    )

    parser.add_argument(
        "-s",
        "--sync-ddl",
        action="store_true",
        required=False,
        help="Enable SYNC_DDL for impala DDL operations",
    )
    parser.add_argument(
        "--no-ansi",
        dest="ansi",
        action="store_false",
        required=False,
        help="Disable terminal colors",
    )

    default_loglevel = cfg.get("logging", "log_level", DEFAULT_LOGGING)
    parser.add_argument(
        "-l",
        "--dev-log-level",
        required=False,
        default=default_loglevel,
        help="Logging level. Default: %s" % default_loglevel,
    )

    parser.epilog = """
=== CONFIGURATION:

Before running this tool, define 'local' and 'remote' 'sections' in configuration file.

Example local ('hdfs') section:

[hdfs]
hdfs_root: %(HDFS_DATA)s
webhdfs_url: http://localhost:50070
hdfs_url: hdfs://localhost:8020
hdfs_user: goe
hdfs_newdir_permissions: 755
hadoop_host: localhost
hadoop_port: 21050
target: impala

Example remote ('cloud' (S3)) section:

[s3-backup]
s3_bucket: goe.backup
s3_prefix: user/goe/backup
server_side_encryption: AES256
storage_class: REDUCED_REDUNDANCY
max_concurrency: 1

Section names can be supplied as (--local-config-section, --remote-config-section) parameters.
(or, if not supplied, default sections: 'hdfs', 's3-backup' will be used)

Enable configuration with:
> export REMOTE_OFFLOAD_CONF=~/offload/conf/remote-offload.conf

=== SECURITY:

For 'hdfs', make sure 'hdfs_user' has access to underlying db directories.

For S3, either set up access with 'aws roles' (preferred), or define access keys in ~/.aws/credentials and
> export AWS_PROFILE=...

=== BASIC USAGE EXAMPLES:

In a nutshell, mandatory --older-than-date or --older-than-days parameters set the 'boundary' between
'local' and 'remote' partitions for a table.

I.e. --older-than-date=2015-01-01 "maps" partitions older than '2015-01-01' to be 'sourced' from 'remote'
while newer partitions are "sourced" from 'local'.

Important: If you do NOT specify -x/--execute parameter, the tool only 'projects' operations, but does not execute them.

# Report current state of 'sh.sales' table
> cloud_present -t sh.sales --older-than-date=2011-08-02

# Project required changes to re-map older partitions to S3
> cloud_present -t sh.sales --older-than-date=2011-08-02

# Execute 'partition remap' to S3 (with 'verbose' mode) (-x is what makes remap happen)
> cloud_present -t sh.sales --older-than-date=2011-08-02 -xv

# See above. Remap some of the partitions back to hdfs
> cloud_present -t sh.sales --older-than-date=2011-07-02 -xv

=== MISCELLANEOUS EXAMPLES:

# Remap in parallel
> cloud_present -t sh.sales --older-than-date=2011-08-02 -xv --parallel 8

# Use non-standard destinations
> cloud_present -t sh.sales --remote-config-section s3-backup-2 --older-than-date=2011-08-02 -xv

"""

    args = parser.parse_args()

    # Postprocessing options
    if not args.impala_config_section:
        args.impala_config_section = args.local_config_section

    if not args.execute:
        args.project = True
        args.remap = False
    else:
        args.project = False
        args.remap = True
    args.execute = True  # Legacy goe compatibility reasons

    # Adjust configuration based on user's input
    cfg.set("logging", "log_level", args.dev_log_level)

    return args


def main():
    """
    MAIN ROUTINE
    """
    check_offload_env()
    check_remote_offload_env()

    # Grab configuration from $OFFLOAD_CONF/offload.conf and read user input
    cfg = GOERemoteConfig()
    args = parse_args(cfg)

    # Print tool header, set logging, etc
    print_title()
    set_logging(cfg, args)
    if not args.ansi:
        disable_terminal_colors()

    exit_value = 0
    # Main loop
    try:
        process_all_databases(
            cfg,
            args,
            OffloadMessages.from_options_dict(vars(args), log_fh=get_default_log()),
        )
    except (CloudPresentException, ConfigException) as e:
        log(str(e), ansi_code="bright-red", options=args)
        exit_value = -1
    finally:
        close_default_log()

    sys.exit(exit_value)


#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
