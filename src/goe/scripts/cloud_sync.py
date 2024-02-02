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
# cloud_sync: Move data and associated "table" objects between 'destinations'
##########################################################################

import argparse
import datetime
import logging
import re
import os
import sys
import traceback

from goe.util.config_file import GOERemoteConfig, ConfigException
from goe.util.hs2_connection import hs2_connection, hs2_env_options
from goe.util.misc_functions import (
    timedelta_to_str,
    disable_terminal_colors,
    check_offload_env,
    check_remote_offload_env,
)
from goe.util.better_impyla import HiveConnection
from goe.util.goe_log import log, init_default_log, get_default_log, close_default_log

from goe.cloud.hive_table_backup import HiveTableBackup
from goe.cloud.cloud_sync_tools import (
    get_databases_and_tables,
    make_older_than_jmespath,
)

from goe.offload.offload_messages import OffloadMessages


# -----------------------------------------------------------------------
# EXCEPTIONS
# -----------------------------------------------------------------------
class CloudSyncException(Exception):
    pass


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

PROG_BANNER = "cloud_sync: Backup, restore and sync Hive/Impala tables to and from remote destinations"
COPYRIGHT_MSG = "GOE Inc (c) 2015-2016"

# "Source" and "target" are sections in offload configuration file:
# $OFFLOAD_CONF/offload.conf
DEFAULT_SRC = "hdfs"  # Default db 'section'
DEFAULT_DST = "s3-backup"  # Default backup 'section'

DEFAULT_LOGGING = "CRITICAL"  # Default logging level

# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("cloud_sync")


# -----------------------------------------------------------------------
# SCRIPT ROUTINES
# -----------------------------------------------------------------------


def report_status(oper, db_name, table_name, elapsed, execute, args, success=True):
    """Report tool execution status"""
    elapsed_str = timedelta_to_str(elapsed)
    oper = oper.capitalize() if execute else "Evaluation"

    print("")

    if not success:
        log(
            "%s for table: %s.%s failed in: %s"
            % (oper, db_name, table_name, elapsed_str),
            ansi_code="red",
            options=args,
        )
    else:
        log(
            "%s successful for table: %s.%s in: %s"
            % (oper, db_name, table_name, elapsed_str),
            ansi_code="green",
            options=args,
        )


def process_all_tables(cfg, args, messages):
    """Process all gualifying tables"""

    def get_oper_name(args):
        return (
            (args.backup and "backup")
            or (args.restore and "restore")
            or (args.clone and "clone")
        )

    def get_oper_args(args):
        oper_args = {
            "overwrite": args.overwrite_files,
            "delete_on_target": args.delete_on_target,
            "parallel": args.parallel,
            "override_empty": args.override_empty,
            "fast_path": args.fastpath,
            "transform_ddl": args.transform_ddl,
        }

        if not args.backup:
            oper_args["drop_table_if_exists"] = args.recreate_tables
            oper_args["rescan_partitions"] = args.rediscover_partitions
            oper_args["skip_statistics"] = args.skip_statistics
            oper_args["validate_counts"] = args.validate_counts

        return oper_args

    oper_name = get_oper_name(args)
    oper_args = get_oper_args(args)

    if not args.execute:
        messages.warning(
            "Execution mode is OFF. The tool will only report table state before: %s"
            % oper_name.upper(),
            ansi_code="red",
        )

    for db_name, table_name in get_databases_and_tables(cfg, args):
        do_single_table(oper_name, oper_args, db_name, table_name, cfg, args, messages)

    messages.log_messages()


def do_single_table(oper_name, oper_args, db_name, table_name, cfg, args, messages):
    """Perform backup/restore/clone for individual table"""
    started = datetime.datetime.now()

    log(
        "Processing table: %s.%s" % (db_name, table_name),
        ansi_code="green",
        options=args,
    )

    bkp = HiveTableBackup(
        db_name=db_name,
        table_name=table_name,
        local=args.local_config_section,
        remote=args.remote_config_section,
        # filters = make_older_than_jmespath(args),
        cfg=cfg,
        messages=messages,
        options=args,
    )

    try:
        getattr(bkp, oper_name)(**oper_args)
    except Exception as e:
        msg = "Exception: %s while executing %s on table: %s.%s" % (
            e,
            oper_name.upper(),
            db_name,
            table_name,
        )
        traceback.print_exc()
        if args.ignore_individual_db_errors:
            messages.warn(msg)
        else:
            raise CloudSyncException(msg)

    elapsed = datetime.datetime.now() - started

    report_status(oper_name, db_name, table_name, elapsed, args.execute, args)


def set_logging(cfg, args):
    """Set "global" logging parameters"""
    init_default_log("cloud_sync", args)

    logging.basicConfig(
        level=logging.getLevelName(cfg.get("logging", "log_level")),
        format=cfg.get("logging", "format"),
        datefmt=cfg.get("logging", "datefmt"),
    )


def print_title():
    """Print utility title"""

    print("%s\n%s" % (PROG_BANNER, COPYRIGHT_MSG))


def parse_args(cfg):
    """
    Parse arguments and return "options" object
    """

    def make_transform_ddl(transform_ddl):
        if not transform_ddl:
            return transform_ddl

        ret = {}
        for remap in transform_ddl:
            if "=" not in remap:
                raise CloudSyncException(
                    "Invalid table DDL remap definition: %s. Expecting: parameter=value"
                    % remap
                )
            key, value = remap.split("=")
            ret[key.lower()] = value

        return ret

    parser = argparse.ArgumentParser(
        description=PROG_BANNER,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument(
        "--backup",
        action="store_true",
        help="Replicate data: LOCAL -> REMOTE + save table DDL on REMOTE",
    )
    operation.add_argument(
        "--restore",
        action="store_true",
        help="Replicate data: REMOTE -> LOCAL + extract table DDL from REMOTE and (re)create table on LOCAL (if necessary)",
    )
    operation.add_argument(
        "--clone",
        action="store_true",
        help="Replicate data: LOCAL -> REMOTE + extract table DDL from LOCAL and (re)create on REMOTE (if necessary)",
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
        help="Local (a.k.a. DB) location (config file section name). Default: %s"
        % DEFAULT_SRC,
    )
    parser.add_argument(
        "-D",
        "--remote-config-section",
        required=False,
        default=None,
        help="Remote (a.k.a. BACKUP/CLONE) location (config file section name). Default: %s"
        % "--local-config-section if 'clone'. Otherwise: %s"
        % DEFAULT_DST,
    )

    # older_then = parser.add_mutually_exclusive_group(required=False)
    # older_then.add_argument('--older-than-days', type=int, default=None, \
    #    help="Backup partitions older than this number of days. Exclusive (i.e. boundary partitions are not selected)")
    # older_then.add_argument('--older-than-date', default=None, \
    #    help="Backup partitions older than this date (use YYYY-MM-DD format). Overrides --older-than-days if both are present.")

    parser.add_argument(
        "-f",
        "--force",
        required=False,
        action="store_true",
        help="--overwrite-files + --recreate-tables + --rediscover-partitions",
    )
    parser.add_argument(
        "--overwrite-files",
        required=False,
        action="store_true",
        help="Overwrite files even if they are the same",
    )
    parser.add_argument(
        "--recreate-tables",
        required=False,
        action="store_true",
        help="Drop and re-create tables if they exist",
    )
    parser.add_argument(
        "--rediscover-partitions",
        required=False,
        action="store_true",
        help="Re-scan table 'directory' + add partitions",
    )
    parser.add_argument(
        "--skip-statistics",
        required=False,
        action="store_true",
        help="Skip statistics collection after restore",
    )
    parser.add_argument(
        "--override-empty",
        required=False,
        action="store_true",
        help="Continue even if SOURCE is empty (dangerous, especially without --no-delete-on-target",
    )
    parser.add_argument(
        "--no-validate-counts",
        dest="validate_counts",
        required=False,
        action="store_false",
        help="Skip SELECT count() validation after restore or clone",
    )
    parser.add_argument(
        "--no-delete-on-target",
        dest="delete_on_target",
        required=False,
        action="store_false",
        help="Disable deletion of TARGET files that do not exist on SOURCE",
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
        help="Replication parallelism (a.k.a 'number of files' to copy simultaneously)",
    )
    parser.add_argument(
        "--fastpath",
        required=False,
        action="store_true",
        help="Skips 'source' <-> 'target' comparison. Faster when 'target' is empty, but disables 'delete on target' functionality",
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
        help="Enable SYNC_DDL for DDL operations (may be needed for Impala)",
    )
    parser.add_argument(
        "-T",
        "--transform-ddl",
        nargs="+",
        required=False,
        help="Adjust table DDL. Supported options: SCHEMA, NAME, EXTERNAL, LOCATION I.e.: schema=test name=tester external=True location=hdfs://localhost:8022/tmp/test.db/tester",
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

Before running this tool, define 'db' (default: 'hdfs') and 'backup' (default: 's3-backup') sections in configuration file.

Example 'db' section:

[hdfs]
hdfs_root: %(HDFS_DATA)s
webhdfs_url: http://localhost:50070
hdfs_url: hdfs://localhost:8020
hdfs_user: goe
hdfs_newdir_permissions: 755
hadoop_host: localhost
hadoop_port: 21050
target: impala

Example 'backup' (S3) section:

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

Tool supports 3 commands:

--backup: Copy files from 'db' to 'backup' section. Extract table DDL and save it as 'metanote' in 'backup' section

--restore: Copy files from 'backup' to 'db' section. If requested, recreate table using DDL from 'metanote' in 'backup' section

--clone: Copy files from 'db' to 'backup' section. If requested, extract table DDL from 'db' and recreate it on 'backup'

Important: If you do NOT specify -x/--execute parameter, the tool only compares and reports the state of local/remote data sources

# Report current 'backup state' of 'sh.sales' table
> cloud_sync --backup -t sh.sales -v

# Backup table 'sh.sales' (to default 'backup': destination: 's3-backup')
> cloud_sync --backup -t sh.sales -xv

# Backup table 'sh.sales', copying 20 files in parallel
# Additional 'intra-file' parallelism can be achieved by setting: 'max_concurrency' in remote-offload.conf (10 by default)
> cloud_sync --backup -t sh.sales -xv --parallel 20

# Backup table 'sh.sales' to custom destination
> cloud_sync --backup -t sh.sales -xv -D s3-backup2

# Backup table 'sh.sales' with force mode (aka: re-copy files even if they are the same)
> cloud_sync --backup -t sh.sales -xv -f

# Restore table 'sh.sales' from default 'backup' destination
> cloud_sync --restore -t sh.sales -xv

# Restore table 'sh.sales' as sh_test.tester
> cloud_sync --restore -t sh.sales -xv -T schema=sh_test name=tester

# Restore table 'sh.sales' with custom options
> cloud_sync --restore -t sh.sales -xv --recreate-tables --rediscover-partitions --skip-statistics

# Sync table 'sh.sales' to remote HDFS cluster
> cloud_sync --clone -t sh.sales -xv -D hdfs-remote

"""

    args = parser.parse_args()

    # Argument post-processing
    if not args.remote_config_section:
        args.remote_config_section = (
            args.local_config_section if args.clone else DEFAULT_DST
        )

    if args.force:
        args.overwrite_files = True
        args.recreate_tables = True
        args.rediscover_partitions = True

    args.transform_ddl = make_transform_ddl(args.transform_ddl)

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
        process_all_tables(
            cfg,
            args,
            OffloadMessages.from_options_dict(vars(args), log_fh=get_default_log()),
        )
    except (CloudSyncException, ConfigException) as e:
        log(str(e), ansi_code="bright-red", options=args)
        exit_value = -1
    finally:
        close_default_log()

    sys.exit(exit_value)


#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
