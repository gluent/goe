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
# table_ddl: (helper script): Extract and print DDL for a Hive table
##########################################################################

import logging
import re
import sys

from goe.config.orchestration_config import OrchestrationConfig
from goe.util.better_impyla import HiveConnection, HiveTable
from goe.util.hs2_connection import hs2_connection_from_env, HS2_OPTIONS
from goe.util.config_file import GOERemoteConfig
from goe.util.hive_ddl_transform import DDLTransform
from goe.util.goe_log import log
from goe.util.misc_functions import (
    options_list_to_dict,
    check_offload_env,
    check_remote_offload_env,
)

from goe.goe import (
    get_options_from_list,
    normalise_owner_table_options,
    init,
    license,
    version,
)


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

# GOE.py options "imported" by this tool
GOE_OPTIONS = (
    "owner_table",
    "dev_log_level",
    "execute",
    "target_owner_name",
    "base_owner_name",
)

PROG_BANNER = "Table DDL (table_ddl) v%s" % version()
COPYRIGHT_MSG = "%s" % license()


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("table_ddl")


def set_logging(log_level):
    """Set "global" logging parameters"""

    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def print_title():
    """Print utility title"""

    print("%s\n%s\n" % (PROG_BANNER, COPYRIGHT_MSG))


def parse_args():
    """
    Parse arguments and return "options" object
    """
    parser = get_options_from_list(GOE_OPTIONS + HS2_OPTIONS)

    parser.add_option(
        "--as",
        dest="as_section",
        help="Adjust table DDL to specific 'configuration section' (specifically: 'location' attribute)",
    )
    parser.add_option(
        "-T",
        "--transform",
        dest="transform",
        default="",
        help="Adjust table DDL. Supported options: SCHEMA, NAME, EXTERNAL, LOCATION I.e.: 'schema=test name=tester external=True location=hdfs://localhost:8022/tmp/test.db/tester' (If specifying multiple options, enclose the entire expression in quotes)",
    )

    args, _ = parser.parse_args()

    args.transform = options_list_to_dict(re.findall("\S+\s*=\s*\S+", args.transform))
    if args.as_section:
        args.transform["location"] = (
            GOERemoteConfig()
            .file_url(args.as_section, args.owner_table, "")
            .rstrip("/")
        )

    return args


def get_hadoop_db(args):
    return "%s:%d" % (args.hadoop_host, int(args.hadoop_port))


def transform(table_ddl, options):
    return DDLTransform().transform(table_ddl, options)


def main():
    """
    MAIN ROUTINE
    """
    check_offload_env()
    check_remote_offload_env()

    args = parse_args()
    set_logging(args.dev_log_level)

    init(args)
    normalise_owner_table_options(args)

    log("Connected to: %s" % get_hadoop_db(args), options=args)

    try:
        config = OrchestrationConfig.as_defaults()
        hive_table = HiveTable(
            args.owner,
            args.table_name,
            HiveConnection.fromconnection(hs2_connection_from_env(config)),
        )

        table_ddl = hive_table.table_ddl()
        table_ddl = transform(table_ddl, args.transform)

        print("\n%s" % table_ddl)

        sys.exit(0)
    except Exception as e:
        log("Exception: %s" % e, ansi_code="red", options=args)
        sys.exit(-1)


#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
