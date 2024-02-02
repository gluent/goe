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
# exec_in_hadoop_db: (helper script): Execute command in Hadoop database
#                    Connection is based on goe configuration
##########################################################################

import logging
import sys

import impala

from goe.util.better_impyla import HiveConnection
from goe.util.hs2_connection import (
    hs2_connection,
    hs2_section_options,
    hs2_env_options,
    HS2_OPTIONS,
)
from goe.util.goe_log import log
from goe.util.misc_functions import check_offload_env, check_remote_offload_env

from goe.goe import get_options_from_list, normalise_db_paths, init


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

# GOE.py options "imported" by this tool
GOE_OPTIONS = (
    "dev_log_level",
    "execute",
)


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("exec_in_hadoop_db")


def set_logging(log_level):
    """Set "global" logging parameters"""

    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args():
    """
    Parse arguments and return "options" object
    """
    parser = get_options_from_list(GOE_OPTIONS + HS2_OPTIONS)

    parser.add_option(
        "--in",
        dest="section",
        help="Execute in database, defined by remote-offload.conf configuration section",
    )

    args, positionals = parser.parse_args()
    init(args)
    normalise_db_paths(args)

    if not positionals:
        raise Exception("Database command is missing")
    args.cmd = " ".join(positionals)

    return args


def get_hadoop_db(args):
    return "%s:%d" % (args.hadoop_host, int(args.hadoop_port))


def main():
    """
    MAIN ROUTINE
    """
    check_offload_env()
    check_remote_offload_env()

    args = parse_args()
    set_logging(args.dev_log_level)

    try:
        connection_options = (
            hs2_section_options(args.section) if args.section else hs2_env_options(args)
        )

        log("Connected to: %s" % get_hadoop_db(connection_options), options=args)
        hive_conn = HiveConnection.fromconnection(hs2_connection(connection_options))

        result = hive_conn.execute(
            args.cmd, cursor_fn=lambda c: c.fetchall(), as_dict=True
        )

        print("\n%s" % result)

        sys.exit(0)
    except impala.error.ProgrammingError as e:
        if "operation with no results" in e:
            log("Operation has no results" % e, ansi_code="yellow", options=args)
            sys.exit(0)
        else:
            raise
    except Exception as e:
        log("Exception: %s" % e, ansi_code="red", options=args)
        sys.exit(-1)


#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
