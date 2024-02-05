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
# exec_in_hdfs: (helper script): Execute HDFS command
#               Connection is based on goe configuration
##########################################################################

import logging
import sys

from goe.cloud.hdfs_store import HdfsStore
from goe.util.goe_log import log
from goe.util.config_file import GOERemoteConfig
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
    parser = get_options_from_list(GOE_OPTIONS)

    parser.add_option(
        "--in",
        dest="section",
        help="Execute in HDFS, defined by remote-offload.conf configuration section",
    )

    args, positionals = parser.parse_args()
    init(args)
    normalise_db_paths(args)

    if not args.section:
        raise Exception("--in parameter is required")
    if not positionals:
        raise Exception("HDFS command is missing")
    args.cmd = positionals[0]
    args.options = positionals[1:] if len(positionals) > 1 else []

    return args


def main():
    """
    MAIN ROUTINE
    """
    check_offload_env()
    check_remote_offload_env()

    args = parse_args()
    set_logging(args.dev_log_level)

    try:
        hdfs_cli = HdfsStore.hdfscli_goe_client(GOERemoteConfig(), args.section)
        log("Connected to: %s" % hdfs_cli, options=args)

        result = getattr(hdfs_cli, args.cmd)(*args.options)
        print("\n%s" % result)

        sys.exit(0)
    except Exception as e:
        log("Exception: %s" % str(e), ansi_code="red", options=args)
        sys.exit(-1)


#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
