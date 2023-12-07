#! /usr/bin/env python3
##########################################################################
# table_ddl: (helper script): Extract and print DDL for a Hive table
#
# LICENSE_TEXT
##########################################################################

import logging
import re
import sys

from goe.config.orchestration_config import OrchestrationConfig
from goe.util.better_impyla import HiveConnection, HiveTable
from goe.util.hs2_connection import hs2_connection_from_env, HS2_OPTIONS
from goe.util.config_file import GluentRemoteConfig
from goe.util.hive_ddl_transform import DDLTransform
from goe.util.gluent_log import log
from goe.util.misc_functions import options_list_to_dict, check_offload_env, check_remote_offload_env

from goe.gluent import get_options_from_list, normalise_owner_table_options, init, license, version


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

# Gluent.py options "imported" by this tool
GLUENT_OPTIONS=(
    'owner_table', 'dev_log_level', 'execute',
    'target_owner_name', 'base_owner_name',
)

PROG_BANNER = "Table DDL (table_ddl) v%s" % version()
COPYRIGHT_MSG = "%s" % license()


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("table_ddl")


def set_logging(log_level):
    """ Set "global" logging parameters
    """

    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def print_title():
    """ Print utility title """

    print("%s\n%s\n" % (PROG_BANNER, COPYRIGHT_MSG))


def parse_args():
    """
      Parse arguments and return "options" object
    """
    parser = get_options_from_list(GLUENT_OPTIONS + HS2_OPTIONS)

    parser.add_option('--as', dest='as_section',
                      help="Adjust table DDL to specific 'configuration section' (specifically: 'location' attribute)")
    parser.add_option('-T', '--transform', dest="transform", default="",
                      help="Adjust table DDL. Supported options: SCHEMA, NAME, EXTERNAL, LOCATION I.e.: 'schema=test name=tester external=True location=hdfs://localhost:8022/tmp/test.db/tester' (If specifying multiple options, enclose the entire expression in quotes)")

    args, _ = parser.parse_args()

    args.transform = options_list_to_dict(re.findall('\S+\s*=\s*\S+', args.transform))
    if args.as_section:
        args.transform['location'] = GluentRemoteConfig().file_url(args.as_section, args.owner_table, "").rstrip("/")

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

    log('Connected to: %s' % get_hadoop_db(args), options=args)

    try:
        config = OrchestrationConfig.as_defaults()
        hive_table = HiveTable(args.owner, args.table_name, HiveConnection.fromconnection(hs2_connection_from_env(config)))

        table_ddl = hive_table.table_ddl()
        table_ddl = transform(table_ddl, args.transform)

        print("\n%s" % table_ddl)

        sys.exit(0)
    except Exception as e:
        log('Exception: %s' % e, ansi_code='red', options=args)
        sys.exit(-1)
        

#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
