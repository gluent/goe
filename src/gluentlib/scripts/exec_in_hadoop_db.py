#! /usr/bin/env python3
##########################################################################
# exec_in_hadoop_db: (helper script): Execute command in Hadoop database
#                    Connection is based on gluent configuration
#
# LICENSE_TEXT
##########################################################################

import logging
import sys

import impala

from gluentlib.util.better_impyla import HiveConnection
from gluentlib.util.hs2_connection import hs2_connection, hs2_section_options, hs2_env_options, HS2_OPTIONS
from gluentlib.util.gluent_log import log
from gluentlib.util.misc_functions import check_offload_env, check_remote_offload_env

from gluent import get_options_from_list, normalise_db_paths, init


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

# Gluent.py options "imported" by this tool
GLUENT_OPTIONS=(
    'dev_log_level', 'execute', 'hash_chars',
)


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("exec_in_hadoop_db")


def set_logging(log_level):
    """ Set "global" logging parameters
    """

    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_args():
    """
      Parse arguments and return "options" object
    """
    parser = get_options_from_list(GLUENT_OPTIONS + HS2_OPTIONS)

    parser.add_option('--in', dest='section', \
        help="Execute in database, defined by remote-offload.conf configuration section")

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
        connection_options = hs2_section_options(args.section) if args.section else hs2_env_options(args)

        log('Connected to: %s' % get_hadoop_db(connection_options), options=args)
        hive_conn = HiveConnection.fromconnection(hs2_connection(connection_options))

        result = hive_conn.execute(args.cmd, cursor_fn=lambda c: c.fetchall(), as_dict=True)

        print("\n%s" % result)

        sys.exit(0)
    except impala.error.ProgrammingError as e:
        if "operation with no results" in e:
            log('Operation has no results' % e, ansi_code='yellow', options=args)
            sys.exit(0)
        else:
            raise
    except Exception as e:
        log('Exception: %s' % e, ansi_code='red', options=args)
        sys.exit(-1)
        

#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
