#! /usr/bin/env python3
##########################################################################
# data_check: Check data consistency between oracle and hive/impala
#             for GOE objects
#
# LICENSE_TEXT
##########################################################################

import argparse
import logging
import os
import re
import sys

from termcolor import cprint

from goe.util.consistency_check import RandomDataCheck
from goe.util.misc_functions import disable_terminal_colors, check_offload_env
from goe.util.password_tools import PasswordTools


# -----------------------------------------------------------------------
# EXCEPTIONS 
# -----------------------------------------------------------------------
class DataCheckException(Exception): pass


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

PROG_BANNER = "data_check: (randomnly) Test data consistency between oracle and hive/impala"
COPYRIGHT_MSG = "Gluent Inc (c) 2015-2016"

# For non-partitioned tables: max table size
# Tables >= than this will not be considered for a check
DEF_MAX_MBYTES = 1000

# For partitioned tables: probability of selection (1-100%)
# Each partition will have this probability of being selected for check
DEF_SAMPLE_PCT = 5

# Default 'developer' logging level
DEF_LOG_LEVEL = "WARNING"

# ORACLE DSN regex
ORACLE_DSN_REGEX = re.compile('^(\S+?)/(\S+?)@(\S+)$')


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("data_check")


# -----------------------------------------------------------------------
# SCRIPT ROUTINES
# -----------------------------------------------------------------------

def parse_dsn(dsn):
    match_dsn = ORACLE_DSN_REGEX.match(dsn)
    if not match_dsn:
        raise DataCheckException("Invalid DSN (not printing actual dsn for security reasons)")

    return match_dsn.groups()


def extract_db_list_from_manifest(manifest_file):
    raise DataCheckException("Manifest feature has not been implemented yet")


def process_all_databases(args):
    if args.manifest:
        db_list = extract_db_list_from_manifest(args.manifest)
    else:
        if args.dsn:
            db = parse_dsn(args.dsn)
        else:
            db_passwd = os.environ.get('ORA_ADM_PASS')
            if os.environ.get('PASSWORD_KEY_FILE'):
                pass_tool = PasswordTools()
                db_passwd = pass_tool.b64decrypt(db_passwd)
            db = (os.environ['ORA_ADM_USER'], db_passwd, os.environ['ORA_CONN'])
        db_list = [db]

    for db in db_list:
        process_database(db, args)


def process_database(db, args):
    db_user, db_passwd, db_tns = db
    db_name, table_name = args.table.split('.', 1)
    probability = args.sample_pct/100.

    cprint("Processing database: %s\n" % db_tns, color='yellow')

    data_check = RandomDataCheck(db_user, db_passwd, db_tns, args.log_dir, args.verbose)
    data_check.check(db_name, table_name, probability, args.max_mbytes)

    cprint("END processing database: %s\n" % db_tns, color='yellow')


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

    print("%s\n%s" % (PROG_BANNER, COPYRIGHT_MSG))


def parse_args():
    """
      Parse arguments and return "options" object
    """

    parser = argparse.ArgumentParser(description=PROG_BANNER, formatter_class=argparse.RawDescriptionHelpFormatter,)

    parser.add_argument('-t', '--table', required=True, \
        help="Required. Owner and table-name, eg. OWNER.TABLE. Supports regular expressions for both OWNER and TABLE")

    db_source = parser.add_mutually_exclusive_group(required=False)
    db_source.add_argument('-d', '--dsn', \
        help="ORACLE database connection. Default: ORA_ADM_ environment variables")
    db_source.add_argument('-m', '--manifest', \
        help="Manifest file with ORACLE connection information")

    parser.add_argument('--max-mbytes', type=int, required=False, default=DEF_MAX_MBYTES, \
        help="Size filter for non-partitioned tables (tables larger than this will not be checked). Default: %d" % \
            DEF_MAX_MBYTES)
    parser.add_argument('-p', '--sample-pct', type=int, required=False, default=DEF_SAMPLE_PCT, \
        help="Percent of data to sample. Represents probability (1-100) of each individual partition/small table to be selected for check. Default: %d" % DEF_SAMPLE_PCT)

    parser.add_argument('-v', '--verbose', type=int, required=False, choices=[0,1,2,3], default=2, \
        help="""Verbose level:
            0: Print nothing
            1: Print final result only
            2: + "Checking segment ..." messages [default]
            3: + "Reproduce SQL"
        """)
    parser.add_argument('-L', '--log-dir', required=False, \
        help="Log directory to record execution logs (verbose level is always: 'the max' for logs)")

    parser.add_argument('--disable-colors', action='store_true', required=False, \
        help="Disable terminal colors")

    parser.add_argument('-l', '--dev-log-level', required=False, default=DEF_LOG_LEVEL, \
        help="(development) Logging level. Default: %s" % DEF_LOG_LEVEL)

    args = parser.parse_args()

    # Post processing arguments
    if args.sample_pct < 1 or args.sample_pct > 100:
        raise DataCheckException("Sample_pct should be within: 1..100 range")

    return args


def main():
    """
      MAIN ROUTINE
    """
    check_offload_env()

    args = parse_args()

    # Print tool header, set logging, etc
    print_title()
    set_logging(args.dev_log_level)
    if args.disable_colors:
        disable_terminal_colors()

    # MAIN loop
    process_all_databases(args)

    sys.exit(0)
        

#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
