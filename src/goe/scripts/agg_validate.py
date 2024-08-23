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
# agg_validate: Check data consistency between Oracle and backend
#     by comparing "column aggregate_functions" (min, max, count, ...) between the two
#
##########################################################################

import logging
import re
import sys

from goe.config import option_descriptions, config_file, orchestration_defaults
from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.offload_validation import (
    CrossDbValidator,
    DEFAULT_SELECT_COLS,
    GROUPBY_PARTITIONS,
    DEFAULT_AGGS,
    SUPPORTED_OPERATIONS,
)
from goe.util.hs2_connection import HS2_OPTIONS
from goe.util.misc_functions import (
    csv_split,
    is_number,
    is_pos_int,
    parse_python_from_string,
)
from goe.util.goe_log import log_exception

from goe.offload.offload_messages import OffloadMessages
from goe.orchestration import command_steps

from goe.goe import (
    get_options_from_list,
    normalise_owner_table_options,
    init,
    init_log,
    log,
    get_log_fh_name,
    version,
    get_log_fh,
    log_timestamp,
)


# -----------------------------------------------------------------------
# EXCEPTIONS
# -----------------------------------------------------------------------
class AggValidateException(Exception):
    pass


# -----------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------

PROG_BANNER = "Validate (agg_validate) v%s" % version()

REGEX_FILTER = re.compile("(\S+)\s+(%s)\s+(\S+)" % "|".join(SUPPORTED_OPERATIONS), re.I)

# GOE.py options "imported" by this tool
GOE_OPTIONS = (
    "owner_table",
    "dev_log_level",
    "execute",
    "target_owner_name",
    "base_owner_name",
    "target_name",
    "ora_app_user",
    "ora_app_pass",
    "oracle_dsn",
    "backend_session_parameters",
    "quiet",
    "verbose",
    "vverbose",
)


# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("agg_validate")


# -----------------------------------------------------------------------
# SCRIPT ROUTINES
# -----------------------------------------------------------------------


def validate_table(args, messages):
    """Validate specific table"""

    def step_fn():
        config = OrchestrationConfig.from_dict({"verbose": args.verbose})
        validator = CrossDbValidator(
            db_name=args.owner,
            table_name=args.table_name,
            connection_options=config,
            messages=messages,
        )

        status, _ = validator.validate(
            selects=args.selects,
            filters=args.filters,
            group_bys=args.group_bys,
            aggs=args.aggregate_functions,
            safe=not args.skip_boundary_check,
            as_of_scn=args.as_of_scn,
            execute=args.execute,
            frontend_parallelism=args.frontend_parallelism,
        )
        if status:
            messages.log("[OK]", ansi_code="green")
            return True
        else:
            messages.log("[ERROR]", ansi_code="red")
            return False

    return (
        messages.offload_step(
            command_steps.STEP_VALIDATE_DATA,
            step_fn,
            execute=args.execute,
        )
        or False
    )


def set_logging(log_level):
    """Set "global" logging parameters"""

    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def print_title():
    """Print utility title"""

    print("%s\n" % (PROG_BANNER))


def post_process_args(args):
    """Post process arguments"""

    def parse_filters(filters):
        def parse_single_filter(filt):
            match = REGEX_FILTER.match(filt)
            if match:
                return (
                    match.group(1),
                    match.group(2),
                    parse_python_from_string(match.group(3)),
                )
            else:
                raise AggValidateException("Invalid FILTER expression: %s" % filt)

        return [parse_single_filter(_.strip()) for _ in filters.split(",")]

    if args.filters:
        args.filters = parse_filters(args.filters)
    if args.selects:
        if 1 == len(args.selects) and is_number(args.selects[0]):
            args.selects = args.selects[0]
        else:
            args.selects = csv_split(args.selects)
    if args.group_bys and GROUPBY_PARTITIONS != args.group_bys:
        args.group_bys = csv_split(args.group_bys)
    if args.aggregate_functions and DEFAULT_AGGS != args.aggregate_functions:
        args.aggregate_functions = csv_split(args.aggregate_functions)
    if args.frontend_parallelism:
        if not is_pos_int(args.frontend_parallelism):
            raise AggValidateException(
                "Invalid frontend parallelism: %s" % args.frontend_parallelism
            )
        else:
            args.frontend_parallelism = int(args.frontend_parallelism)


def parse_args():
    """Parse arguments and return "options" object"""
    parser = get_options_from_list(GOE_OPTIONS + HS2_OPTIONS)

    parser.add_option(
        "--as-of-scn",
        type=int,
        default=None,
        help="Execute validation on front-end site as-of specified SCN (assumes 'ORACLE' front-end)",
    )
    parser.add_option(
        "-S",
        "--selects",
        default=[DEFAULT_SELECT_COLS],
        help="SELECTs: <List of columns> OR <Number of columns> to run aggregations on. If <number> is specified -> the first and last columns and the <number>-2 highests cardinality columns will be selected. Default: %s"
        % DEFAULT_SELECT_COLS,
    )
    parser.add_option(
        "-F",
        "--filters",
        default=None,
        help="FILTERs: List of: (<column> <operation> <value>) expressions, separated by ',' e.g. PROD_ID < 12, CUST_ID >= 1000. Expressions must be supported in both FRONT-END and BACK-END databases",
    )
    parser.add_option(
        "-G",
        "--group-bys",
        default=None,
        help="GROUP BYs: List of group by expressions, separated by ',' e.g. COL1, COL2. Expressions must be supported in both FRONT-END and BACK-END databases. Default: %s"
        % None,
    )
    parser.add_option(
        "-A",
        "--aggregate-functions",
        default=DEFAULT_AGGS,
        help="AGGREGATE FUNCTIONSs: List of aggregate functions to apply, separated by ',' e.g. max, min, count. Functions need to be available and use the same arguments in both FRONT-END and BACK-END databases. Default: %s"
        % [DEFAULT_AGGS],
    )
    parser.add_option(
        "--frontend-parallelism",
        default=orchestration_defaults.verify_parallelism_default(),
        type=int,
        help=option_descriptions.VERIFY_PARALLELISM,
    )
    parser.add_option(
        "--skip-boundary-check",
        action="store_true",
        help="Do NOT include 'offloaded boundary check' in the list of filters 'offloaded boundary check' filter defines data that was offloaded to BACK-END database (as opposed to data that 'is sourced' from FRONT-END). For example: WHERE TIME_ID < timestamp '2015-07-01 00:00:00' which resulted from applying e.g. --older-than-date=2015-07-01 filter during offload.",
    )

    args, _ = parser.parse_args()

    post_process_args(args)

    return args


def main():
    """
    MAIN ROUTINE
    """

    config_file.check_config_path()
    config_file.load_env()

    args = parse_args()
    init(args)

    init_log("agg_validate_%s" % args.owner_table)
    log("")
    log(PROG_BANNER, ansi_code="underline")
    log("Log file: %s" % get_log_fh_name())

    normalise_owner_table_options(args)

    set_logging(args.dev_log_level)

    try:
        messages = OffloadMessages.from_options(args)

        ret = validate_table(args=args, messages=messages)
    except Exception as exc:
        log("Exception caught at top-level", ansi_code="red")
        log_timestamp()
        log_exception(exc, log_fh=get_log_fh(), options=args)
        sys.exit(1)

    sys.exit(0 if ret else 1)


#### MAIN PROGRAM BEGINS HERE
if __name__ == "__main__":
    main()
