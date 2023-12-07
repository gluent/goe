#! /usr/bin/env python3
""" Test results parser for Gluent tests

    LICENSE_TEXT
"""

import logging
import re
import yaml

from collections import OrderedDict
from termcolor import colored


###############################################################################
# EXCEPTIONS
###############################################################################
class TestResultsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Supported test states
STATE_PASS = "PASS"
STATE_FAIL = "FAIL"
STATE_ERROR = "ERROR"
STATE_UNKNOWN = "UNKNOWN"

# 'End of root cause' marker
END_OF_RC_MARKER = "[END]"

# 'End of entire test run' marker
END_OF_TEST_RUN_MARKER = "[END OF TEST RUN]"

# Default 'root cause' section in config file
DEFAULT_RC_SECTION = "DEFAULT"


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class TestResults(object):
    """TestResults: Gluent test result aggregator and parser"""

    def __init__(self, root_cause_file, db_type=None):
        """CONSTRUCTOR

        root_cause_file: YAML file with "root cause" definitions as:

        DEFAULT:
            <message string 1>: RC1
            <message string 2>: RC2

        HIVE:
            <message string 1>: RC1
            <message string 2>: RC2

        ...

        db_type: entry in 'config file' with 'db specific' root causes to be included
        """

        self._raw_results = []  # Raw results
        self._agg_results = {
            STATE_PASS: {},
            STATE_FAIL: {},
            STATE_ERROR: {},
        }  # Aggregated results
        self._agg_stats = {}  # Aggregated statistics

        self._root_cause_config = (
            root_cause_file  # (YAML) configuration file with root cause definition
        )
        self._db_type = (
            db_type.upper()
        )  # 'Db type' marker (to upload additional section from config)

        # Load root causes from config file specific to 'db type'
        self._root_causes = self._load_root_causes(
            self._root_cause_config, self._db_type
        )

        logger.debug("TestResults() object successfully initialized")

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _ordered_yaml_load(
        self, stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict
    ):
        """Load YAML entries in the order they appear in the file"""

        class OrderedLoader(Loader):
            pass

        def construct_mapping(loader, node):
            loader.flatten_mapping(node)
            return object_pairs_hook(loader.construct_pairs(node))

        OrderedLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
        )

        return yaml.load(stream, OrderedLoader)

    def _read_yaml(self, yaml_file):
        """Read YAML and return contents"""
        data = None

        logger.debug("Reading from YAML: %s" % yaml_file)

        with open(yaml_file) as f:
            data = self._ordered_yaml_load(f)

        logger.debug("YAML data: %s" % data)

        return data

    def _load_root_causes(self, config_file, db_type):
        """Load 'root cause' definitions from YAML file and organize them"""

        def add_root_causes(rc1, rc2):
            """Return OrderedDict(rc1 + rc2)"""

            for _ in rc2:
                rc1[_] = rc2[_]

            return rc1

        yaml_contents = self._read_yaml(config_file)
        root_causes = OrderedDict()

        for header in yaml_contents:
            if header in (DEFAULT_RC_SECTION, db_type):
                logger.debug("Adding root causes for header: %s" % header)
                root_causes = add_root_causes(root_causes, yaml_contents[header])

        logger.debug("Final root causes: %s" % root_causes)
        return root_causes

    def _find_root_cause(self, message):
        """Analyze 'error message' and find general root cause

        based on supplied (YAML) configuration
        """
        root_cause = None

        for cause in self._root_causes:
            if cause in message:
                logger.debug("Found root cause: %s for message: %s" % (cause, message))
                root_cause = self._root_causes[cause]
                break

        if not root_cause:
            logger.warning("Unable to find root cause for message: %s" % message)
            root_cause = message.split("\n")[0] if message else None

        return root_cause

    def _calculate_statistics(self, agg_results):
        """Calculate test run statistics (counts)"""

        agg_stats = {}

        for _ in allowed_states():
            agg_stats[_] = sum(
                1 for rc in agg_results[_] for test in agg_results[_][rc]
            )

        logger.debug("Aggregate statistics: %s" % agg_stats)
        return agg_stats

    def _make_state_report(self, tests, report_level):
        """Make report for a 'state' section, based on 'report level'"""

        def level1_subreport(root_cause, tests, report_level):
            """Make 'level 1+' subreport: show root causes"""
            subreport = "%s%s\n" % (("[%d]" % len(tests)).ljust(8), root_cause)

            # Level >=2: + test names
            if report_level >= 2:
                subreport += level2_subreport(tests, report_level)

            return subreport

        def level2_subreport(tests, report_level):
            """Make 'level 2+' subreport: show tests"""
            subreport = "\n"

            for name, message in list(tests.items()):
                subreport += "\t" + colored(name, "blue") + "\n"

                if report_level >= 3:
                    subreport += level3_subreport(message)

            if report_level <= 2:
                subreport += "\n"

            return subreport

        def level3_subreport(message):
            """Make 'level 3' subreport: show messages"""
            subreport = "\n\t\t".join(message.split("\n"))
            return "\n\t\t" + colored(subreport, "yellow") + "\n\n"

        # _make_state_report() body begins here
        report = ""

        for rc, tts in sorted(
            list(tests.items()), key=lambda x: len(x[1]), reverse=True
        ):
            report += level1_subreport(rc, tts, report_level)

        return report

    def _report_state_details(self, state, report_level, rc_filter, name_filter):
        """Report details for a particular 'state' and 'report level'

        for other parameters, see docstring for report_details()
        """

        def filter_by_rc(results, rc_filter):
            """Filter results by 'root cause'"""
            new_results = {}
            regex_rc = re.compile(rc_filter)

            for root_cause in results:
                if regex_rc.search(root_cause):
                    new_results[root_cause] = results[root_cause]

            return new_results

        def filter_by_name(results, name_filter):
            """Filter results by 'test name'"""
            new_results = {}
            regex_name = re.compile(name_filter)

            for root_cause in results:
                for test_name in results[root_cause]:
                    if regex_name.search(test_name):
                        if root_cause not in new_results:
                            new_results[root_cause] = {}
                        new_results[root_cause][test_name] = results[root_cause][
                            test_name
                        ]

            return new_results

        # _report_state_details() body begins here
        state = state.upper()

        if state not in allowed_states():
            logger.warn("Unsupported state: %s. Cannot report, ignoring" % state)
            return ""

        if 0 == report_level:
            logger.debug("Report level: 0. No details to report for state: %s " % state)
            return ""

        printable_headers = {
            STATE_PASS: "PASSED",
            STATE_FAIL: "FAILED",
            STATE_ERROR: "EXCEPTION",
        }

        agg_results = self._agg_results[state]
        if rc_filter:
            agg_results = filter_by_rc(agg_results, rc_filter)
        logger.debug(
            "Root causes left after applying rc filter: %s for state: %s"
            % (list(agg_results.keys()), state)
        )
        if name_filter:
            agg_results = filter_by_name(agg_results, name_filter)
        # logger.debug("Test results after applying name filter: %s for state: %s" % (agg_results, state))

        printable_results = self._make_state_report(agg_results, report_level)

        if not printable_results:
            logger.debug("Nothing to print for section: %s" % state)
            return ""

        report_text = "\n== %s tests ==\n\n" % printable_headers[state]
        report_text += printable_results

        return report_text

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def raw_results(self):
        return self._raw_results

    @property
    def agg_results(self):
        return self._agg_results

    @property
    def agg_stats(self):
        return self._agg_stats

    @property
    def db_type(self):
        return self._db_type

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def add_test(self, name, state, message):
        """Add test to 'raw results'"""
        state = state.upper()
        if state not in allowed_states():
            logger.warn("Unsupported state: %s for test: %s. Ignoring" % (state, name))
            return

        new_test = {"name": name, "state": state, "message": message}

        logger.debug("Adding test: %s to RAW results" % new_test)
        self._raw_results.append(new_test)

    def aggregate(self):
        """Aggregate test results according to state
        and assign general 'root causes'
        """

        for test in self._raw_results:
            name, state, message = test["name"], test["state"], test["message"]
            root_cause = self._find_root_cause(message) if STATE_PASS != state else None

            logger.debug(
                "Aggregating test: %s to state: %s, root cause: %s"
                % (test, state, root_cause)
            )

            if state not in allowed_states():
                logger.warn("Unrecognized state: %s for test: %s" % (state, name))
                continue

            if STATE_PASS == state:
                root_cause = (
                    "OK"  # Default (and the only) 'root cause' for 'good' tests
                )

            if root_cause not in self._agg_results[state]:
                self._agg_results[state][root_cause] = {}
            self._agg_results[state][root_cause][name] = message

            # Calculate test run statistics
            self._agg_stats = self._calculate_statistics(self._agg_results)

    def report_summary(self):
        """Report test results summary

        Returns: 'report_text' if tests were found, '' (a.k.a. False) otherwise
        """
        report_text = ""

        if not self._agg_stats:
            logger.warning("No tests found")
            return ""

        ok_tests = self._agg_stats[STATE_PASS]
        failed_tests = self._agg_stats[STATE_FAIL]
        error_tests = self._agg_stats[STATE_ERROR]
        total_tests = ok_tests + failed_tests + error_tests

        report_text = "\n***** TEST RUN SUMMARY *****\n\n"
        report_text += "TESTS EXECUTED: %d\n\n" % total_tests
        report_text += "\tOK:       %-5d [%.2f%%]\n" % (
            ok_tests,
            100.0 * ok_tests / total_tests,
        )
        report_text += "\tFAILED:   %-5d [%.2f%%]\n" % (
            failed_tests,
            100.0 * failed_tests / total_tests,
        )
        report_text += "\tERRORS:   %-5d [%.2f%%]\n" % (
            error_tests,
            100.0 * error_tests / total_tests,
        )
        report_text += "\n"

        return report_text

    def report_details(self, states, report_level, rc_filter=None, name_filter=None):
        """Report test run details for a particular list of 'states' and 'report level'

        Report levels:
            0 - Basic summary (default)
            1 - Summary + breakdown by root cause
            2 - Summary + breakdown by root cause + test names
            3 - Summary + breakdown by root cause + test names + 'error lines'

        rc_filter:   Only report for selected "root cause" regex
        name_filter: Only report for selected "test name" regex
        """
        report_text = ""

        if not self._agg_results:
            logger.warning("No tests found")
            return ""

        for state in states:
            state = state.upper()
            if state not in allowed_states():
                logger.warn("Unsupported state: %s. Skipping detailed report" % state)
            else:
                report_text += self._report_state_details(
                    state, report_level, rc_filter, name_filter
                )

        return report_text


###########################################################################
# STANDALONE ROUTINES
###########################################################################


def allowed_states():
    """Return allowed 'test' states"""
    return (STATE_PASS, STATE_FAIL, STATE_ERROR)


def normalize_state(raw_state):
    """Normalize 'test' state states"""

    state = raw_state.lower()

    if state.startswith("pass"):
        return STATE_PASS
    elif state.startswith("fail"):
        return STATE_FAIL
    elif "exception" in state:
        return STATE_ERROR
    else:
        logger.warn(
            "Unrecognized state: %s. Returning: %s" % (raw_state, STATE_UNKNOWN)
        )
        return STATE_UNKNOWN
