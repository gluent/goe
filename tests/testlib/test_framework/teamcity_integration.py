#! /usr/bin/env python3
""" A few simple primitives to integrate (gluent custom) test results with TeamCity:

    https://confluence.jetbrains.com/display/TCDL/Build+Script+Interaction+with+TeamCity

    LICENSE_TEXT
"""

import logging
import re
import sys

###############################################################################
# SET IMPORTABLE NAMES
###############################################################################
__all__ = [
    "teamcity_starttestsuite",
    "teamcity_endtestsuite",
    "teamcity_starttestblock",
    "teamcity_endtestblock",
    "teamcity_starttest",
    "teamcity_endtest",
    "teamcity_failtest",
    "teamcity_failtest_notactual",
]


###############################################################################
# EXCEPTIONS
###############################################################################
class TeamCityIntegrationException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

###########################################################################
# OUTPUT DECORATOR
###########################################################################


def emit_1line(func):
    def func_wrapper(*args, **kwargs):
        msg = func(*args, **kwargs)
        # print(msg.replace('\n', '.'))
        # sys.stdout.flush()
        print(msg, flush=True)

    return func_wrapper


###########################################################################
# STANDALONE ROUTINES
###########################################################################


def escape(s):
    return re.sub(r"[\'\[\]|]", r"|\g<0>", s)


@emit_1line
def teamcity_starttestsuite(name):
    """Print START TEST SUITE block"""
    return "##teamcity[testSuiteStarted name='%s']" % name


@emit_1line
def teamcity_endtestsuite(name):
    """Print END TEST SUITE block"""
    return "##teamcity[testSuiteFinished name='%s']" % name


@emit_1line
def teamcity_starttestblock(name):
    """Print START TEST BLOCK block"""
    return "##teamcity[blockOpened name='%s']" % name


@emit_1line
def teamcity_endtestblock(name):
    """Print END TEST BLOCK block"""
    return "##teamcity[blockClosed name='%s']" % name


@emit_1line
def teamcity_starttest(name):
    """Print START TEST block"""
    return "##teamcity[testStarted name='%s' captureStandardOutput='true']" % name


@emit_1line
def teamcity_endtest(name):
    """Print END TEST block"""
    return "##teamcity[testFinished name='%s']" % name


@emit_1line
def teamcity_failtest(name, message):
    """Print FAIL TEST block"""
    return "##teamcity[testFailed name='%s' message='%s']" % (name, escape(message))


@emit_1line
def teamcity_failtest_notactual(name, message, expected, actual):
    """Print FAIL TEST block with expected != actual"""
    return (
        "##teamcity[testFailed type='comparisonFailure' name='%s' message='%s' expected='%s' actual='%s']"
        % (name, escape(message), escape(expected), escape(actual))
    )
