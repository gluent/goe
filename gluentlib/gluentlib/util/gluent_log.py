#! /usr/bin/env python3
""" gluent_log: Gluent general logging routines

    Replacement of 'loggers' from gluent.py that avoid globals
    and do not rely on 'expected' parameter structure

    LICENSE_TEXT
"""

import copy
import logging
import os
import os.path
import sys
import traceback

from argparse import Namespace
from datetime import datetime

from gluentlib.offload.offload_messages import OffloadMessages, NORMAL, VERBOSE, VVERBOSE
from gluentlib.util.misc_functions import get_option, set_option

###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

###############################################################################
# GLOBALS
###############################################################################
global_log_fh = None


def get_default_options(addons=None):
    """ 'Sensible defaults' for 'options' object used in this module's routines
    """

    options = Namespace()
    set_option(options, 'ansi', False)
    set_option(options, 'normal', True)

    if addons:
        for add_opt in addons:
            set_option(options, add_opt, addons[add_opt])

    return options


def get_default_log():
    return global_log_fh


def log(line, detail=NORMAL, ansi_code=None, log_fh=None, options=get_default_options()):
    # Log to a (text, on disk) log file
    log_handle = log_fh or global_log_fh
    if log_handle:
        log_handle.write(line + '\n')
        log_handle.flush()

    # Log to screen
    if get_option(options, 'quiet'):
        sys.stdout.write('.')
        sys.stdout.flush()
    elif detail == NORMAL or (detail <= VERBOSE and get_option(options, 'verbose')) or \
            (detail <= VVERBOSE and get_option(options, 'vverbose')):
        line = ansi(line, ansi_code, options=options)
        sys.stdout.write(line + '\n')
        sys.stdout.flush()


def log_exception(exception, detail=NORMAL, log_fh=None, options=get_default_options()):
    log_handle = log_fh or global_log_fh
    if log_handle and log_handle != sys.stderr:
        log_handle.write(traceback.format_exc() + '\n')
        log_handle.flush()

    if detail > NORMAL or get_option(options, 'verbose') or get_option(options, 'vverbose'):
        sys.stdout.write(traceback.format_exc() + '\n')
        sys.stdout.flush()
    else:
        sys.stdout.write("%s: %s \n" % (str(type(exception).__name__), str(exception)))
        sys.stdout.flush()


def init_default_log(log_name, options=get_default_options()):
    assert log_name
    global global_log_fh

    if global_log_fh:
        logger.warn("Initializing log: %s while previous log: %s is active" % (log_name, global_log_fh.name))

    options.log_path = get_option(options, "log_path", None) or os.environ.get('OFFLOAD_LOGDIR')
    if not options.log_path and os.environ.get('OFFLOAD_HOME'):
        options.log_path = os.path.join(os.environ.get('OFFLOAD_HOME'), 'log')

    options.log_path = options.log_path or '.'

    current_log_name = '%s_%s.log' % (log_name, datetime.now().isoformat())
    log_path = os.path.join(options.log_path, current_log_name)

    logger.debug("Initializing default text on-disk log file as: %s" % log_path)
    global_log_fh = open(log_path, 'w')


def close_default_log():
    if global_log_fh:
        logger.debug("Closing default text on-disk log file as: %s" % global_log_fh.name)
        global_log_fh.close()


def ansi(line, ansi_code, options=get_default_options()):
    return OffloadMessages.ansi_wrap(line, ansi_code, get_option(options, 'ansi'))


def log_timestamp(ansi_code='grey', options=get_default_options()):
    if get_option(options, 'execute'):
        ts = datetime.now()
        ts = ts.replace(microsecond=0)
        log(ts.strftime('%c'), detail=VERBOSE, ansi_code=ansi_code, options=options)
        return ts
    else:
        return None


def log_timedelta(start_time, ansi_code='grey', options=get_default_options()):
    if get_option(options, 'execute'):
        ts2 = datetime.now()
        ts2 = ts2.replace(microsecond=0)
        log('Step time: %s' % (ts2 - start_time), detail=VERBOSE, ansi_code=ansi_code, options=options)
        return ts2 - start_time


def step(title, step_fn, execute=False, skip=None, messages=None, optional=False, options=get_default_options()):
    step_results = None

    # Do not pollute original 'options' object which is passed (upwards) by reference
    step_options = copy.copy(options)
    set_option(step_options, 'execute', execute)

    log('', options=step_options)
    log(title, ansi_code='underline', options=step_options)
    start_time = log_timestamp(options=step_options)

    step_id = title.replace(' ', '_').lower()
    if skip and step_id in skip:
        log('skipped', options=step_options)
    else:
        try:
            if execute:
                step_results = step_fn()
                td = log_timedelta(start_time, options=step_options)
                log('Done', ansi_code='green', options=step_options)
                if messages:
                    messages.step_delta(title, td)
            else:
                step_results = True
        except Exception as exc:
            log_timedelta(start_time, options=step_options)
            if optional:
                if messages:
                    messages.warning('Error in "%s": %s' % (title, str(exc)), ansi_code='red')
                else:
                    log(str(exc), ansi_code='red', options=step_options)
                log('Optional step, continuing', ansi_code='green', options=step_options)
            else:
                raise

    return step_results


if __name__ == "__main__":
    import time

    from functools import partial

    from gluentlib.util.gluent_log import log as log_f, step as step_f, init_default_log, get_default_log, \
        close_default_log


    def main():
        def waiter3():
            time.sleep(3)

        def waiter2():
            time.sleep(2)

        messages = OffloadMessages(VERBOSE)

        print("== DEFAULT AND NORMAL")
        log_f("Hello, maties, I'm colorless !")
        log_f("Hello, maties in red!", ansi_code="red")
        log_f("Hello, maties in green!", ansi_code="green")
        log_f("Hello, maties - I'm VERBOSE (you should see me)!", detail=VERBOSE, ansi_code="green")
        log_f("Hello, maties - I'm VVERBOSE (you should NOT see me)!", detail=VVERBOSE, ansi_code="green")
        step_f("Waiting for 3 seconds and executing", waiter3, messages=messages, execute=True)
        step_f("Waiting for 2 seconds and NOT executing", waiter2, messages=messages)
        print("== END DEFAULT")

        options = get_default_options({'ansi': True, 'verbose': True})
        log = partial(log_f, options=options)
        step = partial(step_f, options=options)

        print("\n\n== CUSTOM OPTIONS AND VERBOSE")
        log("Hello, maties, I'm colorless !")
        log("Hello, maties in red!", ansi_code="red")
        log("Hello, maties in green!", ansi_code="green")
        log("Hello, maties - I'm VERBOSE (you should see me)!", detail=VERBOSE, ansi_code="green")
        log("Hello, maties - I'm VVERBOSE (you should NOT see me)!", detail=VVERBOSE, ansi_code="green")
        step("Waiting for 3 seconds and executing", waiter3, messages=messages, execute=True)
        step("Waiting for 2 seconds and NOT executing", waiter2, messages=messages)
        print("== END CUSTOM OPTIONS")

        options.log_path = "/tmp"
        init_default_log("gluent_log", options)

        print("\n\n== DUP TO LOG FILE")
        log("Hello, maties, I'm colorless !")
        log("Hello, maties in red!", ansi_code="red")
        log("Hello, maties in green!", ansi_code="green")
        log("Hello, maties - I'm VERBOSE (you should see me)!", detail=VERBOSE, ansi_code="green")
        log("Hello, maties - I'm VVERBOSE (you should NOT see me)!", detail=VVERBOSE, ansi_code="green")
        step("Waiting for 3 seconds and executing", waiter3, messages=messages, execute=True)
        step("Waiting for 2 seconds and NOT executing", waiter2, messages=messages)

        log_name = get_default_log().name
        close_default_log()

        print("-- Log lines from file: %s" % log_name)
        with open(log_name) as fh:
            for line in fh:
                print(line)
        print("== END DUP TO LOG FILE")


    main()
