#! /usr/bin/env python3
""" Function "highlights":

    decorators to "close caption" function return and execution time

    i.e.

    @histate('Executing data copy', 'OK', 'FAILED', 'state')
    def xfer_data():
       ...

    may produce:

    Executing data copy		[OK] 
    or 
    Executing data copy		[OK] 
    
    LICENSE_TEXT
"""

import datetime
import logging
import sys

from termcolor import colored, cprint
from functools import wraps, partial

from gluentlib.util.misc_functions import timedelta_to_str

###############################################################################
# CONSTANTS
###############################################################################

DEF_OK_MSG = "OK"
DEF_FAILED_MSG = "FAILED"

###############################################################################
# EXCEPTIONS
###############################################################################
class HistateException(Exception): pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


###########################################################################
# DECORATORS
###########################################################################

def attach_wrapper(obj, func = None):
    """ Let user adjust decorator attributes

        i.e. <decorated callable>.set_force()
    """
    if func is None:
        return partial(attach_wrapper, obj)

    setattr(obj, func.__name__, func)
    return func


def histateandtime(msg=None, ok_msg=DEF_OK_MSG, fail_msg=DEF_FAILED_MSG):
    """ Wrap function in the following logic:

        Print 'msg' ...
        Start time measurement
        Execute 'wrapped function'
        Calculate elapsed time
        If 'wrapped function' returns: 
            True:             cprint [$ok_msg] + time
            False:            cprint [$failed_msg] + time

    """

    def decorate(func):
        fmsg = msg if msg else "Executing function %s()" % func.__name__
        fmsg = colored(fmsg, attrs=['underline'])

        decorate.ok_msg = ok_msg
        decorate.fail_msg = fail_msg

        @wraps(func)
        def wrapper(*args, **kwargs):
            print("\n%-40s" % fmsg)
            cprint(datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y'), 'grey')
            sys.stdout.flush()

            started = datetime.datetime.now()
            result = func(*args, **kwargs)
            elapsed = datetime.datetime.now() - started

            if result:
                status = success_msg(decorate.ok_msg)
            else:
                status = failed_msg(decorate.fail_msg)

            # Print elapsed time and status
            cprint("Step time: %s" % timedelta_to_str(elapsed), 'grey')
            print(status)
            sys.stdout.flush()

            return result


        @attach_wrapper(wrapper)
        def set_state(val):
            logger.debug("Changing histate 'state' attribute to: %s" % val)
            decorate.state = val


        @attach_wrapper(wrapper)
        def set_ok_msg(val):
            logger.debug("Changing histate 'OK' message to: %s" % val)
            decorate.ok_msg = val
      

        @attach_wrapper(wrapper)
        def set_failed_msg(val):
            logger.debug("Changing histate 'FAILED' message to: %s" % val)
            decorate.fail_msg = val
      

        return wrapper


    return decorate


###########################################################################
# USEFUL ONELINERS
###########################################################################

def success_msg(msg=DEF_OK_MSG):
    """ Return 'colorized' SUCCESS messages """
    return colored(msg, 'green', attrs=['bold'])


def failed_msg(msg=DEF_FAILED_MSG):
    """ Return 'colorized' FAILED messages """
    return colored(msg, 'red', attrs=['bold'])


def neutral_msg(msg):
    """ Return 'colorized' "NEUTRAL" messages """
    return colored(msg, 'yellow', attrs=['bold'])


def status_msg(status, s_msg=DEF_OK_MSG, f_msg=DEF_FAILED_MSG):
    """ Return 'colorized' status messages """
    
    if status:
         return success_msg(s_msg)
    else:
         return failed_msg(f_msg)


