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

""" OffloadMessages: Library for handling messages
"""

# Standard Library
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Optional

# Third Party Libraries
import orjson

# GOE
from goe.orchestration import orchestration_constants
from goe.orchestration.command_steps import STEP_TITLES, step_title
from goe.util.goe_log_fh import GOELogFileHandle
from goe.util.misc_functions import standard_log_name
from goe.util.redis_tools import cache
from goe.orchestration import command_steps

if TYPE_CHECKING:
    # GOE
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


class OffloadMessagesException(Exception):
    pass


class OffloadMessagesForcedException(Exception):
    pass


QUIET, NORMAL, VERBOSE, VVERBOSE, SUPPRESS_STDOUT = list(range(-1, 4))
VERBOSENESS = set(("quiet", "normal", "verbose", "vverbose"))

COLORS = {
    "none": "\033[0m",
    "bright-grey": "\033[97m",
    "cyan": "\033[96m",
    "magenta": "\033[95m",
    "blue": "\033[94m",
    "yellow": "\033[93m",
    "green": "\033[92m",
    "red": "\033[91m",
    "grey": "\033[90m",
    "white": "\033[37m",
    "bright-cyan": "\033[36m",
    "bright-magenta": "\033[35m",
    "bright-blue": "\033[34m",
    "bright-yellow": "\033[33m",
    "bright-green": "\033[32m",
    "bright-red": "\033[31m",
    "black": "\033[30m",
    "bold": "\033[1m",
    "underline": "\033[4m",
}

FORCED_EXCEPTION_TEXT = "Forcing exception"

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


def serialize_object(obj) -> str:
    """
    Encodes json with the optimized ORJSON package

    orjson.dumps returns bytearray, so you can't pass it directly as json_serializer
    """
    return orjson.dumps(
        obj,
        option=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY,
    ).decode()


class OffloadMessages(object):
    """Class for logging, storing & reporting messages in Offload & Present"""

    def __init__(
        self,
        detail=NORMAL,
        log_fh=None,
        ansi=True,
        error_before_step=None,
        error_after_step=None,
        skip=None,
        execution_id=None,
        repo_client=None,
        command_type=None,
        cache_enabled: bool = False,
    ):
        """
        Client for Offload logging that also provides step instrumentation, repo logging and Console updates.

        log_fh: Allows init of the log using an existing file handle for integration with existing tools, e.g.: Offload.
                For stand-alone use the init_log method makes more sense.
        repo_client: Used to record execution offload step information in the repo. Only required by offload_step().
                     If we don't have a repo client at instantiation time then setup_offload_step() can be used.
        """
        logger.info("OffloadMessages setup")
        if command_type:
            assert (
                command_type in orchestration_constants.ALL_COMMAND_CODES
            ), f"Unknown command type: {command_type}"

        self.messages = {"notices": [], "warnings": [], "events": []}
        self.steps = {}
        self._log_fh = log_fh
        self._detail = detail
        self._ansi = ansi
        self._skip = skip or []
        self._error_before_step = error_before_step
        self._error_after_step = error_after_step
        self.execution_id = execution_id
        self.cache_enabled = cache_enabled
        self._repo_client = repo_client
        self._command_type = command_type
        self._redis_in_error = False
        self._stdout_in_error = False

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @staticmethod
    def from_options(
        opts,
        log_fh=None,
        execution_id=None,
        repo_client=None,
        command_type=None,
        cache_enabled: bool = False,
    ):
        assert hasattr(opts, "quiet")
        assert hasattr(opts, "verbose")
        assert hasattr(opts, "vverbose")
        assert hasattr(opts, "ansi")
        if opts.quiet:
            detail = QUIET
        elif opts.vverbose:
            detail = VVERBOSE
        elif opts.verbose:
            detail = VERBOSE
        elif opts.suppress_stdout:
            detail = SUPPRESS_STDOUT
        else:
            detail = NORMAL
        if cache_enabled:
            cache.get_client()
        return OffloadMessages(
            detail=detail,
            log_fh=log_fh,
            ansi=opts.ansi,
            error_before_step=getattr(opts, "error_before_step", None),
            error_after_step=getattr(opts, "error_after_step", None),
            skip=getattr(opts, "skip", None),
            execution_id=execution_id,
            repo_client=repo_client,
            command_type=command_type,
            cache_enabled=cache_enabled,
        )

    @staticmethod
    def from_options_dict(
        opts_dict,
        log_fh=None,
        execution_id=None,
        repo_client=None,
        command_type=None,
        cache_enabled: bool = False,
    ):
        if opts_dict.get("quiet"):
            detail = QUIET
        elif opts_dict.get("vverbose"):
            detail = VVERBOSE
        elif opts_dict.get("verbose"):
            detail = VERBOSE
        elif opts_dict.get("suppress_stdout"):
            detail = SUPPRESS_STDOUT
        else:
            detail = NORMAL
        if cache_enabled:
            cache.get_client()
        return OffloadMessages(
            detail=detail,
            log_fh=log_fh,
            ansi=opts_dict.get("ansi"),
            error_before_step=opts_dict.get("error_before_step"),
            error_after_step=opts_dict.get("error_after_step"),
            skip=opts_dict.get("skip"),
            execution_id=execution_id,
            repo_client=repo_client,
            command_type=command_type,
            cache_enabled=cache_enabled,
        )

    @staticmethod
    def ansi_wrap(txt, color_name, ansi):
        """Decorated as staticmethod to allow independent use from goe.py, hence
        not using self._ansi but passing in as parameter.
        """
        if ansi and color_name:
            if type(color_name) not in (list, tuple):
                color_name = [color_name]
            return (
                "".join([COLORS.get(cn, "") for cn in color_name])
                + str(txt)  # noqa: W503
                + COLORS.get("none")  # noqa: W503
            )
        else:
            return txt

    def init_log(self, log_dir, log_name):
        assert log_dir and log_name
        logger.debug("init_log(%s, %s)" % (log_dir, log_name))
        current_log_name = standard_log_name(log_name)
        log_path = os.path.join(log_dir, current_log_name)
        logger.debug("log_path: %s" % log_path)
        self._log_fh = GOELogFileHandle(log_path)

    def close_log(self):
        if self._log_fh:
            self._log_fh.close()

    def get_log_fh(self):
        return self._log_fh

    def get_log_fh_name(self):
        return self._log_fh.name

    ########################################################################################
    #   Use of log function vs info/detail/debug functions
    ########################################################################################
    #
    #   Using the log function
    #   ----------------------
    #
    #   1. log is the function that writes to log file and optionally to STDOUT. Its
    #      simple usage is:
    #
    #        messages.log('Some message')
    #
    #   2. log *always* writes to log file regardless of the verbosity level set for
    #      the session (NORMAL, VERBOSE [-v] or VVERBOSE [--vv]). This means that every
    #      call to messages.log you include in your programs will be logged to file.
    #
    #   3. log will also print to STDOUT unless the verbosity level set in the detail
    #      parameter is higher than the level set for the session execution. If you wish
    #      to prevent your log message appearing on STDOUT, there are two ways to achieve
    #      this:
    #
    #        a) original method - ensure the value passed to the detail parameter is
    #           higher than the logging level set for the session. For example, if your
    #           program is running at VERBOSE (-v) level and you only want the messages
    #           in the log file, this will prevent STDOUT printing:
    #
    #             messages.log('Message to go to log file only', detail=VVERBOSE)
    #
    #        b) new method - a dedicated constant for the detail parameter is now
    #           available with a clear usage:
    #
    #             messages.log('Message to go to log file only', detail=SUPPRESS_STDOUT)
    #
    #   Using the info, detail, debug functions
    #   ---------------------------------------
    #
    #   1. These will only call the log function if your program's verbosity level is
    #      high enough. In other words:
    #
    #        a) messages.info('Some message') will only log if verbosity is at least NORMAL
    #        b) messages.detail('Some message') will only log if verbosity is at least VERBOSE
    #        c) messages.debug('Some message') will only log if verbosity is at least VVERBOSE
    #
    #   2. printing to STDOUT is controlled in the same way as documented above for the log
    #      function, e.g.:
    #
    #        messages.debug('VVERBOSE message in log file only when using --vv', detail=SUPPRESS_STDOUT)
    #
    #   Do I use the log function or the info/detail/debug set of functions in my program?
    #   ----------------------------------------------------------------------------------
    #
    #   1. Most GOE programs use log because we want all information in the log files at
    #      all times, regardless of user's command-line verbosity setting. Something that a
    #      customer might not be able to re-run (such as an offload) but needs support with
    #      clearly has to guarantee sufficient logging at all times.
    #
    #   2. Some GOE programs are trivial to re-run and do not require debug-amounts of
    #      logging for everyday use. If an issue occurs and the program is trivial to re-run
    #      with a --vv option to create debug logging, then the program can use the
    #      info/detail/debug functions to express the developer's intention for when the
    #      information is logged to file (i.e. under which verbosity conditions). A good
    #      example is the Offload Status Report. This is a query-only tool that can
    #      preprocess a large amount of query data. In debug mode it will be useful to
    #      see the intermediate data, but we wouldn't want all of this detailed data dumped
    #      to log file every time. Therefore, messages.debug() calls in the Offload Status
    #      Report program will only write to logfile (and STDOUT unless suppressed) when
    #      the report is executed with the --vv/VVERBOSE logging level.
    #
    ########################################################################################

    def log(self, line, detail=NORMAL, ansi_code=None):
        def fh_log(line):
            self._log_fh.write((line or "") + "\n")
            self._log_fh.flush()

        def stdout_log(line):
            if not self._stdout_in_error:
                try:
                    sys.stdout.write((line or "") + "\n")
                    sys.stdout.flush()
                except OSError as exc:
                    # Writing to screen is non-essential, if we lost stdout we are still logging to file.
                    fh_log("Disabling STDOUT logging due to: {}".format(str(exc)))
                    self._stdout_in_error = True

        if self._log_fh:
            fh_log(line)
        if self._detail == QUIET:
            stdout_log(".")
        elif (detail is None or detail <= self._detail) and (
            self._detail != SUPPRESS_STDOUT
        ):
            line = self.ansi_wrap(line, ansi_code, self._ansi)
            stdout_log(line)
        if self.cache_enabled and not self._redis_in_error:
            try:
                cache.rpush(
                    f"goe:run:{self.execution_id}",
                    serialize_object(
                        {
                            "message": line,
                        }
                    ),
                    ttl=timedelta(hours=48),
                )
            except Exception as exc:
                fh_log("Disabling Redis integration due to: {}".format(str(exc)))
                self._redis_in_error = True

    def info(self, line, detail=NORMAL, ansi_code=None):
        if self._detail >= NORMAL:
            self.log(line, detail, ansi_code)

    def detail(self, line, detail=NORMAL, ansi_code=None):
        if self._detail >= VERBOSE:
            self.log(line, detail, ansi_code)

    def debug(self, line, detail=NORMAL, ansi_code=None):
        if self._detail >= VVERBOSE:
            self.log(line, detail, ansi_code)

    def notice(self, msg, detail=VERBOSE):
        if msg not in self.messages["notices"]:
            self.messages["notices"].append(msg)
        self.log(msg, detail)

    def warning(self, msg, detail=NORMAL, ansi_code=None):
        if msg not in self.messages["warnings"]:
            self.messages["warnings"].append(msg)
        self.log("WARNING:" + msg, detail, ansi_code)

    def event(self, token):
        """Add a token to a list of events that can be interrogated later in an orchestration process.
        For example an Offload may carry out a specific action and record it as an event.
        In our test assertions we may look for the event to confirm the action occurred.
        We would use this when logging information and then looking for strings in logs is not appropriate.
        """
        assert token
        self.messages["events"].append(token)

    def debug_enabled(self):
        return bool(self._detail >= VVERBOSE)

    def log_timestamp(self, ansi_code="grey", execute=True, detail=VERBOSE):
        ts = None
        if execute:
            ts = datetime.now()
            ts = ts.replace(microsecond=0)
            self.log(ts.strftime("%c"), detail=detail, ansi_code=ansi_code)
        return ts

    def offload_step(  # noqa: C901
        self,
        step_constant: str,
        step_fn: Callable,
        execute=True,
        optional=False,
        command_type: Optional[str] = None,
        mandatory_step=False,
        record_step_delta=True,
    ) -> Any:
        """
        Produce nicely formatted offload step output with elapsed time and return the value of step_fn().

        step_constant: an ID identifying the step in the repo.
        step_fn: The function to call for the step. Any return value is passed back though this method.
                 No arugments are passed in, the callable should capture any arguments itself.
        command_type: The parent command of the step. This is optional because 99% of steps are for the same
                      same command. We cater for this so a recursive Present can log its step accordingly.
                      When left as None the value will default to self._command_type.
        mandatory_step: Initially seems to duplicate optional but optional (badly named) actually means
                        don't worry if the step fails. mandatory_step means this step cannot be skipped.
        record_step_delta: Some steps may have steps within them, in this case the outer step should not record
                           its elapsed time for the step summary by passing this flag as False.
        """

        def log_timedelta(start_time, ansi_code="grey"):
            if execute:
                ts2 = datetime.now()
                ts2 = ts2.replace(microsecond=0)
                self.log(
                    "Step time: %s" % (ts2 - start_time),
                    detail=VERBOSE,
                    ansi_code=ansi_code,
                )
                return ts2 - start_time

        def step_repo_logging(check_command_type):
            # TODO In Console phase 1 we will not log the details of the commands below.
            return bool(
                check_command_type
                not in (
                    orchestration_constants.COMMAND_DIAGNOSE,
                    orchestration_constants.COMMAND_OSR,
                    orchestration_constants.COMMAND_SCHEMA_SYNC,
                    orchestration_constants.COMMAND_TEST,
                )
            )

        assert step_constant in STEP_TITLES, f"Unknown step constant: {step_constant}"
        if command_type:
            assert (
                command_type in orchestration_constants.ALL_COMMAND_CODES
            ), f"Unknown command type: {command_type}"

        parent_command_type = command_type or self._command_type
        if step_repo_logging(parent_command_type):
            assert (
                self._repo_client
            ), f"Repository client must be initialized for command type: {parent_command_type}"

        title = step_title(step_constant)

        self.log("")
        self.log(title, ansi_code="underline")
        ts = self.log_timestamp(execute=execute)

        step_id = step_title_to_step_id(title)
        if self._skip and step_id in self._skip:
            if mandatory_step:
                raise OffloadMessagesException(
                    'Step "%s" is mandatory and cannot be skipped' % title
                )
            self.log("skipped")
            return None

        if step_repo_logging(parent_command_type):
            csid = self._repo_client.start_command_step(
                self.execution_id, parent_command_type, step_constant
            )
        else:
            csid = None

        if step_id == self._error_before_step:
            raise OffloadMessagesForcedException(
                "%s before step: %s" % (FORCED_EXCEPTION_TEXT, title)
            )

        try:
            step_results = step_fn()
            td = log_timedelta(ts)

            if execute:
                self.log("Done", ansi_code="green")
                if record_step_delta:
                    self.step_delta(title, td)
            else:
                self.step_no_delta(title)

            if step_repo_logging(parent_command_type):
                self._repo_client.end_command_step(
                    csid, orchestration_constants.COMMAND_SUCCESS
                )

            return step_results
        except Exception as exc:
            log_timedelta(ts)
            if step_repo_logging(parent_command_type):
                error_context = {
                    command_steps.CTX_ERROR_MESSAGE: str(exc),
                    command_steps.CTX_EXCEPTION_STACK: traceback.format_exc(),
                }
                self._repo_client.end_command_step(
                    csid,
                    orchestration_constants.COMMAND_ERROR,
                    step_details=error_context,
                )
            if optional:
                self.log(traceback.format_exc(), detail=VVERBOSE)
                self.warning('Error in "%s": %s' % (title, str(exc)), ansi_code="red")
                self.log("Optional step, continuing", ansi_code="green")
            else:
                raise
        finally:
            if step_id == self._error_after_step:
                raise OffloadMessagesForcedException(
                    "%s after step: %s" % (FORCED_EXCEPTION_TEXT, title)
                )

    def set_execution_id(self, execution_id):
        self.execution_id = execution_id

    def setup_offload_step(
        self,
        skip=None,
        error_before_step=None,
        error_after_step=None,
        repo_client: "Optional[OrchestrationRepoClientInterface]" = None,
        command_type: Optional[str] = None,
    ):
        """Cache some operational attributes that can be used by offload_step().
        error_before_step and error_after_step are for test purposes, they cache a step name before or after
        which we should throw an exception. The step name is case insensitive with spaces replaced by underscores.
        """
        if skip is not None:
            self._skip = skip or []
        if error_before_step is not None:
            self._error_before_step = step_title_to_step_id(error_before_step)
        if error_after_step is not None:
            self._error_after_step = step_title_to_step_id(error_after_step)
        if repo_client is not None:
            self._repo_client = repo_client
        if command_type is not None:
            self._command_type = command_type

    def get_events(self):
        return self.messages["events"]

    def get_messages(self):
        return self.messages["warnings"] + self.messages["notices"]

    def get_notices(self):
        return self.messages["notices"]

    def get_warnings(self):
        return self.messages["warnings"]

    def log_messages(self):
        logger.info("log_messages()")
        if self.get_warnings():
            self.log("Warnings:", ansi_code="red")
            for msg in self.get_warnings():
                self.log(msg)
        if self.get_notices():
            self.log("Notices:", ansi_code="green")
            for msg in self.get_notices():
                self.log(msg)

    def step_delta(self, step, time_delta):
        if time_delta is None:
            return
        if step in self.steps:
            self.steps[step]["seconds"] = (
                self.steps[step]["seconds"] + time_delta.total_seconds()
            )
            self.steps[step]["count"] += 1
        else:
            self.steps[step] = {"seconds": time_delta.total_seconds(), "count": 1}

    def step_no_delta(self, step):
        """Record a step without any time delta, for non-execture mode."""
        if step in self.steps:
            self.steps[step]["count"] += 1
        else:
            self.steps[step] = {"seconds": 0, "count": 1}

    def log_step_deltas(self, topn=10, detail=VVERBOSE):
        logger.info("log_step_deltas()")
        if not self.steps:
            return
        logged_seconds = sum(_["seconds"] for _ in list(self.steps.values()))
        step_width = max([len(step) for step in self.steps])
        step_format = "{0: <" + str(step_width) + "} {1: >9} {2: >7}"
        self.log(step_format.format("Step", "Seconds", "Percent"), detail=detail)
        for i, (step_key, step_dict) in enumerate(
            sorted(
                iter(self.steps.items()), key=lambda x: x[1]["seconds"], reverse=True
            )
        ):
            if i >= topn:
                break
            ratio = (step_dict["seconds"] / logged_seconds) if logged_seconds > 0 else 0
            self.log(
                step_format.format(
                    step_key,
                    "{:9.0f}".format(step_dict["seconds"]),
                    "{:5.1%}".format(ratio),
                ),
                detail=detail,
            )
        if logged_seconds:
            self.log(
                "Total time logged: %s" % str(timedelta(seconds=logged_seconds)),
                detail=VVERBOSE,
            )

    @property
    def verbosity(self):
        return self._detail

    def get_execution_id(self):
        return self.execution_id


class OffloadMessagesMixin:
    """Helper class that injects goe logging routines into child classes

    Usage:
        1. Make OffloadMessagesMixin one of the 'parents'
        2. Call OffloadMessagesMixin constructor with (messages, local_logger) object
        3. Enjoy: self.warn(), self.notice(), self.log_normal() etc

        i.e.:

        # Define
        logger = logging.getLogger()
        ...

        class A(OffloadMessagesMixin, object):
            def __init__(self, messages=None):
                ...
                super(A, self).__init__(messages, logger)

        # Then, use
        a = A()
        a.warn("Hello maties, warning", ansi_code='blue')
        a.notice("Hello maties, notice")
    """

    def __init__(self, messages=None, ext_logger=None):
        def set_func(marker, messages_func, logger_func):
            if messages and isinstance(messages, OffloadMessages):
                logger.debug("Setting: %s call to use 'messages'" % marker)
                return messages_func()
            elif logger and isinstance(ext_logger, logging.Logger):
                logger.debug("Setting: %s call to use 'logger'" % marker)
                return logger_func()
            else:
                logger.debug("Setting: %s call to EMPTY" % marker)
                return lambda x: None

        self.warn = set_func(
            "warn",
            lambda: partial(messages.warning, ansi_code="red"),
            lambda: ext_logger.warn,
        )
        self.notice = set_func(
            "notice", lambda: messages.notice, lambda: ext_logger.info
        )
        self.log = set_func("log", lambda: messages.log, lambda: ext_logger.info)
        self.log_normal = set_func(
            "log_normal",
            lambda: partial(messages.log, detail=NORMAL),
            lambda: ext_logger.info,
        )
        self.log_verbose = set_func(
            "log_verbose",
            lambda: partial(messages.log, detail=VERBOSE),
            lambda: ext_logger.info,
        )
        self.log_vverbose = set_func(
            "log_vverbose",
            lambda: partial(messages.log, detail=VVERBOSE),
            lambda: ext_logger.info,
        )


def step_title_to_step_id(step_title):
    """Helper function to share logic between this module and goe.py"""
    if step_title:
        return step_title.replace(" ", "_").lower()
    else:
        return None
