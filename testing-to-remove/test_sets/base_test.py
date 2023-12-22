""" A base test class used in 'test' and 'test_runner'.
    Provides the basic assertion methods. This class is extended by individual test sets.
"""

from io import StringIO
from contextlib import redirect_stdout
from datetime import datetime
import os
import sys
import traceback

from goe.gluent import ansi

from testlib.test_framework import test_constants
from testlib.test_framework.test_functions import log, test_teamcity_endtest, test_teamcity_endtest_pq,\
    test_teamcity_failtest, test_teamcity_failtest_pq, test_teamcity_starttest, test_teamcity_starttest_pq,\
    test_teamcity_stdout, test_teamcity_stdout_pq
from testlib.test_framework.test_results import END_OF_RC_MARKER
from test_sets.benchmarks import Benchmarks


class TestFailure(Exception):
    pass

global test_lines
test_lines = 0


###############################################################################
# CONSTANTS
###############################################################################

# Constant defining pagination of test benchmark output when run manually
TEST_HEADER_PAGESIZE = 50


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################

def reset_tty():
    if sys.stdout.isatty():
        os.system('stty sane')


###############################################################################
# BaseTest
###############################################################################

class BaseTest:
    def __init__(self, options, name, test_callable):
        self._options = options
        self.name = name
        self.test_callable = test_callable

        self.tc_name = "%s.%s" % (test_constants.TEST_NAMESPACE, name)  # TeamCity test: namespace.name
        self.response_value = self.response_key = None

        self._stdout = StringIO()
        self._benchmarks = Benchmarks()

    def __call__(self, quiet=False, parent_flow_id=None, capture_stdout=False):
        """ Call self.test_callable and deal with any test failures """
        result, result_message = None, ""
        fn_result = None

        try:
            fn_result = self._run_callable(parent_flow_id, capture_stdout)
        except TestFailure as exc:
            result, result_message = self._handle_test_failure(exc, parent_flow_id)
        except Exception as exc:
            result, result_message = self._handle_test_exception(exc, parent_flow_id)
        finally:
            reset_tty()

        if not result and not quiet:
            result = 'Pass'
            if self.response_key:
                bench_result_message = self._bench_result_message()
                if bench_result_message:
                    result_message += bench_result_message

        if self._options.teamcity:
            if capture_stdout:
                self._emit_teamcity_stdout(parent_flow_id)
        elif not quiet or (quiet and result):
            global test_lines
            self._bench_result_header(test_lines)
            self._emit_non_teamcity_test_result(result, result_message)
            test_lines += 1

        if parent_flow_id:
            test_teamcity_endtest_pq(self._options, self.tc_name, parent_flow_id)
        else:
            test_teamcity_endtest(self._options, self.tc_name)

        return fn_result

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _bench_result_header(self, tests_recorded):
        if tests_recorded % TEST_HEADER_PAGESIZE == 0:
            for check_ids_vals in zip(*self._benchmarks.get_checkpoints()):
                if isinstance(check_ids_vals[-1], datetime):
                    check_ids_vals = list(map(self.fmt_checkpoint_dt, check_ids_vals))
                fmt = '{:>76}'
                fmt += (len(check_ids_vals) - 1) * '{:>16}'
                log(fmt.format(*check_ids_vals))

    def _bench_result_message(self):
        self._benchmarks.add_result(self.response_key, self.response_value)
        bench_result = self._benchmarks.get_previous_result(self.response_key)

        result_message = None
        for check in self._benchmarks.get_checkpoints():
            check_value = self._benchmarks.get_result(check, self.response_key)
            if check == self._benchmarks.get_benchmark_id():
                result_message = '{: >16.4}'.format(str(check_value))
            else:
                result_message = '{: >11.4}'.format(str(check_value))

                pc_increase = 0
                if check_value and check_value != 'None' and bench_result:
                    pc_increase = int(100 * (check_value - bench_result) / bench_result)
                result_message += '{: >+4}%'.format(pc_increase)
        return result_message

    def _emit_non_teamcity_test_result(self, result, result_message):
        # Add 'end error' marker to error message if result is 'fail' or 'error'
        if result != 'Pass':
            result_message += '\n' + END_OF_RC_MARKER
        log('%s %s %s%s' %
            (self.name, ansi((54 - len(self.name)) * '.', 'grey'), result, result_message))

    def _emit_teamcity_stdout(self, parent_flow_id):
        output = self._stdout.getvalue()
        if output:
            if parent_flow_id:
                test_teamcity_stdout_pq(self._options, self.tc_name, output)
            else:
                test_teamcity_stdout(self._options, self.tc_name, output)

    def _handle_test_exception(self, exc, parent_flow_id):
        result = 'Test Exception %s:' % type(exc)
        result_message = '\n%s\n%s' % (exc, traceback.format_exc())
        if parent_flow_id:
            test_teamcity_failtest_pq(self._options, self.tc_name, result_message)
        else:
            test_teamcity_failtest(self._options, self.tc_name, result_message)
        return result, result_message

    def _handle_test_failure(self, exc, parent_flow_id):
        result = 'Fail:'
        result_message = "\n%s" % exc
        if parent_flow_id:
            test_teamcity_failtest_pq(self._options, self.tc_name, result_message)
        else:
            test_teamcity_failtest(self._options, self.tc_name, result_message)
        return result, result_message

    def _run_callable(self, parent_flow_id, capture_stdout):
        if parent_flow_id:
            test_teamcity_starttest_pq(self._options, self.tc_name, parent_flow_id)
        else:
            test_teamcity_starttest(self._options, self.tc_name)

        if capture_stdout:
            with redirect_stdout(self._stdout):
                fn_result = self.test_callable(self)
        else:
            fn_result = self.test_callable(self)
        return fn_result

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def assertIsNotNone(self, v, desc):
        if v is None:
            raise TestFailure('%s\nFailed, values is None\n' % desc)

    def assertTrue(self, v, desc):
        if not v:
            raise TestFailure('%s\nFailed, value: %s\n' % (desc, v))

    def assertEqual(self, a, b, desc):
        if a != b:
            raise TestFailure('%s\n%r\n!=\n%r\n' % (desc, a, b))

    def assertGreaterThan(self, a, b, desc):
        if a <= b:
            raise TestFailure('%s\n%s\n<=\n%s\n' % (desc, a, b))

    def fail(self, msg):
        raise TestFailure(msg)

    def response_time(self, key, value):
        self.response_key = key
        self.response_value = value

    @staticmethod
    def fmt_checkpoint_dt(dt):
        return dt.strftime('%d/%m %H:%M') if dt else None
