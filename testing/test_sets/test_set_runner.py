#! /usr/bin/env python3
""" TestSetRunner: co-ordinator for running integration tests.
    LICENSE_TEXT
"""

import logging
import re

from gluentlib.offload.offload_messages import VERBOSE, VVERBOSE
from gluentlib.util.misc_functions import dict_to_namespace

from test_sets.base_test import BaseTest
from test_sets.offload_transport.offload_transport_tests import run_offload_transport_tests
from test_sets.python_unit.unit_test_runner import run_python_unit_tests
from test_sets.type_mapping.type_mapping_tests import run_type_mapping_orchestration_tests
from test_sets.sql_offload.sql_offload_tests import run_sql_offload_tests
from test_sets.stories.story_runner import run_story_tests

from testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory
from testlib.test_framework.factory.frontend_testing_api_factory import frontend_testing_api_factory
from testlib.test_framework.test_functions import test_passes_filter
from testlib.test_framework import test_constants


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# TestSetRunner
###########################################################################

class TestSetRunner:
    def __init__(self, sh_test_user, sh_test_password, hybrid_password, messages, config,
                 filter=None, dry_run=False, teamcity=False, known_failure_blacklist=None, ansi=True,
                 run_blacklist_only=False, test_pq_degree=2, story=None):
        self._messages = messages
        self._config = config
        self._dry_run = dry_run
        self._sh_test_user = sh_test_user
        self._sh_test_password = sh_test_password
        self._hybrid_password = hybrid_password

        # Compiling the re now can save a job later, but mutithreaded test sets will have to do this themselves,
        # this is why we also store the original filter in state.
        self._test_name_re = re.compile(filter, re.I)
        self._filter = filter

        self._backend_api = backend_testing_api_factory(config.target, config, messages, dry_run=self._dry_run)
        self._frontend_api = frontend_testing_api_factory(config.db_type, config, messages, dry_run=self._dry_run)
        self._sh_test_api = None
        # Kept public to comply with test_teamcity_ and other legacy test functions
        self.ansi = ansi
        self.known_failure_blacklist = known_failure_blacklist or []
        self.run_blacklist_only = run_blacklist_only
        self.story = story
        self.teamcity = teamcity
        self.test_pq_degree = test_pq_degree

        self._log(f'Running tests for: {sh_test_user}')
        self._log(f'Frontend connection: {config.db_type} {self._frontend_api.frontend_version()}')
        self._log('Backend connection: {} {}'.format(config.target, self._backend_api.backend_version() or ''))
        if self._dry_run:
            self._log('*** Non-execute mode ***')

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_sh_test_api(self):
        if self._sh_test_api is None:
            self._sh_test_api = self._frontend_api.create_new_connection(self._sh_test_user, self._sh_test_password)
        return self._sh_test_api

    def _log(self, msg, detail=None, ansi_code=None):
        """ Write to offload log file """
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _passes_filter(self, test_name):
        return bool(self._test_name_re and not self._test_name_re.search(test_name))

    def _should_run_test(self, test_name):
        """ Wrapper over function shared by 'test' and 'test_runner' """
        return test_passes_filter(test_name, self._test_name_re, self, self.known_failure_blacklist)

    def _to_legacy_options(self):
        """ For multithreaded test sets we cannot pass this client around. We need to capture user options
            and pass those through instead.
        """
        local_options = dict_to_namespace({'ansi': self.ansi,
                                           'execute': not(self._dry_run),
                                           'filter': self._filter,
                                           'known_failure_blacklist': self.known_failure_blacklist,
                                           'run_blacklist_only': self.run_blacklist_only,
                                           'teamcity': self.teamcity,
                                           'test_pq_degree': self.test_pq_degree,
                                           'test_user': self._sh_test_user,
                                           'test_pass': self._sh_test_password,
                                           'test_hybrid_pass': self._hybrid_password,
                                           'story': self.story,
                                           'quiet': self._config.quiet,
                                           'suppress_stdout': self._config.suppress_stdout,
                                           'verbose': self._config.verbose,
                                           'vverbose': self._config.vverbose})
        return local_options

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def run_set(self, test_set):
        test_options = self._to_legacy_options()
        if test_set == test_constants.SET_PYTHON_UNIT:
            run_python_unit_tests(test_options, self._config, self._messages, self._should_run_test)
        elif test_set == test_constants.SET_OFFLOAD_TRANSPORT:
            run_offload_transport_tests(test_options, self._sh_test_user, self._config, BaseTest)
        elif test_set == test_constants.SET_SQL_OFFLOAD:
            run_sql_offload_tests(test_options, self._sh_test_user, self._config, BaseTest)
        elif test_set in (test_constants.SET_STORIES,
                          test_constants.SET_STORIES_CONTINUOUS,
                          test_constants.SET_STORIES_INTEGRATION):
            run_story_tests(test_options, self._config, teamcity_name=test_set)
        elif test_set == test_constants.SET_TYPE_MAPPING:
            run_type_mapping_orchestration_tests(test_options, self._sh_test_user, self._frontend_api, self._backend_api,
                                                 self._get_sh_test_api(), self._config,
                                                 self._messages, self._should_run_test)
        else:
            # Only a warning as TeamCity config may contain test sets for versions other than the one we are testing.
            self._messages.warning(f'Unknown test set: {test_set}')

