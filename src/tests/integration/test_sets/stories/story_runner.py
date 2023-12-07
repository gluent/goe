from concurrent.futures import ProcessPoolExecutor
import copy
import inspect
import multiprocessing
import pprint
import re
import threading
import time
import traceback
from typing import Callable, Optional, TYPE_CHECKING

from test_sets.base_test import BaseTest

from goe.gluent import get_log_fh, init_log, normalise_owner_table_options,\
    normal, verbose, vverbose
from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.offload_constants import BACKEND_DISTRO_EMR, BACKEND_DISTRO_MAPR, DBTYPE_MSSQL, DBTYPE_TERADATA
from goe.offload.offload_functions import convert_backend_identifier_case, data_db_name, load_db_name
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_validation import DEFAULT_SELECT_COLS, DEFAULT_AGGS
from goe.orchestration import orchestration_constants
from goe.orchestration.execution_id import ExecutionId
from goe.orchestration.orchestration_runner import OrchestrationRunner
from goe.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory
from goe.schema_sync.schema_sync_constants import SCHEMA_SYNC_OP_NAME
from goe.scripts import agg_validate
from schema_sync import normalise_schema_sync_options
from testlib.test_framework import test_constants
from testlib.test_framework.backend_testing_api import subproc_cmd
from testlib.test_framework.test_functions import get_backend_testing_api, get_frontend_testing_api,\
    get_orchestration_options_object, log, response_time_bench, \
    test_passes_filter, \
    test_teamcity_endtestsuite, test_teamcity_endtestsuite_pq, test_teamcity_failtest, \
    test_teamcity_starttest, test_teamcity_starttestsuite_pq, \
    test_teamcity_starttestsuite, test_teamcity_stdout_pq, teamcity_escape, test_teamcity_stdout, to_hybrid_schema
from test_sets.stories.story_globals import STORY_SET_ALL,\
    STORY_SET_CONTINUOUS, STORY_SET_INTEGRATION,\
    STORY_SET_AGG_VALIDATE, STORY_SET_BACKEND_STATS,\
    STORY_SET_CLI_API, STORY_SET_CLI_API_NOSSH, \
    STORY_SET_DATA_GOV, STORY_SET_HYBRID_VIEW_SERVICE,\
    STORY_SET_IDENTIFIERS,\
    STORY_SET_LISTENER_ORCHESTRATION, \
    STORY_SET_MSSQL, STORY_SET_NETEZZA, STORY_SET_OFFLOAD_BACKEND_PART, STORY_SET_OFFLOAD_DATA_TYPE_CONTROLS,\
    STORY_SET_OFFLOAD_ESOTERIC, STORY_SET_OFFLOAD_FULL, STORY_SET_OFFLOAD_HASH_BUCKET,\
    STORY_SET_OFFLOAD_USE_CASE, STORY_SET_OFFLOAD_SUBPART,\
    STORY_SET_OFFLOAD_SORTING, STORY_SET_OFFLOAD_LIST_RPA, \
    STORY_SET_OFFLOAD_LPA, STORY_SET_OFFLOAD_LPA_FULL, STORY_SET_OFFLOAD_PART_FN,\
    STORY_SET_OFFLOAD_PBO, STORY_SET_OFFLOAD_PBO_INTRA, STORY_SET_OFFLOAD_PBO_LATE,\
    STORY_SET_OFFLOAD_RPA, STORY_SET_OFFLOAD_TRANSPORT, STORY_SET_ORCHESTRATION_LOCKS, STORY_SET_ORCHESTRATION_STEP,\
    STORY_SET_SCHEMA_SYNC, STORY_SET_TRANSFORMATIONS, \
    STORY_TYPE_AGG_VALIDATE, STORY_TYPE_LINK_ID,\
    STORY_TYPE_OFFLOAD,\
    STORY_TYPE_SETUP, STORY_TYPE_SCHEMA_SYNC,\
    STORY_TYPE_SHELL_COMMAND, STORY_TYPE_PYTHON_FN, STORY_SETUP_TYPE_FRONTEND,\
    STORY_SETUP_TYPE_MSSQL, STORY_SETUP_TYPE_ORACLE, STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_PYTHON,\
    STORY_SETUP_TYPE_NETEZZA
from test_sets.stories.test_stories.agg_validate import agg_validate_story_tests
from test_sets.stories.test_stories.data_governance import data_governance_story_tests
from test_sets.stories.test_stories.hybrid_view_service import hybrid_view_service_story_tests
from test_sets.stories.test_stories.cli_api import cli_api_story_tests
from test_sets.stories.test_stories.cli_api_nossh import cli_api_nossh_story_tests
from test_sets.stories.test_stories.backend_stats import backend_stats_story_tests
from test_sets.stories.test_stories.identifiers import identifier_story_tests
from test_sets.stories.test_stories.listener_orchestration import listener_orchestration_story_tests
from test_sets.stories.test_stories.offload_backend_part import offload_backend_part_story_tests
from test_sets.stories.test_stories.offload_data_type_controls import offload_data_type_controls_story_tests
from test_sets.stories.test_stories.offload_esoteric import offload_esoteric_story_tests
from test_sets.stories.test_stories.offload_full import offload_full_story_tests
from test_sets.stories.test_stories.offload_hash_bucket import offload_hash_bucket_story_tests
from test_sets.stories.test_stories.offload_list_rpa import offload_list_as_range_ipa_story_tests
from test_sets.stories.test_stories.offload_lpa import offload_list_partition_append_story_tests,\
    offload_list_partition_append_full_story_tests
from test_sets.stories.test_stories.offload_part_fn import offload_part_fn_story_tests
from test_sets.stories.test_stories.offload_pbo import offload_pbo_story_tests, offload_pbo_late_arriving_story_tests,\
    offload_pbo_intra_day_story_tests
from test_sets.stories.test_stories.offload_rpa import offload_range_ipa_story_tests
from test_sets.stories.test_stories.offload_sorting import offload_sorting_story_tests
from test_sets.stories.test_stories.offload_subpart import offload_subpart_story_tests
from test_sets.stories.test_stories.offload_transport import offload_transport_story_tests
from test_sets.stories.test_stories.offload_use_case import offload_use_case_story_tests
from test_sets.stories.test_stories.orchestration_locks import orchestration_locks_story_tests
from test_sets.stories.test_stories.orchestration_step_control import orchestration_step_story_tests
from test_sets.stories.test_stories.mssql import mssql_story_tests
from test_sets.stories.test_stories.netezza import netezza_story_tests
from test_sets.stories.test_stories.schema_sync import schema_sync_story_tests
from test_sets.stories.test_stories.transformations import transformations_story_tests

if TYPE_CHECKING:
    from goe.persistence.orchestration_repo_client import OrchestrationRepoClientInterface


class StoryRunnerException(Exception):
    pass


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

# Only these stories are expected to work for Teradata MVP
TERADATA_MVP_STORIES = [
    STORY_SET_AGG_VALIDATE,
    STORY_SET_CLI_API,
    STORY_SET_OFFLOAD_HASH_BUCKET,
    STORY_SET_IDENTIFIERS,
    STORY_SET_OFFLOAD_BACKEND_PART,
    STORY_SET_OFFLOAD_DATA_TYPE_CONTROLS,
    STORY_SET_OFFLOAD_ESOTERIC,
    STORY_SET_OFFLOAD_FULL,
    STORY_SET_OFFLOAD_PART_FN,
    STORY_SET_OFFLOAD_PBO,
    # STORY_SET_OFFLOAD_PBO_INTRA,
    STORY_SET_OFFLOAD_PBO_LATE,
    STORY_SET_OFFLOAD_RPA,
    STORY_SET_OFFLOAD_SORTING,
    STORY_SET_OFFLOAD_TRANSPORT,
    STORY_SET_OFFLOAD_USE_CASE,
    STORY_SET_ORCHESTRATION_LOCKS,
    STORY_SET_ORCHESTRATION_STEP,
]


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

def get_story_tests(schema, messages: OffloadMessages, orchestration_config: "OrchestrationConfig",
                    story_list: list=[], list_only: Optional[bool]=None, options=None,
                    repo_client: "Optional[OrchestrationRepoClientInterface]"=None,
                    test_cursor_factory: Optional[Callable]=None,
                    hybrid_cursor_factory: Optional[Callable]=None):
    """ story_list is an option list of keys from the story dict, used to differentiate between full/lite runs
        Returns a dict of dictionaries which describe user stories, the individual step dict structure is:
          id: A unique id to allow filters
          type: One of STORY_TYPE_ globals from story_globals.py
          title: Brief description, displayed before the test to assist eyeballs
          narrative: More indepth description, including intention
          prereq: A variable or function that must evaluate to True in order for the test to be executed, useful for tests
                  appropriate to certain backend SQL versions
          setup: Optional lists of Oracle, Hybrid schema and/or Hive statements to be included in 'test --setup', e.g.
                 {STORY_SETUP_TYPE_ORACLE: ['sql1', 'sql2'], STORY_SETUP_TYPE_HYBRID: ['sql1']}
          assertion_pairs: A list of function pairs that should return equal values after a test. e.g.
                           fn(count the TIMESTAMP columns in Oracle table), fn(2)
          options: What to offload and how
          config_override: If you need to override any options settings, do them in this dict. Note that 'execute' and
                           'skip' are still (Apr16) a disease in gluent.py so need to be set in original options object
          expected_status: The expected return value from a test, defaults to True
          expected_exception_string: Some text to identify an expected exception

        TODO Work through user guide ensuring all actions covered
    """

    if not story_list:
        story_list = STORY_SET_ALL
    elif type(story_list) is not list:
        story_list = [story_list]

    if orchestration_config.db_type == DBTYPE_TERADATA:
        stories_to_skip = set(story_list) - set(TERADATA_MVP_STORIES)
        if stories_to_skip:
            messages.log('Skipping stories due to Teradata MVP: {}'.format(str(stories_to_skip)))
            story_list = list(set(story_list).intersection(set(TERADATA_MVP_STORIES)))

    data_db_schema = data_db_name(schema, orchestration_config)
    load_db_schema = load_db_name(schema, orchestration_config)
    schema = schema.upper()
    hybrid_schema = to_hybrid_schema(schema)
    data_db_schema, load_db_schema = convert_backend_identifier_case(orchestration_config, data_db_schema,
                                                                     load_db_schema)

    if list_only:
        backend_api = frontend_api = repo_client = None
        test_cursor_factory = lambda: None
        hybrid_cursor_factory = lambda: None
        # Before we had list_only we used empty options as the flag, so we blank it out here to pick up stragglers.
        orchestration_config = None
    else:
        backend_api = get_backend_testing_api(options, no_caching=True)
        frontend_api = get_frontend_testing_api(options, trace_action='FrontendTestingApi(stories)')
        repo_client = repo_client or orchestration_repo_client_factory(orchestration_config, messages)
        if hybrid_cursor_factory is None or not frontend_api.hybrid_schema_supported():
            hybrid_cursor_factory = lambda: None

    story_gen_fns = {
        STORY_SET_AGG_VALIDATE: lambda: agg_validate_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_BACKEND_STATS: lambda: backend_stats_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            list_only, repo_client
        ),
        STORY_SET_CLI_API: lambda: cli_api_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            list_only, repo_client
        ),
        STORY_SET_CLI_API_NOSSH: lambda: cli_api_nossh_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            list_only, repo_client
        ),
        STORY_SET_DATA_GOV: lambda: data_governance_story_tests(
            schema, hybrid_schema, data_db_schema, load_db_schema, orchestration_config, backend_api,
            frontend_api, list_only
        ),
        STORY_SET_OFFLOAD_HASH_BUCKET: lambda: offload_hash_bucket_story_tests(
            schema, hybrid_schema, data_db_schema, load_db_schema, orchestration_config, backend_api,
            frontend_api, repo_client,
        ),
        STORY_SET_HYBRID_VIEW_SERVICE: lambda: hybrid_view_service_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            test_cursor_factory()
        ),
        STORY_SET_IDENTIFIERS: lambda: identifier_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_LISTENER_ORCHESTRATION: lambda: listener_orchestration_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client,
        ),
        STORY_SET_OFFLOAD_DATA_TYPE_CONTROLS: lambda: offload_data_type_controls_story_tests(
            schema, hybrid_schema, data_db_schema, load_db_schema, orchestration_config, backend_api, frontend_api
        ),
        STORY_SET_OFFLOAD_BACKEND_PART: lambda: offload_backend_part_story_tests(
            schema, hybrid_schema, data_db_schema, load_db_schema, orchestration_config,
            backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_ESOTERIC: lambda: offload_esoteric_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_FULL: lambda: offload_full_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_USE_CASE: lambda: offload_use_case_story_tests(
            schema, hybrid_schema, data_db_schema, load_db_schema, orchestration_config,
            backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_SUBPART: lambda: offload_subpart_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_PART_FN: lambda: offload_part_fn_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            hybrid_cursor_factory(), repo_client
        ),
        STORY_SET_OFFLOAD_PBO: lambda: offload_pbo_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            hybrid_cursor_factory(), repo_client
        ),
        STORY_SET_OFFLOAD_PBO_INTRA: lambda: offload_pbo_intra_day_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            hybrid_cursor_factory(), repo_client
        ),
        STORY_SET_OFFLOAD_PBO_LATE: lambda: offload_pbo_late_arriving_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_RPA: lambda: offload_range_ipa_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_SORTING: lambda: offload_sorting_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            list_only, repo_client
        ),
        STORY_SET_OFFLOAD_LIST_RPA: lambda: offload_list_as_range_ipa_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_LPA: lambda: offload_list_partition_append_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_LPA_FULL: lambda: offload_list_partition_append_full_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api, repo_client
        ),
        STORY_SET_OFFLOAD_TRANSPORT: lambda: offload_transport_story_tests(
            schema, hybrid_schema, data_db_schema, load_db_schema, orchestration_config, backend_api, frontend_api,
            list_only, repo_client
        ),
        STORY_SET_ORCHESTRATION_LOCKS: lambda: orchestration_locks_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api
        ),
        STORY_SET_ORCHESTRATION_STEP: lambda: orchestration_step_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api
        ),
        STORY_SET_SCHEMA_SYNC: lambda: schema_sync_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            test_cursor_factory(), repo_client
        ),
        STORY_SET_TRANSFORMATIONS: lambda: transformations_story_tests(
            schema, hybrid_schema, data_db_schema, orchestration_config, backend_api, frontend_api,
            test_cursor_factory(), list_only, repo_client
        ),
        STORY_SET_MSSQL: lambda: mssql_story_tests(schema, hybrid_schema, data_db_schema, options),
        STORY_SET_NETEZZA: lambda: netezza_story_tests(schema, hybrid_schema, data_db_schema, options)
    }

    if set(story_list) - set(STORY_SET_ALL):
        raise StoryRunnerException('Stories (%s) is not in full list of stories: %s'
                                   % (list(set(story_list) - set(STORY_SET_ALL)), str(STORY_SET_ALL)))

    stories = {k: story_gen_fns[k]() for k in story_list}

    if list_only:
        # A compact list of the contents of the stories dict, format:
        #   {'story': [list of individual ids], ...}
        return {k: [i.get('id') for i in v] for k, v in stories.items()}

    # Only return the stories we asked for
    return {k: stories[k] for k in story_list}


def list_stories(opt_object, messages, orchestration_config):
    test_teamcity_starttestsuite(opt_object, 'list_stories')
    try:
        test_teamcity_starttest(opt_object, 'list_stories')
        output = pprint.pformat(get_story_tests(opt_object.test_user, messages, orchestration_config, list_only=True))
        if opt_object.teamcity:
            test_teamcity_stdout(opt_object, 'list_stories', output)
        else:
            print(output)
    except Exception as exc:
        messages.log('Unhandled exception in --list-stories:\n%s' % traceback.format_exc())
        if opt_object.teamcity:
            test_teamcity_failtest(opt_object, 'list_stories', 'Fail: %s' % str(exc))
    finally:
        test_teamcity_endtestsuite(opt_object, 'list_stories')


def copy_linked_test_attributes(story, story_list):
    # Use 'linked_id' attribute to pull in story details
    assert 'linked_id' in story
    keep_id = story['id']
    new_story = copy.copy([s for s in story_list if s['id'] == story['linked_id']].pop())
    new_story['id'] = keep_id
    return new_story


def check_story_integrity(story_name, story_list, options, block_flow_id, messages):
    """ Check the integrity of the story list, e.g. duplicate IDs or broken links to tests. """
    story_ids = [_['id'] for _ in story_list]
    if len(story_ids) != len(set(story_ids)):
        raise StoryRunnerException('There are duplicate story ids in %s: %s'
                                   % (story_name, str([_ for _ in story_ids if story_ids.count(_) > 1])))
    linked_story_ids = [_['linked_id'] for _ in story_list if _.get('linked_id')]
    broken_story_links = [_ for _ in linked_story_ids if _ not in story_ids]
    if broken_story_links:
        raise StoryRunnerException('There are broken links with story %s: %s'
                                   % (story_name, str(broken_story_links)))

    stories_with_expected_exception_and_assertions = [_.get('id') for _ in story_list
                                                      if _.get('expected_exception_string')
                                                      and _.get('assertion_pairs')]
    if stories_with_expected_exception_and_assertions:
        message = 'WARNING: There are stories in story %s that catch exceptions and also have assertions, ' \
                  'the assertions are ignored: %s' % (
                      story_name.upper(), str(stories_with_expected_exception_and_assertions))

        if options.teamcity:
            print("##teamcity[message text='%s' flowId='%s' status='WARNING']" %
                  (teamcity_escape(message), block_flow_id))

        log(message, detail=vverbose if options.teamcity else normal)

    stories_with_incorrect_setup = [_.get('id') for _ in story_list
                                    if _.get('type') != STORY_TYPE_SETUP and _.get('setup')]
    if stories_with_incorrect_setup:
        raise StoryRunnerException('There are stories with old style embedded setup in %s: %s'
                                   % (story_name, str(stories_with_incorrect_setup)))


def gen_stories_to_run(options, config, set_name) -> list:
    if options.story:
        stories_to_run = options.story.split(',')
    elif set_name == test_constants.SET_STORIES_CONTINUOUS:
        stories_to_run = STORY_SET_CONTINUOUS
    else:
        stories_to_run = STORY_SET_INTEGRATION
        if config.backend_distribution in [BACKEND_DISTRO_EMR, BACKEND_DISTRO_MAPR]:
            log('Excluding ssh_shortcircuit tests on %s' % config.backend_distribution)
            stories_to_run.remove(STORY_SET_CLI_API_NOSSH)
    return stories_to_run


def get_story_type_offload_fn(story, orchestration_config):
    def test_fn(test):
        execution_id = ExecutionId()
        messages = OffloadMessages.from_options(orchestration_config, log_fh=get_log_fh(), execution_id=execution_id,
                                                command_type=orchestration_constants.COMMAND_OFFLOAD)
        expected_exception_string = story.get('expected_exception_string')
        expected_status = story.get('expected_status', True)
        skip_assertions = False
        try:
            config_overrides = story_test_config_overrides(story, orchestration_config)
            offload_fn = lambda: OrchestrationRunner(config_overrides=config_overrides).offload(
                story['options'], execution_id=execution_id, reuse_log=True, messages_override=messages)
            status = response_time_bench(test, story['title'], story['id'], offload_fn)
            if expected_status is not None and status != expected_status:
                raise StoryRunnerException('Tested offload() return != %s' % expected_status)
            if expected_exception_string:
                # We shouldn't get here if we're expecting an exception
                log('Missing exception containing: %s' % expected_exception_string)
                # Can't include exception in error below otherwise we'll end up with a pass
                raise StoryRunnerException('offload() did not throw expected exception')
        except Exception as exc:
            if expected_exception_string and expected_exception_string.lower() in str(exc).lower():
                log('Test caught expected exception:%s\n%s' % (type(exc), str(exc)))
                log('Ignoring exception containing: %s' % expected_exception_string)
                skip_assertions = True
            else:
                log(traceback.format_exc())
                raise

        test.offload_messages = messages
        if story.get('assertion_pairs') and not skip_assertions:
            for i, (fn1, fn2) in enumerate(story['assertion_pairs']):
                test.assertEqual(fn1(test), fn2(test), '%s (assertion %s)' % (story['title'], i))

    return test_fn


def get_story_type_schema_sync_fn(story, config_options):
    def test_fn(test):
        execution_id = ExecutionId()
        messages = OffloadMessages.from_options(config_options, log_fh=get_log_fh(), execution_id=execution_id,
                                                command_type=orchestration_constants.COMMAND_SCHEMA_SYNC)
        expected_exception_string = story.get('expected_exception_string')
        try:
            config_overrides = story_test_config_overrides(story, config_options)
            schema_sync_options = get_orchestration_options_object(operation_name=SCHEMA_SYNC_OP_NAME,
                                                                   log_path=config_options.log_path,
                                                                   verbose=config_options.verbose,
                                                                   vverbose=config_options.vverbose)
            for k in story['options']:
                if hasattr(schema_sync_options, k):
                    setattr(schema_sync_options, k, story['options'][k])
                else:
                    raise StoryRunnerException('schema_sync_options object does not have attribute: %s' % k)
            assert schema_sync_options.include, 'No include specified'
            normalise_schema_sync_options(schema_sync_options)
            status = OrchestrationRunner(config_overrides=config_overrides).schema_sync(
                schema_sync_options, execution_id=execution_id, reuse_log=True, messages_override=messages)
            if status > 0:
                test.fail('%s failed: Schema Sync exceptions encountered' % test.name)

            test.offload_messages = messages
            if story.get('assertion_pairs'):
                for i, (fn1, fn2) in enumerate(story['assertion_pairs']):
                    test.assertEqual(fn1(test), fn2(test), '%s (assertion %s)' % (story['title'], i))

        except Exception as exc:
            if expected_exception_string and expected_exception_string in str(exc):
                log('Test caught expected exception:\n%s' % str(exc))
                log('Ignoring expected exception containing "%s"' % expected_exception_string)
                skip_assertions = True
            else:
                log(traceback.format_exc())
                raise

    return test_fn


def get_story_type_agg_validate_fn(story, config_options):
    def test_fn(test):
        messages = OffloadMessages.from_options(config_options, log_fh=get_log_fh())
        expected_exception_string = story.get('expected_exception_string')
        expected_status = story.get('expected_status', True)
        av_options = copy.copy(config_options)
        # Add agg_validate options and defaults
        av_options.as_of_scn = None
        av_options.aggregate_functions = DEFAULT_AGGS
        av_options.filters = None
        av_options.group_bys = None
        av_options.selects = [DEFAULT_SELECT_COLS]
        av_options.frontend_parallelism = None
        av_options.skip_boundary_check = None
        for opt, value in story['options'].items():
            setattr(av_options, opt, value)
        assert av_options.owner_table, 'No owner_table specified'
        normalise_owner_table_options(av_options)
        agg_validate.post_process_args(av_options)
        try:
            status = agg_validate.validate_table(args=av_options, messages=messages)
            if expected_status is not None and status != expected_status:
                raise StoryRunnerException('Tested validate_table() return != %s' % expected_status)
            if expected_exception_string:
                # We shouldn't get here if we're expecting an exception
                log('Missing exception containing: %s' % expected_exception_string)
                # Can't include exception in error below otherwise we'll end up with a pass
                raise StoryRunnerException('validate_table() did not throw expected exception')
        except Exception as exc:
            if expected_exception_string and expected_exception_string.lower() in str(exc).lower():
                log('Test caught expected exception:\n%s' % str(exc))
                log('Ignoring exception containing "%s"' % expected_exception_string)
            else:
                log(traceback.format_exc())
                raise

        test.offload_messages = messages
        if story.get('assertion_pairs'):
            for i, (fn1, fn2) in enumerate(story['assertion_pairs']):
                test.assertEqual(fn1(test), fn2(test), '%s (assertion %s)' % (story['title'], i))

    return test_fn


def get_story_type_python_fn(story):
    """ Allow us to run any Python function for test. Functions must return True on success. """
    def test_fn(test):
        for fn in (story['python_fns'] or []):
            try:
                log('Running python fn for story: %s' % story['id'], detail=verbose)
                status = fn()
                if not status:
                    raise StoryRunnerException('Tested python function returned False')
            except Exception as exc:
                expected_exception_string = story.get('expected_exception_string')
                if expected_exception_string and expected_exception_string.lower() in str(exc).lower():
                    log('Test caught expected exception:\n%s' % str(exc))
                    log('Ignoring exception containing "%s"' % expected_exception_string)
                    skip_assertions = True
                else:
                    log(traceback.format_exc())
                    raise

    return test_fn


def get_story_type_setup_fn(story, config_options, test_options):
    def test_fn(test):
        """ Not a real test, just a step changing data in some way to facilitate a subsequent step.
            If it fails we want it to show in TeamCity so we must treat this as a test in its own right.
        """
        try:
            story_setup(config_options, test_options, [story])
            backend_api = get_backend_testing_api(test_options, do_not_connect=True)
            if backend_api.test_setup_seconds_delay():
                time.sleep(backend_api.test_setup_seconds_delay())
        except Exception:
            log(traceback.format_exc())
            raise

        if story.get('assertion_pairs'):
            for i, (fn1, fn2) in enumerate(story['assertion_pairs']):
                test.assertEqual(fn1(test), fn2(test), '%s (assertion %s)' % (story['title'], i))

    return test_fn


def get_story_type_shell_cmd_fn(story, config_options, test_options):
    def test_fn(test):
        messages = OffloadMessages.from_options(config_options, log_fh=get_log_fh())
        for cmd in (story['shell_commands'] or []):
            skip_assertions = False
            try:
                log('Running subproc_cmd: %s' % cmd, detail=verbose)
                returncode, output = subproc_cmd(cmd, test_options, messages, cwd=story.get('shell_cwd'),
                                                 env=story.get('shell_env'))
                log('subproc_cmd return code: %s' % returncode, detail=vverbose)
                log('subproc_cmd output: %s' % output, detail=vverbose)
                acceptable_return_codes = story.get('acceptable_return_codes') or [0]
                if returncode not in acceptable_return_codes:
                    raise StoryRunnerException('Tested shell_command return %s not in %s: %s'
                                               % (returncode, acceptable_return_codes, cmd[0]))
            except Exception as exc:
                expected_exception_string = story.get('expected_exception_string')
                if expected_exception_string and expected_exception_string.lower() in str(exc).lower():
                    log('Test caught expected exception:\n%s' % str(exc))
                    log('Ignoring exception containing "%s"' % expected_exception_string)
                    skip_assertions = True
                else:
                    log(traceback.format_exc())
                    raise

            if story.get('assertion_pairs') and not skip_assertions:
                for i, (fn1, fn2) in enumerate(story['assertion_pairs']):
                    test.assertEqual(fn1(test), fn2(test), '%s (assertion %s)' % (story['title'], i))

    return test_fn


def story_setup(config_options, test_options, stories):
    """Run any setup step for a list of story steps."""
    def get_setup_sqls(setup_dict, tech_type):
        if tech_type in setup_dict and inspect.isfunction(setup_dict[tech_type]):
            return setup_dict[tech_type]()
        else:
            return setup_dict.get(tech_type, [])

    for story in stories:
        if story.get('setup'):
            if test_options.teamcity:
                test_teamcity_stdout_pq(test_options, story['id'], 'Setting up %s' % story['id'])
            else:
                log('Setting up %s' % story['id'])
        else:
            if test_options.teamcity:
                test_teamcity_stdout_pq(test_options, story['id'], 'No setup for %s' % story['id'])
            else:
                log('No setup for %s' % story['id'])
            continue

        # FRONTEND supercedes ORACLE, MSSQL, NETEZZA
        frontend_sqls = get_setup_sqls(story['setup'], STORY_SETUP_TYPE_FRONTEND)
        frontend_sqls = frontend_sqls or get_setup_sqls(story['setup'], STORY_SETUP_TYPE_ORACLE)
        frontend_sqls = frontend_sqls or get_setup_sqls(story['setup'], STORY_SETUP_TYPE_MSSQL)
        frontend_sqls = frontend_sqls or get_setup_sqls(story['setup'], STORY_SETUP_TYPE_NETEZZA)
        hybrid_sqls = get_setup_sqls(story['setup'], STORY_SETUP_TYPE_HYBRID)
        python_fns = get_setup_sqls(story['setup'], STORY_SETUP_TYPE_PYTHON)

        non_connected_api = get_frontend_testing_api(test_options, config=config_options, do_not_connect=True)

        if frontend_sqls:
            with non_connected_api.create_new_connection_ctx(test_options.test_user,
                                                             test_options.test_pass,
                                                             trace_action_override='FrontendTestingApi(StorySetup)') as sh_test_api:
                if config_options.db_type == DBTYPE_MSSQL:
                    sh_test_api.execute_ddl('BEGIN TRAN')
                for sql in frontend_sqls:
                    try:
                        sh_test_api.execute_ddl(sql)
                    except Exception as exc:
                        if 'does not exist' in str(exc) and sql.upper().startswith('DROP'):
                            log('Ignoring: ' + str(exc), vverbose)
                        else:
                            log(str(exc))
                            raise
                if config_options.db_type != DBTYPE_TERADATA:
                    # We have autocommit enabled on Teradata:
                    #   COMMIT WORK not allowed for a DBC/SQL session. (-3706)
                    sh_test_api.execute_ddl('COMMIT')

        if hybrid_sqls and non_connected_api.hybrid_schema_supported():
            with non_connected_api.create_new_connection_ctx(to_hybrid_schema(test_options.test_user),
                                                             test_options.test_hybrid_pass,
                                                             trace_action_override='FrontendTestingApi(StoryHybridSetup)') as hybrid_api:
                for sql in hybrid_sqls:
                    try:
                        hybrid_api.execute_ddl(sql)
                    except Exception as exc:
                        if 'does not exist' in str(exc) and 'DROP' in sql.upper():
                            log('Ignoring: ' + str(exc), vverbose)
                        else:
                            log(str(exc))
                            raise
                hybrid_api.execute_ddl('commit')

        for fn in python_fns:
            if not inspect.isfunction(fn):
                raise StoryRunnerException('Row in python_fns is not a function: %s %s' % (type(fn), str(fn)))
            try:
                fn()
            except Exception as exc:
                if ' exist' in str(exc):
                    log('Ignoring: ' + str(exc), vverbose)
                else:
                    raise


def story_test_config_overrides(story, orchestration_config):
    """Return 'config_override' from story enhanced with certain attributes from orchestration_config"""
    base_config = {'execute': True,
                   'verbose': orchestration_config.verbose,
                   'vverbose': orchestration_config.vverbose}
    if story.get('config_override'):
        base_config.update(story['config_override'])
    return base_config


def run_story_test_process_start_flow(test_options, story_name, block_flow_id, flow_id):
    if test_options.teamcity:
        # teamcity_starttestblock(story_name)
        print("##teamcity[flowStarted name='%s' flowId='%s' parent='%s']" % (story_name, block_flow_id, flow_id),
              flush=True)
        print("##teamcity[blockOpened name='%s' flowId='%s']" % (story_name, block_flow_id), flush=True)


def run_story_test_process_end_flow(test_options, story_name, block_flow_id, flow_id):
    if test_options.teamcity:
        # teamcity_endtestblock(story_name)
        print("##teamcity[blockClosed name='%s' flowId='%s']" % (story_name, block_flow_id), flush=True)
        print("##teamcity[flowFinished name='%s' flowId='%s' parent='%s']" % (story_name, block_flow_id, flow_id),
              flush=True)


def run_story_test_process_prereq(test_options, story, test_name, block_flow_id):
    if 'prereq' in story and story['prereq'] is not None:
        # If the prereq function fails or does not exist we want to raise this in teamcity
        # so we must treat this as a test in its own right
        test_name = f'{test_name}_prereq'

        def test_fn(test):
            prereq_fn = story['prereq']
            if inspect.isfunction(prereq_fn):
                result = prereq_fn()
            else:
                result = prereq_fn
            if not result:
                if test_options.teamcity:
                    print("##teamcity[message text='Pre-requisite not met, skipping test' flowId='%s']"
                          % (threading.current_thread().native_id))
                else:
                    log('Pre-requisite not met, skipping test')
                return False
            return True

        t = BaseTest(test_options, test_name, test_fn)
        return t(parent_flow_id=block_flow_id, capture_stdout=test_options.teamcity)
    return True


def run_story_test_process_configs(test_options, orchestration_config, story):
    if story.get('config_override'):
        base_config = {'execute': True,
                       'verbose': test_options.verbose,
                       'vverbose': test_options.vverbose}
        base_config.update(story['config_override'])
        use_configs = OrchestrationConfig.from_dict(base_config)
    else:
        use_configs = copy.copy(orchestration_config)
    return use_configs


def run_story_test_process(options, story_name, orchestration_config, flow_id):
    """
    Mirrored by test.run_story_test_process().
    Ideally we'll retire the test code and keep this version but at the moment it's too difficult.
    """
    test_name_re = re.compile(options.filter, re.I)
    def should_run_test_f(test_name):
        return test_passes_filter(test_name, test_name_re, options, options.known_failure_blacklist)

    test_name = None
    block_flow_id = "%s.%s" % (flow_id, story_name)
    repo_client = None
    try:
        init_log(story_name)
        messages = OffloadMessages.from_options(options, log_fh=get_log_fh())
        repo_client = orchestration_repo_client_factory(options, messages)

        run_story_test_process_start_flow(options, story_name, block_flow_id, flow_id)

        stories = get_story_tests(options.test_user, messages, orchestration_config,
                                  story_list=story_name, options=options)

        story_list = stories[story_name]

        check_story_integrity(story_name, story_list, options, block_flow_id, messages)

        for story in story_list:
            test_name = story['id']
            if not should_run_test_f(test_name):
                continue

            try:
                test_title = story.get('title', test_name)
                if story.get('type') == STORY_TYPE_LINK_ID:
                    story = copy_linked_test_attributes(story, story_list)

                if options.teamcity:
                    print("##teamcity[blockOpened name='%s' description='%s' flowId='%s']" %
                          (test_name, teamcity_escape(test_title), block_flow_id), flush=True)
                    log('\n%s (%s)' % (test_title, test_name), ansi_code='underline', detail=vverbose)
                else:
                    log('\n%s (%s)' % (test_title, test_name), ansi_code='underline')

                if not run_story_test_process_prereq(options, story, test_name, block_flow_id):
                    continue

                use_configs = run_story_test_process_configs(options, orchestration_config, story)
                t = BaseTest(options, test_name, story_test_f(story, use_configs, options))
                t(capture_stdout=options.teamcity, parent_flow_id=block_flow_id)

            finally:
                if options.teamcity:
                    print("##teamcity[blockClosed name='%s' flowId='%s']" %
                          (test_name, block_flow_id), flush=True)

    except Exception as exc:
        log('Aborting story %s due to exception in test %s, further stories will attempt to complete'
            % (story_name, test_name))
        log('Exception:\n%s\n%s' % (str(exc), traceback.format_exc()))
    finally:
        if repo_client:
            try:
                repo_client.close()
            except:
                pass
        run_story_test_process_end_flow(options, story_name, block_flow_id, flow_id)


def story_test_f(story, config_options, test_options):
    """
    This is an incomplete mirror of test.story_test_f().
    Ideally we'll retire this code and keep the story_runner version but at the moment it's too difficult due to
    changes on the Console feature branches.
    TODO complete this function and then deal with the duplication.
    """
    if story['type'] == STORY_TYPE_OFFLOAD:
        test_fn = get_story_type_offload_fn(story, config_options)

    elif story['type'] == STORY_TYPE_SETUP:
        test_fn = get_story_type_setup_fn(story, config_options, test_options)

    elif story['type'] == STORY_TYPE_AGG_VALIDATE:
        test_fn = get_story_type_agg_validate_fn(story, config_options)

    elif story['type'] == STORY_TYPE_SCHEMA_SYNC:
        test_fn = get_story_type_schema_sync_fn(story, config_options)

    elif story['type'] == STORY_TYPE_SHELL_COMMAND:
        test_fn = get_story_type_shell_cmd_fn(story, config_options, test_options)

    elif story['type'] == STORY_TYPE_PYTHON_FN:
        test_fn = get_story_type_python_fn(story)

    else:
        raise NotImplementedError('Story type has not yet been implemented: {}'.format(story['type']))

    return test_fn


def run_story_tests(options, orchestration_config, teamcity_name=test_constants.SET_STORIES):
    """
    Mirrored by test.run_story_tests().
    Ideally we'll retire this code and keep the story_runner version but at the moment it's too difficult.
    TODO deal with this duplication.
    """
    flow_id = test_teamcity_starttestsuite_pq(options, teamcity_name)

    try:
        stories_to_run = gen_stories_to_run(options, orchestration_config, teamcity_name)

        with ProcessPoolExecutor(multiprocessing.cpu_count() if options.teamcity else 1) as pool:
            for story_name in stories_to_run:
                pool.submit(run_story_test_process, options, story_name, orchestration_config, flow_id)

    finally:
        test_teamcity_endtestsuite_pq(options, teamcity_name, flow_id)
