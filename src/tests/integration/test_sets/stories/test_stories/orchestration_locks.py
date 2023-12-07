import threading
import traceback

from test_sets.stories.story_globals import STORY_TYPE_OFFLOAD, STORY_SETUP_TYPE_ORACLE, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl,\
    get_backend_drop_incr_fns,\
    STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_assertion_functions import text_in_log

from goe.offload.offload_messages import VERBOSE
from goe.orchestration.cli_entry_points import offload_by_cli, schema_sync_by_cli
from goe.util.orchestration_lock import OrchestrationLockTimeout
from goe.gluent import get_offload_options, get_options, OFFLOAD_OP_NAME
from schema_sync import get_schema_sync_opts,\
    SCHEMA_SYNC_LOCKED_MESSAGE_TEXT
from testlib.test_framework.test_functions import log


STORY_DIM = 'story_lock_dim'

THREAD1_NAME = 'thread1'
THREAD2_NAME = 'thread2'


def offload_fn(options, schema, table_name, option_modifiers=None):
    def test_fn():
        tmp_opt = get_options(operation_name=OFFLOAD_OP_NAME)
        get_offload_options(tmp_opt)
        offload_options, _ = tmp_opt.parse_args([])
        offload_options.owner_table = schema + '.' + table_name
        offload_options.log_path = options.log_path
        offload_options.execute = True
        offload_options.verbose = options.verbose
        offload_options.vverbose = options.vverbose
        if option_modifiers:
            for opt, value in option_modifiers.items():
                setattr(offload_options, opt, value)
        offload_by_cli(offload_options)
    return test_fn


def schema_sync_fn(options, schema, table_name, option_modifiers=None):
    """ Return a function which will enable Incremental Update on a table """
    def test_fn():
        tmp_opt = get_schema_sync_opts()
        ss_options, _ = tmp_opt.parse_args([])
        ss_options.include = schema + '.' + table_name
        ss_options.command_file = None
        ss_options.log_path = options.log_path
        ss_options.execute = True
        ss_options.verbose = options.verbose
        ss_options.vverbose = options.vverbose
        if option_modifiers:
            for opt, value in option_modifiers.items():
                setattr(ss_options, opt, value)
        return schema_sync_by_cli(ss_options)
    return test_fn


def competing_commands(thread1_fn, thread2_fn, schema_sync=False, iu_cleanup=False):
    """ Run two function concurrently, with a very short delay between 1 and 2, and check that
        process 2 fails with a lock timeout.
        Code to check for exceptions in threads taken from:
            https://stackoverflow.com/questions/12484175/make-python-unittest-fail-on-exception-from-any-thread
    """
    exceptions_caught_in_threads = {}

    def custom_excepthook(args):
        thread_name = args.thread.name
        exceptions_caught_in_threads[thread_name] = {
            'thread': args.thread,
            'exception': {
                'type': args.exc_type,
                'value': args.exc_value,
                'traceback': args.exc_traceback
            }
        }

    # Registering custom excepthook to catch the exception in the threads
    threading.excepthook = custom_excepthook

    t1 = threading.Thread(name=THREAD1_NAME, target=thread1_fn)
    t2 = threading.Thread(name=THREAD2_NAME, target=thread2_fn)
    t1.start()
    # Initially I had a sleep here to try and force which thread would block the other but I was then having
    # problems with some threads finishing inside the fraction of the second I was pausing. And sometimes the delay
    # was not large enough to force the order. Instead we now let them run at the same time and ensure that one
    # worked and one failed acquiring lock.
    t2.start()
    t1.join()
    t2.join()

    if schema_sync:
        # Schema Sync doesn't abort with an exception when locked, instead it logs a notice and moves on.
        # It is not trivial to get at the messages object it populates so instead we're looking for no exceptions
        # at all because one thread will add the column and the other will skip, if the locking didn't work then
        # both threads would add the column and one would fail but again, SS catches any exception and logs it.
        # So the plan is:
        #   1) Ensure no exception at all in either thread
        #   2) Ensure a message was logged saying a thread was blocked
        if exceptions_caught_in_threads:
            log('Unexpected exceptions were detected:\n')
            for thread_name in exceptions_caught_in_threads:
                log('Unexpected %s exception: %s'
                    % (thread_name, str(exceptions_caught_in_threads[thread_name]['exception']['value'])))
                log(''.join(traceback.format_exception(
                    exceptions_caught_in_threads[thread_name]['exception']['type'],
                    exceptions_caught_in_threads[thread_name]['exception']['value'],
                    exceptions_caught_in_threads[thread_name]['exception']['traceback'])))
        assert not exceptions_caught_in_threads, 'Unexpected Schema Sync exceptions detected, something is amiss'
        assert text_in_log(SCHEMA_SYNC_LOCKED_MESSAGE_TEXT,
                           search_from_text='orchestration_locks_schema_sync_vs_schema_sync'),\
            'Did not find Schema Sync lock message in log file'
    else:
        assert exceptions_caught_in_threads, 'No sessions were blocked, something is amiss'
        if THREAD2_NAME in exceptions_caught_in_threads:
            # Assuming thread 2 is being blocked
            blocker = THREAD1_NAME
            waiter = THREAD2_NAME
        else:
            # Assuming thread 1 is being blocked
            blocker = THREAD2_NAME
            waiter = THREAD1_NAME
        log('Assuming %s is blocker due to %s exception: %s'
            % (blocker, waiter, str(exceptions_caught_in_threads[waiter]['exception']['value'])), detail=VERBOSE)
        if blocker in exceptions_caught_in_threads:
            log('Unexpected blocker exception: %s' % str(exceptions_caught_in_threads[blocker]['exception']['value']))
            log(''.join(traceback.format_exception(exceptions_caught_in_threads[blocker]['exception']['type'],
                                                   exceptions_caught_in_threads[blocker]['exception']['value'],
                                                   exceptions_caught_in_threads[blocker]['exception']['traceback'])))
        assert blocker not in exceptions_caught_in_threads
        assert waiter in exceptions_caught_in_threads
        assert exceptions_caught_in_threads[waiter]['exception']['type'] is OrchestrationLockTimeout
        log('Waiter encountered correct exception: %s' % str(exceptions_caught_in_threads[waiter]['exception']['type']),
            detail=VERBOSE)


def schema_sync_add_col_setup(schema, table_name, column_name):
    return 'ALTER TABLE %s.%s ADD (%s NUMBER)' % (schema, table_name, column_name)


def orchestration_locks_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api):
    """ Test that orchestration commands that take a control lock do indeed block other commands that also take a lock
    """
    return [
        {'id': 'orchestration_locks_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % STORY_DIM,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_rdbms_dim_create_ddl(frontend_api, schema, STORY_DIM, pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, STORY_DIM),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, STORY_DIM)}},
        # Check Offload blocks itself
        {'id': 'orchestration_locks_offload_vs_offload',
         'type': STORY_TYPE_SETUP,
         'title': 'Offload vs Offload',
         'setup': {STORY_SETUP_TYPE_PYTHON: [lambda: competing_commands(
             offload_fn(options, schema, STORY_DIM, option_modifiers={'reset_backend_table': True}),
             offload_fn(options, schema, STORY_DIM, option_modifiers={'reset_backend_table': True})
         )]}},
        {'id': 'orchestration_locks_dim_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % STORY_DIM,
         'narrative': 'Offload a dimension to use in later testing',
         'options': {'owner_table': schema + '.' + STORY_DIM,
                     'reset_backend_table': True}},
        {'id': 'orchestration_locks_dim_add_col1',
         'type': STORY_TYPE_SETUP,
         'title': 'Add column to STORY_DIM',
         'setup': {STORY_SETUP_TYPE_ORACLE: [schema_sync_add_col_setup(schema, STORY_DIM, 'test_col1')]},
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.schema_evolution_supported()},
        # Check Schema Sync blocks itself, one of the threads will successfully process changes
        # It is not easy to test Schema Sync blocks Incremental Update because SS takes a lock after doing some work
        # by which time IU may well have been and gone. Relying on it blocking itself instead.
        {'id': 'orchestration_locks_schema_sync_vs_schema_sync',
         'type': STORY_TYPE_SETUP,
         'title': 'Schema Sync vs Schema Sync',
         'setup': {STORY_SETUP_TYPE_PYTHON: [lambda: competing_commands(schema_sync_fn(options, schema, STORY_DIM),
                                                                        schema_sync_fn(options, schema, STORY_DIM),
                                                                        schema_sync=True)]},
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.gluent_schema_sync_supported()},
    ]
