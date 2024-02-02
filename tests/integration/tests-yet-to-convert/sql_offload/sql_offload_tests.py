from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os
import re
import traceback

from goe.gluent import get_log_fh, log
from goe.offload.offload_messages import OffloadMessages
from testlib.test_framework.test_functions import get_backend_testing_api, get_frontend_testing_api,\
    get_test_set_sql_path, test_passes_filter
from testlib.test_framework import test_constants
from test_sets.offload_test_functions import get_offload_test_fn


###############################################################################
# CONSTANTS
###############################################################################

SQL_OFFLOAD_SQL_FILE_NAME_RE = re.compile(r'^gl_.*.sql$')
SQL_OFFLOAD_TPT_FILE_NAME_RE = re.compile(r'^gl_.*.tpt$')


###############################################################################
# FUNCTIONS
###############################################################################

def sql_offload_test_table_scripts(orchestration_config):
    tn_paths = []

    sql_offload_path = get_test_set_sql_path('sql_offload', orchestration_config.db_type)
    try:
        pts = [fn for fn in os.listdir(sql_offload_path) if SQL_OFFLOAD_SQL_FILE_NAME_RE.match(fn)]
    except OSError:
        pts = []

    for fn in pts:
        path = os.path.join(sql_offload_path, fn)
        ddl = open(path).read()
        m = re.search('create table (\w+)', ddl.lower())
        if m:
            tn_paths.append((m.group(1), path))

    return tn_paths


def sql_offload_tpt_scripts(orchestration_config):
    tn_paths = []

    sql_offload_path = get_test_set_sql_path('sql_offload', orchestration_config.db_type)
    try:
        pts = [fn for fn in os.listdir(sql_offload_path) if SQL_OFFLOAD_TPT_FILE_NAME_RE.match(fn)]
    except OSError:
        pts = []

    for fn in pts:
        path = os.path.join(sql_offload_path, fn)
        ddl = open(path).read()
        m = re.search('define job (\w+)', ddl.lower())
        if m:
            tn_paths.append((m.group(1), path))

    return tn_paths


def run_sql_offload_test_process(current_schema, table_name, options, orchestration_config,
                                 test_class, flow_id, hybrid_query_parallelism=2):
    test_name_re = re.compile(options.filter, re.I)
    def should_run_test_f(test_name):
        return test_passes_filter(test_name, test_name_re, options, options.known_failure_blacklist)

    try:
        frontend_api = None
        backend_api = None
        messages = OffloadMessages.from_options(options, get_log_fh())
        test_name = 'offload_minus_%s' % (table_name.split('.')[-1])
        config_modifiers = {'verbose': options.verbose,
                            'vverbose': options.vverbose}
        if should_run_test_f(test_name):
            frontend_api = get_frontend_testing_api(options, trace_action='run_sql_offload_test_process')
            sh_test_api = frontend_api.create_new_connection(options.test_user, options.test_pass)
            backend_api = get_backend_testing_api(options)
            t = test_class(options, test_name,
                           get_offload_test_fn(current_schema, table_name, sh_test_api, backend_api,
                                               orchestration_config, messages,
                                               hybrid_query_parallelism=hybrid_query_parallelism,
                                               config_modifiers=config_modifiers))
            t(parent_flow_id=flow_id, capture_stdout=options.teamcity)
            if table_name == 'gl_timestamps' and frontend_api.hybrid_schema_supported():
                for tz in ['-06:00', '+06:00']:
                    # Repeat the minus check but in other TZs
                    test_name_tz = test_name + '_%s' % tz
                    t = test_class(options, test_name_tz,
                             get_offload_test_fn(current_schema, table_name, frontend_api,
                                                 backend_api, orchestration_config, messages,
                                                 run_offload=False, tz=tz))
                    t(parent_flow_id=flow_id, capture_stdout=options.teamcity)
        test_name = test_name + '_verification'
        if should_run_test_f(test_name):
            frontend_api = frontend_api or get_frontend_testing_api(options)
            backend_api = backend_api or get_backend_testing_api(options)
            t = test_class(options, test_name,
                           get_offload_test_fn(current_schema, table_name, frontend_api, backend_api,
                                               orchestration_config, messages, with_assertions=False,
                                               config_modifiers=config_modifiers, verification_mode=True))
            t(parent_flow_id=flow_id, capture_stdout=options.teamcity)
    except Exception:
        log('Unhandled exception in %s(%s) test set:\n%s'
            % (test_constants.SET_SQL_OFFLOAD, table_name, traceback.format_exc()))


def run_sql_offload_tests(options, current_schema, orchestration_config, test_class):
    flow_id = test_teamcity_starttestsuite_pq(options, test_constants.SET_SQL_OFFLOAD)

    try:
        with ProcessPoolExecutor(multiprocessing.cpu_count() if options.teamcity else 1) as pool:
            for table_name, _ in sorted(sql_offload_test_table_scripts(orchestration_config)):
                pool.submit(run_sql_offload_test_process, current_schema, table_name, options,
                            orchestration_config, test_class, flow_id)
    except Exception:
        log('Unhandled exception in %s test set:\n%s' % (test_constants.SET_SQL_OFFLOAD, traceback.format_exc()))
    finally:
        test_teamcity_endtestsuite_pq(options, test_constants.SET_SQL_OFFLOAD, flow_id)
