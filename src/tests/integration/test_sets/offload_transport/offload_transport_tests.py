from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stdout
from io import StringIO
import multiprocessing
import re
import traceback

from goe.gluent import get_log_fh, normalise_size_option, verbose
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_TERADATA
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_transport import get_offload_transport_method_validation_fns,\
    VALID_OFFLOAD_TRANSPORT_METHODS, OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title
from testlib.test_framework.test_functions import get_backend_testing_api, get_frontend_testing_api,\
    test_passes_filter, teamcity_escape, test_teamcity_failtest_pq,\
    test_teamcity_starttestsuite_pq, test_teamcity_endtestsuite_pq
from testlib.test_framework import test_constants
from test_sets.offload_test_functions import get_offload_test_fn
from testlib.test_framework.test_functions import log


###############################################################################
# CONSTANTS
###############################################################################

# We don't want to use test_constants.GL_TYPE_MAPPING because it has specific data type overrides
OFFLOAD_TRANSPORT_GENERATED_TABLES = [test_constants.GL_WIDE, test_constants.GL_CHARS,
                                      test_constants.GL_TYPES, test_constants.GL_TYPES_QI]


###############################################################################
# FUNCTIONS
#############################################################################

def offload_transport_test_tables(orchestration_config):
    if orchestration_config.db_type == DBTYPE_ORACLE:
        # gl_hash included below to ensure we have a multi-chunk offload on Oracle (via --max-offload-chunk-count=4)
        return sorted(OFFLOAD_TRANSPORT_GENERATED_TABLES
                      + ['gl_binary', 'gl_hash', 'gl_iot', 'gl_iot_number_35', 'gl_iot_str',
                         'gl_null_in_string', 'gl_timezones', 'gl_timestamps'])
    elif orchestration_config.db_type == DBTYPE_TERADATA:
        return sorted(OFFLOAD_TRANSPORT_GENERATED_TABLES
                      + ['gl_binary', 'gl_null_in_string', 'gl_timezones'])


def run_offload_transport_test_process(current_schema, table_name, options, orchestration_config,
                                       test_class, parent_flow_id):
    test_name_re = re.compile(options.filter, re.I)
    def should_run_test_f(test_name):
        return test_passes_filter(test_name, test_name_re, options, options.known_failure_blacklist)

    frontend_api = None
    try:
        frontend_api = get_frontend_testing_api(options, config=orchestration_config,
                                                trace_action='run_offload_transport_test_process')
        sh_test_api = frontend_api.create_new_connection(options.test_user, options.test_pass)
        backend_api = get_backend_testing_api(options)
        messages = OffloadMessages.from_options(options, get_log_fh())
        transport_step = step_title(command_steps.STEP_STAGING_TRANSPORT)

        offload_source_table = OffloadSourceTable.create(current_schema, table_name, orchestration_config, messages)
        if offload_source_table.offload_by_subpartition_capable(valid_for_auto_enable=True):
            offload_source_table.enable_offload_by_subpartition()
        validation_fns = get_offload_transport_method_validation_fns(orchestration_config,
                                                                     offload_source_table=offload_source_table,
                                                                     messages=messages)
        timings = {}
        for staging_format in backend_api.valid_staging_formats():
            for method in VALID_OFFLOAD_TRANSPORT_METHODS:
                test_name = 'offload_transport_minus_%s_%s_%s' % (table_name.split('.')[-1], method,
                                                                  staging_format)
                if should_run_test_f(test_name):
                    if method == OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD:
                        # TODO To test this we need test infra in GCP which we don't currently have
                        log('Skipping method %s due as not valid for testing' % method, detail=verbose)
                        continue

                    if not validation_fns[method]():
                        log('Skipping method %s due to validation_fn() == False' % method, detail=verbose)
                        continue

                    offload_modifiers = {
                        'offload_transport_method': method,
                        'offload_transport_fetch_size': 500,
                        'offload_transport_small_table_threshold':
                            normalise_size_option('1G', strict_name='--offload-transport-small-table-threshold'),
                        'max_offload_chunk_count': 4,  # For gl_hash to get >1 offload chunk
                        'data_sample_pct': 0,  # Purely to speed things up a little
                    }

                    config_modifiers = {
                        'offload_staging_format': staging_format,
                        'offload_transport_livy_idle_session_timeout': 900,
                        'snowflake_file_format_prefix': orchestration_config.snowflake_file_format_prefix,
                    }

                    t = test_class(options, test_name,
                           get_offload_test_fn(current_schema, table_name, sh_test_api, backend_api,
                                               orchestration_config, messages,
                                               offload_modifiers=offload_modifiers,
                                               config_modifiers=config_modifiers))
                    offload_timings = t(capture_stdout=options.teamcity, parent_flow_id=parent_flow_id)
                    if staging_format not in timings:
                        timings[staging_format] = {}
                    if offload_timings and transport_step in offload_timings:
                        timings[staging_format][method] = offload_timings[transport_step]['seconds']
                    else:
                        timings[staging_format][method] = None

        return table_name, timings
    except Exception:
        log('Unhandled exception in %s(%s) test set:\n%s'
            % (test_constants.SET_OFFLOAD_TRANSPORT, table_name, traceback.format_exc()))
    finally:
        try:
            if frontend_api:
                frontend_api.close()
        except:
            pass


def run_offload_transport_tests(options, current_schema, orchestration_config, test_class):
    parent_flow_id = test_teamcity_starttestsuite_pq(options, test_constants.SET_OFFLOAD_TRANSPORT)

    try:
        futures = []

        all_timings = {}
        with ProcessPoolExecutor(multiprocessing.cpu_count() if options.teamcity else 1) as pool:
            for table_name in offload_transport_test_tables(orchestration_config):
                f = pool.submit(run_offload_transport_test_process, current_schema, table_name, options,
                                orchestration_config, test_class, parent_flow_id)

                futures.append(f)

            for future in as_completed(futures):
                result = future.result()
                table_name, timings = result
                for staging_format, method in timings.items():
                    if staging_format not in all_timings:
                        all_timings[staging_format] = {}
                    all_timings[staging_format][table_name] = method

            def log_results(all_timings):
                for sf, tables in all_timings.items():
                    log('Staging format: %s' % sf)
                    log('%-30s %s' % ('Table Name', ' '.join('%14s' % _ for _ in VALID_OFFLOAD_TRANSPORT_METHODS)))
                    log('%s %s' % ('='.ljust(30, '='), ' '.join('='.ljust(14, '=') for _ in VALID_OFFLOAD_TRANSPORT_METHODS)))
                    for table_name, timings in tables.items():
                        log('%-30s %s' % (table_name, ' '.join(('%13ds' % timings[_])
                                                               if timings.get(_) is not None
                                                               else ('%14s' % '') for _ in
                                                               VALID_OFFLOAD_TRANSPORT_METHODS)))

            if options.teamcity:
                output = StringIO()

                with redirect_stdout(output):
                    log_results(all_timings)

                print("##teamcity[message text='%s' flowId='%s']" %
                      (teamcity_escape(output.getvalue()), parent_flow_id))

            else:
                log_results(all_timings)

    except Exception as exc:
        log('Unhandled exception in %s test set:\n%s' % (test_constants.SET_OFFLOAD_TRANSPORT, traceback.format_exc()))
        test_teamcity_failtest_pq(options, 'run_offload_transport', 'Fail: %s' % str(exc), parent_flow_id)
    finally:
        test_teamcity_endtestsuite_pq(options, test_constants.SET_OFFLOAD_TRANSPORT, parent_flow_id)
