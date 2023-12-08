"""Run orchestration commands via the Orchestration Listener REST API"""

# Standard Library
from datetime import datetime
from time import sleep
from typing import TYPE_CHECKING

# Third Party Libraries
import requests

# Gluent
from goe.gluent import get_log_fh, serialize_object, verbose, vverbose
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_metadata_functions import OFFLOAD_TYPE_INCREMENTAL
from goe.orchestration import command_steps, orchestration_constants
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
)

from test_sets.stories.story_globals import (
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_ORACLE,
    STORY_SETUP_TYPE_PYTHON,
    STORY_TYPE_SETUP,
)
from test_sets.stories.story_setup_functions import (
    LOWER_YRMON_NUM,
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_LIST_HV_1,
    SALES_BASED_LIST_HV_3,
    SALES_BASED_LIST_HV_4,
    SALES_BASED_LIST_PNAME_3,
    SALES_BASED_LIST_PNAME_4,
    UPPER_YRMON_NUM,
    drop_backend_test_table,
    gen_hybrid_drop_ddl,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    gen_sales_based_list_create_ddl,
)
from test_sets.stories.story_assertion_functions import (
    offload_dim_assertion,
    offload_fact_assertions,
    offload_lpa_fact_assertion,
)
from test_sets.stories.test_stories.cli_api import get_log_path
from test_sets.stories.test_stories.offload_pbo import pbo_assertion
from testlib.test_framework.backend_testing_api import subproc_cmd
from testlib.test_framework.test_functions import get_lines_from_log, log

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from testlib.test_framework.backend_testing_api import BackendTestingApiInterface
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface


API_DIM = 'STORY_LSNR_DIM'
API_RPA = 'STORY_LSNR_RPA'
API_LPA = 'STORY_LSNR_LPA'
API_PBO = 'STORY_LSNR_PBO'
API_IU_DIM = 'STORY_LSNR_IU_D'

COMMAND_POLL_SECONDS = 15
COMMAND_POLL_TIMEOUT_SECONDS = 360


class TestListenerOrchestrationException(Exception):
    pass


def listener_active(orchestration_config):
    return bool(orchestration_config and orchestration_config.listener_host and orchestration_config.listener_port)


def wait_for_execution_to_complete(execution_id, repo_client, sleep_after_poll=False):
    # Wait for the execution to complete (or time out).
    for _ in range(int(COMMAND_POLL_TIMEOUT_SECONDS / COMMAND_POLL_SECONDS)):
        log(f'Polling execution ID {str(execution_id)}: {_} ({datetime.now()})', detail=vverbose)
        if not sleep_after_poll:
            sleep(COMMAND_POLL_SECONDS)
        command_payload = repo_client.get_command_execution(execution_id)
        if command_payload is None:
            raise TestListenerOrchestrationException(f'Execution was not found in repository: {str(execution_id)}')
        status = command_payload.get('status_code')
        if status == orchestration_constants.COMMAND_SUCCESS:
            # The command is complete
            log(f'Command success polled at: {datetime.now()}', detail=vverbose)
            return True
        elif status == orchestration_constants.COMMAND_ERROR:
            # The command is complete but failed
            log(f'Command error polled at: {datetime.now()}', detail=vverbose)
            raise TestListenerOrchestrationException(f'Command failed, command status: {status}')
        elif status != orchestration_constants.COMMAND_EXECUTING:
            raise TestListenerOrchestrationException(f'Unknown command status: {status}')
        if sleep_after_poll:
            sleep(COMMAND_POLL_SECONDS)
    # If we got to here then we've timed out
    raise TestListenerOrchestrationException(f'Command execution has timed out: {str(execution_id)}')


def wait_for_verification_to_complete(execution_id, options):
    # This is a non-execute command so we cannot monitor via repo.
    # TODO need to think how we can wait long enough for this? Just sleep 15 perhaps?
    log_dir = get_log_path()
    grep_command = ['find', log_dir, '-type', 'f', '-name', 'offload*.log',
                    '-exec', 'grep', '-l', str(execution_id), '{}', ';']
    log(f'Running subproc_cmd: {grep_command}', detail=verbose)
    messages = OffloadMessages.from_options(options, get_log_fh())
    returncode, output = subproc_cmd(grep_command, options, messages, cwd=log_dir)
    log('subproc_cmd return code: %s' % returncode, detail=vverbose)
    log('subproc_cmd output: %s' % output, detail=vverbose)
    if returncode != 0:
        raise TestListenerOrchestrationException(
            f'Unable to locate log file for execution id: {str(execution_id)}'
        )

    log_file = output.strip()
    # Wait for the log file to reach its (near) conclusion (or time out).
    for _ in range(int(COMMAND_POLL_TIMEOUT_SECONDS / COMMAND_POLL_SECONDS)):
        log(f'Polling execution ID {str(execution_id)}: {_} ({datetime.now()})', detail=vverbose)
        sleep(COMMAND_POLL_SECONDS)
        verify_match = get_lines_from_log(command_steps.step_title(command_steps.STEP_VERIFY_EXPORTED_DATA),
                                          file_name_override=log_file)
        if verify_match:
            # The log file is complete(ish).
            log(f'Command success polled at: {datetime.now()}', detail=vverbose)
            return True
    # If we got to here then we've timed out
    raise TestListenerOrchestrationException(f'Command execution has timed out: {str(execution_id)}')


def submit_dict_to_api(options, repo_client, param_dict: dict, endpoint: str, no_wait: bool=False) -> ExecutionId:
    """
    Submit param_dict as JSON to Orchestration Listener and wait for the execution to complete.
    """
    assert param_dict
    assert endpoint

    # No version checks when testing
    param_dict['ver_check'] = False

    url = f'http://{options.listener_host}:{options.listener_port}/api/orchestration/{endpoint}'
    headers = {"Content-Type": "application/json"}
    if options.listener_shared_token:
        headers.update({"x-gluent-console-key": options.listener_shared_token})
    data = serialize_object(param_dict)
    log(f'POST data: {data}', detail=verbose)
    r = requests.post(url, data=data, headers=headers)
    if r.ok:
        log(f'Response text: {r.text}', detail=vverbose)
        execution_id = r.json().get('execution_id')
        if not execution_id:
            raise TestListenerOrchestrationException(f'Execution ID missing from response:{r.json()}')

        execution_id = ExecutionId.from_str(execution_id)
        if not no_wait:
            # All API orchestration commands are execute=True so the else below will not be executed.
            # Retained the code in case we reverse this behaviour.
            if param_dict.get('execute', True):
                wait_for_execution_to_complete(execution_id, repo_client)
            else:
                wait_for_verification_to_complete(execution_id, options)

        return execution_id
    else:
        log(f'Response text: {r.text}')
        raise TestListenerOrchestrationException(f'Unexpected REST API status: {r.status_code}')


def submit_offload_to_api(options, repo_client, param_dict: dict) -> ExecutionId:
    return submit_dict_to_api(options, repo_client, param_dict, 'offload')


def submit_multiple_offloads_to_api(options, repo_client, list_of_param_dicts: list):
    execution_ids = []
    for param_dict in list_of_param_dicts:
        execution_ids.append(submit_dict_to_api(options, repo_client, param_dict, 'offload', no_wait=True))
    assert len(execution_ids) == len(list_of_param_dicts)
    # All ExecutionIds should be different
    assert len(execution_ids) == len(set([_.id for _ in execution_ids]))
    # All Offloads have been requested, now let's wait for completion. Exceptions will be thrown if there's a problem.
    for i, execution_id in enumerate(execution_ids):
        # For subsequent checks we want sleep_after_poll=True to speed up checking.
        wait_for_execution_to_complete(execution_id, repo_client, sleep_after_poll=bool(i > 0))


def listener_orchestration_story_tests(schema: str, hybrid_schema: str, data_db: str,
                                       options: "OrchestrationConfig",
                                       backend_api: "BackendTestingApiInterface",
                                       frontend_api: "FrontendTestingApiInterface",
                                       repo_client: "OrchestrationRepoClientInterface") -> list:
    """Run orchestration commands via the Orchestration Listener REST API"""

    if not listener_active(options):
        log('Skipping listener_api_story_tests() because listener_active() == False')
        return []

    dim_reset_offload_params = {'owner_table': f'{schema}.{API_DIM}',
                                'reset_backend_table': True}
    rpa_reset_offload_params = {'owner_table': f'{schema}.{API_RPA}',
                                'older_than_date': SALES_BASED_FACT_HV_1,
                                'reset_backend_table': True}
    lpa_reset_offload_params = {'owner_table': f'{schema}.{API_LPA}',
                                'equal_to_values': [SALES_BASED_LIST_HV_1],
                                'offload_partition_lower_value': LOWER_YRMON_NUM,
                                'offload_partition_upper_value': UPPER_YRMON_NUM,
                                'reset_backend_table': True}
    pbo_reset_offload_params = {'owner_table': f'{schema}.{API_PBO}',
                                'offload_predicate': 'column(prod_subcategory) = string("Camcorders")',
                                'reset_backend_table': True}

    return [
        {'id': 'listener_api_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Dimension for API Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, API_DIM,
                                                                             pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID:
                       gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, API_DIM),
                   STORY_SETUP_TYPE_PYTHON:
                       [lambda: drop_backend_test_table(options, backend_api, data_db, API_DIM)]}},
        {'id': 'listener_api_dim_offload_run1',
         'type': STORY_TYPE_SETUP,
         'title': 'API Dimension Simple Offload',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client, dim_reset_offload_params)]},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, API_DIM),
              lambda test: True),
         ]},
        {'id': 'listener_api_dim_offload_run2',
         'type': STORY_TYPE_SETUP,
         'title': 'API Dimension Advanced Offload',
         'narrative': 'Offload including as many different parameter types as possible (str, bool, CSV, list, int)',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client,
                                            {'owner_table': f'{schema}.{API_DIM}',
                                             'reset_backend_table': True,
                                             'integer_4_columns_csv': 'prod_id,prod_subcategory_id',
                                             'integer_8_columns_csv': 'prod_category_id',
                                             'date_columns_csv': 'prod_eff_from',
                                             'decimal_columns_csv_list': ['PROD_LIST_PRICE'],
                                             'decimal_columns_type_list': ['12,2'],
                                             'data_sample_pct': 0})]},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, API_DIM),
              lambda test: True),
         ]},
         # TODO Add a test using --target-name. Anything else we should include?
        {'id': 'listener_api_rpa_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE Fact for API Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, API_RPA,
                                                               extra_pred='AND EXTRACT(DAY FROM time_id) <= 10',
                                                               simple_partition_names=True),
                   STORY_SETUP_TYPE_HYBRID:
                       gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, API_RPA),
                   STORY_SETUP_TYPE_PYTHON:
                       [lambda: drop_backend_test_table(options, backend_api, data_db, API_RPA)]}},
        {'id': 'listener_api_rpa_offload1',
         'type': STORY_TYPE_SETUP,
         'title': 'API RANGE Offload 1st Partition',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client, rpa_reset_offload_params)]},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, API_RPA, SALES_BASED_FACT_HV_1,
                                                    check_backend_rowcount=True)},
        {'id': 'listener_api_rpa_offload2',
         'type': STORY_TYPE_SETUP,
         'title': 'API RANGE Partition Append Offload',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client,
                                            {'owner_table': f'{schema}.{API_RPA}',
                                             'older_than_date': SALES_BASED_FACT_HV_3})]},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, API_RPA, SALES_BASED_FACT_HV_3,
                                                    check_backend_rowcount=True)},
        {'id': 'listener_api_pbo_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Dimension for Listener PBO Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, API_PBO, pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID:
                       gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, API_PBO),
                   STORY_SETUP_TYPE_PYTHON:
                       [lambda: drop_backend_test_table(options, backend_api, data_db, API_PBO)]}},
        {'id': 'listener_api_pbo_offload',
         'type': STORY_TYPE_SETUP,
         'title': 'Offload %s' % API_PBO,
         'narrative': 'API PBO Offload',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client, pbo_reset_offload_params)]},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, API_PBO),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, API_PBO,
                                         number_of_predicates=1,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=['Camcorders'],
                                         columns_in_cnt_agg_rule=['prod_subcategory']),
              lambda test: True),
             ]},
        {'id': 'listener_api_lpa_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create LIST Fact for API Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       lambda: gen_sales_based_list_create_ddl(frontend_api, schema, API_LPA),
                   STORY_SETUP_TYPE_HYBRID:
                       gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, API_LPA),
                   STORY_SETUP_TYPE_PYTHON:
                       [lambda: drop_backend_test_table(options, backend_api, data_db, API_LPA)]}},
        {'id': 'listener_api_lpa_offload1',
         'type': STORY_TYPE_SETUP,
         'title': 'API LPA Offload by HV',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client, lpa_reset_offload_params)]},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, API_LPA,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_LIST_HV_1], check_rowcount=True,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True)
         ]},
         {'id': 'listener_api_lpa_offload2',
         'type': STORY_TYPE_SETUP,
         'title': 'API LPA Offload by Partition Name',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_offload_to_api(options, repo_client,
                                            {'owner_table': f'{schema}.{API_LPA}',
                                             'partition_names_csv': ','.join([SALES_BASED_LIST_PNAME_3,
                                                                              SALES_BASED_LIST_PNAME_4])})]},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, API_LPA,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3,
                                                       SALES_BASED_LIST_HV_4], check_rowcount=True,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True)
         ]},
        # Tests below disabled because listener API does not yet support Incremental Update
        # {'id': 'listener_api_iu_setup',
        #  'type': STORY_TYPE_SETUP,
        #  'title': f'Setup {API_IU_DIM} for IU testing',
        #  'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, API_IU_DIM,
        #                                                                      pk_col='prod_id'),
        #            STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, API_IU_DIM,
        #                                                         drop_incr=True),
        #            STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, API_IU_DIM)}},
        # {'id': 'listener_api_iu_changelog_offload',
        #  'type': STORY_TYPE_SETUP,
        #  'title': 'API Enable CHANGELOG Incremental Update',
        #  'setup': {STORY_SETUP_TYPE_PYTHON:
        #      [lambda: submit_offload_to_api(options, repo_client,
        #                                     {'owner_table': f'{schema}.{API_IU_DIM}',
        #                                      'incremental_updates_enabled': True,
        #                                      'incremental_extraction_method': CHANGELOG,
        #                                      'reset_backend_table': True})]}},
        # {'id': 'listener_api_iu_changelog_ship',
        #  'type': STORY_TYPE_SETUP,
        #  'title': 'API Incremental Update Ship',
        #  'narrative': 'There are no changes to ship but that does not stop us monitoring the command for success',
        #  'setup': {STORY_SETUP_TYPE_PYTHON:
        #      [lambda: submit_offload_to_api(options, repo_client,
        #                                     {'owner_table': f'{schema}.{API_IU_DIM}',
        #                                      'incremental_batch_size': 1,
        #                                      'incremental_run_extraction': True})]}},
        # {'id': 'listener_api_iu_changelog_disable',
        #  'type': STORY_TYPE_SETUP,
        #  'title': 'API Disable Incremental Update',
        #  'setup': {STORY_SETUP_TYPE_PYTHON:
        #      [lambda: submit_offload_to_api(options, repo_client,
        #                                     {'owner_table': f'{schema}.{API_IU_DIM}',
        #                                      'incremental_updates_disabled': True})]}},
        # TODO Add more tests when further command functionality is added to the listener.
        {'id': 'listener_api_concurrent_offloads',
         'type': STORY_TYPE_SETUP,
         'title': 'Multiple Concurrent Offloads',
         'setup': {STORY_SETUP_TYPE_PYTHON:
             [lambda: submit_multiple_offloads_to_api(options, repo_client,
                                                      [rpa_reset_offload_params,
                                                       lpa_reset_offload_params,
                                                       dim_reset_offload_params,
                                                       pbo_reset_offload_params])]}},
    ]
