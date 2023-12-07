import os

from gluentlib.config import orchestration_defaults
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.schema_sync.schema_sync_constants import (
    EXCEPTION_SCHEMA_EVOLUTION_NOT_SUPPORTED,
)
from test_sets.stories.story_assertion_functions import (
    backend_table_exists,
)
from test_sets.stories.story_globals import (
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_ORACLE,
    STORY_SETUP_TYPE_PYTHON,
    STORY_TYPE_SETUP,
    STORY_TYPE_SHELL_COMMAND,
)
from test_sets.stories.story_setup_functions import (
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    drop_backend_test_table,
    gen_hybrid_drop_ddl,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    get_backend_drop_incr_fns,
    setup_backend_only,
)

CLI_BACKEND_TABLE = 'STORY_CLI_BACKEND_ONLY'
CLI_DIM = 'STORY_CLI_DIM'
CLI_FACT = 'STORY_CLI_FACT'
CLI_SS_DIM = 'STORY_CLI_SS_DIM'
CLI_BASE_OFFLOADED_TABLE = 'CHANNELS'


def get_bin_path():
    offload_home = os.environ.get('OFFLOAD_HOME')
    assert offload_home, 'OFFLOAD_HOME must be set in order to run tests'
    return os.path.join(offload_home, 'bin')


def get_conf_path():
    offload_home = os.environ.get('OFFLOAD_HOME')
    assert offload_home, 'OFFLOAD_HOME must be set in order to run tests'
    return os.path.join(offload_home, 'conf')


def get_log_path():
    offload_home = os.environ.get('OFFLOAD_HOME')
    assert offload_home, 'OFFLOAD_HOME must be set in order to run tests'
    return os.path.join(offload_home, 'log')


def get_run_path():
    offload_home = os.environ.get('OFFLOAD_HOME')
    assert offload_home, 'OFFLOAD_HOME must be set in order to run tests'
    return os.path.join(offload_home, 'run')


def command_supports_no_version_check(list_of_args):
    return bool(list_of_args[0].endswith('connect') or
                list_of_args[0].endswith('offload'))


def goe_shell_command(list_of_args):
    """ Return list_of_args supplemented with other args if relevant """
    suffix_args = []

    if not os.getenv('TEAMCITY_PROJECT_NAME') and command_supports_no_version_check(list_of_args):
        # No TeamCity so not worried about version match
        suffix_args.append('--no-version-check')

    return list_of_args + suffix_args


def cli_api_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                        list_only, repo_client):
    if list_only:
        smart_connector_test_command = None
    else:
        if not options:
            return []
        channels_be = convert_backend_identifier_case(options, 'channels')
        smart_connector_test_command = backend_api.smart_connector_test_command(db_name=data_db, table_name=channels_be)

    bin_path = get_bin_path()
    cli_backend_table_be = convert_backend_identifier_case(options, CLI_BACKEND_TABLE)
    cli_dim_be = convert_backend_identifier_case(options, CLI_DIM)
    cli_ss_dim_be = convert_backend_identifier_case(options, CLI_SS_DIM)

    return [
        {'id': 'cli_api_connect_help',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Connect -h',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'connect'), '-h'])]},
        {'id': 'cli_api_connect_check',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Basic Connect',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'connect')])],
         'acceptable_return_codes': [0, 2, 3]},
        {'id': 'cli_api_backend_only_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Backend Table for CLI Tests',
         'setup': setup_backend_only(backend_api, frontend_api, hybrid_schema, data_db, CLI_BACKEND_TABLE,
                                     options, repo_client, row_limit=50),
         'assertion_pairs': [(lambda test: backend_table_exists(backend_api, data_db, cli_backend_table_be),
                              lambda test: True)]},
        {'id': 'cli_api_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RDBMS Dimension for CLI Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, CLI_DIM,
                                                                             pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, CLI_DIM),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, cli_dim_be)}},
        {'id': 'cli_api_dim_offload_help',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload -h',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'), '-h'])]},
        {'id': 'cli_api_dim_offload_version',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload --version',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'), '--version'])]},
        {'id': 'cli_api_dim_offload_run1',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_DIM,
                                               '--reset-backend-table', '-x', '--no-create-aggregations'])],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_dim_be),
                              lambda test: True)]},
        {'id': 'cli_api_dim_offload_run2',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Complex Offload',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'),
                                               '-t', schema + '.' + CLI_DIM,
                                               '-x', '--force',
                                               '--reset-backend-table',
                                               '--skip-steps=xyz',
                                               '--allow-decimal-scale-rounding',
                                               '--allow-floating-point-conversions',
                                               '--allow-nanosecond-timestamp-columns',
                                               '--compress-load-table',
                                               '--data-sample-parallelism=2',
                                               '--max-offload-chunk-count=4',
                                               '--offload-fs-scheme={}'.format(orchestration_defaults.offload_fs_scheme_default()),
                                               '--no-verify'])],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_dim_be), lambda test: True)]},
        {'id': 'cli_api_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RDBMS Fact for CLI Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, CLI_FACT,
                                                               extra_pred='AND EXTRACT(DAY FROM time_id) <= 10',
                                                               simple_partition_names=True),
                   STORY_SETUP_TYPE_HYBRID:
                       gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, CLI_FACT),
                   STORY_SETUP_TYPE_PYTHON:
                       [lambda: drop_backend_test_table(options, backend_api, data_db, CLI_FACT)]}},
        {'id': 'cli_api_fact_offload1',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload of Partitions',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_FACT,
                                               '--reset-backend-table', '-x',
                                               '--older-than-date', SALES_BASED_FACT_HV_2])],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_dim_be), lambda test: True)]},
        {'id': 'cli_api_fact_offload2',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI IPA Offload',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_FACT,
                                               '-x', '-v', '--older-than-date', SALES_BASED_FACT_HV_3])]},
        {'id': 'cli_api_schema_sync_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RDBMS Dimension for CLI Tests',
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_rdbms_dim_create_ddl(frontend_api, schema, CLI_SS_DIM, pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, CLI_SS_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: backend_api.drop_table(data_db, cli_ss_dim_be, sync=True)]}},
        {'id': 'cli_api_schema_sync_dim_offload',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_SS_DIM,
                                               '--reset-backend-table', '-x', '--no-create-aggregations'])],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_ss_dim_be), lambda test: True)]},
        {'id': 'cli_api_schema_sync_dim_add_col',
         'type': STORY_TYPE_SETUP,
         'title': 'Add Column to CLI_BACKEND_TABLE',
         'narrative': 'Add Column to CLI_BACKEND_TABLE to facilitate running schema_sync',
         'setup': {STORY_SETUP_TYPE_ORACLE: ['ALTER TABLE %(schema)s.%(table)s ADD ( reject_code NUMBER(1) )'
                                             % {'schema': schema, 'table': CLI_SS_DIM}]},
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.gluent_schema_sync_supported()},
        {'id': 'cli_api_schema_sync_run',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Schema Sync',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'schema_sync'),
                                               '--include', schema + '.' + CLI_SS_DIM, '-x'])],
         'expected_exception_string': (None if backend_api and backend_api.schema_evolution_supported()
                                       else EXCEPTION_SCHEMA_EVOLUTION_NOT_SUPPORTED),
         'prereq': lambda: frontend_api.gluent_schema_sync_supported()},
        {'id': 'cli_api_agg_validate1',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'agg_validate'), '-t', schema + '.' + CLI_DIM])]},
        {'id': 'cli_api_agg_validate2',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'agg_validate'), '-t', schema + '.' + CLI_DIM,
                                               '-x'])]},
        {'id': 'cli_api_diagnose_help',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Diagnose -h',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'diagnose'), '-h'])]},
        {'id': 'cli_api_diagnose_version',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Diagnose --version',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'diagnose'), '--version'])]},
        {'id': 'cli_api_diagnose_run',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Diagnose',
         'shell_commands': [goe_shell_command([os.path.join(bin_path, 'diagnose'),
                                               '-t', schema + '.' + CLI_BASE_OFFLOADED_TABLE, '-x'])],
         'prereq': lambda: frontend_api.gluent_diagnose_supported()},
        {'id': 'cli_api_smart_connector',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Smart Connector Interactive',
         'shell_commands': [[os.path.join(bin_path, 'smart_connector'), '-iq', smart_connector_test_command]],
         'prereq': lambda: frontend_api.hybrid_schema_supported()},
        {'id': 'cli_api_rcpurge',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI RC Purge',
         'shell_commands': [[os.path.join(bin_path, 'rcpurge')]]},
        {'id': 'cli_api_rcpurge_help',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI RC Purge Help',
         'shell_commands': [[os.path.join(bin_path, 'rcpurge'), '-h']]},
        {'id': 'cli_api_osr',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload Status Report',
         'shell_commands': [[os.path.join(bin_path, 'offload_status_report'), '-s', schema,
                             '-t', CLI_BASE_OFFLOADED_TABLE]],
         'prereq': lambda: frontend_api.gluent_offload_status_report_supported()},
        {'id': 'cli_api_osr_help',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'CLI Offload Status Report Help',
         'shell_commands': [[os.path.join(bin_path, 'offload_status_report'), '-s', schema,
                             '-t', CLI_BASE_OFFLOADED_TABLE, '-h']],
         'prereq': lambda: frontend_api.gluent_offload_status_report_supported()},
    ]
