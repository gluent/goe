import os

from gluentlib.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)

# from test_sets.stories.story_assertion_functions import (
#    backend_table_exists,
# )
# from tests.integration.test_sets.stories.story_setup_functions import (
#    SALES_BASED_FACT_HV_2,
#    SALES_BASED_FACT_HV_3,
#    gen_sales_based_fact_create_ddl,
# )

from tests.integration.scenarios.scenario_runner import (
    ScenarioRunnerException,
    run_offload,
    run_setup,
    run_shell_cmd,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
    standard_dimension_frontend_ddl,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.integration.test_sets.stories.story_assertion_functions import (
    backend_column_exists,
    frontend_column_exists,
    hint_text_in_log,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


CLI_BACKEND_TABLE = "STORY_CLI_BACKEND_ONLY"
CLI_DIM = "STORY_CLI_DIM"
CLI_FACT = "STORY_CLI_FACT"
CLI_BASE_OFFLOADED_TABLE = "CHANNELS"


def get_bin_path():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return os.path.join(offload_home, "bin")


def get_log_path():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return os.path.join(offload_home, "log")


def get_run_path():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return os.path.join(offload_home, "run")


def command_supports_no_version_check(list_of_args):
    return bool(
        list_of_args[0].endswith("connect") or list_of_args[0].endswith("offload")
    )


def goe_shell_command(list_of_args):
    """Return list_of_args supplemented with other args if relevant"""
    suffix_args = []

    if command_supports_no_version_check(list_of_args):
        # No TeamCity so not worried about version match
        suffix_args.append("--no-version-check")

    return list_of_args + suffix_args


def test_connect():
    id = "test_connect"
    config = cached_current_options()
    messages = get_test_messages(config, id)
    bin_path = get_bin_path()

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, "connect"), "-h"])
    )

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, "connect")])
    )


def test_offload():
    id = "test_offload"
    config = cached_current_options()
    schema = cached_default_test_user()
    data_db = data_db_name(schema, config)
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages)
    cli_dim_be = convert_backend_identifier_case(config, CLI_DIM)

    bin_path = get_bin_path()

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, "offload"), "-h"])
    )

    run_shell_cmd(
        config, messages, goe_shell_command([os.path.join(bin_path, 'offload'), '--version'])
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=standard_dimension_frontend_ddl(
            frontend_api, config, schema, CLI_DIM
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, CLI_DIM
        ),
    )

    #goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_DIM, '--reset-backend-table', '-x'])

    #goe_shell_command([os.path.join(bin_path, 'offload'),
    #                                           '-t', schema + '.' + CLI_DIM,
    #                                           '-x', '--force',
    #                                           '--reset-backend-table',
    #                                           '--skip-steps=xyz',
    #                                           '--allow-decimal-scale-rounding',
    #                                           '--allow-floating-point-conversions',
    #                                           '--allow-nanosecond-timestamp-columns',
    #                                           '--compress-load-table',
    #                                           '--data-sample-parallelism=2',
    #                                           '--max-offload-chunk-count=4',
    #                                           '--offload-fs-scheme={}'.format(orchestration_defaults.offload_fs_scheme_default()),
    #                                           '--no-verify'])

    #gen_sales_based_fact_create_ddl(frontend_api, schema, CLI_FACT,
    #                                                           extra_pred='AND EXTRACT(DAY FROM time_id) <= 10',
    #                                                           simple_partition_names=True)

    #goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_FACT,
    #                                           '--reset-backend-table', '-x',
    #                                           '--older-than-date', SALES_BASED_FACT_HV_2])],
    #[(lambda test: backend_api.table_exists(data_db, cli_dim_be), lambda test: True)]},
#
#    goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_FACT,
#                                               '-x', '-v', '--older-than-date', SALES_BASED_FACT_HV_3])
