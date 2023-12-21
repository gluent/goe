import os
from socket import gethostname

from test_sets.stories.story_globals import STORY_TYPE_SETUP, STORY_TYPE_SHELL_COMMAND
from test_sets.stories.story_setup_functions import gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl,\
    get_backend_drop_incr_fns
from test_sets.stories.test_stories.cli_api import get_bin_path, get_conf_path, get_log_path, get_run_path,\
    goe_shell_command

from goe.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_transport import is_spark_submit_available, is_sqoop_available, \
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT, OFFLOAD_TRANSPORT_METHOD_SQOOP, OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY
from goe.offload.offload_transport_functions import ssh_cmd_prefix
from goe.util.orchestration_lock import LOCK_FILE_PREFIX, LOCK_FILE_SUFFIX
from testlib.test_framework.test_functions import log


CLI_BACKEND_TABLE = 'story_cli_nossh_be'
CLI_RDBMS_TABLE = 'STORY_CLI_NOSSH_DIM'
CLI_BASE_OFFLOADED_TABLE = 'CHANNELS'


def goe_nossh_command(list_of_args, list_only, extra_envs=None):
    """ Return list_of_args supplemented with other args if relevant """
    if list_only:
        return []
    if extra_envs:
        assert isinstance(extra_envs, list)
        assert isinstance(extra_envs[0], str)

    cmd_args = goe_shell_command(list_of_args)

    gluent = os.getenv('OFFLOAD_TRANSPORT_USER') or os.getenv('HADOOP_SSH_USER')
    assert gluent == 'gluent', '%s != gluent' % gluent
    env_modifiers = ['HDFS_HOST=localhost',
                     'OFFLOAD_TRANSPORT_CMD_HOST=localhost',
                     'SQOOP_HOST=localhost']
    if extra_envs:
        env_modifiers.extend(extra_envs)
    cmd_with_env = ['source', os.path.join(get_conf_path(), 'offload.env'), '&&'] + env_modifiers + cmd_args
    cmd = ssh_cmd_prefix(gluent, 'localhost') + [' '.join(cmd_with_env)]
    return cmd


def local_hadoop_or_spark(options):
    """ Detect whether we can include SSH shortcircuit tests. when either:
        1. A local Hadoop cluster
        2. A local Spark cluster
        We detect this by looking at our own config. If OFFLOAD_TRANSPORT_CMD_HOST is localhost or the current
        host name then we're looking for Hadoop or Spark components on the current host.
    """
    if not options:
        # Pretend we plan to include the tests for --list-stories benefit.
        return True
    return bool(options.offload_transport_cmd_host == 'localhost' or
                options.offload_transport_cmd_host in gethostname())


def jvm_overrides():
    return """\"-Dgluent.fake.option=$'SET \\"CELL_OFFLOAD_PROCESSING\\"=FALSE;'  -Dx.y=z\"""",


def setup_chmod_commands(options):
    if not options:
        return []
    os_cmds = [['chmod', 'o+w', get_log_path()],
               ['chmod', 'o+r', os.path.join(get_conf_path(), 'offload.env')],
               # Having to use find because asterisk is not expanded by shell (due to subprocess escaping)
               ['find', get_conf_path(), '-name', '*.template', '-exec', 'chmod', 'o+r', '{}', ';'],
               # I have a feeling this is hiding a real problem, see comment in FileLockOrchestrationLock.acquire()
               ['chmod', 'o+w', get_run_path()],
               ['find', get_run_path(), '-name', LOCK_FILE_PREFIX + '*' + LOCK_FILE_SUFFIX,
                '-exec', 'chmod', 'o+rw', '{}', ';']]
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        os_cmds.append(['chmod', 'o+r', os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')])
    if options.password_key_file:
        os_cmds.append(['chmod', 'o+r', options.password_key_file])
    return os_cmds


def cli_api_nossh_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                              list_only, repo_client):
    if not list_only:
        if not options:
            return []

    if not local_hadoop_or_spark(options):
        log('Skipping cli_api_nossh_story_tests() as not applicable to this environment')
        return []

    bin_path = get_bin_path()
    cli_backend_table_be = convert_backend_identifier_case(options, CLI_BACKEND_TABLE)
    cli_rdbms_table_be = convert_backend_identifier_case(options, CLI_RDBMS_TABLE)

    return [
        {'id': 'cli_api_nossh_setup',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Setup',
         'narrative': """SSH Shortcircuit tests run as gluent but our test software is installed as oracle,
                         therefore chmod on OFFLOAD_HOME/log, and other files/locations, is required""",
         'shell_commands': setup_chmod_commands(options)},
        {'id': 'cli_api_nossh_connect_check',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Connect',
         'shell_commands': [goe_nossh_command([os.path.join(bin_path, 'connect')], list_only)],
         'acceptable_return_codes': [0, 2, 3]},
        {'id': 'cli_api_nossh_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RDBMS Dimension for CLI Tests',
         'setup': {'oracle': gen_rdbms_dim_create_ddl(frontend_api, schema, CLI_RDBMS_TABLE, pk_col='prod_id'),
                   'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, CLI_RDBMS_TABLE),
                   'python': get_backend_drop_incr_fns(options, backend_api, data_db, cli_rdbms_table_be)}},
        {'id': 'cli_api_nossh_offload1',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Offload',
         'shell_commands': [goe_nossh_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_RDBMS_TABLE,
                                               '--reset-backend-table', '-x', '--no-create-aggregations'], list_only)],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_rdbms_table_be), lambda test: True)]},
        {'id': 'cli_api_nossh_offload2',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Offload (no-webhdfs)',
         'shell_commands': [goe_nossh_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_RDBMS_TABLE,
                                               '--reset-backend-table', '-x', '--no-create-aggregations'],
                                              list_only,
                                              extra_envs=['WEBHDFS_HOST='])],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_rdbms_table_be), lambda test: True)],
         'prereq': lambda: bool(options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS)},
        {'id': 'cli_api_nossh_offload_sqoop1',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Offload (Sqoop)',
         'shell_commands': [goe_nossh_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_RDBMS_TABLE,
                                               '--reset-backend-table', '-x', '--no-create-aggregations',
                                               '--offload-transport-method=%s' % OFFLOAD_TRANSPORT_METHOD_SQOOP,
                                               '--offload-transport-jvm-overrides=%s' % jvm_overrides()], list_only)],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_rdbms_table_be), lambda test: True)],
         'prereq': lambda: is_sqoop_available(None, options)},
        {'id': 'cli_api_nossh_offload_sqoop2',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Offload (Sqoop-by-query)',
         'shell_commands': [goe_nossh_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_RDBMS_TABLE,
                                               '--reset-backend-table', '-x', '--no-create-aggregations',
                                               '--offload-transport-method=%s'
                                               % OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                                               '--offload-transport-jvm-overrides=%s' % jvm_overrides()], list_only)],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_rdbms_table_be), lambda test: True)],
         'prereq': lambda: is_sqoop_available(None, options)},
        {'id': 'cli_api_nossh_offload_spark',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'SSH Shortcircuit Offload (Spark)',
         'shell_commands': [goe_nossh_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + CLI_RDBMS_TABLE,
                                               '--reset-backend-table', '-x', '--no-create-aggregations',
                                               '--offload-transport-method=%s'
                                               % OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT], list_only)],
         'assertion_pairs': [(lambda test: backend_api.table_exists(data_db, cli_rdbms_table_be), lambda test: True)],
         'prereq': lambda: is_spark_submit_available(options, None)},
    ]
