"""
LICENSE_TEXT
"""
import copy
from datetime import datetime
from optparse import SUPPRESS_HELP
import os
import random
import re
import socket
import subprocess
import sys
import traceback

from getpass import getuser
from cx_Oracle import DatabaseError
from gluent import ansi, comp_ver_check, get_common_options, \
    get_log_fh, get_log_fh_name, init, init_log, log as offload_log, log_command_line, log_timestamp, \
    nls_lang_exists, nls_lang_has_charset, \
    oracle_connection, oracle_offload_transport_connection, set_nls_lang_default, version, \
    OptionValueError, normal, verbose, \
    CONFIG_FILE_NAME, NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.config import orchestration_defaults
from gluentlib.connect.connect_constants import CONNECT_DETAIL, CONNECT_STATUS, CONNECT_TEST
from gluentlib.filesystem.cli_hdfs import CliHdfs
from gluentlib.filesystem.web_hdfs import WebHdfs
from gluentlib.filesystem.gluent_dfs import get_scheme_from_location_uri, \
    OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES, OFFLOAD_NON_HDFS_FS_SCHEMES, uri_component_split
from gluentlib.filesystem.gluent_dfs_factory import get_dfs_from_options
from gluentlib.listener.utils.ping import ping as ping_listener
from gluentlib.offload.offload_constants import DBTYPE_IMPALA, DBTYPE_SPARK, DBTYPE_MSSQL, DBTYPE_ORACLE,\
    HADOOP_BASED_BACKEND_DISTRIBUTIONS, BACKEND_DISTRO_GCP, LOG_LEVEL_DEBUG
from gluentlib.offload.backend_api import BackendApiConnectionException
from gluentlib.offload.factory.backend_api_factory import backend_api_factory
from gluentlib.offload.factory.frontend_api_factory import frontend_api_factory
from gluentlib.offload.offload_messages import OffloadMessages, VVERBOSE
from gluentlib.offload.offload_transport_functions import credential_provider_path_jvm_override, ssh_cmd_prefix
from gluentlib.offload.offload_transport import (
    OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
    URL_SEP,
    LIVY_SESSIONS_SUBURL,
    spark_submit_executable_exists,
    is_spark_gcloud_available,
    is_spark_submit_available,
    is_spark_thrift_available,
    is_livy_available,
    spark_dataproc_jdbc_connectivity_checker,
    spark_submit_jdbc_connectivity_checker,
    spark_thrift_jdbc_connectivity_checker,
    spark_livy_jdbc_connectivity_checker,
    sqoop_jdbc_connectivity_checker
)
from gluentlib.offload.offload_transport_livy_requests import OffloadTransportLivyRequests
from gluentlib.orchestration import orchestration_constants
from gluentlib.util.better_impyla import BetterImpylaException
from gluentlib.util.gluent_log import log_exception
from gluentlib.util.redis_tools import RedisClient


TEST_HDFS_DIRS_SERVICE_HDFS = 'HDFS'
TEST_HDFS_DIRS_SERVICE_WEBHDFS = 'WebHDFS'

CONNECT_HIVE_TIMEOUT_S = 60


class ConnectException(Exception):
    pass


class FatalTestFailure(Exception):
    # Use this instead of sys.exit(1)
    pass


def log(line: str, detail: int=normal, ansi_code=None):
    """Write log entry but without Redis interaction."""
    offload_log(line, detail=detail, ansi_code=ansi_code, redis_publish=False)


def section_header(h):
    log('\n%s' % h, ansi_code='underline')


def test_header(h):
    log('\n%s' % h)


def detail(d):
    log(str(d), ansi_code='grey')


def success(t):
    log('%s %s' % (t, ansi('Passed', 'green')))


def failure(t, hint=None):
    log('%s %s' % (t, ansi('Failed', 'red')))
    global failures
    failures = True
    if hint:
        log(hint, ansi_code='magenta')


def skipped(t):
    log('%s %s' % (t, ansi('Skipped', 'yellow')))


def warning(t, hint=None):
    log('%s %s' % (t, ansi('Warning', 'yellow')))
    global warnings
    warnings = True
    if hint:
        log(hint, ansi_code='magenta')


def debug(d):
    if not isinstance(d, str):
        log(str(d), detail=VVERBOSE)
    else:
        log(d, detail=VVERBOSE)


def get_cli_hdfs(orchestration_config, host, messages):
    return CliHdfs(host, orchestration_config.hadoop_ssh_user, dry_run=(not orchestration_config.execute),
                   messages=messages, db_path_suffix=orchestration_config.hdfs_db_path_suffix,
                   hdfs_data=orchestration_config.hdfs_data)


def get_backend_api(options, orchestration_config, messages=None, dry_run=False):
    api_messages = messages or OffloadMessages.from_options(options, get_log_fh())
    return backend_api_factory(orchestration_config.target, orchestration_config, api_messages, dry_run=dry_run)


def ssh_hdfs(ssh_options):
    return ssh_cmd_prefix(ssh_options['hadoop_ssh_user'], ssh_options['hdfs_host'])


def get_one_host_from_option(option_host_value):
    """ simple function but there were at least 3 different techniques in play for this so
        standardising here
    """
    return random.choice(option_host_value.split(',')) if option_host_value else None


def test_oracle_connectivity(orchestration_config):
    test_name = 'Oracle connectivity'
    test_header(test_name)
    try:
        for user_type, gluent_user, connection_fn in [('app user',
                                                       orchestration_config.rdbms_app_user,
                                                       oracle_offload_transport_connection),
                                                      ('admin user',
                                                       orchestration_config.ora_adm_user,
                                                       oracle_connection)]:
            if orchestration_config.use_oracle_wallet:
                display_text = 'using Oracle Wallet'
            else:
                display_text = gluent_user.upper()
            detail('Testing %s (%s)' % (user_type, display_text))
            cx = connection_fn(orchestration_config)
    except DatabaseError as exc:
        detail(str(exc))
        failure(test_name)
        raise FatalTestFailure

    # success -> cx is a gluent_adm connection
    success(test_name)
    return cx


def test_oracle(orchestration_config, messages):
    test_name = 'Oracle NLS_LANG'
    test_header(test_name)
    if not nls_lang_exists():
        orchestration_config.db_type = 'oracle'
        set_nls_lang_default(orchestration_config)
        detail('NLS_LANG not specified in environment, this will be set at offload time to "%s"' % os.environ['NLS_LANG'])
        failure(test_name)
    else:
        if not nls_lang_has_charset():
            detail(NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE % os.environ['NLS_LANG'])
            failure(test_name)
            raise FatalTestFailure
        else:
            detail(os.environ['NLS_LANG'])
            success(test_name)

    cx = test_oracle_connectivity(orchestration_config)
    frontend_api = frontend_api_factory(orchestration_config.db_type, orchestration_config, messages, dry_run=True,
                                        existing_connection=cx, trace_action='Connect')

    test_name = 'Oracle version'
    test_header(test_name)
    ov = frontend_api.frontend_version()
    detail(ov)
    ovc = ov.split('.')
    if int(ovc[0]) > 11 or (int(ovc[0]) == 11 and int(ovc[1]) > 1 and int(ovc[3]) > 3):
        success(test_name)
    else:
        failure(test_name)

    test_name = 'Oracle component version'
    test_header(test_name)
    test_hint = """Please ensure you have upgraded the OFFLOAD package in your Oracle database:

 $ cd $OFFLOAD_HOME/setup
 SQL> @upgrade_offload"""

    match, v_goe, v_ora = comp_ver_check(frontend_api)
    if match:
        detail('Oracle component version matches binary version')
        success(test_name)
    elif v_goe[-3:] == '-RC':
        detail('Binary version is release candidate (RC) cannot verify match with Oracle component version')
        success(test_name)
    else:
        detail('Mismatch between Oracle component version (' + v_ora + ') and binary version (' + v_goe + ')!')
        failure(test_name, test_hint)

    test_name = 'Oracle charactersets (IANA)'
    test_header(test_name)
    sql = "SELECT PARAMETER, UTL_I18N.MAP_CHARSET(VALUE) IANA FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER IN ('NLS_NCHAR_CHARACTERSET','NLS_CHARACTERSET') ORDER BY 1"
    r = frontend_api.execute_query_fetch_all(sql)
    for row in r:
        detail('%s: %s' % (row[0], row[1]))

    attrs = ['DB_UNIQUE_NAME', 'SESSION_USER']

    for attr in attrs:
        try:
            test_name = 'Oracle %s' % (attr)
            test_header(test_name)
            r = frontend_api.execute_query_fetch_one('SELECT SYS_CONTEXT(\'USERENV\', \'' + attr + '\') FROM dual')[0]
            detail(r)
            success(test_name)

        except Exception as exc:
            detail(exc)
            failure(test_name)

    test_name = 'Oracle Parameters'
    test_header(test_name)

    db_parameters = ['processes', 'sessions', 'query_rewrite_enabled', '_optimizer_cartesian_enabled']
    binds = {}

    for count, value in enumerate(db_parameters):
        binds['b' + str(count)] = value

    sql = 'select name, value from gv$parameter where name in (%s) group by name, value order by 1,2' % ','.join([':' + _ for _ in binds])
    db_parameter_values = frontend_api.execute_query_fetch_all(sql, query_params=binds)
    col1_width = max(max([len(row[0]) for row in db_parameter_values]), len('Parameter'))
    col2_width = max(max([len(row[1]) for row in db_parameter_values]), len('Value'))
    parameter_format = '{0:' + str(col1_width) + '}     {1:>' + str(col2_width) + '}'
    detail(parameter_format.format('Parameter', 'Value'))
    for row in db_parameter_values:
        detail(parameter_format.format(row[0], row[1]))

    cx.close()


def test_webhdfs_config(orchestration_config, messages):
    test_name = 'WebHDFS configuration'
    test_header(test_name)
    if not orchestration_config.webhdfs_host:
        detail('WebHDFS host/port not supplied, using shell commands for HDFS operations (hdfs dfs, scp, etc)')
        detail('Utilizing WebHDFS will reduce latency of Offload operations')
        warning(test_name)
    else:
        webhdfs_security = (['Kerberos'] if orchestration_config.kerberos_service else []) + ([] if orchestration_config.webhdfs_verify_ssl is None else ['SSL'])
        webhdfs_security = ('using ' +  ' and '.join(webhdfs_security)) if webhdfs_security else 'unsecured'
        detail('HDFS operations will use WebHDFS (%s:%s) %s' % (orchestration_config.webhdfs_host,
                                                                orchestration_config.webhdfs_port,
                                                                webhdfs_security))
        success(test_name)

        hdfs = WebHdfs(orchestration_config.webhdfs_host, orchestration_config.webhdfs_port,
                       orchestration_config.hadoop_ssh_user,
                       True if orchestration_config.kerberos_service else False,
                       orchestration_config.webhdfs_verify_ssl, dry_run=not orchestration_config.execute,
                       messages=messages, db_path_suffix=orchestration_config.hdfs_db_path_suffix,
                       hdfs_data=orchestration_config.hdfs_data)
        test_hdfs_dirs(orchestration_config, messages, hdfs=hdfs, test_host=orchestration_config.webhdfs_host,
                       service_name=TEST_HDFS_DIRS_SERVICE_WEBHDFS)


def static_backend_name(orchestration_config):
    """ We would normally rely on BackendApi to tell us its name but this test is checking we can
        create a BackendApi object (which connects to the backend). So, unfortunately, we need a display
        name independent of the Api.
    """
    if is_hadoop_environment(orchestration_config):
        return orchestration_config.hadoop_host
    elif orchestration_config.backend_distribution == BACKEND_DISTRO_GCP:
        return 'BigQuery'
    else:
        return orchestration_config.target.capitalize()


def static_frontend_name(orchestration_config):
    """ We would normally rely on FrontendApi to tell us its name but this test is checking we can
        create a FrontendApi object (which connects to the frontend). So, unfortunately, we need a display
        name independent of the Api.
    """
    if orchestration_config.db_type == DBTYPE_MSSQL:
        return orchestration_config.db_type.upper()
    else:
        return orchestration_config.db_type.capitalize()


def test_backend_db_connectivity(options, orchestration_config, messages):
    test_name = '%s connectivity' % static_backend_name(orchestration_config)
    try:
        test_header(test_name)
        backend_api = get_backend_api(options, orchestration_config, messages=messages)
        success(test_name)
        return backend_api
    except BackendApiConnectionException as exc:
        log(traceback.format_exc())
        sys.exit(1)
    except Exception as exc:
        failure(test_name)
        if orchestration_config.hadoop_host and orchestration_config.hadoop_port:
            detail('Connectivity failed with: %s - Performing network socket test' % str(exc))
            test_raw_conn(orchestration_config.hadoop_host, orchestration_config.hadoop_port)
        else:
            log(traceback.format_exc(), detail=verbose)
            detail('Connectivity failed with: %s' % str(exc))
        sys.exit(1)


def test_frontend_db_connectivity(orchestration_config, messages):
    frontend_api = None
    test_name = '%s connectivity' % static_frontend_name(orchestration_config)
    try:
        test_header(test_name)
        frontend_api = frontend_api_factory(orchestration_config.db_type, orchestration_config, messages,
                                            dry_run=True, trace_action='Connect')
        success(test_name)
    except Exception as exc:
        failure(test_name)
        log(traceback.format_exc(), detail=verbose)
        detail('Connectivity failed with: %s' % str(exc))
        sys.exit(1)

    if orchestration_config.rdbms_app_user:
        test_name = '%s transport user connectivity' % static_frontend_name(orchestration_config)
        try:
            test_header(test_name)
            # Ignore the client returned below, it is no use to us in connect.
            frontend_api_factory(orchestration_config.db_type, orchestration_config, messages,
                                 conn_user_override=orchestration_config.rdbms_app_user,
                                 dry_run=True, trace_action='Connect')
            success(test_name)
        except Exception as exc:
            failure(test_name)
            log(traceback.format_exc(), detail=verbose)
            detail('Connectivity failed with: %s' % str(exc))

    return frontend_api


def test_raw_conn(hadoop_host, hadoop_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    test_name = 'Network socket test for %s:%s' % (hadoop_host, int(hadoop_port))
    test_header(test_name)
    try:
        s.connect((hadoop_host, int(hadoop_port)))
    except Exception as exc:
        detail(exc)
        failure(test_name)
        sys.exit(2)
    success(test_name)


def test_ssh(orchestration_config):
    def normalise_host_list(list_of_hosts):
        """ ensure no CSVs or empty values
        """
        expanded_list = []
        [expanded_list.extend(_.split(',')) if _ else [] for _ in list_of_hosts]
        return set(expanded_list)

    test_name = None
    try:
        for host in normalise_host_list([orchestration_config.hdfs_host,
                                         orchestration_config.offload_transport_cmd_host]):
            ssh_user = orchestration_config.hadoop_ssh_user
            if host == orchestration_config.offload_transport_cmd_host:
                test_name = '%s (OFFLOAD_TRANSPORT_CMD_HOST): password-less ssh' % host
                ssh_user = orchestration_config.offload_transport_user or ssh_user
            elif host == orchestration_config.hdfs_host:
                test_name = '%s (HDFS_CMD_HOST): password-less ssh' % host
                if not is_hadoop_environment(orchestration_config):
                    test_header(test_name)
                    detail('Skipping SSH test in non-Hadoop environment')
                    continue
            else:
                test_name = '%s (HIVE_SERVER_HOST): password-less ssh' % host
            test_header(test_name)
            if host == 'localhost' and ssh_user == getuser():
                detail('Skipping SSH test because user is current user and host is localhost')
                continue

            cmd = ssh_cmd_prefix(ssh_user, host=host) + ['-o StrictHostKeyChecking=no', 'id']
            detail(' '.join(cmd))
            groups = subprocess.check_output(cmd)
            detail(groups.strip())

            success(test_name)

    except:
        failure(test_name)
        raise


def test_credential_api_alias(options, orchestration_config):
    if not orchestration_config.offload_transport_password_alias:
        return

    host = orchestration_config.offload_transport_cmd_host or get_one_host_from_option(options.original_hadoop_host)
    test_name = 'Offload Transport Password Alias'
    test_header(test_name)
    jvm_overrides = []
    if orchestration_config.sqoop_overrides or orchestration_config.offload_transport_spark_overrides:
        jvm_overrides.append(orchestration_config.sqoop_overrides or orchestration_config.offload_transport_spark_overrides)
    if orchestration_config.offload_transport_credential_provider_path:
        jvm_overrides.append(credential_provider_path_jvm_override(orchestration_config.offload_transport_credential_provider_path))
    # Using hadoop_ssh_user below and not offload_transport_user because this is running a Hadoop CLI command
    cmd = ssh_cmd_prefix(orchestration_config.hadoop_ssh_user, host=host) + ['hadoop', 'credential'] + jvm_overrides + ['list']

    try:
        log('Cmd: %s' % ' '.join(cmd), detail=VVERBOSE)
        cmd_out = subprocess.check_output(cmd)
        m = re.search(r'^%s[\r]?$' % re.escape(orchestration_config.offload_transport_password_alias), cmd_out, re.M | re.I)

        if m:
            detail('Found alias: %s' % m.group())
            success(test_name)
        else:
            detail('Alias "%s" not found in Hadoop credential API' % orchestration_config.offload_transport_password_alias)
            failure(test_name)

    except Exception as e:
        detail(str(e))
        failure(test_name)


def test_sqoop_pwd_file(options, orchestration_config, messages):
    if not orchestration_config.sqoop_password_file:
        return

    test_host = get_one_host_from_option(options.original_hadoop_host)

    test_name = '%s: Sqoop Password File' % test_host
    test_header(test_name)
    hdfs = get_cli_hdfs(orchestration_config, test_host, messages)
    if hdfs.stat(orchestration_config.sqoop_password_file):
        detail('%s found in HDFS' % orchestration_config.sqoop_password_file)
        success(test_name)
    else:
        detail('Sqoop Password File not found: %s' % orchestration_config.sqoop_password_file)
        failure(test_name)


def test_sentry_privs(orchestration_config, backend_api, messages):
    """ The code in here makes some backend specific assumptions regarding cursor format
        Because Sentry is CDH Impala only and expected to be deprecated in CDP we've not tried to
        generalise this functionality. It remains largely as it was before multiple backend support
    """
    if not backend_api.sentry_supported():
        log('Skipping Sentry steps due to backend system', detail=VVERBOSE)
        return

    dfs_client = get_dfs_from_options(orchestration_config, messages)
    uris_left_to_check = get_hdfs_dirs(orchestration_config, dfs_client, include_hdfs_home=False)
    passed = True
    test_hint = None

    test_name = '%s: Sentry privileges' % orchestration_config.hadoop_host
    try:
        test_header(test_name)
        all_on_server = False

        q = 'SHOW CURRENT ROLES'
        detail(q)
        rows = backend_api.execute_query_fetch_all(q)
        for r in rows:
            q = 'SHOW GRANT ROLE %s' % r[0]
            detail(q)
            backend_cursor = backend_api.execute_query_get_cursor(q)
            detail('Sentry is enabled')
            detail([c[0] for c in backend_cursor.description])
            priv_pos = [c[0] for c in backend_cursor.description].index('privilege')
            uri_pos = [c[0] for c in backend_cursor.description].index('uri')
            for r in backend_cursor.fetchall():
                detail(r)
                if (r[0].upper(), r[priv_pos].upper()) == ('SERVER', 'ALL'):
                    all_on_server = True
                if (r[0].upper(), r[priv_pos].upper()) == ('URI', 'ALL'):
                    for chk_uri in uris_left_to_check[:]:
                        _, _, hdfs_path = uri_component_split(r[uri_pos])
                        if chk_uri.startswith((r[uri_pos], hdfs_path)):
                            uris_left_to_check.remove(chk_uri)
                            detail('Gluent target URI %s is covered by this privilege' % chk_uri)
    except BetterImpylaException as exc:
        if any(_ in str(exc) for _ in ('incomplete and disabled', 'Authorization is not enabled')):
            detail('Sentry is not enabled')
            success(test_name)
        elif 'AnalysisException: Cannot execute authorization statement using a file based policy' in str(exc):
            detail('Cannot determine permissions from file based Sentry policy')
            warning(test_name)
        else:
            raise
        return
    except Exception as exc:
        failure(test_name)
        raise

    if uris_left_to_check and not all_on_server:
        detail('URIs not covered by Sentry privileges: %s' % str(uris_left_to_check))
        passed = False
        test_hint = 'Grant ALL on URIs to cover the relevant locations'

    if passed:
        success(test_name)
    else:
        failure(test_name, test_hint)


def test_ranger_privs(orchestration_config, backend_api, messages):

    if not backend_api.ranger_supported():
        log('Skipping Ranger steps due to backend system', detail=VVERBOSE)
        return

    passed = True
    test_name = '%s: Ranger privileges' % orchestration_config.hadoop_host
    test_header(test_name)
    # Remove any @REALM from Kerberos principal
    user = backend_api.get_user_name().split('@')[0]
    debug('Backend username: %s' % user)
    dfs_client = get_cli_hdfs(orchestration_config, orchestration_config.hdfs_host, messages)

    def run_ranger_query(sql, validations, list_missing=True):
        """ Run SQL in Impala to determine user grants.
            Validations is a list of expected outcomes as dictionaries.
            Validations are removed from the list when matched, with the unmatched
            remainder returned to the caller, or an empty list if all matched.
        """
        try:
            detail(sql)
            backend_cursor = backend_api.execute_query_get_cursor(sql)
            detail([c[0] for c in backend_cursor.description])
            test_validations = copy.copy(validations)

            for r in backend_cursor.fetchall():
                detail(r)
                for validation in test_validations:
                    matched = 0
                    for col in validation:
                        col_pos = [c[0] for c in backend_cursor.description].index(col)
                        if r[col_pos].upper() == validation[col].upper():
                            matched += 1
                    if matched == len(validation):
                        if validation in validations:
                            validations.remove(validation)
            if list_missing:
                for missing in validations:
                    warning('Missing required privilege: %s' % missing)
            return validations
        except BetterImpylaException as exc:
            if 'authorization is not enabled' in str(exc).lower():
                return 'Ranger is not enabled'
            else:
                raise
        except Exception:
            failure(test_name)
            raise

    # Check if we have ALL ON SERVER
    all_required = [{'database': '*', 'privilege': 'all'},
                    {'table': '*', 'privilege': 'all'},
                    {'column': '*', 'privilege': 'all'},
                    {'uri': '*', 'privilege': 'all'},
                    {'udf': '*', 'privilege': 'all'}]
    all_query = 'SHOW GRANT USER %s ON SERVER' % backend_api.enclose_identifier(user)
    all_result = run_ranger_query(all_query, all_required, list_missing=False)
    if all_result and all_result == 'Ranger is not enabled':
        detail('Ranger is not enabled')
        success(test_name)
        return
    detail('Ranger is enabled')
    if not all_result:
        # We have ALL ON SERVER so can do what we need to
        detail('Able to perform all required operations')
    else:
        # Can we create Gluent Data Platform databases
        detail('\nDatabase Creation')
        db_required = [{'privilege': 'create', 'database': '*', 'table': '*', 'column': '*'}]
        db_query = 'SHOW GRANT USER %s ON SERVER' % backend_api.enclose_identifier(user)
        db_result = run_ranger_query(db_query, db_required)

        db_uri_results = []
        dirs = get_hdfs_dirs(orchestration_config, dfs_client, include_hdfs_home=False)

        for dir in dirs:
            db_uri_required = [{'privilege': 'all'}]
            db_uri_query = 'SHOW GRANT USER %s ON URI \'%s\'' % (backend_api.enclose_identifier(user), dir)
            db_uri_results += run_ranger_query(db_uri_query, db_uri_required)

        if not db_result and not db_uri_results:
            detail('Able to create Impala databases')
        else:
            passed = False
            detail('User grants indicate Impala databases cannot be created')

    if passed:
        success(test_name)
    else:
        test_hint = 'Refer to Gluent Data Platform documentation for the required Ranger privileges'
        warning(test_name, test_hint)


def get_hdfs_dirs(orchestration_config, dfs_client, service_name=TEST_HDFS_DIRS_SERVICE_HDFS, include_hdfs_home=True):
    """ return a list of HDFS directories but NOT as a set(), we want to retain the order so
        using an "if" to ensure no duplicate output
    """
    dirs = []
    if include_hdfs_home:
        dirs.append(orchestration_config.hdfs_home)
    dirs.append(orchestration_config.hdfs_load)
    offload_data_uri = dfs_client.gen_uri(orchestration_config.offload_fs_scheme,
                                          orchestration_config.offload_fs_container,
                                          orchestration_config.offload_fs_prefix)
    if offload_data_uri not in dirs:
        if service_name == TEST_HDFS_DIRS_SERVICE_HDFS or orchestration_config.offload_fs_scheme in OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES:
            dirs.append(offload_data_uri)
    return dirs


def check_dir_with_msgs(hdfs_client, chk_dir, hdfs_data, msgs):
    passed = True
    dir_scheme = get_scheme_from_location_uri(chk_dir).upper()
    file_stat = hdfs_client.stat(chk_dir)
    if file_stat:
        msgs.append('%s found in %s' % (chk_dir, dir_scheme))
        if chk_dir == hdfs_data:
            if 'permission' not in file_stat:
                msgs.append('Unable to read path permissions for %s' % chk_dir)
                passed = False
            elif file_stat['permission'] and file_stat['permission'][1] in ('3', '6', '7'):
                msgs.append('%s is group writable' % chk_dir)
            else:
                msgs.append('%s is NOT group writable' % chk_dir)
                passed = False
    else:
        msgs.append('%s is NOT present in %s' % (chk_dir, dir_scheme))
        passed = False
    return passed


def test_hdfs_dirs(orchestration_config, messages, hdfs=None, test_host=None, service_name=TEST_HDFS_DIRS_SERVICE_HDFS):
    test_host = test_host or orchestration_config.hdfs_host or orchestration_config.hadoop_host
    test_name = '%s: %s directory' % (test_host, service_name)
    test_header(test_name)

    # Setting this up for each host to prove directories visible regardless of WebHDFS usage.
    # WebHDFS will only test namenode knows of them, not test each node is configured correctly.
    use_hdfs = hdfs or get_cli_hdfs(orchestration_config, test_host, messages)
    passed = True
    msgs = []
    test_hdfs_home = False if (
            orchestration_config.offload_fs_scheme and orchestration_config.offload_fs_scheme in OFFLOAD_NON_HDFS_FS_SCHEMES) else True

    for chk_dir in get_hdfs_dirs(orchestration_config, use_hdfs, service_name, include_hdfs_home=test_hdfs_home):
        try:
            if not check_dir_with_msgs(use_hdfs, chk_dir, orchestration_config.hdfs_data, msgs):
                passed = False
        except Exception as exc:
            detail('%s: %s' % (chk_dir, exc))
            detail(traceback.format_exc())
            passed = False

    for line in msgs:
        detail(line)

    if passed:
        success(test_name)
    else:
        failure(test_name)


def test_os_version():
    test_name = 'Operating system version'
    try:
        test_header(test_name)

        if not os.path.isfile('/etc/redhat-release') and not os.path.isfile('/etc/SuSE-release'):
            detail('Unsupported operating system')
            failure(test_name)
            return
        os_ver = ''
        if os.path.isfile('/etc/redhat-release'):
            cmd = ['cat', '/etc/redhat-release']
            os_ver = subprocess.check_output(cmd).decode()
        elif os.path.isfile('/etc/SuSE-release'):
            cmd = ['cat', '/etc/SuSE-release']
            out = subprocess.check_output(cmd).decode().splitlines()
            os_ver = '%s (%s)' % (out[:1][0], ', '.join([o for o in out[1:]]))
        cmd = ['uname', '-r']
        kern_ver = subprocess.check_output(cmd).decode()
        detail('%s - %s' % (os_ver.rstrip(), kern_ver.rstrip()))
        success(test_name)

    except Exception:
        failure(test_name)
        raise


def test_krb_bin(orchestration_config):
    test_name = 'Path to kinit (Kerberos)'
    try:
        test_header(test_name)
        if orchestration_config.kerberos_service:
            cmd = ['which', 'kinit']
            kinit_path = subprocess.check_output(cmd).decode()
            detail('kinit found: %s' % (kinit_path.rstrip('\n')))
            success(test_name)
        else:
            detail('Use of Kerberos (KERBEROS_SERVICE) not configured in environment')
            success(test_name)

    except Exception:
        failure(test_name)
        raise


def configured_num_location_files(options, orchestration_config):
    num_loc_files = options.num_location_files or orchestration_defaults.num_location_files_default()
    num_buckets_max = orchestration_config.num_buckets_max or orchestration_defaults.num_buckets_max_default()
    if orchestration_config.target == DBTYPE_IMPALA:
        # On Impala present is capped by num_location_files, offload is capped by num_buckets_max
        return max(num_loc_files, num_buckets_max)
    else:
        return num_loc_files


def get_environment_file_name(orchestration_config):
    frontend_id = orchestration_config.db_type.lower()
    if is_hadoop_environment(orchestration_config):
        backend_id = 'hadoop'
    else:
        backend_id = orchestration_config.target.lower()
    return '-'.join([frontend_id, backend_id, CONFIG_FILE_NAME + '.template'])


def get_environment_file_path():
    return os.path.join(os.environ.get('OFFLOAD_HOME'), 'conf', CONFIG_FILE_NAME)


def get_template_file_path(orchestration_config):
    template_name = get_environment_file_name(orchestration_config)
    return os.path.join(os.environ.get('OFFLOAD_HOME'), 'conf', template_name)


def test_conf_perms():
    test_name = 'Configuration file permissions'
    test_header(test_name)
    hint = 'Expected permissions are 640'
    environment_file = get_environment_file_path()
    perms = oct(os.stat(environment_file).st_mode & 0o777)
    # Removing oct prefix deemed safe for display only.
    detail('%s has permissions: %s' % (environment_file, perms[2:]))
    if perms[-3:] != '640':
        warning(test_name, hint)
    else:
        success(test_name)


def test_dir(dir_name, expected_perms):
    test_name = 'Directory permissions: %s' % dir_name
    hint = 'Expected permissions are %s' % expected_perms
    test_header(test_name)
    log_dir = os.path.join(os.environ.get('OFFLOAD_HOME'), dir_name)
    perms = oct(os.stat(log_dir).st_mode & 0o2777)
    # Removing oct prefix deemed safe for display only.
    detail('%s has permissions: %s' % (log_dir, perms[2:]))
    if perms[-4:] != expected_perms:
        failure(test_name, hint)
    else:
        success(test_name)


def test_log_level(orchestration_config):
    test_name = 'Logging level'
    test_header(test_name)
    if orchestration_config.log_level == LOG_LEVEL_DEBUG:
        detail('LOG_LEVEL of "%s" should only be used under the guidance of Gluent Support'
               % orchestration_config.log_level)
        warning(test_name)
    else:
        detail('LOG_LEVEL: %s' % orchestration_config.log_level)
        success(test_name)


def test_listener(orchestration_config):
    test_name = orchestration_constants.PRODUCT_NAME_GEL
    test_header(test_name)
    if not orchestration_config.listener_host and orchestration_config.listener_port is None:
        detail(f'{orchestration_constants.PRODUCT_NAME_GEL} not configured')
        success(test_name)
        return

    try:
        # Check Listener is up
        if ping_listener(orchestration_config):
            detail('Listener ping successful: {}:{}'.format(orchestration_config.listener_host,
                                                            orchestration_config.listener_port))
        else:
            detail('Listener ping unsuccessful')
            # If the listener status failed then no need to check the cache status
            failure(test_name)
            return
    except Exception as exc:
        detail(str(exc))
        log(traceback.format_exc(), detail=verbose)
        # If the listener status failed then no need to check the cache status
        failure(test_name)
        return

    try:
        # Check Redis is up
        if orchestration_defaults.cache_enabled():
            # We're expecting to interact with Redis.
            cache = RedisClient.connect()
            if cache.ping():
                detail('Listener cache found: {}:{}'.format(orchestration_defaults.listener_redis_host_default(),
                                                            orchestration_defaults.listener_redis_port_default()))
                success(test_name)
            else:
                detail('Cache ping unsuccessful')
                warning(test_name)
    except Exception as exc:
        detail(str(exc))
        log(traceback.format_exc(), detail=verbose)
        failure(test_name)


def run_frontend_tests(orchestration_config, messages):
    if orchestration_config.db_type == DBTYPE_ORACLE:
        test_oracle(orchestration_config, messages)
    else:
        frontend_api = test_frontend_db_connectivity(orchestration_config, messages)
        test_name = '{} version'.format(static_frontend_name(orchestration_config))
        test_header(test_name)
        detail(frontend_api.frontend_version())


def test_sqoop_import(orchestration_config, messages):
    data_transport_client = sqoop_jdbc_connectivity_checker(orchestration_config, messages)
    verify_offload_transport_rdbms_connectivity(data_transport_client, 'Sqoop')


def run_sqoop_tests(options, orchestration_config, messages):
    test_sqoop_pwd_file(options, orchestration_config, messages)
    test_sqoop_import(orchestration_config, messages)


def is_spark_thrift_connector_available(orchestration_config):
    return bool(orchestration_config.spark_thrift_host
                and orchestration_config.spark_thrift_port
                and orchestration_config.connector_sql_engine == DBTYPE_SPARK)


def run_spark_tests(options, orchestration_config, messages):
    if not (is_livy_available(orchestration_config, None)
            or is_spark_submit_available(orchestration_config, None)
            or is_spark_gcloud_available(orchestration_config, None)
            or is_spark_thrift_available(orchestration_config, None)
            or is_spark_thrift_connector_available(orchestration_config)):
        log('Skipping Spark tests because not configured', detail=VVERBOSE)
        return

    section_header('Spark')
    test_spark_thrift_server(options, orchestration_config, messages)
    test_spark_submit(orchestration_config, messages)
    test_spark_gcloud(orchestration_config, messages)
    test_spark_livy_api(orchestration_config, messages)


def verify_offload_transport_rdbms_connectivity(data_transport_client, transport_type):
    test_name = 'RDBMS connection: %s' % transport_type
    test_header(test_name)
    try:
        if data_transport_client.ping_source_rdbms():
            detail('Connection successful')
            success(test_name)
        else:
            failure(test_name)
    except Exception as exc:
        debug(traceback.format_exc())
        detail(str(exc))
        failure(test_name)


def test_spark_thrift_server(options, orchestration_config, messages):
    hosts_to_check, extra_connector_hosts = set(), set()
    if options.original_offload_transport_spark_thrift_host and orchestration_config.offload_transport_spark_thrift_port:
        hosts_to_check = set((_, orchestration_config.offload_transport_spark_thrift_port)
                             for _ in options.original_offload_transport_spark_thrift_host.split(','))
        log('Spark thrift hosts for Offload transport: %s' % str(hosts_to_check), detail=VVERBOSE)

    if not hosts_to_check:
        log('Skipping Spark Thrift Server tests due to absent config', detail=VVERBOSE)
        return

    detail('Testing Spark Thrift Server hosts individually: %s' % ', '.join(host for host, _ in hosts_to_check))
    spark_options = copy.copy(orchestration_config)
    for host, port in hosts_to_check:
        spark_options.hadoop_host = host
        spark_options.hadoop_port = port
        backend_spark_api = test_backend_db_connectivity(spark_options, orchestration_config, messages)
        del backend_spark_api

    # test_backend_db_connectivity() will exit on failure so we know it is sound to proceed if we get this far
    if orchestration_config.offload_transport_spark_thrift_host and orchestration_config.offload_transport_spark_thrift_port:
        # we only connect back to the RDBMS from Spark for offload transport
        data_transport_client = spark_thrift_jdbc_connectivity_checker(orchestration_config, messages)
        verify_offload_transport_rdbms_connectivity(data_transport_client, 'Spark Thrift Server')


def test_spark_livy_api(orchestration_config, messages):
    if not orchestration_config.offload_transport_livy_api_url:
        log('Skipping Spark Livy tests due to absent config', detail=VVERBOSE)
        return

    test_name = 'Spark Livy settings'
    test_header(test_name)
    try:
        sessions_url = URL_SEP.join([orchestration_config.offload_transport_livy_api_url, LIVY_SESSIONS_SUBURL])
        detail(sessions_url)
        livy_requests = OffloadTransportLivyRequests(orchestration_config, messages)
        resp = livy_requests.get(sessions_url)
        if resp.ok:
            log('Good Livy response: %s' % str(resp.text), detail=VVERBOSE)
        else:
            detail('Response code: %s' % resp.status_code)
            detail('Response text: %s' % resp.text)
            resp.raise_for_status()
        success(test_name)
    except Exception as exc:
        detail(str(exc))
        failure(test_name)
        # no sense in more Livy checks if this fails
        return

    data_transport_client = spark_livy_jdbc_connectivity_checker(orchestration_config, messages)
    verify_offload_transport_rdbms_connectivity(data_transport_client, 'Livy Server')


def test_spark_submit(orchestration_config, messages):
    if not orchestration_config.offload_transport_spark_submit_executable or not orchestration_config.offload_transport_cmd_host:
        log('Skipping Spark Submit tests due to absent config', detail=VVERBOSE)
        return

    test_name = 'Spark Submit settings'
    test_header(test_name)
    if spark_submit_executable_exists(orchestration_config, messages):
        detail('Executable %s exists' % orchestration_config.offload_transport_spark_submit_executable)
        success(test_name)
    else:
        detail('Executable %s does not exist' % orchestration_config.offload_transport_spark_submit_executable)
        failure(test_name)
        # no sense in more spark-submit checks if this fails
        return

    data_transport_client = spark_submit_jdbc_connectivity_checker(orchestration_config, messages)
    verify_offload_transport_rdbms_connectivity(data_transport_client, 'Spark Submit')


def test_spark_gcloud(orchestration_config, messages):
    if not orchestration_config.google_dataproc_cluster or not orchestration_config.offload_transport_cmd_host:
        log('Skipping Spark gcloud tests due to absent config', detail=VVERBOSE)
        return

    test_name = 'Spark Dataproc settings'
    test_header(test_name)
    if spark_submit_executable_exists(
        orchestration_config,
        messages,
        executable_override=OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE
    ):
        detail(f'Executable {OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE} exists')
        success(test_name)
    else:
        detail(f'Executable {OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE} does not exist')
        failure(test_name)
        # no sense in more gcloud checks if this fails
        return

    data_transport_client = spark_dataproc_jdbc_connectivity_checker(orchestration_config, messages)
    verify_offload_transport_rdbms_connectivity(data_transport_client, 'Spark Dataproc')


def run_check_backend_supporting_objects(backend_api, orchestration_config, test_container):
    for test_details in backend_api.check_backend_supporting_objects(orchestration_config):
        test_name = '%s: %s' % (test_container, test_details[CONNECT_TEST])
        test_header(test_name)
        if test_details[CONNECT_DETAIL]:
            detail(test_details[CONNECT_DETAIL])
        if test_details[CONNECT_STATUS]:
            success(test_name)
        else:
            failure(test_name)


def run_hs2_tests(options, orchestration_config, messages):
    # Tests required to pass for all listed hosts
    detail('HS2 hosts: %s' % ', '.join(orchestration_defaults.hadoop_host_default().split(',')))
    original_host_option = orchestration_config.hadoop_host
    first_host = True
    for hh in orchestration_defaults.hadoop_host_default().split(','):
        orchestration_config.hadoop_host = hh
        backend_api = test_backend_db_connectivity(options, orchestration_config, messages)
        run_check_backend_supporting_objects(backend_api, orchestration_config, hh)
        if first_host:
            test_sentry_privs(orchestration_config, backend_api, messages)
            test_ranger_privs(orchestration_config, backend_api, messages)
        if not orchestration_config.hdfs_host:
            # if hdfs_host is not specified then check directories from all Hadoop nodes
            test_hdfs_dirs(orchestration_config, messages)
        first_host = False
    orchestration_config.hadoop_host = original_host_option


def run_backend_tests(options, orchestration_config, messages):
    backend_api = test_backend_db_connectivity(options, orchestration_config, messages)
    run_check_backend_supporting_objects(backend_api, orchestration_config, static_backend_name(orchestration_config))


def dict_from_environment_file(environment_file):
    d = {}
    with open(environment_file) as f:
        for line in f:
            if re.match("^(#.*e|e)xport(.*)", line):
                (k, v) = re.sub("^(#.*e|e)xport ", "", line).split('=', 1)[0], line
                d[k] = v
    return d

def check_offload_env(environment_file, template_file):
    left = os.path.basename(environment_file)
    right = os.path.basename(template_file)
    test_name = '%s vs %s' % (left, right)
    test_header(test_name)

    try:
        configuration = dict_from_environment_file(environment_file)
    except IOError:
        debug(traceback.format_exc())
        # If offload.env does not exist then we need to abort because the OFFLOAD_HOME is in bad shape
        raise ConnectException('Unable to access environment file: %s' % environment_file)

    try:
        template = dict_from_environment_file(template_file)
    except IOError:
        debug(traceback.format_exc())
        failure(test_name, 'Unable to access template file: %s' % template_file)
        return

    test_hint = 'Remove deprecated/unsupported parameters from %s' % left
    if set(configuration.keys()) - set(template.keys()):
        for key in sorted(set(configuration.keys()) - set(template.keys())):
            detail('%s is present in %s but not in %s' % (key, left, right))
        warning(test_name, test_hint)
    else:
        detail('No entries in %s not in %s' % (left, right))
        success(test_name)

    left = os.path.basename(template_file)
    right = os.path.basename(environment_file)
    test_name = '%s vs %s' % (left, right)
    test_hint = 'Use ./connect --upgrade-environment-file to copy missing configuration from %s to %s' % (left, right)

    test_header(test_name)
    if set(template.keys()) - set(configuration.keys()):
        for key in sorted(set(template.keys()) - set(configuration.keys())):
            detail('%s is present in %s but not in %s' % (key, left, right))
        warning(test_name, test_hint)
    else:
        detail('No entries in %s not in %s' % (left, right))
        success(test_name)


def upgrade_environment_file(environment_file, template_file):
    log('Upgrading environment file using current template')

    configuration = dict_from_environment_file(environment_file)
    template = dict_from_environment_file(template_file)

    if set(template.keys()) - set(configuration.keys()):
        with open(environment_file, 'a') as f:
            f.write('\n# Below variables added by connect --upgrade-environment-file on %s - See docs.gluent.com for their usage.\n' % datetime.now().strftime('%x %X'))
            for key in sorted(set(template.keys()) - set(configuration.keys())):
                detail('Adding %s' % key)
                f.write(template[key])

        log('%s has been updated' % environment_file, ansi_code='green')
    else:
        log('No update required', ansi_code='yellow')


def is_hadoop_environment(orchestration_config):
    return bool(orchestration_config.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS)


def check_environment(options, orchestration_config):
    global warnings
    warnings = False
    global failures
    failures = False
    log('\nChecking your current Gluent Offload Engine environment...')

    section_header('Configuration')

    check_offload_env(get_environment_file_path(), get_template_file_path(orchestration_config))
    test_conf_perms()
    test_log_level(orchestration_config)

    # A number of checks require a messages object, rather than creating in multiple place we create early and re-use
    messages = OffloadMessages.from_options(options, get_log_fh())

    section_header('Frontend')

    run_frontend_tests(orchestration_config, messages)

    section_header('Backend')

    if orchestration_config.use_ssl:
        if orchestration_config.ca_cert:
            detail('Using SSL with authority certificate %s' % orchestration_config.ca_cert)
        else:
            detail('Using SSL without certificate')

    if is_hadoop_environment(orchestration_config):
        run_hs2_tests(options, orchestration_config, messages)
    else:
        run_backend_tests(options, orchestration_config, messages)

    test_ssh(orchestration_config)
    if orchestration_config.hdfs_host and is_hadoop_environment(orchestration_config):
        test_hdfs_dirs(orchestration_config, messages)
    if is_hadoop_environment(orchestration_config):
        run_sqoop_tests(options, orchestration_config, messages)
        # Important to test WebHDFS before test_offload_fs_container to ensure
        # option normalization happens
        test_webhdfs_config(orchestration_config, messages)
    test_credential_api_alias(options, orchestration_config)
    if orchestration_config.offload_fs_container:
        test_offload_fs_container(orchestration_config, messages)
    run_spark_tests(options, orchestration_config, messages)

    section_header('Local')
    test_os_version()
    test_krb_bin(orchestration_config)
    #test_listener(orchestration_config)
    if failures:
        sys.exit(2)
    if warnings:
        sys.exit(3)


def test_offload_fs_container(orchestration_config, messages):
    test_name = 'Offload filesystem container'
    test_header(test_name)
    dfs_client = get_dfs_from_options(orchestration_config, messages)
    display_uri = dfs_client.gen_uri(orchestration_config.offload_fs_scheme, orchestration_config.offload_fs_container, '')
    test_hint = 'Check existence and permissions of %s' % display_uri
    if dfs_client.container_exists(orchestration_config.offload_fs_scheme, orchestration_config.offload_fs_container):
        detail(display_uri)
        success(test_name)
    else:
        failure(test_name, test_hint)


def get_config_with_connect_overrides(connect_options):
    override_dict = {'execute': True,
                     'verbose': connect_options.verbose,
                     'hive_timeout_s': CONNECT_HIVE_TIMEOUT_S}
    config = OrchestrationConfig.from_dict(override_dict)
    return config


def get_connect_opts():

    opt = get_common_options()

    opt.remove_option('--execute')

    opt.add_option('--upgrade-environment-file', dest='upgrade_environment_file', default=False, action='store_true',
                   help="Adds missing configuration variables from the environment file template to the environment file")
    # Hidden options to keep TeamCity testing functioning
    opt.add_option('--validate-udfs', dest='validate_udfs', default=False, action='store_true', help=SUPPRESS_HELP)
    opt.add_option('--install-udfs', dest='install_udfs', default=False, action='store_true', help=SUPPRESS_HELP)
    opt.add_option('--create-backend-db', dest='create_backend_db', action='store_true', help=SUPPRESS_HELP)

    return opt


def connect():
    options = None
    try:
        opt = get_connect_opts()
        options, args = opt.parse_args()

        init(options)
        init_log('connect')
        section_header('Connect v%s' % version())
        log('Log file: %s' % get_log_fh_name())
        log_command_line()

        orchestration_config = get_config_with_connect_overrides(options)

        if is_hadoop_environment(orchestration_config) and not orchestration_config.hadoop_host:
            raise OptionValueError('HIVE_SERVER_HOST is mandatory')

        # We need the original host CSV for comprehensive checking later
        options.original_offload_transport_spark_thrift_host = orchestration_defaults.offload_transport_spark_thrift_host_default()
        options.original_hadoop_host = orchestration_defaults.hadoop_host_default()

        if options.upgrade_environment_file:
            upgrade_environment_file(get_environment_file_path(), get_template_file_path(orchestration_config))
        else:
            check_environment(options, orchestration_config)
        sys.exit(0)

    except FatalTestFailure:
        sys.exit(1) # has already been reported
    except Exception as exc:
        log('Exception caught at top-level', ansi_code='red')
        log_timestamp()
        log_exception(exc, log_fh=get_log_fh(), options=options)
        sys.exit(1)
