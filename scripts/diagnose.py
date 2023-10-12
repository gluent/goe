# LICENSE_TEXT

import os
import pprint
import re
import sys
from datetime import datetime
from optparse import OptionGroup, OptionValueError

from gluent import OptionError,\
    init_log, version, get_log_fh_name, log_command_line, init, log, log_timestamp, get_log_fh, \
    get_common_options, hybrid_owner, parse_size_expr
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.config.orchestration_defaults import hadoop_host_default, offload_transport_cmd_host_default
from gluentlib.offload.factory.backend_table_factory import backend_table_factory
from gluentlib.offload.offload_constants import DBTYPE_IMPALA, DBTYPE_HIVE, DBTYPE_SPARK,\
    DBTYPE_BIGQUERY, DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE
from gluentlib.offload.offload_functions import data_db_name
from gluentlib.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from gluentlib.orchestration import command_steps, orchestration_constants
from gluentlib.persistence.orchestration_metadata import OrchestrationMetadata
from gluentlib.util.diagnostics import Diagnose, DiagnoseException, PermissionsTuple
from gluentlib.util.gluent_log import log_exception


DEFAULT_IMPALAD_HTTP_PORT = 25000
DEFAULT_HS2_WEBUI_PORT = 10002
DEFAULT_LLAP_WEBUI_PORT = 10502
DEFAULT_SPARK_HISTORY_PORT = 18081
# Days to search back for Spark applications
SPARK_HISTORIC_APPLICATION_DAYS = 7
DENSE = pprint.PrettyPrinter(indent=2, width=100)

BACKEND_LOGS_SUPPORTED = [DBTYPE_IMPALA, DBTYPE_HIVE, DBTYPE_SPARK]
BACKEND_CONFIG_SUPPORTED = [DBTYPE_IMPALA, DBTYPE_HIVE, DBTYPE_SPARK, DBTYPE_SYNAPSE]
QUERY_LOGS_SUPPORTED = [DBTYPE_IMPALA, DBTYPE_HIVE, DBTYPE_BIGQUERY, DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE]


def get_diagnose_opts():

    opt = get_common_options(usage='usage: %prog [options]')

    opt.add_option('--output-location', dest='output_location',
                   help='Location in which to save files created by this tool. Default: OFFLOAD_HOME/log',
                   default='%s/log' % os.environ['OFFLOAD_HOME'])
    opt.add_option('--retain-created-files', dest='retain_created_files',
                   help='By default, after they have been packaged, files created by this tool in --output-location are removed. Specify this parameter to retain them', action='store_true')
    opt.add_option('-t', '--table', dest='diag_include_table_metadata', metavar='OWNER.TABLE',
                   help='Optional owner and table-name of offloaded or presented table, e.g. OWNER.TABLE. Collect metadata (e.g. DDL, statistics) from source and backend for this table and all dependent Gluent objects')

    logs_group = OptionGroup(opt, "Log Options", "Options for collecting log files.")
    logs_group.add_option('--log-location', dest='log_location',
                          help='Location in which to search for log files. Default: OFFLOAD_HOME/log',
                          default='%s/log' % os.environ['OFFLOAD_HOME'])
    logs_group.add_option('--include-logs-last', dest='diag_include_logs_last_n', metavar='<n><d|h>',
                          help='Collate and package log files modified or created in the last n [d]ays (e.g. 3d) or [h]ours (e.g. 7h)')
    logs_group.add_option('--include-logs-from', dest='diag_include_logs_from', metavar='YYYY-MM-DD[_HH24:MM:SS]',
                          help='Collate and package log files modified or created since date (format: YYYY-MM-DD) or date/time (format: YYYY-MM-DD_HH24:MM:SS). Can be used in conjunction with the --include-logs-to parameter to specify a search range')
    logs_group.add_option('--include-logs-to', dest='diag_include_logs_to', metavar='YYYY-MM-DD[_HH24:MM:SS]',
                          help='Collate and package log files modified or created since date (format: YYYY-MM-DD) or date/time (format: YYYY-MM-DD_HH24:MM:SS). Can be used in conjunction with the --include-logs-from parameter to specify a search range')
    opt.add_option_group(logs_group)

    backend_group = OptionGroup(opt, "Backend Component Options",
                                "Options for collecting information about backend components (not applicable to all backend distributions).")
    backend_group.add_option('--include-backend-logs', dest='diag_include_backend_logs',
                             help='Retrieve backend query engine logs. Applies to: %s'
                                  % ', '.join([_.upper() for _ in BACKEND_LOGS_SUPPORTED]), action='store_true')
    backend_group.add_option('--include-backend-config', dest='diag_include_backend_config',
                             help='Retrieve backend query engine config. Applies to: %s'
                                  % ', '.join([_.upper() for _ in BACKEND_CONFIG_SUPPORTED]), action='store_true')
    backend_group.add_option('--include-query-logs', dest='diag_include_query_logs', metavar='QUERY ID',
                             help='Retrieve logs for supplied query id. Applies to: %s'
                                  % ', '.join([_.upper() for _ in QUERY_LOGS_SUPPORTED]))
    backend_group.add_option('--backend-log-size-limit', dest='backend_log_size_limit', metavar='<n><K|M|G|T>',
                             help='Size limit of data returned from each backend log [\d.]+[KMGT] eg. 100K, 0.5M, 1G. Default: 10M', default='10M')
    backend_group.add_option('--impalad-http-port', dest='impalad_http_port', metavar='PORT NUMBER',
                             help='Port of the Impala Daemon HTTP Server. Default: %s. Applies to: %s'
                                  % (DEFAULT_IMPALAD_HTTP_PORT, DBTYPE_IMPALA.upper()), type="posint")
    backend_group.add_option('--hive-http-endpoint', dest='hive_http_endpoint', metavar='<SERVER/IP ADDRESS>:<PORT>',
                             help='Endpoint of the HiveServer2 or HiveServer2 Interactive (LLAP) service in the format <SERVER/IP ADDRESS>:<PORT>. Applies to: %s' % DBTYPE_HIVE.upper())
    backend_group.add_option('--spark-application-id', dest='spark_application_id', metavar='APPLICATION ID',
                             help='Retrieve logs for supplied application id. Applies to: %s' % DBTYPE_SPARK.upper())
    opt.add_option_group(backend_group)

    gluent_group = OptionGroup(opt, "Gluent Component Options", "Options for collecting information about Gluent components.")
    gluent_group.add_option('--include-processes', dest='diag_include_processes',
                            help='Collect details for running Gluent related processes', action='store_true')
    gluent_group.add_option('--include-permissions', dest='diag_include_permissions',
                            help='Collect permissions of Gluent related files and directories', action='store_true')
    opt.add_option_group(gluent_group)

    remove_options = ['--skip-steps', '--log-path']

    # Strip away the options we don't need
    for option in remove_options:
        opt.remove_option(option)

    return opt


def normalise_options(options, orchestration_config):

    def normalise_date_input(dt, param):
        try:
            d = datetime.strptime(dt, '%Y-%m-%d')
        except:
            try:
                d = datetime.strptime(dt, '%Y-%m-%d_%H:%M:%S')
            except:
                raise OptionError('Invalid value specified for --%s parameter. Format must be date (YYYY-MM-DD) or date/time (YYYY-MM-DD_HH24:MM:SS)' % param)
        return d.strftime('%Y-%m-%d_%H:%M:%S')

    # If connecting to CDP using HiveServer2 HTTP transport, remove support for some functionality
    if orchestration_config.hiveserver2_http_transport:
        BACKEND_LOGS_SUPPORTED.remove(DBTYPE_IMPALA)
        BACKEND_CONFIG_SUPPORTED.remove(DBTYPE_IMPALA)
        QUERY_LOGS_SUPPORTED.remove(DBTYPE_IMPALA)

    if options.vverbose:
        options.verbose = True
    elif options.quiet:
        options.vverbose = options.verbose = False

    options.target = orchestration_config.target
    options.connector_sql_engine = orchestration_config.connector_sql_engine
    options.spark_history_server = os.environ.get('SPARK_HISTORY_SERVER')
    options.hadoop_ssh_user = orchestration_config.hadoop_ssh_user
    options.offload_transport_cmd_host = offload_transport_cmd_host_default() or orchestration_config.hadoop_host
    hive_server_hosts = hadoop_host_default()

    if options.impalad_http_port and not options.connector_sql_engine == DBTYPE_IMPALA:
        raise OptionError('--impalad-http-port valid only against %s' % DBTYPE_IMPALA.title())

    if options.hive_http_endpoint and not options.connector_sql_engine == DBTYPE_HIVE:
        raise OptionError('--hive-http-endpoint valid only against %s' % DBTYPE_HIVE.title())

    if options.spark_application_id and not options.connector_sql_engine == DBTYPE_SPARK:
        raise OptionError('--spark-application-id valid only against %s' % DBTYPE_SPARK.title())

    # If no diagnostic options were set then pick some sensible defaults
    if not {k: v for k, v in vars(options).items() if 'diag_' in k and v is not None}:
        options.diag_include_permissions = True
        options.diag_include_processes = True
        options.diag_include_logs_last_n = '8h'
        options.diag_include_backend_logs = True if options.connector_sql_engine in BACKEND_LOGS_SUPPORTED else False
        options.diag_include_backend_config = True if options.connector_sql_engine in BACKEND_CONFIG_SUPPORTED else False

    if options.diag_include_logs_last_n and not re.search('[0-9]+(H|h|D|d)', options.diag_include_logs_last_n):
        raise OptionError('Invalid value specified for --include-logs-last parameter. Valid examples include: 3h, 7d, 1H, 2D')

    if hasattr(options, 'diag_include_logs_from') and options.diag_include_logs_from:
        options.diag_include_logs_from = normalise_date_input(options.diag_include_logs_from, 'include-logs-from')

    if hasattr(options, 'diag_include_logs_to') and options.diag_include_logs_to:
        options.diag_include_logs_to = normalise_date_input(options.diag_include_logs_to, 'include-logs-to')

    if hasattr(options, 'diag_include_logs_from') and hasattr(options, 'diag_include_logs_to') and options.diag_include_logs_from and options.diag_include_logs_to:
        from_dt = datetime.strptime(options.diag_include_logs_from, '%Y-%m-%d_%H:%M:%S')
        to_dt = datetime.strptime(options.diag_include_logs_to, '%Y-%m-%d_%H:%M:%S')
        if from_dt > to_dt:
            raise OptionError('Value specified for --include-logs-from parameter cannot be greater than value supplied for --include-logs-to parameter')

    if hasattr(options, 'diag_include_table_metadata') and options.diag_include_table_metadata and len(options.diag_include_table_metadata.split('.')) != 2:
        raise OptionError('Option --table required in form OWNER.TABLE')

    if hasattr(options, 'hive_http_endpoint') and options.hive_http_endpoint and len(options.hive_http_endpoint.split(':')) != 2:
        raise OptionError('Option --hive-http-endpoint required in form <SERVER/IP ADDRESS>:<PORT>')

    if hasattr(options, 'backend_log_size_limit') and options.backend_log_size_limit:
        if not re.search('[0-9]+(K|M|G|T)', options.backend_log_size_limit):
            raise OptionError('Invalid value specified for --backend-log-size-limit parameter. Valid examples include: 100K, 0.5M, 1G')
        else:
            options.backend_log_size_limit = parse_size_expr(options.backend_log_size_limit) or 10485760

    # get backend server and port information
    if options.connector_sql_engine == DBTYPE_IMPALA:
        options.backend_servers = hive_server_hosts
        options.backend_port = options.impalad_http_port if options.impalad_http_port else DEFAULT_IMPALAD_HTTP_PORT
    elif options.connector_sql_engine == DBTYPE_HIVE:
        if options.hive_http_endpoint:
            hive_server, hive_port = options.hive_http_endpoint.split(':')
            if not hive_port.isdigit():
                raise OptionError('Option --hive-http-endpoint <PORT> must be a positive integer')
            options.backend_servers, options.backend_port = hive_server, int(hive_port)
        else:
            options.backend_servers, options.backend_port = hive_server_hosts, None
    elif options.connector_sql_engine == DBTYPE_SPARK:
        if options.target != DBTYPE_HIVE:
            raise OptionError('QUERY_ENGINE environment variable value of %s not valid when CONNECTOR_SQL_ENGINE is %s'
                              % (options.target.title(), DBTYPE_SPARK.title()))
        options.backend_servers = orchestration_config.spark_thrift_host.split(',')[0]
        options.backend_port = orchestration_config.spark_thrift_port
    else:
        options.backend_servers = options.backend_port = options.hadoop_ssh_user = None

    if hasattr(options, 'diag_include_query_logs') and options.diag_include_query_logs:
        if options.connector_sql_engine in QUERY_LOGS_SUPPORTED:
            if options.connector_sql_engine == DBTYPE_IMPALA:
                if len(options.diag_include_query_logs.split(':')) != 2:
                    raise OptionValueError('Option --include-query-logs must be two alpha-numeric strings separated by a colon (e.g. 964562b9f758450d:d883af57981b53bf)')
                else:
                    l, r = options.diag_include_query_logs.split(':')
                    if not str(l).isalnum() or not str(r).isalnum():
                        raise OptionValueError('Option --include-query-logs must be two alpha-numeric strings separated by a colon (e.g. 964562b9f758450d:d883af57981b53bf)')
            if options.connector_sql_engine == DBTYPE_SNOWFLAKE:
                if len(options.diag_include_query_logs.split(':')) != 2:
                    raise OptionValueError('Option --include-query-logs must be a numeric session id and a alpha-numeric query id separated by a colon (e.g. 2064895723475934:01989f9b-050c-0c86-0007-560300367602')
                else:
                    l, r = options.diag_include_query_logs.split(':')
                    if not str(l).isdigit() or not str(r).replace('-','').isalnum():
                        raise OptionValueError('Option --include-query-logs must be a numeric session id and a alpha-numeric query id separated by a colon (e.g. 2064895723475934:01989f9b-050c-0c86-0007-560300367602')
            elif options.connector_sql_engine == DBTYPE_HIVE and not options.diag_include_query_logs.lower().startswith('hive_'):
                raise OptionValueError('Option --include-query-logs must begin with hive_ (e.g. hive_20180703155726_6ebfb2cd-24d2-4f92-aa58-e2c016966778)')
        else:
            raise OptionValueError(
                'Option --include-query-logs not supported for %s' % options.connector_sql_engine.upper())

    if hasattr(options, 'diag_include_backend_logs') and options.diag_include_backend_logs and options.connector_sql_engine not in BACKEND_LOGS_SUPPORTED:
        raise OptionValueError('Option --include-backend-logs not supported for %s' % options.connector_sql_engine.upper())

    if hasattr(options, 'diag_include_backend_config') and options.diag_include_backend_config and options.connector_sql_engine not in BACKEND_CONFIG_SUPPORTED:
        raise OptionValueError('Option --include-backend-config not supported for %s' % options.connector_sql_engine.upper())


def log_matched_files(messages, files):
    """ Log file matches (files)
    """
    if not files:
        messages.log('No files matched', VERBOSE)
    elif len(files) > 2:
        messages.log('File match: %s' % files[0], VERBOSE)
        messages.log('File match: (<%s more files>)' % int(len(files)-1), VERBOSE)
    else:
        for f in files:
            messages.log('File match: %s' % f, VERBOSE)


def logs_last_n_step(diag, messages, n, log_location, execute):
    """ Execute step. Return file(s) to be packaged.

        Return files modified since n in log_location with a file extension
        not listed in an excluded list (diag.EXCLUDED_FILE_EXTENSIONS).
    """
    messages.log(f'Logs last {n}', detail=VERBOSE)
    messages.log('Searching for files modified since %s, in %s, without file extensions: %s'
                 % (n, log_location, diag.EXCLUDED_FILE_EXTENSIONS), VVERBOSE)

    if execute:
        logs_matched = diag.logs_last_n(log_location, n)
        log_matched_files(messages, logs_matched)
        return logs_matched
    else:
        return []


def logs_from_step(diag, messages, from_dt, log_location, execute):
    """ Execute step. Return file(s) to be packaged.

        Return files modified since from_dt in log_location with a file extension
        not listed in an excluded list (diag.EXCLUDED_FILE_EXTENSIONS).
    """
    messages.log(f'Logs from {from_dt}', detail=VERBOSE)
    messages.log('Searching for files modified since %s, in %s, without file extensions: %s'
                 % (from_dt, log_location, diag.EXCLUDED_FILE_EXTENSIONS), VVERBOSE)

    if execute:
        logs_matched = diag.logs_from(log_location, from_dt)
        log_matched_files(messages, logs_matched)
        return logs_matched
    else:
        return []


def logs_to_step(diag, messages, to_dt, log_location, execute):
    """ Execute step. Return file(s) to be packaged.

        Return files modified until to_dt in log_location with a file extension
        not listed in an excluded list (diag.EXCLUDED_FILE_EXTENSIONS).
    """
    messages.log(f'Logs to {to_dt}', detail=VERBOSE)
    messages.log('Searching for files modified until %s, in %s, without file extensions: %s'
                 % (to_dt, log_location, diag.EXCLUDED_FILE_EXTENSIONS), VVERBOSE)

    if execute:
        logs_matched = diag.logs_to(log_location, to_dt)
        log_matched_files(messages, logs_matched)
        return logs_matched
    else:
        return []


def logs_between_step(diag, messages, from_dt, to_dt, log_location, execute):
    """ Execute step. Return file(s) to be packaged.

        Return files modified between from_dt and to_dt in log_location with a file extension
        not listed in an excluded list (diag.EXCLUDED_FILE_EXTENSIONS).
    """
    messages.log(f'Logs between {from_dt} and {to_dt}', detail=VERBOSE)
    messages.log('Searching for files modified between %s and %s, in %s, without file extensions: %s'
                 % (from_dt, to_dt, log_location, diag.EXCLUDED_FILE_EXTENSIONS), VVERBOSE)

    if execute:
        logs_matched = diag.logs_between(log_location, from_dt, to_dt)
        log_matched_files(messages, logs_matched)
        return logs_matched
    else:
        return []


def processes_step(diag, messages, output_location, execute):
    """ Execute step. Return file(s) to be packaged.

        Search for processes using ps grep for matches with proc_filter.
    """
    proc_filter = ['agg_validate', 'cloud_', 'connect', 'datad', 'dbsync', 'logmgr', 'metad', 'offload',
                   'pass_tool', 'present', 'scand_client', 'schema_sync', 'smart_connector', 'tns', 'smon']

    messages.log('Searching for processes matching filter: %s' % ', '.join(proc_filter), VVERBOSE)

    if execute:
        out = diag.processes(proc_filter)
        if out:
            return [diag.create_file(output_location, 'process_list', out.split('\n'),
                                     header='STAT   PID  PPID  PGID USER     GROUP    RUSER    RGROUP   START     TIME CMD')]
        else:
            return []
    else:
        return []


def permissions_step(diag, messages, output_location, execute):
    """ Execute step. Return file(s) to be packaged.

        List (recursive) permissions on defined files/directories.
    """
    perm_results = []
    perm_list_of_tuples = [PermissionsTuple('directory', '%s' % os.environ['OFFLOAD_HOME'], False),
                           PermissionsTuple('directory', '%s' % os.environ['OFFLOAD_HOME'], True)]

    try:
        perm_list_of_tuples.append(PermissionsTuple('file', '%s/bin/oracle' % os.environ['ORACLE_HOME'], False))
    except KeyError:
        pass

    for perm_tuple in perm_list_of_tuples:
        messages.log('Listing %spermissions for %s : %s'
                     % ('recursive ' if perm_tuple.recursive == True else '', perm_tuple.type, perm_tuple.path),
                     VVERBOSE)

        if execute:
            out = diag.permissions(perm_tuple).decode()
            if out:
                perm_results.extend(out.split('\n'))

    if perm_results:
        return [diag.create_file(output_location, 'permissions_list', perm_results)]
    else:
        return []


def table_metadata_step(diag, messages, output_location, owner_table, execute, orchestration_config):
    """ Execute step. Return file(s) to be packaged.

        1. Get DDL for owner_table
        2. Get table stats for owner_table
        3. For each owner_table dependent Oracle and Hadoop object, get
            a) DDL
            b) statistics (if object is a table)
    """
    ddls = []

    def format_ddl_output(source, owner, name, ddl):
        return '\n%s DDL: %s.%s\n%s%s' % (source.title(), owner, name, '\n' if source.lower() == 'hadoop' else '', ddl)

    def format_stats_output(source, owner, name, stats):
        return '\n%s statistics: %s.%s\n\n%s' % (source.title(), owner, name, DENSE.pformat(stats))

    def format_rewrite_output(owner, name, source, dest, mode):
        return '\nOracle rewrite equivalence: %s.%s\n\nSource: %s\n\nDestination: %s\n\nMode: %s'\
               % (owner, name, source, dest, mode)

    messages.log(f'Table metadata for {owner_table}', detail=VERBOSE)

    if execute:
        owner, table = owner_table.upper().split('.')

        if diag.frontend_table_exists(owner, table):
            ddls.extend([format_ddl_output('Oracle', owner, table, diag.frontend_ddl(owner, table, 'TABLE'))])
            ddls.extend([format_stats_output('Oracle', owner, table, diag.oracle_table_stats(owner, table))])
        else:
            messages.log('Source table %s.%s does not exist' % (owner, table), VVERBOSE)

        # Backend: need to use metadata to find the offloaded table, we cannot guarantee target name hasn't been used
        offload_metadata = OrchestrationMetadata.from_name(hybrid_owner(owner), table,
                                                           connection_options=orchestration_config, messages=messages)
        if offload_metadata:
            messages.log('Offload metadata found for %s.%s: %s' % (hybrid_owner(owner), table, offload_metadata), VVERBOSE)
            if offload_metadata.backend_owner and offload_metadata.backend_table:
                db_name = offload_metadata.backend_owner
                table = offload_metadata.backend_table
            else:
                messages.log('Offload metadata missing HADOOP_OWNER and/or HADOOP_TABLE', VVERBOSE)
        else:
            messages.log('No offload metadata found for %s.%s' % (hybrid_owner(owner), table), VVERBOSE)
            lower_owner, table = owner_table.lower().split('.')
            db_name = data_db_name(lower_owner, orchestration_config)

        backend_table = backend_table_factory(db_name, table, orchestration_config.target, orchestration_config,
                                              messages, hybrid_metadata=offload_metadata, dry_run=bool(not execute))

        if backend_table.exists():
            ddls.extend([format_ddl_output('Backend', backend_table.db_name, table, diag.backend_ddl(backend_table.db_name, table))])
            ddls.extend([format_stats_output('Backend', backend_table.db_name, table, diag.backend_table_stats(backend_table.db_name, table))])
        else:
            messages.log('Backend table %s.%s does not exist' % (backend_table.db_name, table), VVERBOSE)

    if ddls:
        messages.log('Metadata generated: %s' % len(ddls), VERBOSE)
        return [diag.create_file(output_location, '%s_%s_metadata' % (owner, table), diag.divide_list(ddls))]
    else:
        messages.log('No metadata generated', VERBOSE)
        return []


def backend_logs_step(diag, messages, output_location, target, hadoop_host, port, log_size_limit, execute, spark_history_server, spark_application_id):
    """ Execute step. Return file(s) to be packaged.

        Impala:
            For each server in hadoop_host (HIVE_SERVER_HOST), connect to the Impala Daemon HTTP Server
            on port and retrieve the last 1MB (or log_size_limit, whichever is lower) of impalad.INFO
            log entries from the "logs" URI on this endpoint.

        Hive:
            Determine if LLAP/Non-LLAP by checking "hive.execution.mode" parameter. The endpoint we connect
            to will be based on this response, unless diagnose was invoked with the --hive-http-endpoint
            parameter, in which case that will be used.

            HiveServer2 Interactive (LLAP):
                For each server in hadoop_host (HIVE_SERVER_HOST), determine which is running the
                HiveServer2 Interactive (LLAP) service.

            HiveServer2:
                A random entry from HIVE_SERVER_HOST is chosen.

            Connect to this endpoint to reach the HiveServer2 Web UI. From the logs URI on this
            endpoint retrieve the last log_size_limit of:

            HiveServer2 Interactive (LLAP):

                1. hiveserver2Interactive.log
                2. hive-server2-interactive.err

            HiveServer2:

                1. hive-server2.log
                2. hive-server2.out

        Spark:
              Determine the spark history server address and use it to connect with the API.

              If --spark-application-id passed, retrieve the stages from the history server API.

              If no app id supplied, get a list of all applications from the API in the past
              SPARK_HISTORIC_APPLICATION_DAYS days, and for each application retrieve the stages from the
              history server API.
    """
    messages.log('%s logs' % diag.backend_target(), detail=VERBOSE)
    logs = []

    if target == DBTYPE_IMPALA:
        for host in hadoop_host.split(','):
            if execute:
                log = [diag.impala_info_log(host, port, int(log_size_limit))]
                if log[0]:
                    logs.extend([diag.create_file(output_location, '%s_impalad_log' % host, log, suffix='.html', gluent_header=False)])
                else:
                    messages.log('Request at %s:%s returned no output' % (host, port), VERBOSE)
    elif target == DBTYPE_HIVE:
        if diag.llap_enabled():
            port = port or DEFAULT_LLAP_WEBUI_PORT
            llap_host, llap_appid, llap_config = locate_hs2_interactive(diag, messages, hadoop_host, port)
            if llap_appid and llap_host and llap_config:
                for logfile in ['hiveserver2Interactive.log', 'hive-server2-interactive.err']:
                    if execute:
                        log = [diag.hs2_log(llap_host, port, logfile, int(log_size_limit))]
                        if log[0]:
                            logs.extend([diag.create_file(output_location, '%s_%s' % (llap_host, logfile.replace('.', '_')), log)])
                        else:
                            messages.log('Request at %s:%s for %s returned no output' % (llap_host, port, logfile), VERBOSE)
            else:
                messages.log('Unable to determine HiveServer2 Interactive configuration.', VERBOSE)
                messages.log('Configuration response was: \n%s' % llap_config, VVERBOSE)
        else:
            port = port or DEFAULT_HS2_WEBUI_PORT
            for logfile in ['hive-server2.log', 'hive-server2.out']:
                if execute:
                    log = [diag.hs2_log(hadoop_host, port, logfile, int(log_size_limit))]
                    if log[0]:
                        logs.extend([diag.create_file(output_location, '%s_%s' % (hadoop_host, logfile.replace('.', '_')), log)])
                    else:
                        messages.log('Request at %s:%s for %s returned no output' % (hadoop_host, port, logfile), VERBOSE)
    elif target == DBTYPE_SPARK:
        spark_history_server_address = diag.spark_history_server_address(spark_history_server, hadoop_host, port, DEFAULT_SPARK_HISTORY_PORT)
        if spark_application_id:
            spark_application_ids = [spark_application_id]
        else:
            spark_application_ids = diag.spark_application_history(spark_history_server_address, SPARK_HISTORIC_APPLICATION_DAYS)
        for application_id in spark_application_ids:
            if execute:
                log = [diag.spark_application_log(spark_history_server_address, str(application_id))]
                if log[0]:
                    logs.extend([diag.create_file(output_location, '%s_%s_log' % (hadoop_host, str(application_id)), log)])
                else:
                    messages.log('Request at %s for %s returned no output' % (spark_history_server_address, str(application_id)), VERBOSE)

    return logs


def backend_config_step(diag, messages, output_location, target, hadoop_host, port, execute,
                        spark_history_server, spark_application_id):
    """ Execute step. Return file(s) to be packaged.

        Impala:
            For each server in hadoop_host (HIVE_SERVER_HOST), connect to the Impala Daemon HTTP Server
            on port and retrieve the configuration from the "varz" URI on this endpoint.

        Hive:
            Determine if LLAP/Non-LLAP by checking "hive.execution.mode" parameter. The endpoint we connect
            to will be based on this response, unless diagnose was invoked with the --hive-http-endpoint
            parameter, in which case that will be used.

            HiveServer2 Interactive (LLAP):
                For each server in hadoop_host (HIVE_SERVER_HOST), determine which is running the
                HiveServer2 Interactive (LLAP) service.

            HiveServer2:
                A random entry from HIVE_SERVER_HOST is chosen.

            Connect to this endpoint to reach the HiveServer2 Web UI. Retrieve the Hive Configuration from
            the "conf" URI on this endpoint.

        Spark:
              Determine the spark history server address and use it to connect with the API.

              If --spark-application-id passed, retrieve the environment from the history server API.

              If no app id supplied, get a list of all applications from the API in the past
              SPARK_HISTORIC_APPLICATION_DAYS days, and for each application retrieve the environment from the
              history server API.
    """
    logs = []

    if target == DBTYPE_IMPALA:
        for host in hadoop_host.split(','):
            if execute:
                log = [diag.impala_config(host, port)]
                if log[0]:
                    logs.extend([diag.create_file(output_location, '%s_impalad_config' % host, log,
                                                  suffix='.html', gluent_header=False)])
                else:
                    messages.log('Request at %s:%s returned no output' % (host, port), VERBOSE)
    elif target == DBTYPE_HIVE:
        if diag.llap_enabled():
            port = port or DEFAULT_LLAP_WEBUI_PORT
            llap_host, llap_appid, llap_config = locate_hs2_interactive(diag, messages, hadoop_host, port)
            if llap_appid and llap_host and llap_config:
                if execute:
                    log = [diag.hs2_config(llap_host, port)]
                    if log[0]:
                        logs.extend([diag.create_file(output_location, '%s_hive_config' % llap_host, log,
                                                      suffix='.xml', gluent_header=False)])
                    else:
                        messages.log('Request at %s:%s returned no output' % (llap_host, port), VERBOSE)
                    logs.extend([diag.create_file(output_location, '%s_llap_config' % llap_host,
                                                  [DENSE.pformat(llap_config)])])
            else:
                messages.log('Unable to determine HiveServer2 Interactive configuration.', VERBOSE)
                messages.log('Configuration response was: \n%s' % llap_config, VVERBOSE)
        else:
            port = port or DEFAULT_HS2_WEBUI_PORT
            if execute:
                log = [diag.hs2_config(hadoop_host, port)]
                if log[0]:
                    logs.extend([diag.create_file(output_location, '%s_hive_config' % hadoop_host, log,
                                                  suffix='.xml', gluent_header=False)])
                else:
                    messages.log('Request at %s:%s returned no output' % (hadoop_host, port), VERBOSE)
    elif target == DBTYPE_SPARK:
        spark_history_server_address = diag.spark_history_server_address(spark_history_server, hadoop_host,
                                                                         port, DEFAULT_SPARK_HISTORY_PORT)
        if spark_application_id:
            spark_application_ids = [spark_application_id]
        else:
            spark_application_ids = diag.spark_application_history(spark_history_server_address,
                                                                   SPARK_HISTORIC_APPLICATION_DAYS)
        for application_id in spark_application_ids:
            if execute:
                log = [diag.spark_application_config(spark_history_server_address, str(application_id))]
                if log[0]:
                    logs.extend([diag.create_file(output_location,
                                                  '%s_%s_config' % (hadoop_host, str(application_id)), log)])
                else:
                    messages.log('Request at %s for %s returned no output'
                                 % (spark_history_server_address, str(application_id)), VERBOSE)
    elif target == DBTYPE_SYNAPSE:
        if execute:
            out = diag.synapse_configuration()
            if out:
                logs.extend([diag.create_file(output_location, '%s_configuration' % target, out)])
            else:
                messages.log('Unable to retrieve backend configuration', VERBOSE)

    return logs


def query_logs_step(diag, messages, output_location, target, hadoop_host, port, log_size_limit,
                    ssh_user, ssh_host, query_id, execute):
    """ Execute step. Return file(s) to be packaged.

        Impala:
            For each server in hadoop_host (HIVE_SERVER_HOST), connect to the Impala Daemon HTTP Server
            on port and retrieve query_id information from the following URIs on this endpoint:

            1. Query (query_stmt)
            2. Text Plan (query_plan_text)
            3. Summary (query_summary)
            4. Profile (query_profile)

            Once this has been received, break from the loop.

        HiveServer2 Interactive (LLAP):
            For each server in hadoop_host (HIVE_SERVER_HOST), determine which is running the
            HiveServer2 Interactive (LLAP) service. This will return the LLAP application ID.

            Connect to ssh_host (OFFLOAD_TRANSPORT_CMD_HOST or a random HIVE_SERVER_HOST) as
            ssh_user (HADOOP_SSH_USER) and run a yarn logs command with -applicationId=<llap application id>
            and -log_files_pattern=query_id.

            If this run of diagnose does not have the --include-backend-logs option, then also connect to this
            server on port to reach the HiveServer2 Web UI. From the logs URI on this endpoint retrieve the
            last log_size_limit of:

            1. hiveserver2Interactive.log
            2. hive-server2-interactive.err

         HiveServer2:
            Not implemented.

         Spark:
            Not implemented.
    """
    logs = []

    if target == DBTYPE_IMPALA:
        for host in hadoop_host.split(','):
            if execute:
                log = [diag.impala_query_log(host, port, query_id)]
                if log[0]:
                    logs.extend([diag.create_file(output_location, 'query_%s' % query_id.replace(':', '_'), log,
                                                  suffix='.html', gluent_header=False)])
                    break
                else:
                    messages.log('Request at %s:%s returned no output' % (host, port), VERBOSE)
        if not logs:
            messages.log('Unable to retrieve query logs', VERBOSE)
    elif target == DBTYPE_HIVE:
        if diag.llap_enabled():
            port = port or DEFAULT_LLAP_WEBUI_PORT
            llap_host, llap_appid, llap_config = locate_hs2_interactive(diag, messages, hadoop_host, port)
            if llap_appid and llap_host and llap_config:
                messages.log('Retrieving YARN logs for query: %s' % query_id, VVERBOSE)
                if execute:
                    out = diag.yarn_log(ssh_user, ssh_host, llap_appid, query_id, int(log_size_limit))
                    if out:
                        yarn_log = [o for o in out.split('\n') if o]
                        messages.log('YARN logs request for query: %s successful' % query_id, VERBOSE)
                        logs.extend([diag.create_file(output_location, 'query_%s' % query_id, yarn_log)])
                    else:
                        messages.log('YARN logs request returned no output', VERBOSE)
        else:
            messages.log('Not implemented for Hive in non-LLAP mode', VERBOSE)
    elif target == DBTYPE_BIGQUERY:
        if execute:
            out = diag.bq_query_log(query_id)
            if out:
                messages.log('Request for query: %s successful' % query_id, VERBOSE)
                logs.extend([diag.create_file(output_location, 'query_%s' % query_id, out)])
            else:
                messages.log('Unable to retrieve query logs', VERBOSE)
    elif target == DBTYPE_SNOWFLAKE:
        if execute:
            session, query = query_id.split(':')
            out = diag.snowflake_query_log(session, query)
            if out:
                messages.log('Request for session: %s, query: %s successful' % (session, query), VERBOSE)
                logs.extend([diag.create_file(output_location, 'query_%s' % query_id.replace(':', '_'), out)])
            else:
                messages.log('Unable to retrieve query logs', VERBOSE)
    elif target == DBTYPE_SYNAPSE:
        if execute:
            out = diag.synapse_query_log(query_id)
            if out:
                messages.log('Request for query: %s successful' % query_id, VERBOSE)
                logs.extend([diag.create_file(output_location, 'query_%s' % query_id, out)])
            else:
                messages.log('Unable to retrieve query logs', VERBOSE)

    return logs


def locate_hs2_interactive(diag, messages, hadoop_host, port):
    """ Locate HiveServer2 Interactive from hosts in hadoop_hosts
    """
    llap_appid = llap_host = None
    messages.log('Locating HiveServer2 Interactive', VVERBOSE)
    for host in hadoop_host.split(','):
        llap_config = diag.llap_configuration(host, port)
        try:
            llap_appid = str(llap_config['amInfo']['appId'])
            llap_host = host
            messages.log('Located HiveServer2 Interactive at %s:%s' % (host, port), VVERBOSE)
            break
        except:
            messages.log('HiveServer2 Interactive not located at %s:%s' % (host, port), VVERBOSE)

    return llap_host, llap_appid, llap_config


def diagnose():
    try:
        opt = get_diagnose_opts()
        options, _ = opt.parse_args()

        init(options)
        init_log('diagnose')

        log('')
        log('Diagnose v%s' % version(), ansi_code='underline')
        log('Log file: %s' % get_log_fh_name())
        log('', VERBOSE)
        log_command_line()

        orchestration_config = OrchestrationConfig.from_dict({'verbose': options.verbose,
                                                              'vverbose': options.vverbose})

        normalise_options(options, orchestration_config)

        messages = OffloadMessages.from_options(options, log_fh=get_log_fh(),
                                                command_type=orchestration_constants.COMMAND_DIAGNOSE)
        diag = Diagnose.from_options(options, messages)
        package_files = []

        # Run steps
        if options.diag_include_logs_last_n:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_LOGS,
                                                       lambda: logs_last_n_step(diag, messages, options.diag_include_logs_last_n, options.log_location, options.execute),
                                                       options.execute))

        if options.diag_include_logs_from and not options.diag_include_logs_to:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_LOGS,
                                                       lambda: logs_from_step(diag, messages, options.diag_include_logs_from, options.log_location, options.execute),
                                                       options.execute))

        if options.diag_include_logs_to and not options.diag_include_logs_from:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_LOGS,
                                                       lambda: logs_to_step(diag, messages, options.diag_include_logs_to, options.log_location, options.execute),
                                                       options.execute))

        if options.diag_include_logs_to and options.diag_include_logs_from:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_LOGS,
                                                       lambda: logs_between_step(diag, messages, options.diag_include_logs_from, options.diag_include_logs_to, options.log_location, options.execute),
                                                       options.execute))

        if options.diag_include_processes:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_PROCESSES,
                                                       lambda: processes_step(diag, messages, options.output_location, options.execute),
                                                       options.execute))

        if options.diag_include_permissions:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_PERMISSIONS,
                                                       lambda: permissions_step(diag, messages, options.output_location, options.execute),
                                                       options.execute))

        if options.diag_include_table_metadata:
            package_files.extend(messages.offload_step(command_steps.STEP_GDP_TABLE_METADATA,
                                                       lambda: table_metadata_step(diag, messages, options.output_location, options.diag_include_table_metadata, options.execute, orchestration_config),
                                                       options.execute))

        if options.diag_include_backend_logs:
            package_files.extend(messages.offload_step(command_steps.STEP_BACKEND_LOGS,
                                                       lambda: backend_logs_step(diag, messages, options.output_location, options.connector_sql_engine, options.backend_servers, options.backend_port, options.backend_log_size_limit, options.execute, options.spark_history_server, options.spark_application_id),
                                                       options.execute))

        if options.diag_include_backend_config:
            package_files.extend(
                messages.offload_step(command_steps.STEP_BACKEND_CONFIG,
                                      lambda: backend_config_step(diag, messages, options.output_location,
                                                                  options.connector_sql_engine, options.backend_servers,
                                                                  options.backend_port, options.execute,
                                                                  options.spark_history_server,
                                                                  options.spark_application_id),
                                      options.execute))

        if options.diag_include_query_logs:
            package_files.extend(
                messages.offload_step(command_steps.STEP_BACKEND_QUERY_LOGS,
                                      lambda: query_logs_step(diag, messages, options.output_location,
                                                              options.connector_sql_engine, options.backend_servers,
                                                              options.backend_port, options.backend_log_size_limit,
                                                              options.hadoop_ssh_user, options.offload_transport_cmd_host,
                                                              options.diag_include_query_logs, options.execute),
                                      options.execute))

        # Create final diagnostics package as a zip of all files returned
        if package_files and options.execute:
            bundle_file = diag.create_zip_archive(options.output_location, package_files)
            if bundle_file:
                messages.log('')
                messages.log('Gluent diagnostics package created: %s' % bundle_file)
                messages.log('')
                if not options.retain_created_files:
                    diag.remove_files(options.output_location, package_files)
            else:
                raise DiagnoseException('Unable to create Gluent diagnostics package.')

    except OptionError as exc:
        log('Option error: %s' % exc.detail, ansi_code='red')
        log('')
        opt.print_help()
        sys.exit(1)
    except Exception as exc:
        log('Exception caught at top-level', ansi_code='red')
        log_timestamp()
        log_exception(exc, log_fh=get_log_fh(), options=options)
        sys.exit(1)
