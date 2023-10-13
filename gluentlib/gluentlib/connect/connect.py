"""
LICENSE_TEXT
"""
from datetime import datetime
from optparse import SUPPRESS_HELP
import os
import re
import subprocess
import sys
import traceback

from getpass import getuser
from gluent import (
    get_common_options,
    get_log_fh,
    get_log_fh_name,
    init,
    init_log,
    log_command_line,
    log_timestamp,
    version,
    OptionValueError,
    verbose,
    CONFIG_FILE_NAME,
)
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.config import orchestration_defaults
from gluentlib.connect.connect_backend import (
    is_hadoop_environment,
    run_backend_tests,
    run_hs2_tests,
    test_hdfs_dirs,
    test_webhdfs_config,
)
from gluentlib.connect.connect_constants import CONNECT_HIVE_TIMEOUT_S
from gluentlib.connect.connect_frontend import run_frontend_tests
from gluentlib.connect.connect_functions import (
    FatalTestFailure,
    debug,
    detail,
    failure,
    log,
    section_header,
    success,
    test_header,
    warning,
)
from gluentlib.connect.connect_transport import (
    run_spark_tests,
    run_sqoop_tests,
    test_credential_api_alias,
)
from gluentlib.filesystem.gluent_dfs_factory import get_dfs_from_options
from gluentlib.offload.offload_constants import DBTYPE_IMPALA, LOG_LEVEL_DEBUG
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.offload.offload_transport_functions import ssh_cmd_prefix
from gluentlib.orchestration import orchestration_constants
from gluentlib.util.gluent_log import log_exception
from gluentlib.util.redis_tools import RedisClient


class ConnectException(Exception):
    pass


def test_ssh(orchestration_config):
    def normalise_host_list(list_of_hosts):
        """ensure no CSVs or empty values"""
        expanded_list = []
        [expanded_list.extend(_.split(",")) if _ else [] for _ in list_of_hosts]
        return set(expanded_list)

    test_name = None
    try:
        for host in normalise_host_list(
            [
                orchestration_config.hdfs_host,
                orchestration_config.offload_transport_cmd_host,
            ]
        ):
            ssh_user = orchestration_config.hadoop_ssh_user
            if host == orchestration_config.offload_transport_cmd_host:
                test_name = "%s (OFFLOAD_TRANSPORT_CMD_HOST): password-less ssh" % host
                ssh_user = orchestration_config.offload_transport_user or ssh_user
            elif host == orchestration_config.hdfs_host:
                test_name = "%s (HDFS_CMD_HOST): password-less ssh" % host
                if not is_hadoop_environment(orchestration_config):
                    test_header(test_name)
                    detail("Skipping SSH test in non-Hadoop environment")
                    continue
            else:
                test_name = "%s (HIVE_SERVER_HOST): password-less ssh" % host
            test_header(test_name)
            if host == "localhost" and ssh_user == getuser():
                detail(
                    "Skipping SSH test because user is current user and host is localhost"
                )
                continue

            cmd = ssh_cmd_prefix(ssh_user, host=host) + [
                "-o StrictHostKeyChecking=no",
                "id",
            ]
            detail(" ".join(cmd))
            groups = subprocess.check_output(cmd)
            detail(groups.strip())

            success(test_name)

    except:
        failure(test_name)
        raise


def test_os_version():
    test_name = "Operating system version"
    try:
        test_header(test_name)

        if not os.path.isfile("/etc/redhat-release") and not os.path.isfile(
            "/etc/SuSE-release"
        ):
            detail("Unsupported operating system")
            failure(test_name)
            return
        os_ver = ""
        if os.path.isfile("/etc/redhat-release"):
            cmd = ["cat", "/etc/redhat-release"]
            os_ver = subprocess.check_output(cmd).decode()
        elif os.path.isfile("/etc/SuSE-release"):
            cmd = ["cat", "/etc/SuSE-release"]
            out = subprocess.check_output(cmd).decode().splitlines()
            os_ver = "%s (%s)" % (out[:1][0], ", ".join([o for o in out[1:]]))
        cmd = ["uname", "-r"]
        kern_ver = subprocess.check_output(cmd).decode()
        detail("%s - %s" % (os_ver.rstrip(), kern_ver.rstrip()))
        success(test_name)

    except Exception:
        failure(test_name)
        raise


def test_krb_bin(orchestration_config):
    test_name = "Path to kinit (Kerberos)"
    try:
        test_header(test_name)
        if orchestration_config.kerberos_service:
            cmd = ["which", "kinit"]
            kinit_path = subprocess.check_output(cmd).decode()
            detail("kinit found: %s" % (kinit_path.rstrip("\n")))
            success(test_name)
        else:
            detail("Use of Kerberos (KERBEROS_SERVICE) not configured in environment")
            success(test_name)

    except Exception:
        failure(test_name)
        raise


def configured_num_location_files(options, orchestration_config):
    num_loc_files = (
        options.num_location_files
        or orchestration_defaults.num_location_files_default()
    )
    num_buckets_max = (
        orchestration_config.num_buckets_max
        or orchestration_defaults.num_buckets_max_default()
    )
    if orchestration_config.target == DBTYPE_IMPALA:
        # On Impala present is capped by num_location_files, offload is capped by num_buckets_max
        return max(num_loc_files, num_buckets_max)
    else:
        return num_loc_files


def get_environment_file_name(orchestration_config):
    frontend_id = orchestration_config.db_type.lower()
    if is_hadoop_environment(orchestration_config):
        backend_id = "hadoop"
    else:
        backend_id = orchestration_config.target.lower()
    return "-".join([frontend_id, backend_id, CONFIG_FILE_NAME + ".template"])


def get_environment_file_path():
    return os.path.join(os.environ.get("OFFLOAD_HOME"), "conf", CONFIG_FILE_NAME)


def get_template_file_path(orchestration_config):
    template_name = get_environment_file_name(orchestration_config)
    return os.path.join(os.environ.get("OFFLOAD_HOME"), "conf", template_name)


def test_conf_perms():
    test_name = "Configuration file permissions"
    test_header(test_name)
    hint = "Expected permissions are 640"
    environment_file = get_environment_file_path()
    perms = oct(os.stat(environment_file).st_mode & 0o777)
    # Removing oct prefix deemed safe for display only.
    detail("%s has permissions: %s" % (environment_file, perms[2:]))
    if perms[-3:] != "640":
        warning(test_name, hint)
    else:
        success(test_name)


def test_dir(dir_name, expected_perms):
    test_name = "Directory permissions: %s" % dir_name
    hint = "Expected permissions are %s" % expected_perms
    test_header(test_name)
    log_dir = os.path.join(os.environ.get("OFFLOAD_HOME"), dir_name)
    perms = oct(os.stat(log_dir).st_mode & 0o2777)
    # Removing oct prefix deemed safe for display only.
    detail("%s has permissions: %s" % (log_dir, perms[2:]))
    if perms[-4:] != expected_perms:
        failure(test_name, hint)
    else:
        success(test_name)


def test_log_level(orchestration_config):
    test_name = "Logging level"
    test_header(test_name)
    if orchestration_config.log_level == LOG_LEVEL_DEBUG:
        detail(
            'LOG_LEVEL of "%s" should only be used under the guidance of Gluent Support'
            % orchestration_config.log_level
        )
        warning(test_name)
    else:
        detail("LOG_LEVEL: %s" % orchestration_config.log_level)
        success(test_name)


def test_listener(orchestration_config):
    test_name = orchestration_constants.PRODUCT_NAME_GEL
    test_header(test_name)
    if (
        not orchestration_config.listener_host
        and orchestration_config.listener_port is None
    ):
        detail(f"{orchestration_constants.PRODUCT_NAME_GEL} not configured")
        success(test_name)
        return

    try:
        # Avoid importing Listener modules if the listener is not configured.
        from gluentlib.listener.utils.ping import ping as ping_listener

        # Check Listener is up.
        if ping_listener(orchestration_config):
            detail(
                "Listener ping successful: {}:{}".format(
                    orchestration_config.listener_host,
                    orchestration_config.listener_port,
                )
            )
        else:
            detail("Listener ping unsuccessful")
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
                detail(
                    "Listener cache found: {}:{}".format(
                        orchestration_defaults.listener_redis_host_default(),
                        orchestration_defaults.listener_redis_port_default(),
                    )
                )
                success(test_name)
            else:
                detail("Cache ping unsuccessful")
                warning(test_name)
    except Exception as exc:
        detail(str(exc))
        log(traceback.format_exc(), detail=verbose)
        failure(test_name)


def dict_from_environment_file(environment_file):
    d = {}
    with open(environment_file) as f:
        for line in f:
            if re.match("^(#.*e|e)xport(.*)", line):
                (k, v) = re.sub("^(#.*e|e)xport ", "", line).split("=", 1)[0], line
                d[k] = v
    return d


def check_offload_env(environment_file, template_file):
    left = os.path.basename(environment_file)
    right = os.path.basename(template_file)
    test_name = "%s vs %s" % (left, right)
    test_header(test_name)

    try:
        configuration = dict_from_environment_file(environment_file)
    except IOError:
        debug(traceback.format_exc())
        # If offload.env does not exist then we need to abort because the OFFLOAD_HOME is in bad shape
        raise ConnectException(
            "Unable to access environment file: %s" % environment_file
        )

    try:
        template = dict_from_environment_file(template_file)
    except IOError:
        debug(traceback.format_exc())
        failure(test_name, "Unable to access template file: %s" % template_file)
        return

    test_hint = "Remove deprecated/unsupported parameters from %s" % left
    if set(configuration.keys()) - set(template.keys()):
        for key in sorted(set(configuration.keys()) - set(template.keys())):
            detail("%s is present in %s but not in %s" % (key, left, right))
        warning(test_name, test_hint)
    else:
        detail("No entries in %s not in %s" % (left, right))
        success(test_name)

    left = os.path.basename(template_file)
    right = os.path.basename(environment_file)
    test_name = "%s vs %s" % (left, right)
    test_hint = (
        "Use ./connect --upgrade-environment-file to copy missing configuration from %s to %s"
        % (left, right)
    )

    test_header(test_name)
    if set(template.keys()) - set(configuration.keys()):
        for key in sorted(set(template.keys()) - set(configuration.keys())):
            detail("%s is present in %s but not in %s" % (key, left, right))
        warning(test_name, test_hint)
    else:
        detail("No entries in %s not in %s" % (left, right))
        success(test_name)


def upgrade_environment_file(environment_file, template_file):
    log("Upgrading environment file using current template")

    configuration = dict_from_environment_file(environment_file)
    template = dict_from_environment_file(template_file)

    if set(template.keys()) - set(configuration.keys()):
        with open(environment_file, "a") as f:
            f.write(
                "\n# Below variables added by connect --upgrade-environment-file on %s - See docs.gluent.com for their usage.\n"
                % datetime.now().strftime("%x %X")
            )
            for key in sorted(set(template.keys()) - set(configuration.keys())):
                detail("Adding %s" % key)
                f.write(template[key])

        log("%s has been updated" % environment_file, ansi_code="green")
    else:
        log("No update required", ansi_code="yellow")


def check_environment(options, orchestration_config):
    global warnings
    warnings = False
    global failures
    failures = False
    log("\nChecking your current Gluent Offload Engine environment...")

    section_header("Configuration")

    check_offload_env(
        get_environment_file_path(), get_template_file_path(orchestration_config)
    )
    test_conf_perms()
    test_log_level(orchestration_config)

    # A number of checks require a messages object, rather than creating in multiple place we create early and re-use
    messages = OffloadMessages.from_options(options, get_log_fh())

    section_header("Frontend")

    run_frontend_tests(orchestration_config, messages)

    section_header("Backend")

    if orchestration_config.use_ssl:
        if orchestration_config.ca_cert:
            detail(
                "Using SSL with authority certificate %s" % orchestration_config.ca_cert
            )
        else:
            detail("Using SSL without certificate")

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

    section_header("Local")
    test_os_version()
    test_krb_bin(orchestration_config)
    test_listener(orchestration_config)
    if failures:
        sys.exit(2)
    if warnings:
        sys.exit(3)


def test_offload_fs_container(orchestration_config, messages):
    test_name = "Offload filesystem container"
    test_header(test_name)
    dfs_client = get_dfs_from_options(orchestration_config, messages)
    display_uri = dfs_client.gen_uri(
        orchestration_config.offload_fs_scheme,
        orchestration_config.offload_fs_container,
        "",
    )
    test_hint = "Check existence and permissions of %s" % display_uri
    if dfs_client.container_exists(
        orchestration_config.offload_fs_scheme,
        orchestration_config.offload_fs_container,
    ):
        detail(display_uri)
        success(test_name)
    else:
        failure(test_name, test_hint)


def get_config_with_connect_overrides(connect_options):
    override_dict = {
        "execute": True,
        "verbose": connect_options.verbose,
        "hive_timeout_s": CONNECT_HIVE_TIMEOUT_S,
    }
    config = OrchestrationConfig.from_dict(override_dict)
    return config


def get_connect_opts():
    opt = get_common_options()

    opt.remove_option("--execute")

    opt.add_option(
        "--upgrade-environment-file",
        dest="upgrade_environment_file",
        default=False,
        action="store_true",
        help="Adds missing configuration variables from the environment file template to the environment file",
    )
    # Hidden options to keep TeamCity testing functioning
    opt.add_option(
        "--validate-udfs",
        dest="validate_udfs",
        default=False,
        action="store_true",
        help=SUPPRESS_HELP,
    )
    opt.add_option(
        "--install-udfs",
        dest="install_udfs",
        default=False,
        action="store_true",
        help=SUPPRESS_HELP,
    )
    opt.add_option(
        "--create-backend-db",
        dest="create_backend_db",
        action="store_true",
        help=SUPPRESS_HELP,
    )

    return opt


def connect():
    options = None
    try:
        opt = get_connect_opts()
        options, args = opt.parse_args()

        init(options)
        init_log("connect")
        section_header("Connect v%s" % version())
        log("Log file: %s" % get_log_fh_name())
        log_command_line()

        orchestration_config = get_config_with_connect_overrides(options)

        if (
            is_hadoop_environment(orchestration_config)
            and not orchestration_config.hadoop_host
        ):
            raise OptionValueError("HIVE_SERVER_HOST is mandatory")

        # We need the original host CSV for comprehensive checking later
        options.original_offload_transport_spark_thrift_host = (
            orchestration_defaults.offload_transport_spark_thrift_host_default()
        )
        options.original_hadoop_host = orchestration_defaults.hadoop_host_default()

        if options.upgrade_environment_file:
            upgrade_environment_file(
                get_environment_file_path(),
                get_template_file_path(orchestration_config),
            )
        else:
            check_environment(options, orchestration_config)
        sys.exit(0)

    except FatalTestFailure:
        sys.exit(1)  # has already been reported
    except Exception as exc:
        log("Exception caught at top-level", ansi_code="red")
        log_timestamp()
        log_exception(exc, log_fh=get_log_fh(), options=options)
        sys.exit(1)
