"""
LICENSE_TEXT
"""
import copy
import re
import subprocess
import traceback

from gluentlib.connect.connect_backend import (
    test_backend_db_connectivity,
)
from gluentlib.connect.connect_constants import (
    TEST_HDFS_DIRS_SERVICE_HDFS,
    TEST_HDFS_DIRS_SERVICE_WEBHDFS,
)
from gluentlib.connect.connect_functions import (
    debug,
    detail,
    get_one_host_from_option,
    failure,
    log,
    section_header,
    success,
    test_header,
    warning,
)
from gluentlib.filesystem.cli_hdfs import CliHdfs
from gluentlib.filesystem.gluent_dfs import (
    OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES,
    OFFLOAD_NON_HDFS_FS_SCHEMES,
    get_scheme_from_location_uri,
    uri_component_split,
)
from gluentlib.filesystem.web_hdfs import WebHdfs
from gluentlib.offload.offload_constants import DBTYPE_SPARK
from gluentlib.offload.offload_messages import VVERBOSE
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
    sqoop_jdbc_connectivity_checker,
)
from gluentlib.offload.offload_transport_functions import (
    credential_provider_path_jvm_override,
    ssh_cmd_prefix,
)
from gluentlib.offload.offload_transport_livy_requests import (
    OffloadTransportLivyRequests,
)


def check_dir_with_msgs(hdfs_client, chk_dir, hdfs_data, msgs):
    passed = True
    dir_scheme = get_scheme_from_location_uri(chk_dir).upper()
    file_stat = hdfs_client.stat(chk_dir)
    if file_stat:
        msgs.append("%s found in %s" % (chk_dir, dir_scheme))
        if chk_dir == hdfs_data:
            if "permission" not in file_stat:
                msgs.append("Unable to read path permissions for %s" % chk_dir)
                passed = False
            elif file_stat["permission"] and file_stat["permission"][1] in (
                "3",
                "6",
                "7",
            ):
                msgs.append("%s is group writable" % chk_dir)
            else:
                msgs.append("%s is NOT group writable" % chk_dir)
                passed = False
    else:
        msgs.append("%s is NOT present in %s" % (chk_dir, dir_scheme))
        passed = False
    return passed


def get_hdfs_dirs(
    orchestration_config,
    dfs_client,
    service_name=TEST_HDFS_DIRS_SERVICE_HDFS,
    include_hdfs_home=True,
):
    """return a list of HDFS directories but NOT as a set(), we want to retain the order so
    using an "if" to ensure no duplicate output
    """
    dirs = []
    if include_hdfs_home:
        dirs.append(orchestration_config.hdfs_home)
    dirs.append(orchestration_config.hdfs_load)
    offload_data_uri = dfs_client.gen_uri(
        orchestration_config.offload_fs_scheme,
        orchestration_config.offload_fs_container,
        orchestration_config.offload_fs_prefix,
    )
    if offload_data_uri not in dirs:
        if (
            service_name == TEST_HDFS_DIRS_SERVICE_HDFS
            or orchestration_config.offload_fs_scheme
            in OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES
        ):
            dirs.append(offload_data_uri)
    return dirs


def get_cli_hdfs(orchestration_config, host, messages):
    return CliHdfs(
        host,
        orchestration_config.hadoop_ssh_user,
        dry_run=(not orchestration_config.execute),
        messages=messages,
        db_path_suffix=orchestration_config.hdfs_db_path_suffix,
        hdfs_data=orchestration_config.hdfs_data,
    )


def test_hdfs_dirs(
    orchestration_config,
    messages,
    hdfs=None,
    test_host=None,
    service_name=TEST_HDFS_DIRS_SERVICE_HDFS,
):
    test_host = (
        test_host or orchestration_config.hdfs_host or orchestration_config.hadoop_host
    )
    test_name = "%s: %s directory" % (test_host, service_name)
    test_header(test_name)

    # Setting this up for each host to prove directories visible regardless of WebHDFS usage.
    # WebHDFS will only test namenode knows of them, not test each node is configured correctly.
    use_hdfs = hdfs or get_cli_hdfs(orchestration_config, test_host, messages)
    passed = True
    msgs = []
    test_hdfs_home = (
        False
        if (
            orchestration_config.offload_fs_scheme
            and orchestration_config.offload_fs_scheme in OFFLOAD_NON_HDFS_FS_SCHEMES
        )
        else True
    )

    for chk_dir in get_hdfs_dirs(
        orchestration_config, use_hdfs, service_name, include_hdfs_home=test_hdfs_home
    ):
        try:
            if not check_dir_with_msgs(
                use_hdfs, chk_dir, orchestration_config.hdfs_data, msgs
            ):
                passed = False
        except Exception as exc:
            detail("%s: %s" % (chk_dir, exc))
            detail(traceback.format_exc())
            passed = False

    for line in msgs:
        detail(line)

    if passed:
        success(test_name)
    else:
        failure(test_name)


def test_webhdfs_config(orchestration_config, messages):
    test_name = "WebHDFS configuration"
    test_header(test_name)
    if not orchestration_config.webhdfs_host:
        detail(
            "WebHDFS host/port not supplied, using shell commands for HDFS operations (hdfs dfs, scp, etc)"
        )
        detail("Utilizing WebHDFS will reduce latency of Offload operations")
        warning(test_name)
    else:
        webhdfs_security = (
            ["Kerberos"] if orchestration_config.kerberos_service else []
        ) + ([] if orchestration_config.webhdfs_verify_ssl is None else ["SSL"])
        webhdfs_security = (
            ("using " + " and ".join(webhdfs_security))
            if webhdfs_security
            else "unsecured"
        )
        detail(
            "HDFS operations will use WebHDFS (%s:%s) %s"
            % (
                orchestration_config.webhdfs_host,
                orchestration_config.webhdfs_port,
                webhdfs_security,
            )
        )
        success(test_name)

        hdfs = WebHdfs(
            orchestration_config.webhdfs_host,
            orchestration_config.webhdfs_port,
            orchestration_config.hadoop_ssh_user,
            True if orchestration_config.kerberos_service else False,
            orchestration_config.webhdfs_verify_ssl,
            dry_run=not orchestration_config.execute,
            messages=messages,
            db_path_suffix=orchestration_config.hdfs_db_path_suffix,
            hdfs_data=orchestration_config.hdfs_data,
        )
        test_hdfs_dirs(
            orchestration_config,
            messages,
            hdfs=hdfs,
            test_host=orchestration_config.webhdfs_host,
            service_name=TEST_HDFS_DIRS_SERVICE_WEBHDFS,
        )


def test_credential_api_alias(options, orchestration_config):
    if not orchestration_config.offload_transport_password_alias:
        return

    host = orchestration_config.offload_transport_cmd_host or get_one_host_from_option(
        options.original_hadoop_host
    )
    test_name = "Offload Transport Password Alias"
    test_header(test_name)
    jvm_overrides = []
    if (
        orchestration_config.sqoop_overrides
        or orchestration_config.offload_transport_spark_overrides
    ):
        jvm_overrides.append(
            orchestration_config.sqoop_overrides
            or orchestration_config.offload_transport_spark_overrides
        )
    if orchestration_config.offload_transport_credential_provider_path:
        jvm_overrides.append(
            credential_provider_path_jvm_override(
                orchestration_config.offload_transport_credential_provider_path
            )
        )
    # Using hadoop_ssh_user below and not offload_transport_user because this is running a Hadoop CLI command
    cmd = (
        ssh_cmd_prefix(orchestration_config.hadoop_ssh_user, host=host)
        + ["hadoop", "credential"]
        + jvm_overrides
        + ["list"]
    )

    try:
        log("Cmd: %s" % " ".join(cmd), detail=VVERBOSE)
        cmd_out = subprocess.check_output(cmd)
        m = re.search(
            r"^%s[\r]?$"
            % re.escape(orchestration_config.offload_transport_password_alias),
            cmd_out,
            re.M | re.I,
        )

        if m:
            detail("Found alias: %s" % m.group())
            success(test_name)
        else:
            detail(
                'Alias "%s" not found in Hadoop credential API'
                % orchestration_config.offload_transport_password_alias
            )
            failure(test_name)

    except Exception as e:
        detail(str(e))
        failure(test_name)


def test_sqoop_pwd_file(options, orchestration_config, messages):
    if not orchestration_config.sqoop_password_file:
        return

    test_host = get_one_host_from_option(options.original_hadoop_host)

    test_name = "%s: Sqoop Password File" % test_host
    test_header(test_name)
    hdfs = get_cli_hdfs(orchestration_config, test_host, messages)
    if hdfs.stat(orchestration_config.sqoop_password_file):
        detail("%s found in HDFS" % orchestration_config.sqoop_password_file)
        success(test_name)
    else:
        detail(
            "Sqoop Password File not found: %s"
            % orchestration_config.sqoop_password_file
        )
        failure(test_name)


def test_sqoop_import(orchestration_config, messages):
    data_transport_client = sqoop_jdbc_connectivity_checker(
        orchestration_config, messages
    )
    verify_offload_transport_rdbms_connectivity(data_transport_client, "Sqoop")


def run_sqoop_tests(options, orchestration_config, messages):
    test_sqoop_pwd_file(options, orchestration_config, messages)
    test_sqoop_import(orchestration_config, messages)


def is_spark_thrift_connector_available(orchestration_config):
    return bool(
        orchestration_config.spark_thrift_host
        and orchestration_config.spark_thrift_port
        and orchestration_config.connector_sql_engine == DBTYPE_SPARK
    )


def run_spark_tests(options, orchestration_config, messages):
    if not (
        is_livy_available(orchestration_config, None)
        or is_spark_submit_available(orchestration_config, None)
        or is_spark_gcloud_available(orchestration_config, None)
        or is_spark_thrift_available(orchestration_config, None)
        or is_spark_thrift_connector_available(orchestration_config)
    ):
        log("Skipping Spark tests because not configured", detail=VVERBOSE)
        return

    section_header("Spark")
    test_spark_thrift_server(options, orchestration_config, messages)
    test_spark_submit(orchestration_config, messages)
    test_spark_gcloud(orchestration_config, messages)
    test_spark_livy_api(orchestration_config, messages)


def test_spark_thrift_server(options, orchestration_config, messages):
    hosts_to_check, extra_connector_hosts = set(), set()
    if (
        options.original_offload_transport_spark_thrift_host
        and orchestration_config.offload_transport_spark_thrift_port
    ):
        hosts_to_check = set(
            (_, orchestration_config.offload_transport_spark_thrift_port)
            for _ in options.original_offload_transport_spark_thrift_host.split(",")
        )
        log(
            "Spark thrift hosts for Offload transport: %s" % str(hosts_to_check),
            detail=VVERBOSE,
        )

    if not hosts_to_check:
        log("Skipping Spark Thrift Server tests due to absent config", detail=VVERBOSE)
        return

    detail(
        "Testing Spark Thrift Server hosts individually: %s"
        % ", ".join(host for host, _ in hosts_to_check)
    )
    spark_options = copy.copy(orchestration_config)
    for host, port in hosts_to_check:
        spark_options.hadoop_host = host
        spark_options.hadoop_port = port
        backend_spark_api = test_backend_db_connectivity(
            spark_options, orchestration_config, messages
        )
        del backend_spark_api

    # test_backend_db_connectivity() will exit on failure so we know it is sound to proceed if we get this far
    if (
        orchestration_config.offload_transport_spark_thrift_host
        and orchestration_config.offload_transport_spark_thrift_port
    ):
        # we only connect back to the RDBMS from Spark for offload transport
        data_transport_client = spark_thrift_jdbc_connectivity_checker(
            orchestration_config, messages
        )
        verify_offload_transport_rdbms_connectivity(
            data_transport_client, "Spark Thrift Server"
        )


def test_spark_livy_api(orchestration_config, messages):
    if not orchestration_config.offload_transport_livy_api_url:
        log("Skipping Spark Livy tests due to absent config", detail=VVERBOSE)
        return

    test_name = "Spark Livy settings"
    test_header(test_name)
    try:
        sessions_url = URL_SEP.join(
            [orchestration_config.offload_transport_livy_api_url, LIVY_SESSIONS_SUBURL]
        )
        detail(sessions_url)
        livy_requests = OffloadTransportLivyRequests(orchestration_config, messages)
        resp = livy_requests.get(sessions_url)
        if resp.ok:
            log("Good Livy response: %s" % str(resp.text), detail=VVERBOSE)
        else:
            detail("Response code: %s" % resp.status_code)
            detail("Response text: %s" % resp.text)
            resp.raise_for_status()
        success(test_name)
    except Exception as exc:
        detail(str(exc))
        failure(test_name)
        # no sense in more Livy checks if this fails
        return

    data_transport_client = spark_livy_jdbc_connectivity_checker(
        orchestration_config, messages
    )
    verify_offload_transport_rdbms_connectivity(data_transport_client, "Livy Server")


def test_spark_submit(orchestration_config, messages):
    if (
        not orchestration_config.offload_transport_spark_submit_executable
        or not orchestration_config.offload_transport_cmd_host
    ):
        log("Skipping Spark Submit tests due to absent config", detail=VVERBOSE)
        return

    test_name = "Spark Submit settings"
    test_header(test_name)
    if spark_submit_executable_exists(orchestration_config, messages):
        detail(
            "Executable %s exists"
            % orchestration_config.offload_transport_spark_submit_executable
        )
        success(test_name)
    else:
        detail(
            "Executable %s does not exist"
            % orchestration_config.offload_transport_spark_submit_executable
        )
        failure(test_name)
        # no sense in more spark-submit checks if this fails
        return

    data_transport_client = spark_submit_jdbc_connectivity_checker(
        orchestration_config, messages
    )
    verify_offload_transport_rdbms_connectivity(data_transport_client, "Spark Submit")


def test_spark_gcloud(orchestration_config, messages):
    if (
        not orchestration_config.google_dataproc_cluster
        or not orchestration_config.offload_transport_cmd_host
    ):
        log("Skipping Spark gcloud tests due to absent config", detail=VVERBOSE)
        return

    test_name = "Spark Dataproc settings"
    test_header(test_name)
    if spark_submit_executable_exists(
        orchestration_config,
        messages,
        executable_override=OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
    ):
        detail(f"Executable {OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE} exists")
        success(test_name)
    else:
        detail(f"Executable {OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE} does not exist")
        failure(test_name)
        # no sense in more gcloud checks if this fails
        return

    data_transport_client = spark_dataproc_jdbc_connectivity_checker(
        orchestration_config, messages
    )
    verify_offload_transport_rdbms_connectivity(data_transport_client, "Spark Dataproc")


def verify_offload_transport_rdbms_connectivity(data_transport_client, transport_type):
    test_name = "RDBMS connection: %s" % transport_type
    test_header(test_name)
    try:
        if data_transport_client.ping_source_rdbms():
            detail("Connection successful")
            success(test_name)
        else:
            failure(test_name)
    except Exception as exc:
        debug(traceback.format_exc())
        detail(str(exc))
        failure(test_name)
