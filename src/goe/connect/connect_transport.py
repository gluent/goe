# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import re
import subprocess
import traceback

from goe.connect.connect_backend import (
    get_cli_hdfs,
    test_backend_db_connectivity,
)
from goe.connect.connect_functions import (
    debug,
    detail,
    get_one_host_from_option,
    failure,
    log,
    section_header,
    success,
    test_header,
)
from goe.offload.factory.offload_transport_factory import (
    spark_dataproc_batches_jdbc_connectivity_checker,
    spark_dataproc_jdbc_connectivity_checker,
    spark_submit_jdbc_connectivity_checker,
    spark_thrift_jdbc_connectivity_checker,
    spark_livy_jdbc_connectivity_checker,
    sqoop_jdbc_connectivity_checker,
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_transport import (
    OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
    spark_submit_executable_exists,
    is_spark_gcloud_available,
    is_spark_gcloud_batches_available,
    is_spark_gcloud_dataproc_available,
    is_spark_submit_available,
    is_spark_thrift_available,
    is_livy_available,
)
from goe.offload.offload_transport_functions import (
    credential_provider_path_jvm_override,
    ssh_cmd_prefix,
)
from goe.offload.spark.livy_offload_transport import (
    URL_SEP,
    LIVY_SESSIONS_SUBURL,
)
from goe.offload.spark.offload_transport_livy_requests import (
    OffloadTransportLivyRequests,
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


def run_spark_tests(options, orchestration_config, messages):
    if not (
        is_livy_available(orchestration_config, None)
        or is_spark_submit_available(orchestration_config, None)
        or is_spark_gcloud_available(orchestration_config, None)
        or is_spark_thrift_available(orchestration_config, None)
    ):
        log("Skipping Spark tests because not configured", detail=VVERBOSE)
        return

    section_header("Spark")
    test_spark_thrift_server(options, orchestration_config, messages)
    test_spark_submit(orchestration_config, messages)
    test_spark_gcloud(orchestration_config, messages)
    test_spark_livy_api(orchestration_config, messages)


def test_spark_thrift_server(options, orchestration_config, messages):
    hosts_to_check = set()
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
        and not orchestration_config.google_dataproc_batches_version
    ) or not orchestration_config.offload_transport_cmd_host:
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

    if is_spark_gcloud_dataproc_available(
        orchestration_config, None, messages=messages
    ):
        data_transport_client = spark_dataproc_jdbc_connectivity_checker(
            orchestration_config, messages
        )
        verify_offload_transport_rdbms_connectivity(
            data_transport_client, "Spark Dataproc"
        )

    if is_spark_gcloud_batches_available(orchestration_config, None, messages=messages):
        data_transport_client = spark_dataproc_batches_jdbc_connectivity_checker(
            orchestration_config, messages
        )
        verify_offload_transport_rdbms_connectivity(
            data_transport_client, "Spark Dataproc Batches"
        )


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
