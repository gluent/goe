# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from unittest.mock import Mock

from goe.offload.factory.offload_transport_factory import (
    offload_transport_factory,
    spark_dataproc_batches_jdbc_connectivity_checker,
    spark_dataproc_jdbc_connectivity_checker,
    spark_submit_jdbc_connectivity_checker,
    sqoop_jdbc_connectivity_checker,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_transport import (
    is_query_import_available,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
)
from goe.offload.oracle.oracle_column import (
    OracleColumn,
    ORACLE_TYPE_VARCHAR2,
)
from goe.offload.oracle.oracle_offload_source_table import OracleSourceTable

from tests.unit.test_functions import (
    build_mock_options,
    build_mock_offload_operation,
    FAKE_ORACLE_BQ_ENV,
)


FRONTEND_COLUMNS = [
    OracleColumn(
        "COL_VARCHAR2",
        ORACLE_TYPE_VARCHAR2,
        char_length=100,
        data_length=100,
    )
]


@pytest.fixture
def config():
    return build_mock_options(FAKE_ORACLE_BQ_ENV)


@pytest.fixture
def messages():
    return OffloadMessages()


@pytest.fixture
def oracle_table(config, messages):
    test_table_object = OracleSourceTable(
        "no_user",
        "no_table",
        config,
        messages,
        dry_run=True,
        do_not_connect=True,
    )
    test_table_object._columns = FRONTEND_COLUMNS
    test_table_object._columns_with_partition_info = FRONTEND_COLUMNS
    test_table_object._size_in_bytes = 1024 * 1024
    return test_table_object


@pytest.fixture
def fake_operation():
    return build_mock_offload_operation()


def test_query_import_construct(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    _ = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )


@pytest.mark.parametrize(
    "small_table_threshold,expected_status",
    [(0, False), (1, False), (999_999_999_999, True)],
)
def test_is_query_import_available(
    config,
    messages,
    oracle_table,
    fake_operation,
    small_table_threshold,
    expected_status,
):
    fake_operation.offload_transport_small_table_threshold = small_table_threshold
    assert (
        is_query_import_available(
            fake_operation, config, oracle_table, messages=messages
        )
        == expected_status
    )


def test_sqoop_construct(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    _ = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SQOOP,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )


def test_sqoop_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = sqoop_jdbc_connectivity_checker(config, messages)


def test_spark_submit_construct(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    _ = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )


def test_spark_submit_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = spark_submit_jdbc_connectivity_checker(config, messages)


def test_dataproc_cmd(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    client = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )
    cmd = client._gcloud_dataproc_submit_command()
    assert isinstance(cmd, list)

    assert (
        f"--project={config.google_dataproc_project}" in cmd
    ), f"project option is missing from cmd: {cmd}"
    assert (
        f"--cluster={config.google_dataproc_cluster}" in cmd
    ), f"cluster option is missing from cmd: {cmd}"
    assert (
        f"--region={config.google_dataproc_region}" in cmd
    ), f"region option is missing from cmd: {cmd}"
    # batch option should NOT be in standard Dataproc job commands.
    assert all(
        "--batch=" not in _ for _ in cmd
    ), f"batch option is incorrectly in cmd: {cmd}"


def test_dataproc_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = spark_dataproc_jdbc_connectivity_checker(config, messages)


def test_dataproc_batches_cmd(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    client = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )
    cmd = client._gcloud_dataproc_submit_command()
    assert isinstance(cmd, list)

    assert (
        f"--project={config.google_dataproc_project}" in cmd
    ), f"project option is missing from cmd: {cmd}"
    assert (
        f"--region={config.google_dataproc_region}" in cmd
    ), f"region option is missing from cmd: {cmd}"
    assert any("--batch=" in _ for _ in cmd), f"batch option is missing from cmd: {cmd}"
    assert (
        f"--service-account={config.google_dataproc_service_account}" in cmd
    ), f"service account option is missing from cmd: {cmd}"
    assert (
        f"--ttl={config.google_dataproc_batches_ttl}" in cmd
    ), f"ttl option is missing from cmd: {cmd}"


def test_dataproc_batches_describe_cmd(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    client = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )
    batch_name = "my-unit-batch"
    cmd = client._gcloud_dataproc_describe_command(batch_name)
    assert isinstance(cmd, list)

    assert (
        f"--project={config.google_dataproc_project}" in cmd
    ), f"project option is missing from cmd: {cmd}"
    assert (
        f"--region={config.google_dataproc_region}" in cmd
    ), f"region option is missing from cmd: {cmd}"
    assert batch_name in cmd, f"batch '{batch_name}' is missing from cmd: {cmd}"


@pytest.mark.parametrize(
    "cmd_output,expect_exception",
    [
        # Describe output for a successful job.
        (
            """{
  "createTime": "2024-04-26T08:10:01.214382Z",
  "creator": "sa@p.iam.gserviceaccount.com",
  "environmentConfig": {
    "executionConfig": {
      "serviceAccount": "sa@p.iam.gserviceaccount.com",
      "subnetworkUri": "projects/p/regions/west1/subnetworks/s",
      "ttl": "86400s"
    },
    "peripheralsConfig": {
      "sparkHistoryServerConfig": {}
    }
  },
  "name": "projects/p/locations/west1/batches/goe-batch-20240426080958",
  "operation": "projects/p/locations/west1/operations/b4873-4952-3f5d-884d-cec5a2d09",
  "state": "SUCCEEDED",
  "stateHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2024-04-26T08:10:01.214382Z"
    },
    {
      "state": "RUNNING",
      "stateStartTime": "2024-04-26T08:12:02.032731Z"
    }
  ],
  "stateTime": "2024-04-26T08:13:55.084692Z",
  "uuid": "12cea"
}""",
            False,
        ),
        # Describe output for a cancelled job.
        (
            """{
  "createTime": "2024-04-26T10:09:11.916627Z",
  "creator": "sa@p.iam.gserviceaccount.com",
  "environmentConfig": {
    "executionConfig": {
      "serviceAccount": "sa@p.iam.gserviceaccount.com",
      "subnetworkUri": "projects/p/regions/west1/subnetworks/s",
      "ttl": "600s"
    },
    "peripheralsConfig": {
      "sparkHistoryServerConfig": {}
    }
  },
  "name": "projects/p/locations/west1/batches/goe-batch-20240426080958",
  "operation": "projects/p/locations/west1/operations/b4873-4952-3f5d-884d-cec5a2d09",
  "state": "CANCELLED",
  "stateHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2024-04-26T10:09:11.916627Z"
    },
    {
      "state": "RUNNING",
      "stateStartTime": "2024-04-26T10:10:33.461859Z"
    },
    {
      "state": "CANCELLING",
      "stateMessage": "Cancelling batch as ttl exceeded",
      "stateStartTime": "2024-04-26T10:19:12.387649Z"
    }
  ],
  "stateTime": "2024-04-26T10:19:12.454387Z",
  "uuid": "12cea"
}""",
            True,
        ),
        # Describe output for a failed job.
        (
            """{
  "createTime": "2024-04-26T10:09:11.916627Z",
  "creator": "sa@p.iam.gserviceaccount.com",
  "environmentConfig": {
    "executionConfig": {
      "serviceAccount": "sa@p.iam.gserviceaccount.com",
      "subnetworkUri": "projects/p/regions/west1/subnetworks/s",
      "ttl": "600s"
    },
    "peripheralsConfig": {
      "sparkHistoryServerConfig": {}
    }
  },
  "name": "projects/p/locations/west1/batches/goe-batch-20240426080958",
  "operation": "projects/p/locations/west1/operations/a2d09",
  "state": "FAILED",
  "stateHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2024-04-26T10:25:59.242854Z"
    },
    {
      "state": "RUNNING",
      "stateStartTime": "2024-04-26T10:27:24.305264Z"
    }
  ],
  "stateMessage": "Job failed with message [SyntaxError: invalid syntax]. Additional details can be found at:\\nhttps://console.cloud.google.com/dataproc/batches/west1/goe-batch-20240426080958?project=p\\ngcloud dataproc batches wait 'goe-batch-20240426080958' --region 'west1' --project 'p'\\nhttps://console.cloud.google.com/storage/browser/dataproc-staging-west1-123-l/batch-3347f/\\ngs://dataproc-staging-west1-123-l/google-cloud-dataproc-metainfo/2ad11/jobs/srvls-batch-3347f/driveroutput.*",
  "stateTime": "2024-04-26T10:27:48.237750Z",
  "uuid": "12cea"
}""",
            True,
        ),
    ],
)
def test_dataproc_batch_describe(
    config,
    messages,
    oracle_table,
    fake_operation,
    cmd_output: str,
    expect_exception: bool,
):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    client = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )
    if expect_exception:
        with pytest.raises(Exception) as _:
            client._verify_batch_describe_response(cmd_output)
    else:
        client._verify_batch_describe_response(cmd_output)


def test_dataproc_batches_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = spark_dataproc_batches_jdbc_connectivity_checker(config, messages)
