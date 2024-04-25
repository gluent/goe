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


def test_dataproc(config, messages, oracle_table, fake_operation):
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
    cmd = client._gcloud_dataproc_command()
    assert isinstance(cmd, list)


def test_dataproc_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = spark_dataproc_jdbc_connectivity_checker(config, messages)


def test_dataproc_batches_cmd(config, messages, oracle_table, fake_operation):
    fake_dfs_client = Mock()
    fake_target_table = Mock()
    # Mock some settings that we can check in the final command.
    config.google_dataproc_project = "p"
    config.google_dataproc_batches_ttl = "4d"

    client = offload_transport_factory(
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        oracle_table,
        fake_target_table,
        fake_operation,
        config,
        messages,
        fake_dfs_client,
    )
    cmd = client._gcloud_dataproc_command()
    assert isinstance(cmd, list)

    assert (
        f"--project={config.google_dataproc_project}" in cmd
    ), f"project option is missing from cmd: {cmd}"

    # TTL.
    assert any("--ttl=" in _ for _ in cmd), f"ttl option is missing from cmd: {cmd}"


@pytest.mark.parametrize(
    "cmd_output,expect_exception",
    [
        (
            """Batch [goe-goetest-20240423092754] submitted.
Using the default container image
Waiting for container log creation
PYSPARK_PYTHON=/opt/dataproc/conda/bin/python
JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
SPARK_EXTRA_CLASSPATH=
:: loading settings :: file = /etc/spark/conf/ivysettings.xml
com.oracle.database.jdbc#ojdbc11 added as a dependency
[SUCCESSFUL ] com.oracle.database.jdbc#ojdbc11;23.2.0.0!ojdbc11.jar (470ms)
:: resolution report :: resolve 2419ms :: artifacts dl 822ms
---------------------------------------------------------------------
|                  |            modules            ||   artifacts   |
|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
---------------------------------------------------------------------
|      default     |   4   |   4   |   4   |   0   ||   4   |   4   |
---------------------------------------------------------------------
4 artifacts copied, 0 already retrieved (7106kB/18ms)
('spark.app.id', 'app-20240423092933-0000')
root
|-- PROD_ID: integer (nullable = true)
|-- CUST_ID: integer (nullable = true)
|-- TIME_ID: string (nullable = true)
|-- CHANNEL_ID: integer (nullable = true)
|-- PROMO_ID: string (nullable = true)
|-- QUANTITY_SOLD: string (nullable = true)
|-- AMOUNT_SOLD: string (nullable = true)
|-- GOE_OFFLOAD_BATCH: decimal(38,10) (nullable = true)

24/04/23 09:29:44 INFO PathOutputCommitterFactory: No output committer factory defined, defaulting to FileOutputCommitterFactory
24/04/23 09:29:57 INFO GOETaskListener: {"taskInfo.id":"7.0","taskInfo.taskId":7,"taskInfo.launchTime":1713864587841,"taskInfo.finishTime":1713864597253,"duration":9412,"recordsWritten":0,"executorRunTime":7588}
24/04/23 09:29:57 INFO GOETaskListener: {"taskInfo.id":"5.0","taskInfo.taskId":5,"taskInfo.launchTime":1713864587840,"taskInfo.finishTime":1713864597258,"duration":9418,"recordsWritten":0,"executorRunTime":7584}
Batch [goe-goetest-20240423092754] finished.
metadata:
'@type': type.com/g.c.d.v1.BatchOperationMetadata
batch: projects/goe/locations/west1/batches/goe-goetest-storyfact-20240423092754
batchUuid: 01f7d-ce45-4756-aafc-1c3c9015a
createTime: '2024-04-23T09:27:56.885549Z'
description: Batch
labels:
goog-dataproc-batch-id: goe-goetest-20240423092754
goog-dataproc-batch-uuid: 01f7d-ce45-4756-aafc-1cc4b015a
goog-dataproc-location: west1
operationType: BATCH
name: projects/goe/regions/west1/operations/efb33-a3f8-3d12-babe-de36f5147""",
            False,
        ),
        (
            """Batch [goe-goetest-20240423092754] submitted.
Using the default container image
Waiting for container log creation
PYSPARK_PYTHON=/opt/dataproc/conda/bin/python
JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
SPARK_EXTRA_CLASSPATH=
:: loading settings :: file = /etc/spark/conf/ivysettings.xml
com.oracle.database.jdbc#ojdbc11 added as a dependency
[SUCCESSFUL ] com.oracle.database.jdbc#ojdbc11;23.2.0.0!ojdbc11.jar (470ms)
:: resolution report :: resolve 2419ms :: artifacts dl 822ms
---------------------------------------------------------------------
|                  |            modules            ||   artifacts   |
|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
---------------------------------------------------------------------
|      default     |   4   |   4   |   4   |   0   ||   4   |   4   |
---------------------------------------------------------------------
4 artifacts copied, 0 already retrieved (7106kB/18ms)
('spark.app.id', 'app-20240423092933-0000')
root
|-- PROD_ID: integer (nullable = true)
|-- CUST_ID: integer (nullable = true)
|-- TIME_ID: string (nullable = true)
|-- CHANNEL_ID: integer (nullable = true)
|-- PROMO_ID: string (nullable = true)
|-- QUANTITY_SOLD: string (nullable = true)
|-- AMOUNT_SOLD: string (nullable = true)
|-- GOE_OFFLOAD_BATCH: decimal(38,10) (nullable = true)

24/04/23 09:29:44 INFO PathOutputCommitterFactory: No output committer factory defined, defaulting to FileOutputCommitterFactory
24/04/23 09:29:57 INFO GOETaskListener: {"taskInfo.id":"7.0","taskInfo.taskId":7,"taskInfo.launchTime":1713864587841,"taskInfo.finishTime":1713864597253,"duration":9412,"recordsWritten":0,"executorRunTime":7588}
24/04/23 09:29:57 INFO GOETaskListener: {"taskInfo.id":"5.0","taskInfo.taskId":5,"taskInfo.launchTime":1713864587840,"taskInfo.finishTime":1713864597258,"duration":9418,"recordsWritten":0,"executorRunTime":7584}
WARNING: Batch job is CANCELLED.
Batch [goe-goetest-20240423092754] finished.
metadata:
'@type': type.com/g.c.d.v1.BatchOperationMetadata
batch: projects/goe/locations/west1/batches/goe-goetest-storyfact-20240423092754
batchUuid: 01f7d-ce45-4756-aafc-1c3c9015a
createTime: '2024-04-23T09:27:56.885549Z'
description: Batch
labels:
goog-dataproc-batch-id: goe-goetest-20240423092754
goog-dataproc-batch-uuid: 01f7d-ce45-4756-aafc-1cc4b015a
goog-dataproc-location: west1
operationType: BATCH
name: projects/goe/regions/west1/operations/efb33-a3f8-3d12-babe-de36f5147""",
            True,
        ),
    ],
)
def test_dataproc_batches_output(
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
            client._verify_batches_log(cmd_output)
    else:
        client._verify_batches_log(cmd_output)


def test_dataproc_batches_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = spark_dataproc_batches_jdbc_connectivity_checker(config, messages)
