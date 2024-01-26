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


def test_query_import_construct(config, messages, oracle_table):
    fake_operation = build_mock_offload_operation()
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
    config, messages, oracle_table, small_table_threshold, expected_status
):
    fake_operation = build_mock_offload_operation()
    fake_operation.offload_transport_small_table_threshold = small_table_threshold
    assert (
        is_query_import_available(
            fake_operation, config, oracle_table, messages=messages
        )
        == expected_status
    )


def test_sqoop_construct(config, messages, oracle_table):
    fake_operation = build_mock_offload_operation()
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


def test_spark_submit_construct(config, messages, oracle_table):
    fake_operation = build_mock_offload_operation()
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


def test_dataproc(config, messages, oracle_table):
    fake_operation = build_mock_offload_operation()
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


def test_dataproc_batches(config, messages, oracle_table):
    fake_operation = build_mock_offload_operation()
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
    cmd = client._gcloud_dataproc_command()
    assert isinstance(cmd, list)


def test_dataproc_batches_canary_construct():
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    messages = OffloadMessages()
    _ = spark_dataproc_batches_jdbc_connectivity_checker(config, messages)
