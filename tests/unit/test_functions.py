import os
from unittest import mock

import numpy

from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.column_metadata import ColumnPartitionInfo
from goe.offload.offload_source_table import RdbmsPartition
from goe.offload.oracle.oracle_column import (
    OracleColumn,
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
)
from goe.offload.oracle.oracle_offload_source_table import OracleSourceTable


FAKE_COMMON_ENV = {
    "DB_NAME_PREFIX": "x",
    "OFFLOAD_HOME": "/opt/goe/offload",
    "OFFLOAD_LOG": "/tmp",
    "OFFLOAD_FS_CONTAINER": "b",
    "OFFLOAD_TRANSPORT_USER": "a",
    "OFFLOAD_TRANSPORT_CMD_HOST": "localhost",
    "OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE": "spark-submit",
}

#
# Frontend mock environment variables.
#
FAKE_MSSQL_ENV = {
    "FRONTEND_DISTRIBUTION": "MSSQL",
    "MSSQL_CONN": "c",
    "MSSQL_APP_USER": "a",
    "MSSQL_APP_PASS": "b",
    "FRONTEND_ODBC_DRIVER_NAME": "o",
}
FAKE_MSSQL_ENV.update(FAKE_COMMON_ENV)

FAKE_NETEZZA_ENV = {
    "FRONTEND_DISTRIBUTION": "NETEZZA",
    "NETEZZA_CONN": "c",
    "NETEZZA_APP_USER": "a",
    "NETEZZA_APP_PASS": "b",
}
FAKE_NETEZZA_ENV.update(FAKE_COMMON_ENV)

FAKE_ORACLE_ENV = {
    "FRONTEND_DISTRIBUTION": "ORACLE",
    "ORA_CONN": "hostname:1521/service",
    "ORA_ADM_USER": "a",
    "ORA_ADM_PASS": "b",
    "ORA_APP_USER": "a",
    "ORA_APP_PASS": "b",
    "ORA_REPO_USER": "a",
}
FAKE_ORACLE_ENV.update(FAKE_COMMON_ENV)

FAKE_TERADATA_ENV = {
    "FRONTEND_DISTRIBUTION": "TERADATA",
    "TERADATA_SERVER": "s",
    "TERADATA_ADM_USER": "a",
    "TERADATA_ADM_PASS": "b",
    "TERADATA_APP_USER": "a",
    "TERADATA_APP_PASS": "b",
    "TERADATA_REPO_USER": "a",
    "FRONTEND_ODBC_DRIVER_NAME": "o",
}
FAKE_TERADATA_ENV.update(FAKE_COMMON_ENV)

#
# Full mock environment variable combinations.
#
FAKE_ORACLE_BQ_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_BQ_ENV.update(
    {
        "BACKEND_DISTRIBUTION": "GCP",
        "BIGQUERY_DATASET_PROJECT": "bq-project",
        "GOOGLE_DATAPROC_CLUSTER": "cluster-name",
        "GOOGLE_DATAPROC_PROJECT": "dp-project",
        "GOOGLE_DATAPROC_REGION": "us-central1",
        "GOOGLE_DATAPROC_BATCHES_SUBNET": "my-subnet1",
        "GOOGLE_DATAPROC_BATCHES_VERSION": "1.1",
        "GOOGLE_KMS_KEY_RING_PROJECT": "kms-project",
        "GOOGLE_KMS_KEY_RING_LOCATION": "US",
        "GOOGLE_KMS_KEY_RING_NAME": "ring-name",
        "GOOGLE_KMS_KEY_NAME": "key-name",
        "OFFLOAD_FS_SCHEME": "gs",
        "QUERY_ENGINE": "BIGQUERY",
    }
)

FAKE_ORACLE_HIVE_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_HIVE_ENV.update(
    {
        "HADOOP_SSH_USER": "a",
        "HDFS_DATA": "/tmp/a",
        "HDFS_HOME": "/tmp/a",
        "HDFS_LOAD": "/tmp/a_load",
        "HIVE_SERVER_HOST": "h",
        "HIVE_SERVER_USER": "x",
        "OFFLOAD_FS_SCHEME": "inherit",
        "QUERY_ENGINE": "HIVE",
    }
)

FAKE_ORACLE_IMPALA_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_IMPALA_ENV.update(
    {
        "HADOOP_SSH_USER": "a",
        "HDFS_DATA": "/tmp/a",
        "HDFS_HOME": "/tmp/a",
        "HDFS_LOAD": "/tmp/a_load",
        "HIVE_SERVER_HOST": "h",
        "OFFLOAD_FS_SCHEME": "inherit",
        "QUERY_ENGINE": "IMPALA",
    }
)

FAKE_ORACLE_SNOWFLAKE_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_SNOWFLAKE_ENV.update(
    {
        "BACKEND_DISTRIBUTION": "SNOWFLAKE",
        "SNOWFLAKE_USER": "u",
        "SNOWFLAKE_PASS": "p",
        "SNOWFLAKE_ACCOUNT": "a",
        "SNOWFLAKE_DATABASE": "d",
        "SNOWFLAKE_ROLE": "r",
        "SNOWFLAKE_FILE_FORMAT_PREFIX": "GOE_OFFLOAD_FILE_FORMAT",
        "SNOWFLAKE_INTEGRATION": "i",
        "SNOWFLAKE_STAGE": "s",
        "SNOWFLAKE_WAREHOUSE": "w",
        "OFFLOAD_FS_SCHEME": "gs",
        "QUERY_ENGINE": "SNOWFLAKE",
    }
)


FAKE_ORACLE_SYNAPSE_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_SYNAPSE_ENV.update(
    {
        "BACKEND_DISTRIBUTION": "MSAZURE",
        "SYNAPSE_DATABASE": "d",
        "SYNAPSE_SERVER": "p",
        "SYNAPSE_PORT": "123",
        "SYNAPSE_ROLE": "r",
        "SYNAPSE_AUTH_MECHANISM": "SqlPassword",
        "SYNAPSE_USER": "u",
        "SYNAPSE_PASS": "p",
        "SYNAPSE_DATA_SOURCE": "d",
        "SYNAPSE_FILE_FORMAT": "f",
        "BACKEND_ODBC_DRIVER_NAME": "ms",
        "OFFLOAD_FS_SCHEME": "wasb",
        "QUERY_ENGINE": "SYNAPSE",
    }
)

FAKE_ORACLE_COLUMNS = [
    OracleColumn(
        "ID",
        ORACLE_TYPE_NUMBER,
        data_precision=8,
        data_scale=0,
    ),
    OracleColumn(
        "TIME_ID",
        ORACLE_TYPE_DATE,
        partition_info=ColumnPartitionInfo(
            position=0, range_end=None, range_start=None, source_column_name=None
        ),
    ),
    OracleColumn(
        "PROD_ID",
        ORACLE_TYPE_NUMBER,
        data_precision=4,
        data_scale=0,
    ),
]

FAKE_ORACLE_PARTITIONS = [
    RdbmsPartition.by_name(
        partition_name="P4",
        partition_count=1,
        partition_position=8,
        subpartition_count=0,
        subpartition_name=None,
        subpartition_names=None,
        subpartition_position=None,
        high_values_csv="TO_DATE(' 2012-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        high_values_python=(numpy.datetime64("2012-04-01T00:00:00"),),
        partition_size=1_000_000,
        num_rows=2,
        high_values_individual=(
            "TO_DATE(' 2012-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        ),
    ),
    RdbmsPartition.by_name(
        partition_name="P3",
        partition_count=1,
        partition_position=3,
        subpartition_count=0,
        subpartition_name=None,
        subpartition_names=None,
        subpartition_position=None,
        high_values_csv="TO_DATE(' 2012-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        high_values_python=(numpy.datetime64("2012-03-01T00:00:00"),),
        partition_size=1_000_000,
        num_rows=2,
        high_values_individual=(
            "TO_DATE(' 2012-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        ),
    ),
    RdbmsPartition.by_name(
        partition_name="P2",
        partition_count=1,
        partition_position=2,
        subpartition_count=0,
        subpartition_name=None,
        subpartition_names=None,
        subpartition_position=None,
        high_values_csv="TO_DATE(' 2012-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        high_values_python=(numpy.datetime64("2012-02-01T00:00:00"),),
        partition_size=1_000_000,
        num_rows=2,
        high_values_individual=(
            "TO_DATE(' 2012-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        ),
    ),
    RdbmsPartition.by_name(
        partition_name="P1",
        partition_count=1,
        partition_position=1,
        subpartition_count=0,
        subpartition_name=None,
        subpartition_names=None,
        subpartition_position=None,
        high_values_csv="TO_DATE(' 2012-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        high_values_python=(numpy.datetime64("2012-01-01T00:00:00"),),
        partition_size=1_000_000,
        num_rows=2,
        high_values_individual=(
            "TO_DATE(' 2012-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
        ),
    ),
]


def build_mock_options(mock_env: dict):
    assert mock_env
    k = mock.patch.dict(os.environ, mock_env)
    k.start()
    c = OrchestrationConfig.from_dict({"verbose": False, "execute": False})
    k.stop()
    return c


def build_mock_offload_operation():
    fake_operation = mock.Mock()
    fake_operation.execute = False
    fake_operation.allow_floating_point_conversions = False
    fake_operation.offload_transport_fetch_size = 100
    fake_operation.offload_transport_small_table_threshold = 1024 * 1024
    fake_operation.offload_transport_spark_properties = {}
    fake_operation.unicode_string_columns_csv = None
    fake_operation.max_offload_chunk_size = 100 * 1024 * 1024
    fake_operation.max_offload_chunk_count = 100
    return fake_operation


def build_fake_oracle_table(config, messages) -> OracleSourceTable:
    test_table_object = OracleSourceTable(
        "no_user",
        "no_table",
        config,
        messages,
        dry_run=True,
        do_not_connect=True,
    )
    test_table_object._columns = FAKE_ORACLE_COLUMNS
    test_table_object._columns_with_partition_info = FAKE_ORACLE_COLUMNS
    test_table_object._primary_key_columns = ["ID"]
    test_table_object._partitions = FAKE_ORACLE_PARTITIONS
    test_table_object._iot_type = None
    return test_table_object
