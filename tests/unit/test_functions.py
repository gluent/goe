import os
from unittest import mock

from goe.config.orchestration_config import OrchestrationConfig


FAKE_COMMON_ENV = {
    "DB_NAME_PREFIX": "x",
    "OFFLOAD_LOG": "/tmp",
    "OFFLOAD_TRANSPORT_USER": "a",
    "OFFLOAD_TRANSPORT_CMD_HOST": "localhost",
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
        "QUERY_ENGINE": "IMPALA",
    }
)

FAKE_ORACLE_SNOWFLAKE_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_SNOWFLAKE_ENV.update(
    {
        "BACKEND_DISTRIBUTION": "SNOWFLAKE",
        "QUERY_ENGINE": "SNOWFLAKE",
        "SNOWFLAKE_USER": "u",
        "SNOWFLAKE_PASS": "p",
        "SNOWFLAKE_ACCOUNT": "a",
        "SNOWFLAKE_DATABASE": "d",
        "SNOWFLAKE_ROLE": "r",
        "SNOWFLAKE_FILE_FORMAT_PREFIX": "GLUENT_OFFLOAD_FILE_FORMAT",
        "SNOWFLAKE_INTEGRATION": "i",
        "SNOWFLAKE_STAGE": "s",
        "SNOWFLAKE_WAREHOUSE": "w",
    }
)


FAKE_ORACLE_SYNAPSE_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_SYNAPSE_ENV.update(
    {
        "BACKEND_DISTRIBUTION": "MSAZURE",
        "QUERY_ENGINE": "SYNAPSE",
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
    }
)


def build_mock_options(mock_env: dict):
    assert mock_env
    k = mock.patch.dict(os.environ, mock_env)
    k.start()
    c = OrchestrationConfig.from_dict({"verbose": False, "execute": False})
    k.stop()
    return c
