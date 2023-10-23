import os
from unittest import mock

from gluentlib.config.orchestration_config import OrchestrationConfig


FAKE_ORACLE_ENV = {
    # Common config
    "DB_NAME_PREFIX": "x",
    "OFFLOAD_LOG": "/tmp",
    "OFFLOAD_TRANSPORT_USER": "a",
    # Oracle specific config
    "ORA_CONN": "hostname:1521/service",
    "ORA_ADM_USER": "a",
    "ORA_ADM_PASS": "b",
    "ORA_APP_USER": "a",
    "ORA_APP_PASS": "b",
    "ORA_REPO_USER": "a",
}

FAKE_ORACLE_BQ_ENV = dict(FAKE_ORACLE_ENV)

FAKE_ORACLE_HIVE_ENV = dict(FAKE_ORACLE_ENV)
FAKE_ORACLE_HIVE_ENV.update(
    {
        "HADOOP_SSH_USER": "a",
        "HDFS_HOME": "/tmp/a",
        "HIVE_SERVER_USER": "x",
        "QUERY_ENGINE": "HIVE",
    }
)


def build_mock_options():
    k = mock.patch.dict(os.environ, FAKE_ORACLE_BQ_ENV)
    k.start()
    c = OrchestrationConfig.from_dict({"verbose": False, "execute": False})
    k.stop()
    return c
