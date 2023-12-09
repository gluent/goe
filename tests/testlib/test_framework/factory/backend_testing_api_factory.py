#! /usr/bin/env python3
""" LICENSE_TEXT
"""

from goe.offload.backend_api import VALID_REMOTE_DB_TYPES
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SPARK,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SYNAPSE,
)


def backend_testing_api_factory(
    backend_type,
    connection_options,
    messages,
    dry_run=None,
    no_caching=False,
    do_not_connect=False,
):
    assert backend_type in VALID_REMOTE_DB_TYPES, "%s not in %s" % (
        backend_type,
        VALID_REMOTE_DB_TYPES,
    )
    if dry_run is None:
        if hasattr(connection_options, "execute"):
            dry_run = bool(not connection_options.execute)
        else:
            dry_run = False
    if backend_type == DBTYPE_HIVE:
        from tests.testlib.test_framework.hadoop.hive_backend_testing_api import (
            BackendHiveTestingApi,
        )

        return BackendHiveTestingApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_IMPALA:
        from tests.testlib.test_framework.hadoop.impala_backend_testing_api import (
            BackendImpalaTestingApi,
        )

        return BackendImpalaTestingApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_BIGQUERY:
        from tests.testlib.test_framework.bigquery.bigquery_backend_testing_api import (
            BackendBigQueryTestingApi,
        )

        return BackendBigQueryTestingApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_SPARK:
        from tests.testlib.test_framework.spark.spark_thrift_backend_testing_api import (
            BackendSparkThriftTestingApi,
        )

        return BackendSparkThriftTestingApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_SNOWFLAKE:
        from tests.testlib.test_framework.snowflake.snowflake_backend_testing_api import (
            BackendSnowflakeTestingApi,
        )

        return BackendSnowflakeTestingApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_SYNAPSE:
        from tests.testlib.test_framework.microsoft.synapse_backend_testing_api import (
            BackendSynapseTestingApi,
        )

        return BackendSynapseTestingApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    else:
        raise NotImplementedError("Unsupported remote system type: %s" % backend_type)
