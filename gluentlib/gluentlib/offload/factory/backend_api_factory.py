#! /usr/bin/env python3
""" LICENSE_TEXT
"""

from contextlib import contextmanager
from gluentlib.offload.backend_api import VALID_REMOTE_DB_TYPES
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_HIVE, DBTYPE_IMPALA,\
    DBTYPE_SNOWFLAKE, DBTYPE_SPARK, DBTYPE_SYNAPSE


def backend_api_factory(backend_type, connection_options, messages, dry_run=None, no_caching=False,
                        do_not_connect=False):
    assert backend_type in VALID_REMOTE_DB_TYPES, '%s not in %s' % (backend_type, VALID_REMOTE_DB_TYPES)
    if dry_run is None:
        if hasattr(connection_options, 'execute'):
            dry_run = bool(not connection_options.execute)
        else:
            dry_run = False
    if backend_type == DBTYPE_HIVE:
        from gluentlib.offload.hadoop.hive_backend_api import BackendHiveApi
        return BackendHiveApi(connection_options, backend_type, messages, dry_run=dry_run, no_caching=no_caching,
                              do_not_connect=do_not_connect)
    elif backend_type == DBTYPE_IMPALA:
        from gluentlib.offload.hadoop.impala_backend_api import BackendImpalaApi
        return BackendImpalaApi(connection_options, backend_type, messages, dry_run=dry_run, no_caching=no_caching,
                                do_not_connect=do_not_connect)
    elif backend_type == DBTYPE_BIGQUERY:
        from gluentlib.offload.bigquery.bigquery_backend_api import BackendBigQueryApi
        return BackendBigQueryApi(connection_options, backend_type, messages, dry_run=dry_run, no_caching=no_caching,
                                  do_not_connect=do_not_connect)
    elif backend_type == DBTYPE_SPARK:
        from gluentlib.offload.spark.spark_thrift_backend_api import BackendSparkThriftApi
        return BackendSparkThriftApi(connection_options, backend_type, messages, dry_run=dry_run, no_caching=no_caching,
                                     do_not_connect=do_not_connect)
    elif backend_type == DBTYPE_SNOWFLAKE:
        from gluentlib.offload.snowflake.snowflake_backend_api import BackendSnowflakeApi
        return BackendSnowflakeApi(connection_options, backend_type, messages, dry_run=dry_run, no_caching=no_caching,
                                   do_not_connect=do_not_connect)
    elif backend_type == DBTYPE_SYNAPSE:
        from gluentlib.offload.microsoft.synapse_backend_api import BackendSynapseApi
        return BackendSynapseApi(connection_options, backend_type, messages, dry_run=dry_run, no_caching=no_caching,
                                 do_not_connect=do_not_connect)
    else:
        raise NotImplementedError('Unsupported remote system type: %s' % backend_type)


@contextmanager
def backend_api_factory_ctx(*args, **kwargs):
    """Provide BackendApi via a context manager."""
    api = None
    try:
        api = backend_api_factory(*args, **kwargs)
        yield api
    finally:
        if api:
            try:
                api.close()
            except:
                pass
