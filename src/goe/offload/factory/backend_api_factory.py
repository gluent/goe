#! /usr/bin/env python3

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

from contextlib import contextmanager
from goe.offload.backend_api import VALID_REMOTE_DB_TYPES
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SPARK,
    DBTYPE_SYNAPSE,
)


def backend_api_factory(
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
        elif do_not_connect:
            dry_run = True
        else:
            dry_run = False
    if backend_type == DBTYPE_HIVE:
        from goe.offload.hadoop.hive_backend_api import BackendHiveApi

        return BackendHiveApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_IMPALA:
        from goe.offload.hadoop.impala_backend_api import BackendImpalaApi

        return BackendImpalaApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_BIGQUERY:
        from goe.offload.bigquery.bigquery_backend_api import BackendBigQueryApi

        return BackendBigQueryApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_SPARK:
        from goe.offload.spark.spark_thrift_backend_api import BackendSparkThriftApi

        return BackendSparkThriftApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_SNOWFLAKE:
        from goe.offload.snowflake.snowflake_backend_api import BackendSnowflakeApi

        return BackendSnowflakeApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    elif backend_type == DBTYPE_SYNAPSE:
        from goe.offload.microsoft.synapse_backend_api import BackendSynapseApi

        return BackendSynapseApi(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )
    else:
        raise NotImplementedError("Unsupported remote system type: %s" % backend_type)


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
