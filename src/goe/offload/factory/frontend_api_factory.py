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
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)


def frontend_api_factory(
    frontend_type,
    connection_options,
    messages,
    conn_user_override=None,
    existing_connection=None,
    dry_run=None,
    do_not_connect=False,
    trace_action=None,
):
    assert frontend_type
    if dry_run is None:
        if hasattr(connection_options, "execute"):
            dry_run = bool(not connection_options.execute)
        else:
            dry_run = False
    if frontend_type == DBTYPE_MSSQL:
        from goe.offload.microsoft.mssql_frontend_api import MSSQLFrontendApi

        return MSSQLFrontendApi(
            connection_options,
            frontend_type,
            messages,
            conn_user_override=conn_user_override,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )
    elif frontend_type == DBTYPE_ORACLE:
        from goe.offload.oracle.oracle_frontend_api import OracleFrontendApi

        return OracleFrontendApi(
            connection_options,
            frontend_type,
            messages,
            conn_user_override=conn_user_override,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )
    elif frontend_type == DBTYPE_TERADATA:
        from goe.offload.teradata.teradata_frontend_api import TeradataFrontendApi

        return TeradataFrontendApi(
            connection_options,
            frontend_type,
            messages,
            conn_user_override=conn_user_override,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )
    else:
        raise NotImplementedError("Unsupported RDBMS: %s" % frontend_type)


@contextmanager
def frontend_api_factory_ctx(*args, **kwargs):
    """Provide FrontendApi via a context manager."""
    api = None
    try:
        api = frontend_api_factory(*args, **kwargs)
        yield api
    finally:
        if api:
            try:
                api.close()
            except:
                pass
