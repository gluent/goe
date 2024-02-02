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

from goe.offload.offload_constants import DBTYPE_MSSQL, DBTYPE_ORACLE, DBTYPE_TERADATA


def frontend_testing_api_factory(
    frontend_type,
    connection_options,
    messages,
    existing_connection=None,
    dry_run=None,
    do_not_connect=False,
    trace_action=None,
):
    if dry_run is None:
        if hasattr(connection_options, "execute"):
            dry_run = bool(not connection_options.execute)
        else:
            dry_run = False
    if frontend_type == DBTYPE_ORACLE:
        from tests.testlib.test_framework.oracle.oracle_frontend_testing_api import (
            OracleFrontendTestingApi,
        )

        return OracleFrontendTestingApi(
            frontend_type,
            connection_options,
            messages,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )
    elif frontend_type == DBTYPE_TERADATA:
        from tests.testlib.test_framework.teradata.teradata_frontend_testing_api import (
            TeradataFrontendTestingApi,
        )

        return TeradataFrontendTestingApi(
            frontend_type,
            connection_options,
            messages,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )
    elif frontend_type == DBTYPE_MSSQL:
        from tests.testlib.test_framework.microsoft.mssql_frontend_testing_api import (
            MSSQLFrontendTestingApi,
        )

        return MSSQLFrontendTestingApi(
            frontend_type,
            connection_options,
            messages,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )
    else:
        raise NotImplementedError("Unsupported frontend system: %s" % frontend_type)


@contextmanager
def frontend_testing_api_factory_ctx(*args, **kwargs):
    """Provide FrontendTestingApi via a context manager."""
    api = None
    try:
        api = frontend_testing_api_factory(*args, **kwargs)
        yield api
    finally:
        pass
