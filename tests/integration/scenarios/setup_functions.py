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

from typing import TYPE_CHECKING

from goe.offload import offload_transport
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_messages import VERBOSE

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from tests.testlib.test_framework.backend_testing_api import (
        BackendTestingApiInterface,
    )
    from tests.testlib.test_framework.frontend_testing_api import (
        FrontendTestingApiInterface,
    )
    from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages


def drop_backend_test_table(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    test_messages: "OffloadTestMessages",
    db: str,
    table_name: str,
    drop_any=False,
    view=False,
):
    """Convert the db and table name to the correct case before issuing the drop."""
    db, table_name = convert_backend_identifier_case(config, db, table_name)
    if not backend_api.database_exists(db):
        test_messages.log(
            "drop_backend_test_table(%s, %s) DB does not exist" % (db, table_name),
            detail=VERBOSE,
        )
        return
    if drop_any:
        test_messages.log(
            "drop_backend_test_table(%s, %s, drop_any=True)" % (db, table_name),
            detail=VERBOSE,
        )
        backend_api.drop(db, table_name, sync=True)
    elif view:
        test_messages.log(
            "drop_backend_test_table(%s, %s, view=True)" % (db, table_name),
            detail=VERBOSE,
        )
        backend_api.drop_view(db, table_name, sync=True)
    else:
        test_messages.log(
            "drop_backend_test_table(%s, %s, table=True)" % (db, table_name),
            detail=VERBOSE,
        )
        backend_api.drop_table(db, table_name, sync=True)


def drop_backend_test_load_table(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    test_messages: "OffloadTestMessages",
    db: str,
    table_name: str,
):
    if backend_api and not backend_api.load_db_transport_supported():
        return
    drop_backend_test_table(config, backend_api, test_messages, db, table_name)


def drop_offload_metadata(
    repo_client: "OrchestrationRepoClientInterface", schema: str, table_name: str
):
    """Simple wrapper over drop_offload_metadata() in case we need to catch exceptions in the future."""
    repo_client.drop_offload_metadata(schema, table_name)


def gen_drop_sales_based_fact_partition_ddls(
    schema: str,
    table_name: str,
    hv_string_list,
    frontend_api: "FrontendTestingApiInterface",
    truncate_instead_of_drop=False,
    dropping_oldest=None,
) -> list:
    """hv_string_list in format YYYY-MM-DD
    dropping_oldest=True gives frontends with no DROP PARTITION command an opportunity to re-partition for
    the same effect.
    """
    if truncate_instead_of_drop:
        return frontend_api.sales_based_fact_truncate_partition_ddl(
            schema, table_name, hv_string_list=hv_string_list
        )
    else:
        return frontend_api.sales_based_fact_drop_partition_ddl(
            schema,
            table_name,
            hv_string_list=hv_string_list,
            dropping_oldest=dropping_oldest,
        )


def gen_truncate_sales_based_fact_partition_ddls(
    schema: str,
    table_name: str,
    hv_string_list,
    frontend_api: "FrontendTestingApiInterface",
):
    """hv_string_list in format YYYY-MM-DD"""
    return gen_drop_sales_based_fact_partition_ddls(
        schema, table_name, hv_string_list, frontend_api, truncate_instead_of_drop=True
    )


def get_sales_based_fact_partition_list(
    schema: str,
    table_name: str,
    hv_string_list,
    frontend_api: "FrontendTestingApiInterface",
) -> list:
    """Return a list of partitions matching a date high value string, used for SALES based tests
    hv_string_list in format YYYY-MM-DD
    """
    if not frontend_api:
        return []
    if isinstance(hv_string_list, str):
        hv_string_list = [hv_string_list]

    partitions = frontend_api.frontend_table_partition_list(
        schema, table_name, hv_string_list=hv_string_list
    )
    return partitions


def no_query_import_transport_method(
    config: "OrchestrationConfig", no_table_centric_sqoop=False
):
    if not config:
        return offload_transport.OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
    if offload_transport.is_spark_thrift_available(config, None):
        return offload_transport.OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT
    elif offload_transport.is_spark_submit_available(config, None):
        return offload_transport.OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT
    elif offload_transport.is_sqoop_available(None, config):
        if no_table_centric_sqoop:
            return offload_transport.OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY
        else:
            return offload_transport.OFFLOAD_TRANSPORT_METHOD_SQOOP
    else:
        return offload_transport.OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT


def sales_based_fact_partition_exists(schema, table_name, hv_string_list, frontend_api):
    """hv_string_list in format YYYY-MM-DD"""
    return bool(
        get_sales_based_fact_partition_list(
            schema, table_name, hv_string_list, frontend_api
        )
    )


def partition_columns_if_supported(backend_api, offload_partition_columns):
    if backend_api and backend_api.partition_by_column_supported():
        return offload_partition_columns
    else:
        return None
