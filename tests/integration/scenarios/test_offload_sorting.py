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

import pytest

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.operation.sort_columns import (
    SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT,
    SORT_COLUMN_NO_MODIFY_EXCEPTION_TEXT,
    UNKNOWN_SORT_COLUMN_EXCEPTION_TEXT,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import check_metadata
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import drop_backend_test_table
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


OFFLOAD_DIM = "STORY_SORT_DIM"
OFFLOAD_FACT = "STORY_SORT_FACT"


@pytest.fixture
def config():
    return cached_current_options()


@pytest.fixture
def schema():
    return cached_default_test_user()


@pytest.fixture
def data_db(schema, config):
    data_db = data_db_name(schema, config)
    data_db = convert_backend_identifier_case(config, data_db)
    return data_db


def dim_sorted_table_supported(backend_api, modify=False):
    if modify:
        return bool(
            backend_api.sorted_table_supported()
            and backend_api.sorted_table_modify_supported()
        )
    else:
        return backend_api.sorted_table_supported()


def fact_sorted_table_supported(backend_api, modify=False):
    if modify:
        return bool(
            backend_api.sorted_table_supported()
            and backend_api.sorted_table_modify_supported()
        )
    else:
        return backend_api.sorted_table_supported()


def column_supports_sorting(backend_api, data_db, table_name, column_name):
    backend_col = backend_api.get_column(data_db, table_name, column_name)
    if backend_api.is_valid_sort_data_type(backend_col.data_type):
        return column_name
    else:
        return None


def offload_sorting_fact_offload1_partition_columns(backend_api):
    if not backend_api.partition_by_column_supported():
        return None
    if backend_api.max_partition_columns() == 1:
        return "TIME_ID"
    else:
        return "TIME_ID,CHANNEL_ID"


def offload_sorting_fact_offload1_granularity(backend_api, date=True):
    if backend_api.max_partition_columns() <= 1:
        # Let default kick in for date based columns.
        return None if date else "1"
    else:
        return "%s,1" % ("M" if date else "1")


def sort_story_assertion(
    schema,
    table_name,
    backend_name,
    data_db,
    backend_api,
    messages,
    repo_client,
    offload_sort_columns="NULL",
):
    if not backend_api.sorted_table_supported():
        # Ignore the desired offload_sort_columns, it should always be blank.
        offload_sort_columns = "NULL"
    if offload_sort_columns is None:
        offload_sort_columns = "NULL"
    if not check_metadata(
        schema,
        table_name,
        messages,
        repo_client,
        offload_sort_columns=offload_sort_columns,
    ):
        return False
    # Hive doesn't store sort columns in the metastore.
    if backend_api.backend_type() != offload_constants.DBTYPE_HIVE:
        table_sort_columns = (
            backend_api.get_table_sort_columns(data_db, backend_name) or "NULL"
        ).upper()
        if table_sort_columns.upper() != offload_sort_columns.upper():
            messages.log(
                "table_sort_columns (%s) != offload_sort_columns (%s)"
                % (table_sort_columns, offload_sort_columns)
            )
            return False
    return True


def test_offload_sorting_dim(config, schema, data_db):
    id = "test_offload_sorting_dim"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )
    backend_name = convert_backend_identifier_case(config, OFFLOAD_DIM)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, OFFLOAD_DIM),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, OFFLOAD_DIM
            ),
        ],
    )

    # Default Offload Of Dimension.
    # No column defaults for a non-partitioned table.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "sort_columns_csv": offload_constants.SORT_COLUMNS_NO_CHANGE,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sort_story_assertion(
        schema,
        OFFLOAD_DIM,
        backend_name,
        data_db,
        backend_api,
        messages,
        repo_client,
        offload_sort_columns="NULL",
    )

    if not dim_sorted_table_supported(backend_api):
        # Connections are being left open, explicitly close them.
        frontend_api.close()
        return

    # Offload table with 2 sort columns.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "sort_columns_csv": "txn_day,Txn_Rate",
        "reset_backend_table": True,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sort_story_assertion(
        schema,
        OFFLOAD_DIM,
        backend_name,
        data_db,
        backend_api,
        messages,
        repo_client,
        offload_sort_columns="TXN_DAY,TXN_RATE",
    )

    if backend_api.max_sort_columns() < 5:
        # Offload table with too many sort columns.
        # BigQuery has a limit on cluster columns, this test confirms we throw an exception.
        options = {
            "owner_table": schema + "." + OFFLOAD_DIM,
            "sort_columns_csv": "txn_day,Txn_Rate,prod_id,txn_desc,TXN_CODE",
            "reset_backend_table": True,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT,
        )

    # Offload with bad sort column - expect exception.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "sort_columns_csv": "not_a_column,txn_day",
        "reset_backend_table": True,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=UNKNOWN_SORT_COLUMN_EXCEPTION_TEXT,
    )

    # Match sort columns with a wildcard.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "sort_columns_csv": "*rate",
        "reset_backend_table": True,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sort_story_assertion(
        schema,
        OFFLOAD_DIM,
        backend_name,
        data_db,
        backend_api,
        messages,
        repo_client,
        offload_sort_columns="TXN_RATE",
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_sorting_fact(config, schema, data_db):
    id = "test_offload_sorting_fact"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)

    if not fact_sorted_table_supported(backend_api):
        messages.log(f"Skipping {id} due to fact_sorted_table_supported() == False")
        return

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )
    backend_name = convert_backend_identifier_case(config, OFFLOAD_FACT)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, OFFLOAD_FACT, simple_partition_names=True
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, OFFLOAD_FACT
        ),
    )

    # Initial offload with custom sorting.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "sort_columns_csv": "channel_id,promo_id",
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "offload_partition_columns": offload_sorting_fact_offload1_partition_columns(
            backend_api
        ),
        "offload_partition_granularity": offload_sorting_fact_offload1_granularity(
            backend_api
        ),
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sort_story_assertion(
        schema,
        OFFLOAD_FACT,
        backend_name,
        data_db,
        backend_api,
        messages,
        repo_client,
        offload_sort_columns="CHANNEL_ID,PROMO_ID",
    )

    # Incremental offload table with modified sorting.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "sort_columns_csv": "channel_id,promo_id,prod_id",
        "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
        "execute": True,
    }
    if backend_api.sorted_table_modify_supported():
        # Fail to modify existing sorting.
        # This only runs if changing the sort columns is not supported and should throw an exception.
        run_offload(options, config, messages)
        assert sort_story_assertion(
            schema,
            OFFLOAD_FACT,
            backend_name,
            data_db,
            backend_api,
            messages,
            repo_client,
            offload_sort_columns="CHANNEL_ID,PROMO_ID,PROD_ID",
        )
    else:
        # Fail to modify existing sorting.
        # This only runs if changing the sort columns is not supported and should throw an exception.
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=SORT_COLUMN_NO_MODIFY_EXCEPTION_TEXT,
        )

    if not backend_api.sorted_table_modify_supported():
        # No need to run the remainer of these tests.
        frontend_api.close()
        return

    # Incremental offload with default sorting.
    # Sort columns remain the same as defined in previous offload.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_3,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sort_story_assertion(
        schema,
        OFFLOAD_FACT,
        backend_name,
        data_db,
        backend_api,
        messages,
        repo_client,
        offload_sort_columns="CHANNEL_ID,PROMO_ID,PROD_ID",
    )

    # Incremental offload with empty sorting parameter.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "sort_columns_csv": "",
        "older_than_date": test_constants.SALES_BASED_FACT_HV_4,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sort_story_assertion(
        schema,
        OFFLOAD_FACT,
        backend_name,
        data_db,
        backend_api,
        messages,
        repo_client,
        offload_sort_columns="NULL",
    )

    # Test below disabled until we tackle issue-81
    # No-op offload (same HV as previous) but still expect to change sort columns.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "sort_columns_csv": "channel_id,promo_id",
        "older_than_date": test_constants.SALES_BASED_FACT_HV_4,
        "force": True,
        "execute": True,
    }
    # run_offload(options, config, messages)
    # assert sort_story_assertion(
    #    schema,
    #    OFFLOAD_FACT,
    #    backend_name,
    #    data_db,
    #    backend_api,
    #    messages,
    #    repo_client,
    #    offload_sort_columns="CHANNEL_ID,PROMO_ID",
    # )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
