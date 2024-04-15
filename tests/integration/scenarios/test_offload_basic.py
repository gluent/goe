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

from goe.offload.backend_api import IMPALA_NOSHUFFLE_HINT
from goe.offload.column_metadata import (
    match_table_column,
    str_list_of_columns,
)
from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
    load_db_name,
)
from goe.offload.offload_messages import FORCED_EXCEPTION_TEXT
from goe.offload.offload_metadata_functions import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.offload.offload_source_data import MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    backend_table_count,
    backend_table_exists,
    date_goe_part_column_name,
    sales_based_fact_assertion,
    standard_dimension_assertion,
    text_in_events,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_load_table,
    drop_backend_test_table,
    drop_offload_metadata,
    gen_truncate_sales_based_fact_partition_ddls,
    partition_columns_if_supported,
)
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


OFFLOAD_DIM = "STORY_DIM"
OFFLOAD_DIM2 = "STORY_EXISTS_DIM"
OFFLOAD_FACT = "STORY_FACT"
OFFLOAD_FACT2 = "STORY_EXISTS_FACT"


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


def offload_basic_dim_assertion(backend_api, messages, data_db, backend_name):
    def check_column_exists(column_name: str, list_of_columns: list) -> bool:
        if not match_table_column(column_name, list_of_columns):
            messages.log(
                "False from: match_table_column(%s, %s)"
                % (column_name, str_list_of_columns(list_of_columns))
            )
            return False
        return True

    if backend_api.partition_by_column_supported():
        if backend_api.backend_type() == offload_constants.DBTYPE_BIGQUERY:
            part_cols = backend_api.get_partition_columns(data_db, backend_name)
            if not check_column_exists("prod_id", part_cols):
                return False
        else:
            # Hadoop based
            if not backend_column_exists(
                backend_api,
                data_db,
                backend_name,
                "goe_part_000000000000001_prod_id",
            ):
                return False
            if not backend_column_exists(
                backend_api, data_db, backend_name, "goe_part_1_txn_code"
            ):
                return False

    return True


def offload_basic_fact_init_assertion(
    config, backend_api, messages, data_db, backend_name
):
    if backend_api.partition_by_column_supported():
        if backend_api.backend_type() in [
            offload_constants.DBTYPE_IMPALA,
            offload_constants.DBTYPE_HIVE,
        ]:
            if not backend_column_exists(
                config,
                backend_api,
                messages,
                data_db,
                backend_name,
                date_goe_part_column_name(backend_api, "TIME_ID"),
            ):
                return False
            if not backend_column_exists(
                config,
                backend_api,
                messages,
                data_db,
                backend_name,
                "goe_part_000000000000001_channel_id",
            ):
                return False
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "cust_id",
        search_type=backend_api.backend_test_type_canonical_int_8(),
    ):
        return False
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "channel_id",
        search_type=backend_api.backend_test_type_canonical_int_2(),
    ):
        return False
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "prod_id",
        search_type=backend_api.backend_test_type_canonical_int_8(),
    ):
        return False
    if backend_api.backend_type() in [
        offload_constants.DBTYPE_IMPALA,
        offload_constants.DBTYPE_HIVE,
    ]:
        search_type1 = "decimal(18,4)"
        search_type2 = "decimal(38,4)"
    else:
        search_type1 = backend_api.backend_test_type_canonical_decimal()
        search_type2 = backend_api.backend_test_type_canonical_decimal()
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "quantity_sold",
        search_type=search_type1,
    ):
        return False
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "amount_sold",
        search_type=search_type2,
    ):
        return False
    return True


def offload_basic_fact_1st_incr_assertion(
    config, backend_api, messages, data_db, backend_name
):
    backend_columns = backend_api.get_partition_columns(data_db, backend_name)
    if not backend_columns:
        return True
    # Check that OffloadSourceData added an optimistic partition pruning clause when appropriate
    granularity = (
        offload_constants.PART_COL_GRANULARITY_MONTH
        if config.target == offload_constants.DBTYPE_IMPALA
        else offload_constants.PART_COL_GRANULARITY_DAY
    )
    expect_optimistic_prune_clause = (
        backend_api.partition_column_requires_synthetic_column(
            backend_columns[0], granularity
        )
    )
    if (
        text_in_events(messages, MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE)
        != expect_optimistic_prune_clause
    ):
        messages.log(
            "text_in_events(MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE) != %s"
            % expect_optimistic_prune_clause
        )
        return False
    return True


def offload_basic_fact_2nd_incr_assertion(
    config, backend_api, messages, data_db, backend_name
):
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "cust_id",
        search_type=backend_api.backend_test_type_canonical_int_8(),
    ):
        return False
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "channel_id",
        search_type=backend_api.backend_test_type_canonical_int_4(),
    ):
        return False
    return True


def test_offload_basic_dim(config, schema, data_db):
    id = "test_offload_basic_dim"
    load_db = load_db_name(schema, config)
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    test_table = OFFLOAD_DIM
    backend_name = convert_backend_identifier_case(config, test_table)
    copy_stats_available = backend_api.table_stats_set_supported()

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, test_table),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
            lambda: drop_backend_test_load_table(
                config, backend_api, messages, load_db, test_table
            ),
            lambda: drop_offload_metadata(repo_client, schema, test_table),
        ],
    )
    # Frontend API is not used for anything else so let's close it.
    frontend_api.close()

    assert not backend_table_exists(config, backend_api, messages, data_db, test_table)
    assert not backend_table_exists(config, backend_api, messages, load_db, test_table)

    # Basic verification mode offload of a simple dimension.
    options = {
        "owner_table": schema + "." + test_table,
        "reset_backend_table": True,
        "execute": False,
    }
    run_offload(options, config, messages)

    assert not backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), "Backend table should NOT exist"

    # Basic offload of a simple dimension.
    options = {
        "owner_table": schema + "." + test_table,
        "offload_stats_method": (
            offload_constants.OFFLOAD_STATS_METHOD_COPY
            if copy_stats_available
            else offload_constants.OFFLOAD_STATS_METHOD_NATIVE
        ),
        "compute_load_table_stats": True,
        "preserve_load_table": True,
        "impala_insert_hint": IMPALA_NOSHUFFLE_HINT,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, load_db, test_table
    ), "Backend load table should exist"
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, test_table
    )

    # Attempt to re-offload, expect to fail.
    options = {
        "owner_table": schema + "." + test_table,
        "execute": True,
    }
    run_offload(options, config, messages, expected_status=False)

    # Reset offload the dimension adding backend partitioning (if supported).
    options = {
        "owner_table": schema + "." + test_table,
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 1000,
        "reset_backend_table": True,
        "execute": True,
    }
    if backend_api.partition_by_column_supported():
        if backend_api.max_partition_columns() == 1:
            options.update(
                {
                    "offload_partition_columns": "prod_id",
                    "offload_partition_granularity": "1",
                }
            )
        else:
            options.update(
                {
                    "offload_partition_columns": "prod_id,txn_code",
                    "offload_partition_granularity": "1,1",
                }
            )
    run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), "Backend table should exist"
    assert not backend_table_exists(
        config, backend_api, messages, load_db, test_table
    ), "Backend load table should NOT exist"
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, test_table
    )
    assert offload_basic_dim_assertion(backend_api, messages, data_db, backend_name)

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_basic_fact(config, schema, data_db):
    id = "test_offload_basic_fact"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    test_table = OFFLOAD_FACT
    backend_name = convert_backend_identifier_case(config, test_table)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, test_table, simple_partition_names=True
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
            lambda: drop_offload_metadata(repo_client, schema, test_table),
        ],
    )

    assert not backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), "The backend table should NOT exist"

    # Non-Execute offload of first partition with basic options.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "reset_backend_table": True,
        "execute": False,
    }
    run_offload(options, config, messages)

    assert not backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), "The backend table should NOT exist"

    # Offload of RANGE requesting LIST.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_LIST,
        "reset_backend_table": True,
        "execute": False,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=offload_constants.IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
    )

    assert not backend_table_exists(
        config, backend_api, messages, data_db, test_table
    ), "The backend table should NOT exist"

    if config.db_type != offload_constants.DBTYPE_TERADATA:
        # Offloads only empty partitions. Ensure 0 rows in backend.
        options = {
            "owner_table": schema + "." + test_table,
            "older_than_date": test_constants.SALES_BASED_FACT_PRE_HV,
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        run_offload(options, config, messages)

        assert backend_table_exists(
            config, backend_api, messages, data_db, test_table
        ), "Backend table should exist"
        assert (
            backend_table_count(config, backend_api, messages, data_db, test_table) == 0
        ), "Backend table should be empty"

    # Non-Execute offload of first partition with advanced options.
    offload_stats_method = (
        offload_constants.OFFLOAD_STATS_METHOD_COPY
        if config.target == offload_constants.DBTYPE_IMPALA
        else offload_constants.OFFLOAD_STATS_METHOD_NATIVE
    )
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "integer_2_columns_csv": "channel_id",
        "integer_8_columns_csv": "cust_id,prod_id,promo_id",
        "decimal_columns_csv_list": ["quantity_sold", "amount_sold"],
        "decimal_columns_type_list": ["10,2", "20,2"],
        "offload_stats_method": offload_stats_method,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": False,
    }
    if backend_api.partition_by_column_supported():
        if config.target == offload_constants.DBTYPE_BIGQUERY:
            options.update(
                {
                    "offload_partition_granularity": offload_constants.PART_COL_GRANULARITY_DAY
                }
            )
        else:
            options.update(
                {
                    "offload_partition_columns": "time_id,channel_id",
                    "offload_partition_granularity": offload_constants.PART_COL_GRANULARITY_MONTH
                    + ",1",
                }
            )
    run_offload(options, config, messages)

    # Offload some partitions from a fact table.
    # The fact is partitioned by multiple columns (if possible) with appropriate granularity.
    # We use COPY stats on this initial offload, also specify some specific data types.
    options["execute"] = True
    run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        test_table,
        test_constants.SALES_BASED_FACT_HV_1,
        check_backend_rowcount=True,
    )
    assert offload_basic_fact_init_assertion(
        config, backend_api, messages, data_db, backend_name
    )

    # Incremental Offload of Fact - Non-Execute.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
        "execute": False,
    }
    run_offload(options, config, messages)

    assert offload_basic_fact_1st_incr_assertion(
        config, backend_api, messages, data_db, backend_name
    )

    # Offloads next partition from fact table.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        test_table,
        test_constants.SALES_BASED_FACT_HV_2,
    )

    # Try re-offload same partition which will result in no action and early abort.
    run_offload(options, config, messages, expected_status=False)
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        test_table,
        test_constants.SALES_BASED_FACT_HV_2,
    )

    # Offloads next partition with dodgy settings, offload will override these with sensible options.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_3,
        "integer_1_columns_csv": "cust_id,channel_id,prod_id",
        "offload_partition_granularity": 100,
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 10000,
        "offload_partition_columns": partition_columns_if_supported(
            backend_api, "promo_id"
        ),
        "synthetic_partition_digits": 5,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        test_table,
        test_constants.SALES_BASED_FACT_HV_3,
    )
    assert offload_basic_fact_2nd_incr_assertion(
        config, backend_api, messages, data_db, backend_name
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_truncate_sales_based_fact_partition_ddls(
            schema, test_table, [test_constants.SALES_BASED_FACT_HV_4], frontend_api
        ),
    )

    # Offloads next partition from fact table after all offloaded partitions have been truncated.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_4,
        "execute": True,
    }
    run_offload(options, config, messages)

    # TODO We need to be able to assert on whether the empty partitions were picked up or not,
    #      needs access to the offload log file...
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        test_table,
        test_constants.SALES_BASED_FACT_HV_4,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_dim_to_existing_table(config, schema, data_db):
    id = "test_offload_dim_to_existing_table"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    test_table = OFFLOAD_DIM2

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, test_table),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
            lambda: drop_offload_metadata(repo_client, schema, test_table),
        ],
    )

    # Offload the table to create the backend table but exit before doing anything else.
    options = {
        "owner_table": schema + "." + test_table,
        "error_after_step": step_title(command_steps.STEP_CREATE_TABLE),
        "reset_backend_table": True,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=FORCED_EXCEPTION_TEXT,
    )

    assert (
        backend_table_count(config, backend_api, messages, data_db, test_table) == 0
    ), "Backend table should be empty"

    # Now we can attempt to offload to a pre-created empty backend table, this should succeed.
    options = {
        "owner_table": schema + "." + test_table,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
    )

    # If we try the offload again it should fail because the table has contents.
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_fact_to_existing_table(config, schema, data_db):
    id = "test_offload_fact_to_existing_table"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    test_table = OFFLOAD_FACT2

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, test_table, simple_partition_names=True
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, test_table
            ),
            lambda: drop_offload_metadata(repo_client, schema, test_table),
        ],
    )

    # Offload the table to create the backend table but exit before doing anything else.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "error_after_step": step_title(command_steps.STEP_CREATE_TABLE),
        "reset_backend_table": True,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=FORCED_EXCEPTION_TEXT,
    )
    assert (
        backend_table_count(config, backend_api, messages, data_db, test_table) == 0
    ), "Backend table should be empty"

    # Now we can attempt to offload to a pre-created empty backend table, this should succeed.
    options = {
        "owner_table": schema + "." + test_table,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
    )

    # If we try the offload again it should fail because the table has metadata.
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
