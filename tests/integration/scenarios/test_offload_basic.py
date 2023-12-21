import pytest

from goe.offload.backend_api import IMPALA_NOSHUFFLE_HINT
from goe.offload.column_metadata import (
    match_table_column,
    str_list_of_columns,
)
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_TERADATA,
    IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
    OFFLOAD_STATS_METHOD_COPY,
    OFFLOAD_STATS_METHOD_NATIVE,
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
)
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
    load_db_name,
)
from goe.offload.offload_metadata_functions import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.offload.offload_source_data import MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    backend_table_count,
    backend_table_exists,
    date_gl_part_column_name,
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
    gen_truncate_sales_based_fact_partition_ddls,
    partition_columns_if_supported,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework.test_constants import (
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_FACT_HV_4,
    SALES_BASED_FACT_PRE_HV,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


OFFLOAD_DIM = "STORY_DIM"
DEPENDENT_VIEW_DIM = "STORY_VIEW_DIM"
OFFLOAD_FACT = "STORY_FACT"


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
        if backend_api.backend_type() == DBTYPE_BIGQUERY:
            part_cols = backend_api.get_partition_columns(data_db, backend_name)
            if not check_column_exists("prod_id", part_cols):
                return False
        else:
            # Hadoop based
            if not backend_column_exists(
                backend_api,
                data_db,
                backend_name,
                "gl_part_000000000000001_prod_id",
            ):
                return False
            if not backend_column_exists(
                backend_api, data_db, backend_name, "gl_part_1_txn_code"
            ):
                return False

    return True


def offload_basic_fact_init_assertion(
    config, backend_api, messages, data_db, backend_name
):
    if backend_api.partition_by_column_supported():
        if backend_api.backend_type() in [DBTYPE_IMPALA, DBTYPE_HIVE]:
            if not backend_column_exists(
                config,
                backend_api,
                messages,
                data_db,
                backend_name,
                date_gl_part_column_name(backend_api, "TIME_ID"),
            ):
                return False
            if not backend_column_exists(
                config,
                backend_api,
                messages,
                data_db,
                backend_name,
                "gl_part_000000000000001_channel_id",
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
    if backend_api.backend_type() in [DBTYPE_IMPALA, DBTYPE_HIVE]:
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
        PART_COL_GRANULARITY_MONTH
        if config.target == DBTYPE_IMPALA
        else PART_COL_GRANULARITY_DAY
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
    repo_client = orchestration_repo_client_factory(config, messages)

    backend_name = convert_backend_identifier_case(config, OFFLOAD_DIM)
    copy_stats_available = backend_api.table_stats_set_supported()

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
            lambda: drop_backend_test_load_table(
                config, backend_api, messages, load_db, OFFLOAD_DIM
            ),
        ],
    )

    assert not backend_table_exists(config, backend_api, messages, data_db, OFFLOAD_DIM)
    assert not backend_table_exists(config, backend_api, messages, load_db, OFFLOAD_DIM)

    # Basic verification mode offload of a simple dimension.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages, config_overrides={"execute": False})

    assert not backend_table_exists(
        config, backend_api, messages, data_db, OFFLOAD_DIM
    ), "Backend table should NOT exist"

    # Basic offload of a simple dimension.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "offload_stats_method": OFFLOAD_STATS_METHOD_COPY
        if copy_stats_available
        else OFFLOAD_STATS_METHOD_NATIVE,
        "compute_load_table_stats": True,
        "preserve_load_table": True,
        "impala_insert_hint": IMPALA_NOSHUFFLE_HINT,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, load_db, OFFLOAD_DIM
    ), "Backend load table should exist"
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, OFFLOAD_DIM
    )

    # Attempt to re-offload, expect to fail.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
    }
    run_offload(options, config, messages, expected_status=False)

    # Reset offload the dimension adding backend partitioning (if supported).
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 1000,
        "reset_backend_table": True,
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
        config, backend_api, messages, data_db, OFFLOAD_DIM
    ), "Backend table should exist"
    assert not backend_table_exists(
        config, backend_api, messages, load_db, OFFLOAD_DIM
    ), "Backend load table should NOT exist"
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, OFFLOAD_DIM
    )
    assert offload_basic_dim_assertion(backend_api, messages, data_db, backend_name)


def test_offload_basic_fact(config, schema, data_db):
    id = "test_offload_basic_fact"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(config, messages)

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

    assert not backend_table_exists(
        config, backend_api, messages, data_db, OFFLOAD_FACT
    ), "The backend table should NOT exist"

    # Non-Execute offload of first partition with basic options.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages, config_overrides={"execute": False})

    assert not backend_table_exists(
        config, backend_api, messages, data_db, OFFLOAD_FACT
    ), "The backend table should NOT exist"

    # Offload of RANGE requesting LIST.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_LIST,
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
    )

    assert not backend_table_exists(
        config, backend_api, messages, data_db, OFFLOAD_FACT
    ), "The backend table should NOT exist"

    if config.db_type != DBTYPE_TERADATA:
        # Offloads only empty partitions. Ensure 0 rows in backend.
        options = {
            "owner_table": schema + "." + OFFLOAD_FACT,
            "older_than_date": SALES_BASED_FACT_PRE_HV,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

        assert backend_table_exists(
            config, backend_api, messages, data_db, OFFLOAD_FACT
        ), "Backend table should exist"
        assert (
            backend_table_count(config, backend_api, messages, data_db, OFFLOAD_FACT)
            == 0
        ), "Backend table should be empty"

    # Non-Execute offload of first partition with advanced options.
    offload_stats_method = (
        OFFLOAD_STATS_METHOD_COPY
        if config.target == DBTYPE_IMPALA
        else OFFLOAD_STATS_METHOD_NATIVE
    )
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_1,
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "integer_2_columns_csv": "channel_id",
        "integer_8_columns_csv": "cust_id,prod_id,promo_id",
        "decimal_columns_csv_list": ["quantity_sold", "amount_sold"],
        "decimal_columns_type_list": ["10,2", "20,2"],
        "offload_stats_method": offload_stats_method,
        "reset_backend_table": True,
    }
    if backend_api.partition_by_column_supported():
        if config.target == DBTYPE_BIGQUERY:
            options.update({"offload_partition_granularity": PART_COL_GRANULARITY_DAY})
        else:
            options.update(
                {
                    "offload_partition_columns": "time_id,channel_id",
                    "offload_partition_granularity": PART_COL_GRANULARITY_MONTH + ",1",
                }
            )
    run_offload(options, config, messages, config_overrides={"execute": False})

    # Offload some partitions from a fact table.
    # The fact is partitioned by multiple columns (if possible) with appropriate granularity.
    # We use COPY stats on this initial offload, also specify some specific data types.
    run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        OFFLOAD_FACT,
        SALES_BASED_FACT_HV_1,
        check_backend_rowcount=True,
    )
    assert offload_basic_fact_init_assertion(
        config, backend_api, messages, data_db, backend_name
    )

    # Incremental Offload of Fact - Non-Execute.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_2,
    }
    run_offload(options, config, messages, config_overrides={"execute": False})

    assert offload_basic_fact_1st_incr_assertion(
        config, backend_api, messages, data_db, backend_name
    )

    # Offloads next partition from fact table.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_2,
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
        OFFLOAD_FACT,
        SALES_BASED_FACT_HV_2,
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
        OFFLOAD_FACT,
        SALES_BASED_FACT_HV_2,
    )

    # Offloads next partition with dodgy settings, offload will override these with sensible options.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_3,
        "integer_1_columns_csv": "cust_id,channel_id,prod_id",
        "offload_partition_granularity": 100,
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 10000,
        "offload_partition_columns": partition_columns_if_supported(
            backend_api, "promo_id"
        ),
        "synthetic_partition_digits": 5,
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
        OFFLOAD_FACT,
        SALES_BASED_FACT_HV_3,
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
            schema, OFFLOAD_FACT, [SALES_BASED_FACT_HV_4], frontend_api
        ),
    )

    # Offloads next partition from fact table after all offloaded partitions have been truncated.
    options = {
        "owner_table": schema + "." + OFFLOAD_FACT,
        "older_than_date": SALES_BASED_FACT_HV_4,
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
        OFFLOAD_FACT,
        SALES_BASED_FACT_HV_4,
    )
