import pytest

from goe.filesystem.goe_dfs_factory import get_dfs_from_options
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
from goe.offload.offload_metadata_functions import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.offload.offload_source_data import MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE
from goe.offload.offload_transport import OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    backend_table_count,
    backend_table_exists,
    date_goe_part_column_name,
    load_table_is_compressed,
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
    no_query_import_transport_method,
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


LOAD_TABLE_COMP_DIM1 = "STORY_OT_LOADT_COMP1"
LOAD_TABLE_COMP_DIM2 = "STORY_OT_LOADT_COMP2"
ORACLE_FACT_TABLE = "STORY_OTRANSP_FACT"
OFFLOAD_SPLIT_FACT = "STORY_SPLIT_TYPE_FACT"
OFFLOAD_NULLS = "STORY_OTRANSP_NULLS"
LPA_LARGE_NUMS = "STORY_LPA_LG_NUMS"
RPA_LARGE_NUMS = "STORY_RPA_LG_NUMS"
LOTS_NUMS = "12345678901234567890123456789012345678"

OFFLOAD_TRANSPORT_YARN_QUEUE_NAME = "default"


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


def load_table_compression_tests(
    config, schema, data_db, table_name, frontend_api, messages
):
    load_db = load_db_name(schema, config)
    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )
    dfs = get_dfs_from_options(config, messages=messages)
    backend_name = convert_backend_identifier_case(config, table_name)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, table_name),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
            lambda: drop_backend_test_load_table(
                config, backend_api, messages, load_db, table_name
            ),
        ],
    )

    # Offload dimension with Query Import WITHOUT load table compression.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        "preserve_load_table": True,
        "compress_load_table": False,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, load_db, table_name
    ), "Backend load table should exist"
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, table_name
    )
    assert not load_table_is_compressed(data_db, backend_name, config, dfs, messages)

    # Offload dimension with Query Import WITH load table compression.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        "preserve_load_table": True,
        "compress_load_table": True,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, load_db, table_name
    ), "Backend load table should exist"
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, table_name
    )
    assert load_table_is_compressed(data_db, backend_name, config, dfs, messages)


def test_offload_transport_load_table_qi(config, schema, data_db):
    """Test load table controls when using Query Import."""
    id = "test_offload_transport_load_table_qi"
    messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    load_table_compression_tests(
        config, schema, data_db, LOAD_TABLE_COMP_DIM1, frontend_api, messages
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_transport_load_table_no_qi(config, schema, data_db):
    """Test load table controls when using anying other than Query Import."""
    id = "test_offload_transport_load_table_no_qi"
    messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    if (
        no_query_import_transport_method(config)
        == OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
    ):
        messages.log(
            f"Skipping {id} tests because we only have Query Import at our disposal"
        )
        return

    load_table_compression_tests(
        config, schema, data_db, LOAD_TABLE_COMP_DIM2, frontend_api, messages
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
