import pytest

from goe.offload.column_metadata import (
    match_table_column,
    str_list_of_columns,
)
from goe.offload.offload_constants import (
    ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT,
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_ORACLE,
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
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_metadata_functions import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.offload.offload_source_data import MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE
from goe.offload.offload_transport import (
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    is_query_import_available,
    is_spark_gcloud_dataproc_available,
    is_spark_submit_available,
    is_spark_thrift_available,
    is_sqoop_available,
    is_sqoop_by_query_available,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.test_sets.stories.story_setup_functions import (
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_FACT_HV_4,
    SALES_BASED_FACT_PRE_HV,
    gen_truncate_sales_based_fact_partition_ddls,
    partition_columns_if_supported,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    backend_table_count,
    backend_table_exists,
    date_gl_part_column_name,
    sales_based_fact_assertion,
    standard_dimension_assertion,
    text_in_events,
    text_in_log,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_load_table,
    drop_backend_test_table,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


KEYWORD_COL_TABLE = "KEYWORD_COLS"
BAD_CHAR_COL_TABLE = "BAD_CHAR_COLS"
CASE_DIM = "CASE_DIM"


def gen_keyword_col_table_ddl(config, frontend_api, schema, table_name) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = """SELECT TRUNC(SYSDATE) AS "DATE"
        ,      CAST('ABC' AS VARCHAR2(5))   AS "SELECT"
        FROM   dual"""
    elif config.db_type == DBTYPE_TERADATA:
        subquery = """SELECT CURRENT_DATE AS "DATE"
        ,      CAST('ABC' AS VARCHAR(5))  AS "SELECT" """
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )


@pytest.fixture
def config():
    return cached_current_options()


@pytest.fixture
def schema():
    return cached_default_test_user()


@pytest.fixture
def data_db(schema, config):
    db = data_db_name(schema, config)
    db = convert_backend_identifier_case(config, db)
    return db


def backend_case_offload_assertion(messages, search_token: str, test_id: str) -> bool:
    search_string = f"{ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT}: {search_token}"
    result = text_in_log(messages, search_string, f"{test_id}")
    if not result:
        messages.log(
            f'Search string "{search_string}" not found in log (beyond token: "{test_id}")'
        )
    return result


def log_test_marker(messages, test_id):
    messages.log(test_id, detail=VVERBOSE)


def test_identifiers_keyword_column_names(config, schema, data_db):
    id = "test_identifiers_keyword_column_names"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_keyword_col_table_ddl(
            config, frontend_api, schema, KEYWORD_COL_TABLE
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, KEYWORD_COL_TABLE
            ),
        ],
    )

    if is_query_import_available(None, config):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + KEYWORD_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_sqoop_by_query_available(config):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + KEYWORD_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_sqoop_available(None, config):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + KEYWORD_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SQOOP,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_spark_submit_available(config, None):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + KEYWORD_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_spark_thrift_available(config, None):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + KEYWORD_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_spark_gcloud_dataproc_available(config, None):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + KEYWORD_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)


def test_identifiers_bad_char_column_names(config, schema, data_db):
    id = "test_identifiers_bad_char_column_names"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    if config.target == DBTYPE_IMPALA:
        messages.log(f"Skipping {id} for Impala")
        return

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_keyword_col_table_ddl(
            config, frontend_api, schema, BAD_CHAR_COL_TABLE
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, BAD_CHAR_COL_TABLE
            ),
        ],
    )
    if is_query_import_available(None, config):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + BAD_CHAR_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_spark_submit_available(config, None):
        # Used to confirm that a table with a column name containing space or hyphen can be offloaded
        options = {
            "owner_table": schema + "." + BAD_CHAR_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)

    if is_spark_gcloud_dataproc_available(config, None):
        # Offload table with keyword column names. No assertions, just checking it runs to completion.
        options = {
            "owner_table": schema + "." + BAD_CHAR_COL_TABLE,
            "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)


def test_identifiers_table_name_case(config, schema, data_db):
    id = "test_identifiers_table_name_case"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)

    if not backend_api.case_sensitive_identifiers():
        messages.log(f"Skipping {id} because case_sensitive_identifiers() == False")
        return

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, CASE_DIM),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, CASE_DIM
            ),
        ],
    )

    # Offload the dimension checking log that we attempted to create in correct case.
    # In non-execute mode because to actually run it would apply to db name too.
    options = {
        "owner_table": schema + "." + CASE_DIM,
        "reset_backend_table": True,
    }
    log_test_marker(messages, f"{id}1")
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False, "backend_identifier_case": "LOWER"},
    )
    assert backend_case_offload_assertion(
        messages, f"{data_db}.{CASE_DIM}".lower(), f"{id}1"
    )

    options = {
        "owner_table": schema + "." + CASE_DIM.lower(),
        "reset_backend_table": True,
    }
    log_test_marker(messages, f"{id}2")
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False, "backend_identifier_case": "UPPER"},
    )
    assert backend_case_offload_assertion(
        messages, f"{data_db}.{CASE_DIM}".upper(), f"{id}2"
    )

    options = {
        "owner_table": schema.upper() + "." + CASE_DIM.capitalize(),
        "reset_backend_table": True,
    }
    log_test_marker(messages, f"{id}3")
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False, "backend_identifier_case": "NO_MODIFY"},
    )
    assert backend_case_offload_assertion(
        messages, f"{data_db.upper()}.{CASE_DIM.capitalize()}", f"{id}3"
    )
