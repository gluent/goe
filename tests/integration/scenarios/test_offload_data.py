"""Integration tests for Offload with specific data scenarios."""

import pytest

from goe.config import orchestration_defaults
from goe.offload.backend_api import IMPALA_NOSHUFFLE_HINT
from goe.offload.column_metadata import (
    match_table_column,
    str_list_of_columns,
)
from goe.offload.offload_constants import (
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


NAN_TABLE = "STORY_NAN"
US_FACT = "MICRO_SEC_FACT"
NS_FACT = "NANO_SEC_FACT"


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


def gen_nan_table_ddl(frontend_api, schema, table_name):
    ddl = [
        "DROP TABLE %(schema)s.%(table)s" % {"schema": schema, "table": table_name},
        """CREATE TABLE %(schema)s.%(table)s
              (id INTEGER, bf BINARY_FLOAT, bd BINARY_DOUBLE) STORAGE (INITIAL 64K NEXT 64K)"""
        % {"schema": schema, "table": table_name},
    ]
    for id, val in enumerate(["nan", "inf", "-inf", "123.456"]):
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s (id, bf, bd) VALUES (%(id)d, '%(val)s', '%(val)s')"""
            % {"schema": schema, "table": table_name, "id": id, "val": val}
        )
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def no_nan_assertions(config, schema, data_db, frontend_api, backend_api, messages):
    # All of NaN, inf and -inf should be NULL so COUNT expected to be 3 in backend and 0 in frontend
    if frontend_api.get_table_row_count(schema, NAN_TABLE, "bf IS NULL") != 0:
        messages.log(f"frontend_api.get_table_row_count({data_db}.{NAN_TABLE}) != 0")
        return False
    be_table_name = convert_backend_identifier_case(config, NAN_TABLE)
    if backend_api.get_table_row_count(data_db, be_table_name, "bf IS NULL") != 3:
        messages.log(f"backend_api.get_table_row_count({data_db}.{be_table_name}) != 0")
        return False
    return True


def gen_fractional_second_partition_table_ddl(
    config, frontend_api, schema, table_name, scale
):
    assert scale in (6, 9)
    fractional_9s = "9".ljust(scale, "9")
    fractional_0s = "0".ljust(scale - 1, "0")
    ddls = [f"DROP TABLE {schema}.{table_name}"]
    if config.db_type == DBTYPE_ORACLE:
        ddls.extend(
            [
                """CREATE TABLE %(schema)s.%(table)s
              (id INTEGER, dt DATE, ts TIMESTAMP(%(scale)s), cat INTEGER)
              STORAGE (INITIAL 64K NEXT 64K)
              PARTITION BY RANGE (ts)
              (PARTITION %(table)s_1_998 VALUES LESS THAN (TIMESTAMP' 2030-01-01 23:59:59.%(fractional_9s)s')
              ,PARTITION %(table)s_1_999 VALUES LESS THAN (TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s0')
              ,PARTITION %(table)s_2_000 VALUES LESS THAN (TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s1')
              ,PARTITION %(table)s_2_002 VALUES LESS THAN (TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s3')
              ,PARTITION %(table)s_2_999 VALUES LESS THAN (TIMESTAMP' 2030-01-03 00:00:00.%(fractional_0s)s1'))"""
                % {
                    "schema": schema,
                    "table": table_name,
                    "scale": scale,
                    "fractional_9s": fractional_9s,
                    "fractional_0s": fractional_0s,
                },
                """INSERT INTO %(schema)s.%(table)s (id, cat, dt, ts)
              SELECT ROWNUM,1,TO_DATE('2001-01-01','YYYY-MM-DD'),TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s2'-(NUMTODSINTERVAL(ROWNUM, 'SECOND')/1e%(scale)s)
              FROM dual CONNECT BY ROWNUM <= 4"""
                % {
                    "schema": schema,
                    "table": table_name,
                    "scale": scale,
                    "fractional_0s": fractional_0s,
                },
            ]
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    ddls.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddls


def test_offload_data_nan_inf_not_supported(config, schema, data_db):
    """Tests Offload with Nan and Inf values when the backend system does not support them."""
    id = "test_offload_data_nan_inf_not_supported"
    messages = get_test_messages(config, id)

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    if not frontend_api.nan_supported():
        messages.log(
            f"Skipping {id} because NaN values are not supported for this frontend system"
        )
        return

    backend_api = get_backend_testing_api(config, messages)
    if backend_api.nan_supported():
        messages.log(
            f"Skipping {id} because NaN values are supported for this backend system"
        )
        return

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_nan_table_ddl(frontend_api, schema, NAN_TABLE),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, NAN_TABLE
            ),
        ],
    )

    # Fails to offload Nan and Inf values to a backend system that doesn't support it without allow_floating_point_conversions.
    options = {
        "owner_table": schema + "." + NAN_TABLE,
        "allow_floating_point_conversions": False,
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_status=False,
    )

    # Offload Nan and Inf values even when the backend doesn't support them.
    options = {
        "owner_table": schema + "." + NAN_TABLE,
        "allow_floating_point_conversions": True,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)

    assert no_nan_assertions(
        options, schema, data_db, frontend_api, backend_api, messages
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_data_partition_by_microsecond(config, schema, data_db):
    """Tests Offload of a microsecond partitioned fact."""
    id = "test_offload_data_partition_by_microsecond"
    messages = get_test_messages(config, id)

    if config.db_type == DBTYPE_TERADATA:
        messages.log(f"Skipping {id} on Teradata")
        return

    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )
    frontend_datetime = frontend_api.test_type_canonical_timestamp()

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_fractional_second_partition_table_ddl(
            config, frontend_api, schema, US_FACT, 6
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, US_FACT
            ),
        ],
    )

    # Offload first partition from a microsecond partitioned table.
    options = {
        "owner_table": schema + "." + US_FACT,
        "less_than_value": "2030-01-02",
        "reset_backend_table": True,
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
        US_FACT,
        "2030-01-02",
        incremental_key="TS",
        incremental_key_type=frontend_datetime,
        check_backend_rowcount=True,
    )

    # Offload second partition from a microsecond partitioned table.
    options = {
        "owner_table": schema + "." + US_FACT,
        "less_than_value": "2030-01-03",
        "verify_row_count": "aggregate"
        if backend_api.sql_microsecond_predicate_supported()
        else orchestration_defaults.verify_row_count_default(),
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
        US_FACT,
        "2030-01-02 00:00:00.000003000",
        incremental_key="TS",
        incremental_key_type=frontend_datetime,
        check_backend_rowcount=True,
    )

    # No-op offload of microsecond partitioned table.
    options = {
        "owner_table": schema + "." + US_FACT,
        "less_than_value": "2030-01-03",
    }
    run_offload(options, config, messages, expected_status=False)

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_data_partition_by_nanosecond(config, schema, data_db):
    """Tests Offload of a nanosecond partitioned table."""
    id = "test_offload_data_partition_by_nanosecond"
    messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    if not frontend_api.nanoseconds_supported():
        messages.log(f"Skipping {id} on frontend system")
        return

    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )
    frontend_datetime = frontend_api.test_type_canonical_timestamp()

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_fractional_second_partition_table_ddl(
            config, frontend_api, schema, NS_FACT, 9
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, NS_FACT
            ),
        ],
    )

    if not backend_api.nanoseconds_supported():
        # Fails to offload nanosecond partitioned table to a backend system that doesn't support it.
        options = {
            "owner_table": schema + "." + NS_FACT,
            "allow_nanosecond_timestamp_columns": False,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages, expected_status=False)

        # Successfully offload nanosecond partitioned table to a backend system that doesn't support it.
        options = {
            "owner_table": schema + "." + NS_FACT,
            "allow_nanosecond_timestamp_columns": True,
            "less_than_value": "2030-01-02",
            "verify_row_count": False,
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)
    else:
        # Offload first partition from a nanosecond partitioned table.
        options = {
            "owner_table": schema + "." + NS_FACT,
            "less_than_value": "2030-01-02",
            "reset_backend_table": True,
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
            NS_FACT,
            "2030-01-02",
            incremental_key="TS",
            incremental_key_type=frontend_datetime,
        )

        # Offload second partition from a nanosecond partitioned table.
        # Also test aggregation verification method works.
        options = {
            "owner_table": schema + "." + NS_FACT,
            "less_than_value": "2030-01-03",
            "verify_row_count": "aggregate"
            if backend_api.sql_microsecond_predicate_supported()
            else orchestration_defaults.verify_row_count_default(),
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
            NS_FACT,
            "2030-01-02",
            incremental_key="TS",
            incremental_key_type=frontend_datetime,
        )
