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

"""Integration tests for Offload with specific data scenarios."""

import pytest

from goe.config import orchestration_defaults
from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_metadata_functions import INCREMENTAL_PREDICATE_TYPE_LIST
from goe.offload.offload_transport import OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios import scenario_constants
from tests.integration.scenarios.assertion_functions import sales_based_fact_assertion
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.test_offload_lpa import offload_lpa_fact_assertion
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
    no_query_import_transport_method,
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


NAN_TABLE = "STORY_NAN"
US_FACT = "MICRO_SEC_FACT"
NS_FACT = "NANO_SEC_FACT"
NULLS_TABLE1 = "STORY_DATA_NULLS1"
NULLS_TABLE2 = "STORY_DATA_NULLS2"
XMLTYPE_TABLE = "XMLTYPE_TABLE"
LPA_LARGE_NUMS = "LPA_LG_NUMS"
RPA_LARGE_NUMS = "RPA_LG_NUMS"
LOTS_NUMS = "12345678901234567890123456789012345678"


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


def get_max_decimal_magnitude(backend_api):
    if backend_api:
        max_decimal_magnitude = backend_api.max_decimal_integral_magnitude()
        if backend_api.backend_type() == offload_constants.DBTYPE_BIGQUERY:
            # Bigquery can only partition by an 8-byte integer so further reduce part_col
            max_decimal_magnitude = 18
    else:
        max_decimal_magnitude = 38
    return max_decimal_magnitude


def gen_large_num_list_part_literal(backend_api, all_nines=False):
    if all_nines:
        return "9" * get_max_decimal_magnitude(backend_api)
    else:
        return LOTS_NUMS[: get_max_decimal_magnitude(backend_api)]


def goe1938_vulnerable_test(options):
    """Negative partition keys and Impala are an issue: GOE-1938"""
    return bool(
        options
        and options.target
        in [offload_constants.DBTYPE_HIVE, offload_constants.DBTYPE_IMPALA]
    )


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
    ddl = [f"DROP TABLE {schema}.{table_name}"]
    if config.db_type == offload_constants.DBTYPE_ORACLE:
        ddl.extend(
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
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def gen_offload_nulls_create_ddl(
    schema, table_name, backend_api, frontend_api, config, to_allow_query_import
):
    ddl = ["DROP TABLE %(schema)s.%(table)s" % {"schema": schema, "table": table_name}]
    if config.db_type == offload_constants.DBTYPE_ORACLE:
        binary_float = (
            "\n, bfval binary_float"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        interval_ym = (
            "" if to_allow_query_import else "\n, iyval interval year(9) to month"
        )
        ddl.append(
            """CREATE TABLE %(schema)s.%(table)s
            ( id NUMBER
            , nmval NUMBER
            , vcval VARCHAR2(10)
            , chval CHAR(10)
            , dtval DATE
            , tmval TIMESTAMP%(float)s
            , bdval BINARY_DOUBLE%(interval_ym)s
            , idval INTERVAL DAY(9) TO SECOND(9))"""
            % {
                "schema": schema,
                "table": table_name,
                "float": binary_float,
                "interval_ym": interval_ym,
            }
        )
        binary_float = (
            "\n,      CAST(NULL as BINARY_FLOAT)"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        interval_ym = (
            ""
            if to_allow_query_import
            else "\n,      CAST(NULL as interval year(9) to month)"
        )

        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
            SELECT 1
            ,      CAST(NULL as NUMBER)
            ,      CAST(NULL as VARCHAR2(10))
            ,      CAST(NULL as CHAR(10))
            ,      CAST(NULL as DATE)
            ,      CAST(NULL as TIMESTAMP)%(float)s
            ,      CAST(NULL as BINARY_DOUBLE)%(interval_ym)s
            ,      CAST(NULL as interval day(9) to second(9))
            FROM   dual"""
            % {
                "schema": schema,
                "table": table_name,
                "float": binary_float,
                "interval_ym": interval_ym,
            }
        )
    elif config.db_type == offload_constants.DBTYPE_TERADATA:
        binary_float = (
            "\n, bfval float"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        # TODO add interval types
        ddl.append(
            """CREATE TABLE %(schema)s.%(table)s
            ( id NUMBER
            , nmval NUMBER
            , vcval VARCHAR(10)
            , chval CHAR(10)
            , dtval DATE
            , tmval TIMESTAMP%(float)s
            , bdval DOUBLE PRECISION)"""
            % {"schema": schema, "table": table_name, "float": binary_float}
        )
        binary_float = (
            "\n,      CAST(NULL AS FLOAT)"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
            SELECT 1
            ,      CAST(NULL as NUMBER)
            ,      CAST(NULL as VARCHAR(10))
            ,      CAST(NULL as CHAR(10))
            ,      CAST(NULL as DATE)
            ,      CAST(NULL as TIMESTAMP)%(float)s
            ,      CAST(NULL as DOUBLE PRECISION)"""
            % {"schema": schema, "table": table_name, "float": binary_float}
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def gen_large_num_create_ddl(
    schema, table_name, config, backend_api, frontend_api, part_type="LIST"
):
    assert part_type in ["LIST", "RANGE"]
    if backend_api:
        max_decimal_precision = min(backend_api.max_decimal_precision(), 38)
        max_decimal_scale = backend_api.max_decimal_scale()
    else:
        max_decimal_precision, max_decimal_scale = 38, 18
    mid_decimal_scale = min(18, max_decimal_scale)
    num_data = (
        LOTS_NUMS[: max_decimal_precision - min(max_decimal_scale, 18)]
        + "."
        + LOTS_NUMS[:mid_decimal_scale]
    )
    tiny_data = "0." + LOTS_NUMS[:max_decimal_scale]
    params = {
        "schema": schema,
        "table": table_name,
        "part_col_type": "NUMBER(%s)" % max_decimal_precision,
        "num_data_type": "NUMBER(%s,%s)" % (max_decimal_precision, mid_decimal_scale),
        "tiny_data_type": "NUMBER(%s,%s)" % (max_decimal_precision, max_decimal_scale),
        "part_lit_1": "-" + gen_large_num_list_part_literal(backend_api),
        "part_lit_2": gen_large_num_list_part_literal(backend_api),
        "part_lit_3": gen_large_num_list_part_literal(backend_api, all_nines=True),
        # For RANGE we need to INSERT smaller values
        "part_lit_mod": "- 1" if part_type == "RANGE" else "",
        "num_data_lit_1": "-" + num_data,
        "num_data_lit_2": num_data,
        "tiny_data_lit_1": "-" + tiny_data,
        "tiny_data_lit_2": tiny_data,
    }
    if goe1938_vulnerable_test(config):
        # No negative partition keys due to GOE-1938
        params.update({"part_lit_1": "1"})

    ddl = ["""DROP TABLE %(schema)s.%(table)s""" % params]
    if config.db_type == offload_constants.DBTYPE_ORACLE:
        create_sql = """CREATE TABLE %(schema)s.%(table)s
                ( id NUMBER(8)
                , str_data NVARCHAR2(30)
                , num_data %(num_data_type)s
                , tiny_data %(tiny_data_type)s
                , part_col %(part_col_type)s)
                STORAGE (INITIAL 64K NEXT 64K)"""
        if part_type == "LIST":
            create_sql += """
                PARTITION BY LIST (part_col)
                ( PARTITION P_1 VALUES (%(part_lit_1)s)
                , PARTITION P_2 VALUES (%(part_lit_2)s)
                , PARTITION P_3 VALUES (%(part_lit_3)s))"""
        else:
            create_sql += """
                PARTITION BY RANGE (part_col)
                ( PARTITION P_1 VALUES LESS THAN (%(part_lit_1)s)
                , PARTITION P_2 VALUES LESS THAN (%(part_lit_2)s)
                , PARTITION P_3 VALUES LESS THAN (%(part_lit_3)s))"""
        ddl.append(create_sql % params)
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
               (id, str_data, num_data, tiny_data, part_col)
               SELECT ROWNUM,DBMS_RANDOM.STRING('u', 15)
               ,      CASE MOD(ROWNUM,2)
                      WHEN 0 THEN %(num_data_lit_1)s
                      ELSE %(num_data_lit_2)s
                      END AS num_data
               ,      CASE MOD(ROWNUM,2)
                      WHEN 0 THEN %(tiny_data_lit_1)s
                      ELSE %(tiny_data_lit_2)s
                      END AS tiny_data
               ,      CASE MOD(ROWNUM,3)
                      WHEN 0 THEN %(part_lit_1)s
                      WHEN 1 THEN %(part_lit_2)s
                      ELSE %(part_lit_3)s
                      END %(part_lit_mod)s AS part_col
               FROM   dual
               CONNECT BY ROWNUM <= 100"""
            % params
        )
    elif config.db_type == offload_constants.DBTYPE_TERADATA:
        if part_type == "LIST":
            raise NotImplementedError(
                f"LIST (CASE_N) pending implementation: {config.db_type}"
            )
        params["step"] = "1".ljust(get_max_decimal_magnitude(backend_api), "0")
        create_sql = """CREATE TABLE %(schema)s.%(table)s
                ( id NUMBER(8)
                , str_data VARCHAR(30)
                , num_data %(num_data_type)s
                , tiny_data %(tiny_data_type)s
                , part_col %(part_col_type)s)
                PRIMARY INDEX (id)
                PARTITION BY(
                    RANGE_N(part_col BETWEEN %(part_lit_1)s AND %(part_lit_3)s EACH %(step)s)
                ) """
        ddl.append(create_sql % params)
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
               (id, str_data, num_data, tiny_data, part_col)
               SELECT id,'blah'
               ,      CASE MOD(id,2)
                      WHEN 0 THEN %(num_data_lit_1)s
                      ELSE %(num_data_lit_2)s
                      END AS num_data
               ,      CASE MOD(id,2)
                      WHEN 0 THEN %(tiny_data_lit_1)s
                      ELSE %(tiny_data_lit_2)s
                      END AS tiny_data
               ,      CASE MOD(id,3)
                      WHEN 0 THEN %(part_lit_1)s
                      WHEN 1 THEN %(part_lit_2)s
                      ELSE %(part_lit_3)s
                      END %(part_lit_mod)s AS part_col
               FROM   %(schema)s.generated_ids
               WHERE id <= 100"""
            % params
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def test_offload_data_nan_inf_not_supported(config, schema, data_db):
    """Tests Offload with Nan and Inf values when the backend system does not support them."""
    id = "test_offload_data_nan_inf_not_supported"
    messages = get_test_messages(config, id)

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    if not frontend_api.nan_supported():
        messages.log(
            f"Skipping {id} because NaN values are not supported for this frontend system"
        )
        pytest.skip(
            f"Skipping {id} because NaN values are not supported for this frontend system"
        )

    backend_api = get_backend_testing_api(config, messages)
    if backend_api.nan_supported():
        messages.log(
            f"Skipping {id} because NaN values are supported for this backend system"
        )
        pytest.skip(
            f"Skipping {id} because NaN values are supported for this backend system"
        )

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
        "execute": False,
    }
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # Offload Nan and Inf values even when the backend doesn't support them.
    options = {
        "owner_table": schema + "." + NAN_TABLE,
        "allow_floating_point_conversions": True,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
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

    if config.db_type == offload_constants.DBTYPE_TERADATA:
        messages.log(f"Skipping {id} on Teradata")
        pytest.skip(f"Skipping {id} on Teradata")

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
        "create_backend_db": True,
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
        "verify_row_count": (
            "aggregate"
            if backend_api.sql_microsecond_predicate_supported()
            else orchestration_defaults.verify_row_count_default()
        ),
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
        "execute": True,
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
        pytest.skip(f"Skipping {id} on rontend system")

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
            "create_backend_db": True,
            "execute": True,
        }
        run_offload(options, config, messages, expected_status=False)

        # Successfully offload nanosecond partitioned table to a backend system that doesn't support it.
        options = {
            "owner_table": schema + "." + NS_FACT,
            "allow_nanosecond_timestamp_columns": True,
            "less_than_value": "2030-01-02",
            "verify_row_count": False,
            "reset_backend_table": True,
            "execute": True,
        }
        run_offload(options, config, messages)
    else:
        # Offload first partition from a nanosecond partitioned table.
        options = {
            "owner_table": schema + "." + NS_FACT,
            "less_than_value": "2030-01-02",
            "reset_backend_table": True,
            "create_backend_db": True,
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
            "verify_row_count": (
                "aggregate"
                if backend_api.sql_microsecond_predicate_supported()
                else orchestration_defaults.verify_row_count_default()
            ),
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
            NS_FACT,
            "2030-01-02",
            incremental_key="TS",
            incremental_key_type=frontend_datetime,
        )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_data_oracle_xmltype(config, schema, data_db):
    """Tests Offload of Oracle XMLTYPE."""
    id = "test_offload_data_oracle_xmltype"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} on frontend system: {config.db_type}")
        pytest.skip(f"Skipping {id} on frontend system: {config.db_type}")

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    backend_api = get_backend_testing_api(config, messages)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=[
            "DROP TABLE %(schema)s.%(table)s"
            % {"schema": schema, "table": XMLTYPE_TABLE},
            """CREATE TABLE %(schema)s.%(table)s
                           (id NUMBER(8), data XMLTYPE)"""
            % {"schema": schema, "table": XMLTYPE_TABLE},
            """INSERT INTO %(schema)s.%(table)s (id, data)
                           SELECT ROWNUM, SYS_XMLGEN(table_name) FROM all_tables WHERE ROWNUM <= 10"""
            % {"schema": schema, "table": XMLTYPE_TABLE},
            frontend_api.collect_table_stats_sql_text(schema, XMLTYPE_TABLE),
        ],
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, XMLTYPE_TABLE
            ),
        ],
    )

    # Offload XMLTYPE (Query Import).
    options = {
        "owner_table": schema + "." + XMLTYPE_TABLE,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)

    if (
        no_query_import_transport_method(config)
        != OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
    ):
        # Offload XMLTYPE (no Query Import).
        options = {
            "owner_table": schema + "." + XMLTYPE_TABLE,
            "offload_transport_method": no_query_import_transport_method(
                config, no_table_centric_sqoop=True
            ),
            "reset_backend_table": True,
            "execute": True,
        }
        run_offload(options, config, messages)

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_data_nulls_qi(config, schema, data_db):
    """Tests Offload of NULLs in all data types with Query Import."""
    id = "test_offload_data_nulls_qi"

    messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    backend_api = get_backend_testing_api(config, messages)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_offload_nulls_create_ddl(
            schema,
            NULLS_TABLE1,
            backend_api,
            frontend_api,
            config,
            to_allow_query_import=True,
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, NULLS_TABLE1
            ),
        ],
    )

    # Offload with Query Import.
    options = {
        "owner_table": schema + "." + NULLS_TABLE1,
        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        "allow_floating_point_conversions": True,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_data_nulls_no_qi(config, schema, data_db):
    """Tests Offload of NULLs in all data types with Spark or Sqoop."""
    id = "test_offload_data_nulls_no_qi"
    messages = get_test_messages(config, id)

    if (
        no_query_import_transport_method(config)
        == OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
    ):
        messages.log(f"Skipping {id} because only Query Import is configured")
        pytest.skip(f"Skipping {id} because only Query Import is configured")

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    backend_api = get_backend_testing_api(config, messages)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_offload_nulls_create_ddl(
            schema,
            NULLS_TABLE2,
            backend_api,
            frontend_api,
            config,
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, NULLS_TABLE2
            ),
        ],
    )

    # Offload with Query Import.
    options = {
        "owner_table": schema + "." + NULLS_TABLE2,
        "offload_transport_method": no_query_import_transport_method(config),
        "allow_floating_point_conversions": True,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_data_large_decimals_lpa(config, schema, data_db):
    """Tests Offload list-partition-append with extreme numeric partition values."""
    id = "test_offload_data_large_decimals_lpa"

    messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    if not frontend_api.goe_lpa_supported():
        messages.log(f"Skipping {id} because frontend_api.goe_lpa_supported() == false")
        pytest.skip(f"Skipping {id} because frontend_api.goe_lpa_supported() == false")

    partition_keys_larger_than_bigint_valid = bool(
        config.db_type != offload_constants.DBTYPE_TERADATA
    )

    if not partition_keys_larger_than_bigint_valid:
        messages.log(
            f"Skipping {id} because partition_keys_larger_than_bigint_valid == false"
        )
        pytest.skip(
            f"Skipping {id} because partition_keys_larger_than_bigint_valid == false"
        )

    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_large_num_create_ddl(
            schema, LPA_LARGE_NUMS, config, backend_api, frontend_api
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_LARGE_NUMS
            ),
        ],
    )

    # Offload 1st partition.
    options = {
        "owner_table": schema + "." + LPA_LARGE_NUMS,
        "partition_names_csv": "P_1",
        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        "offload_partition_granularity": "1".ljust(
            get_max_decimal_magnitude(backend_api), "0"
        ),
        "offload_partition_lower_value": "-"
        + "9".ljust(get_max_decimal_magnitude(backend_api), "9"),
        "offload_partition_upper_value": "9".ljust(
            get_max_decimal_magnitude(backend_api), "9"
        ),
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_LARGE_NUMS,
        config,
        backend_api,
        messages,
        repo_client,
        [
            (
                "1"
                if goe1938_vulnerable_test(config)
                else "-" + gen_large_num_list_part_literal(backend_api)
            )
        ],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload 2nd partition.
    # Without Query Import if availabe.
    options = {
        "owner_table": schema + "." + LPA_LARGE_NUMS,
        "partition_names_csv": "P_2",
        "offload_transport_method": no_query_import_transport_method(config),
        "execute": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_LARGE_NUMS,
        config,
        backend_api,
        messages,
        repo_client,
        [
            (
                "1"
                if goe1938_vulnerable_test(config)
                else "-" + gen_large_num_list_part_literal(backend_api)
            ),
            gen_large_num_list_part_literal(backend_api),
        ],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload 3nd partition, partition literal all 9s.
    options = {
        "owner_table": schema + "." + LPA_LARGE_NUMS,
        "partition_names_csv": "P_3",
        "offload_transport_method": no_query_import_transport_method(config),
        "execute": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_LARGE_NUMS,
        config,
        backend_api,
        messages,
        repo_client,
        [
            (
                "1"
                if goe1938_vulnerable_test(config)
                else "-" + gen_large_num_list_part_literal(backend_api)
            ),
            gen_large_num_list_part_literal(backend_api),
            gen_large_num_list_part_literal(backend_api, all_nines=True),
        ],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )


def test_offload_data_large_decimals_rpa(config, schema, data_db):
    """Tests Offload range-partition-append with extreme numeric partition values."""
    id = "test_offload_data_large_decimals_rpa"

    messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    partition_keys_larger_than_bigint_valid = bool(
        config.db_type != offload_constants.DBTYPE_TERADATA
    )

    if not partition_keys_larger_than_bigint_valid:
        messages.log(
            f"Skipping {id} because partition_keys_larger_than_bigint_valid == false"
        )
        pytest.skip(
            f"Skipping {id} because partition_keys_larger_than_bigint_valid == false"
        )

    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_large_num_create_ddl(
            schema, RPA_LARGE_NUMS, config, backend_api, frontend_api, part_type="RANGE"
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, RPA_LARGE_NUMS
            ),
        ],
    )

    # Offload 1st partition.
    options = {
        "owner_table": schema + "." + RPA_LARGE_NUMS,
        "partition_names_csv": "P_1",
        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        "offload_partition_granularity": "1".ljust(
            get_max_decimal_magnitude(backend_api), "0"
        ),
        "offload_partition_lower_value": "-"
        + "9".ljust(get_max_decimal_magnitude(backend_api), "9"),
        "offload_partition_upper_value": "9".ljust(
            get_max_decimal_magnitude(backend_api), "9"
        ),
        "reset_backend_table": True,
        "create_backend_db": True,
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
        RPA_LARGE_NUMS,
        (
            "1"
            if goe1938_vulnerable_test(config)
            else ("-" + gen_large_num_list_part_literal(backend_api))
        ),
        offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
        incremental_key="part_col",
    )

    # Offload 2nd partition.
    # Without Query Import if availabe.
    options = {
        "owner_table": schema + "." + RPA_LARGE_NUMS,
        "partition_names_csv": "P_2",
        "offload_transport_method": no_query_import_transport_method(config),
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
        RPA_LARGE_NUMS,
        gen_large_num_list_part_literal(backend_api),
        offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
        incremental_key="part_col",
    )

    # Offload 3rd partition, HWM all 9s.
    options = {
        "owner_table": schema + "." + RPA_LARGE_NUMS,
        "partition_names_csv": "P_3",
        "offload_transport_method": no_query_import_transport_method(config),
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
        RPA_LARGE_NUMS,
        gen_large_num_list_part_literal(backend_api, all_nines=True),
        offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
        incremental_key="part_col",
    )
