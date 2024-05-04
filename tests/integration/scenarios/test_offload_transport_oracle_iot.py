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

"""Integration tests for Offload Transport specifics."""

import pytest

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_transport import (
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    is_query_import_available,
    is_spark_gcloud_batches_available,
    is_spark_gcloud_dataproc_available,
    is_spark_submit_available,
    is_sqoop_available,
)
from goe.offload.offload_transport_rdbms_api import (
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    standard_dimension_assertion,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
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


BATCHES_NUM_DIM = "OT_IOT_NUM_BATCHES_DIM"
BATCHES_STR_DIM = "OT_IOT_STR_BATCHES_DIM"
BATCHES_TS_DIM = "OT_IOT_TS_BATCHES_DIM"
DATAPROC_NUM_DIM = "OT_IOT_NUM_DATAPROC_DIM"
DATAPROC_STR_DIM = "OT_IOT_STR_DATAPROC_DIM"
DATAPROC_TS_DIM = "OT_IOT_TS_DATAPROC_DIM"
SPARK_SUBMIT_NUM_DIM = "OT_IOT_NUM_SPARKSUBMIT_DIM"
SPARK_SUBMIT_STR_DIM = "OT_IOT_STR_SPARKSUBMIT_DIM"
SPARK_SUBMIT_TS_DIM = "OT_IOT_TS_SPARKSUBMIT_DIM"
SQOOP_NUM_DIM = "OT_IOT_NUM_SQOOP_DIM"
SQOOP_STR_DIM = "OT_IOT_STR_SQOOP_DIM"
SQOOP_TS_DIM = "OT_IOT_TS_SQOOP_DIM"


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


def log_test_marker(messages, test_id):
    messages.log(test_id, detail=VVERBOSE)


def gen_num_dim_table_ddl(frontend_api, schema, table_name):
    ddl = [
        f"DROP TABLE {schema}.{table_name}",
        f"""CREATE TABLE {schema}.{table_name}
            (id INTEGER PRIMARY KEY, data VARCHAR2(30), dt DATE)
            ORGANIZATION INDEX
            STORAGE (INITIAL 64K NEXT 64K)""",
        f"""INSERT INTO {schema}.{table_name}
            SELECT ROWNUM, 'Data', SYSDATE FROM dual CONNECT BY ROWNUM <= 50""",
    ]
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def gen_str_dim_table_ddl(frontend_api, schema, table_name):
    ddl = [
        f"DROP TABLE {schema}.{table_name}",
        f"""CREATE TABLE {schema}.{table_name}
            (id VARCHAR2(10) PRIMARY KEY, data VARCHAR2(30), dt DATE)
            ORGANIZATION INDEX
            STORAGE (INITIAL 64K NEXT 64K)""",
        f"""INSERT INTO {schema}.{table_name}
            SELECT TO_CHAR(ROWNUM), 'Data', SYSDATE FROM dual CONNECT BY ROWNUM <= 50""",
    ]
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def gen_ts_dim_table_ddl(frontend_api, schema, table_name):
    ddl = [
        f"DROP TABLE {schema}.{table_name}",
        f"""CREATE TABLE {schema}.{table_name}
            (id TIMESTAMP(0) PRIMARY KEY, data VARCHAR2(30), dt DATE)
            ORGANIZATION INDEX
            STORAGE (INITIAL 64K NEXT 64K)""",
        f"""INSERT INTO {schema}.{table_name}
            SELECT SYSDATE+ROWNUM, 'Data', SYSDATE FROM dual CONNECT BY ROWNUM <= 50""",
    ]
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def iot_num_dim_tests(
    config,
    schema,
    data_db,
    table_name,
    transport_method,
    messages,
    test_id,
    expected_split_type,
):
    frontend_api = get_frontend_testing_api(config, messages, trace_action=test_id)
    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({test_id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_num_dim_table_ddl(frontend_api, schema, table_name),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
        ],
    )

    # Offload with transport_method.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    log_test_marker(messages, test_id)
    run_offload(options, config, messages)

    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        split_type=expected_split_type,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def iot_str_dim_tests(
    config,
    schema,
    data_db,
    table_name,
    transport_method,
    messages,
    test_id,
    expected_split_type,
):
    frontend_api = get_frontend_testing_api(config, messages, trace_action=test_id)
    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({test_id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_num_dim_table_ddl(frontend_api, schema, table_name),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
        ],
    )

    # Offload with transport_method.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    log_test_marker(messages, test_id)
    run_offload(options, config, messages)

    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        split_type=expected_split_type,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def iot_ts_dim_tests(
    config,
    schema,
    data_db,
    table_name,
    transport_method,
    messages,
    test_id,
    expected_split_type,
):
    frontend_api = get_frontend_testing_api(config, messages, trace_action=test_id)
    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({test_id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_ts_dim_table_ddl(frontend_api, schema, table_name),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
        ],
    )

    # Offload with transport_method.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    log_test_marker(messages, test_id)
    run_offload(options, config, messages)

    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        split_type=expected_split_type,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


#
# IOT with NUMBER PK
#
def test_offload_transport_oracle_iot_num_qi(config, schema, data_db):
    """Test IOT offload with Query Import."""
    id = "test_offload_transport_oracle_iot_num_qi"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_query_import_available(None, config, None, messages=messages):
        messages.log(f"Skipping {id} because Query Import is not configured")
        pytest.skip(f"Skipping {id} because Query Import is not configured")

    iot_num_dim_tests(
        config,
        schema,
        data_db,
        DATAPROC_NUM_DIM,
        OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        messages,
        id,
        None,
    )


def test_offload_transport_oracle_iot_num_dataproc_cluster(config, schema, data_db):
    """Test IOT offload with Dataproc."""
    id = "test_offload_transport_oracle_iot_num_dataproc_cluster"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_gcloud_dataproc_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Dataproc is not configured")
        pytest.skip(f"Skipping {id} because Dataproc is not configured")

    iot_num_dim_tests(
        config,
        schema,
        data_db,
        DATAPROC_NUM_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    )


def test_offload_transport_oracle_iot_num_dataproc_batches(config, schema, data_db):
    """Test IOT offload with Dataproc Batches."""
    id = "test_offload_transport_oracle_iot_num_dataproc_batches"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_gcloud_batches_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Dataproc Batches is not configured")
        pytest.skip(f"Skipping {id} because Dataproc Batches is not configured")

    iot_num_dim_tests(
        config,
        schema,
        data_db,
        BATCHES_NUM_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    )


def test_offload_transport_oracle_iot_num_spark_submit(config, schema, data_db):
    """Test IOT offload with Spark Submit."""
    id = "test_offload_transport_oracle_iot_num_spark_submit"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_submit_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Spark Submit is not configured")
        pytest.skip(f"Skipping {id} because Spark Submit is not configured")

    iot_num_dim_tests(
        config,
        schema,
        data_db,
        SPARK_SUBMIT_NUM_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    )


def test_offload_transport_oracle_iot_num_sqoop(config, schema, data_db):
    """Test IOT offload with Sqoop."""
    id = "test_offload_transport_oracle_iot_num_sqoop"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_sqoop_available(None, config, messages=messages):
        messages.log(f"Skipping {id} because Sqoop is not configured")
        pytest.skip(f"Skipping {id} because Sqoop is not configured")

    iot_num_dim_tests(
        config,
        schema,
        data_db,
        SQOOP_NUM_DIM,
        OFFLOAD_TRANSPORT_METHOD_SQOOP,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    )


#
# IOT with TIMESTAMP PK
#
def test_offload_transport_oracle_iot_ts_dataproc_cluster(config, schema, data_db):
    """Test IOT offload with Dataproc."""
    id = "test_offload_transport_oracle_iot_ts_dataproc_cluster"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_gcloud_dataproc_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Dataproc is not configured")
        pytest.skip(f"Skipping {id} because Dataproc is not configured")

    iot_ts_dim_tests(
        config,
        schema,
        data_db,
        DATAPROC_TS_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    )


def test_offload_transport_oracle_iot_ts_dataproc_batches(config, schema, data_db):
    """Test IOT offload with Dataproc Batches."""
    id = "test_offload_transport_oracle_iot_ts_dataproc_batches"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_gcloud_batches_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Dataproc Batches is not configured")
        pytest.skip(f"Skipping {id} because Dataproc Batches is not configured")

    iot_ts_dim_tests(
        config,
        schema,
        data_db,
        BATCHES_TS_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    )


def test_offload_transport_oracle_iot_ts_spark_submit(config, schema, data_db):
    """Test IOT offload with Spark Submit."""
    id = "test_offload_transport_oracle_iot_ts_spark_submit"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_submit_available(None, config, messages=messages):
        messages.log(f"Skipping {id} because Spark Submit is not configured")
        pytest.skip(f"Skipping {id} because Spark Submit is not configured")

    iot_ts_dim_tests(
        config,
        schema,
        data_db,
        SPARK_SUBMIT_TS_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    )


def test_offload_transport_oracle_iot_ts_sqoop(config, schema, data_db):
    """Test IOT offload with Sqoop."""
    id = "test_offload_transport_oracle_iot_ts_sqoop"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_sqoop_available(None, config, messages=messages):
        messages.log(f"Skipping {id} because Sqoop is not configured")
        pytest.skip(f"Skipping {id} because Sqoop is not configured")

    iot_ts_dim_tests(
        config,
        schema,
        data_db,
        SQOOP_TS_DIM,
        OFFLOAD_TRANSPORT_METHOD_SQOOP,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    )


#
# IOT with STRING PK
#
def test_offload_transport_oracle_iot_str_dataproc_cluster(config, schema, data_db):
    """Test IOT offload with Dataproc."""
    id = "test_offload_transport_oracle_iot_str_dataproc_cluster"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_gcloud_dataproc_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Dataproc is not configured")
        pytest.skip(f"Skipping {id} because Dataproc is not configured")

    iot_str_dim_tests(
        config,
        schema,
        data_db,
        DATAPROC_STR_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    )


def test_offload_transport_oracle_iot_str_dataproc_batches(config, schema, data_db):
    """Test IOT offload with Dataproc Batches."""
    id = "test_offload_transport_oracle_iot_str_dataproc_batches"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_gcloud_batches_available(config, None, messages=messages):
        messages.log(f"Skipping {id} because Dataproc Batches is not configured")
        pytest.skip(f"Skipping {id} because Dataproc Batches is not configured")

    iot_str_dim_tests(
        config,
        schema,
        data_db,
        BATCHES_STR_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    )


def test_offload_transport_oracle_iot_str_spark_submit(config, schema, data_db):
    """Test IOT offload with Spark Submit."""
    id = "test_offload_transport_oracle_iot_str_spark_submit"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_spark_submit_available(None, config, messages=messages):
        messages.log(f"Skipping {id} because Spark Submit is not configured")
        pytest.skip(f"Skipping {id} because Spark Submit is not configured")

    iot_str_dim_tests(
        config,
        schema,
        data_db,
        SPARK_SUBMIT_STR_DIM,
        OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    )


def test_offload_transport_oracle_iot_str_sqoop(config, schema, data_db):
    """Test IOT offload with Sqoop."""
    id = "test_offload_transport_oracle_iot_str_sqoop"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for frontend: {config.db_type}")
        pytest.skip(f"Skipping {id} for frontend: {config.db_type}")

    if not is_sqoop_available(None, config, messages=messages):
        messages.log(f"Skipping {id} because Sqoop is not configured")
        pytest.skip(f"Skipping {id} because Sqoop is not configured")

    iot_str_dim_tests(
        config,
        schema,
        data_db,
        SQOOP_STR_DIM,
        OFFLOAD_TRANSPORT_METHOD_SQOOP,
        messages,
        id,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    )
