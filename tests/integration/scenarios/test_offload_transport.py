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

from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
    load_db_name,
)
from goe.offload.offload_transport import (
    MISSING_ROWS_IMPORTED_WARNING,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
    POLLING_VALIDATION_TEXT,
    is_spark_gcloud_batches_available,
    is_spark_gcloud_dataproc_available,
    is_livy_available,
    is_spark_submit_available,
    is_spark_thrift_available,
    is_sqoop_available,
    is_sqoop_by_query_available,
)
from goe.offload.offload_transport_rdbms_api import (
    OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_table_exists,
    load_table_is_compressed,
    standard_dimension_assertion,
    text_in_log,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_load_table,
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
    get_test_messages_ctx,
)


LOAD_TABLE_COMP_DIM1 = "STORY_OT_LOADT_COMP1"
LOAD_TABLE_COMP_DIM2 = "STORY_OT_LOADT_COMP2"
POLL_VALIDATION_SLIVY = "STORY_OTRANSP_POLLVAL_SL"
POLL_VALIDATION_SSUBMIT = "STORY_OTRANSP_POLLVAL_SS"
POLL_VALIDATION_STHRIFT = "STORY_OTRANSP_POLLVAL_ST"
QI_DIM = "STORY_OTRANSP_QI"
SPARK_BATCHES_DIM = "STORY_OTRANSP_SPARK_BATCHES"
SPARK_DATAPROC_DIM = "STORY_OTRANSP_SPARK_DATAPROC"
SPARK_SUBMIT_DIM = "STORY_OTRANSP_SPARK_SUBMIT"
SPARK_THRIFT_DIM = "STORY_OTRANSP_SPARK_THRIFT"
SPARK_LIVY_DIM = "STORY_OTRANSP_SPARK_LIVY"
SQOOP_DIM = "STORY_OTRANSP_SQOOP"
SQOOP_BY_QUERY_DIM = "STORY_OTRANSP_SQOOP_BY_QUERY"


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


def simple_offload_test(
    config,
    schema,
    data_db,
    table_name,
    transport_method,
    messages,
    test_id,
    with_snapshot=False,
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
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, table_name),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
        ],
    )

    # Offload dimension with transport_method.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }

    current_scn = None
    if with_snapshot and config.db_type == offload_constants.DBTYPE_ORACLE:
        current_scn = frontend_api.get_current_scn()
        options["offload_transport_snapshot"] = current_scn

    if transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY:
        # Setting timeout low to allow any subsequent test to reset config and not re-use session.
        offload_messages = run_offload(
            options,
            config,
            messages,
            config_overrides={"offload_transport_livy_idle_session_timeout": 4},
        )
    else:
        offload_messages = run_offload(options, config, messages)

    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        offload_messages=offload_messages,
    )
    if current_scn:
        assert text_in_log(
            offload_messages,
            f"AS OF SCN {current_scn}",
            messages,
        )
        assert text_in_log(
            offload_messages,
            f"'OFFLOAD_SNAPSHOT': {current_scn}",
            messages,
        )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def load_table_compression_tests(
    config, schema, data_db, table_name, transport_method, messages, test_id
):
    load_db = load_db_name(schema, config)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=test_id)
    backend_api = get_backend_testing_api(config, messages)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({test_id})"
    )
    dfs = get_dfs_from_options(config, messages=messages, dry_run=False)
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
        "offload_transport_method": transport_method,
        "preserve_load_table": True,
        "compress_load_table": False,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, load_db, table_name
    ), "Backend load table should exist"
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        offload_messages=offload_messages,
    )
    assert not load_table_is_compressed(data_db, backend_name, config, dfs, messages)

    # Offload dimension with Query Import WITH load table compression.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "preserve_load_table": True,
        "compress_load_table": True,
        "reset_backend_table": True,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)

    assert backend_table_exists(
        config, backend_api, messages, load_db, table_name
    ), "Backend load table should exist"
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        offload_messages=offload_messages,
    )
    assert load_table_is_compressed(data_db, backend_name, config, dfs, messages)

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def offload_transport_polling_validation_tests(
    config,
    messages,
    schema,
    data_db,
    table_name,
    transport_method,
    test_id,
    expect_missing_validation_warning=False,
):
    frontend_api = get_frontend_testing_api(config, messages, trace_action=test_id)
    backend_api = get_backend_testing_api(config, messages)

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
        ],
    )

    # Offload with polling validation for transported rows.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "offload_transport_validation_polling_interval": 1,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)
    assert text_in_log(
        offload_messages,
        POLLING_VALIDATION_TEXT % "OffloadTransportSqlStatsThread",
        messages,
    )

    # Offload with disabled SQL stats validation.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_transport_method": transport_method,
        "offload_transport_validation_polling_interval": OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
        "reset_backend_table": True,
        "execute": True,
    }
    run_offload(options, config, messages)
    assert not text_in_log(
        offload_messages,
        OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE,
        messages,
    )
    assert (
        text_in_log(
            offload_messages,
            MISSING_ROWS_IMPORTED_WARNING,
            messages,
        )
        == expect_missing_validation_warning
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_transport_query_import(config, schema, data_db):
    """Test simple offload with Query Import."""
    id = "test_offload_transport_query_import"
    with get_test_messages_ctx(config, id) as messages:
        simple_offload_test(
            config,
            schema,
            data_db,
            QI_DIM,
            OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
            messages,
            id,
            with_snapshot=True,
        )


def test_offload_transport_spark_submit(config, schema, data_db):
    """Test simple offload with spark-submit."""
    id = "test_offload_transport_spark_submit"
    with get_test_messages_ctx(config, id) as messages:
        if not is_spark_submit_available(config, None, messages=messages):
            pytest.skip(f"Skipping {id} because spark-submit is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SPARK_SUBMIT_DIM,
            OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
            messages,
            id,
        )


def test_offload_transport_dataproc_cluster(config, schema, data_db):
    """Test simple offload with Dataproc."""
    id = "test_offload_transport_dataproc_cluster"
    with get_test_messages_ctx(config, id) as messages:
        if not is_spark_gcloud_dataproc_available(config, None, messages=messages):
            pytest.skip(f"Skipping {id} because Dataproc is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SPARK_DATAPROC_DIM,
            OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
            messages,
            id,
        )


def test_offload_transport_dataproc_batches(config, schema, data_db):
    """Test simple offload with Dataproc."""
    id = "test_offload_transport_dataproc_batches"
    with get_test_messages_ctx(config, id) as messages:
        if not is_spark_gcloud_batches_available(config, None, messages=messages):
            pytest.skip(f"Skipping {id} because Dataproc Batches is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SPARK_BATCHES_DIM,
            OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
            messages,
            id,
            with_snapshot=True,
        )


def test_offload_transport_spark_thrift(config, schema, data_db):
    """Test simple offload with Spark Thriftserver."""
    id = "test_offload_transport_spark_thrift"
    with get_test_messages_ctx(config, id) as messages:
        if not is_spark_thrift_available(config, None, messages=messages):
            pytest.skip(f"Skipping {id} because Spark Thriftserver is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SPARK_THRIFT_DIM,
            OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
            messages,
            id,
        )


def test_offload_transport_spark_livy(config, schema, data_db):
    """Test simple offload with Spark Livy."""
    id = "test_offload_transport_spark_livy"
    with get_test_messages_ctx(config, id) as messages:
        if not is_livy_available(config, None, messages=messages):
            pytest.skip(f"Skipping {id} because Spark Livy is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SPARK_LIVY_DIM,
            OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
            messages,
            id,
        )


def test_offload_transport_sqoop_table(config, schema, data_db):
    """Test simple offload with table centric Sqoop."""
    id = "test_offload_transport_sqoop_table"
    with get_test_messages_ctx(config, id) as messages:
        if not is_sqoop_available(None, config, messages=messages):
            pytest.skip(f"Skipping {id} because Sqoop is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SQOOP_DIM,
            OFFLOAD_TRANSPORT_METHOD_SQOOP,
            messages,
            id,
        )


def test_offload_transport_sqoop_by_query(config, schema, data_db):
    """Test simple offload with Sqoop by query."""
    id = "test_offload_transport_sqoop_by_query"
    with get_test_messages_ctx(config, id) as messages:
        if not is_sqoop_by_query_available(config, messages=messages):
            pytest.skip(f"Skipping {id} because Sqoop is not configured")

        simple_offload_test(
            config,
            schema,
            data_db,
            SQOOP_BY_QUERY_DIM,
            OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
            messages,
            id,
        )


def test_offload_transport_load_table_qi(config, schema, data_db):
    """Test load table controls when using Query Import."""
    id = "test_offload_transport_load_table_qi"
    with get_test_messages_ctx(config, id) as messages:
        load_table_compression_tests(
            config,
            schema,
            data_db,
            LOAD_TABLE_COMP_DIM1,
            OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
            messages,
            id,
        )


def test_offload_transport_load_table_no_qi(config, schema, data_db):
    """Test load table controls when using anying other than Query Import."""
    id = "test_offload_transport_load_table_no_qi"
    with get_test_messages_ctx(config, id) as messages:
        if (
            no_query_import_transport_method(config)
            == OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
        ):
            pytest.skip(
                f"Skipping {id} because we only have Query Import at our disposal"
            )

        load_table_compression_tests(
            config,
            schema,
            data_db,
            LOAD_TABLE_COMP_DIM2,
            no_query_import_transport_method(config),
            messages,
            id,
        )


def test_offload_transport_polling_validation_spark_submit(config, schema, data_db):
    """Offload with Spark submit transport method and use polling validation for transported rows."""
    id = "test_offload_transport_polling_validation_spark_submit"
    with get_test_messages_ctx(config, id) as messages:
        if not is_spark_submit_available(config, None):
            pytest.skip(f"Skipping {id} because spark-submit is not configured")

        offload_transport_polling_validation_tests(
            config,
            messages,
            schema,
            data_db,
            POLL_VALIDATION_SSUBMIT,
            OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
            id,
        )


def test_offload_transport_polling_validation_spark_thrift(config, schema, data_db):
    """Offload with Spark Thriftserver transport method and use polling validation for transported rows."""
    id = "test_offload_transport_polling_validation_spark_thrift"
    with get_test_messages_ctx(config, id) as messages:
        if not is_spark_thrift_available(config, None):
            pytest.skip(f"Skipping {id} because Spark Thriftserver is not configured")

        offload_transport_polling_validation_tests(
            config,
            messages,
            schema,
            data_db,
            POLL_VALIDATION_STHRIFT,
            OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
            id,
            # Without SQL stats validation we cannot validate staged row count.
            expect_missing_validation_warning=True,
        )


def test_offload_transport_polling_validation_spark_livy(config, schema, data_db):
    """Offload with Spark Livy transport method and use polling validation for transported rows."""
    id = "test_offload_transport_polling_validation_spark_livy"
    with get_test_messages_ctx(config, id) as messages:
        if not is_livy_available(config, None):
            pytest.skip(f"Skipping {id} because Spark Livy is not configured")

        offload_transport_polling_validation_tests(
            config,
            messages,
            schema,
            data_db,
            POLL_VALIDATION_SLIVY,
            OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
            id,
            # Without SQL stats validation we cannot validate staged row count.
            expect_missing_validation_warning=True,
        )
