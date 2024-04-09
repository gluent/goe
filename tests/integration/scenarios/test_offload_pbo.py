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

from textwrap import dedent

import pytest

from goe.offload.offload_constants import (
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
    CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
)
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_metadata_functions import (
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_source_data import (
    PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
    PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT,
    PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT,
)
from goe.offload.predicate_offload import GenericPredicate
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)

from tests.integration.scenarios.assertion_functions import (
    backend_table_exists,
    check_metadata,
    get_offload_row_count_from_log,
    standard_dimension_assertion,
)
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


EXC_TABLE = "STORY_PBO_EXC"
DIM_TABLE = "STORY_PBO_DIM"
DIM_TABLE_LATE = "STORY_PBO_D_LATE"
RANGE_TABLE = "STORY_PBO_RANGE"
LIST_TABLE = "STORY_PBO_LIST"

UNICODE_TABLE = "STORY_PBO_UNI"
UCODE_VALUE1 = "\u03a3"
UCODE_VALUE2 = "\u30ad"
CHAR_TABLE = "STORY_PBO_CHAR"
TS_TABLE = "STORY_PBO_TS"


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


def const_to_date_expr(config, constant):
    if config.db_type == DBTYPE_TERADATA:
        return f"DATE '{constant}'"
    else:
        return f"DATE' {constant}'"


def late_dim_filter_clause(config):
    return "time_id BETWEEN {} AND  {}".format(
        const_to_date_expr(config, test_constants.SALES_BASED_FACT_HV_3),
        const_to_date_expr(config, test_constants.SALES_BASED_FACT_HV_4),
    )


def gen_simple_unicode_dimension_ddl(
    config, frontend_api, schema, table_name, unicode_ch1, unicode_ch2
) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = (
            """SELECT 1 AS id, '%s' AS data FROM dual UNION ALL SELECT 2 AS id, '%s' AS data FROM dual"""
            % (unicode_ch1, unicode_ch2)
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = dedent(
            f"""\
        SELECT id, CAST('{unicode_ch1}' AS VARCHAR(3) CHARACTER SET UNICODE) AS data
        FROM {schema}.generated_ids WHERE id = 1
        UNION ALL
        SELECT id, CAST('{unicode_ch2}' AS VARCHAR(3) CHARACTER SET UNICODE) AS data
        FROM {schema}.generated_ids WHERE id = 2"""
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery)


def gen_char_frontend_ddl(config, frontend_api, schema, table_name) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = dedent(
            """\
        SELECT 1 AS id, CAST('a' AS CHAR(3)) AS data FROM dual
        UNION ALL SELECT 2 AS id, CAST('b' AS CHAR(3)) AS data FROM dual"""
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = f"""SELECT id, CAST('a' AS CHAR(3)) AS data FROM {schema}.generated_ids WHERE id = 1
        UNION ALL SELECT id, CAST('b' AS CHAR(3)) AS data FROM {schema}.generated_ids WHERE id = 2"""
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery)


def gen_ts_frontend_ddl(config, frontend_api, schema, table_name) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = dedent(
            """\
        SELECT 1 AS id, CAST(TIMESTAMP '2020-01-01 12:12:12.0' AS TIMESTAMP(9)) AS ts FROM dual
        UNION ALL
        SELECT 2 AS id, CAST(TIMESTAMP '2020-02-01 21:21:21.0' AS TIMESTAMP(9)) AS ts FROM dual"""
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = dedent(
            f"""\
        SELECT id, TIMESTAMP '2020-01-01 12:12:12.0' AS ts FROM {schema}.generated_ids WHERE id = 1
        UNION ALL
        SELECT id, TIMESTAMP '2020-02-01 21:21:21.0' AS ts FROM {schema}.generated_ids WHERE id = 2"""
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery)


def check_pbo_metadata(
    messages,
    repo_client,
    frontend_owner,
    frontend_name,
    number_of_predicates=None,
    expected_offload_type=None,
    expected_incremental_key=None,
    expected_incremental_range=None,
    expected_predicate_type=None,
    expected_incremental_hv=None,
    values_in_predicate_value_metadata=None,
    values_not_in_predicate_value_metadata=None,
):
    """Fetches hybrid metadata and runs checks pertinent to PBO offloads."""
    metadata = repo_client.get_offload_metadata(frontend_owner, frontend_name)

    if not check_metadata(
        frontend_owner,
        frontend_name,
        messages,
        repo_client,
        metadata_override=metadata,
        offload_type=expected_offload_type,
        incremental_key=expected_incremental_key,
        incremental_high_value=expected_incremental_hv,
        incremental_range=expected_incremental_range,
        incremental_predicate_type=expected_predicate_type,
    ):
        messages.log("Failed metadata check")
        return False
    if number_of_predicates is not None:
        if (
            len(metadata.decode_incremental_predicate_values() or [])
            != number_of_predicates
        ):
            messages.log(
                "Length of decode_incremental_predicate_values(%s) != %s"
                % (frontend_name, number_of_predicates)
            )
            return False
    for check_val in values_in_predicate_value_metadata or []:
        if not any(check_val in _ for _ in metadata.incremental_predicate_value):
            messages.log(
                "Value not found in INCREMENTAL_PREDICATE_VALUE: %s" % check_val
            )
            return False
    for check_val in values_not_in_predicate_value_metadata or []:
        if any(check_val in _ for _ in (metadata.incremental_predicate_value or [])):
            messages.log(
                "Value should NOT be in INCREMENTAL_PREDICATE_VALUE: %s" % check_val
            )
            return False
    return True


def check_predicate_count_matches_log(
    frontend_api, messages, schema, table_name, test_id, where_clause
):
    """Compare RDBMS count to logged count."""
    messages.log("check_predicate_count_matches_log(%s)" % test_id)
    app_count = frontend_api.get_table_row_count(
        schema, table_name, filter_clause=where_clause
    )
    offload_count = get_offload_row_count_from_log(messages, test_id)
    if app_count != offload_count:
        messages.log("%s != %s" % (app_count, offload_count))
        return False
    return True


def pbo_assertion(
    messages,
    repo_client,
    schema,
    table_name,
    number_of_predicates=None,
    expected_offload_type=None,
    expected_incremental_key=None,
    expected_incremental_range=None,
    expected_predicate_type=None,
    values_in_predicate_value_metadata=None,
    values_not_in_predicate_value_metadata=None,
):
    assert schema and table_name
    if values_in_predicate_value_metadata:
        assert isinstance(values_in_predicate_value_metadata, list)
    if values_not_in_predicate_value_metadata:
        assert isinstance(values_not_in_predicate_value_metadata, list)

    if not check_pbo_metadata(
        messages,
        repo_client,
        schema,
        table_name,
        number_of_predicates=number_of_predicates,
        expected_offload_type=expected_offload_type,
        expected_incremental_key=expected_incremental_key,
        expected_incremental_range=expected_incremental_range,
        expected_predicate_type=expected_predicate_type,
        values_in_predicate_value_metadata=values_in_predicate_value_metadata,
        values_not_in_predicate_value_metadata=values_not_in_predicate_value_metadata,
    ):
        messages.log("check_pbo_metadata(%s) return False" % table_name)
        return False

    return True


def test_offload_pbo_exceptions(config, schema, data_db):
    """Check exceptions are thrown in correct cases."""
    id = "test_offload_pbo_exceptions"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, EXC_TABLE),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, EXC_TABLE
            ),
        ],
    )

    # Initial offload with --no-modify-hybrid-view should fail.
    options = {
        "owner_table": schema + "." + EXC_TABLE,
        "offload_predicate": GenericPredicate(
            'column(prod_subcategory) = string("Camcorders")'
        ),
        "offload_predicate_modify_hybrid_view": False,
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT,
    )

    # Attempt to offload with partition and predicate identification.
    options = {
        "owner_table": schema + "." + EXC_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("ABC")'),
        "older_than_date": "2012-01-01",
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT,
    )

    # Attempt to offload with offload_type FULL and predicate identification.
    options = {
        "owner_table": schema + "." + EXC_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("ABC")'),
        "offload_type": OFFLOAD_TYPE_FULL,
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT,
    )

    # Badly formatted predicates should be covered by unit tests so we're testing other types of bad predicate.

    # Offload With Invalid Options: unknown column name.
    options = {
        "owner_table": schema + "." + EXC_TABLE,
        "offload_predicate": GenericPredicate('column(not_a_column) = string("NOPE")'),
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string="Unable to resolve column",
    )

    # Offload with no matching rows.
    options = {
        "owner_table": schema + "." + EXC_TABLE,
        "offload_predicate": GenericPredicate(
            'column(txn_desc) = string("No such data")'
        ),
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_status=False,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_pbo_dim(config, schema, data_db):
    """Standard PBO on a non-partitioned table."""
    id = "test_offload_pbo_dim"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, DIM_TABLE),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, DIM_TABLE
            ),
        ],
    )

    # Verification offload of dimension by predicate.
    options = {
        "owner_table": schema + "." + DIM_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("ABC")'),
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
    )
    assert not backend_table_exists(config, backend_api, messages, data_db, DIM_TABLE)

    # Offload 1st string predicate of dimension.
    options = {
        "owner_table": schema + "." + DIM_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("ABC")'),
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
    )
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, DIM_TABLE
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        DIM_TABLE,
        number_of_predicates=1,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=["ABC"],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        DIM_TABLE,
        f"{id}:1",
        "txn_desc = 'ABC'",
    )

    # Offload 2nd string predicate of dimension.
    # Also confirm data with aggregate method.
    options = {
        "owner_table": schema + "." + DIM_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("DEF")'),
        "verify_row_count": "aggregate",
    }
    run_offload(
        options,
        config,
        messages,
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        DIM_TABLE,
        number_of_predicates=2,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=["ABC", "DEF"],
    )

    # Attempt to re-offload same predicate, will return False.
    options = {
        "owner_table": schema + "." + DIM_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("DEF")'),
    }
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # Re-offload same predicate in force mode, should process successfully but move no data.
    # TODO this has been commented out until we decide what --force should do, see issue 53.
    # options = {
    #    "owner_table": schema + "." + DIM_TABLE,
    #    "offload_predicate": GenericPredicate('column(txn_desc) = string("DEF")'),
    #    "force": True,
    # }
    # run_offload(
    #    options,
    #    config,
    #    messages,
    # )
    # assert pbo_assertion(
    #    messages,
    #    repo_client,
    #    schema,
    #    DIM_TABLE,
    #    number_of_predicates=2,
    #    expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
    #    expected_incremental_key="NULL",
    #    expected_incremental_range="NULL",
    #    expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    #    values_in_predicate_value_metadata=["ABC", "DEF"],
    # )
    # assert (
    #    messages_step_executions(messages, step_title(command_steps.STEP_FINAL_LOAD))
    #    == 0
    # )

    # Offload 3rd predicate of dimension but reset hybrid view.
    options = {
        "owner_table": schema + "." + DIM_TABLE,
        "offload_predicate": GenericPredicate('column(txn_desc) = string("GHI")'),
        "reset_hybrid_view": True,
    }
    messages.log(f"{id}:2", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        DIM_TABLE,
        number_of_predicates=1,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=["GHI"],
        values_not_in_predicate_value_metadata=[
            "ABC",
            "DEF",
        ],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        DIM_TABLE,
        f"{id}:2",
        "txn_desc = 'GHI'",
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_pbo_unicode(config, schema, data_db):
    """PBO testing with unicode data."""
    id = "test_offload_pbo_unicode"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_simple_unicode_dimension_ddl(
            config, frontend_api, schema, UNICODE_TABLE, UCODE_VALUE1, UCODE_VALUE2
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, UNICODE_TABLE
            ),
        ],
    )

    # Offload 1st unicode predicate of dimension.
    options = {
        "owner_table": schema + "." + UNICODE_TABLE,
        "unicode_string_columns_csv": "data",
        "offload_predicate": GenericPredicate(
            '((column(id) = numeric(1)) and (column(data) = string("%s")))'
            % UCODE_VALUE1
        ),
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
    )
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, UNICODE_TABLE
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        UNICODE_TABLE,
        number_of_predicates=1,
        values_in_predicate_value_metadata=[UCODE_VALUE1],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        UNICODE_TABLE,
        f"{id}:1",
        "data = '%s'" % UCODE_VALUE1,
    )

    # Offload 2nd unicode predicate of dimension.
    options = {
        "owner_table": schema + "." + UNICODE_TABLE,
        "offload_predicate": GenericPredicate(
            '((column(data) = string("%s")))' % UCODE_VALUE2
        ),
    }
    messages.log(f"{id}:2", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
    )
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, UNICODE_TABLE
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        UNICODE_TABLE,
        number_of_predicates=2,
        values_in_predicate_value_metadata=[UCODE_VALUE2],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        UNICODE_TABLE,
        f"{id}:2",
        "data = '%s'" % UCODE_VALUE2,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_pbo_char_pad(config, schema, data_db):
    """PBO testing with CHAR padded column."""
    id = "test_offload_pbo_char_pad"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_char_frontend_ddl(config, frontend_api, schema, CHAR_TABLE),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, CHAR_TABLE
            ),
        ],
    )

    # Offload 1st CHAR padded predicate of dimension.
    options = {
        "owner_table": schema + "." + CHAR_TABLE,
        "offload_predicate": GenericPredicate('(column(data) = string("a  "))'),
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
    )
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, CHAR_TABLE
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        CHAR_TABLE,
        number_of_predicates=1,
        values_in_predicate_value_metadata=['("a  ")'],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        CHAR_TABLE,
        f"{id}:1",
        "data = 'a  '",
    )

    # Attempt to re-offload same CHAR padded predicate, will return False.
    options = {
        "owner_table": schema + "." + CHAR_TABLE,
        "offload_predicate": GenericPredicate('(column(data) = string("a  "))'),
    }
    run_offload(
        options,
        config,
        messages,
        expected_status=False,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_pbo_ts(config, schema, data_db):
    """PBO testing with a TIMESTAMP column."""
    id = "test_offload_pbo_ts"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_ts_frontend_ddl(config, frontend_api, schema, TS_TABLE),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, TS_TABLE
            ),
        ],
    )

    # Offload 1st TIMESTAMP predicate.
    options = {
        "owner_table": schema + "." + TS_TABLE,
        "offload_predicate": GenericPredicate("(column(ts) < datetime(2020-02-01))"),
        "allow_nanosecond_timestamp_columns": True,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
    )
    assert standard_dimension_assertion(
        config, backend_api, messages, repo_client, schema, data_db, TS_TABLE
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        TS_TABLE,
        number_of_predicates=1,
        values_in_predicate_value_metadata=["(2020-02-01)"],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        TS_TABLE,
        f"{id}:1",
        "id = 1",
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_pbo_range(config, schema, data_db):
    """PBO testing with a RANGE partitioned table."""
    id = "test_offload_pbo_range"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, RANGE_TABLE, simple_partition_names=True
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, RANGE_TABLE
            ),
        ],
    )

    # Attempt to offload for first time with predicate and "range" predicate type.
    options = {
        "owner_table": schema + "." + RANGE_TABLE,
        "offload_predicate": GenericPredicate(
            "(column(time_id) = datetime(%s))" % (test_constants.SALES_BASED_FACT_HV_1)
        ),
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_RANGE,
        "reset_backend_table": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT,
    )

    # Offload 1st datetime predicate of RANGE partitioned table.
    options = {
        "owner_table": schema + "." + RANGE_TABLE,
        "offload_predicate": GenericPredicate(
            "(column(time_id) >= datetime(%s)) and (column(time_id) < datetime(%s))"
            % (
                test_constants.SALES_BASED_FACT_HV_2,
                test_constants.SALES_BASED_FACT_HV_3,
            )
        ),
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(options, config, messages)
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        RANGE_TABLE,
        number_of_predicates=1,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=[
            test_constants.SALES_BASED_FACT_HV_2,
            test_constants.SALES_BASED_FACT_HV_3,
        ],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        RANGE_TABLE,
        f"{id}:1",
        "time_id >= %s and time_id < %s"
        % (
            const_to_date_expr(config, test_constants.SALES_BASED_FACT_HV_2),
            const_to_date_expr(config, test_constants.SALES_BASED_FACT_HV_3),
        ),
    )

    # Offload 2nd datetime predicate of RANGE partitioned table.
    options = {
        "owner_table": schema + "." + RANGE_TABLE,
        "offload_predicate": GenericPredicate(
            "column(time_id) = datetime(%s)" % test_constants.SALES_BASED_FACT_HV_1
        ),
    }
    run_offload(options, config, messages)
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        RANGE_TABLE,
        number_of_predicates=2,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=[
            test_constants.SALES_BASED_FACT_HV_2,
            test_constants.SALES_BASED_FACT_HV_3,
            test_constants.SALES_BASED_FACT_HV_1,
        ],
    )

    # Offload With Invalid Options.
    # Attempt to offload by partition while in PREDICATE mode is not valid.
    options = {
        "owner_table": schema + "." + RANGE_TABLE,
        "older_than_date": test_constants.SALES_BASED_FACT_HV_1,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
    )

    # Offload With Invalid Options.
    # Attempt to use offload type FULL while in PREDICATE mode is not valid.
    options = {
        "owner_table": schema + "." + RANGE_TABLE,
        "offload_predicate": GenericPredicate(
            "(column(time_id) = datetime(%s)) and (column(channel_id) = numeric(3))"
            % test_constants.SALES_BASED_FACT_HV_4
        ),
        "offload_type": OFFLOAD_TYPE_FULL,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_pbo_list(config, schema, data_db):
    """PBO testing with a LIST partitioned table."""
    id = "test_offload_pbo_list"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    if not frontend_api.goe_lpa_supported():
        messages.log(f"Skipping {id} for system/type: {config.db_type}/LIST")
        pytest.skip(f"Skipping {id} for system/type: {config.db_type}/LIST")

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
            schema,
            LIST_TABLE,
            part_key_type=frontend_api.test_type_canonical_date(),
            default_partition=True,
            with_drop=True,
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LIST_TABLE
            ),
        ],
    )

    # Offload 1st partition putting table in LIST mode.
    options = {
        "owner_table": "%s.%s" % (schema, LIST_TABLE),
        "equal_to_values": [test_constants.SALES_BASED_FACT_HV_1],
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)

    # Attempt to Offload 1st predicate over LIST IPA table, this is not valid.
    options = {
        "owner_table": schema + "." + LIST_TABLE,
        "offload_predicate": GenericPredicate(
            "((column(yrmon) = datetime(%s)) and (column(channel_id) = numeric(3)))"
            % (test_constants.SALES_BASED_FACT_HV_1)
        ),
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
    )

    # Offload 1st predicate over LIST table.
    options = {
        "owner_table": schema + "." + LIST_TABLE,
        "offload_predicate": GenericPredicate(
            "((column(yrmon) = datetime(%s)) and (column(channel_id) = numeric(3)))"
            % (test_constants.SALES_BASED_FACT_HV_1)
        ),
        "reset_backend_table": True,
    }
    messages.log(f"{id}:1", detail=VVERBOSE)
    run_offload(options, config, messages)
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        LIST_TABLE,
        number_of_predicates=1,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=[
            test_constants.SALES_BASED_FACT_HV_1,
            "(3)",
        ],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        LIST_TABLE,
        f"{id}:1",
        "yrmon = %s AND channel_id = 3"
        % const_to_date_expr(config, test_constants.SALES_BASED_FACT_HV_1),
    )

    # Attempt to offload partition from LIST table while already in PREDICATE mode.
    options = {
        "owner_table": "%s.%s" % (schema, LIST_TABLE),
        "equal_to_values": [test_constants.SALES_BASED_FACT_HV_1],
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
    )

    # Offload 2nd predicate over LIST table.
    options = {
        "owner_table": schema + "." + LIST_TABLE,
        "offload_predicate": GenericPredicate(
            "((column(yrmon) = datetime(%s)) and (column(channel_id) = numeric(4)))"
            % (test_constants.SALES_BASED_FACT_HV_1)
        ),
    }
    messages.log(f"{id}:2", detail=VVERBOSE)
    run_offload(options, config, messages)
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        LIST_TABLE,
        number_of_predicates=2,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_key="NULL",
        expected_incremental_range="NULL",
        expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        values_in_predicate_value_metadata=[
            test_constants.SALES_BASED_FACT_HV_1,
            "(4)",
        ],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        LIST_TABLE,
        f"{id}:2",
        "yrmon = %s AND channel_id = 4"
        % const_to_date_expr(config, test_constants.SALES_BASED_FACT_HV_1),
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
