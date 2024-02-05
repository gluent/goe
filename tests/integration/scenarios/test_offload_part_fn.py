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

""" Test functionality around requesting and validating partition functions.
    The testing is done on a simple dimension table.
    Partition functions are also tested in the assorted partition append stories:
        $ grep -l "offload_partition_functions" tests/integration/scenarios/test_*.py
"""

import pytest

from goe.offload import offload_constants
from goe.offload.backend_table import (
    PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT,
    PARTITION_FUNCTION_DOES_NOT_EXIST_EXCEPTION_TEXT,
    PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT,
)
from goe.offload.bigquery.bigquery_column import (
    BIGQUERY_TYPE_FLOAT64,
    BIGQUERY_TYPE_INT64,
)
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import VERBOSE
from goe.offload.operation.partition_controls import (
    PARTITION_FUNCTIONS_ELEMENT_EXCEPTION_TEXT,
    PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    standard_dimension_assertion,
    synthetic_part_col_name,
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
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


DIM_EXC = "PFN_DIM_EXC"
DIM_NUM = "PFN_DIM_NUM"
DIM_DEC = "PFN_DIM_DEC"
DIM_STR = "PFN_DIM_STR"

# UDF names
INT8_UDF = test_constants.PARTITION_FUNCTION_TEST_FROM_INT8
INT38_UDF = test_constants.PARTITION_FUNCTION_TEST_FROM_DEC1
DEC19_UDF = test_constants.PARTITION_FUNCTION_TEST_FROM_DEC2
STRING_UDF = test_constants.PARTITION_FUNCTION_TEST_FROM_STRING
NO_ARG_UDF = "TEST_NO_ARG_UDF"
TWO_ARG_UDF = "TEST_TWO_ARG_UDF"
DOUBLE_UDF = "TEST_INT_TO_FLOAT64"


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


def create_incompatible_test_bigquery_udf_fns(backend_api, data_db):
    """In addition to the std UDFs we want a few extra incompatble UDFs for testing exceptions."""
    udf_fns = [
        # Incompatible UDFs
        lambda: backend_api.create_udf(
            data_db,
            NO_ARG_UDF,
            BIGQUERY_TYPE_INT64,
            [],
            "CAST(12345 AS INT64)",
            or_replace=True,
        ),
        lambda: backend_api.create_udf(
            data_db,
            TWO_ARG_UDF,
            BIGQUERY_TYPE_INT64,
            [("p_arg1", BIGQUERY_TYPE_INT64), ("p_arg2", BIGQUERY_TYPE_INT64)],
            "p_arg1+p_arg2",
            or_replace=True,
        ),
        lambda: backend_api.create_udf(
            data_db,
            DOUBLE_UDF,
            BIGQUERY_TYPE_FLOAT64,
            [("p_arg", BIGQUERY_TYPE_INT64)],
            "CAST(p_arg AS FLOAT64)",
            or_replace=True,
        ),
    ]
    return udf_fns


def expected_udf_metadata(config, data_db, udf_name_option):
    if "." in udf_name_option:
        return udf_name_option
    elif config.udf_db:
        return config.udf_db + "." + udf_name_option
    elif config.target == offload_constants.DBTYPE_IMPALA:
        return "default." + udf_name_option
    else:
        return data_db + "." + udf_name_option


def offload_part_fn_assertion(
    config,
    backend_api,
    messages,
    data_db,
    table_name,
    source_column="prod_id",
    synth_position=0,
    udf=None,
):
    synth_col = synthetic_part_col_name(f"U{synth_position}", source_column)
    synth_col = convert_backend_identifier_case(config, synth_col)
    if not backend_column_exists(
        config, backend_api, messages, data_db, table_name, synth_col
    ):
        return False
    if udf:
        backend_name = convert_backend_identifier_case(config, table_name)
        check_sql = "SELECT COUNT(*) FROM {} WHERE {}({}) != {}".format(
            backend_api.enclose_object_reference(data_db, backend_name),
            backend_api.enclose_object_reference(data_db, udf),
            source_column,
            synth_col,
        )
        mismatch_row = backend_api.execute_query_fetch_one(check_sql, log_level=VERBOSE)
        if not mismatch_row:
            messages.log("No data in backend_table")
            return False
        if mismatch_row[0] != 0:
            messages.log(
                "Unexpected values in backend_table synthetic column for %s rows"
                % mismatch_row[0]
            )
            return False
    return True


def test_offload_part_fn_exceptions(config, schema, data_db):
    id = "test_offload_part_fn_exceptions"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
            schema, DIM_EXC, extra_col_tuples=[("'-123'", "STR_PROD_ID")]
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, DIM_EXC
            ),
        ],
    )

    if not backend_api.goe_partition_functions_supported():
        # Offload with partition functions when not supported.
        options = {
            "owner_table": schema + "." + DIM_NUM,
            "offload_partition_functions": "anything",
            "reset_backend_table": True,
        }
        run_offload(
            options,
            config,
            messages,
            config_overrides={"execute": False},
            expected_exception_string=PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT,
        )

        messages.log(
            f"Skipping most of {id} due to goe_partition_functions_supported() == False"
        )
        return

    # Create a series of UDFs, some incompatible with GOE, for use throughout these tests.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        python_fns=create_incompatible_test_bigquery_udf_fns(backend_api, data_db),
    )

    # Offload with unsupported partition function UDF.
    options = {
        "owner_table": schema + "." + DIM_EXC,
        "offload_partition_functions": NO_ARG_UDF,
        "integer_8_columns_csv": "prod_id",
        "offload_partition_columns": "prod_id",
        "offload_partition_granularity": "10",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 5000,
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT,
    )

    # Offload with unsupported partition function UDF.
    options["offload_partition_functions"] = TWO_ARG_UDF
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT,
    )

    # Offload with non-existent partition function UDF.
    options["offload_partition_functions"] = INT8_UDF + "-not-real"
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PARTITION_FUNCTION_DOES_NOT_EXIST_EXCEPTION_TEXT,
    )

    # Ensure UDF exists before attempting test.
    backend_api.create_test_partition_functions(data_db, udf=INT8_UDF)

    # Offload with STRING input to INT8 partition function.
    options["offload_partition_columns"] = "TXN_DESC"
    options["offload_partition_functions"] = INT8_UDF
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT,
    )

    # Offload with DATE input to partition function.
    options = {
        "owner_table": schema + "." + DIM_EXC,
        "offload_partition_functions": INT8_UDF,
        "offload_partition_columns": "TXN_DATE",
        "offload_partition_granularity": "M",
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT,
    )

    # Offload with too many partition functions.
    options = {
        "owner_table": schema + "." + DIM_EXC,
        "offload_partition_functions": INT8_UDF + "," + INT38_UDF,
        "offload_partition_columns": "TXN_DATE",
        "offload_partition_granularity": "M",
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        config_overrides={"execute": False},
        expected_exception_string=PARTITION_FUNCTIONS_ELEMENT_EXCEPTION_TEXT,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_part_fn_num(config, schema, data_db):
    id = "test_offload_part_fn_num"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)

    if not backend_api.goe_partition_functions_supported():
        messages.log(
            f"Skipping most of {id} due to goe_partition_functions_supported() == False"
        )
        return

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
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, DIM_NUM),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, DIM_NUM
            ),
        ],
    )

    backend_api.create_test_partition_functions(data_db, udf=INT8_UDF)

    # Offload with db prefixed partition function UDF.
    options = {
        "owner_table": schema + "." + DIM_NUM,
        "integer_8_columns_csv": "prod_id",
        "offload_partition_columns": "prod_id",
        "offload_partition_functions": data_db + "." + INT8_UDF,
        "offload_partition_granularity": "10",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 5000,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        DIM_NUM,
        partition_functions=expected_udf_metadata(
            config, data_db, data_db + "." + INT8_UDF
        ),
    )
    assert offload_part_fn_assertion(
        config, backend_api, messages, data_db, DIM_NUM, udf=INT8_UDF
    )

    # Offload with non-prefixed INT64 partition function UDF.
    options = {
        "owner_table": schema + "." + DIM_NUM,
        "integer_8_columns_csv": "prod_id",
        "offload_partition_columns": "prod_id",
        "offload_partition_functions": INT8_UDF,
        "offload_partition_granularity": "10",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 5000,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        DIM_NUM,
        partition_functions=expected_udf_metadata(config, data_db, INT8_UDF),
    )
    assert offload_part_fn_assertion(
        config, backend_api, messages, data_db, DIM_NUM, udf=INT8_UDF
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_part_fn_dec(config, schema, data_db):
    id = "test_offload_part_fn_dec"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)

    if not backend_api.goe_partition_functions_supported():
        messages.log(
            f"Skipping most of {id} due to goe_partition_functions_supported() == False"
        )
        return

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
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, DIM_DEC),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, DIM_DEC
            ),
        ],
    )

    backend_api.create_test_partition_functions(data_db, udf=[INT38_UDF, DEC19_UDF])

    # Offload with a BIGNUMERIC partition function UDF.
    options = {
        "owner_table": schema + "." + DIM_DEC,
        "integer_38_columns_csv": "prod_id",
        "offload_partition_columns": "prod_id",
        "offload_partition_functions": INT38_UDF,
        "offload_partition_granularity": "10",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 5000,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        DIM_DEC,
        partition_functions=expected_udf_metadata(config, data_db, INT38_UDF),
    )
    assert offload_part_fn_assertion(
        config, backend_api, messages, data_db, DIM_DEC, udf=INT38_UDF
    )

    # Offload with a BIGNUMERIC partition function UDF.
    options = {
        "owner_table": schema + "." + DIM_DEC,
        "decimal_columns_csv_list": ["prod_id"],
        "decimal_columns_type_list": ["19,0"],
        "offload_partition_columns": "prod_id",
        "offload_partition_functions": DEC19_UDF,
        "offload_partition_granularity": "10",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 5000,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        DIM_DEC,
        partition_functions=expected_udf_metadata(config, data_db, DEC19_UDF),
    )
    assert offload_part_fn_assertion(
        config, backend_api, messages, data_db, DIM_DEC, udf=DEC19_UDF
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_part_fn_str(config, schema, data_db):
    id = "test_offload_part_fn_str"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)

    if not backend_api.goe_partition_functions_supported():
        messages.log(
            f"Skipping most of {id} due to goe_partition_functions_supported() == False"
        )
        return

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
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
            schema, DIM_STR, extra_col_tuples=[("'-123'", "STR_PROD_ID")]
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, DIM_STR
            ),
        ],
    )

    backend_api.create_test_partition_functions(
        data_db, udf=test_constants.PARTITION_FUNCTION_TEST_FROM_STRING
    )

    # Offload with a STRING partition function UDF.
    options = {
        "owner_table": schema + "." + DIM_STR,
        "offload_partition_columns": "str_prod_id",
        "offload_partition_functions": STRING_UDF,
        "offload_partition_granularity": "10",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 5000,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert standard_dimension_assertion(
        config,
        backend_api,
        messages,
        repo_client,
        schema,
        data_db,
        DIM_STR,
        partition_functions=expected_udf_metadata(config, data_db, STRING_UDF),
    )
    assert offload_part_fn_assertion(
        config,
        backend_api,
        messages,
        data_db,
        DIM_STR,
        source_column="str_prod_id",
        udf=STRING_UDF,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
