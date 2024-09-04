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

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_source_data import (
    TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT,
)
from goe.offload.oracle.oracle_column import ORACLE_TYPE_NVARCHAR2
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    sales_based_fact_assertion,
    synthetic_part_col_name,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
    gen_drop_sales_based_fact_partition_ddls,
    sales_based_fact_partition_exists,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api_ctx,
    get_test_messages_ctx,
)


RPA_FACT_TABLE_NUM = "RPA_NUM"
RPA_FACT_TABLE_NUM_UDF = "RPA_UNUM"
RPA_FACT_TABLE_DATE = "RPA_DATE"
RPA_FACT_TABLE_TS = "RPA_TS"
RPA_FACT_TABLE_STR = "RPA_STR"
RPA_FACT_TABLE_STR_UDF = "RPA_USTR"
RPA_FACT_TABLE_NSTR = "RPA_NSTR"
RPA_ALPHA_FACT_TABLE = "RPA_ALPHA"
NOSEG_FACT = "RPA_NOSEG"
FACT_MCOL_MAXVAL = "RPA_MCOL_MAXVAL"


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


def offload_range_ipa_standard_tests(
    schema,
    data_db,
    config,
    backend_api,
    frontend_api,
    messages,
    repo_client,
    part_key_type,
    partition_function=None,
):
    canonical_int_8 = frontend_api.test_type_canonical_int_8()
    canonical_string = frontend_api.test_type_canonical_string()
    hv_0 = test_constants.SALES_BASED_FACT_PRE_HV_NUM
    hv_1 = test_constants.SALES_BASED_FACT_HV_1_NUM
    hv_2 = test_constants.SALES_BASED_FACT_HV_2_NUM
    hv_3 = test_constants.SALES_BASED_FACT_HV_3_NUM
    hv_4 = test_constants.SALES_BASED_FACT_HV_4_NUM
    less_than_option = "less_than_value"
    lower_value = None
    upper_value = None
    udf = different_udf = None
    synthetic_partition_column_expected = False
    if config.target in [
        offload_constants.DBTYPE_IMPALA,
        offload_constants.DBTYPE_HIVE,
    ]:
        synthetic_partition_column_expected = True
    synthetic_partition_digits = different_partition_digits = None

    if part_key_type == canonical_int_8:
        if partition_function:
            table_name = RPA_FACT_TABLE_NUM_UDF
            udf = data_db + "." + partition_function
            different_udf = udf + "_unknown"
            synthetic_partition_column_expected = True
        else:
            table_name = RPA_FACT_TABLE_NUM
        granularity = "100"
        different_granularity = "1000"
        lower_value = test_constants.SALES_BASED_FACT_PRE_HV_NUM
        upper_value = test_constants.SALES_BASED_FACT_HV_7_NUM
        if config.target in [
            offload_constants.DBTYPE_IMPALA,
            offload_constants.DBTYPE_HIVE,
        ]:
            synthetic_partition_digits = 15
            different_partition_digits = 16
    elif part_key_type == canonical_string:
        if partition_function:
            table_name = RPA_FACT_TABLE_STR_UDF
            udf = data_db + "." + partition_function
            different_udf = udf + "_unknown"
            synthetic_partition_column_expected = True
            granularity = "100"
            different_granularity = "1000"
            lower_value = test_constants.SALES_BASED_FACT_PRE_HV_NUM
            upper_value = test_constants.SALES_BASED_FACT_HV_7_NUM
        else:
            table_name = RPA_FACT_TABLE_STR
            granularity = "4"
            different_granularity = "6"
    elif part_key_type == ORACLE_TYPE_NVARCHAR2:
        table_name = RPA_FACT_TABLE_NSTR
        granularity = "4"
        different_granularity = "6"
    else:
        if part_key_type == frontend_api.test_type_canonical_timestamp():
            table_name = RPA_FACT_TABLE_TS
        else:
            table_name = RPA_FACT_TABLE_DATE
        hv_0 = test_constants.SALES_BASED_FACT_PRE_HV
        hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_2 = test_constants.SALES_BASED_FACT_HV_2
        hv_3 = test_constants.SALES_BASED_FACT_HV_3
        hv_4 = test_constants.SALES_BASED_FACT_HV_4
        less_than_option = "older_than_date"
        granularity = "M"
        different_granularity = "Y"

    offload3_opt_name = "partition_names_csv"
    offload3_opt_value = "P3"
    if config.db_type == offload_constants.DBTYPE_TERADATA:
        offload3_opt_name = less_than_option
        offload3_opt_value = hv_3

    expected_goe_part_name = None
    if synthetic_partition_column_expected:
        expected_goe_part_name = synthetic_part_col_name(
            granularity,
            "time_id",
            partition_function=udf,
            synthetic_partition_digits=synthetic_partition_digits,
        )
        expected_goe_part_name = convert_backend_identifier_case(
            config, expected_goe_part_name
        )

    backend_name = convert_backend_identifier_case(config, table_name)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_fact_create_ddl(
            schema, table_name, part_key_type=part_key_type, simple_partition_names=True
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, table_name
            ),
        ],
    )

    # Non-Execute RANGE offload of empty partition.
    if config.db_type != offload_constants.DBTYPE_TERADATA:
        # Empty partitions do not exist on Teradata.
        options = {
            "owner_table": schema + "." + table_name,
            less_than_option: hv_0,
            "offload_partition_functions": udf,
            "offload_partition_granularity": granularity,
            "offload_partition_lower_value": lower_value,
            "offload_partition_upper_value": upper_value,
            "synthetic_partition_digits": synthetic_partition_digits,
            "reset_backend_table": True,
            "execute": False,
        }
        run_offload(options, config, messages)

    # RANGE Offload 1st Partition.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_1,
        "offload_partition_functions": udf,
        "offload_partition_granularity": granularity,
        "offload_partition_lower_value": lower_value,
        "offload_partition_upper_value": upper_value,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_1,
        incremental_key_type=part_key_type,
        backend_table=backend_name,
        partition_functions=udf,
        synthetic_partition_column_name=expected_goe_part_name,
        offload_messages=offload_messages,
    )

    # RANGE Offload 2nd Partition.
    # Attempt to change some partition settings which is ignored because settings come from metadata.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_2,
        "offload_partition_functions": different_udf,
        "offload_partition_granularity": different_granularity,
        "synthetic_partition_digits": different_partition_digits,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_2,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        synthetic_partition_column_name=expected_goe_part_name,
        offload_messages=offload_messages,
    )

    # RANGE Offload 3rd Partition - Verification.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_3,
        "execute": False,
    }
    offload_messages = run_offload(options, config, messages)

    # Assert HV is still from prior offload.
    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_2,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        synthetic_partition_column_name=expected_goe_part_name,
        offload_messages=offload_messages,
    )

    # RANGE Offload With Multiple Partition Names - Expect Exception.
    if config.db_type != offload_constants.DBTYPE_TERADATA:
        # TODO Partition names are unpredicatble on Teradata, for MVP we'll not test this.
        options = {
            "owner_table": schema + "." + table_name,
            "partition_names_csv": "P4,P5",
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT,
        )

    # RANGE Offload 3rd Partition.
    # Test with partition identification by name (if not Teradata) and use agg validate.
    options = {
        "owner_table": schema + "." + table_name,
        offload3_opt_name: offload3_opt_value,
        "verify_row_count": "aggregate",
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_3,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        offload_messages=offload_messages,
    )

    # RANGE No-op of 3rd Partition.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_3,
        "execute": True,
    }
    # On Teradata we can't test by partition name in previous test so this test will not be a no-op.
    offload_messages = run_offload(
        options,
        config,
        messages,
        expected_status=bool(config.db_type == offload_constants.DBTYPE_TERADATA),
    )

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_3,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        offload_messages=offload_messages,
    )

    # Setup - drop oldest partition from fact.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_drop_sales_based_fact_partition_ddls(
            schema, table_name, [hv_0, hv_1], frontend_api, dropping_oldest=True
        ),
    )

    assert not sales_based_fact_partition_exists(
        schema, table_name, [hv_1], frontend_api
    )

    # RANGE Offload After Partition Drop.
    # Offloads next partition from fact table after the oldest partition was dropped.
    # The verification step should still succeed.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_4,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_4,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        offload_messages=offload_messages,
    )

    # Setup - drop all offloaded partitions from fact.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_drop_sales_based_fact_partition_ddls(
            schema,
            table_name,
            [hv_1, hv_2, hv_3, hv_4],
            frontend_api,
            dropping_oldest=True,
        ),
    )

    assert not sales_based_fact_partition_exists(
        schema, table_name, [hv_1, hv_2, hv_3, hv_4], frontend_api
    )

    # RANGE No-op Offload After Partition Drop.
    # Offloads no partitions from fact after all offloaded partitions have been dropped (GOE-1035)
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_4,
        "execute": True,
    }
    offload_messages = run_offload(options, config, messages, expected_status=False)

    assert sales_based_fact_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        repo_client,
        schema,
        data_db,
        table_name,
        hv_4,
        incremental_key_type=part_key_type,
        partition_functions=udf,
        offload_messages=offload_messages,
    )


def test_offload_rpa_int8(config, schema, data_db):
    id = "test_offload_rpa_int8"
    if config.db_type == offload_constants.DBTYPE_TERADATA:
        # TODO We need our numeric sales table to be partitioned on YYYYMM for assertions to make sense.
        #      Didn't have time to rectify this for Teradata MVP.
        pytest.skip(f"Skipping {id} for system/type: {config.db_type}")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            frontend_api.test_type_canonical_int_8(),
        )


def test_offload_rpa_date(config, schema, data_db):
    id = "test_offload_rpa_date"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            frontend_api.test_type_canonical_date(),
        )


def test_offload_rpa_timestamp(config, schema, data_db):
    id = "test_offload_rpa_timestamp"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            frontend_api.test_type_canonical_timestamp(),
        )


def test_offload_rpa_string(config, schema, data_db):
    id = "test_offload_rpa_string"
    if config.db_type == offload_constants.DBTYPE_TERADATA:
        # TODO In Teradata MVP we don't support string based partitioning.
        pytest.skip(f"Skipping {id} for system/type: {config.db_type}")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            frontend_api.test_type_canonical_string(),
        )


def test_offload_rpa_nvarchar2(config, schema, data_db):
    id = "test_offload_rpa_nvarchar2"

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        pytest.skip(
            f"Skipping {id} for system/type: {config.db_type}/{ORACLE_TYPE_NVARCHAR2}"
        )

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            ORACLE_TYPE_NVARCHAR2,
        )


def test_offload_rpa_udf_int8(config, schema, data_db):
    id = "test_offload_rpa_udf_int8"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)

        if not backend_api.goe_partition_functions_supported():
            pytest.skip(
                f"Skipping {id} partition function tests due to goe_partition_functions_supported() == False"
            )

        if config.db_type == offload_constants.DBTYPE_TERADATA:
            # TODO We need our numeric sales table to be partitioned on YYYYMM for assertions to make sense.
            #      Didn't have time to rectify this for Teradata MVP.
            pytest.skip(
                f"Skipping {id} for system/type: {config.db_type}/{frontend_api.test_type_canonical_int_8()}"
            )

        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        backend_api.create_test_partition_functions(
            data_db, udf=test_constants.PARTITION_FUNCTION_TEST_FROM_INT8
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            frontend_api.test_type_canonical_int_8(),
            partition_function=test_constants.PARTITION_FUNCTION_TEST_FROM_INT8,
        )


def test_offload_rpa_udf_string(config, schema, data_db):
    id = "test_offload_rpa_udf_string"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)

        if not backend_api.goe_partition_functions_supported():
            messages.log(
                f"Skipping {id} partition function tests due to goe_partition_functions_supported() == False"
            )
            pytest.skip(
                f"Skipping {id} partition function tests due to goe_partition_functions_supported() == False"
            )

        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        backend_api.create_test_partition_functions(
            data_db, udf=test_constants.PARTITION_FUNCTION_TEST_FROM_STRING
        )

        offload_range_ipa_standard_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            frontend_api.test_type_canonical_string(),
            partition_function=test_constants.PARTITION_FUNCTION_TEST_FROM_STRING,
        )


def test_offload_rpa_alpha(config, schema, data_db):
    """Tests ensuring lower and upper case alpha characters are differentiated correctly."""
    id = "test_offload_rpa_alpha"

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        pytest.skip(f"Skipping {id} for system/type: {config.db_type}/AlphaString")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        canonical_string = frontend_api.test_type_canonical_string()

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=[
                f"DROP TABLE {schema}.{RPA_ALPHA_FACT_TABLE}",
                f"""CREATE TABLE {schema}.{RPA_ALPHA_FACT_TABLE}
                    STORAGE (INITIAL 64K NEXT 64K)
                    PARTITION BY RANGE(str)
                    ( PARTITION upper_a_j VALUES LESS THAN ('K')
                    , PARTITION upper_k_t VALUES LESS THAN ('U')
                    , PARTITION upper_u_z VALUES LESS THAN ('a')
                    , PARTITION lower_a_j VALUES LESS THAN ('k')
                    , PARTITION lower_k_t VALUES LESS THAN ('u')
                    , PARTITION lower_u_z VALUES LESS THAN ('zzzzzz'))
                    AS
                    SELECT owner
                    ,      object_name
                    ,      dbms_random.string('a',5) AS str
                    FROM   all_objects
                    WHERE  ROWNUM <= 100""",
            ],
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, RPA_ALPHA_FACT_TABLE
                ),
            ],
        )

        # 1st Offload By Upper Case Character.
        options = {
            "owner_table": schema + "." + RPA_ALPHA_FACT_TABLE,
            "less_than_value": "U",
            "offload_partition_granularity": "1",
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)

        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            RPA_ALPHA_FACT_TABLE,
            "U",
            incremental_key="STR",
            incremental_key_type=canonical_string,
            offload_messages=offload_messages,
        )

        # 2nd Offload By Lower Case Character.
        options = {
            "owner_table": schema + "." + RPA_ALPHA_FACT_TABLE,
            "less_than_value": "u",
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)

        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            RPA_ALPHA_FACT_TABLE,
            "u",
            incremental_key="STR",
            incremental_key_type=canonical_string,
            offload_messages=offload_messages,
        )


def test_offload_rpa_empty_partitions(config, schema, data_db):
    """Tests ensuring empty partitions are offloaded correctly."""
    id = "test_offload_rpa_empty_partitions"

    if config.db_type == offload_constants.DBTYPE_TERADATA:
        pytest.skip(
            f"Skipping {id} for {config.db_type} because empty partitions are not a thing"
        )

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup, create a partitioned table with one populated partition and a series of empty ones
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_fact_create_ddl(
                schema,
                NOSEG_FACT,
                simple_partition_names=True,
                extra_pred="AND time_id = TO_DATE('2012-01-01','YYYY-MM-DD')",
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, NOSEG_FACT
                ),
            ],
        )

        # Offload first partition of fact, this one is populated.
        options = {
            "owner_table": schema + "." + NOSEG_FACT,
            "older_than_date": test_constants.SALES_BASED_FACT_HV_2,
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)
        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            NOSEG_FACT,
            test_constants.SALES_BASED_FACT_HV_2,
            offload_messages=offload_messages,
        )

        # Offload remaining empty partitions, metadata should still reflect this.
        options = {
            "owner_table": schema + "." + NOSEG_FACT,
            "older_than_date": test_constants.SALES_BASED_FACT_HV_5,
            "max_offload_chunk_count": 1,
            "execute": True,
        }
        offload_messages = run_offload(options, config, messages)
        assert sales_based_fact_assertion(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            NOSEG_FACT,
            test_constants.SALES_BASED_FACT_HV_5,
            offload_messages=offload_messages,
        )
