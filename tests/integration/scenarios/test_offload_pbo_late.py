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

from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_TERADATA,
    IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_EXCEPTION_TEXT,
)
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_metadata_functions import (
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_source_data import (
    OFFLOAD_TYPE_CHANGE_FOR_PBO_EXCEPTION_TEXT,
    PREDICATE_APPEND_HWM_MESSAGE_TEXT,
    PREDICATE_TYPE_NO_MODIFY_RESET_EXCEPTION_TEXT,
)
from goe.offload.predicate_offload import GenericPredicate
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)

from tests.integration.scenarios.assertion_functions import (
    sales_based_fact_assertion,
    text_in_log,
)
from tests.integration.scenarios.scenario_constants import (
    OFFLOAD_PATTERN_100_0,
    OFFLOAD_PATTERN_100_10,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import drop_backend_test_table
from tests.integration.scenarios.test_offload_pbo import (
    const_to_date_expr,
    check_predicate_count_matches_log,
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


RANGE_TABLE_LATE = "STORY_PBO_R_LATE"
RANGE_TABLE_LATE_100_0 = "STORY_PBO_R_LATE_100_0"
LAR_TABLE_LATE = "STORY_PBO_LAR_LATE"
LAR_TABLE_LATE_100_0 = "STORY_PBO_LAR_LATE_100_0"
LIST_TABLE = "STORY_PBO_LIST"
LIST_TABLE_LATE = "STORY_PBO_L90_10_LATE"
LIST_100_0_LATE = "STORY_PBO_L100_0_LATE"
MCOL_TABLE_LATE = "STORY_PBO_MC_LATE"
RANGE_SP_LATE = "STORY_PBO_RR_LATE"

OLD_HV_1 = "1970-01-01"


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


def gen_insert_late_arriving_sales_based_multi_pcol_data(
    schema, table_name, new_time_id_string
):
    ins = f"""INSERT INTO {schema}.{table_name}
             SELECT prod_id, cust_id, TO_DATE('{new_time_id_string}','YYYY-MM-DD HH24:MI:SS') AS time_id
             ,      channel_id, promo_id, quantity_sold, amount_sold
             ,      EXTRACT(YEAR FROM DATE' {new_time_id_string}') AS time_year
             ,      EXTRACT(MONTH FROM DATE' {new_time_id_string}') AS time_month
             FROM   {schema}.{table_name}
             WHERE  ROWNUM = 1"""
    return [ins]


def offload_pbo_late_100_x_tests(
    config,
    backend_api,
    frontend_api,
    messages,
    repo_client,
    schema,
    data_db,
    table_name,
    offload_pattern,
    test_id,
):
    """Tests for testing 100/0 and 100/10 which were similar enough to share the config"""
    # TODO I think 100/10 is no longer applicable.
    assert offload_pattern in (OFFLOAD_PATTERN_100_0, OFFLOAD_PATTERN_100_10)
    assert table_name in (RANGE_TABLE_LATE_100_0, LAR_TABLE_LATE_100_0)

    part_key_type = frontend_api.test_type_canonical_date()
    if table_name == RANGE_TABLE_LATE_100_0:
        inc_key = "TIME_ID"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE

        def add_rows_fn():
            return frontend_api.sales_based_fact_late_arriving_data_sql(
                schema, table_name, OLD_HV_1
            )

        if offload_pattern == OFFLOAD_PATTERN_100_0:
            test_id = "range_100_0"
            hv_1 = chk_hv_1 = None
        else:
            test_id = "range_100_10"
            hv_1 = chk_hv_1 = test_constants.SALES_BASED_FACT_HV_2
    elif table_name == LAR_TABLE_LATE_100_0:
        if config.db_type == DBTYPE_TERADATA:
            messages.log(
                "Skipping LAR tests on Teradata because CASE_N is not yet supported"
            )
            return []
        inc_key = "YRMON"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE

        def add_rows_fn():
            return frontend_api.sales_based_list_fact_late_arriving_data_sql(
                schema, table_name, OLD_HV_1, test_constants.SALES_BASED_FACT_HV_1
            )

        if offload_pattern == OFFLOAD_PATTERN_100_0:
            test_id = "lar_100_0"
            hv_1 = chk_hv_1 = None
        else:
            test_id = "lar_100_10"
            hv_1 = test_constants.SALES_BASED_FACT_HV_2
            chk_hv_1 = test_constants.SALES_BASED_FACT_HV_1
    else:
        raise NotImplementedError(f"Test table not implemented: {table_name}")

    # Offload 1st partition putting table in "range" mode.
    options = {
        "owner_table": schema + "." + table_name,
        "older_than_date": hv_1,
        "offload_type": OFFLOAD_TYPE_FULL,
        "ipa_predicate_type": ipa_predicate_type,
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
        chk_hv_1,
        offload_pattern=offload_pattern,
        incremental_key=inc_key,
        incremental_key_type=part_key_type,
        ipa_predicate_type=ipa_predicate_type,
        offload_messages=offload_messages,
    )

    # Add late arriving data below the HWM.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=add_rows_fn(),
    )
    assert (
        frontend_api.get_table_row_count(
            schema,
            table_name,
            filter_clause="time_id = %s" % const_to_date_expr(config, OLD_HV_1),
        )
        > 0
    )

    # Attempt to Switch to INCREMENTAL During LAPBO Offload.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(
            "column(time_id) = datetime(%s)" % OLD_HV_1
        ),
        "ipa_predicate_type": ipa_predicate_type,
        "offload_type": OFFLOAD_TYPE_INCREMENTAL,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=OFFLOAD_TYPE_CHANGE_FOR_PBO_EXCEPTION_TEXT,
    )

    # Offload Late Arriving Data.
    # Late arriving data should be invisible as far as metadata and hybrid view is concerned.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(
            "column(time_id) = datetime(%s)" % OLD_HV_1
        ),
        "ipa_predicate_type": ipa_predicate_type,
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
        chk_hv_1,
        offload_pattern=offload_pattern,
        incremental_key=inc_key,
        incremental_key_type=part_key_type,
        ipa_predicate_type=ipa_predicate_type,
        incremental_predicate_value="NULL",
        offload_messages=offload_messages,
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        offload_messages,
        schema,
        table_name,
        test_id,
        "time_id = %s" % const_to_date_expr(config, OLD_HV_1),
    )

    # Attempt to re-offload same predicate.
    run_offload(options, config, messages, expected_status=False)


def offload_pbo_late_arriving_std_range_tests(
    schema,
    data_db,
    config,
    backend_api,
    frontend_api,
    messages,
    repo_client,
    table_name,
    test_id,
):
    assert table_name in (
        RANGE_TABLE_LATE,
        LAR_TABLE_LATE,
        MCOL_TABLE_LATE,
        RANGE_SP_LATE,
    )

    offload_partition_granularity = None
    less_than_option = "older_than_date"
    chk_cnt_filter = "time_id = %s" % const_to_date_expr(config, OLD_HV_1)
    expected_incremental_range = None
    offload_by_subpartition = False
    offload_partition_columns = None
    part_key_type = frontend_api.test_type_canonical_date()
    check_hwm_in_metadata = True
    if table_name == RANGE_TABLE_LATE:
        inc_key = "TIME_ID"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        hv_1 = chk_hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_pred = "(column(time_id) = datetime(%s))" % OLD_HV_1

        def add_row_fn():
            return frontend_api.sales_based_fact_late_arriving_data_sql(
                schema, table_name, OLD_HV_1
            )

    elif table_name == LAR_TABLE_LATE:
        inc_key = "YRMON"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
        chk_hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_1 = test_constants.SALES_BASED_FACT_HV_2
        hv_pred = (
            "(column(yrmon) = datetime(%s)) and (column(time_id) = datetime(%s))"
            % (test_constants.SALES_BASED_FACT_HV_1, OLD_HV_1)
        )

        def add_row_fn():
            return frontend_api.sales_based_list_fact_late_arriving_data_sql(
                schema, table_name, OLD_HV_1, test_constants.SALES_BASED_FACT_HV_1
            )

    elif table_name == MCOL_TABLE_LATE:
        inc_key = "TIME_YEAR, TIME_MONTH, TIME_ID"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        hv_1 = chk_hv_1 = "2012,12,2012-12-01"
        offload_partition_granularity = "1,1,Y"
        less_than_option = "less_than_value"
        hv_pred = "(column(time_id) = datetime(%s))" % OLD_HV_1

        def add_row_fn():
            return gen_insert_late_arriving_sales_based_multi_pcol_data(
                schema, table_name, OLD_HV_1
            )

        check_hwm_in_metadata = False
        if config.target == DBTYPE_BIGQUERY:
            offload_partition_columns = "TIME_ID"
            offload_partition_granularity = None
    elif table_name == RANGE_SP_LATE:
        inc_key = "TIME_ID"
        offload_by_subpartition = True
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        hv_1 = chk_hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_pred = "(column(time_id) = datetime(%s))" % OLD_HV_1

        def add_row_fn():
            return frontend_api.sales_based_fact_late_arriving_data_sql(
                schema, table_name, OLD_HV_1, channel_id_literal=2
            )

        expected_incremental_range = "SUBPARTITION"
    else:
        raise NotImplementedError(f"Test table not implemented: {table_name}")

    # Offload 1st partition putting table in "range" mode.
    options = {
        "owner_table": schema + "." + table_name,
        less_than_option: hv_1,
        "offload_by_subpartition": offload_by_subpartition,
        "offload_partition_columns": offload_partition_columns,
        "offload_partition_granularity": offload_partition_granularity,
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
        chk_hv_1,
        incremental_key=inc_key,
        incremental_key_type=part_key_type,
        ipa_predicate_type=ipa_predicate_type,
        incremental_predicate_value="NULL",
        incremental_range=expected_incremental_range,
        check_hwm_in_metadata=check_hwm_in_metadata,
        offload_messages=offload_messages,
    )

    # No-op Offload Late Arriving Data.
    # Offload by predicate but when there isn't anything to offload yet.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(hv_pred),
        "ipa_predicate_type": ipa_predicate_type,
        "execute": True,
    }
    run_offload(options, config, messages, expected_status=False)

    # Add late arriving data below the HWM.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=add_row_fn(),
    )
    assert (
        frontend_api.get_table_row_count(
            schema, table_name, filter_clause=chk_cnt_filter
        )
        > 0
    )

    # Attempt offload late arriving predicate with --reset-hybrid-view, fails.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(hv_pred),
        "reset_hybrid_view": True,
        "ipa_predicate_type": ipa_predicate_type,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=PREDICATE_TYPE_NO_MODIFY_RESET_EXCEPTION_TEXT,
    )

    if table_name in (RANGE_TABLE_LATE, RANGE_SP_LATE):
        # Attempt to subvert RANGE with late arriving predicate and --offload-predicate-type=LIST_AS_RANGE.
        options = {
            "owner_table": schema + "." + table_name,
            "offload_predicate": GenericPredicate(hv_pred),
            "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=IPA_PREDICATE_TYPE_EXCEPTION_TEXT,
        )

    if table_name == LAR_TABLE_LATE:
        # Attempt to subvert LIST_AS_RANGE with late arriving predicate and --offload-predicate-type=LIST.
        options = {
            "owner_table": schema + "." + table_name,
            "offload_predicate": GenericPredicate(hv_pred),
            "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_LIST,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT,
        )

    # Attempt to subvert "range" config with --offload-predicate-type=PREDICATE'.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(hv_pred),
        "ipa_predicate_type": INCREMENTAL_PREDICATE_TYPE_PREDICATE,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string="is not valid for existing",
    )

    # Offload Late Arriving Data.
    # Late arriving data should be invisible as far as metadata is concerned.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(hv_pred),
        "ipa_predicate_type": ipa_predicate_type,
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
        chk_hv_1,
        incremental_key=inc_key,
        incremental_key_type=part_key_type,
        ipa_predicate_type=ipa_predicate_type,
        incremental_predicate_value="NULL",
        check_hwm_in_metadata=check_hwm_in_metadata,
        offload_messages=offload_messages,
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        offload_messages,
        schema,
        table_name,
        test_id,
        chk_cnt_filter,
    )
    assert text_in_log(offload_messages, PREDICATE_APPEND_HWM_MESSAGE_TEXT, messages)

    # Attempt to re-offload same predicate.
    run_offload(options, config, messages, expected_status=False)


def test_offload_pbo_late_range_90_10(config, schema, data_db):
    """Tests for Late Arriving Predicate Based Offload."""
    id = "test_offload_pbo_late_range_90_10"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
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
            frontend_sqls=frontend_api.sales_based_fact_create_ddl(
                schema, RANGE_TABLE_LATE, simple_partition_names=True
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, RANGE_TABLE_LATE
                ),
            ],
        )

        offload_pbo_late_arriving_std_range_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            RANGE_TABLE_LATE,
            id,
        )

        # TODO do we need to create a test for below 100_10 tests?
        # offload_pbo_late_100_x_tests(config, backend_api, frontend_api, messages, repo_client, schema,
        # data_db, RANGE_TABLE_LATE, OFFLOAD_PATTERN_100_10, id)


def test_offload_pbo_late_range_100_0(config, schema, data_db):
    """Tests for Late Arriving Predicate Based Offload."""
    id = "test_offload_pbo_late_range_100_0"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
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
            frontend_sqls=frontend_api.sales_based_fact_create_ddl(
                schema, RANGE_TABLE_LATE_100_0, simple_partition_names=True
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, RANGE_TABLE_LATE_100_0
                ),
            ],
        )

        offload_pbo_late_100_x_tests(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            RANGE_TABLE_LATE_100_0,
            OFFLOAD_PATTERN_100_0,
            id,
        )


def test_offload_pbo_late_list_as_range(config, schema, data_db):
    """Tests for Late Arriving Predicate Based Offload on a LIST_AS_RANGE."""
    id = "test_offload_pbo_late_list_as_range"
    if config.db_type == DBTYPE_TERADATA:
        pytest.skip(f"Skipping {id} on Teradata because CASE_N is not yet supported")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
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
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LAR_TABLE_LATE,
                part_key_type=frontend_api.test_type_canonical_date(),
                with_drop=True,
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, LAR_TABLE_LATE
                ),
            ],
        )

        offload_pbo_late_arriving_std_range_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            LAR_TABLE_LATE,
            id,
        )

        # TODO do we need to create a test for below 100_10 tests?
        # offload_pbo_late_100_x_tests(options, backend_api, frontend_api, messages, repo_client, schema,
        #                                    hybrid_schema, data_db, LAR_TABLE_LATE, OFFLOAD_PATTERN_100_10, id)


def test_offload_pbo_late_list_as_range_100_0(config, schema, data_db):
    """Tests for Late Arriving Predicate Based Offload on a LIST_AS_RANGE 100/0."""
    id = "test_offload_pbo_late_list_as_range_100_0"
    if config.db_type == DBTYPE_TERADATA:
        pytest.skip(f"Skipping {id} on Teradata because CASE_N is not yet supported")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
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
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LAR_TABLE_LATE_100_0,
                part_key_type=frontend_api.test_type_canonical_date(),
                with_drop=True,
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, LAR_TABLE_LATE_100_0
                ),
            ],
        )

        offload_pbo_late_100_x_tests(
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            schema,
            data_db,
            LAR_TABLE_LATE_100_0,
            OFFLOAD_PATTERN_100_0,
            id,
        )


def test_offload_pbo_late_mcol_range(config, schema, data_db):
    """Tests for Late Arriving Predicate Based Offload on a multi-partition column table."""
    id = "test_offload_pbo_late_mcol_range"
    if config.db_type == DBTYPE_TERADATA:
        pytest.skip(
            "Skipping multi-column tests on Teradata because we currently only support RANGE with a single column"
        )

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
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
            frontend_sqls=frontend_api.sales_based_multi_col_fact_create_ddl(
                schema,
                MCOL_TABLE_LATE,
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, MCOL_TABLE_LATE
                ),
            ],
        )

        offload_pbo_late_arriving_std_range_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            MCOL_TABLE_LATE,
            id,
        )


def test_offload_pbo_late_range_sub(config, schema, data_db):
    """Tests for Late Arriving Predicate Based Offload on a RANGE/RANGE table."""
    id = "test_offload_pbo_late_range_sub"

    if config.db_type == DBTYPE_TERADATA:
        pytest.skip(
            "Skipping subpartition tests on Teradata because we currently only support RANGE at top level"
        )

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
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
            frontend_sqls=frontend_api.sales_based_subpartitioned_fact_ddl(
                schema,
                RANGE_SP_LATE,
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, RANGE_SP_LATE
                ),
            ],
        )

        offload_pbo_late_arriving_std_range_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            RANGE_SP_LATE,
            id,
        )
