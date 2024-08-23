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
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_metadata_functions import (
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_source_data import (
    PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
    PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT,
    PREDICATE_TYPE_REQUIRED_EXCEPTION_TEXT,
    RANGE_AND_PREDICATE_WITHOUT_PART_KEY_EXCEPTION_TEXT,
)
from goe.offload.predicate_offload import GenericPredicate
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
)

from tests.integration.scenarios.assertion_functions import (
    sales_based_fact_assertion,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
    no_query_import_transport_method,
)
from tests.integration.scenarios.test_offload_pbo import (
    check_predicate_count_matches_log,
    pbo_assertion,
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


RANGE_TABLE_INTRA = "STORY_PBO_R_INTRA"
LAR_TABLE_INTRA = "STORY_PBO_LAR_INTRA"
LIST_TABLE_INTRA = "STORY_PBO_L_INTRA"


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


def offload_pbo_intra_day_std_range_tests(
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
    def gen_pred(hv_template, hv_1, hv_2, channel_id):
        date_part = hv_template % {"hv_1": hv_1, "hv_2": hv_2}
        if isinstance(channel_id, (list, tuple)):
            channel_part = "(column(channel_id) in (%s))" % ",".join(
                "numeric({})".format(_) for _ in channel_id
            )
        else:
            channel_part = "(column(channel_id) = numeric(%s))" % channel_id
        return date_part + " and" + channel_part

    assert table_name in (RANGE_TABLE_INTRA, LAR_TABLE_INTRA)

    part_key_type = frontend_api.test_type_canonical_date()
    if table_name == RANGE_TABLE_INTRA:
        test_id = "range"
        inc_key = "TIME_ID"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        ipa_and_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE
        hv_1 = chk_hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_2 = test_constants.SALES_BASED_FACT_HV_2
        hv_pred = "((column(time_id) >= datetime(%(hv_1)s)) and (column(time_id) < datetime(%(hv_2)s)))"
        chk_cnt_filter = "time_id >= DATE' %(hv_1)s' AND time_id < DATE' %(hv_2)s' AND channel_id IN (%(channel)s)"
        metadata_chk_hvs = [hv_1, hv_2]
    elif table_name == LAR_TABLE_INTRA:
        if config.db_type == offload_constants.DBTYPE_TERADATA:
            messages.log(
                "Skipping LAR tests on Teradata because CASE_N is not yet supported"
            )
            return
        test_id = "lar"
        inc_key = "YRMON"
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
        ipa_and_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE
        chk_hv_1 = test_constants.SALES_BASED_FACT_HV_1
        hv_1 = test_constants.SALES_BASED_FACT_HV_2
        hv_2 = test_constants.SALES_BASED_FACT_HV_3
        hv_pred = "(column(yrmon) = datetime(%(hv_2)s))"
        chk_cnt_filter = "yrmon = DATE' %(hv_2)s' AND channel_id IN (%(channel)s)"
        metadata_chk_hvs = [hv_2]

    # Attempt to offload for first time with predicate and predicate type ..._AND_PREDICATE.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "2")),
        "ipa_predicate_type": ipa_and_predicate_type,
        "reset_backend_table": True,
        "create_backend_db": True,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=offload_constants.IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT,
    )

    # Offload 1st partition putting table in "range" mode.
    options = {
        "owner_table": schema + "." + table_name,
        "older_than_date": hv_1,
        "reset_backend_table": True,
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
        table_name,
        chk_hv_1,
        incremental_key=inc_key,
        incremental_key_type=part_key_type,
        incremental_predicate_value="NULL",
        ipa_predicate_type=ipa_predicate_type,
    )

    # Attempts to use a predicate without supplying the partition column which is invalid.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate("(column(channel_id) = numeric(2))"),
        "ipa_predicate_type": ipa_and_predicate_type,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=RANGE_AND_PREDICATE_WITHOUT_PART_KEY_EXCEPTION_TEXT,
    )

    # Attempts to use a predicate without supplying the partition column which is invalid.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "2")),
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=PREDICATE_TYPE_REQUIRED_EXCEPTION_TEXT,
    )

    # Offload 1st predicate on top of HV_1 and explicitly state predicate type ..._AND_PREDICATE.
    # Runs in verification mode so we don't have to re-do prior step to test without pred type..
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "2")),
        "ipa_predicate_type": ipa_and_predicate_type,
        "offload_transport_method": no_query_import_transport_method(
            config, no_table_centric_sqoop=True
        ),
        "execute": False,
    }
    run_offload(
        options,
        config,
        messages,
    )

    # Offload 1st predicate on top of HV_1 which will switch table to ..._AND_PREDICATE.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "2")),
        "ipa_predicate_type": ipa_and_predicate_type,
        "offload_transport_method": no_query_import_transport_method(
            config, no_table_centric_sqoop=True
        ),
        "execute": True,
    }
    messages.log(f"{test_id}:1", detail=VVERBOSE)
    run_offload(options, config, messages)
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
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        table_name,
        number_of_predicates=1,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        expected_incremental_range="PARTITION",
        values_in_predicate_value_metadata=metadata_chk_hvs + ["(2)"],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        table_name,
        f"{test_id}:1",
        chk_cnt_filter % {"hv_1": hv_1, "hv_2": hv_2, "channel": "2"},
    )

    # Attempt to switch to FULL while in ..._AND_PREDICATE config.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "2")),
        "ipa_predicate_type": ipa_and_predicate_type,
        "offload_type": OFFLOAD_TYPE_FULL,
        "execute": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT,
    )

    # Offloads 2nd predicate (IN list) on top of HV_1 to top up data.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate": GenericPredicate(
            gen_pred(hv_pred, hv_1, hv_2, ["3", "4"])
        ),
        "offload_transport_method": no_query_import_transport_method(
            config, no_table_centric_sqoop=True
        ),
        "execute": True,
    }
    messages.log(f"{test_id}:2", detail=VVERBOSE)
    run_offload(options, config, messages)
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
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        table_name,
        number_of_predicates=2,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        values_in_predicate_value_metadata=metadata_chk_hvs + ["(2)", "(3)", "(4)"],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        table_name,
        f"{test_id}:2",
        chk_cnt_filter % {"hv_1": hv_1, "hv_2": hv_2, "channel": "3,4"},
    )

    # Offloads 3rd predicate but with --no-modify-hybrid-view - only moves data, leaves HV as before.
    options = {
        "owner_table": schema + "." + table_name,
        "offload_predicate_modify_hybrid_view": False,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "5")),
        "offload_transport_method": no_query_import_transport_method(
            config, no_table_centric_sqoop=True
        ),
        "execute": True,
    }
    messages.log(f"{test_id}:3", detail=VVERBOSE)
    run_offload(options, config, messages)
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
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        table_name,
        number_of_predicates=2,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        values_in_predicate_value_metadata=metadata_chk_hvs + ["(2)", "(3)", "(4)"],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        table_name,
        f"{test_id}:3",
        chk_cnt_filter % {"hv_1": hv_1, "hv_2": hv_2, "channel": "5"},
    )

    # Offloads predicate on top of HV_1 but with --reset-hybrid-view - moves data and resets metadata.
    options = {
        "owner_table": schema + "." + table_name,
        "reset_hybrid_view": True,
        "offload_predicate": GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, "1")),
        "ipa_predicate_type": ipa_and_predicate_type,
        "execute": True,
    }
    messages.log(f"{test_id}:4", detail=VVERBOSE)
    run_offload(options, config, messages)
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
        check_backend_rowcount=False,
    )
    assert pbo_assertion(
        messages,
        repo_client,
        schema,
        table_name,
        number_of_predicates=1,
        expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
        values_in_predicate_value_metadata=metadata_chk_hvs + ["(1)"],
    )
    assert check_predicate_count_matches_log(
        frontend_api,
        messages,
        schema,
        table_name,
        f"{test_id}:4",
        chk_cnt_filter % {"hv_1": hv_1, "hv_2": hv_2, "channel": "1"},
    )

    # Offload 2nd partition putting the table back into "range" mode, no data should be moved.
    options = {
        "owner_table": schema + "." + table_name,
        "older_than_date": hv_2,
        "execute": True,
    }
    # Disabled until issue-99 is fixed.
    # messages.log(f"{test_id}:5", detail=VVERBOSE)
    # run_offload(options, config, messages)
    # assert sales_based_fact_assertion(
    #    config,
    #    backend_api,
    #    frontend_api,
    #    messages,
    #    repo_client,
    #    schema,
    #    data_db,
    #    table_name,
    #    chk_hv_1,
    #    incremental_key=inc_key,
    #    incremental_key_type=part_key_type,
    #    ipa_predicate_type=ipa_predicate_type,
    #    incremental_predicate_value="NULL",
    #    check_backend_rowcount=False,
    # )


def test_offload_pbo_intra_range(config, schema, data_db):
    """Tests for Intra Day Predicate Based Offload."""
    id = "test_offload_pbo_intra_range"
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
                schema, RANGE_TABLE_INTRA, simple_partition_names=True
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, RANGE_TABLE_INTRA
                ),
            ],
        )

        offload_pbo_intra_day_std_range_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            RANGE_TABLE_INTRA,
            id,
        )


def test_offload_pbo_intra_lar(config, schema, data_db):
    """Tests for intra day predicate based offload on LIST_AS_RANGE table."""
    id = "test_offload_pbo_intra_lar"
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
                LAR_TABLE_INTRA,
                part_key_type=frontend_api.test_type_canonical_date(),
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, LAR_TABLE_INTRA
                ),
            ],
        )

        offload_pbo_intra_day_std_range_tests(
            schema,
            data_db,
            config,
            backend_api,
            frontend_api,
            messages,
            repo_client,
            LAR_TABLE_INTRA,
            id,
        )


def test_offload_pbo_intra_list(config, schema, data_db):
    """Tests for Intra Day Predicate Based Offload."""
    id = "test_offload_pbo_intra_list_as_range"
    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
                schema,
                LIST_TABLE_INTRA,
                part_key_type=frontend_api.test_type_canonical_date(),
                default_partition=True,
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, LIST_TABLE_INTRA
                ),
            ],
        )

        # Offload 1st partition putting table in LIST mode.
        options = {
            "owner_table": schema + "." + LIST_TABLE_INTRA,
            "equal_to_values": [test_constants.SALES_BASED_FACT_HV_1],
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        run_offload(options, config, messages)

        # Intra day is only supported for RANGE/LIST_AS_RANGE - not LIST.
        options = {
            "owner_table": schema + "." + LIST_TABLE_INTRA,
            "offload_predicate": GenericPredicate(
                "(column(time_id) = datetime(%s)) and (column(channel_id) = numeric(3))"
                % test_constants.SALES_BASED_FACT_HV_3
            ),
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
        )
