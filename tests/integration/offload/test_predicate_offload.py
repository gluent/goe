#! /usr/bin/env python3

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    Offload predicate test code.
"""

from copy import copy
import pytest

from goe.goe import OffloadOperation
from goe.offload.factory.backend_table_factory import backend_table_factory
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload import OffloadException
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.predicate_offload import GenericPredicate
from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
    run_offload,
    run_setup_ddl,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


DIM_NAME = "INTEG_PBO_DIM"
FACT_NAME = "INTEG_PBO_FACT"


@pytest.fixture
def config():
    return build_current_options()


@pytest.fixture
def schema():
    return get_default_test_user()


@pytest.fixture
def data_db(schema, config):
    data_db = data_db_name(schema, config)
    data_db = convert_backend_identifier_case(config, data_db)
    return data_db


def create_and_offload_dim_table(config, frontend_api, messages, schema):
    """To create backend test tables we need to create them in the frontend and offload them."""
    # Setup non-partitioned table
    run_setup_ddl(
        config,
        frontend_api,
        messages,
        frontend_api.standard_dimension_frontend_ddl(schema, DIM_NAME),
    )
    # Ignore return status, if the table has already been offloaded previously then we'll re-use it.
    try:
        run_offload(
            {
                "owner_table": schema + "." + DIM_NAME,
                "create_backend_db": True,
                "execute": True,
            }
        )
    except OffloadException:
        # If this one fails then we let the exception bubble up.
        run_offload(
            {
                "owner_table": schema + "." + DIM_NAME,
                "reset_backend_table": True,
                "create_backend_db": True,
                "execute": True,
            }
        )


def create_fact_table(config, frontend_api, messages, schema):
    # Setup partitioned table
    run_setup_ddl(
        config,
        frontend_api,
        messages,
        frontend_api.sales_based_fact_create_ddl(
            schema, FACT_NAME, simple_partition_names=True
        ),
    )


def source_to_canonical_mappings(
    backend_columns,
    backend_table,
    canonical_overrides,
    char_semantics_overrides,
    detect_sizes=False,
    max_rdbms_time_scale=None,
) -> list:
    canonical_columns = []
    for tab_col in backend_columns:
        new_col = backend_table.to_canonical_column_with_overrides(
            tab_col,
            canonical_overrides,
            detect_sizes=detect_sizes,
            max_rdbms_time_scale=max_rdbms_time_scale,
        )
        canonical_columns.append(new_col)
    # Process any char semantics overrides
    for cs_col in [
        _ for _ in canonical_columns if _.name in char_semantics_overrides.keys()
    ]:
        cs_col.char_semantics = char_semantics_overrides[cs_col.name]
        cs_col.from_override = True

    return canonical_columns


def fake_partition_col_on_backend_table(
    frontend_table, backend_table, config, operation, messages
):
    # This is a bit hacky but allows us to test synthetic part cols. The hacks:
    # 1) We don't pass reset_backend_table=True, ordinarily we would in order to change partition info
    # 2) We use defaults_for_fresh_offload() below even though we're not doing a reset, this is so we can get
    #    fresh synthetic column info rather than picking up defaults from the existing table.
    # 3) We map backend cols to canonical and then generate synthetic partition columns again based on operation
    new_backend_table = copy(backend_table)
    operation.defaults_for_fresh_offload(
        frontend_table,
        config,
        messages,
        new_backend_table,
    )
    canonical_columns = source_to_canonical_mappings(
        new_backend_table.get_non_synthetic_columns(),
        new_backend_table,
        [],
        {},
    )
    canonical_columns = operation.set_partition_info_on_canonical_columns(
        canonical_columns,
        frontend_table.columns,
        new_backend_table,
    )
    backend_columns = new_backend_table.convert_canonical_columns_to_backend(
        canonical_columns
    )
    new_backend_table.set_columns(backend_columns)
    new_backend_table.refresh_operational_settings(operation, frontend_table.columns)
    return new_backend_table


def test_ida_predicate_render_to_sql_dim(config, schema, data_db):
    id = "test_ida_predicate_render_to_sql"
    test_messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, test_messages)
    frontend_api = get_frontend_testing_api(config, test_messages, trace_action=id)
    be_dim_table = convert_backend_identifier_case(config, DIM_NAME)

    messages = OffloadMessages.from_options(config, log_fh=test_messages.get_log_fh())
    create_and_offload_dim_table(config, frontend_api, test_messages, schema)
    frontend_table = OffloadSourceTable.create(
        schema, DIM_NAME, config, messages, dry_run=True
    )
    operation = OffloadOperation.from_dict(
        {"owner_table": "%s.%s" % (schema, DIM_NAME)},
        config,
        messages,
    )
    backend_table = backend_table_factory(
        data_db,
        be_dim_table,
        config.target,
        config,
        messages,
        operation,
        dry_run=True,
    )

    # Setup complete, on to some actual testing.

    # Frontend predicates.
    pred_expect_equal = frontend_api.expected_std_dim_offload_predicates()

    for predicate_dsl, expected_sql in pred_expect_equal:
        assert (
            frontend_table.predicate_to_where_clause(GenericPredicate(predicate_dsl))
            == expected_sql
        )

    # Backend predicates.
    pred_expect_equal = backend_api.expected_std_dim_offload_predicates()
    for predicate_dsl, expected_sql in pred_expect_equal:
        backend_predicate = backend_table.predicate_to_where_clause(
            GenericPredicate(predicate_dsl)
        )
        assert backend_predicate == expected_sql

    # Synthetic partition backend predicates.
    expect_equal_by_synth_part = (
        backend_api.expected_std_dim_synthetic_offload_predicates()
    )

    for synth_part, expect_equal in expect_equal_by_synth_part:
        partition_column, granularity, digits = synth_part

        operation = OffloadOperation.from_dict(
            {
                "owner_table": "%s.%s" % (schema, DIM_NAME),
                "offload_partition_columns": partition_column,
                "offload_partition_granularity": granularity,
                "synthetic_partition_digits": digits,
            },
            config,
            messages,
        )

        partitioned_backend_table = fake_partition_col_on_backend_table(
            frontend_table, backend_table, config, operation, messages
        )

        for predicate_dsl, expected_sql in expect_equal:
            backend_sql = partitioned_backend_table.predicate_to_where_clause(
                GenericPredicate(predicate_dsl)
            )
            assert backend_sql == expected_sql


def test_ida_predicate_render_to_sql_fact(config, schema):
    id = "test_ida_predicate_render_to_sql"
    test_messages = get_test_messages(config, id)
    frontend_api = get_frontend_testing_api(config, test_messages, trace_action=id)

    messages = OffloadMessages.from_options(config, log_fh=test_messages.get_log_fh())
    create_fact_table(config, frontend_api, test_messages, schema)
    frontend_table = OffloadSourceTable.create(
        schema, FACT_NAME, config, messages, dry_run=True
    )

    # Setup complete, on to some actual testing.
    pred_expect_equal = frontend_api.expected_sales_offload_predicates()

    for (
        predicate_dsl,
        expected_sql,
        expected_bind_sql,
        expected_binds,
    ) in pred_expect_equal:
        assert (
            frontend_table.predicate_to_where_clause(GenericPredicate(predicate_dsl))
            == expected_sql
        )
        if expected_bind_sql or expected_binds:
            (
                where_clause,
                binds,
            ) = frontend_table.predicate_to_where_clause_with_binds(
                GenericPredicate(predicate_dsl)
            )
            assert where_clause == expected_bind_sql
            if expected_binds:
                for p in binds:
                    assert p.param_name in expected_binds
                    assert p.param_value == expected_binds[p.param_name]
