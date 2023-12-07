#! /usr/bin/env python3
"""
    Offload predicate test code.
    LICENSE_TEXT
"""

from unittest import TestCase, main
from optparse import OptionValueError

from lark import Tree, Token
import numpy as np

from tests.integration.offload.unittest_functions import (
    build_current_options,
    get_default_test_user,
)

from gluent import OffloadOperation
from goe.offload.factory.backend_table_factory import backend_table_factory
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_messages import OffloadMessages
from goe.persistence.orchestration_metadata import OrchestrationMetadata
from goe.offload.predicate_offload import GenericPredicate, parse_predicate_dsl
from goe.offload.bigquery import bigquery_predicate
from goe.offload.hadoop import hadoop_predicate
from goe.offload.microsoft import synapse_predicate
from goe.offload.oracle import oracle_predicate
from goe.offload.snowflake import snowflake_predicate
from goe.offload.teradata import teradata_predicate
from tests.testlib.test_framework.factory.backend_testing_api_factory import (
    backend_testing_api_factory,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)


class TestIdaPredicateRenderToSQL(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateRenderToSQL, self).__init__(*args, **kwargs)
        self.schema = get_default_test_user()
        self.hybrid_schema = get_default_test_user(hybrid=True)
        self.be_test_api = None
        self.fe_test_api = None

    def setUp(self):
        self.options = build_current_options()
        messages = OffloadMessages()

        channels_table_name = "CHANNELS"
        self.channels_rdbms_table = OffloadSourceTable.create(
            self.schema, channels_table_name, self.options, messages, dry_run=True
        )

        sales_table_name = "SALES"
        operation = OffloadOperation.from_dict(
            {"owner_table": "%s.%s" % (self.schema, sales_table_name)},
            self.options,
            messages,
        )
        metadata = OrchestrationMetadata.from_name(
            self.hybrid_schema,
            sales_table_name,
            connection_options=self.options,
            messages=messages,
        )
        self.sales_backend_table = backend_table_factory(
            metadata.backend_owner,
            metadata.backend_table,
            self.options.target,
            self.options,
            messages,
            operation,
            dry_run=True,
        )
        self.sales_rdbms_table = OffloadSourceTable.create(
            self.schema, sales_table_name, self.options, messages, dry_run=True
        )

        operation.defaults_for_existing_table(
            self.options, self.sales_rdbms_table.get_frontend_api()
        )
        self.sales_backend_table.refresh_operational_settings(
            offload_operation=operation
        )

        self.customers_table_name = "CUSTOMERS"
        operation = OffloadOperation.from_dict(
            {"owner_table": "%s.%s" % (self.schema, self.customers_table_name)},
            self.options,
            messages,
        )
        metadata = OrchestrationMetadata.from_name(
            self.hybrid_schema,
            self.customers_table_name,
            connection_options=self.options,
            messages=messages,
        )
        self.customers_rdbms_table = OffloadSourceTable.create(
            self.schema, self.customers_table_name, self.options, messages, dry_run=True
        )
        self.customers_backend_table = backend_table_factory(
            metadata.backend_owner,
            metadata.backend_table,
            self.options.target,
            self.options,
            messages,
            operation,
            dry_run=True,
        )

        self.be_test_api = backend_testing_api_factory(
            self.options.target, self.options, messages, dry_run=True
        )
        self.fe_test_api = frontend_testing_api_factory(
            self.options.db_type, self.options, messages, dry_run=True
        )

    def test_frontend_sql_generation(self):
        channels_expect_equal = self.fe_test_api.expected_channels_offload_predicates()

        for predicate_dsl, expected_sql in channels_expect_equal:
            self.assertEqual(
                self.channels_rdbms_table.predicate_to_where_clause(
                    GenericPredicate(predicate_dsl)
                ),
                expected_sql,
            )

        sales_expect_equal = self.fe_test_api.expected_sales_offload_predicates()

        for (
            predicate_dsl,
            expected_sql,
            expected_bind_sql,
            expected_binds,
        ) in sales_expect_equal:
            self.assertEqual(
                self.sales_rdbms_table.predicate_to_where_clause(
                    GenericPredicate(predicate_dsl)
                ),
                expected_sql,
            )
            if expected_bind_sql or expected_binds:
                (
                    where_clause,
                    binds,
                ) = self.sales_rdbms_table.predicate_to_where_clause_with_binds(
                    GenericPredicate(predicate_dsl)
                )
                self.assertEqual(where_clause, expected_bind_sql)
                if expected_binds:
                    for p in binds:
                        self.assertIn(p.param_name, expected_binds)
                        self.assertEqual(p.param_value, expected_binds[p.param_name])

    def test_backend_sql_generation(self):
        customers_expect_equal = (
            self.be_test_api.expected_customers_offload_predicates()
        )
        for predicate_dsl, expected_sql in customers_expect_equal:
            backend_predicate = self.customers_backend_table.predicate_to_where_clause(
                GenericPredicate(predicate_dsl)
            )
            self.assertEqual(backend_predicate, expected_sql)

    def test_backend_synthetic_partition_sql_generation(self):
        def source_to_canonical_mappings(
            backend_columns,
            backend_table,
            canonical_overrides,
            char_semantics_overrides,
            detect_sizes=False,
            max_rdbms_time_scale=None,
        ):
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
                _
                for _ in canonical_columns
                if _.name in char_semantics_overrides.keys()
            ]:
                cs_col.char_semantics = char_semantics_overrides[cs_col.name]
                cs_col.from_override = True

            return canonical_columns

        def fake_customers_backend_table(operation, metadata, messages):
            # This is a bit hacky but allows us to test synthetic part cols. The hacks:
            # 1) We don't pass reset_backend_table=True above, ordinarily we would in order to change partition info
            # 2) We use defaults_for_fresh_offload() below even though we're not doing a reset, this is so we can get
            #    fresh synthetic column info rather than picking up defaults from the existing table.
            # 3) We map backend cols to canonical and then generate synthetic partition columns again based on operation
            customers_backend_table = backend_table_factory(
                metadata.backend_owner,
                metadata.backend_table,
                self.options.target,
                self.options,
                messages,
                operation,
                dry_run=True,
            )
            operation.defaults_for_fresh_offload(
                self.customers_rdbms_table,
                self.options,
                messages,
                customers_backend_table,
            )
            canonical_columns = source_to_canonical_mappings(
                customers_backend_table.get_non_synthetic_columns(),
                customers_backend_table,
                [],
                {},
            )
            canonical_columns = operation.set_partition_info_on_canonical_columns(
                canonical_columns,
                self.customers_rdbms_table.columns,
                customers_backend_table,
            )
            backend_columns = (
                customers_backend_table.convert_canonical_columns_to_backend(
                    canonical_columns
                )
            )
            customers_backend_table.set_columns(backend_columns)
            customers_backend_table.refresh_operational_settings(
                operation, self.customers_rdbms_table.columns
            )
            return customers_backend_table

        messages = OffloadMessages()
        metadata = OrchestrationMetadata.from_name(
            self.hybrid_schema,
            self.customers_table_name,
            connection_options=self.options,
            messages=messages,
        )

        expect_equal_by_synth_part = (
            self.be_test_api.expected_customers_synthetic_offload_predicates()
        )

        for synth_part, expect_equal in expect_equal_by_synth_part:
            partition_column, granularity, digits = synth_part

            operation = OffloadOperation.from_dict(
                {
                    "owner_table": "%s.%s" % (self.schema, self.customers_table_name),
                    "offload_partition_columns": partition_column,
                    "offload_partition_granularity": granularity,
                    "synthetic_partition_digits": digits,
                },
                self.options,
                messages,
            )

            customers_backend_table = fake_customers_backend_table(
                operation, metadata, messages
            )

            for predicate_dsl, expected_sql in expect_equal:
                backend_sql = customers_backend_table.predicate_to_where_clause(
                    GenericPredicate(predicate_dsl)
                )
                self.assertEqual(backend_sql, expected_sql)


class TestIdaPredicateDataTypes(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateDataTypes, self).__init__(*args, **kwargs)
        self.schema = get_default_test_user()
        self.hybrid_schema = get_default_test_user(hybrid=True)

    def setUp(self):
        self.options = build_current_options()

        customers_table_name = "CUSTOMERS"
        messages = OffloadMessages()
        operation = OffloadOperation.from_dict(
            {"owner_table": "%s.%s" % (self.schema, customers_table_name)},
            self.options,
            messages,
        )
        metadata = OrchestrationMetadata.from_name(
            self.hybrid_schema,
            customers_table_name,
            connection_options=self.options,
            messages=messages,
        )
        self.customers_backend_table = backend_table_factory(
            metadata.backend_owner,
            metadata.backend_table,
            self.options.target,
            self.options,
            messages,
            operation,
            dry_run=True,
        )
        self.customers_rdbms_table = OffloadSourceTable.create(
            self.schema, customers_table_name, self.options, messages, dry_run=True
        )

    def test_data_type_errors(self):
        expect_data_type_error = [
            "column(CUST_CITY) = numeric(34)",
            "column(CUST_CITY) = datetime(2012-01-01)",
            "column(CUST_CITY) = datetime(2012-01-01 00:00:00.123456789)",
            "column(CUST_CITY) IN (numeric(34))",
            "column(CUST_CITY) NOT IN (numeric(34))",
            'column(CUST_CITY) IN (string("NYC"), numeric(34))',
            "numeric(34) > column(CUST_CITY)",
            "column(CUST_ID) = datetime(2020-12-30)",
            'column(CUST_ID) = string("2020-12-30")',
            'column(CUST_CREDIT_LIMIT) < string("nan")',
            "column(CUST_EFF_TO) = numeric(34)",
            'column(CUST_EFF_TO) = string("34")',
        ]

        for predicate_dsl in expect_data_type_error:
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                oracle_predicate.predicate_to_where_clause,
                self.customers_rdbms_table.columns,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                hadoop_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                bigquery_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                snowflake_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                synapse_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                teradata_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )


if __name__ == "__main__":
    main()
