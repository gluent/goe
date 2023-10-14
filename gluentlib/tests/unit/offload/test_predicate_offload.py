#! /usr/bin/env python3
"""
    Offload predicate test code.
    LICENSE_TEXT
"""

from unittest import TestCase, main
from optparse import OptionValueError

from lark import Tree, Token
import numpy as np

from tests.offload.unittest_functions import build_current_options, get_default_test_user

from gluent import OffloadOperation
from gluentlib.offload.factory.backend_table_factory import backend_table_factory
from gluentlib.offload.factory.offload_source_table_factory import OffloadSourceTable
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.persistence.orchestration_metadata import OrchestrationMetadata
from gluentlib.offload.predicate_offload import GenericPredicate, parse_predicate_dsl
from gluentlib.offload.bigquery import bigquery_predicate
from gluentlib.offload.hadoop import hadoop_predicate
from gluentlib.offload.microsoft import synapse_predicate
from gluentlib.offload.oracle import oracle_predicate
from gluentlib.offload.snowflake import snowflake_predicate
from gluentlib.offload.teradata import teradata_predicate
from testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory
from testlib.test_framework.factory.frontend_testing_api_factory import frontend_testing_api_factory


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

        channels_table_name = 'CHANNELS'
        self.channels_rdbms_table = OffloadSourceTable.create(self.schema, channels_table_name,
                                                              self.options, messages, dry_run=True)

        sales_table_name = 'SALES'
        operation = OffloadOperation.from_dict({'owner_table': '%s.%s' % (self.schema, sales_table_name)},
                                               self.options, messages)
        metadata = OrchestrationMetadata.from_name(self.hybrid_schema, sales_table_name,
                                                   connection_options=self.options, messages=messages)
        self.sales_backend_table = backend_table_factory(metadata.backend_owner, metadata.backend_table,
                                                         self.options.target, self.options, messages,
                                                         operation, dry_run=True)
        self.sales_rdbms_table = OffloadSourceTable.create(self.schema, sales_table_name, self.options,
                                                           messages, dry_run=True)

        operation.defaults_for_existing_table(self.options, self.sales_rdbms_table.get_frontend_api())
        self.sales_backend_table.refresh_operational_settings(offload_operation=operation)

        self.customers_table_name = 'CUSTOMERS'
        operation = OffloadOperation.from_dict({'owner_table': '%s.%s' % (self.schema, self.customers_table_name)},
                                               self.options, messages)
        metadata = OrchestrationMetadata.from_name(self.hybrid_schema, self.customers_table_name,
                                                   connection_options=self.options, messages=messages)
        self.customers_rdbms_table = OffloadSourceTable.create(self.schema, self.customers_table_name,
                                                               self.options, messages, dry_run=True)
        self.customers_backend_table = backend_table_factory(metadata.backend_owner, metadata.backend_table,
                                                             self.options.target, self.options, messages,
                                                             operation, dry_run=True)

        self.be_test_api = backend_testing_api_factory(self.options.target, self.options, messages, dry_run=True)
        self.fe_test_api = frontend_testing_api_factory(self.options.db_type, self.options, messages, dry_run=True)

    def test_frontend_sql_generation(self):
        channels_expect_equal = self.fe_test_api.expected_channels_offload_predicates()

        for predicate_dsl, expected_sql in channels_expect_equal:
            self.assertEqual(
                self.channels_rdbms_table.predicate_to_where_clause(GenericPredicate(predicate_dsl)),
                expected_sql
            )

        sales_expect_equal = self.fe_test_api.expected_sales_offload_predicates()

        for predicate_dsl, expected_sql, expected_bind_sql, expected_binds in sales_expect_equal:
            self.assertEqual(
                self.sales_rdbms_table.predicate_to_where_clause(GenericPredicate(predicate_dsl)),
                expected_sql
            )
            if expected_bind_sql or expected_binds:
                where_clause, binds = self.sales_rdbms_table.predicate_to_where_clause_with_binds(GenericPredicate(predicate_dsl))
                self.assertEqual(where_clause, expected_bind_sql)
                if expected_binds:
                    for p in binds:
                        self.assertIn(p.param_name, expected_binds)
                        self.assertEqual(p.param_value, expected_binds[p.param_name])

    def test_backend_sql_generation(self):
        customers_expect_equal = self.be_test_api.expected_customers_offload_predicates()
        for predicate_dsl, expected_sql in customers_expect_equal:
            backend_predicate = self.customers_backend_table.predicate_to_where_clause(GenericPredicate(predicate_dsl))
            self.assertEqual(backend_predicate, expected_sql)

    def test_backend_synthetic_partition_sql_generation(self):
        def source_to_canonical_mappings(backend_columns, backend_table, canonical_overrides, char_semantics_overrides,
                                         detect_sizes=False, max_rdbms_time_scale=None):
            canonical_columns = []
            for tab_col in backend_columns:
              new_col = backend_table.to_canonical_column_with_overrides(tab_col, canonical_overrides,
                                                                         detect_sizes=detect_sizes,
                                                                         max_rdbms_time_scale=max_rdbms_time_scale)
              canonical_columns.append(new_col)
            # Process any char semantics overrides
            for cs_col in [_ for _ in canonical_columns if _.name in char_semantics_overrides.keys()]:
                cs_col.char_semantics = char_semantics_overrides[cs_col.name]
                cs_col.from_override = True

            return canonical_columns

        def fake_customers_backend_table(operation, metadata, messages):
            # This is a bit hacky but allows us to test synthetic part cols. The hacks:
            # 1) We don't pass reset_backend_table=True above, ordinarily we would in order to change partition info
            # 2) We use defaults_for_fresh_offload() below even though we're not doing a reset, this is so we can get
            #    fresh synthetic column info rather than picking up defaults from the existing table.
            # 3) We map backend cols to canonical and then generate synthetic partition columns again based on operation
            customers_backend_table = backend_table_factory(metadata.backend_owner, metadata.backend_table, self.options.target,
                                                            self.options, messages, operation, dry_run=True)
            operation.defaults_for_fresh_offload(self.customers_rdbms_table, self.options, messages, customers_backend_table)
            canonical_columns = source_to_canonical_mappings(customers_backend_table.get_non_synthetic_columns(),
                                                             customers_backend_table, [], {})
            canonical_columns = operation.set_partition_info_on_canonical_columns(canonical_columns,
                                                                                  self.customers_rdbms_table.columns,
                                                                                  customers_backend_table)
            backend_columns = customers_backend_table.convert_canonical_columns_to_backend(canonical_columns)
            customers_backend_table.set_columns(backend_columns)
            customers_backend_table.refresh_operational_settings(operation, self.customers_rdbms_table.columns)
            return customers_backend_table

        messages = OffloadMessages()
        metadata = OrchestrationMetadata.from_name(self.hybrid_schema, self.customers_table_name,
                                                   connection_options=self.options, messages=messages)

        expect_equal_by_synth_part = self.be_test_api.expected_customers_synthetic_offload_predicates()

        for synth_part, expect_equal in expect_equal_by_synth_part:
            partition_column, granularity, digits = synth_part

            operation = OffloadOperation.from_dict({'owner_table': '%s.%s' % (self.schema, self.customers_table_name),
                                                    'offload_partition_columns': partition_column,
                                                    'offload_partition_granularity': granularity,
                                                    'synthetic_partition_digits': digits}, self.options, messages)

            customers_backend_table = fake_customers_backend_table(operation, metadata, messages)

            for predicate_dsl, expected_sql in expect_equal:
                backend_sql = customers_backend_table.predicate_to_where_clause(GenericPredicate(predicate_dsl))
                self.assertEqual(backend_sql, expected_sql)


class TestIdaPredicateParse(TestCase):
    def _test_parse_single_numeric(self, dsl_value_str, ast_token_values):
        self.assertEqual(
            parse_predicate_dsl('numeric({})'.format(dsl_value_str), top_level_node='value'),
            Tree('numeric_value', [Token(*ast_token_values)])
        )

    def test_parse_numeric(self):
        self._test_parse_single_numeric('3.141', ('SIGNED_DECIMAL', float(3.141)))
        self._test_parse_single_numeric('+3.141', ('SIGNED_DECIMAL', float(3.141)))
        self._test_parse_single_numeric('-3.141', ('SIGNED_DECIMAL', float(-3.141)))
        self._test_parse_single_numeric(' 3.141', ('SIGNED_DECIMAL', float(3.141)))
        self._test_parse_single_numeric(' 3.141 ', ('SIGNED_DECIMAL', float(3.141)))
        self._test_parse_single_numeric('3.', ('SIGNED_DECIMAL', float(3)))

        self._test_parse_single_numeric('3', ('SIGNED_INTEGER', int(3)))
        self._test_parse_single_numeric('+3', ('SIGNED_INTEGER', int(3)))
        self._test_parse_single_numeric('-3', ('SIGNED_INTEGER', int(-3)))

        self._test_parse_single_numeric(' 3', ('SIGNED_INTEGER', int(3)))
        self._test_parse_single_numeric('3 ', ('SIGNED_INTEGER', int(3)))
        self._test_parse_single_numeric(' 3 ', ('SIGNED_INTEGER', int(3)))

    def _test_parse_single_datetime(self, dsl_value_str, ast_token_values):
        self.assertEqual(
            parse_predicate_dsl('datetime({})'.format(dsl_value_str), top_level_node='value'),
            Tree('datetime_value', [Token(*ast_token_values)])
        )

    def test_parse_datetime(self):
        self._test_parse_single_datetime('2012-01-01', ('DATE', np.datetime64('2012-01-01')))
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-0', None)
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-0101', None)
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, '', None)

        # below literal value correctness testing is out of scope
        # self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-00', None)
        # self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-00-01', None)

        self._test_parse_single_datetime('2001-01-01 12:00:01', ('TIMESTAMP', np.datetime64('2001-01-01 12:00:01')))
        self._test_parse_single_datetime('2001-01-01 06:00:01', ('TIMESTAMP', np.datetime64('2001-01-01 06:00:01')))
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-01 6:00:01', ('TIMESTAMP', np.datetime64('2012-01-01 06:00:01')))

        self._test_parse_single_datetime('2001-01-01 12:00:01.050', ('TIMESTAMP_FRACTIONAL', np.datetime64('2001-01-01 12:00:01.050')))
        self._test_parse_single_datetime('2001-01-01 06:00:01.000090', ('TIMESTAMP_FRACTIONAL', np.datetime64('2001-01-01 06:00:01.000090')))
        self._test_parse_single_datetime('2001-01-01 06:00:01.000000123', ('TIMESTAMP_FRACTIONAL', np.datetime64('2001-01-01 06:00:01.000000123')))
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-01 06:00:01.', ('TIMESTAMP_FRACTIONAL', np.datetime64('2012-01-01 06:00:01.000')))
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-01 06:00:01.0000000000', ('TIMESTAMP_FRACTIONAL', np.datetime64('2012-01-01 06:00:01.000')))

        # below literal value correctness testing is out of scope
        # self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-01 25:00:01', None)

    def _test_parse_single_string(self, dsl_value_str, ast_value_str=None):
        ast_value_str = dsl_value_str if ast_value_str is None else ast_value_str
        self.assertEqual(
            parse_predicate_dsl('string("{}")'.format(dsl_value_str), top_level_node='value'),
            Tree('string_value', [Token('ESCAPED_STRING', ast_value_str.replace('\\"', '"'))])
        )

    def test_parse_string(self):
        self._test_parse_single_string('test string')
        self._test_parse_single_string('test\' string')
        self._test_parse_single_string('test \\" string')
        self._test_parse_single_string('')
        self._test_parse_single_string('`~!@#$%^&*()_+-={}[]|\:;?/>.<,0987654321')
        self._test_parse_single_string('\u00f6')

    def test_column_names(self):
        expect_col_names = [
            ('column(baz) = numeric(1)', ['BAZ']),
            ('column(sh.baz) = numeric(1)', ['BAZ']),
            ('(column(baz) = numeric(1)) and (column(baz) != numeric(1))', ['BAZ']),
            ('(column(baz) = numeric(1)) and (column(bar) != numeric(1))', ['BAR', 'BAZ']),
            ('(column(baz) = numeric(1)) and ((column(zip) = string("hey")) or (column(bar) != numeric(1)))', ['BAR', 'BAZ', 'ZIP']),
            ('column(_baz_123) = numeric(1)', ['_BAZ_123']),
        ]
        for dsl, col_names in expect_col_names:
            self.assertEqual(
                set(GenericPredicate(dsl).column_names()),
                set(col_names)
            )

    def test_parse_errors(self):
        self.assertRaises(OptionValueError, parse_predicate_dsl, '')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'column(hi)')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'column(hi) >')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'column(hi) > numeric()')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'column(hi) > numeric(+-23)')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'column(hi) == numeric(23)')
        self.assertRaises(OptionValueError, parse_predicate_dsl, '(column(hi) = numeric(23)')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'Column(hi) = numeric(23)')
        self.assertRaises(OptionValueError, parse_predicate_dsl, 'column(hi) = column(there)')


    def test_parse_complex_dsl(self):
        parse_dsl = [
            'column(YEAR) < numeric(2012) OR (column(YEAR) = numeric(2012) AND column(MONTH) < numeric(6))',
            '(column(YEAR) < numeric(2012) OR (column(YEAR) = numeric(2012) AND column(MONTH) < numeric(6)))',
            '((column(YEAR) < numeric(2012)) OR ((column(YEAR) = numeric(2012)) AND (column(MONTH) < numeric(6))))',
            '(((column(YEAR) < numeric(2012)) ' +
            'OR ((column(YEAR) = numeric(2012)) AND (column(MONTH) < numeric(6)))) ' +
            'OR (((column(YEAR) = numeric(2012)) AND (column(MONTH) = numeric(6))) AND (column(DAY) < numeric(30))))'
        ]

        for predicate_dsl in parse_dsl:
            GenericPredicate(predicate_dsl)


class TestIdaPredicateDataTypes(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateDataTypes, self).__init__(*args, **kwargs)
        self.schema = get_default_test_user()
        self.hybrid_schema = get_default_test_user(hybrid=True)

    def setUp(self):
        self.options = build_current_options()

        customers_table_name = 'CUSTOMERS'
        messages = OffloadMessages()
        operation = OffloadOperation.from_dict({'owner_table': '%s.%s' % (self.schema, customers_table_name)},
                                               self.options, messages)
        metadata = OrchestrationMetadata.from_name(self.hybrid_schema, customers_table_name,
                                                   connection_options=self.options, messages=messages)
        self.customers_backend_table = backend_table_factory(metadata.backend_owner, metadata.backend_table, self.options.target,
                                                             self.options, messages, operation, dry_run=True)
        self.customers_rdbms_table = OffloadSourceTable.create(self.schema, customers_table_name, self.options,
                                                               messages, dry_run=True)

    def test_data_type_errors(self):
        expect_data_type_error = [
            'column(CUST_CITY) = numeric(34)',
            'column(CUST_CITY) = datetime(2012-01-01)',
            'column(CUST_CITY) = datetime(2012-01-01 00:00:00.123456789)',
            'column(CUST_CITY) IN (numeric(34))',
            'column(CUST_CITY) NOT IN (numeric(34))',
            'column(CUST_CITY) IN (string("NYC"), numeric(34))',
            'numeric(34) > column(CUST_CITY)',
            'column(CUST_ID) = datetime(2020-12-30)',
            'column(CUST_ID) = string("2020-12-30")',
            'column(CUST_CREDIT_LIMIT) < string("nan")',
            'column(CUST_EFF_TO) = numeric(34)',
            'column(CUST_EFF_TO) = string("34")',
        ]

        for predicate_dsl in expect_data_type_error:
            self.assertRaisesRegex(
                OptionValueError, 'cannot be compared to',
                oracle_predicate.predicate_to_where_clause,
                self.customers_rdbms_table.columns,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError, 'cannot be compared to',
                hadoop_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError, 'cannot be compared to',
                bigquery_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError, 'cannot be compared to',
                snowflake_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError, 'cannot be compared to',
                synapse_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError, 'cannot be compared to',
                teradata_predicate.predicate_to_where_clause,
                self.customers_backend_table.get_columns(),
                GenericPredicate(predicate_dsl),
            )


class TestIdaPredicateMethods(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateMethods, self).__init__(*args, **kwargs)

    def test_rename_column(self):
        pred = GenericPredicate('column(original_name) IS NOT NULL')
        self.assertEqual(pred.column_names(), ['ORIGINAL_NAME'])
        pred.rename_column('original_name', 'new_name')
        self.assertEqual(pred.column_names(), ['NEW_NAME'])
        self.assertEqual(pred.dsl, 'column(NEW_NAME) IS NOT NULL')

        pred = GenericPredicate('column(original_name) = numeric(3)')
        self.assertEqual(pred.column_names(), ['ORIGINAL_NAME'])
        pred.rename_column('original_name', 'new_name')
        self.assertEqual(pred.column_names(), ['NEW_NAME'])
        self.assertEqual(pred.dsl, 'column(NEW_NAME) = numeric(3)')

        dsl = '((column(original_name) = numeric(3) AND column(original_name) != datetime(2001-01-01)) OR (column(new_name) IN (numeric(7)) OR column(other_name) NOT IN (numeric(12))))'
        pred = GenericPredicate(dsl)
        self.assertEqual(set(pred.column_names()), set(['ORIGINAL_NAME', 'NEW_NAME', 'OTHER_NAME']))
        pred.rename_column('original_name', 'new_name')
        self.assertEqual(set(pred.column_names()), set(['NEW_NAME', 'OTHER_NAME']))
        self.assertEqual(pred.ast, GenericPredicate(pred.dsl.replace('ORIGINAL_NAME', 'NEW_NAME')).ast)
        self.assertEqual(pred.dsl, dsl.replace('original_name', 'NEW_NAME').replace('new_name', 'NEW_NAME').replace('other_name', 'OTHER_NAME'))

    def test_set_column_alias(self):
        pred = GenericPredicate('column(col) = numeric(1)')
        pred.set_column_alias('test_alias')
        self.assertEqual(pred.column_names(), ['COL'])
        self.assertEqual(pred.alias_column_names(), [('TEST_ALIAS', 'COL')])

        pred = GenericPredicate('column(alias.col) = numeric(1)')
        self.assertEqual(pred.alias_column_names(), [('ALIAS', 'COL')])
        pred.set_column_alias('test_alias')
        self.assertEqual(pred.alias_column_names(), [('TEST_ALIAS', 'COL')])

        pred = GenericPredicate('column(alias.col) IS NULL')
        self.assertEqual(pred.alias_column_names(), [('ALIAS', 'COL')])
        pred.set_column_alias('test_alias')
        self.assertEqual(pred.alias_column_names(), [('TEST_ALIAS', 'COL')])

        pred = GenericPredicate('column(alias.col) = numeric(1)')
        self.assertEqual(pred.alias_column_names(), [('ALIAS', 'COL')])
        pred.set_column_alias(None)
        self.assertEqual(pred.alias_column_names(), [(None, 'COL')])


class TestIdaPredicateRenderToDSL(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateRenderToDSL, self).__init__(*args, **kwargs)

    def test_render_dsl(self):
        expect_dsl = [
            ('column(col) = numeric(1)', 'column(COL) = numeric(1)'),
            ('column(col) = numeric(1.1)', 'column(COL) = numeric(1.1)'),
            ('column(alias.column) IN (string("hi there"))', 'column(ALIAS.COLUMN) IN (string("hi there"))'),
            ('column(COLUMN) IN (string("1"), string("2"))', 'column(COLUMN) IN (string("1"), string("2"))'),
            ('(column(c1) not in (numeric(1))) and ((column(c2) <= string("c2")) or (column(c3) IS NOT NULL))', '(column(C1) NOT IN (numeric(1)) AND (column(C2) <= string("c2") OR column(C3) IS NOT NULL))'),
        ]
        for input_dsl, render_dsl in expect_dsl:
            self.assertEqual(GenericPredicate(input_dsl).dsl, render_dsl)


if __name__ == '__main__':
    main()
