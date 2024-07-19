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

from unittest import TestCase, main
from optparse import OptionValueError

from lark import Tree, Token
import numpy as np

from goe.offload.predicate_offload import GenericPredicate, parse_predicate_dsl
from goe.offload.bigquery import bigquery_column, bigquery_predicate
from goe.offload.hadoop import hadoop_column, hadoop_predicate
from goe.offload.microsoft import synapse_column, synapse_predicate
from goe.offload.oracle import oracle_column, oracle_predicate
from goe.offload.snowflake import snowflake_column, snowflake_predicate
from goe.offload.teradata import teradata_column, teradata_predicate


BIGQUERY_COLUMNS = [
    bigquery_column.BigQueryColumn("COL_INT", bigquery_column.BIGQUERY_TYPE_INT64),
    bigquery_column.BigQueryColumn("COL_STRING", bigquery_column.BIGQUERY_TYPE_STRING),
    bigquery_column.BigQueryColumn("COL_DATE", bigquery_column.BIGQUERY_TYPE_DATETIME),
    bigquery_column.BigQueryColumn(
        "COL_DECIMAL",
        bigquery_column.BIGQUERY_TYPE_NUMERIC,
        data_precision=10,
        data_scale=2,
    ),
]
HADOOP_COLUMNS = [
    hadoop_column.HadoopColumn("COL_INT", hadoop_column.HADOOP_TYPE_INT),
    hadoop_column.HadoopColumn("COL_STRING", hadoop_column.HADOOP_TYPE_STRING),
    hadoop_column.HadoopColumn("COL_DATE", hadoop_column.HADOOP_TYPE_TIMESTAMP),
    hadoop_column.HadoopColumn(
        "COL_DECIMAL",
        hadoop_column.HADOOP_TYPE_DECIMAL,
        data_precision=10,
        data_scale=2,
    ),
]
ORACLE_COLUMNS = [
    oracle_column.OracleColumn(
        "COL_INT", oracle_column.ORACLE_TYPE_NUMBER, data_precision=5, data_scale=0
    ),
    oracle_column.OracleColumn("COL_STRING", oracle_column.ORACLE_TYPE_VARCHAR2),
    oracle_column.OracleColumn("COL_DATE", oracle_column.ORACLE_TYPE_DATE),
    oracle_column.OracleColumn(
        "COL_DECIMAL", oracle_column.ORACLE_TYPE_NUMBER, data_precision=10, data_scale=2
    ),
]
SNOWFLAKE_COLUMNS = [
    snowflake_column.SnowflakeColumn(
        "COL_INT",
        snowflake_column.SNOWFLAKE_TYPE_NUMBER,
        data_precision=5,
        data_scale=0,
    ),
    snowflake_column.SnowflakeColumn(
        "COL_STRING", snowflake_column.SNOWFLAKE_TYPE_TEXT
    ),
    snowflake_column.SnowflakeColumn(
        "COL_DATE", snowflake_column.SNOWFLAKE_TYPE_TIMESTAMP_NTZ
    ),
    snowflake_column.SnowflakeColumn(
        "COL_DECIMAL",
        snowflake_column.SNOWFLAKE_TYPE_NUMBER,
        data_precision=10,
        data_scale=2,
    ),
]
SYNAPSE_COLUMNS = [
    synapse_column.SynapseColumn("COL_INT", synapse_column.SYNAPSE_TYPE_BIGINT),
    synapse_column.SynapseColumn("COL_STRING", synapse_column.SYNAPSE_TYPE_VARCHAR),
    synapse_column.SynapseColumn("COL_DATE", synapse_column.SYNAPSE_TYPE_DATETIME2),
    synapse_column.SynapseColumn(
        "COL_DECIMAL",
        synapse_column.SYNAPSE_TYPE_DECIMAL,
        data_precision=10,
        data_scale=2,
    ),
]
TERADATA_COLUMNS = [
    teradata_column.TeradataColumn(
        "COL_INT", teradata_column.TERADATA_TYPE_NUMBER, data_precision=5, data_scale=0
    ),
    teradata_column.TeradataColumn("COL_STRING", teradata_column.TERADATA_TYPE_VARCHAR),
    teradata_column.TeradataColumn("COL_DATE", teradata_column.TERADATA_TYPE_DATE),
    teradata_column.TeradataColumn(
        "COL_DECIMAL",
        teradata_column.TERADATA_TYPE_NUMBER,
        data_precision=10,
        data_scale=2,
    ),
]


class TestIdaPredicateParse(TestCase):
    def _test_parse_single_numeric(self, dsl_value_str, ast_token_values):
        self.assertEqual(
            parse_predicate_dsl(
                "numeric({})".format(dsl_value_str), top_level_node="value"
            ),
            Tree("numeric_value", [Token(*ast_token_values)]),
        )

    def test_parse_numeric(self):
        self._test_parse_single_numeric("3.141", ("SIGNED_DECIMAL", float(3.141)))
        self._test_parse_single_numeric("+3.141", ("SIGNED_DECIMAL", float(3.141)))
        self._test_parse_single_numeric("-3.141", ("SIGNED_DECIMAL", float(-3.141)))
        self._test_parse_single_numeric(" 3.141", ("SIGNED_DECIMAL", float(3.141)))
        self._test_parse_single_numeric(" 3.141 ", ("SIGNED_DECIMAL", float(3.141)))
        self._test_parse_single_numeric("3.", ("SIGNED_DECIMAL", float(3)))

        self._test_parse_single_numeric("3", ("SIGNED_INTEGER", int(3)))
        self._test_parse_single_numeric("+3", ("SIGNED_INTEGER", int(3)))
        self._test_parse_single_numeric("-3", ("SIGNED_INTEGER", int(-3)))

        self._test_parse_single_numeric(" 3", ("SIGNED_INTEGER", int(3)))
        self._test_parse_single_numeric("3 ", ("SIGNED_INTEGER", int(3)))
        self._test_parse_single_numeric(" 3 ", ("SIGNED_INTEGER", int(3)))

    def _test_parse_single_datetime(self, dsl_value_str, ast_token_values):
        self.assertEqual(
            parse_predicate_dsl(
                "datetime({})".format(dsl_value_str), top_level_node="value"
            ),
            Tree("datetime_value", [Token(*ast_token_values)]),
        )

    def test_parse_datetime(self):
        self._test_parse_single_datetime(
            "2012-01-01", ("DATE", np.datetime64("2012-01-01"))
        )
        self.assertRaises(
            OptionValueError, self._test_parse_single_datetime, "2012-01-0", None
        )
        self.assertRaises(
            OptionValueError, self._test_parse_single_datetime, "2012-0101", None
        )
        self.assertRaises(OptionValueError, self._test_parse_single_datetime, "", None)

        # below literal value correctness testing is out of scope
        # self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-00', None)
        # self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-00-01', None)

        self._test_parse_single_datetime(
            "2001-01-01 12:00:01", ("TIMESTAMP", np.datetime64("2001-01-01 12:00:01"))
        )
        self._test_parse_single_datetime(
            "2001-01-01 06:00:01", ("TIMESTAMP", np.datetime64("2001-01-01 06:00:01"))
        )
        self.assertRaises(
            OptionValueError,
            self._test_parse_single_datetime,
            "2012-01-01 6:00:01",
            ("TIMESTAMP", np.datetime64("2012-01-01 06:00:01")),
        )

        self._test_parse_single_datetime(
            "2001-01-01 12:00:01.050",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2001-01-01 12:00:01.050")),
        )
        self._test_parse_single_datetime(
            "2001-01-01 06:00:01.000090",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2001-01-01 06:00:01.000090")),
        )
        self._test_parse_single_datetime(
            "2001-01-01 06:00:01.000000123",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2001-01-01 06:00:01.000000123")),
        )
        self.assertRaises(
            OptionValueError,
            self._test_parse_single_datetime,
            "2012-01-01 06:00:01.",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2012-01-01 06:00:01.000")),
        )
        self.assertRaises(
            OptionValueError,
            self._test_parse_single_datetime,
            "2012-01-01 06:00:01.0000000000",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2012-01-01 06:00:01.000")),
        )

        # below literal value correctness testing is out of scope
        # self.assertRaises(OptionValueError, self._test_parse_single_datetime, '2012-01-01 25:00:01', None)

    def _test_parse_single_string(self, dsl_value_str, ast_value_str=None):
        ast_value_str = dsl_value_str if ast_value_str is None else ast_value_str
        self.assertEqual(
            parse_predicate_dsl(
                'string("{}")'.format(dsl_value_str), top_level_node="value"
            ),
            Tree(
                "string_value",
                [Token("ESCAPED_STRING", ast_value_str.replace('\\"', '"'))],
            ),
        )

    def test_parse_string(self):
        self._test_parse_single_string("test string")
        self._test_parse_single_string("test' string")
        self._test_parse_single_string('test \\" string')
        self._test_parse_single_string("")
        self._test_parse_single_string("`~!@#$%^&*()_+-={}[]|:;?/>.<,0987654321")
        self._test_parse_single_string("\u00f6")

    def test_column_names(self):
        expect_col_names = [
            ("column(baz) = numeric(1)", ["BAZ"]),
            ("column(sh.baz) = numeric(1)", ["BAZ"]),
            ("(column(baz) = numeric(1)) and (column(baz) != numeric(1))", ["BAZ"]),
            (
                "(column(baz) = numeric(1)) and (column(bar) != numeric(1))",
                ["BAR", "BAZ"],
            ),
            (
                '(column(baz) = numeric(1)) and ((column(zip) = string("hey")) or (column(bar) != numeric(1)))',
                ["BAR", "BAZ", "ZIP"],
            ),
            ("column(_baz_123) = numeric(1)", ["_BAZ_123"]),
        ]
        for dsl, col_names in expect_col_names:
            self.assertEqual(set(GenericPredicate(dsl).column_names()), set(col_names))

    def test_parse_errors(self):
        self.assertRaises(OptionValueError, parse_predicate_dsl, "")
        self.assertRaises(OptionValueError, parse_predicate_dsl, "column(hi)")
        self.assertRaises(OptionValueError, parse_predicate_dsl, "column(hi) >")
        self.assertRaises(
            OptionValueError, parse_predicate_dsl, "column(hi) > numeric()"
        )
        self.assertRaises(
            OptionValueError, parse_predicate_dsl, "column(hi) > numeric(+-23)"
        )
        self.assertRaises(
            OptionValueError, parse_predicate_dsl, "column(hi) == numeric(23)"
        )
        self.assertRaises(
            OptionValueError, parse_predicate_dsl, "(column(hi) = numeric(23)"
        )
        self.assertRaises(
            OptionValueError, parse_predicate_dsl, "Column(hi) = numeric(23)"
        )
        self.assertRaises(
            OptionValueError, parse_predicate_dsl, "column(hi) = column(there)"
        )

    def test_parse_complex_dsl(self):
        parse_dsl = [
            "column(YEAR) < numeric(2012) OR (column(YEAR) = numeric(2012) AND column(MONTH) < numeric(6))",
            "(column(YEAR) < numeric(2012) OR (column(YEAR) = numeric(2012) AND column(MONTH) < numeric(6)))",
            "((column(YEAR) < numeric(2012)) OR ((column(YEAR) = numeric(2012)) AND (column(MONTH) < numeric(6))))",
            "(((column(YEAR) < numeric(2012)) "
            + "OR ((column(YEAR) = numeric(2012)) AND (column(MONTH) < numeric(6)))) "
            + "OR (((column(YEAR) = numeric(2012)) AND (column(MONTH) = numeric(6))) AND (column(DAY) < numeric(30))))",
        ]

        for predicate_dsl in parse_dsl:
            GenericPredicate(predicate_dsl)


class TestIdaPredicateMethods(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateMethods, self).__init__(*args, **kwargs)

    def test_rename_column(self):
        pred = GenericPredicate("column(original_name) IS NOT NULL")
        self.assertEqual(pred.column_names(), ["ORIGINAL_NAME"])
        pred.rename_column("original_name", "new_name")
        self.assertEqual(pred.column_names(), ["NEW_NAME"])
        self.assertEqual(pred.dsl, "column(NEW_NAME) IS NOT NULL")

        pred = GenericPredicate("column(original_name) = numeric(3)")
        self.assertEqual(pred.column_names(), ["ORIGINAL_NAME"])
        pred.rename_column("original_name", "new_name")
        self.assertEqual(pred.column_names(), ["NEW_NAME"])
        self.assertEqual(pred.dsl, "column(NEW_NAME) = numeric(3)")

        dsl = "((column(original_name) = numeric(3) AND column(original_name) != datetime(2001-01-01)) OR (column(new_name) IN (numeric(7)) OR column(other_name) NOT IN (numeric(12))))"
        pred = GenericPredicate(dsl)
        self.assertEqual(
            set(pred.column_names()), set(["ORIGINAL_NAME", "NEW_NAME", "OTHER_NAME"])
        )
        pred.rename_column("original_name", "new_name")
        self.assertEqual(set(pred.column_names()), set(["NEW_NAME", "OTHER_NAME"]))
        self.assertEqual(
            pred.ast,
            GenericPredicate(pred.dsl.replace("ORIGINAL_NAME", "NEW_NAME")).ast,
        )
        self.assertEqual(
            pred.dsl,
            dsl.replace("original_name", "NEW_NAME")
            .replace("new_name", "NEW_NAME")
            .replace("other_name", "OTHER_NAME"),
        )

    def test_set_column_alias(self):
        pred = GenericPredicate("column(col) = numeric(1)")
        pred.set_column_alias("test_alias")
        self.assertEqual(pred.column_names(), ["COL"])
        self.assertEqual(pred.alias_column_names(), [("TEST_ALIAS", "COL")])

        pred = GenericPredicate("column(alias.col) = numeric(1)")
        self.assertEqual(pred.alias_column_names(), [("ALIAS", "COL")])
        pred.set_column_alias("test_alias")
        self.assertEqual(pred.alias_column_names(), [("TEST_ALIAS", "COL")])

        pred = GenericPredicate("column(alias.col) IS NULL")
        self.assertEqual(pred.alias_column_names(), [("ALIAS", "COL")])
        pred.set_column_alias("test_alias")
        self.assertEqual(pred.alias_column_names(), [("TEST_ALIAS", "COL")])

        pred = GenericPredicate("column(alias.col) = numeric(1)")
        self.assertEqual(pred.alias_column_names(), [("ALIAS", "COL")])
        pred.set_column_alias(None)
        self.assertEqual(pred.alias_column_names(), [(None, "COL")])


class TestIdaPredicateRenderToDSL(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIdaPredicateRenderToDSL, self).__init__(*args, **kwargs)

    def test_render_dsl(self):
        expect_dsl = [
            ("column(col) = numeric(1)", "column(COL) = numeric(1)"),
            ("column(col) = numeric(1.1)", "column(COL) = numeric(1.1)"),
            (
                'column(alias.column) IN (string("hi there"))',
                'column(ALIAS.COLUMN) IN (string("hi there"))',
            ),
            (
                'column(COLUMN) IN (string("1"), string("2"))',
                'column(COLUMN) IN (string("1"), string("2"))',
            ),
            (
                '(column(c1) not in (numeric(1))) and ((column(c2) <= string("c2")) or (column(c3) IS NOT NULL))',
                '(column(C1) NOT IN (numeric(1)) AND (column(C2) <= string("c2") OR column(C3) IS NOT NULL))',
            ),
        ]
        for input_dsl, render_dsl in expect_dsl:
            self.assertEqual(GenericPredicate(input_dsl).dsl, render_dsl)


class TestIdaPredicateDataTypes(TestCase):
    def test_data_type_errors(self):
        expect_data_type_error = [
            "column(COL_STRING) = numeric(34)",
            "column(COL_STRING) = datetime(2012-01-01)",
            "column(COL_STRING) = datetime(2012-01-01 00:00:00.123456789)",
            "column(COL_STRING) IN (numeric(34))",
            "column(COL_STRING) NOT IN (numeric(34))",
            'column(COL_STRING) IN (string("NYC"), numeric(34))',
            "numeric(34) > column(COL_STRING)",
            "column(COL_INT) = datetime(2020-12-30)",
            'column(COL_INT) = string("2020-12-30")',
            'column(COL_DECIMAL) < string("nan")',
            "column(COL_DATE) = numeric(34)",
            'column(COL_DATE) = string("34")',
        ]

        for predicate_dsl in expect_data_type_error:
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                oracle_predicate.predicate_to_where_clause,
                ORACLE_COLUMNS,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                hadoop_predicate.predicate_to_where_clause,
                HADOOP_COLUMNS,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                bigquery_predicate.predicate_to_where_clause,
                BIGQUERY_COLUMNS,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                snowflake_predicate.predicate_to_where_clause,
                SNOWFLAKE_COLUMNS,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                synapse_predicate.predicate_to_where_clause,
                SYNAPSE_COLUMNS,
                GenericPredicate(predicate_dsl),
            )
            self.assertRaisesRegex(
                OptionValueError,
                "cannot be compared to",
                teradata_predicate.predicate_to_where_clause,
                TERADATA_COLUMNS,
                GenericPredicate(predicate_dsl),
            )


if __name__ == "__main__":
    main()
