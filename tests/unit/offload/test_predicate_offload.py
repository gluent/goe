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
import pytest

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


@pytest.mark.parametrize(
    "dsl_value_str, ast_token_values",
    [
        ("3.141", ("SIGNED_DECIMAL", float(3.141))),
        ("+3.141", ("SIGNED_DECIMAL", float(3.141))),
        ("-3.141", ("SIGNED_DECIMAL", float(-3.141))),
        (" 3.141", ("SIGNED_DECIMAL", float(3.141))),
        (" 3.141 ", ("SIGNED_DECIMAL", float(3.141))),
        ("3.", ("SIGNED_DECIMAL", float(3))),
        ("3", ("SIGNED_INTEGER", int(3))),
        ("+3", ("SIGNED_INTEGER", int(3))),
        ("-3", ("SIGNED_INTEGER", int(-3))),
        (" 3", ("SIGNED_INTEGER", int(3))),
        ("3 ", ("SIGNED_INTEGER", int(3))),
        (" 3 ", ("SIGNED_INTEGER", int(3))),
    ],
)
def test_predicate_parse_single_numeric(dsl_value_str, ast_token_values):
    assert parse_predicate_dsl(
        "numeric({})".format(dsl_value_str), top_level_node="value"
    ) == Tree("numeric_value", [Token(*ast_token_values)])


@pytest.mark.parametrize(
    "dsl_value_str, ast_token_values",
    [
        ("2012-01-01", ("DATE", np.datetime64("2012-01-01"))),
        ("2001-01-01 12:00:01", ("TIMESTAMP", np.datetime64("2001-01-01 12:00:01"))),
        ("2001-01-01 06:00:01", ("TIMESTAMP", np.datetime64("2001-01-01 06:00:01"))),
        (
            "2001-01-01 12:00:01.050",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2001-01-01 12:00:01.050")),
        ),
        (
            "2001-01-01 06:00:01.000090",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2001-01-01 06:00:01.000090")),
        ),
        (
            "2001-01-01 06:00:01.000000123",
            ("TIMESTAMP_FRACTIONAL", np.datetime64("2001-01-01 06:00:01.000000123")),
        ),
        ("2012-01-01", ("DATE", np.datetime64("2012-01-01"))),
    ],
)
def test_predicate_parse_single_datetime(dsl_value_str, ast_token_values):
    assert parse_predicate_dsl(
        "datetime({})".format(dsl_value_str), top_level_node="value"
    ) == Tree("datetime_value", [Token(*ast_token_values)])


@pytest.mark.parametrize(
    "dsl_value_str",
    [
        "test string",
        "test' string",
        'test \\" string',
        "",
        "`~!@#$%^&*()_+-={}[]|\\:;?/>.<,0987654321",
        "\u00f6",
    ],
)
def test_predicate_parse_single_string(dsl_value_str):
    assert parse_predicate_dsl(
        'string("{}")'.format(dsl_value_str), top_level_node="value"
    ) == Tree(
        "string_value",
        [Token("ESCAPED_STRING", dsl_value_str.replace('\\"', '"'))],
    )


class TestIdaPredicateParse(TestCase):

    def _test_parse_single_datetime(self, dsl_value_str, ast_token_values):
        self.assertEqual(
            parse_predicate_dsl(
                "datetime({})".format(dsl_value_str), top_level_node="value"
            ),
            Tree("datetime_value", [Token(*ast_token_values)]),
        )

    def test_parse_datetime(self):
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

        self.assertRaises(
            OptionValueError,
            self._test_parse_single_datetime,
            "2012-01-01 6:00:01",
            ("TIMESTAMP", np.datetime64("2012-01-01 06:00:01")),
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


@pytest.mark.parametrize(
    "predicate_dsl",
    [
        "column(YEAR) < numeric(2012) OR (column(YEAR) = numeric(2012) AND column(MONTH) < numeric(6))",
        "(column(YEAR) < numeric(2012) OR (column(YEAR) = numeric(2012) AND column(MONTH) < numeric(6)))",
        "((column(YEAR) < numeric(2012)) OR ((column(YEAR) = numeric(2012)) AND (column(MONTH) < numeric(6))))",
        (
            "(((column(YEAR) < numeric(2012)) "
            "OR ((column(YEAR) = numeric(2012)) AND (column(MONTH) < numeric(6)))) "
            "OR (((column(YEAR) = numeric(2012)) AND (column(MONTH) = numeric(6))) AND (column(DAY) < numeric(30))))"
        ),
    ],
)
def test_parse_complex_dsl(predicate_dsl):
    GenericPredicate(predicate_dsl)


@pytest.mark.parametrize(
    "predicate_dsl",
    [
        "",
        "column(hi)",
        "column(hi) >",
        "column(hi) > numeric()",
        "column(hi) > numeric(+-23)",
        "column(hi) == numeric(23)",
        "(column(hi) = numeric(23)",
        "Column(hi) = numeric(23)",
        "column(hi) = column(there)",
    ],
)
def test_parse_errors(predicate_dsl):
    with pytest.raises(OptionValueError) as _:
        parse_predicate_dsl(predicate_dsl)


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

        dsl = (
            "((column(original_name) = numeric(3) "
            "AND column(original_name) != datetime(2001-01-01)) "
            "OR (column(new_name) IN (numeric(7)) "
            "OR column(other_name) NOT IN (numeric(12))))"
        )
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


@pytest.mark.parametrize(
    "predicate_dsl,column_name,comparison_value,expect_pass",
    [
        # We expect to use this for predicates with ANDs only.
        (
            "column(YEAR) < numeric(2012) OR column(YEAR) = numeric(2013)",
            "year",
            2012,
            False,
        ),
        # eq
        (
            "column(YEAR) = numeric(2012)",
            "YEAR",
            2012,
            True,
        ),
        (
            "numeric(2012) = column(YEAR)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(MONTH) = numeric(11) AND column(year) = numeric(2012)",
            "year",
            2012,
            True,
        ),
        (
            "column(YEAR) = numeric(2013)",
            "Year",
            2012,
            False,
        ),
        (
            "column(YEAR) = numeric(2013) AND column(YEAR) = numeric(2012)",
            "YEAR",
            2012,
            False,
        ),
        # lt
        (
            "column(YEAR) < numeric(2013)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) < numeric(2012)",
            "YEAR",
            2012,
            False,
        ),
        # le
        (
            "column(YEAR) <= numeric(2013)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) <= numeric(2012)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) <= numeric(2011)",
            "YEAR",
            2012,
            False,
        ),
        # gt
        (
            "column(YEAR) > numeric(2011)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) > numeric(2012)",
            "YEAR",
            2012,
            False,
        ),
        # ge
        (
            "column(YEAR) >= numeric(2011)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) >= numeric(2012)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) >= numeric(2013)",
            "YEAR",
            2012,
            False,
        ),
        (
            "numeric(2013) >= column(YEAR)",
            "YEAR",
            2012,
            True,
        ),
        # ne
        (
            "column(YEAR) != numeric(2011)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) != numeric(2012)",
            "YEAR",
            2012,
            False,
        ),
        # in
        (
            "column(YEAR) in (numeric(2011),numeric(2012))",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) in (numeric(2011),numeric(2013))",
            "YEAR",
            2012,
            False,
        ),
        # not in
        (
            "column(YEAR) not in (numeric(2011),numeric(2013))",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) not in (numeric(2011),numeric(2012))",
            "YEAR",
            2012,
            False,
        ),
        # combinations
        (
            "column(YEAR) >= numeric(2010) AND column(YEAR) < numeric(2013)",
            "YEAR",
            2012,
            True,
        ),
        (
            "column(YEAR) >= numeric(2010) AND column(YEAR) < numeric(2012)",
            "YEAR",
            2012,
            False,
        ),
    ],
)
def test_generic_predicate_column_value_match_numeric(
    predicate_dsl: str, column_name: str, comparison_value, expect_pass: bool
):
    if " OR " in predicate_dsl:
        with pytest.raises(Exception) as _:
            GenericPredicate(predicate_dsl).column_value_match(
                column_name, comparison_value
            )
    else:
        assert (
            GenericPredicate(predicate_dsl).column_value_match(
                column_name, comparison_value
            )
            == expect_pass
        )


@pytest.mark.parametrize(
    "predicate_dsl,column_name,comparison_value,expect_pass",
    [
        # eq
        (
            "column(DTTM) = datetime(2012-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(id) = numeric(11) AND column(dttm) = datetime(2012-01-01)",
            "dttm",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) = datetime(2012-01-02)",
            "DtTm",
            np.datetime64("2012-01-01"),
            False,
        ),
        (
            "column(DTTM) = datetime(2012-01-01) AND column(DTTM) = datetime(2012-01-02)",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # lt
        (
            "column(DTTM) < datetime(2012-01-02)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) < datetime(2012-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # le
        (
            "column(DTTM) <= datetime(2012-01-02)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) <= datetime(2012-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) <= datetime(2011-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # gt
        (
            "column(DTTM) > datetime(2011-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) > datetime(2012-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # ge
        (
            "column(DTTM) >= datetime(2011-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) >= datetime(2012-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) >= datetime(2013-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # ne
        (
            "column(DTTM) != datetime(2013-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) != datetime(2012-01-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # in
        (
            "column(DTTM) in (datetime(2012-01-01),datetime(2013-01-01))",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) in (datetime(2011-01-01),datetime(2013-01-01))",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # not in
        (
            "column(DTTM) not in (datetime(2011-01-01),datetime(2013-01-01))",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) not in (datetime(2012-01-01),datetime(2013-01-01))",
            "DTTM",
            np.datetime64("2012-01-01"),
            False,
        ),
        # combinations
        (
            "column(DTTM) >= datetime(2012-01-01) AND column(DTTM) < datetime(2012-02-01)",
            "DTTM",
            np.datetime64("2012-01-01"),
            True,
        ),
        (
            "column(DTTM) >= datetime(2012-01-01) AND column(DTTM) < datetime(2012-02-01)",
            "DTTM",
            np.datetime64("2012-03-01"),
            False,
        ),
    ],
)
def test_generic_predicate_column_value_match_datetime(
    predicate_dsl: str, column_name: str, comparison_value, expect_pass: bool
):
    if " OR " in predicate_dsl:
        with pytest.raises(Exception) as _:
            GenericPredicate(predicate_dsl).column_value_match(
                column_name, comparison_value
            )
    else:
        assert (
            GenericPredicate(predicate_dsl).column_value_match(
                column_name, comparison_value
            )
            == expect_pass
        )


@pytest.mark.parametrize(
    "predicate_dsl,column_name,comparison_range,expect_pass",
    [
        # eq
        (
            "column(YEAR) = numeric(2011)",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) = numeric(2012)",
            "YEAR",
            (None, 2012),
            False,
        ),
        (
            "column(YEAR) = numeric(2009)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) = numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) = numeric(2012)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        # lt
        (
            "column(YEAR) < numeric(2011)",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) < numeric(2011)",
            "YEAR",
            (None, 2010),
            True,
        ),
        (
            "column(YEAR) < numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) < numeric(2012)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) < numeric(2010)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        # le
        (
            "column(YEAR) <= numeric(2011)",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) <= numeric(2011)",
            "YEAR",
            (None, 2010),
            True,
        ),
        (
            "column(YEAR) <= numeric(2010)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) <= numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) <= numeric(2012)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) <= numeric(2000)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        # gt
        (
            "column(YEAR) > numeric(2011)",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) > numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) > numeric(2012)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) > numeric(2013)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        # ge
        (
            "column(YEAR) >= numeric(2011)",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) >= numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) >= numeric(2012)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) >= numeric(2013)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        # ne - everything is a match for ne.
        (
            "column(YEAR) != numeric(2011)",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) != numeric(2012)",
            "YEAR",
            (2011, 2012),
            True,
        ),
        # in
        (
            "column(YEAR) in (numeric(2011),numeric(2012))",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) in (numeric(2012),numeric(2013))",
            "YEAR",
            (None, 2012),
            False,
        ),
        (
            "column(YEAR) in (numeric(2013),numeric(2014))",
            "YEAR",
            (None, 2012),
            False,
        ),
        # not in - everything is a match for not in.
        (
            "column(YEAR) not in (numeric(2011),numeric(2012))",
            "YEAR",
            (None, 2012),
            True,
        ),
        (
            "column(YEAR) not in (numeric(2011),numeric(2012))",
            "YEAR",
            (2011, 2012),
            True,
        ),
        # combinations
        (
            "column(YEAR) >= numeric(2009) AND column(YEAR) < numeric(2010)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) >= numeric(2010) AND column(YEAR) < numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) >= numeric(2011) AND column(YEAR) < numeric(2012)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) >= numeric(2012) AND column(YEAR) < numeric(2013)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) > numeric(2009) AND column(YEAR) < numeric(2013)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) > numeric(2009) AND column(YEAR) < numeric(2011)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) > numeric(2011) AND column(YEAR) < numeric(2013)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) > numeric(2012) AND column(YEAR) < numeric(2013)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) > numeric(2009) AND column(YEAR) < numeric(2010)",
            "YEAR",
            (2010, 2012),
            False,
        ),
        (
            "column(YEAR) > numeric(2001) AND column(YEAR) < numeric(2020)",
            "YEAR",
            (2010, 2012),
            True,
        ),
        (
            "column(YEAR) > numeric(2001) AND column(YEAR) < numeric(2020) AND column(YEAR) = numeric(2015)",
            "YEAR",
            (2010, 2012),
            False,
        ),
    ],
)
def test_generic_predicate_column_value_range_num(
    predicate_dsl: str, column_name: str, comparison_range: tuple, expect_pass: bool
):
    if " OR " in predicate_dsl:
        with pytest.raises(Exception) as _:
            GenericPredicate(predicate_dsl).column_range_match(
                column_name, comparison_range[0], comparison_range[1]
            )
    elif expect_pass:
        assert GenericPredicate(predicate_dsl).column_range_match(
            column_name, comparison_range
        ), f"Predicate ({predicate_dsl}) not in range: {comparison_range}"
    else:
        assert not GenericPredicate(predicate_dsl).column_range_match(
            column_name, comparison_range
        ), f"Predicate ({predicate_dsl}) should NOT match range: {comparison_range}"


# Not been as comprehensive for datetime, just shake down each comparison.
@pytest.mark.parametrize(
    "predicate_dsl,column_name,comparison_range,expect_pass",
    [
        # eq
        (
            "column(DTTM) = datetime(2012-03-01)",
            "DTTM",
            (None, np.datetime64("2012-02-01")),
            False,
        ),
        (
            "column(DTTM) = datetime(2012-01-01)",
            "DTTM",
            (np.datetime64("2000-02-01"), np.datetime64("2012-02-01")),
            True,
        ),
        (
            "column(DTTM) = datetime(2012-02-01)",
            "DTTM",
            (np.datetime64("2000-02-01"), np.datetime64("2012-02-01")),
            False,
        ),
        # lt
        (
            "column(DTTM) < datetime(2012-03-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
        (
            "column(DTTM) < datetime(2012-02-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            False,
        ),
        # le
        (
            "column(DTTM) <= datetime(2012-03-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
        (
            "column(DTTM) <= datetime(2012-02-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
        (
            "column(DTTM) <= datetime(2012-01-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            False,
        ),
        # gt
        (
            "column(DTTM) > datetime(2012-03-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            False,
        ),
        (
            "column(DTTM) > datetime(2012-02-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
        # ge
        (
            "column(DTTM) >= datetime(2012-03-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            False,
        ),
        (
            "column(DTTM) >= datetime(2012-02-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
        (
            "column(DTTM) >= datetime(2012-01-01)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
        # ne
        (
            "column(DTTM) != datetime(2012-02-10)",
            "DTTM",
            (np.datetime64("2012-02-01"), np.datetime64("2012-03-01")),
            True,
        ),
    ],
)
def test_generic_predicate_column_value_range_datetime(
    predicate_dsl: str, column_name: str, comparison_range: tuple, expect_pass: bool
):
    if " OR " in predicate_dsl:
        with pytest.raises(Exception) as _:
            GenericPredicate(predicate_dsl).column_range_match(
                column_name, comparison_range[0], comparison_range[1]
            )
    else:
        assert (
            GenericPredicate(predicate_dsl).column_range_match(
                column_name, comparison_range
            )
            == expect_pass
        )


if __name__ == "__main__":
    main()
