#! /usr/bin/env python3
"""
    Offload predicate test code.
    LICENSE_TEXT
"""

from unittest import TestCase, main
from optparse import OptionValueError

from lark import Tree, Token
import numpy as np

from gluentlib.offload.predicate_offload import GenericPredicate, parse_predicate_dsl


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
        self._test_parse_single_string("`~!@#$%^&*()_+-={}[]|\:;?/>.<,0987654321")
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


if __name__ == "__main__":
    main()
