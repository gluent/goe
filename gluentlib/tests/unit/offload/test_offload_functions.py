""" TestOffloadFunctions: Unit test library to test global functions defined in offload_functions.py
"""
import re
from unittest import TestCase, main

from gluentlib.offload.offload_functions import (
    convert_backend_identifier_case,
    expand_columns_csv,
    hybrid_threshold_clauses,
    hybrid_view_list_clauses,
    trunc_with_hash,
)
from gluentlib.offload.column_metadata import (
    CanonicalColumn,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_VARIABLE_STRING,
)
from gluentlib.offload.predicate_offload import GenericPredicate


class TestOffloadFunctions(TestCase):
    def test_convert_backend_identifier_case(self):
        class Opts(object):
            def __init__(self, backend_identifier_case):
                self.backend_identifier_case = backend_identifier_case

        opts = Opts("UPPER")
        self.assertEqual(convert_backend_identifier_case(opts, "db_name"), "DB_NAME")
        self.assertEqual(
            convert_backend_identifier_case(opts, "db_name", "table_name"),
            ("DB_NAME", "TABLE_NAME"),
        )
        opts = Opts("LOWER")
        self.assertEqual(convert_backend_identifier_case(opts, "DB_NAME"), "db_name")
        self.assertEqual(
            convert_backend_identifier_case(opts, "DB_NAME", "t1", "T2"),
            ("db_name", "t1", "t2"),
        )
        opts = Opts("NO_MODIFY")
        self.assertEqual(convert_backend_identifier_case(opts, "DB_Name"), "DB_Name")
        self.assertEqual(
            convert_backend_identifier_case(opts, "DB_Name", "Tab1", "TAB2"),
            ("DB_Name", "Tab1", "TAB2"),
        )

    def test_expand_columns_csv(self):
        columns = [
            CanonicalColumn("COL1_ID", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL2_ID", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL3_KEY", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL4_KEY", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL5_YEAR", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL6_MONTH", GLUENT_TYPE_INTEGER_2),
        ]
        self.assertListEqual(
            expand_columns_csv("COL1_ID,COL4_KEY", columns), ["COL1_ID", "COL4_KEY"]
        )
        self.assertListEqual(
            expand_columns_csv("COL1_ID,COL5_YEAR", columns), ["COL1_ID", "COL5_YEAR"]
        )
        self.assertListEqual(
            expand_columns_csv("COL1_ID,*_YEAR", columns), ["COL1_ID", "COL5_YEAR"]
        )
        self.assertListEqual(
            expand_columns_csv("*_ID,*_YEAR", columns),
            ["COL1_ID", "COL2_ID", "COL5_YEAR"],
        )
        self.assertListEqual(expand_columns_csv("COL1*", columns), ["COL1_ID"])
        self.assertListEqual(expand_columns_csv("*COL1_ID*", columns), ["COL1_ID"])

    def test_hybrid_view_list_clauses(self):
        def check_call(
            columns, pretend_hvs, in_list_chunk_size, expected_number_of_clauses, ch
        ):
            def quote_fn(x):
                return (ch + x + ch) if ch else x

            clauses = hybrid_view_list_clauses(
                columns,
                columns,
                pretend_hvs,
                col_enclosure_fn=quote_fn,
                in_list_chunk_size=in_list_chunk_size,
            )
            self.assertIsInstance(clauses, tuple)
            self.assertTrue(len(clauses), 2)
            self.assertIsInstance(clauses[0], str)
            self.assertIsInstance(clauses[1], str)
            col_name = ch + columns[0].name + ch
            self.assertEqual(
                clauses[0].count("{} IN ".format(col_name)), expected_number_of_clauses
            )
            self.assertEqual(
                clauses[1].count("{} NOT IN ".format(col_name)),
                expected_number_of_clauses,
            )
            # Check that IN list items are same in number as HVs
            in_lists = re.findall(r"IN \(([a-z0-9, \']+)\)", clauses[0], flags=re.I)
            self.assertEqual(sum(len(_.split(",")) for _ in in_lists), len(pretend_hvs))
            in_lists = re.findall(r"NOT IN \(([a-z0-9, \']+)\)", clauses[1], flags=re.I)
            self.assertEqual(sum(len(_.split(",")) for _ in in_lists), len(pretend_hvs))

        # Numeric list
        columns = [CanonicalColumn("COL_NAME", GLUENT_TYPE_INTEGER_2)]
        pretend_hvs = [(_,) for _ in range(20)]
        check_call(columns, pretend_hvs, 9, 3, '"')
        check_call(columns, pretend_hvs, 10, 2, '"')
        check_call(columns, pretend_hvs, 21, 1, '"')
        check_call(columns, pretend_hvs, 10, 2, "`")
        # Character list
        columns = [CanonicalColumn("COL_NAME", GLUENT_TYPE_VARIABLE_STRING)]
        pretend_hvs = [("'{}'".format(_),) for _ in range(20)]
        check_call(columns, pretend_hvs, 9, 3, '"')
        check_call(columns, pretend_hvs, 10, 2, '"')
        check_call(columns, pretend_hvs, 21, 1, '"')

    def test_hybrid_threshold_clauses(self):
        def check_call(columns, pretend_hvs, ch=None, as_dsl=False):
            def quote_fn(x):
                return (ch + x + ch) if ch else x

            expected_lt_col1_count = len(columns)
            expected_gte_col1_count = 1 if len(columns) == 1 else 3
            clauses = hybrid_threshold_clauses(
                columns,
                columns,
                pretend_hvs,
                col_enclosure_fn=quote_fn,
                as_predicate_dsl=as_dsl,
            )
            self.assertIsInstance(clauses, tuple)
            self.assertTrue(len(clauses), 2)
            lt_clause, gte_clause = clauses
            self.assertIsInstance(lt_clause, str)
            self.assertIsInstance(gte_clause, str)
            col_name = (ch + columns[0].name + ch) if ch else columns[0].name
            self.assertEqual(lt_clause.count(col_name), expected_lt_col1_count)
            self.assertEqual(gte_clause.count(col_name), expected_gte_col1_count)
            if as_dsl:
                # Check each DSL parses successfully
                GenericPredicate(lt_clause)
                GenericPredicate(gte_clause)
            if len(columns) == 2:
                expected_lt_col2_count = 2
                expected_gte_col2_count = 1
                col_name = (ch + columns[1].name + ch) if ch else columns[1].name
                self.assertTrue(lt_clause.count(col_name), expected_lt_col2_count)
                self.assertTrue(gte_clause.count(col_name), expected_gte_col2_count)

        # Numeric range
        columns = [CanonicalColumn("COL_NAME", GLUENT_TYPE_INTEGER_2)]
        check_call(columns, (10,), ch='"')
        check_call(columns, (10,), ch="`")
        # Multi column
        columns = [
            CanonicalColumn("COL_NAME1", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_NAME2", GLUENT_TYPE_INTEGER_2),
        ]
        check_call(
            columns,
            (
                10,
                5,
            ),
            ch='"',
        )
        check_call(
            columns,
            (
                10,
                5,
            ),
            as_dsl=True,
        )

    def test_trunc_with_hash(self):
        data = [
            # Centurylink metadata (CNT_AGG)
            (
                "NWX_PFM_E2E_DAY_SUMM_GLT",
                "NWX_PFM_E2E_DAY_SUMM_GLT__64SV",
                "NWX_PFM_E2E_DAY_SUMM_GLT__MO15",
            ),
            (
                "NWX_PFM_PS_HOUR_SUMM_GLT",
                "NWX_PFM_PS_HOUR_SUMM_GLT__Z4T6",
                "NWX_PFM_PS_HOUR_SUMM_GLT__UOAS",
            ),
            (
                "SUMMARY_PUC_MONTHLY_GLT",
                "SUMMARY_PUC_MONTHLY_GLT_C_MBYX",
                "SUMMARY_PUC_MONTHLY_GLT_C_HQ15",
            ),
            (
                "DAILY_DISCOUNT_QTY_T_GLT",
                "DAILY_DISCOUNT_QTY_T_GLT__TTMO",
                "DAILY_DISCOUNT_QTY_T_GLT__LKQJ",
            ),
            (
                "ENS_BILL_GL_SUMMARY_T_GLT",
                "ENS_BILL_GL_SUMMARY_T_GLT_NG9Q",
                "ENS_BILL_GL_SUMMARY_T_GLT_JYBB",
            ),
            (
                "PROVISIONINGORDER_USOC",
                "PROVISIONINGORDER_USOC_CNT_AGG",
                "PROVISIONINGORDER_USOC_CN_FYPJ",
            ),
            (
                "MONTHLYFEATURESUMMARY_GLT",
                "MONTHLYFEATURESUMMARY_GLT_IILW",
                "MONTHLYFEATURESUMMARY_GLT_NOWC",
            ),
            # Securus metadata (CNT_AGG)
            (
                "TRACKED_OFFENDER_CONTACT",
                "TRACKED_OFFENDER_CONTACT__XIXZ",
                "TRACKED_OFFENDER_CONTACT__S5FA",
            ),
            (
                "TRACKED_OFFENDER_TRACK",
                "TRACKED_OFFENDER_TRACK_CNT_AGG",
                "TRACKED_OFFENDER_TRACK_CN_CQZP",
            ),
            (
                "DEVICE_BTHDRMSG_OLD",
                "DEVICE_BTHDRMSG_OLD_CNT_AGG",
                "DEVICE_BTHDRMSG_OLD_CNT_A_BXUS",
            ),
            (
                "VTCMD_CONFIRMATIONS",
                "VTCMD_CONFIRMATIONS_CNT_AGG",
                "VTCMD_CONFIRMATIONS_CNT_A_XCKW",
            ),
        ]

        for offloaded_table, hybrid_view, hybrid_external_table in data:
            self.assertEqual(
                hybrid_view,
                trunc_with_hash("{}_cnt_agg".format(offloaded_table), 4, 30).upper(),
            )
            self.assertEqual(
                hybrid_external_table,
                trunc_with_hash("{}_EXT".format(hybrid_view), 4, 30).upper(),
            )


if __name__ == "__main__":
    main()
