""" Unit tests for TeradataPartitionExpression
"""
from unittest import TestCase, main

from gluentlib.offload.offload_source_table import OFFLOAD_PARTITION_TYPE_RANGE
from gluentlib.offload.teradata.teradata_partition_expression import PARTITION_TYPE_COLUMNAR, TeradataPartitionExpression,\
    UnsupportedCaseNPartitionExpression, UnsupportedPartitionExpression


class TestTeradataPartitionExpression(TestCase):

    def test_single_range_n_on_date(self):
        partition_check_clauses = [
            ("CHECK ((RANGE_N(dt  BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH )) BETWEEN 1 AND 65535)",
             'dt', [("DATE '2020-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("""CHECK ((RANGE_N("dt"  BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH )) BETWEEN 1 AND 65535)""",
             'dt', [("DATE '2020-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("""CHECK ((RANGE_N("DT"  BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH )) BETWEEN 1 AND 65535)""",
             'DT', [("DATE '2020-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("CHECK ((RANGE_N(dt  BETWEEN DATE '2018-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH )) BETWEEN 1 AND 00096)",
             'dt', [("DATE '2018-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("CHECK ((RANGE_N(dt  BETWEEN DATE '2019-01-01' AND DATE '2020-12-31' EACH INTERVAL '6' MONTH ,DATE '2021-01-01' AND DATE '2021-12-31' EACH INTERVAL '3' MONTH ,DATE '2022-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH )) BETWEEN 1 AND 00056)",
             'dt', [("DATE '2019-01-01'", "DATE '2020-12-31'", "INTERVAL '6' MONTH"),
                    ("DATE '2021-01-01'", "DATE '2021-12-31'", "INTERVAL '3' MONTH"),
                    ("DATE '2022-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("""CHECK ((RANGE_N("TIME_ID"  BETWEEN DATE '2011-02-01' AND DATE '2015-07-01' EACH INTERVAL '1' DAY ,  NO RANGE)) BETWEEN 1 AND 65535)""",
             'TIME_ID', [("DATE '2011-02-01'", "DATE '2015-07-01'", "INTERVAL '1' DAY")]),
            ("""CHECK ((RANGE_N("TIME_ID"  BETWEEN DATE '2011-02-01' AND DATE '2015-07-01' EACH INTERVAL '3' DAY ,  NO RANGE)) BETWEEN 1 AND 00539)""",
             'TIME_ID', [("DATE '2011-02-01'", "DATE '2015-07-01'", "INTERVAL '3' DAY")]),
            ("""CHECK ((RANGE_N(TIME_ID  BETWEEN DATE '2011-02-01' AND DATE '2015-07-01' EACH INTERVAL '1' DAY , NO RANGE)) BETWEEN 1 AND 01613)""",
             'TIME_ID', [("DATE '2011-02-01'", "DATE '2015-07-01'", "INTERVAL '1' DAY")]),
            ("""CHECK (/*02 02 02*/ RANGE_N(dt  BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH , NO RANGE) /*1 73+3047*/ IS NOT NULL AND PARTITION#L2 /*2 10+10*/ =1)""",
             'dt', [("DATE '2020-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("""CHECK (/*02 02 02*/ RANGE_N(dt  BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH , NO RANGE, UNKNOWN) /*1 74+3046*/ IS NOT NULL AND PARTITION#L2 /*2 10+10*/ =1)""",
             'dt', [("DATE '2020-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            ("""CHECK (/*02 02 02*/ RANGE_N(dt  BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH , NO RANGE OR UNKNOWN) /*1 73+3047*/ IS NOT NULL AND PARTITION#L2 /*2 10+10*/ =1)""",
             'dt', [("DATE '2020-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
        ]
        expected_type = OFFLOAD_PARTITION_TYPE_RANGE
        for ck, expected_column, expected_ranges in partition_check_clauses:
            e = TeradataPartitionExpression(ck)
            self.assertEqual(e.partition_type, expected_type)
            self.assertEqual(e.column, expected_column)
            self.assertIsInstance(e.ranges, list)
            for actual_r, expected_r in zip(e.ranges, expected_ranges):
                self.assertEqual(actual_r[0], expected_r[0])
                self.assertEqual(actual_r[1], expected_r[1])
                self.assertEqual(actual_r[2], expected_r[2])

    def test_single_range_n_on_timestamp(self):
        partition_check_clauses = [
            ("CHECK (/*01 02 00*/ RANGE_N(ts  BETWEEN TIMESTAMP '2018-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' MONTH ) /*1 96+65439*/ IS NOT NULL )",
             'ts', [("TIMESTAMP '2018-01-01 00:00:00+00:00'",
                     "TIMESTAMP '2025-12-31 23:59:59+00:00'",
                     "INTERVAL '1' MONTH")]),
            ("""CHECK (/*01 02 00*/ RANGE_N("TS"  BETWEEN TIMESTAMP '2018-01-01 00:00:00.000+00:00' AND TIMESTAMP '2025-12-31 23:59:59.999+00:00' EACH INTERVAL '1' MONTH ) /*1 96+65439*/ IS NOT NULL )""",
             'TS', [("TIMESTAMP '2018-01-01 00:00:00.000+00:00'",
                     "TIMESTAMP '2025-12-31 23:59:59.999+00:00'",
                     "INTERVAL '1' MONTH")]),
            ("CHECK (/*01 08 00*/ RANGE_N(JobStartTime  BETWEEN TIMESTAMP '2016-01-01 00:00:00.000000+00:00' AND TIMESTAMP '9999-12-30 23:23:59.999999+00:00' EACH INTERVAL '1' DAY ) /*1 2916095+9223372036851859712*/ IS NOT NULL )",
             'JobStartTime', [("TIMESTAMP '2016-01-01 00:00:00.000000+00:00'",
                               "TIMESTAMP '9999-12-30 23:23:59.999999+00:00'",
                               "INTERVAL '1' DAY")]),
            ("""CHECK (RANGE_N(ts  BETWEEN TIMESTAMP '2018-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' HOUR ) /*2 70128+0*/ IS NOT NULL)""",
             'ts', [("TIMESTAMP '2018-01-01 00:00:00+00:00'",
                     "TIMESTAMP '2025-12-31 23:59:59+00:00'",
                     "INTERVAL '1' HOUR")]),
            ("""CHECK (/*01 02 00*/ RANGE_N(ts  BETWEEN TIMESTAMP '2020-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' MONTH , NO RANGE OR UNKNOWN) /*1 73+65462*/ IS NOT NULL )""",
             'ts', [("TIMESTAMP '2020-01-01 00:00:00+00:00'",
                     "TIMESTAMP '2025-12-31 23:59:59+00:00'",
                     "INTERVAL '1' MONTH")]),
            ("""CHECK (/*01 02 00*/ RANGE_N(ts  BETWEEN TIMESTAMP '2020-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' MONTH , NO RANGE) /*1 73+65462*/ IS NOT NULL )""",
             'ts', [("TIMESTAMP '2020-01-01 00:00:00+00:00'",
                     "TIMESTAMP '2025-12-31 23:59:59+00:00'",
                     "INTERVAL '1' MONTH")]),
            ("""CHECK (/*01 02 00*/ RANGE_N(ts  BETWEEN TIMESTAMP '2020-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' MONTH , NO RANGE, UNKNOWN) /*1 74+65461*/ IS NOT NULL )""",
             'ts', [("TIMESTAMP '2020-01-01 00:00:00+00:00'",
                     "TIMESTAMP '2025-12-31 23:59:59+00:00'",
                     "INTERVAL '1' MONTH")]),
        ]
        expected_type = OFFLOAD_PARTITION_TYPE_RANGE
        for ck, expected_column, expected_ranges in partition_check_clauses:
            e = TeradataPartitionExpression(ck)
            self.assertEqual(e.partition_type, expected_type)
            self.assertEqual(e.column, expected_column)
            self.assertIsInstance(e.ranges, list)
            for actual_r, expected_r in zip(e.ranges, expected_ranges):
                self.assertEqual(actual_r[0], expected_r[0])
                self.assertEqual(actual_r[1], expected_r[1])
                self.assertEqual(actual_r[2], expected_r[2])

    def test_single_range_n_on_number(self):
        partition_check_clauses = [
            ("CHECK ((RANGE_N(ts_year  BETWEEN 2018  AND 2025  EACH 1 )) BETWEEN 1 AND 00008)",
             'ts_year', [("2018", "2025", "1")]),
            ("CHECK (/*01 02 00*/ RANGE_N(ts_year  BETWEEN 2018  AND 2025  EACH 1 ) /*1 8+65527*/ IS NOT NULL )",
             'ts_year', [("2018", "2025", "1")]),
            ("CHECK ((RANGE_N(TIME_ID  BETWEEN 20111201  AND 20120731  EACH 30 )) BETWEEN 1 AND 00318)",
             'TIME_ID', [("20111201", "20120731", "30")]),
        ]
        expected_type = OFFLOAD_PARTITION_TYPE_RANGE
        for ck, expected_column, expected_ranges in partition_check_clauses:
            e = TeradataPartitionExpression(ck)
            self.assertEqual(e.partition_type, expected_type)
            self.assertEqual(e.column, expected_column)
            self.assertIsInstance(e.ranges, list)
            for actual_r, expected_r in zip(e.ranges, expected_ranges):
                self.assertEqual(actual_r[0], expected_r[0])
                self.assertEqual(actual_r[1], expected_r[1])
                self.assertEqual(actual_r[2], expected_r[2])

    def test_single_range_n_on_string(self):
        partition_check_clauses = [
            "CHECK ((RANGE_N(flag  BETWEEN 'A','B','C','D' AND 'Z', NO RANGE, UNKNOWN)) BETWEEN 1 AND 00006)",
        ]
        # We don't support this yet so don't test anything

    def test_columnar_with_range_n(self):
        partition_check_clauses = [
            # DATE followed by columnar
            ("CHECK (/*02 02 02*/ RANGE_N(dt  BETWEEN DATE '2018-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH ) /*1 96+3024*/ IS NOT NULL AND PARTITION#L2 /*2 10+10*/ =1)",
             'dt', [("DATE '2018-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            # Columnar followed by DATE
            ("CHECK (/*02 02 01*/ PARTITION#L1 /*1 10+10*/ =1 AND RANGE_N(dt  BETWEEN DATE '2018-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH ) /*2 96+3024*/ IS NOT NULL)",
             'dt', [("DATE '2018-01-01'", "DATE '2025-12-31'", "INTERVAL '1' MONTH")]),
            # Columnar followed by TIMESTAMP
            ("""CHECK (/*02 02 01*/ PARTITION#L1 /*1 10+10*/ =1 AND RANGE_N(ts  BETWEEN TIMESTAMP '2018-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' MONTH ) /*2 96+3024*/ IS NOT NULL)""",
             'ts', [("TIMESTAMP '2018-01-01 00:00:00+00:00'", "TIMESTAMP '2025-12-31 23:59:59+00:00'", "INTERVAL '1' MONTH")]),
        ]
        expected_type = OFFLOAD_PARTITION_TYPE_RANGE
        for ck, expected_column, expected_ranges in partition_check_clauses:
            e = TeradataPartitionExpression(ck)
            self.assertEqual(e.partition_type, expected_type)
            self.assertEqual(e.column, expected_column)
            self.assertIsInstance(e.ranges, list)
            for actual_r, expected_r in zip(e.ranges, expected_ranges):
                self.assertEqual(actual_r[0], expected_r[0])
                self.assertEqual(actual_r[1], expected_r[1])
                self.assertEqual(actual_r[2], expected_r[2])

    def test_single_case_n(self):
        partition_check_clauses = [
            """CHECK ((CASE_N(cat =  0 ,cat =  1 ,cat =  2 ,cat =  3 , UNKNOWN)) BETWEEN 1 AND 00005)""",
            """CHECK ((CASE_N(dt BETWEEN DATE '2020-01-01'AND DATE '2020-12-31',dt BETWEEN DATE '2021-01-01'AND DATE '2021-12-31',dt BETWEEN DATE '2022-01-01'AND DATE '2022-12-31',dt BETWEEN DATE '2023-01-01'AND DATE '2023-12-31',dt BETWEEN DATE '2024-01-01'AND DATE '2024-12-31', UNKNOWN)) BETWEEN 1 AND 00006)""",
        ]
        # We don't support CASE_N yet and expect these to throw an exception
        for ck in partition_check_clauses:
            self.assertRaises(UnsupportedCaseNPartitionExpression, lambda: TeradataPartitionExpression(ck))

    def test_columnar_alone(self):
        partition_check_clauses = [
            # DATE followed by columnar
            ("CHECK (/*01 02 01*/ PARTITION#L1 /*1 10+65524*/ =1)", None, None),
        ]
        expected_type = PARTITION_TYPE_COLUMNAR
        for ck, expected_column, expected_ranges in partition_check_clauses:
            e = TeradataPartitionExpression(ck)
            self.assertEqual(e.partition_type, expected_type)
            self.assertEqual(e.column, expected_column)
            self.assertEqual(e.ranges, expected_ranges)

    def test_multi_range_n(self):
        partition_check_clauses = [
            # DATE/DATE
            ("CHECK (/*02 08 00*/ RANGE_N(dt  BETWEEN DATE '2018-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY ) /*1 2922+131521960367468*/ IS NOT NULL AND RANGE_N(ts  BETWEEN TIMESTAMP '2018-01-01 00:00:00+00:00' AND TIMESTAMP '2025-12-31 23:59:59+00:00' EACH INTERVAL '1' HOUR ) /*2 70128+0*/ IS NOT NULL)",
             'dt', [("DATE '2018-01-01'", "DATE '2025-12-31'", "INTERVAL '1' DAY")]),
            # INTEGER/INTEGER
            ("CHECK (/*03 02 01*/ PARTITION#L1 /*1 10+10*/ =1 AND RANGE_N(ts_year  BETWEEN 2018  AND 2025  EACH 1 ) /*2 8+252*/ IS NOT NULL AND RANGE_N(ts_month  BETWEEN 1  AND 12  EACH 1 ) /*3 12+0*/ IS NOT NULL)",
             'ts_year', [("2018", "2025", "1")]),
            ("CHECK (/*02*/ RANGE_N(ts_year  BETWEEN 2018  AND 2025  EACH 1 ) IS NOT NULL AND RANGE_N(ts_month  BETWEEN 1  AND 12  EACH 1 ) IS NOT NULL )",
             'ts_year', [("2018", "2025", "1")]),
        ]
        # Expected behaviour for MVP is to only take the first RANGE expression, subsequent levels are ignored.
        expected_type = OFFLOAD_PARTITION_TYPE_RANGE
        for ck, expected_column, expected_ranges in partition_check_clauses:
            e = TeradataPartitionExpression(ck)
            self.assertEqual(e.partition_type, expected_type)
            self.assertEqual(e.column, expected_column)
            self.assertIsInstance(e.ranges, list)
            for actual_r, expected_r in zip(e.ranges, expected_ranges):
                self.assertEqual(actual_r[0], expected_r[0])
                self.assertEqual(actual_r[1], expected_r[1])
                self.assertEqual(actual_r[2], expected_r[2])


if __name__ == '__main__':
    main()
