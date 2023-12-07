""" TestColumnMetadata: Unit test TestColumnMetadata library to test all functions """
from unittest import TestCase, main

from goe.offload.column_metadata import (
    CanonicalColumn,
    ColumnBucketInfo,
    ColumnPartitionInfo,
    get_column_names,
    get_partition_columns,
    is_synthetic_partition_column,
    match_partition_column_by_source,
    match_table_column,
    match_table_column_position,
    regex_real_column_from_part_column,
    valid_column_list,
    GLUENT_TYPE_BINARY,
    GLUENT_TYPE_BOOLEAN,
    GLUENT_TYPE_DATE,
    GLUENT_TYPE_DECIMAL,
    GLUENT_TYPE_FLOAT,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_TIME,
    GLUENT_TYPE_VARIABLE_STRING,
    SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE,
)
from goe.offload.offload_constants import OFFLOAD_BUCKET_NAME


class TestColumnMetadata(TestCase):
    def canonical_columns(self):
        return [
            CanonicalColumn("ID", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_NAME", GLUENT_TYPE_VARIABLE_STRING),
            CanonicalColumn("COL_DATA", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_OTHER_ID", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn(
                "COL_COST", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_RATE", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_BOOL", GLUENT_TYPE_BOOLEAN),
            CanonicalColumn(
                SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE % ("M", "COL_DATE"),
                GLUENT_TYPE_VARIABLE_STRING,
                partition_info=ColumnPartitionInfo(
                    0, source_column_name="COL_DATE", granularity="M"
                ),
            ),
            CanonicalColumn(
                SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE % ("1", "COL_NAME"),
                GLUENT_TYPE_VARIABLE_STRING,
                partition_info=ColumnPartitionInfo(
                    1, source_column_name="COL_NAME", granularity="1"
                ),
            ),
            CanonicalColumn(
                "NATIVE_PART_COL",
                GLUENT_TYPE_INTEGER_2,
                partition_info=ColumnPartitionInfo(2),
            ),
            CanonicalColumn(
                OFFLOAD_BUCKET_NAME,
                GLUENT_TYPE_INTEGER_2,
                bucket_info=ColumnBucketInfo(
                    source_column_name="ID", num_buckets=16, bucket_hash_method=None
                ),
            ),
        ]

    def test_get_column_names(self):
        names = get_column_names(self.canonical_columns())
        self.assertIsInstance(names, list)
        self.assertIsInstance(names[0], str)
        self.assertEqual(
            get_column_names(self.canonical_columns(), conv_fn=str.upper)[0],
            get_column_names(self.canonical_columns())[0].upper(),
        )
        self.assertEqual(
            get_column_names(self.canonical_columns(), conv_fn=str.lower)[0],
            get_column_names(self.canonical_columns())[0].lower(),
        )

    def test_get_partition_columns(self):
        self.assertIsInstance(get_partition_columns(self.canonical_columns()), list)
        self.assertIsInstance(
            match_table_column(
                "NATIVE_PART_COL", get_partition_columns(self.canonical_columns())
            ),
            CanonicalColumn,
        )
        self.assertIsNone(
            match_table_column(
                "COL_COST", get_partition_columns(self.canonical_columns())
            ),
            CanonicalColumn,
        )

    def test_is_synthetic_partition_column(self):
        """Test synthetic identification using a list of column names grepped from test logs, plus some extras"""
        for synth_name, expected_source_name in [
            ("gl_part_y_time_id", "time_id"),
            ("gl_part_m_time_id", "time_id"),
            ("gl_part_d_time_id", "time_id"),
            ("gl_part_m_h1_time_id", "h1_time_id"),
            ("gl_part_000000000000001_time_year", "time_year"),
            ("gl_part_000000000000001_time_month", "time_month"),
            ("gl_part_1_yrmon", "yrmon"),
            ("gl_part_6_yrmon", "yrmon"),
            ("gl_part_000000000000001_yrmon", "yrmon"),
            ("GL_PART_0000000100_TIME_ID", "TIME_ID"),
            ("GL_PART_100_TIME_ID", "TIME_ID"),
            ("GL_PART_1000_PROD_LIST_PRICE", "PROD_LIST_PRICE"),
            ("GL_PART_1000_PROD_LIST_PRICE", "PROD_LIST_PRICE"),
            ("gl_part_00001000_prod_list_price", "PROD_LIST_PRICE"),
            ("gl_part_000000000001000_id", "id"),
            (
                "gl_part_000000000000001_this_part_column_name_is_long_100_123456789012345678901234567890123456789012345678901234567890123456",
                "this_part_column_name_is_long_100_123456789012345678901234567890123456789012345678901234567890123456",
            ),
            ("gl_part_000000000001000_id", "id"),
            ("gl_part_000000000100000_id", "id"),
            ("gl_part_00000010_prod_id", "prod_id"),
            ("gl_part_1_prod_category_desc", "prod_category_desc"),
            ("gl_part_m_dt", "dt"),
            ("gl_part_y_dt", "dt"),
            ("GL_PART_100000000000000000_PART_COL", "PART_COL"),
            ("gl_part_u0_time_id", "time_id"),
            ("gl_part_u3_time_id", "time_id"),
        ]:
            self.assertTrue(is_synthetic_partition_column(synth_name.upper()))
            self.assertTrue(is_synthetic_partition_column(synth_name.lower()))
            # While we are here we can test the extraction of metadata from the column name
            match = regex_real_column_from_part_column(synth_name)
            self.assertTrue(bool(match))
            self.assertEqual(match.group(3).upper(), expected_source_name.upper())

        for synth_name in [
            "time_id",
            "gl_part_p_time_id",
            "gl_part_-1_time_id" "gl_part_year_time_id",
            "gl_part_month_time_id",
            "gl_part_day_time_id",
            "gl_part_U_time_id",
            "gl_part_1U_time_id",
            "gl_part_U__time_id",
        ]:
            self.assertFalse(is_synthetic_partition_column(synth_name))

    def test_match_table_column(self):
        self.assertIsInstance(
            match_table_column("iD", self.canonical_columns()), CanonicalColumn
        )
        self.assertIsNone(match_table_column("not_a_column", self.canonical_columns()))

    def test_match_partition_column_by_source(self):
        part_col = match_partition_column_by_source(
            "col_name", self.canonical_columns()
        )
        self.assertIsInstance(part_col, CanonicalColumn)
        self.assertEqual(
            part_col.name, SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE % ("1", "COL_NAME")
        )
        self.assertIsNone(
            match_partition_column_by_source("not_a_column", self.canonical_columns())
        )
        self.assertIsNone(
            match_partition_column_by_source("COL_BOOL", self.canonical_columns())
        )

    def test_match_table_column_position(self):
        self.assertEqual(match_table_column_position("iD", self.canonical_columns()), 0)
        self.assertIsNone(
            match_table_column_position("not_a_column", self.canonical_columns())
        )

    def test_valid_column_list(self):
        self.assertTrue(valid_column_list(self.canonical_columns()))
        self.assertFalse(valid_column_list(None))
        self.assertFalse(valid_column_list([1, 2, 3]))
        self.assertFalse(valid_column_list(self))


if __name__ == "__main__":
    main()
