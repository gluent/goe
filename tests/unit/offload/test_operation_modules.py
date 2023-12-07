""" Unit test library to test modules in offload.operation package
"""

from unittest import TestCase, main

from goe.offload.offload_constants import (
    NOT_NULL_PROPAGATION_AUTO,
    NOT_NULL_PROPAGATION_NONE,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.operation.data_type_controls import (
    OffloadDataTypeControlsException,
    canonical_columns_from_columns_csv,
)
from goe.offload.operation.not_null_columns import (
    OffloadNotNullControlsException,
    apply_not_null_columns_csv,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    GLUENT_TYPE_DECIMAL,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_INTEGER_4,
)


class TestOperationDataTypeControls(TestCase):
    """Test operation.data_type_controls"""

    def test_canonical_columns_from_columns_csv(self):
        reference_columns = [
            CanonicalColumn("COL1_ID", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL2_ID", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL3_KEY", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL4_KEY", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL5_YEAR", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL6_MONTH", GLUENT_TYPE_INTEGER_2),
        ]
        col_list = canonical_columns_from_columns_csv(
            GLUENT_TYPE_INTEGER_4, "COL1_ID,COL2_ID", [], reference_columns
        )
        self.assertEqual(len(col_list), 2)
        self.assertEqual(col_list[0].data_type, GLUENT_TYPE_INTEGER_4)
        col_list = canonical_columns_from_columns_csv(
            GLUENT_TYPE_INTEGER_4, "*_ID", [], reference_columns
        )
        self.assertEqual(len(col_list), 2)
        self.assertEqual(col_list[0].data_type, GLUENT_TYPE_INTEGER_4)
        col_list = canonical_columns_from_columns_csv(
            GLUENT_TYPE_INTEGER_4, "*_ID,*KEY", [], reference_columns
        )
        self.assertEqual(len(col_list), 4)
        col_list = canonical_columns_from_columns_csv(
            GLUENT_TYPE_DECIMAL,
            "COL5_YEAR",
            [],
            reference_columns,
            precision=4,
            scale=0,
        )
        self.assertEqual(len(col_list), 1)
        self.assertEqual(col_list[0].data_type, GLUENT_TYPE_DECIMAL)
        self.assertEqual(col_list[0].data_precision, 4)
        self.assertEqual(col_list[0].data_scale, 0)
        col_list = canonical_columns_from_columns_csv(
            GLUENT_TYPE_INTEGER_4, "*", [], reference_columns
        )
        self.assertEqual(len(col_list), len(reference_columns))
        # Ensure overlaps are caught
        self.assertRaises(
            OffloadDataTypeControlsException,
            lambda: canonical_columns_from_columns_csv(
                GLUENT_TYPE_INTEGER_4,
                "COL1_ID,COL2_ID",
                reference_columns,
                reference_columns,
            ),
        )


class TestOperationNotNullColumns(TestCase):
    """Test operation.not_null_columns"""

    reference_columns = [
        CanonicalColumn("COL1_ID", GLUENT_TYPE_INTEGER_2, nullable=True),
        CanonicalColumn("COL2_ID_NN", GLUENT_TYPE_INTEGER_2, nullable=False),
        CanonicalColumn("COL3_MONTH", GLUENT_TYPE_INTEGER_2, nullable=True),
        CanonicalColumn("COL4_MONTH_NN", GLUENT_TYPE_INTEGER_2, nullable=False),
    ]

    def test_not_null_columns_auto(self):
        messages = OffloadMessages()
        names = [_.name for _ in self.reference_columns]
        new_cols = apply_not_null_columns_csv(
            self.reference_columns, None, NOT_NULL_PROPAGATION_AUTO, names, messages
        )
        # Nullable should be unchanged for AUTO
        for new_col, ref_col in zip(new_cols, self.reference_columns):
            self.assertEqual(new_col.name, ref_col.name)
            self.assertEqual(new_col.nullable, ref_col.nullable)

    def test_not_null_columns_none(self):
        messages = OffloadMessages()
        names = [_.name for _ in self.reference_columns]
        new_cols = apply_not_null_columns_csv(
            self.reference_columns, None, NOT_NULL_PROPAGATION_NONE, names, messages
        )
        # Nullable should be True for all columns for NONE
        for new_col, ref_col in zip(new_cols, self.reference_columns):
            self.assertEqual(new_col.name, ref_col.name)
            self.assertTrue(new_col.nullable)

    def test_not_null_columns_list(self):
        messages = OffloadMessages()
        names = [_.name for _ in self.reference_columns]
        new_cols = apply_not_null_columns_csv(
            self.reference_columns, "*_NN", NOT_NULL_PROPAGATION_AUTO, names, messages
        )
        for new_col, ref_col in zip(new_cols, self.reference_columns):
            self.assertEqual(new_col.name, ref_col.name)
            if new_col.name.endswith("_NN"):
                self.assertFalse(
                    new_col.nullable, f"Nullable incorrect for {new_col.name}"
                )
            else:
                self.assertTrue(
                    new_col.nullable, f"Nullable incorrect for {new_col.name}"
                )

        # Ensure nonsense values are caught.
        self.assertRaises(
            OffloadNotNullControlsException,
            lambda: apply_not_null_columns_csv(
                self.reference_columns,
                "*XYZ",
                NOT_NULL_PROPAGATION_AUTO,
                names,
                messages,
            ),
        )
        self.assertRaises(
            OffloadNotNullControlsException,
            lambda: apply_not_null_columns_csv(
                self.reference_columns,
                "COLX",
                NOT_NULL_PROPAGATION_AUTO,
                names,
                messages,
            ),
        )


if __name__ == "__main__":
    main()
