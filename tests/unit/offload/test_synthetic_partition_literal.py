# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Unit tests for SyntheticPartitionLiteral
"""
from datetime import datetime
import decimal
from numpy import datetime64
from unittest import TestCase, main

from goe.offload.synthetic_partition_literal import SyntheticPartitionLiteral


class TestSyntheticPartitionLiteral(TestCase):
    def test__gen_synthetic_part_date_as_string_expr(self):
        """Double underscore because we are unit testing a private method."""
        dt = datetime(2020, 10, 31, 12, 12, 00)
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_as_string_literal(dt, "Y"), "2020"
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_as_string_literal(dt, "M"), "2020-10"
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_as_string_literal(dt, "D"), "2020-10-31"
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_as_string_literal(datetime64(dt), "Y"),
            "2020",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_as_string_literal(datetime64(dt), "M"),
            "2020-10",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_as_string_literal(datetime64(dt), "D"),
            "2020-10-31",
        )

    def test__gen_synthetic_part_date_truncated_expr(self):
        """Double underscore because we are unit testing a private method."""
        dt = datetime(2020, 10, 31, 12, 12, 00)
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_truncated_literal(dt, "Y"),
            datetime(2020, 1, 1),
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_truncated_literal(dt, "M"),
            datetime(2020, 10, 1),
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_truncated_literal(dt, "D"),
            datetime(2020, 10, 31),
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_truncated_literal(datetime64(dt), "Y"),
            datetime64("2020-01-01"),
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_truncated_literal(datetime64(dt), "M"),
            datetime64("2020-10-01"),
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_date_truncated_literal(datetime64(dt), "D"),
            datetime64("2020-10-31"),
        )

    def test__gen_synthetic_part_number_expr(self):
        """Double underscore because we are unit testing a private method."""
        # Synthetic column is string
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(12345, 10, 10),
            "0000012340",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(12345, 1000, 10),
            "0000012000",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(-12345, 10, 10),
            "0000-12350",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(-12345, 1000, 10),
            "0000-13000",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(12345.12345, 10, 10),
            "0000012340",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(12345.12345, 1000, 10),
            "0000012000",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(-12345.12345, 10, 10),
            "0000-12350",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(
                -12345.12345, 1000, 10
            ),
            "0000-13000",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(12399.6, 10, 10),
            "0000012390",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(-12399.6, 10, 10),
            "0000-12400",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(99999999, 10, 10),
            "0099999990",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(99999999, 1000, 10),
            "0099999000",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(-99999999, 10, 10),
            "-100000000",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(-99999999, 1000, 10),
            "-100000000",
        )
        # Large fractional numbers used in GOE-1938 with g=1234
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(
                decimal.Decimal("1998849133104929799110640.00301078"), 1234, 25
            ),
            "1998849133104929799109602",
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_string_literal(
                decimal.Decimal("1999987308865045632950456.3171156200"), 1234, 30
            ),
            "000001999987308865045632949658",
        )
        # Synthetic column is numeric
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(12345, 10), 12340
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(12345, 1000), 12000
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(-12345, 10), -12350
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(-12345, 1000), -13000
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(12399.6, 10), 12390
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(-12399.6, 10), -12400
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(99999999, 10),
            99999990,
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(99999999, 1000),
            99999000,
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(-99999999, 10),
            -100000000,
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(-99999999, 1000),
            -100000000,
        )
        # Large fractional numbers used in GOE-1938 with g=1234
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(
                decimal.Decimal("1998849133104929799110640.00301078"), 1234
            ),
            1998849133104929799109602,
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_number_integral_literal(
                decimal.Decimal("1999987308865045632950456.3171156200"), 1234
            ),
            1999987308865045632949658,
        )
        # Test with difficult numbers, all positive which keeps check value simple
        pad_to = 38
        for num in [
            12345,
            # Numbers around the upper limit of 64 bit int
            int("9" * 17),
            int("9" * 18),
            int("9" * 19),
            # Numbers around the upper limit of typical decimal type
            int("9" * 37),
            int("9" * 38),
            # Value known to be a problem for Impala BIGINT
            int(("9" * 17) + "7"),
            # Round up or down fractional number used in GOE-1865
            12399.6,
            # Large fractional numbers used in GOE-1938, although not with g=1234, that was above
            decimal.Decimal("1998849133104929799110640.00301078"),
            decimal.Decimal("1999987308865045632950456.3171156200"),
        ]:
            for granularity in [10, 1000]:
                integral_part = str(num).split(".")[0]
                # Synthetic column is string
                pad_with = "0" * (pad_to - len(integral_part))
                if num < 0:
                    pad_with = pad_with[1:]
                trim_off = len(str(granularity)) - 1
                # String check
                synth_val = SyntheticPartitionLiteral._gen_number_string_literal(
                    num, granularity, pad_to
                )
                check_val = pad_with + str(integral_part)[:-trim_off] + ("0" * trim_off)
                self.assertEqual(synth_val, check_val)
                # Numeric check
                synth_val = SyntheticPartitionLiteral._gen_number_integral_literal(
                    num, granularity
                )
                check_val = int(str(integral_part)[:-trim_off] + ("0" * trim_off))
                self.assertEqual(synth_val, check_val)

    def test__gen_synthetic_part_string_expr(self):
        """Double underscore because we are unit testing a private method"""
        self.assertEqual(
            SyntheticPartitionLiteral._gen_string_literal("S1234", 2), "S1"
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_string_literal("S1234", 4), "S123"
        )
        self.assertEqual(
            SyntheticPartitionLiteral._gen_string_literal("S1234", 6), "S1234"
        )


if __name__ == "__main__":
    main()
