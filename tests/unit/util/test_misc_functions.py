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

""" TestMiscFunctions: Unit test library to test functions from misc_functions module.
"""
import decimal
from unittest import TestCase, main
from textwrap import dedent

from goe.util.misc_functions import (
    add_prefix_in_same_case,
    add_suffix_in_same_case,
    case_insensitive_in,
    substitute_in_same_case,
    backtick_sandwich,
    bytes_to_human_size,
    format_list_for_logging,
    get_integral_part_magnitude,
    get_temp_path,
    human_size_to_bytes,
    is_number,
    is_pos_int,
    is_power_of_10,
    remove_chars,
    split_not_in_quotes,
    standard_file_name,
    standard_log_name,
    trunc_with_hash,
    truncate_number,
    wildcard_matches_in_list,
)


class TestMiscFunctions(TestCase):
    def test_add_prefix_in_same_case(self):
        self.assertEqual(add_prefix_in_same_case("hello", "db_"), "db_hello")
        self.assertEqual(add_prefix_in_same_case("hello", "DB_"), "db_hello")
        self.assertEqual(add_prefix_in_same_case("HELLO", "db_"), "DB_HELLO")
        self.assertEqual(add_prefix_in_same_case("HELLO", "DB_"), "DB_HELLO")
        self.assertEqual(add_prefix_in_same_case("Hello", "db_"), "db_Hello")
        self.assertEqual(add_prefix_in_same_case("Hello", "DB_"), "DB_Hello")

    def test_add_suffix_in_same_case(self):
        self.assertEqual(add_suffix_in_same_case("hello", ".txt"), "hello.txt")
        self.assertEqual(add_suffix_in_same_case("hello", ".TXT"), "hello.txt")
        self.assertEqual(add_suffix_in_same_case("HELLO", ".txt"), "HELLO.TXT")
        self.assertEqual(add_suffix_in_same_case("HELLO", ".TXT"), "HELLO.TXT")
        self.assertEqual(add_suffix_in_same_case("Hello", ".txt"), "Hello.txt")
        self.assertEqual(add_suffix_in_same_case("Hello", ".TXT"), "Hello.TXT")

    def test_backtick_sandwich(self):
        self.assertEqual(backtick_sandwich("hello"), "`hello`")
        self.assertEqual(backtick_sandwich("`hello`"), "`hello`")
        self.assertEqual(backtick_sandwich("hello", ch="_"), "_hello_")

    def test_case_insensitive_in(self):
        # With a list
        self.assertEqual(case_insensitive_in("Hello", ["hello", "blah"]), "hello")
        self.assertEqual(case_insensitive_in("Hello", ["HELLO", "blah"]), "HELLO")
        self.assertEqual(case_insensitive_in("Hello", ["Hello", "blah"]), "Hello")
        self.assertTrue(bool(case_insensitive_in("Hello", ["hello", "blah"])))
        self.assertIsNone(case_insensitive_in("Hello", ["ello", "blah"]))
        self.assertFalse(bool(case_insensitive_in("Hello", ["ello", "blah"])))
        self.assertIsNone(case_insensitive_in("Hello", []))
        self.assertIsNone(case_insensitive_in("Hello", None))
        # With a set
        self.assertEqual(case_insensitive_in("Hello", set(["hello", "blah"])), "hello")
        self.assertEqual(case_insensitive_in("Hello", set(["HELLO", "blah"])), "HELLO")
        self.assertEqual(case_insensitive_in("Hello", set(["Hello", "blah"])), "Hello")
        self.assertTrue(bool(case_insensitive_in("Hello", set(["hello", "blah"]))))
        self.assertIsNone(case_insensitive_in("Hello", set(["ello", "blah"])))
        self.assertFalse(bool(case_insensitive_in("Hello", set(["ello", "blah"]))))
        self.assertIsNone(case_insensitive_in("Hello", set([])))
        # With a str
        self.assertEqual(case_insensitive_in("Hello", "hello"), "hello")
        self.assertEqual(case_insensitive_in("Hello", "HELLO"), "HELLO")
        self.assertEqual(case_insensitive_in("Hello", "Hello"), "Hello")
        self.assertIsNone(case_insensitive_in("Hello", "ello"))

    def test_get_integral_part_magnitude(self):
        self.assertEqual(get_integral_part_magnitude(1234567890), 10)
        self.assertEqual(get_integral_part_magnitude(1234567890.1234), 10)
        self.assertEqual(get_integral_part_magnitude(12345678901234567890), 20)
        self.assertEqual(get_integral_part_magnitude(12345678901234567890.1234), 20)
        self.assertEqual(
            get_integral_part_magnitude(123456789012345678901234567890), 30
        )
        self.assertEqual(
            get_integral_part_magnitude(123456789012345678901234567890.1234), 30
        )

    def test_substitute_in_same_case(self):
        self.assertEqual(substitute_in_same_case("%s_h", "hello"), "hello_h")
        self.assertEqual(substitute_in_same_case("%s_H", "hello"), "hello_h")
        self.assertEqual(substitute_in_same_case("%s_h", "HELLO"), "HELLO_H")
        self.assertEqual(substitute_in_same_case("%s_H", "HELLO"), "HELLO_H")
        self.assertEqual(substitute_in_same_case("%s_h", "Hello"), "Hello_h")
        self.assertEqual(substitute_in_same_case("%s_H", "Hello"), "Hello_H")

    def test_truncate_number(self):
        for n, digits, expected_result in [
            # Regular ints and floats truncating decimal places
            (0, 0, 0),
            (0, 5, 0),
            (1234, 0, 1234),
            (-1234, 0, -1234),
            (1234.1, 0, 1234),
            (-1234.1, 0, -1234),
            (1234.9, 0, 1234),
            (-1234.9, 0, -1234),
            (1234.1, 1, 1234.1),
            (-1234.1, 1, -1234.1),
            (1234.9, 1, 1234.9),
            (-1234.9, 1, -1234.9),
            (1234, 2, 1234),
            (-1234, 2, -1234),
            (1234.1, 2, 1234.1),
            (-1234.1, 2, -1234.1),
            (1234.9, 2, 1234.9),
            (-1234.9, 2, -1234.9),
            (123456789.123456789, 0, 123456789),
            (-123456789.123456789, 0, -123456789),
            (123456789.123456789, 4, 123456789.1234),
            (-123456789.123456789, 4, -123456789.1234),
            (0.1, 0, 0),
            (0.1, 10, 0.1),
            # Larger precision numbers truncating decimal places
            (int("9" * 38), 0, int("9" * 38)),
            (
                decimal.Decimal("1998849133104929799110640.00301078"),
                0,
                decimal.Decimal("1998849133104929799110640"),
            ),
            (
                decimal.Decimal("-1998849133104929799110640.00301078"),
                0,
                decimal.Decimal("-1998849133104929799110640"),
            ),
            (decimal.Decimal("9" * 37), 0, decimal.Decimal("9" * 37)),
            (decimal.Decimal("-" + "9" * 37), 0, decimal.Decimal("-" + "9" * 37)),
            (decimal.Decimal("9" * 38), 0, decimal.Decimal("9" * 38)),
            (decimal.Decimal("-" + "9" * 38), 0, decimal.Decimal("-" + "9" * 38)),
            (decimal.Decimal("0." + "1" * 37), 0, 0),
            (decimal.Decimal("-0." + "1" * 37), 0, 0),
            (decimal.Decimal("0." + "9" * 37), 0, 0),
            (decimal.Decimal("-0." + "9" * 37), 0, 0),
            (decimal.Decimal("0." + "1" * 38), 0, 0),
            (decimal.Decimal("-0." + "1" * 38), 0, 0),
            (decimal.Decimal("0." + "9" * 38), 0, 0),
            (decimal.Decimal("-0." + "9" * 38), 0, 0),
            (decimal.Decimal("0." + "1" * 37), 5, decimal.Decimal("0." + "1" * 5)),
            # Regular ints and floats truncating to left of decimal place
            (0, -2, 0),
            (1234, -2, 1200),
            (-1234, -2, -1200),
            (1234.5678, -2, 1200),
            (-1234.5678, -2, -1200),
            (1234, -10, 0),
            (-1234, -10, 0),
            (1234, -20, 0),
            (-1234, -20, 0),
            (1234.5678, -20, 0),
            (-1234.5678, -20, 0),
            # Larger precision numbers truncating to left of decimal place
            (int("9" * 38), -1, int("9" * 37 + "0")),
            (int("9" * 38), -35, int("999" + "0" * 35)),
            (
                decimal.Decimal("1998849133104929799110640.00301078"),
                -2,
                decimal.Decimal("1998849133104929799110600"),
            ),
            (
                decimal.Decimal("-1998849133104929799110640.00301078"),
                -2,
                decimal.Decimal("-1998849133104929799110600"),
            ),
            (
                decimal.Decimal("1998849133104929799110640.00301078"),
                -10,
                decimal.Decimal("1998849133104920000000000"),
            ),
            (
                decimal.Decimal("-1998849133104929799110640.00301078"),
                -10,
                decimal.Decimal("-1998849133104920000000000"),
            ),
            (decimal.Decimal("0." + "1" * 38), -20, 0),
            (decimal.Decimal("-0." + "1" * 38), -20, 0),
            (decimal.Decimal("0." + "9" * 38), -20, 0),
            (decimal.Decimal("-0." + "9" * 38), -20, 0),
        ]:
            self.assertEqual(
                truncate_number(n, digits),
                expected_result,
                f"Test input: truncate_number({n}, {digits})",
            )
            if not isinstance(n, decimal.Decimal):
                # Test same value as Decimal input
                dn = decimal.Decimal(str(n))
                dexpected = decimal.Decimal(str(expected_result))
                self.assertEqual(
                    truncate_number(dn, digits),
                    dexpected,
                    f"Test input: truncate_number({dn}, {digits})",
                )
            if digits == 0:
                self.assertEqual(
                    truncate_number(n),
                    expected_result,
                    f"Test input: truncate_number({n}, {digits})",
                )

    def test_bytes_to_human_size(self):
        self.assertEqual(bytes_to_human_size(0), "0B")
        self.assertEqual(bytes_to_human_size(1000), "1000B")
        self.assertEqual(bytes_to_human_size(1024, scale=0), "1KB")
        self.assertEqual(bytes_to_human_size(1024, scale=1), "1.0KB")
        self.assertEqual(bytes_to_human_size(1024 + 512, scale=1), "1.5KB")
        self.assertEqual(bytes_to_human_size(1024 + 256, scale=2), "1.25KB")

    def test_human_size_to_bytes(self):
        self.assertEqual(human_size_to_bytes(0), 0)
        self.assertEqual(human_size_to_bytes("0B"), 0)
        self.assertEqual(human_size_to_bytes("64B"), 64)
        self.assertEqual(human_size_to_bytes("64K"), 64 * 1024)
        self.assertEqual(human_size_to_bytes("64KB"), 64 * 1024)
        self.assertEqual(human_size_to_bytes("64M"), 64 * 1024 * 1024)
        self.assertEqual(human_size_to_bytes("64MB"), 64 * 1024 * 1024)
        self.assertEqual(human_size_to_bytes("64G"), 64 * 1024 * 1024 * 1024)
        self.assertEqual(human_size_to_bytes("64GB"), 64 * 1024 * 1024 * 1024)
        self.assertEqual(human_size_to_bytes("64B", binary_sizes=False), 64)
        self.assertEqual(human_size_to_bytes("64K", binary_sizes=False), 64 * 1000)
        self.assertEqual(human_size_to_bytes("64KB", binary_sizes=False), 64 * 1000)
        self.assertEqual(
            human_size_to_bytes("64M", binary_sizes=False), 64 * 1000 * 1000
        )
        self.assertEqual(
            human_size_to_bytes("64MB", binary_sizes=False), 64 * 1000 * 1000
        )
        self.assertEqual(
            human_size_to_bytes("64G", binary_sizes=False), 64 * 1000 * 1000 * 1000
        )
        self.assertEqual(
            human_size_to_bytes("64GB", binary_sizes=False), 64 * 1000 * 1000 * 1000
        )

    def test_is_number(self):
        self.assertTrue(is_number("0"))
        self.assertTrue(is_number("123"))
        self.assertTrue(is_number("-123"))
        self.assertTrue(is_number("123.123"))
        self.assertTrue(is_number("-123.123"))
        self.assertFalse(is_number("Hello"))
        self.assertFalse(is_number(None))

    def test_is_pos_int(self):
        self.assertFalse(is_pos_int(None))
        self.assertFalse(is_pos_int(-1))
        self.assertFalse(is_pos_int(-1, allow_zero=True))
        self.assertFalse(is_pos_int(0))
        self.assertTrue(is_pos_int(0, allow_zero=True))
        self.assertTrue(is_pos_int(1))

    def test_is_power_of_10(self):
        self.assertFalse(is_power_of_10(None))
        self.assertFalse(is_power_of_10("0"))
        self.assertFalse(is_power_of_10(0))
        self.assertFalse(is_power_of_10(0.1))
        self.assertTrue(is_power_of_10(1))
        self.assertFalse(is_power_of_10(1.1))
        self.assertFalse(is_power_of_10(1.00000001))
        self.assertFalse(is_power_of_10(2))
        self.assertTrue(is_power_of_10(10))
        self.assertFalse(is_power_of_10(12))
        self.assertTrue(is_power_of_10(1000))
        self.assertTrue(is_power_of_10(1000000))
        self.assertTrue(is_power_of_10("1" + ("0" * 10)))
        self.assertTrue(is_power_of_10(int("1" + ("0" * 10))))
        self.assertFalse(is_power_of_10("9" * 18))
        self.assertFalse(is_power_of_10(int("9" * 18)))
        self.assertTrue(is_power_of_10("1" + ("0" * 18)))
        self.assertTrue(is_power_of_10(int("1" + ("0" * 18))))
        self.assertFalse(is_power_of_10("9" * 28))
        self.assertFalse(is_power_of_10(int("9" * 28)))
        self.assertTrue(is_power_of_10("1" + ("0" * 28)))
        self.assertTrue(is_power_of_10(int("1" + ("0" * 28))))

    def test_split_not_in_quotes(self):
        assert_tuples = [
            ("a,b,c", ",", False, ["a", "b", "c"]),
            ("a b  c", r"\s", False, ["a", "b", "", "c"]),
            ("a b  c", r"\s", True, ["a", "b", "c"]),
            ("""a,"b,c",d,""", ",", False, ["a", '"b,c"', "d", ""]),
            ("""a,"b,c",d,""", ",", True, ["a", '"b,c"', "d"]),
            ("""a,'b,c',d,""", ",", False, ["a", "'b,c'", "d", ""]),
            ("""a,'b,"c d"',e""", ",", False, ["a", "'b,\"c d\"'", "e"]),
            (
                '''-Dmore.driver.prms="-Dthis='this and that'" "-Dextra.driver.prms=-Dthis='this and that'"''',
                r"\s",
                False,
                [
                    '''-Dmore.driver.prms="-Dthis='this and that'"''',
                    '''"-Dextra.driver.prms=-Dthis='this and that'"''',
                ],
            ),
        ]
        for to_split, sep, exclude_empty_tokens, should_match in assert_tuples:
            self.assertEqual(
                split_not_in_quotes(
                    to_split, sep, exclude_empty_tokens=exclude_empty_tokens
                ),
                should_match,
            )

    def test_get_temp_path(self):
        self.assertIsInstance(get_temp_path(), str)
        self.assertTrue(get_temp_path().startswith("/tmp"))
        self.assertTrue(get_temp_path(suffix=".txt").endswith(".txt"))
        self.assertTrue(get_temp_path(suffix="txt").endswith(".txt"))
        self.assertTrue(get_temp_path(tmp_dir="/temp").startswith("/temp"))
        self.assertTrue(
            get_temp_path(tmp_dir="/temp", prefix="unit").startswith("/temp/unit")
        )
        self.assertTrue(
            get_temp_path(prefix="unit", suffix=".tmp").startswith("/tmp/unit")
        )
        self.assertTrue(
            get_temp_path(tmp_dir="/temp", prefix="unit", suffix=".tmp").endswith(
                ".tmp"
            )
        )

    def test_format_list_for_logging(self):
        test_input = [("Header1", "LongerHeader"), ("Blah", 123)]
        expected_output1 = dedent(
            """\
            Header1 LongerHeader
            ======= ============
            Blah             123"""
        )
        self.assertEqual(format_list_for_logging(test_input), expected_output1)
        expected_output2 = dedent(
            """\
            Header1 LongerHeader
            ------- ------------
            Blah             123"""
        )
        self.assertEqual(
            format_list_for_logging(test_input, underline_char="-"), expected_output2
        )

    def test_remove_chars(self):
        self.assertIsNone(
            remove_chars(None, "ace"),
        )
        self.assertEqual(remove_chars("ABCDEF", "ace"), "ABCDEF")
        self.assertEqual(remove_chars("abcdef", "ace"), "bdf")
        self.assertEqual(remove_chars("abcdef", ""), "abcdef")

    def test_trunc_with_hash(self):
        test_string = "xyz".ljust(20, "X")
        # Length 20 string is trimmed to 10
        self.assertEqual(len(trunc_with_hash(test_string, 4, 10)), 10)
        # Length 10 output with 4 hash chars has 5 of original value then underscore
        self.assertTrue(
            trunc_with_hash(test_string, 4, 10).startswith(test_string[:5] + "_")
        )
        # Length 8 output with 4 hash chars has 3 of original value then underscore
        self.assertTrue(
            trunc_with_hash(test_string, 4, 8).startswith(test_string[:3] + "_")
        )
        # Length 20 string is trimmed to 10 with different number of hash chars
        self.assertEqual(len(trunc_with_hash(test_string, 6, 10)), 10)
        # Length 10 output with 6 hash chars has 3 of original value then underscore
        self.assertTrue(
            trunc_with_hash(test_string, 6, 10).startswith(test_string[:3] + "_")
        )
        # String shorter than max length is unaffected
        self.assertEqual(
            trunc_with_hash(test_string, 4, len(test_string) + 1), test_string
        )
        # String equal to max length is unaffected
        self.assertEqual(trunc_with_hash(test_string, 4, len(test_string)), test_string)

    def test_standard_file_name(self):
        std_prefix = "offload_db_name.table_name"
        self.assertTrue(standard_file_name(std_prefix).startswith(std_prefix))
        self.assertEqual(
            standard_file_name(std_prefix, extension=".txt"), std_prefix + ".txt"
        )
        self.assertEqual(
            standard_file_name(std_prefix, extension=".log"), std_prefix + ".log"
        )
        self.assertEqual(standard_file_name(std_prefix), std_prefix)
        self.assertEqual(
            standard_file_name(std_prefix, name_suffix="more", extension=".txt"),
            std_prefix + "_more.txt",
        )
        # The timestamp will add 22 characters, plus 4 for the extension and 5 for the hash, therefore trim above 31.
        self.assertEqual(
            len(standard_file_name(std_prefix, max_name_length=45, with_datetime=True)),
            45,
        )
        self.assertEqual(
            len(
                standard_file_name(
                    std_prefix,
                    max_name_length=45,
                    with_datetime=True,
                    max_name_length_extra_slack=5,
                )
            ),
            40,
        )
        # Length 50 output will have at least 10 characters of the original name
        self.assertTrue(
            standard_file_name(
                std_prefix, max_name_length=45, with_datetime=True
            ).startswith(std_prefix[:10])
        )
        # Length 50 output will NOT have 20 characters of the original name
        self.assertFalse(
            standard_file_name(
                std_prefix, max_name_length=45, with_datetime=True
            ).startswith(std_prefix[:20])
        )
        # No slashes in file name and other bad inputs
        self.assertRaises(AssertionError, lambda: standard_file_name("file/name"))
        self.assertRaises(AssertionError, lambda: standard_file_name(123))
        self.assertRaises(AssertionError, lambda: standard_file_name(None))
        self.assertRaises(
            AssertionError, lambda: standard_file_name("file_name", extension=None)
        )
        self.assertRaises(
            AssertionError, lambda: standard_file_name("file_name", extension=123)
        )
        self.assertRaises(
            AssertionError, lambda: standard_file_name("file_name", name_suffix=None)
        )
        self.assertRaises(
            AssertionError, lambda: standard_file_name("file_name", name_suffix=123)
        )

    def test_standard_log_name(self):
        std_prefix = "offload_db_name.table_name"
        self.assertTrue(standard_log_name(std_prefix).startswith(std_prefix))
        self.assertTrue(standard_log_name(std_prefix).endswith(".log"))

    def test_wildcard_matches_in_list(self):
        list_of_names = [
            "UPPER_ID",
            "lower_id",
            "extra_after_idx",
            "UPPER_YEAR",
            "lower_year",
            "lower2_year",
            "UPPER_BOTH_EXTENSIONS_ID_YEAR",
            "QUESTION_MARK_?_",
            "DOT_._",
        ]
        self.assertListEqual(wildcard_matches_in_list("_id", list_of_names), [])
        self.assertListEqual(
            wildcard_matches_in_list("*_id", list_of_names), ["lower_id"]
        )
        self.assertListEqual(
            wildcard_matches_in_list("*_id", list_of_names, case_sensitive=False),
            ["UPPER_ID", "lower_id"],
        )
        self.assertListEqual(
            wildcard_matches_in_list("*_year", list_of_names),
            ["lower_year", "lower2_year"],
        )
        self.assertEqual(
            len(
                wildcard_matches_in_list("*_year", list_of_names, case_sensitive=False)
            ),
            4,
        )
        self.assertEqual(
            len(wildcard_matches_in_list("*_*", list_of_names)), len(list_of_names)
        )
        self.assertEqual(
            len(wildcard_matches_in_list("*_*", list_of_names, case_sensitive=False)),
            len(list_of_names),
        )
        # Ensure other wildcards do not have an effect
        self.assertListEqual(wildcard_matches_in_list("lower?id", list_of_names), [])
        self.assertListEqual(wildcard_matches_in_list(".*id", list_of_names), [])
        self.assertListEqual(
            wildcard_matches_in_list("*?*", list_of_names), ["QUESTION_MARK_?_"]
        )
        self.assertListEqual(wildcard_matches_in_list("*.*", list_of_names), ["DOT_._"])


if __name__ == "__main__":
    main()
