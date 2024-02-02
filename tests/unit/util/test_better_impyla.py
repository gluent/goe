""" TestBetterImpyla: Unit test library to test functions from better_impyla module.
    BetterImpyla class is tested via BackendApi therefore this is about standalone routines.
"""
from unittest import TestCase, main
from tests.unit.test_functions import optional_hadoop_dependency_exception

try:
    from goe.util.better_impyla import BetterImpylaException, from_impala_size
except ModuleNotFoundError as e:
    if optional_hadoop_dependency_exception(e):
        from_impala_size = None
    else:
        raise


class TestBetterImpyla(TestCase):
    def test_from_impala_size(self):
        if not from_impala_size:
            return

        test_tuples = [
            ("0B", 0),
            ("0.3B", 0),
            ("18790B", 18790),
            ("18.35KB", 18790),
            ("18KB", 18 * 1024),
            ("18.35MB", round(18.35 * 1024 * 1024)),
            ("18MB", 18 * 1024 * 1024),
            ("18.35GB", round(18.35 * 1024 * 1024 * 1024)),
            ("18GB", 18 * 1024 * 1024 * 1024),
            ("18.35TB", round(18.35 * 1024 * 1024 * 1024 * 1024)),
            ("18TB", 18 * 1024 * 1024 * 1024 * 1024),
        ]
        for str_size, byte_size in test_tuples:
            self.assertEqual(
                from_impala_size(str_size), byte_size, f"Input: {str_size}"
            )
        for bad_input in [None, ""]:
            self.assertRaises(AssertionError, lambda: from_impala_size(bad_input))
        for bad_input in ["0", "12345"]:
            self.assertRaises(
                BetterImpylaException, lambda: from_impala_size(bad_input)
            )


if __name__ == "__main__":
    main()
