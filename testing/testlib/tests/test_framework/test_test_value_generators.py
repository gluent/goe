from decimal import Decimal
from unittest import TestCase, main
from gluentlib.util.misc_functions import remove_chars
from testlib.test_framework.test_value_generators import TestDecimal


class TestTestValueGenerators(TestCase):

    def test_decimal(self):
        self.assertEqual(TestDecimal.max(1), 9)
        self.assertEqual(TestDecimal.min(2), -99)
        self.assertEqual(TestDecimal.max(10, 1), Decimal('999999999.9'))
        self.assertEqual(TestDecimal.min(10, 10), Decimal('-0.9999999999'))
        self.assertEqual(TestDecimal.max(38, 0), int('9' * 38))
        self.assertEqual(TestDecimal.min(38, 38), Decimal('-0.' + ('9' * 38)))

        self.assertIsInstance(TestDecimal.rnd(1), Decimal)
        self.assertEqual(len(remove_chars(str(TestDecimal.rnd(1)), '-')), 1)
        self.assertIsInstance(TestDecimal.rnd(2), Decimal)
        self.assertTrue(len(remove_chars(str(TestDecimal.rnd(2)), '-')) <= 2)
        self.assertIsInstance(TestDecimal.rnd(10, 1), Decimal)
        dec_10_1 = str(TestDecimal.rnd(10, 1))
        self.assertTrue(len(remove_chars(dec_10_1.split('.')[0], '-')) <= 9)
        self.assertEqual(len(dec_10_1.split('.')[1]), 1)
        dec_10_10 = TestDecimal.rnd(10, 10)
        self.assertIsInstance(dec_10_10, Decimal)
        self.assertEqual(remove_chars(str(dec_10_10).split('.')[0], '-'), '0')
        self.assertEqual(len(str(dec_10_10).split('.')[1]), 10)
        self.assertIsInstance(TestDecimal.rnd(38, 0), Decimal)
        self.assertTrue(len(remove_chars(str(TestDecimal.rnd(38, 0)), '-.')) <= 38)
        self.assertIsInstance(TestDecimal.rnd(38, 38), Decimal)
        # Expected length 39 below to allow for leading 0.
        self.assertEqual(len(remove_chars(str(TestDecimal.rnd(38, 38)), '-.')), 39)


if __name__ == '__main__':
    main()
