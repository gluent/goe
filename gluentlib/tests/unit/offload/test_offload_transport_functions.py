""" TestOffloadTransportFunctions: Unit test library to test global functions defined in offload_transport_functions.py
"""
from unittest import TestCase, main

from gluentlib.offload.offload_transport_functions import running_as_same_user_and_host, offload_transport_file_name,\
    split_lists_for_id_list, split_ranges_for_id_range
from gluentlib.util.misc_functions import get_os_username, LINUX_FILE_NAME_LENGTH_LIMIT


class TestOffloadTransportFunctions(TestCase):

    def running_as_same_user_and_host(self):
        self.assertNotIn('ssh', running_as_same_user_and_host(get_os_username(), 'localhost'))
        self.assertIn('ssh', running_as_same_user_and_host(get_os_username(), 'other-host'))
        self.assertIn('ssh', running_as_same_user_and_host('other-user', 'localhost'))

    def test_offload_transport_file_name(self):
        std_prefix = 'db_name.table_name'
        self.assertEqual(offload_transport_file_name(std_prefix), std_prefix)
        self.assertTrue(len(offload_transport_file_name(std_prefix, extension='.dat')) < LINUX_FILE_NAME_LENGTH_LIMIT)

        long_prefix = 'db_name'.ljust(128, 'x') + '.' + 'table_name'.ljust(128, 'x')
        # long_prefix would need to be truncated
        self.assertNotEqual(offload_transport_file_name(long_prefix), long_prefix)
        self.assertTrue(offload_transport_file_name(long_prefix).startswith(long_prefix[:200]))
        # long_prefix would need to be truncated but extension still honoured
        self.assertTrue(offload_transport_file_name(long_prefix).startswith(long_prefix[:200]))
        self.assertFalse(offload_transport_file_name(long_prefix, extension='').endswith('.dat'))
        self.assertTrue(len(offload_transport_file_name(long_prefix, extension='')) == LINUX_FILE_NAME_LENGTH_LIMIT)
        self.assertTrue(offload_transport_file_name(long_prefix, extension='.dat').endswith('.dat'))
        self.assertTrue(len(offload_transport_file_name(long_prefix, extension='.dat')) == LINUX_FILE_NAME_LENGTH_LIMIT)
        self.assertRaises(AssertionError, lambda: offload_transport_file_name('file/name'))
        self.assertRaises(AssertionError, lambda: offload_transport_file_name(123))
        self.assertRaises(AssertionError, lambda: offload_transport_file_name(None))
        self.assertRaises(AssertionError, lambda: offload_transport_file_name('file_name', extension=None))
        self.assertRaises(AssertionError, lambda: offload_transport_file_name('file_name', extension=123))

    def test_split_ranges_for_id_range(self):
        test_data = [
            # Simple range of 1-10 with assorted parallelism
            ((1, 10, 1),
             [(1, 11)]),
            ((1, 10, 2),
             [(1, 6), (6, 11)]),
            ((1, 10, 5),
             [(1, 3), (3, 5), (5, 7), (7, 9), (9, 11)]),
            # Range with min == max and assorted parallelism
            ((1, 1, 1),
             [(1, 2)]),
            ((1, 1, 2),
             [(1, 1.5), (1.5, 2)]),
            ((0, 0, 1),
             [(0, 1)]),
            ((0, 0, 4),
             [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1)]),
            # Range smaller than parallelism - what should we do?
            ((1, 2, 4),
             [(1, 1.5), (1.5, 2), (2, 2.5), (2.5, 3)]),
            # Bigger inputs that overflow float
            ((9999999999999991, 9999999999999999, 1),
             [(9999999999999991, 10000000000000000)]),
            ((int(('9' * 30) + '000'), int(('9' * 30) + '999'), 4),
             [(int(('9' * 30) + '000'), int(('9' * 30) + '250')), (int(('9' * 30) + '250'), int(('9' * 30) + '500')),
              (int(('9' * 30) + '500'), int(('9' * 30) + '750')), (int(('9' * 30) + '750'), int('1' + ('0' * 30) + '000'))
              ]),
        ]
        for inputs, expected_result in test_data:
            result = split_ranges_for_id_range(*inputs)
            self.assertEqual(result, expected_result,
                             f'Inputs: min={inputs[0]}, max={inputs[1]}, parallel={inputs[2]}')
            for i in range(inputs[0], inputs[1]+1):
                # Assert that for each integer between min/max there is only a single capturing range
                capturing_ranges = [_ for _ in result if _[0] <= i < _[1]]
                self.assertEqual(len(capturing_ranges), 1,
                                 f'Value {i} in capturing ranges = {capturing_ranges}')

    def test_split_lists_for_id_list(self):
        test_data = [
            # Round robin, CSV=False
            [[8, 4, 1, 3, 6, 9], 1, True, False, [[8, 4, 1, 3, 6, 9]]],
            [[8, 4, 1, 3, 6, 9], 2, True, False, [[8, 1, 6], [4, 3, 9]]],
            [[8, 4, 1, 3, 6, 9], 3, True, False, [[8, 3], [4, 6], [1, 9]]],
            [[8, 4, 1, 3, 6], 1, True, False, [[8, 4, 1, 3, 6]]],
            [[8, 4, 1, 3, 6], 2, True, False, [[8, 1, 6], [4, 3]]],
            [[8, 4, 1, 3, 6], 3, True, False, [[8, 3], [4, 6], [1]]],
            [[8], 1, True, False, [[8]]],
            [[8], 2, True, False, [[8], []]],
            [[8], 3, True, False, [[8], [], []]],
            # Round robin, CSV=True
            [[8, 4, 1, 3, 6, 9], 1, True, True, ['8,4,1,3,6,9']],
            [[8, 4, 1, 3, 6, 9], 2, True, True, ['8,1,6', '4,3,9']],
            [[8, 4, 1, 3, 6, 9], 3, True, True, ['8,3', '4,6', '1,9']],
            [[8, 4, 1, 3, 6], 1, True, True, ['8,4,1,3,6']],
            [[8, 4, 1, 3, 6], 2, True, True, ['8,1,6', '4,3']],
            [[8, 4, 1, 3, 6], 3, True, True, ['8,3', '4,6', '1']],
            [[8], 1, True, True, ['8']],
            [[8], 2, True, True, ['8', None]],
            [[8], 3, True, True, ['8', None, None]],
            # Contiguous, CSV=False
            [[8, 4, 1, 3, 6, 9], 1, False, False, [[8, 4, 1, 3, 6, 9]]],
            [[8, 4, 1, 3, 6, 9], 2, False, False, [[8, 4, 1], [3, 6, 9]]],
            [[8, 4, 1, 3, 6, 9], 3, False, False, [[8, 4], [1, 3], [6, 9]]],
            [[8, 4, 1, 3, 6], 1, False, False, [[8, 4, 1, 3, 6]]],
            [[8, 4, 1, 3, 6], 2, False, False, [[8, 4, 1], [3, 6]]],
            [[8, 4, 1, 3, 6], 3, False, False, [[8, 4], [1, 3], [6]]],
            [[8], 1, False, False, [[8]]],
            [[8], 2, False, False, [[8], []]],
            [[8], 3, False, False, [[8], [], []]],
        ]
        for input_list, parallelism, round_robin, csv, expected_result in test_data:
            result = split_lists_for_id_list(input_list, parallelism, round_robin=round_robin, as_csvs=csv)
            self.assertEqual(result, expected_result,
                             f'Input: ids={input_list}, parallel={parallelism}, round_robin={round_robin}, csv={csv}')


if __name__ == '__main__':
    main()
