import os

from goe.offload.offload_constants import DBTYPE_BIGQUERY
from tests.testlib.test_framework import test_constants

# Per table defaults for synthetic_partition_digits
OFFLOAD_CUSTOM_DIGITS = {'gl_range_bigint': '18',
                         test_constants.PART_RANGE_TABLE_BIG_INTS1: '20',
                         test_constants.PART_RANGE_TABLE_BIG_INTS2: '20',
                         test_constants.PART_RANGE_TABLE_DEC_INTS1: '20',
                         test_constants.PART_RANGE_TABLE_DEC_INTS2: '20',
                         test_constants.PART_RANGE_TABLE_DEC_INTS3: '38',
                         test_constants.PART_RANGE_TABLE_DEC_INTS4: '38',
                         test_constants.PART_RANGE_TABLE_BIG_DECS1: '38',
                         test_constants.PART_RANGE_TABLE_BIG_DECS2: '38'}

PART_RANGE_GRANULARITIES = {
    test_constants.PART_RANGE_TABLE_BIG_INTS1: '1' + ('0' * 15),
    test_constants.PART_RANGE_TABLE_BIG_INTS2: str(2**50),
    test_constants.PART_RANGE_TABLE_DEC_INTS1: '1' + ('0' * 15),
    test_constants.PART_RANGE_TABLE_DEC_INTS2: str(2**50),
    test_constants.PART_RANGE_TABLE_DEC_INTS3: '1' + ('0' * 35),
    test_constants.PART_RANGE_TABLE_DEC_INTS4: '1000',
    test_constants.PART_RANGE_TABLE_BIG_DECS1:
        ('1' + ('0' * 15)) if os.environ.get('QUERY_ENGINE', '').lower() == DBTYPE_BIGQUERY else '1000000000',
    test_constants.PART_RANGE_TABLE_BIG_DECS2:
        str(2**50) if os.environ.get('QUERY_ENGINE', '').lower() == DBTYPE_BIGQUERY else str(2**20),
}
# These are needed for BigQuery to try and avoid range partitioning limit of 10000
PART_RANGE_LOWER_VALUES = {test_constants.PART_RANGE_TABLE_BIG_INTS1: '1' + ('0' * 15),
                           test_constants.PART_RANGE_TABLE_BIG_INTS2: '1' + ('0' * 15),
                           test_constants.PART_RANGE_TABLE_DEC_INTS1: '1' + ('0' * 15),
                           test_constants.PART_RANGE_TABLE_DEC_INTS2: '1' + ('0' * 15),
                           test_constants.PART_RANGE_TABLE_DEC_INTS3: '1' + ('0' * 35),
                           test_constants.PART_RANGE_TABLE_DEC_INTS4: '1' + ('0' * 35),
                           test_constants.PART_RANGE_TABLE_BIG_DECS1: '1' + ('0' * 15),
                           test_constants.PART_RANGE_TABLE_BIG_DECS2: '1' + ('0' * 15)}
PART_RANGE_UPPER_VALUES = {}

PART_RANGE_PART_FUNCTIONS = {test_constants.PART_RANGE_TABLE_UNUM: test_constants.PARTITION_FUNCTION_TEST_FROM_INT8,
                             test_constants.PART_RANGE_TABLE_UDEC: test_constants.PARTITION_FUNCTION_TEST_FROM_DEC1,
                             test_constants.PART_RANGE_TABLE_UDEC2: test_constants.PARTITION_FUNCTION_TEST_FROM_DEC2,
                             test_constants.PART_RANGE_TABLE_USTR: test_constants.PARTITION_FUNCTION_TEST_FROM_STRING}
