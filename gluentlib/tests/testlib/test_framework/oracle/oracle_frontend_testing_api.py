#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" OracleFrontendTestingApi: An extension of (not yet created) FrontendApi used purely for code relating to the setup,
    processing and verification of integration tests.
    LICENSE_TEXT
"""

import datetime
import logging
import random
import re
from textwrap import dedent
from typing import Optional, Union

from gluentlib.offload.column_metadata import CanonicalColumn,\
    CANONICAL_CHAR_SEMANTICS_UNICODE, GLUENT_TYPE_BINARY, GLUENT_TYPE_DATE, GLUENT_TYPE_DECIMAL,\
    GLUENT_TYPE_DOUBLE, GLUENT_TYPE_FIXED_STRING, GLUENT_TYPE_FLOAT,\
    GLUENT_TYPE_INTEGER_1, GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8, GLUENT_TYPE_INTEGER_38,\
    GLUENT_TYPE_INTERVAL_DS, GLUENT_TYPE_INTERVAL_YM, GLUENT_TYPE_LARGE_BINARY, GLUENT_TYPE_LARGE_STRING,\
    GLUENT_TYPE_TIME, GLUENT_TYPE_TIMESTAMP, GLUENT_TYPE_TIMESTAMP_TZ, GLUENT_TYPE_VARIABLE_STRING, match_table_column
from gluentlib.offload.offload_messages import VERBOSE, VVERBOSE
from gluentlib.offload.oracle.oracle_column import OracleColumn, \
    ORACLE_TYPE_CHAR, ORACLE_TYPE_NCHAR, ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB, \
    ORACLE_TYPE_VARCHAR, ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2, \
    ORACLE_TYPE_RAW, ORACLE_TYPE_BLOB, ORACLE_TYPE_NUMBER, ORACLE_TYPE_FLOAT, \
    ORACLE_TYPE_BINARY_FLOAT, ORACLE_TYPE_BINARY_DOUBLE, ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP, \
    ORACLE_TYPE_TIMESTAMP_TZ, ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM
from gluentlib.offload.teradata.teradata_column import TERADATA_TYPE_TIMESTAMP_TZ
from tests.testlib.setup import gen_test_data
from tests.testlib.test_framework.test_constants import TEST_GEN_DATA_ASCII7_NONULL, UNICODE_NAME_TOKEN
from tests.testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface
from tests.testlib.test_framework.test_value_generators import TestDecimal
from test_sets.stories.story_setup_functions import SALES_BASED_FACT_PRE_HV, SALES_BASED_FACT_HV_1, \
    SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3, SALES_BASED_FACT_HV_4, SALES_BASED_FACT_HV_5, \
    SALES_BASED_FACT_HV_6, SALES_BASED_FACT_HV_7, \
    SALES_BASED_FACT_PRE_HV_NUM, SALES_BASED_FACT_HV_1_NUM, SALES_BASED_FACT_HV_2_NUM, SALES_BASED_FACT_HV_3_NUM, \
    SALES_BASED_FACT_HV_4_NUM, SALES_BASED_FACT_HV_5_NUM, SALES_BASED_FACT_HV_6_NUM, SALES_BASED_FACT_HV_7_NUM, \
    SALES_BASED_LIST_PRE_HV, SALES_BASED_LIST_PNAME_0, SALES_BASED_LIST_HV_1, SALES_BASED_LIST_PNAME_1, \
    SALES_BASED_LIST_HV_2, SALES_BASED_LIST_PNAME_2, SALES_BASED_LIST_HV_3, SALES_BASED_LIST_PNAME_3, \
    SALES_BASED_LIST_HV_4, SALES_BASED_LIST_PNAME_4, SALES_BASED_LIST_HV_5, SALES_BASED_LIST_PNAME_5, \
    SALES_BASED_LIST_HV_6, SALES_BASED_LIST_PNAME_6, SALES_BASED_LIST_HV_7, SALES_BASED_LIST_PNAME_7


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# CONSTANTS
###############################################################################

def lob_to_hash(column_type, column_name):
    if column_type == ORACLE_TYPE_BLOB:
        # literal 3 below = DBMS_CRYPTO.HASH_SH1
        return 'DBMS_CRYPTO.HASH(COALESCE(%s, empty_blob()), 3)' % column_name
    elif column_type in (ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB):
        return 'DBMS_CRYPTO.HASH(COALESCE(%s, empty_clob()), 3)' % column_name
    else:
        raise NotImplementedError('lob_to_hash for %s not supported' % column_type)


###########################################################################
# OracleFrontendTestingApi
###########################################################################

class OracleFrontendTestingApi(FrontendTestingApiInterface):

    def __init__(self, frontend_type, connection_options, messages, existing_connection=None, dry_run=False,
                 do_not_connect=False, trace_action=None):
        """ CONSTRUCTOR
        """
        super().__init__(frontend_type, connection_options, messages,
                         existing_connection=existing_connection, dry_run=dry_run, do_not_connect=do_not_connect,
                         trace_action=trace_action)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _create_new_testing_client(self, existing_connection,
                                   trace_action_override=None) -> FrontendTestingApiInterface:
        return OracleFrontendTestingApi(self._frontend_type, self._connection_options, self._messages,
                                        existing_connection=existing_connection,
                                        dry_run=self._dry_run, trace_action=trace_action_override)

    def _data_type_supports_precision_and_scale(self, column_or_type):
        if isinstance(column_or_type, OracleColumn):
            data_type = column_or_type.data_type
        else:
            data_type = column_or_type
        return bool(data_type == ORACLE_TYPE_NUMBER)

    def _gen_column_data(self, column, row_index, from_list=None, ascii_only=False, all_chars_notnull=False,
                         ordered=False, no_newlines=False, allow_nan=True, allow_inf=True):
        assert isinstance(column, OracleColumn)
        if column.data_type in [ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_VARCHAR, ORACLE_TYPE_NVARCHAR2]:
            return gen_test_data.gen_varchar(row_index, column.char_length or column.data_length, from_list=from_list,
                                             ordered=ordered, ascii7_only=ascii_only, notnull=all_chars_notnull,
                                             no_newlines=no_newlines)
        elif column.data_type in [ORACLE_TYPE_CHAR, ORACLE_TYPE_NCHAR]:
            return gen_test_data.gen_char(row_index, column.char_length or column.data_length, from_list=from_list,
                                          ordered=ordered, ascii7_only=ascii_only, notnull=all_chars_notnull,
                                          no_newlines=no_newlines)
        elif column.data_type == ORACLE_TYPE_NUMBER:
            return gen_test_data.gen_number(row_index, precision=column.data_precision, scale=column.data_scale,
                                            from_list=from_list)
        elif column.data_type == ORACLE_TYPE_DATE:
            return gen_test_data.gen_datetime(row_index, from_list=from_list, ordered=ordered)
        elif column.data_type in [ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ]:
            return gen_test_data.gen_timestamp(row_index, from_list=from_list, ordered=ordered)
        elif column.data_type == ORACLE_TYPE_INTERVAL_YM:
            return gen_test_data.gen_interval_ym(precision=column.data_precision)
        elif column.data_type == ORACLE_TYPE_INTERVAL_DS:
            return gen_test_data.gen_interval_ds(precision=column.data_precision, scale=column.data_scale)
        elif column.data_type == ORACLE_TYPE_BINARY_DOUBLE:
            return gen_test_data.gen_float(row_index, from_list=from_list, allow_nan=allow_nan, allow_inf=allow_inf)
        elif column.data_type == ORACLE_TYPE_BINARY_FLOAT:
            return gen_test_data.gen_float(row_index, from_list=from_list, allow_nan=allow_nan, allow_inf=allow_inf)
        elif column.data_type == ORACLE_TYPE_FLOAT:
            return gen_test_data.gen_number(row_index, from_list=from_list)
        elif column.data_type in (ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB):
            return gen_test_data.gen_varchar(row_index, 32767, from_list=from_list, no_newlines=no_newlines)
        elif column.data_type == ORACLE_TYPE_BLOB:
            return gen_test_data.gen_varchar(row_index, 32767, from_list=from_list, no_newlines=no_newlines)
        elif column.data_type == ORACLE_TYPE_RAW:
            if column.data_length == 16:
                return gen_test_data.gen_uuid(row_index)
            elif column.data_length:
                return gen_test_data.gen_bytes(row_index, column.data_length)
            else:
                return gen_test_data.gen_bytes(row_index, 2000)
        else:
            self._log(f'Attempt to generate data for unsupported RDBMS type: {column.data_type}')

    def _gl_chars_column_definitions(self, ascii_only=False, all_chars_notnull=False,
                                     supported_canonical_types=None) -> list:
        name_id = 0

        def name():
            nonlocal name_id
            name_id = name_id + 1
            col_name = f'COLUMN_{name_id}'
            return col_name

        column_list = [
            {'column': self._id_column(),
             'ordered': True},
            {'column': OracleColumn(name(), ORACLE_TYPE_CHAR, data_length=100),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull,
             'ordered': True},
            {'column': OracleColumn(name(), ORACLE_TYPE_NCHAR, char_length=100),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull,
             'ordered': True},
        ]

        return column_list

    def _gl_type_mapping_column_definitions(self, max_backend_precision, max_backend_scale,
                                            max_decimal_integral_magnitude, ascii_only=False, all_chars_notnull=False,
                                            supported_canonical_types=None, filter_column=None):
        """ Returns a dict of dicts defining columns for GL_TYPE_MAPPING test table.
            Individual backends have an expected_canonical_to_backend_type_map() method defining which of these columns,
            by expected canonical type, are to be included for that implementation.
            filter_column can be used to fetch just a single column dict.
        """
        def name(*args):
            assert args
            col_name = 'COL_' + '_'.join(args).replace(' ', '_')
            return col_name

        generic_scale = min(max_backend_scale, 18)
        max_precision = min(max_backend_precision, 38)
        all_columns = {
            name(ORACLE_TYPE_BINARY_DOUBLE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_BINARY_DOUBLE), ORACLE_TYPE_BINARY_DOUBLE),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_BINARY_DOUBLE), GLUENT_TYPE_DOUBLE)},
            name(ORACLE_TYPE_BINARY_FLOAT):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_BINARY_FLOAT), ORACLE_TYPE_BINARY_FLOAT),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_BINARY_FLOAT), GLUENT_TYPE_FLOAT)},
            name(ORACLE_TYPE_BINARY_FLOAT, GLUENT_TYPE_DOUBLE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_BINARY_FLOAT, GLUENT_TYPE_DOUBLE), ORACLE_TYPE_BINARY_FLOAT),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_BINARY_FLOAT, GLUENT_TYPE_DOUBLE), GLUENT_TYPE_DOUBLE),
                 'offload_options': {'double_columns_csv': name(ORACLE_TYPE_BINARY_FLOAT, GLUENT_TYPE_DOUBLE)},
                 'literals': [1.5, 2.5, 3.5]},
            name(ORACLE_TYPE_BLOB):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_BLOB), ORACLE_TYPE_BLOB),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_BLOB), GLUENT_TYPE_LARGE_BINARY)},
            name(ORACLE_TYPE_CHAR):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_CHAR), ORACLE_TYPE_CHAR, data_length=3),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_CHAR), GLUENT_TYPE_FIXED_STRING),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_CHAR, UNICODE_NAME_TOKEN):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_CHAR, UNICODE_NAME_TOKEN), ORACLE_TYPE_CHAR, data_length=3),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_CHAR, UNICODE_NAME_TOKEN), GLUENT_TYPE_FIXED_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'offload_options': {'unicode_string_columns_csv': name(ORACLE_TYPE_CHAR, UNICODE_NAME_TOKEN)},
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_CLOB):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_CLOB), ORACLE_TYPE_CLOB),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_CLOB), GLUENT_TYPE_LARGE_STRING),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_CLOB, UNICODE_NAME_TOKEN):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_CLOB, UNICODE_NAME_TOKEN), ORACLE_TYPE_CLOB),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_CLOB, UNICODE_NAME_TOKEN), GLUENT_TYPE_LARGE_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'offload_options': {'unicode_string_columns_csv': name(ORACLE_TYPE_CLOB, UNICODE_NAME_TOKEN)},
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_DATE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_DATE), ORACLE_TYPE_DATE),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_DATE), GLUENT_TYPE_TIMESTAMP)},
            name(ORACLE_TYPE_DATE, GLUENT_TYPE_DATE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_DATE, GLUENT_TYPE_DATE), ORACLE_TYPE_DATE),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_DATE, GLUENT_TYPE_DATE), GLUENT_TYPE_DATE),
                 'offload_options': {'date_columns_csv': name(ORACLE_TYPE_DATE, GLUENT_TYPE_DATE)},
                 'literals': [datetime.date(1970, 1, 1), datetime.date(1970, 1, 2),
                              datetime.date(1971, 2, 1), datetime.date(1971, 2, 2)]},
            name(ORACLE_TYPE_DATE, GLUENT_TYPE_VARIABLE_STRING):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_DATE, GLUENT_TYPE_VARIABLE_STRING), ORACLE_TYPE_DATE),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_DATE, GLUENT_TYPE_VARIABLE_STRING), GLUENT_TYPE_VARIABLE_STRING),
                 'offload_options': {'variable_string_columns_csv': name(ORACLE_TYPE_DATE,
                                                                         GLUENT_TYPE_VARIABLE_STRING)}},
            name(ORACLE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP_TZ):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP_TZ), ORACLE_TYPE_DATE),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP_TZ), GLUENT_TYPE_TIMESTAMP_TZ),
                 'offload_options': {
                     'timestamp_tz_columns_csv': name(ORACLE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP_TZ)}},
            name(ORACLE_TYPE_FLOAT):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_FLOAT), ORACLE_TYPE_FLOAT),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_FLOAT), GLUENT_TYPE_DECIMAL)},
            name(ORACLE_TYPE_FLOAT, GLUENT_TYPE_DOUBLE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_FLOAT, GLUENT_TYPE_DOUBLE), ORACLE_TYPE_FLOAT),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_FLOAT, GLUENT_TYPE_DOUBLE), GLUENT_TYPE_DOUBLE),
                 'offload_options': {'double_columns_csv': name(ORACLE_TYPE_FLOAT, GLUENT_TYPE_DOUBLE)},
                 'literals': [1.5, 2.5, 3.5]},
            name(ORACLE_TYPE_INTERVAL_DS):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_INTERVAL_DS), ORACLE_TYPE_INTERVAL_DS,
                                  data_precision=9, data_scale=9),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_INTERVAL_DS), GLUENT_TYPE_INTERVAL_DS)},
            name(ORACLE_TYPE_INTERVAL_YM):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_INTERVAL_YM), ORACLE_TYPE_INTERVAL_YM, data_precision=9),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_INTERVAL_YM), GLUENT_TYPE_INTERVAL_YM)},
            name(ORACLE_TYPE_NCHAR):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NCHAR), ORACLE_TYPE_NCHAR, char_length=3),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NCHAR), GLUENT_TYPE_FIXED_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_NCLOB):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NCLOB), ORACLE_TYPE_NCLOB),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NCLOB), GLUENT_TYPE_LARGE_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_NUMBER):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER), GLUENT_TYPE_DECIMAL)},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_1):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_1), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_1), GLUENT_TYPE_INTEGER_1),
                 'offload_options': {'integer_1_columns_csv': name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_1)},
                 'literals': [TestDecimal.min(2), TestDecimal.rnd(2), TestDecimal.max(2)]},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_2):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_2), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_2), GLUENT_TYPE_INTEGER_2),
                 'offload_options': {'integer_2_columns_csv': name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_2)},
                 'literals': [TestDecimal.min(4), TestDecimal.rnd(4), TestDecimal.max(4)]},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_4):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_4), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_4), GLUENT_TYPE_INTEGER_4),
                 'offload_options': {'integer_4_columns_csv': name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_4)},
                 'literals': [TestDecimal.min(9), TestDecimal.rnd(9), TestDecimal.max(9)]},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_8):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_8), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_8), GLUENT_TYPE_INTEGER_8),
                 'offload_options': {'integer_8_columns_csv': name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_8)},
                 'literals': [TestDecimal.min(18), TestDecimal.rnd(18), TestDecimal.max(18)]},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_38):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_38), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_38), GLUENT_TYPE_INTEGER_38),
                 'offload_options': {'integer_38_columns_csv': name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_INTEGER_38)},
                 # 'test' imposes a max precision of 35, I think due to shortcomings of cx-Oracle.
                 # We impose the same limit here to ensure no loss of value accuracy.
                 'literals': [TestDecimal.min(35), TestDecimal.rnd(35), TestDecimal.max(35)]},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DOUBLE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DOUBLE), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DOUBLE), GLUENT_TYPE_DOUBLE),
                 'offload_options': {'double_columns_csv': name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DOUBLE)},
                 'literals': [1, 2, 3]},
            name(ORACLE_TYPE_NUMBER, '2'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, '2'), ORACLE_TYPE_NUMBER, data_precision=2, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, '2'), GLUENT_TYPE_INTEGER_1)},
            name(ORACLE_TYPE_NUMBER, '4'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, '4'), ORACLE_TYPE_NUMBER, data_precision=4, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, '4'), GLUENT_TYPE_INTEGER_2)},
            name(ORACLE_TYPE_NUMBER, '9'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, '9'), ORACLE_TYPE_NUMBER, data_precision=9, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, '9'), GLUENT_TYPE_INTEGER_4)},
            name(ORACLE_TYPE_NUMBER, '18'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, '18'), ORACLE_TYPE_NUMBER, data_precision=18, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, '18'), GLUENT_TYPE_INTEGER_8)},
            name(ORACLE_TYPE_NUMBER, '19'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, '19'), ORACLE_TYPE_NUMBER, data_precision=19, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, '19'), GLUENT_TYPE_INTEGER_38, data_precision=19)},
            name(ORACLE_TYPE_NUMBER, str(max_decimal_integral_magnitude)):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, str(max_decimal_integral_magnitude)), ORACLE_TYPE_NUMBER,
                                  data_precision=max_decimal_integral_magnitude, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, str(max_decimal_integral_magnitude)),
                                     GLUENT_TYPE_INTEGER_38)},
            name(ORACLE_TYPE_NUMBER, str(max_precision), str(generic_scale)):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, str(max_precision), str(generic_scale)), ORACLE_TYPE_NUMBER,
                                  data_precision=max_precision, data_scale=generic_scale),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, str(max_precision), str(generic_scale)),
                                     GLUENT_TYPE_DECIMAL, data_precision=max_precision, data_scale=generic_scale)},
            name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DECIMAL, '10', '3'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DECIMAL, '10', '3'), ORACLE_TYPE_NUMBER),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DECIMAL, '10', '3'),
                                     GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=3),
                 'offload_options': {
                     'decimal_columns_csv_list': [name(ORACLE_TYPE_NUMBER, GLUENT_TYPE_DECIMAL, '10', '3')],
                     'decimal_columns_type_list': ['10,3']},
                 'literals': [TestDecimal.min(10, 3), TestDecimal.rnd(10, 3), TestDecimal.max(10, 3)]},
            name(ORACLE_TYPE_NUMBER, '9', '2', GLUENT_TYPE_DECIMAL, '10', '3'):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NUMBER, '9', '2', GLUENT_TYPE_DECIMAL, '10', '3'),
                                  ORACLE_TYPE_NUMBER, data_precision=9, data_scale=2),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NUMBER, '9', '2', GLUENT_TYPE_DECIMAL, '10', '3'),
                                     GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=3),
                 'offload_options': {
                     'decimal_columns_csv_list': [name(ORACLE_TYPE_NUMBER, '9', '2', GLUENT_TYPE_DECIMAL, '10', '3')],
                     'decimal_columns_type_list': ['10,3']},
                 'literals': [TestDecimal.min(9, 2), TestDecimal.rnd(9, 2), TestDecimal.max(9, 2)]
                 },
            name(ORACLE_TYPE_NVARCHAR2):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_NVARCHAR2), ORACLE_TYPE_NVARCHAR2, char_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_NVARCHAR2), GLUENT_TYPE_VARIABLE_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_RAW):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_RAW), ORACLE_TYPE_RAW, data_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_RAW), GLUENT_TYPE_BINARY)},
            name(ORACLE_TYPE_TIMESTAMP):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_TIMESTAMP), ORACLE_TYPE_TIMESTAMP),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_TIMESTAMP), GLUENT_TYPE_TIMESTAMP)},
            name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_DATE):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_DATE), ORACLE_TYPE_TIMESTAMP),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_DATE), GLUENT_TYPE_DATE),
                 'offload_options': {'date_columns_csv': name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_DATE)},
                 'literals': [datetime.date(1970, 1, 1), datetime.date(1970, 1, 2),
                              datetime.date(1971, 2, 1), datetime.date(1971, 2, 2)]},
            name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_VARIABLE_STRING):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_VARIABLE_STRING), ORACLE_TYPE_TIMESTAMP),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_VARIABLE_STRING),
                                     GLUENT_TYPE_VARIABLE_STRING),
                 'offload_options': {'variable_string_columns_csv': name(ORACLE_TYPE_TIMESTAMP,
                                                                         GLUENT_TYPE_VARIABLE_STRING)}},
            name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_TIMESTAMP_TZ):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_TIMESTAMP_TZ), ORACLE_TYPE_TIMESTAMP),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_TIMESTAMP, GLUENT_TYPE_TIMESTAMP_TZ), GLUENT_TYPE_TIMESTAMP_TZ),
                 'offload_options': {'timestamp_tz_columns_csv': name(ORACLE_TYPE_TIMESTAMP,
                                                                      GLUENT_TYPE_TIMESTAMP_TZ)}},
            name(ORACLE_TYPE_TIMESTAMP_TZ):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_TIMESTAMP_TZ), ORACLE_TYPE_TIMESTAMP_TZ),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_TIMESTAMP_TZ), GLUENT_TYPE_TIMESTAMP_TZ)},
            name(ORACLE_TYPE_VARCHAR2):
                {'column': OracleColumn(name(ORACLE_TYPE_VARCHAR2), ORACLE_TYPE_VARCHAR2, data_length=30),
                 'expected_canonical_column': CanonicalColumn(name(ORACLE_TYPE_VARCHAR2), GLUENT_TYPE_VARIABLE_STRING),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
            name(ORACLE_TYPE_VARCHAR2, UNICODE_NAME_TOKEN):
                {'column':
                     OracleColumn(name(ORACLE_TYPE_VARCHAR2, UNICODE_NAME_TOKEN), ORACLE_TYPE_VARCHAR2, data_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(ORACLE_TYPE_VARCHAR2, UNICODE_NAME_TOKEN), GLUENT_TYPE_VARIABLE_STRING),
                 'offload_options': {'unicode_string_columns_csv': name(ORACLE_TYPE_VARCHAR2, UNICODE_NAME_TOKEN)},
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
        }

        if supported_canonical_types and GLUENT_TYPE_FLOAT not in supported_canonical_types:
            keys_to_remove = [_ for _ in all_columns if all_columns[_]['column'].data_type == ORACLE_TYPE_BINARY_FLOAT]
            for k in keys_to_remove:
                del all_columns[k]

        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    def _gl_types_column_definitions(self, ascii_only=False, all_chars_notnull=False,
                                     supported_canonical_types=None, include_interval_columns=True) -> list:
        name_id = 0

        def name():
            nonlocal name_id
            name_id = name_id + 1
            col_name = f'COLUMN_{name_id}'
            return col_name

        column_list = [
            {'column': self._id_column(),
             'ordered': True},
            {'column': OracleColumn(name(), ORACLE_TYPE_CHAR, data_length=100),
             'ascii_only': True,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_VARCHAR2, data_length=1100),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_NCHAR, char_length=100),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_NVARCHAR2, char_length=1100),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_DATE)},
            {'column': OracleColumn(name(), ORACLE_TYPE_TIMESTAMP, data_scale=0)},
            {'column': OracleColumn(name(), ORACLE_TYPE_TIMESTAMP)},
            {'column': OracleColumn(name(), ORACLE_TYPE_TIMESTAMP_TZ)},
        ]
        if include_interval_columns:
            column_list.extend([
                {'column': OracleColumn(name(), ORACLE_TYPE_INTERVAL_YM, data_precision=9)},
                {'column': OracleColumn(name(), ORACLE_TYPE_INTERVAL_DS, data_precision=9, data_scale=9)},
            ])
        column_list.extend([
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER)},
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=2)},
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=4)},
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=9)},
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=18)},
            # GOE-1503: due to a cx_Oracle 7.1.0 - 7.3.0 issue with binding over 36 digits for negative numbers, we've
            # temporarily dropped the spec to 36. We'll raise a bug with cx_Oracle and re-instate extreme testing when
            # a fix is available.
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=36)},
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=None, data_scale=0)},
        ])

        if supported_canonical_types and GLUENT_TYPE_FLOAT in supported_canonical_types:
            column_list.append({'column': OracleColumn(name(), ORACLE_TYPE_BINARY_FLOAT)})

        column_list.extend([
            {'column': OracleColumn(name(), ORACLE_TYPE_BINARY_DOUBLE)},
            {'column': OracleColumn(name(), ORACLE_TYPE_FLOAT)},
            {'column': OracleColumn(name(), ORACLE_TYPE_RAW, data_length=16)},
            {'column': OracleColumn(name(), ORACLE_TYPE_RAW, data_length=2000)},
            {'column': OracleColumn(name(), ORACLE_TYPE_CLOB),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_BLOB),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_NCLOB),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull},
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=1),
             'column_spec_extra_clause': 'AS (MOD(id,10)) VIRTUAL'},
        ])

        return column_list

    def _gl_wide_column_definitions(self, ascii_only=False, all_chars_notnull=False,
                                    supported_canonical_types=None, backend_max_test_column_count=None) -> list:
        name_id = 0

        def name():
            nonlocal name_id
            name_id = name_id + 1
            col_name = f'COLUMN_{name_id}'
            return col_name

        column_list = [
            {'column': self._id_column(),
             'ordered': True}
        ]

        column_list.extend(
            {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=9)} for _ in range(5)
        )
        column_list.extend(
            {'column': OracleColumn(name(), ORACLE_TYPE_VARCHAR, data_length=10),
             'ascii_only': ascii_only,
             'notnull': all_chars_notnull} for _ in range(5)
        )
        column_list.extend(
            {'column': OracleColumn(name(), ORACLE_TYPE_DATE)} for _ in range(5)
        )
        column_list.extend(
            {'column': OracleColumn(name(), ORACLE_TYPE_TIMESTAMP, data_scale=0)} for _ in range(5)
        )

        extra_column_count = min(self._gl_wide_max_test_column_count(),
                                 backend_max_test_column_count) - len(column_list)

        column_list.extend(
            random.choice([
                {'column': OracleColumn(name(), ORACLE_TYPE_NUMBER, data_precision=9)},
                {'column': OracleColumn(name(), ORACLE_TYPE_VARCHAR, data_length=10),
                 'ascii_only': ascii_only,
                 'notnull': all_chars_notnull},
                {'column': OracleColumn(name(), ORACLE_TYPE_DATE)},
                {'column': OracleColumn(name(), ORACLE_TYPE_TIMESTAMP, data_scale=0)}
            ])
            for _ in range(extra_column_count)
        )

        return column_list

    def _id_column(self):
        return OracleColumn('ID', ORACLE_TYPE_NUMBER, data_precision=18, data_scale=0)

    def _populate_generated_test_table(self, schema, table_name, columns, rows, fastexecute=False):
        """ Using cx-Oracle specific syntax due to copying code directly from 'test --setup' """
        column_list = [_['column'] for _ in columns]
        bindnames = []
        for i in range(len(column_list)):
            bindnames.append(':%s' % i)
        col_types = [_.data_type for _ in column_list]
        q = 'INSERT INTO %s.%s (%s) VALUES (%s)' % (schema, table_name,
                                                    ','.join(_.name for _ in column_list),
                                                    ','.join(bindnames))
        self._log(q, detail=VERBOSE)
        ora_conn = self._db_api.get_oracle_connection_object()
        ora_curs = ora_conn.cursor()
        ora_curs.prepare(q)
        ora_curs.setinputsizes(*[gen_test_data.gen_cxo_type_spec(col.data_type, col.data_length,
                                                                 col.data_precision, col.data_scale)
                                 for col in column_list])

        self._seed_randomness_for_generated_tables()
        d = [[self._gen_column_data(col['column'], ri,
                                    from_list=col.get('literals'),
                                    ascii_only=col.get('ascii_only'),
                                    all_chars_notnull=col.get('notnull'),
                                    ordered=col.get('ordered'),
                                    no_newlines=col.get('no_newlines'),
                                    allow_nan=col.get('allow_nan', True),
                                    allow_inf=col.get('allow_inf', True))
              for col in columns] for ri in range(int(rows))]

        # We are seeing this error when using executemany():
        # cx_Oracle.DatabaseError: ORA-01458: invalid length inside variable character string
        #    c.executemany(None, d)
        if not self._dry_run:
            for bind_values in d:
                ora_curs.execute(None, bind_values)
            ora_curs.connection.commit()
            self._log('%s rows inserted: %s' % (rows, col_types), detail=VERBOSE)
        else:
            self._log('%s rows mock-inserted: %s' % (rows, col_types), detail=VERBOSE)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def collect_table_stats_sql_text(self, schema, table_name) -> str:
        return f"BEGIN DBMS_STATS.GATHER_TABLE_STATS('{schema}','{table_name}'); END;"

    def remove_table_stats_sql_text(self, schema, table_name) -> str:
        return f"BEGIN DBMS_STATS.DELETE_TABLE_STATS('{schema}','{table_name}'); END;"

    def drop_table(self, schema, table_name):
        """ Obviously this is dangerous, that's why it is in this TestingApi only. """
        try:
            return self._db_api.execute_ddl(f'DROP TABLE {schema}.{table_name}', log_level=VERBOSE)
        except Exception as exc:
            if 'ORA-00942' in str(exc):
                # Nothing to drop
                pass
            else:
                self._log('Drop table exception: {}'.format(str(exc)), detail=VERBOSE)
                raise

    def expected_channels_offload_predicates(self):
        """ Return a list of tuples of Gluent offload predicates and expected frontend predicate """
        return [
            ('(column(CHANNEL_ID) = numeric(10)) AND (column(CHANNEL_ID) < numeric(2.2))',
             '("CHANNEL_ID" = 10 AND "CHANNEL_ID" < 2.2)'),
            ('(column(CHANNEL_ID) IS NULL) AND (column(CHANNEL_ID) IS NOT NULL)',
             '("CHANNEL_ID" IS NULL AND "CHANNEL_ID" IS NOT NULL)'),
            ('(column(CHANNEL_ID) is null) AND (column(CHANNEL_ID) is not null)',
             '("CHANNEL_ID" IS NULL AND "CHANNEL_ID" IS NOT NULL)'),
            ('(column(CHANNEL_ID) = numeric(1234567890123456789012345))',
             '"CHANNEL_ID" = 1234567890123456789012345'),
            ('(column(CHANNEL_ID) = numeric(-1234567890123456789012345))',
             '"CHANNEL_ID" = -1234567890123456789012345'),
            ('(column(channel_id) = numeric(0.00000000000000000001))',
             '"CHANNEL_ID" = 0.00000000000000000001'),
            ('(column(CHANNEL_ID) = numeric(-0.00000000000000000001))',
             '"CHANNEL_ID" = -0.00000000000000000001'),
            ('(column(CHANNEL_ID) in (numeric(-10),numeric(0),numeric(10)))',
             '"CHANNEL_ID" IN (-10, 0, 10)'),
            ('column(CHANNEL_DESC) = string("Internet")',
             '"CHANNEL_DESC" = \'Internet\''),
            ('(column(CHANNEL_DESC) = string("Internet"))',
             '"CHANNEL_DESC" = \'Internet\''),
            ('(column(ALIAS.CHANNEL_DESC) = string("Internet"))',
             '"ALIAS"."CHANNEL_DESC" = \'Internet\''),
            ('column(CHANNEL_DESC) = string("column(CHANNEL_DESC)")',
             '"CHANNEL_DESC" = \'column(CHANNEL_DESC)\''),
            ('column(CHANNEL_DESC) NOT IN (string("A"),string("B"),string("C"))',
             "\"CHANNEL_DESC\" NOT IN ('A', 'B', 'C')"),
            ('(column(CHANNEL_DESC) = string("Internet"))',
             '"CHANNEL_DESC" = \'Internet\''),
        ]

    def expected_sales_offload_predicates(self):
        return [(
            'column(PROD_ID) = numeric(171)',
            '"PROD_ID" = 171',
            '"PROD_ID" = :bind_0',
            {'bind_0': 171}
        ), (
            'column(TIME_ID) < literal("TO_DATE(\' 2012-01-01 00:00:00\', \'SYYYY-MM-DD HH24:MI:SS\', \'NLS_CALENDAR=GREGORIAN\')")',
            '"TIME_ID" < TO_DATE(\' 2012-01-01 00:00:00\', \'SYYYY-MM-DD HH24:MI:SS\', \'NLS_CALENDAR=GREGORIAN\')',
            '"TIME_ID" < TO_DATE(\' 2012-01-01 00:00:00\', \'SYYYY-MM-DD HH24:MI:SS\', \'NLS_CALENDAR=GREGORIAN\')',
            {}
        ), (
            'column(TIME_ID) = datetime(2011-01-01 05:06:07)',
            '"TIME_ID" = TO_DATE(\'2011-01-01 05:06:07\', \'YYYY-MM-DD HH24:MI:SS\')',
            '"TIME_ID" = TO_DATE(:bind_0, \'YYYY-MM-DD HH24:MI:SS\')',
            {'bind_0': '2011-01-01 05:06:07'}
        ), (
            'column(TIME_ID) = datetime(2011-01-01)',
            '"TIME_ID" = TO_DATE(\'2011-01-01 00:00:00\', \'YYYY-MM-DD HH24:MI:SS\')',
            '"TIME_ID" = TO_DATE(:bind_0, \'YYYY-MM-DD HH24:MI:SS\')',
            {'bind_0': '2011-01-01 00:00:00'}
        )]

    def gen_ctas_from_subquery(self, schema: str, table_name: str, subquery: str,
                               pk_col_name: Optional[str]=None, table_parallelism: Optional[str]=None,
                               with_drop: bool=True, with_stats_collection: bool=False) -> list:
        """Return a list[str] of SQL required to CTAS a table from a subquery."""
        degree_clause = f' PARALLEL {table_parallelism}' if table_parallelism else ''
        params = {'schema': schema, 'table_name': table_name, 'subquery': subquery,
                  'degree_clause': degree_clause, 'pk_col_name': pk_col_name}
        sqls = []
        if with_drop:
            sqls.append('DROP TABLE %(schema)s.%(table_name)s' % params)
        ctas = dedent("""\
            CREATE TABLE %(schema)s.%(table_name)s%(degree_clause)s
            AS
            %(subquery)s
        """) % params
        sqls.append(ctas)
        if pk_col_name:
            sqls.append('ALTER TABLE %(schema)s.%(table_name)s ADD PRIMARY KEY (%(pk_col_name)s)' % params)
        if with_stats_collection:
            sqls.append(self.collect_table_stats_sql_text(schema, table_name))
        return sqls

    def get_test_table_owner(self, expected_schema: str, table_name: str) -> str:
        sql = f"""SELECT owner
              FROM   dba_tables
              WHERE  UPPER(table_name) = UPPER('{table_name}')
              ORDER BY CASE WHEN UPPER(owner) = UPPER('{expected_schema}') THEN '0'
                            WHEN UPPER(owner) = 'SH' THEN '1'
                            ELSE owner END"""
        row = self.execute_query_fetch_one(sql)
        return row[0] if row else None

    def gl_type_mapping_generated_table_col_specs(self, max_backend_precision, max_backend_scale,
                                                  max_decimal_integral_magnitude, supported_canonical_types,
                                                  ascii_only=False):
        gl_type_mapping_cols, gl_type_mapping_names = [], []
        definitions = self._gl_type_mapping_column_definitions(max_backend_precision, max_backend_scale,
                                                               max_decimal_integral_magnitude)
        for col_dict in [definitions[col_name] for col_name in sorted(definitions.keys())]:
            frontend_column = col_dict['column']
            if col_dict['expected_canonical_column'].data_type not in supported_canonical_types:
                # The canonical type is not supported by the backend
                continue
            gl_type_mapping_names.append(frontend_column.name)
            offload_options = col_dict.get('offload_options')
            literals = col_dict.get('literals')
            if frontend_column.data_type == ORACLE_TYPE_NUMBER and (frontend_column.data_precision is not None
                                                                    or frontend_column.data_scale is not None):
                gl_type_mapping_cols.append((frontend_column.data_type,
                                             frontend_column.data_precision,
                                             frontend_column.data_scale))
            elif frontend_column.data_type == ORACLE_TYPE_FLOAT and offload_options:
                gl_type_mapping_cols.append((frontend_column.data_type, literals or [1.5, 2.5, 3.5]))
            elif frontend_column.is_number_based() and offload_options:
                # This is a number of some kind and being CAST to something else so we provide specific test data.
                literals = literals or [1, 2, 3]
                if col_dict['expected_canonical_column'].data_type in [GLUENT_TYPE_INTEGER_1, GLUENT_TYPE_INTEGER_2,
                                                                       GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8,
                                                                       GLUENT_TYPE_INTEGER_38]:
                    precision = self._canonical_integer_precision(col_dict['expected_canonical_column'].data_type)
                    if precision > 35:
                        # scripts/test imposes a max precision of 35, I think due to shortcomings of cx-Oracle.
                        # We impose the same limit here to ensure no loss of value accuracy.
                        precision = 35
                    literals = [TestDecimal.min(precision), TestDecimal.rnd(precision), TestDecimal.max(precision)]
                elif (col_dict['expected_canonical_column'].data_type == GLUENT_TYPE_DECIMAL
                      and offload_options.get('decimal_columns_type_list')):
                    precision, scale = offload_options['decimal_columns_type_list'][0].split(',')
                    precision = int(precision)
                    scale = int(scale)
                    literals = [TestDecimal.min(precision, scale),
                                TestDecimal.rnd(precision, scale),
                                TestDecimal.max(precision, scale)]

                if self._data_type_supports_precision_and_scale(frontend_column):
                    gl_type_mapping_cols.append((frontend_column.data_type, None, None, literals))
                else:
                    gl_type_mapping_cols.append((frontend_column.data_type, literals))
            elif frontend_column.is_string_based():
                if frontend_column.data_type in [ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB]:
                    if ascii_only:
                        gl_type_mapping_cols.append((frontend_column.data_type,
                                                     TEST_GEN_DATA_ASCII7_NONULL))
                    else:
                        gl_type_mapping_cols.append(frontend_column.data_type)
                elif ascii_only:
                    gl_type_mapping_cols.append((frontend_column.data_type,
                                                 frontend_column.data_length or frontend_column.char_length,
                                                 TEST_GEN_DATA_ASCII7_NONULL))
                else:
                    gl_type_mapping_cols.append((frontend_column.data_type,
                                                 frontend_column.data_length or frontend_column.char_length))
            elif frontend_column.data_type == ORACLE_TYPE_RAW:
                gl_type_mapping_cols.append((frontend_column.data_type, frontend_column.data_length))
            elif frontend_column.data_type in [ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ]:
                if offload_options:
                    # This is being CAST to something else so keep the values as simple canonical dates.
                    # Throw in 1970-01-01 because Python datetime64 understands this as False due to being Unix epoch.
                    gl_type_mapping_cols.append((frontend_column.data_type, frontend_column.data_scale,
                                                 [datetime.date(1970, 1, 1), datetime.date(1970, 1, 2),
                                                  datetime.date(1971, 2, 1), datetime.date(1971, 2, 2)]))
                else:
                    gl_type_mapping_cols.append((frontend_column.data_type, frontend_column.data_scale))
            elif frontend_column.data_type == ORACLE_TYPE_INTERVAL_DS:
                gl_type_mapping_cols.append((frontend_column.data_type,
                                             frontend_column.data_precision or 9,
                                             frontend_column.data_scale or 9))
            elif frontend_column.data_type == ORACLE_TYPE_INTERVAL_YM:
                gl_type_mapping_cols.append((frontend_column.data_type, frontend_column.data_precision or 9))
            else:
                gl_type_mapping_cols.append(frontend_column.data_type)
        return gl_type_mapping_cols, gl_type_mapping_names

    def host_compare_sql_projection(self, column_list: list) -> str:
        """ Return a SQL projection (CSV of column expressions) used to validate offloaded data.
            Because of systems variations all date based values must be normalised to UTC in format:
                'YYYY-MM-DD HH24:MI:SS.FFF +00:00'
        """
        assert isinstance(column_list, list)
        projection = []
        for column in column_list:
            if column.data_type == ORACLE_TYPE_TIMESTAMP_TZ:
                projection.append("TO_CHAR({} AT TIME ZONE '+00:00', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM')".format(
                    self._db_api.enclose_identifier(column.name)))
            elif column.is_date_based():
                projection.append("TO_CHAR({}, 'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM')".format(
                    self._db_api.enclose_identifier(column.name)))
            elif column.is_number_based():
                if column.is_nan_capable():
                    # NaN and inf break the TM9 format
                    projection.append("TO_CHAR({})".format(self._db_api.enclose_identifier(column.name)))
                else:
                    projection.append("TO_CHAR({},'TM9')".format(self._db_api.enclose_identifier(column.name)))
            elif column.is_interval():
                projection.append("TO_CHAR({})".format(self._db_api.enclose_identifier(column.name)))
            else:
                projection.append(self._db_api.enclose_identifier(column.name))
        return ','.join(projection)

    def lobs_support_minus_operator(self):
        return False

    def lob_safe_table_projection(self, schema, table_name) -> str:
        """ Returns a CSV of columns names for use in SQL with LOB columns protected.
        """
        columns = self._db_api.get_columns(schema, table_name)
        return ','.join(lob_to_hash(_.data_type, _.name) if 'LOB' in _.data_type else _.name for _ in columns)

    def run_sql_in_file(self, local_path):
        """ This method creates frontend objects. It is expected to be called from an SH_TEST connection
            and not an ADM or APP one.
        """
        ddl = open(local_path).read()
        self._log(f'Running {local_path}')

        if 'declare' in ddl.lower():
            # This file contains PL/SQL so we cannot split on semi-colon, instead we
            # will split on the first semi-colon and take the rest as a single PL/SQL block
            # Also strip any trailing "/"
            ddls = [_.strip().strip('/') for _ in ddl.split(';', 1)]
        else:
            ddls = [d.replace('\n', ' ').strip() for d in ddl.split(';') if d.strip()]

        for ddl in ddls:
            if re.match('^exec .*', ddl, re.I):
                ddl = 'BEGIN %s; END;' % re.sub('^exec ', '', ddl, flags=re.I)
            self._db_api.execute_ddl(ddl)

        # Commit because some commands above could be DML
        self._db_api.execute_dml('commit')

    def sales_based_fact_create_ddl(self, schema: str, table_name: str, maxval_partition: bool=False,
                                    extra_pred: Optional[str]=None,
                                    degree: Optional[str]=None, subpartitions: int=0,
                                    enable_row_movement: bool=False, noseg_partition: bool=True,
                                    part_key_type: Optional[str]=None, time_id_column_name: Optional[str]=None,
                                    extra_col_tuples: Optional[list]=None, simple_partition_names: bool=False,
                                    with_drop: bool=True, range_start_literal_override=None) -> list:
        # range_start_literal_override ignored on Oracle
        if extra_col_tuples:
            assert isinstance(extra_col_tuples, list)
            assert isinstance(extra_col_tuples[0], tuple)

        if not part_key_type:
            part_key_type = ORACLE_TYPE_DATE
        assert part_key_type in [ORACLE_TYPE_NUMBER, ORACLE_TYPE_NVARCHAR2, ORACLE_TYPE_VARCHAR2,
                                 ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP], \
                                     'Unsupported part_key_type: %s' % part_key_type

        extra_pred = extra_pred or ''
        degree_clause = ''
        subpartition_clause = ''
        extra_cols = ''
        if degree:
            degree_clause = ' PARALLEL' if degree == 'DEFAULT' else (' PARALLEL %s' % degree)
        if subpartitions > 0:
            subpartition_clause = 'SUBPARTITION BY HASH(cust_id) SUBPARTITIONS %s' % subpartitions
        if extra_col_tuples:
            extra_cols = ',' + ','.join('{} AS {}'.format(_[0], _[1]) for _ in extra_col_tuples)
        time_id_alias = time_id_column_name or 'time_id'

        params = {'schema': schema,
                  'table': table_name,
                  'extra_pred': extra_pred,
                  'degree': degree_clause,
                  'row_move': ' ENABLE ROW MOVEMENT' if enable_row_movement else '',
                  'sub_clause': subpartition_clause,
                  'pre_hv': SALES_BASED_FACT_PRE_HV,
                  'hv1': SALES_BASED_FACT_HV_1,
                  'hv2': SALES_BASED_FACT_HV_2,
                  'hv3': SALES_BASED_FACT_HV_3,
                  'hv4': SALES_BASED_FACT_HV_4,
                  'hv5': SALES_BASED_FACT_HV_5,
                  'hv6': SALES_BASED_FACT_HV_6,
                  'time_id_alias': time_id_alias,
                  'extra_cols': extra_cols,
                  'part_prefix': '' if simple_partition_names else table_name + '_'}
        if part_key_type == ORACLE_TYPE_NUMBER:
            params.update({'chr': "", 'time_id_expr': "TO_NUMBER(TO_CHAR(time_id,'YYYYMMDD'))",
                           'datefn': '', 'datefnmask': ''})
        elif part_key_type == ORACLE_TYPE_DATE:
            params.update({'chr': "'", 'time_id_expr': 'time_id',
                           'datefn': 'TO_DATE(', 'datefnmask': ",'YYYY-MM-DD')"})
        elif part_key_type == ORACLE_TYPE_TIMESTAMP:
            params.update({'chr': "'", 'time_id_expr': "CAST(time_id AS TIMESTAMP(0))",
                           'datefn': 'TO_DATE(', 'datefnmask': ",'YYYY-MM-DD')"})
        elif part_key_type == ORACLE_TYPE_NVARCHAR2:
            params.update({'chr': "'", 'time_id_expr': "TO_NCHAR(time_id,'YYYYMMDD')",
                           'datefn': '', 'datefnmask': ''})
        else:
            # VC2
            params.update({'chr': "'", 'time_id_expr': "TO_CHAR(time_id,'YYYYMMDD')",
                           'datefn': '', 'datefnmask': ''})
        hv7 = SALES_BASED_FACT_HV_7
        if part_key_type in (ORACLE_TYPE_NUMBER, ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2):
            params['pre_hv'] = SALES_BASED_FACT_PRE_HV_NUM
            params['hv1'] = SALES_BASED_FACT_HV_1_NUM
            params['hv2'] = SALES_BASED_FACT_HV_2_NUM
            params['hv3'] = SALES_BASED_FACT_HV_3_NUM
            params['hv4'] = SALES_BASED_FACT_HV_4_NUM
            params['hv5'] = SALES_BASED_FACT_HV_5_NUM
            params['hv6'] = SALES_BASED_FACT_HV_6_NUM
            hv7 = SALES_BASED_FACT_HV_7_NUM

        if maxval_partition:
            params.update({'part_7_name': 'PMAXVAL', 'part_7_expr': "MAXVALUE"})
        else:
            part_7_expr = "%(datefn)s%(chr)s%%s%(chr)s%(datefnmask)s" % params
            if part_key_type in (ORACLE_TYPE_NUMBER, ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2):
                hv7 = hv7.replace('-', '')
            params.update({'part_7_name': 'P7', 'part_7_expr': part_7_expr % hv7})

        create_ddl = dedent("""\
        CREATE TABLE %(schema)s.%(table)s PARTITION BY RANGE (%(time_id_alias)s) %(sub_clause)s
        (""")
        if noseg_partition:
            create_ddl += dedent("""\
            PARTITION %(part_prefix)sP0 VALUES LESS THAN (%(datefn)s%(chr)s%(pre_hv)s%(chr)s%(datefnmask)s)
            ,""")
        create_ddl += dedent("""\
               PARTITION %(part_prefix)sP1 VALUES LESS THAN (%(datefn)s%(chr)s%(hv1)s%(chr)s%(datefnmask)s)
               ,PARTITION %(part_prefix)sP2 VALUES LESS THAN (%(datefn)s%(chr)s%(hv2)s%(chr)s%(datefnmask)s)
               ,PARTITION %(part_prefix)sP3 VALUES LESS THAN (%(datefn)s%(chr)s%(hv3)s%(chr)s%(datefnmask)s)
               ,PARTITION %(part_prefix)sP4 VALUES LESS THAN (%(datefn)s%(chr)s%(hv4)s%(chr)s%(datefnmask)s)
               ,PARTITION %(part_prefix)sP5 VALUES LESS THAN (%(datefn)s%(chr)s%(hv5)s%(chr)s%(datefnmask)s)
               ,PARTITION %(part_prefix)sP6 VALUES LESS THAN (%(datefn)s%(chr)s%(hv6)s%(chr)s%(datefnmask)s)
               ,PARTITION %(part_prefix)s%(part_7_name)s VALUES LESS THAN (%(part_7_expr)s))%(degree)s%(row_move)s
               AS
               SELECT prod_id, cust_id, %(time_id_expr)s AS %(time_id_alias)s, channel_id, promo_id
               , quantity_sold, amount_sold%(extra_cols)s
               FROM %(schema)s.sales
               WHERE time_id BETWEEN TO_DATE('%(pre_hv)s','YYYY-MM-DD') AND TO_DATE('%(hv6)s','YYYY-MM-DD')
               %(extra_pred)s""")

        sqls = []
        if with_drop:
            sqls.append('DROP TABLE %(schema)s.%(table)s' % params)
        sqls.append(create_ddl % params)
        sqls.append(self.collect_table_stats_sql_text(schema, table_name))
        return sqls

    def sales_based_fact_add_partition_ddl(self, schema: str, table_name: str) -> list:
        _, hv = self.get_max_range_partition_name_and_hv(schema, table_name)
        q = "SELECT TO_CHAR(next_date,'YYYY'), TO_CHAR(next_date,'MM'), TO_CHAR(next_date,'DD') FROM (SELECT ADD_MONTHS(%(hv)s,1) next_date FROM dual)" % {'hv': hv}
        next_y, next_m, next_d = self.execute_query_fetch_one(q)
        new_hv = "TO_DATE('%(next_y)s-%(next_m)s-%(next_d)s','YYYY-MM-DD')" % {'next_y': next_y, 'next_m': next_m, 'next_d': next_d}
        alt = """ALTER TABLE %(schema)s.%(table)s ADD PARTITION %(table)s_%(next_y)s%(next_m)s
                 VALUES LESS THAN (%(new_hv)s)""" \
            % {'schema': schema, 'table': table_name, 'next_y': next_y, 'next_m': next_m, 'new_hv': new_hv}
        ins = """INSERT INTO %(schema)s.%(table)s
                 SELECT * FROM %(schema)s.sales
                 WHERE time_id BETWEEN %(hv)s AND %(new_hv)s-1""" \
            % {'schema': schema, 'table': table_name, 'hv': hv, 'new_hv': new_hv}
        return [alt, ins]

    def sales_based_fact_drop_partition_ddl(self, schema: str, table_name: str,
                                            hv_string_list: list, dropping_oldest: Optional[bool]=None) -> list:
        ddls = ['ALTER TABLE %s.%s DROP PARTITION %s' % (schema, table_name, _.partition_name)
                for _ in self.frontend_table_partition_list(schema, table_name, hv_string_list=hv_string_list)]
        return ddls

    def sales_based_fact_truncate_partition_ddl(self, schema: str, table_name: str,
                                                hv_string_list: Optional[list]=None) -> list:
        ddls = ['ALTER TABLE %s.%s TRUNCATE PARTITION %s DROP ALL STORAGE' % (schema, table_name, _.partition_name)
                for _ in self.frontend_table_partition_list(schema, table_name, hv_string_list)]
        return ddls

    def sales_based_fact_hwm_literal(self, sales_literal: str, data_type: str) -> tuple:
        hwm_literal = sales_literal
        if data_type == ORACLE_TYPE_DATE:
            if hwm_literal and ':' not in hwm_literal:
                hwm_literal += ' 00:00:00'
            meta_hwm_check_literal = f"TO_DATE(' {hwm_literal}'"
            sql_literal = f"TO_DATE('{hwm_literal}','YYYY-MM-DD HH24:MI:SS')"
        elif data_type == ORACLE_TYPE_TIMESTAMP:
            if hwm_literal and ':' not in hwm_literal:
                hwm_literal += ' 00:00:00'
            meta_hwm_check_literal = f"TIMESTAMP' {hwm_literal}'"
            sql_literal = f"TIMESTAMP' {hwm_literal}'"
        elif data_type in [ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2]:
            meta_hwm_check_literal = "'%s'" % hwm_literal
            sql_literal = meta_hwm_check_literal
        else:
            meta_hwm_check_literal = hwm_literal
            sql_literal = meta_hwm_check_literal

        return hwm_literal, meta_hwm_check_literal, sql_literal

    def sales_based_fact_late_arriving_data_sql(self, schema: str, table_name: str, time_id_literal: str) -> list:
        ins = """INSERT INTO %(schema)s.%(table_name)s
        SELECT prod_id, cust_id, TO_DATE('%(time_id)s','YYYY-MM-DD HH24:MI:SS'), channel_id,
               promo_id, quantity_sold, amount_sold
        FROM   %(schema)s.sales
        WHERE  ROWNUM = 1""" % {'schema': schema, 'table_name': table_name, 'time_id': time_id_literal}
        return [ins]

    def sales_based_list_fact_create_ddl(self, schema: str, table_name: str, default_partition: bool=False,
                                         extra_pred: Optional[str]=None, part_key_type: Optional[str]=None,
                                         out_of_sequence: bool=False, include_older_partition: bool=False,
                                         yrmon_column_name: Optional[str]=None, extra_col_tuples: Optional[list]=None,
                                         with_drop: bool=True) -> list:
        if extra_col_tuples:
            assert isinstance(extra_col_tuples, list)
            assert isinstance(extra_col_tuples[0], tuple)

        if not part_key_type:
            part_key_type = ORACLE_TYPE_NUMBER
        assert part_key_type in [ORACLE_TYPE_NUMBER, ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP], \
            'Unsupported part_key_type: %s' % part_key_type

        extra_pred = extra_pred or ''
        yrmon = yrmon_column_name or 'yrmon'
        extra_cols = ''
        if extra_col_tuples:
            extra_cols = ',' + ','.join('{} AS {}'.format(_[0], _[1]) for _ in extra_col_tuples)
        params = {'schema': schema,
                  'table': table_name,
                  'extra_pred': extra_pred,
                  'p0_partition': '',
                  'hv0': SALES_BASED_LIST_PRE_HV, 'pname0': SALES_BASED_LIST_PNAME_0,
                  'hv1': SALES_BASED_LIST_HV_1, 'pname1': SALES_BASED_LIST_PNAME_1,
                  'hv2': SALES_BASED_LIST_HV_2, 'pname2': SALES_BASED_LIST_PNAME_2,
                  'hv3': SALES_BASED_LIST_HV_3, 'pname3': SALES_BASED_LIST_PNAME_3,
                  'hv4': SALES_BASED_LIST_HV_4, 'pname4': SALES_BASED_LIST_PNAME_4,
                  'hv5': SALES_BASED_LIST_HV_5, 'pname5': SALES_BASED_LIST_PNAME_5,
                  'hv6': SALES_BASED_LIST_HV_6, 'pname6': SALES_BASED_LIST_PNAME_6,
                  'hv7': SALES_BASED_LIST_HV_7, 'pname7': SALES_BASED_LIST_PNAME_7,
                  'hv7_actual': SALES_BASED_LIST_HV_7,
                  'yrmon_alias': yrmon,
                  'extra_cols': extra_cols}
        if out_of_sequence:
            # Swap some partitions around to confuse RANGE based logic
            params['hv3'], params['pname3'], params['hv5'], params['pname5'] = \
                params['hv5'], params['pname5'], params['hv3'], params['pname3']
        if part_key_type == ORACLE_TYPE_NUMBER:
            params.update({'chr': "", 'yrmon_expr': "TO_NUMBER(TO_CHAR(time_id,'YYYYMM'))", 'datefn': '', 'datefnmask': ''})
        elif part_key_type == ORACLE_TYPE_DATE:
            params.update({'chr': "'", 'yrmon_expr': "TRUNC(time_id,'MM')", 'datefn': 'TO_DATE(', 'datefnmask': ",'YYYYMM')"})
        elif part_key_type == ORACLE_TYPE_TIMESTAMP:
            params.update({'chr': "'", 'yrmon_expr': "CAST(TRUNC(time_id,'MM') AS TIMESTAMP(0))", 'datefn': 'TO_DATE(', 'datefnmask': ",'YYYYMM')"})
        else:
            params.update({'chr': "'", 'yrmon_expr': "TO_CHAR(time_id,'YYYYMM')", 'datefn': '', 'datefnmask': ''})
        params['hv7chr'] = params['chr']
        params['hv7datefn'] = params['datefn']
        params['hv7datefnmask'] = params['datefnmask']
        if default_partition:
            params.update({'pname7': 'P_DEF', 'hv7': 'DEFAULT', 'hv7chr': '', 'hv7datefn': '', 'hv7datefnmask': ''})
        if include_older_partition:
            # In order to mimic RANGE for LIST_AS_RANGE we need an older partition
            params['p0_partition'] = 'PARTITION %(pname0)s VALUES (%(datefn)s%(chr)s%(hv0)s%(chr)s%(datefnmask)s),\n' % params

        create_ddl = dedent("""\
        CREATE TABLE %(schema)s.%(table)s PARTITION BY LIST (%(yrmon_alias)s)
        (%(p0_partition)sPARTITION %(pname1)s VALUES (%(datefn)s%(chr)s%(hv1)s%(chr)s%(datefnmask)s)
        ,PARTITION %(pname2)s VALUES (%(datefn)s%(chr)s%(hv2)s%(chr)s%(datefnmask)s)
        ,PARTITION %(pname3)s VALUES (%(datefn)s%(chr)s%(hv3)s%(chr)s%(datefnmask)s)
        ,PARTITION %(pname4)s VALUES (%(datefn)s%(chr)s%(hv4)s%(chr)s%(datefnmask)s)
        ,PARTITION %(pname5)s VALUES (%(datefn)s%(chr)s%(hv5)s%(chr)s%(datefnmask)s)
        ,PARTITION %(pname6)s VALUES (%(datefn)s%(chr)s%(hv6)s%(chr)s%(datefnmask)s)
        ,PARTITION %(pname7)s VALUES (%(hv7datefn)s%(hv7chr)s%(hv7)s%(hv7chr)s%(hv7datefnmask)s))
        AS
        SELECT sales.*, %(yrmon_expr)s AS %(yrmon_alias)s%(extra_cols)s
        FROM %(schema)s.sales
        WHERE time_id BETWEEN TO_DATE('%(hv1)s','YYYYMM') AND TO_DATE('%(hv7_actual)s','YYYYMM')
        AND TO_CHAR(time_id, 'YYYYMM') <> '%(hv2)s'
        %(extra_pred)s""") % params

        sqls = []
        if with_drop:
            sqls.append('DROP TABLE %(schema)s.%(table)s' % params)
        sqls.append(create_ddl % params)
        sqls.append(self.collect_table_stats_sql_text(schema, table_name))
        return sqls

    def sales_based_list_fact_add_partition_ddl(self, schema: str, table_name: str,
                                                next_ym_override: Optional[tuple]=None) -> list:
        columns = self._db_api.get_columns(schema, table_name)
        part_col = match_table_column('YRMON', columns)

        if next_ym_override:
            assert isinstance(next_ym_override, tuple)
            next_y, next_m = next_ym_override
        else:
            partitions = self.frontend_table_partition_list(schema, table_name)
            hvs = [_.high_values_csv for _ in partitions]
            hv_lit = sorted(hvs)[-1]
            if part_col.data_type == ORACLE_TYPE_DATE:
                hv_expr = hv_lit
            else:
                hv_expr = "TO_DATE(%s,'YYYYMM')" % hv_lit
            q = "SELECT TO_CHAR(next_date,'YYYY'), TO_CHAR(next_date,'MM') FROM (SELECT ADD_MONTHS(%(hv)s,1) next_date FROM dual)" % {'hv': hv_expr}
            self._log('gen_add_sales_based_list_partition_ddl q: %s' % q, detail=VERBOSE)
            next_y, next_m = self._db_api.execute_query_fetch_one(q)

        self._log('gen_add_sales_based_list_partition_ddl adding partition: %s%s' % (next_y, next_m), detail=VERBOSE)
        if part_col.data_type == ORACLE_TYPE_NUMBER:
            ch, datefn, datefnmask, yrmon_expr = '', '', '', "TO_NUMBER(TO_CHAR(time_id,'YYYYMM'))"
        elif part_col.data_type == ORACLE_TYPE_DATE:
            ch, datefn, datefnmask, yrmon_expr = "'", 'TO_DATE(', ",'YYYYMM')", "TRUNC(time_id,'MM')"
        else:
            ch, datefn, datefnmask, yrmon_expr = "'", '', '', "TO_CHAR(time_id,'YYYYMM')"
        alt = """ALTER TABLE %(schema)s.%(table)s ADD PARTITION P_%(next_y)s%(next_m)s VALUES (%(datefn)s%(ch)s%(next_y)s%(next_m)s%(ch)s%(datefnmask)s)""" \
            % {'schema': schema, 'table': table_name, 'next_y': next_y, 'next_m': next_m, 'ch': ch, 'datefn': datefn, 'datefnmask': datefnmask}

        ins = """INSERT INTO %(schema)s.%(table)s
            SELECT sales.*, %(yrmon_expr)s AS yrmon
            FROM %(schema)s.sales
            WHERE TO_CHAR(time_id,'YYYYMM') = '%(next_y)s%(next_m)s'""" \
            % {'schema': schema, 'table': table_name, 'next_y': next_y, 'next_m': next_m, 'yrmon_expr': yrmon_expr}
        self._log('Add DDL: %s' % alt, detail=VERBOSE)
        self._log('Ins DML: %s' % ins, detail=VERBOSE)
        return [alt, ins]

    def sales_based_list_fact_late_arriving_data_sql(self, schema: str, table_name: str,
                                                     time_id_literal: str, yrmon_string: str) -> list:
        ins = """INSERT INTO %(schema)s.%(table_name)s
        SELECT prod_id, cust_id, TO_DATE('%(time_id)s','YYYY-MM-DD HH24:MI:SS') AS time_id,
               channel_id, promo_id, quantity_sold, amount_sold,
               TO_DATE('%(yrmon)s','YYYY-MM-DD') AS yrmon
        FROM   %(schema)s.sales
        WHERE  time_id = TO_DATE('%(yrmon)s','YYYY-MM-DD')
        AND    ROWNUM = 1"""  % {'schema': schema, 'table_name': table_name,
                                 'time_id': time_id_literal, 'yrmon': yrmon_string}
        return [ins]

    def select_grant_exists(self, schema: str, table_name: str, to_user: str, grantable: Optional[bool]=None) -> bool:
        self._log('select_grant_exists(%s, %s, %s, %s)' % (schema, table_name, to_user, grantable), detail=VERBOSE)
        q = """SELECT grantable
               FROM  dba_tab_privs
               WHERE owner = :owner
               AND   table_name = :tab
               AND   grantee = :grantee
               AND   privilege = 'SELECT'"""
        row = self._db_api.execute_query_fetch_one(q, query_params={'owner': schema.upper(),
                                                                    'tab': table_name.upper(),
                                                                    'grantee': to_user.upper()})
        if not row:
            return False
        elif grantable is None:
            return True
        if grantable:
            return bool(row[0] == 'YES')
        else:
            return bool(row[0] == 'NO')

    def table_row_count_from_stats(self, schema: str, table_name: str) -> Union[int, None]:
        self._log('table_row_count_from_stats for %s' % table_name, detail=VERBOSE)
        q = 'SELECT num_rows FROM all_tables WHERE owner = :o AND table_name = :t'
        r = self._db_api.execute_query_fetch_one(q, query_params={'o': schema.upper(), 't': table_name.upper()})
        stats_value = r[0] if r else r
        self._log('table_row_count_from_stats: %s' % str(stats_value), detail=VVERBOSE)
        return stats_value

    def test_type_canonical_int_8(self) -> str:
        return ORACLE_TYPE_NUMBER

    def test_type_canonical_date(self) -> str:
        return ORACLE_TYPE_DATE

    def test_type_canonical_decimal(self) -> str:
        return ORACLE_TYPE_NUMBER

    def test_type_canonical_string(self) -> str:
        return ORACLE_TYPE_VARCHAR2

    def test_type_canonical_timestamp(self) -> str:
        return ORACLE_TYPE_TIMESTAMP

    def test_time_zone_query_option(self, tz) -> dict:
        return {'TIME_ZONE': f"'{tz}'"} if tz else {}

    def unit_test_query_options(self):
        return {'tracefile_identifier': "'GLUENT'"}

    def view_is_valid(self, schema, view_name) -> bool:
        self._log(f'ora_view_is_valid: {schema}.{view_name}', detail=VERBOSE)
        q = """SELECT 1 FROM dba_objects
               WHERE owner = UPPER(:o) AND object_name = UPPER(:v)
               AND object_type = 'VIEW' AND status = 'VALID'"""
        row = self._db_api.execute_query_fetch_one(q, query_params={'o': schema, 'v': view_name})
        return bool(row)
