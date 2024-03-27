#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" BackendTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests.
"""

import logging

from goe.offload.snowflake.snowflake_column import (
    SnowflakeColumn,
    SNOWFLAKE_TYPE_BOOLEAN,
    SNOWFLAKE_TYPE_BINARY,
    SNOWFLAKE_TYPE_DATE,
    SNOWFLAKE_TYPE_FLOAT,
    SNOWFLAKE_TYPE_TIME,
    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
    SNOWFLAKE_TYPE_TIMESTAMP_TZ,
    SNOWFLAKE_TYPE_NUMBER,
    SNOWFLAKE_TYPE_TEXT,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_VARIABLE_STRING,
    GOE_TYPE_BINARY,
    GOE_TYPE_LARGE_BINARY,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_DATE,
    GOE_TYPE_TIME,
    GOE_TYPE_TIMESTAMP,
    GOE_TYPE_TIMESTAMP_TZ,
    GOE_TYPE_INTERVAL_DS,
    GOE_TYPE_INTERVAL_YM,
    GOE_TYPE_BOOLEAN,
)
from tests.testlib.test_framework.backend_testing_api import (
    BackendTestingApiInterface,
    BackendTestingApiException,
    STORY_TEST_BACKEND_DECIMAL_PS_COL,
    STORY_TEST_BACKEND_DOUBLE_COL,
    STORY_TEST_BACKEND_INT_1_COL,
    STORY_TEST_BACKEND_INT_2_COL,
    STORY_TEST_BACKEND_INT_4_COL,
    STORY_TEST_BACKEND_INT_8_COL,
    STORY_TEST_BACKEND_DECIMAL_DEF_COL,
    STORY_TEST_BACKEND_VAR_STR_COL,
    STORY_TEST_BACKEND_VAR_STR_LONG_COL,
    STORY_TEST_BACKEND_DATE_COL,
    STORY_TEST_BACKEND_TIMESTAMP_COL,
    STORY_TEST_BACKEND_TIMESTAMP_TZ_COL,
    STORY_TEST_BACKEND_BLOB_COL,
    STORY_TEST_BACKEND_NULL_STR_COL,
    STORY_TEST_BACKEND_RAW_COL,
    STORY_TEST_OFFLOAD_NUMS_BARE_NUM,
    STORY_TEST_OFFLOAD_NUMS_BARE_FLT,
    STORY_TEST_OFFLOAD_NUMS_NUM_4,
    STORY_TEST_OFFLOAD_NUMS_DEC_10_0,
    STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
    STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
    STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_2,
    STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_5,
    STORY_TEST_OFFLOAD_NUMS_NUM_10_M5,
)
from tests.testlib.test_framework.test_constants import UNICODE_NAME_TOKEN
from tests.testlib.test_framework.test_value_generators import TestDecimal


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendSnowflakeTestingApi
###########################################################################


class BackendSnowflakeTestingApi(BackendTestingApiInterface):
    """Snowflake methods"""

    def __init__(
        self,
        connection_options,
        backend_type,
        messages,
        dry_run=False,
        no_caching=False,
        do_not_connect=False,
    ):
        """CONSTRUCTOR"""
        super(BackendSnowflakeTestingApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _define_test_partition_function(self, udf_name):
        raise NotImplementedError(
            "_define_test_partition_function() not implemented for Snowflake"
        )

    def _goe_type_mapping_column_definitions(self, filter_column=None):
        def name(*args):
            return self._goe_type_mapping_column_name(*args)

        all_columns = {
            name(SNOWFLAKE_TYPE_BINARY, "2000"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2000"),
                    SNOWFLAKE_TYPE_BINARY,
                    data_length=2000,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2000"), GOE_TYPE_BINARY
                ),
            },
            name(SNOWFLAKE_TYPE_BINARY, "2000", GOE_TYPE_LARGE_BINARY): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2000", GOE_TYPE_LARGE_BINARY),
                    SNOWFLAKE_TYPE_BINARY,
                    data_length=2000,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2000", GOE_TYPE_LARGE_BINARY),
                    GOE_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SNOWFLAKE_TYPE_BINARY, "2000", GOE_TYPE_LARGE_BINARY
                    )
                },
            },
            name(SNOWFLAKE_TYPE_BINARY, "2001"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2001"),
                    SNOWFLAKE_TYPE_BINARY,
                    data_length=2001,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2001"), GOE_TYPE_LARGE_BINARY
                ),
            },
            name(SNOWFLAKE_TYPE_BINARY, "2001", GOE_TYPE_BINARY): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2001", GOE_TYPE_BINARY),
                    SNOWFLAKE_TYPE_BINARY,
                    data_length=2001,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_BINARY, "2001", GOE_TYPE_BINARY),
                    GOE_TYPE_BINARY,
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SNOWFLAKE_TYPE_BINARY, "2001", GOE_TYPE_BINARY
                    )
                },
            },
            name(SNOWFLAKE_TYPE_DATE): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_DATE), SNOWFLAKE_TYPE_DATE
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_DATE), GOE_TYPE_DATE
                ),
            },
            name(SNOWFLAKE_TYPE_DATE, GOE_TYPE_TIMESTAMP): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_DATE, GOE_TYPE_TIMESTAMP),
                    SNOWFLAKE_TYPE_DATE,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_DATE, GOE_TYPE_TIMESTAMP),
                    GOE_TYPE_TIMESTAMP,
                ),
                "present_options": {
                    "timestamp_columns_csv": name(
                        SNOWFLAKE_TYPE_DATE, GOE_TYPE_TIMESTAMP
                    )
                },
            },
            name(SNOWFLAKE_TYPE_FLOAT): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_FLOAT), SNOWFLAKE_TYPE_FLOAT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_FLOAT), GOE_TYPE_DOUBLE
                ),
            },
            name(SNOWFLAKE_TYPE_FLOAT, GOE_TYPE_DECIMAL): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_FLOAT, GOE_TYPE_DECIMAL),
                    SNOWFLAKE_TYPE_FLOAT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_FLOAT, GOE_TYPE_DECIMAL), GOE_TYPE_DECIMAL
                ),
                "present_options": {
                    "decimal_columns_csv_list": [
                        name(SNOWFLAKE_TYPE_FLOAT, GOE_TYPE_DECIMAL)
                    ],
                    "decimal_columns_type_list": ["38,18"],
                },
            },
            name(SNOWFLAKE_TYPE_NUMBER): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=38,
                    data_scale=18,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER), GOE_TYPE_DECIMAL
                ),
            },
            name(SNOWFLAKE_TYPE_NUMBER, "2", "0"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "2", "0"),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "2", "0"), GOE_TYPE_INTEGER_1
                ),
            },
            name(SNOWFLAKE_TYPE_NUMBER, "4", "0"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "4", "0"),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "4", "0"), GOE_TYPE_INTEGER_2
                ),
            },
            name(SNOWFLAKE_TYPE_NUMBER, "9", "0"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "9", "0"),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "9", "0"), GOE_TYPE_INTEGER_4
                ),
            },
            name(SNOWFLAKE_TYPE_NUMBER, "18", "0"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "18", "0"),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "18", "0"), GOE_TYPE_INTEGER_8
                ),
            },
            # Trimmed down to NUMBER(36) because cx_Oracle has issues beyond that
            name(SNOWFLAKE_TYPE_NUMBER, "36", "0"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "36", "0"),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=36,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, "36", "0"), GOE_TYPE_INTEGER_38
                ),
            },
            name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_1): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_1),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=38,
                    data_scale=18,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_1),
                    GOE_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_1_columns_csv": name(
                        SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_1
                    )
                },
            },
            name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_2): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_2),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=38,
                    data_scale=18,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_2),
                    GOE_TYPE_INTEGER_2,
                ),
                "present_options": {
                    "integer_2_columns_csv": name(
                        SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_2
                    )
                },
            },
            name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_4): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_4),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=38,
                    data_scale=18,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_4),
                    GOE_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_4_columns_csv": name(
                        SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_4
                    )
                },
            },
            name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_8): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_8),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=38,
                    data_scale=18,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_8),
                    GOE_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_8
                    )
                },
            },
            name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_38): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_38),
                    SNOWFLAKE_TYPE_NUMBER,
                    data_precision=38,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_38),
                    GOE_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_38_columns_csv": name(
                        SNOWFLAKE_TYPE_NUMBER, GOE_TYPE_INTEGER_38
                    )
                },
            },
            # Column below commented out because --fixed-string-columns does not exist
            # This is listed in support matrix as:
            #   Snowflake CHAR datatype is stored as VARCHAR and is not space-padded. It’s listed as a possibility for
            #   support at present but will need to be reviewed for correctness.
            # name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_FIXED_STRING):
            # {'column':
            # SnowflakeColumn(name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_FIXED_STRING),
            # SNOWFLAKE_TYPE_TEXT),
            # 'expected_canonical_column':
            # CanonicalColumn(name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_FIXED_STRING), GOE_TYPE_FIXED_STRING),
            # 'present_options': {'fixed_string_columns_csv': name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_FIXED_STRING)}},
            name(SNOWFLAKE_TYPE_TEXT, "4000"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "4000"),
                    SNOWFLAKE_TYPE_TEXT,
                    char_length=4000,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "4000"), GOE_TYPE_VARIABLE_STRING
                ),
            },
            name(SNOWFLAKE_TYPE_TEXT, "4001"): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "4001"),
                    SNOWFLAKE_TYPE_TEXT,
                    char_length=4001,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "4001"), GOE_TYPE_LARGE_STRING
                ),
            },
            name(SNOWFLAKE_TYPE_TEXT, "30", GOE_TYPE_LARGE_STRING): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "30", GOE_TYPE_LARGE_STRING),
                    SNOWFLAKE_TYPE_TEXT,
                    char_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "30", GOE_TYPE_LARGE_STRING),
                    GOE_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT, "30", GOE_TYPE_LARGE_STRING
                    )
                },
            },
            name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_BINARY): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_BINARY), SNOWFLAKE_TYPE_TEXT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_BINARY), GOE_TYPE_BINARY
                ),
                "present_options": {
                    "binary_columns_csv": name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_BINARY)
                },
            },
            name(SNOWFLAKE_TYPE_TEXT, "2000", UNICODE_NAME_TOKEN): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "2000", UNICODE_NAME_TOKEN),
                    SNOWFLAKE_TYPE_TEXT,
                    char_length=2000,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "2000", UNICODE_NAME_TOKEN),
                    GOE_TYPE_LARGE_STRING,
                    char_length=2000,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT, "2000", UNICODE_NAME_TOKEN
                    )
                },
            },
            name(SNOWFLAKE_TYPE_TEXT, "2001", UNICODE_NAME_TOKEN): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "2001", UNICODE_NAME_TOKEN),
                    SNOWFLAKE_TYPE_TEXT,
                    char_length=2001,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, "2001", UNICODE_NAME_TOKEN),
                    GOE_TYPE_LARGE_STRING,
                    char_length=2001,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT, "2001", UNICODE_NAME_TOKEN
                    )
                },
            },
            name(
                SNOWFLAKE_TYPE_TEXT, "30", GOE_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN
            ): {
                "column": SnowflakeColumn(
                    name(
                        SNOWFLAKE_TYPE_TEXT,
                        "30",
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    SNOWFLAKE_TYPE_TEXT,
                    char_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SNOWFLAKE_TYPE_TEXT,
                        "30",
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    GOE_TYPE_LARGE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT,
                        "30",
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    "unicode_string_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT,
                        "30",
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                },
            },
            name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_LARGE_BINARY): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_LARGE_BINARY),
                    SNOWFLAKE_TYPE_TEXT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_LARGE_BINARY),
                    GOE_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT, GOE_TYPE_LARGE_BINARY
                    )
                },
            },
            name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_DS): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_DS),
                    SNOWFLAKE_TYPE_TEXT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_DS),
                    GOE_TYPE_INTERVAL_DS,
                ),
                "present_options": {
                    "interval_ds_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_DS
                    )
                },
            },
            name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_YM): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_YM),
                    SNOWFLAKE_TYPE_TEXT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_YM),
                    GOE_TYPE_INTERVAL_YM,
                ),
                "present_options": {
                    "interval_ym_columns_csv": name(
                        SNOWFLAKE_TYPE_TEXT, GOE_TYPE_INTERVAL_YM
                    )
                },
            },
            name(SNOWFLAKE_TYPE_TIME): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TIME), SNOWFLAKE_TYPE_TIME
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TIME), GOE_TYPE_TIME
                ),
            },
            name(SNOWFLAKE_TYPE_TIMESTAMP_NTZ): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_NTZ), SNOWFLAKE_TYPE_TIMESTAMP_NTZ
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_NTZ), GOE_TYPE_TIMESTAMP
                ),
            },
            name(SNOWFLAKE_TYPE_TIMESTAMP_NTZ, GOE_TYPE_DATE): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_NTZ, GOE_TYPE_DATE),
                    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_NTZ, GOE_TYPE_DATE),
                    GOE_TYPE_DATE,
                ),
                "present_options": {
                    "date_columns_csv": name(
                        SNOWFLAKE_TYPE_TIMESTAMP_NTZ, GOE_TYPE_DATE
                    )
                },
            },
            name(SNOWFLAKE_TYPE_TIMESTAMP_TZ): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_TZ), SNOWFLAKE_TYPE_TIMESTAMP_TZ
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_TZ), GOE_TYPE_TIMESTAMP_TZ
                ),
            },
            name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_DATE): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_DATE),
                    SNOWFLAKE_TYPE_TIMESTAMP_TZ,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_DATE),
                    GOE_TYPE_DATE,
                ),
                "present_options": {
                    "date_columns_csv": name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_DATE)
                },
            },
            name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_TIMESTAMP): {
                "column": SnowflakeColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_TIMESTAMP),
                    SNOWFLAKE_TYPE_TIMESTAMP_TZ,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_TIMESTAMP),
                    GOE_TYPE_TIMESTAMP,
                ),
                "present_options": {
                    "timestamp_columns_csv": name(
                        SNOWFLAKE_TYPE_TIMESTAMP_TZ, GOE_TYPE_TIMESTAMP
                    )
                },
            },
        }

        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_test_type_canonical_date(self):
        return SNOWFLAKE_TYPE_DATE

    def backend_test_type_canonical_decimal(self):
        return SNOWFLAKE_TYPE_NUMBER

    def backend_test_type_canonical_int_2(self):
        return SNOWFLAKE_TYPE_NUMBER

    def backend_test_type_canonical_int_4(self):
        return SNOWFLAKE_TYPE_NUMBER

    def backend_test_type_canonical_int_8(self):
        return SNOWFLAKE_TYPE_NUMBER

    def backend_test_type_canonical_int_38(self):
        return SNOWFLAKE_TYPE_NUMBER

    def backend_test_type_canonical_time(self):
        return SNOWFLAKE_TYPE_TIME

    def backend_test_type_canonical_timestamp(self):
        return SNOWFLAKE_TYPE_TIMESTAMP_NTZ

    def backend_test_type_canonical_timestamp_tz(self):
        return SNOWFLAKE_TYPE_TIMESTAMP_TZ

    def create_backend_offload_location(self, goe_user=None):
        """Unsupported for Snowflake"""
        raise NotImplementedError(
            "create_backend_offload_location() unsupported for Snowflake"
        )

    def create_partitioned_test_table(
        self,
        db_name,
        table_name,
        source_table_name,
        storage_format,
        compute_stats=False,
        filter_clauses=None,
    ):
        """No table partitioning on Snowflake"""
        raise NotImplementedError(
            "create_partitioned_test_table() unsupported for Snowflake"
        )

    def create_table_as_select(
        self,
        db_name,
        table_name,
        storage_format,
        column_tuples,
        from_db_name=None,
        from_table_name=None,
        row_limit=None,
        compute_stats=None,
    ):
        """Snowflake override that doesn't compute stats"""
        sql = self._db_api.gen_ctas_sql_text(
            db_name,
            table_name,
            storage_format,
            column_tuples,
            from_db_name=from_db_name,
            from_table_name=from_table_name,
            row_limit=row_limit,
        )
        executed_sqls = self.execute_ddl(sql)
        return executed_sqls

    def drop_column(self, db_name, table_name, column_name, sync=None):
        """Utilise Snowflake ALTER TABLE ... DROP COLUMN statement."""
        assert db_name and table_name and column_name
        assert isinstance(column_name, str)
        sql = "ALTER TABLE %s DROP COLUMN %s" % (
            self._db_api.enclose_object_reference(db_name, table_name),
            self.enclose_identifier(column_name),
        )
        return self.execute_ddl(sql, sync=sync)

    def expected_backend_column(
        self, canonical_column, override_used=None, decimal_padding_digits=None
    ):
        expected_data_type = self.expected_canonical_to_backend_type_map(
            override_used=override_used
        ).get(canonical_column.data_type)
        expected_precision_scale = self.expected_backend_precision_scale(
            canonical_column, decimal_padding_digits=decimal_padding_digits
        )
        if expected_precision_scale:
            return SnowflakeColumn(
                canonical_column.name,
                expected_data_type,
                data_precision=expected_precision_scale[0],
                data_scale=expected_precision_scale[1],
            )
        else:
            return SnowflakeColumn(canonical_column.name, expected_data_type)

    def expected_backend_precision_scale(
        self, canonical_column, decimal_padding_digits=None
    ):
        if canonical_column.data_type == GOE_TYPE_DECIMAL:
            if (
                canonical_column.data_precision is None
                and canonical_column.data_scale is None
            ):
                # We can't check this because these columns are sampled and have an unreliable spec
                return None
            else:
                # This should be a one-to-one mapping
                return canonical_column.data_precision, canonical_column.data_scale
        elif canonical_column.data_type == GOE_TYPE_INTEGER_38:
            return 38, 0
        else:
            return None

    def expected_canonical_to_backend_type_map(self, override_used=None):
        return {
            GOE_TYPE_FIXED_STRING: SNOWFLAKE_TYPE_TEXT,
            GOE_TYPE_LARGE_STRING: SNOWFLAKE_TYPE_TEXT,
            GOE_TYPE_VARIABLE_STRING: SNOWFLAKE_TYPE_TEXT,
            GOE_TYPE_BINARY: SNOWFLAKE_TYPE_BINARY,
            GOE_TYPE_LARGE_BINARY: SNOWFLAKE_TYPE_BINARY,
            GOE_TYPE_INTEGER_1: SNOWFLAKE_TYPE_NUMBER,
            GOE_TYPE_INTEGER_2: SNOWFLAKE_TYPE_NUMBER,
            GOE_TYPE_INTEGER_4: SNOWFLAKE_TYPE_NUMBER,
            GOE_TYPE_INTEGER_8: SNOWFLAKE_TYPE_NUMBER,
            GOE_TYPE_INTEGER_38: SNOWFLAKE_TYPE_NUMBER,
            GOE_TYPE_DECIMAL: SNOWFLAKE_TYPE_NUMBER,
            GOE_TYPE_DOUBLE: SNOWFLAKE_TYPE_FLOAT,
            GOE_TYPE_DATE: SNOWFLAKE_TYPE_DATE,
            GOE_TYPE_TIME: SNOWFLAKE_TYPE_TIME,
            GOE_TYPE_TIMESTAMP: SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
            GOE_TYPE_TIMESTAMP_TZ: SNOWFLAKE_TYPE_TIMESTAMP_TZ,
            GOE_TYPE_INTERVAL_DS: SNOWFLAKE_TYPE_TEXT,
            GOE_TYPE_INTERVAL_YM: SNOWFLAKE_TYPE_TEXT,
            GOE_TYPE_BOOLEAN: SNOWFLAKE_TYPE_BOOLEAN,
        }

    def expected_std_dim_offload_predicates(self) -> list:
        return [
            ("column(id) IS NULL", '"ID" IS NULL'),
            ("column(id) IS NOT NULL", '"ID" IS NOT NULL'),
            ("column(id) > numeric(4)", '"ID" > 4'),
            (
                "(column(ID) = numeric(10)) AND (column(ID) < numeric(2.2))",
                '("ID" = 10 AND "ID" < 2.2)',
            ),
            (
                "(column(ID) = numeric(10)) AND (column(ID) IS NULL)",
                '("ID" = 10 AND "ID" IS NULL)',
            ),
            ('column(TXN_DESC) = string("Oxford")', "\"TXN_DESC\" = 'Oxford'"),
            (
                "column(TXN_TIME) = datetime(1970-01-01)",
                "\"TXN_TIME\" = '1970-01-01'::TIMESTAMP_NTZ",
            ),
            (
                "column(TXN_TIME) = datetime(1970-01-01 12:13:14)",
                "\"TXN_TIME\" = '1970-01-01 12:13:14'::TIMESTAMP_NTZ",
            ),
        ]

    def expected_std_dim_synthetic_offload_predicates(self) -> list:
        """No partitioning on Snowflake"""
        return []

    def goe_type_mapping_generated_table_col_specs(self):
        definitions = self._goe_type_mapping_column_definitions()
        goe_type_mapping_cols, goe_type_mapping_names = [], []

        for col_dict in [
            definitions[col_name] for col_name in sorted(definitions.keys())
        ]:
            backend_column = col_dict["column"]
            goe_type_mapping_names.append(backend_column.name)
            if backend_column.data_type == SNOWFLAKE_TYPE_NUMBER:
                if col_dict.get("present_options"):
                    # This is a number of some kind and being CAST to something else so we provide specific test data.
                    literals = [1, 2, 3]
                    if col_dict["expected_canonical_column"].data_type in [
                        GOE_TYPE_INTEGER_1,
                        GOE_TYPE_INTEGER_2,
                        GOE_TYPE_INTEGER_4,
                        GOE_TYPE_INTEGER_8,
                    ]:
                        precision = self._canonical_integer_precision(
                            col_dict["expected_canonical_column"].data_type
                        )
                        literals = [
                            TestDecimal.min(precision),
                            TestDecimal.rnd(precision),
                            TestDecimal.max(precision),
                        ]
                    goe_type_mapping_cols.append(
                        {"column": backend_column, "literals": literals}
                    )
                else:
                    goe_type_mapping_cols.append({"column": backend_column})
            elif (
                col_dict["expected_canonical_column"].data_type == GOE_TYPE_INTERVAL_DS
            ):
                goe_type_mapping_cols.append(
                    {
                        "column": backend_column,
                        "literals": self._goe_type_mapping_interval_ds_test_values(),
                    }
                )
            elif (
                col_dict["expected_canonical_column"].data_type == GOE_TYPE_INTERVAL_YM
            ):
                goe_type_mapping_cols.append(
                    {
                        "column": backend_column,
                        "literals": self._goe_type_mapping_interval_ym_test_values(),
                    }
                )
            elif backend_column.is_string_based():
                goe_type_mapping_cols.append({"column": backend_column})
            elif col_dict["expected_canonical_column"].data_type in [
                GOE_TYPE_BINARY,
                GOE_TYPE_LARGE_BINARY,
            ]:
                goe_type_mapping_cols.append(
                    {
                        "column": backend_column,
                        "literals": ["binary1", "binary2", "binary3"],
                    }
                )
            else:
                goe_type_mapping_cols.append({"column": backend_column})
        return goe_type_mapping_cols, goe_type_mapping_names

    def load_table_fs_scheme_is_correct(self, load_db, table_name):
        """On Snowflake there are no load tables, always returns True."""
        return True

    def partition_has_stats(
        self, db_name, table_name, partition_tuples, colstats=False
    ):
        raise NotImplementedError("partition_has_stats() unsupported for Snowflake")

    def rename_column(self, db_name, table_name, column_name, new_name, sync=None):
        """Issue Snowflake ALTER TABLE ... RENAME COLUMN SQL statement."""
        assert db_name and table_name
        assert column_name and new_name
        assert isinstance(column_name, str)
        assert isinstance(new_name, str)

        if not self.get_column(db_name, table_name, column_name):
            raise BackendTestingApiException(
                "Table %s.%s does not have a column %s to rename"
                % (db_name, table_name, column_name)
            )
        sql = (
            "ALTER TABLE %(db_name)s.%(table_name)s RENAME COLUMN %(orig_name)s TO %(new_name)s"
            % {
                "db_name": self.enclose_identifier(db_name),
                "table_name": self.enclose_identifier(table_name),
                "orig_name": self.enclose_identifier(column_name),
                "new_name": self.enclose_identifier(new_name),
            }
        )
        return self.execute_ddl(sql, sync=sync)

    def select_single_non_null_value(
        self, db_name, table_name, column_name, project_expression
    ):
        return self._select_single_non_null_value_common(
            db_name, table_name, column_name, project_expression
        )

    def smart_connector_test_command(self, db_name=None, table_name=None):
        assert db_name and table_name
        return f"SELECT current_date() FROM {db_name}.{table_name} LIMIT 1"

    def sql_median_expression(self, db_name, table_name, column_name):
        """No single Snowflake function for median of any data type so pick to suit column"""
        column = self.get_column(db_name, table_name, column_name)
        if column.is_number_based():
            # Can't use MEDIAN because that splits the difference on a tie
            return (
                "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY %s)"
                % self.enclose_identifier(column_name)
            )
        elif column.is_string_based():
            return (
                "CHR(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY ASCII(%s)))"
                % self.enclose_identifier(column_name)
            )
        elif column.data_type == SNOWFLAKE_TYPE_DATE:
            return (
                "TO_TIMESTAMP(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY DATE_PART('EPOCH_SECOND',%s)))::DATE"
                % self.enclose_identifier(column_name)
            )
        elif column.data_type == SNOWFLAKE_TYPE_TIMESTAMP_NTZ:
            return (
                "TO_TIMESTAMP_NTZ(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY DATE_PART('EPOCH_NANOSECOND',%s)),9)"
                % self.enclose_identifier(column_name)
            )
        elif column.data_type == SNOWFLAKE_TYPE_TIMESTAMP_TZ:
            return (
                "TO_TIMESTAMP_TZ(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY DATE_PART('EPOCH_NANOSECOND',%s)),9)"
                % self.enclose_identifier(column_name)
            )
        else:
            # There is no simple function we can use therefore just accept the first value
            # Types dropping in here would be BINARY, TIME, BOOLEAN
            return self.enclose_identifier(column_name)

    def story_test_offload_nums_expected_backend_types(self, sampling_enabled=True):
        def number(p, s):
            return "%s(%s,%s)" % (SNOWFLAKE_TYPE_NUMBER, p, s)

        non_sampled_type = self.gen_default_numeric_column("x").format_data_type()
        return {
            STORY_TEST_OFFLOAD_NUMS_BARE_NUM: (
                number(4, 3) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_BARE_FLT: (
                SNOWFLAKE_TYPE_NUMBER if sampling_enabled else non_sampled_type
            ),
            # NUM_10_M5 is NUMBER(4,0) mapped to 2-BYTE and back to NUMBER(5)
            STORY_TEST_OFFLOAD_NUMS_NUM_4: number(5, 0),
            STORY_TEST_OFFLOAD_NUMS_NUM_3_2: number(3, 2),
            STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: number(38, 4),
            STORY_TEST_OFFLOAD_NUMS_NUM_3_5: number(5, 5),
            # NUM_10_M5 is NUMBER(10,0) mapped to 8-BYTE and back to NUMBER(19)
            STORY_TEST_OFFLOAD_NUMS_NUM_10_M5: number(19, 0),
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0: (
                number(10, 0) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3: (
                number(36, 3) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3: (
                number(37, 3) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3: (
                number(38, 3) if sampling_enabled else non_sampled_type
            ),
        }

    def story_test_table_extra_col_info(self):
        """Return a dict describing extra columns we can tag onto a present test table in order to test
        data type controls/outcomes.
        The returned dict is keyed by column name and has the following fields for each column:
            'sql_expression': A backend SQL expression to generate test data and "type" the column
            'length': The length of any string column
            'precision': Precision for number columns that support it
            'scale': Scale for number columns that support it
        """
        extra_cols = {
            STORY_TEST_BACKEND_DOUBLE_COL: {
                "sql_expression": "CAST(123.123 AS %s)" % SNOWFLAKE_TYPE_FLOAT
            },
            STORY_TEST_BACKEND_DECIMAL_PS_COL: {
                "sql_expression": "CAST(123.123 AS %s(10,3))" % SNOWFLAKE_TYPE_NUMBER,
                "precision": 10,
                "scale": 3,
            },
            STORY_TEST_BACKEND_DECIMAL_DEF_COL: {
                "sql_expression": "CAST(123 AS %s)" % SNOWFLAKE_TYPE_NUMBER
            },
            STORY_TEST_BACKEND_INT_1_COL: {
                "sql_expression": "CAST(1 AS %s(2))" % SNOWFLAKE_TYPE_NUMBER,
                "precision": 2,
                "scale": 0,
            },
            STORY_TEST_BACKEND_INT_2_COL: {
                "sql_expression": "CAST(1234 AS %s(4))" % SNOWFLAKE_TYPE_NUMBER,
                "precision": 4,
                "scale": 0,
            },
            STORY_TEST_BACKEND_INT_4_COL: {
                "sql_expression": "CAST(123456 AS %s(9))" % SNOWFLAKE_TYPE_NUMBER,
                "precision": 9,
                "scale": 0,
            },
            STORY_TEST_BACKEND_INT_8_COL: {
                "sql_expression": "CAST(1234567890123 AS %s(18))"
                % SNOWFLAKE_TYPE_NUMBER,
                "precision": 18,
                "scale": 0,
            },
            STORY_TEST_BACKEND_VAR_STR_COL: {
                "sql_expression": "CAST('this is text' AS %s(50))"
                % SNOWFLAKE_TYPE_TEXT,
                "length": 50,
                "char_semantics": CANONICAL_CHAR_SEMANTICS_CHAR,
            },
            STORY_TEST_BACKEND_VAR_STR_LONG_COL: {
                "sql_expression": "CAST('very long text' AS %s(5000))"
                % SNOWFLAKE_TYPE_TEXT,
                "length": 5000,
                "char_semantics": CANONICAL_CHAR_SEMANTICS_CHAR,
            },
            STORY_TEST_BACKEND_DATE_COL: {"sql_expression": "CURRENT_DATE()"},
            STORY_TEST_BACKEND_TIMESTAMP_COL: {
                "sql_expression": "CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)"
            },
            STORY_TEST_BACKEND_TIMESTAMP_TZ_COL: {
                "sql_expression": "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())"
            },
            STORY_TEST_BACKEND_BLOB_COL: {
                "sql_expression": "CAST(TO_BINARY('this is binary', 'UTF-8') AS %s(2001))"
                % SNOWFLAKE_TYPE_BINARY
            },
            STORY_TEST_BACKEND_RAW_COL: {
                "sql_expression": "CAST(TO_BINARY('this is binary', 'UTF-8') AS %s(100))"
                % SNOWFLAKE_TYPE_BINARY,
                "length": 100,
            },
            STORY_TEST_BACKEND_NULL_STR_COL: {
                "sql_expression": "CAST(NULL AS %s(30))" % SNOWFLAKE_TYPE_TEXT
            },
        }
        return extra_cols

    def transient_query_error_identification_strings(self) -> list:
        """No additional known transient errors on Snowflake"""
        return self._transient_query_error_identification_global_strings()

    def unit_test_query_options(self):
        return {"QUERY_TAG": "'my-tag'"}
