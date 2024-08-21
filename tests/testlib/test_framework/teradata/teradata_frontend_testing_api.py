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

""" TeradataFrontendTestingApi: An extension of (not yet created) FrontendApi used purely for code relating to the setup,
    processing and verification of integration tests.
"""

import datetime
import logging
import os
import random
from textwrap import dedent
from typing import Optional, Union

from goe.offload.column_metadata import (
    CANONICAL_CHAR_SEMANTICS_UNICODE,
    GOE_TYPE_BINARY,
    GOE_TYPE_DATE,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_FLOAT,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_INTERVAL_DS,
    GOE_TYPE_INTERVAL_YM,
    GOE_TYPE_LARGE_BINARY,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_TIME,
    GOE_TYPE_TIMESTAMP,
    GOE_TYPE_TIMESTAMP_TZ,
    GOE_TYPE_VARIABLE_STRING,
    CanonicalColumn,
)
from goe.offload.frontend_api import QueryParameter
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_transport_functions import run_os_cmd
from goe.offload.teradata.teradata_column import (
    TERADATA_TYPE_BIGINT,
    TERADATA_TYPE_BLOB,
    TERADATA_TYPE_BYTE,
    TERADATA_TYPE_BYTEINT,
    TERADATA_TYPE_CHAR,
    TERADATA_TYPE_CLOB,
    TERADATA_TYPE_DATE,
    TERADATA_TYPE_DECIMAL,
    TERADATA_TYPE_DOUBLE,
    TERADATA_TYPE_INTEGER,
    TERADATA_TYPE_INTERVAL_DS,
    TERADATA_TYPE_INTERVAL_YM,
    TERADATA_TYPE_NUMBER,
    TERADATA_TYPE_SMALLINT,
    TERADATA_TYPE_TIME,
    TERADATA_TYPE_TIME_TZ,
    TERADATA_TYPE_TIMESTAMP,
    TERADATA_TYPE_TIMESTAMP_TZ,
    TERADATA_TYPE_VARBYTE,
    TERADATA_TYPE_VARCHAR,
    TeradataColumn,
)
from goe.offload.teradata.teradata_frontend_api import (
    teradata_get_primary_partition_expression,
)
from pyodbc import SQL_WVARCHAR
from tests.testlib.test_framework.test_constants import (
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_FACT_HV_4,
    SALES_BASED_FACT_HV_5,
    SALES_BASED_FACT_HV_5_END,
    SALES_BASED_FACT_HV_5_END_NUM,
    SALES_BASED_FACT_HV_6,
    SALES_BASED_FACT_HV_6_END,
    SALES_BASED_FACT_HV_6_END_NUM,
    SALES_BASED_FACT_HV_7,
    SALES_BASED_FACT_PRE_HV,
    SALES_BASED_FACT_PRE_HV_NUM,
    SALES_BASED_FACT_PRE_LOWER,
    SALES_BASED_FACT_PRE_LOWER_NUM,
    SALES_BASED_LIST_HV_1,
    SALES_BASED_LIST_HV_2,
    SALES_BASED_LIST_HV_3,
    SALES_BASED_LIST_HV_4,
    SALES_BASED_LIST_HV_5,
    SALES_BASED_LIST_HV_6,
    SALES_BASED_LIST_HV_7,
    SALES_BASED_LIST_PRE_HV,
    UNICODE_NAME_TOKEN,
)
from tests.testlib.setup import gen_test_data
from tests.testlib.test_framework.frontend_testing_api import (
    FrontendTestingApiException,
    FrontendTestingApiInterface,
)
from tests.testlib.test_framework.test_functions import (
    get_test_set_sql_path,
    goe_wide_max_columns,
)
from tests.testlib.test_framework.test_value_generators import TestDecimal

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


##########################################################################
# CONSTANTS
##########################################################################

###########################################################################
# TeradataFrontendTestingApi
###########################################################################


class TeradataFrontendTestingApi(FrontendTestingApiInterface):
    def __init__(
        self,
        frontend_type,
        connection_options,
        messages,
        existing_connection=None,
        dry_run=False,
        do_not_connect=False,
        trace_action=None,
    ):
        """CONSTRUCTOR"""
        super().__init__(
            frontend_type,
            connection_options,
            messages,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _create_new_testing_client(
        self, existing_connection, trace_action_override=None
    ) -> FrontendTestingApiInterface:
        return TeradataFrontendTestingApi(
            self._frontend_type,
            self._connection_options,
            self._messages,
            existing_connection=existing_connection,
            dry_run=self._dry_run,
            trace_action=trace_action_override,
        )

    def _data_type_supports_precision_and_scale(self, column_or_type):
        if isinstance(column_or_type, TeradataColumn):
            data_type = column_or_type.data_type
        else:
            data_type = column_or_type
        return bool(data_type in (TERADATA_TYPE_DECIMAL, TERADATA_TYPE_NUMBER))

    def _gen_column_data(
        self,
        column,
        row_index,
        from_list=None,
        ascii_only=False,
        all_chars_notnull=False,
        ordered=False,
        no_newlines=False,
        allow_nan=True,
        allow_inf=True,
    ):
        assert isinstance(column, TeradataColumn)
        if column.data_type == TERADATA_TYPE_VARCHAR:
            return gen_test_data.gen_varchar(
                row_index,
                column.char_length or column.data_length,
                from_list=from_list,
                ordered=ordered,
                ascii7_only=ascii_only,
                notnull=all_chars_notnull,
                no_newlines=no_newlines,
            )
        elif column.data_type == TERADATA_TYPE_CHAR:
            return gen_test_data.gen_char(
                row_index,
                column.char_length or column.data_length,
                from_list=from_list,
                ordered=ordered,
                ascii7_only=ascii_only,
                notnull=all_chars_notnull,
                no_newlines=no_newlines,
            )
        elif column.data_type in (TERADATA_TYPE_NUMBER, TERADATA_TYPE_DECIMAL):
            return gen_test_data.gen_number(
                row_index,
                precision=column.data_precision,
                scale=column.data_scale,
                from_list=from_list,
                ordered=ordered,
            )
        elif column.data_type == TERADATA_TYPE_BYTEINT:
            return gen_test_data.gen_int(
                row_index, precision=2, from_list=from_list, ordered=ordered
            )
        elif column.data_type == TERADATA_TYPE_SMALLINT:
            return gen_test_data.gen_int(
                row_index, precision=4, from_list=from_list, ordered=ordered
            )
        elif column.data_type == TERADATA_TYPE_INTEGER:
            return gen_test_data.gen_int(
                row_index, precision=9, from_list=from_list, ordered=ordered
            )
        elif column.data_type == TERADATA_TYPE_BIGINT:
            return gen_test_data.gen_int(
                row_index, precision=18, from_list=from_list, ordered=ordered
            )
        elif column.data_type == TERADATA_TYPE_DATE:
            return gen_test_data.gen_date(
                row_index, from_list=from_list, ordered=ordered
            )
        elif column.data_type in [TERADATA_TYPE_TIMESTAMP, TERADATA_TYPE_TIMESTAMP_TZ]:
            return gen_test_data.gen_timestamp(
                row_index, scale=column.data_scale, from_list=from_list, ordered=ordered
            )
        elif column.data_type in [TERADATA_TYPE_TIME, TERADATA_TYPE_TIME_TZ]:
            return gen_test_data.gen_time(
                row_index, scale=column.data_scale, from_list=from_list, ordered=ordered
            )
        elif column.data_type == TERADATA_TYPE_INTERVAL_YM:
            return gen_test_data.gen_interval_ym(precision=column.data_precision)
        elif column.data_type == TERADATA_TYPE_INTERVAL_DS:
            return gen_test_data.gen_interval_ds(
                precision=column.data_precision, scale=column.data_scale
            )
        elif column.data_type == TERADATA_TYPE_DOUBLE:
            return gen_test_data.gen_float(
                row_index, from_list=from_list, allow_nan=allow_nan, allow_inf=allow_inf
            )
        elif column.data_type == TERADATA_TYPE_CLOB:
            return gen_test_data.gen_varchar(
                row_index, 100, from_list=from_list, no_newlines=no_newlines
            )
        elif column.data_type == TERADATA_TYPE_BLOB:
            return gen_test_data.gen_varchar(
                row_index, 100, from_list=from_list, no_newlines=no_newlines
            )
        elif column.data_type in (TERADATA_TYPE_BYTE, TERADATA_TYPE_VARBYTE):
            if column.data_length == 16:
                return gen_test_data.gen_uuid(row_index)
            elif column.data_length:
                return gen_test_data.gen_bytes(row_index, column.data_length)
            else:
                return gen_test_data.gen_bytes(row_index, 2000)
        else:
            self._log(
                f"Attempt to generate data for unsupported RDBMS type: {column.data_type}"
            )

    def _goe_chars_column_definitions(
        self, ascii_only=False, all_chars_notnull=False, supported_canonical_types=None
    ) -> list:
        name_id = 0

        def name():
            nonlocal name_id
            name_id = name_id + 1
            col_name = f"COLUMN_{name_id}"
            return col_name

        column_list = [
            {"column": self._id_column(), "ordered": True},
            {
                "column": TeradataColumn(name(), TERADATA_TYPE_CHAR, data_length=100),
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
                "ordered": True,
            },
        ]

        return column_list

    def _goe_type_mapping_column_definitions(
        self,
        max_backend_precision,
        max_backend_scale,
        max_decimal_integral_magnitude,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        filter_column=None,
    ):
        """Returns a dict of dicts defining columns for GOE_TYPE_MAPPING test table.
        Individual backends have an expected_canonical_to_backend_type_map() method defining which of these columns,
        by expected canonical type, are to be included for that implementation.
        filter_column can be used to fetch just a single column dict.
        """

        def name(*args):
            assert args
            col_name = "COL_" + "_".join(args).replace(" ", "_")
            return col_name

        generic_scale = min(max_backend_scale, 18)
        max_precision = min(max_backend_precision, 38)
        all_columns = {
            name(TERADATA_TYPE_BIGINT): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_BIGINT), TERADATA_TYPE_BIGINT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_BIGINT), GOE_TYPE_INTEGER_8
                ),
            },
            name(TERADATA_TYPE_BIGINT, GOE_TYPE_INTEGER_4): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_BIGINT, GOE_TYPE_INTEGER_4),
                    TERADATA_TYPE_BIGINT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_BIGINT, GOE_TYPE_INTEGER_4),
                    GOE_TYPE_INTEGER_4,
                ),
                "literals": [
                    TestDecimal.min(9),
                    TestDecimal.rnd(9),
                    TestDecimal.max(9),
                ],
            },
            name(TERADATA_TYPE_BLOB): {
                "column": TeradataColumn(name(TERADATA_TYPE_BLOB), TERADATA_TYPE_BLOB),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_BLOB), GOE_TYPE_LARGE_BINARY
                ),
                "literals": ["lob-a", "lob-b", "lob-c"],
            },
            name(TERADATA_TYPE_BYTE): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_BYTE), TERADATA_TYPE_BYTE, data_length=30
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_BYTE), GOE_TYPE_BINARY
                ),
            },
            name(TERADATA_TYPE_BYTEINT): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_BYTEINT), TERADATA_TYPE_BYTEINT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_BYTEINT), GOE_TYPE_INTEGER_1
                ),
            },
            name(TERADATA_TYPE_BYTEINT, GOE_TYPE_INTEGER_2): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_BYTEINT, GOE_TYPE_INTEGER_2),
                    TERADATA_TYPE_BYTEINT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_BYTEINT, GOE_TYPE_INTEGER_2),
                    GOE_TYPE_INTEGER_2,
                ),
            },
            name(TERADATA_TYPE_CHAR, "3"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_CHAR, "3"), TERADATA_TYPE_CHAR, data_length=3
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_CHAR, "3"),
                    GOE_TYPE_FIXED_STRING,
                    data_length=3,
                ),
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
            },
            name(TERADATA_TYPE_CHAR, "3", UNICODE_NAME_TOKEN): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_CHAR, "3", UNICODE_NAME_TOKEN),
                    TERADATA_TYPE_CHAR,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_CHAR, "3", UNICODE_NAME_TOKEN),
                    GOE_TYPE_FIXED_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "offload_options": {
                    "unicode_string_columns_csv": name(
                        TERADATA_TYPE_CHAR, "3", UNICODE_NAME_TOKEN
                    )
                },
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
            },
            name(TERADATA_TYPE_CLOB): {
                "column": TeradataColumn(name(TERADATA_TYPE_CLOB), TERADATA_TYPE_CLOB),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_CLOB), GOE_TYPE_LARGE_STRING
                ),
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
                # Keep row size small for mapping table
                "literals": ["lob-a", "lob-b", "lob-c"],
            },
            name(TERADATA_TYPE_CLOB, UNICODE_NAME_TOKEN): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_CLOB, UNICODE_NAME_TOKEN),
                    TERADATA_TYPE_CLOB,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_CLOB, UNICODE_NAME_TOKEN),
                    GOE_TYPE_LARGE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "offload_options": {
                    "unicode_string_columns_csv": name(
                        TERADATA_TYPE_CLOB, UNICODE_NAME_TOKEN
                    )
                },
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
                # Keep row size small for mapping table
                "literals": ["lob-a", "lob-b", "lob-c"],
            },
            name(TERADATA_TYPE_DATE): {
                "column": TeradataColumn(name(TERADATA_TYPE_DATE), TERADATA_TYPE_DATE),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DATE), GOE_TYPE_DATE
                ),
            },
            name(TERADATA_TYPE_DATE, GOE_TYPE_VARIABLE_STRING): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DATE, GOE_TYPE_VARIABLE_STRING),
                    TERADATA_TYPE_DATE,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DATE, GOE_TYPE_VARIABLE_STRING),
                    GOE_TYPE_VARIABLE_STRING,
                ),
                "offload_options": {
                    "variable_string_columns_csv": name(
                        TERADATA_TYPE_DATE, GOE_TYPE_VARIABLE_STRING
                    )
                },
            },
            name(TERADATA_TYPE_DECIMAL): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL), TERADATA_TYPE_DECIMAL
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL), GOE_TYPE_INTEGER_4
                ),
                # Naked DECIMAL defaults to DECIMAL(5,0) so we expect INT4 and insert suitable data below.
                "literals": [
                    TestDecimal.min(5, 0),
                    TestDecimal.rnd(5, 0),
                    TestDecimal.max(5, 0),
                ],
            },
            name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_1): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_1),
                    TERADATA_TYPE_DECIMAL,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_1),
                    GOE_TYPE_INTEGER_1,
                ),
                "offload_options": {
                    "integer_1_columns_csv": name(
                        TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_1
                    )
                },
                "literals": [
                    TestDecimal.min(2),
                    TestDecimal.rnd(2),
                    TestDecimal.max(2),
                ],
            },
            name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_2): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_2),
                    TERADATA_TYPE_DECIMAL,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_2),
                    GOE_TYPE_INTEGER_2,
                ),
                "offload_options": {
                    "integer_2_columns_csv": name(
                        TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_2
                    )
                },
                "literals": [
                    TestDecimal.min(4),
                    TestDecimal.rnd(4),
                    TestDecimal.max(4),
                ],
            },
            name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_4): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_4),
                    TERADATA_TYPE_DECIMAL,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_4),
                    GOE_TYPE_INTEGER_4,
                ),
                "offload_options": {
                    "integer_4_columns_csv": name(
                        TERADATA_TYPE_DECIMAL, GOE_TYPE_INTEGER_4
                    )
                },
                "literals": [
                    TestDecimal.min(5),
                    TestDecimal.rnd(5),
                    TestDecimal.max(5),
                ],
            },
            # Naked DECIMAL cannot hold data beyond GOE_TYPE_INTEGER_4 therefore not testing int8/38 mappings.
            name(TERADATA_TYPE_DECIMAL, GOE_TYPE_DOUBLE): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_DOUBLE),
                    TERADATA_TYPE_DECIMAL,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, GOE_TYPE_DOUBLE), GOE_TYPE_DOUBLE
                ),
                "offload_options": {
                    "double_columns_csv": name(TERADATA_TYPE_DECIMAL, GOE_TYPE_DOUBLE)
                },
                "literals": [1.5, 2.5, 3.5],
            },
            name(TERADATA_TYPE_DECIMAL, "2"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, "2"),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, "2"), GOE_TYPE_INTEGER_1
                ),
            },
            name(TERADATA_TYPE_DECIMAL, "4"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, "4"),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, "4"), GOE_TYPE_INTEGER_2
                ),
            },
            name(TERADATA_TYPE_DECIMAL, "9"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, "9"),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, "9"), GOE_TYPE_INTEGER_4
                ),
            },
            name(TERADATA_TYPE_DECIMAL, "18"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, "18"),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, "18"), GOE_TYPE_INTEGER_8
                ),
            },
            name(TERADATA_TYPE_DECIMAL, "19"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, "19"),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=19,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, "19"),
                    GOE_TYPE_INTEGER_38,
                    data_precision=19,
                ),
            },
            name(TERADATA_TYPE_DECIMAL, str(max_decimal_integral_magnitude)): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, str(max_decimal_integral_magnitude)),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=max_decimal_integral_magnitude,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, str(max_decimal_integral_magnitude)),
                    GOE_TYPE_INTEGER_38,
                ),
            },
            name(TERADATA_TYPE_DECIMAL, str(max_precision), str(generic_scale)): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, str(max_precision), str(generic_scale)),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=max_precision,
                    data_scale=generic_scale,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, str(max_precision), str(generic_scale)),
                    GOE_TYPE_DECIMAL,
                    data_precision=max_precision,
                    data_scale=generic_scale,
                ),
            },
            name(TERADATA_TYPE_DECIMAL, "9", "2", GOE_TYPE_DECIMAL, "10", "3"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DECIMAL, "9", "2", GOE_TYPE_DECIMAL, "10", "3"),
                    TERADATA_TYPE_DECIMAL,
                    data_precision=9,
                    data_scale=2,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DECIMAL, "9", "2", GOE_TYPE_DECIMAL, "10", "3"),
                    GOE_TYPE_DECIMAL,
                    data_precision=10,
                    data_scale=3,
                ),
                "offload_options": {
                    "decimal_columns_csv_list": [
                        name(
                            TERADATA_TYPE_DECIMAL,
                            "9",
                            "2",
                            GOE_TYPE_DECIMAL,
                            "10",
                            "3",
                        )
                    ],
                    "decimal_columns_type_list": ["10,3"],
                },
                "literals": [
                    TestDecimal.min(9, 2),
                    TestDecimal.rnd(9, 2),
                    TestDecimal.max(9, 2),
                ],
            },
            name(TERADATA_TYPE_DOUBLE): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_DOUBLE), TERADATA_TYPE_DOUBLE
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_DOUBLE), GOE_TYPE_DOUBLE
                ),
            },
            name(TERADATA_TYPE_INTEGER): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_INTEGER), TERADATA_TYPE_INTEGER
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_INTEGER), GOE_TYPE_INTEGER_4
                ),
            },
            name(TERADATA_TYPE_INTEGER, GOE_TYPE_INTEGER_2): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_INTEGER, GOE_TYPE_INTEGER_2),
                    TERADATA_TYPE_INTEGER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_INTEGER, GOE_TYPE_INTEGER_2),
                    GOE_TYPE_INTEGER_2,
                ),
                "offload_options": {
                    "integer_2_columns_csv": name(
                        TERADATA_TYPE_INTEGER, GOE_TYPE_INTEGER_2
                    )
                },
                "literals": [
                    TestDecimal.min(4),
                    TestDecimal.rnd(4),
                    TestDecimal.max(4),
                ],
            },
            name(TERADATA_TYPE_INTERVAL_DS): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_INTERVAL_DS),
                    TERADATA_TYPE_INTERVAL_DS,
                    data_precision=4,
                    data_scale=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_INTERVAL_DS), GOE_TYPE_INTERVAL_DS
                ),
            },
            name(TERADATA_TYPE_INTERVAL_YM): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_INTERVAL_YM),
                    TERADATA_TYPE_INTERVAL_YM,
                    data_precision=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_INTERVAL_YM), GOE_TYPE_INTERVAL_YM
                ),
            },
            name(TERADATA_TYPE_NUMBER): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER), TERADATA_TYPE_NUMBER
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER), GOE_TYPE_DECIMAL
                ),
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_1): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_1),
                    TERADATA_TYPE_NUMBER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_1),
                    GOE_TYPE_INTEGER_1,
                ),
                "offload_options": {
                    "integer_1_columns_csv": name(
                        TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_1
                    )
                },
                "literals": [
                    TestDecimal.min(2),
                    TestDecimal.rnd(2),
                    TestDecimal.max(2),
                ],
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_2): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_2),
                    TERADATA_TYPE_NUMBER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_2),
                    GOE_TYPE_INTEGER_2,
                ),
                "offload_options": {
                    "integer_2_columns_csv": name(
                        TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_2
                    )
                },
                "literals": [
                    TestDecimal.min(4),
                    TestDecimal.rnd(4),
                    TestDecimal.max(4),
                ],
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_4): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_4),
                    TERADATA_TYPE_NUMBER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_4),
                    GOE_TYPE_INTEGER_4,
                ),
                "offload_options": {
                    "integer_4_columns_csv": name(
                        TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_4
                    )
                },
                "literals": [
                    TestDecimal.min(9),
                    TestDecimal.rnd(9),
                    TestDecimal.max(9),
                ],
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_8): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_8),
                    TERADATA_TYPE_NUMBER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_8),
                    GOE_TYPE_INTEGER_8,
                ),
                "offload_options": {
                    "integer_8_columns_csv": name(
                        TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_8
                    )
                },
                "literals": [
                    TestDecimal.min(18),
                    TestDecimal.rnd(18),
                    TestDecimal.max(18),
                ],
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_38): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_38),
                    TERADATA_TYPE_NUMBER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_38),
                    GOE_TYPE_INTEGER_38,
                ),
                "offload_options": {
                    "integer_38_columns_csv": name(
                        TERADATA_TYPE_NUMBER, GOE_TYPE_INTEGER_38
                    )
                },
                # 'test' imposes a max precision of 35, I think due to shortcomings of cx-Oracle.
                # We impose the same limit here to ensure no loss of value accuracy.
                "literals": [
                    TestDecimal.min(35),
                    TestDecimal.rnd(35),
                    TestDecimal.max(35),
                ],
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_DOUBLE): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_DOUBLE), TERADATA_TYPE_NUMBER
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_DOUBLE), GOE_TYPE_DOUBLE
                ),
                "offload_options": {
                    "double_columns_csv": name(TERADATA_TYPE_NUMBER, GOE_TYPE_DOUBLE)
                },
                "literals": [1.5, 2.5, 3.5],
            },
            name(TERADATA_TYPE_NUMBER, "2"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, "2"),
                    TERADATA_TYPE_NUMBER,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, "2"), GOE_TYPE_INTEGER_1
                ),
            },
            name(TERADATA_TYPE_NUMBER, "4"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, "4"),
                    TERADATA_TYPE_NUMBER,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, "4"), GOE_TYPE_INTEGER_2
                ),
            },
            name(TERADATA_TYPE_NUMBER, "9"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, "9"),
                    TERADATA_TYPE_NUMBER,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, "9"), GOE_TYPE_INTEGER_4
                ),
            },
            name(TERADATA_TYPE_NUMBER, "18"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, "18"),
                    TERADATA_TYPE_NUMBER,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, "18"), GOE_TYPE_INTEGER_8
                ),
            },
            name(TERADATA_TYPE_NUMBER, "19"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, "19"),
                    TERADATA_TYPE_NUMBER,
                    data_precision=19,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, "19"),
                    GOE_TYPE_INTEGER_38,
                    data_precision=19,
                ),
            },
            name(TERADATA_TYPE_NUMBER, str(max_decimal_integral_magnitude)): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, str(max_decimal_integral_magnitude)),
                    TERADATA_TYPE_NUMBER,
                    data_precision=max_decimal_integral_magnitude,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, str(max_decimal_integral_magnitude)),
                    GOE_TYPE_INTEGER_38,
                ),
            },
            name(TERADATA_TYPE_NUMBER, str(max_precision), str(generic_scale)): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, str(max_precision), str(generic_scale)),
                    TERADATA_TYPE_NUMBER,
                    data_precision=max_precision,
                    data_scale=generic_scale,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, str(max_precision), str(generic_scale)),
                    GOE_TYPE_DECIMAL,
                    data_precision=max_precision,
                    data_scale=generic_scale,
                ),
            },
            name(TERADATA_TYPE_NUMBER, GOE_TYPE_DECIMAL, "10", "3"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_DECIMAL, "10", "3"),
                    TERADATA_TYPE_NUMBER,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, GOE_TYPE_DECIMAL, "10", "3"),
                    GOE_TYPE_DECIMAL,
                    data_precision=10,
                    data_scale=3,
                ),
                "offload_options": {
                    "decimal_columns_csv_list": [
                        name(TERADATA_TYPE_NUMBER, GOE_TYPE_DECIMAL, "10", "3")
                    ],
                    "decimal_columns_type_list": ["10,3"],
                },
                "literals": [
                    TestDecimal.min(10, 3),
                    TestDecimal.rnd(10, 3),
                    TestDecimal.max(10, 3),
                ],
            },
            name(TERADATA_TYPE_NUMBER, "9", "2", GOE_TYPE_DECIMAL, "10", "3"): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_NUMBER, "9", "2", GOE_TYPE_DECIMAL, "10", "3"),
                    TERADATA_TYPE_NUMBER,
                    data_precision=9,
                    data_scale=2,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_NUMBER, "9", "2", GOE_TYPE_DECIMAL, "10", "3"),
                    GOE_TYPE_DECIMAL,
                    data_precision=10,
                    data_scale=3,
                ),
                "offload_options": {
                    "decimal_columns_csv_list": [
                        name(
                            TERADATA_TYPE_NUMBER,
                            "9",
                            "2",
                            GOE_TYPE_DECIMAL,
                            "10",
                            "3",
                        )
                    ],
                    "decimal_columns_type_list": ["10,3"],
                },
                "literals": [
                    TestDecimal.min(9, 2),
                    TestDecimal.rnd(9, 2),
                    TestDecimal.max(9, 2),
                ],
            },
            name(TERADATA_TYPE_SMALLINT): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_SMALLINT), TERADATA_TYPE_SMALLINT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_SMALLINT), GOE_TYPE_INTEGER_4
                ),
            },
            name(TERADATA_TYPE_SMALLINT, GOE_TYPE_INTEGER_4): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_SMALLINT, GOE_TYPE_INTEGER_4),
                    TERADATA_TYPE_SMALLINT,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_SMALLINT, GOE_TYPE_INTEGER_4),
                    GOE_TYPE_INTEGER_4,
                ),
                "offload_options": {
                    "integer_4_columns_csv": name(
                        TERADATA_TYPE_SMALLINT, GOE_TYPE_INTEGER_4
                    )
                },
                "literals": [
                    TestDecimal.min(4),
                    TestDecimal.rnd(4),
                    TestDecimal.max(4),
                ],
            },
            name(TERADATA_TYPE_TIME): {
                "column": TeradataColumn(name(TERADATA_TYPE_TIME), TERADATA_TYPE_TIME),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_TIME), GOE_TYPE_TIME
                ),
            },
            name(TERADATA_TYPE_TIMESTAMP): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_TIMESTAMP), TERADATA_TYPE_TIMESTAMP
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_TIMESTAMP), GOE_TYPE_TIMESTAMP
                ),
            },
            name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_DATE): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_DATE),
                    TERADATA_TYPE_TIMESTAMP,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_DATE), GOE_TYPE_DATE
                ),
                "offload_options": {
                    "date_columns_csv": name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_DATE)
                },
                # Including 1970-01-01 because Python datetime64 understands this as False due to being Unix epoch.
                "literals": [
                    datetime.datetime(1970, 1, 1),
                    datetime.datetime(1970, 1, 2),
                    datetime.datetime(1971, 2, 1),
                    datetime.datetime(1971, 2, 2),
                ],
            },
            name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP_TZ): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP_TZ),
                    TERADATA_TYPE_TIMESTAMP,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP_TZ),
                    GOE_TYPE_TIMESTAMP_TZ,
                ),
                "offload_options": {
                    "timestamp_tz_columns_csv": name(
                        TERADATA_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP_TZ
                    )
                },
            },
            name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_VARIABLE_STRING): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_VARIABLE_STRING),
                    TERADATA_TYPE_TIMESTAMP,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_TIMESTAMP, GOE_TYPE_VARIABLE_STRING),
                    GOE_TYPE_VARIABLE_STRING,
                ),
                "offload_options": {
                    "variable_string_columns_csv": name(
                        TERADATA_TYPE_TIMESTAMP, GOE_TYPE_VARIABLE_STRING
                    )
                },
            },
            name(TERADATA_TYPE_TIMESTAMP_TZ): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_TIMESTAMP_TZ), TERADATA_TYPE_TIMESTAMP_TZ
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_TIMESTAMP_TZ), GOE_TYPE_TIMESTAMP_TZ
                ),
            },
            name(TERADATA_TYPE_VARBYTE): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_VARBYTE), TERADATA_TYPE_VARBYTE, data_length=30
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_VARBYTE), GOE_TYPE_BINARY
                ),
            },
            name(TERADATA_TYPE_VARCHAR): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_VARCHAR), TERADATA_TYPE_VARCHAR, data_length=30
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_VARCHAR), GOE_TYPE_VARIABLE_STRING
                ),
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
            },
            name(TERADATA_TYPE_VARCHAR, UNICODE_NAME_TOKEN): {
                "column": TeradataColumn(
                    name(TERADATA_TYPE_VARCHAR, UNICODE_NAME_TOKEN),
                    TERADATA_TYPE_VARCHAR,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(TERADATA_TYPE_VARCHAR, UNICODE_NAME_TOKEN),
                    GOE_TYPE_VARIABLE_STRING,
                ),
                "offload_options": {
                    "unicode_string_columns_csv": name(
                        TERADATA_TYPE_VARCHAR, UNICODE_NAME_TOKEN
                    )
                },
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
            },
        }
        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    def _goe_types_column_definitions(
        self,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        include_interval_columns=True,
    ) -> list:
        name_id = 0

        def name():
            nonlocal name_id
            name_id = name_id + 1
            col_name = f"COLUMN_{name_id}"
            return col_name

        column_list = [
            {"column": self._id_column(), "ordered": True},
            {
                "column": TeradataColumn(name(), TERADATA_TYPE_CHAR, data_length=100),
                "ascii_only": True,
                "notnull": all_chars_notnull,
            },
            {
                "column": TeradataColumn(
                    name(), TERADATA_TYPE_VARCHAR, data_length=1100
                ),
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
            },
            {"column": TeradataColumn(name(), TERADATA_TYPE_DATE)},
            {"column": TeradataColumn(name(), TERADATA_TYPE_TIMESTAMP, data_scale=0)},
            {"column": TeradataColumn(name(), TERADATA_TYPE_TIMESTAMP)},
            {"column": TeradataColumn(name(), TERADATA_TYPE_TIMESTAMP_TZ)},
            {"column": TeradataColumn(name(), TERADATA_TYPE_TIME)},
        ]
        if include_interval_columns:
            # TODO there may be more intervals to add to this list as we continue implementation
            column_list.extend(
                [
                    {
                        "column": TeradataColumn(
                            name(), TERADATA_TYPE_INTERVAL_YM, data_precision=4
                        )
                    },
                    {
                        "column": TeradataColumn(
                            name(),
                            TERADATA_TYPE_INTERVAL_DS,
                            data_precision=4,
                            data_scale=6,
                        )
                    },
                ]
            )
        column_list.extend(
            [
                {"column": TeradataColumn(name(), TERADATA_TYPE_BYTEINT)},
                {"column": TeradataColumn(name(), TERADATA_TYPE_SMALLINT)},
                {"column": TeradataColumn(name(), TERADATA_TYPE_INTEGER)},
                {"column": TeradataColumn(name(), TERADATA_TYPE_BIGINT)},
                {"column": TeradataColumn(name(), TERADATA_TYPE_NUMBER)},
                {
                    "column": TeradataColumn(
                        name(), TERADATA_TYPE_NUMBER, data_precision=38
                    )
                },
                {"column": TeradataColumn(name(), TERADATA_TYPE_DECIMAL)},
                {
                    "column": TeradataColumn(
                        name(), TERADATA_TYPE_DECIMAL, data_precision=38
                    )
                },
                {"column": TeradataColumn(name(), TERADATA_TYPE_DOUBLE)},
                {"column": TeradataColumn(name(), TERADATA_TYPE_BYTE, data_length=16)},
                {"column": TeradataColumn(name(), TERADATA_TYPE_BYTE, data_length=100)},
                {
                    "column": TeradataColumn(
                        name(), TERADATA_TYPE_VARBYTE, data_length=100
                    )
                },
                {
                    "column": TeradataColumn(name(), TERADATA_TYPE_CLOB),
                    "ascii_only": ascii_only,
                    "notnull": all_chars_notnull,
                },
                {
                    "column": TeradataColumn(name(), TERADATA_TYPE_BLOB),
                    "ascii_only": ascii_only,
                    "notnull": all_chars_notnull,
                },
                {
                    "column": TeradataColumn(
                        name(), TERADATA_TYPE_NUMBER, data_precision=1
                    ),
                    "column_spec_extra_clause": "AS (MOD(id,10)) VIRTUAL",
                },
            ]
        )

        return column_list

    def _goe_wide_column_definitions(
        self,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        backend_max_test_column_count=None,
    ) -> list:
        name_id = 0

        def name():
            nonlocal name_id
            name_id = name_id + 1
            col_name = f"COLUMN_{name_id}"
            return col_name

        column_list = [{"column": self._id_column(), "ordered": True}]
        column_list.extend(
            {"column": TeradataColumn(name(), TERADATA_TYPE_NUMBER, data_precision=9)}
            for _ in range(5)
        )
        column_list.extend(
            {
                "column": TeradataColumn(name(), TERADATA_TYPE_VARCHAR, data_length=10),
                "ascii_only": ascii_only,
                "notnull": all_chars_notnull,
            }
            for _ in range(5)
        )
        column_list.extend(
            {"column": TeradataColumn(name(), TERADATA_TYPE_DATE)} for _ in range(5)
        )
        column_list.extend(
            {"column": TeradataColumn(name(), TERADATA_TYPE_TIMESTAMP, data_scale=0)}
            for _ in range(5)
        )

        extra_column_count = (
            goe_wide_max_columns(self, backend_max_test_column_count) - 20
        )

        extra_cols = [
            random.choice(
                [
                    {
                        "column": TeradataColumn(
                            "no-name", TERADATA_TYPE_NUMBER, data_precision=9
                        )
                    },
                    {
                        "column": TeradataColumn(
                            "no-name", TERADATA_TYPE_VARCHAR, data_length=10
                        ),
                        "ascii_only": ascii_only,
                        "notnull": all_chars_notnull,
                    },
                    {"column": TeradataColumn("no-name", TERADATA_TYPE_DATE)},
                    {
                        "column": TeradataColumn(
                            "no-name", TERADATA_TYPE_TIMESTAMP, data_scale=0
                        )
                    },
                ]
            )
            for _ in range(extra_column_count)
        ]
        for col in extra_cols:
            col["column"].name = name()

        column_list.extend(extra_cols)
        return column_list

    def _id_column(self):
        return TeradataColumn("ID", TERADATA_TYPE_BIGINT)

    def _populate_generated_test_table(
        self, schema, table_name, columns, rows, fastexecute=True
    ):
        column_list = [_["column"] for _ in columns]
        sql = "INSERT INTO %s (%s) VALUES (%s);" % (
            self._db_api.enclose_object_reference(schema, table_name),
            ",".join(_.name for _ in column_list),
            ",".join("?" for _ in column_list),
        )
        data = [
            [
                self._gen_column_data(
                    col["column"],
                    ri,
                    from_list=col.get("literals"),
                    ascii_only=col.get("ascii_only"),
                    all_chars_notnull=col.get("notnull"),
                    ordered=col.get("ordered"),
                    no_newlines=col.get("no_newlines"),
                    allow_nan=col.get("allow_nan", True),
                    allow_inf=col.get("allow_inf", True),
                )
                for col in columns
            ]
            for ri in range(int(rows))
        ]
        query_params = [[QueryParameter(None, _) for _ in row] for row in data]

        if fastexecute:
            # Loop over the columns and see if we need to setinputsizes for any
            inputsize_datatypes = [
                TERADATA_TYPE_INTERVAL_DS,
                TERADATA_TYPE_INTERVAL_YM,
                TERADATA_TYPE_BLOB,
                TERADATA_TYPE_CLOB,
                TERADATA_TYPE_VARBYTE,
                TERADATA_TYPE_BYTE,
            ]
            inputsizes = []
            if any(
                map(lambda x: x["column"].data_type in inputsize_datatypes, columns)
            ):
                for i, col in enumerate(columns):
                    if col["column"].data_type in inputsize_datatypes:
                        # Get the max size from the data list
                        inputsize = (
                            SQL_WVARCHAR,
                            max(map(lambda x: len(x[i]) if x[i] else 0, data)),
                            0,
                        )
                    else:
                        inputsize = None
                    inputsizes.append(inputsize)
            self._db_api.fast_executemany_dml(
                sql, query_params=query_params, param_inputsizes=inputsizes
            )
        else:
            self._db_api.executemany_dml(sql, query_params=query_params)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def collect_table_stats_sql_text(self, schema, table_name) -> str:
        return f"COLLECT SUMMARY STATISTICS ON {schema}.{table_name}"

    def drop_table(self, schema, table_name):
        """Obviously this is dangerous, that's why it is in this TestingApi only."""
        try:
            return self._db_api.execute_ddl(
                f"DROP TABLE {schema}.{table_name}", log_level=VERBOSE
            )
        except Exception as exc:
            if "42S02" in str(exc):
                # Nothing to drop
                pass
            else:
                self._log("Drop table exception: {}".format(str(exc)), detail=VERBOSE)
                raise

    def expected_std_dim_offload_predicates(self):
        """Return a list of tuples of GOE offload predicates and expected frontend predicate"""
        return [
            (
                "(column(ID) = numeric(10)) AND (column(ID) < numeric(2.2))",
                '("ID" = 10 AND "ID" < 2.2)',
            ),
            (
                "(column(ID) IS NULL) AND (column(ID) IS NOT NULL)",
                '("ID" IS NULL AND "ID" IS NOT NULL)',
            ),
            (
                "(column(ID) is null) AND (column(ID) is not null)",
                '("ID" IS NULL AND "ID" IS NOT NULL)',
            ),
            (
                "(column(ID) = numeric(1234567890123456789012345))",
                '"ID" = 1234567890123456789012345',
            ),
            (
                "(column(ID) = numeric(-1234567890123456789012345))",
                '"ID" = -1234567890123456789012345',
            ),
            (
                "(column(id) = numeric(0.00000000000000000001))",
                '"ID" = 0.00000000000000000001',
            ),
            (
                "(column(ID) = numeric(-0.00000000000000000001))",
                '"ID" = -0.00000000000000000001',
            ),
            (
                "(column(ID) in (numeric(-10),numeric(0),numeric(10)))",
                '"ID" IN (-10, 0, 10)',
            ),
            (
                'column(TXN_DESC) = string("Internet")',
                "\"TXN_DESC\" = 'Internet'",
            ),
            (
                '(column(TXN_DESC) = string("Internet"))',
                "\"TXN_DESC\" = 'Internet'",
            ),
            (
                '(column(ALIAS.TXN_DESC) = string("Internet"))',
                '"ALIAS"."TXN_DESC" = \'Internet\'',
            ),
            (
                'column(TXN_DESC) = string("column(TXN_DESC)")',
                "\"TXN_DESC\" = 'column(TXN_DESC)'",
            ),
            (
                'column(TXN_DESC) NOT IN (string("A"),string("B"),string("C"))',
                "\"TXN_DESC\" NOT IN ('A', 'B', 'C')",
            ),
            (
                '(column(TXN_DESC) = string("Internet"))',
                "\"TXN_DESC\" = 'Internet'",
            ),
        ]

    def expected_sales_offload_predicates(self):
        # TODO I don't yet know what literal inputs we need to support for predicate offload
        return [
            ("column(PROD_ID) = numeric(171)", '"PROD_ID" = 171', None, None),
            (
                "column(TIME_ID) = datetime(2011-01-01 05:06:07)",
                "\"TIME_ID\" = TIMESTAMP '2011-01-01 05:06:07'",
                None,
                None,
            ),
            (
                "column(TIME_ID) = datetime(2011-01-01)",
                "\"TIME_ID\" = DATE '2011-01-01')",
                None,
                None,
            ),
            (
                "column(TIME_ID) < literal(\"DATE ' 2012-01-01'\")",
                "\"TIME_ID\" < DATE ' 2012-01-01'",
                None,
                None,
            ),
            (
                "column(TIME_ID) < literal(\"TIMESTAMP ' 2012-01-01 12:13:14'\")",
                "\"TIME_ID\" < TIMESTAMP ' 2012-01-01 12:13:14'",
                None,
                None,
            ),
        ]

    def gen_ctas_from_subquery(
        self,
        schema: str,
        table_name: str,
        subquery: str,
        pk_col_name: Optional[str] = None,
        table_parallelism: Optional[str] = None,
        with_drop: bool = True,
        with_stats_collection: bool = False,
    ) -> list:
        """Return a list[str] of SQL required to CTAS a table from a subquery."""
        params = {
            "schema": schema,
            "table_name": table_name,
            "subquery": subquery,
            "pk_col_name": pk_col_name,
        }
        sqls = []
        if with_drop:
            sqls.append("DROP TABLE %(schema)s.%(table_name)s" % params)
        ctas = (
            dedent(
                """\
            CREATE TABLE %(schema)s.%(table_name)s
            AS (
            %(subquery)s
            ) WITH DATA
        """
            )
            % params
        )
        if pk_col_name:
            ctas += f" UNIQUE PRIMARY INDEX ({pk_col_name})"
        sqls.append(ctas)
        if pk_col_name:
            sqls.append(
                "ALTER TABLE %(schema)s.%(table_name)s ADD %(pk_col_name)s NOT NULL"
                % params
            )
            sqls.append(
                "ALTER TABLE %(schema)s.%(table_name)s ADD PRIMARY KEY (%(pk_col_name)s)"
                % params
            )
        if with_stats_collection:
            sqls.append(self.collect_table_stats_sql_text(schema, table_name))
        return sqls

    def get_test_table_owner(self, expected_schema: str, table_name: str) -> str:
        sql = f"""SELECT DatabaseName
              FROM   DBC.TablesV
              WHERE  UPPER(TableName) = UPPER('{table_name}')
              ORDER BY CASE WHEN UPPER(DatabaseName) = UPPER('{expected_schema}') THEN '0'
                            WHEN UPPER(DatabaseName) = 'SH' THEN '1'
                            ELSE DatabaseName END"""
        row = self.execute_query_fetch_one(sql)
        return row[0] if row else None

    def goe_type_mapping_generated_table_col_specs(
        self,
        max_backend_precision,
        max_backend_scale,
        max_decimal_integral_magnitude,
        supported_canonical_types,
        ascii_only=False,
    ):
        """This is not required for non-Oracle builds"""
        raise NotImplementedError(
            "Teradata goe_type_mapping_generated_table_col_specs() not yet implemented"
        )

    def remove_table_stats_sql_text(self, schema, table_name) -> str:
        return f"DROP STATISTICS ON {schema}.{table_name}"

    def run_sql_in_file(self, local_path):
        self._log(f"Running {local_path}")
        file_contents = open(local_path).read()
        # This is a bit primitive and assumes there are no semi colons within strings
        ddls = [
            _.replace("\n", " ").strip() for _ in file_contents.split(";") if _.strip()
        ]
        for ddl in ddls:
            self._db_api.execute_ddl(ddl)
        # No trailing commit because we have enabled autocommit on Teradata

    def run_tpt_file(self, local_path, schema):
        self._log(f"Running {local_path}")
        tpt_job_variables = [
            "SourceSchema='%s'" % schema,
            "Tdpid='%s'" % os.environ.get("TERADATA_SERVER", "localhost"),
            "UserName='%s'" % os.environ.get("DBC_USERNAME", "dbc"),
            "UserPassword='%s'" % os.environ.get("DBC_PASSWORD", "dbc"),
            "SourceDirectoryPath='/tmp'",
        ]
        cmd = [
            "tbuild",
            "-f",
            local_path,
            "-u",
            ",".join([j for j in tpt_job_variables]),
        ]
        self._log(f"cmd: {cmd}", detail=VVERBOSE)
        run_os_cmd(cmd, self._messages, self._connection_options)

    def sales_based_fact_create_ddl(
        self,
        schema: str,
        table_name: str,
        maxval_partition: bool = False,
        extra_pred: Optional[str] = None,
        degree: Optional[str] = None,
        subpartitions: int = 0,
        enable_row_movement: bool = False,
        noseg_partition: bool = True,
        part_key_type: Optional[str] = None,
        time_id_column_name: Optional[str] = None,
        extra_col_tuples: Optional[list] = None,
        simple_partition_names: bool = False,
        with_drop: bool = True,
        range_start_literal_override=None,
    ) -> list:
        if extra_col_tuples:
            assert isinstance(extra_col_tuples, list)
            assert isinstance(extra_col_tuples[0], tuple)

        if not part_key_type:
            part_key_type = TERADATA_TYPE_DATE
        assert part_key_type in [
            TERADATA_TYPE_NUMBER,
            TERADATA_TYPE_BIGINT,
            TERADATA_TYPE_VARCHAR,
            TERADATA_TYPE_DATE,
            TERADATA_TYPE_TIMESTAMP,
        ], (
            "Unsupported part_key_type: %s" % part_key_type
        )

        extra_pred = extra_pred or ""
        subpartition_clause = ""
        extra_cols = ""
        if extra_col_tuples:
            extra_cols = "," + ",".join(
                "{} AS {}".format(_[0], _[1]) for _ in extra_col_tuples
            )
        time_id_alias = time_id_column_name or "time_id".upper()

        sql_params = {
            "schema": schema,
            "table": table_name,
            "extra_pred": extra_pred,
            "pre_hv": SALES_BASED_FACT_PRE_HV,
            "hv6": SALES_BASED_FACT_HV_6_END,
            "time_id_alias": time_id_alias,
            "extra_cols": extra_cols,
            "step": "INTERVAL '1' MONTH",
            "maxval_clause": (",NO RANGE" if maxval_partition else ""),
        }

        if part_key_type in (TERADATA_TYPE_NUMBER, TERADATA_TYPE_BIGINT):
            part_literal_template1 = part_literal_template2 = """%s"""
            part_literal_hv1 = (
                SALES_BASED_FACT_PRE_LOWER_NUM
                if noseg_partition
                else SALES_BASED_FACT_PRE_HV_NUM
            )
            part_literal_hv2 = (
                SALES_BASED_FACT_HV_5_END_NUM
                if maxval_partition
                else SALES_BASED_FACT_HV_6_END_NUM
            )
            sql_params.update(
                {
                    "time_id_expr": "CAST(TO_CHAR(time_id,'YYYYMMDD') AS INTEGER)",
                    "step": "1",
                }
            )
        elif part_key_type == TERADATA_TYPE_DATE:
            part_literal_template1 = part_literal_template2 = """DATE '%s'"""
            part_literal_hv1 = (
                SALES_BASED_FACT_PRE_LOWER
                if noseg_partition
                else SALES_BASED_FACT_PRE_HV
            )
            part_literal_hv2 = (
                SALES_BASED_FACT_HV_5_END
                if maxval_partition
                else SALES_BASED_FACT_HV_6_END
            )
            sql_params.update(
                {
                    "time_id_expr": "time_id",
                }
            )
        elif part_key_type == TERADATA_TYPE_TIMESTAMP:
            part_literal_template1 = """TIMESTAMP '%s 00:00:00+00:00'"""
            part_literal_template2 = """TIMESTAMP '%s 23:59:59+00:00'"""
            part_literal_hv1 = (
                SALES_BASED_FACT_PRE_LOWER
                if noseg_partition
                else SALES_BASED_FACT_PRE_HV
            )
            part_literal_hv2 = (
                SALES_BASED_FACT_HV_5_END
                if maxval_partition
                else SALES_BASED_FACT_HV_6_END
            )
            sql_params.update(
                {
                    "time_id_expr": "CAST(time_id AS TIMESTAMP(0))",
                }
            )
        else:
            # VC
            part_literal_template1 = part_literal_template2 = """'%s'"""
            part_literal_hv1 = (
                SALES_BASED_FACT_PRE_LOWER_NUM
                if noseg_partition
                else SALES_BASED_FACT_PRE_HV_NUM
            )
            part_literal_hv2 = (
                SALES_BASED_FACT_HV_5_END_NUM
                if maxval_partition
                else SALES_BASED_FACT_HV_6_END_NUM
            )
            sql_params.update({"time_id_expr": "TO_CHAR(time_id,'YYYYMMDD')"})

        part_literal_hv1 = range_start_literal_override or part_literal_hv1

        sql_params.update(
            {
                "start_literal": part_literal_template1 % part_literal_hv1,
                "end_literal": part_literal_template2 % part_literal_hv2,
            }
        )

        raise Exception(
            "This function has not been implemented, we need a Teradata row generator"
        )
        # TODO Maybe this will help: https://stackoverflow.com/questions/67131817/create-row-level-data-from-a-range-in-teradata
        create_ddl = (
            dedent(
                """\
        CREATE TABLE %(schema)s.%(table)s
        AS (
            SELECT prod_id, cust_id, %(time_id_expr)s AS %(time_id_alias)s, channel_id, promo_id
            , quantity_sold, amount_sold%(extra_cols)s
            FROM (
                    SELECT CAST(MOD(ROWNUM,100)+1 AS NUMBER(4))  prod_id
                    ,      CAST(MOD(ROWNUM,1000)+1 AS NUMBER(5)) cust_id
                    ,      ADD_MONTHS(DATE'%(hv6_date)s',-(MOD(ROWNUM,7))) - MOD(ROWNUM,31) time_id
                    ,      CAST(MOD(ROWNUM,5)+1 AS NUMBER(2))    channel_id
                    ,      CAST(MOD(ROWNUM,100)+1 AS NUMBER)     promo_id
                    ,      CAST(MOD(ROWNUM,5)+1 AS NUMBER(10,2)) quantity_sold
                    ,      CAST(ROWNUM*1.75 AS NUMBER(10,2))     amount_sold
                    FROM   (... we ned a 500 row generator here ...)
               ) sales
            WHERE time_id BETWEEN TO_DATE('%(pre_hv)s','YYYY-MM-DD') AND TO_DATE('%(hv6)s','YYYY-MM-DD')
            %(extra_pred)s
        )
        WITH DATA
        PRIMARY INDEX (cust_id)
        PARTITION BY (
            RANGE_N(%(time_id_alias)s BETWEEN %(start_literal)s AND %(end_literal)s EACH %(step)s%(maxval_clause)s)
        )
        """
            )
            % sql_params
        )

        sqls = []
        if with_drop:
            sqls.append("DROP TABLE %(schema)s.%(table)s" % sql_params)
        sqls.append(create_ddl)
        sqls.append(self.collect_table_stats_sql_text(schema, table_name))
        return sqls

    def sales_based_fact_hwm_literal(self, sales_literal: str, data_type: str) -> tuple:
        hwm_literal = sales_literal
        if data_type == TERADATA_TYPE_DATE:
            meta_hwm_check_literal = f"DATE '{hwm_literal}'"
        elif data_type == TERADATA_TYPE_TIMESTAMP:
            if hwm_literal and ":" not in hwm_literal:
                hwm_literal += " 00:00:00"
            meta_hwm_check_literal = f"TIMESTAMP '{hwm_literal}"
        elif data_type == TERADATA_TYPE_VARCHAR:
            meta_hwm_check_literal = f"'{hwm_literal}'"
        else:
            meta_hwm_check_literal = hwm_literal
        sql_literal = meta_hwm_check_literal
        return hwm_literal, meta_hwm_check_literal, sql_literal

    def sales_based_fact_add_partition_ddl(self, schema: str, table_name: str) -> list:
        _, next_hv_start = self.get_max_range_partition_name_and_hv(schema, table_name)
        q = dedent(
            f"""\
            SELECT TO_CHAR(next_date,'YYYY-MM-DD')
            FROM (SELECT ADD_MONTHS({next_hv_start},1)-1 next_date) AS v"""
        )
        ymd = self.execute_query_fetch_one(q)
        range_end_hv = f"DATE '{ymd[0]}'"
        alt = f"""ALTER TABLE {schema}.{table_name} MODIFY
        ADD RANGE BETWEEN {next_hv_start} AND {range_end_hv} EACH INTERVAL '1' MONTH"""
        ins = f"""INSERT INTO {schema}.{table_name}
                 SELECT * FROM {schema}.sales
                 WHERE time_id BETWEEN {next_hv_start} AND {range_end_hv}"""
        return [alt, ins]

    def sales_based_fact_drop_partition_ddl(
        self,
        schema: str,
        table_name: str,
        hv_string_list: list,
        dropping_oldest: Optional[bool] = None,
    ) -> list:
        if not dropping_oldest:
            raise NotImplementedError("Drop partition is not valid on Teradata")
        partitions = self.frontend_table_partition_list(schema, table_name)
        if not partitions:
            raise FrontendTestingApiException(
                f"No partitions found: {schema}.{table_name}"
            )

        last_drop_partition = None
        # partitions are newest to oldest but we need them oldest to newest for this code.
        for p in reversed(partitions):
            if any(_ in p.high_values_csv for _ in hv_string_list):
                last_drop_partition = p
            else:
                # This is the first partition after the range we are dropping
                break
        if not last_drop_partition:
            raise FrontendTestingApiException(
                f"Cannot identify end of DROP range, list: {partitions}"
            )
        if "TIMESTAMP" in last_drop_partition.high_values_csv:
            # TODO This is a workaround for Teradata MVP. SALES TIME_IDs have no time part and scale=0.
            #      Teradata is being picky about a trailing .0 on RANGE_N literal so remove it here.
            hv = last_drop_partition.high_values_csv
            if hv.endswith(".0'"):
                hv = hv[:-3] + "'"
            range_end_hv = f"{hv} - INTERVAL '1' SECOND"
        else:
            range_end_hv = last_drop_partition.high_values_csv + "-1"

        part_expr = teradata_get_primary_partition_expression(
            schema, table_name, self._db_api
        )
        range_start_hv = part_expr.ranges.pop()[0]
        sql = """ALTER TABLE {}.{} MODIFY
        DROP RANGE BETWEEN {} AND {} EACH INTERVAL '1' MONTH
        WITH DELETE""".format(
            schema, table_name, range_start_hv, range_end_hv
        )
        return [sql]

    def sales_based_fact_truncate_partition_ddl(
        self, schema: str, table_name: str, hv_string_list: Optional[list] = None
    ) -> list:
        # Truncate partition on Teradata is just a delete
        partition_names = [
            _.partition_name
            for _ in self.frontend_table_partition_list(
                schema, table_name, hv_string_list
            )
        ]
        sql = """DELETE {}.{} WHERE partition#l1 IN ({})""".format(
            schema, table_name, ",".join(partition_names)
        )
        return [sql]

    def sales_based_fact_late_arriving_data_sql(
        self,
        schema: str,
        table_name: str,
        time_id_literal: str,
        channel_id_literal: int = 1,
    ) -> list:
        ins = """INSERT INTO %(schema)s.%(table_name)s
        SELECT TOP 1
               prod_id, cust_id, DATE '%(time_id)s', channel_id,
               promo_id, quantity_sold, amount_sold
        FROM   %(schema)s.sales""" % {
            "schema": schema,
            "table_name": table_name,
            "time_id": time_id_literal,
        }
        return [ins]

    def sales_based_list_fact_create_ddl(
        self,
        schema: str,
        table_name: str,
        default_partition: bool = False,
        extra_pred: Optional[str] = None,
        part_key_type: Optional[str] = None,
        out_of_sequence: bool = False,
        include_older_partition: bool = False,
        yrmon_column_name: Optional[str] = None,
        extra_col_tuples: Optional[list] = None,
        with_drop: bool = True,
    ) -> list:
        if extra_col_tuples:
            assert isinstance(extra_col_tuples, list)
            assert isinstance(extra_col_tuples[0], tuple)

        if not part_key_type:
            part_key_type = TERADATA_TYPE_NUMBER
        assert part_key_type in [
            TERADATA_TYPE_NUMBER,
            TERADATA_TYPE_VARCHAR,
            TERADATA_TYPE_DATE,
            TERADATA_TYPE_TIMESTAMP,
        ], (
            "Unsupported part_key_type: %s" % part_key_type
        )

        extra_pred = extra_pred or ""
        yrmon = (yrmon_column_name or "yrmon").upper()
        extra_cols = ""
        if extra_col_tuples:
            extra_cols = "," + ",".join(
                "{} AS {}".format(_[0], _[1]) for _ in extra_col_tuples
            )

        params = {
            "schema": schema,
            "table": table_name,
            "extra_pred": extra_pred,
            "p0_partition": "",
            "qry_hv1": SALES_BASED_LIST_HV_1,
            "qry_hv2": SALES_BASED_LIST_HV_2,
            "qry_hv7": SALES_BASED_LIST_HV_7,
            "p0_literal": SALES_BASED_LIST_PRE_HV,
            "p1_literal": SALES_BASED_LIST_HV_1,
            "p2_literal": SALES_BASED_LIST_HV_2,
            "p3_literal": SALES_BASED_LIST_HV_3,
            "p4_literal": SALES_BASED_LIST_HV_4,
            "p5_literal": SALES_BASED_LIST_HV_5,
            "p6_literal": SALES_BASED_LIST_HV_6,
            "p7_literal": SALES_BASED_LIST_HV_7,
            "yrmon_alias": yrmon,
            "extra_cols": extra_cols,
            "chr": "'",
        }
        if out_of_sequence:
            # Swap some partitions around to confuse RANGE based logic
            (
                params["hv3"],
                params["hv5"],
            ) = (
                params["hv5"],
                params["hv3"],
            )

        if part_key_type == TERADATA_TYPE_NUMBER:
            params.update(
                {"chr": "", "yrmon_expr": "TO_NUMBER(TO_CHAR(time_id,'YYYYMM'))"}
            )
        elif part_key_type == TERADATA_TYPE_DATE:
            params.update(
                {
                    "yrmon_expr": "TRUNC(time_id,'MM')",
                    "p0_literal": f"DATE '{SALES_BASED_FACT_PRE_HV}'",
                    "p1_literal": f"DATE '{SALES_BASED_FACT_HV_1}'",
                    "p2_literal": f"DATE '{SALES_BASED_FACT_HV_2}'",
                    "p3_literal": f"DATE '{SALES_BASED_FACT_HV_3}'",
                    "p4_literal": f"DATE '{SALES_BASED_FACT_HV_4}'",
                    "p5_literal": f"DATE '{SALES_BASED_FACT_HV_5}'",
                    "p6_literal": f"DATE '{SALES_BASED_FACT_HV_6}'",
                    "p7_literal": f"DATE '{SALES_BASED_FACT_HV_7}'",
                }
            )
        elif part_key_type == TERADATA_TYPE_TIMESTAMP:
            params.update(
                {
                    "yrmon_expr": "CAST(TRUNC(time_id,'MM') AS TIMESTAMP(0))",
                    "p0_literal": f"TIMESTAMP '{SALES_BASED_FACT_PRE_HV} 00:00:00'",
                    "p1_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_1} 00:00:00'",
                    "p2_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_2} 00:00:00'",
                    "p3_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_3} 00:00:00'",
                    "p4_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_4} 00:00:00'",
                    "p5_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_5} 00:00:00'",
                    "p6_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_6} 00:00:00'",
                    "p7_literal": f"TIMESTAMP '{SALES_BASED_FACT_HV_7} 00:00:00'",
                }
            )
        else:
            params.update({"yrmon_expr": "TO_CHAR(time_id,'YYYYMM')"})
        if default_partition:
            params["p7_partition"] = "NO CASE OR UNKNOWN"
        else:
            params["p7_partition"] = "%(yrmon_alias)s=%(p7_literal)s" % params
        if include_older_partition:
            # In order to mimic RANGE for LIST_AS_RANGE we need an older partition
            params["p0_partition"] = "%(yrmon_alias)s=%(p0_literal)s,\n    " % params

        create_ddl = (
            dedent(
                """\
        CREATE TABLE %(schema)s.%(table)s
        AS (
            SELECT sales.*, %(yrmon_expr)s AS %(yrmon_alias)s%(extra_cols)s
            FROM %(schema)s.sales
            WHERE time_id BETWEEN TO_DATE('%(qry_hv1)s','YYYYMM') AND TO_DATE('%(qry_hv7)s','YYYYMM')
            AND TO_CHAR(time_id, 'YYYYMM') <> '%(qry_hv2)s' %(extra_pred)s
        )
        WITH DATA
        PRIMARY INDEX (cust_id)
        PARTITION BY (
            CASE_N(
            %(p0_partition)s%(yrmon_alias)s=%(p1_literal)s,
            %(yrmon_alias)s=%(p2_literal)s,
            %(yrmon_alias)s=%(p3_literal)s,
            %(yrmon_alias)s=%(p4_literal)s,
            %(yrmon_alias)s=%(p5_literal)s,
            %(yrmon_alias)s=%(p6_literal)s,
            %(p7_partition)s
            )
        )
        """
            )
            % params
        )

        sqls = []
        if with_drop:
            sqls.append("DROP TABLE %(schema)s.%(table)s" % params)
        sqls.append(create_ddl % params)
        sqls.append(self.collect_table_stats_sql_text(schema, table_name))
        return sqls

    def sales_based_list_fact_add_partition_ddl(
        self, schema: str, table_name: str, next_ym_override: Optional[tuple] = None
    ) -> list:
        raise NotImplementedError(
            "Teradata sales_based_list_fact_add_partition_ddl() pending implementation"
        )

    def sales_based_list_fact_late_arriving_data_sql(
        self, schema: str, table_name: str, time_id_literal: str, yrmon_string: str
    ) -> list:
        ins = """INSERT INTO %(schema)s.%(table_name)s
        SELECT TOP 1
               prod_id, cust_id, DATE '%(time_id)s' AS time_id,
               channel_id, promo_id, quantity_sold, amount_sold,
               DATE '%(yrmon)s' AS yrmon
        FROM   %(schema)s.sales
        WHERE  time_id = DATE '%(yrmon)s'""" % {
            "schema": schema,
            "table_name": table_name,
            "time_id": time_id_literal,
            "yrmon": yrmon_string,
        }
        return [ins]

    def sales_based_multi_col_fact_create_ddl(
        self, schema: str, table_name: str, maxval_partition=False
    ) -> list:
        raise NotImplementedError(
            "Teradata sales_based_multi_col_fact_create_ddl() not implemented"
        )

    def sales_based_subpartitioned_fact_ddl(
        self, schema: str, table_name: str, top_level="LIST", rowdependencies=False
    ) -> list:
        raise NotImplementedError(
            "Teradata sales_based_subpartitioned_fact_ddl() not implemented"
        )

    def select_grant_exists(
        self,
        schema: str,
        table_name: str,
        to_user: str,
        grantable: Optional[bool] = None,
    ) -> bool:
        self._log(
            "select_grant_exists(%s, %s, %s, %s)"
            % (schema, table_name, to_user, grantable),
            detail=VERBOSE,
        )
        q = """SELECT GrantAuthority
        FROM   DBC.UserRightsV
        WHERE  DatabaseName = ?
        AND    TableName = ?
        AND    Grantee = ?
        AND    AccessRight = 'R'"""
        row = self._db_api.execute_query_fetch_one(q, [schema, table_name, to_user])
        if not row:
            return False
        elif grantable is None:
            return True
        if grantable:
            return bool(row[0] == "YES")
        else:
            return bool(row[0] == "NO")

    def standard_dimension_frontend_ddl(
        self,
        schema: str,
        table_name: str,
        extra_col_tuples: Optional[list] = None,
        empty: bool = False,
        pk_col_name: str = None,
    ) -> list:
        extra_cols = ""
        if extra_col_tuples:
            extra_cols = "," + ",".join(
                "{} AS {}".format(_[0], _[1]) for _ in extra_col_tuples
            )
        if empty:
            subquery = dedent(
                f"""\
                SELECT CAST(1 AS NUMBER(15))          AS id
                ,      CAST(2 AS NUMBER(4))           AS prod_id
                ,      CAST(20120931 AS NUMBER(8))    AS txn_day
                ,      DATE'2012-10-31'               AS txn_date
                ,      CAST(TIMESTAMP'2012-10-31 01:15:00' AS TIMESTAMP(3)) AS txn_time
                ,      CAST(17.5 AS NUMBER(10,2))     AS txn_rate
                ,      CAST('ABC' AS VARCHAR(50))     AS txn_desc
                ,      CAST('ABC' AS CHAR(3))         AS txn_code
                {extra_cols}
                WHERE 1 = 2
                """
            )
        else:
            subquery = dedent(
                f"""\
                SELECT CAST(1 AS NUMBER(15))          AS id
                ,      CAST(2 AS NUMBER(4))           AS prod_id
                ,      CAST(20120931 AS NUMBER(8))    AS txn_day
                ,      DATE'2012-10-31'               AS txn_date
                ,      CAST(TIMESTAMP'2012-10-31 01:15:00' AS TIMESTAMP(3)) AS txn_time
                ,      CAST(17.5 AS NUMBER(10,2))     AS txn_rate
                ,      CAST('ABC' AS VARCHAR(50))     AS txn_desc
                ,      CAST('ABC' AS CHAR(3))         AS txn_code
                {extra_cols}
                UNION ALL
                SELECT CAST(2 AS NUMBER(15))          AS id
                ,      CAST(3 AS NUMBER(4))           AS prod_id
                ,      CAST(20121031 AS NUMBER(8))    AS txn_day
                ,      DATE'2012-10-31'               AS txn_date
                ,      CAST(TIMESTAMP'2012-10-31 02:15:00' AS TIMESTAMP(3)) AS txn_time
                ,      CAST(20 AS NUMBER(10,2))       AS txn_rate
                ,      CAST('DEF' AS VARCHAR(50))     AS txn_desc
                ,      CAST('DEF' AS CHAR(3))         AS txn_code
                {extra_cols}
                UNION ALL
                SELECT CAST(3 AS NUMBER(15))          AS id
                ,      CAST(4 AS NUMBER(4))           AS prod_id
                ,      CAST(20121031 AS NUMBER(8))    AS txn_day
                ,      DATE'2012-10-31'               AS txn_date
                ,      CAST(TIMESTAMP'2012-10-31 03:15:00' AS TIMESTAMP(3)) AS txn_time
                ,      CAST(10.55 AS NUMBER(10,2))    AS txn_rate
                ,      CAST('GHI' AS VARCHAR(50))     AS txn_desc
                ,      CAST('GHI' AS CHAR(3))         AS txn_code
                {extra_cols}
                """
            )
        return self.gen_ctas_from_subquery(
            schema, table_name, subquery, pk_col_name=pk_col_name, with_stats_collection=True
        )

    def table_row_count_from_stats(
        self, schema: str, table_name: str
    ) -> Union[int, None]:
        raise NotImplementedError(
            "Teradata table_row_count_from_stats() not implemented"
        )

    def test_type_canonical_int_8(self) -> str:
        return TERADATA_TYPE_BIGINT

    def test_type_canonical_date(self) -> str:
        return TERADATA_TYPE_DATE

    def test_type_canonical_decimal(self) -> str:
        return TERADATA_TYPE_NUMBER

    def test_type_canonical_string(self) -> str:
        return TERADATA_TYPE_VARCHAR

    def test_type_canonical_timestamp(self) -> str:
        return TERADATA_TYPE_TIMESTAMP

    def test_time_zone_query_option(self, tz) -> dict:
        raise NotImplementedError(
            "Teradata test_time_zone_query_option() not implemented"
        )

    def unit_test_query_options(self):
        return None
