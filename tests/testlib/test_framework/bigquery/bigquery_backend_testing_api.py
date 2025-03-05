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

from goe.filesystem.goe_dfs import get_scheme_from_location_uri, OFFLOAD_FS_SCHEME_GS
from goe.offload.bigquery.bigquery_column import (
    BigQueryColumn,
    BIGQUERY_TYPE_INT64,
    BIGQUERY_TYPE_NUMERIC,
    BIGQUERY_TYPE_BIGNUMERIC,
    BIGQUERY_TYPE_STRING,
    BIGQUERY_TYPE_DATE,
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_TIME,
    BIGQUERY_TYPE_FLOAT64,
    BIGQUERY_TYPE_TIMESTAMP,
    BIGQUERY_TYPE_BYTES,
    BIGQUERY_TYPE_BOOLEAN,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    ColumnPartitionInfo,
    match_table_column,
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
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from tests.testlib.test_framework.test_constants import (
    PARTITION_FUNCTION_TEST_FROM_DEC1,
    PARTITION_FUNCTION_TEST_FROM_DEC2,
    PARTITION_FUNCTION_TEST_FROM_INT8,
    PARTITION_FUNCTION_TEST_FROM_STRING,
)
from tests.testlib.test_framework.backend_testing_api import (
    BackendTestingApiInterface,
    STORY_TEST_BACKEND_DOUBLE_COL,
    STORY_TEST_BACKEND_INT_8_COL,
    STORY_TEST_BACKEND_DECIMAL_DEF_COL,
    STORY_TEST_BACKEND_VAR_STR_COL,
    STORY_TEST_BACKEND_DATE_COL,
    STORY_TEST_BACKEND_DATETIME_COL,
    STORY_TEST_BACKEND_TIMESTAMP_COL,
    STORY_TEST_BACKEND_BLOB_COL,
    STORY_TEST_BACKEND_NULL_STR_COL,
    STORY_TEST_OFFLOAD_NUMS_BARE_NUM,
    STORY_TEST_OFFLOAD_NUMS_BARE_FLT,
    STORY_TEST_OFFLOAD_NUMS_NUM_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_18,
    STORY_TEST_OFFLOAD_NUMS_NUM_19,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_2,
    STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_5,
    STORY_TEST_OFFLOAD_NUMS_NUM_10_M5,
    STORY_TEST_OFFLOAD_NUMS_DEC_10_0,
    STORY_TEST_OFFLOAD_NUMS_DEC_13_9,
    STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
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
# BackendBigQueryTestingApi
###########################################################################


class BackendBigQueryTestingApi(BackendTestingApiInterface):
    """BigQuery methods"""

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
        super(BackendBigQueryTestingApi, self).__init__(
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
        arg_types = {
            PARTITION_FUNCTION_TEST_FROM_INT8: BIGQUERY_TYPE_INT64,
            PARTITION_FUNCTION_TEST_FROM_DEC1: BIGQUERY_TYPE_BIGNUMERIC,
            PARTITION_FUNCTION_TEST_FROM_DEC2: BIGQUERY_TYPE_NUMERIC,
            PARTITION_FUNCTION_TEST_FROM_STRING: BIGQUERY_TYPE_STRING,
        }
        udf_expr = {
            PARTITION_FUNCTION_TEST_FROM_INT8: "CAST(TRUNC(p_arg,-1) AS INT64)",
            PARTITION_FUNCTION_TEST_FROM_DEC1: "CAST(TRUNC(p_arg,-1) AS INT64)",
            PARTITION_FUNCTION_TEST_FROM_DEC2: "CAST(TRUNC(p_arg,-1) AS INT64)",
            PARTITION_FUNCTION_TEST_FROM_STRING: "CAST(TRUNC(CAST(p_arg AS INT64),-1) AS INT64)",
        }
        return BIGQUERY_TYPE_INT64, arg_types[udf_name], udf_expr[udf_name]

    def _goe_type_mapping_column_definitions(self, filter_column=None):
        """Returns a dict of dicts defining columns for GOE_BACKEND_TYPE_MAPPING test table.
        filter_column can be used to fetch just a single column dict.
        """

        def name(*args):
            return self._goe_type_mapping_column_name(*args)

        all_columns = {
            name(BIGQUERY_TYPE_BIGNUMERIC): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_BIGNUMERIC), BIGQUERY_TYPE_BIGNUMERIC
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_BIGNUMERIC), GOE_TYPE_DECIMAL
                ),
            },
            name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_INTEGER_8): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_INTEGER_8),
                    BIGQUERY_TYPE_BIGNUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_INTEGER_8),
                    GOE_TYPE_INTEGER_8,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_INTEGER_8
                    )
                },
            },
            name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_DECIMAL, "38", "18"): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_DECIMAL, "38", "18"),
                    BIGQUERY_TYPE_BIGNUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_DECIMAL, "38", "18"),
                    GOE_TYPE_DECIMAL,
                ),
                "present_options": {
                    "decimal_columns_csv_list": [
                        name(BIGQUERY_TYPE_BIGNUMERIC, GOE_TYPE_DECIMAL, "38", "18")
                    ],
                    "decimal_columns_type_list": ["38,18"],
                },
            },
            name(BIGQUERY_TYPE_BYTES): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_BYTES), BIGQUERY_TYPE_BYTES
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_BYTES), GOE_TYPE_BINARY
                ),
            },
            name(BIGQUERY_TYPE_BYTES, GOE_TYPE_LARGE_BINARY): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_BYTES, GOE_TYPE_LARGE_BINARY),
                    BIGQUERY_TYPE_BYTES,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_BYTES, GOE_TYPE_LARGE_BINARY),
                    GOE_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        BIGQUERY_TYPE_BYTES, GOE_TYPE_LARGE_BINARY
                    )
                },
            },
            name(BIGQUERY_TYPE_DATE): {
                "column": BigQueryColumn(name(BIGQUERY_TYPE_DATE), BIGQUERY_TYPE_DATE),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_DATE), GOE_TYPE_DATE
                ),
            },
            name(BIGQUERY_TYPE_DATE, GOE_TYPE_TIMESTAMP): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_DATE, GOE_TYPE_TIMESTAMP), BIGQUERY_TYPE_DATE
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_DATE, GOE_TYPE_TIMESTAMP),
                    GOE_TYPE_TIMESTAMP,
                ),
                "present_options": {
                    "timestamp_columns_csv": name(
                        BIGQUERY_TYPE_DATE, GOE_TYPE_TIMESTAMP
                    )
                },
            },
            name(BIGQUERY_TYPE_DATETIME): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_DATETIME), BIGQUERY_TYPE_DATETIME
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_DATETIME), GOE_TYPE_TIMESTAMP
                ),
            },
            name(BIGQUERY_TYPE_DATETIME, GOE_TYPE_DATE): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_DATETIME, GOE_TYPE_DATE),
                    BIGQUERY_TYPE_DATETIME,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_DATETIME, GOE_TYPE_DATE), GOE_TYPE_DATE
                ),
                "present_options": {
                    "date_columns_csv": name(BIGQUERY_TYPE_DATETIME, GOE_TYPE_DATE)
                },
            },
            name(BIGQUERY_TYPE_FLOAT64): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_FLOAT64), BIGQUERY_TYPE_FLOAT64
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_FLOAT64), GOE_TYPE_DOUBLE
                ),
            },
            name(BIGQUERY_TYPE_FLOAT64, GOE_TYPE_DECIMAL, "38", "9"): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_FLOAT64, GOE_TYPE_DECIMAL, "38", "9"),
                    BIGQUERY_TYPE_FLOAT64,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_FLOAT64, GOE_TYPE_DECIMAL, "38", "9"),
                    GOE_TYPE_DECIMAL,
                ),
                "present_options": {
                    "decimal_columns_csv_list": [
                        name(BIGQUERY_TYPE_FLOAT64, GOE_TYPE_DECIMAL, "38", "9")
                    ],
                    "decimal_columns_type_list": ["38,9"],
                },
            },
            name(BIGQUERY_TYPE_INT64): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_INT64), BIGQUERY_TYPE_INT64
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_INT64), GOE_TYPE_INTEGER_8
                ),
            },
            name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_1): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_1),
                    BIGQUERY_TYPE_NUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_1),
                    GOE_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_1_columns_csv": name(
                        BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_1
                    )
                },
            },
            name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_2): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_2),
                    BIGQUERY_TYPE_NUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_2),
                    GOE_TYPE_INTEGER_2,
                ),
                "present_options": {
                    "integer_2_columns_csv": name(
                        BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_2
                    )
                },
            },
            name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_4): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_4),
                    BIGQUERY_TYPE_NUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_4),
                    GOE_TYPE_INTEGER_4,
                ),
                "present_options": {
                    "integer_4_columns_csv": name(
                        BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_4
                    )
                },
            },
            name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_8): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_8),
                    BIGQUERY_TYPE_NUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_8),
                    GOE_TYPE_INTEGER_8,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_8
                    )
                },
            },
            name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_38): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_38),
                    BIGQUERY_TYPE_NUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_38),
                    GOE_TYPE_INTEGER_38,
                ),
                "present_options": {
                    "integer_38_columns_csv": name(
                        BIGQUERY_TYPE_NUMERIC, GOE_TYPE_INTEGER_38
                    )
                },
            },
            name(BIGQUERY_TYPE_NUMERIC): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC), BIGQUERY_TYPE_NUMERIC
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC), GOE_TYPE_DECIMAL
                ),
            },
            name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_DECIMAL, "38", "18"): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_DECIMAL, "38", "18"),
                    BIGQUERY_TYPE_NUMERIC,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_DECIMAL, "38", "18"),
                    GOE_TYPE_DECIMAL,
                ),
                "present_options": {
                    "decimal_columns_csv_list": [
                        name(BIGQUERY_TYPE_NUMERIC, GOE_TYPE_DECIMAL, "38", "18")
                    ],
                    "decimal_columns_type_list": ["38,18"],
                },
            },
            name(BIGQUERY_TYPE_STRING): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING), BIGQUERY_TYPE_STRING
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING),
                    GOE_TYPE_VARIABLE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
                ),
            },
            name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_STRING): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_STRING),
                    BIGQUERY_TYPE_STRING,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_STRING),
                    GOE_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_STRING
                    )
                },
            },
            name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN): {
                "column": BigQueryColumn(
                    name(
                        BIGQUERY_TYPE_STRING,
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    BIGQUERY_TYPE_STRING,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        BIGQUERY_TYPE_STRING,
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    GOE_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        BIGQUERY_TYPE_STRING,
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    "unicode_string_columns_csv": name(
                        BIGQUERY_TYPE_STRING,
                        GOE_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                },
            },
            name(BIGQUERY_TYPE_STRING, GOE_TYPE_BINARY): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_BINARY), BIGQUERY_TYPE_STRING
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_BINARY), GOE_TYPE_BINARY
                ),
                "present_options": {
                    "binary_columns_csv": name(BIGQUERY_TYPE_STRING, GOE_TYPE_BINARY)
                },
            },
            name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_BINARY): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_BINARY),
                    BIGQUERY_TYPE_STRING,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_BINARY),
                    GOE_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        BIGQUERY_TYPE_STRING, GOE_TYPE_LARGE_BINARY
                    )
                },
            },
            name(BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_DS): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_DS),
                    BIGQUERY_TYPE_STRING,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_DS),
                    GOE_TYPE_INTERVAL_DS,
                ),
                "present_options": {
                    "interval_ds_columns_csv": name(
                        BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_DS
                    )
                },
            },
            name(BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_YM): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_YM),
                    BIGQUERY_TYPE_STRING,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_YM),
                    GOE_TYPE_INTERVAL_YM,
                ),
                "present_options": {
                    "interval_ym_columns_csv": name(
                        BIGQUERY_TYPE_STRING, GOE_TYPE_INTERVAL_YM
                    )
                },
            },
            name(BIGQUERY_TYPE_STRING, UNICODE_NAME_TOKEN): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_STRING, UNICODE_NAME_TOKEN), BIGQUERY_TYPE_STRING
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_STRING, UNICODE_NAME_TOKEN),
                    GOE_TYPE_VARIABLE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        BIGQUERY_TYPE_STRING, UNICODE_NAME_TOKEN
                    )
                },
            },
            name(BIGQUERY_TYPE_TIME): {
                "column": BigQueryColumn(name(BIGQUERY_TYPE_TIME), BIGQUERY_TYPE_TIME),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_TIME), GOE_TYPE_TIME
                ),
            },
            name(BIGQUERY_TYPE_TIMESTAMP): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_TIMESTAMP), BIGQUERY_TYPE_TIMESTAMP
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_TIMESTAMP), GOE_TYPE_TIMESTAMP_TZ
                ),
            },
            name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_DATE): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_DATE),
                    BIGQUERY_TYPE_TIMESTAMP,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_DATE), GOE_TYPE_DATE
                ),
                "present_options": {
                    "date_columns_csv": name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_DATE)
                },
            },
            name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP): {
                "column": BigQueryColumn(
                    name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP),
                    BIGQUERY_TYPE_TIMESTAMP,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP),
                    GOE_TYPE_TIMESTAMP,
                ),
                "present_options": {
                    "timestamp_columns_csv": name(
                        BIGQUERY_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP
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
        return BIGQUERY_TYPE_DATE

    def backend_test_type_canonical_decimal(self):
        return BIGQUERY_TYPE_NUMERIC

    def backend_test_type_canonical_int_2(self):
        return BIGQUERY_TYPE_INT64

    def backend_test_type_canonical_int_4(self):
        return BIGQUERY_TYPE_INT64

    def backend_test_type_canonical_int_8(self):
        return BIGQUERY_TYPE_INT64

    def backend_test_type_canonical_int_38(self):
        return BIGQUERY_TYPE_BIGNUMERIC

    def backend_test_type_canonical_time(self):
        return BIGQUERY_TYPE_TIME

    def backend_test_type_canonical_timestamp(self):
        return BIGQUERY_TYPE_DATETIME

    def backend_test_type_canonical_timestamp_tz(self):
        return BIGQUERY_TYPE_TIMESTAMP

    def create_backend_offload_location(self, goe_user=None):
        """Unsupported for BigQuery"""
        raise NotImplementedError(
            "create_backend_offload_location() unsupported for BigQuery"
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
        """Create partitioned BigQuery test table.
        compute_stats is ignored on BigQuery.
        """
        create_cols = self._db_api.get_columns(db_name, source_table_name)
        insert_col_tuples = [(_.name, _.name) for _ in create_cols]
        partition_info = ColumnPartitionInfo(
            position=0,
            source_column_name="YEARMON",
            granularity=100,
            range_start=0,
            range_end=999999,
        )
        create_cols.append(
            self.gen_column_object(
                "YEARMON", data_type=BIGQUERY_TYPE_INT64, partition_info=partition_info
            )
        )
        partition_column_names = ["YEARMON"]
        partition_source_column = (
            self._find_source_column_for_create_partitioned_test_table(create_cols)
        )
        extract_fn = "FORMAT_%s" % partition_source_column.data_type.upper()
        insert_col_tuples.append(
            (
                "CAST(%(fn)s('%%E4Y%%m', %(source_name)s) AS %(cast_type)s)"
                % {
                    "fn": extract_fn,
                    "source_name": partition_source_column.name,
                    "cast_type": BIGQUERY_TYPE_INT64,
                },
                "YEARMON",
            )
        )

        cmds = self._db_api.create_table(
            db_name, table_name, create_cols, partition_column_names
        )
        cmds.extend(
            self.insert_table_as_select(
                db_name,
                table_name,
                db_name,
                source_table_name,
                insert_col_tuples,
                None,
                filter_clauses=filter_clauses,
            )
        )
        return cmds

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
        """BigQuery override that doesn't compute stats"""
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
        """BigQuery doesn't have drop column so we need to recreate the table without the column.
        We utilise the "table rewrite" feature to replace the table:
            https://medium.com/google-cloud/bigquery-drop-or-change-column-43409ae12bc4
            CREATE OR REPLACE TABLE `transactions.test_table` AS
            SELECT * EXCEPT (c)
            FROM `transactions.test_table`;
        This doesn't support partitioned tables (yet), because this is for testing only we've not
        added support for anything beyond the minimum required.
        """
        assert db_name and table_name and column_name
        existing_columns = self.get_columns(db_name, table_name)
        assert isinstance(column_name, str)
        assert match_table_column(column_name, existing_columns), (
            "Column %s is not in the table" % column_name
        )

        orig_ddl = self._db_api.get_table_ddl(
            db_name, table_name, as_list=True, for_replace=True
        )
        self._log("drop_column original DDL: %s" % str(orig_ddl), detail=VVERBOSE)
        # Remove the column from the DDL and stitch it together as a string
        new_ddl = "\n".join(
            _
            for _ in orig_ddl
            if self.enclose_identifier(column_name).lower() not in _.lower()
        )
        new_ddl += """\nAS
SELECT * EXCEPT (%(column_name)s)
FROM %(db_table)s""" % {
            "db_table": self._db_api.enclose_object_reference(db_name, table_name),
            "column_name": column_name,
        }
        return self.execute_ddl(new_ddl)

    def expected_backend_column(
        self, canonical_column, override_used=None, decimal_padding_digits=None
    ):
        def is_bigquery_numeric(column):
            return bool(
                (
                    column.data_type == GOE_TYPE_DECIMAL
                    and column.data_precision
                    and column.data_precision <= 38
                    and column.data_scale
                    and column.data_scale <= 9
                )
                or (
                    column.data_type == GOE_TYPE_INTEGER_38
                    and column.data_precision
                    and column.data_precision <= 29
                )
            )

        expected_data_type = self.expected_canonical_to_backend_type_map(
            override_used=override_used
        ).get(canonical_column.data_type)

        if is_bigquery_numeric(canonical_column):
            expected_data_type = BIGQUERY_TYPE_NUMERIC

        return BigQueryColumn(canonical_column.name, expected_data_type)

    def expected_backend_precision_scale(
        self, canonical_column, decimal_padding_digits=None
    ):
        """All decimals (NUMERIC) on BigQuery have no controllable precision or scale"""
        return None

    def expected_canonical_to_backend_type_map(self, override_used=None):
        # We have frontend CASTs which change some columns to be smaller than limit of NUMERIC
        numeric_override = (
            BIGQUERY_TYPE_NUMERIC
            if "decimal_columns_csv_list" in (override_used or {})
            else None
        )
        return {
            GOE_TYPE_FIXED_STRING: BIGQUERY_TYPE_STRING,
            GOE_TYPE_LARGE_STRING: BIGQUERY_TYPE_STRING,
            GOE_TYPE_VARIABLE_STRING: BIGQUERY_TYPE_STRING,
            GOE_TYPE_BINARY: BIGQUERY_TYPE_BYTES,
            GOE_TYPE_LARGE_BINARY: BIGQUERY_TYPE_BYTES,
            GOE_TYPE_INTEGER_1: BIGQUERY_TYPE_INT64,
            GOE_TYPE_INTEGER_2: BIGQUERY_TYPE_INT64,
            GOE_TYPE_INTEGER_4: BIGQUERY_TYPE_INT64,
            GOE_TYPE_INTEGER_8: BIGQUERY_TYPE_INT64,
            GOE_TYPE_INTEGER_38: BIGQUERY_TYPE_BIGNUMERIC,
            GOE_TYPE_DECIMAL: numeric_override or BIGQUERY_TYPE_BIGNUMERIC,
            GOE_TYPE_DOUBLE: BIGQUERY_TYPE_FLOAT64,
            GOE_TYPE_DATE: BIGQUERY_TYPE_DATE,
            GOE_TYPE_TIME: BIGQUERY_TYPE_TIME,
            GOE_TYPE_TIMESTAMP: BIGQUERY_TYPE_DATETIME,
            GOE_TYPE_TIMESTAMP_TZ: BIGQUERY_TYPE_TIMESTAMP,
            GOE_TYPE_INTERVAL_DS: BIGQUERY_TYPE_STRING,
            GOE_TYPE_INTERVAL_YM: BIGQUERY_TYPE_STRING,
            GOE_TYPE_BOOLEAN: BIGQUERY_TYPE_BOOLEAN,
        }

    def expected_std_dim_offload_predicates(self) -> list:
        return [
            ("column(id) IS NULL", "`ID` IS NULL"),
            ("column(id) IS NOT NULL", "`ID` IS NOT NULL"),
            ("column(id) > numeric(4)", "`ID` > 4"),
            (
                "(column(ID) = numeric(10)) AND (column(ID) < numeric(2.2))",
                "(`ID` = 10 AND `ID` < 2.2)",
            ),
            (
                "(column(ID) = numeric(10)) AND (column(ID) IS NULL)",
                "(`ID` = 10 AND `ID` IS NULL)",
            ),
            ('column(TXN_DESC) = string("Oxford")', "`TXN_DESC` = 'Oxford'"),
            (
                "column(TXN_TIME) = datetime(1970-01-01)",
                "`TXN_TIME` = DATETIME '1970-01-01'",
            ),
            (
                "column(TXN_TIME) = datetime(1970-01-01 12:13:14)",
                "`TXN_TIME` = DATETIME '1970-01-01 12:13:14'",
            ),
        ]

    def expected_std_dim_synthetic_offload_predicates(self) -> list:
        date_column = "TXN_DATE"
        return [
            (
                (date_column, "D", None),
                [
                    (
                        "column(TXN_DATE) = datetime(2010-01-01)",
                        "`TXN_DATE` = DATETIME '2010-01-01'",
                    ),
                    (
                        "datetime(2010-01-01) = column(TXN_DATE)",
                        "DATETIME '2010-01-01' = `TXN_DATE`",
                    ),
                    (
                        "column(TXN_DATE) != datetime(2010-01-01)",
                        "`TXN_DATE` != DATETIME '2010-01-01'",
                    ),
                    (
                        "column(TXN_DATE) < datetime(2010-01-01)",
                        "`TXN_DATE` < DATETIME '2010-01-01'",
                    ),
                    (
                        "column(TXN_DATE) <= datetime(2010-01-01)",
                        "`TXN_DATE` <= DATETIME '2010-01-01'",
                    ),
                    (
                        "column(TXN_DATE) > datetime(2010-01-01)",
                        "`TXN_DATE` > DATETIME '2010-01-01'",
                    ),
                    (
                        "column(TXN_DATE) >= datetime(2010-01-01)",
                        "`TXN_DATE` >= DATETIME '2010-01-01'",
                    ),
                ],
            ),
        ]

    def goe_type_mapping_generated_table_col_specs(self):
        definitions = self._goe_type_mapping_column_definitions()
        goe_type_mapping_cols, goe_type_mapping_names = [], []
        for col_dict in [
            definitions[col_name] for col_name in sorted(definitions.keys())
        ]:
            backend_column = col_dict["column"]
            goe_type_mapping_names.append(backend_column.name)
            if backend_column.data_type in [
                BIGQUERY_TYPE_BIGNUMERIC,
                BIGQUERY_TYPE_NUMERIC,
            ] and col_dict.get("present_options"):
                # This is a number of some kind and being CAST to something else so we provide specific test data.
                present_options = col_dict["present_options"]
                literals = [1, 2, 3]
                if present_options.get("decimal_columns_type_list"):
                    precision, scale = present_options["decimal_columns_type_list"][
                        0
                    ].split(",")
                    precision = int(precision)
                    scale = int(scale)
                    if backend_column.data_type == BIGQUERY_TYPE_NUMERIC:
                        if scale < 9:
                            precision = min(precision, 38 - (9 - scale))
                        elif scale > 9:
                            precision = precision - (scale - 9)
                            scale = 9
                    literals = [
                        TestDecimal.min(precision, scale),
                        TestDecimal.rnd(precision, scale),
                        TestDecimal.max(precision, scale),
                    ]
                elif col_dict["expected_canonical_column"].data_type in [
                    GOE_TYPE_INTEGER_1,
                    GOE_TYPE_INTEGER_2,
                    GOE_TYPE_INTEGER_4,
                    GOE_TYPE_INTEGER_8,
                    GOE_TYPE_INTEGER_38,
                ]:
                    precision = self._canonical_integer_precision(
                        col_dict["expected_canonical_column"].data_type
                    )
                    if backend_column.data_type == BIGQUERY_TYPE_NUMERIC:
                        precision = min(precision, 29)
                    literals = [
                        TestDecimal.min(precision),
                        TestDecimal.rnd(precision),
                        TestDecimal.max(precision),
                    ]
                goe_type_mapping_cols.append(
                    {"column": backend_column, "literals": literals}
                )
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
            elif col_dict["expected_canonical_column"].data_type == GOE_TYPE_BINARY:
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
        """BigQuery load tables should always be in GCS"""
        self._log(
            "load_table_fs_scheme_is_correct(%s, %s)" % (load_db, table_name),
            detail=VERBOSE,
        )
        location = self.get_table_location(load_db, table_name)
        scheme = get_scheme_from_location_uri(location) if location else None
        self._log("Identified scheme: %s" % scheme, detail=VERBOSE)
        return bool(scheme == OFFLOAD_FS_SCHEME_GS)

    def partition_has_stats(
        self, db_name, table_name, partition_tuples, colstats=False
    ):
        raise NotImplementedError("partition_has_stats() unsupported for BigQuery")

    def rename_column(self, db_name, table_name, column_name, new_name, sync=None):
        """BigQuery doesn't have rename column so we need to recreate the table with the column renamed.
        We utilise the "table rewrite" feature to replace the table:
            https://medium.com/google-cloud/bigquery-drop-or-change-column-43409ae12bc4
            CREATE OR REPLACE TABLE `transactions.test_table` AS
            SELECT col1, col2, col3 AS col3_new
            FROM `transactions.test_table`;
        This doesn't support partitioned tables (yet), because this is for testing only we've not
        added support for anything beyond the minimum required.
        """

        def rename_ddl_fn(ddl_line, column_name, new_name):
            if (
                ddl_line
                and self.enclose_identifier(column_name).lower() in ddl_line.lower()
            ):
                return ddl_line.lower().replace(
                    self.enclose_identifier(column_name).lower(),
                    self.enclose_identifier(new_name).lower(),
                )
            elif ddl_line and (" " + column_name.lower() + " ") in ddl_line.lower():
                # Column was not escaped in backticks but found it surrounded by spaces
                return ddl_line.lower().replace(
                    (" " + column_name.lower() + " "), (" " + new_name.lower() + " ")
                )
            else:
                return ddl_line

        assert db_name and table_name and column_name
        assert isinstance(column_name, str)
        existing_columns = self.get_columns(db_name, table_name)
        assert match_table_column(column_name, existing_columns), (
            "Column %s is not in the table" % column_name
        )

        self._log(
            "Recreating %s with column %s renamed to %s"
            % (
                self._db_api.enclose_object_reference(db_name, table_name),
                column_name,
                new_name,
            ),
            detail=VVERBOSE,
        )
        orig_ddl = self._db_api.get_table_ddl(
            db_name, table_name, as_list=True, for_replace=True
        )
        self._log("rename_column original DDL: %s" % str(orig_ddl), detail=VVERBOSE)

        # Rename the column in the CREATE DDL and stitch it together as a string
        new_ddl = "\n".join(rename_ddl_fn(_, column_name, new_name) for _ in orig_ddl)

        # Rename the column in the SELECT portion and stitch it together as a string
        rename_sel_fn = lambda x: (
            "{} AS {}".format(x, new_name) if x.lower() == column_name.lower() else x
        )
        projection = "\n,      ".join(rename_sel_fn(_.name) for _ in existing_columns)
        new_ddl += """\nAS
SELECT %(projection)s
FROM %(db_table)s""" % {
            "db_table": self._db_api.enclose_object_reference(db_name, table_name),
            "projection": projection,
        }

        return self.execute_ddl(new_ddl)

    def select_single_non_null_value(
        self, db_name, table_name, column_name, project_expression
    ):
        return self._select_single_non_null_value_common(
            db_name, table_name, column_name, project_expression
        )

    def sql_median_expression(self, db_name, table_name, column_name):
        """BigQuery PERCENTILE_DISC suits all data types."""
        return "PERCENTILE_DISC(%s, 0.5) OVER ()" % self.enclose_identifier(column_name)

    def story_test_offload_nums_expected_backend_types(self, sampling_enabled=True):
        def numeric(p, s):
            return "%s(%s,%s)" % (BIGQUERY_TYPE_NUMERIC, p, s)

        def bignumeric(p, s):
            return "%s(%s,%s)" % (BIGQUERY_TYPE_BIGNUMERIC, p, s)

        non_sampled_type = self.gen_default_numeric_column("x").format_data_type()
        return {
            STORY_TEST_OFFLOAD_NUMS_BARE_NUM: (
                BIGQUERY_TYPE_NUMERIC if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_BARE_FLT: (
                BIGQUERY_TYPE_INT64 if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_NUM_4: BIGQUERY_TYPE_INT64,
            STORY_TEST_OFFLOAD_NUMS_NUM_18: BIGQUERY_TYPE_INT64,
            STORY_TEST_OFFLOAD_NUMS_NUM_19: numeric(19, 0),
            STORY_TEST_OFFLOAD_NUMS_NUM_3_2: numeric(3, 2),
            STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: (
                BIGQUERY_TYPE_NUMERIC if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_NUM_3_5: (
                bignumeric(5, 5) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_NUM_10_M5: BIGQUERY_TYPE_INT64,
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0: (
                numeric(10, 0) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_DEC_13_9: (
                numeric(13, 9) if sampling_enabled else non_sampled_type
            ),
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3: (
                bignumeric(38, 3) if sampling_enabled else non_sampled_type
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
                "sql_expression": "CAST(123.123 AS %s)" % BIGQUERY_TYPE_FLOAT64
            },
            STORY_TEST_BACKEND_INT_8_COL: {
                "sql_expression": "CAST(1234567890123 AS %s)" % BIGQUERY_TYPE_INT64
            },
            STORY_TEST_BACKEND_DECIMAL_DEF_COL: {
                "sql_expression": "CAST(123.123 AS %s)" % BIGQUERY_TYPE_NUMERIC
            },
            STORY_TEST_BACKEND_VAR_STR_COL: {
                "sql_expression": "CAST('this is string' AS %s)" % BIGQUERY_TYPE_STRING
            },
            STORY_TEST_BACKEND_DATE_COL: {"sql_expression": "CURRENT_DATE()"},
            STORY_TEST_BACKEND_DATETIME_COL: {"sql_expression": "CURRENT_DATETIME()"},
            STORY_TEST_BACKEND_TIMESTAMP_COL: {"sql_expression": "CURRENT_TIMESTAMP()"},
            STORY_TEST_BACKEND_BLOB_COL: {
                "sql_expression": "CAST('this is string' AS %s)" % BIGQUERY_TYPE_BYTES
            },
            STORY_TEST_BACKEND_NULL_STR_COL: {
                "sql_expression": "CAST(NULL AS %s)" % BIGQUERY_TYPE_STRING
            },
        }
        return extra_cols

    def unit_test_query_options(self):
        return {}
