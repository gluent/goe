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

""" Unit tests for FormatLiteralInterface implementations
"""
from datetime import date, datetime
from decimal import Decimal
from unittest import TestCase, main
from numpy import datetime64

from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_TIMESTAMP,
)
from goe.offload.hadoop.hive_literal import HiveLiteral
from goe.offload.hadoop.impala_literal import ImpalaLiteral
from goe.offload.bigquery.bigquery_literal import BigQueryLiteral
from goe.offload.bigquery.bigquery_column import (
    BIGQUERY_TYPE_DATE,
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_FLOAT64,
    BIGQUERY_TYPE_INT64,
    BIGQUERY_TYPE_NUMERIC,
    BIGQUERY_TYPE_BIGNUMERIC,
    BIGQUERY_TYPE_STRING,
    BIGQUERY_TYPE_TIME,
)
from goe.offload.microsoft.synapse_column import (
    SYNAPSE_TYPE_BIGINT,
    SYNAPSE_TYPE_BINARY,
    SYNAPSE_TYPE_CHAR,
    SYNAPSE_TYPE_DATE,
    SYNAPSE_TYPE_DATETIME,
    SYNAPSE_TYPE_DATETIME2,
    SYNAPSE_TYPE_FLOAT,
    SYNAPSE_TYPE_INT,
    SYNAPSE_TYPE_NCHAR,
    SYNAPSE_TYPE_NUMERIC,
    SYNAPSE_TYPE_NVARCHAR,
    SYNAPSE_TYPE_SMALLDATETIME,
    SYNAPSE_TYPE_TIME,
    SYNAPSE_TYPE_UNIQUEIDENTIFIER,
    SYNAPSE_TYPE_VARBINARY,
    SYNAPSE_TYPE_VARCHAR,
)
from goe.offload.microsoft.synapse_literal import SynapseLiteral
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_VARCHAR2,
)
from goe.offload.oracle.oracle_literal import OracleLiteral
from goe.offload.snowflake.snowflake_column import (
    SNOWFLAKE_TYPE_DATE,
    SNOWFLAKE_TYPE_FLOAT,
    SNOWFLAKE_TYPE_NUMBER,
    SNOWFLAKE_TYPE_TEXT,
    SNOWFLAKE_TYPE_TIME,
    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
)
from goe.offload.snowflake.snowflake_literal import SnowflakeLiteral
from goe.offload.teradata.teradata_column import (
    TERADATA_TYPE_BIGINT,
    TERADATA_TYPE_BYTE,
    TERADATA_TYPE_BYTEINT,
    TERADATA_TYPE_CHAR,
    TERADATA_TYPE_CLOB,
    TERADATA_TYPE_DATE,
    TERADATA_TYPE_DECIMAL,
    TERADATA_TYPE_DOUBLE,
    TERADATA_TYPE_INTEGER,
    TERADATA_TYPE_NUMBER,
    TERADATA_TYPE_SMALLINT,
    TERADATA_TYPE_TIME,
    TERADATA_TYPE_TIMESTAMP,
    TERADATA_TYPE_VARBYTE,
    TERADATA_TYPE_VARCHAR,
)
from goe.offload.teradata.teradata_literal import TeradataLiteral


class TestFormatLiteral(TestCase):
    def test_format_hive_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            ("Hello", HADOOP_TYPE_STRING, "'Hello'"),
            (123, HADOOP_TYPE_BIGINT, "123"),
            (123.123, HADOOP_TYPE_DECIMAL, "123.123"),
            (
                Decimal("1234567890.123456789"),
                HADOOP_TYPE_DECIMAL,
                "1234567890.123456789",
            ),
            (123.123, HADOOP_TYPE_DOUBLE, "123.123"),
            (
                Decimal("1234567890.123456789"),
                HADOOP_TYPE_DOUBLE,
                "1234567890.123456789",
            ),
            (date_time, HADOOP_TYPE_TIMESTAMP, "timestamp '2020-10-31 12:12:00'"),
            (
                date_time_frac,
                HADOOP_TYPE_TIMESTAMP,
                "timestamp '2020-10-31 12:12:00.012345'",
            ),
            (
                datetime64(date_time),
                HADOOP_TYPE_TIMESTAMP,
                "timestamp '2020-10-31 12:12:00'",
            ),
            (
                datetime64(date_time_frac),
                HADOOP_TYPE_TIMESTAMP,
                "timestamp '2020-10-31 12:12:00.012345'",
            ),
            (date(2020, 10, 31), HADOOP_TYPE_DATE, "date '2020-10-31'"),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            self.assertEqual(
                HiveLiteral.format_literal(py_val, data_type), expected_literal
            )

    def test_format_impala_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            ("Hello", HADOOP_TYPE_STRING, "'Hello'"),
            (123, HADOOP_TYPE_BIGINT, "123"),
            (123.123, HADOOP_TYPE_DECIMAL, "123.123"),
            (
                Decimal("1234567890.123456789"),
                HADOOP_TYPE_DECIMAL,
                "1234567890.123456789",
            ),
            (123.123, HADOOP_TYPE_DOUBLE, "123.123"),
            (
                Decimal("1234567890.123456789"),
                HADOOP_TYPE_DOUBLE,
                "1234567890.123456789",
            ),
            (date_time, HADOOP_TYPE_TIMESTAMP, "'2020-10-31 12:12:00'"),
            (date_time_frac, HADOOP_TYPE_TIMESTAMP, "'2020-10-31 12:12:00.012345'"),
            (datetime64(date_time), HADOOP_TYPE_TIMESTAMP, "'2020-10-31 12:12:00'"),
            (
                datetime64(date_time_frac),
                HADOOP_TYPE_TIMESTAMP,
                "'2020-10-31 12:12:00.012345'",
            ),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            self.assertEqual(
                ImpalaLiteral.format_literal(py_val, data_type), expected_literal
            )

    def test_format_bigquery_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            ("Hello", BIGQUERY_TYPE_STRING, "'Hello'"),
            (123, BIGQUERY_TYPE_INT64, "123"),
            (123.123, BIGQUERY_TYPE_NUMERIC, "NUMERIC '123.123'"),
            (
                Decimal("1234567890.123456789"),
                BIGQUERY_TYPE_NUMERIC,
                "NUMERIC '1234567890.123456789'",
            ),
            (123.123, BIGQUERY_TYPE_BIGNUMERIC, "BIGNUMERIC '123.123'"),
            (
                Decimal("1234567890.123456789"),
                BIGQUERY_TYPE_BIGNUMERIC,
                "BIGNUMERIC '1234567890.123456789'",
            ),
            (123.123, BIGQUERY_TYPE_FLOAT64, "123.123"),
            (
                Decimal("1234567890.123456789"),
                BIGQUERY_TYPE_FLOAT64,
                "1234567890.123456789",
            ),
            (date_time, BIGQUERY_TYPE_DATETIME, "DATETIME '2020-10-31 12:12:00.0'"),
            (
                date_time_frac,
                BIGQUERY_TYPE_DATETIME,
                "DATETIME '2020-10-31 12:12:00.012345'",
            ),
            (
                datetime64(date_time),
                BIGQUERY_TYPE_DATETIME,
                "DATETIME '2020-10-31 12:12:00.0'",
            ),
            (
                datetime64(date_time_frac),
                BIGQUERY_TYPE_DATETIME,
                "DATETIME '2020-10-31 12:12:00.012345'",
            ),
            (date(2020, 10, 31), BIGQUERY_TYPE_DATE, "DATE '2020-10-31'"),
            (datetime(2020, 10, 31, 0, 0, 0), BIGQUERY_TYPE_DATE, "DATE '2020-10-31'"),
            (datetime64(date(2020, 10, 31)), BIGQUERY_TYPE_DATE, "DATE '2020-10-31'"),
            (
                datetime64(datetime(2020, 10, 31, 0, 0, 0)),
                BIGQUERY_TYPE_DATE,
                "DATE '2020-10-31'",
            ),
            ("12:13:14", BIGQUERY_TYPE_TIME, "TIME '12:13:14'"),
            (
                datetime64(date_time_frac),
                BIGQUERY_TYPE_TIME,
                "TIME '%s'" % date_time_frac.strftime("%H:%M:%S.%f"),
            ),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            self.assertEqual(
                BigQueryLiteral.format_literal(py_val, data_type), expected_literal
            )

    def test_format_oracle_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            ("Hello", ORACLE_TYPE_VARCHAR2, "'Hello'"),
            (123, ORACLE_TYPE_NUMBER, "123"),
            (123.123, ORACLE_TYPE_NUMBER, "123.123"),
            (
                Decimal("1234567890.123456789"),
                ORACLE_TYPE_NUMBER,
                "1234567890.123456789",
            ),
            (date(2020, 10, 31), ORACLE_TYPE_DATE, "DATE' 2020-10-31 00:00:00'"),
            (
                datetime(2020, 10, 31, 0, 0, 0),
                ORACLE_TYPE_DATE,
                "DATE' 2020-10-31 00:00:00'",
            ),
            (
                datetime64(date(2020, 10, 31)),
                ORACLE_TYPE_DATE,
                "DATE' 2020-10-31 00:00:00'",
            ),
            (
                datetime64(datetime(2020, 10, 31, 0, 0, 0)),
                ORACLE_TYPE_DATE,
                "DATE' 2020-10-31 00:00:00'",
            ),
            (date_time, ORACLE_TYPE_DATE, "DATE' 2020-10-31 12:12:00'"),
            (datetime64(date_time), ORACLE_TYPE_DATE, "DATE' 2020-10-31 12:12:00'"),
            (
                date_time,
                ORACLE_TYPE_TIMESTAMP,
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF9')",
            ),
            (
                date_time,
                ORACLE_TYPE_TIMESTAMP + "(6)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF6')",
            ),
            (
                date_time_frac,
                ORACLE_TYPE_TIMESTAMP,
                "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF9')",
            ),
            (
                date_time_frac,
                ORACLE_TYPE_TIMESTAMP + "(6)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF6')",
            ),
            (
                date_time_frac,
                ORACLE_TYPE_TIMESTAMP + "(0)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF')",
            ),
            (
                datetime64(date_time),
                ORACLE_TYPE_TIMESTAMP,
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF9')",
            ),
            (
                datetime64(date_time),
                ORACLE_TYPE_TIMESTAMP + "(6)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF6')",
            ),
            (
                datetime64(date_time),
                ORACLE_TYPE_TIMESTAMP + "(0)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF')",
            ),
            (
                datetime64(date_time_frac),
                ORACLE_TYPE_TIMESTAMP,
                "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF9')",
            ),
            (
                datetime64(date_time_frac),
                ORACLE_TYPE_TIMESTAMP + "(6)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF6')",
            ),
            (
                datetime64(date_time_frac),
                ORACLE_TYPE_TIMESTAMP + "(0)",
                "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF')",
            ),
            # Some tests inferring the result rather than using a column data type
            ("Hello", None, "'Hello'"),
            (123, None, "123"),
            (123.123, None, "123.123"),
            (Decimal("1234567890.123456789"), None, "1234567890.123456789"),
            (date(2020, 10, 31), None, "DATE' 2020-10-31 00:00:00'"),
            (datetime(2020, 10, 31, 0, 0, 0), None, "DATE' 2020-10-31 00:00:00'"),
            (datetime64(date(2020, 10, 31)), None, "DATE' 2020-10-31 00:00:00'"),
            (
                datetime64(datetime(2020, 10, 31, 0, 0, 0)),
                None,
                "TO_TIMESTAMP('2020-10-31 00:00:00.0','YYYY-MM-DD HH24:MI:SS.FF9')",
            ),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            formatted_value = OracleLiteral.format_literal(py_val, data_type)
            self.assertEqual(
                formatted_value,
                expected_literal,
                "Input value: ({}) {}".format(type(py_val), py_val),
            )

    def test_format_snowflake_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            ("Hello", SNOWFLAKE_TYPE_TEXT, "'Hello'"),
            (123, SNOWFLAKE_TYPE_NUMBER, "123"),
            (123.123, SNOWFLAKE_TYPE_NUMBER, "123.123"),
            (
                Decimal("1234567890.123456789"),
                SNOWFLAKE_TYPE_NUMBER,
                "1234567890.123456789",
            ),
            (123.123, SNOWFLAKE_TYPE_FLOAT, "123.123"),
            (
                Decimal("1234567890.123456789"),
                SNOWFLAKE_TYPE_FLOAT,
                "1234567890.123456789",
            ),
            (
                date_time,
                SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                "'2020-10-31 12:12:00.0'::TIMESTAMP_NTZ",
            ),
            (
                date_time_frac,
                SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                "'2020-10-31 12:12:00.012345'::TIMESTAMP_NTZ",
            ),
            (
                datetime64(date_time),
                SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                "'2020-10-31 12:12:00.0'::TIMESTAMP_NTZ",
            ),
            (
                datetime64(date_time_frac),
                SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                "'2020-10-31 12:12:00.012345'::TIMESTAMP_NTZ",
            ),
            (date(2020, 10, 31), SNOWFLAKE_TYPE_DATE, "'2020-10-31'::DATE"),
            (
                datetime(2020, 10, 31, 0, 0, 0),
                SNOWFLAKE_TYPE_DATE,
                "'2020-10-31'::DATE",
            ),
            (datetime64(date(2020, 10, 31)), SNOWFLAKE_TYPE_DATE, "'2020-10-31'::DATE"),
            (
                datetime64(datetime(2020, 10, 31, 0, 0, 0)),
                SNOWFLAKE_TYPE_DATE,
                "'2020-10-31'::DATE",
            ),
            ("12:13:14", SNOWFLAKE_TYPE_TIME, "'12:13:14'::TIME"),
            (
                datetime64(date_time_frac),
                SNOWFLAKE_TYPE_TIME,
                "'%s'::TIME" % date_time_frac.strftime("%H:%M:%S.%f"),
            ),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            self.assertEqual(
                SnowflakeLiteral.format_literal(py_val, data_type), expected_literal
            )

    def test_format_synapse_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            ("Hello", SYNAPSE_TYPE_VARCHAR, "'Hello'"),
            (None, SYNAPSE_TYPE_VARCHAR, "NULL"),
            ("Hello", SYNAPSE_TYPE_NVARCHAR, "N'Hello'"),
            (None, SYNAPSE_TYPE_NVARCHAR, "NULL"),
            ("Hello", SYNAPSE_TYPE_CHAR, "'Hello'"),
            (None, SYNAPSE_TYPE_CHAR, "NULL"),
            ("Hello", SYNAPSE_TYPE_NCHAR, "N'Hello'"),
            (None, SYNAPSE_TYPE_NCHAR, "NULL"),
            (123, SYNAPSE_TYPE_NUMERIC, "123"),
            (123.123, SYNAPSE_TYPE_NUMERIC, "123.123"),
            (
                Decimal("1234567890.123456789"),
                SYNAPSE_TYPE_NUMERIC,
                "1234567890.123456789",
            ),
            (123, SYNAPSE_TYPE_INT, "123"),
            (123, SYNAPSE_TYPE_BIGINT, "123"),
            (123.123, SYNAPSE_TYPE_FLOAT, "123.123"),
            (
                Decimal("1234567890.123456789"),
                SYNAPSE_TYPE_FLOAT,
                "1234567890.123456789",
            ),
            (date(2020, 10, 31), SYNAPSE_TYPE_DATE, "'2020-10-31'"),
            (date_time, SYNAPSE_TYPE_DATE, "'2020-10-31'"),
            (datetime64(date_time), SYNAPSE_TYPE_DATE, "'2020-10-31'"),
            (date_time, SYNAPSE_TYPE_DATETIME, "'2020-10-31 12:12:00.0'"),
            (date_time_frac, SYNAPSE_TYPE_DATETIME, "'2020-10-31 12:12:00.012345'"),
            (datetime64(date_time), SYNAPSE_TYPE_DATETIME, "'2020-10-31 12:12:00.0'"),
            (
                datetime64(date_time_frac),
                SYNAPSE_TYPE_DATETIME,
                "'2020-10-31 12:12:00.012345'",
            ),
            (date_time, SYNAPSE_TYPE_DATETIME2, "'2020-10-31 12:12:00.0'"),
            (date_time, SYNAPSE_TYPE_SMALLDATETIME, "'2020-10-31 12:12:00.0'"),
            ("12:13:14", SYNAPSE_TYPE_TIME, "'12:13:14'"),
            (
                datetime64(date_time_frac),
                SYNAPSE_TYPE_TIME,
                "'%s'" % date_time_frac.strftime("%H:%M:%S.%f"),
            ),
            ("Hello", SYNAPSE_TYPE_BINARY, "0x48656c6c6f"),
            (b"Hello", SYNAPSE_TYPE_BINARY, "0x48656c6c6f"),
            ("Hello", SYNAPSE_TYPE_VARBINARY, "0x48656c6c6f"),
            (b"Hello", SYNAPSE_TYPE_VARBINARY, "0x48656c6c6f"),
            (
                "1AFC7F5C-FFA0-4741-81CF-F12EAAB822BF",
                SYNAPSE_TYPE_UNIQUEIDENTIFIER,
                "'1AFC7F5C-FFA0-4741-81CF-F12EAAB822BF'",
            ),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            formatted_value = SynapseLiteral.format_literal(py_val, data_type)
            self.assertEqual(
                formatted_value,
                expected_literal,
                "Input value: ({}) {}".format(data_type or type(py_val), py_val),
            )

    def test_format_teradata_literal(self):
        date_time = datetime(2020, 10, 31, 12, 12, 00)
        date_time_frac = datetime(2020, 10, 31, 12, 12, 00, 12345)
        literals_to_test = [
            # Strings
            ("Hello", TERADATA_TYPE_VARCHAR, "'Hello'"),
            (None, TERADATA_TYPE_VARCHAR, "NULL"),
            ("Hello", TERADATA_TYPE_CHAR, "'Hello'"),
            (None, TERADATA_TYPE_CHAR, "NULL"),
            ("Hello", TERADATA_TYPE_CLOB, "'Hello'"),
            # Numbers
            (3, TERADATA_TYPE_BYTEINT, "3"),
            (Decimal("3"), TERADATA_TYPE_BYTEINT, "3"),
            (123, TERADATA_TYPE_SMALLINT, "123"),
            (1234567890, TERADATA_TYPE_INTEGER, "1234567890"),
            (1234567890, TERADATA_TYPE_BIGINT, "1234567890"),
            (Decimal("1234567890"), TERADATA_TYPE_BIGINT, "1234567890"),
            (
                123456789012345678901234567890,
                TERADATA_TYPE_NUMBER,
                "123456789012345678901234567890",
            ),
            (
                -123456789012345678901234567890,
                TERADATA_TYPE_NUMBER,
                "-123456789012345678901234567890",
            ),
            (123.123, TERADATA_TYPE_NUMBER, "123.123"),
            (
                Decimal("1234567890.123456789"),
                TERADATA_TYPE_NUMBER,
                "1234567890.123456789",
            ),
            (
                123456789012345678901234567890,
                TERADATA_TYPE_DECIMAL,
                "123456789012345678901234567890",
            ),
            (
                -123456789012345678901234567890,
                TERADATA_TYPE_DECIMAL,
                "-123456789012345678901234567890",
            ),
            (123.123, TERADATA_TYPE_DECIMAL, "123.123"),
            (
                Decimal("1234567890.123456789"),
                TERADATA_TYPE_DECIMAL,
                "1234567890.123456789",
            ),
            (123.123, TERADATA_TYPE_DOUBLE, "123.123"),
            (
                Decimal("1234567890.123456789"),
                TERADATA_TYPE_DOUBLE,
                "1234567890.123456789",
            ),
            # Dates and times
            (date(2020, 10, 31), TERADATA_TYPE_DATE, "DATE '2020-10-31'"),
            (date(2020, 10, 31), None, "DATE '2020-10-31'"),
            (date_time, TERADATA_TYPE_DATE, "DATE '2020-10-31'"),
            (datetime64(date_time), TERADATA_TYPE_DATE, "DATE '2020-10-31'"),
            (date_time, TERADATA_TYPE_TIMESTAMP, "TIMESTAMP '2020-10-31 12:12:00.0'"),
            (
                date_time_frac,
                TERADATA_TYPE_TIMESTAMP,
                "TIMESTAMP '2020-10-31 12:12:00.012345'",
            ),
            (
                datetime64(date_time),
                TERADATA_TYPE_TIMESTAMP,
                "TIMESTAMP '2020-10-31 12:12:00.0'",
            ),
            (
                datetime64(date_time_frac),
                TERADATA_TYPE_TIMESTAMP,
                "TIMESTAMP '2020-10-31 12:12:00.012345'",
            ),
            ("12:13:14", TERADATA_TYPE_TIME, "TIME '12:13:14'"),
            (
                datetime64(date_time_frac),
                TERADATA_TYPE_TIME,
                "TIME '%s'" % date_time_frac.strftime("%H:%M:%S.%f"),
            ),
            # Binary
            ("Hello", TERADATA_TYPE_BYTE, "48656c6c6fXB"),
            (b"Hello", TERADATA_TYPE_BYTE, "48656c6c6fXB"),
            ("Hello", TERADATA_TYPE_VARBYTE, "48656c6c6fXBV"),
            (b"Hello", TERADATA_TYPE_VARBYTE, "48656c6c6fXBV"),
        ]

        for py_val, data_type, expected_literal in literals_to_test:
            formatted_value = TeradataLiteral.format_literal(py_val, data_type)
            self.assertEqual(
                formatted_value,
                expected_literal,
                "Input value: ({}) {}".format(data_type or type(py_val), py_val),
            )


if __name__ == "__main__":
    main()
