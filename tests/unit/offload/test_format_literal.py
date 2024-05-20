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
from numpy import datetime64
import pytest

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
from goe.offload.spark.pyspark_literal import PysparkLiteral
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

DATE_TIME = datetime(2020, 10, 31, 12, 12, 00)
DATE_TIME_FRAC = datetime(2020, 10, 31, 12, 12, 00, 12345)


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
        (DATE_TIME, HADOOP_TYPE_TIMESTAMP, "timestamp '2020-10-31 12:12:00'"),
        (
            DATE_TIME_FRAC,
            HADOOP_TYPE_TIMESTAMP,
            "timestamp '2020-10-31 12:12:00.012345'",
        ),
        (
            datetime64(DATE_TIME),
            HADOOP_TYPE_TIMESTAMP,
            "timestamp '2020-10-31 12:12:00'",
        ),
        (
            datetime64(DATE_TIME_FRAC),
            HADOOP_TYPE_TIMESTAMP,
            "timestamp '2020-10-31 12:12:00.012345'",
        ),
        (date(2020, 10, 31), HADOOP_TYPE_DATE, "date '2020-10-31'"),
    ],
)
def test_format_hive_literal(py_val, data_type, expected_literal):
    assert HiveLiteral.format_literal(py_val, data_type) == expected_literal


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
        (DATE_TIME, HADOOP_TYPE_TIMESTAMP, "'2020-10-31 12:12:00'"),
        (DATE_TIME_FRAC, HADOOP_TYPE_TIMESTAMP, "'2020-10-31 12:12:00.012345'"),
        (datetime64(DATE_TIME), HADOOP_TYPE_TIMESTAMP, "'2020-10-31 12:12:00'"),
        (
            datetime64(DATE_TIME_FRAC),
            HADOOP_TYPE_TIMESTAMP,
            "'2020-10-31 12:12:00.012345'",
        ),
    ],
)
def test_format_impala_literal(py_val, data_type, expected_literal):
    assert ImpalaLiteral.format_literal(py_val, data_type) == expected_literal


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
        (DATE_TIME, BIGQUERY_TYPE_DATETIME, "DATETIME '2020-10-31 12:12:00.0'"),
        (
            DATE_TIME_FRAC,
            BIGQUERY_TYPE_DATETIME,
            "DATETIME '2020-10-31 12:12:00.012345'",
        ),
        (
            datetime64(DATE_TIME),
            BIGQUERY_TYPE_DATETIME,
            "DATETIME '2020-10-31 12:12:00.0'",
        ),
        (
            datetime64(DATE_TIME_FRAC),
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
            datetime64(DATE_TIME_FRAC),
            BIGQUERY_TYPE_TIME,
            "TIME '%s'" % DATE_TIME_FRAC.strftime("%H:%M:%S.%f"),
        ),
    ],
)
def test_format_bigquery_literal(py_val, data_type, expected_literal):
    assert BigQueryLiteral.format_literal(py_val, data_type) == expected_literal


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
        (DATE_TIME, ORACLE_TYPE_DATE, "DATE' 2020-10-31 12:12:00'"),
        (datetime64(DATE_TIME), ORACLE_TYPE_DATE, "DATE' 2020-10-31 12:12:00'"),
        (
            DATE_TIME,
            ORACLE_TYPE_TIMESTAMP,
            "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF9')",
        ),
        (
            DATE_TIME,
            ORACLE_TYPE_TIMESTAMP + "(6)",
            "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF6')",
        ),
        (
            DATE_TIME_FRAC,
            ORACLE_TYPE_TIMESTAMP,
            "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF9')",
        ),
        (
            DATE_TIME_FRAC,
            ORACLE_TYPE_TIMESTAMP + "(6)",
            "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF6')",
        ),
        (
            DATE_TIME_FRAC,
            ORACLE_TYPE_TIMESTAMP + "(0)",
            "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF')",
        ),
        (
            datetime64(DATE_TIME),
            ORACLE_TYPE_TIMESTAMP,
            "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF9')",
        ),
        (
            datetime64(DATE_TIME),
            ORACLE_TYPE_TIMESTAMP + "(6)",
            "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF6')",
        ),
        (
            datetime64(DATE_TIME),
            ORACLE_TYPE_TIMESTAMP + "(0)",
            "TO_TIMESTAMP('2020-10-31 12:12:00.0','YYYY-MM-DD HH24:MI:SS.FF')",
        ),
        (
            datetime64(DATE_TIME_FRAC),
            ORACLE_TYPE_TIMESTAMP,
            "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF9')",
        ),
        (
            datetime64(DATE_TIME_FRAC),
            ORACLE_TYPE_TIMESTAMP + "(6)",
            "TO_TIMESTAMP('2020-10-31 12:12:00.012345','YYYY-MM-DD HH24:MI:SS.FF6')",
        ),
        (
            datetime64(DATE_TIME_FRAC),
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
    ],
)
def test_format_oracle_literal(py_val, data_type, expected_literal):
    formatted_value = OracleLiteral.format_literal(py_val, data_type)
    assert formatted_value == expected_literal, "Input value: ({}) {}".format(
        type(py_val), py_val
    )


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
            DATE_TIME,
            SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
            "'2020-10-31 12:12:00.0'::TIMESTAMP_NTZ",
        ),
        (
            DATE_TIME_FRAC,
            SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
            "'2020-10-31 12:12:00.012345'::TIMESTAMP_NTZ",
        ),
        (
            datetime64(DATE_TIME),
            SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
            "'2020-10-31 12:12:00.0'::TIMESTAMP_NTZ",
        ),
        (
            datetime64(DATE_TIME_FRAC),
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
            datetime64(DATE_TIME_FRAC),
            SNOWFLAKE_TYPE_TIME,
            "'%s'::TIME" % DATE_TIME_FRAC.strftime("%H:%M:%S.%f"),
        ),
    ],
)
def test_format_snowflake_literal(py_val, data_type, expected_literal):
    assert SnowflakeLiteral.format_literal(py_val, data_type) == expected_literal


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
        (DATE_TIME, SYNAPSE_TYPE_DATE, "'2020-10-31'"),
        (datetime64(DATE_TIME), SYNAPSE_TYPE_DATE, "'2020-10-31'"),
        (DATE_TIME, SYNAPSE_TYPE_DATETIME, "'2020-10-31 12:12:00.0'"),
        (DATE_TIME_FRAC, SYNAPSE_TYPE_DATETIME, "'2020-10-31 12:12:00.012345'"),
        (datetime64(DATE_TIME), SYNAPSE_TYPE_DATETIME, "'2020-10-31 12:12:00.0'"),
        (
            datetime64(DATE_TIME_FRAC),
            SYNAPSE_TYPE_DATETIME,
            "'2020-10-31 12:12:00.012345'",
        ),
        (DATE_TIME, SYNAPSE_TYPE_DATETIME2, "'2020-10-31 12:12:00.0'"),
        (DATE_TIME, SYNAPSE_TYPE_SMALLDATETIME, "'2020-10-31 12:12:00.0'"),
        ("12:13:14", SYNAPSE_TYPE_TIME, "'12:13:14'"),
        (
            datetime64(DATE_TIME_FRAC),
            SYNAPSE_TYPE_TIME,
            "'%s'" % DATE_TIME_FRAC.strftime("%H:%M:%S.%f"),
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
    ],
)
def test_format_synapse_literal(py_val, data_type, expected_literal):
    formatted_value = SynapseLiteral.format_literal(py_val, data_type)
    assert formatted_value == expected_literal, "Input value: ({}) {}".format(
        data_type or type(py_val), py_val
    )


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
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
        (DATE_TIME, TERADATA_TYPE_DATE, "DATE '2020-10-31'"),
        (datetime64(DATE_TIME), TERADATA_TYPE_DATE, "DATE '2020-10-31'"),
        (DATE_TIME, TERADATA_TYPE_TIMESTAMP, "TIMESTAMP '2020-10-31 12:12:00.0'"),
        (
            DATE_TIME_FRAC,
            TERADATA_TYPE_TIMESTAMP,
            "TIMESTAMP '2020-10-31 12:12:00.012345'",
        ),
        (
            datetime64(DATE_TIME),
            TERADATA_TYPE_TIMESTAMP,
            "TIMESTAMP '2020-10-31 12:12:00.0'",
        ),
        (
            datetime64(DATE_TIME_FRAC),
            TERADATA_TYPE_TIMESTAMP,
            "TIMESTAMP '2020-10-31 12:12:00.012345'",
        ),
        ("12:13:14", TERADATA_TYPE_TIME, "TIME '12:13:14'"),
        (
            datetime64(DATE_TIME_FRAC),
            TERADATA_TYPE_TIME,
            "TIME '%s'" % DATE_TIME_FRAC.strftime("%H:%M:%S.%f"),
        ),
        # Binary
        ("Hello", TERADATA_TYPE_BYTE, "48656c6c6fXB"),
        (b"Hello", TERADATA_TYPE_BYTE, "48656c6c6fXB"),
        ("Hello", TERADATA_TYPE_VARBYTE, "48656c6c6fXBV"),
        (b"Hello", TERADATA_TYPE_VARBYTE, "48656c6c6fXBV"),
    ],
)
def test_format_teradata_literal(py_val, data_type, expected_literal):
    formatted_value = TeradataLiteral.format_literal(py_val, data_type)
    assert formatted_value == expected_literal, "Input value: ({}) {}".format(
        data_type or type(py_val), py_val
    )


@pytest.mark.parametrize(
    "py_val,data_type,expected_literal",
    [
        # Strings
        ("Hello", None, "'Hello'"),
        (None, None, "None"),
        # Numbers
        (3, None, "3"),
        (Decimal("1234567890"), None, "1234567890"),
        (
            123456789012345678901234567890,
            None,
            "123456789012345678901234567890",
        ),
        # Dates and times
        (DATE_TIME, None, "'2020-10-31 12:12:00'"),
        (DATE_TIME_FRAC, None, "'2020-10-31 12:12:00.012345'"),
        (datetime64(DATE_TIME), None, "'2020-10-31 12:12:00'"),
        (
            datetime64(DATE_TIME_FRAC),
            None,
            "'2020-10-31 12:12:00.012345'",
        ),
    ],
)
def test_format_pysparkl_literal(py_val, data_type, expected_literal):
    formatted_value = PysparkLiteral.format_literal(py_val, data_type)
    assert formatted_value == expected_literal, "Input value: ({}) {}".format(
        data_type or type(py_val), py_val
    )
