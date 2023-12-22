from unittest import TestCase, main

from goe.offload.bigquery.bigquery_column import (
    BigQueryColumn,
    BIGQUERY_TYPE_BIGNUMERIC,
    BIGQUERY_TYPE_BOOLEAN,
    BIGQUERY_TYPE_BYTES,
    BIGQUERY_TYPE_DATE,
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_FLOAT64,
    BIGQUERY_TYPE_INT64,
    BIGQUERY_TYPE_NUMERIC,
    BIGQUERY_TYPE_STRING,
    BIGQUERY_TYPE_TIME,
    BIGQUERY_TYPE_TIMESTAMP,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    match_table_column,
    CANONICAL_CHAR_SEMANTICS_BYTE,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
    GLUENT_TYPE_FIXED_STRING,
    GLUENT_TYPE_LARGE_STRING,
    GLUENT_TYPE_VARIABLE_STRING,
    GLUENT_TYPE_BINARY,
    GLUENT_TYPE_LARGE_BINARY,
    GLUENT_TYPE_INTEGER_1,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_INTEGER_4,
    GLUENT_TYPE_INTEGER_8,
    GLUENT_TYPE_INTEGER_38,
    GLUENT_TYPE_DECIMAL,
    GLUENT_TYPE_FLOAT,
    GLUENT_TYPE_DOUBLE,
    GLUENT_TYPE_DATE,
    GLUENT_TYPE_TIME,
    GLUENT_TYPE_TIMESTAMP,
    GLUENT_TYPE_TIMESTAMP_TZ,
    GLUENT_TYPE_INTERVAL_DS,
    GLUENT_TYPE_INTERVAL_YM,
    GLUENT_TYPE_BOOLEAN,
)
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.hadoop.hadoop_column import HadoopColumn
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SYNAPSE,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.microsoft.synapse_column import (
    SynapseColumn,
    SYNAPSE_TYPE_BIGINT,
    SYNAPSE_TYPE_BINARY,
    SYNAPSE_TYPE_BIT,
    SYNAPSE_TYPE_CHAR,
    SYNAPSE_TYPE_DATE,
    SYNAPSE_TYPE_DATETIME,
    SYNAPSE_TYPE_DATETIME2,
    SYNAPSE_TYPE_DATETIMEOFFSET,
    SYNAPSE_TYPE_FLOAT,
    SYNAPSE_TYPE_INT,
    SYNAPSE_TYPE_MONEY,
    SYNAPSE_TYPE_NCHAR,
    SYNAPSE_TYPE_NUMERIC,
    SYNAPSE_TYPE_NVARCHAR,
    SYNAPSE_TYPE_REAL,
    SYNAPSE_TYPE_SMALLDATETIME,
    SYNAPSE_TYPE_SMALLINT,
    SYNAPSE_TYPE_SMALLMONEY,
    SYNAPSE_TYPE_TIME,
    SYNAPSE_TYPE_TINYINT,
    SYNAPSE_TYPE_VARBINARY,
    SYNAPSE_TYPE_UNIQUEIDENTIFIER,
    SYNAPSE_TYPE_VARCHAR,
)
from goe.offload.microsoft.mssql_offload_source_table import MSSQLSourceTable
from goe.offload.microsoft.mssql_column import (
    MSSQLColumn,
    MSSQL_TYPE_BIGINT,
    MSSQL_TYPE_BIT,
    MSSQL_TYPE_DECIMAL,
    MSSQL_TYPE_INT,
    MSSQL_TYPE_MONEY,
    MSSQL_TYPE_NUMERIC,
    MSSQL_TYPE_SMALLINT,
    MSSQL_TYPE_SMALLMONEY,
    MSSQL_TYPE_TINYINT,
    MSSQL_TYPE_FLOAT,
    MSSQL_TYPE_REAL,
    MSSQL_TYPE_DATE,
    MSSQL_TYPE_DATETIME2,
    MSSQL_TYPE_DATETIME,
    MSSQL_TYPE_SMALLDATETIME,
    MSSQL_TYPE_TIME,
    MSSQL_TYPE_CHAR,
    MSSQL_TYPE_VARCHAR,
    MSSQL_TYPE_NCHAR,
    MSSQL_TYPE_NVARCHAR,
    MSSQL_TYPE_UNIQUEIDENTIFIER,
    MSSQL_TYPE_TEXT,
    MSSQL_TYPE_NTEXT,
    MSSQL_TYPE_BINARY,
    MSSQL_TYPE_VARBINARY,
    MSSQL_TYPE_IMAGE,
)
from goe.offload.netezza.netezza_column import (
    NetezzaColumn,
    NETEZZA_TYPE_BIGINT,
    NETEZZA_TYPE_INTEGER,
    NETEZZA_TYPE_SMALLINT,
    NETEZZA_TYPE_BYTEINT,
    NETEZZA_TYPE_DOUBLE_PRECISION,
    NETEZZA_TYPE_REAL,
    NETEZZA_TYPE_NUMERIC,
    NETEZZA_TYPE_TIME,
    NETEZZA_TYPE_TIMESTAMP,
    NETEZZA_TYPE_DATE,
    NETEZZA_TYPE_CHARACTER_VARYING,
    NETEZZA_TYPE_CHARACTER,
    NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING,
    NETEZZA_TYPE_NATIONAL_CHARACTER,
    NETEZZA_TYPE_BOOLEAN,
    NETEZZA_TYPE_TIME_WITH_TIME_ZONE,
    NETEZZA_TYPE_BINARY_VARYING,
    NETEZZA_TYPE_ST_GEOMETRY,
)
from goe.offload.netezza.netezza_offload_source_table import NetezzaSourceTable
from goe.offload.oracle.oracle_column import (
    OracleColumn,
    ORACLE_TYPE_CHAR,
    ORACLE_TYPE_NCHAR,
    ORACLE_TYPE_CLOB,
    ORACLE_TYPE_NCLOB,
    ORACLE_TYPE_LONG,
    ORACLE_TYPE_VARCHAR2,
    ORACLE_TYPE_NVARCHAR2,
    ORACLE_TYPE_RAW,
    ORACLE_TYPE_BLOB,
    ORACLE_TYPE_LONG_RAW,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_FLOAT,
    ORACLE_TYPE_BINARY_FLOAT,
    ORACLE_TYPE_BINARY_DOUBLE,
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_TIMESTAMP_TZ,
    ORACLE_TYPE_INTERVAL_DS,
    ORACLE_TYPE_INTERVAL_YM,
)
from goe.offload.oracle.oracle_offload_source_table import OracleSourceTable
from goe.offload.snowflake.snowflake_column import (
    SnowflakeColumn,
    SNOWFLAKE_TYPE_BINARY,
    SNOWFLAKE_TYPE_BOOLEAN,
    SNOWFLAKE_TYPE_DATE,
    SNOWFLAKE_TYPE_FLOAT,
    SNOWFLAKE_TYPE_NUMBER,
    SNOWFLAKE_TYPE_TEXT,
    SNOWFLAKE_TYPE_TIME,
    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
    SNOWFLAKE_TYPE_TIMESTAMP_TZ,
)
from goe.offload.factory.staging_file_factory import staging_file_factory
from goe.offload.staging.avro.avro_column import (
    StagingAvroColumn,
    AVRO_TYPE_STRING,
    AVRO_TYPE_LONG,
    AVRO_TYPE_BYTES,
    AVRO_TYPE_INT,
    AVRO_TYPE_BOOLEAN,
    AVRO_TYPE_FLOAT,
    AVRO_TYPE_DOUBLE,
)
from goe.offload.staging.parquet.parquet_column import (
    StagingParquetColumn,
    PARQUET_TYPE_STRING,
    PARQUET_TYPE_FLOAT,
    PARQUET_TYPE_INT32,
    PARQUET_TYPE_BINARY,
    PARQUET_TYPE_BOOLEAN,
    PARQUET_TYPE_DOUBLE,
    PARQUET_TYPE_INT64,
)
from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_BOOLEAN,
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_BINARY,
    HADOOP_TYPE_CHAR,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_DOUBLE_PRECISION,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_INTERVAL_DS,
    HADOOP_TYPE_INTERVAL_YM,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_TIMESTAMP,
    HADOOP_TYPE_TINYINT,
    HADOOP_TYPE_VARCHAR,
)
from tests.unit.test_functions import (
    build_mock_options,
    FAKE_MSSQL_ENV,
    FAKE_NETEZZA_ENV,
    FAKE_ORACLE_BQ_ENV,
    FAKE_ORACLE_HIVE_ENV,
    FAKE_ORACLE_IMPALA_ENV,
    FAKE_ORACLE_SNOWFLAKE_ENV,
    FAKE_ORACLE_SYNAPSE_ENV,
)


class TestDataTypeMappings(TestCase):
    def _stealth_unicode_char_semantics(self, canonical_column):
        if canonical_column.name.endswith("_U"):
            # CANONICAL_CHAR_SEMANTICS_UNICODE comes from a present option and is applied to
            # canonical columns, we do it by stealth here.
            canonical_column.char_semantics = CANONICAL_CHAR_SEMANTICS_UNICODE

    def canonical_columns(self):
        return [
            CanonicalColumn("COL_FIXED_STR", GLUENT_TYPE_FIXED_STRING, data_length=10),
            CanonicalColumn(
                "COL_FIXED_STR_2000", GLUENT_TYPE_FIXED_STRING, data_length=2000
            ),
            CanonicalColumn(
                "COL_FIXED_STR_2000_C",
                GLUENT_TYPE_FIXED_STRING,
                char_length=2000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_FIXED_STR_2001", GLUENT_TYPE_FIXED_STRING, data_length=2001
            ),
            CanonicalColumn(
                "COL_FIXED_STR_2001_C",
                GLUENT_TYPE_FIXED_STRING,
                char_length=2001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_FIXED_STR_1000_U",
                GLUENT_TYPE_FIXED_STRING,
                char_length=1000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            CanonicalColumn(
                "COL_FIXED_STR_1001_U",
                GLUENT_TYPE_FIXED_STRING,
                char_length=1001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            CanonicalColumn("COL_LARGE_STR", GLUENT_TYPE_LARGE_STRING),
            CanonicalColumn("COL_VARIABLE_STR", GLUENT_TYPE_VARIABLE_STRING),
            CanonicalColumn(
                "COL_VARIABLE_STR_4000", GLUENT_TYPE_VARIABLE_STRING, data_length=4000
            ),
            CanonicalColumn(
                "COL_VARIABLE_STR_4000_C",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=4000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_VARIABLE_STR_4001", GLUENT_TYPE_VARIABLE_STRING, data_length=4001
            ),
            CanonicalColumn(
                "COL_VARIABLE_STR_4001_C",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=4001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_VARIABLE_STR_2000_U",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=2000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            CanonicalColumn(
                "COL_VARIABLE_STR_2001_U",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=2001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_BINARY_2000", GLUENT_TYPE_BINARY, data_length=2000),
            CanonicalColumn("COL_BINARY_2001", GLUENT_TYPE_BINARY, data_length=2001),
            CanonicalColumn("COL_LARGE_BINARY", GLUENT_TYPE_LARGE_BINARY),
            CanonicalColumn("COL_INT_1", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_2", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_4", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_8", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn("COL_DEC_NO_P_S", GLUENT_TYPE_DECIMAL),
            CanonicalColumn(
                "COL_DEC_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_FLOAT", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_TIMESTAMP", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_TIMESTAMP_TZ", GLUENT_TYPE_TIMESTAMP_TZ),
            CanonicalColumn("COL_INTERVAL_DS", GLUENT_TYPE_INTERVAL_DS),
            CanonicalColumn("COL_INTERVAL_YM", GLUENT_TYPE_INTERVAL_YM),
            CanonicalColumn("COL_BOOLEAN", GLUENT_TYPE_BOOLEAN),
        ]

    def column_assertions(self, test_column, expected_column):
        """Compares attributes in the test_column with populated attributes in expected_column
        The obvious flaw being, what if we want to find attributes that are None, a future
        enhancement perhaps...
        """

        def check(test_column, expected_column, attribute_name):
            msg = "%s.%s mismatch: %s != %s" % (
                test_column.name,
                attribute_name,
                getattr(test_column, attribute_name),
                getattr(expected_column, attribute_name),
            )
            self.assertEqual(
                getattr(test_column, attribute_name),
                getattr(expected_column, attribute_name),
                msg,
            )

        def check_if_not_none(test_column, expected_column, attribute_name):
            if getattr(expected_column, attribute_name, None) is not None:
                check(test_column, expected_column, attribute_name)

        check(test_column, expected_column, "data_type")
        check_if_not_none(test_column, expected_column, "char_semantics")
        check_if_not_none(test_column, expected_column, "char_length")
        check_if_not_none(test_column, expected_column, "data_length")
        check_if_not_none(test_column, expected_column, "data_precision")
        check_if_not_none(test_column, expected_column, "data_scale")
        check_if_not_none(test_column, expected_column, "nullable")

    def validate_source_vs_expected_columns(self, source_columns, expected_columns):
        assert len(source_columns) == len(expected_columns), "%s != %s" % (
            len(source_columns),
            len(expected_columns),
        )
        source_names = [_.name for _ in source_columns]
        expected_names = [_.name for _ in expected_columns]
        assert source_names == expected_names, "Column name mismatch: %s != %s" % (
            source_names,
            expected_names,
        )


class TestOracleDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_BQ_ENV)
        messages = OffloadMessages()
        self.test_table_object = OracleSourceTable(
            "no_user", "no_table", self.options, messages, do_not_connect=True
        )

    def _get_rdbms_source_columns(self):
        return [
            # CHAR(100 BYTE)
            OracleColumn(
                "COL_CHAR",
                ORACLE_TYPE_CHAR,
                char_length=100,
                data_length=100,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            # NCHAR(100 CHAR)
            OracleColumn(
                "COL_NCHAR",
                ORACLE_TYPE_NCHAR,
                char_length=100,
                data_length=200,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            # VARCHAR2(1100 BYTE)
            OracleColumn(
                "COL_VARCHAR2",
                ORACLE_TYPE_VARCHAR2,
                char_length=1100,
                data_length=1100,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            # NVARCHAR2(1100 CHAR)
            OracleColumn(
                "COL_NVARCHAR2",
                ORACLE_TYPE_NVARCHAR2,
                char_length=1100,
                data_length=2200,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            # LOBS
            OracleColumn("COL_LONG", ORACLE_TYPE_LONG),
            OracleColumn("COL_CLOB", ORACLE_TYPE_CLOB, data_length=4000),
            OracleColumn("COL_NCLOB", ORACLE_TYPE_NCLOB, data_length=4000),
            OracleColumn("COL_RAW", ORACLE_TYPE_RAW, data_length=10),
            OracleColumn("COL_LONG_RAW", ORACLE_TYPE_LONG_RAW),
            OracleColumn("COL_BLOB", ORACLE_TYPE_BLOB, data_length=4000),
            # NUMBER(2)
            OracleColumn(
                "COL_NUMBER_2",
                ORACLE_TYPE_NUMBER,
                char_length=0,
                data_length=22,
                data_precision=2,
                data_scale=0,
            ),
            # NUMBER(4)
            OracleColumn(
                "COL_NUMBER_4",
                ORACLE_TYPE_NUMBER,
                char_length=0,
                data_length=22,
                data_precision=4,
                data_scale=0,
            ),
            # NUMBER(9)
            OracleColumn(
                "COL_NUMBER_9",
                ORACLE_TYPE_NUMBER,
                char_length=0,
                data_length=22,
                data_precision=9,
                data_scale=0,
            ),
            # NUMBER(18)
            OracleColumn(
                "COL_NUMBER_18",
                ORACLE_TYPE_NUMBER,
                char_length=0,
                data_length=22,
                data_precision=18,
                data_scale=0,
            ),
            # NUMBER(38)
            OracleColumn(
                "COL_NUMBER_38",
                ORACLE_TYPE_NUMBER,
                char_length=0,
                data_length=22,
                data_precision=38,
                data_scale=0,
            ),
            # NUMBER
            OracleColumn(
                "COL_NUMBER_NO_P_S", ORACLE_TYPE_NUMBER, char_length=0, data_length=22
            ),
            # NUMBER(10,2)
            OracleColumn(
                "COL_NUMBER_10_2",
                ORACLE_TYPE_NUMBER,
                char_length=0,
                data_length=22,
                data_precision=10,
                data_scale=2,
            ),
            # FLOAT - in offload_source_table we blank out precision for FLOAT
            OracleColumn(
                "COL_NUMBER_FLOAT",
                ORACLE_TYPE_FLOAT,
                data_length=22,
                data_precision=None,
            ),
            # Real numbers
            OracleColumn("COL_FLOAT", ORACLE_TYPE_BINARY_FLOAT, data_length=4),
            OracleColumn("COL_DOUBLE", ORACLE_TYPE_BINARY_DOUBLE, data_length=8),
            # Datetime
            OracleColumn("COL_DATE", ORACLE_TYPE_DATE, data_length=7),
            OracleColumn(
                "COL_TIMESTAMP_6", ORACLE_TYPE_TIMESTAMP, data_length=11, data_scale=6
            ),
            OracleColumn(
                "COL_TIMESTAMP_3", ORACLE_TYPE_TIMESTAMP, data_length=11, data_scale=3
            ),
            OracleColumn(
                "COL_TIMESTAMP_TZ_6",
                ORACLE_TYPE_TIMESTAMP_TZ,
                data_length=13,
                data_scale=6,
            ),
            OracleColumn(
                "COL_TIMESTAMP_TZ_9",
                ORACLE_TYPE_TIMESTAMP_TZ,
                data_length=13,
                data_scale=9,
            ),
            # INTERVAL DAY(9) TO SECOND(9)
            OracleColumn(
                "COL_INTERVAL_DS",
                ORACLE_TYPE_INTERVAL_DS,
                data_length=11,
                data_precision=9,
                data_scale=9,
            ),
            # INTERVAL YEAR(9) TO MONTH
            OracleColumn(
                "COL_INTERVAL_YM",
                ORACLE_TYPE_INTERVAL_YM,
                data_length=5,
                data_precision=9,
                data_scale=0,
            ),
        ]

    ###########################################################
    # RDBMS -> Formatted data type
    ###########################################################

    def test_oracle_format_data_type(self):
        for source_column in self._get_rdbms_source_columns():
            # Not testing the output but at least testing no exceptions
            self.assertIsNotNone(source_column.format_data_type())
            # Check string "None" is not in the formatted type
            self.assertNotIn(
                "None", source_column.format_data_type(), source_column.name
            )

    ###########################################################
    # RDBMS -> Canonical
    ###########################################################

    def test_oracle_to_canonical(self):
        source_columns = self._get_rdbms_source_columns()
        # Expected outcomes of source_columns when converted to canonical
        expected_columns = [
            CanonicalColumn("COL_CHAR", GLUENT_TYPE_FIXED_STRING, data_length=100),
            CanonicalColumn("COL_NCHAR", GLUENT_TYPE_FIXED_STRING, data_length=200),
            CanonicalColumn(
                "COL_VARCHAR2", GLUENT_TYPE_VARIABLE_STRING, data_length=1100
            ),
            CanonicalColumn(
                "COL_NVARCHAR2", GLUENT_TYPE_VARIABLE_STRING, data_length=2200
            ),
            CanonicalColumn("COL_LONG", GLUENT_TYPE_LARGE_STRING),
            CanonicalColumn("COL_CLOB", GLUENT_TYPE_LARGE_STRING),
            CanonicalColumn("COL_NCLOB", GLUENT_TYPE_LARGE_STRING),
            CanonicalColumn("COL_RAW", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_LONG_RAW", GLUENT_TYPE_LARGE_BINARY),
            CanonicalColumn("COL_BLOB", GLUENT_TYPE_LARGE_BINARY),
            CanonicalColumn("COL_NUMBER_2", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_NUMBER_4", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_NUMBER_9", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_NUMBER_18", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_NUMBER_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn("COL_NUMBER_NO_P_S", GLUENT_TYPE_DECIMAL),
            CanonicalColumn(
                "COL_NUMBER_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn(
                "COL_NUMBER_FLOAT",
                GLUENT_TYPE_DECIMAL,
                data_precision=None,
                data_scale=None,
            ),
            CanonicalColumn("COL_FLOAT", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_TIMESTAMP_6", GLUENT_TYPE_TIMESTAMP, data_scale=6),
            CanonicalColumn("COL_TIMESTAMP_3", GLUENT_TYPE_TIMESTAMP, data_scale=3),
            CanonicalColumn(
                "COL_TIMESTAMP_TZ_6", GLUENT_TYPE_TIMESTAMP_TZ, data_scale=6
            ),
            CanonicalColumn(
                "COL_TIMESTAMP_TZ_9", GLUENT_TYPE_TIMESTAMP_TZ, data_scale=9
            ),
            CanonicalColumn("COL_INTERVAL_DS", GLUENT_TYPE_INTERVAL_DS),
            CanonicalColumn("COL_INTERVAL_YM", GLUENT_TYPE_INTERVAL_YM),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            canonical_column = self.test_table_object.to_canonical_column(source_column)
            self.column_assertions(canonical_column, expected_column)

    ###########################################################
    # Canonical -> RDBMS
    ###########################################################

    def test_canonical_to_oracle(self):
        # No Bool support for Oracle
        canonical_columns = [
            _ for _ in self.canonical_columns() if _.data_type != GLUENT_TYPE_BOOLEAN
        ]
        # Expected outcomes of source_columns when converted to RDBMS
        expected_columns = [
            OracleColumn("COL_FIXED_STR", ORACLE_TYPE_CHAR, data_length=10),
            OracleColumn("COL_FIXED_STR_2000", ORACLE_TYPE_CHAR, data_length=2000),
            OracleColumn("COL_FIXED_STR_2000_C", ORACLE_TYPE_CHAR, char_length=2000),
            OracleColumn("COL_FIXED_STR_2001", ORACLE_TYPE_CLOB),
            OracleColumn("COL_FIXED_STR_2001_C", ORACLE_TYPE_CLOB),
            OracleColumn("COL_FIXED_STR_1000_U", ORACLE_TYPE_NCHAR, char_length=1000),
            OracleColumn("COL_FIXED_STR_1001_U", ORACLE_TYPE_NCLOB),
            OracleColumn("COL_LARGE_STR", ORACLE_TYPE_CLOB),
            OracleColumn("COL_VARIABLE_STR", ORACLE_TYPE_VARCHAR2, data_length=4000),
            OracleColumn(
                "COL_VARIABLE_STR_4000", ORACLE_TYPE_VARCHAR2, data_length=4000
            ),
            OracleColumn(
                "COL_VARIABLE_STR_4000_C", ORACLE_TYPE_VARCHAR2, char_length=4000
            ),
            OracleColumn("COL_VARIABLE_STR_4001", ORACLE_TYPE_CLOB),
            OracleColumn("COL_VARIABLE_STR_4001_C", ORACLE_TYPE_CLOB),
            OracleColumn(
                "COL_VARIABLE_STR_2000_U", ORACLE_TYPE_NVARCHAR2, char_length=2000
            ),
            OracleColumn("COL_VARIABLE_STR_2001_U", ORACLE_TYPE_NCLOB),
            OracleColumn("COL_BINARY", ORACLE_TYPE_RAW),
            OracleColumn("COL_BINARY_2000", ORACLE_TYPE_RAW, data_length=2000),
            OracleColumn("COL_BINARY_2001", ORACLE_TYPE_BLOB),
            OracleColumn("COL_LARGE_BINARY", ORACLE_TYPE_BLOB),
            OracleColumn("COL_INT_1", ORACLE_TYPE_NUMBER, data_scale=0),
            OracleColumn("COL_INT_2", ORACLE_TYPE_NUMBER, data_scale=0),
            OracleColumn("COL_INT_4", ORACLE_TYPE_NUMBER, data_scale=0),
            OracleColumn("COL_INT_8", ORACLE_TYPE_NUMBER, data_scale=0),
            OracleColumn("COL_INT_38", ORACLE_TYPE_NUMBER, data_scale=0),
            OracleColumn("COL_DEC_NO_P_S", ORACLE_TYPE_NUMBER),
            OracleColumn(
                "COL_DEC_10_2", ORACLE_TYPE_NUMBER, data_precision=10, data_scale=2
            ),
            OracleColumn("COL_FLOAT", ORACLE_TYPE_BINARY_FLOAT),
            OracleColumn("COL_DOUBLE", ORACLE_TYPE_BINARY_DOUBLE),
            OracleColumn("COL_DATE", ORACLE_TYPE_DATE),
            OracleColumn("COL_TIME", ORACLE_TYPE_VARCHAR2, data_length=18),
            OracleColumn("COL_TIMESTAMP", ORACLE_TYPE_TIMESTAMP),
            OracleColumn("COL_TIMESTAMP_TZ", ORACLE_TYPE_TIMESTAMP_TZ),
            OracleColumn("COL_INTERVAL_DS", ORACLE_TYPE_INTERVAL_DS),
            OracleColumn("COL_INTERVAL_YM", ORACLE_TYPE_INTERVAL_YM),
        ]
        self.validate_source_vs_expected_columns(canonical_columns, expected_columns)

        for source_column in canonical_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_table_object.from_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)

    ###########################################################
    # Backend -> RDBMS end-to-end tests
    ###########################################################

    #
    # BigQuery
    def test_bigquery_to_oracle(self):
        backend_api = backend_api_factory(
            DBTYPE_BIGQUERY,
            self.options,
            OffloadMessages(),
            do_not_connect=True,
        )
        backend_columns = [
            BigQueryColumn(name="COL_STRING", data_type=BIGQUERY_TYPE_STRING),
            BigQueryColumn(name="COL_STRING_U", data_type=BIGQUERY_TYPE_STRING),
            BigQueryColumn(name="COL_BYTES", data_type=BIGQUERY_TYPE_BYTES),
        ]
        expected_columns = [
            OracleColumn(
                name="COL_STRING", data_type=ORACLE_TYPE_VARCHAR2, data_length=4000
            ),
            OracleColumn(
                name="COL_STRING_U", data_type=ORACLE_TYPE_NVARCHAR2, char_length=2000
            ),
            OracleColumn(name="COL_BYTES", data_type=ORACLE_TYPE_RAW, data_length=2000),
        ]
        for backend_column, expected_column in zip(backend_columns, expected_columns):
            if backend_column.data_type == BIGQUERY_TYPE_STRING:
                # On BigQuery all strings are CHAR semantics
                backend_column.char_semantics = CANONICAL_CHAR_SEMANTICS_CHAR
            canonical_column = backend_api.to_canonical_column(backend_column)
            self._stealth_unicode_char_semantics(canonical_column)
            frontend_column = self.test_table_object.from_canonical_column(
                canonical_column
            )
            self.column_assertions(frontend_column, expected_column)

    #
    # Hive
    def test_hive_to_oracle(self):
        backend_api = backend_api_factory(
            DBTYPE_HIVE,
            self.options,
            OffloadMessages(),
            do_not_connect=True,
        )
        backend_columns = [
            HadoopColumn(
                name="COL_CHAR_255", data_type=HADOOP_TYPE_CHAR, data_length=255
            ),
            HadoopColumn(
                name="COL_CHAR_255_U", data_type=HADOOP_TYPE_CHAR, data_length=255
            ),
            HadoopColumn(
                name="COL_VARCHAR_4000", data_type=HADOOP_TYPE_VARCHAR, data_length=4000
            ),
            HadoopColumn(
                name="COL_VARCHAR_4001", data_type=HADOOP_TYPE_VARCHAR, data_length=4001
            ),
            HadoopColumn(
                name="COL_VARCHAR_2000_U",
                data_type=HADOOP_TYPE_VARCHAR,
                data_length=2000,
            ),
            HadoopColumn(
                name="COL_VARCHAR_2001_U",
                data_type=HADOOP_TYPE_VARCHAR,
                data_length=2001,
            ),
            HadoopColumn(name="COL_BYTES", data_type=HADOOP_TYPE_BINARY),
        ]
        expected_columns = [
            OracleColumn(
                name="COL_CHAR_255", data_type=ORACLE_TYPE_CHAR, data_length=255
            ),
            OracleColumn(
                name="COL_CHAR_255_U", data_type=ORACLE_TYPE_NCHAR, char_length=255
            ),
            OracleColumn(
                name="COL_VARCHAR_4000",
                data_type=ORACLE_TYPE_VARCHAR2,
                data_length=4000,
            ),
            OracleColumn(name="COL_VARCHAR_4001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_VARCHAR_2000_U",
                data_type=ORACLE_TYPE_NVARCHAR2,
                char_length=2000,
            ),
            OracleColumn(name="COL_VARCHAR_2001_U", data_type=ORACLE_TYPE_NCLOB),
            OracleColumn(name="COL_BYTES", data_type=ORACLE_TYPE_RAW, data_length=2000),
        ]
        for backend_column, expected_column in zip(backend_columns, expected_columns):
            canonical_column = backend_api.to_canonical_column(backend_column)
            self._stealth_unicode_char_semantics(canonical_column)
            frontend_column = self.test_table_object.from_canonical_column(
                canonical_column
            )
            self.column_assertions(frontend_column, expected_column)

    #
    # Impala
    def test_impala_to_oracle(self):
        backend_api = backend_api_factory(
            DBTYPE_IMPALA,
            self.options,
            OffloadMessages(),
            do_not_connect=True,
        )
        backend_columns = [
            HadoopColumn(
                name="COL_CHAR_255", data_type=HADOOP_TYPE_CHAR, data_length=255
            ),
            HadoopColumn(
                name="COL_CHAR_255_U", data_type=HADOOP_TYPE_CHAR, data_length=255
            ),
            HadoopColumn(
                name="COL_VARCHAR_4000", data_type=HADOOP_TYPE_VARCHAR, data_length=4000
            ),
            HadoopColumn(
                name="COL_VARCHAR_4001", data_type=HADOOP_TYPE_VARCHAR, data_length=4001
            ),
            HadoopColumn(
                name="COL_VARCHAR_2000_U",
                data_type=HADOOP_TYPE_VARCHAR,
                data_length=2000,
            ),
            HadoopColumn(
                name="COL_VARCHAR_2001_U",
                data_type=HADOOP_TYPE_VARCHAR,
                data_length=2001,
            ),
        ]
        expected_columns = [
            OracleColumn(
                name="COL_CHAR_255", data_type=ORACLE_TYPE_CHAR, data_length=255
            ),
            OracleColumn(
                name="COL_CHAR_255_U", data_type=ORACLE_TYPE_NCHAR, char_length=255
            ),
            OracleColumn(
                name="COL_VARCHAR_4000",
                data_type=ORACLE_TYPE_VARCHAR2,
                data_length=4000,
            ),
            OracleColumn(name="COL_VARCHAR_4001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_VARCHAR_2000_U",
                data_type=ORACLE_TYPE_NVARCHAR2,
                char_length=2000,
            ),
            OracleColumn(name="COL_VARCHAR_2001_U", data_type=ORACLE_TYPE_NCLOB),
        ]
        for backend_column, expected_column in zip(backend_columns, expected_columns):
            canonical_column = backend_api.to_canonical_column(backend_column)
            self._stealth_unicode_char_semantics(canonical_column)
            frontend_column = self.test_table_object.from_canonical_column(
                canonical_column
            )
            self.column_assertions(frontend_column, expected_column)

    #
    # Snowflake
    def test_snowflake_to_oracle(self):
        backend_api = backend_api_factory(
            DBTYPE_SNOWFLAKE,
            self.options,
            OffloadMessages(),
            do_not_connect=True,
        )
        backend_columns = [
            SnowflakeColumn(
                name="COL_TEXT_4000",
                data_type=SNOWFLAKE_TYPE_TEXT,
                char_length=4000,
                data_length=16000,
            ),
            SnowflakeColumn(
                name="COL_TEXT_4001",
                data_type=SNOWFLAKE_TYPE_TEXT,
                char_length=4001,
                data_length=16004,
            ),
            SnowflakeColumn(
                name="COL_TEXT_2000_U",
                data_type=SNOWFLAKE_TYPE_TEXT,
                char_length=2000,
                data_length=8000,
            ),
            SnowflakeColumn(
                name="COL_TEXT_2001_U",
                data_type=SNOWFLAKE_TYPE_TEXT,
                char_length=2001,
                data_length=8004,
            ),
            SnowflakeColumn(
                name="COL_BINARY_2000",
                data_type=SNOWFLAKE_TYPE_BINARY,
                data_length=2000,
            ),
            SnowflakeColumn(
                name="COL_BINARY_2001",
                data_type=SNOWFLAKE_TYPE_BINARY,
                data_length=2001,
            ),
        ]
        expected_columns = [
            OracleColumn(
                name="COL_VARCHAR_4000",
                data_type=ORACLE_TYPE_VARCHAR2,
                char_length=4000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            OracleColumn(name="COL_VARCHAR_4001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_VARCHAR_2000_U",
                data_type=ORACLE_TYPE_NVARCHAR2,
                char_length=2000,
            ),
            OracleColumn(name="COL_VARCHAR_2001_U", data_type=ORACLE_TYPE_NCLOB),
            OracleColumn(
                name="COL_BINARY_2000", data_type=ORACLE_TYPE_RAW, data_length=2000
            ),
            OracleColumn(name="COL_BINARY_2001", data_type=ORACLE_TYPE_BLOB),
        ]
        for backend_column, expected_column in zip(backend_columns, expected_columns):
            if backend_column.data_type == SNOWFLAKE_TYPE_TEXT:
                # On Snowflake all strings are CHAR semantics
                backend_column.char_semantics = CANONICAL_CHAR_SEMANTICS_CHAR
            canonical_column = backend_api.to_canonical_column(backend_column)
            self._stealth_unicode_char_semantics(canonical_column)
            frontend_column = self.test_table_object.from_canonical_column(
                canonical_column
            )
            self.column_assertions(frontend_column, expected_column)

    #
    # Synapse
    def test_synapse_to_oracle(self):
        backend_api = backend_api_factory(
            DBTYPE_SYNAPSE,
            self.options,
            OffloadMessages(),
            do_not_connect=True,
        )
        backend_columns = [
            SynapseColumn(
                name="COL_CHAR_2000",
                data_type=SYNAPSE_TYPE_CHAR,
                char_length=2000,
                data_length=2000,
            ),
            SynapseColumn(
                name="COL_CHAR_2001",
                data_type=SYNAPSE_TYPE_CHAR,
                char_length=2001,
                data_length=2001,
            ),
            SynapseColumn(
                name="COL_CHAR_1000_U",
                data_type=SYNAPSE_TYPE_CHAR,
                char_length=1000,
                data_length=1000,
            ),
            SynapseColumn(
                name="COL_CHAR_1001_U",
                data_type=SYNAPSE_TYPE_CHAR,
                char_length=1001,
                data_length=1001,
            ),
            SynapseColumn(
                name="COL_NCHAR_2000",
                data_type=SYNAPSE_TYPE_NCHAR,
                char_length=2000,
                data_length=4000,
            ),
            SynapseColumn(
                name="COL_NCHAR_2001",
                data_type=SYNAPSE_TYPE_NCHAR,
                char_length=2001,
                data_length=4002,
            ),
            SynapseColumn(
                name="COL_NCHAR_1000_U",
                data_type=SYNAPSE_TYPE_NCHAR,
                char_length=1000,
                data_length=2000,
            ),
            SynapseColumn(
                name="COL_NCHAR_1001_U",
                data_type=SYNAPSE_TYPE_NCHAR,
                char_length=1001,
                data_length=2002,
            ),
            SynapseColumn(
                name="COL_VARCHAR_4000",
                data_type=SYNAPSE_TYPE_VARCHAR,
                char_length=4000,
                data_length=4000,
            ),
            SynapseColumn(
                name="COL_VARCHAR_4001",
                data_type=SYNAPSE_TYPE_VARCHAR,
                char_length=4001,
                data_length=4001,
            ),
            SynapseColumn(
                name="COL_VARCHAR_2000_U",
                data_type=SYNAPSE_TYPE_VARCHAR,
                char_length=2000,
                data_length=2000,
            ),
            SynapseColumn(
                name="COL_VARCHAR_2001_U",
                data_type=SYNAPSE_TYPE_VARCHAR,
                char_length=2001,
                data_length=2001,
            ),
            SynapseColumn(
                name="COL_NVARCHAR_4000",
                data_type=SYNAPSE_TYPE_NVARCHAR,
                char_length=4000,
                data_length=8000,
            ),
            SynapseColumn(
                name="COL_NVARCHAR_4001",
                data_type=SYNAPSE_TYPE_NVARCHAR,
                char_length=4001,
                data_length=8001,
            ),
            SynapseColumn(
                name="COL_NVARCHAR_2000_U",
                data_type=SYNAPSE_TYPE_NVARCHAR,
                char_length=2000,
                data_length=4000,
            ),
            SynapseColumn(
                name="COL_NVARCHAR_2001_U",
                data_type=SYNAPSE_TYPE_NVARCHAR,
                char_length=2001,
                data_length=4001,
            ),
            SynapseColumn(
                name="COL_BINARY_2000", data_type=SYNAPSE_TYPE_BINARY, data_length=2000
            ),
            SynapseColumn(
                name="COL_BINARY_2001", data_type=SYNAPSE_TYPE_BINARY, data_length=2001
            ),
            SynapseColumn(
                name="COL_VARBINARY_2000",
                data_type=SYNAPSE_TYPE_VARBINARY,
                data_length=2000,
            ),
            SynapseColumn(
                name="COL_VARBINARY_2001",
                data_type=SYNAPSE_TYPE_VARBINARY,
                data_length=2001,
            ),
        ]
        expected_columns = [
            OracleColumn(
                name="COL_CHAR_2000",
                data_type=ORACLE_TYPE_CHAR,
                data_length=2000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            OracleColumn(name="COL_CHAR_2001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_CHAR_1000_U",
                data_type=ORACLE_TYPE_NCHAR,
                char_length=1000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            OracleColumn(name="COL_CHAR_1001_U", data_type=ORACLE_TYPE_NCLOB),
            OracleColumn(
                name="COL_NCHAR_2000",
                data_type=ORACLE_TYPE_CHAR,
                char_length=2000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            OracleColumn(name="COL_NCHAR_2001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_NCHAR_1000_U",
                data_type=ORACLE_TYPE_NCHAR,
                char_length=1000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            OracleColumn(name="COL_NCHAR_1001_U", data_type=ORACLE_TYPE_NCLOB),
            OracleColumn(
                name="COL_VARCHAR_4000",
                data_type=ORACLE_TYPE_VARCHAR2,
                data_length=4000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            OracleColumn(name="COL_VARCHAR_4001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_VARCHAR_2000_U",
                data_type=ORACLE_TYPE_NVARCHAR2,
                char_length=2000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
            ),
            OracleColumn(name="COL_VARCHAR_2001_U", data_type=ORACLE_TYPE_NCLOB),
            OracleColumn(
                name="COL_NVARCHAR_4000",
                data_type=ORACLE_TYPE_VARCHAR2,
                char_length=4000,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            OracleColumn(name="COL_NVARCHAR_4001", data_type=ORACLE_TYPE_CLOB),
            OracleColumn(
                name="COL_NVARCHAR_2000_U",
                data_type=ORACLE_TYPE_NVARCHAR2,
                char_length=2000,
            ),
            OracleColumn(name="COL_NVARCHAR_2001_U", data_type=ORACLE_TYPE_NCLOB),
            OracleColumn(
                name="COL_BINARY_2000", data_type=ORACLE_TYPE_RAW, data_length=2000
            ),
            OracleColumn(name="COL_BINARY_2001", data_type=ORACLE_TYPE_BLOB),
            OracleColumn(
                name="COL_VARBINARY_2000", data_type=ORACLE_TYPE_RAW, data_length=2000
            ),
            OracleColumn(name="COL_VARBINARY_2001", data_type=ORACLE_TYPE_BLOB),
        ]
        for backend_column, expected_column in zip(backend_columns, expected_columns):
            canonical_column = backend_api.to_canonical_column(backend_column)
            self._stealth_unicode_char_semantics(canonical_column)
            frontend_column = self.test_table_object.from_canonical_column(
                canonical_column
            )
            self.column_assertions(frontend_column, expected_column)


class TestMSSQLDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_MSSQL_ENV)
        messages = OffloadMessages()
        self.test_table_object = MSSQLSourceTable(
            "no_user", "no_table", self.options, messages, do_not_connect=True
        )

    def _get_rdbms_source_columns(self):
        return [
            MSSQLColumn("COL_BIT", MSSQL_TYPE_BIT, data_length=1),
            MSSQLColumn("COL_CHAR", MSSQL_TYPE_CHAR, char_length=100, data_length=100),
            MSSQLColumn(
                "COL_NCHAR", MSSQL_TYPE_NCHAR, char_length=100, data_length=200
            ),
            MSSQLColumn(
                "COL_VARCHAR", MSSQL_TYPE_VARCHAR, char_length=1100, data_length=1100
            ),
            MSSQLColumn(
                "COL_NVARCHAR", MSSQL_TYPE_NVARCHAR, char_length=1100, data_length=2200
            ),
            MSSQLColumn(
                "COL_UNIQ_ID",
                MSSQL_TYPE_UNIQUEIDENTIFIER,
                char_length=16,
                data_length=16,
            ),
            MSSQLColumn("COL_TEXT", MSSQL_TYPE_TEXT),
            MSSQLColumn("COL_NTEXT", MSSQL_TYPE_NTEXT),
            MSSQLColumn("COL_BINARY", MSSQL_TYPE_BINARY, data_length=500),
            MSSQLColumn("COL_VARBINARY", MSSQL_TYPE_VARBINARY, data_length=500),
            MSSQLColumn("COL_IMAGE", MSSQL_TYPE_IMAGE),
            MSSQLColumn(
                "COL_DECIMAL_2",
                MSSQL_TYPE_DECIMAL,
                char_length=0,
                data_length=5,
                data_precision=2,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_DECIMAL_4",
                MSSQL_TYPE_DECIMAL,
                char_length=0,
                data_length=5,
                data_precision=4,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_DECIMAL_9",
                MSSQL_TYPE_DECIMAL,
                char_length=0,
                data_length=5,
                data_precision=9,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_DECIMAL_18",
                MSSQL_TYPE_DECIMAL,
                char_length=0,
                data_length=9,
                data_precision=18,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_DECIMAL_38",
                MSSQL_TYPE_DECIMAL,
                char_length=0,
                data_length=17,
                data_precision=38,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_DECIMAL_10_2",
                MSSQL_TYPE_DECIMAL,
                char_length=0,
                data_length=9,
                data_precision=10,
                data_scale=2,
            ),
            MSSQLColumn(
                "COL_NUMERIC_2",
                MSSQL_TYPE_NUMERIC,
                char_length=0,
                data_length=5,
                data_precision=2,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_NUMERIC_4",
                MSSQL_TYPE_NUMERIC,
                char_length=0,
                data_length=5,
                data_precision=4,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_NUMERIC_9",
                MSSQL_TYPE_NUMERIC,
                char_length=0,
                data_length=5,
                data_precision=9,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_NUMERIC_18",
                MSSQL_TYPE_NUMERIC,
                char_length=0,
                data_length=9,
                data_precision=18,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_NUMERIC_38",
                MSSQL_TYPE_NUMERIC,
                char_length=0,
                data_length=17,
                data_precision=38,
                data_scale=0,
            ),
            MSSQLColumn(
                "COL_NUMERIC_10_2",
                MSSQL_TYPE_NUMERIC,
                char_length=0,
                data_length=9,
                data_precision=10,
                data_scale=2,
            ),
            MSSQLColumn("COL_TINYINT", MSSQL_TYPE_TINYINT, data_length=1),
            MSSQLColumn("COL_SMALLINT", MSSQL_TYPE_SMALLINT, data_length=2),
            MSSQLColumn("COL_INT", MSSQL_TYPE_INT, data_length=4),
            MSSQLColumn("COL_BIGINT", MSSQL_TYPE_BIGINT, data_length=8),
            MSSQLColumn("COL_REAL", MSSQL_TYPE_REAL, data_length=4),
            MSSQLColumn("COL_FLOAT_4", MSSQL_TYPE_FLOAT, data_length=4),
            MSSQLColumn("COL_FLOAT_8", MSSQL_TYPE_FLOAT, data_length=8),
            MSSQLColumn(
                "COL_MONEY",
                MSSQL_TYPE_MONEY,
                char_length=0,
                data_length=8,
                data_scale=4,
            ),
            MSSQLColumn(
                "COL_SMALLMONEY",
                MSSQL_TYPE_SMALLMONEY,
                char_length=0,
                data_length=4,
                data_scale=2,
            ),
            MSSQLColumn("COL_DATE", MSSQL_TYPE_DATE, data_length=3),
            MSSQLColumn("COL_TIME", MSSQL_TYPE_TIME, data_length=5),
            MSSQLColumn("COL_DATETIME", MSSQL_TYPE_DATETIME, data_length=8),
            MSSQLColumn("COL_DATETIME2", MSSQL_TYPE_DATETIME2, data_length=8),
            MSSQLColumn("COL_SMALLDATETIME", MSSQL_TYPE_SMALLDATETIME, data_length=4),
        ]

    ###########################################################
    # RDBMS -> Formatted data type
    ###########################################################

    def test_mssql_format_data_type(self):
        for source_column in self._get_rdbms_source_columns():
            try:
                # Not testing the output but at least testing no unexpected exceptions
                self.assertIsNotNone(source_column.format_data_type())
                # Check string "None" is not in the formatted type
                self.assertNotIn(
                    "None", source_column.format_data_type(), source_column.name
                )
            except NotImplementedError:
                pass

    ###########################################################
    # RDBMS -> Canonical
    ###########################################################

    def test_mssql_to_canonical(self):
        source_columns = self._get_rdbms_source_columns()
        # Expected outcomes of source_columns when converted to canonical
        expected_columns = [
            CanonicalColumn("COL_BIT", GLUENT_TYPE_BOOLEAN),
            CanonicalColumn("COL_CHAR", GLUENT_TYPE_FIXED_STRING, data_length=100),
            CanonicalColumn("COL_NCHAR", GLUENT_TYPE_FIXED_STRING, data_length=200),
            CanonicalColumn(
                "COL_VARCHAR", GLUENT_TYPE_VARIABLE_STRING, data_length=1100
            ),
            CanonicalColumn(
                "COL_NVARCHAR", GLUENT_TYPE_VARIABLE_STRING, data_length=2200
            ),
            CanonicalColumn("COL_UNIQ_ID", GLUENT_TYPE_VARIABLE_STRING, data_length=16),
            CanonicalColumn("COL_TEXT", GLUENT_TYPE_LARGE_STRING),
            CanonicalColumn("COL_NTEXT", GLUENT_TYPE_LARGE_STRING),
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY, data_length=500),
            CanonicalColumn("COL_VARBINARY", GLUENT_TYPE_BINARY, data_length=500),
            CanonicalColumn("COL_IMAGE", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_DECIMAL_2", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_DECIMAL_4", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_DECIMAL_9", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_DECIMAL_18", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_DECIMAL_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn(
                "COL_DECIMAL_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_NUMERIC_2", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_NUMERIC_4", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_NUMERIC_9", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_NUMERIC_18", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_NUMERIC_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn(
                "COL_NUMERIC_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_TINYINT", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_SMALLINT", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_BIGINT", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_REAL", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_FLOAT_4", GLUENT_TYPE_DOUBLE),
            # CanonicalColumn('COL_FLOAT_4', GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_FLOAT_8", GLUENT_TYPE_DOUBLE),
            # CanonicalColumn('COL_MONEY', GLUENT_TYPE_DECIMAL, data_precision=19, data_scale=4),
            CanonicalColumn("COL_MONEY", GLUENT_TYPE_DECIMAL, data_scale=4),
            # CanonicalColumn('COL_SMALLMONEY', GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2),
            CanonicalColumn("COL_SMALLMONEY", GLUENT_TYPE_DECIMAL, data_scale=2),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_DATETIME", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_DATETIME2", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_SMALLDATETIME", GLUENT_TYPE_TIMESTAMP),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            canonical_column = self.test_table_object.to_canonical_column(source_column)
            self.column_assertions(canonical_column, expected_column)

    ###########################################################
    # Canonical -> RDBMS
    ###########################################################

    # Present to MSSQL is not currently supported
    # def test_canonical_to_mssql(self):
    #     canonical_columns = self.canonical_columns()
    #     # Expected outcomes of source_columns when converted to RDBMS
    #     expected_columns = [
    #         MSSQLColumn('COL_FIXED_STR', MSSQL_TYPE_CHAR, data_length=10),
    #         ...
    #         MSSQLColumn('COL_BOOLEAN', MSSQL_TYPE_BIT),
    #     ]
    #     self.validate_source_vs_expected_columns(canonical_columns, expected_columns)

    #     for source_column in canonical_columns:
    #         expected_column = match_table_column(source_column.name, expected_columns)
    #         assert expected_column
    #         new_column = self.test_table_object.from_canonical_column(source_column)
    #         self.column_assertions(new_column, expected_column)


class TestNetezzaDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_NETEZZA_ENV)
        messages = OffloadMessages()
        self.test_table_object = NetezzaSourceTable(
            "no_user", "no_table", self.options, messages, do_not_connect=True
        )

    def _get_rdbms_source_columns(self):
        return [
            NetezzaColumn("COL_BOOLEAN", NETEZZA_TYPE_BOOLEAN),
            NetezzaColumn(
                "COL_CHARACTER",
                NETEZZA_TYPE_CHARACTER,
                char_length=100,
                data_length=100,
            ),
            NetezzaColumn(
                "COL_NCHARACTER",
                NETEZZA_TYPE_NATIONAL_CHARACTER,
                char_length=100,
                data_length=200,
            ),
            NetezzaColumn(
                "COL_CHARACTERV",
                NETEZZA_TYPE_CHARACTER_VARYING,
                char_length=100,
                data_length=100,
            ),
            NetezzaColumn(
                "COL_NCHARACTERV",
                NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING,
                char_length=100,
                data_length=200,
            ),
            NetezzaColumn("COL_BINARY", NETEZZA_TYPE_BINARY_VARYING, data_length=5000),
            NetezzaColumn("COL_ST_GEOMETRY", NETEZZA_TYPE_ST_GEOMETRY, data_length=50),
            NetezzaColumn(
                "COL_NUMERIC_2",
                NETEZZA_TYPE_NUMERIC,
                char_length=0,
                data_length=4,
                data_precision=2,
                data_scale=0,
            ),
            NetezzaColumn(
                "COL_NUMERIC_4",
                NETEZZA_TYPE_NUMERIC,
                char_length=0,
                data_length=4,
                data_precision=4,
                data_scale=0,
            ),
            NetezzaColumn(
                "COL_NUMERIC_9",
                NETEZZA_TYPE_NUMERIC,
                char_length=0,
                data_length=4,
                data_precision=9,
                data_scale=0,
            ),
            NetezzaColumn(
                "COL_NUMERIC_18",
                NETEZZA_TYPE_NUMERIC,
                char_length=0,
                data_length=8,
                data_precision=18,
                data_scale=0,
            ),
            NetezzaColumn(
                "COL_NUMERIC_38",
                NETEZZA_TYPE_NUMERIC,
                char_length=0,
                data_length=16,
                data_precision=38,
                data_scale=0,
            ),
            NetezzaColumn(
                "COL_NUMERIC_10_2",
                NETEZZA_TYPE_NUMERIC,
                char_length=0,
                data_length=16,
                data_precision=10,
                data_scale=2,
            ),
            NetezzaColumn("COL_BYTEINT", NETEZZA_TYPE_BYTEINT, data_length=1),
            NetezzaColumn("COL_SMALLINT", NETEZZA_TYPE_SMALLINT, data_length=2),
            NetezzaColumn("COL_INTEGER", NETEZZA_TYPE_INTEGER, data_length=4),
            NetezzaColumn("COL_BIGINT", NETEZZA_TYPE_BIGINT, data_length=8),
            NetezzaColumn("COL_REAL", NETEZZA_TYPE_REAL, data_length=4),
            NetezzaColumn("COL_DOUBLE", NETEZZA_TYPE_DOUBLE_PRECISION, data_length=8),
            NetezzaColumn("COL_DATE", NETEZZA_TYPE_DATE),
            NetezzaColumn("COL_TIME", NETEZZA_TYPE_TIME),
            NetezzaColumn("COL_TIMESTAMP", NETEZZA_TYPE_TIMESTAMP),
            # NetezzaColumn('COL_INTERVAL', NETEZZA_TYPE_INTERVAL),
            NetezzaColumn("COL_TIME_TZ", NETEZZA_TYPE_TIME_WITH_TIME_ZONE),
        ]

    ###########################################################
    # RDBMS -> Formatted data type
    ###########################################################

    def test_netezza_format_data_type(self):
        for source_column in self._get_rdbms_source_columns():
            try:
                # Not testing the output but at least testing no unexpected exceptions
                self.assertIsNotNone(source_column.format_data_type())
                # Check string "None" is not in the formatted type
                self.assertNotIn(
                    "None", source_column.format_data_type(), source_column.name
                )
            except NotImplementedError:
                pass

    ###########################################################
    # RDBMS -> Canonical
    ###########################################################

    def test_netezza_to_canonical(self):
        source_columns = self._get_rdbms_source_columns()
        # Expected outcomes of source_columns when converted to canonical
        expected_columns = [
            CanonicalColumn("COL_BOOLEAN", GLUENT_TYPE_BOOLEAN),
            CanonicalColumn("COL_CHARACTER", GLUENT_TYPE_FIXED_STRING, data_length=100),
            CanonicalColumn(
                "COL_NCHARACTER", GLUENT_TYPE_FIXED_STRING, data_length=200
            ),
            CanonicalColumn(
                "COL_CHARACTERV", GLUENT_TYPE_VARIABLE_STRING, data_length=100
            ),
            CanonicalColumn(
                "COL_NCHARACTERV", GLUENT_TYPE_VARIABLE_STRING, data_length=200
            ),
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY, data_length=5000),
            CanonicalColumn("COL_ST_GEOMETRY", GLUENT_TYPE_BINARY, data_length=50),
            CanonicalColumn("COL_NUMERIC_2", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_NUMERIC_4", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_NUMERIC_9", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_NUMERIC_18", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_NUMERIC_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn(
                "COL_NUMERIC_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_BYTEINT", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_SMALLINT", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INTEGER", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_BIGINT", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_REAL", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_TIMESTAMP", GLUENT_TYPE_TIMESTAMP),
            # CanonicalColumn('COL_INTERVAL', GLUENT_TYPE_INTERVAL),
            CanonicalColumn("COL_TIME_TZ", GLUENT_TYPE_TIMESTAMP_TZ),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            canonical_column = self.test_table_object.to_canonical_column(source_column)
            self.column_assertions(canonical_column, expected_column)

    ###########################################################
    # Canonical -> RDBMS
    ###########################################################

    # Present to Netezza is not currently supported
    # def test_canonical_to_netezza(self):
    #     canonical_columns = self.canonical_columns()
    #     # Expected outcomes of source_columns when converted to RDBMS
    #     expected_columns = [
    #         MSSQLColumn('COL_FIXED_STR', NETEZZA_TYPE_CHARACTER, data_length=10),
    #         ...
    #         MSSQLColumn('COL_BOOLEAN', NETEZZA_TYPE_BOLLEAN),
    #     ]
    #     self.validate_source_vs_expected_columns(canonical_columns, expected_columns)

    #     for source_column in canonical_columns:
    #         expected_column = match_table_column(source_column.name, expected_columns)
    #         assert expected_column
    #         new_column = self.test_table_object.from_canonical_column(source_column)
    #         self.column_assertions(new_column, expected_column)


class TestBackendHiveDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_HIVE_ENV)
        messages = OffloadMessages()
        self.test_api = backend_api_factory(
            self.options.target, self.options, messages, do_not_connect=True
        )

    def _get_hive_source_columns(self):
        return [
            HadoopColumn("COL_FIXED_STR", HADOOP_TYPE_CHAR, data_length=10),
            HadoopColumn("COL_VARIABLE_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR2", HADOOP_TYPE_VARCHAR, data_length=10),
            HadoopColumn("COL_BINARY", HADOOP_TYPE_BINARY),
            HadoopColumn("COL_INT_1", HADOOP_TYPE_TINYINT),
            HadoopColumn("COL_INT_2", HADOOP_TYPE_SMALLINT),
            HadoopColumn("COL_INT_4", HADOOP_TYPE_INT),
            HadoopColumn("COL_INT_8", HADOOP_TYPE_BIGINT),
            HadoopColumn(
                "COL_INT_P_2", HADOOP_TYPE_DECIMAL, data_precision=2, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_4", HADOOP_TYPE_DECIMAL, data_precision=4, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_9", HADOOP_TYPE_DECIMAL, data_precision=9, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_18", HADOOP_TYPE_DECIMAL, data_precision=18, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_38", HADOOP_TYPE_DECIMAL, data_precision=38, data_scale=0
            ),
            HadoopColumn("COL_DEC_NO_P_S", HADOOP_TYPE_DECIMAL),
            HadoopColumn(
                "COL_DEC_10_2", HADOOP_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            HadoopColumn("COL_FLOAT", HADOOP_TYPE_FLOAT),
            HadoopColumn("COL_DOUBLE", HADOOP_TYPE_DOUBLE),
            HadoopColumn("COL_DOUBLE_PRECISION", HADOOP_TYPE_DOUBLE_PRECISION),
            HadoopColumn("COL_DATE", HADOOP_TYPE_DATE),
            HadoopColumn("COL_TIMESTAMP", HADOOP_TYPE_TIMESTAMP),
            HadoopColumn("COL_INTERVAL_DS", HADOOP_TYPE_INTERVAL_DS),
            HadoopColumn("COL_INTERVAL_YM", HADOOP_TYPE_INTERVAL_YM),
            HadoopColumn("COL_BOOLEAN", HADOOP_TYPE_BOOLEAN),
        ]

    ###########################################################
    # Hive -> Formatted data type
    ###########################################################

    def test_hive_format_data_type(self):
        for source_column in self._get_hive_source_columns():
            # Not testing the output but at least testing no exceptions
            self.assertIsNotNone(source_column.format_data_type())
            # Check string "None" is not in the formatted type
            self.assertNotIn(
                "None", source_column.format_data_type(), source_column.name
            )

    ###########################################################
    # Hive -> Canonical
    ###########################################################

    def test_hive_to_canonical(self):
        source_columns = self._get_hive_source_columns()
        expected_columns = [
            CanonicalColumn("COL_FIXED_STR", GLUENT_TYPE_FIXED_STRING, data_length=10),
            CanonicalColumn("COL_VARIABLE_STR", GLUENT_TYPE_VARIABLE_STRING),
            CanonicalColumn(
                "COL_VARIABLE_STR2", GLUENT_TYPE_VARIABLE_STRING, data_length=10
            ),
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_INT_1", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_2", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_4", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_8", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_P_2", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_P_4", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_P_9", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_P_18", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_P_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn("COL_DEC_NO_P_S", GLUENT_TYPE_DECIMAL),
            CanonicalColumn(
                "COL_DEC_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_FLOAT", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DOUBLE_PRECISION", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIMESTAMP", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_INTERVAL_DS", GLUENT_TYPE_INTERVAL_DS),
            CanonicalColumn("COL_INTERVAL_YM", GLUENT_TYPE_INTERVAL_YM),
            CanonicalColumn("COL_BOOLEAN", GLUENT_TYPE_BOOLEAN),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.to_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)

    ###########################################################
    # Canonical -> Hive
    ###########################################################

    def test_canonical_to_hive(self):
        # Expected outcomes of source_columns when converted to Hive
        expected_columns = [
            HadoopColumn("COL_FIXED_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2000", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2000_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2001", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2001_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_1000_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_1001_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_LARGE_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4000", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4000_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4001", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4001_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_2000_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_2001_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_BINARY", HADOOP_TYPE_BINARY),
            HadoopColumn("COL_BINARY_2000", HADOOP_TYPE_BINARY),
            HadoopColumn("COL_BINARY_2001", HADOOP_TYPE_BINARY),
            HadoopColumn("COL_LARGE_BINARY", HADOOP_TYPE_BINARY),
            HadoopColumn("COL_INT_1", HADOOP_TYPE_BIGINT),
            HadoopColumn("COL_INT_2", HADOOP_TYPE_BIGINT),
            HadoopColumn("COL_INT_4", HADOOP_TYPE_BIGINT),
            HadoopColumn("COL_INT_8", HADOOP_TYPE_BIGINT),
            HadoopColumn(
                "COL_INT_38", HADOOP_TYPE_DECIMAL, data_precision=38, data_scale=0
            ),
            HadoopColumn("COL_DEC_NO_P_S", HADOOP_TYPE_DECIMAL),
            # With decimal_padding_digits=2 and UDF alignment 10,2 increases to 18,4
            HadoopColumn(
                "COL_DEC_10_2", HADOOP_TYPE_DECIMAL, data_precision=18, data_scale=4
            ),
            HadoopColumn("COL_FLOAT", HADOOP_TYPE_FLOAT),
            HadoopColumn("COL_DOUBLE", HADOOP_TYPE_DOUBLE),
            HadoopColumn("COL_DATE", HADOOP_TYPE_DATE),
            HadoopColumn("COL_TIME", HADOOP_TYPE_STRING),
            HadoopColumn("COL_TIMESTAMP", HADOOP_TYPE_TIMESTAMP),
            HadoopColumn("COL_TIMESTAMP_TZ", HADOOP_TYPE_TIMESTAMP),
            HadoopColumn("COL_INTERVAL_DS", HADOOP_TYPE_STRING),
            HadoopColumn("COL_INTERVAL_YM", HADOOP_TYPE_STRING),
            HadoopColumn("COL_BOOLEAN", HADOOP_TYPE_BOOLEAN),
        ]
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.from_canonical_column(
                source_column, decimal_padding_digits=2
            )
            self.column_assertions(new_column, expected_column)


class TestBackendImpalaDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_IMPALA_ENV)
        messages = OffloadMessages()
        self.test_api = backend_api_factory(
            self.options.target, self.options, messages, do_not_connect=True
        )

    def _get_impala_source_columns(self):
        return [
            HadoopColumn("COL_FIXED_STR", HADOOP_TYPE_CHAR, data_length=10),
            HadoopColumn("COL_VARIABLE_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR2", HADOOP_TYPE_VARCHAR, data_length=10),
            HadoopColumn("COL_INT_1", HADOOP_TYPE_TINYINT),
            HadoopColumn("COL_INT_2", HADOOP_TYPE_SMALLINT),
            HadoopColumn("COL_INT_4", HADOOP_TYPE_INT),
            HadoopColumn("COL_INT_8", HADOOP_TYPE_BIGINT),
            HadoopColumn(
                "COL_INT_P_2", HADOOP_TYPE_DECIMAL, data_precision=2, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_4", HADOOP_TYPE_DECIMAL, data_precision=4, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_9", HADOOP_TYPE_DECIMAL, data_precision=9, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_18", HADOOP_TYPE_DECIMAL, data_precision=18, data_scale=0
            ),
            HadoopColumn(
                "COL_INT_P_38", HADOOP_TYPE_DECIMAL, data_precision=38, data_scale=0
            ),
            HadoopColumn("COL_DEC_NO_P_S", HADOOP_TYPE_DECIMAL),
            HadoopColumn(
                "COL_DEC_10_2", HADOOP_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            HadoopColumn("COL_FLOAT", HADOOP_TYPE_FLOAT),
            HadoopColumn("COL_DOUBLE", HADOOP_TYPE_DOUBLE),
            HadoopColumn("COL_REAL", HADOOP_TYPE_REAL),
            HadoopColumn("COL_DATE", HADOOP_TYPE_DATE),
            HadoopColumn("COL_TIMESTAMP", HADOOP_TYPE_TIMESTAMP),
        ]

    ###########################################################
    # Impala -> Formatted data type
    ###########################################################

    def test_impala_format_data_type(self):
        for source_column in self._get_impala_source_columns():
            # Not testing the output but at least testing no exceptions
            self.assertIsNotNone(source_column.format_data_type())
            # Check string "None" is not in the formatted type
            self.assertNotIn(
                "None", source_column.format_data_type(), source_column.name
            )

    ###########################################################
    # Impala -> Canonical
    ###########################################################

    def test_impala_to_canonical(self):
        source_columns = self._get_impala_source_columns()
        expected_columns = [
            CanonicalColumn("COL_FIXED_STR", GLUENT_TYPE_FIXED_STRING, data_length=10),
            CanonicalColumn("COL_VARIABLE_STR", GLUENT_TYPE_VARIABLE_STRING),
            CanonicalColumn(
                "COL_VARIABLE_STR2", GLUENT_TYPE_VARIABLE_STRING, data_length=10
            ),
            CanonicalColumn("COL_INT_1", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_2", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_4", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_8", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_P_2", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_P_4", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_P_9", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_P_18", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_P_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn("COL_DEC_NO_P_S", GLUENT_TYPE_DECIMAL),
            CanonicalColumn(
                "COL_DEC_10_2", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=2
            ),
            CanonicalColumn("COL_FLOAT", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_REAL", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIMESTAMP", GLUENT_TYPE_TIMESTAMP),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.to_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)

    ###########################################################
    # Canonical -> Impala
    ###########################################################

    def test_canonical_to_impala(self):
        # Expected outcomes of source_columns when converted to Hive
        expected_columns = [
            HadoopColumn("COL_FIXED_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2000", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2000_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2001", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_2001_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_1000_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_FIXED_STR_1001_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_LARGE_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4000", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4000_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4001", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_4001_C", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_2000_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_VARIABLE_STR_2001_U", HADOOP_TYPE_STRING),
            HadoopColumn("COL_BINARY", HADOOP_TYPE_STRING),
            HadoopColumn("COL_BINARY_2000", HADOOP_TYPE_STRING),
            HadoopColumn("COL_BINARY_2001", HADOOP_TYPE_STRING),
            HadoopColumn("COL_LARGE_BINARY", HADOOP_TYPE_STRING),
            HadoopColumn("COL_INT_1", HADOOP_TYPE_BIGINT),
            HadoopColumn("COL_INT_2", HADOOP_TYPE_BIGINT),
            HadoopColumn("COL_INT_4", HADOOP_TYPE_BIGINT),
            HadoopColumn("COL_INT_8", HADOOP_TYPE_BIGINT),
            HadoopColumn(
                "COL_INT_38", HADOOP_TYPE_DECIMAL, data_precision=38, data_scale=0
            ),
            HadoopColumn("COL_DEC_NO_P_S", HADOOP_TYPE_DECIMAL),
            # With decimal_padding_digits=2 and UDF alignment 10,2 increases to 18,4
            HadoopColumn(
                "COL_DEC_10_2", HADOOP_TYPE_DECIMAL, data_precision=18, data_scale=4
            ),
            HadoopColumn("COL_FLOAT", HADOOP_TYPE_FLOAT),
            HadoopColumn("COL_DOUBLE", HADOOP_TYPE_DOUBLE),
            HadoopColumn("COL_DATE", HADOOP_TYPE_TIMESTAMP),
            HadoopColumn("COL_TIME", HADOOP_TYPE_STRING),
            HadoopColumn("COL_TIMESTAMP", HADOOP_TYPE_TIMESTAMP),
            HadoopColumn("COL_TIMESTAMP_TZ", HADOOP_TYPE_TIMESTAMP),
            HadoopColumn("COL_INTERVAL_DS", HADOOP_TYPE_STRING),
            HadoopColumn("COL_INTERVAL_YM", HADOOP_TYPE_STRING),
            HadoopColumn("COL_BOOLEAN", HADOOP_TYPE_BOOLEAN),
        ]
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.from_canonical_column(
                source_column, decimal_padding_digits=2
            )
            self.column_assertions(new_column, expected_column)


class TestBackendBigQueryDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_BQ_ENV)
        messages = OffloadMessages()
        self.test_api = backend_api_factory(
            self.options.target, self.options, messages, do_not_connect=True
        )

    def _get_bigquery_source_columns(self):
        return [
            BigQueryColumn("COL_VARIABLE_STR", BIGQUERY_TYPE_STRING),
            BigQueryColumn("COL_BINARY", BIGQUERY_TYPE_BYTES),
            BigQueryColumn("COL_INT_8", BIGQUERY_TYPE_INT64),
            BigQueryColumn("COL_DEC_NO_P_S", BIGQUERY_TYPE_BIGNUMERIC),
            BigQueryColumn("COL_DEC_10_2", BIGQUERY_TYPE_NUMERIC),
            BigQueryColumn("COL_DOUBLE", BIGQUERY_TYPE_FLOAT64),
            BigQueryColumn("COL_DATE", BIGQUERY_TYPE_DATE),
            BigQueryColumn("COL_DATETIME", BIGQUERY_TYPE_DATETIME),
            BigQueryColumn("COL_TIME", BIGQUERY_TYPE_TIME),
            BigQueryColumn("COL_TIMESTAMP", BIGQUERY_TYPE_TIMESTAMP),
            BigQueryColumn("COL_BOOLEAN", BIGQUERY_TYPE_BOOLEAN),
        ]

    ###########################################################
    # BigQuery -> Formatted data type
    ###########################################################

    def test_bigquery_format_data_type(self):
        for source_column in self._get_bigquery_source_columns():
            # Not testing the output but at least testing no exceptions
            self.assertIsNotNone(source_column.format_data_type())
            # Check string "None" is not in the formatted type
            self.assertNotIn(
                "None", source_column.format_data_type(), source_column.name
            )

    ###########################################################
    # BigQuery -> Canonical
    ###########################################################

    def test_bigquery_to_canonical(self):
        source_columns = self._get_bigquery_source_columns()
        expected_columns = [
            CanonicalColumn("COL_VARIABLE_STR", GLUENT_TYPE_VARIABLE_STRING),
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_INT_8", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn(
                "COL_DEC_NO_P_S", GLUENT_TYPE_DECIMAL, data_precision=76, data_scale=38
            ),
            CanonicalColumn(
                "COL_DEC_10_2", GLUENT_TYPE_DECIMAL, data_precision=38, data_scale=9
            ),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_DATETIME", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_TIMESTAMP", GLUENT_TYPE_TIMESTAMP_TZ),
            CanonicalColumn("COL_BOOLEAN", GLUENT_TYPE_BOOLEAN),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.to_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)

    ###########################################################
    # Canonical -> BigQuery
    ###########################################################

    def test_canonical_to_bigquery(self):
        # Expected outcomes of source_columns when converted to BigQuery
        expected_columns = [
            BigQueryColumn("COL_FIXED_STR", BIGQUERY_TYPE_STRING),
            BigQueryColumn(
                "COL_FIXED_STR_2000", BIGQUERY_TYPE_STRING, char_length=2000
            ),
            BigQueryColumn(
                "COL_FIXED_STR_2000_C", BIGQUERY_TYPE_STRING, char_length=2000
            ),
            BigQueryColumn(
                "COL_FIXED_STR_2001", BIGQUERY_TYPE_STRING, char_length=2001
            ),
            BigQueryColumn(
                "COL_FIXED_STR_2001_C", BIGQUERY_TYPE_STRING, char_length=2001
            ),
            BigQueryColumn(
                "COL_FIXED_STR_1000_U", BIGQUERY_TYPE_STRING, char_length=1000
            ),
            BigQueryColumn(
                "COL_FIXED_STR_1001_U", BIGQUERY_TYPE_STRING, char_length=1001
            ),
            BigQueryColumn("COL_LARGE_STR", BIGQUERY_TYPE_STRING),
            BigQueryColumn("COL_VARIABLE_STR", BIGQUERY_TYPE_STRING),
            BigQueryColumn(
                "COL_VARIABLE_STR_4000", BIGQUERY_TYPE_STRING, char_length=4000
            ),
            BigQueryColumn(
                "COL_VARIABLE_STR_4000_C", BIGQUERY_TYPE_STRING, char_length=4000
            ),
            BigQueryColumn(
                "COL_VARIABLE_STR_4001", BIGQUERY_TYPE_STRING, char_length=4001
            ),
            BigQueryColumn(
                "COL_VARIABLE_STR_4001_C", BIGQUERY_TYPE_STRING, char_length=4001
            ),
            BigQueryColumn(
                "COL_VARIABLE_STR_2000_U", BIGQUERY_TYPE_STRING, char_length=2000
            ),
            BigQueryColumn(
                "COL_VARIABLE_STR_2001_U", BIGQUERY_TYPE_STRING, char_length=2001
            ),
            BigQueryColumn("COL_BINARY", BIGQUERY_TYPE_BYTES),
            BigQueryColumn("COL_BINARY_2000", BIGQUERY_TYPE_BYTES, data_length=2000),
            BigQueryColumn("COL_BINARY_2001", BIGQUERY_TYPE_BYTES, data_length=2001),
            BigQueryColumn("COL_LARGE_BINARY", BIGQUERY_TYPE_BYTES),
            BigQueryColumn("COL_INT_1", BIGQUERY_TYPE_INT64),
            BigQueryColumn("COL_INT_2", BIGQUERY_TYPE_INT64),
            BigQueryColumn("COL_INT_4", BIGQUERY_TYPE_INT64),
            BigQueryColumn("COL_INT_8", BIGQUERY_TYPE_INT64),
            BigQueryColumn(
                "COL_INT_38", BIGQUERY_TYPE_BIGNUMERIC, data_precision=38, data_scale=0
            ),
            BigQueryColumn("COL_DEC_NO_P_S", BIGQUERY_TYPE_BIGNUMERIC),
            BigQueryColumn(
                "COL_DEC_10_2", BIGQUERY_TYPE_NUMERIC, data_precision=10, data_scale=2
            ),
            BigQueryColumn("COL_FLOAT", BIGQUERY_TYPE_FLOAT64),
            BigQueryColumn("COL_DOUBLE", BIGQUERY_TYPE_FLOAT64),
            BigQueryColumn("COL_DATE", BIGQUERY_TYPE_DATE),
            BigQueryColumn("COL_TIME", BIGQUERY_TYPE_TIME),
            BigQueryColumn("COL_TIMESTAMP", BIGQUERY_TYPE_DATETIME),
            BigQueryColumn("COL_TIMESTAMP_TZ", BIGQUERY_TYPE_TIMESTAMP),
            BigQueryColumn("COL_INTERVAL_DS", BIGQUERY_TYPE_STRING),
            BigQueryColumn("COL_INTERVAL_YM", BIGQUERY_TYPE_STRING),
            BigQueryColumn("COL_BOOLEAN", BIGQUERY_TYPE_BOOLEAN),
        ]
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            if source_column.data_type == GLUENT_TYPE_FLOAT:
                # No default mapping for 32bit float
                continue
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.from_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)


class TestAvroDataTypeMappings(TestDataTypeMappings):
    def _run_avro_column_tests(self, target, staging_file):
        # Expected outcomes of self.canonical_columns() when converted to staging types
        expected_columns = self.get_expected_staging_columns(target)
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            staging_column = staging_file.from_canonical_column(source_column)
            self.column_assertions(staging_column, expected_column)

    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_HIVE_ENV)

    def get_expected_staging_columns(self, target):
        stage_binary_as_string = bool(target in [DBTYPE_IMPALA, DBTYPE_SNOWFLAKE])
        return [
            StagingAvroColumn("COL_FIXED_STR", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FIXED_STR_2000", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FIXED_STR_2000_C", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FIXED_STR_2001", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FIXED_STR_2001_C", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FIXED_STR_1000_U", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FIXED_STR_1001_U", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_LARGE_STR", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR_4000", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR_4000_C", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR_4001", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR_4001_C", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR_2000_U", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_VARIABLE_STR_2001_U", AVRO_TYPE_STRING),
            StagingAvroColumn(
                "COL_BINARY",
                AVRO_TYPE_STRING if stage_binary_as_string else AVRO_TYPE_BYTES,
            ),
            StagingAvroColumn(
                "COL_BINARY_2000",
                AVRO_TYPE_STRING if stage_binary_as_string else AVRO_TYPE_BYTES,
            ),
            StagingAvroColumn(
                "COL_BINARY_2001",
                AVRO_TYPE_STRING if stage_binary_as_string else AVRO_TYPE_BYTES,
            ),
            StagingAvroColumn(
                "COL_LARGE_BINARY",
                AVRO_TYPE_STRING if stage_binary_as_string else AVRO_TYPE_BYTES,
            ),
            StagingAvroColumn("COL_INT_1", AVRO_TYPE_INT),
            StagingAvroColumn("COL_INT_2", AVRO_TYPE_INT),
            StagingAvroColumn("COL_INT_4", AVRO_TYPE_INT),
            StagingAvroColumn("COL_INT_8", AVRO_TYPE_LONG),
            StagingAvroColumn("COL_INT_38", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_DEC_NO_P_S", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_DEC_10_2", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_FLOAT", AVRO_TYPE_FLOAT),
            StagingAvroColumn("COL_DOUBLE", AVRO_TYPE_DOUBLE),
            StagingAvroColumn("COL_DATE", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_TIME", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_TIMESTAMP", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_TIMESTAMP_TZ", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_INTERVAL_DS", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_INTERVAL_YM", AVRO_TYPE_STRING),
            StagingAvroColumn("COL_BOOLEAN", AVRO_TYPE_BOOLEAN),
        ]

    ###########################################################
    # Canonical -> Staging Avro on Hive
    ###########################################################

    def test_canonical_to_avro_on_hive(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_HIVE
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_AVRO,
            self.canonical_columns(),
            self.options,
            False,
            messages,
            dry_run=True,
        )
        self._run_avro_column_tests(self.options.target, staging_file)

    ###########################################################
    # Canonical -> Staging Avro on Impala
    ###########################################################

    def test_canonical_to_avro_on_impala(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_IMPALA
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_AVRO,
            self.canonical_columns(),
            self.options,
            False,
            messages,
            dry_run=True,
        )
        self._run_avro_column_tests(self.options.target, staging_file)

    ###########################################################
    # Canonical -> Staging Avro for BigQuery
    ###########################################################

    def test_canonical_to_avro_for_bigquery(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_BIGQUERY
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_AVRO,
            self.canonical_columns(),
            self.options,
            False,
            messages,
            dry_run=True,
        )
        self._run_avro_column_tests(self.options.target, staging_file)

    ###########################################################
    # Canonical -> Staging Avro for Snowflake
    ###########################################################

    def test_canonical_to_avro_for_snowflake(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_SNOWFLAKE
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_AVRO,
            self.canonical_columns(),
            self.options,
            True,
            messages,
            dry_run=True,
        )
        self._run_avro_column_tests(self.options.target, staging_file)


class TestParquetDataTypeMappings(TestDataTypeMappings):
    def _run_parquet_column_tests(self, target, staging_file):
        # Expected outcomes of self.canonical_columns() when converted to staging types
        expected_columns = self.get_expected_staging_columns(target)
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            staging_column = staging_file.from_canonical_column(source_column)
            self.column_assertions(staging_column, expected_column)

    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_SNOWFLAKE_ENV)

    def get_expected_staging_columns(self, target):
        stage_binary_as_string = bool(target in [DBTYPE_SNOWFLAKE])
        return [
            StagingParquetColumn("COL_FIXED_STR", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FIXED_STR_2000", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FIXED_STR_2000_C", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FIXED_STR_2001", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FIXED_STR_2001_C", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FIXED_STR_1000_U", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FIXED_STR_1001_U", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_LARGE_STR", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR_4000", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR_4000_C", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR_4001", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR_4001_C", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR_2000_U", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_VARIABLE_STR_2001_U", PARQUET_TYPE_STRING),
            StagingParquetColumn(
                "COL_BINARY",
                PARQUET_TYPE_STRING if stage_binary_as_string else PARQUET_TYPE_BINARY,
            ),
            StagingParquetColumn(
                "COL_BINARY_2000",
                PARQUET_TYPE_STRING if stage_binary_as_string else PARQUET_TYPE_BINARY,
            ),
            StagingParquetColumn(
                "COL_BINARY_2001",
                PARQUET_TYPE_STRING if stage_binary_as_string else PARQUET_TYPE_BINARY,
            ),
            StagingParquetColumn(
                "COL_LARGE_BINARY",
                PARQUET_TYPE_STRING if stage_binary_as_string else PARQUET_TYPE_BINARY,
            ),
            StagingParquetColumn("COL_INT_1", PARQUET_TYPE_INT32),
            StagingParquetColumn("COL_INT_2", PARQUET_TYPE_INT32),
            StagingParquetColumn("COL_INT_4", PARQUET_TYPE_INT32),
            StagingParquetColumn("COL_INT_8", PARQUET_TYPE_INT64),
            StagingParquetColumn("COL_INT_38", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_DEC_NO_P_S", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_DEC_10_2", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_FLOAT", PARQUET_TYPE_FLOAT),
            StagingParquetColumn("COL_DOUBLE", PARQUET_TYPE_DOUBLE),
            StagingParquetColumn("COL_DATE", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_TIME", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_TIMESTAMP", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_TIMESTAMP_TZ", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_INTERVAL_DS", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_INTERVAL_YM", PARQUET_TYPE_STRING),
            StagingParquetColumn("COL_BOOLEAN", PARQUET_TYPE_BOOLEAN),
        ]

    ###########################################################
    # Canonical -> Staging Parquet for BigQuery
    ###########################################################

    def test_canonical_to_parquet_for_bigquery(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_BIGQUERY
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_PARQUET,
            self.canonical_columns(),
            self.options,
            False,
            messages,
            dry_run=True,
        )
        self._run_parquet_column_tests(self.options.target, staging_file)

    ###########################################################
    # Canonical -> Staging Parquet for Snowflake
    ###########################################################

    def test_canonical_to_parquet_for_snowflake(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_SNOWFLAKE
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_PARQUET,
            self.canonical_columns(),
            self.options,
            True,
            messages,
            dry_run=True,
        )
        self._run_parquet_column_tests(self.options.target, staging_file)

    ###########################################################
    # Canonical -> Staging Parquet for Synapse
    ###########################################################

    def test_canonical_to_parquet_for_synapse(self):
        messages = OffloadMessages()
        self.options.target = DBTYPE_SYNAPSE
        staging_file = staging_file_factory(
            "no_db",
            "no_table",
            FILE_STORAGE_FORMAT_PARQUET,
            self.canonical_columns(),
            self.options,
            False,
            messages,
            dry_run=True,
        )
        self._run_parquet_column_tests(self.options.target, staging_file)


class TestBackendSnowflakeDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_SNOWFLAKE_ENV)
        messages = OffloadMessages()
        self.test_api = backend_api_factory(
            self.options.target, self.options, messages, do_not_connect=True
        )

    def _get_snowflake_source_columns(self):
        return [
            SnowflakeColumn("COL_BINARY", SNOWFLAKE_TYPE_BINARY),
            SnowflakeColumn("COL_BINARY_2000", SNOWFLAKE_TYPE_BINARY, data_length=2000),
            SnowflakeColumn("COL_BINARY_2001", SNOWFLAKE_TYPE_BINARY, data_length=2001),
            SnowflakeColumn("COL_BOOLEAN", SNOWFLAKE_TYPE_BOOLEAN),
            SnowflakeColumn("COL_VARIABLE_STR", SNOWFLAKE_TYPE_TEXT),
            SnowflakeColumn(
                "COL_INT_1", SNOWFLAKE_TYPE_NUMBER, data_precision=2, data_scale=0
            ),
            SnowflakeColumn(
                "COL_INT_2", SNOWFLAKE_TYPE_NUMBER, data_precision=4, data_scale=0
            ),
            SnowflakeColumn(
                "COL_INT_4", SNOWFLAKE_TYPE_NUMBER, data_precision=9, data_scale=0
            ),
            SnowflakeColumn(
                "COL_INT_8", SNOWFLAKE_TYPE_NUMBER, data_precision=18, data_scale=0
            ),
            SnowflakeColumn(
                "COL_INT_38", SNOWFLAKE_TYPE_NUMBER, data_precision=38, data_scale=0
            ),
            SnowflakeColumn(
                "COL_DEC_20_10", SNOWFLAKE_TYPE_NUMBER, data_precision=20, data_scale=10
            ),
            SnowflakeColumn("COL_DOUBLE", SNOWFLAKE_TYPE_FLOAT),
            SnowflakeColumn("COL_DATE", SNOWFLAKE_TYPE_DATE),
            SnowflakeColumn("COL_TIME", SNOWFLAKE_TYPE_TIME),
            SnowflakeColumn("COL_TIMESTAMP", SNOWFLAKE_TYPE_TIMESTAMP_NTZ),
            SnowflakeColumn("COL_TIMESTAMP_TZ", SNOWFLAKE_TYPE_TIMESTAMP_TZ),
        ]

    ###########################################################
    # Snowflake -> Formatted data type
    ###########################################################

    def test_snowflake_format_data_type(self):
        for source_column in self._get_snowflake_source_columns():
            # Not testing the output but at least testing no exceptions
            self.assertIsNotNone(source_column.format_data_type())
            # Check string "None" is not in the formatted type
            self.assertNotIn(
                "None", source_column.format_data_type(), source_column.name
            )

    ###########################################################
    # Snowflake -> Canonical
    ###########################################################

    def test_snowflake_to_canonical(self):
        source_columns = self._get_snowflake_source_columns()
        expected_columns = [
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_BINARY_2000", GLUENT_TYPE_BINARY, data_length=2000),
            CanonicalColumn("COL_BINARY_2001", GLUENT_TYPE_BINARY, data_length=2001),
            CanonicalColumn("COL_BOOLEAN", GLUENT_TYPE_BOOLEAN),
            # All Snowflake strings are defined in CHARs
            CanonicalColumn(
                "COL_VARIABLE_STR",
                GLUENT_TYPE_VARIABLE_STRING,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn("COL_INT_1", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_2", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_4", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_8", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn(
                "COL_DEC_20_10", GLUENT_TYPE_DECIMAL, data_precision=20, data_scale=10
            ),
            CanonicalColumn("COL_DOUBLE", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_TIMESTAMP", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_TIMESTAMP_TZ", GLUENT_TYPE_TIMESTAMP_TZ),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.to_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)

    ###########################################################
    # Canonical -> Snowflake
    ###########################################################

    def test_canonical_to_snowflake(self):
        # Expected outcomes of source_columns when converted to Snowflake
        expected_columns = [
            SnowflakeColumn("COL_FIXED_STR", SNOWFLAKE_TYPE_TEXT),
            SnowflakeColumn(
                "COL_FIXED_STR_2000", SNOWFLAKE_TYPE_TEXT, char_length=2000
            ),
            SnowflakeColumn(
                "COL_FIXED_STR_2000_C", SNOWFLAKE_TYPE_TEXT, char_length=2000
            ),
            SnowflakeColumn(
                "COL_FIXED_STR_2001", SNOWFLAKE_TYPE_TEXT, char_length=2001
            ),
            SnowflakeColumn(
                "COL_FIXED_STR_2001_C", SNOWFLAKE_TYPE_TEXT, char_length=2001
            ),
            SnowflakeColumn(
                "COL_FIXED_STR_1000_U", SNOWFLAKE_TYPE_TEXT, char_length=1000
            ),
            SnowflakeColumn(
                "COL_FIXED_STR_1001_U", SNOWFLAKE_TYPE_TEXT, char_length=1001
            ),
            SnowflakeColumn("COL_LARGE_STR", SNOWFLAKE_TYPE_TEXT),
            SnowflakeColumn("COL_VARIABLE_STR", SNOWFLAKE_TYPE_TEXT),
            SnowflakeColumn(
                "COL_VARIABLE_STR_4000", SNOWFLAKE_TYPE_TEXT, char_length=4000
            ),
            SnowflakeColumn(
                "COL_VARIABLE_STR_4000_C", SNOWFLAKE_TYPE_TEXT, char_length=4000
            ),
            SnowflakeColumn(
                "COL_VARIABLE_STR_4001", SNOWFLAKE_TYPE_TEXT, char_length=4001
            ),
            SnowflakeColumn(
                "COL_VARIABLE_STR_4001_C", SNOWFLAKE_TYPE_TEXT, char_length=4001
            ),
            SnowflakeColumn(
                "COL_VARIABLE_STR_2000_U", SNOWFLAKE_TYPE_TEXT, char_length=2000
            ),
            SnowflakeColumn(
                "COL_VARIABLE_STR_2001_U", SNOWFLAKE_TYPE_TEXT, char_length=2001
            ),
            SnowflakeColumn("COL_BINARY", SNOWFLAKE_TYPE_BINARY),
            SnowflakeColumn("COL_BINARY_2000", SNOWFLAKE_TYPE_BINARY, data_length=2000),
            SnowflakeColumn("COL_BINARY_2001", SNOWFLAKE_TYPE_BINARY, data_length=2001),
            SnowflakeColumn("COL_LARGE_BINARY", SNOWFLAKE_TYPE_BINARY),
            SnowflakeColumn("COL_INT_1", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_INT_2", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_INT_4", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_INT_8", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_INT_38", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_DEC_NO_P_S", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_DEC_10_2", SNOWFLAKE_TYPE_NUMBER),
            SnowflakeColumn("COL_FLOAT", SNOWFLAKE_TYPE_FLOAT),
            SnowflakeColumn("COL_DOUBLE", SNOWFLAKE_TYPE_FLOAT),
            SnowflakeColumn("COL_DATE", SNOWFLAKE_TYPE_DATE),
            SnowflakeColumn("COL_TIME", SNOWFLAKE_TYPE_TIME),
            SnowflakeColumn("COL_TIMESTAMP", SNOWFLAKE_TYPE_TIMESTAMP_NTZ),
            SnowflakeColumn("COL_TIMESTAMP_TZ", SNOWFLAKE_TYPE_TIMESTAMP_TZ),
            SnowflakeColumn("COL_INTERVAL_DS", SNOWFLAKE_TYPE_TEXT),
            SnowflakeColumn("COL_INTERVAL_YM", SNOWFLAKE_TYPE_TEXT),
            SnowflakeColumn("COL_BOOLEAN", SNOWFLAKE_TYPE_BOOLEAN),
        ]
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            if source_column.data_type == GLUENT_TYPE_FLOAT:
                # No default mapping for 32bit float
                continue
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.from_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)


class TestBackendSynapseDataTypeMappings(TestDataTypeMappings):
    def setUp(self):
        self.options = build_mock_options(FAKE_ORACLE_SYNAPSE_ENV)
        messages = OffloadMessages()
        self.test_api = backend_api_factory(
            self.options.target, self.options, messages, do_not_connect=True
        )

    def _get_synapse_source_columns(self):
        return [
            SynapseColumn("COL_BIGINT", SYNAPSE_TYPE_BIGINT),
            SynapseColumn("COL_BINARY", SYNAPSE_TYPE_BINARY),
            SynapseColumn("COL_BINARY_2001", SYNAPSE_TYPE_BINARY, data_length=2001),
            SynapseColumn("COL_CHAR", SYNAPSE_TYPE_CHAR, data_length=3),
            SynapseColumn("COL_CHAR_1001", SYNAPSE_TYPE_CHAR, data_length=1001),
            SynapseColumn("COL_CHAR_2001", SYNAPSE_TYPE_CHAR, data_length=2001),
            SynapseColumn("COL_DATE", SYNAPSE_TYPE_DATE),
            SynapseColumn("COL_DATETIME", SYNAPSE_TYPE_DATETIME),
            SynapseColumn("COL_DATETIME2", SYNAPSE_TYPE_DATETIME2),
            SynapseColumn("COL_DATETIMEOFFSET", SYNAPSE_TYPE_DATETIMEOFFSET),
            SynapseColumn(
                "COL_INT_1", SYNAPSE_TYPE_NUMERIC, data_precision=2, data_scale=0
            ),
            SynapseColumn(
                "COL_INT_2", SYNAPSE_TYPE_NUMERIC, data_precision=4, data_scale=0
            ),
            SynapseColumn(
                "COL_INT_4", SYNAPSE_TYPE_NUMERIC, data_precision=9, data_scale=0
            ),
            SynapseColumn(
                "COL_INT_8", SYNAPSE_TYPE_NUMERIC, data_precision=18, data_scale=0
            ),
            SynapseColumn(
                "COL_INT_38", SYNAPSE_TYPE_NUMERIC, data_precision=38, data_scale=0
            ),
            SynapseColumn(
                "COL_DEC_20_10", SYNAPSE_TYPE_NUMERIC, data_precision=20, data_scale=10
            ),
            SynapseColumn("COL_FLOAT32", SYNAPSE_TYPE_REAL),
            SynapseColumn("COL_FLOAT64", SYNAPSE_TYPE_FLOAT),
            SynapseColumn("COL_INT", SYNAPSE_TYPE_INT),
            SynapseColumn("COL_MONEY", SYNAPSE_TYPE_MONEY),
            SynapseColumn(
                "COL_NCHAR", SYNAPSE_TYPE_NCHAR, char_length=3, data_length=6
            ),
            SynapseColumn(
                "COL_NCHAR_1001", SYNAPSE_TYPE_NCHAR, char_length=1001, data_length=2002
            ),
            SynapseColumn(
                "COL_NCHAR_2001", SYNAPSE_TYPE_NCHAR, char_length=2001, data_length=4002
            ),
            SynapseColumn(
                "COL_NVARCHAR", SYNAPSE_TYPE_NVARCHAR, char_length=30, data_length=60
            ),
            SynapseColumn(
                "COL_NVARCHAR_2001",
                SYNAPSE_TYPE_NVARCHAR,
                char_length=2001,
                data_length=4002,
            ),
            SynapseColumn(
                "COL_NVARCHAR_4001",
                SYNAPSE_TYPE_NVARCHAR,
                char_length=4001,
                data_length=8002,
            ),
            SynapseColumn("COL_SMALLDATETIME", SYNAPSE_TYPE_SMALLDATETIME),
            SynapseColumn("COL_SMALLINT", SYNAPSE_TYPE_SMALLINT),
            SynapseColumn("COL_SMALLMONEY", SYNAPSE_TYPE_SMALLMONEY),
            SynapseColumn("COL_TIME", SYNAPSE_TYPE_TIME),
            SynapseColumn("COL_TINYINT", SYNAPSE_TYPE_TINYINT),
            SynapseColumn("COL_UNIQUEIDENTIFIER", SYNAPSE_TYPE_UNIQUEIDENTIFIER),
            SynapseColumn("COL_VARBINARY", SYNAPSE_TYPE_VARBINARY, data_length=30),
            SynapseColumn(
                "COL_VARBINARY_2001", SYNAPSE_TYPE_VARBINARY, data_length=2001
            ),
            SynapseColumn("COL_VARCHAR", SYNAPSE_TYPE_VARCHAR, data_length=30),
            SynapseColumn("COL_VARCHAR_2001", SYNAPSE_TYPE_VARCHAR, data_length=2001),
            SynapseColumn("COL_VARCHAR_4001", SYNAPSE_TYPE_VARCHAR, data_length=4001),
        ]

    ###########################################################
    # Synapse -> Formatted data type
    ###########################################################

    def test_synapse_format_data_type(self):
        for source_column in self._get_synapse_source_columns():
            # Not testing the output but at least testing no exceptions
            self.assertIsNotNone(source_column.format_data_type())
            # Check string "None" is not in the formatted type
            self.assertNotIn(
                "None", source_column.format_data_type(), source_column.name
            )

    ###########################################################
    # Synapse -> Canonical
    ###########################################################

    def test_synapse_to_canonical(self):
        source_columns = self._get_synapse_source_columns()
        expected_columns = [
            CanonicalColumn("COL_BIGINT", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_BINARY", GLUENT_TYPE_BINARY),
            CanonicalColumn("COL_BINARY_2001", GLUENT_TYPE_BINARY, data_length=2001),
            CanonicalColumn(
                "COL_CHAR",
                GLUENT_TYPE_FIXED_STRING,
                data_length=3,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            CanonicalColumn(
                "COL_CHAR_1001",
                GLUENT_TYPE_FIXED_STRING,
                data_length=1001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            CanonicalColumn(
                "COL_CHAR_2001",
                GLUENT_TYPE_FIXED_STRING,
                data_length=2001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            CanonicalColumn("COL_DATE", GLUENT_TYPE_DATE),
            CanonicalColumn("COL_DATETIME", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_DATETIME2", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_DATETIMEOFFSET", GLUENT_TYPE_TIMESTAMP_TZ),
            CanonicalColumn("COL_INT_1", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_INT_2", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn("COL_INT_4", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn("COL_INT_8", GLUENT_TYPE_INTEGER_8),
            CanonicalColumn("COL_INT_38", GLUENT_TYPE_INTEGER_38),
            CanonicalColumn(
                "COL_DEC_20_10", GLUENT_TYPE_DECIMAL, data_precision=20, data_scale=10
            ),
            CanonicalColumn("COL_FLOAT32", GLUENT_TYPE_FLOAT),
            CanonicalColumn("COL_FLOAT64", GLUENT_TYPE_DOUBLE),
            CanonicalColumn("COL_INT", GLUENT_TYPE_INTEGER_4),
            CanonicalColumn(
                "COL_MONEY", GLUENT_TYPE_DECIMAL, data_precision=19, data_scale=4
            ),
            CanonicalColumn(
                "COL_NCHAR",
                GLUENT_TYPE_FIXED_STRING,
                char_length=3,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_NCHAR_1001",
                GLUENT_TYPE_FIXED_STRING,
                char_length=1001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_NCHAR_2001",
                GLUENT_TYPE_FIXED_STRING,
                char_length=2001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_NVARCHAR",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=30,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_NVARCHAR_2001",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=2001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn(
                "COL_NVARCHAR_4001",
                GLUENT_TYPE_VARIABLE_STRING,
                char_length=4001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            ),
            CanonicalColumn("COL_SMALLDATETIME", GLUENT_TYPE_TIMESTAMP),
            CanonicalColumn("COL_SMALLINT", GLUENT_TYPE_INTEGER_2),
            CanonicalColumn(
                "COL_SMALLMONEY", GLUENT_TYPE_DECIMAL, data_precision=10, data_scale=4
            ),
            CanonicalColumn("COL_TIME", GLUENT_TYPE_TIME),
            CanonicalColumn("COL_TINYINT", GLUENT_TYPE_INTEGER_1),
            CanonicalColumn("COL_UNIQUEIDENTIFIER", GLUENT_TYPE_FIXED_STRING),
            CanonicalColumn("COL_VARBINARY", GLUENT_TYPE_BINARY, data_length=30),
            CanonicalColumn("COL_VARBINARY_2001", GLUENT_TYPE_BINARY, data_length=2001),
            CanonicalColumn(
                "COL_VARCHAR",
                GLUENT_TYPE_VARIABLE_STRING,
                data_length=30,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            CanonicalColumn(
                "COL_VARCHAR_2001",
                GLUENT_TYPE_VARIABLE_STRING,
                data_length=2001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
            CanonicalColumn(
                "COL_VARCHAR_4001",
                GLUENT_TYPE_VARIABLE_STRING,
                data_length=4001,
                char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
            ),
        ]
        self.validate_source_vs_expected_columns(source_columns, expected_columns)

        for source_column in source_columns:
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.to_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)

    ###########################################################
    # Canonical -> Synapse
    ###########################################################

    def test_canonical_to_synapse(self):
        # Expected outcomes of source_columns when converted to Synapse
        expected_columns = [
            SynapseColumn("COL_FIXED_STR", SYNAPSE_TYPE_CHAR),
            SynapseColumn("COL_FIXED_STR_2000", SYNAPSE_TYPE_CHAR, data_length=2000),
            SynapseColumn("COL_FIXED_STR_2000_C", SYNAPSE_TYPE_CHAR, char_length=2000),
            SynapseColumn("COL_FIXED_STR_2001", SYNAPSE_TYPE_CHAR, data_length=2001),
            SynapseColumn("COL_FIXED_STR_2001_C", SYNAPSE_TYPE_CHAR, char_length=2001),
            SynapseColumn("COL_FIXED_STR_1000_U", SYNAPSE_TYPE_NCHAR, char_length=1000),
            SynapseColumn("COL_FIXED_STR_1001_U", SYNAPSE_TYPE_NCHAR, char_length=1001),
            SynapseColumn("COL_LARGE_STR", SYNAPSE_TYPE_VARCHAR),
            SynapseColumn("COL_VARIABLE_STR", SYNAPSE_TYPE_VARCHAR),
            SynapseColumn(
                "COL_VARIABLE_STR_4000", SYNAPSE_TYPE_VARCHAR, data_length=4000
            ),
            SynapseColumn(
                "COL_VARIABLE_STR_4000_C", SYNAPSE_TYPE_VARCHAR, char_length=4000
            ),
            SynapseColumn(
                "COL_VARIABLE_STR_4001", SYNAPSE_TYPE_VARCHAR, data_length=4001
            ),
            SynapseColumn(
                "COL_VARIABLE_STR_4001_C", SYNAPSE_TYPE_VARCHAR, char_length=4001
            ),
            SynapseColumn(
                "COL_VARIABLE_STR_2000_U", SYNAPSE_TYPE_NVARCHAR, char_length=2000
            ),
            SynapseColumn(
                "COL_VARIABLE_STR_2001_U", SYNAPSE_TYPE_NVARCHAR, char_length=2001
            ),
            SynapseColumn("COL_BINARY", SYNAPSE_TYPE_VARBINARY),
            SynapseColumn("COL_BINARY_2000", SYNAPSE_TYPE_VARBINARY, data_length=2000),
            SynapseColumn("COL_BINARY_2001", SYNAPSE_TYPE_VARBINARY, data_length=2001),
            SynapseColumn("COL_LARGE_BINARY", SYNAPSE_TYPE_VARBINARY),
            SynapseColumn("COL_INT_1", SYNAPSE_TYPE_SMALLINT),
            SynapseColumn("COL_INT_2", SYNAPSE_TYPE_SMALLINT),
            SynapseColumn("COL_INT_4", SYNAPSE_TYPE_INT),
            SynapseColumn("COL_INT_8", SYNAPSE_TYPE_BIGINT),
            SynapseColumn("COL_INT_38", SYNAPSE_TYPE_NUMERIC),
            SynapseColumn("COL_DEC_NO_P_S", SYNAPSE_TYPE_NUMERIC),
            SynapseColumn(
                "COL_DEC_10_2", SYNAPSE_TYPE_NUMERIC, data_precision=10, data_scale=2
            ),
            SynapseColumn("COL_FLOAT", SYNAPSE_TYPE_REAL),
            SynapseColumn("COL_DOUBLE", SYNAPSE_TYPE_FLOAT),
            SynapseColumn("COL_DATE", SYNAPSE_TYPE_DATE),
            SynapseColumn("COL_TIME", SYNAPSE_TYPE_TIME),
            SynapseColumn("COL_TIMESTAMP", SYNAPSE_TYPE_DATETIME2),
            SynapseColumn("COL_TIMESTAMP_TZ", SYNAPSE_TYPE_DATETIMEOFFSET),
            SynapseColumn("COL_INTERVAL_DS", SYNAPSE_TYPE_VARCHAR),
            SynapseColumn("COL_INTERVAL_YM", SYNAPSE_TYPE_VARCHAR),
            SynapseColumn("COL_BOOLEAN", SYNAPSE_TYPE_BIT),
        ]
        self.validate_source_vs_expected_columns(
            self.canonical_columns(), expected_columns
        )

        for source_column in self.canonical_columns():
            expected_column = match_table_column(source_column.name, expected_columns)
            assert expected_column
            new_column = self.test_api.from_canonical_column(source_column)
            self.column_assertions(new_column, expected_column)


if __name__ == "__main__":
    main()
