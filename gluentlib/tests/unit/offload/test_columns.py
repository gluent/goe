""" Test that frontend/backend system column objects are functional """
from unittest import TestCase, main

from gluentlib.offload.bigquery.bigquery_column import BigQueryColumn,\
    BIGQUERY_TYPE_BIGNUMERIC, BIGQUERY_TYPE_BOOLEAN, BIGQUERY_TYPE_BYTES, BIGQUERY_TYPE_DATE,\
    BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_FLOAT64, BIGQUERY_TYPE_INT64,\
    BIGQUERY_TYPE_NUMERIC, BIGQUERY_TYPE_STRING, BIGQUERY_TYPE_TIME, BIGQUERY_TYPE_TIMESTAMP
from gluentlib.offload.hadoop.hadoop_column import HadoopColumn,\
    HADOOP_TYPE_CHAR, HADOOP_TYPE_STRING, HADOOP_TYPE_VARCHAR,\
    HADOOP_TYPE_TINYINT, HADOOP_TYPE_SMALLINT, HADOOP_TYPE_INT, HADOOP_TYPE_BIGINT, HADOOP_TYPE_DECIMAL,\
    HADOOP_TYPE_FLOAT, HADOOP_TYPE_DOUBLE, HADOOP_TYPE_REAL, HADOOP_TYPE_DATE, HADOOP_TYPE_TIMESTAMP,\
    HADOOP_TYPE_BINARY
from gluentlib.offload.snowflake.snowflake_column import SnowflakeColumn,\
    SNOWFLAKE_TYPE_BINARY, SNOWFLAKE_TYPE_BOOLEAN, SNOWFLAKE_TYPE_DATE, SNOWFLAKE_TYPE_FLOAT,\
    SNOWFLAKE_TYPE_NUMBER, SNOWFLAKE_TYPE_TEXT,\
    SNOWFLAKE_TYPE_TIME, SNOWFLAKE_TYPE_TIMESTAMP_NTZ, SNOWFLAKE_TYPE_TIMESTAMP_TZ
from gluentlib.offload.microsoft.synapse_column import SynapseColumn,\
    SYNAPSE_TYPE_BIGINT, SYNAPSE_TYPE_BINARY, SYNAPSE_TYPE_BIT, SYNAPSE_TYPE_CHAR, SYNAPSE_TYPE_DATE,\
    SYNAPSE_TYPE_DATETIME, SYNAPSE_TYPE_DATETIME2, SYNAPSE_TYPE_DATETIMEOFFSET, SYNAPSE_TYPE_DECIMAL,\
    SYNAPSE_TYPE_FLOAT, SYNAPSE_TYPE_INT, SYNAPSE_TYPE_MONEY, SYNAPSE_TYPE_NCHAR, SYNAPSE_TYPE_NUMERIC,\
    SYNAPSE_TYPE_NVARCHAR, SYNAPSE_TYPE_REAL, SYNAPSE_TYPE_SMALLDATETIME, SYNAPSE_TYPE_SMALLINT,\
    SYNAPSE_TYPE_SMALLMONEY, SYNAPSE_TYPE_TIME, SYNAPSE_TYPE_TINYINT, SYNAPSE_TYPE_UNIQUEIDENTIFIER,\
    SYNAPSE_TYPE_VARBINARY, SYNAPSE_TYPE_VARCHAR
from gluentlib.offload.oracle.oracle_column import OracleColumn,\
    ORACLE_TYPE_CHAR, ORACLE_TYPE_NCHAR, ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB, \
    ORACLE_TYPE_LONG, ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2, \
    ORACLE_TYPE_RAW, ORACLE_TYPE_BLOB, ORACLE_TYPE_LONG_RAW, ORACLE_TYPE_NUMBER, \
    ORACLE_TYPE_FLOAT, ORACLE_TYPE_BINARY_FLOAT, ORACLE_TYPE_BINARY_DOUBLE, \
    ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ, \
    ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM
from gluentlib.offload.teradata.teradata_column import TeradataColumn,\
    TERADATA_TYPE_BIGINT, TERADATA_TYPE_BYTE, TERADATA_TYPE_BLOB, TERADATA_TYPE_BYTEINT, TERADATA_TYPE_CHAR,\
    TERADATA_TYPE_CLOB, TERADATA_TYPE_DATE, TERADATA_TYPE_DECIMAL, TERADATA_TYPE_DOUBLE, TERADATA_TYPE_INTEGER,\
    TERADATA_TYPE_INTERVAL_YR, TERADATA_TYPE_INTERVAL_YM, TERADATA_TYPE_INTERVAL_MO, TERADATA_TYPE_INTERVAL_DY,\
    TERADATA_TYPE_INTERVAL_DH, TERADATA_TYPE_INTERVAL_DM, TERADATA_TYPE_INTERVAL_DS, TERADATA_TYPE_INTERVAL_HR,\
    TERADATA_TYPE_INTERVAL_HM, TERADATA_TYPE_INTERVAL_HS, TERADATA_TYPE_INTERVAL_MI, TERADATA_TYPE_INTERVAL_MS,\
    TERADATA_TYPE_INTERVAL_SC, TERADATA_TYPE_NUMBER, TERADATA_TYPE_SMALLINT,\
    TERADATA_TYPE_TIME, TERADATA_TYPE_TIMESTAMP, TERADATA_TYPE_TIME_TZ, TERADATA_TYPE_TIMESTAMP_TZ,\
    TERADATA_TYPE_VARBYTE, TERADATA_TYPE_VARCHAR


class TestColumns(TestCase):
    """ Test that frontend/backend system column objects are functional """

    def _inspect_column_object(self, col):
        self.assertIsInstance(col.format_data_type(), str)
        self.assertIsInstance(col.has_time_element(), bool)
        self.assertIsInstance(col.is_binary(), bool)
        self.assertIsInstance(col.is_date_based(), bool)
        self.assertIsInstance(col.is_nan_capable(), bool)
        self.assertIsInstance(col.is_number_based(), bool)
        self.assertIsInstance(col.is_string_based(), bool)
        self.assertIsInstance(col.is_time_zone_based(), bool)
        self.assertIsInstance(col.valid_for_offload_predicate(), bool)

    def test_bigquery_column(self):
        for data_type in [BIGQUERY_TYPE_BIGNUMERIC, BIGQUERY_TYPE_BOOLEAN,
                          BIGQUERY_TYPE_BYTES, BIGQUERY_TYPE_DATE, BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_FLOAT64,
                          BIGQUERY_TYPE_INT64, BIGQUERY_TYPE_NUMERIC, BIGQUERY_TYPE_STRING, BIGQUERY_TYPE_TIME,
                          BIGQUERY_TYPE_TIMESTAMP]:
            column = BigQueryColumn('test_column', data_type)
            self._inspect_column_object(column)

    def test_hadoop_column(self):
        for data_type in [HADOOP_TYPE_CHAR, HADOOP_TYPE_STRING, HADOOP_TYPE_VARCHAR,
                          (HADOOP_TYPE_VARCHAR, 30, None, None),
                          HADOOP_TYPE_TINYINT, HADOOP_TYPE_SMALLINT, HADOOP_TYPE_INT, HADOOP_TYPE_BIGINT,
                          HADOOP_TYPE_DECIMAL, (HADOOP_TYPE_DECIMAL, None, 20, 5),
                          HADOOP_TYPE_FLOAT, HADOOP_TYPE_DOUBLE, HADOOP_TYPE_REAL,
                          HADOOP_TYPE_DATE, HADOOP_TYPE_TIMESTAMP, HADOOP_TYPE_BINARY]:
            if isinstance(data_type, tuple):
                data_type, data_length, data_precision, data_scale = data_type
            else:
                data_length = data_precision = data_scale = None
            column = HadoopColumn('test_column', data_type, data_length=data_length,
                                  data_precision=data_precision, data_scale=data_scale)
            self._inspect_column_object(column)

    def test_oracle_column(self):
        for data_type in [
            (ORACLE_TYPE_CHAR, 5, None, None), (ORACLE_TYPE_NCHAR, 5, None, None),
            ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB, ORACLE_TYPE_LONG,
            (ORACLE_TYPE_VARCHAR2, 30, None, None), (ORACLE_TYPE_NVARCHAR2, 30, None, None),
            ORACLE_TYPE_RAW, (ORACLE_TYPE_RAW, 16, None, None),
            ORACLE_TYPE_BLOB, ORACLE_TYPE_LONG_RAW,
            ORACLE_TYPE_NUMBER, (ORACLE_TYPE_NUMBER, None, 12, None), (ORACLE_TYPE_NUMBER, None, 20, 2),
            ORACLE_TYPE_FLOAT, ORACLE_TYPE_BINARY_FLOAT, ORACLE_TYPE_BINARY_DOUBLE,
            ORACLE_TYPE_DATE,
            ORACLE_TYPE_TIMESTAMP, (ORACLE_TYPE_TIMESTAMP, None, None, 3),
            ORACLE_TYPE_TIMESTAMP_TZ, (ORACLE_TYPE_TIMESTAMP_TZ, None, None, 3),
            ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM
        ]:
            if isinstance(data_type, tuple):
                data_type, data_length, data_precision, data_scale = data_type
            else:
                data_length = data_precision = data_scale = None
            column = OracleColumn('test_column', data_type, data_length=data_length,
                                  data_precision=data_precision, data_scale=data_scale)
            self._inspect_column_object(column)

    def test_snowflake_column(self):
        for data_type in [
            SNOWFLAKE_TYPE_BINARY, (SNOWFLAKE_TYPE_BINARY, 16, None, None),
            SNOWFLAKE_TYPE_BOOLEAN, SNOWFLAKE_TYPE_DATE, SNOWFLAKE_TYPE_FLOAT,
            SNOWFLAKE_TYPE_NUMBER, (SNOWFLAKE_TYPE_NUMBER, None, 12, None), (SNOWFLAKE_TYPE_NUMBER, None, 20, 2),
            SNOWFLAKE_TYPE_TEXT, (SNOWFLAKE_TYPE_TEXT, 30, None, None),
            SNOWFLAKE_TYPE_TIME, SNOWFLAKE_TYPE_TIMESTAMP_NTZ, SNOWFLAKE_TYPE_TIMESTAMP_TZ
        ]:
            if isinstance(data_type, tuple):
                data_type, data_length, data_precision, data_scale = data_type
            else:
                data_length = data_precision = data_scale = None
            column = SnowflakeColumn('test_column', data_type, data_length=data_length,
                                     data_precision=data_precision, data_scale=data_scale)
            self._inspect_column_object(column)

    def test_synapse_column(self):
        for data_type in [
            SYNAPSE_TYPE_BIGINT, SYNAPSE_TYPE_BINARY, (SYNAPSE_TYPE_BINARY, 16, None, None), SYNAPSE_TYPE_BIT,
            SYNAPSE_TYPE_CHAR, (SYNAPSE_TYPE_CHAR, 2, None, None),
            SYNAPSE_TYPE_DATE, SYNAPSE_TYPE_DATETIME, SYNAPSE_TYPE_DATETIME2, SYNAPSE_TYPE_DATETIMEOFFSET,
            SYNAPSE_TYPE_DECIMAL, (SYNAPSE_TYPE_DECIMAL, None, 20, 2),
            SYNAPSE_TYPE_FLOAT, SYNAPSE_TYPE_INT, SYNAPSE_TYPE_MONEY, SYNAPSE_TYPE_NCHAR,
            SYNAPSE_TYPE_NUMERIC, (SYNAPSE_TYPE_NUMERIC, None, 20, 2),
            SYNAPSE_TYPE_NVARCHAR, (SYNAPSE_TYPE_NVARCHAR, 30, None, None),
            SYNAPSE_TYPE_REAL, SYNAPSE_TYPE_SMALLDATETIME, SYNAPSE_TYPE_SMALLINT,
            SYNAPSE_TYPE_SMALLMONEY, SYNAPSE_TYPE_TIME, SYNAPSE_TYPE_TINYINT, SYNAPSE_TYPE_UNIQUEIDENTIFIER,
            SYNAPSE_TYPE_VARBINARY, (SYNAPSE_TYPE_VARBINARY, 30, None, None),
            SYNAPSE_TYPE_VARCHAR, (SYNAPSE_TYPE_VARCHAR, 30, None, None)
        ]:
            if isinstance(data_type, tuple):
                data_type, data_length, data_precision, data_scale = data_type
            else:
                data_length = data_precision = data_scale = None
            column = SynapseColumn('test_column', data_type, data_length=data_length,
                                   data_precision=data_precision, data_scale=data_scale)
            self._inspect_column_object(column)

    def test_teradata_column(self):
        for data_type in [
            TERADATA_TYPE_BIGINT, TERADATA_TYPE_BYTE,
            TERADATA_TYPE_BLOB, TERADATA_TYPE_BYTEINT,
            TERADATA_TYPE_CHAR, (TERADATA_TYPE_CHAR, 2, None, None),
            TERADATA_TYPE_CLOB, TERADATA_TYPE_DATE,
            TERADATA_TYPE_DECIMAL, (TERADATA_TYPE_DECIMAL, None, 10, None), (TERADATA_TYPE_DECIMAL, None, 20, 2),
            TERADATA_TYPE_DOUBLE, TERADATA_TYPE_INTEGER,
            TERADATA_TYPE_INTERVAL_YR, TERADATA_TYPE_INTERVAL_YM, TERADATA_TYPE_INTERVAL_MO, TERADATA_TYPE_INTERVAL_DY,
            TERADATA_TYPE_INTERVAL_DH, TERADATA_TYPE_INTERVAL_DM, TERADATA_TYPE_INTERVAL_DS, TERADATA_TYPE_INTERVAL_HR,
            TERADATA_TYPE_INTERVAL_HM, TERADATA_TYPE_INTERVAL_HS, TERADATA_TYPE_INTERVAL_MI, TERADATA_TYPE_INTERVAL_MS,
            TERADATA_TYPE_INTERVAL_SC,
            TERADATA_TYPE_NUMBER, (TERADATA_TYPE_NUMBER, None, 10, None), (TERADATA_TYPE_NUMBER, None, 20, 2),
            TERADATA_TYPE_SMALLINT,
            TERADATA_TYPE_TIME, (TERADATA_TYPE_TIME, None, None, 3),
            TERADATA_TYPE_TIMESTAMP, (TERADATA_TYPE_TIMESTAMP, None, None, 3),
            TERADATA_TYPE_TIME_TZ,
            TERADATA_TYPE_TIMESTAMP_TZ, TERADATA_TYPE_VARBYTE,
            TERADATA_TYPE_VARCHAR, (TERADATA_TYPE_VARCHAR, 30, None, None),
        ]:
            if isinstance(data_type, tuple):
                data_type, data_length, data_precision, data_scale = data_type
            else:
                data_length = data_precision = data_scale = None
            column = TeradataColumn('test_column', data_type, data_length=data_length,
                                    data_precision=data_precision, data_scale=data_scale)
            self._inspect_column_object(column)


if __name__ == '__main__':
    main()
