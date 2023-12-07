""" Expected GL_BACKEND_TYPE_MAPPING frontend mappings: a dictionary of expected frontend column specs by column name.
    We do this via this hardcoded dict to avoid relying on to/from_canonical_column() methods in test code because
    they are the very same methods we are trying to test.
"""

from goe.offload.column_metadata import CANONICAL_CHAR_SEMANTICS_BYTE, CANONICAL_CHAR_SEMANTICS_CHAR
from goe.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_IMPALA, DBTYPE_HIVE, DBTYPE_ORACLE,\
    DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE
from goe.offload.oracle.oracle_column import OracleColumn, \
    ORACLE_TYPE_CHAR, ORACLE_TYPE_NCHAR, ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB, \
    ORACLE_TYPE_VARCHAR2, ORACLE_TYPE_NVARCHAR2, \
    ORACLE_TYPE_RAW, ORACLE_TYPE_BLOB, ORACLE_TYPE_NUMBER, \
    ORACLE_TYPE_BINARY_DOUBLE, ORACLE_TYPE_BINARY_FLOAT, ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP, \
    ORACLE_TYPE_TIMESTAMP_TZ, ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM
from goe.offload.teradata.teradata_column import TeradataColumn

# BigQuery expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
BIGQUERY_TO_ORACLE_MAPPINGS = {
    'COL_BIGNUMERIC':
        OracleColumn('COL_BIGNUMERIC', ORACLE_TYPE_NUMBER),
    'COL_BIGNUMERIC_DECIMAL_38_18':
        OracleColumn('COL_BIGNUMERIC_DECIMAL_38_18', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_BIGNUMERIC_INTEGER_8':
        OracleColumn('COL_BIGNUMERIC_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_BYTES':
        OracleColumn('COL_BYTES', ORACLE_TYPE_RAW, data_length=2000),
    'COL_BYTES_LARGE_BIN':
        OracleColumn('COL_BYTES_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_DATE':
        OracleColumn('COL_DATE', ORACLE_TYPE_DATE),
    'COL_DATETIME':
        OracleColumn('COL_DATETIME', ORACLE_TYPE_TIMESTAMP, data_scale=6),
    'COL_DATETIME_DATE':
        OracleColumn('COL_DATETIME_DATE', ORACLE_TYPE_DATE),
    'COL_DATE_TIMESTAMP':
        OracleColumn('COL_DATE_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=6),
    'COL_FLOAT64':
        OracleColumn('COL_FLOAT64', ORACLE_TYPE_BINARY_DOUBLE),
    'COL_FLOAT64_DECIMAL_38_9':
        OracleColumn('COL_FLOAT64_DECIMAL_38_9', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=9),
    'COL_INT64':
        OracleColumn('COL_INT64', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC':
        OracleColumn('COL_NUMERIC', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=9),
    'COL_NUMERIC_DECIMAL_38_18':
        OracleColumn('COL_NUMERIC_DECIMAL_38_18', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_NUMERIC_INTEGER_1':
        OracleColumn('COL_NUMERIC_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_2':
        OracleColumn('COL_NUMERIC_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_38':
        OracleColumn('COL_NUMERIC_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_4':
        OracleColumn('COL_NUMERIC_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_8':
        OracleColumn('COL_NUMERIC_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_STRING':
        OracleColumn('COL_STRING', ORACLE_TYPE_VARCHAR2, char_length=4000,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR),
    'COL_STRING_BINARY':
        OracleColumn('COL_STRING_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_STRING_INTERVAL_DS':
        OracleColumn('COL_STRING_INTERVAL_DS', ORACLE_TYPE_INTERVAL_DS, data_precision=9, data_scale=9),
    'COL_STRING_INTERVAL_YM':
        OracleColumn('COL_STRING_INTERVAL_YM', ORACLE_TYPE_INTERVAL_YM, data_precision=9),
    'COL_STRING_LARGE_BIN':
        OracleColumn('COL_STRING_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_STRING_LARGE_STRING':
        OracleColumn('COL_STRING_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_STRING_LARGE_STRING_U':
        OracleColumn('COL_STRING_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_STRING_U':
        OracleColumn('COL_STRING_U', ORACLE_TYPE_NVARCHAR2, char_length=2000),
    'COL_TIME':
        OracleColumn('COL_TIME', ORACLE_TYPE_VARCHAR2, data_length=18),
    'COL_TIMESTAMP':
        OracleColumn('COL_TIMESTAMP', ORACLE_TYPE_TIMESTAMP_TZ, data_scale=6),
    'COL_TIMESTAMP_DATE':
        OracleColumn('COL_TIMESTAMP_DATE', ORACLE_TYPE_DATE),
    'COL_TIMESTAMP_TIMESTAMP':
        OracleColumn('COL_TIMESTAMP_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=6),
}

# Impala expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
HADOOP_TO_ORACLE_MAPPINGS = {
    'COL_BIGINT':
        OracleColumn('COL_BIGINT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_CHAR_3':
        OracleColumn('COL_CHAR_3', ORACLE_TYPE_CHAR, data_length=3,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE),
    'COL_CHAR_3_U':
        OracleColumn('COL_CHAR_3_U', ORACLE_TYPE_NCHAR, char_length=3),
    'COL_DATE':
        OracleColumn('COL_DATE', ORACLE_TYPE_DATE),
    'COL_DATE_TIMESTAMP':
        OracleColumn('COL_DATE_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=9),
    'COL_DECIMAL':
        OracleColumn('COL_DECIMAL', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_18_0':
        OracleColumn('COL_DECIMAL_18_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_2_0':
        OracleColumn('COL_DECIMAL_2_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_36_0':
        OracleColumn('COL_DECIMAL_36_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_4_0':
        OracleColumn('COL_DECIMAL_4_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_9_0':
        OracleColumn('COL_DECIMAL_9_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_1':
        OracleColumn('COL_DECIMAL_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_2':
        OracleColumn('COL_DECIMAL_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_38':
        OracleColumn('COL_DECIMAL_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_4':
        OracleColumn('COL_DECIMAL_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_8':
        OracleColumn('COL_DECIMAL_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DOUBLE':
        OracleColumn('COL_DOUBLE', ORACLE_TYPE_BINARY_DOUBLE),
    'COL_DOUBLE_DECIMAL':
        OracleColumn('COL_DOUBLE_DECIMAL', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_FLOAT':
        OracleColumn('COL_FLOAT', ORACLE_TYPE_BINARY_FLOAT),
    'COL_FLOAT_DECIMAL':
        OracleColumn('COL_FLOAT_DECIMAL', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_INT':
        OracleColumn('COL_INT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_SMALLINT':
        OracleColumn('COL_SMALLINT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_STRING':
        OracleColumn('COL_STRING', ORACLE_TYPE_VARCHAR2, char_length=4000,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE),
    'COL_STRING_BINARY':
        OracleColumn('COL_STRING_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_STRING_INTERVAL_DS':
        OracleColumn('COL_STRING_INTERVAL_DS', ORACLE_TYPE_INTERVAL_DS, data_precision=9, data_scale=9),
    'COL_STRING_INTERVAL_YM':
        OracleColumn('COL_STRING_INTERVAL_YM', ORACLE_TYPE_INTERVAL_YM, data_precision=9),
    'COL_STRING_LARGE_BIN':
        OracleColumn('COL_STRING_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_STRING_LARGE_STRING':
        OracleColumn('COL_STRING_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_STRING_LARGE_STRING_U':
        OracleColumn('COL_STRING_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_STRING_U':
        OracleColumn('COL_STRING_U', ORACLE_TYPE_NVARCHAR2, char_length=2000),
    'COL_TIMESTAMP':
        OracleColumn('COL_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=9),
    'COL_TIMESTAMP_DATE':
        OracleColumn('COL_TIMESTAMP_DATE', ORACLE_TYPE_DATE),
    'COL_TINYINT':
        OracleColumn('COL_TINYINT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_VARCHAR_4000':
        OracleColumn('COL_VARCHAR_4000', ORACLE_TYPE_VARCHAR2, data_length=4000,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE),
    'COL_VARCHAR_4001':
        OracleColumn('COL_VARCHAR_4001', ORACLE_TYPE_CLOB),
    'COL_VARCHAR_2000_U':
        OracleColumn('COL_VARCHAR_2000_U', ORACLE_TYPE_NVARCHAR2, char_length=2000),
    'COL_VARCHAR_30_BINARY':
        OracleColumn('COL_VARCHAR_30_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_VARCHAR_30_LARGE_BIN':
        OracleColumn('COL_VARCHAR_30_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_VARCHAR_30_LARGE_STRING':
        OracleColumn('COL_VARCHAR_30_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_VARCHAR_30_LARGE_STRING_U':
        OracleColumn('COL_VARCHAR_30_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_VARCHAR_2001_U':
        OracleColumn('COL_VARCHAR_2001_U', ORACLE_TYPE_NCLOB),
}

# Hive expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
HIVE_TO_ORACLE_MAPPINGS = HADOOP_TO_ORACLE_MAPPINGS
HIVE_TO_ORACLE_MAPPINGS.update({
    'COL_BINARY':
        OracleColumn('COL_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_BINARY_LARGE_BIN':
        OracleColumn('COL_BINARY_LARGE_BIN', ORACLE_TYPE_BLOB),
})

# Impala expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
IMPALA_TO_ORACLE_MAPPINGS = HADOOP_TO_ORACLE_MAPPINGS
IMPALA_TO_ORACLE_MAPPINGS.update({
    'COL_REAL':
        OracleColumn('COL_REAL', ORACLE_TYPE_BINARY_DOUBLE),
    'COL_REAL_DECIMAL':
        OracleColumn('COL_REAL_DECIMAL', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
})

# Snowflake expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
SNOWFLAKE_TO_ORACLE_MAPPINGS = {
    'COL_BINARY_2000':
        OracleColumn('COL_BINARY_2000', ORACLE_TYPE_RAW, data_length=2000),
    'COL_BINARY_2000_LARGE_BIN':
        OracleColumn('COL_BINARY_2000_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_BINARY_2001':
        OracleColumn('COL_BINARY_2001', ORACLE_TYPE_BLOB),
    'COL_BINARY_2001_BINARY':
        OracleColumn('COL_BINARY_2001_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_DATE':
        OracleColumn('COL_DATE', ORACLE_TYPE_DATE),
    'COL_DATE_TIMESTAMP':
        OracleColumn('COL_DATE_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=9),
    'COL_FLOAT':
        OracleColumn('COL_FLOAT', ORACLE_TYPE_BINARY_DOUBLE),
    'COL_FLOAT_DECIMAL':
        OracleColumn('COL_FLOAT_DECIMAL', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_NUMBER':
        OracleColumn('COL_NUMBER', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_NUMBER_18_0':
        OracleColumn('COL_NUMBER_18_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_2_0':
        OracleColumn('COL_NUMBER_2_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_36_0':
        OracleColumn('COL_NUMBER_36_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_4_0':
        OracleColumn('COL_NUMBER_4_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_9_0':
        OracleColumn('COL_NUMBER_9_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_INTEGER_1':
        OracleColumn('COL_NUMBER_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_INTEGER_2':
        OracleColumn('COL_NUMBER_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_INTEGER_38':
        OracleColumn('COL_NUMBER_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_INTEGER_4':
        OracleColumn('COL_NUMBER_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMBER_INTEGER_8':
        OracleColumn('COL_NUMBER_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_TEXT_2000_U':
        OracleColumn('COL_TEXT_2000_U', ORACLE_TYPE_NVARCHAR2, char_length=2000),
    'COL_TEXT_30_LARGE_STRING_U':
        OracleColumn('COL_TEXT_30_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_TEXT_2001_U':
        OracleColumn('COL_TEXT_2001_U', ORACLE_TYPE_NCLOB),
    'COL_TEXT_4000':
        OracleColumn('COL_TEXT_4000', ORACLE_TYPE_VARCHAR2, char_length=4000,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR),
    'COL_TEXT_30_LARGE_STRING':
        OracleColumn('COL_TEXT_30_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_TEXT_4001':
        OracleColumn('COL_TEXT_4001', ORACLE_TYPE_CLOB),
    'COL_TEXT_BINARY':
        OracleColumn('COL_TEXT_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_TEXT_INTERVAL_DS':
        OracleColumn('COL_TEXT_INTERVAL_DS', ORACLE_TYPE_INTERVAL_DS, data_precision=9, data_scale=9),
    'COL_TEXT_INTERVAL_YM':
        OracleColumn('COL_TEXT_INTERVAL_YM', ORACLE_TYPE_INTERVAL_YM, data_precision=9),
    'COL_TEXT_LARGE_BIN':
        OracleColumn('COL_TEXT_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_TIME':
        OracleColumn('COL_TIME', ORACLE_TYPE_VARCHAR2, data_length=18),
    'COL_TIMESTAMP_NTZ':
        OracleColumn('COL_TIMESTAMP_NTZ', ORACLE_TYPE_TIMESTAMP, data_scale=9),
    'COL_TIMESTAMP_NTZ_DATE':
        OracleColumn('COL_TIMESTAMP_NTZ_DATE', ORACLE_TYPE_DATE),
    'COL_TIMESTAMP_TZ':
        OracleColumn('COL_TIMESTAMP_TZ', ORACLE_TYPE_TIMESTAMP_TZ, data_scale=9),
    'COL_TIMESTAMP_TZ_DATE':
        OracleColumn('COL_TIMESTAMP_TZ_DATE', ORACLE_TYPE_DATE),
    'COL_TIMESTAMP_TZ_TIMESTAMP':
        OracleColumn('COL_TIMESTAMP_TZ_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=9),
}

# Synapse expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
SYNAPSE_TO_ORACLE_MAPPINGS = {
    'COL_BIGINT':
        OracleColumn('COL_BIGINT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_BINARY_10':
        OracleColumn('COL_BINARY_10', ORACLE_TYPE_RAW, data_length=10),
    'COL_BINARY_10_LARGE_BIN':
        OracleColumn('COL_BINARY_10_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_BINARY_10_BINARY':
        OracleColumn('COL_BINARY_10_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_CHAR_3':
        OracleColumn('COL_CHAR_3', ORACLE_TYPE_CHAR, data_length=3,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE),
    'COL_CHAR_3_BINARY':
        OracleColumn('COL_CHAR_3_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_CHAR_3_LARGE_BIN':
        OracleColumn('COL_CHAR_3_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_CHAR_3_LARGE_STRING':
        OracleColumn('COL_CHAR_3_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_CHAR_3_LARGE_STRING_U':
        OracleColumn('COL_CHAR_3_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_CHAR_3_U':
        OracleColumn('COL_CHAR_3_U', ORACLE_TYPE_NCHAR, char_length=3),
    'COL_DATE':
        OracleColumn('COL_DATE', ORACLE_TYPE_DATE),
    'COL_DATETIME':
        OracleColumn('COL_DATETIME', ORACLE_TYPE_TIMESTAMP, data_scale=3),
    'COL_DATETIME2':
        OracleColumn('COL_DATETIME2', ORACLE_TYPE_TIMESTAMP, data_scale=7),
    'COL_DATETIME2_3':
        OracleColumn('COL_DATETIME2_3', ORACLE_TYPE_TIMESTAMP, data_scale=3),
    'COL_DATETIME2_DATE':
        OracleColumn('COL_DATETIME2_DATE', ORACLE_TYPE_DATE),
    'COL_DATETIMEOFFSET':
        OracleColumn('COL_DATETIMEOFFSET', ORACLE_TYPE_TIMESTAMP_TZ, data_scale=7),
    'COL_DATETIMEOFFSET_3':
        OracleColumn('COL_DATETIMEOFFSET_3', ORACLE_TYPE_TIMESTAMP_TZ, data_scale=3),
    'COL_DATETIMEOFFSET_DATE':
        OracleColumn('COL_DATETIMEOFFSET_DATE', ORACLE_TYPE_DATE),
    'COL_DATETIMEOFFSET_TIMESTAMP':
        OracleColumn('COL_DATETIMEOFFSET_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=7),
    'COL_DATETIME_DATE':
        OracleColumn('COL_DATETIME_DATE', ORACLE_TYPE_DATE),
    'COL_DATE_TIMESTAMP':
        OracleColumn('COL_DATE_TIMESTAMP', ORACLE_TYPE_TIMESTAMP, data_scale=7),
    'COL_DECIMAL_10_3':
        OracleColumn('COL_DECIMAL_10_3', ORACLE_TYPE_NUMBER, data_precision=10, data_scale=3),
    'COL_DECIMAL_18_0':
        OracleColumn('COL_DECIMAL_18_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_2_0':
        OracleColumn('COL_DECIMAL_2_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_36_0':
        OracleColumn('COL_DECIMAL_36_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_4_0':
        OracleColumn('COL_DECIMAL_4_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_9_0':
        OracleColumn('COL_DECIMAL_9_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_1':
        OracleColumn('COL_DECIMAL_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_2':
        OracleColumn('COL_DECIMAL_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_38':
        OracleColumn('COL_DECIMAL_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_4':
        OracleColumn('COL_DECIMAL_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_DECIMAL_INTEGER_8':
        OracleColumn('COL_DECIMAL_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_FLOAT':
        OracleColumn('COL_FLOAT', ORACLE_TYPE_BINARY_DOUBLE),
    'COL_FLOAT_DECIMAL':
        OracleColumn('COL_FLOAT_DECIMAL', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_INT':
        OracleColumn('COL_INT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_MONEY':
        OracleColumn('COL_MONEY', ORACLE_TYPE_NUMBER, data_precision=19, data_scale=4),
    'COL_MONEY_INTEGER_1':
        OracleColumn('COL_MONEY_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_MONEY_INTEGER_2':
        OracleColumn('COL_MONEY_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_MONEY_INTEGER_38':
        OracleColumn('COL_MONEY_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_MONEY_INTEGER_4':
        OracleColumn('COL_MONEY_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_MONEY_INTEGER_8':
        OracleColumn('COL_MONEY_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NCHAR_3':
        OracleColumn('COL_NCHAR_3', ORACLE_TYPE_CHAR, char_length=6,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR),
    'COL_NCHAR_3_BINARY':
        OracleColumn('COL_NCHAR_3_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_NCHAR_3_LARGE_BIN':
        OracleColumn('COL_NCHAR_3_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_NCHAR_3_LARGE_STRING':
        OracleColumn('COL_NCHAR_3_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_NCHAR_3_LARGE_STRING_U':
        OracleColumn('COL_NCHAR_3_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_NCHAR_3_U':
        OracleColumn('COL_NCHAR_3_U', ORACLE_TYPE_NCHAR, char_length=6),
    'COL_NUMERIC_10_3':
        OracleColumn('COL_NUMERIC_10_3', ORACLE_TYPE_NUMBER, data_precision=10, data_scale=3),
    'COL_NUMERIC_18_0':
        OracleColumn('COL_NUMERIC_18_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_2_0':
        OracleColumn('COL_NUMERIC_2_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_36_0':
        OracleColumn('COL_NUMERIC_36_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_4_0':
        OracleColumn('COL_NUMERIC_4_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_9_0':
        OracleColumn('COL_NUMERIC_9_0', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_1':
        OracleColumn('COL_NUMERIC_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_2':
        OracleColumn('COL_NUMERIC_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_38':
        OracleColumn('COL_NUMERIC_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_4':
        OracleColumn('COL_NUMERIC_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NUMERIC_INTEGER_8':
        OracleColumn('COL_NUMERIC_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_NVARCHAR_30':
        OracleColumn('COL_NVARCHAR_30', ORACLE_TYPE_VARCHAR2, char_length=60,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR),
    'COL_NVARCHAR_30_BINARY':
        OracleColumn('COL_NVARCHAR_30_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_NVARCHAR_30_LARGE_BIN':
        OracleColumn('COL_NVARCHAR_30_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_NVARCHAR_30_LARGE_STRING':
        OracleColumn('COL_NVARCHAR_30_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_NVARCHAR_30_LARGE_STRING_U':
        OracleColumn('COL_NVARCHAR_30_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_NVARCHAR_30_U':
        OracleColumn('COL_NVARCHAR_30_U', ORACLE_TYPE_NVARCHAR2, char_length=60),
    'COL_REAL':
        OracleColumn('COL_REAL', ORACLE_TYPE_BINARY_FLOAT),
    'COL_REAL_DECIMAL':
        OracleColumn('COL_REAL_DECIMAL', ORACLE_TYPE_NUMBER, data_precision=38, data_scale=18),
    'COL_SMALLDATETIME':
        OracleColumn('COL_SMALLDATETIME', ORACLE_TYPE_DATE),
    'COL_SMALLDATETIME_DATE':
        OracleColumn('COL_SMALLDATETIME_DATE', ORACLE_TYPE_DATE),
    'COL_SMALLINT':
        OracleColumn('COL_SMALLINT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_SMALLMONEY':
        OracleColumn('COL_SMALLMONEY', ORACLE_TYPE_NUMBER, data_precision=10, data_scale=4),
    'COL_SMALLMONEY_INTEGER_1':
        OracleColumn('COL_SMALLMONEY_INTEGER_1', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_SMALLMONEY_INTEGER_2':
        OracleColumn('COL_SMALLMONEY_INTEGER_2', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_SMALLMONEY_INTEGER_38':
        OracleColumn('COL_SMALLMONEY_INTEGER_38', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_SMALLMONEY_INTEGER_4':
        OracleColumn('COL_SMALLMONEY_INTEGER_4', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_SMALLMONEY_INTEGER_8':
        OracleColumn('COL_SMALLMONEY_INTEGER_8', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_TIME':
        OracleColumn('COL_TIME', ORACLE_TYPE_VARCHAR2, data_length=18),
    'COL_TINYINT':
        OracleColumn('COL_TINYINT', ORACLE_TYPE_NUMBER, data_scale=0),
    'COL_UNIQUEIDENTIFIER':
        OracleColumn('COL_UNIQUEIDENTIFIER', ORACLE_TYPE_CHAR, data_length=36),
    'COL_VARBINARY_30':
        OracleColumn('COL_VARBINARY_30', ORACLE_TYPE_RAW, data_length=30),
    'COL_VARBINARY_30_LARGE_BIN':
        OracleColumn('COL_VARBINARY_30_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_VARBINARY_2001_BINARY':
        OracleColumn('COL_VARBINARY_2001_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_VARCHAR_30':
        OracleColumn('COL_VARCHAR_30', ORACLE_TYPE_VARCHAR2, data_length=30,
                     char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE),
    'COL_VARCHAR_30_BINARY':
        OracleColumn('COL_VARCHAR_30_BINARY', ORACLE_TYPE_RAW, data_length=2000),
    'COL_VARCHAR_30_INTERVAL_DS':
        OracleColumn('COL_VARCHAR_30_INTERVAL_DS', ORACLE_TYPE_INTERVAL_DS, data_precision=9, data_scale=9),
    'COL_VARCHAR_30_INTERVAL_YM':
        OracleColumn('COL_VARCHAR_30_INTERVAL_YM', ORACLE_TYPE_INTERVAL_YM, data_precision=9),
    'COL_VARCHAR_30_LARGE_BIN':
        OracleColumn('COL_VARCHAR_30_LARGE_BIN', ORACLE_TYPE_BLOB),
    'COL_VARCHAR_30_LARGE_STRING':
        OracleColumn('COL_VARCHAR_30_LARGE_STRING', ORACLE_TYPE_CLOB),
    'COL_VARCHAR_30_LARGE_STRING_U':
        OracleColumn('COL_VARCHAR_30_LARGE_STRING_U', ORACLE_TYPE_NCLOB),
    'COL_VARCHAR_30_U':
        OracleColumn('COL_VARCHAR_30_U', ORACLE_TYPE_NVARCHAR2, char_length=30),
}

# Expected frontend columns for GL_BACKEND_TYPE_MAPPING after present.
TYPE_MAPPING_PRESENT_MAPPINGS = {
    DBTYPE_ORACLE: {
        DBTYPE_BIGQUERY: BIGQUERY_TO_ORACLE_MAPPINGS,
        DBTYPE_HIVE: HIVE_TO_ORACLE_MAPPINGS,
        DBTYPE_IMPALA: IMPALA_TO_ORACLE_MAPPINGS,
        DBTYPE_SNOWFLAKE: SNOWFLAKE_TO_ORACLE_MAPPINGS,
        DBTYPE_SYNAPSE: SYNAPSE_TO_ORACLE_MAPPINGS,
    }
}


if __name__ == "__main__":
    for backend in TYPE_MAPPING_PRESENT_MAPPINGS[DBTYPE_ORACLE]:
        print('Checking backend', backend)
        print('Column name mismatches:', [k for k, v in TYPE_MAPPING_PRESENT_MAPPINGS[DBTYPE_ORACLE][backend].items()
                                          if k != v.name])
