#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" Hadoop implementation of BackendTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests.
    LICENSE_TEXT
"""

from datetime import datetime
import logging
import re

from goe.filesystem.gluent_dfs import get_scheme_from_location_uri, \
    OFFLOAD_FS_SCHEME_HDFS, OFFLOAD_FS_SCHEME_MAPRFS
from goe.offload.column_metadata import CanonicalColumn,\
    CANONICAL_CHAR_SEMANTICS_UNICODE, GLUENT_TYPE_FIXED_STRING, GLUENT_TYPE_LARGE_STRING,\
    GLUENT_TYPE_VARIABLE_STRING, GLUENT_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY, GLUENT_TYPE_INTEGER_1,\
    GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8, GLUENT_TYPE_INTEGER_38, GLUENT_TYPE_DECIMAL,\
    GLUENT_TYPE_FLOAT, GLUENT_TYPE_DOUBLE, GLUENT_TYPE_DATE, GLUENT_TYPE_TIMESTAMP,\
    GLUENT_TYPE_TIMESTAMP_TZ, GLUENT_TYPE_INTERVAL_DS, GLUENT_TYPE_INTERVAL_YM
from goe.offload.hadoop.hadoop_column import HadoopColumn
from goe.offload.offload_messages import VERBOSE
from goe.util.better_impyla import HADOOP_TYPE_BIGINT, HADOOP_TYPE_CHAR, HADOOP_TYPE_DATE,\
    HADOOP_TYPE_DECIMAL, HADOOP_TYPE_DOUBLE, HADOOP_TYPE_FLOAT, HADOOP_TYPE_INT, HADOOP_TYPE_SMALLINT,\
    HADOOP_TYPE_STRING, HADOOP_TYPE_TIMESTAMP, HADOOP_TYPE_TINYINT, HADOOP_TYPE_VARCHAR
from tests.testlib.test_framework.backend_testing_api import BackendTestingApiInterface, BackendTestingApiException,\
    subproc_cmd,\
    STORY_TEST_BACKEND_DATE_COL, STORY_TEST_BACKEND_DOUBLE_COL, STORY_TEST_BACKEND_INT_1_COL,\
    STORY_TEST_BACKEND_INT_2_COL, STORY_TEST_BACKEND_INT_4_COL, STORY_TEST_BACKEND_INT_8_COL,\
    STORY_TEST_BACKEND_DECIMAL_PS_COL, STORY_TEST_BACKEND_DECIMAL_DEF_COL, STORY_TEST_BACKEND_VAR_STR_COL,\
    STORY_TEST_BACKEND_VAR_STR_LONG_COL, STORY_TEST_BACKEND_FIX_STR_COL, STORY_TEST_BACKEND_TIMESTAMP_COL,\
    STORY_TEST_BACKEND_NULL_STR_COL, STORY_TEST_OFFLOAD_NUMS_BARE_NUM, STORY_TEST_OFFLOAD_NUMS_BARE_FLT,\
    STORY_TEST_OFFLOAD_NUMS_NUM_4, STORY_TEST_OFFLOAD_NUMS_NUM_18, STORY_TEST_OFFLOAD_NUMS_NUM_19,\
    STORY_TEST_OFFLOAD_NUMS_NUM_3_2, STORY_TEST_OFFLOAD_NUMS_NUM_13_3, STORY_TEST_OFFLOAD_NUMS_NUM_16_1,\
    STORY_TEST_OFFLOAD_NUMS_NUM_20_5, STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4, STORY_TEST_OFFLOAD_NUMS_NUM_3_5,\
    STORY_TEST_OFFLOAD_NUMS_NUM_10_M5, STORY_TEST_OFFLOAD_NUMS_DEC_10_0, STORY_TEST_OFFLOAD_NUMS_DEC_13_9,\
    STORY_TEST_OFFLOAD_NUMS_DEC_15_9, STORY_TEST_OFFLOAD_NUMS_DEC_36_3, STORY_TEST_OFFLOAD_NUMS_DEC_37_3,\
    STORY_TEST_OFFLOAD_NUMS_DEC_38_3
from tests.testlib.test_framework.test_constants import UNICODE_NAME_TOKEN


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendHadoopTestingApi
###########################################################################

class BackendHadoopTestingApi(BackendTestingApiInterface):
    """ Common Hadoop methods for both Hive and Impala
    """

    def __init__(self, connection_options, backend_type, messages, dry_run=False,
                 no_caching=False, do_not_connect=False):
        """ CONSTRUCTOR
        """
        super(BackendHadoopTestingApi, self).__init__(connection_options, backend_type, messages, dry_run=dry_run,
                                                      no_caching=no_caching, do_not_connect=do_not_connect)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _create_table_properties(self):
        raise NotImplementedError('_create_table_properties() not implemented for common Hadoop class')

    def _get_create_table_ddl_remove_spark_props(self, ddl):
        """ Remove table properties added if the data was generated using Spark
            If these are left in then schema evolution tests break Spark selects
        """
        assert ddl
        ddl_filter_re = re.compile(r'^\s*.*\'spark.sql.*,\s*$')
        if isinstance(ddl, str):
            return '\n'.join(_ for _ in ddl.split('\n') if not re.match(ddl_filter_re, _))
        else:
            return [_ for _ in ddl if not re.match(ddl_filter_re, _)]

    def _gl_type_mapping_column_definitions(self, filter_column=None):
        """ Returns a dict of dicts defining columns for GL_BACKEND_TYPE_MAPPING test table.
            filter_column can be used to fetch just a single column dict.
        """

        def name(*args):
            return self._gl_type_mapping_column_name(*args)

        all_columns = {
            name(HADOOP_TYPE_BIGINT):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_BIGINT), HADOOP_TYPE_BIGINT),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_BIGINT), GLUENT_TYPE_INTEGER_8)},
            name(HADOOP_TYPE_CHAR, '3'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_CHAR, '3'), HADOOP_TYPE_CHAR, data_length=3),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_CHAR, '3'), GLUENT_TYPE_FIXED_STRING)},
            name(HADOOP_TYPE_CHAR, '3', UNICODE_NAME_TOKEN):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_CHAR, '3', UNICODE_NAME_TOKEN), HADOOP_TYPE_CHAR, data_length=3),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_CHAR, '3', UNICODE_NAME_TOKEN), GLUENT_TYPE_FIXED_STRING),
                 'present_options':
                     {'unicode_string_columns_csv': name(HADOOP_TYPE_CHAR, '3', UNICODE_NAME_TOKEN)}},
            name(HADOOP_TYPE_DECIMAL, '2', '0'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, '2', '0'), HADOOP_TYPE_DECIMAL,
                                  data_precision=2, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, '2', '0'), GLUENT_TYPE_INTEGER_1)},
            name(HADOOP_TYPE_DECIMAL, '4', '0'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, '4', '0'), HADOOP_TYPE_DECIMAL,
                                  data_precision=4, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, '4', '0'), GLUENT_TYPE_INTEGER_2)},
            name(HADOOP_TYPE_DECIMAL, '9', '0'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, '9', '0'), HADOOP_TYPE_DECIMAL,
                                  data_precision=9, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, '9', '0'), GLUENT_TYPE_INTEGER_4)},
            name(HADOOP_TYPE_DECIMAL, '18', '0'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, '18', '0'), HADOOP_TYPE_DECIMAL,
                                  data_precision=18, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, '18', '0'), GLUENT_TYPE_INTEGER_8)},
            # Trimmed down to DECIMAL(36) because cx_Oracle has issues beyond that
            name(HADOOP_TYPE_DECIMAL, '36', '0'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, '36', '0'), HADOOP_TYPE_DECIMAL,
                                  data_precision=36, data_scale=0),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, '36', '0'), GLUENT_TYPE_INTEGER_38)},
            name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1), HADOOP_TYPE_DECIMAL,
                                  data_precision=2),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1), GLUENT_TYPE_INTEGER_1),
                 'present_options': {'integer_1_columns_csv': name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1)}},
            name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2), HADOOP_TYPE_DECIMAL,
                                  data_precision=4),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2), GLUENT_TYPE_INTEGER_2),
                 'present_options': {'integer_2_columns_csv': name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2)}},
            name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4), HADOOP_TYPE_DECIMAL,
                                  data_precision=9),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4), GLUENT_TYPE_INTEGER_4),
                 'present_options': {'integer_4_columns_csv': name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4)}},
            name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8), HADOOP_TYPE_DECIMAL,
                                  data_precision=18),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8), GLUENT_TYPE_INTEGER_8),
                 'present_options': {'integer_8_columns_csv': name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8)}},
            name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38), HADOOP_TYPE_DECIMAL,
                                  data_precision=38),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38), GLUENT_TYPE_INTEGER_38),
                 'present_options': {'integer_38_columns_csv': name(HADOOP_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38)}},
            name(HADOOP_TYPE_DECIMAL):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DECIMAL), HADOOP_TYPE_DECIMAL),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DECIMAL), GLUENT_TYPE_DECIMAL)},
            name(HADOOP_TYPE_DOUBLE):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DOUBLE), HADOOP_TYPE_DOUBLE),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DOUBLE), GLUENT_TYPE_DOUBLE)},
            name(HADOOP_TYPE_DOUBLE, GLUENT_TYPE_DECIMAL):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_DOUBLE, GLUENT_TYPE_DECIMAL), HADOOP_TYPE_DOUBLE),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_DOUBLE, GLUENT_TYPE_DECIMAL), GLUENT_TYPE_DECIMAL),
                 'present_options': {'decimal_columns_csv_list': [name(HADOOP_TYPE_DOUBLE, GLUENT_TYPE_DECIMAL)],
                                     'decimal_columns_type_list': ['38,18']}},
            name(HADOOP_TYPE_FLOAT):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_FLOAT), HADOOP_TYPE_FLOAT),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_FLOAT), GLUENT_TYPE_FLOAT)},
            name(HADOOP_TYPE_FLOAT, GLUENT_TYPE_DECIMAL):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_FLOAT, GLUENT_TYPE_DECIMAL), HADOOP_TYPE_FLOAT),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_FLOAT, GLUENT_TYPE_DECIMAL), GLUENT_TYPE_DECIMAL),
                 'present_options': {'decimal_columns_csv_list': [name(HADOOP_TYPE_FLOAT, GLUENT_TYPE_DECIMAL)],
                                     'decimal_columns_type_list': ['38,18']}},
            name(HADOOP_TYPE_INT):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_INT), HADOOP_TYPE_INT),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_INT), GLUENT_TYPE_INTEGER_4)},
            name(HADOOP_TYPE_SMALLINT):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_SMALLINT), HADOOP_TYPE_SMALLINT),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_SMALLINT), GLUENT_TYPE_INTEGER_2)},
            name(HADOOP_TYPE_STRING):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING), GLUENT_TYPE_VARIABLE_STRING)},
            name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING), GLUENT_TYPE_LARGE_STRING),
                 'present_options': {'large_string_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING)}},
            name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN),
                                  HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN),
                                     GLUENT_TYPE_LARGE_STRING),
                 'present_options': {'large_string_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING,
                                                                      UNICODE_NAME_TOKEN),
                                     'unicode_string_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_STRING,
                                                                        UNICODE_NAME_TOKEN)}},
            name(HADOOP_TYPE_STRING, GLUENT_TYPE_BINARY):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_BINARY), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_BINARY), GLUENT_TYPE_BINARY),
                 'present_options': {'binary_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_BINARY)}},
            name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_BINARY):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_BINARY), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_BINARY), GLUENT_TYPE_LARGE_BINARY),
                 'present_options': {'large_binary_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_LARGE_BINARY)}},
            name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_DS):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_DS), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_DS), GLUENT_TYPE_INTERVAL_DS),
                 'present_options': {'interval_ds_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_DS)}},
            name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_YM):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_YM), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_YM), GLUENT_TYPE_INTERVAL_YM),
                 'present_options': {'interval_ym_columns_csv': name(HADOOP_TYPE_STRING, GLUENT_TYPE_INTERVAL_YM)}},
            name(HADOOP_TYPE_STRING, UNICODE_NAME_TOKEN):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_STRING, UNICODE_NAME_TOKEN), HADOOP_TYPE_STRING),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_STRING, UNICODE_NAME_TOKEN), GLUENT_TYPE_VARIABLE_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'present_options': {'unicode_string_columns_csv': name(HADOOP_TYPE_STRING, UNICODE_NAME_TOKEN)}},
            name(HADOOP_TYPE_TIMESTAMP):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_TIMESTAMP), HADOOP_TYPE_TIMESTAMP),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_TIMESTAMP), GLUENT_TYPE_TIMESTAMP)},
            name(HADOOP_TYPE_TIMESTAMP, GLUENT_TYPE_DATE):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_TIMESTAMP, GLUENT_TYPE_DATE), HADOOP_TYPE_TIMESTAMP),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_TIMESTAMP, GLUENT_TYPE_DATE), GLUENT_TYPE_DATE),
                 'present_options': {'date_columns_csv': name(HADOOP_TYPE_TIMESTAMP, GLUENT_TYPE_DATE)}},
            name(HADOOP_TYPE_TINYINT):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_TINYINT), HADOOP_TYPE_TINYINT),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_TINYINT), GLUENT_TYPE_INTEGER_1)},
            name(HADOOP_TYPE_VARCHAR, '4000'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '4000'), HADOOP_TYPE_VARCHAR, data_length=4000),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '4001'), GLUENT_TYPE_VARIABLE_STRING)},
            name(HADOOP_TYPE_VARCHAR, '4001'):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '4001'), HADOOP_TYPE_VARCHAR, data_length=4001),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '4001'), GLUENT_TYPE_LARGE_STRING)},
            name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_BINARY):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_BINARY), HADOOP_TYPE_VARCHAR,
                                  data_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_BINARY), GLUENT_TYPE_BINARY),
                 'present_options':
                     {'binary_columns_csv': name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_BINARY)}},
            name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_BINARY):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_BINARY), HADOOP_TYPE_VARCHAR,
                                  data_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_BINARY),
                                     GLUENT_TYPE_LARGE_BINARY),
                 'present_options':
                     {'large_binary_columns_csv': name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_BINARY)}},
            name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING), HADOOP_TYPE_VARCHAR,
                                  data_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING),
                                     GLUENT_TYPE_LARGE_STRING),
                 'present_options':
                     {'large_string_columns_csv': name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING)}},
            name(HADOOP_TYPE_VARCHAR, '2000', UNICODE_NAME_TOKEN):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '2000', UNICODE_NAME_TOKEN), HADOOP_TYPE_VARCHAR,
                                  data_length=2000),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '2000', UNICODE_NAME_TOKEN), GLUENT_TYPE_VARIABLE_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'present_options':
                     {'unicode_string_columns_csv': name(HADOOP_TYPE_VARCHAR, '2000', UNICODE_NAME_TOKEN)}},
            name(HADOOP_TYPE_VARCHAR, '2001', UNICODE_NAME_TOKEN):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '2001', UNICODE_NAME_TOKEN), HADOOP_TYPE_VARCHAR,
                                  data_length=2001),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '2001', UNICODE_NAME_TOKEN), GLUENT_TYPE_VARIABLE_STRING,
                                     char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE),
                 'present_options': {
                     'unicode_string_columns_csv': name(HADOOP_TYPE_VARCHAR, '2001', UNICODE_NAME_TOKEN)}},
            name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN):
                {'column':
                     HadoopColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN),
                                  HADOOP_TYPE_VARCHAR, data_length=30),
                 'expected_canonical_column':
                     CanonicalColumn(name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN),
                                     GLUENT_TYPE_LARGE_STRING),
                 'present_options':
                     {'large_string_columns_csv': name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING,
                                                       UNICODE_NAME_TOKEN),
                      'unicode_string_columns_csv': name(HADOOP_TYPE_VARCHAR, '30', GLUENT_TYPE_LARGE_STRING,
                                                         UNICODE_NAME_TOKEN)}},
        }
        if self.canonical_date_supported():
            all_columns.update({
                name(HADOOP_TYPE_DATE): {'column': HadoopColumn(name(HADOOP_TYPE_DATE), HADOOP_TYPE_DATE),
                                         'expected_canonical_column':
                                             CanonicalColumn(name(HADOOP_TYPE_DATE), GLUENT_TYPE_DATE)},
                name(HADOOP_TYPE_DATE, GLUENT_TYPE_TIMESTAMP):
                    {'column': HadoopColumn(name(HADOOP_TYPE_DATE, GLUENT_TYPE_TIMESTAMP), HADOOP_TYPE_DATE),
                     'expected_canonical_column':
                         CanonicalColumn(name(HADOOP_TYPE_DATE, GLUENT_TYPE_TIMESTAMP), GLUENT_TYPE_TIMESTAMP),
                     'present_options': {'timestamp_columns_csv': name(HADOOP_TYPE_DATE, GLUENT_TYPE_TIMESTAMP)}}
            })
        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    def _sudo_hdfs_dfs(self, hdfs_dfs_options):
        cmd = ['/usr/bin/sudo', '-u', 'hdfs', 'hdfs', 'dfs'] + hdfs_dfs_options
        self._log('sudo_hdfs_dfs: %s' % cmd, detail=VERBOSE)
        returncode, output = subproc_cmd(cmd, self._connection_options, self._messages)
        if returncode != 0:
            self._log('Non-zero response: %s' % output)
            return False
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def create_backend_offload_location(self, gluent_user=None):
        raise NotImplementedError('create_backend_offload_location() not implemented for common Hadoop class')

    def create_partitioned_test_table(self, db_name, table_name, source_table_name,
                                      storage_format, compute_stats=False, filter_clauses=None):
        """ Common Hadoop version of create_partitioned_test_table()
        """
        source_cols = self._db_api.get_non_synthetic_columns(db_name, source_table_name)
        # Get insert tuples before we add partition column to the column list
        insert_col_tuples = [(_.name, _.name) for _ in source_cols]
        source_cols.append(self.gen_column_object('YEARMON', data_type=HADOOP_TYPE_INT))
        partition_column_names = ['YEARMON']
        partition_source_column = self._find_source_column_for_create_partitioned_test_table(source_cols)
        extract_year = "LPAD(CAST(EXTRACT(YEAR FROM %s) * 100 AS STRING),6,'0')" % partition_source_column.name
        extract_month = "LPAD(CAST(EXTRACT(MONTH FROM %s) AS STRING),2,'0')" % partition_source_column.name
        partition_col_tuples = [('CAST(CONCAT(%s, %s) AS %s)'
                                 % (extract_year, extract_month, HADOOP_TYPE_INT),
                                 'YEARMON')]
        table_properties, external_table = self._create_table_properties()
        cmds = self._db_api.create_table(db_name, table_name, source_cols, partition_column_names,
                                         storage_format=storage_format, external=external_table,
                                         table_properties=table_properties)
        cmds.extend(self.insert_table_as_select(db_name, table_name, db_name, source_table_name, insert_col_tuples,
                                                partition_col_tuples, compute_stats=compute_stats,
                                                filter_clauses=filter_clauses))
        return cmds

    def backend_test_type_canonical_date(self):
        if self.canonical_date_supported():
            return HADOOP_TYPE_DATE
        else:
            return HADOOP_TYPE_TIMESTAMP

    def backend_test_type_canonical_decimal(self):
        return HADOOP_TYPE_DECIMAL

    def backend_test_type_canonical_int_2(self):
        return HADOOP_TYPE_SMALLINT

    def backend_test_type_canonical_int_4(self):
        return HADOOP_TYPE_INT

    def backend_test_type_canonical_int_8(self):
        return HADOOP_TYPE_BIGINT

    def backend_test_type_canonical_int_38(self):
        return HADOOP_TYPE_DECIMAL

    def backend_test_type_canonical_time(self):
        raise NotImplementedError('backend_test_type_canonical_time() is not implemented for Hadoop')

    def backend_test_type_canonical_timestamp(self):
        return HADOOP_TYPE_TIMESTAMP

    def backend_test_type_canonical_timestamp_tz(self):
        return HADOOP_TYPE_TIMESTAMP

    def drop_database(self, db_name, cascade=False):
        assert db_name
        drop_sql = 'DROP DATABASE IF EXISTS %s' % (self.enclose_identifier(db_name))
        if cascade:
            drop_sql += ' CASCADE'
        return self.execute_ddl(drop_sql)

    def expected_backend_column(self, canonical_column, override_used=None, decimal_padding_digits=None):
        expected_data_type = self.expected_canonical_to_backend_type_map(
            override_used=override_used
        ).get(canonical_column.data_type)
        expected_precision_scale = self.expected_backend_precision_scale(canonical_column,
                                                                         decimal_padding_digits=decimal_padding_digits)
        if expected_precision_scale:
            return HadoopColumn(canonical_column.name, expected_data_type,
                                data_precision=expected_precision_scale[0],
                                data_scale=expected_precision_scale[1])
        else:
            return HadoopColumn(canonical_column.name, expected_data_type)

    def expected_backend_precision_scale(self, canonical_column, decimal_padding_digits=None):
        if canonical_column.data_type == GLUENT_TYPE_DECIMAL:
            # On Hadoop we have decimal and scale padding. We don't want to re-use the production logic here
            # because then we're not testing the logic is right. So we have a crude approximation.
            if canonical_column.data_precision is None and canonical_column.data_scale is None:
                # We can't check this because these columns are sampled and have an unreliable spec
                return None
            if canonical_column.data_scale == 0 or canonical_column.data_precision == self.max_decimal_precision():
                expected_precision = canonical_column.data_precision
                expected_scale = canonical_column.data_scale
            else:
                # Round scale up to even
                expected_precision = canonical_column.data_precision + (canonical_column.data_scale % 2)
                expected_scale = canonical_column.data_scale + (canonical_column.data_scale % 2)
                # Add padding to scale
                if decimal_padding_digits and canonical_column.data_precision < self.max_decimal_precision():
                    expected_scale = expected_scale + decimal_padding_digits
            if canonical_column.data_precision < self.max_decimal_precision():
                # Round precision up to either 18 or 38
                if canonical_column.data_precision:
                    expected_precision = 18
                else:
                    expected_precision = 38
            return expected_precision, expected_scale
        elif canonical_column.data_type == GLUENT_TYPE_INTEGER_38:
            return 38, 0
        else:
            return None

    def expected_customers_offload_predicates(self):
        return [
            ('column(cust_id) IS NULL', '`CUST_ID` IS NULL'),
            ('column(cust_id) IS NOT NULL', '`CUST_ID` IS NOT NULL'),
            ('column(cust_id) > numeric(4)', '`CUST_ID` > 4'),
            ('(column(CUST_ID) = numeric(10)) AND (column(CUST_ID) < numeric(2.2))',
             '(`CUST_ID` = 10 AND `CUST_ID` < 2.2)'),
            ('(column(CUST_ID) = numeric(10)) AND (column(CUST_ID) IS NULL)',
             '(`CUST_ID` = 10 AND `CUST_ID` IS NULL)'),
            ('column(CUST_CITY) = string("Oxford")', '`CUST_CITY` = \'Oxford\''),
            ('column(CUST_EFF_FROM) = datetime(1970-01-01)', '`CUST_EFF_FROM` = \'1970-01-01\''),
            ('column(CUST_EFF_FROM) = datetime(1970-01-01 12:13:14)',
             '`CUST_EFF_FROM` = \'1970-01-01 12:13:14\''),
        ]

    def expected_customers_synthetic_offload_predicates(self):
        date_column = 'CUST_EFF_FROM'
        number_column = 'CUST_ID'
        string_column = 'CUST_POSTAL_CODE'
        return [
            ((date_column, 'D', None), [
                ('column(CUST_EFF_FROM) = datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` = \'2010-01-01\' AND `GL_PART_D_CUST_EFF_FROM` = \'2010-01-01\')'),
                ('datetime(2010-01-01) = column(CUST_EFF_FROM)',
                 '(\'2010-01-01\' = `CUST_EFF_FROM` AND \'2010-01-01\' = `GL_PART_D_CUST_EFF_FROM`)'),
                ('column(CUST_EFF_FROM) != datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` != \'2010-01-01\' AND `GL_PART_D_CUST_EFF_FROM` != \'2010-01-01\')'),
                ('column(CUST_EFF_FROM) < datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` < \'2010-01-01\' AND `GL_PART_D_CUST_EFF_FROM` < \'2010-01-01\')'),
                ('column(CUST_EFF_FROM) <= datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` <= \'2010-01-01\' AND `GL_PART_D_CUST_EFF_FROM` <= \'2010-01-01\')'),
                ('column(CUST_EFF_FROM) > datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` > \'2010-01-01\' AND `GL_PART_D_CUST_EFF_FROM` > \'2010-01-01\')'),
                ('column(CUST_EFF_FROM) >= datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` >= \'2010-01-01\' AND `GL_PART_D_CUST_EFF_FROM` >= \'2010-01-01\')'),
            ]),
            ((date_column, 'M', None), [
                ('column(CUST_EFF_FROM) = datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` = \'2010-01-01\' AND `GL_PART_M_CUST_EFF_FROM` = \'2010-01\')'),
                ('datetime(2010-01-01) = column(CUST_EFF_FROM)',
                 '(\'2010-01-01\' = `CUST_EFF_FROM` AND \'2010-01\' = `GL_PART_M_CUST_EFF_FROM`)'),
            ]),
            ((date_column, 'Y', None), [
                ('column(CUST_EFF_FROM) = datetime(2010-01-01)',
                 '(`CUST_EFF_FROM` = \'2010-01-01\' AND `GL_PART_Y_CUST_EFF_FROM` = \'2010\')'),
                ('datetime(2010-01-01) = column(CUST_EFF_FROM)',
                 '(\'2010-01-01\' = `CUST_EFF_FROM` AND \'2010\' = `GL_PART_Y_CUST_EFF_FROM`)'),
            ]),
            ((number_column, '1000', 10), [
                ('column(CUST_ID) = numeric(17)',
                 '(`CUST_ID` = 17 AND `GL_PART_0000001000_CUST_ID` = \'0000000000\')'),
                ('column(CUST_ID) = numeric(1700)',
                 '(`CUST_ID` = 1700 AND `GL_PART_0000001000_CUST_ID` = \'0000001000\')'),
            ]),
            ((number_column, '10000', 12), [
                ('column(CUST_ID) = numeric(170)',
                 '(`CUST_ID` = 170 AND `GL_PART_000000010000_CUST_ID` = \'000000000000\')'),
                ('column(CUST_ID) = numeric(17000)',
                 '(`CUST_ID` = 17000 AND `GL_PART_000000010000_CUST_ID` = \'000000010000\')'),
            ]),
            ((string_column, '1', None), [
                ('column(CUST_POSTAL_CODE) = string("123456")',
                 '(`CUST_POSTAL_CODE` = \'123456\' AND `GL_PART_1_CUST_POSTAL_CODE` = \'1\')'),
                ('column(CUST_POSTAL_CODE) = string("")',
                 '(`CUST_POSTAL_CODE` = \'\' AND `GL_PART_1_CUST_POSTAL_CODE` = \'\')'),
            ]),
            ((string_column, '2', None), [
                ('column(CUST_POSTAL_CODE) = string("123456")',
                 '(`CUST_POSTAL_CODE` = \'123456\' AND `GL_PART_2_CUST_POSTAL_CODE` = \'12\')'),
                ('column(CUST_POSTAL_CODE) = string("")',
                 '(`CUST_POSTAL_CODE` = \'\' AND `GL_PART_2_CUST_POSTAL_CODE` = \'\')'),
            ]),
        ]

    def gl_type_mapping_generated_table_col_specs(self):
        definitions = self._gl_type_mapping_column_definitions()
        gl_type_mapping_cols, gl_type_mapping_names = [], []
        for col_dict in [definitions[col_name] for col_name in sorted(definitions.keys())]:
            backend_column = col_dict['column']
            gl_type_mapping_names.append(backend_column.name)
            if backend_column.data_type == HADOOP_TYPE_DECIMAL and (backend_column.data_precision is not None
                                                                    or backend_column.data_scale is not None):
                gl_type_mapping_cols.append({'column': backend_column})
            elif (backend_column.data_type == HADOOP_TYPE_DECIMAL
                  and backend_column.data_precision is None
                  and backend_column.data_scale is None):
                # Hadoop DECIMAL defaults to DECIMAL(9,0). Include small literals here to avoid complications later.
                gl_type_mapping_cols.append({'column': backend_column, 'literals': [100, 200, 300, 400]})
            elif col_dict['expected_canonical_column'].data_type == GLUENT_TYPE_INTERVAL_DS:
                gl_type_mapping_cols.append({'column': backend_column,
                                             'literals': self._gl_type_mapping_interval_ds_test_values()})
            elif col_dict['expected_canonical_column'].data_type == GLUENT_TYPE_INTERVAL_YM:
                gl_type_mapping_cols.append({'column': backend_column,
                                             'literals': self._gl_type_mapping_interval_ym_test_values()})
            elif backend_column.is_string_based():
                gl_type_mapping_cols.append({'column': backend_column})
            else:
                gl_type_mapping_cols.append({'column': backend_column})
        return gl_type_mapping_cols, gl_type_mapping_names

    def host_compare_sql_projection(self, column_list: list) -> str:
        """ Return a SQL projection (CSV of column expressions) used to validate offloaded data.
            Because of systems variations all date based values must be normalised to:
                'YYYY-MM-DD HH24:MI:SS.FFF TZH:TZM'.
        """
        assert isinstance(column_list, list)
        projection = []
        for column in column_list:
            if column.is_date_based():
                projection.append("concat(from_unixtime(unix_timestamp(({}, 'yyyy-MM-dd HH:mm:ss.sss'),' +00:00'".format(
                    self._db_api.enclose_identifier(column.name)))
            elif column.is_number_based():
                projection.append(
                    "CAST({} AS STRING)".format(self._db_api.enclose_identifier(column.name)))
            else:
                projection.append(self._db_api.enclose_identifier(column.name))
        return ','.join(projection)

    def load_table_fs_scheme_is_correct(self, load_db, table_name):
        """ Hadoop load tables should always be in HDFS
        """
        self._log('load_table_fs_scheme_is_correct(%s, %s)' % (load_db, table_name), detail=VERBOSE)
        location = self.get_table_location(load_db, table_name)
        scheme = get_scheme_from_location_uri(location) if location else None
        self._log('Identified scheme: %s' % scheme, detail=VERBOSE)
        return bool(scheme in (OFFLOAD_FS_SCHEME_HDFS, OFFLOAD_FS_SCHEME_MAPRFS))

    def rename_column(self, db_name, table_name, column_name, new_name, sync=None):
        """ Issue SQL rename column command for Hadoop backends.
            Hive/Impala require data type when renaming a column.
        """
        assert db_name and table_name
        assert column_name and new_name
        assert isinstance(column_name, str)
        assert isinstance(new_name, str)

        data_type = [_.format_data_type() for _ in self.get_columns(db_name, table_name)
                     if _.name.lower() == column_name.lower()]
        if not data_type:
            raise BackendTestingApiException('Table %s.%s does not have a column %s to rename'
                                             % (db_name, table_name, column_name))
        data_type = data_type[0]
        sql = 'ALTER TABLE %(db_name)s.%(table_name)s CHANGE %(orig_name)s %(new_name)s %(data_type)s'\
              % {'db_name': self.enclose_identifier(db_name), 'table_name': self.enclose_identifier(table_name),
                 'orig_name': self.enclose_identifier(column_name), 'new_name': self.enclose_identifier(new_name),
                 'data_type': data_type.upper()}
        return self.execute_ddl(sql, sync=sync)

    def select_single_non_null_value(self, db_name, table_name, column_name, project_expression):
        val = self._select_single_non_null_value_common(db_name, table_name, column_name, project_expression)
        column = self.get_column(db_name, table_name, column_name)
        if column.data_type == HADOOP_TYPE_DATE and isinstance(val, str):
            # This is a backend DATE which we need to convert to datetime.date because of Impyla issue 410:
            # https://github.com/cloudera/impyla/issues/410
            val = datetime.strptime(val, '%Y-%m-%d').date()
        return val

    def smart_connector_test_command(self, db_name=None, table_name=None):
        return 'show databases'

    def story_test_offload_nums_expected_backend_types(self, sampling_enabled=True):
        non_sampled_type = self.gen_default_numeric_column('x').format_data_type()
        return {STORY_TEST_OFFLOAD_NUMS_BARE_NUM: 'decimal(18,6)' if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_BARE_FLT: HADOOP_TYPE_BIGINT if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_NUM_4: HADOOP_TYPE_BIGINT,
                STORY_TEST_OFFLOAD_NUMS_NUM_18: HADOOP_TYPE_BIGINT,
                STORY_TEST_OFFLOAD_NUMS_NUM_19: 'decimal(38,0)',
                STORY_TEST_OFFLOAD_NUMS_NUM_3_2: 'decimal(18,4)',
                STORY_TEST_OFFLOAD_NUMS_NUM_13_3: 'decimal(18,6)',
                STORY_TEST_OFFLOAD_NUMS_NUM_16_1: 'decimal(38,4)',
                STORY_TEST_OFFLOAD_NUMS_NUM_20_5: 'decimal(38,8)',
                STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: 'decimal(38,4)',
                STORY_TEST_OFFLOAD_NUMS_NUM_3_5: 'decimal(18,8)',
                STORY_TEST_OFFLOAD_NUMS_NUM_10_M5: HADOOP_TYPE_BIGINT,
                STORY_TEST_OFFLOAD_NUMS_DEC_10_0: 'decimal(18,2)' if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_DEC_13_9: 'decimal(18,12)' if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_DEC_15_9: 'decimal(38,12)' if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_DEC_36_3: 'decimal(38,4)' if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_DEC_37_3: 'decimal(38,4)' if sampling_enabled else non_sampled_type,
                STORY_TEST_OFFLOAD_NUMS_DEC_38_3: 'decimal(38,3)' if sampling_enabled else non_sampled_type}

    def story_test_table_extra_col_info(self):
        """ Return a dict describing extra columns we can tag onto a present test table in order to test
            data type controls/outcomes.
            The returned dict is keyed by column name and has the following fields for each column:
                'sql_expression': A backend SQL expression to generate test data and "type" the column
                'length': The length of any string column
                'precision': Precision for number columns that support it
                'scale': Scale for number columns that support it
        """
        extra_cols = {
            STORY_TEST_BACKEND_DOUBLE_COL: {'sql_expression': 'CAST(123.123 AS %s)' % HADOOP_TYPE_DOUBLE},
            STORY_TEST_BACKEND_DECIMAL_PS_COL: {'sql_expression': 'CAST(123.123 AS %s(10,3))'
                                                                  % HADOOP_TYPE_DECIMAL, 'precision': 10, 'scale': 3},
            STORY_TEST_BACKEND_DECIMAL_DEF_COL: {'sql_expression': 'CAST(123.123 AS %s(38,18))'
                                                                   % HADOOP_TYPE_DECIMAL, 'precision': 38, 'scale': 18},
            STORY_TEST_BACKEND_INT_1_COL: {'sql_expression': 'CAST(1 AS %s)' % HADOOP_TYPE_TINYINT},
            STORY_TEST_BACKEND_INT_2_COL: {'sql_expression': 'CAST(1234 AS %s)' % HADOOP_TYPE_SMALLINT},
            STORY_TEST_BACKEND_INT_4_COL: {'sql_expression': 'CAST(123456 AS %s)' % HADOOP_TYPE_INT},
            STORY_TEST_BACKEND_INT_8_COL: {'sql_expression': 'CAST(1234567890123 AS %s)' % HADOOP_TYPE_BIGINT},
            STORY_TEST_BACKEND_VAR_STR_COL: {'sql_expression': "CAST('this is varchar' AS %s(50))"
                                                               % HADOOP_TYPE_VARCHAR, 'length': 50},
            STORY_TEST_BACKEND_VAR_STR_LONG_COL: {'sql_expression': "CAST('very long varchar' AS %s(5000))"
                                                                    % HADOOP_TYPE_VARCHAR, 'length': 5000},
            STORY_TEST_BACKEND_FIX_STR_COL: {'sql_expression': "CAST('this is char' AS %s(15))"
                                                               % HADOOP_TYPE_CHAR, 'length': 15},
            STORY_TEST_BACKEND_TIMESTAMP_COL: {'sql_expression': 'CURRENT_TIMESTAMP()'},
            STORY_TEST_BACKEND_NULL_STR_COL: {'sql_expression': 'CAST(NULL AS %s)' % HADOOP_TYPE_STRING}
        }
        if self.canonical_date_supported():
            extra_cols[STORY_TEST_BACKEND_DATE_COL] = {'sql_expression': 'CURRENT_DATE()'}
        return extra_cols

    def transient_query_error_identification_strings(self) -> list:
        """ No additional known transient errors on Hadoop """
        return self._transient_query_error_identification_global_strings()
