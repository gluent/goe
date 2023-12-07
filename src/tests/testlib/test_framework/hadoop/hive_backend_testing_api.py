#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" Hive implementation of BackendTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests.
    LICENSE_TEXT
"""

import logging

from goe.offload.column_metadata import CanonicalColumn,\
    GLUENT_TYPE_FIXED_STRING, GLUENT_TYPE_LARGE_STRING,\
    GLUENT_TYPE_VARIABLE_STRING, GLUENT_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY, GLUENT_TYPE_INTEGER_1,\
    GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8, GLUENT_TYPE_INTEGER_38, GLUENT_TYPE_DECIMAL,\
    GLUENT_TYPE_FLOAT, GLUENT_TYPE_DOUBLE, GLUENT_TYPE_DATE, GLUENT_TYPE_TIME, GLUENT_TYPE_TIMESTAMP,\
    GLUENT_TYPE_TIMESTAMP_TZ, GLUENT_TYPE_INTERVAL_DS, GLUENT_TYPE_INTERVAL_YM, GLUENT_TYPE_BOOLEAN
from goe.offload.hadoop.hadoop_backend_api import hive_enable_dynamic_partitions_for_insert_sqls
from goe.offload.hadoop.hadoop_column import HadoopColumn
from goe.offload.offload_messages import NORMAL, VVERBOSE
from goe.util.better_impyla import HADOOP_TYPE_BIGINT, HADOOP_TYPE_BINARY, HADOOP_TYPE_BOOLEAN, HADOOP_TYPE_DATE,\
    HADOOP_TYPE_DECIMAL, HADOOP_TYPE_DOUBLE, HADOOP_TYPE_FLOAT,\
    HADOOP_TYPE_INT, HADOOP_TYPE_SMALLINT, HADOOP_TYPE_STRING, HADOOP_TYPE_TIMESTAMP, HADOOP_TYPE_TINYINT
from tests.testlib.test_framework.hadoop.hadoop_backend_testing_api import BackendHadoopTestingApi, BackendTestingApiException


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
# BackendHiveTestingApi
###########################################################################

class BackendHiveTestingApi(BackendHadoopTestingApi):
    """ Hive implementation
        Assumes remote system talks HiveQL via HS2
    """

    def __init__(self, connection_options, backend_type, messages, dry_run=False, no_caching=False, do_not_connect=False):
        """ CONSTRUCTOR
        """
        super(BackendHiveTestingApi, self).__init__(connection_options, backend_type, messages, dry_run=dry_run,
                                                    no_caching=no_caching, do_not_connect=do_not_connect)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _create_table_properties(self):
        return None, False

    def _define_test_partition_function(self, udf_name):
        raise NotImplementedError('_define_test_partition_function() not implemented for Hive')

    def _gl_type_mapping_column_definitions(self, filter_column=None):
        """ Returns a dict of dicts defining columns for GL_BACKEND_TYPE_MAPPING test table.
            filter_column can be used to fetch just a single column dict.
        """

        def name(*args):
            return self._gl_type_mapping_column_name(*args)

        all_columns = super(BackendHiveTestingApi, self)._gl_type_mapping_column_definitions(filter_column=filter_column)
        all_columns.update(
            {name(HADOOP_TYPE_BINARY):
                 {'column': HadoopColumn(name(HADOOP_TYPE_BINARY), HADOOP_TYPE_BINARY),
                  'expected_canonical_column':
                      CanonicalColumn(name(HADOOP_TYPE_BINARY), GLUENT_TYPE_BINARY)},
             name(HADOOP_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY):
                 {'column': HadoopColumn(name(HADOOP_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY), HADOOP_TYPE_BINARY),
                  'expected_canonical_column':
                      CanonicalColumn(name(HADOOP_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY), GLUENT_TYPE_LARGE_BINARY),
                  'present_options': {'large_binary_columns_csv': name(HADOOP_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY)}},
            })
        # When GOE-1458 is complete we can remove the del of HADOOP_TYPE_DATE below
        if name(HADOOP_TYPE_DATE) in all_columns:
            del(all_columns[name(HADOOP_TYPE_DATE)])
        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_test_type_canonical_time(self):
        raise NotImplementedError('backend_test_type_canonical_time() is not implemented for Hive')

    def create_backend_offload_location(self, gluent_user=None):
        """ Create HDFS_HOME and HDFS_DATA for Hive
        """
        gluent_user = gluent_user or 'gluent'
        user_dir = '/user/%s' % gluent_user
        data_dir = user_dir + '/offload'
        self._sudo_hdfs_dfs(['-mkdir', '-p', data_dir])
        self._sudo_hdfs_dfs(['-chown', gluent_user, user_dir])
        self._sudo_hdfs_dfs(['-chown', '%s:%s' % (gluent_user, 'hadoop'), data_dir])
        self._sudo_hdfs_dfs(['-chmod', 'g+w', data_dir])

    def create_table_as_select(self, db_name, table_name, storage_format, column_tuples, from_db_name=None, from_table_name=None, row_limit=None, compute_stats=None):
        """ CTAS a table on Hive.
            We need to explicitly set hive.stats.autogather if compute_stats is not None.
        """
        query_options = {}
        sql = self._db_api.gen_ctas_sql_text(db_name, table_name, storage_format, column_tuples, from_db_name=from_db_name, from_table_name=from_table_name, row_limit=row_limit)
        if compute_stats is not None:
            query_options = {'hive.stats.autogather': 'true' if compute_stats else 'false'}
        return self.execute_ddl(sql, query_options=query_options)

    def drop_column(self, db_name, table_name, column_name, sync=None):
        """ Hive doesn't have drop column so we need to recreate the table without the column.
            This doesn't work if trying to drop the last column in the table (i.e. the right-most column), because this is
            test only I'm (NJ) not investing too much time in this just now (2019-11-27).
        """
        assert db_name and table_name and column_name
        assert isinstance(column_name, str)

        tmp_table_name = table_name + '_drop_test_tmp'
        orig_table_ddl = self._db_api.get_table_ddl(db_name, table_name, as_list=True)
        # remove the column from the DDL and stitch it together as a string
        create_tmp_ddl = '\n'.join(_ for _ in orig_table_ddl if column_name.lower() not in _.lower())

        # switch the table name
        def find_and_replace_owner_table(owner_table):
            if owner_table in create_tmp_ddl:
                return create_tmp_ddl.replace(owner_table, db_name + '.' + tmp_table_name)
            else:
                return None
        new_create_tmp_ddl = find_and_replace_owner_table(self.enclose_identifier(db_name + '.' + table_name))
        if not new_create_tmp_ddl:
            new_create_tmp_ddl = find_and_replace_owner_table(self.enclose_identifier(db_name) + '.' + self.enclose_identifier(table_name))
        if not new_create_tmp_ddl:
            new_create_tmp_ddl = find_and_replace_owner_table(db_name + '.' + table_name)
        if not new_create_tmp_ddl:
            raise BackendTestingApiException('Cannot match original table name %s.%s in DDL: %s' % (db_name, table_name, create_tmp_ddl))
        # replace any table name entry in a filesystem location
        new_create_tmp_ddl = new_create_tmp_ddl.replace('/' + table_name, '/' + tmp_table_name)

        new_create_tmp_ddl = self._get_create_table_ddl_remove_spark_props(new_create_tmp_ddl)

        orig_cols = self.get_columns(db_name, table_name)
        part_col_list = self._db_api.get_partition_columns(db_name, table_name)
        part_col_names = [_.name.lower() for _ in part_col_list]
        self._debug('Extracted partition columns: %s' % str(part_col_names))
        # Omit the dropped column and partition columns
        new_cols = [_.name for _ in orig_cols if column_name.lower() != _.name.lower() and _.name.lower() not in part_col_names]
        self._debug('Reduced columns to: %s' % str(new_cols))

        executed_sqls = []
        executed_sqls.extend(self._db_api.drop_table(db_name, tmp_table_name))
        executed_sqls.extend(self.execute_ddl(new_create_tmp_ddl))
        executed_sqls.extend(self.insert_table_as_select(db_name, tmp_table_name, db_name, table_name, [(_, _) for _ in new_cols], [(_, _) for _ in part_col_names]))
        executed_sqls.extend(self.drop_table(db_name, table_name))
        executed_sqls.extend(self._db_api.rename_table(db_name, tmp_table_name, db_name, table_name))
        return executed_sqls

    def expected_canonical_to_backend_type_map(self, override_used=None):
        bigint_override = HADOOP_TYPE_BIGINT if 'integer_8_columns_csv' in (override_used or {}) else None
        return {GLUENT_TYPE_FIXED_STRING: HADOOP_TYPE_STRING,
                GLUENT_TYPE_LARGE_STRING: HADOOP_TYPE_STRING,
                GLUENT_TYPE_VARIABLE_STRING: HADOOP_TYPE_STRING,
                GLUENT_TYPE_BINARY: HADOOP_TYPE_BINARY,
                GLUENT_TYPE_LARGE_BINARY: HADOOP_TYPE_BINARY,
                GLUENT_TYPE_INTEGER_1: bigint_override or HADOOP_TYPE_TINYINT,
                GLUENT_TYPE_INTEGER_2: bigint_override or HADOOP_TYPE_SMALLINT,
                GLUENT_TYPE_INTEGER_4: bigint_override or HADOOP_TYPE_INT,
                GLUENT_TYPE_INTEGER_8: HADOOP_TYPE_BIGINT,
                GLUENT_TYPE_INTEGER_38: HADOOP_TYPE_DECIMAL,
                GLUENT_TYPE_DECIMAL: HADOOP_TYPE_DECIMAL,
                GLUENT_TYPE_FLOAT: HADOOP_TYPE_FLOAT,
                GLUENT_TYPE_DOUBLE: HADOOP_TYPE_DOUBLE,
                GLUENT_TYPE_DATE: HADOOP_TYPE_DATE,
                GLUENT_TYPE_TIME: HADOOP_TYPE_STRING,
                GLUENT_TYPE_TIMESTAMP: HADOOP_TYPE_TIMESTAMP,
                GLUENT_TYPE_TIMESTAMP_TZ: HADOOP_TYPE_TIMESTAMP,
                GLUENT_TYPE_INTERVAL_DS: HADOOP_TYPE_STRING,
                GLUENT_TYPE_INTERVAL_YM: HADOOP_TYPE_STRING,
                GLUENT_TYPE_BOOLEAN: HADOOP_TYPE_BOOLEAN}

    def insert_table_as_select(self, db_name, table_name, from_db_name, from_table_name, select_expr_tuples, partition_expr_tuples, compute_stats=None, filter_clauses=None):
        insert_sql = self._db_api.gen_insert_select_sql_text(db_name, table_name, from_db_name, from_table_name, select_expr_tuples, partition_expr_tuples, filter_clauses=filter_clauses)
        query_options = hive_enable_dynamic_partitions_for_insert_sqls(as_dict=True)
        if compute_stats is not None:
            query_options.update({'hive.stats.autogather': 'true' if compute_stats else 'false'})
        return self.execute_ddl(insert_sql, query_options=query_options, log_level=NORMAL)

    def partition_has_stats(self, db_name, table_name, partition_tuples, colstats=False):
        """ This code was moved from test suite, I don't fully understand the logic hence not put in
            BackendApi. If we come to need this functionality in production code then we should
            make this a wrapper for a fully understood version.
            Hive implementation.
        """
        assert db_name and table_name
        assert partition_tuples
        assert isinstance(partition_tuples, list) and isinstance(partition_tuples[0], (tuple, list))
        partition = self._db_api.format_hadoop_partition_clause(partition_tuples)
        self._debug('Testing partition has stats: %s' % partition)
        _, part_stats, col_stats = self._db_api.get_table_and_partition_stats(db_name, table_name, as_dict=False)
        if colstats:
            self._log('Checking column stats in: %s' % str(col_stats), detail=VVERBOSE)
            part_stat = [part[2] for part in col_stats if part[0].replace('"', '').replace(',', '/') == partition.replace('"', '')]
            if part_stat:
                if part_stat[0] > -1:
                    return True
        else:
            self._log('Checking partition stats in: %s' % str(part_stats), detail=VVERBOSE)
            part_stat = [part[1] for part in part_stats if part[0].replace('"', '').replace(',', '/') == partition.replace('"', '')]
            if part_stat:
                if part_stat[0] > -1:
                    return True
        return False

    def sql_median_expression(self, db_name, table_name, column_name):
        column = self.get_column(db_name, table_name, column_name)
        if column.is_string_based():
            return 'CHR(PERCENTILE(CAST(ASCII(%s) AS BIGINT), 0.5))' % self.enclose_identifier(column_name)
        elif column.is_date_based():
            # There is no function we can use so just accept the first value
            return column_name
        else:
            return 'PERCENTILE(CAST(%s AS BIGINT), 0.5)' % self.enclose_identifier(column_name)

    def unit_test_query_options(self):
        return {'hive.compute.query.using.stats': 'false'}
