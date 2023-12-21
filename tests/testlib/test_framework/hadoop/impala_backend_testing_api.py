#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" Impala implementation of BackendTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests.
    LICENSE_TEXT
"""

import logging

from goe.offload.column_metadata import (
    CanonicalColumn,
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
from goe.offload.hadoop.hadoop_column import (
    HadoopColumn,
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_BOOLEAN,
    HADOOP_TYPE_CHAR,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_TIMESTAMP,
    HADOOP_TYPE_TINYINT,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from tests.testlib.test_framework.hadoop.hadoop_backend_testing_api import (
    BackendHadoopTestingApi,
)


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
# BackendImpalaTestingApi
###########################################################################


class BackendImpalaTestingApi(BackendHadoopTestingApi):
    """Impala implementation"""

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
        super(BackendImpalaTestingApi, self).__init__(
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

    def _create_table_properties(self):
        if self._db_api.transactional_tables_default():
            return {"external.table.purge": "TRUE"}, True
        else:
            return None, False

    def _define_test_partition_function(self, udf_name):
        raise NotImplementedError(
            "_define_test_partition_function() not implemented for Impala"
        )

    def _gl_type_mapping_column_definitions(self, filter_column=None):
        """Returns a dict of dicts defining columns for GL_BACKEND_TYPE_MAPPING test table.
        filter_column can be used to fetch just a single column dict.
        """

        def name(*args):
            return self._gl_type_mapping_column_name(*args)

        all_columns = super(
            BackendImpalaTestingApi, self
        )._gl_type_mapping_column_definitions(filter_column=filter_column)
        all_columns.update(
            {
                name(HADOOP_TYPE_REAL): {
                    "column": HadoopColumn(name(HADOOP_TYPE_REAL), HADOOP_TYPE_REAL),
                    "expected_canonical_column": CanonicalColumn(
                        name(HADOOP_TYPE_REAL), GLUENT_TYPE_DOUBLE
                    ),
                },
                name(HADOOP_TYPE_REAL, GLUENT_TYPE_DECIMAL): {
                    "column": HadoopColumn(
                        name(HADOOP_TYPE_REAL, GLUENT_TYPE_DECIMAL), HADOOP_TYPE_REAL
                    ),
                    "expected_canonical_column": CanonicalColumn(
                        name(HADOOP_TYPE_REAL, GLUENT_TYPE_DECIMAL), GLUENT_TYPE_DECIMAL
                    ),
                    "present_options": {
                        "decimal_columns_csv_list": [
                            name(HADOOP_TYPE_REAL, GLUENT_TYPE_DECIMAL)
                        ],
                        "decimal_columns_type_list": ["38,18"],
                    },
                },
            }
        )
        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_test_type_canonical_time(self):
        raise NotImplementedError(
            "backend_test_type_canonical_time() is not implemented for Impala"
        )

    def create_backend_offload_location(self, gluent_user=None):
        """Create HDFS_HOME and HDFS_DATA for Impala"""
        gluent_user = gluent_user or "gluent"
        user_dir = "/user/%s" % gluent_user
        data_dir = user_dir + "/offload"
        self._sudo_hdfs_dfs(["-mkdir", "-p", data_dir])
        self._sudo_hdfs_dfs(["-chown", gluent_user, user_dir])
        self._sudo_hdfs_dfs(["-chown", "%s:%s" % (gluent_user, "hive"), data_dir])
        self._sudo_hdfs_dfs(["-chmod", "g+w", data_dir])

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
        """CTAS a table on Impala.
        We need to create an external table on CDP.
        """
        table_properties, external_table = self._create_table_properties()
        sql = self._db_api.gen_ctas_sql_text(
            db_name,
            table_name,
            storage_format,
            column_tuples,
            from_db_name=from_db_name,
            from_table_name=from_table_name,
            row_limit=row_limit,
            external=external_table,
            table_properties=table_properties,
        )
        executed_sqls = self.execute_ddl(sql)
        if compute_stats:
            executed_sqls.extend(self._db_api.compute_stats(db_name, table_name))
        return executed_sqls

    def drop_column(self, db_name, table_name, column_name, sync=None):
        """Utilise Impala DROP column statement."""
        assert db_name and table_name and column_name
        assert isinstance(column_name, str)
        sql = "ALTER TABLE %s DROP %s" % (
            self._db_api.enclose_object_reference(db_name, table_name),
            self.enclose_identifier(column_name),
        )
        return self.execute_ddl(sql, sync=sync)

    def expected_canonical_to_backend_type_map(self, override_used=None):
        tinyint_override = (
            HADOOP_TYPE_TINYINT
            if "integer_1_columns_csv" in (override_used or {})
            else None
        )
        smallint_override = (
            HADOOP_TYPE_SMALLINT
            if "integer_2_columns_csv" in (override_used or {})
            else None
        )
        int_override = (
            HADOOP_TYPE_INT
            if "integer_4_columns_csv" in (override_used or {})
            else None
        )
        return {
            GLUENT_TYPE_FIXED_STRING: HADOOP_TYPE_STRING,
            GLUENT_TYPE_LARGE_STRING: HADOOP_TYPE_STRING,
            GLUENT_TYPE_VARIABLE_STRING: HADOOP_TYPE_STRING,
            GLUENT_TYPE_BINARY: HADOOP_TYPE_STRING,
            GLUENT_TYPE_LARGE_BINARY: HADOOP_TYPE_STRING,
            GLUENT_TYPE_INTEGER_1: tinyint_override or HADOOP_TYPE_BIGINT,
            GLUENT_TYPE_INTEGER_2: smallint_override or HADOOP_TYPE_BIGINT,
            GLUENT_TYPE_INTEGER_4: int_override or HADOOP_TYPE_BIGINT,
            GLUENT_TYPE_INTEGER_8: HADOOP_TYPE_BIGINT,
            GLUENT_TYPE_INTEGER_38: HADOOP_TYPE_DECIMAL,
            GLUENT_TYPE_DECIMAL: HADOOP_TYPE_DECIMAL,
            GLUENT_TYPE_FLOAT: HADOOP_TYPE_FLOAT,
            GLUENT_TYPE_DOUBLE: HADOOP_TYPE_DOUBLE,
            GLUENT_TYPE_DATE: HADOOP_TYPE_DATE
            if self.canonical_date_supported()
            else HADOOP_TYPE_TIMESTAMP,
            GLUENT_TYPE_TIME: HADOOP_TYPE_STRING,
            GLUENT_TYPE_TIMESTAMP: HADOOP_TYPE_TIMESTAMP,
            GLUENT_TYPE_TIMESTAMP_TZ: HADOOP_TYPE_TIMESTAMP,
            GLUENT_TYPE_INTERVAL_DS: HADOOP_TYPE_STRING,
            GLUENT_TYPE_INTERVAL_YM: HADOOP_TYPE_STRING,
            GLUENT_TYPE_BOOLEAN: HADOOP_TYPE_BOOLEAN,
        }

    def partition_has_stats(
        self, db_name, table_name, partition_tuples, colstats=False
    ):
        """This code was moved from test suite, I don't fully understand the logic hence not put in
        BackendApi. If we come to need this functionality in production code then we should
        make this a wrapper for a fully understood version
        Impala implementation
        """
        assert db_name and table_name
        assert partition_tuples
        assert isinstance(partition_tuples, list) and isinstance(
            partition_tuples[0], (tuple, list)
        )
        partition = self._db_api.format_hadoop_partition_clause(partition_tuples)
        self._debug("Testing partition has stats: %s" % partition)
        _, part_stats, col_stats = self._db_api.get_table_and_partition_stats(
            db_name, table_name, as_dict=False
        )
        if colstats:
            self._log("Checking column stats in: %s" % str(col_stats), detail=VVERBOSE)
            if not col_stats:
                return False
            part_stat = col_stats[0]
            if part_stat:
                if part_stat[1] > 0:
                    return True
                else:
                    return False
        else:
            self._log(
                "Checking partition stats in: %s" % str(part_stats), detail=VVERBOSE
            )
            part_stat = [
                part[1]
                for part in part_stats
                if part[0].replace('"', "").replace(",", "/")
                == partition.replace('"', "")
            ]
            if part_stat:
                if part_stat[0] > -1:
                    return True
        return False

    def sql_median_expression(self, db_name, table_name, column_name):
        """Impala APPX_MEDIAN suits all data types."""
        return "APPX_MEDIAN(%s)" % self.enclose_identifier(column_name)

    def test_setup_seconds_delay(self):
        return 0

    def unit_test_query_options(self):
        return {"MEM_LIMIT": "0"}
