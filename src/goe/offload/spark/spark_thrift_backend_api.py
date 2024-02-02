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

""" BackendSparkThriftApi: BackendApi implementation for a Spark ThriftServer SQL backend.

    Spark ThriftServer is based on HiveQL and therefore subclassed from Hive
    This has complications, Hive assumes it is in Hadoop but Spark ThriftServer can be in Hadoop,
    with access to HDFS, or stand alone with no HDFS integration. For this reason we may, in the
    future, choose to subclass again differentiating between the two Spark ThriftServer types.
"""

import logging

from goe.offload.backend_api import (
    BackendApiException,
    REPORT_ATTR_BACKEND_CLASS,
    REPORT_ATTR_BACKEND_TYPE,
    REPORT_ATTR_BACKEND_DISPLAY_NAME,
    REPORT_ATTR_BACKEND_HOST_INFO_TYPE,
    REPORT_ATTR_BACKEND_HOST_INFO,
)
from goe.offload.hadoop.hadoop_backend_api import BackendHadoopApi
from goe.offload.hadoop.hive_backend_api import BackendHiveApi
from goe.offload.offload_constants import SPARK_BACKEND_CAPABILITIES
from goe.offload.offload_messages import VVERBOSE
from goe.util.hs2_connection import HS2_OPTIONS

###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###########################################################################
# BackendSparkThriftApi
###########################################################################


class BackendSparkThriftApi(BackendHiveApi):
    """Spark ThriftServer backend implementation, based on HiveQL and therefore subclassed from Hive"""

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
        # We want code from HiveBackendApi but not the constructor, for that we go right back to the interface
        super(BackendHadoopApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        self._sql_engine_name = "Spark"

        self._target_version = None
        self._spark_thrift_host = connection_options.offload_transport_spark_thrift_host
        self._spark_thrift_port = connection_options.offload_transport_spark_thrift_port

        if do_not_connect:
            self._hive_conn = None
        else:
            self._validate_connection_options(HS2_OPTIONS)
            self._hive_conn = self._connect_to_backend(
                host_override=self._spark_thrift_host,
                port_override=self._spark_thrift_port,
            )
            # Whenever we make a new connection we should set global session parameters
            self._execute_global_session_parameters()

        self._cached_hive_tables = {}
        self._cached_hive_table_stats = {}

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _backend_capabilities(self):
        return SPARK_BACKEND_CAPABILITIES

    def _format_storage_format_clause(self, storage_format):
        return "STORED AS %s" % storage_format

    def _get_hadoop_connection_exception_message_template(self):
        return """Successfully connected to %(host)s:%(port)s, but rejected. Possible reasons:

        1) Kerberos is configured for Impala, but not configured in your environment file (or vice versa)
        2) A process is listening at %(host)s:%(port)s, but it is not HiveServer2
        3) LDAP is configured for the ThriftServer, but incorrect%(enc_text)s credentials were supplied
        4) Invalid hiveserver2 authentication method is used"""

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_report_info(self):
        """Reporting information about the type of backend and how to display
        its configuration details (e.g. Hadoop host:port or BigQuery project).
        """
        return {
            REPORT_ATTR_BACKEND_CLASS: "Hadoop",
            REPORT_ATTR_BACKEND_TYPE: self._backend_type,
            REPORT_ATTR_BACKEND_DISPLAY_NAME: self.backend_db_name(),
            REPORT_ATTR_BACKEND_HOST_INFO_TYPE: "Host:port",
            REPORT_ATTR_BACKEND_HOST_INFO: "%s:%s"
            % (self._spark_thrift_host, self._spark_thrift_port),
        }

    def backend_version(self):
        raise NotImplementedError("backend_version() is not implemented for Spark")

    def database_exists(self, db_name):
        assert db_name
        return bool(self.list_databases(db_name))

    def exists(self, db_name, object_name):
        """Does the object exist.
        Impyla does not work for Spark so we cannot piggy back the Hive implementation.
        """
        assert db_name
        assert object_name
        return bool(
            self.database_exists(db_name) and self.list_tables(db_name, object_name)
        )

    def get_columns(self, db_name, table_name):
        assert db_name and table_name
        legacy_columns = self.execute_query_fetch_all(
            "DESCRIBE %s" % self.enclose_object_reference(db_name, table_name)
        )
        return self._legacy_column_list_to_type(legacy_columns)

    def get_missing_hive_table_stats(
        self, db_name, table_name, colstats=True, as_dict=False
    ):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError(
            "get_missing_hive_table_stats() is not implemented for Spark"
        )

    def get_partition_columns(self, db_name, table_name):
        # Hive relies on better_impyla.HiveTable to get this info and it doesn't work because
        # Impyla get_tables()/get_databases() methods don't work for Spark.
        # If Spark ever become a first class citizen we need address this.
        raise NotImplementedError(
            "get_partition_columns() is not implemented for Spark"
        )

    def get_session_option(self, option_name):
        """Get config variable from Spark, set returns tuple of (prm, value)"""
        assert option_name
        sql = "set %s" % option_name
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        if row and row[1] != "<undefined>":
            option_setting = row[1]
        else:
            option_setting = None
        return option_setting

    def get_table_ddl(
        self, db_name, table_name, as_list=False, terminate_sql=False, for_replace=False
    ):
        """Return table DDL as a string (or a list of strings split on CR if as_list=True)
        for_replace ignored on Spark
        """
        assert db_name and table_name
        self._log("Fetching table DDL: %s.%s" % (db_name, table_name), detail=VVERBOSE)
        ddl_row = self.execute_query_fetch_one(
            "SHOW CREATE TABLE %s" % self.enclose_object_reference(db_name, table_name)
        )
        if not ddl_row:
            raise BackendApiException(
                "Table does not exist for DDL retrieval: %s.%s" % (db_name, table_name)
            )
        ddl_str = ddl_row[0]
        self._debug("Table DDL: %s" % ddl_str)
        if as_list:
            return ddl_str.split("\n")
        else:
            return ddl_str

    def get_table_location(self, db_name, table_name):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError("get_table_location() is not implemented for Spark")

    def get_table_partition_count(self, db_name, table_name):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError(
            "get_table_partition_count() is not implemented for Spark"
        )

    def get_table_partitions(self, db_name, table_name):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError("get_table_partitions() is not implemented for Spark")

    def get_table_row_count_from_metadata(self, db_name, table_name):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError(
            "get_table_row_count_from_metadata() is not implemented for Spark"
        )

    def get_table_size(self, db_name, table_name):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError("get_table_size() is not implemented for Spark")

    def get_table_stats(self, db_name, table_name, as_dict=False):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError("get_table_stats() is not implemented for Spark")

    def get_user_name(self):
        raise NotImplementedError("get_user_name() is not implemented for Spark")

    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        assert db_name
        if table_name_filter:
            sql = "SHOW TABLES IN %s LIKE '%s'" % (db_name, table_name_filter.lower())
        else:
            sql = "SHOW TABLES IN %s" % db_name
        rows = self.execute_query_fetch_all(sql, log_level=VVERBOSE)
        if rows:
            return [_[1] for _ in rows]
        else:
            return []

    def rename_table(
        self, from_db_name, from_table_name, to_db_name, to_table_name, sync=None
    ):
        # See note in get_partition_columns() for commentary
        raise NotImplementedError("rename_table() is not implemented for Spark")

    def target_version(self):
        """No version available via SQL for Spark"""
        return None
