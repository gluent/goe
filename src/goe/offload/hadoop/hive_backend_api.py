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

""" BackendHiveApi: BackendApi implementation for a Hive backend.

    See BackendHadoopApi for better+impyla justification.
"""

import logging

from numpy import datetime64

from goe.offload.backend_api import BackendApiException
from goe.offload.column_metadata import (
    match_table_column,
    str_list_of_columns,
    valid_column_list,
)
from goe.offload.offload_constants import (
    DBTYPE_HIVE,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_ORC,
    FILE_STORAGE_FORMAT_PARQUET,
    HIVE_BACKEND_CAPABILITIES,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.hadoop.hive_literal import HiveLiteral
from goe.offload.hadoop.hadoop_backend_api import BackendHadoopApi

from goe.util.better_impyla import HDFS_NULL_PART_KEY_CONSTANT
from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_CHAR,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_VARCHAR,
    HADOOP_TYPE_BINARY,
    HADOOP_TYPE_TINYINT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_DOUBLE_PRECISION,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_TIMESTAMP,
    HADOOP_TYPE_INTERVAL_DS,
    HADOOP_TYPE_INTERVAL_YM,
    HADOOP_TYPE_BOOLEAN,
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
# BackendHiveApi
###########################################################################


class BackendHiveApi(BackendHadoopApi):
    """Hive implementation
    Assumes remote system talks HiveQL via HS2
    """

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
        super(BackendHiveApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        logger.info("BackendHiveApi")
        if dry_run:
            logger.info("* Dry run *")

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _backend_capabilities(self):
        return HIVE_BACKEND_CAPABILITIES

    def _execute_ddl_or_dml(
        self,
        sql,
        sync=None,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        """See interface for parameter descriptions
        sync: No concept of sync vs async on Hive
        profile: profile not available on Hive
        """
        assert sql
        assert isinstance(sql, (str, list))

        if self._hive_conn:
            self._hive_conn.refresh_cursor()
            self._execute_global_session_parameters(log_level=None)
        run_opts = self._execute_session_options(query_options, log_level=log_level)
        run_sqls = self._execute_sqls(
            sql, log_level=log_level, no_log_items=no_log_items
        )
        return run_opts + run_sqls

    def _format_storage_format_clause(self, storage_format):
        if storage_format == FILE_STORAGE_FORMAT_AVRO:
            return """ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'"""
        else:
            return "STORED AS %s" % storage_format

    def _get_hadoop_connection_exception_message_template(self):
        """Hive"""
        return """Successfully connected to %(host)s:%(port)s, but rejected. Possible reasons:

 1) Kerberos is configured for Impala, but not configured in your environment file (or vice versa)
 2) A process is listening at %(host)s:%(port)s, but it is not HiveServer2
 3) LDAP is configured for Hive, but incorrect%(enc_text)s credentials were supplied
 4) Invalid hiveserver2 authentication method is used. In non-kerberized environments Hive
    must match hive-site.xml: hive.server2.authentication"""

    def _get_table_stats(self, hive_stats, as_dict=False, part_stats=False):
        """Hive"""
        metastore = self.get_session_option("hive.metastore.client.factory.class")
        if metastore and "AWSGLUE" in metastore.upper():
            col_stats = False
        else:
            col_stats = True
        tab_stats, part_stats, col_stats = hive_stats.get_table_stats(
            self._backend_type,
            as_dict=as_dict,
            messages=None,
            partstats=part_stats,
            colstats=col_stats,
        )
        return tab_stats, part_stats, col_stats

    def _partition_clause_null_constant(self):
        return HDFS_NULL_PART_KEY_CONSTANT

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def alter_sort_columns(self, db_name, table_name, sort_column_names, sync=None):
        """Do nothing on Hive, sorting is implemented in INSERT rather than table DDL"""
        return []

    def backend_version(self):
        """Hive has version() on 2.1 and higher but without the function we don't whether we are on
        a high enough version to use it or not, chicken and egg. So we try use it anyway and
        catch any resulting exception
        """
        sql = "SELECT VERSION()"
        try:
            row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        except Exception as exc:
            self._log(
                "Assuming VERSION()=None because of exception: %s" % str(exc),
                detail=VVERBOSE,
            )
            row = None
        return row[0] if row else None

    def compute_stats(
        self,
        db_name,
        table_name,
        incremental=None,
        for_columns=False,
        partition_tuples=None,
        sync=None,
    ):
        """Hive compute stats
        incremental: not applicable on Hive
        """
        assert db_name and table_name
        if partition_tuples:
            assert isinstance(partition_tuples, list)
            assert isinstance(
                partition_tuples[0], (tuple, list)
            ), "%s is not tuple" % type(partition_tuples[0])
        if not self.table_stats_compute_supported():
            return None
        for_columns_clause = " FOR COLUMNS" if for_columns else ""
        partition_clause = (
            " PARTITION ({})".format(
                self._format_partition_clause_for_sql(
                    db_name, table_name, partition_tuples
                )
            )
            if partition_tuples
            else ""
        )
        sql = "ANALYZE TABLE %s.%s%s COMPUTE STATISTICS%s" % (
            self.enclose_identifier(db_name),
            self.enclose_identifier(table_name),
            partition_clause,
            for_columns_clause,
        )
        return self.execute_ddl(sql, sync=sync)

    def create_table(
        self,
        db_name,
        table_name,
        column_list,
        partition_column_names,
        storage_format=None,
        location=None,
        external=False,
        table_properties=None,
        sort_column_names=None,
        without_db_name=False,
        sync=None,
        with_terminator=False,
    ):
        """Create a table using HiveQL
        See abstract method for more description
        Hive ignores sort_column_names for DDL
        """
        assert db_name or without_db_name
        assert table_name
        assert column_list
        assert valid_column_list(column_list), (
            "Incorrectly formed column_list: %s" % column_list
        )
        if partition_column_names:
            assert isinstance(partition_column_names, list)
        assert storage_format
        if table_properties:
            assert isinstance(table_properties, dict)

        non_synthetic_columns = [
            _ for _ in column_list if _.name not in (partition_column_names or [])
        ]
        col_projection = self._create_table_columns_clause_common(
            non_synthetic_columns, external=external
        )

        db_clause = (self.enclose_identifier(db_name) + ".") if db_name else ""

        external_clause = " EXTERNAL" if external else ""

        if partition_column_names:
            part_col_pairs = []
            for part_col in partition_column_names:
                real_col = match_table_column(part_col, column_list)
                if not real_col:
                    self._log(
                        "Proposed table columns: %s" % str_list_of_columns(column_list),
                        detail=VERBOSE,
                    )
                    raise BackendApiException(
                        "Partition column is not in table columns: %s" % part_col
                    )
                part_col_pairs.append(
                    "%s %s"
                    % (
                        self.enclose_identifier(part_col.lower()),
                        real_col.format_data_type(),
                    )
                )
            part_clause = "\nPARTITIONED BY (%s)" % ", ".join(part_col_pairs)
        else:
            part_clause = ""

        stored_as_clause = "\n" + self._format_storage_format_clause(storage_format)

        if location:
            location_clause = "\nLOCATION '%s'" % location
        else:
            location_clause = ""

        if table_properties:
            table_prop_clause = "\nTBLPROPERTIES (%s)" % ", ".join(
                "%s=%s" % (self.to_backend_literal(k), self.to_backend_literal(v))
                for k, v in table_properties.items()
            )
        else:
            table_prop_clause = ""

        sql = """CREATE%(external_clause)s TABLE %(db_clause)s%(table)s (
%(col_projection)s
)%(part_clause)s%(stored_as_clause)s%(location_clause)s%(table_prop_clause)s""" % {
            "db_clause": db_clause,
            "table": self.enclose_identifier(table_name),
            "external_clause": external_clause,
            "col_projection": col_projection,
            "part_clause": part_clause,
            "stored_as_clause": stored_as_clause,
            "location_clause": location_clause,
            "table_prop_clause": table_prop_clause,
        }
        if with_terminator:
            sql += ";"

        return self.execute_ddl(sql, sync=sync)

    def create_udf(
        self,
        db_name,
        udf_name,
        return_data_type,
        parameter_tuples,
        udf_body,
        or_replace=False,
        spec_as_string=None,
        sync=None,
        log_level=VERBOSE,
    ):
        raise NotImplementedError("create_udf() is not implemented for Hive backend")

    def current_date_sql_expression(self):
        return "CURRENT_DATE()"

    @staticmethod
    def default_storage_format():
        return FILE_STORAGE_FORMAT_ORC

    def gen_insert_select_sql_text(
        self,
        db_name,
        table_name,
        from_db_name,
        from_table_name,
        select_expr_tuples,
        partition_expr_tuples=None,
        filter_clauses=None,
        sort_expr_list=None,
        distribute_columns=None,
        insert_hint=None,
        from_object_override=None,
    ):
        """Hive override
        Ignores insert_hint
        See abstractmethod spec for parameter descriptions
        """
        self._gen_insert_select_sql_assertions(
            db_name,
            table_name,
            from_db_name,
            from_table_name,
            select_expr_tuples,
            partition_expr_tuples,
            filter_clauses,
            from_object_override,
        )

        projected_expressions = select_expr_tuples[:]
        part_clause = ""
        if partition_expr_tuples:
            part_clause = " PARTITION (%s)" % ",".join(
                self.enclose_identifier(n) for _, n in partition_expr_tuples
            )
            projected_expressions += partition_expr_tuples
        projection = self._format_select_projection(projected_expressions)
        from_db_table = from_object_override or self.enclose_object_reference(
            from_db_name, from_table_name
        )

        where_clause = ""
        if filter_clauses:
            where_clause = "\nWHERE  " + "\nAND    ".join(filter_clauses)

        sort_by_clause = (
            ("\nSORT BY " + ",".join(sort_expr_list)) if sort_expr_list else ""
        )
        dist_by_clause = (
            (
                "\nDISTRIBUTE BY %s"
                % ",".join(self.enclose_identifier(n) for n in distribute_columns)
            )
            if distribute_columns
            else ""
        )

        insert_sql = """INSERT INTO %(db_table)s%(part_clause)s
SELECT %(proj)s
FROM   %(from_db_table)s%(where)s%(dist_by)s%(sort_by)s""" % {
            "db_table": self.enclose_object_reference(db_name, table_name),
            "part_clause": part_clause,
            "proj": projection,
            "from_db_table": from_db_table,
            "where": where_clause,
            "dist_by": dist_by_clause,
            "sort_by": sort_by_clause,
        }
        return insert_sql

    def get_max_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
        not_when_dry_running=False,
    ):
        """Hive override"""
        query_options = {"hive.llap.execution.mode": "all"}
        return self._get_max_column_values_common(
            db_name,
            table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            optimistic_prune_clause=optimistic_prune_clause,
            not_when_dry_running=not_when_dry_running,
            query_options=query_options,
        )

    def get_missing_hive_table_stats(
        self, db_name, table_name, colstats=True, as_dict=False
    ):
        """Code to get partitions with missing stats on Hive
        This is not shared with other backends, Hive specific
        """
        assert self._backend_type == DBTYPE_HIVE
        hive_stats = self._get_hive_stats_table(db_name, table_name)
        return hive_stats.get_table_stats(
            self._backend_type,
            as_dict=as_dict,
            messages=self._messages,
            missing=True,
            colstats=colstats,
        )

    def get_query_profile(self, query_identifier=None):
        """No profile on Hive."""
        return ""

    def get_session_option(self, option_name):
        """Get config variable from Hive, set returns single strings with = embedded"""
        assert option_name
        sql = "set %s" % option_name
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        if row and "=" in row[0]:
            option_setting = row[0].split("=")[1]
        else:
            option_setting = None
        return option_setting

    def get_table_partitions(self, db_name, table_name):
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name)
        hive_parts = hive_table.table_partitions()
        # HiveTable on Hive doesn't give us anything other than the partition key
        table_partitions = {
            k: self._table_partition_info(partition_id=k)
            for k in list(hive_parts.keys())
        }
        return table_partitions

    def get_table_row_count(
        self,
        db_name,
        table_name,
        filter_clause=None,
        not_when_dry_running=False,
        log_level=VVERBOSE,
    ):
        """On Hive we need to protect from getting count from stats"""
        sql = self._gen_select_count_sql_text_common(
            db_name, table_name, filter_clause=filter_clause
        )
        query_options = {"hive.compute.query.using.stats": "false"}
        row = self.execute_query_fetch_one(
            sql,
            query_options=query_options,
            log_level=log_level,
            time_sql=True,
            not_when_dry_running=not_when_dry_running,
        )
        return row[0] if row else None

    def get_table_size(self, db_name, table_name, no_cache=False):
        """HiveTable (better_impyla) doesn't support getting the table size unless on LLAP
        therefore there's a strong chance this will return 0
        """
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name, no_cache=no_cache)
        size_bytes = hive_table.table_size() or 0
        return size_bytes

    def get_table_sort_columns(self, db_name, table_name, as_csv=True):
        """Irrelevant on Hive"""
        return []

    def get_user_name(self):
        sql = "SELECT CURRENT_USER()"
        row = self.execute_query_fetch_one(sql, log_level=VERBOSE)
        return row[0] if row else None

    def is_nan_sql_expression(self, column_expr):
        """is_nan for Hive"""
        return "%s = 'NaN'" % column_expr

    def is_valid_storage_format(self, storage_format):
        return bool(
            storage_format in [FILE_STORAGE_FORMAT_ORC, FILE_STORAGE_FORMAT_PARQUET]
        )

    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        raise NotImplementedError("list_udfs() is not implemented for Hive backend")

    def max_column_name_length(self):
        return 767

    def max_datetime_value(self):
        return datetime64("9999-12-31T23:59:59")

    def max_table_name_length(self):
        return 128

    def min_datetime_value(self):
        # TODO nj@2020-01-10 Return value should be year 0001 but using 1000 due to OffloadTransport bug: GOE-1441
        # return datetime64('0001-01-01')
        return datetime64("1000-01-01")

    def refresh_table_files(self, db_name, table_name, sync=None):
        """No requirement to scan files for a table on Hive but drop from cache because that will be stale.
        MSCK REPAIR TABLE is for finding new partitions rather than just rescanning for files
        therefore is not what this is intended for.
        """
        self.drop_state()

    def supported_backend_data_types(self):
        return [
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
        ]

    def to_backend_literal(self, py_val, data_type=None):
        """Translate a Python value to a Hive literal"""
        return HiveLiteral.format_literal(py_val, data_type=data_type)

    def udf_details(self, db_name, udf_name):
        raise NotImplementedError("udf_details is not implemented on Hive")
