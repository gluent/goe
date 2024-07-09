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

from copy import copy
import logging
import os
import re
import traceback

from numpy import datetime64

from goe.offload.backend_api import (
    BackendApiException,
    MissingSequenceTableException,
    UdfDetails,
    UdfParameter,
)
from goe.offload.column_metadata import (
    is_safe_mapping,
    match_table_column,
    str_list_of_columns,
    valid_column_list,
    GOE_TYPE_BINARY,
    GOE_TYPE_LARGE_BINARY,
)
from goe.offload.offload_constants import (
    CAPABILITY_CANONICAL_DATE,
    CAPABILITY_COLUMN_STATS_SET,
    CAPABILITY_DROP_COLUMN,
    CAPABILITY_FS_SCHEME_ABFS,
    CAPABILITY_FS_SCHEME_ADL,
    CAPABILITY_FS_SCHEME_S3A,
    CAPABILITY_RANGER,
    CAPABILITY_REFRESH_FUNCTIONS,
    CAPABILITY_SENTRY,
    CAPABILITY_SORTED_TABLE,
    FILE_STORAGE_FORMAT_PARQUET,
    IMPALA_BACKEND_CAPABILITIES,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.hadoop.hadoop_backend_api import (
    BackendHadoopApi,
    HADOOP_DECIMAL_MAX_PRECISION,
    IMPALA_UDF_LIB,
)
from goe.offload.hadoop.hadoop_column import (
    HadoopColumn,
    HADOOP_TYPE_CHAR,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_VARCHAR,
    HADOOP_TYPE_TINYINT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_TIMESTAMP,
    HADOOP_TYPE_BOOLEAN,
)
from goe.offload.hadoop.impala_literal import ImpalaLiteral
from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.filesystem.goe_dfs import OFFLOAD_NON_HDFS_FS_SCHEMES
from goe.util.better_impyla import BetterImpylaException, from_impala_size
from goe.util.goe_version import GOEVersion


###############################################################################
# CONSTANTS
###############################################################################


def gen_decimal_impala_functions_in_constants(fpq=False):
    """Generate UDF overloads for all supported DECIMAL combinations.
    Needs to be declared before constants due to usage in constant generation.
    """
    if fpq:
        # This is the original even-only scale list because that's what any FPQ UDFs would have been created with
        ps_list = [(18, s * 2) for s in range(9)] + [
            (HADOOP_DECIMAL_MAX_PRECISION, s * 2) for s in range(20)
        ]
    else:
        ps_list = [(18, _) for _ in range(18)] + [
            (HADOOP_DECIMAL_MAX_PRECISION, _) for _ in range(39)
        ]
    for precision, scale in ps_list:
        int_input = 1 if precision != scale else 0.1
        if fpq:
            yield (
                "GOE_TO_FPQ",
                "STRING",
                "DECIMAL(%s,%s)" % (precision, scale),
                "CAST(%s AS DECIMAL(%s,%s))" % (int_input, precision, scale),
                "goeToFpqNumber",
            )
        else:
            yield (
                "GOE_TO_INTERNAL_NUMBER",
                "STRING",
                "DECIMAL(%s,%s)" % (precision, scale),
                "CAST(%s AS DECIMAL(%s,%s))" % (int_input, precision, scale),
                "toInternal",
            )


IMPALA_UDF_SPECS = list(gen_decimal_impala_functions_in_constants()) + [
    (
        "GOE_TO_INTERNAL_DOUBLE",
        "STRING",
        "DOUBLE",
        "CAST(1.1 AS DOUBLE)",
        "toInternal",
    ),
    (
        "GOE_TO_INTERNAL_FLOAT",
        "STRING",
        "FLOAT",
        "CAST(1.1 AS FLOAT)",
        "toInternalFloat",
    ),
    (
        "GOE_TO_INTERNAL_FLOAT",
        "STRING",
        "DOUBLE",
        "CAST(1.1 AS FLOAT)",
        "toInternalFloat",
    ),
    (
        "GOE_TO_INTERNAL_NUMBER",
        "STRING",
        "TINYINT",
        "CAST(1 AS TINYINT)",
        "toInternal",
    ),
    (
        "GOE_TO_INTERNAL_NUMBER",
        "STRING",
        "SMALLINT",
        "CAST(1 AS SMALLINT)",
        "toInternal",
    ),
    ("GOE_TO_INTERNAL_NUMBER", "STRING", "INT", "CAST(1 AS INT)", "toInternal"),
    (
        "GOE_TO_INTERNAL_NUMBER",
        "STRING",
        "BIGINT",
        "CAST(1 AS BIGINT)",
        "toInternal",
    ),
    (
        "GOE_TO_INTERNAL_DATE",
        "STRING",
        "TIMESTAMP",
        "CAST(0 AS TIMESTAMP)",
        "toInternalDate",
    ),
    (
        "GOE_TO_INTERNAL_DATE",
        "STRING",
        "STRING",
        "'0100-01-01 00:00:00.000000000'",
        "toInternalDate",
    ),
    ("GOE_FIELD_RUN_LENGTH", "STRING", "STRING, INT", "'foo', 1", "fieldRunLength"),
    ("GOE_UTF8_RUN_LENGTH", "STRING", "STRING, INT", "'bar', 1", "utf8RunLength"),
    ("GOE_ROW_RUN_LENGTH", "STRING", "STRING, INT", "'rowrow', 1", "rowRunLength"),
    ("GOE_VERSION", "STRING", "", "", "goeVersion"),
    ("GOE_UPPER", "STRING", "STRING", "'lowercase'", "goeToUpper"),
    ("GOE_UPPER", "STRING", "STRING,STRING", "'áéíóú','UTF-8'", "goeToUpper"),
    ("GOE_LOWER", "STRING", "STRING", "'ÁÉÍÓÚ'", "goeToLower"),
    ("GOE_LOWER", "STRING", "STRING,STRING", "'ÁÉÍÓÚ','UTF-8'", "goeToLower"),
    ("GOE_TOUTF8", "STRING", "STRING,STRING", "'abcd','UTF-8'", "goeToUtf8"),
    (
        "GOE_TRANSFORM",
        "STRING",
        "STRING,STRING,STRING",
        "'UPPER', 'abc', '[^a]'",
        "goeTransform",
    ),
    (
        "GOE_TRANSCODE",
        "STRING",
        "STRING, STRING, STRING",
        "'foo','UTF8','UTF16'",
        ("goeTranscode", "goeTranscodePrepare", "goeTranscodeClose"),
    ),
    ("GOE_BUCKET", "SMALLINT", "BIGINT,SMALLINT", "1,16", "goeBucket"),
    (
        "GOE_BUCKET",
        "SMALLINT",
        "DECIMAL(38,0),SMALLINT",
        "CAST(1234 AS DECIMAL(38,0)),16",
        "goeBucket",
    ),
]

IMPALA_FPQ_UDF_SPECS = (
    [
        (
            "GOE_FPQ_NULL",
            "STRING",
            "SMALLINT",
            "CAST(1 AS SMALLINT)",
            "goeFpqNull",
        ),
        (
            "GOE_FPQ_ROW",
            "STRING",
            "BOOLEAN, STRING",
            "TRUE,%(fn_db)sGOE_TO_FPQ('rowrow')",
            "goeFpqRow",
        ),
        (
            "GOE_FPQ_ROW",
            "STRING",
            "BOOLEAN, STRING, STRING ...",
            "TRUE,%(fn_db)sGOE_TO_FPQ('foo'),%(fn_db)sGOE_TO_FPQ('bar'),%(fn_db)sGOE_TO_FPQ('baz')",
            "goeFpqRow",
        ),
    ]
    + list(gen_decimal_impala_functions_in_constants(fpq=True))
    + [
        ("GOE_TO_FPQ", "STRING", "BIGINT", "CAST(1 AS BIGINT)", "goeToFpqNumber"),
        ("GOE_TO_FPQ", "STRING", "INT", "CAST(1 AS INT)", "goeToFpqNumber"),
        (
            "GOE_TO_FPQ",
            "STRING",
            "SMALLINT",
            "CAST(1 AS SMALLINT)",
            "goeToFpqNumber",
        ),
        (
            "GOE_TO_FPQ",
            "STRING",
            "TINYINT",
            "CAST(1 AS TINYINT)",
            "goeToFpqNumber",
        ),
        ("GOE_TO_FPQ", "STRING", "TIMESTAMP", "CAST(0 AS TIMESTAMP)", "goeToFpq"),
        (
            "GOE_TO_FPQ_TIMESTAMP",
            "STRING",
            "TIMESTAMP",
            "CAST(0 AS TIMESTAMP)",
            "goeToFpqTimestamp",
        ),
        (
            "GOE_TO_FPQ_TIMESTAMP",
            "STRING",
            "TIMESTAMP, SMALLINT",
            "CAST(0 AS TIMESTAMP), 5180",
            "goeToFpqTimestamp",
        ),
        ("GOE_TO_FPQ", "STRING", "STRING", "'foo'", "goeToFpq"),
        ("GOE_TO_FPQ", "STRING", "FLOAT", "CAST(1.123 AS FLOAT)", "goeToFpq"),
        ("GOE_TO_FPQ", "STRING", "DOUBLE", "CAST(1.123 AS DOUBLE)", "goeToFpq"),
        (
            "GOE_TO_FPQ_INTERVAL",
            "STRING",
            "STRING,BOOLEAN",
            "'44517008-8',TRUE",
            "goeToFpqInterval",
        ),
    ]
)


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendImpalaApi
###########################################################################


class BackendImpalaApi(BackendHadoopApi):
    """Impala implementation."""

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
        super(BackendImpalaApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        logger.info("BackendImpalaApi")
        if dry_run:
            logger.info("* Dry run *")

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _backend_capabilities(self):
        return IMPALA_BACKEND_CAPABILITIES

    def _create_impala_udf(
        self,
        function_name,
        function_spec,
        function_symbol,
        function_return,
        udf_db=None,
        sync=None,
    ):
        """Impala UDF.
        More code from here may move to create_udf(). create_udf() came later as part of BigQuery work
        and didn't want to destabilise this UDF installation code too much.
        """
        assert function_name
        assert function_spec is not None
        assert function_symbol
        assert function_return

        log_level = VVERBOSE if self._dry_run else VERBOSE

        if isinstance(function_symbol, tuple):
            symbol_clause = (
                "SYMBOL='%s' PREPARE_FN='%s' CLOSE_FN='%s'" % function_symbol
            )
        else:
            symbol_clause = "SYMBOL='%s'" % function_symbol
        hdfs_client = get_dfs_from_options(
            self._connection_options, messages=self._messages, dry_run=self._dry_run
        )
        target_uri = (
            hdfs_client.gen_uri(
                self._connection_options.offload_fs_scheme,
                self._connection_options.offload_fs_container,
                self._connection_options.offload_fs_prefix,
            )
            if self._connection_options.offload_fs_scheme in OFFLOAD_NON_HDFS_FS_SCHEMES
            else self._connection_options.hdfs_home
        )
        target_file = os.path.join(target_uri, IMPALA_UDF_LIB)
        function_body = "LOCATION '%s' %s" % (target_file, symbol_clause)

        return self.create_udf(
            udf_db,
            function_name,
            function_return,
            None,
            function_body,
            sync=sync,
            log_level=log_level,
            spec_as_string=function_spec,
        )

    def _execute_ddl_or_dml(
        self,
        sql,
        sync=None,
        query_options=None,
        log_level=VERBOSE,
        profile=None,
        no_log_items=None,
    ):
        """See interface for parameter descriptions."""
        assert sql
        assert isinstance(sql, (str, list))

        if sync is not None:
            # don't change the original query_options
            query_options = copy(query_options) if query_options else {}
            query_options["SYNC_DDL"] = "TRUE" if sync else "FALSE"

        if self._hive_conn:
            self._hive_conn.refresh_cursor()
            self._execute_global_session_parameters(log_level=None)
        run_opts = self._execute_session_options(query_options, log_level=log_level)
        run_sqls = self._execute_sqls(
            sql, log_level=log_level, profile=profile, no_log_items=no_log_items
        )
        return run_opts + run_sqls

    def _fetch_show_functions(self, db_name, udf_name_filter=None):
        assert db_name
        if udf_name_filter:
            sql = "SHOW FUNCTIONS IN %s LIKE '%s'" % (
                self.enclose_identifier(db_name),
                udf_name_filter.lower(),
            )
        else:
            sql = "SHOW FUNCTIONS IN %s" % self.enclose_identifier(db_name)
        return self.execute_query_fetch_all(sql, log_level=VVERBOSE)

    def _get_hadoop_connection_exception_message_template(self):
        """Impala"""
        return """Successfully connected to %(host)s:%(port)s, but rejected. Possible reasons:

 1) Kerberos is configured for Impala, but not configured in your environment file (or vice versa)
 2) SSL is configured for Impala, but not configured in your environment file (or vice versa)
 3) A process is listening at %(host)s:%(port)s, but it is not HiveServer2
 4) LDAP is configured for Impala, but incorrect%(enc_text)s credentials were supplied
 5) Invalid hiveserver2 authentication method is used. In non-kerberized environments Impala requires NOSASL
"""

    def _get_query_profile(self, query_identifier=None):
        """Get Impala profile. query_identifier ignored on Impala."""
        return self._hive_conn.get_profile()

    def _get_table_stats(self, hive_stats, as_dict=False, part_stats=False):
        """Impala override
        Unfortunately HiveTableStats is not consistent in return value on Impala!
        Therefore we have this Impala overload to ensure we always return three items
        """
        if part_stats:
            tab_stats, col_stats, part_stats = hive_stats.get_table_stats(
                self._backend_type, as_dict=as_dict, messages=None, partstats=part_stats
            )
        else:
            part_stats = None
            tab_stats, col_stats = hive_stats.get_table_stats(
                self._backend_type, as_dict=as_dict, messages=None, partstats=part_stats
            )
        return tab_stats, part_stats, col_stats

    def _insert_literal_values_format_sql(
        self, db_name, table_name, column_names, literal_csv_list, split_by_cr=True
    ):
        return self._insert_literals_using_insert_values_sql_text(
            db_name, table_name, column_names, literal_csv_list, split_by_cr=split_by_cr
        )

    def _partition_clause_null_constant(self):
        return "NULL"

    def _udf_installation_sql(self, udf_db=None):
        """Impala"""

        def get_sync(i, num_fns):
            if i == 0:
                return False
            elif i == num_fns - 1:
                return True
            else:
                return None

        goe_impala_functions_all = IMPALA_UDF_SPECS + IMPALA_FPQ_UDF_SPECS
        assert [
            tupl[0] for tupl in goe_impala_functions_all if len(tupl) != 5
        ] == [], "Unexpected item count in IMPALA_UDF_SPECS"

        cmds = []
        log_level = VVERBOSE if self._dry_run else VERBOSE

        if (udf_db and self.database_exists(udf_db)) or not udf_db:
            db_clause = " IN %s" % self.enclose_identifier(udf_db) if udf_db else ""
            if self.refresh_functions_supported():
                self.execute_ddl(
                    "REFRESH FUNCTIONS %s"
                    % self.enclose_identifier(udf_db if udf_db else "default"),
                    sync=None,
                    log_level=log_level,
                )
            rows = self.execute_query_fetch_all(
                "SHOW FUNCTIONS%s" % db_clause, log_level=log_level
            )
            installed_udfs = [r[1].upper().replace(" ", "") for r in rows]

            # Build a list of functions to drop based on what is installed
            goe_impala_functions_drop = []
            for fnc in goe_impala_functions_all:
                if (
                    "%s(%s)" % (fnc[0].upper(), fnc[2].upper().replace(" ", ""))
                    in installed_udfs
                ):
                    goe_impala_functions_drop.append(fnc)

            for i, (f_name, _, f_type, _, _) in enumerate(goe_impala_functions_drop):
                sync = get_sync(i, len(goe_impala_functions_drop))
                cmds.extend(
                    self._drop_udf(
                        f_name,
                        udf_db=udf_db,
                        function_spec=f_type,
                        sync=sync,
                        if_exists=False,
                    )
                )

        for i, (f_name, f_ret, f_type, _, f_symbol) in enumerate(IMPALA_UDF_SPECS):
            sync = get_sync(i, len(IMPALA_UDF_SPECS))
            cmds.extend(
                self._create_impala_udf(
                    f_name, f_type, f_symbol, f_ret, udf_db=udf_db, sync=sync
                )
            )

        return cmds

    def _udf_test_sql(self, udf_db=None):
        """Return a list of SQLs to be used for testing all UDFs"""
        db_clause = (self.enclose_identifier(udf_db) + ".") if udf_db else ""
        test_args = [(f_name, f_arg) for f_name, _, _, f_arg, _ in IMPALA_UDF_SPECS]
        sqls = []
        for f_name, f_arg in test_args:
            sql = "SELECT %s%s(%s)" % (
                db_clause,
                self.enclose_identifier(f_name),
                f_arg % {"fn_db": db_clause},
            )
            sqls.append(sql)
        return sqls

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def alter_sort_columns(self, db_name, table_name, sort_column_names, sync=None):
        assert db_name and table_name
        assert isinstance(sort_column_names, list)
        sort_csv = ",".join([self.enclose_identifier(_) for _ in sort_column_names])
        sql = "ALTER TABLE %s.%s SORT BY (%s)" % (
            self.enclose_identifier(db_name),
            self.enclose_identifier(table_name),
            sort_csv,
        )
        return self.execute_ddl(sql, log_level=VERBOSE, sync=sync)

    def backend_version(self):
        sql = "SELECT VERSION()"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return row[0] if row else None

    def canonical_date_supported(self):
        if self.is_capability_supported(CAPABILITY_CANONICAL_DATE):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("3.3.0"))
        else:
            return False

    def column_stats_set_supported(self):
        """Setting column stats is not valid in Impala before v2.6.0"""
        if self.is_capability_supported(CAPABILITY_COLUMN_STATS_SET):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("2.6.0"))
        else:
            return False

    def compute_stats(
        self,
        db_name,
        table_name,
        incremental=None,
        for_columns=False,
        partition_tuples=None,
        sync=None,
    ):
        """Impala compute stats
        for_columns: No use on Impala
        """
        assert db_name and table_name
        if partition_tuples:
            assert isinstance(partition_tuples, list) and isinstance(
                partition_tuples[0], (tuple, list)
            )
        if not self.table_stats_compute_supported():
            return
        partition_clause = (
            " PARTITION ({})".format(
                self._format_partition_clause_for_sql(
                    db_name, table_name, partition_tuples
                )
            )
            if partition_tuples
            else ""
        )
        incremental_clause = " INCREMENTAL" if incremental or partition_tuples else ""
        sql = "COMPUTE%s STATS %s.%s%s" % (
            incremental_clause,
            self.enclose_identifier(db_name),
            self.enclose_identifier(table_name),
            partition_clause,
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
        """Create a table using Impala SQL
        See abstract method for more description
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
        if sort_column_names:
            assert isinstance(sort_column_names, list)

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

        if sort_column_names:
            sort_csv = ",".join([self.enclose_identifier(_) for _ in sort_column_names])
            sort_by_clause = ("\nSORT BY (%s)" % sort_csv) if sort_csv else ""
        else:
            sort_by_clause = ""

        if storage_format:
            stored_as_clause = "\nSTORED AS %s" % storage_format
        else:
            stored_as_clause = ""

        sql = """CREATE%(external_clause)s TABLE %(db_clause)s%(table)s (
%(col_projection)s
)%(part_clause)s%(sort_by_clause)s%(stored_as_clause)s%(location_clause)s%(table_prop_clause)s""" % {
            "db_clause": db_clause,
            "table": self.enclose_identifier(table_name),
            "external_clause": external_clause,
            "col_projection": col_projection,
            "part_clause": part_clause,
            "sort_by_clause": sort_by_clause,
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
        """Create an Impala UDF.
        On Impala db_name is optional and parameter name in parameter_tuples is ignored.
        Also or_replace is ignored.
        """

        def format_parameter_tuples(parameter_tuples):
            if not parameter_tuples:
                return ""
            return ",".join(_[1] for _ in parameter_tuples)

        assert udf_name
        assert udf_body
        if parameter_tuples:
            assert isinstance(parameter_tuples, list)
            assert isinstance(parameter_tuples[0], tuple)

        udf_param_clause = spec_as_string or format_parameter_tuples(parameter_tuples)

        db_clause = (self.enclose_identifier(db_name) + ".") if db_name else ""
        sql = "CREATE FUNCTION %s%s(%s) RETURNS %s %s" % (
            db_clause,
            udf_name,
            udf_param_clause,
            return_data_type,
            udf_body,
        )
        return self.execute_ddl(sql, sync=sync, log_level=log_level)

    def current_date_sql_expression(self):
        return "TRUNC(NOW(),'DAY')"

    @staticmethod
    def default_storage_format():
        return FILE_STORAGE_FORMAT_PARQUET

    def drop_column_supported(self):
        """Using Impala version 3.3.0 to identify CDP. On CDP Private & Public Cloud even when
        PARQUET_FALLBACK_SCHEMA_RESOLUTION=NAME, dropping a column in an external table
        causes an exception
        """
        if self.is_capability_supported(CAPABILITY_DROP_COLUMN):
            return bool(GOEVersion(self.target_version()) < GOEVersion("3.3.0"))
        else:
            return False

    def filesystem_scheme_abfs_supported(self):
        """ABFS is not valid in Impala before v3.1.0"""
        if self.is_capability_supported(CAPABILITY_FS_SCHEME_ABFS):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("3.1.0"))
        else:
            return False

    def filesystem_scheme_adl_supported(self):
        """ADL is not valid in Impala before v2.9.0"""
        if self.is_capability_supported(CAPABILITY_FS_SCHEME_ADL):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("2.9.0"))
        else:
            return False

    def filesystem_scheme_s3a_supported(self):
        """S3A is not valid in Impala before v2.6.0"""
        if self.is_capability_supported(CAPABILITY_FS_SCHEME_S3A):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("2.6.0"))
        else:
            return False

    def from_canonical_column(self, column, decimal_padding_digits=0):
        """Translate an internal GOE column to an Impala column."""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable & data_default forward from RDBMS."""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return HadoopColumn(
                col.name,
                data_type=data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
            )

        # Impala has a different outcome to Hive for GOE_TYPE_BINARY and GOE_TYPE_LARGE_BINARY
        # We deal with them here and then fall back into the parent code
        if column.data_type == GOE_TYPE_BINARY:
            return new_column(column, HADOOP_TYPE_STRING)
        elif column.data_type == GOE_TYPE_LARGE_BINARY:
            return new_column(column, HADOOP_TYPE_STRING)
        else:
            return super(BackendImpalaApi, self).from_canonical_column(
                column, decimal_padding_digits=decimal_padding_digits
            )

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
        """Impala override
        Ignores sort_expr_list and distribute_columns
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

        projected_expressions = [e for e, _ in select_expr_tuples]
        part_clause = ""
        if partition_expr_tuples:
            part_clause = " PARTITION (%s)" % ",".join(
                self.enclose_identifier(n) for _, n in partition_expr_tuples
            )
            projected_expressions += [e for e, _ in partition_expr_tuples]
        projection = "\n,      ".join(_ for _ in projected_expressions)
        from_db_table = from_object_override or self.enclose_object_reference(
            from_db_name, from_table_name
        )

        where_clause = ""
        if filter_clauses:
            where_clause = "\nWHERE  " + "\nAND    ".join(filter_clauses)

        insert_sql = """INSERT INTO %(db_table)s%(part_clause)s %(hint)s
SELECT %(proj)s
FROM   %(from_db_table)s%(where)s""" % {
            "db_table": self.enclose_object_reference(db_name, table_name),
            "part_clause": part_clause,
            "hint": insert_hint or "",
            "proj": projection,
            "from_db_table": from_db_table,
            "where": where_clause,
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
        """Impala override"""
        return self._get_max_column_values_common(
            db_name,
            table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            optimistic_prune_clause=optimistic_prune_clause,
            not_when_dry_running=not_when_dry_running,
        )

    def get_session_option(self, option_name):
        """Get config variable from Impala, no filtering in Impala so loop through SET result filtering ourselves"""
        assert option_name
        option_setting = None
        rows = self.execute_query_fetch_all("SET", log_level=VVERBOSE)
        for opt in rows:
            if opt[0].upper() == option_name.upper():
                option_setting = opt[1]
                break
        return option_setting

    def get_table_row_count(
        self,
        db_name,
        table_name,
        filter_clause=None,
        not_when_dry_running=False,
        log_level=VVERBOSE,
    ):
        sql = self._gen_select_count_sql_text_common(
            db_name, table_name, filter_clause=filter_clause
        )
        row = self.execute_query_fetch_one(
            sql,
            log_level=log_level,
            time_sql=True,
            not_when_dry_running=not_when_dry_running,
        )
        return row[0] if row else None

    def get_table_partitions(self, db_name, table_name):
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name)
        hive_parts = hive_table.table_partitions()
        # HiveTable on Impala contains these keys that we'll pick out and use:
        # {'Format', '#Rows', 'Size'}
        table_partitions = {
            k: self._table_partition_info(
                partition_id=k,
                num_rows=v["#Rows"],
                size_in_bytes=from_impala_size(v["Size"]),
                data_format=v["Format"],
            )
            for k, v in hive_parts.items()
        }
        return table_partitions

    def get_table_size(self, db_name, table_name, no_cache=False):
        """On Impala HiveTable (better_impyla) requires us to sum the size from partitions"""
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name, no_cache=no_cache)
        if self.get_partition_columns(db_name, table_name):
            # we get the size from each partition
            partitions = self.get_table_partitions(db_name, table_name)
            self._log(
                "Summing size of %s partitions" % len(partitions), detail=VVERBOSE
            )
            size_bytes = 0
            for k, v in partitions.items():
                size_bytes += v["size_in_bytes"] or 0
        else:
            size_bytes = hive_table.table_size() or 0
        return size_bytes

    def get_table_sort_columns(self, db_name, table_name, as_csv=True):
        assert db_name and table_name
        hive_table = self._get_hive_table(db_name, table_name)
        sort_cols = hive_table.sort_columns()
        if sort_cols:
            return sort_cols if as_csv else sort_cols.split(",")
        else:
            return []

    def get_user_name(self):
        sql = "SELECT %s" % (
            "EFFECTIVE_USER()"
            if self._connection_options.hiveserver2_http_transport
            else "USER()"
        )
        row = self.execute_query_fetch_one(sql, log_level=VERBOSE)
        return row[0] if row else None

    def is_nan_sql_expression(self, column_expr):
        """is_nan for Impala"""
        return "is_nan(%s)" % column_expr

    def is_valid_partitioning_data_type(self, data_type):
        if not data_type:
            return False
        valid_types = [
            HADOOP_TYPE_BIGINT,
            HADOOP_TYPE_CHAR,
            HADOOP_TYPE_DECIMAL,
            HADOOP_TYPE_DOUBLE,
            HADOOP_TYPE_FLOAT,
            HADOOP_TYPE_INT,
            HADOOP_TYPE_REAL,
            HADOOP_TYPE_STRING,
            HADOOP_TYPE_SMALLINT,
            HADOOP_TYPE_TIMESTAMP,
            HADOOP_TYPE_TINYINT,
            HADOOP_TYPE_VARCHAR,
        ]
        if self.canonical_date_supported():
            valid_types.append(HADOOP_TYPE_DATE)
        return bool(data_type.upper() in valid_types)

    def is_valid_storage_format(self, storage_format):
        return bool(storage_format in [FILE_STORAGE_FORMAT_PARQUET])

    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        """In Hadoop all object names are lower case so case_sensitive does not apply."""

        def convert_show_output_to_returnable_list(row):
            return_type = row[0]
            signature = row[1]
            fn_name = signature.split("(")[0]
            return [fn_name, return_type]

        return [
            convert_show_output_to_returnable_list(_)
            for _ in self._fetch_show_functions(
                db_name, udf_name_filter=udf_name_filter
            )
        ]

    def max_datetime_value(self):
        return datetime64("9999-12-31T23:59:59")

    def max_table_name_length(self):
        return 128

    def min_datetime_value(self):
        return datetime64("1400-01-01")

    def populate_sequence_table(
        self, db_name, table_name, starting_seq, target_seq, split_by_cr=False
    ):
        """Used to populate data in the sequence file, for Impala/Hive a single insert with many literal clauses works fine
        options.sequence_table_name is tricky to deal with because we can leave the db part out,
        e.g. both of these are valid:
            options.sequence_table_name = 'udf_db.my_sequence_table
            options.sequence_table_name = 'my_sequence_table
        This code inserts in fairly large chunks (max_chunk_elements) to try get the whole table in a single file,
        multiple smaller inserts would create multiple files.
        This doesn't use insert_literal_values() because of the options db name "thing"
        """
        assert table_name
        assert starting_seq is not None
        assert target_seq is not None
        db_clause = (self.enclose_identifier(db_name) + ".") if db_name else ""

        cmds = []

        join_str = "\n," if split_by_cr else ","
        max_chunk_elements = 100000
        insert_data = "INSERT INTO %s%s VALUES " % (
            db_clause,
            self.enclose_identifier(table_name),
        )
        sequence_data = list(range(int(starting_seq) + 1, int(target_seq) + 1))
        while sequence_data:
            sequence_data_chunk = sequence_data[:max_chunk_elements]
            sequence_data = sequence_data[max_chunk_elements:]
            sql = insert_data + join_str.join("(%s)" % i for i in sequence_data_chunk)
            cmds.extend(self.execute_dml(sql, sync=True))
        return cmds

    def ranger_supported(self):
        """CDP/CDH >= 7 has moved from Sentry to Ranger. We do not currently establish the platform/distribution
        version so using Impala version as a proxy for CDP/CDH version.
        """
        if self.is_capability_supported(CAPABILITY_RANGER):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("3.3.0"))
        else:
            return False

    def refresh_functions_supported(self):
        """REFRESH FUNCTIONS is not valid in Impala before v2.9.0"""
        if self.is_capability_supported(CAPABILITY_REFRESH_FUNCTIONS):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("2.9.0"))
        else:
            return False

    def refresh_table_files(self, db_name, table_name, sync=None):
        """Rescan files for a table."""
        assert db_name and table_name
        sql = "REFRESH %s.%s" % (
            self.enclose_identifier(db_name),
            self.enclose_identifier(table_name),
        )
        return self.execute_ddl(sql, sync=sync)

    def sentry_supported(self):
        """CDP/CDH >= 7 has moved from Sentry to Ranger. We do not currently establish the platform/distribution
        version so using Impala version as a proxy for CDP/CDH version.
        """
        if self.is_capability_supported(CAPABILITY_SENTRY):
            return bool(GOEVersion(self.target_version()) < GOEVersion("3.3.0"))
        else:
            return False

    def sequence_table_max(self, db_name, table_name):
        """options.sequence_table_name is tricky to deal with because we can leave the db part out,
        e.g. both of these are valid:
            options.sequence_table_name = 'udf_db.my_sequence_table
            options.sequence_table_name = 'my_sequence_table
        This makes table existence difficult to check because we only have part of the identifying data.
        What connect has always done, and continues to do here is combine the existence check with
        the select for a max value. We then use any exception to detect a missing table.
        Not ideal but I'm not convinced that forcing "default" as the db is reliable.
        Because underlying exceptions cannot be trusted across different backends we need to be sure to
        raise the specific MissingSequenceTableException exception.
        """
        assert table_name
        db_clause = (self.enclose_identifier(db_name) + ".") if db_name else ""
        sql = "SELECT MAX(n) FROM %s%s" % (
            db_clause,
            self.enclose_identifier(table_name),
        )
        try:
            row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
            if row:
                return row[0]
            else:
                return None
        except BetterImpylaException as exc:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            raise MissingSequenceTableException(str(exc))

    def sorted_table_supported(self):
        """SORT BY is not valid in Impala before v2.9.0"""
        if self.is_capability_supported(CAPABILITY_SORTED_TABLE):
            return bool(GOEVersion(self.target_version()) >= GOEVersion("2.9.0"))
        else:
            return False

    def supported_backend_data_types(self):
        data_types = [
            HADOOP_TYPE_BOOLEAN,
            HADOOP_TYPE_BIGINT,
            HADOOP_TYPE_CHAR,
            HADOOP_TYPE_DECIMAL,
            HADOOP_TYPE_DOUBLE,
            HADOOP_TYPE_FLOAT,
            HADOOP_TYPE_INT,
            HADOOP_TYPE_REAL,
            HADOOP_TYPE_STRING,
            HADOOP_TYPE_SMALLINT,
            HADOOP_TYPE_TIMESTAMP,
            HADOOP_TYPE_TINYINT,
            HADOOP_TYPE_VARCHAR,
        ]
        if self.canonical_date_supported():
            data_types.append(HADOOP_TYPE_DATE)
        return data_types

    def to_backend_literal(self, py_val, data_type=None):
        """Translate a Python value to an Impala literal"""
        return ImpalaLiteral.format_literal(py_val, data_type=data_type)

    def transactional_tables_default(self):
        """CDP uses ACID tables by default.
        We do not currently establish the platform/distribution version so using Impala version as a proxy for CDP version.
        """
        return bool(GOEVersion(self.target_version()) >= GOEVersion("3.3.0"))

    def udf_details(self, db_name, udf_name):
        rows = self._fetch_show_functions(db_name, udf_name_filter=udf_name)
        udfs = []
        for row in rows:
            return_type = row[0] if row else None
            parameters = []
            if row and row[1]:
                # UDF parameters are embedded in a string, no names for parameters, e.g.:
                # goe_bucket(DECIMAL(38,0), SMALLINT)
                # Find all text in the signature between the outer parentheses
                m = re.search(r"[^(]*\((.*)\)", row[1])
                if m:
                    arg_types = [_.strip() for _ in m.groups()[0].split(", ")]
                    parameters = [UdfParameter(None, _) for _ in arg_types]
            udfs.append(UdfDetails(db_name, udf_name, return_type, parameters))
        return udfs

    def udf_installation_os(self, user_udf_version):
        """Impala
        Returns a list of commands executed
        """

        if not self.goe_udfs_supported():
            self._messages.log(
                "Skipping installation of UDFs due to backend: %s" % self._backend_type
            )
            return

        cmds = []
        udf_lib_source = IMPALA_UDF_LIB
        udf_lib_destination = IMPALA_UDF_LIB
        cmds.extend(
            self._udf_installation_copy_library_to_hdfs(
                udf_lib_source, udf_lib_destination
            )
        )

        return cmds
