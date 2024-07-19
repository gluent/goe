#! /usr/bin/env python3

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

""" BackendImpalaTable: Library for logic/interaction with a table that will
    be either:
      1) The target of an offload
      2) The source of a present
"""

import logging

from goe.offload.column_metadata import get_column_names, valid_column_list
from goe.offload.hadoop.hadoop_backend_table import BackendHadoopTable
from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_TINYINT,
)
from goe.offload.offload_constants import (
    FILE_STORAGE_FORMAT_PARQUET,
    FILE_STORAGE_FORMAT_PARQUET_IMPALA,
)
from goe.util.goe_version import GOEVersion

###############################################################################
# CONSTANTS
###############################################################################

FNV_HASH_BUCKET_EXPRESSION = (
    "CAST(COALESCE(PMOD(FNV_HASH(%(dividend)s),%(divisor)s),0) AS SMALLINT)"
)

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendImpalaTable
###########################################################################


class BackendImpalaTable(BackendHadoopTable):
    """Impala implementation.
    This is an extension of BackendHadoopTable because the overall implementation is very similar to
    Hive and differences are catered for in the original better_impyla building block.
    """

    def __init__(
        self,
        db_name,
        table_name,
        backend_type,
        orchestration_options,
        messages,
        orchestration_operation=None,
        hybrid_metadata=None,
        dry_run=False,
        existing_backend_api=None,
        do_not_connect=False,
    ):
        """CONSTRUCTOR"""
        super(BackendImpalaTable, self).__init__(
            db_name,
            table_name,
            backend_type,
            orchestration_options,
            messages,
            orchestration_operation=orchestration_operation,
            hybrid_metadata=hybrid_metadata,
            dry_run=dry_run,
            existing_backend_api=existing_backend_api,
            do_not_connect=do_not_connect,
        )

        logger.info("BackendImpalaTable setup: (%s, %s)" % (db_name, table_name))
        if dry_run:
            logger.info("* Dry run *")
        self._log_profile_after_final_table_load = True
        # Impala profiles are pretty big so we don't log after verification queries, only INSERT
        self._log_profile_after_verification_queries = False

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _cast_verification_query_options(self):
        return self._disable_decimal_v2_query_option()

    def _create_new_backend_table(self, sort_column_names=None):
        """Generate & execute DDL to create a table in Impala.
        The table doesn't exist but self._columns should have been set and
        be ready to drive both columns and partition details.
        """
        assert self._columns and valid_column_list(self._columns)

        partition_column_names = get_column_names(self.get_partition_columns())

        location = self._get_data_table_hdfs_dir()
        if self._user_requested_storage_format == FILE_STORAGE_FORMAT_PARQUET:
            storage_format = FILE_STORAGE_FORMAT_PARQUET_IMPALA
        else:
            storage_format = self._user_requested_storage_format

        if self._db_api.transactional_tables_default():
            table_properties = {"external.table.purge": "TRUE"}
            external_table = True
        else:
            table_properties = None
            external_table = False

        return self._db_api.create_table(
            self.db_name,
            self.table_name,
            self.get_columns(),
            partition_column_names,
            storage_format=storage_format,
            location=location,
            external=external_table,
            table_properties=table_properties,
            sort_column_names=sort_column_names,
            sync=True,
        )

    def _compute_impala_table_statistics(self, incremental_stats):
        self._db_api.compute_stats(
            self.db_name, self.table_name, incremental=incremental_stats
        )

    def _disable_decimal_v2_query_option(self):
        # For consistency we want to insert data using DECIMAL V1 which is no longer the default from Impala v3.
        # This is to avoid issues such as being unable to reduce a 34 digit number:
        #   select cast(lpad('9',34,'9') as decimal(38,0)) / 10;
        #   ERROR: UDF ERROR: Decimal expression overflowed
        if bool(GOEVersion(self.target_version()) >= GOEVersion("3.0.0")):
            return {"DECIMAL_V2": "FALSE"}
        else:
            return {}

    def _final_insert_format_sql(
        self,
        select_expr_tuples,
        filter_clauses,
        sort_expr_list,
        for_materialized_join=False,
    ):
        """Format INSERT/SELECT for Impala.
        Ignores sort_expr_list
        """
        assert select_expr_tuples
        assert isinstance(select_expr_tuples, list)
        if filter_clauses:
            assert isinstance(filter_clauses, list)

        impala_hint = (
            ("\n[%s]" % self._user_requested_impala_insert_hint)
            if self._user_requested_impala_insert_hint
            else ""
        )
        partition_expr_tuples = [
            (self.get_final_table_cast(_), _)
            for _ in get_column_names(self.get_partition_columns())
        ]

        return self._db_api.gen_insert_select_sql_text(
            self.db_name,
            self.table_name,
            self._load_db_name,
            self._load_table_name,
            select_expr_tuples=select_expr_tuples,
            partition_expr_tuples=partition_expr_tuples,
            filter_clauses=filter_clauses,
            insert_hint=impala_hint,
        )

    def _final_insert_query_options(self):
        """Returns a dict of Impala query options to be executed before the final insert"""
        query_options = {}
        if (
            self._user_requested_storage_format == FILE_STORAGE_FORMAT_PARQUET
            and self._user_requested_storage_compression
        ):
            query_options["COMPRESSION_CODEC"] = (
                self._user_requested_storage_compression
            )
        query_options.update(self._disable_decimal_v2_query_option())
        return query_options

    def _gen_synthetic_part_number_granularity_sql_expr(
        self,
        column_expr,
        backend_col,
        granularity,
        synthetic_partition_digits,
        with_padding=True,
    ):
        to_synth_expr = column_expr
        if backend_col.data_type == HADOOP_TYPE_BIGINT:
            # There are a number of values around 15-18 digits that round weirdly when using BIGINT, for example:
            # > select FLOOR(CAST(99999999999999992 AS BIGINT)/1000);
            # +-------------------------------------------------+
            # | floor(cast(99999999999999992 as bigint) / 1000) |
            # +-------------------------------------------------+
            # | 100000000000000                                 |
            # +-------------------------------------------------+
            # Therefore we use DECIMAL instead, at the cost of extra CPU
            to_synth_expr = f"CAST({to_synth_expr} AS DECIMAL(19))"

        # Apply the granularity division/floor/multiplication to generate synthetic value
        to_synth_expr = f"FLOOR({to_synth_expr}/{granularity})"
        # There is scope for the division above to change the data type again so let's fix it (again)
        if backend_col.data_type in (
            HADOOP_TYPE_BIGINT,
            HADOOP_TYPE_INT,
            HADOOP_TYPE_SMALLINT,
            HADOOP_TYPE_TINYINT,
        ):
            recast_type = backend_col.format_data_type()
        else:
            recast_type = "DECIMAL({},0)".format(backend_col.data_precision)
        to_synth_expr = f"CAST({to_synth_expr} AS {recast_type})"
        to_synth_expr = f"{to_synth_expr} * {granularity}"

        # On Hadoop all synthetic values are strings
        to_synth_expr = f"CAST({to_synth_expr} AS STRING)"
        if with_padding:
            return "LPAD(%s, %s, '0')" % (to_synth_expr, synthetic_partition_digits)
        else:
            return to_synth_expr

    def _synthetic_bucket_filter_non_udf_sql_expression(self):
        """Impala override"""
        return FNV_HASH_BUCKET_EXPRESSION

    def _tzoffset_to_timestamp_sql_expression(self, col_name):
        """Impala tzoffset equivalent SQL expression"""
        return """CAST(REGEXP_EXTRACT(%(tstz)s,'([^ ]+ [^ ]+) (.+)',1) AS TIMESTAMP)
            - INTERVAL CAST(REGEXP_EXTRACT(%(tstz)s,'([^ ]+ [^ ]+) ([+-][01]?[0-9]):([0-9][05])$',2) AS INT) hours
            - INTERVAL CAST(CONCAT(REGEXP_EXTRACT(%(tstz)s,'([^ ]+ [^ ]+) ([+-])([01]?[0-9]):([0-9][05])$',2),
                                   REGEXP_EXTRACT(%(tstz)s,'([^ ]+ [^ ]+) ([+-])([01]?[0-9]):([0-9][05])$',4)) AS INT) minutes""" % {
            "tstz": self.enclose_identifier(col_name)
        }

    def _validate_staged_data_query_options(self):
        return self._disable_decimal_v2_query_option()

    ###########################################################################
    # PUBLIC METHODS - HIGH LEVEL STEP METHODS
    ###########################################################################

    def compute_final_table_stats(self, incremental_stats, materialized_join=False):
        if self.table_stats_compute_supported():
            self._compute_impala_table_statistics(incremental_stats)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################
