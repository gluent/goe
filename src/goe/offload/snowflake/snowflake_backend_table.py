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

""" BackendSnowflakeTable: Library for logic/interaction with a table that will
    be either:
      1) The target of an offload
      2) The source of a present
"""

import logging
from textwrap import dedent

from goe.offload.column_metadata import valid_column_list
from goe.offload.snowflake.snowflake_column import (
    SnowflakeColumn,
    SNOWFLAKE_TYPE_BINARY,
    SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
    SNOWFLAKE_TYPE_TIME,
)
from goe.offload.snowflake import snowflake_predicate
from goe.offload.column_metadata import ColumnMetadataInterface
from goe.offload.backend_table import BackendTableInterface, BackendTableException
from goe.offload.offload_constants import FILE_STORAGE_FORMAT_AVRO
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.filesystem.goe_dfs import (
    AZURE_OFFLOAD_FS_SCHEMES,
    OFFLOAD_FS_SCHEME_S3A,
    OFFLOAD_FS_SCHEME_GS,
    OFFLOAD_FS_SCHEME_AZURE,
    OFFLOAD_FS_SCHEME_WASB,
    OFFLOAD_FS_SCHEME_WASBS,
    OFFLOAD_FS_SCHEME_ADL,
    OFFLOAD_FS_SCHEME_ABFS,
    OFFLOAD_FS_SCHEME_ABFSS,
)
from goe.filesystem.goe_azure import azure_fq_container_name
from goe.util.misc_functions import add_suffix_in_same_case


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendSnowflakeTable
###########################################################################


class BackendSnowflakeTable(BackendTableInterface):
    """Snowflake backend implementation"""

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
        super(BackendSnowflakeTable, self).__init__(
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

        self._sql_engine_name = "Snowflake"
        self._snowflake_integration = orchestration_options.snowflake_integration
        self._snowflake_stage = orchestration_options.snowflake_stage
        self._snowflake_file_format = add_suffix_in_same_case(
            orchestration_options.snowflake_file_format_prefix,
            "_" + self._offload_staging_format,
        )
        self._log_profile_after_final_table_load = True
        self._log_profile_after_verification_queries = True
        # Load DB is also final DB
        self._load_db_name = self.db_name
        self._load_table_path = self._get_dfs_client().gen_uri(
            self._orchestration_config.offload_fs_scheme,
            self._orchestration_config.offload_fs_container,
            self._orchestration_config.offload_fs_prefix,
            backend_db=self._load_db_name,
            table_name=self._load_table_name,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _cast_verification_query_options(self):
        """Returns a dict of query options to be executed before the final insert.
        This method does nothing but individual backends can override as required.
        """
        return {"BINARY_INPUT_FORMAT": "BASE64"}

    def _create_load_table(self, staging_file, with_terminator=False) -> list:
        raise NotImplementedError(self._not_implemented_message("Load table"))

    def _drop_load_table(self, sync=None):
        raise NotImplementedError(self._not_implemented_message("Load table staging"))

    def _final_insert_query_options(self):
        return self._cast_verification_query_options()

    def _final_insert_format_sql(
        self,
        select_expr_tuples,
        filter_clauses,
        sort_expr_list,
        for_materialized_join=False,
    ):
        """Format INSERT/SELECT for Snowflake"""
        assert select_expr_tuples
        assert isinstance(select_expr_tuples, list)
        if filter_clauses:
            assert isinstance(filter_clauses, list)

        if for_materialized_join:
            from_object_override = None
            return self._db_api.gen_insert_select_sql_text(
                self.db_name,
                self.table_name,
                self._load_view_db_name,
                self._load_table_name,
                select_expr_tuples=select_expr_tuples,
                filter_clauses=filter_clauses,
                from_object_override=from_object_override,
            )
        else:
            # For a standard data load we use COPY INTO on Snowflake
            # from_object_override is a STAGE
            from_object_override = self._format_staging_object_name()
            return self._db_api.gen_copy_into_sql_text(
                self.db_name,
                self.table_name,
                from_object_clause=from_object_override,
                select_expr_tuples=select_expr_tuples,
                filter_clauses=filter_clauses,
            )

    def _format_staging_column_name(self, column):
        if isinstance(column, ColumnMetadataInterface):
            column_name = column.staging_file_column_name or column.name
        else:
            column_name = column
        return "$1:{}".format(self.enclose_identifier(column_name))

    def _format_staging_object_name(self):
        return (
            "'@%(stage)s/%(table)s/' (FILE_FORMAT=>%(file_format)s, PATTERN=>'.*\.%(file_ext)s')"
            % {
                "stage": self._db_api.enclose_object_reference(
                    self._load_db_name, self._snowflake_stage
                ),
                "table": self._load_table_name,
                "file_format": self._db_api.enclose_object_reference(
                    self._load_db_name, self._snowflake_file_format
                ),
                "file_ext": self._offload_staging_format.lower(),
            }
        )

    def _gen_create_file_format_sql_text(
        self, file_format_name, staging_format, with_terminator=False
    ):
        null_if = (
            " NULL_IF = 'null'" if staging_format == FILE_STORAGE_FORMAT_AVRO else ""
        )
        sql = "CREATE FILE FORMAT %s TYPE = %s%s" % (
            self._db_api.enclose_object_reference(self.db_name, file_format_name),
            staging_format,
            null_if,
        )
        if with_terminator:
            sql += ";"
        return sql

    def _gen_create_stage_sql_text(self, with_terminator=False):
        stage_url = self._get_dfs_client().gen_uri(
            self._offload_fs_scheme_override(),
            self._orchestration_config.offload_fs_container,
            self._orchestration_config.offload_fs_prefix,
            backend_db=self.db_name,
            container_override=self._offload_fs_container_override(),
        )
        sql = (
            dedent(
                """\
                      CREATE STAGE %s
                      URL = '%s/'
                      STORAGE_INTEGRATION = %s"""
            )
            % (
                self._db_api.enclose_object_reference(
                    self.db_name, self._snowflake_stage
                ),
                stage_url,
                self._db_api.enclose_identifier(self._snowflake_integration),
            )
        )
        if with_terminator:
            sql += ";"
        return sql

    def _gen_synthetic_partition_column_object(self, synthetic_name, partition_info):
        raise NotImplementedError(
            self._not_implemented_message("Synthetic partitioning")
        )

    def _gen_synthetic_partition_date_truncated_sql_expr(
        self, synthetic_name, column_name, granularity, source_column_cast
    ):
        raise NotImplementedError(
            self._not_implemented_message("Synthetic partitioning")
        )

    def _gen_synthetic_partition_date_as_string_sql_expr(
        self, extract_name, pad_size, source_column_cast
    ):
        raise NotImplementedError(
            self._not_implemented_message("Synthetic partitioning")
        )

    def _gen_synthetic_part_number_granularity_sql_expr(
        self,
        column_name,
        backend_col,
        granularity,
        synthetic_partition_digits,
        with_padding=True,
    ):
        raise NotImplementedError(
            self._not_implemented_message("Synthetic partitioning")
        )

    def _gen_synthetic_part_string_granularity_sql_expr(self, column_name, granularity):
        raise NotImplementedError(
            self._not_implemented_message("Synthetic partitioning")
        )

    def _offload_fs_container_override(self):
        container_override = None
        if self._orchestration_config.offload_fs_scheme in AZURE_OFFLOAD_FS_SCHEMES:
            # For Azure STAGE the container is 'account/container' not 'container@account'
            container_override = azure_fq_container_name(
                self._orchestration_config.offload_fs_azure_account_name,
                self._orchestration_config.offload_fs_azure_account_domain,
                self._orchestration_config.offload_fs_container,
                account_in_path=True,
            )
        return container_override or self._orchestration_config.offload_fs_container

    def _offload_fs_scheme_override(self):
        """Map value in OFFLOAD_FS_SCHEME to Snowflake counterpart"""
        scheme = self._orchestration_config.offload_fs_scheme
        mappings = {
            OFFLOAD_FS_SCHEME_GS: "gcs",
            OFFLOAD_FS_SCHEME_S3A: "s3",
            OFFLOAD_FS_SCHEME_WASB: OFFLOAD_FS_SCHEME_AZURE,
            OFFLOAD_FS_SCHEME_WASBS: OFFLOAD_FS_SCHEME_AZURE,
            OFFLOAD_FS_SCHEME_ADL: OFFLOAD_FS_SCHEME_AZURE,
            OFFLOAD_FS_SCHEME_ABFS: OFFLOAD_FS_SCHEME_AZURE,
            OFFLOAD_FS_SCHEME_ABFSS: OFFLOAD_FS_SCHEME_AZURE,
        }
        return mappings.get(scheme) or scheme

    def _staging_column_is_not_null_sql_expr(self, column_name):
        """Return a column IS NOT NULL expression.
        When Snowflake stages to Avro is doesn't convert the Avro "null" value to NULL so
        we have the workaround below.
        """
        if self._offload_staging_format == FILE_STORAGE_FORMAT_AVRO:
            return "%s != 'null'" % self._format_staging_column_name(column_name)
        else:
            # Call the common code on the interface
            return super(
                BackendSnowflakeTable, self
            )._staging_column_is_not_null_sql_expr(column_name)

    def _staging_to_backend_cast(
        self, rdbms_column, backend_column, staging_column
    ) -> tuple:
        """Returns correctly cast or overridden columns ready for insert/select to final table"""

        def cast(expr, data_type, safe_cast=False):
            if safe_cast:
                # TRY_CAST only works with TEXT but all staging columns are VARIANT. Std CAST is fine.
                return "TRY_CAST(CAST(%s AS TEXT) AS %s)" % (expr, data_type)
            else:
                return "CAST(%s AS %s)" % (expr, data_type)

        def cast_date(expr, data_type, safe_cast=False):
            if safe_cast:
                # TRY_TO_... only works with TEXT but all staging columns are VARIANT. Std CAST is fine.
                return "TRY_TO_{}(CAST({} AS TEXT),'YYYY-MM-DD')".format(
                    data_type, expr
                )
            else:
                return "TO_{}({})".format(data_type, expr)

        def cast_datetime(expr, data_type, safe_cast=False):
            if safe_cast:
                # TRY_TO_... only works with TEXT but all staging columns are VARIANT. Std CAST is fine.
                return "TRY_TO_{}(CAST({} AS TEXT),'YYYY-MM-DD HH24:MI:SS.FF')".format(
                    data_type, expr
                )
            else:
                return "TO_{}({})".format(data_type, expr)

        def cast_time(expr, data_type, safe_cast=False):
            if safe_cast:
                # TRY_TO_... only works with TEXT but all staging columns are VARIANT. Std CAST is fine.
                return "TRY_TO_{}(CAST({} AS TEXT),'HH24:MI:SS.FF')".format(
                    data_type, expr
                )
            else:
                return "TO_{}({})".format(data_type, expr)

        def cast_tz(expr, data_type, safe_cast=False):
            if safe_cast:
                # TRY_TO_... only works with TEXT but all staging columns are VARIANT. Std CAST is fine.
                return "TRY_TO_{}(CAST({} AS TEXT),'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')".format(
                    data_type, expr
                )
            else:
                return "TO_{}({})".format(data_type, expr)

        assert backend_column
        assert isinstance(backend_column, SnowflakeColumn)
        assert rdbms_column, (
            "RDBMS column missing for backend column: %s" % backend_column.name
        )
        assert isinstance(rdbms_column, ColumnMetadataInterface)
        assert staging_column
        assert isinstance(staging_column, ColumnMetadataInterface)

        return_cast = None
        return_vcast = None
        return_type = backend_column.format_data_type().upper()

        if (
            isinstance(staging_column, SnowflakeColumn)
            and staging_column.data_type == backend_column.data_type
        ):
            # Same column type and data type so no need to CAST, this is only possible when materializing a join
            self._log(
                "No cast of %s required for matching load data type: %s"
                % (backend_column.name, staging_column.data_type),
                detail=VVERBOSE,
            )
        elif backend_column.is_number_based():
            return_cast = cast(
                self._format_staging_column_name(staging_column),
                backend_column.format_data_type().upper(),
            )
            return_vcast = cast(
                self._format_staging_column_name(staging_column),
                backend_column.format_data_type().upper(),
                safe_cast=True,
            )
        elif rdbms_column.is_time_zone_based():
            if not backend_column.is_string_based():
                # Time zoned so we do not normalise to UTC.
                return_cast = cast_tz(
                    self._format_staging_column_name(staging_column),
                    backend_column.data_type,
                )
                return_vcast = cast_tz(
                    self._format_staging_column_name(staging_column),
                    backend_column.data_type,
                    safe_cast=True,
                )
        elif rdbms_column.is_date_based():
            if backend_column.is_string_based():
                if rdbms_column.has_time_element():
                    # We need a string containing the full timestamp spec including nanoseconds and 4 digit year.
                    return_cast = cast_datetime(
                        self._format_staging_column_name(staging_column),
                        SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                    )
                    return_vcast = cast_datetime(
                        self._format_staging_column_name(staging_column),
                        SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                        safe_cast=True,
                    )
                    cast_staged_date_to_string_template = """CASE
                    WHEN {col} IS NOT NULL THEN
                        TO_CHAR({cast},'YYYY-MM-DD HH24:MI:SS.FF9')
                    END"""
                else:
                    return_cast = cast_date(
                        self._format_staging_column_name(staging_column),
                        SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                    )
                    return_vcast = cast_date(
                        self._format_staging_column_name(staging_column),
                        SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                        safe_cast=True,
                    )
                    cast_staged_date_to_string_template = """CASE
                    WHEN {col} IS NOT NULL THEN
                        TO_CHAR({cast},'YYYY-MM-DD')
                    END"""
                return_cast = cast_staged_date_to_string_template.format(
                    col=self._format_staging_column_name(staging_column),
                    cast=return_cast,
                )
                return_vcast = cast_staged_date_to_string_template.format(
                    col=self._format_staging_column_name(staging_column),
                    cast=return_vcast,
                )
            else:
                if rdbms_column.has_time_element():
                    return_cast = cast_datetime(
                        self._format_staging_column_name(staging_column),
                        backend_column.data_type,
                    )
                    return_vcast = cast_datetime(
                        self._format_staging_column_name(staging_column),
                        backend_column.data_type,
                        safe_cast=True,
                    )
                else:
                    return_cast = cast_date(
                        self._format_staging_column_name(staging_column),
                        backend_column.data_type,
                    )
                    return_vcast = cast_date(
                        self._format_staging_column_name(staging_column),
                        backend_column.data_type,
                        safe_cast=True,
                    )
        elif backend_column.data_type == SNOWFLAKE_TYPE_TIME:
            return_cast = cast_time(
                self._format_staging_column_name(staging_column),
                backend_column.data_type,
            )
            return_vcast = cast_time(
                self._format_staging_column_name(staging_column),
                backend_column.data_type,
                safe_cast=True,
            )
        elif backend_column.data_type == SNOWFLAKE_TYPE_BINARY:
            return_cast = cast(
                self._format_staging_column_name(staging_column),
                backend_column.format_data_type().upper(),
            )
            return_vcast = cast(
                self._format_staging_column_name(staging_column),
                backend_column.format_data_type().upper(),
                safe_cast=True,
            )

        if return_cast:
            return return_cast, return_type, return_vcast
        else:
            return_cast = self._format_staging_column_name(staging_column)
            return return_cast, None, return_cast

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def cleanup_staging_area(self):
        self._rm_dfs_dir(self.get_staging_table_location())

    def compute_final_table_stats(self, incremental_stats, materialized_join=False):
        """We cannot influence stats on Snowflake and this should never be called due to capability setting"""
        pass

    def create_backend_table(self, with_terminator=False) -> list:
        """Create a table in Snowflake based on object state.
        Creating a new table may change our world view so the function drops state if in execute mode.
        If dry_run then we leave state in place to allow other operations to preview.
        """
        no_partition_columns = None
        cmds = self._db_api.create_table(
            self.db_name,
            self.table_name,
            self.get_columns(),
            no_partition_columns,
            sort_column_names=self._sort_columns,
            with_terminator=with_terminator,
        )
        if not self._dry_run:
            self._drop_state()
        return cmds

    def create_db(self, with_terminator=False) -> list:
        """On Snowflake we create a SCHEMA, STAGE and all FILE FORMATs"""
        cmds = self._create_final_db()
        # Create STAGE and FILE FORMAT
        sqls = []
        if self._db_api.snowflake_stage_exists(self.db_name, self._snowflake_stage):
            self._log(
                "Snowflake stage already exists: %s.%s"
                % (self.db_name, self._snowflake_stage),
                detail=VERBOSE,
            )
        else:
            sqls.append(
                self._gen_create_stage_sql_text(with_terminator=with_terminator)
            )

        for staging_format in self._db_api.valid_staging_formats():
            file_format = add_suffix_in_same_case(
                self._orchestration_config.snowflake_file_format_prefix,
                "_" + staging_format,
            )
            if self._db_api.snowflake_file_format_exists(self.db_name, file_format):
                self._log(
                    "Snowflake file format already exists: %s.%s"
                    % (self.db_name, file_format),
                    detail=VERBOSE,
                )
            else:
                sqls.append(
                    self._gen_create_file_format_sql_text(
                        file_format, staging_format, with_terminator=with_terminator
                    )
                )

        # TODO NJ@2020-11-12 Prepare result cache area for Hybrid Queries, temporary solution that will be removed
        cmds.extend(self._create_result_cache_db())

        if sqls:
            cmds.extend(self._execute_ddl(sqls, log_level=VERBOSE))
        return cmds

    def empty_staging_area(self, staging_file):
        self._rm_dfs_dir(self.get_staging_table_location())

    def get_default_location(self):
        """Not applicable to Snowflake"""
        return None

    def get_staging_table_location(self):
        # Use cached location to avoid re-doing same thing multiple times
        return self._load_table_path

    def load_final_table(self, sync=None):
        """Copy data from the staged load table into the final Snowflake table"""
        self._debug(
            "Loading %s.%s from %s.%s"
            % (
                self.db_name,
                self.table_name,
                self._load_db_name,
                self._load_table_name,
            )
        )
        select_expression_tuples = [
            (self.get_final_table_cast(col), col.name.upper())
            for col in self.get_columns()
        ]
        sqls, query_options = self._gen_final_insert_sqls(select_expression_tuples)
        self._execute_dml(
            sqls,
            query_options=query_options,
            profile=self._log_profile_after_final_table_load,
        )

    def load_materialized_join(
        self,
        threshold_cols=None,
        gte_threshold_vals=None,
        lt_threshold_vals=None,
        insert_predicates=None,
        sync=None,
    ):
        """Copy data from the load view into the final Snowflake join table"""
        if insert_predicates:
            assert isinstance(insert_predicates, list)
        self._debug(
            "Loading %s.%s from %s.%s"
            % (
                self.db_name,
                self.table_name,
                self._load_view_db_name,
                self._load_table_name,
            )
        )
        select_expression_tuples = [
            (self.enclose_identifier(col.name), self.enclose_identifier(col.name))
            for col in self.get_columns()
        ]
        sqls, query_options = self._gen_mat_join_insert_sqls(
            select_expression_tuples,
            threshold_cols,
            gte_threshold_vals,
            lt_threshold_vals,
            insert_predicates=insert_predicates,
        )
        self._execute_dml(
            sqls,
            query_options=query_options,
            profile=self._log_profile_after_final_table_load,
        )

    def partition_function_requires_granularity(self):
        """No partitioning on Snowflake"""
        return False

    def post_transport_tasks(self, staging_file):
        """Nothing to do on Snowflake"""
        pass

    def predicate_has_rows(self, predicate):
        if not self.exists():
            return False

        sql = "SELECT 1 FROM %s WHERE (%s) LIMIT 1" % (
            self._db_api.enclose_object_reference(self.db_name, self.table_name),
            snowflake_predicate.predicate_to_where_clause(
                self.get_columns(), predicate
            ),
        )
        return bool(
            self._execute_query_fetch_one(
                sql, log_level=VERBOSE, not_when_dry_running=False
            )
        )

    def predicate_to_where_clause(self, predicate, columns_override=None):
        return snowflake_predicate.predicate_to_where_clause(
            columns_override or self.get_columns(), predicate
        )

    def result_cache_area_exists(self):
        return self._result_cache_db_exists()
        # When we return to using a stage for result cache we can reinstate the code below
        # return self._db_api.snowflake_stage_exists(self._load_db_name, self._snowflake_stage)

    def set_columns(self, new_columns):
        """Set column lists in state, see interface for description.
        NOTE: This Snowflake override ensures all column names are upper case as specified in the design.
              If we remove this overload then modify the description in the interface.
        """
        assert valid_column_list(new_columns)
        cols = [_.clone(name=_.name.upper()) for _ in new_columns]
        self._columns = cols

    def setup_result_cache_area(self):
        """Prepare result cache area for Hybrid Queries"""
        # TODO NJ@2020-11-12 Temporary solution that will be removed
        if self.create_database_supported() and self._user_requested_create_backend_db:
            self._create_result_cache_db()
        # When we return to using a stage for result cache we can reinstate the code below and
        # remove calls to _create_result_cache_db().
        # if self._db_api.snowflake_stage_exists(self.db_name, self._snowflake_stage):
        # self._log('Snowflake stage already exists: %s.%s' % (self.db_name, self._snowflake_stage))
        # else:
        # return self._execute_ddl(self._gen_create_stage_sql_text(), log_level=VERBOSE)

    def setup_staging_area(self, staging_file):
        """Prepare any staging area for Snowflake OffloadTransport"""
        self._rm_dfs_dir(self.get_staging_table_location())
        # FIXME nj@2021-01-04 Remove the code/comment below once Snowflake issue 00172945 is resolved publicly
        # We have an issue where SCHEMA.FILE_FORMAT referenced in staging SQL ignores the SCHEMA
        self._db_api.set_session_db(self._load_db_name, log_level=VVERBOSE)

    def staging_area_exists(self):
        """On Snowflake this checks that the Snowflake INTEGRATION and STAGE objects exist and are correct.
        A missing integration is fatal.
        A missing stage or file format is False.
        """
        if not self._db_api.snowflake_integration_exists(self._snowflake_integration):
            raise BackendTableException(
                "Snowflake integration does not exist: %s" % self._snowflake_integration
            )
        status = True
        expected_stage_prefix = self._get_dfs_client().gen_uri(
            self._offload_fs_scheme_override(),
            self._orchestration_config.offload_fs_container,
            self._orchestration_config.offload_fs_prefix,
            container_override=self._offload_fs_container_override(),
        )
        if not self._db_api.snowflake_stage_exists(
            self._load_db_name, self._snowflake_stage, url_prefix=expected_stage_prefix
        ):
            self._warning(
                "Snowflake stage %s.%s does not exist"
                % (self._load_db_name, self._snowflake_stage)
            )
            status = False
        if not self._db_api.snowflake_file_format_exists(
            self._load_db_name,
            self._snowflake_file_format,
            format_type=self._offload_staging_format,
        ):
            self._warning(
                "Snowflake file format %s.%s does not exist"
                % (self._load_db_name, self._snowflake_file_format)
            )
            status = False
        if not self.result_cache_area_exists():
            self._warning("Result cache area %s does not exist" % self._load_db_name)
            status = False
        return status

    def synthetic_bucket_data_type(self):
        raise NotImplementedError(self._not_implemented_message("Synthetic bucketing"))

    def synthetic_bucket_filter_capable_column(self, backend_column):
        raise NotImplementedError(self._not_implemented_message("Synthetic bucketing"))

    def transport_binary_data_in_base64(self):
        """Stage binary data, during offload transport, encoded as Base64"""
        return True

    def validate_type_conversions(self, staging_columns: list):
        """Validate CASTs before copying data from the staged load table into the final backend table"""
        self._validate_final_table_casts(
            staging_columns, log_profile=self._log_profile_after_verification_queries
        )
