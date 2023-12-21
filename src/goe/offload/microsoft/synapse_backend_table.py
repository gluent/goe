#! /usr/bin/env python3
""" BackendSynapseTable: Library for logic/interaction with a table that will
    be either:
      1) The target of an offload
      2) The source of a present
    LICENSE_TEXT
"""

import logging
import os
from textwrap import dedent

from goe.data_governance.hadoop_data_governance import data_governance_register_new_table_step, \
    get_data_governance_register
from goe.data_governance.hadoop_data_governance_constants import DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_TABLE
from goe.offload.microsoft.synapse_column import SynapseColumn, SYNAPSE_TYPE_BIGINT,\
    SYNAPSE_TYPE_DATETIME2, SYNAPSE_TYPE_FLOAT, SYNAPSE_TYPE_INT,\
    SYNAPSE_TYPE_NCHAR, SYNAPSE_TYPE_NVARCHAR, SYNAPSE_TYPE_REAL, SYNAPSE_TYPE_TIME, SYNAPSE_TYPE_VARBINARY
from goe.offload.microsoft.synapse_backend_api import synapse_collation_clause
from goe.offload.microsoft import synapse_predicate
from goe.offload.column_metadata import ColumnMetadataInterface
from goe.offload.backend_table import BackendTableInterface
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.staging.avro.avro_staging_file import AVRO_TYPE_DOUBLE, AVRO_TYPE_FLOAT,\
    AVRO_TYPE_INT, AVRO_TYPE_LONG
from goe.offload.staging.parquet.parquet_column import PARQUET_TYPE_DOUBLE, PARQUET_TYPE_FLOAT,\
    PARQUET_TYPE_INT32, PARQUET_TYPE_INT64
from goe.offload.hadoop.hadoop_backend_table import COMPUTE_LOAD_TABLE_STATS_LOG_TEXT
from goe.offload.offload_constants import OFFLOAD_STATS_METHOD_NONE
from goe.util import exception_trigger

###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendSynapseTable
###########################################################################

class BackendSynapseTable(BackendTableInterface):
    """ Synapse backend implementation
    """

    def __init__(self, db_name, table_name, backend_type, orchestration_options, messages, orchestration_operation=None,
                 hybrid_metadata=None, data_gov_client=None, dry_run=False, existing_backend_api=None, do_not_connect=False):
        """ CONSTRUCTOR
        """
        super(BackendSynapseTable, self).__init__(db_name, table_name, backend_type, orchestration_options, messages,
                                                  orchestration_operation=orchestration_operation,
                                                  hybrid_metadata=hybrid_metadata, data_gov_client=data_gov_client,
                                                  dry_run=dry_run, existing_backend_api=existing_backend_api, do_not_connect=do_not_connect)

        self._ext_table_location = os.path.join(self._orchestration_config.offload_fs_prefix, self._load_db_name,
                                                self._load_table_name)
        self._load_table_path = self._get_dfs_client().gen_uri(self._orchestration_config.offload_fs_scheme,
                                                               self._orchestration_config.offload_fs_container,
                                                               self._orchestration_config.offload_fs_prefix,
                                                               backend_db=self._load_db_name,
                                                               table_name=self._load_table_name)
        self._log_profile_after_final_table_load = True
        self._log_profile_after_verification_queries = True
        self._offload_stats_method = getattr(orchestration_operation, 'offload_stats_method', None)
        self._sql_engine_name = 'Synapse'
        self._synapse_data_source = orchestration_options.synapse_data_source
        self._synapse_file_format = orchestration_options.synapse_file_format
        self._synapse_role = orchestration_options.synapse_role

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _compute_load_table_statistics(self):
        if not self._user_requested_compute_load_table_stats:
            return
        self._log(COMPUTE_LOAD_TABLE_STATS_LOG_TEXT, detail=VVERBOSE)
        self._db_api.compute_stats(self._load_db_name, self._load_table_name, for_columns=True)

    def _create_load_table(self, staging_file):
        no_partition_cols = []
        self._db_api.create_table(
            self._load_db_name, self._load_table_name,
            self.convert_canonical_columns_to_backend(
                staging_file.get_canonical_staging_columns(use_staging_file_names=True)
            ),
            no_partition_cols, storage_format=staging_file.file_format,
            location=self._ext_table_location,
            external=True,
            table_properties={'DATA_SOURCE': self._db_api.enclose_identifier(self._synapse_data_source),
                              'FILE_FORMAT': self._db_api.enclose_identifier(self._synapse_file_format)},
            sync=True)

    def _drop_load_table(self, sync=None):
        if not self._db_api.table_exists(self._load_db_name, self._load_table_name):
            # Nothing to do
            return []
        drop_sql = 'DROP EXTERNAL TABLE %s' % self._db_api.enclose_object_reference(self._load_db_name,
                                                                                    self._load_table_name)
        return self._db_api.execute_ddl(drop_sql, sync=sync)

    def _final_insert_format_sql(self, select_expr_tuples, filter_clauses, sort_expr_list, for_materialized_join=False):
        """ Format INSERT/SELECT for Synapse.
         """
        assert select_expr_tuples
        assert isinstance(select_expr_tuples, list)
        if filter_clauses:
            assert isinstance(filter_clauses, list)

        return self._db_api.gen_insert_select_sql_text(self.db_name, self._base_table_name,
                                                       self._load_db_name, self._load_table_name,
                                                       select_expr_tuples=select_expr_tuples,
                                                       filter_clauses=filter_clauses)

    def _gen_synthetic_bucket_column_object(self, bucket_info=None):
        raise NotImplementedError(self._not_implemented_message('Synthetic bucketing'))

    def _gen_synthetic_partition_column_object(self, synthetic_name, canonical_column):
        raise NotImplementedError(self._not_implemented_message('Synthetic partitioning'))

    def _gen_synthetic_partition_date_truncated_sql_expr(self, synthetic_name, column_name, granularity,
                                                         source_column_cast):
        raise NotImplementedError(self._not_implemented_message('Synthetic partitioning'))

    def _gen_synthetic_partition_date_as_string_sql_expr(self, extract_name, pad_size, source_column_cast):
        raise NotImplementedError(self._not_implemented_message('Synthetic partitioning'))

    def _gen_synthetic_part_number_granularity_sql_expr(self, column_expr, backend_col, granularity,
                                                        synthetic_partition_digits, with_padding=True):
        raise NotImplementedError(self._not_implemented_message('Synthetic partitioning'))

    def _gen_synthetic_part_string_granularity_sql_expr(self, column_expr, granularity):
        raise NotImplementedError(self._not_implemented_message('Synthetic partitioning'))

    def _incremental_update_delta_column_cast(self, delta_table_column):
        """ Returns a CAST expression converting a merge source delta column into it's final data type.
            Synapse specific override in order to add any COLLATE clause.
        """
        assert isinstance(delta_table_column, ColumnMetadataInterface)
        return self.get_final_table_cast(delta_table_column) + synapse_collation_clause(delta_table_column)

    def _incremental_update_dedupe_merge_sql_template(self):
        """ SQL templates to merge staged Incremental Update delta records into the final Synapse table using
            a temporary table and INSERT, UPDATE and DELETE commands.
            This was done because:
                1) MERGE is only compatible with DISTRIBUTION=HASH tables and we also support ROUND_ROBIN.
                2) When testing with 50 million delta records this technique was approx 50% faster than MERGE.
            The temporary table is HEAP because many staging columns are expected to be VARCHAR(MAX).
            Flow:
                1) Create temporary table containing de-duped delta changes. We have our client set AUTOCOMMIT=ON.
                  DDL requires this.
                2) Begin a transaction. We can do this even with AUTOCOMMIT=ON.
                   https://docs.microsoft.com/en-us/sql/t-sql/language-elements/transactions-sql-data-warehouse?view=aps-pdw-2016-au7#arguments
                   "Explicit transactions are allowed when AUTOCOMMIT is ON."
                3) INSERT/UPDATE/DELETE constructed to mimic the MATCHED/NOT MATCHED blocks of MERGE.
                4) Commit the transaction.
                5) Drop the temporary delta table.
        """
        create_temp_table = dedent("""\
            CREATE TABLE #{delta_alias}
            WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
            AS
            SELECT {delta_columns_to_insert},{meta_operation}
            FROM (
                  SELECT {delta_projection}
                  ,      {meta_operation}
                  ,      ROW_NUMBER() OVER (PARTITION BY {key_columns} ORDER BY {meta_hwm} DESC) AS row_rank
                  FROM   {delta_table}
                  ) {delta_alias}
            WHERE row_rank = 1
            """)
        txn_begin = 'BEGIN TRANSACTION'
        insert_template = (
            dedent("""\
            INSERT INTO {base_table}
            SELECT {delta_columns_to_insert}
            FROM   #{delta_alias} {delta_alias}
            WHERE  {meta_operation} IN ('I', 'U')
            AND NOT EXISTS (SELECT 1 FROM {base_table} {base_alias} WHERE {key_column_match_clause})
            """),
            exception_trigger.INCREMENTAL_MERGE_POST_INSERT
        )
        update_template = (
            dedent("""\
            UPDATE {base_alias}
            SET    {non_key_column_update_clause}
            FROM   {base_table} {base_alias}
            INNER JOIN #{delta_alias} {delta_alias} ON {key_column_match_clause}
            WHERE  {delta_alias}.{meta_operation} IN ('I', 'U')
            """),
            exception_trigger.INCREMENTAL_MERGE_POST_UPDATE
        )
        delete_template = (
            dedent("""\
            DELETE {base_alias}
            FROM   {base_table} {base_alias}
            INNER JOIN #{delta_alias} {delta_alias} ON {key_column_match_clause}
            WHERE  {delta_alias}.{meta_operation} = 'D'
            """),
            exception_trigger.INCREMENTAL_MERGE_POST_DELETE
        )
        txn_end = 'COMMIT'
        drop_temp_table = dedent("""\
            DROP TABLE #{delta_alias}
            """)

        return [create_temp_table,
                txn_begin,
                insert_template,
                update_template,
                delete_template,
                txn_end,
                drop_temp_table]

        # Original MERGE code below for future reference
        # """ SQL template to merge staged Incremental Update delta records into the final Synapse table.
        #     Note (true as of 2021-11-10):
        #         MERGE is currently in preview for Azure Synapse Analytics.
        #     Also note:
        #         https://docs.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql?view=sql-server-ver15#remarks
        #         The MERGE statement requires a semicolon (;) as a statement terminator. Error 10713 is raised when
        #         a MERGE statement is run without the terminator.
        #     Therefore SQL statement below is terminated with ; to avoid:
        #         [SQL Server]Parse error at line: 10, column: 1: Incorrect syntax near ')'. (103010) (SQLExecDirectW)
        #     Also note:
        #         MERGE only supports DISTRIBUTION = HASH tables, GOE also supports ROUND_ROBIN.
        # """
        # return dedent("""\
        #     MERGE INTO {base_table} {base_alias}
        #     USING (
        #             SELECT {delta_columns_to_insert}
        #             ,      {delta_alias}.{meta_operation}
        #             FROM (
        #                   SELECT {delta_projection}
        #                   ,      {delta_meta_operation} AS {meta_operation}
        #                   ,      ROW_NUMBER() OVER (PARTITION BY {key_columns} ORDER BY {meta_hwm} DESC) AS row_rank
        #                   FROM   {delta_table}
        #                  ) {delta_alias}
        #             WHERE row_rank = 1
        #           ) {delta_alias}
        #     ON  {key_column_match_clause}
        #     WHEN NOT MATCHED
        #     AND  {delta_alias}.{meta_operation} IN ('I', 'U')
        #     THEN
        #         INSERT
        #             ( {base_columns} )
        #         VALUES
        #             ( {delta_columns_to_insert} )
        #     WHEN MATCHED
        #     AND  {delta_alias}.{meta_operation} = 'D'
        #     THEN
        #         DELETE
        #     WHEN MATCHED
        #     AND  {delta_alias}.{meta_operation} IN ('I', 'U')
        #     THEN
        #         UPDATE
        #         SET    {non_key_column_update_clause};
        #     """)

    def _incremental_update_dedupe_merge_sql_template_delta_projection_indentation(self):
        return 6

    def _incremental_update_dedupe_merge_sql_template_delta_update_indentation(self):
        return 0

    def _staging_to_backend_cast(self, rdbms_column, backend_column, staging_column) -> tuple:
        """ Returns correctly cast or overridden columns ready for insert/select to final table.
            Synapse implementation.
        """
        def convert(expr, data_type, style=0, try_convert=False):
            # CONVERT ( data_type [ ( length ) ] , expression [ , style ] )
            if try_convert:
                return 'TRY_CONVERT(%s, %s, %s)' % (data_type, expr, style)
            else:
                return 'CONVERT(%s, %s, %s)' % (data_type, expr, style)

        def staging_file_int_match(backend_column, staging_column):
            return bool((backend_column.data_type == SYNAPSE_TYPE_BIGINT
                         and staging_column.data_type in [AVRO_TYPE_LONG, PARQUET_TYPE_INT64])
                        or (backend_column.data_type == SYNAPSE_TYPE_INT
                            and staging_column.data_type in [AVRO_TYPE_INT, PARQUET_TYPE_INT32]))

        def staging_file_float_match(backend_column, staging_column):
            return bool((backend_column.data_type == SYNAPSE_TYPE_REAL
                         and staging_column.data_type in [AVRO_TYPE_FLOAT, PARQUET_TYPE_FLOAT])
                        or (backend_column.data_type == SYNAPSE_TYPE_FLOAT
                            and staging_column.data_type in [AVRO_TYPE_DOUBLE, PARQUET_TYPE_DOUBLE]))

        assert backend_column
        assert isinstance(backend_column, SynapseColumn)
        assert rdbms_column, 'RDBMS column missing for backend column: %s' % backend_column.name
        assert isinstance(rdbms_column, ColumnMetadataInterface)
        assert staging_column
        assert isinstance(staging_column, ColumnMetadataInterface)

        return_cast = None
        return_vcast = None
        return_type = backend_column.format_data_type().upper()

        if isinstance(staging_column, SynapseColumn) and staging_column.data_type == backend_column.data_type:
            # Same column type and data type so no need to CAST, this is only possible when materializing a join
            self._log('No cast of %s required for matching load data type: %s'
                      % (backend_column.name, staging_column.data_type), detail=VVERBOSE)
        elif backend_column.is_number_based():
            if staging_column and (staging_file_int_match(backend_column, staging_column)
                                   or staging_file_float_match(backend_column, staging_column)):
                # Same data type so no need to CAST
                self._log('No cast required for matching data types: %s/%s'
                          % (staging_column.data_type, backend_column.data_type), detail=VVERBOSE)
            else:
                return_cast = convert(self._format_staging_column_name(staging_column),
                                      backend_column.format_data_type().upper())
                return_vcast = convert(self._format_staging_column_name(staging_column),
                                       backend_column.format_data_type().upper(), try_convert=True)
        elif rdbms_column.is_time_zone_based():
            if backend_column.is_string_based():
                # This is not a valid translation, time zoned data can not be offloaded to string.
                raise NotImplementedError('Offload of time zoned data to %s is not supported'
                                          % backend_column.data_type)
            else:
                return_cast = convert(self._format_staging_column_name(staging_column),
                                      backend_column.format_data_type().upper())
                return_vcast = convert(self._format_staging_column_name(staging_column),
                                       backend_column.format_data_type().upper(), try_convert=True)
        elif rdbms_column.is_date_based():
            if backend_column.is_string_based():
                # We need a string containing the full timestamp spec including microseconds and 4 digit year.
                # This section produces a double CONVERT:
                # 1) Inner most CONVERT translates staged data to Synapse datetime2
                # 2) Outer most CONVERT translates Synapse datetime2 to Synapse VARCHAR
                # Seems inefficient, string->date->string but we need to be explicit to be sure of final format.
                return_cast = convert(self._format_staging_column_name(staging_column),
                                      SYNAPSE_TYPE_DATETIME2)
                return_vcast = convert(self._format_staging_column_name(staging_column),
                                       SYNAPSE_TYPE_DATETIME2, try_convert=True)
                cast_staged_date_to_string_template = """CASE
                WHEN {col} IS NOT NULL THEN
                    {convert}
                END"""
                return_cast = cast_staged_date_to_string_template.format(
                    col=self._format_staging_column_name(staging_column),
                    convert=convert(return_cast, backend_column.format_data_type().upper(), style=121))
                return_vcast = cast_staged_date_to_string_template.format(
                    col=self._format_staging_column_name(staging_column),
                    convert=convert(return_vcast, backend_column.format_data_type().upper(), style=121,
                                    try_convert=True))
            else:
                return_cast = convert(self._format_staging_column_name(staging_column),
                                      backend_column.format_data_type().upper())
                return_vcast = convert(self._format_staging_column_name(staging_column),
                                       backend_column.format_data_type().upper(), try_convert=True)
        elif backend_column.data_type == SYNAPSE_TYPE_TIME:
            return_cast = convert(self._format_staging_column_name(staging_column),
                                  backend_column.format_data_type().upper(), style=8)
            return_vcast = convert(self._format_staging_column_name(staging_column),
                                   backend_column.format_data_type().upper(), style=8, try_convert=True)
        elif backend_column.data_type == SYNAPSE_TYPE_VARBINARY:
            return_cast = convert(self._format_staging_column_name(staging_column),
                                  backend_column.format_data_type().upper())
            return_vcast = convert(self._format_staging_column_name(staging_column),
                                   backend_column.format_data_type().upper(), try_convert=True)

        if return_cast:
            return return_cast, return_type, return_vcast
        else:
            return_cast = self._format_staging_column_name(staging_column)
            return return_cast, None, return_cast

    def _validate_final_table_casts_verification_sql(self, projection_list, predicate_or_list, limit=None):
        """ Verification SQL to display when final table casts have failed.
            Override for Synapse as it uses TOP(n) and not LIMIT n
        """
        limit_clause = ('TOP(%s) ' % limit) if limit else ''
        return """SELECT %s%s\nFROM   %s\nWHERE  %s""" % (limit_clause,
                                                          '\n,      '.join(projection_list),
                                                          self._format_staging_object_name(),
                                                          '\nOR     '.join(predicate_or_list))

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def cleanup_staging_area(self):
        self._drop_load_table()
        self._rm_dfs_dir(self.get_staging_table_location())

    def compute_final_table_stats(self, incremental_stats, materialized_join=False):
        return self._db_api.compute_stats(self.db_name, self.table_name)

    def create_backend_table(self):
        """ Create a table in Synapse based on object state.
            For efficiency, we compute backend stats immediately after table creation to initialise empty stats
            objects on each column. These will be updated using a single table level command after the final load.
            Creating a new table may change our world view so the function drops state if in execute mode.
            If dry_run then we leave state in place to allow other operations to preview.
        """
        no_partition_columns = None
        table_properties = {}
        if self._bucket_hash_col:
            table_properties['DISTRIBUTION'] = 'HASH(%s)' % self._db_api.enclose_identifier(self._bucket_hash_col)

        cmds = self._db_api.create_table(self.db_name, self.table_name, self.get_columns(),
                                         no_partition_columns,
                                         table_properties=table_properties,
                                         sort_column_names=self._sort_columns)
        if self._offload_stats_method and self._offload_stats_method != OFFLOAD_STATS_METHOD_NONE:
            cmds.extend(self._db_api.compute_stats(self.db_name, self.table_name, for_columns=True))

        if not self._dry_run:
            self._drop_state()
        return cmds

    def create_db(self):
        cmds = self._create_final_db()
        return cmds

    def derive_unicode_string_columns(self, as_csv=False):
        """ Return a list of columns that indicate the user requested were offloaded as unicode.
            Synapse override of common method.
        """
        unicode_columns = [_ for _ in self.get_columns()
                           if _.data_type in [SYNAPSE_TYPE_NCHAR, SYNAPSE_TYPE_NVARCHAR]]
        return ','.join(_.name for _ in unicode_columns) if as_csv else unicode_columns

    def empty_staging_area(self, staging_file):
        self._rm_dfs_dir(self.get_staging_table_location())

    def get_default_location(self):
        """ Not applicable to Synapse """
        return None

    def get_staging_table_location(self):
        # Use cached location to avoid re-doing same thing multiple times
        return self._load_table_path

    def is_incremental_update_enabled(self):
        return self._is_cloud_incremental_update_enabled()

    def load_final_table(self, sync=None):
        """ Copy data from the staged load table into the final Synapse table """
        self._debug('Loading %s.%s from %s.%s' % (self.db_name, self._base_table_name,
                                                  self._load_db_name, self._load_table_name))
        select_expression_tuples = [(self.get_final_table_cast(col), col.name.upper()) for col in self.get_columns()]
        sqls, query_options = self._gen_final_insert_sqls(select_expression_tuples)
        self._execute_dml(sqls, query_options=query_options, profile=self._log_profile_after_final_table_load)

    def load_materialized_join(self, threshold_cols=None, gte_threshold_vals=None, lt_threshold_vals=None,
                               insert_predicates=None, sync=None):
        """ Copy data from the load view into the final Synapse table """
        if insert_predicates:
            assert isinstance(insert_predicates, list)
        self._debug('Loading %s.%s from %s.%s' % (self.db_name, self._base_table_name,
                                                  self._load_db_name, self._load_table_name))
        select_expression_tuples = [(self.get_final_table_cast(col), col.name.upper())
                                    for col in self.get_columns()]
        sqls, query_options = self._gen_mat_join_insert_sqls(select_expression_tuples, threshold_cols,
                                                             gte_threshold_vals, lt_threshold_vals,
                                                             insert_predicates=insert_predicates)
        self._execute_dml(sqls, query_options=query_options,
                          profile=self._log_profile_after_final_table_load)

    def post_transport_tasks(self, staging_file):
        """ On Synapse we create the load table AFTER creating the Parquet datafiles. """
        pre_register_data_gov_fn, post_register_data_gov_fn = get_data_governance_register(
            self._data_gov_client,
            lambda: data_governance_register_new_table_step(self._load_db_name, self.table_name, self._data_gov_client,
                                                            self._messages, DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_TABLE,
                                                            self._orchestration_config)
        )
        pre_register_data_gov_fn()
        if self.create_database_supported() and self._user_requested_create_backend_db:
            self._create_load_db()
        self._recreate_load_table(staging_file)
        if self.table_stats_compute_supported():
            self._compute_load_table_statistics()
        post_register_data_gov_fn()

    def predicate_has_rows(self, predicate):
        if not self.exists():
            return False

        sql = "SELECT TOP(1) 1 FROM %s WHERE (%s)" % (
            self._db_api.enclose_object_reference(self.db_name, self.table_name),
            synapse_predicate.predicate_to_where_clause(self.get_columns(), predicate)
        )
        return bool(self._execute_query_fetch_one(sql, log_level=VERBOSE, not_when_dry_running=False))

    def predicate_to_where_clause(self, predicate, columns_override=None):
        return synapse_predicate.predicate_to_where_clause(columns_override or self.get_columns(), predicate)

    def result_cache_area_exists(self):
        return self._result_cache_db_exists()

    def staging_area_exists(self):
        """ On Synapse this checks:
                a) the external data source exists
                b) the file format exists
                c) the result cache area exists
                d) the load schema exists
        """
        status = True
        if not self._db_api.synapse_external_data_source_exists(self._synapse_data_source):
            self._warning('Synapse external data source does not exist: %s' % self._synapse_data_source)
            status = False
        if not self._db_api.synapse_file_format_exists(self._synapse_file_format):
            self._warning('Synapse file format does not exist or is not a supported format type: %s'
                          % self._synapse_data_source)
            status = False
        if not self.result_cache_area_exists():
            self._warning('Result cache area %s does not exist' % self._load_db_name)
            status = False
        if not self._load_db_exists():
            self._warning('Staging schema %s does not exist' % self._load_db_name)
            status = False
        return status

    def synthetic_bucket_data_type(self):
        raise NotImplementedError(self._not_implemented_message('Synthetic bucketing'))

    def synthetic_bucket_filter_capable_column(self, backend_column):
        raise NotImplementedError(self._not_implemented_message('Synthetic bucketing'))

    def setup_result_cache_area(self):
        """ Prepare result cache area for Hybrid Queries """
        cmds = []
        if self.create_database_supported() and self._user_requested_create_backend_db:
            cmds.extend(self._create_result_cache_db())
        return cmds

    def setup_staging_area(self, staging_file):
        """ Prepare any staging area for Azure Synapse OffloadTransport """
        self._rm_dfs_dir(self.get_staging_table_location())

    def validate_type_conversions(self, staging_columns: list):
        """ Validate the staged data before we insert it into the final backend table.
            Synapse catches overflowing casts but this way we can spot them beforehand and
            provide the user with SQL to view offending values.
        """
        self._validate_final_table_casts(staging_columns, log_profile=self._log_profile_after_verification_queries)

    def partition_function_requires_granularity(self):
        """ Table partitioning not implemented at this stage on Synapse """
        return False
