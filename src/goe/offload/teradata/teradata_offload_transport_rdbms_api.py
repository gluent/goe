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

""" OffloadTransportTeradataApi: Library for logic/interaction with source Oracle system during offload transport.
"""

from contextlib import contextmanager
from typing import TYPE_CHECKING

from goe.offload.column_metadata import match_table_column
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_constants import OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_transport_functions import (
    split_lists_for_id_list,
    split_ranges_for_id_range,
)
from goe.offload.offload_transport_rdbms_api import (
    OffloadTransportRdbmsApiInterface,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_AMP,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_TYPE_TEXT,
)
from goe.offload.teradata.teradata_column import (
    TERADATA_TYPE_BIGINT,
    TERADATA_TYPE_BYTEINT,
    TERADATA_TYPE_DECIMAL,
    TERADATA_TYPE_INTEGER,
    TERADATA_TYPE_NUMBER,
    TERADATA_TYPE_SMALLINT,
    TERADATA_TYPE_DATE,
    TERADATA_TYPE_TIME,
    TERADATA_TYPE_TIMESTAMP,
    TERADATA_TYPE_TIMESTAMP_TZ,
    TERADATA_TYPE_INTERVAL_DS,
    TERADATA_TYPE_INTERVAL_YM,
)
from goe.util.misc_functions import id_generator

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.column_metadata import ColumnMetadataInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.offload.offload_source_table import OffloadSourceTableInterface


###########################################################################
# CONSTANTS
###########################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# OffloadTransportTeradataApi
###########################################################################


class OffloadTransportTeradataApi(OffloadTransportRdbmsApiInterface):
    """Library for logic/interaction with source Oracle system during offload transport."""

    def __init__(
        self,
        rdbms_owner: str,
        rdbms_table_name: str,
        offload_options: "OrchestrationConfig",
        messages: "OffloadMessages",
        dry_run=False,
    ):
        super().__init__(
            rdbms_owner,
            rdbms_table_name,
            offload_options,
            messages,
            dry_run=dry_run,
        )
        self.debug(
            "OffloadTransportTeradataApi setup: (%s, %s)"
            % (rdbms_owner, rdbms_table_name)
        )
        self._transport_row_source_query_split_methods = [
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_AMP,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP,
        ]
        self._transport_frontend_api = None
        self._teradata_amps = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_transport_frontend_api(self):
        """Return a frontend API for use when transporting data.
        Connects as the APP user because that is what we use for transport, not the ADM user.
        """
        if self._transport_frontend_api is None:
            self._transport_frontend_api = frontend_api_factory(
                self._offload_options.db_type,
                self._offload_options,
                self._messages,
                conn_user_override=self._offload_options.rdbms_app_user,
                dry_run=self._dry_run,
            )
        return self._transport_frontend_api

    def _get_ts_ff_scale(self, ts_scale):
        """Return a string scale which can be tagged onto 'HH24:MI:SS.FF'"""
        if ts_scale:
            assert isinstance(ts_scale, int)
            assert 0 <= ts_scale <= 6
        return "" if ts_scale is None else str(ts_scale)

    def _table_amps(self, rdbms_table) -> list:
        if self._teradata_amps is None:
            frontend_api = rdbms_table.get_frontend_api()
            sql = "SELECT Vproc FROM DBC.TableSizeV WHERE DatabaseName = ? AND TableName = ? ORDER BY CurrentPerm DESC"
            rows = frontend_api.execute_query_fetch_all(
                sql,
                query_params=[rdbms_table.owner, rdbms_table.table_name],
                log_level=VVERBOSE,
            )
            self._teradata_amps = [_[0] for _ in rows] if rows else []
        return self._teradata_amps

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def generate_transport_action(self):
        id_str = id_generator()
        return "%s_%s" % (id_str, self._rdbms_table_name.lower())

    def get_id_range(
        self, rdbms_col_name: str, predicate_offload_clause: str, partition_chunk=None
    ) -> tuple:
        """Function to get the MIN and MAX values for an id column.

        Used to create non-overlapping ranges for splitting IOT tables between transport processes.
        partition_chunk: Used to restrict the MIN/MAX query to a single partition IF chunk is for single partition.
        """
        predicates = []
        if partition_chunk and partition_chunk.count() == 1:
            predicates.append(
                "PARTITION = {}".format(partition_chunk.partition_names().pop())
            )
        if predicate_offload_clause:
            predicates.append(predicate_offload_clause)

        predicate = ""
        if predicates:
            predicate = "\nWHERE " + "\nAND ".join(predicates)

        min_max_qry = (
            'SELECT MIN(%(col)s), MAX(%(col)s) FROM "%(owner)s"."%(table)s"%(predicate)s'
            % {
                "owner": self._rdbms_owner,
                "table": self._rdbms_table_name,
                "col": rdbms_col_name,
                "predicate": predicate,
            }
        )
        transport_frontend_api = self._get_transport_frontend_api()
        min_max_row = transport_frontend_api.execute_query_fetch_one(
            min_max_qry, log_level=VVERBOSE
        )
        if min_max_row:
            self.log(
                "MIN/MAX: %s (%s)/%s (%s)"
                % (
                    min_max_row[0],
                    type(min_max_row[0]),
                    min_max_row[1],
                    type(min_max_row[1]),
                ),
                detail=VVERBOSE,
            )
            return min_max_row[0], min_max_row[1]
        else:
            return None, None

    def get_offload_transport_sql_stats_function(
        self, rdbms_module, rdbms_action, conn_action=None
    ):
        raise NotImplementedError(
            "Teradata get_offload_transport_sql_stats_function() not implemented"
        )

    def get_rdbms_query_cast(
        self,
        column_expression,
        rdbms_column,
        staging_column,
        max_ts_scale,
        convert_expressions_on_rdbms_side=False,
        for_spark=False,
        nan_values_as_null=False,
    ):
        """Returns an expression suitable for reading a specific column from the RDBMS table"""
        ff_scale = self._get_ts_ff_scale(max_ts_scale)
        cast_expression = column_expression
        if rdbms_column.data_type == TERADATA_TYPE_TIMESTAMP_TZ:
            # We need to cast this in the DB to ensure we use tzinfo matching the DB - not matching the client
            cast_expression = (
                "TO_CHAR(%s AT TIME ZONE '00:00','YYYY-MM-DD HH24:MI:SS.FF%s TZH:TZM')"
                % (column_expression, ff_scale)
            )
        elif (
            rdbms_column.data_type == TERADATA_TYPE_DATE
            and convert_expressions_on_rdbms_side
        ):
            cast_expression = "TO_CHAR(%s,'YYYY-MM-DD')" % column_expression
        elif rdbms_column.data_type == TERADATA_TYPE_TIMESTAMP:
            cast_expression = (
                "TO_CHAR(%s AT TIME ZONE '00:00','YYYY-MM-DD HH24:MI:SS.FF%s')"
                % (column_expression, ff_scale)
            )
        elif rdbms_column.data_type == TERADATA_TYPE_TIME:
            cast_expression = "TO_CHAR(%s AT TIME ZONE '00:00','HH24:MI:SS.FF%s')" % (
                column_expression,
                ff_scale,
            )
        elif (
            staging_column.is_string_based()
            and rdbms_column.data_type
            in (
                TERADATA_TYPE_NUMBER,
                TERADATA_TYPE_BIGINT,
                TERADATA_TYPE_BYTEINT,
                TERADATA_TYPE_INTEGER,
                TERADATA_TYPE_SMALLINT,
                TERADATA_TYPE_DECIMAL,
            )
            and convert_expressions_on_rdbms_side
        ):
            cast_expression = "TO_CHAR(%s,'TM')" % column_expression
        elif rdbms_column.data_type in (
            TERADATA_TYPE_INTERVAL_DS,
            TERADATA_TYPE_INTERVAL_YM,
        ) and (convert_expressions_on_rdbms_side or for_spark):
            cast_expression = "TO_CHAR(%s)" % column_expression
        # Teradata appears to not officially have NaN or Inf
        # elif rdbms_column.data_type == TERADATA_TYPE_DOUBLE and nan_values_as_null:
        #     cast_expression = "CASE WHEN %s IN ('NaN','Infinity','-Infinity') THEN NULL ELSE %s END"\
        #                       % (column_expression, column_expression)
        return cast_expression

    def get_rdbms_session_setup_commands(
        self,
        module,
        action,
        custom_session_parameters,
        include_fixed_sqoop=True,
        for_plsql=False,
        include_semi_colons=False,
        escape_semi_colons=False,
        max_ts_scale=None,
    ) -> list:
        """Return an empty command signifying no commands are to be executed.
        This may become an actual list of commands if the need arises.
        """
        commands = []
        return commands

    def get_rdbms_session_setup_hint(self, custom_session_parameters, max_ts_scale):
        """No hints on Teradata"""
        return ""

    def get_rdbms_scn(self):
        raise NotImplementedError("Teradata get_rdbms_scn() not implemented")

    def get_id_column_for_range_splitting(
        self, rdbms_table: "OffloadSourceTableInterface"
    ) -> "ColumnMetadataInterface":
        if len(rdbms_table.get_primary_key_columns()) != 1:
            return None
        pk_col = match_table_column(
            rdbms_table.get_primary_key_columns()[0], rdbms_table.columns
        )
        if pk_col.is_number_based() or pk_col.is_date_based():
            return pk_col
        return None

    def get_transport_split_type(
        self,
        partition_chunk,
        rdbms_table,
        parallelism,
        rdbms_partition_type,
        offload_by_subpartition: bool,
        predicate_offload_clause: str,
        native_range_split_available: bool = False,
    ) -> tuple:
        """
        Return split type and any tuned transport parallelism.

        Splitter decisions:
        1) If # partitions >= parallelism then split by partition. Parallelism unaffected.
        2) If we have a primary index, a partition or predicate filter and # AMPs >= parallelism then split by HASHAMP.
           Parallelism unaffected.
        3) If we a primary key then split by id range. Parallelism unaffected.
        4) If we have a primary index, a partition or predicate filter and # AMPs >= parallelism then split by HASHAMP.
           Parallelism tuned down to # AMPs.
        5) Probably a full offload without a primary key. Split by AMP and tuned parallelism down to # AMPs.
        """

        def hashamp_transport_available(
            partition_count, predicate_offload_clause, primary_index_cols
        ) -> bool:
            """
            Return True/False if HASHAMP splitter is available.
            We can't use AMP splitter when offloading by partition or predicate because no additional WHERE clause
            can be supplied but we can combine a secondary filter with a HASHAMP clause as long as we have a
            PRIMARY INDEX we can utilise.
            """
            hashamp_available = bool(
                (partition_count > 0 or predicate_offload_clause) and primary_index_cols
            )
            self.log(
                f"HASHAMP split type available: {hashamp_available}", detail=VVERBOSE
            )
            if not hashamp_available:
                self.debug(f"partition_count: {partition_count}")
                self.debug(f"predicate_offload_clause: {predicate_offload_clause}")
                self.debug(f"primary_index_cols: {primary_index_cols}")
            return hashamp_available

        self.debug("get_transport_split_type()")
        id_split_col = self.get_id_column_for_range_splitting(rdbms_table)
        primary_index_cols = rdbms_table.get_primary_index_columns()
        partition_by = None
        if partition_chunk and partition_chunk.count() > 0:
            partition_count = partition_chunk.count()
        else:
            partition_count = 0
        self.debug(f"parallelism: {parallelism}")
        tuned_parallelism = parallelism
        self.debug(f"partition_count: {partition_count}")
        if partition_count > 0 and partition_count >= parallelism:
            # There are more partitions than parallel threads therefore it should be efficient to split by partition.
            self.log("Splitting partitioned table by partition", detail=VVERBOSE)
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION
        elif (
            hashamp_transport_available(
                partition_count, predicate_offload_clause, primary_index_cols
            )
            and len(self._table_amps(rdbms_table)) >= parallelism
        ):
            self.log("Splitting table by HASHAMP", detail=VVERBOSE)
            # Initially use HASHAMP if #AMPS >= parallelism. Then try id range and then resort to HASHAMP again.
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP
        elif id_split_col and native_range_split_available:
            self.log("Splitting table into native id ranges", detail=VVERBOSE)
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE
        elif id_split_col and id_split_col.is_number_based():
            self.log("Splitting table into numeric id ranges", detail=VVERBOSE)
            # TODO we should extend this to other index types, e.g. PRIMARY INDEX or UNIQUE INDEX
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE
        elif hashamp_transport_available(
            partition_count, predicate_offload_clause, primary_index_cols
        ):
            self.log(
                "Splitting table by HASHAMP and reducing parallelism to AMP count",
                detail=VVERBOSE,
            )
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP
            tuned_parallelism = len(self._table_amps(rdbms_table))
        elif partition_count == 0 and not predicate_offload_clause:
            self.log(
                "Splitting table by AMP and reducing parallelism to AMP count",
                detail=VVERBOSE,
            )
            # TODO If parallelism > AMP count then we should cap parallelism to AMP count?
            # TODO Need to test what happens if we offload with parallelism > AMP count.
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_AMP
            tuned_parallelism = len(self._table_amps(rdbms_table))
        elif partition_count > 0:
            # This will be inefficient but, if no better options, we'll just split by partition anyway
            # and reduce the parallelism to match.
            self.log("Splitting partitioned table by partition", detail=VVERBOSE)
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION
            tuned_parallelism = partition_count

        self.log(
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_TYPE_TEXT + partition_by, detail=VVERBOSE
        )
        assert partition_by in self._transport_row_source_query_split_methods
        return partition_by, tuned_parallelism

    def get_transport_row_source_query(
        self,
        partition_by_prm,
        rdbms_table,
        consistent_read,
        parallelism,
        offload_by_subpartition,
        mod_column,
        predicate_offload_clause: str,
        partition_chunk=None,
        pad=None,
        id_col_min=None,
        id_col_max=None,
    ) -> str:
        """Define a frontend query that will retrieve data from a table dividing it by a split method
        such as partition or id range.
        Groups are numbered from 0 to (parallelism - 1).
        pad: Number of spaces with which to pad any CRs.
        """

        def get_chunk_partition_filter(
            pseudo_part_column, partition_chunk, filter_operator="AND"
        ):
            partition_filter = ""
            if partition_count > 0:
                partition_csv = ",".join(partition_chunk.partition_names())
                partition_filter = (
                    f" {filter_operator} {pseudo_part_column} IN ({partition_csv})"
                )
            return partition_filter

        assert partition_by_prm in self._transport_row_source_query_split_methods

        owner_table = '"%s"."%s"' % (self._rdbms_owner, self._rdbms_table_name)
        id_split_col = self.get_id_column_for_range_splitting(
            rdbms_table,
        )
        if partition_chunk and partition_chunk.count() > 0:
            partition_count = partition_chunk.count()
            pseudo_part_column = rdbms_table.teradata_partition_pseudo_column()
        else:
            partition_count = 0
            pseudo_part_column = None

        union_all = self._row_source_query_union_all_clause(pad)

        if partition_by_prm == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION:
            part_csvs = split_lists_for_id_list(
                partition_chunk.partition_names(), parallelism, as_csvs=True
            )
            union_branch_template = "SELECT g.*, %(batch)s AS %(batch_col)s FROM %(owner_table)s g WHERE %(pseudo_column)s IN (%(part_csv)s)"
            row_source = union_all.join(
                [
                    union_branch_template
                    % {
                        "batch": (i % parallelism),
                        "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                        "owner_table": owner_table,
                        "pseudo_column": pseudo_part_column,
                        "part_csv": csv,
                    }
                    for i, csv in enumerate(part_csvs)
                    if csv
                ]
            )
        elif partition_by_prm == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE:
            row_source = (
                f"SELECT g.*, {rdbms_table.enclose_identifier(id_split_col.name)} AS {TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN}"
                f"\nFROM {owner_table} g"
            )
            if predicate_offload_clause:
                row_source += "\nWHERE (%s)" % predicate_offload_clause
        elif partition_by_prm == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE:
            partition_filter = get_chunk_partition_filter(
                pseudo_part_column, partition_chunk
            )
            # Create a range of min/max tuples spanning the entire id range
            id_ranges = split_ranges_for_id_range(id_col_min, id_col_max, parallelism)
            union_branch_template = (
                "SELECT g.*, %(batch)s AS %(batch_col)s FROM %(owner_table)s g "
                "WHERE %(batch_source_col)s >= %(low_val)s AND %(batch_source_col)s < %(high_val)s%(partition_filter)s"
            )
            if predicate_offload_clause:
                union_branch_template += " AND (%s)" % predicate_offload_clause
            row_source = union_all.join(
                [
                    union_branch_template
                    % {
                        "batch": (i % parallelism),
                        "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                        "owner_table": owner_table,
                        "batch_source_col": id_split_col.name,
                        "low_val": lowhigh[0],
                        "high_val": lowhigh[1],
                        "partition_filter": partition_filter,
                    }
                    for i, (lowhigh) in enumerate(id_ranges)
                ]
            )
        elif partition_by_prm == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_AMP:
            table_amps = self._table_amps(rdbms_table)
            amp_csvs = split_lists_for_id_list(table_amps, parallelism, as_csvs=True)
            union_branch_template = "SELECT g.*, %(batch)s AS %(batch_col)s FROM TDAMPCOPY(ON %(owner_table)s USING AMPList(%(amp_csv)s)) AS g"
            row_source = union_all.join(
                [
                    union_branch_template
                    % {
                        "batch": (i % parallelism),
                        "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                        "owner_table": owner_table,
                        "amp_csv": csv,
                    }
                    for i, csv in enumerate(amp_csvs)
                    if csv
                ]
            )
        elif partition_by_prm == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP:
            # Split by HASHAMP taken from example if Teradata docs:
            #     HASHAMP (HASHBUCKET (HASHROW (column_1,column_2)))
            partition_filter = get_chunk_partition_filter(
                pseudo_part_column, partition_chunk
            )
            table_amps = self._table_amps(rdbms_table)
            amp_csvs = split_lists_for_id_list(table_amps, parallelism, as_csvs=True)
            pi_col_csv = ",".join(
                rdbms_table.enclose_identifier(_.name)
                for _ in rdbms_table.get_primary_index_columns()
            )
            union_branch_template = "SELECT g.*, %(batch)s AS %(batch_col)s FROM %(owner_table)s g WHERE HASHAMP(HASHBUCKET(HASHROW(%(pi_col_csv)s))) = %(batch)s%(partition_filter)s"
            if predicate_offload_clause:
                union_branch_template += " AND (%s)" % predicate_offload_clause
            row_source = union_all.join(
                [
                    union_branch_template
                    % {
                        "batch": (i % parallelism),
                        "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                        "owner_table": owner_table,
                        "pi_col_csv": pi_col_csv,
                        "partition_filter": partition_filter,
                        "amp_csv": csv,
                    }
                    for i, csv in enumerate(amp_csvs)
                    if csv
                ]
            )
        else:
            raise NotImplementedError(
                f"Unsupported Teradata extraction splitter: {partition_by_prm}"
            )
        return row_source

    def get_transport_row_source_query_hint_block(self) -> str:
        return ""

    def jdbc_driver_name(self) -> str:
        return "com.teradata.jdbc.TeraDriver"

    def jdbc_url(self):
        return f"jdbc:teradata://{self._offload_transport_dsn}"

    def log_sql_stats(
        self,
        module,
        action,
        payload,
        validation_polling_interval=OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
    ):
        return None

    def get_rdbms_canary_query(self):
        return "SELECT CURRENT_USER AS c"

    @contextmanager
    def query_import_extraction(
        self,
        staging_columns,
        source_query,
        source_binds,
        fetch_size,
        compress,
        rdbms_session_setup_commands,
    ):
        transport_frontend_api = self._get_transport_frontend_api()

        self.log(
            "Importing load data with arraysize=%s, compression=%s"
            % (fetch_size, compress),
            detail=VERBOSE,
        )

        try:
            db_cursor = transport_frontend_api.execute_query_get_cursor(
                source_query, query_params=source_binds, log_level=VERBOSE
            )
            yield db_cursor
        finally:
            db_cursor.close()
            del transport_frontend_api

    def sqoop_rdbms_specific_jvm_overrides(self, rdbms_session_setup_commands) -> list:
        return []

    def sqoop_rdbms_specific_jvm_table_options(
        self, partition_chunk, partition_type=None, is_iot=False, consistent_read=False
    ) -> list:
        return []

    def sqoop_rdbms_specific_options(self) -> list:
        return ["--driver=com.teradata.jdbc.TeraDriver"]

    def sqoop_rdbms_specific_table_options(self, rdbms_owner, table_name) -> list:
        return ["--table", self._ssh_cli_safe_value((rdbms_owner + "." + table_name))]
