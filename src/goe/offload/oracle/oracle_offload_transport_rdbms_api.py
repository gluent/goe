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

""" OffloadTransportOracleApi: Library for logic/interaction with source Oracle system during offload transport.
"""

from contextlib import contextmanager
from itertools import groupby
from typing import TYPE_CHECKING

import cx_Oracle as cxo

from goe.offload.column_metadata import match_table_column
from goe.offload.frontend_api import FRONTEND_TRACE_ID
from goe.offload.offload_constants import OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_source_table import OFFLOAD_PARTITION_TYPE_RANGE
from goe.offload.offload_transport_functions import (
    get_rdbms_connection_for_oracle,
    split_ranges_for_id_range,
)
from goe.offload.offload_transport_rdbms_api import (
    OffloadTransportRdbmsApiInterface,
    OffloadTransportRdbmsApiException,
    OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_TYPE_TEXT,
)
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_FLOAT,
    ORACLE_TYPE_NUMBER,
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_TIMESTAMP_TZ,
    ORACLE_TYPE_TIMESTAMP_LOCAL_TZ,
    ORACLE_TYPE_INTERVAL_DS,
    ORACLE_TYPE_INTERVAL_YM,
    ORACLE_TYPE_BINARY_FLOAT,
    ORACLE_TYPE_BINARY_DOUBLE,
    ORACLE_TYPE_XMLTYPE,
)
from goe.offload.oracle.oracle_offload_source_table import (
    oracle_version_is_smart_scan_unsafe,
)
from goe.util.misc_functions import id_generator

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.column_metadata import ColumnMetadataInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.offload.offload_source_data import OffloadSourcePartitions
    from goe.offload.offload_source_table import OffloadSourceTableInterface


###########################################################################
# CONSTANTS
###########################################################################

MAX_UNION_ALL_SPLITS = 1024

LOG_SQL_STATS_QUERY_TEXT = """SELECT sql_id,
   child_number,
   rows_processed,
   ROUND(cpu_time/1e6)           AS cpu_seconds,
   ROUND(user_io_wait_time/1e6)  AS io_wait_seconds,
   executions
FROM   gv$sql
WHERE  parsing_schema_name = sys_context('USERENV','SESSION_USER')
AND    module = :module
AND    action = :action
AND    sql_fulltext LIKE '%'||:action||'%'
ORDER BY sql_id, child_number"""


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# OffloadTransportOracleApi
###########################################################################


class OffloadTransportOracleApi(OffloadTransportRdbmsApiInterface):
    """Oracle specific methods"""

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
            "OffloadTransportOracleApi setup: (%s, %s)"
            % (rdbms_owner, rdbms_table_name)
        )
        self._offload_transport_auth_using_oracle_wallet = (
            offload_options.offload_transport_auth_using_oracle_wallet
        )
        self._transport_row_source_query_split_methods = [
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
        ]

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_adm_connection(self):
        if not self._rdbms_adm_conn:
            self._rdbms_adm_conn = get_rdbms_connection_for_oracle(
                self._offload_options.ora_adm_user,
                self._offload_options.ora_adm_pass,
                self._rdbms_adm_dsn,
                self._offload_options.use_oracle_wallet,
            )
        return self._rdbms_adm_conn

    def _close_adm_connection(self):
        if self._rdbms_adm_conn:
            self._rdbms_adm_conn.close()
        self._rdbms_adm_conn = None

    def _get_app_connection(self):
        if not self._rdbms_app_conn:
            self._rdbms_app_conn = get_rdbms_connection_for_oracle(
                self._offload_options.rdbms_app_user,
                self._offload_options.rdbms_app_pass,
                self._offload_transport_dsn,
                self._offload_options.use_oracle_wallet,
            )
        return self._rdbms_app_conn

    def _close_app_connection(self):
        if self._rdbms_app_conn:
            self._rdbms_app_conn.close()
        self._rdbms_app_conn = None

    def _get_fixed_goe_parameters(self):
        if self._fixed_goe_parameters is None:
            # GOE-1375
            ora_conn = self._get_adm_connection()
            if oracle_version_is_smart_scan_unsafe(ora_conn.version):
                self._fixed_goe_parameters = {"CELL_OFFLOAD_PROCESSING": "FALSE"}
            else:
                self._fixed_goe_parameters = {}
            self._close_adm_connection()
        return self._fixed_goe_parameters

    def _get_fixed_sqoop_parameters(self, max_ts_scale):
        # These are defined to match parameters set by Sqoop/Oraoop
        # Dependency on this TIME_ZONE=UTC with logic for avro_conv_fns appending UTC
        ff_scale = self._get_ts_ff_scale(max_ts_scale)
        return {
            "TRACEFILE_IDENTIFIER": "'%s'" % FRONTEND_TRACE_ID,
            "TIME_ZONE": "'UTC'",
            "NLS_TIMESTAMP_TZ_FORMAT": "'YYYY-MM-DD HH24:MI:SS.FF%s TZH:TZM'"
            % ff_scale,
            "NLS_TIMESTAMP_FORMAT": "'YYYY-MM-DD HH24:MI:SS.FF%s'" % ff_scale,
            "NLS_DATE_FORMAT": "'YYYY-MM-DD HH24:MI:SS'",
            '"_SERIAL_DIRECT_READ"': "TRUE",
        }

    def _get_ts_ff_scale(self, ts_scale):
        """Return a string scale which can be tagged onto 'HH24:MI:SS.FF'"""
        if ts_scale:
            assert isinstance(ts_scale, int)
            assert 0 <= ts_scale <= 9
        return "" if ts_scale is None else str(ts_scale)

    def _oracle_alter_session_statements(
        self,
        session_parameter_dict,
        for_plsql=False,
        include_semi_colons=False,
        escape_semi_colons=False,
    ):
        """Format ALTER SESSION statements for inclusion in a data transport operation
        return statements as a list, empty list if nothing to do
        """
        if not session_parameter_dict:
            return []
        alter_template = "ALTER SESSION SET %s=%s"
        semi_colon = "\\;" if escape_semi_colons else ";"
        if include_semi_colons:
            alter_template += semi_colon

        if for_plsql:
            plsql_wrap_template = "EXECUTE IMMEDIATE q'!%s!'"
            use_template = (plsql_wrap_template % alter_template) + semi_colon
        else:
            use_template = alter_template

        alter_session_statements = [
            use_template % (k, v) for k, v in sorted(session_parameter_dict.items())
        ]
        return alter_session_statements

    def _cx_getvalue(self, incoming):
        return (
            incoming.getvalue()
            if incoming and hasattr(incoming, "getvalue")
            else incoming
        )

    def _get_iot_single_partition_clause(self, partition_chunk):
        """Return PARTITION(partition_name) clause partition_chunk if there is a single partition.
        This is because we treat single partition IOT offloads like non-partitioned tables. It's a small
        optimization to make up for the fact we cannot split IOT partitions by rowid range.
        """
        if partition_chunk and partition_chunk.count() == 1:
            return " PARTITION ({})".format(partition_chunk.partition_names().pop())
        else:
            return ""

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def convert_query_import_expressions_on_rdbms_side(self):
        return False

    def generate_transport_action(self):
        id_str = id_generator()
        # Action in Oracle is trimmed at 32 bytes, we do the same here
        return ("%s_%s" % (id_str, self._rdbms_table_name.lower()))[:32]

    def get_id_column_for_range_splitting(
        self,
        rdbms_table: "OffloadSourceTableInterface",
    ) -> "ColumnMetadataInterface":
        if len(rdbms_table.get_primary_key_columns()) != 1:
            return None
        pk_col = match_table_column(
            rdbms_table.get_primary_key_columns()[0], rdbms_table.columns
        )
        if pk_col.data_type in (
            ORACLE_TYPE_NUMBER,
            ORACLE_TYPE_DATE,
            ORACLE_TYPE_TIMESTAMP,
        ):
            return pk_col
        return None

    def get_id_range(
        self, rdbms_col_name: str, predicate_offload_clause: str, partition_chunk=None
    ) -> tuple:
        """Function to get the MIN and MAX values for an id column.

        Used to create non-overlapping ranges for splitting IOT tables between transport processes.
        partition_chunk: Used to restrict the MIN/MAX query to a single partition IF chunk is for single partition.
        The query is structured specifically to take advantage of Oracle's MIN/MAX optimization, e.g.:
            ----------------------------------------------------------------
            | Id  | Operation                     | Name                   |
            ----------------------------------------------------------------
            |   0 | SELECT STATEMENT              |                        |
            |   1 |  NESTED LOOPS                 |                        |
            |   2 |   VIEW                        |                        |
            |   3 |    SORT AGGREGATE             |                        |
            |   4 |     PARTITION RANGE SINGLE    |                        |
            |   5 |      INDEX FULL SCAN (MIN/MAX)| GOE_RANGE_IOT_MONTH_PK |
            |   6 |   VIEW                        |                        |
            |   7 |    SORT AGGREGATE             |                        |
            |   8 |     PARTITION RANGE SINGLE    |                        |
            |   9 |      INDEX FULL SCAN (MIN/MAX)| GOE_RANGE_IOT_MONTH_PK |
            ----------------------------------------------------------------
        """
        predicate_clause = ""
        if predicate_offload_clause:
            predicate_clause = f"\n    WHERE {predicate_offload_clause}"
        min_max_row = None
        min_max_qry = """SELECT min_v.val, max_v.val
FROM  (
    SELECT MIN(%(col)s) AS val
    FROM   "%(owner)s"."%(table)s"%(partition_clause)s%(predicate_clause)s
) min_v
, (
    SELECT MAX(%(col)s) AS val
    FROM   "%(owner)s"."%(table)s"%(partition_clause)s%(predicate_clause)s
) max_v""" % {
            "owner": self._rdbms_owner,
            "table": self._rdbms_table_name,
            "col": rdbms_col_name,
            "partition_clause": self._get_iot_single_partition_clause(partition_chunk),
            "predicate_clause": predicate_clause,
        }
        self.log("Oracle SQL:\n%s" % min_max_qry, detail=VVERBOSE)
        cx = get_rdbms_connection_for_oracle(
            self._offload_options.rdbms_app_user,
            self._offload_options.rdbms_app_pass,
            self._offload_transport_dsn,
            self._offload_options.use_oracle_wallet,
        )
        try:
            ora_cursor = cx.cursor()
            min_max_row = ora_cursor.execute(min_max_qry).fetchone()
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
        finally:
            try:
                ora_cursor.close()
            except:
                pass
            try:
                cx.close()
            except:
                pass
        return min_max_row[0], min_max_row[1]

    def get_rdbms_query_cast(
        self,
        column_expression,
        rdbms_column,
        staging_column,
        max_ts_scale,
        convert_expressions_on_rdbms_side=False,
        for_spark=False,
        nan_values_as_null=False,
        for_qi=False,
    ):
        """Returns an expression suitable for reading a specific column from the RDBMS table.
        Takes column_expression as an input rather than a column name because a transformation may already have
        taken place.
        for_spark: Spark fails to process TS WITH LOCAL TIME ZONE and INTERVALs so we convert to character in the DB.
        for_qi: Allows different behaviour for Query Import.
        """
        ff_scale = self._get_ts_ff_scale(max_ts_scale)
        cast_expression = column_expression
        if rdbms_column.data_type == ORACLE_TYPE_TIMESTAMP_LOCAL_TZ:
            cast_expression = (
                f"CONCAT(CAST({column_expression} AS VARCHAR2(64)),' UTC')"
            )
        elif rdbms_column.data_type == ORACLE_TYPE_TIMESTAMP_TZ:
            # We need to cast this in the DB to ensure we use tzinfo matching the DB - not matching the client
            cast_expression = "TO_CHAR(%s,'YYYY-MM-DD HH24:MI:SS.FF%s TZH:TZM')" % (
                column_expression,
                ff_scale,
            )
        elif (
            rdbms_column.data_type == ORACLE_TYPE_DATE
            and convert_expressions_on_rdbms_side
        ):
            cast_expression = f"TO_CHAR({column_expression},'YYYY-MM-DD HH24:MI:SS')"
        elif (
            rdbms_column.data_type == ORACLE_TYPE_TIMESTAMP
            and convert_expressions_on_rdbms_side
        ):
            cast_expression = (
                f"TO_CHAR({column_expression},'YYYY-MM-DD HH24:MI:SS.FF{ff_scale}')"
            )
        elif (
            staging_column.is_string_based()
            and rdbms_column.data_type in (ORACLE_TYPE_NUMBER, ORACLE_TYPE_FLOAT)
            and convert_expressions_on_rdbms_side
        ):
            cast_expression = f"TO_CHAR({column_expression},'TM')"
        elif rdbms_column.data_type in (
            ORACLE_TYPE_INTERVAL_DS,
            ORACLE_TYPE_INTERVAL_YM,
        ) and (for_spark or for_qi):
            cast_expression = f"TO_CHAR({column_expression})"
        elif (
            rdbms_column.data_type == ORACLE_TYPE_NUMBER
            and rdbms_column.data_precision
            and rdbms_column.data_scale
            and rdbms_column.data_scale > rdbms_column.data_precision
            and for_spark
        ):
            # Oracle allows numbers with scale > precision (e.g. NUMBER(3,5)). Even with a customSchema of String
            # Spark cannot handle this (tested up to Spark 2.3):
            #   pyspark.sql.utils.AnalysisException: u'Decimal scale (5) cannot be greater than precision (3).;'
            # Therefore we have this workaround:
            cast_expression = f"CAST({column_expression} AS NUMBER)"
        elif (
            rdbms_column.data_type
            in (ORACLE_TYPE_BINARY_FLOAT, ORACLE_TYPE_BINARY_DOUBLE)
            and nan_values_as_null
        ):
            cast_expression = f"CASE WHEN {column_expression} IN ('NaN','Infinity','-Infinity') THEN NULL ELSE {column_expression} END"
        elif rdbms_column.data_type == ORACLE_TYPE_XMLTYPE:
            # Note, getClobVal() only works when the input column includes the table/alias reference.
            cast_expression = (
                f'("{self._rdbms_table_name}".{column_expression}).getClobVal()'
            )
        return cast_expression

    def get_offload_transport_sql_stats_function(
        self, rdbms_module, rdbms_action, conn_action=None
    ):
        """Return a function that executes the LOG_SQL_STATS_QUERY_TEXT query for a given
        module and action
        """
        conn = self._get_app_connection()
        conn.module = rdbms_module
        if conn_action:
            conn.action = conn_action
        cursor = conn.cursor()

        def stats_function():
            return cursor.execute(
                LOG_SQL_STATS_QUERY_TEXT,
                {"module": rdbms_module, "action": rdbms_action},
            ).fetchall()

        return stats_function

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
        """Returns a list of strings to be executed in the data transport source RDBMS session"""
        assert type(custom_session_parameters) is dict
        fixed_sqoop_parameters = self._get_fixed_sqoop_parameters(max_ts_scale)

        commands = []
        if include_fixed_sqoop:
            if for_plsql:
                commands.append(
                    "DBMS_APPLICATION_INFO.SET_MODULE('%s','%s');" % (module, action)
                )
            else:
                commands.append(
                    "BEGIN DBMS_APPLICATION_INFO.SET_MODULE('%s','%s'); END;"
                    % (module, action)
                )
            commands.extend(
                self._oracle_alter_session_statements(
                    fixed_sqoop_parameters,
                    for_plsql=for_plsql,
                    include_semi_colons=include_semi_colons,
                    escape_semi_colons=escape_semi_colons,
                )
            )
        commands.extend(
            self._oracle_alter_session_statements(
                self._get_fixed_goe_parameters(),
                for_plsql=for_plsql,
                include_semi_colons=include_semi_colons,
                escape_semi_colons=escape_semi_colons,
            )
        )
        commands.extend(
            self._oracle_alter_session_statements(
                custom_session_parameters,
                for_plsql=for_plsql,
                include_semi_colons=include_semi_colons,
                escape_semi_colons=escape_semi_colons,
            )
        )
        return commands

    def get_rdbms_session_setup_hint(self, custom_session_parameters, max_ts_scale):
        """Returns a hint containing OPT_PARAM() calls for each optimizer parameter we control.
        We always include NO_PARALLEL because we handle parallelism in Sqoop/Spark, Oracle queries should be serial.
        """

        def enquote(x):
            if '"' not in x and "'" not in x:
                return f"'{x}'"
            else:
                return x

        def valid_hint_param(x):
            if not x:
                return False
            x = x.upper().strip('"').strip("'")
            return bool(
                x
                and not x.startswith("NLS_")
                and x not in self._get_fixed_sqoop_parameters(None)
            )

        def to_opt_param(kv_dict):
            if not kv_dict:
                return ""
            return " ".join(
                "OPT_PARAM({}, {})".format(enquote(k), enquote(v))
                for k, v in kv_dict.items()
                if valid_hint_param(k)
            )

        assert type(custom_session_parameters) is dict
        opt_param_hints = (
            to_opt_param(self._get_fixed_sqoop_parameters(max_ts_scale))
            + " "
            + to_opt_param(self._get_fixed_goe_parameters())
            + " "
            + to_opt_param(custom_session_parameters)
        ).strip()
        return f"/*+ NO_PARALLEL {opt_param_hints} */"

    def get_rdbms_scn(self):
        self.debug("get_rdbms_scn()")
        ora_curs = None
        ora_conn = self._get_adm_connection()
        try:
            ora_curs = ora_conn.cursor()
            scn = ora_curs.execute("SELECT current_scn FROM v$database").fetchone()[0]
            ora_curs.close()
            self._close_adm_connection()
            return scn
        except:
            try:
                if ora_curs:
                    ora_curs.close()
                self._close_adm_connection()
            except Exception as exc:
                self.log(
                    "Failed to close RDBMS session after error: %s" % str(exc),
                    detail=VVERBOSE,
                )
            raise

    def get_transport_split_type(
        self,
        partition_chunk: "OffloadSourcePartitions",
        rdbms_table: "OffloadSourceTableInterface",
        parallelism: int,
        rdbms_partition_type: str,
        offload_by_subpartition: bool,
        predicate_offload_clause: str,
        native_range_split_available: bool = False,
    ) -> tuple:
        """
        Return split type and any tuned transport parallelism.
        If there are more partitions/subpartitions than the requested parallelism then split by
        partition/subpartition, otherwise we split the few partitions by Oracle extent (ROWID ranges),
        id ranges or MOD ranges.
        """
        self.debug("get_transport_split_type()")
        is_iot = rdbms_table.is_iot()
        id_split_col = self.get_id_column_for_range_splitting(rdbms_table)
        partition_by = None
        if partition_chunk and partition_chunk.count() > 0:
            partition_count = partition_chunk.count()
            subpartition_count = len(partition_chunk.subpartition_names())
        else:
            partition_count = 0
            subpartition_count = 0
        self.debug(f"parallelism: {parallelism}")
        tuned_parallelism = parallelism
        self.debug(f"partition_count: {partition_count}")
        self.debug(f"subpartition_count: {subpartition_count}")
        if (
            partition_count > 0
            and partition_count >= parallelism
            and partition_count <= MAX_UNION_ALL_SPLITS
        ):
            # There are more partitions than parallel threads therefore it should be
            # efficient to split by partition/subpartition, applies to both heap tables and IOTs
            if offload_by_subpartition:
                self.log(
                    "Splitting subpartitioned table by subpartition", detail=VVERBOSE
                )
                partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION
            else:
                self.log("Splitting partitioned table by partition", detail=VVERBOSE)
                partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION
        elif (
            partition_count > 0
            and subpartition_count >= parallelism
            and subpartition_count <= MAX_UNION_ALL_SPLITS
        ):
            self.log(
                "Splitting subpartitioned table by subpartition as there are fewer partitions than requested parallelism: %s < %s"
                % (str(partition_count), str(parallelism)),
                detail=VVERBOSE,
            )
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION
        elif is_iot:
            # partition_count > 1 below because we treat a single partition IOT offload like a non-partitioned one.
            if (
                partition_count > 1
                and partition_count <= MAX_UNION_ALL_SPLITS
                and rdbms_partition_type in ("RANGE", "LIST", "HASH")
            ):
                if partition_count < parallelism:
                    self.log(
                        "Splitting IOT by partition even though fewer partitions than requested parallelism: %s < %s"
                        % (str(partition_count), str(parallelism)),
                        detail=VVERBOSE,
                    )
                    tuned_parallelism = partition_count
                partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION
            elif id_split_col:
                if partition_count == 1:
                    self.log("Splitting IOT for single partition", detail=VVERBOSE)
                else:
                    self.log("Splitting non-partitioned IOT", detail=VVERBOSE)
                if id_split_col and native_range_split_available:
                    partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE
                elif id_split_col and id_split_col.is_number_based():
                    partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE
                else:
                    partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD
            else:
                if partition_count == 1:
                    self.log(
                        "Splitting IOT by MOD() for single partition", detail=VVERBOSE
                    )
                else:
                    self.log("Splitting non-partitioned IOT by MOD()", detail=VVERBOSE)
                partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD
        else:
            self.log("Splitting table into ROWID ranges", detail=VVERBOSE)
            partition_by = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT
        self.log(
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_TYPE_TEXT + partition_by, detail=VVERBOSE
        )
        assert partition_by in self._transport_row_source_query_split_methods
        return partition_by, tuned_parallelism

    def get_transport_row_source_query(
        self,
        partition_by_prm: str,
        rdbms_table: "OffloadSourceTableInterface",
        consistent_read,
        parallelism,
        offload_by_subpartition,
        mod_column,
        predicate_offload_clause: str,
        partition_chunk: "OffloadSourcePartitions" = None,
        pad: int = None,
        id_col_min=None,
        id_col_max=None,
    ) -> str:
        """Define a frontend query that will retrieve data from a table dividing it by a split method
        such as partition or rowid range.
        These row source queries can be split between a number of processes by either partition name or segment
        extent, this is defined by a PL/SQL function offload.offload_rowid_ranges which ensures we have one group
        for each degree of transport parallelism.
        Groups are numbered from 0 to (parallelism - 1).
        pad: Number of spaces with which to pad any CRs.
        """
        assert partition_by_prm in self._transport_row_source_query_split_methods

        is_iot = rdbms_table.is_iot()
        id_split_col = self.get_id_column_for_range_splitting(rdbms_table)

        if partition_by_prm == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT and is_iot:
            raise NotImplementedError(
                "RDBMS extent splitting is not supported for IOTs"
            )

        if consistent_read:
            # Do this before anything else, we want the SCN captured before any other preamble.
            transport_scn = self.get_rdbms_scn()
            self.log(
                "Using RDBMS SCN %s for consistent data retrieval" % str(transport_scn),
                detail=VVERBOSE,
            )
            scn_clause = " AS OF SCN %s" % str(transport_scn)
        else:
            scn_clause = ""

        owner_table = '"%s"."%s"' % (self._rdbms_owner, self._rdbms_table_name)
        partition_by = partition_by_prm
        union_all = self._row_source_query_union_all_clause(pad)

        if partition_by in (
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
        ):
            if partition_by == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION:
                partition_clause = "PARTITION"
                partition_names = partition_chunk.partition_names()
            else:
                partition_clause = "SUBPARTITION"
                if offload_by_subpartition:
                    # subpartition-range offloads store their subpartition names as partition names
                    # (i.e. one partition record per subpartition)
                    partition_names = partition_chunk.partition_names()
                else:
                    # partition-range offloads with few partitions but many subpartitions store their
                    # subpartition names as a list per partition
                    partition_names = partition_chunk.subpartition_names()

            # There are more partitions than parallel threads therefore it should be
            # efficient to split by partition, applies to both heap tables and IOTs
            union_branch_template = 'SELECT g.*, %(batch)s AS %(batch_col)s FROM %(owner_table)s %(part_clause)s ("%(part_name)s")%(scn_clause)s g'
            row_source = union_all.join(
                [
                    union_branch_template
                    % {
                        "batch": (i % parallelism),
                        "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                        "owner_table": owner_table,
                        "part_clause": partition_clause,
                        "scn_clause": scn_clause,
                        "part_name": p,
                    }
                    for i, p in enumerate(partition_names)
                ]
            )
        elif partition_by == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT:
            partition_param, subpartition_param = "NULL", "NULL"
            if partition_chunk and partition_chunk.count() > 0:
                partition_param = "offload_vc2_ntt(%s)" % ",".join(
                    ["'%s'" % _ for _ in partition_chunk.partition_names()]
                )
                if offload_by_subpartition:
                    subpartition_param = partition_param
                    partition_param = "NULL"
            union_branch_template = (
                "SELECT /*+ NO_INDEX(g) USE_NL(g) LEADING(r) */ g.*, %(batch)s AS %(batch_col)s "
                "FROM %(owner_table)s%(scn_clause)s g, "
                "TABLE(offload.offload_rowid_ranges('%(owner)s', '%(table)s', %(partition_param)s, %(subpartition_param)s, %(degree)s, %(batch)s)) r "
                "WHERE g.rowid BETWEEN CHARTOROWID(r.min_rowid_vc) and CHARTOROWID(r.max_rowid_vc)"
            )
            if predicate_offload_clause:
                union_branch_template += " AND (%s)" % predicate_offload_clause
            row_source = union_all.join(
                [
                    union_branch_template
                    % {
                        "batch": i,
                        "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                        "degree": parallelism,
                        "owner_table": owner_table,
                        "scn_clause": scn_clause,
                        "owner": self._rdbms_owner.upper(),
                        "table": self._rdbms_table_name.upper(),
                        "partition_param": partition_param,
                        "subpartition_param": subpartition_param,
                    }
                    for i in range(parallelism)
                ]
            )
        elif partition_by == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD:
            if not mod_column:
                raise OffloadTransportRdbmsApiException(
                    "Table %s cannot be split by mod without suitable column"
                    % owner_table
                )
            batch_expr = "MOD(ORA_HASH(%(batch_source_col)s), %(degree)s)" % {
                "batch_source_col": mod_column,
                "degree": parallelism,
            }
            union_branch_template = "SELECT g.*, %(batch)s AS %(batch_col)s FROM %(owner_table)s%(part_clause)s%(scn_clause)s g%(extra_where_clause)s"
            extra_where_clause = ""
            if predicate_offload_clause:
                extra_where_clause = " WHERE (%s)" % predicate_offload_clause
            row_source = union_branch_template % {
                "batch": batch_expr,
                "batch_col": TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
                "owner_table": owner_table,
                "part_clause": self._get_iot_single_partition_clause(partition_chunk),
                "scn_clause": scn_clause,
                "extra_where_clause": extra_where_clause,
            }
        elif partition_by == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE:
            part_clause = self._get_iot_single_partition_clause(partition_chunk)
            row_source = (
                f"SELECT g.*, {rdbms_table.enclose_identifier(id_split_col.name)} AS {TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN}"
                f"\nFROM {owner_table}{part_clause}{scn_clause} g"
            )
            if predicate_offload_clause:
                row_source += "\nWHERE (%s)" % predicate_offload_clause
        elif partition_by == TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE:
            # Create a range of min/max tuples spanning the entire id range
            id_ranges = split_ranges_for_id_range(id_col_min, id_col_max, parallelism)
            union_branch_template = (
                "SELECT g.*, %(batch)s AS %(batch_col)s FROM %(owner_table)s%(part_clause)s%(scn_clause)s g "
                "WHERE %(batch_source_col)s >= %(low_val)s AND %(batch_source_col)s < %(high_val)s"
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
                        "part_clause": self._get_iot_single_partition_clause(
                            partition_chunk
                        ),
                        "scn_clause": scn_clause,
                        "batch_source_col": id_split_col.name,
                        "low_val": lowhigh[0],
                        "high_val": lowhigh[1],
                    }
                    for i, (lowhigh) in enumerate(id_ranges)
                ]
            )
        return row_source

    def get_transport_row_source_query_hint_block(self) -> str:
        return "/*+ NO_PARALLEL */"

    def jdbc_driver_name(self) -> str:
        return "oracle.jdbc.driver.OracleDriver"

    def jdbc_url(self) -> str:
        auto_user = "/" if self._offload_transport_auth_using_oracle_wallet else ""
        return "jdbc:oracle:thin:%s@%s" % (auto_user, self._offload_transport_dsn)

    def log_sql_stats(
        self,
        module,
        action,
        payload,
        validation_polling_interval=OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
    ):
        """Retrieve Oracle SQL stats for a given module/action and log it.
        Also returns ROWS_PROCESSED in case the calling code needs it.
        Important that we use GV$SQL for this because Offload Transport tasks may have executed on different
        database instances to this orchestration code.

        Two methods are available for stats collection:

        1)  Polling thread. In this case the thread will have already collected snapshots of GV$SQL queries
            taken at an interval defined by an offload parameter. The snapshot results are held in a queue
            in the thread class instance. The queue should be drained by the caller of this method and the
            results passed in to this method as the payload parameter.

        2)  Query (default). If the payload parameter is None we need to run the query against GV$SQL now.
        """

        def log_sql_info(sql_info):
            if sql_info:
                tot_rows_processed = sum([_[2] for _ in sql_info])
                tot_cpu_seconds = sum([_[3] for _ in sql_info])
                tot_io_wait_seconds = sum([_[4] for _ in sql_info])
                tot_executions = sum([_[5] for _ in sql_info])
                row_format = "{0: <13} {1: >12} {2: >12} {3: >12} {4: >12} {5: >12}"
                self.log(OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE, detail=VVERBOSE)
                self.log(
                    row_format.format(
                        "SQL Id", "Child#", "Executions", "Rows", "CPU(s)", "IO Wait(s)"
                    ),
                    detail=VVERBOSE,
                )
                for (
                    sql_id,
                    child_number,
                    rows_processed,
                    cpu_seconds,
                    io_wait_seconds,
                    executions,
                ) in sql_info:
                    self.log(
                        row_format.format(
                            sql_id,
                            child_number,
                            executions,
                            rows_processed,
                            cpu_seconds,
                            io_wait_seconds,
                        ),
                        detail=VVERBOSE,
                    )
                self.log(
                    row_format.format(
                        "Total",
                        "",
                        tot_executions,
                        tot_rows_processed,
                        tot_cpu_seconds,
                        tot_io_wait_seconds,
                    ),
                    detail=VVERBOSE,
                )
                return tot_rows_processed

        self.debug("log_sql_stats()")
        if validation_polling_interval == OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED:
            self.log(
                "Offload transport SQL stats collection is disabled", detail=VVERBOSE
            )
            return None

        tot_rows_processed = None
        try:
            if payload:
                sql_info = []
                row_snapshots = [item for sublist in payload for item in sublist]
                for sql_id_child_num, row_data in groupby(
                    sorted(row_snapshots), lambda x: "%s:%s" % (x[0], x[1])
                ):
                    rows = list(row_data)
                    sql_id, child_number = sql_id_child_num.split(":")
                    sql_info.append(
                        (
                            sql_id,
                            child_number,
                            max([_[2] for _ in rows]),
                            max([_[3] for _ in rows]),
                            max([_[4] for _ in rows]),
                            max([_[5] for _ in rows]),
                        )
                    )
            else:
                q_params = {"module": module, "action": action}
                self.log(
                    "Searching for transport SQL matching: %s" % str(q_params),
                    detail=VVERBOSE,
                )
                fn = self.get_offload_transport_sql_stats_function(module, action)
                sql_info = fn()
            tot_rows_processed = log_sql_info(sql_info)
        finally:
            self._close_app_connection()
        return tot_rows_processed

    def log_ash_usage(self, start_datetime, module, action):
        """Retrieve Oracle Active Session History for a given module/action
        and log it.
        THIS IS CURRENTLY NOT USED because we don't want to increase our
        dependency on add-on Oracle packs
        """
        self.debug("log_ash_usage()")
        ora_conn = self._get_adm_connection()
        ash_q = """SELECT sql_id, NVL(event,'CPU'), COUNT(*) seconds
FROM  gv$active_session_history
WHERE user_id = (SELECT user_id FROM dba_users WHERE username = :rdbms_app_user)
AND   sample_time >= :start_datetime
AND   module = :module
AND   action = :action
GROUP BY sql_id, NVL(event,'CPU')
ORDER BY seconds DESC"""
        q_params = {
            "rdbms_app_user": str(self._offload_options.rdbms_app_user.upper()),
            "start_datetime": start_datetime,
            "module": module,
            "action": action,
        }
        try:
            ora_cursor = ora_conn.cursor()
            ash = ora_cursor.execute(ash_q, q_params).fetchall()
            if ash:
                event_width = max([len(_[1]) for _ in ash])
                row_format = "{0: <15} {1: <" + str(event_width) + "} {2: >8}"
                self.log("Active Session History", detail=VVERBOSE)
                self.log(
                    row_format.format("SQL Id", "Event", "Seconds"), detail=VVERBOSE
                )
                for sql_id, event, seconds in ash:
                    self.log(row_format.format(sql_id, event, seconds), detail=VVERBOSE)
        finally:
            ora_cursor.close()
            self._close_adm_connection()

    def get_rdbms_canary_query(self):
        return "SELECT dummy FROM dual"

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
        def cx_type_handler(cursor, name, default_type, size, precision, scale):
            if default_type == cxo.NUMBER:
                staging_column = match_table_column(name, staging_columns)
                if (
                    not staging_column
                    or not staging_column.safe_mapping
                    or staging_column.is_string_based()
                ):
                    # We are offloading to string and should convert the value to string
                    return cursor.var(str, 255, cursor.arraysize)
            elif default_type in (cxo.STRING, cxo.FIXED_CHAR):
                return cursor.var(str, size, cursor.arraysize)
            elif default_type in (cxo.DATETIME, cxo.TIMESTAMP):
                return cursor.var(str, 255, cursor.arraysize)

        def setup_rdbms_session(ora_cursor):
            # Dependency on this TIME_ZONE=UTC (inside offload.setup_offload_session()) with encode logic appending UTC.
            for session_statement in rdbms_session_setup_commands:
                self.log("Oracle sql: %s" % session_statement, detail=VERBOSE)
                if not self._dry_run:
                    ora_cursor.execute(session_statement)

        cx = get_rdbms_connection_for_oracle(
            self._offload_options.rdbms_app_user,
            self._offload_options.rdbms_app_pass,
            self._offload_transport_dsn,
            self._offload_options.use_oracle_wallet,
        )
        ora_cursor = cx.cursor()
        cx.outputtypehandler = cx_type_handler

        ora_cursor.arraysize = fetch_size

        self.log(
            "Importing load data with arraysize=%s, compression=%s"
            % (ora_cursor.arraysize, compress),
            detail=VERBOSE,
        )

        try:
            setup_rdbms_session(ora_cursor)
            self.log("Oracle sql: %s" % source_query, detail=VERBOSE)
            if source_binds:
                self.log("Bind values: %s" % str(source_binds), detail=VERBOSE)
            if not self._dry_run:
                if source_binds:
                    ora_cursor.execute(source_query, source_binds)
                else:
                    ora_cursor.execute(source_query)
            yield ora_cursor
        finally:
            ora_cursor.close()
            cx.close()

    def sqoop_by_query_boundary_query(self, offload_transport_parallelism):
        """Return a query providing a value range for Sqoop --boundary-query option."""
        return '"SELECT 0, {} FROM dual"'.format(offload_transport_parallelism - 1)

    def sqoop_rdbms_specific_jvm_overrides(self, rdbms_session_setup_commands) -> list:
        oraoop_d_options = [
            "-Doracle.sessionTimeZone=UTC",
            "-Doraoop.timestamp.string=true",
            "-Doraoop.jdbc.url.verbatim=true",
        ]
        if rdbms_session_setup_commands:
            oraoop_d_options += [
                "-Doraoop.oracle.session.initialization.statements=%s"
                % self._ssh_cli_safe_value("".join(rdbms_session_setup_commands))
            ]
        return oraoop_d_options

    def sqoop_rdbms_specific_options(self) -> list:
        sqoop_opts = []
        if not self._offload_options.sqoop_disable_direct:
            sqoop_opts.append("--direct")
        return sqoop_opts

    def sqoop_rdbms_specific_jvm_table_options(
        self, partition_chunk, partition_type=None, is_iot=False, consistent_read=False
    ) -> list:
        oraoop_options = []
        if is_iot and partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
            # Tactical work-a-round for CERN, sqoop offload of range partitioned IOTs requires parameter modifications
            oraoop_options += ["-Doraoop.chunk.method=partition"]
            oraoop_options += ["-Doraoop.import.consistent.read=false"]
        else:
            oraoop_options += [
                "-Doraoop.import.consistent.read={true_or_false}".format(
                    true_or_false="true" if consistent_read else "false"
                )
            ]

        if partition_chunk and partition_chunk.count() > 0:
            oraoop_options += [
                "-Doraoop.import.partitions="
                + self._ssh_cli_safe_value(",".join(partition_chunk.partition_names()))
            ]
        return oraoop_options

    def sqoop_rdbms_specific_table_options(self, rdbms_owner, table_name) -> list:
        return [
            "--table",
            self._ssh_cli_safe_value((rdbms_owner + "." + table_name).upper()),
        ]
