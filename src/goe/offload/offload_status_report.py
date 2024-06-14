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

from copy import copy
import json
import os
import re
import sys
import traceback
from datetime import datetime
from numpy import datetime64
from optparse import OptionGroup, SUPPRESS_HELP

from jinja2 import Environment, FileSystemLoader

from goe.goe import (
    init_log,
    version,
    get_log_fh_name,
    log_command_line,
    log_timestamp,
    init,
    log,
    get_log_fh,
    get_common_options,
    get_rdbms_db_name,
)
from goe.config.orchestration_config import OrchestrationConfig
from goe.exceptions import OffloadOptionError
from goe.offload import offload_constants
from goe.offload.backend_api import (
    REPORT_ATTR_BACKEND_DISPLAY_NAME,
    REPORT_ATTR_BACKEND_HOST_INFO_TYPE,
    REPORT_ATTR_BACKEND_HOST_INFO,
)
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.factory.backend_table_factory import backend_table_factory
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_functions import STARTS_WITH_DATE_PATTERN_RE
from goe.offload.offload_messages import (
    OffloadMessages,
    VERBOSE,
    VVERBOSE,
    SUPPRESS_STDOUT,
)
from goe.offload.offload_metadata_functions import (
    decode_metadata_incremental_high_values,
    INCREMENTAL_PREDICATE_TYPE_LIST,
)
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.oracle.oracle_offload_source_table import (
    oracle_datetime_literal_to_python,
)
from goe.orchestration import command_steps, orchestration_constants
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.persistence.orchestration_metadata import OrchestrationMetadata
from goe.util.misc_functions import backtick_sandwich, format_json_list, plural
from goe.util.ora_query import OracleQuery, OracleQueryException, get_oracle_connection
from goe.util.goe_log import log_exception


# Default output formats...
HTML = "html"
TEXT = "text"
JSON = "json"
RAW = "raw"
CSV = "csv"
DETAIL_LEVEL = "detail"
SUMMARY_LEVEL = "summary"
DEFAULT_OUTPUT_FORMAT = TEXT
DEFAULT_OUTPUT_LEVEL = SUMMARY_LEVEL
DEFAULT_REPORT_NAME = "Offload_Status_Report"
DEFAULT_REPORT_DIRECTORY = "%s/log" % os.environ["OFFLOAD_HOME"]
DEFAULT_CSV_DELIMITER = ","
DEFAULT_CSV_ENCLOSURE = '"'
DEFAULT_DEMO_MODE = False


# Report formatting...
divider = "=" * 100
sub_divider = "-" * 100


# Data model constants for dict keys...
SOURCE_OWNER = "SOURCE_OWNER"
SOURCE_TABLE = "SOURCE_TABLE"
OFFLOADED_TABLES = "_OFFLOADED_TABLES"
OFFLOADED_PARTS = "_OFFLOADED_PARTS"
OFFLOADED_BYTES = "_OFFLOADED_BYTES"
OFFLOADED_ROWS = "_OFFLOADED_ROWS"
RETAINED_BYTES = "_RETAINED_BYTES"
RETAINED_PARTS = "_RETAINED_PARTS"
RETAINED_ROWS = "_RETAINED_ROWS"
RECLAIMABLE_BYTES = "_RECLAIMABLE_BYTES"
RECLAIMABLE_PARTS = "_RECLAIMABLE_PARTS"
RECLAIMABLE_ROWS = "_RECLAIMABLE_ROWS"
OFFLOAD_TYPE = "OFFLOAD_TYPE"
OFFLOAD_OWNER = "OFFLOAD_OWNER"
OFFLOAD_TABLE = "OFFLOAD_TABLE"
OFFLOAD_TABLE_EXISTS = "OFFLOAD_TABLE_EXISTS"
OFFLOAD_PART_KEY = "OFFLOAD_PART_KEY"
HYBRID_OWNER = "HYBRID_OWNER"
HYBRID_VIEW = "HYBRID_VIEW"
HYBRID_VIEW_TYPE = "HYBRID_VIEW_TYPE"
HYBRID_EXTERNAL_TABLE = "HYBRID_EXTERNAL_TABLE"
AAPD_OBJECTS = "AAPD_OBJECTS"
OBJECT_OWNER = "OBJECT_OWNER"
OBJECT_NAME = "OBJECT_NAME"
OBJECT_TYPE_DB = "OBJECT_TYPE_DB"
OBJECT_TYPE_GOE = "OBJECT_TYPE_GOE"
JOIN_OBJECTS_RDBMS = "JOIN_OBJECTS_RDBMS"
JOIN_OBJECTS_OFFLOAD = "JOIN_OBJECTS_OFFLOAD"
INCREMENTAL_UPDATE_OBJECTS_RDBMS = "INCREMENTAL_UPDATE_OBJECTS_RDBMS"
INCREMENTAL_UPDATE_OBJECTS_OFFLOAD = "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD"
DEPENDENT_OBJECTS = "DEPENDENT_OBJECTS"
INCREMENTAL_KEY = "INCREMENTAL_KEY"
INCREMENTAL_HIGH_VALUE = "INCREMENTAL_HIGH_VALUE"
INCREMENTAL_PREDICATE_TYPE = "INCREMENTAL_PREDICATE_TYPE"
INCREMENTAL_PREDICATE_VALUE = "INCREMENTAL_PREDICATE_VALUE"
INCREMENTAL_RANGE = "INCREMENTAL_RANGE"
INCREMENTAL_UPDATE_METHOD = "INCREMENTAL_UPDATE_METHOD"
OFFLOAD_BUCKET_COUNT = "OFFLOAD_BUCKET_COUNT"
OFFLOAD_BUCKET_COLUMN = "OFFLOAD_BUCKET_COLUMN"
OFFLOAD_BUCKET_METHOD = "OFFLOAD_BUCKET_METHOD"
OFFLOAD_SORT_COLUMNS = "OFFLOAD_SORT_COLUMNS"
OFFLOAD_PARTITION_FUNCTIONS = "OFFLOAD_PARTITION_FUNCTIONS"
OFFLOAD_VERSION = "OFFLOAD_VERSION"
SOURCE_IOT_TYPE = "SOURCE_IOT_TYPE"

SUMMARY_KEYS = [
    OFFLOADED_TABLES,
    OFFLOADED_BYTES,
    OFFLOADED_PARTS,
    OFFLOADED_ROWS,
    RETAINED_BYTES,
    RETAINED_PARTS,
    RETAINED_ROWS,
    RECLAIMABLE_BYTES,
    RECLAIMABLE_PARTS,
    RECLAIMABLE_ROWS,
]

MULTIVALUE_KEYS = [
    AAPD_OBJECTS,
    DEPENDENT_OBJECTS,
    INCREMENTAL_UPDATE_OBJECTS_RDBMS,
    INCREMENTAL_UPDATE_OBJECTS_OFFLOAD,
    JOIN_OBJECTS_RDBMS,
    JOIN_OBJECTS_OFFLOAD,
]


# General...
SUCCESS = 0
FAILURE = 1


class OffloadStatusReportException(Exception):
    pass


class OffloadStatusReport(object):
    """Class for generating the Offload Status Report data and optional report."""

    def __init__(self, orchestration_config, messages, ora_adm_conn=None):
        """OffloadStatusReport constructor. Essentially a range of the database connection parameters."""
        self._messages = messages
        self._orchestration_config = orchestration_config

        # Log all parameters (except the password)
        self._messages.debug("Parameters:")
        self._messages.debug(
            "%-50s : %s" % ("ora_adm_user", self._orchestration_config.ora_adm_user),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-50s : %s" % ("oracle_dsn", self._orchestration_config.oracle_dsn),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-50s : %s"
            % ("ora_adm_conn", "initialized" if ora_adm_conn else "not initialized"),
            detail=SUPPRESS_STDOUT,
        )

        self._backend_api = backend_api_factory(
            self._orchestration_config.target,
            self._orchestration_config,
            self._messages,
            dry_run=(not self._execute),
        )
        self._backend_info = self._backend_api.backend_report_info()
        self._backend_db_type = self._orchestration_config.target

        self._messages.debug(
            "%-50s : %s" % ("_backend_db_type", self._backend_db_type),
            detail=SUPPRESS_STDOUT,
        )
        for k, v in self._backend_info.items():
            self._messages.debug(
                "%-50s : %s" % ("_backend_info.%s" % k, v), detail=SUPPRESS_STDOUT
            )

        # RDBMS initialisation...
        if ora_adm_conn:
            self._ora_conn = ora_adm_conn
            self._messages.debug("Using existing Oracle connection")
        else:
            self._ora_conn = get_oracle_connection(
                self._orchestration_config.ora_adm_user,
                self._orchestration_config.ora_adm_pass,
                self._orchestration_config.oracle_dsn,
                self._orchestration_config.use_oracle_wallet,
            )
            self._messages.debug("Oracle connection made")
        self._ora_service = OracleQuery.fromconnection(self._ora_conn)
        self._ora_adm_user = self._orchestration_config.ora_adm_user
        self._ora_adm_pass = self._orchestration_config.ora_adm_pass
        self._ora_db_name = get_rdbms_db_name(
            self._orchestration_config, ora_conn=self._ora_conn
        )
        self._oracle_dsn = self._orchestration_config.oracle_dsn
        self._db_type = offload_constants.DBTYPE_ORACLE
        self._use_oracle_wallet = self._orchestration_config.use_oracle_wallet

        self._set_module("Offload Status Report")

        self._messages.debug(
            "%-50s : %s"
            % ("_ora_conn", "initialized" if self._ora_conn else "not initialized"),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-50s : %s"
            % (
                "_ora_service",
                "initialized" if self._ora_service else "not initialized",
            ),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-50s : %s" % ("ora_db_name", self._ora_db_name), detail=SUPPRESS_STDOUT
        )

        self._repo_client = orchestration_repo_client_factory(
            self._orchestration_config, self._messages, dry_run=(not self._execute)
        )

        # General initialisations...
        self._execute = True
        self._start_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self._start_time = datetime.now().strftime("%d %B %Y %H:%M:%S")

        self._messages.debug(
            "%-50s : %s" % ("_execute", self._execute), detail=SUPPRESS_STDOUT
        )
        self._messages.debug(
            "%-50s : %s" % ("_start_ts", self._start_ts), detail=SUPPRESS_STDOUT
        )
        self._messages.debug(
            "%-50s : %s" % ("_start_time", self._start_time), detail=SUPPRESS_STDOUT
        )

        self._messages.debug("OffloadStatusReport object initialized")

    def __del__(self):
        """Destructor."""
        if hasattr(self, "_ora_conn"):
            self._ora_conn.close()

    def _set_module(self, module):
        """Set V$SESSION.MODULE."""
        self._ora_conn.module = "{0:.64}".format(module)
        self._messages.debug("Set module to %s" % module, SUPPRESS_STDOUT)

    def _set_action(self, action):
        """Set V$SESSION.ACTION."""
        self._ora_conn.action = "{0:.64}".format(action)
        self._messages.debug("Set action to %s" % action, SUPPRESS_STDOUT)

    def _set_client_info(self, client_info):
        """Set V$SESSION.CLIENT_INFO."""
        self._ora_conn.clientinfo = client_info
        self._messages.debug("Set clientinfo to %s" % client_info, SUPPRESS_STDOUT)

    def _format_table_name(self, schema_name, table_name, uppercase=True):
        """Return qualified table name in uppercase if required."""
        qualified_table_name = "%s.%s" % (schema_name, table_name)
        return qualified_table_name.upper() if uppercase else qualified_table_name

    def _valid_sql_name(self, sql_name):
        """Validate that the schema or table name is a valid SQL name (return boolean)."""
        try:
            self._ora_service.execute(
                "SELECT DBMS_ASSERT.SIMPLE_SQL_NAME(:name) FROM dual",
                binds={"name": sql_name},
            )
            return True
        except OracleQueryException as exc:
            return False

    def _initialise_metadata_dict(self):
        """Return dictionary of metadata pertaining to this report"""
        return {
            "version": version(),
            "schema": self._schema,
            "table": self._table,
            "output_format": self._output_format,
            "output_level": self._output_level,
            "report_name": self._report_name,
            "report_directory": self._report_directory,
            "csv_delimiter": self._csv_delimiter,
            "csv_enclosure": self._csv_enclosure,
            "demo_mode": self._demo_mode,
            "db_name": self._ora_db_name,
            "db_login_user": self._ora_adm_user,
            "db_connection": self._oracle_dsn,
            "run_date": self._start_time,
            "backend_db_type": self._backend_info[REPORT_ATTR_BACKEND_DISPLAY_NAME],
            "backend_host_label": self._backend_info[
                REPORT_ATTR_BACKEND_HOST_INFO_TYPE
            ],
            "backend_host_value": self._backend_info[REPORT_ATTR_BACKEND_HOST_INFO],
        }

    def _initialise_summary_dict(self):
        return {
            OFFLOADED_TABLES: 0,
            OFFLOADED_PARTS: 0,
            OFFLOADED_BYTES: 0,
            OFFLOADED_ROWS: 0,
            RETAINED_BYTES: 0,
            RETAINED_PARTS: 0,
            RETAINED_ROWS: 0,
            RECLAIMABLE_BYTES: 0,
            RECLAIMABLE_PARTS: 0,
            RECLAIMABLE_ROWS: 0,
        }

    def _get_offloaded_tables(self):
        """Return the set of offloaded tables to cycle through for the report.

        Data format:
            {
              SOURCE_OWNER:                string
              SOURCE_TABLE:                string
              OFFLOAD_TYPE:                string
              OFFLOAD_OWNER:               string
              OFFLOAD_TABLE:               string
              HYBRID_OWNER:                string
              HYBRID_VIEW:                 string
              HYBRID_VIEW_TYPE:            string
              HYBRID_EXTERNAL_TABLE:       string
              INCREMENTAL_KEY:             string
              INCREMENTAL_HIGH_VALUE:      string
              INCREMENTAL_RANGE:           string
              INCREMENTAL_PREDICATE_TYPE:  string
              INCREMENTAL_PREDICATE_VALUE: string
              OFFLOAD_BUCKET_COLUMN:       string
              OFFLOAD_BUCKET_METHOD:       string
              OFFLOAD_BUCKET_COUNT:        string
              OFFLOAD_VERSION:             string
              OFFLOAD_SORT_COLUMNS:        string
              OFFLOAD_PARTITION_FUNCTIONS: string
              SOURCE_IOT_TYPE:             string
            }
        """

        self._messages.log("Get offloaded tables for report", detail=VERBOSE)

        sql = """SELECT o.offloaded_owner AS source_owner
,      o.offloaded_table AS source_table
,      o.offload_type
,      o.hadoop_owner    AS offload_owner
,      o.hadoop_table    AS offload_table
,      o.hybrid_owner
,      o.hybrid_view
,      o.hybrid_view_type
,      o.hybrid_external_table
,      o.incremental_key
,      o.incremental_high_value
,      o.incremental_range
,      o.incremental_predicate_type
,      o.incremental_predicate_value
,      o.offload_bucket_column
,      o.offload_bucket_method
,      o.offload_bucket_count
,      o.offload_version
,      o.offload_sort_columns
,      o.offload_partition_functions
,      t.iot_type AS source_iot_type
FROM  {goe_adm_schema}.offload_objects o
       LEFT OUTER JOIN
       dba_all_tables                  t
       ON (    t.owner      = o.offloaded_owner
           AND t.table_name = o.offloaded_table )
WHERE (o.offloaded_owner  = :offloaded_owner OR :offloaded_owner IS NULL)
AND   (o.offloaded_table  = :offloaded_table OR :offloaded_table IS NULL)
AND    o.hybrid_view_type = 'GOE_OFFLOAD_HYBRID_VIEW'
AND    o.offloaded_table  = o.hybrid_view
ORDER  BY
       o.offloaded_owner
,      o.offloaded_table""".format(
            goe_adm_schema=self._ora_adm_user
        )

        binds = {"offloaded_owner": self._schema, "offloaded_table": self._table}

        self._messages.debug("SQL: %s" % sql, detail=SUPPRESS_STDOUT)
        self._messages.debug("Binds: %s" % binds, detail=SUPPRESS_STDOUT)

        results = self._ora_service.execute(
            sql, binds=binds, cursor_fn=lambda c: c.fetchall(), as_dict=True
        )

        self._messages.debug(
            "Fetched %s offloaded %s" % (len(results), plural("table", len(results)))
        )
        self._messages.debug(
            "Offloaded table data: %s" % results, detail=SUPPRESS_STDOUT
        )

        return results

    def _decode_metadata_incremental_high_values(
        self,
        incremental_predicate_type,
        incremental_key,
        incremental_high_value,
        rdbms_base_table,
    ):
        _, hvs, _ = decode_metadata_incremental_high_values(
            incremental_predicate_type,
            incremental_key,
            incremental_high_value,
            rdbms_base_table,
        )
        return hvs

    def _get_rdbms_space_data(self, offloaded_table, offload_data, ost):
        """Return the RDBMS data for an offloaded table. Data includes space and partitions
        retained in the RDBMS along with some metadata about the table(s).

        Data format:
            {
              SOURCE_OWNER:      string
              SOURCE_TABLE:      string
              RETAINED_BYTES:    int
              RETAINED_PARTS:    int
              RECLAIMABLE_BYTES: int
              RECLAIMABLE_PARTS: int
            }
        """

        self._messages.detail(
            "Get RDBMS space data for offloaded table: %s"
            % self._format_table_name(
                offloaded_table[SOURCE_OWNER], offloaded_table[SOURCE_TABLE]
            )
        )

        # Initialise return data...
        rdbms_data = self._initialise_summary_dict()
        rdbms_data[SOURCE_OWNER] = offloaded_table[SOURCE_OWNER]
        rdbms_data[SOURCE_TABLE] = offloaded_table[SOURCE_TABLE]

        sql = """WITH table_data AS (
        SELECT t.owner
        ,      t.table_name
        ,      t.parent_table_name
        ,      NVL(t.num_rows, 0)   AS table_rows
        ,      tp.partition_name
        ,      tp.partition_position
        ,      tp.high_value        AS partition_high_value
        ,      NVL(tp.num_rows, 0)  AS partition_rows
        ,      tsp.subpartition_name
        ,      tsp.subpartition_position
        ,      tsp.high_value       AS subpartition_high_value
        ,      NVL(tsp.num_rows, 0) AS subpartition_rows
        FROM  (
               SELECT t.owner
               ,      t.table_name
               ,      t.table_name AS parent_table_name
               ,      t.num_rows
               FROM   dba_all_tables t
               WHERE  t.owner      = :source_owner
               AND    t.table_name = :source_table
               UNION ALL
               SELECT t.owner
               ,      t.table_name
               ,      t.iot_name AS parent_table_name
               ,      t.num_rows
               FROM   dba_all_tables t
               WHERE  t.owner    = :source_owner
               AND    t.iot_name = :source_table
               AND    'IOT'      = :source_iot_type
              )                           t
               LEFT OUTER JOIN
               dba_tab_partitions         tp
               ON (    tp.table_owner     = t.owner
                   AND tp.table_name      = t.table_name )
               LEFT OUTER JOIN
               dba_tab_subpartitions      tsp
               ON (    tsp.table_owner    = tp.table_owner
                   AND tsp.table_name     = tp.table_name
                   AND tsp.partition_name = tp.partition_name )
        )
,    index_data AS (
        SELECT i.owner
        ,      i.index_name
        ,      i.index_type
        ,      i.table_owner
        ,      i.table_name
        ,      ip.partition_name
        ,      CASE
                  WHEN pi.locality = 'LOCAL'
                  THEN ip.partition_position
               END AS partition_position
        ,      isp.subpartition_name
        ,      CASE
                  WHEN pi.locality = 'LOCAL'
                  THEN isp.subpartition_position
               END AS subpartition_position
        FROM   dba_indexes                i
               LEFT OUTER JOIN
               dba_part_indexes           pi
               ON (    pi.owner           = i.owner
                   AND pi.index_name      = i.index_name )
               LEFT OUTER JOIN
               dba_ind_partitions         ip
               ON (    ip.index_owner     = i.owner
                   AND ip.index_name      = i.index_name )
               LEFT OUTER JOIN
               dba_ind_subpartitions      isp
               ON (    isp.index_owner    = ip.index_owner
                   AND isp.index_name     = ip.index_name
                   AND isp.partition_name = ip.partition_name )
        WHERE i.table_owner = :source_owner
        AND   i.table_name  = :source_table
        )
,    lob_data AS (
        SELECT l.owner
        ,      l.segment_name AS lob_name
        ,      l.table_name
        ,      lp.partition_name
        ,      lp.partition_position
        ,      lp.lob_partition_name
        ,      lsp.lob_subpartition_name
        ,      lsp.subpartition_position
        FROM   dba_lobs                       l
               LEFT OUTER JOIN
               dba_lob_partitions             lp
               ON (    lp.table_owner         = l.owner
                   AND lp.table_name          = l.table_name
                   AND lp.lob_name            = l.segment_name )
               LEFT OUTER JOIN
               dba_lob_subpartitions          lsp
               ON (    lsp.table_owner        = lp.table_owner
                   AND lsp.table_name         = lp.table_name
                   AND lsp.lob_partition_name = lp.lob_partition_name )
        WHERE  l.owner      = :source_owner
        AND    l.table_name = :source_table
        )
,    segment_mapping AS (
        SELECT 'TABLES'                   AS segment_group
        ,      NVL(s.bytes,0)             AS segment_bytes
        ,      t.owner                    AS table_owner
        ,      t.parent_table_name        AS table_name
        ,      t.partition_position       AS table_partition_position
        ,      t.subpartition_position    AS table_subpartition_position
        ,      t.partition_high_value     AS table_partition_high_value
        ,      t.subpartition_high_value  AS table_subpartition_high_value
        ,      t.table_rows               AS table_rows
        ,      t.partition_rows           AS table_partition_rows
        ,      t.subpartition_rows        AS table_subpartition_rows
        FROM   table_data                 t
               LEFT OUTER JOIN
               dba_segments               s
               ON (    t.owner            = s.owner
                   AND t.table_name       = s.segment_name
                   AND COALESCE(t.subpartition_name, t.partition_name, t.table_name) = COALESCE(s.partition_name, s.segment_name)
                   AND s.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION') )
        --
        UNION ALL
        --
        SELECT CASE
                  WHEN i.index_type = 'IOT - TOP'
                  THEN 'TABLES'
                  ELSE 'INDEXES'
               END                        AS segment_group
        ,      NVL(s.bytes,0)             AS segment_bytes
        ,      i.table_owner              AS table_owner
        ,      i.table_name               AS table_name
        ,      i.partition_position       AS table_partition_position
        ,      i.subpartition_position    AS table_subpartition_position
        ,      NULL                       AS table_partition_high_value
        ,      NULL                       AS table_subpartition_high_value
        ,      NULL                       AS table_rows
        ,      NULL                       AS table_partition_rows
        ,      NULL                       AS table_subpartition_rows
        FROM   index_data                 i
               LEFT OUTER JOIN
               dba_segments               s
               ON (    i.owner            = s.owner
                   AND i.index_name       = s.segment_name
                   AND COALESCE(i.subpartition_name, i.partition_name, i.index_name) = COALESCE(s.partition_name, s.segment_name)
                   AND s.segment_type IN ('INDEX', 'LOBINDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION') )
        WHERE  i.index_type != 'CLUSTER'
        --
        UNION ALL
        --
        SELECT 'LOBS'                     AS segment_group
        ,      NVL(s.bytes,0)             AS segment_bytes
        ,      l.owner                    AS table_owner
        ,      l.table_name               AS table_name
        ,      l.partition_position       AS table_partition_position
        ,      l.subpartition_position    AS table_subpartition_position
        ,      NULL                       AS table_partition_high_value
        ,      NULL                       AS table_subpartition_high_value
        ,      NULL                       AS table_rows
        ,      NULL                       AS table_partition_rows
        ,      NULL                       AS table_subpartition_rows
        FROM   lob_data                   l
               LEFT OUTER JOIN
               dba_segments               s
               ON (    l.owner            = s.owner
                   AND l.lob_name         = s.segment_name
                   AND COALESCE(l.lob_subpartition_name, l.lob_partition_name, l.lob_name) = COALESCE(s.partition_name, s.segment_name)
                   AND s.segment_type IN ('LOBSEGMENT', 'LOB PARTITION', 'LOB SUBPARTITION') )
        )
,    segment_sizes AS (
        SELECT s.segment_group
        ,      MAX(NVL2(s.table_partition_position, s.table_partition_rows, s.table_rows)) OVER (
                  PARTITION BY s.table_owner, s.table_name, s.table_partition_position) AS table_partition_rows
        ,      MAX(s.table_subpartition_rows) OVER (
                  PARTITION BY s.table_owner, s.table_name, s.table_partition_position, s.table_subpartition_position) AS table_subpartition_rows
        ,      s.table_partition_position
        ,      s.table_subpartition_position
        ,      s.table_partition_high_value
        ,      s.table_subpartition_high_value
        ,      SUM(s.segment_bytes) OVER (
                  PARTITION BY s.table_owner, s.table_name, s.table_partition_position) AS table_partition_bytes
        ,      SUM(s.segment_bytes) OVER (
                  PARTITION BY s.table_owner, s.table_name, s.table_partition_position, s.table_subpartition_position) AS table_subpartition_bytes
        ,      COUNT(DISTINCT s.table_partition_position) OVER (
                  PARTITION BY s.table_owner, s.table_name) AS table_partition_segments
        ,      COUNT(s.table_subpartition_position) OVER (
                  PARTITION BY s.table_owner, s.table_name) AS table_subpartition_segments
        ,      SUM(NVL2(s.table_partition_position, 0, s.segment_bytes)) OVER (
                  PARTITION BY s.table_owner, s.table_name) AS table_global_index_bytes
        ,      ROW_NUMBER() OVER (
                  PARTITION BY s.segment_group, s.table_owner, s.table_name, s.table_partition_position
                  ORDER BY CASE
                              WHEN s.table_subpartition_high_value IS NOT NULL
                              THEN 1
                              WHEN s.table_partition_high_value IS NOT NULL
                              THEN 2
                              ELSE 3
                           END, s.table_subpartition_position) AS table_partition_rn
        FROM   segment_mapping s
        )
SELECT CASE :incremental_range
          WHEN 'SUBPARTITION'
          THEN s.table_subpartition_high_value
          ELSE s.table_partition_high_value
       END AS high_value
,      CASE :incremental_range
          WHEN 'SUBPARTITION'
          THEN s.table_subpartition_rows
          ELSE s.table_partition_rows
       END AS num_rows
,      CASE :incremental_range
          WHEN 'SUBPARTITION'
          THEN s.table_subpartition_bytes + CASE
                                               WHEN s.table_subpartition_position IS NOT NULL
                                               THEN (s.table_global_index_bytes/s.table_subpartition_segments)
                                               ELSE 0
                                            END
          ELSE s.table_partition_bytes + CASE
                                            WHEN s.table_partition_position IS NOT NULL
                                            THEN (s.table_global_index_bytes/s.table_partition_segments)
                                            ELSE 0
                                         END
       END AS bytes
FROM   segment_sizes s
WHERE  s.segment_group = 'TABLES'
AND    CASE :incremental_range
          WHEN 'SUBPARTITION'
          THEN 1
          ELSE s.table_partition_rn
       END = 1
ORDER  BY
       s.table_partition_position
,      s.table_subpartition_position"""

        binds = {
            "source_owner": offloaded_table[SOURCE_OWNER],
            "source_table": offloaded_table[SOURCE_TABLE],
            "source_iot_type": offloaded_table[SOURCE_IOT_TYPE],
            "incremental_range": offloaded_table[INCREMENTAL_RANGE],
        }

        self._messages.debug("SQL: %s" % sql, detail=SUPPRESS_STDOUT)
        self._messages.debug("Binds: %s" % binds, detail=SUPPRESS_STDOUT)

        rows = self._ora_service.execute(
            sql, binds=binds, cursor_fn=lambda c: c.fetchall(), as_dict=True
        )

        self._messages.debug("Fetched %s %s" % (len(rows), plural("row", len(rows))))

        if len(rows) > 0:
            # Determine reclaimable space or retained space.
            #
            # Reclaimable if:
            #
            #   1) it's a 90-10 (RANGE[_AND_PREDICATE], LIST_AS_RANGE[_AND_PREDICATE] only) and the partition
            #      has been offloaded
            #
            #   2) it's a 100-10 and the partition has been offloaded and the table is not enabled for
            #       certain Incremental Update methods that would mean the partition might possibly be
            #       modified
            #
            #   3) it's a 100-0 and the table is not enabled for certain Incremental Update methods that
            #       would mean the partition might possibly be modified
            #
            #   For item 1), 90-10 tables offloaded by PREDICATE cannot be reclaimed as we have no current
            #   means of calculating how much space could be reclaimed by deleting data that matches offloaded
            #   predicates. And even then space is only reclaimed for the table itself (unless re-organising it
            #   with a MOVE).
            #
            #   For item 2), just because a table is enabled with Incremental Update methods
            #   such as CHANGELOG does not necessarily mean that an offloaded partition will be updated.
            #   It's probably more likely that the data above the HWM would be updated. However, we can't
            #   assume this and therefore we have to assume that an offloaded partition for a table
            #   with a non-reclaimable Incremental Update method cannot be reclaimed.

            offload_metadata = self._repo_metadata_from_osr_metadata(offloaded_table)
            offload_type = offloaded_table[OFFLOAD_TYPE]
            incremental_predicate_type = offloaded_table[INCREMENTAL_PREDICATE_TYPE]
            if (
                offloaded_table[INCREMENTAL_KEY]
                and offloaded_table[INCREMENTAL_HIGH_VALUE]
            ):
                table_high_value = self._decode_metadata_incremental_high_values(
                    offload_metadata.incremental_predicate_type,
                    offload_metadata.incremental_key,
                    offload_metadata.incremental_high_value,
                    ost,
                )
                ipa_offload = True
            else:
                table_high_value = None
                ipa_offload = False

            for row in rows:
                if ipa_offload:
                    row_high_value = self._decode_metadata_incremental_high_values(
                        offload_metadata.incremental_predicate_type,
                        offload_metadata.incremental_key,
                        row["HIGH_VALUE"],
                        ost,
                    )
                    partition_offloaded = incremental_partition_match(
                        row_high_value, table_high_value, incremental_predicate_type
                    )
                else:
                    partition_offloaded = None

                if (
                    (offload_type == "INCREMENTAL" and partition_offloaded)
                    or (offload_type == "FULL" and ipa_offload and partition_offloaded)
                    or (offload_type == "FULL" and (not ipa_offload))
                ):
                    rdbms_data[RECLAIMABLE_PARTS] += 1
                    rdbms_data[RECLAIMABLE_BYTES] += row["BYTES"]
                    rdbms_data[RECLAIMABLE_ROWS] += row["NUM_ROWS"]
                else:
                    rdbms_data[RETAINED_PARTS] += 1
                    rdbms_data[RETAINED_BYTES] += row["BYTES"]
                    rdbms_data[RETAINED_ROWS] += row["NUM_ROWS"]

        self._messages.debug(
            "RDBMS space data: %s" % rdbms_data, detail=SUPPRESS_STDOUT
        )

        return rdbms_data

    def _get_rdbms_objects_data(self, offloaded_table, ost):
        """Return all of the RDBMS objects for an offloaded table. Data includes AAPD objects,
        Incremental Update objects, Join Pushdown objects, dependent views.

        Data format:
            {
              SOURCE_OWNER:                     string
              SOURCE_TABLE:                     string
              AAPD_OBJECTS:                     [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
              JOIN_OBJECTS_RDBMS:               [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
              INCREMENTAL_UPDATE_OBJECTS_RDBMS: [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
              DEPENDENT_OBJECTS:                [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
            }
        """

        # Initialise return data...
        rdbms_data = {}
        rdbms_data[SOURCE_OWNER] = offloaded_table[SOURCE_OWNER]
        rdbms_data[SOURCE_TABLE] = offloaded_table[SOURCE_TABLE]
        rdbms_data[AAPD_OBJECTS] = []
        rdbms_data[JOIN_OBJECTS_RDBMS] = []
        rdbms_data[INCREMENTAL_UPDATE_OBJECTS_RDBMS] = []
        rdbms_data[DEPENDENT_OBJECTS] = []

        if self._output_level == DETAIL_LEVEL:
            self._messages.detail(
                "Get RDBMS objects data for offloaded table: %s"
                % self._format_table_name(
                    offloaded_table[SOURCE_OWNER], offloaded_table[SOURCE_TABLE]
                )
            )

            self._messages.debug(
                "RDBMS objects data: %s" % rdbms_data, detail=SUPPRESS_STDOUT
            )

        else:
            self._messages.detail(
                "RDBMS objects data collection skipped in summary mode"
            )

        return rdbms_data

    def _repo_metadata_from_osr_metadata(self, offloaded_table):
        """Because this tool reads metadata en-masse from offload_objects we convert
        the OSR metadata back to true metadata here.
        """
        new_metadata_dict = copy(offloaded_table)
        new_metadata_dict["OFFLOADED_OWNER"] = offloaded_table[SOURCE_OWNER]
        new_metadata_dict["OFFLOADED_TABLE"] = offloaded_table[SOURCE_TABLE]
        new_metadata_dict["HADOOP_OWNER"] = offloaded_table[OFFLOAD_OWNER]
        new_metadata_dict["HADOOP_TABLE"] = offloaded_table[OFFLOAD_TABLE]
        return OrchestrationMetadata(new_metadata_dict, client=self._repo_client)

    def _get_offload_data(self, offloaded_table, rdbms_join_objects):
        """Return the offload data for an offloaded table. Data includes space and partitions
        offloaded to the backend. Also adds any materialized offload objects to the dataset.

        Data format:
            {
              SOURCE_OWNER:                       string
              SOURCE_TABLE:                       string
              OFFLOADED_BYTES:                    int
              OFFLOADED_PARTS:                    int
              OFFLOADED_ROWS:                     int
              OFFLOADED_TABLES:                   int
              OFFLOAD_PART_KEY:                   string
              OFFLOAD_TABLE_EXISTS:               boolean
              INCREMENTAL_UPDATE_METHOD:          string
              INCREMENTAL_UPDATE_OBJECTS_OFFLOAD: [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
              JOIN_OBJECTS_OFFLOAD:               [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
            }
        """
        self._messages.detail(
            "Get offload data for offloaded table: %s"
            % self._format_table_name(
                offloaded_table[SOURCE_OWNER], offloaded_table[SOURCE_TABLE]
            )
        )

        # Initialise return data...
        offload_data = {}
        offload_data[SOURCE_OWNER] = offloaded_table[SOURCE_OWNER]
        offload_data[SOURCE_TABLE] = offloaded_table[SOURCE_TABLE]
        offload_data[OFFLOADED_BYTES] = 0
        offload_data[OFFLOADED_PARTS] = 0
        offload_data[OFFLOADED_ROWS] = 0
        offload_data[OFFLOADED_TABLES] = 0
        offload_data[OFFLOAD_TABLE_EXISTS] = None
        offload_data[OFFLOAD_PART_KEY] = None
        offload_data[INCREMENTAL_UPDATE_METHOD] = None
        offload_data[INCREMENTAL_UPDATE_OBJECTS_OFFLOAD] = []
        offload_data[JOIN_OBJECTS_OFFLOAD] = []

        # Build a backend table instance to get access to Incremental Update information...
        # We re-use the existing backend_api to avoid unnecessary connections.
        db, table = offloaded_table[OFFLOAD_OWNER], offloaded_table[OFFLOAD_TABLE]
        table_metadata = self._repo_metadata_from_osr_metadata(offloaded_table)
        offload_table = backend_table_factory(
            db,
            table,
            self._backend_db_type,
            self._orchestration_config,
            self._messages,
            hybrid_metadata=table_metadata,
            dry_run=bool(not self._execute),
            existing_backend_api=self._backend_api,
        )
        self._messages.debug(
            "Fetched details for offload table/view", detail=SUPPRESS_STDOUT
        )

        if offload_table.exists():
            # This will replace the Oracle-side metadata for backend owner/table with the backend format...
            offload_data[OFFLOAD_OWNER] = offload_table.db_name
            offload_data[OFFLOAD_TABLE] = offload_table.table_name
            offload_data[OFFLOAD_TABLE_EXISTS] = True
            offload_data[OFFLOADED_TABLES] = 1
            self._messages.debug(
                "Offload table/view: %s"
                % self._format_table_name(
                    offload_data[OFFLOAD_OWNER], offload_data[OFFLOAD_TABLE]
                )
            )
            offload_parts = offload_table.get_table_partitions()

            table_size, table_row_count = offload_table.get_table_size_and_row_count()
            offload_data[OFFLOADED_BYTES] += table_size or 0
            offload_data[OFFLOADED_ROWS] += table_row_count or 0

            # Since adding BigQuery support, not all offloaded tables will be partitioned...
            if offload_parts:
                self._messages.debug(
                    "Offload table has %s partitions" % len(offload_parts)
                )
                self._messages.debug(
                    "Offload partitions: %s" % offload_parts, detail=SUPPRESS_STDOUT
                )
                offload_data[OFFLOADED_PARTS] += len(offload_parts)
                backend_part_col_names = [
                    _.name for _ in offload_table.get_partition_columns()
                ]
                offload_data[OFFLOAD_PART_KEY] = ", ".join(backend_part_col_names)
                self._messages.debug(
                    "Offload partition key: %s" % backend_part_col_names,
                    detail=SUPPRESS_STDOUT,
                )
            else:
                offload_data[OFFLOADED_PARTS] = 1
                self._messages.debug("Offload table is not partitioned")

            # Add in any materialized join objects...
            if self._output_level == DETAIL_LEVEL:
                for join_object in rdbms_join_objects:
                    if join_object[OBJECT_TYPE_GOE] == "JOIN_VIEW":
                        db, table = (
                            offload_data[OFFLOAD_OWNER],
                            join_object[OBJECT_NAME],
                        )
                        backend_api = offload_table.get_backend_api()
                        if backend_api.exists(db, table):
                            object_type = (
                                "VIEW" if backend_api.is_view(db, table) else "TABLE"
                            )
                            offload_data[JOIN_OBJECTS_OFFLOAD].append(
                                {
                                    OBJECT_OWNER: db,
                                    OBJECT_NAME: table,
                                    OBJECT_TYPE_DB: object_type,
                                    OBJECT_TYPE_GOE: "JOIN_%s" % object_type,
                                }
                            )
                        else:
                            self._messages.debug(
                                "Unable to find corresponding offload join object for %s"
                                % self._format_table_name(
                                    join_object[OBJECT_OWNER], join_object[OBJECT_NAME]
                                ),
                                detail=SUPPRESS_STDOUT,
                            )
            else:
                self._messages.debug(
                    "Skipped Join Objects data collection in summary mode"
                )

        else:
            offload_data[OFFLOAD_TABLE_EXISTS] = False
            offload_data[OFFLOADED_TABLES] = 0
            self._messages.debug(
                "Offload table/view %s does not exist"
                % self._format_table_name(
                    offloaded_table[OFFLOAD_OWNER], offloaded_table[OFFLOAD_TABLE]
                )
            )

        self._messages.debug("Offload data: %s" % offload_data, detail=SUPPRESS_STDOUT)

        return offload_data

    def _get_table_data(self, offloaded_tables):
        """Get the RDBMS and backend table data for a set of target tables, ready for processing.
        Returns a list of dicts.
        Data format:
                [  {
                        SOURCE_OWNER:                       string
                        SOURCE_TABLE:                       string
                        OFFLOADED_PARTS:                    int
                        OFFLOADED_TABLES:                   int
                        OFFLOADED_BYTES:                    int
                        OFFLOADED_ROWS:                     int
                        RETAINED_BYTES:                     int
                        RETAINED_PARTS:                     int
                        RETAINED_ROWS:                      int
                        RECLAIMABLE_BYTES:                  int
                        RECLAIMABLE_PARTS:                  int
                        RECLAIMABLE_ROWS:                   int
                        OFFLOAD_TYPE:                       string
                        OFFLOAD_OWNER:                      string
                        OFFLOAD_TABLE:                      string
                        OFFLOAD_PART_KEY:                   string
                        OFFLOAD_TABLE_EXISTS:               boolean
                        HYBRID_OWNER:                       string
                        HYBRID_VIEW:                        string
                        HYBRID_VIEW_TYPE:                   string
                        HYBRID_EXTERNAL_TABLE:              string
                        INCREMENTAL_KEY:                    string
                        INCREMENTAL_HIGH_VALUE:             string
                        INCREMENTAL_RANGE:                  string
                        INCREMENTAL_PREDICATE_TYPE:         string
                        INCREMENTAL_PREDICATE_VALUE:        string
                        INCREMENTAL_UPDATE_METHOD:          string
                        OFFLOAD_BUCKET_COLUMN:              string
                        OFFLOAD_BUCKET_METHOD:              string
                        OFFLOAD_BUCKET_COUNT:               string
                        OFFLOAD_VERSION:                    string
                        OFFLOAD_SORT_COLUMNS:               string
                        OFFLOAD_PARTITION_FUNCTIONS:        string
                        AAPD_OBJECTS:                       [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
                        JOIN_OBJECTS_RDBMS:                 [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
                        JOIN_OBJECTS_OFFLOAD:               [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
                        INCREMENTAL_UPDATE_OBJECTS_RDBMS:   [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
                        INCREMENTAL_UPDATE_OBJECTS_OFFLOAD: [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
                        DEPENDENT_OBJECTS:                  [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]
                    },
                    {
                        ...etc...
                    } ]
        """

        def get_offload_source_table(offloaded_table):
            conn_options = lambda: None
            conn_options.db_type = self._db_type
            conn_options.ora_adm_user = self._ora_adm_user
            conn_options.ora_adm_pass = self._ora_adm_pass
            conn_options.rdbms_dsn = self._oracle_dsn
            conn_options.use_oracle_wallet = self._use_oracle_wallet
            return OffloadSourceTable.create(
                offloaded_table[SOURCE_OWNER],
                offloaded_table[SOURCE_TABLE],
                conn_options,
                self._messages,
                conn=self._ora_conn,
            )

        table_data = []
        offloaded_count = len(offloaded_tables)
        self._messages.log(
            "Fetching report data for %s offloaded %s"
            % (offloaded_count, plural("table", offloaded_count)),
            detail=VERBOSE,
        )

        # Populate the RDBMS and backend dataset, table-by-table...
        for n, offloaded_table in enumerate(offloaded_tables, 1):
            qualified_table_name = self._format_table_name(
                offloaded_table[SOURCE_OWNER], offloaded_table[SOURCE_TABLE]
            )
            progress_message = "Fetching %s of %s" % (n, offloaded_count)
            self._set_action(qualified_table_name)
            self._set_client_info(progress_message)
            self._messages.log(
                "%s: %s" % (progress_message, qualified_table_name), detail=VVERBOSE
            )

            ost = get_offload_source_table(offloaded_table)

            rdbms_objects_data = self._get_rdbms_objects_data(offloaded_table, ost)
            self._messages.debug("Fetched RDBMS objects data")

            offload_data = self._get_offload_data(
                offloaded_table, rdbms_objects_data[JOIN_OBJECTS_RDBMS]
            )
            self._messages.debug("Fetched offload data")

            rdbms_space_data = self._get_rdbms_space_data(
                offloaded_table, offload_data, ost
            )
            self._messages.debug("Fetched RDBMS space data")

            # Prepare table data from offloaded table metadata, rdbms_data, offload_data and JSON-converted predicates...
            offloaded_table.update(rdbms_space_data)
            offloaded_table.update(rdbms_objects_data)
            offloaded_table.update(offload_data)
            offloaded_table.pop(SOURCE_IOT_TYPE, None)
            if offloaded_table[INCREMENTAL_PREDICATE_VALUE]:
                offloaded_table[INCREMENTAL_PREDICATE_VALUE] = json.loads(
                    offloaded_table[INCREMENTAL_PREDICATE_VALUE]
                )
            table_data.append(offloaded_table)

            self._messages.debug(
                "RDBMS and offload data merged for %s" % qualified_table_name
            )
            self._messages.debug(
                "Merged data:" % offloaded_table, detail=SUPPRESS_STDOUT
            )

        self._messages.debug("Table data:" % table_data, detail=SUPPRESS_STDOUT)
        return table_data

    def _get_demo_table_data(self):
        """Test data to demo the report and to assist with speeding up development
        (to avoid database queries and fetch times)...
        """
        table_data = []
        if (self._schema is None) or (self._schema.upper() == "SH"):
            if (self._table is None) or (self._table.upper() == "COSTS"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_PART_KEY": "offload_bucket_id, goe_part_m_time_id",
                        "HYBRID_VIEW": "COSTS",
                        "HYBRID_EXTERNAL_TABLE": "COSTS_EXT",
                        "SOURCE_OWNER": "SH",
                        "OFFLOAD_BUCKET_METHOD": None,
                        "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2015-07-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
                        "OFFLOAD_OWNER": "sh",
                        "SOURCE_TABLE": "COSTS",
                        "OFFLOAD_SORT_COLUMNS": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": "UDFS.COST2PKEY",
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "OFFLOAD_TABLE": "costs",
                        "OFFLOAD_TABLE_EXISTS": True,
                        "_RECLAIMABLE_PARTS": 229,
                        "_RETAINED_BYTES": 0,
                        "_RETAINED_ROWS": 0,
                        "_OFFLOADED_BYTES": 6113146.879999999,
                        "OFFLOAD_BUCKET_COLUMN": "UNIT_COST",
                        "OFFLOAD_BUCKET_COUNT": "16",
                        "_RECLAIMABLE_ROWS": 1787540,
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 141492224,
                        "_OFFLOADED_ROWS": 1787540,
                        "OFFLOAD_VERSION": None,
                        "OFFLOAD_TYPE": "INCREMENTAL",
                        "_RETAINED_PARTS": 0,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": "PARTITION",
                        "_OFFLOADED_PARTS": 864,
                        "INCREMENTAL_KEY": "TIME_ID",
                        "HYBRID_OWNER": "SH_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "COSTS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "COSTS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "COSTS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "COSTS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_OFFLOAD": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_TABLE",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "sales_costs",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "sales_costs_promos",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                        ],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "DEPENDENT_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "PROFITS",
                                "OBJECT_TYPE_DB": "VIEW",
                            }
                        ],
                    }
                )
                self._messages.debug("Added SH.COSTS to demo dataset")
            if (self._table is None) or (self._table.upper() == "CUSTOMERS"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_PART_KEY": "offload_bucket_id",
                        "_RECLAIMABLE_PARTS": 1,
                        "HYBRID_VIEW": "CUSTOMERS",
                        "HYBRID_EXTERNAL_TABLE": "CUSTOMERS_EXT",
                        "SOURCE_OWNER": "SH",
                        "_OFFLOADED_BYTES": 2296381.4400000004,
                        "OFFLOAD_BUCKET_METHOD": "GOE_BUCKET",
                        "OFFLOAD_OWNER": "sh",
                        "SOURCE_TABLE": "CUSTOMERS",
                        "_RETAINED_ROWS": 0,
                        "OFFLOAD_SORT_COLUMNS": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": None,
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "_RETAINED_BYTES": 0,
                        "INCREMENTAL_HIGH_VALUE": None,
                        "OFFLOAD_TABLE_EXISTS": True,
                        "OFFLOAD_TABLE": "customers",
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 12582912,
                        "_OFFLOADED_ROWS": 55500,
                        "OFFLOAD_VERSION": "2.9.0",
                        "OFFLOAD_TYPE": "FULL",
                        "_RETAINED_PARTS": 0,
                        "_RECLAIMABLE_ROWS": 55500,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": None,
                        "_OFFLOADED_PARTS": 2,
                        "INCREMENTAL_KEY": None,
                        "HYBRID_OWNER": "SH_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMERS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMERS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMERS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMERS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMERS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMERS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTS_BY_GENDER",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTS_BY_GENDER",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTS_BY_GENDER_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_BY_COUNTRY",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_BY_COUNTRY",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_BY_COUNTRY_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUSTOMER_COUNTRIES_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "CUST_COUNTRY_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_CNT__KYE2",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_OFFLOAD": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "cust_country_join",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_TABLE",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "salescountry_join",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_TABLE",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "sales_cust_prod_join",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "OFFLOAD_BUCKET_COLUMN": "CUST_ID",
                        "OFFLOAD_BUCKET_COUNT": "2",
                        "DEPENDENT_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "GOE_SALES_100_JOIN_DV",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "GOE_SALES_JOIN_DV",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                        ],
                    }
                )
                self._messages.debug("Added SH.CUSTOMERS to demo dataset")
            if (self._table is None) or (self._table.upper() == "SALES"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_BUCKET_COLUMN": "ID",
                        "OFFLOAD_BUCKET_COUNT": "1",
                        "OFFLOAD_PART_KEY": "offload_bucket_id, goe_part_m_time_id",
                        "_RECLAIMABLE_PARTS": 36,
                        "HYBRID_VIEW": "SALES",
                        "HYBRID_EXTERNAL_TABLE": "SALES_EXT",
                        "SOURCE_OWNER": "SH",
                        "_OFFLOADED_BYTES": 22473584.640000004,
                        "OFFLOAD_BUCKET_METHOD": "GOE_BUCKET",
                        "OFFLOAD_OWNER": "sh",
                        "SOURCE_TABLE": "SALES",
                        "_RETAINED_ROWS": 978554,
                        "OFFLOAD_SORT_COLUMNS": None,
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": None,
                        "_RETAINED_BYTES": 198049792,
                        "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2014-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
                        "OFFLOAD_TABLE_EXISTS": True,
                        "OFFLOAD_TABLE": "sales",
                        "_RECLAIMABLE_ROWS": 1944973,
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 232062976,
                        "_OFFLOADED_ROWS": 1888830,
                        "OFFLOAD_VERSION": "2.11.0",
                        "OFFLOAD_TYPE": "INCREMENTAL",
                        "_RETAINED_PARTS": 193,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": "PARTITION",
                        "_OFFLOADED_PARTS": 35,
                        "INCREMENTAL_KEY": "TIME_ID",
                        "HYBRID_OWNER": "SH_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_COSTS_PROMOS_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_CNT__KYE2",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_CUST_PROD_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_OFFLOAD": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_TABLE",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "salescountry_join",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_TABLE",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "sales_costs",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "sales_costs_promos",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_TABLE",
                                "OBJECT_OWNER": "sh",
                                "OBJECT_NAME": "sales_cust_prod_join",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "DEPENDENT_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "PROFITS",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_H",
                                "OBJECT_NAME": "SALES_TRICK_VIEW",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                        ],
                    }
                )
                self._messages.debug("Added SH.SALES to demo dataset")
        if (self._schema is None) or (self._schema.upper() == "SH_TEST"):
            if (self._table is None) or (self._table.upper() == "COSTS"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_PART_KEY": "offload_bucket_id, goe_part_m_time_id",
                        "_RECLAIMABLE_PARTS": 24,
                        "HYBRID_VIEW": "COSTS",
                        "HYBRID_EXTERNAL_TABLE": "COSTS_EXT",
                        "SOURCE_OWNER": "SH_TEST",
                        "_OFFLOADED_BYTES": 3616266.2400000007,
                        "OFFLOAD_BUCKET_METHOD": None,
                        "OFFLOAD_OWNER": "sh_test",
                        "SOURCE_TABLE": "COSTS",
                        "_RETAINED_ROWS": 1020075,
                        "OFFLOAD_SORT_COLUMNS": None,
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": None,
                        "_RETAINED_BYTES": 89063424,
                        "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2013-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
                        "OFFLOAD_TABLE_EXISTS": True,
                        "OFFLOAD_TABLE": "costs",
                        "OFFLOAD_BUCKET_COLUMN": "UNIT_COST",
                        "OFFLOAD_BUCKET_COUNT": "16",
                        "_RECLAIMABLE_ROWS": 767465,
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 52428800,
                        "_OFFLOADED_ROWS": 1704070,
                        "OFFLOAD_VERSION": "2.8.0",
                        "OFFLOAD_TYPE": "INCREMENTAL",
                        "_RETAINED_PARTS": 205,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": "PARTITION",
                        "_OFFLOADED_PARTS": 102,
                        "INCREMENTAL_KEY": "TIME_ID",
                        "HYBRID_OWNER": "SH_TEST_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_BY_TIME",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_BY_TIME",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_BY_TIME_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "COSTS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [],
                        "JOIN_OBJECTS_OFFLOAD": [],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "DEPENDENT_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PROFITS",
                                "OBJECT_TYPE_DB": "VIEW",
                            }
                        ],
                    }
                )
                self._messages.debug("Added SH_TEST.COSTS to demo dataset")
            if (self._table is None) or (self._table.upper() == "CUSTOMERS"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_PART_KEY": "offload_bucket_id",
                        "_RECLAIMABLE_PARTS": 1,
                        "HYBRID_VIEW": "CUSTOMERS",
                        "HYBRID_EXTERNAL_TABLE": "CUSTOMERS_EXT",
                        "SOURCE_OWNER": "SH_TEST",
                        "_OFFLOADED_BYTES": 2925885.4400000004,
                        "OFFLOAD_BUCKET_METHOD": "GOE_BUCKET",
                        "OFFLOAD_OWNER": "sh_test",
                        "SOURCE_TABLE": "CUSTOMERS",
                        "_RETAINED_ROWS": 0,
                        "OFFLOAD_SORT_COLUMNS": None,
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": None,
                        "_RETAINED_BYTES": 0,
                        "INCREMENTAL_HIGH_VALUE": None,
                        "OFFLOAD_TABLE_EXISTS": True,
                        "OFFLOAD_TABLE": "customers",
                        "OFFLOAD_BUCKET_COLUMN": "CUST_ID",
                        "OFFLOAD_BUCKET_COUNT": "16",
                        "_RECLAIMABLE_ROWS": 55500,
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 14024704,
                        "_OFFLOADED_ROWS": 55500,
                        "OFFLOAD_VERSION": None,
                        "OFFLOAD_TYPE": "FULL",
                        "_RETAINED_PARTS": 0,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": None,
                        "_OFFLOADED_PARTS": 16,
                        "INCREMENTAL_KEY": None,
                        "HYBRID_OWNER": "SH_TEST_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "CUSTOMERS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "CUSTOMERS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "CUSTOMERS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "CUSTOMERS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "CUSTOMERS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "CUSTOMERS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_OFFLOAD": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "sh_test",
                                "OBJECT_NAME": "salescountry_join",
                                "OBJECT_TYPE_DB": "VIEW",
                            }
                        ],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "DEPENDENT_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "GOE_CUSTOMERS_MIXED_V1",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "GOE_CUSTOMERS_MIXED_V2",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "GOE_SALES_100_JOIN_DV",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "GOE_SALES_JOIN_DV",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                        ],
                    }
                )
                self._messages.debug("Added SH_TEST.CUSTOMERS to demo dataset")
            if (self._table is None) or (self._table.upper() == "PRODUCTS"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_PART_KEY": "offload_bucket_id",
                        "_RECLAIMABLE_PARTS": 1,
                        "HYBRID_VIEW": "PRODUCTS",
                        "HYBRID_EXTERNAL_TABLE": "PRODUCTS_EXT",
                        "SOURCE_OWNER": "SH_TEST",
                        "_OFFLOADED_BYTES": 68177.92,
                        "OFFLOAD_BUCKET_METHOD": "GOE_BUCKET",
                        "OFFLOAD_OWNER": "sh_test",
                        "SOURCE_TABLE": "PRODUCTS",
                        "_RETAINED_ROWS": 0,
                        "OFFLOAD_SORT_COLUMNS": None,
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": None,
                        "_RETAINED_BYTES": 0,
                        "INCREMENTAL_HIGH_VALUE": None,
                        "OFFLOAD_TABLE_EXISTS": True,
                        "OFFLOAD_TABLE": "products",
                        "OFFLOAD_BUCKET_COLUMN": "PROD_ID",
                        "OFFLOAD_BUCKET_COUNT": "16",
                        "_RECLAIMABLE_ROWS": 72,
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 327680,
                        "_OFFLOADED_ROWS": 72,
                        "OFFLOAD_VERSION": "2.8.0",
                        "OFFLOAD_TYPE": "FULL",
                        "_RETAINED_PARTS": 0,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": None,
                        "_OFFLOADED_PARTS": 16,
                        "INCREMENTAL_KEY": None,
                        "HYBRID_OWNER": "SH_TEST_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PRODUCTS_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PRODUCTS_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PRODUCTS_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PRODUCTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PRODUCTS_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PRODUCTS_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [],
                        "JOIN_OBJECTS_OFFLOAD": [],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "DEPENDENT_OBJECTS": [],
                    }
                )
                self._messages.debug("Added SH_TEST.PRODUCTS to demo dataset")
            if (self._table is None) or (self._table.upper() == "SALES"):
                table_data.append(
                    {
                        "HYBRID_VIEW_TYPE": "GOE_OFFLOAD_HYBRID_VIEW",
                        "OFFLOAD_PART_KEY": "offload_bucket_id, goe_part_m_time_id",
                        "_RECLAIMABLE_PARTS": 24,
                        "HYBRID_VIEW": "SALES",
                        "HYBRID_EXTERNAL_TABLE": "SALES_EXT",
                        "SOURCE_OWNER": "SH_TEST",
                        "_OFFLOADED_BYTES": 18300405.760000005,
                        "OFFLOAD_BUCKET_METHOD": "GOE_BUCKET",
                        "OFFLOAD_OWNER": "sh_test",
                        "SOURCE_TABLE": "SALES",
                        "_RETAINED_ROWS": 1641231,
                        "OFFLOAD_SORT_COLUMNS": None,
                        "INCREMENTAL_PREDICATE_TYPE": None,
                        "INCREMENTAL_PREDICATE_VALUE": None,
                        "OFFLOAD_PARTITION_FUNCTIONS": None,
                        "_RETAINED_BYTES": 204537856,
                        "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2013-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
                        "OFFLOAD_TABLE_EXISTS": True,
                        "OFFLOAD_TABLE": "sales",
                        "OFFLOAD_BUCKET_COLUMN": "CUST_ID",
                        "OFFLOAD_BUCKET_COUNT": "16",
                        "_RECLAIMABLE_ROWS": 1282296,
                        "INCREMENTAL_UPDATE_METHOD": None,
                        "_RECLAIMABLE_BYTES": 104464384,
                        "_OFFLOADED_ROWS": 2784039,
                        "OFFLOAD_VERSION": "2.8.0",
                        "OFFLOAD_TYPE": "INCREMENTAL",
                        "_RETAINED_PARTS": 205,
                        "_OFFLOADED_TABLES": 1,
                        "INCREMENTAL_RANGE": "PARTITION",
                        "_OFFLOADED_PARTS": 102,
                        "INCREMENTAL_KEY": "TIME_ID",
                        "HYBRID_OWNER": "SH_TEST_H",
                        "AAPD_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_PROD",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_PROD",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_PROD_CUST_CHAN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_PROD_CUST_CHAN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_PROD_CUST_C_8OU8",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_BY_TIME_PROD_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALES_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_RDBMS": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_RULE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "MATERIALIZED VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG",
                                "OBJECT_TYPE_DB": "VIEW",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_AAPD_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_CNT_AGG_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                            {
                                "OBJECT_TYPE_GOE": "JOIN_EXTERNAL_TABLE",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "SALESCOUNTRY_JOIN_EXT",
                                "OBJECT_TYPE_DB": "TABLE",
                            },
                        ],
                        "JOIN_OBJECTS_OFFLOAD": [
                            {
                                "OBJECT_TYPE_GOE": "JOIN_VIEW",
                                "OBJECT_OWNER": "sh_test",
                                "OBJECT_NAME": "salescountry_join",
                                "OBJECT_TYPE_DB": "VIEW",
                            }
                        ],
                        "INCREMENTAL_UPDATE_OBJECTS_RDBMS": [],
                        "INCREMENTAL_UPDATE_OBJECTS_OFFLOAD": [],
                        "DEPENDENT_OBJECTS": [
                            {
                                "OBJECT_TYPE_GOE": "DEPENDENT_VIEW",
                                "OBJECT_OWNER": "SH_TEST_H",
                                "OBJECT_NAME": "PROFITS",
                                "OBJECT_TYPE_DB": "VIEW",
                            }
                        ],
                    }
                )
                self._messages.debug("Added SH_TEST.SALES to demo dataset")
        self._messages.debug("Returning demo dataset")
        return table_data

    def _process_report_data(self, table_data):
        """Formats a list of pre-processed table data into the final reporting data model.

        Data format:
        {
            OFFLOADED_TABLES:  int    -- report summary
            OFFLOADED_PARTS:   int
            OFFLOADED_BYTES:   int
            OFFLOADED_ROWS:    int
            RETAINED_BYTES:    int
            RETAINED_PARTS:    int
            RETAINED_ROWS:     int
            RECLAIMABLE_BYTES: int
            RECLAIMABLE_PARTS: int
            RECLAIMABLE_ROWS:  int
            'SCHEMA_1':         {
                                    OFFLOADED_TABLES:  int    -- schema summary
                                    OFFLOADED_PARTS:   int
                                    OFFLOADED_BYTES:   int
                                    OFFLOADED_ROWS:    int
                                    RETAINED_BYTES:    int
                                    RETAINED_PARTS:    int
                                    RETAINED_ROWS:     int
                                    RECLAIMABLE_BYTES: int
                                    RECLAIMABLE_PARTS: int
                                    RECLAIMABLE_ROWS:  int
                                    'TABLE_1':          {
                                                            OFFLOADED_TABLES:                   int      -- table summary
                                                            OFFLOADED_PARTS:                    int
                                                            OFFLOADED_BYTES:                    int
                                                            OFFLOADED_ROWS:                     int
                                                            RETAINED_BYTES:                     int
                                                            RETAINED_PARTS:                     int
                                                            RETAINED_ROWS:                      int
                                                            RECLAIMABLE_BYTES:                  int
                                                            RECLAIMABLE_PARTS:                  int
                                                            RECLAIMABLE_ROWS:                   int
                                                            SOURCE_OWNER:                       string   -- detail level only
                                                            SOURCE_TABLE:                       string   -- detail level only
                                                            OFFLOAD_TYPE:                       string   -- detail level only
                                                            OFFLOAD_OWNER:                      string   -- detail level only
                                                            OFFLOAD_TABLE:                      string   -- detail level only
                                                            OFFLOAD_PART_KEY:                   string   -- detail level only
                                                            OFFLOAD_TABLE_EXISTS:               boolean  -- detail level only
                                                            HYBRID_OWNER:                       string   -- detail level only
                                                            HYBRID_VIEW:                        string   -- detail level only
                                                            HYBRID_VIEW_TYPE:                   string   -- detail level only
                                                            HYBRID_EXTERNAL_TABLE:              string   -- detail level only
                                                            INCREMENTAL_KEY:                    string   -- detail level only
                                                            INCREMENTAL_HIGH_VALUE:             string   -- detail level only
                                                            INCREMENTAL_RANGE:                  string   -- detail level only
                                                            INCREMENTAL_PREDICATE_TYPE:         string   -- detail level only
                                                            INCREMENTAL_PREDICATE_VALUE:        string   -- detail level only
                                                            INCREMENTAL_UPDATE_METHOD:          string   -- detail level only
                                                            OFFLOAD_BUCKET_COLUMN:              string   -- detail level only
                                                            OFFLOAD_BUCKET_METHOD:              string   -- detail level only
                                                            OFFLOAD_BUCKET_COUNT:               string   -- detail level only
                                                            OFFLOAD_VERSION:                    string   -- detail level only
                                                            OFFLOAD_SORT_COLUMNS:               string   -- detail level only
                                                            OFFLOAD_PARTITION_FUNCTIONS:        string   -- detail level only
                                                            AAPD_OBJECTS:                       [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]  -- detail level only
                                                            JOIN_OBJECTS_RDBMS:                 [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]  -- detail level only
                                                            JOIN_OBJECTS_OFFLOAD:               [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]  -- detail level only
                                                            INCREMENTAL_UPDATE_OBJECTS_RDBMS:   [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]  -- detail level only
                                                            INCREMENTAL_UPDATE_OBJECTS_OFFLOAD: [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]  -- detail level only
                                                            DEPENDENT_OBJECTS:                  [ { OBJECT_OWNER: string, OBJECT_NAME: string, OBJECT_TYPE_DB: string, OBJECT_TYPE_GOE: string } ]  -- detail level only
                                                        }
                                    'TABLE_2':          {
                                                            ...as TABLE_1...
                                                        }
                                }
            'SCHEMA_2':         {
                                    ...as SCHEMA_2...
                                }
        }
        """

        def update_summary(summary_data, detail_data):
            summary_data[OFFLOADED_TABLES] += detail_data[OFFLOADED_TABLES]
            summary_data[OFFLOADED_BYTES] += detail_data[OFFLOADED_BYTES]
            summary_data[OFFLOADED_PARTS] += detail_data[OFFLOADED_PARTS]
            summary_data[OFFLOADED_ROWS] += detail_data[OFFLOADED_ROWS]
            summary_data[RETAINED_BYTES] += detail_data[RETAINED_BYTES]
            summary_data[RETAINED_PARTS] += detail_data[RETAINED_PARTS]
            summary_data[RETAINED_ROWS] += detail_data[RETAINED_ROWS]
            summary_data[RECLAIMABLE_BYTES] += detail_data[RECLAIMABLE_BYTES]
            summary_data[RECLAIMABLE_PARTS] += detail_data[RECLAIMABLE_PARTS]
            summary_data[RECLAIMABLE_ROWS] += detail_data[RECLAIMABLE_ROWS]
            return summary_data

        # Summarise the data and process it into final data model...
        report_data = self._initialise_summary_dict()
        table_count = len(table_data)
        self._messages.log(
            "Processing report data for %s offloaded %s"
            % (table_count, plural("table", table_count)),
            detail=VERBOSE,
        )

        for n, table_data in enumerate(table_data, 1):
            schema, table = table_data[SOURCE_OWNER], table_data[SOURCE_TABLE]
            qualified_table_name = self._format_table_name(schema, table)
            self._messages.log(
                "Processing %s of %s: %s" % (n, table_count, qualified_table_name),
                detail=VVERBOSE,
            )

            if schema not in report_data:
                report_data[schema] = self._initialise_summary_dict()
                self._messages.debug(
                    "Initializing new schema %s:" % schema, detail=SUPPRESS_STDOUT
                )

            if self._output_level == SUMMARY_LEVEL:
                table_data = self._strip_detail(
                    table_data
                )  # remove detail attributes from dict if in summary mode
            report_data[schema][
                table
            ] = table_data  # add table to schema dict (each table includes its own summary total)
            self._messages.debug(
                "Added %s to %s:" % (table, schema), detail=SUPPRESS_STDOUT
            )
            update_summary(
                report_data[schema], table_data
            )  # add summary total to schema dict (each schema has its own summary total)
            self._messages.debug(
                "Added %s summary to %s summary:" % (table, schema),
                detail=SUPPRESS_STDOUT,
            )
            update_summary(
                report_data, table_data
            )  # add summary total to report dict (the entire report, i.e. all schemas/tables, has its own summary total)
            self._messages.debug(
                "Added %s summary to report summary:" % table, detail=SUPPRESS_STDOUT
            )

            self._messages.debug(
                "Final processed report data: %s" % report_data, detail=SUPPRESS_STDOUT
            )

        return report_data

    def _gen_report_file_name(self, extension=None):
        if self._report_name == DEFAULT_REPORT_NAME:
            self._report_file = os.path.join(
                self._report_directory,
                "{repname}_{dbname}_{ts}.{ext}".format(
                    repname=self._report_name,
                    dbname=self._ora_db_name,
                    ts=self._start_ts,
                    ext=extension or self._output_format,
                ),
            )
        else:
            _, ext = os.path.splitext(self._report_name)
            self._report_file = os.path.join(
                self._report_directory,
                "{repname}{ext}".format(
                    repname=self._report_name,
                    ext="" if ext else ".%s" % extension or self._output_format,
                ),
            )
        self._messages.debug(
            "Report file path: %s" % self._report_file, detail=SUPPRESS_STDOUT
        )
        return self._report_file

    def _strip_summary(self, data):
        """Strips the summary keys off a dict to leave the remaining detail dict iterable."""
        return {k: v for k, v in list(data.items()) if k not in SUMMARY_KEYS}

    def _strip_detail(self, data):
        """Strips the detail keys off a dict to leave the remaining summary dict iterable."""
        return {k: v for k, v in list(data.items()) if k in SUMMARY_KEYS}

    def _gen_csv_report(self, data):
        """Generates a report in CSV format. Its content can be controlled by detail level (summary for
        schema-level or detail for table-level). The delimiter and enclosure character can also be
        controlled by options.
        """
        report_data = []
        schema_data = self._strip_summary(data)

        # Delimiter and enclosure functions...
        def nvl(x, r=""):
            return x or r

        def quote(x):
            if x:
                value = (
                    backtick_sandwich(x, self._csv_enclosure)
                    if isinstance(x, str)
                    else x
                )
            else:
                value = ""
            return value

        def delim(x, override_delimiter=None):
            d = (
                override_delimiter
                if override_delimiter is not None
                else self._csv_delimiter
            )
            return "%s" % d.join(str(_) if not isinstance(_, str) else _ for _ in x)

        if self.is_summary:
            self._messages.debug("Preparing summary level CSV report")

            # Prepare the header...
            line = (
                [quote("SCHEMA")]
                + [quote("TABLE")]
                + [quote(_.strip("_")) for _ in SUMMARY_KEYS]
            )
            report_data.append(delim(line))
            self._messages.debug("Report header generated", detail=SUPPRESS_STDOUT)

            # Format the data...
            for schema in sorted(schema_data):
                table_data = self._strip_summary(schema_data[schema])
                for table in sorted(table_data):
                    line = [quote(schema), quote(table)]
                    for k in SUMMARY_KEYS:
                        line.append(quote(table_data[table][k]))
                    report_data.append(delim(line))
            self._messages.debug("Report data added", detail=SUPPRESS_STDOUT)

        else:
            self._messages.debug("Preparing detail level CSV report")

            header = []
            header_written = False
            for schema in sorted(schema_data):
                table_data = self._strip_summary(schema_data[schema])
                for table in sorted(table_data):
                    line = []
                    for k, v in sorted(table_data[table].items()):
                        if not header_written:
                            header.append(quote(k.strip("_")))

                        if k in MULTIVALUE_KEYS:
                            multifield = []
                            for mv in v:
                                multifield.append(
                                    self._format_table_name(
                                        mv[OBJECT_OWNER],
                                        mv[OBJECT_NAME],
                                        k != INCREMENTAL_UPDATE_OBJECTS_OFFLOAD,
                                    )
                                )
                            line.append(quote(delim(multifield, DEFAULT_CSV_DELIMITER)))
                            self._messages.debug(
                                "Data for multivalue key %s added" % k,
                                detail=SUPPRESS_STDOUT,
                            )
                        else:
                            line.append(
                                quote(v)
                                if k != INCREMENTAL_PREDICATE_VALUE
                                else quote(format_json_list(v, DEFAULT_CSV_DELIMITER))
                            )

                    if not header_written:
                        report_data.append(delim(header))
                        header_written = True
                        self._messages.debug(
                            "Report header generated", detail=SUPPRESS_STDOUT
                        )
                    report_data.append(delim(line))

        self._messages.debug("Saving report to CSV file")
        status = self._save_report(report_data, CSV)
        return status

    def _gen_text_report(self, data):
        """Generates a report in text format. This is a less-rich format than HTML. Its content
        can be controlled by detail level (summary for schema-level or detail for table-level).
        """
        report_data = []

        def line(length, char="-"):
            return "%s" % (char * length)

        def underline(string, indent=0):
            return "%s\n%s%s\n" % (string, lpad("", indent), line(len(string)))

        def heading(string):
            return "\n\n%s\n%s\n" % (string, line(len(string), "="))

        def gb(n):
            return round(float(n) / (1024**3), 2)

        def rows(n):
            if n is None:
                value = "-"
            else:
                value = n
            return value

        def lpad(string, spaces=4):
            return "%s%s" % ((" " * spaces), string)

        self._messages.debug("Preparing %s level text report" % self._output_level)

        report_data.append(
            "\nOffload Status Report for %s database at %s.\n"
            % (self._ora_db_name, self._start_time)
        )

        report_data.append(heading("1. Options"))
        self._messages.debug("Adding Section 1 to text report", detail=SUPPRESS_STDOUT)
        width = max(40, len(self._schema or ""), len(self._table or ""))
        report_data.append(
            "%-30s %-*s %s" % ("Option Name", width, "Option Value", "Default")
        )
        report_data.append("%s %s %s" % (line(30), line(width), line(10)))
        report_data.append(
            "%-30s %-*s %s"
            % ("--schema", width, self._schema or "-", self._schema is None)
        )
        report_data.append(
            "%-30s %-*s %s"
            % ("--table", width, self._table or "-", self._table is None)
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--output-format",
                width,
                self._output_format,
                self._output_format == DEFAULT_OUTPUT_FORMAT,
            )
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--output-level",
                width,
                self._output_level,
                self._output_level == DEFAULT_OUTPUT_LEVEL,
            )
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--report-name",
                width,
                self._report_name,
                self._report_name == DEFAULT_REPORT_NAME,
            )
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--report-directory",
                width,
                self._report_directory,
                self._report_directory == DEFAULT_REPORT_DIRECTORY,
            )
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--csv-delimiter",
                width,
                self._csv_delimiter,
                self._csv_delimiter == DEFAULT_CSV_DELIMITER,
            )
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--csv-enclosure",
                width,
                self._csv_enclosure,
                self._csv_enclosure == DEFAULT_CSV_ENCLOSURE,
            )
        )
        report_data.append(
            "%-30s %-*s %s"
            % (
                "--demo-mode",
                width,
                self._demo_mode,
                self._demo_mode == DEFAULT_DEMO_MODE,
            )
        )
        self._messages.debug("Section 1 added to text report", detail=SUPPRESS_STDOUT)

        report_data.append(heading("2. Database Environment"))
        self._messages.debug("Adding Section 2 to text report", detail=SUPPRESS_STDOUT)
        report_data.append("%-30s %s" % ("Name", "Value"))
        report_data.append("%s %s" % (line(30), line(40)))
        report_data.append("%-30s %s" % ("RDBMS name", self.metadata["db_name"]))
        report_data.append(
            "%-30s %s" % ("RDBMS login user", self.metadata["db_login_user"])
        )
        report_data.append(
            "%-30s %s" % ("RDBMS connection", self.metadata["db_connection"])
        )
        report_data.append(
            "%-30s %s" % ("Offload database type", self.metadata["backend_db_type"])
        )
        report_data.append(
            "%-30s %s"
            % (
                "Offload %s" % self.metadata["backend_host_label"].lower(),
                self.metadata["backend_host_value"],
            )
        )
        self._messages.debug("Section 2 added to text report", detail=SUPPRESS_STDOUT)

        report_data.append(heading("3. Schema Summary"))
        self._messages.debug("Adding Section 3 to text report", detail=SUPPRESS_STDOUT)

        # Prepare the schema-level data up-front to determine the appropriate reporting width...
        s3_raw_data = []
        schema_width = 30
        schema_data = self._strip_summary(data)
        for k, v in sorted(schema_data.items()):
            schema_width = max(schema_width, len(k))
            s3_raw_data.append(
                (
                    k,
                    v[OFFLOADED_TABLES],
                    v[OFFLOADED_PARTS],
                    gb(v[OFFLOADED_BYTES]),
                    rows(v[OFFLOADED_ROWS]),
                    v[RETAINED_PARTS],
                    gb(v[RETAINED_BYTES]),
                    rows(v[RETAINED_ROWS]),
                    v[RECLAIMABLE_PARTS],
                    gb(v[RECLAIMABLE_BYTES]),
                    rows(v[RECLAIMABLE_ROWS]),
                )
            )

        # Add section 3 data to the report...
        report_data.append(
            "%-*s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s"
            % (
                schema_width,
                "",
                "Offloaded",
                "Offloaded",
                "Offloaded",
                "Offloaded",
                "Retained",
                "Retained",
                "Retained",
                "Reclaimable",
                "Reclaimable",
                "Reclaimable",
            )
        )
        report_data.append(
            "%-*s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s"
            % (
                schema_width,
                "Schema Name",
                "Tables",
                "Segments",
                "Size (GB)",
                "Rows (Stats)",
                "Segments",
                "Size (GB)",
                "Rows (Stats)",
                "Segments",
                "Size (GB)",
                "Rows (Stats)",
            )
        )
        report_data.append(
            "%s %s %s %s %s %s %s %s %s %s %s"
            % (
                line(schema_width),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
            )
        )
        for rv in s3_raw_data:
            report_data.append(
                "%-*s %15s %15s %15.2f %15s %15s %15.2f %15s %15s %15.2f %15s"
                % (schema_width, *rv)
            )
        report_data.append(
            "%s %s %s %s %s %s %s %s %s %s %s"
            % (
                line(schema_width),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
            )
        )
        report_data.append(
            "%-*s %15s %15s %15.2f %15s %15s %15.2f %15s %15s %15.2f %15s"
            % (
                schema_width,
                "Totals",
                data[OFFLOADED_TABLES],
                data[OFFLOADED_PARTS],
                gb(data[OFFLOADED_BYTES]),
                rows(data[OFFLOADED_ROWS]),
                data[RETAINED_PARTS],
                gb(data[RETAINED_BYTES]),
                rows(data[RETAINED_ROWS]),
                data[RECLAIMABLE_PARTS],
                gb(data[RECLAIMABLE_BYTES]),
                rows(data[RECLAIMABLE_ROWS]),
            )
        )
        self._messages.debug("Section 3 added to text report", detail=SUPPRESS_STDOUT)

        report_data.append(heading("4. Table Summary"))
        self._messages.debug("Adding Section 4 to text report", detail=SUPPRESS_STDOUT)

        # Prepare the table-level data up-front to determine the appropriate reporting width...
        s4_raw_data = []
        table_width = 30
        for k, v in sorted(schema_data.items()):
            table_data = self._strip_summary(schema_data[k])
            for tk, tv in sorted(table_data.items()):
                table_width = max(table_width, len(tk))
                s4_raw_data.append(
                    (
                        k,
                        tk,
                        tv[OFFLOADED_TABLES],
                        tv[OFFLOADED_PARTS],
                        gb(tv[OFFLOADED_BYTES]),
                        rows(tv[OFFLOADED_ROWS]),
                        tv[RETAINED_PARTS],
                        gb(tv[RETAINED_BYTES]),
                        rows(tv[RETAINED_ROWS]),
                        tv[RECLAIMABLE_PARTS],
                        gb(tv[RECLAIMABLE_BYTES]),
                        rows(tv[RECLAIMABLE_ROWS]),
                    )
                )

        # Add section 4 data to the report...
        report_data.append(
            "%-*s %-*s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s"
            % (
                schema_width,
                "",
                table_width,
                "",
                "Offloaded",
                "Offloaded",
                "Offloaded",
                "Offloaded",
                "Retained",
                "Retained",
                "Retained",
                "Reclaimable",
                "Reclaimable",
                "Reclaimable",
            )
        )
        report_data.append(
            "%-*s %-*s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s"
            % (
                schema_width,
                "Schema Name",
                table_width,
                "Table Name",
                "Tables",
                "Segments",
                "Size (GB)",
                "Rows (Stats)",
                "Segments",
                "Size (GB)",
                "Rows (Stats)",
                "Segments",
                "Size (GB)",
                "Rows (Stats)",
            )
        )
        report_data.append(
            "%s %s %s %s %s %s %s %s %s %s %s %s"
            % (
                line(schema_width),
                line(table_width),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
            )
        )
        for rv in s4_raw_data:
            schema_slice = rv[0]
            table_slice = rv[1:]
            report_data.append(
                "%-*s %-*s %15s %15s %15.2f %15s %15s %15.2f %15s %15s %15.2f %15s"
                % (schema_width, schema_slice, table_width, *table_slice)
            )
        report_data.append(
            "%s %s %s %s %s %s %s %s %s %s %s %s"
            % (
                line(schema_width),
                line(table_width),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
                line(15),
            )
        )
        report_data.append(
            "%-*s %-*s %15s %15s %15.2f %15s %15s %15.2f %15s %15s %15.2f %15s"
            % (
                schema_width,
                "Totals",
                table_width,
                "",
                data[OFFLOADED_TABLES],
                data[OFFLOADED_PARTS],
                gb(data[OFFLOADED_BYTES]),
                rows(data[OFFLOADED_ROWS]),
                data[RETAINED_PARTS],
                gb(data[RETAINED_BYTES]),
                rows(data[RETAINED_ROWS]),
                data[RECLAIMABLE_PARTS],
                gb(data[RECLAIMABLE_BYTES]),
                rows(data[RECLAIMABLE_ROWS]),
            )
        )
        self._messages.debug("Section 4 added to text report", detail=SUPPRESS_STDOUT)

        if self.is_detail:
            report_data.append(heading("5. Table Detail"))
            self._messages.debug(
                "Adding Section 5 to text report", detail=SUPPRESS_STDOUT
            )

            if not schema_data:
                report_data.append("No offloaded tables to report on")

            for k, v in sorted(schema_data.items()):
                table_data = self._strip_summary(schema_data[k])
                for tk, tv in sorted(table_data.items()):
                    report_data.append(underline(self._format_table_name(k, tk)))

                    self._messages.debug(
                        "Adding Offload Summary Statistics to Section 5",
                        detail=SUPPRESS_STDOUT,
                    )
                    indent = 4
                    report_data.append(
                        lpad(underline("Offload Summary Statistics", indent))
                    )
                    indent = 8
                    report_data.append(
                        lpad(
                            "%-30s %20s %20s %20s"
                            % ("Type", "Size (GB)", "Segments", "Rows (From Stats)"),
                            indent,
                        )
                    )
                    report_data.append(
                        lpad(
                            "%s %s %s %s" % (line(30), line(20), line(20), line(20)),
                            indent,
                        )
                    )
                    report_data.append(
                        lpad(
                            "%-30s %20.2f %20s %20s"
                            % (
                                "Offloaded",
                                gb(tv[OFFLOADED_BYTES]),
                                tv[OFFLOADED_PARTS],
                                tv[OFFLOADED_ROWS],
                            ),
                            indent,
                        )
                    )
                    report_data.append(
                        lpad(
                            "%-30s %20.2f %20s %20s"
                            % (
                                "Retained (RDBMS)",
                                gb(tv[RETAINED_BYTES]),
                                tv[RETAINED_PARTS],
                                tv[RETAINED_ROWS],
                            ),
                            indent,
                        )
                    )
                    report_data.append(
                        lpad(
                            "%-30s %20.2f %20s %20s"
                            % (
                                "Reclaimable (RDBMS)",
                                gb(tv[RECLAIMABLE_BYTES]),
                                tv[RECLAIMABLE_PARTS],
                                tv[RECLAIMABLE_ROWS],
                            ),
                            indent,
                        )
                    )

                    def append_table_detail(name, value, indent):
                        if value:
                            report_data.append(lpad("%-30s %s" % (name, value), indent))

                    self._messages.debug(
                        "Adding Offloaded Table Details to Section 5",
                        detail=SUPPRESS_STDOUT,
                    )
                    indent = 4
                    report_data.append("")
                    report_data.append(
                        lpad(underline("Offloaded Table Details", indent), indent)
                    )
                    indent = 8
                    append_table_detail("Name", "Value", indent)
                    append_table_detail(line(30), line(85), indent)
                    append_table_detail(
                        "Source table",
                        self._format_table_name(tv[SOURCE_OWNER], tv[SOURCE_TABLE]),
                        indent,
                    )
                    append_table_detail(
                        "Hybrid view",
                        self._format_table_name(tv[HYBRID_OWNER], tv[HYBRID_VIEW]),
                        indent,
                    )
                    append_table_detail(
                        "Hybrid external table",
                        self._format_table_name(
                            tv[HYBRID_OWNER], tv[HYBRID_EXTERNAL_TABLE]
                        ),
                        indent,
                    )
                    if tv[OFFLOAD_TABLE_EXISTS]:
                        append_table_detail(
                            "Offloaded table",
                            self._format_table_name(
                                tv[OFFLOAD_OWNER], tv[OFFLOAD_TABLE], False
                            ),
                            indent,
                        )
                    else:
                        append_table_detail(
                            "Offloaded table",
                            "Offloaded table not found (%s)"
                            % self._format_table_name(
                                tv[OFFLOAD_OWNER], tv[OFFLOAD_TABLE]
                            ),
                            indent,
                        )

                    self._messages.debug(
                        "Adding Offload Parameters to Section 5", detail=SUPPRESS_STDOUT
                    )
                    indent = 4
                    report_data.append("")
                    report_data.append(
                        lpad(underline("Offload Parameters", indent), indent)
                    )
                    indent = 8
                    append_table_detail("Name", "Value", indent)
                    append_table_detail(line(30), line(85), indent)
                    append_table_detail("Offload type", tv[OFFLOAD_TYPE], indent)
                    append_table_detail(
                        "Offload predicate type", tv[INCREMENTAL_PREDICATE_TYPE], indent
                    )
                    append_table_detail(
                        "Offload high water mark", tv[INCREMENTAL_HIGH_VALUE], indent
                    )
                    append_table_detail(
                        "Offload predicates",
                        format_json_list(
                            tv[INCREMENTAL_PREDICATE_VALUE], indent=30 + indent + 1
                        ),
                        indent,
                    )
                    append_table_detail(
                        "Partition columns (RDBMS)", tv[INCREMENTAL_KEY], indent
                    )
                    append_table_detail(
                        "Partition columns (offloaded)", tv[OFFLOAD_PART_KEY], indent
                    )
                    append_table_detail(
                        "Partition functions", tv[OFFLOAD_PARTITION_FUNCTIONS], indent
                    )
                    append_table_detail(
                        "Partition offload level", tv[INCREMENTAL_RANGE], indent
                    )
                    if tv[INCREMENTAL_UPDATE_METHOD]:
                        append_table_detail("Incremental Update enabled", "Yes", indent)
                    else:
                        append_table_detail(
                            "Incremental Update enabled",
                            "No" if tv[OFFLOAD_TABLE_EXISTS] else "Unknown",
                            indent,
                        )
                    append_table_detail(
                        "Incremental Update method",
                        tv[INCREMENTAL_UPDATE_METHOD],
                        indent,
                    )
                    append_table_detail("Offload version", tv[OFFLOAD_VERSION], indent)
                    append_table_detail(
                        "Offload bucket count", tv[OFFLOAD_BUCKET_COUNT], indent
                    )
                    append_table_detail(
                        "Offload bucket column", tv[OFFLOAD_BUCKET_COLUMN], indent
                    )
                    append_table_detail(
                        "Offload bucket method", tv[OFFLOAD_BUCKET_METHOD], indent
                    )
                    append_table_detail(
                        "Offload sort columns", tv[OFFLOAD_SORT_COLUMNS], indent
                    )

                    def multivalue_subsection(data, data_type, title):
                        if data:
                            self._messages.debug(
                                "Adding %s to Section 5" % title, detail=SUPPRESS_STDOUT
                            )
                            indent = 4
                            report_data.append("")
                            report_data.append(lpad(underline(title, indent)))
                            indent = 8
                            report_data.append(
                                lpad(
                                    "%-30s %-24s %s"
                                    % (
                                        "%s Object Type" % data_type,
                                        "Object Type",
                                        "Object Name",
                                    ),
                                    indent,
                                )
                            )
                            report_data.append(
                                lpad(
                                    "%s %s %s" % (line(30), line(24), line(60)), indent
                                )
                            )
                            for o in sorted(data, key=lambda k: k[OBJECT_NAME]):
                                report_data.append(
                                    lpad(
                                        "%-30s %-24s %s"
                                        % (
                                            o[OBJECT_TYPE_GOE]
                                            .replace("_", " ")
                                            .capitalize(),
                                            o[OBJECT_TYPE_DB],
                                            self._format_table_name(
                                                o[OBJECT_OWNER], o[OBJECT_NAME]
                                            ),
                                        ),
                                        indent,
                                    )
                                )

                    multivalue_subsection(
                        tv[AAPD_OBJECTS],
                        "AAPD",
                        "Advanced Aggregation Pushdown (AAPD) Objects",
                    )
                    multivalue_subsection(
                        tv[INCREMENTAL_UPDATE_OBJECTS_RDBMS],
                        "Incremental Update",
                        "Incremental Update Objects (RDBMS)",
                    )
                    multivalue_subsection(
                        tv[INCREMENTAL_UPDATE_OBJECTS_OFFLOAD],
                        "Incremental Update",
                        "Incremental Update Objects (Offloaded)",
                    )
                    multivalue_subsection(
                        tv[JOIN_OBJECTS_RDBMS],
                        "Join Pushdown",
                        "Advanced Join Pushdown Objects (RDBMS)",
                    )
                    multivalue_subsection(
                        tv[JOIN_OBJECTS_OFFLOAD],
                        "Join Pushdown",
                        "Advanced Join Pushdown Objects (Offloaded)",
                    )
                    multivalue_subsection(
                        tv[DEPENDENT_OBJECTS], "Dependent", "Dependent Objects (RDBMS)"
                    )
                    report_data.append("")
            self._messages.debug(
                "Section 5 added to text report", detail=SUPPRESS_STDOUT
            )
        else:
            report_data.append("")

        self._messages.debug("Saving report to TXT file")
        status = self._save_report(report_data, "txt", footer=True)
        return status

    def _gen_html_report(self, data):
        """Generates a fully-rich report in HTML format."""
        this_dir = os.path.dirname(os.path.abspath(__file__))
        j2_env = Environment(
            loader=FileSystemLoader(this_dir + "/../templates/"), trim_blocks=True
        )
        report_data = [
            j2_env.get_template(
                "offload_status_report/goe_offload_status_report.html"
            ).render(data=data, metadata=self.metadata)
        ]
        status = self._save_report(report_data, HTML)
        return status

    def _gen_json_report(self, data):
        """Generates a report in JSON format (simple conversion of Python dict to JSON)."""
        status = self._save_report(json.dumps(data), JSON)
        return status

    def _gen_raw_report(self, data):
        """Generates a report data in its raw format (Python dict)."""
        status = self._save_report(data, "dat")
        return status

    def _save_report(self, report_data, report_format=None, footer=False):
        goe_report = "\nOffload Status Report v%s%s" % (
            version(),
            " (Demo Mode)" if self._demo_mode else "",
        )
        goe_copyright = (
            "\nCopyright 2015-%s GOE Inc. All rights reserved."
            % datetime.now().strftime("%Y")
        )
        try:
            self._messages.debug("Saving data to file", detail=SUPPRESS_STDOUT)
            self._gen_report_file_name(extension=report_format)
            if not isinstance(report_data, list):
                report_data = [report_data]
            with open(self._report_file, "w") as rf:
                if not report_format == HTML:
                    rf.write("\n%s" % divider)
                    rf.write(goe_report)
                    rf.write(goe_copyright)
                    rf.write("\n%s\n\n" % divider)
                [rf.write("%s\n" % line) for line in report_data]
                if not report_format == HTML and footer:
                    rf.write(goe_report)
                    rf.write("%s\n\n" % goe_copyright)
            self._messages.log("Report saved to %s" % self._report_file)
        except IOError as exc:
            raise OffloadStatusReportException(
                "Unable to write report %s\n%s"
                % (self._report_file, traceback.format_exc())
            )
        return SUCCESS

    def _pretty_print_report_data(self, report_data, as_json=True):
        """To help with debugging and also gives an idea of how to traverse the outer dicts..."""
        if as_json:
            print(json.dumps(report_data, indent=1, sort_keys=True))
        else:
            schema_data = self._strip_summary(report_data)
            for schema in sorted(schema_data):
                print("\n%s" % ("=" * 100))
                print("Schema %s" % schema)
                print("=" * 100)
                table_data = self._strip_summary(schema_data[schema])
                for table in sorted(table_data):
                    print("\nTable %s.%s" % (schema, table))
                    print("%s" % ("-" * 50))
                    for k, v in sorted(table_data[table].items()):
                        if k not in MULTIVALUE_KEYS:
                            print("   %-50s: %s" % (k, v))
                        else:
                            if len(v) > 0:
                                for n, mv in enumerate(v, 1):
                                    print(
                                        "   %-50s: %s.%s (%s) (%s)"
                                        % (
                                            "%s (%s)" % (k, n),
                                            mv[OBJECT_OWNER],
                                            mv[OBJECT_NAME],
                                            mv[OBJECT_TYPE_DB],
                                            mv[OBJECT_TYPE_GOE],
                                        )
                                    )
                                print("%s%s" % ("   ", "-" * 30))
                            else:
                                print("   %-50s: %s" % (k, v))
                print("\nSummary for %s" % schema)
                print("%s" % ("-" * 50))
                for k in SUMMARY_KEYS:
                    print("   %-50s: %s" % (k.strip("_"), report_data[schema][k]))
            print("\n%s" % ("=" * 100))
            print("Report summary")
            print("=" * 100)
            for k in SUMMARY_KEYS:
                print("   %-50s: %s" % (k.strip("_"), report_data[k]))
        return

    ###############################################################################
    # PROPERTIES
    ###############################################################################
    @property
    def metadata(self):
        """Return metadata about the report."""
        return self._initialise_metadata_dict()

    @property
    def report_file(self):
        """Return name of the report being saved if applicable."""
        return self._report_file if hasattr(self, "_report_file") else None

    @property
    def as_json(self):
        """Return whether the data is to be returned as JSON."""
        return self._output_format == JSON

    @property
    def as_raw(self):
        """Return whether the data is to be returned as Python raw data."""
        return self._output_format == RAW

    @property
    def is_summary(self):
        """Return whether the output format is at summary level."""
        return self._output_level == SUMMARY_LEVEL

    @property
    def is_detail(self):
        """Return whether the output format is at detail level."""
        return self._output_level == DETAIL_LEVEL

    ###############
    # PUBLIC API
    ###############
    def get_offload_status_report_data(
        self,
        schema=None,
        table=None,
        output_level=DEFAULT_OUTPUT_LEVEL,
        demo_mode=DEFAULT_DEMO_MODE,
    ):
        """Fetch and pre-process all of the data required for either the Offload Status Report or API consumer."""
        if schema is not None:
            assert self._valid_sql_name(schema), "invalid value for schema parameter"
            self._schema = schema.upper()
        else:
            self._schema = schema

        if table is not None:
            assert self._valid_sql_name(table), "invalid value for table parameter"
            self._table = table.upper()
        else:
            self._table = table

        self._output_level = (
            output_level.lower() if output_level is not None else DEFAULT_OUTPUT_LEVEL
        )
        self._demo_mode = demo_mode

        self._messages.debug(
            "%-30s : %s" % ("_schema", self._schema), detail=SUPPRESS_STDOUT
        )
        self._messages.debug(
            "%-30s : %s" % ("_table", self._table), detail=SUPPRESS_STDOUT
        )
        self._messages.debug(
            "%-30s : %s" % ("_output_level", self._output_level), detail=SUPPRESS_STDOUT
        )
        self._messages.debug(
            "%-30s : %s" % ("_demo_mode", self._demo_mode), detail=SUPPRESS_STDOUT
        )

        assert self._output_level in (
            SUMMARY_LEVEL,
            DETAIL_LEVEL,
        ), "output_level parameter must be either %s or %s" % (
            SUMMARY_LEVEL,
            DETAIL_LEVEL,
        )

        if not self._demo_mode:
            offloaded_tables = self._messages.offload_step(
                command_steps.STEP_OSR_FIND_TABLES,
                lambda: self._get_offloaded_tables(),
                self._execute,
            )
            table_data = self._messages.offload_step(
                command_steps.STEP_OSR_FETCH_DATA,
                lambda: self._get_table_data(offloaded_tables),
                self._execute,
            )
        else:
            table_data = self._messages.offload_step(
                command_steps.STEP_OSR_DEMO_DATA,
                lambda: self._get_demo_table_data(),
                self._execute,
            )

        report_data = self._messages.offload_step(
            command_steps.STEP_OSR_PROCESS_DATA,
            lambda: self._process_report_data(table_data),
            self._execute,
        )

        return report_data

    def gen_offload_status_report(
        self,
        report_data,
        output_format=DEFAULT_OUTPUT_FORMAT,
        report_name=DEFAULT_REPORT_NAME,
        report_directory=DEFAULT_REPORT_DIRECTORY,
        csv_delimiter=DEFAULT_CSV_DELIMITER,
        csv_enclosure=DEFAULT_CSV_ENCLOSURE,
    ):
        """Fetch and pre-process all of the data required for either the Offload Status Report or API consumer."""
        self._output_format = (
            output_format.lower()
            if output_format is not None
            else DEFAULT_OUTPUT_FORMAT
        )
        self._report_name = report_name or DEFAULT_REPORT_NAME
        self._report_directory = report_directory or DEFAULT_REPORT_DIRECTORY
        self._csv_delimiter = csv_delimiter or DEFAULT_CSV_DELIMITER
        self._csv_enclosure = csv_enclosure or DEFAULT_CSV_ENCLOSURE

        self._messages.debug(
            "%-30s : %s" % ("_output_format", self._output_format),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-30s : %s" % ("_report_name", self._report_name), detail=SUPPRESS_STDOUT
        )
        self._messages.debug(
            "%-30s : %s" % ("_report_directory", self._report_directory),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-30s : %s" % ("_csv_delimiter", self._csv_delimiter),
            detail=SUPPRESS_STDOUT,
        )
        self._messages.debug(
            "%-30s : %s" % ("_csv_enclosure", self._csv_enclosure),
            detail=SUPPRESS_STDOUT,
        )

        assert self._output_format in (
            CSV,
            TEXT,
            HTML,
            JSON,
            RAW,
        ), "output_format parameter must be one of [%s, %s, %s, %s, %s]" % (
            CSV,
            TEXT,
            HTML,
            JSON,
            RAW,
        )
        assert (
            " " not in self._report_name
        ), "report_name parameter cannot contain spaces"
        assert (
            " " not in self._report_directory
        ), "report_directory parameter cannot contain spaces"
        assert (
            len(self._csv_delimiter) == 1
        ), "csv_delimiter parameter must be a single character"
        assert (
            len(self._csv_enclosure) == 1
        ), "csv_enclosure parameter must be a single character"

        if self._output_format == CSV:
            rep_fn = lambda: self._gen_csv_report(report_data)
        elif self._output_format == TEXT:
            rep_fn = lambda: self._gen_text_report(report_data)
        elif self._output_format == HTML:
            rep_fn = lambda: self._gen_html_report(report_data)
        elif self._output_format == JSON:
            rep_fn = lambda: self._gen_json_report(report_data)
        elif self._output_format == RAW:
            rep_fn = lambda: self._gen_raw_report(report_data)
        status = self._messages.offload_step(
            command_steps.STEP_OSR_GENERATE_REPORT, rep_fn, execute=self._execute
        )
        return status


def get_offload_status_report_opts():
    opt = get_common_options(usage="usage: %prog [options]")

    osr_group = OptionGroup(
        opt, "Offload Status Report Options", "Options for Offload Status Report."
    )
    osr_group.add_option(
        "-s",
        "--schema",
        dest="schema",
        help="Optional name of schema to run the Offload Status Report for",
    )
    osr_group.add_option(
        "-t",
        "--table",
        dest="table",
        help="Optional name of table to run the Offload Status Report for",
    )
    osr_group.add_option(
        "-o",
        "--output-format",
        dest="output_format",
        default=DEFAULT_OUTPUT_FORMAT,
        choices=[HTML, TEXT, JSON, RAW, CSV],
        help="Output format for the Offload Status Report data. Default: %s"
        % DEFAULT_OUTPUT_FORMAT,
    )
    osr_group.add_option(
        "-d",
        "--demo",
        dest="demo_mode",
        action="store_true",
        default=DEFAULT_DEMO_MODE,
        help=SUPPRESS_HELP,
    )
    osr_group.add_option(
        "--output-level",
        dest="output_level",
        default=DEFAULT_OUTPUT_LEVEL,
        choices=[DETAIL_LEVEL, SUMMARY_LEVEL],
        help="Level of detail required for the Offload Status Report. Default: %s"
        % DEFAULT_OUTPUT_LEVEL,
    )
    osr_group.add_option(
        "--report-name",
        dest="report_name",
        default=DEFAULT_REPORT_NAME,
        help="Name of report. Default: %s_{DB_NAME}_{YYYY}-{MM}-{DD}_{HH}-{MI}-{SS}.[html|txt|csv]"
        % DEFAULT_REPORT_NAME,
    )
    osr_group.add_option(
        "--report-directory",
        dest="report_directory",
        default=DEFAULT_REPORT_DIRECTORY,
        help="Directory to save the report in. Default: %s" % DEFAULT_REPORT_DIRECTORY,
    )
    osr_group.add_option(
        "--csv-delimiter",
        dest="csv_delimiter",
        default=DEFAULT_CSV_DELIMITER,
        help="Field delimiter character for CSV output. Must be a single character. Default: %s"
        % DEFAULT_CSV_DELIMITER,
    )
    osr_group.add_option(
        "--csv-enclosure",
        dest="csv_enclosure",
        default=DEFAULT_CSV_ENCLOSURE,
        help="Enclosure character for string fields in CSV output. Default: %s"
        % DEFAULT_CSV_ENCLOSURE,
    )
    opt.add_option_group(osr_group)

    # Strip away the options we don't need
    remove_options = ["--skip-steps", "--log-path", "-x"]
    [opt.remove_option(option) for option in remove_options]

    return opt


def normalise_offload_status_report_options(options):
    if options.vverbose:
        options.verbose = True
    elif options.quiet:
        options.vverbose = options.verbose = False

    if not os.path.exists(options.report_directory):
        raise OffloadOptionError(
            "Invalid value specified for --report-directory parameter. Directory %s does not exist."
            % options.report_directory
        )

    if len(options.csv_delimiter) > 1:
        raise OffloadOptionError(
            "Invalid value specified for --csv-delimiter parameter (%s). Delimiter must be a single character."
            % options.csv_delimiter
        )

    if len(options.csv_enclosure) > 1:
        raise OffloadOptionError(
            "Invalid value specified for --csv-enclosure parameter (%s). Enclosure must be a single character or NULL."
            % options.csv_enclosure
        )

    return


def datetime_literal_to_hv(dt_literal, col_type):
    """Take a literal chopped out of incremental metadata and convert it to a Python
    value. Reuses code from the Oracle version of OffloadSourceTable.
    Seems safe to do for now because this tool is heavily Oracle based.
    """
    if oracle_datetime_literal_to_python(dt_literal, strict=False) is not None:
        # must use "is not None" above as 1970-01-01 is considered False. GOE-1014
        return oracle_datetime_literal_to_python(dt_literal)
    elif re.match(STARTS_WITH_DATE_PATTERN_RE, dt_literal):
        try:
            return datetime64(dt_literal)
        except ValueError as exc:
            raise OffloadStatusReportException(
                'Failed to parse "%s" value: "%s"' % (col_type, dt_literal)
            )
    else:
        raise OffloadStatusReportException(
            'Failed to parse "%s" value: "%s"' % (col_type, dt_literal)
        )


def incremental_partition_match(
    row_high_value, table_high_value, incremental_predicate_type
):
    """Check if a partition is classified as offloaded"""
    if incremental_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        assert isinstance(
            row_high_value, list
        ), "LPA High Value type != list: {}".format(type(row_high_value))
        return any(_ in row_high_value for _ in table_high_value)
    else:
        if table_high_value is None:
            # Maintaining Python 2 behaviour that <= None is False
            return False
        assert isinstance(
            row_high_value, tuple
        ), "RPA High Value type != tuple: {}".format(type(row_high_value))
        if row_high_value == (offload_constants.PART_OUT_OF_LIST,):
            # If we have a table high value then we are IPA and cannot have offloaded a DEFAULT partition
            # This maintains Python 2 behaviour that str(...) <= datetime64(...) is False
            return False
        return bool(row_high_value <= table_high_value)


def offload_status_report_run():
    options = None
    try:
        opt = get_offload_status_report_opts()
        (options, args) = opt.parse_args()

        init(options)
        init_log("offload_status_report")

        log("")
        log("Offload Status Report v%s" % version(), ansi_code="underline")
        log("Log file: %s" % get_log_fh_name())
        log("", VERBOSE)
        log_command_line()

        normalise_offload_status_report_options(options)
        messages = OffloadMessages.from_options(
            options,
            log_fh=get_log_fh(),
            command_type=orchestration_constants.COMMAND_OSR,
        )
        orchestration_config = OrchestrationConfig.from_dict(
            {"verbose": options.verbose, "vverbose": options.vverbose}
        )
        frontend_api = frontend_api_factory(
            orchestration_config.db_type,
            orchestration_config,
            messages,
            dry_run=True,
            do_not_connect=True,
        )
        if not frontend_api.goe_offload_status_report_supported():
            raise OffloadStatusReportException(
                "Offload Status Report is not supported for frontend system: %s"
                % frontend_api.frontend_db_name()
            )

        osr = OffloadStatusReport(orchestration_config, messages)
        report_data = osr.get_offload_status_report_data(
            options.schema, options.table, options.output_level, options.demo_mode
        )
        status = osr.gen_offload_status_report(
            report_data,
            options.output_format,
            options.report_name,
            options.report_directory,
            options.csv_delimiter,
            options.csv_enclosure,
        )

        sys.exit(status)

    except OffloadOptionError as exc:
        log("Option error: %s" % exc.detail, ansi_code="red")
        log("")
        opt.print_help()
        sys.exit(FAILURE)
    except Exception as exc:
        log("Exception caught at top-level", ansi_code="red")
        log_timestamp()
        log_exception(exc, log_fh=get_log_fh(), options=options)
        sys.exit(1)
