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

""" BigQueryBackendApi: Library for logic/interaction with a remote BigQuery backend.
    This module enforces an interface with common, highlevel, methods and an implementation
    for each supported remote system, e.g. Impala, Hive, Google BigQuery.
"""

from datetime import datetime
import logging
from math import ceil
import pprint
import re
from textwrap import dedent
import traceback

from google.cloud import bigquery, kms
from google.cloud.exceptions import NotFound
from numpy import datetime64

from goe.connect.connect_constants import CONNECT_DETAIL, CONNECT_STATUS, CONNECT_TEST
from goe.offload.column_metadata import (
    CanonicalColumn,
    ColumnMetadataInterface,
    ColumnPartitionInfo,
    is_safe_mapping,
    match_table_column,
    str_list_of_columns,
    valid_column_list,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_VARIABLE_STRING,
    GOE_TYPE_BINARY,
    GOE_TYPE_LARGE_BINARY,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_FLOAT,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_DATE,
    GOE_TYPE_TIME,
    GOE_TYPE_TIMESTAMP,
    GOE_TYPE_TIMESTAMP_TZ,
    GOE_TYPE_INTERVAL_DS,
    GOE_TYPE_INTERVAL_YM,
    GOE_TYPE_BOOLEAN,
    ALL_CANONICAL_TYPES,
    DATE_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
)
from goe.offload.backend_api import (
    BackendApiInterface,
    BackendApiException,
    UdfDetails,
    UdfParameter,
    FETCH_ACTION_ALL,
    FETCH_ACTION_ONE,
    REPORT_ATTR_BACKEND_CLASS,
    REPORT_ATTR_BACKEND_TYPE,
    REPORT_ATTR_BACKEND_DISPLAY_NAME,
    REPORT_ATTR_BACKEND_HOST_INFO_TYPE,
    REPORT_ATTR_BACKEND_HOST_INFO,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_constants import (
    EMPTY_BACKEND_TABLE_STATS_DICT,
    EMPTY_BACKEND_COLUMN_STATS_LIST,
    EMPTY_BACKEND_COLUMN_STATS_DICT,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_BIGTABLE,
    BIGQUERY_BACKEND_CAPABILITIES,
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
    PART_COL_GRANULARITY_YEAR,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.offload.bigquery.bigquery_column import (
    BigQueryColumn,
    BIGQUERY_TYPE_BIGNUMERIC,
    BIGQUERY_TYPE_BOOLEAN,
    BIGQUERY_TYPE_BYTES,
    BIGQUERY_TYPE_DATE,
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_FLOAT64,
    BIGQUERY_TYPE_INT64,
    BIGQUERY_TYPE_NUMERIC,
    BIGQUERY_TYPE_STRING,
    BIGQUERY_TYPE_TIME,
    BIGQUERY_TYPE_TIMESTAMP,
)
from goe.offload.bigquery.bigquery_literal import BigQueryLiteral

from goe.util.misc_functions import backtick_sandwich, format_list_for_logging

###############################################################################
# CONSTANTS
###############################################################################

# Regular expression matching invalid identifier characters, constant to ensure compiled only once
BIGQUERY_INVALID_IDENTIFIER_CHARS_RE = re.compile(r"[^A-Z0-9_ ]", re.I)

SUPPORTED_BQ_DATE_PARTITIONING_TYPES = ["DAY", "MONTH", "YEAR"]
# This is a surrogate for the lack of a type exposed within BigQuery RangePartitioning() object
SUPPORTED_BQ_RANGE_PARTITIONING_TYPE = "RANGE"

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BackendBigQueryApi
###########################################################################


class BackendBigQueryApi(BackendApiInterface):
    """BigQuery backend implementation"""

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
        super(BackendBigQueryApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

        logger.info("BackendBigQueryApi")
        if dry_run:
            logger.info("* Dry run *")

        self._client = None
        self._bigquery_dataset_location = (
            connection_options.bigquery_dataset_location or None
        )
        self._bigquery_dataset_project = (
            connection_options.bigquery_dataset_project or None
        )

        self._google_kms_key_ring_location = (
            connection_options.google_kms_key_ring_location or None
        )
        self._google_kms_key_ring_name = (
            connection_options.google_kms_key_ring_name or None
        )
        self._google_kms_key_ring_project = (
            connection_options.google_kms_key_ring_project or None
        )
        self._google_kms_key_name = connection_options.google_kms_key_name or None

        self._sql_engine_name = "BigQuery"
        self._log_query_id_tag = "BigQuery Job ID"
        self._cached_bq_tables = {}

        if not do_not_connect:
            self._client = self._get_bq_client()

        self._kms_key_name = self._kms_client = None
        if (
            self._google_kms_key_ring_location
            and self._google_kms_key_ring_name
            and self._google_kms_key_name
            and not do_not_connect
        ):
            self._kms_client = self._get_kms_client()
            self._kms_key_name = self.kms_key_name()

    def __del__(self):
        self.close()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _add_kms_key_to_job_config(self, job_config):
        """Add kms key details (if set) to job config"""
        if self._kms_key_name:
            self._log(
                "Setting job session option: kms_key_name=%s" % self._kms_key_name,
                detail=VVERBOSE,
            )
            encryption_config = bigquery.EncryptionConfiguration(
                kms_key_name=self._kms_key_name
            )
            job_config.destination_encryption_configuration = encryption_config

    def _add_query_params_to_job_config(self, query_params, job_config):
        """Convert query_params dict to QueryJobConfig attribute."""
        if query_params:
            assert isinstance(query_params, list)
            assert isinstance(query_params[0], tuple)
            param_list = []
            for param_name, param_type, param_value in query_params:
                param_list.append(
                    bigquery.ScalarQueryParameter(param_name, param_type, param_value)
                )
            job_config.query_parameters = param_list

    def _backend_capabilities(self) -> dict:
        return BIGQUERY_BACKEND_CAPABILITIES

    def _backend_project_name(self):
        """Get name of BigQuery project from client"""
        if self._bigquery_dataset_project:
            return self._bigquery_dataset_project
        elif self._client:
            return self._client.project
        else:
            return None

    def _bq_dataset_id(self, db_name: str) -> str:
        """Return a dataset id for a db name.
        Returns str() because dataset drop/exists calls were failing when passing in unicode.
        """
        assert db_name
        if self._backend_project_name():
            return str("%s.%s" % (self._backend_project_name(), db_name))
        else:
            # Non-connecting calls (for unit tests) will not have set _client
            return str("%s.%s" % ("a-test-project", db_name))

    def _bq_table_id(self, db_name: str, table_name: str) -> str:
        assert db_name and table_name
        return self._bq_dataset_id(db_name) + "." + table_name

    def _check_kms_key_name(self, kms_key_name: str, key_type="job"):
        """Use startswith() to verify custom key for key name.
        Example of custom key name:
            projects/goe-teamcity/locations/us-west3/keyRings/krname/cryptoKeys/etl5
        Example of what we get from a query_job object:
            projects/goe-teamcity/locations/us-west3/keyRings/krname/cryptoKeys/etl5/cryptoKeyVersions/1
        It's worth noting that this has not always been the case, BigQuery behaviour has changed in the past.
        """
        if self._kms_key_name and not (kms_key_name or "").startswith(
            self._kms_key_name
        ):
            self._warning(
                "BigQuery %s KMS key: %s\n != expected KMS key: %s"
                % (key_type, kms_key_name, self._kms_key_name)
            )

    def _create_external_table(
        self,
        db_name,
        table_name,
        storage_format,
        location=None,
        with_terminator=False,
    ) -> list:
        """Create a BigQuery external table using SQL"""
        assert db_name
        assert table_name
        assert storage_format in (
            FILE_STORAGE_FORMAT_AVRO,
            FILE_STORAGE_FORMAT_PARQUET,
        ), f"Unsupported staging format: {storage_format}"
        assert location

        sql = """CREATE EXTERNAL TABLE {db_table}
OPTIONS (format ='{format}',
         uris = ['{location}'],
         description = 'GOE staging table');
""".format(
            db_table=self.enclose_object_reference(db_name, table_name),
            format=storage_format,
            location=location,
        )
        if with_terminator:
            sql += ";"
        return self.execute_ddl(sql)

    def _create_table_properties_clause(self, table_properties):
        """Build OPTIONS clause for CREATE TABLE statements from table_properties dict. Add kms key details (if set)"""
        if table_properties:
            assert isinstance(table_properties, dict)
        else:
            table_properties = {}

        if self._kms_key_name:
            table_properties["kms_key_name"] = self.to_backend_literal(
                self._kms_key_name
            )

        if table_properties:
            # We don't enforce literals for values as they can be of different forms, see the following for examples:
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list
            table_prop_clause = "\nOPTIONS (%s)" % ", ".join(
                "%s=%s" % (k, v) for k, v in table_properties.items()
            )
        else:
            table_prop_clause = ""
        return table_prop_clause

    def _default_job_config(
        self, query_options: dict = None
    ) -> bigquery.QueryJobConfig:
        """All connections are normalized to UTC to match data extractions."""
        if query_options and "use_legacy_sql" in query_options:
            # No time_zone manipulation for legacy_sql.
            return bigquery.QueryJobConfig()
        else:
            return bigquery.QueryJobConfig(
                connection_properties=[bigquery.ConnectionProperty("time_zone", "UTC")]
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
        """See interface for parameter descriptions
        sync: No concept of sync vs async on BigQuery
        """

        assert sql
        assert isinstance(sql, (str, list))
        return_list = []
        job_config = self._default_job_config(query_options=query_options)
        self._add_query_options_to_job_config(query_options, job_config)
        sqls = [sql] if isinstance(sql, str) else sql
        for i, run_sql in enumerate(sqls):
            self._log_or_not(
                "%s SQL: %s" % (self._sql_engine_name, run_sql),
                log_level=log_level,
                no_log_items=no_log_items,
            )
            return_list.append(run_sql)
            if not self._dry_run:
                query_job = self._client.query(run_sql, job_config=job_config)
                self._log(
                    "%s: %s" % (self._log_query_id_tag, query_job.job_id),
                    detail=VVERBOSE,
                )
                query_job.result()
                if query_job.state != "DONE":
                    raise BackendApiException(
                        "Unexpected BigQuery job state: %s" % query_job.state
                    )

                if (
                    query_options
                    and "destination_encryption_configuration" in query_options
                ):
                    self._check_kms_key_name(
                        query_job.destination_encryption_configuration.kms_key_name
                    )

                if profile:
                    self._log(self._get_query_profile(query_job), detail=VVERBOSE)
        return return_list

    def _execute_global_session_parameters(self, log_level=VVERBOSE):
        """Build and return a default QueryJobConfig object for global backend session parameters.
        As per docs:
            default_query_job_config – (Optional) Will be merged into job configs passed into the query method.
        """
        default_config_white_list = ["maximum_bytes_billed", "use_query_cache"]
        if self._global_session_parameters:
            job_config = self._default_job_config()
            self._log("Setting global session options:", detail=log_level)
            for k, v in [
                (str(k).lower(), v) for k, v in self._global_session_parameters.items()
            ]:
                if k not in default_config_white_list:
                    raise BackendApiException(
                        "Modification of job configuration %s is not permitted, valid values: %s"
                        % (k, ", ".join(default_config_white_list))
                    )
                self._log("%s: %s" % (k, v), detail=log_level)
                setattr(job_config, k, v)
            return job_config
        else:
            return None

    def _cursor_projection(self, cursor) -> list:
        """
        Returns a list of strings describing the projection of a BigQuery result set.
        Matches behaviour of interface _cursor_projection().
        """
        return [_.name.lower() for _ in cursor.schema]

    def _execute_query_fetch_x(
        self,
        sql,
        fetch_action=FETCH_ACTION_ALL,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
        as_dict=False,
    ):
        """Run a query and fetch results
        query_options: key/value pairs for session settings
        query_params: list of (param, type, value) tuples matching params in sql
        log_level: None = no logging. Otherwise VERBOSE/VVERBOSE etc
        time_sql: If logging then log SQL elapsed time
        not_when_dry_running: Some SQL will only work in execute mode, such as queries on a load table
        as_dict: When True each row is returned as a dict keyed by column name rather than a tuple/list.
        """
        assert sql
        assert fetch_action in (FETCH_ACTION_ALL, FETCH_ACTION_ONE)

        self._log_or_not(
            "%s SQL: %s" % (self._sql_engine_name, sql), log_level=log_level
        )

        if self._dry_run and not_when_dry_running:
            return None

        t1 = datetime.now().replace(microsecond=0)

        job_config = self._default_job_config(query_options=query_options)
        self._add_kms_key_to_job_config(job_config)
        self._add_query_options_to_job_config(query_options, job_config)
        self._add_query_params_to_job_config(query_params, job_config)
        query_job = self._client.query(sql, job_config=job_config)
        self._log(
            "%s: %s" % (self._log_query_id_tag, query_job.job_id), detail=VVERBOSE
        )

        if query_options and "destination_encryption_configuration" in query_options:
            self._check_kms_key_name(
                query_job.destination_encryption_configuration.kms_key_name
            )

        if fetch_action == FETCH_ACTION_ALL:
            results = query_job.result()
            if query_job.state != "DONE":
                raise BackendApiException(
                    "Unexpected BigQuery job state: %s" % query_job.state
                )
            rows = list(results)
            if as_dict:
                columns = self._cursor_projection(results)
                rows = [self._cursor_row_to_dict(columns, _) for _ in rows]
            else:
                # Convert the result set from a RowIterator to a list of tuples, matching other backends
                rows = [tuple(_) for _ in rows]
        else:
            results = query_job.result(max_results=1)
            # lower() matching
            if query_job.state != "DONE":
                raise BackendApiException(
                    "Unexpected BigQuery job state: %s" % query_job.state
                )
            if results and results.total_rows == 0:
                rows = None
            else:
                row_iter = iter(results)
                rows = tuple(next(row_iter))
                if as_dict:
                    columns = self._cursor_projection(results)
                    rows = self._cursor_row_to_dict(columns, rows)
                else:
                    # Convert the result set from a RowIterator to a list of tuples, matching other backends
                    rows = tuple(rows)

        if time_sql:
            t2 = datetime.now().replace(microsecond=0)
            self._log("Elapsed time: %s" % (t2 - t1), detail=VVERBOSE)

        if profile:
            self._log(self._get_query_profile(query_job), detail=VVERBOSE)

        return rows

    def _forget_bq_table(self, db_name, table_name):
        """Remove a cached table from state"""
        table_id = self._bq_table_id(db_name, table_name)
        if table_id in self._cached_bq_tables:
            del self._cached_bq_tables[table_id]

    def _get_bq_client(self):
        # Whenever we make a new connection we should set global session parameters
        return bigquery.Client(
            default_query_job_config=self._execute_global_session_parameters(),
            location=self._bigquery_dataset_location,
        )

    def _get_kms_client(self):
        return kms.KeyManagementServiceClient()

    def _gen_sample_stats_sql_sample_clause(
        self, db_name, table_name, sample_perc=None
    ):
        """No Query SAMPLE clause on BigQuery"""
        return None

    def _gen_bigquery_create_table_sql_text(
        self,
        db_name,
        table_name,
        column_list,
        partition_column_names,
        table_properties=None,
        sort_column_names=None,
        for_replace=False,
    ):
        """Build a CREATE TABLE SQL statement."""
        assert db_name
        assert table_name
        assert column_list
        assert valid_column_list(column_list), (
            "Incorrectly formed column_list: %s" % column_list
        )
        if partition_column_names:
            assert isinstance(partition_column_names, list)
            assert (
                len(partition_column_names) == 1
            ), "Only a single partition column is supported"
        if table_properties:
            assert isinstance(table_properties, dict)

        col_projection = self._create_table_columns_clause_common(
            column_list, external=False
        )

        if partition_column_names:
            real_col = match_table_column(partition_column_names[0], column_list)
            if not real_col:
                self._log(
                    "Proposed table columns: %s" % str_list_of_columns(column_list),
                    detail=VERBOSE,
                )
                raise BackendApiException(
                    "Partition column is not in table columns: %s"
                    % partition_column_names[0]
                )
            self._debug("Partitioning by: %s %s" % (real_col.name, real_col.data_type))
            part_clause = (
                "\nPARTITION BY %s" % self.gen_native_range_partition_key_cast(real_col)
            )
        else:
            part_clause = ""

        if sort_column_names:
            sort_csv = ",".join([self.enclose_identifier(_) for _ in sort_column_names])
            cluster_by_clause = ("\nCLUSTER BY %s" % sort_csv) if sort_csv else ""
        else:
            cluster_by_clause = ""

        table_prop_clause = self._create_table_properties_clause(table_properties)

        sql = """CREATE%(or_replace)s TABLE %(db_table)s (
%(col_projection)s
)%(part_clause)s%(cluster_by_clause)s%(table_prop_clause)s""" % {
            "or_replace": " OR REPLACE" if for_replace else "",
            "db_table": self.enclose_object_reference(db_name, table_name),
            "col_projection": col_projection,
            "part_clause": part_clause,
            "cluster_by_clause": cluster_by_clause,
            "table_prop_clause": table_prop_clause,
        }
        return sql

    def _get_bq_job(self, job_id):
        """Retrieve BigQuery job"""
        try:
            job_details = self._client.get_job(job_id)
        except NotFound:
            raise BackendApiException("Backend job not found for job id: %s" % job_id)
        return job_details

    def _get_bq_table(self, db_name, table_name):
        """Instantiates bigquery.Table() if required or retrieves from cache"""
        table_id = self._bq_table_id(db_name, table_name)
        if table_id not in self._cached_bq_tables:
            bq_table = self._client.get_table(table_id)
            if self._no_caching:
                return bq_table
            self._cached_bq_tables[table_id] = bq_table
        return self._cached_bq_tables[table_id]

    def _get_bq_table_partition_field(self, table):
        """Return the partition field name for a BigQuery client table object as returned by self._get_bq_table()."""
        assert table
        if table.time_partitioning and table.time_partitioning.field:
            return table.time_partitioning.field
        elif table.range_partitioning and table.range_partitioning.field:
            return table.range_partitioning.field
        else:
            return None

    def _get_bq_table_partition_type(self, table):
        """Return the partition type for a BigQuery client table object as returned by self._get_bq_table()."""
        assert table
        if table.time_partitioning and table.time_partitioning.type_:
            return table.time_partitioning.type_
        elif table.range_partitioning and table.range_partitioning.range_:
            return SUPPORTED_BQ_RANGE_PARTITIONING_TYPE
        else:
            return None

    def _get_query_profile(self, query_identifier=None):
        """On BigQuery query_identifier is a QueryJob object.
        Slot Usage: This is a statistic derived by us. Text from GOE-1797:
            When looking at QueryStatistics, slot usage is defined as:
                ceil(slotMillis/(endTime-creationTime))
            Using the attributes available from this API, the calculation would be:
                ceil(slot_millis-(ended-created))
            Where ended and created are DATETIME, meaning that the interval will need to be converted to ms.
        """
        stats = [
            ("Statistic", "Value"),
            ("Job id", query_identifier.job_id),
            ("Started", str(query_identifier.started)),
            ("Ended", str(query_identifier.ended)),
            ("Bytes processed", query_identifier.total_bytes_processed),
            ("Bytes billed", query_identifier.total_bytes_billed),
            ("Slot milliseconds", str(query_identifier.slot_millis)),
        ]
        if query_identifier.num_dml_affected_rows is not None:
            stats.append(
                ("Num DML affected rows", query_identifier.num_dml_affected_rows)
            )
        try:
            if (
                query_identifier.slot_millis
                and query_identifier.started
                and query_identifier.ended
            ):
                elapsed_ms = (
                    query_identifier.ended - query_identifier.started
                ).total_seconds() * 1000
                stats.append(
                    ("Slot usage", str(ceil(query_identifier.slot_millis / elapsed_ms)))
                )
        except Exception as exc:
            self._log(
                "Exception formatting BigQuery profile (non-fatal): %s" % str(exc),
                detail=VERBOSE,
            )
            self._log(traceback.format_exc(), detail=VVERBOSE)
        return format_list_for_logging(stats)

    def _get_goe_granularity_for_bq_partitioning_type(self, table):
        """Return the goe granularity for a BigQuery date partition type.
        table is a table object as returned by self._get_bq_table().
        """
        assert table
        partitioning_type = self._get_bq_table_partition_type(table)
        if partitioning_type not in SUPPORTED_BQ_DATE_PARTITIONING_TYPES:
            raise NotImplementedError(
                "BigQuery partitioning type is not supported: %s" % partitioning_type
            )
        # BQ granularities start with same letter as GOE granularities
        return partitioning_type[0]

    def _get_table_ddl(self, db_name, table_name, terminate_sql=False):
        """Return DDL string for a table or view."""
        assert db_name and table_name
        self._log("Fetching DDL: %s.%s" % (db_name, table_name), detail=VVERBOSE)
        sql = (
            dedent(
                """\
                                SELECT ddl
                                FROM   `%s`.INFORMATION_SCHEMA.TABLES
                                WHERE  table_catalog = @project
                                AND    table_schema = @db_name
                                AND    table_name = @table_name"""
            )
            % self._bq_dataset_id(db_name)
        )
        query_params = [
            ("project", BIGQUERY_TYPE_STRING, self._backend_project_name()),
            ("db_name", BIGQUERY_TYPE_STRING, db_name),
            ("table_name", BIGQUERY_TYPE_STRING, table_name),
        ]
        try:
            row = self.execute_query_fetch_one(
                sql, log_level=VVERBOSE, query_params=query_params
            )
        except NotFound as exc:
            self._log("NotFound exception: {}".format(str(exc)))
            raise BackendApiException(
                "BigQuery dataset not found: %s" % self._bq_dataset_id(db_name)
            ) from None
        if not row:
            raise BackendApiException(
                "Object does not exist for DDL retrieval: %s.%s" % (db_name, table_name)
            )
        ddl_str = row[0]
        if not terminate_sql:
            ddl_str = ddl_str.rstrip(";")
        return ddl_str

    def _has_outstanding_streaming_inserts(self, db_name, table_name):
        table = self._get_bq_table(db_name, table_name)
        return bool(table.streaming_buffer and table.streaming_buffer.estimated_rows)

    def _invalid_identifier_character_re(self):
        return BIGQUERY_INVALID_IDENTIFIER_CHARS_RE

    def _is_external_table(self, db_name, table_name):
        table = self._get_bq_table(db_name, table_name)
        return bool(table.table_type == "EXTERNAL")

    def _object_exists(self, db_name, table_name, table_type=None):
        """Test if an object (TABLE or VIEW) exists.
        If table_type is set then we look for specific type, otherwise either will do.
        """
        try:
            table = self._client.get_table(self._bq_table_id(db_name, table_name))
            if not table_type:
                return True
            elif table_type.upper() == "TABLE" and table.table_type.upper() in [
                "TABLE",
                "EXTERNAL",
            ]:
                return True
            elif table.table_type.upper() == table_type.upper():
                return True
            else:
                return False
        except NotFound:
            return False

    def _add_query_options_to_job_config(
        self, query_options: dict, job_config: bigquery.QueryJobConfig
    ):
        """Convert query_options dict to QueryJobConfig attribute."""
        if query_options:
            assert isinstance(query_options, dict)
            for k, v in query_options.items():
                setattr(job_config, k, v)
                self._log("Setting job session option: %s=%s" % (k, v), detail=VVERBOSE)

    def _table_is_partitioned(self, db_name: str, table_name: str):
        """BigQuery specific helper to identify if a table is partitioned"""
        assert db_name and table_name
        table = self._get_bq_table(db_name, table_name)
        return bool(
            self._get_bq_table_partition_type(table)
            in (
                SUPPORTED_BQ_DATE_PARTITIONING_TYPES
                + [SUPPORTED_BQ_RANGE_PARTITIONING_TYPE]
            )
        )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_db_name(self):
        """Override required for BigQuery due to stylistic formatting vs "capitalize()" """
        return "BigQuery"

    def add_columns(self, db_name, table_name, column_tuples, sync=None):
        """See interface spec for description"""
        assert db_name and table_name
        assert column_tuples
        assert isinstance(column_tuples, list), "%s is not list" % type(column_tuples)
        assert isinstance(column_tuples[0], (tuple, list))
        table = self._get_bq_table(db_name, table_name)
        original_schema = table.schema
        self._debug("add_columns original schema: %s" % original_schema)
        submittable_lines = ", ".join(
            [
                "ADD COLUMN IF NOT EXISTS " + " ".join([val for val in column])
                for column in column_tuples
            ]
        )
        sql = "ALTER TABLE %s %s" % (
            self.enclose_object_reference(db_name, table_name),
            submittable_lines,
        )
        if not self._dry_run:
            self.execute_ddl(sql, sync=sync)
        self._forget_bq_table(db_name, table_name)
        return [sql]

    def alter_sort_columns(self, db_name, table_name, sort_column_names, sync=None):
        """Use BigQuery API to modify clustering fields, there is no SQL syntax for this yet."""
        assert db_name and table_name
        assert isinstance(sort_column_names, list)
        table = self._get_bq_table(db_name, table_name)
        table.clustering_fields = sort_column_names if sort_column_names else None
        log_cmd = "update_table(%s, %s, clustering_fields=%s)" % (
            db_name,
            table_name,
            str(table.clustering_fields),
        )
        self._log("BigQuery call: %s" % log_cmd, detail=VERBOSE)
        if not self._dry_run:
            table = self._client.update_table(table, ["clustering_fields"])
        self._forget_bq_table(db_name, table_name)
        return [log_cmd]

    def backend_report_info(self):
        """Reporting information about the type of backend and how to display
        its configuration details (e.g. Hadoop host:port or BigQuery project).
        """
        return {
            REPORT_ATTR_BACKEND_CLASS: "Cloud Warehouse",
            REPORT_ATTR_BACKEND_TYPE: self._backend_type,
            REPORT_ATTR_BACKEND_DISPLAY_NAME: self.backend_db_name(),
            REPORT_ATTR_BACKEND_HOST_INFO_TYPE: "Project",
            REPORT_ATTR_BACKEND_HOST_INFO: self._backend_project_name(),
        }

    def backend_version(self):
        """No BigQuery version available via SQL or API"""
        return None

    def bq_client(self):
        """Get BigQuery client for use in BigQuery testing API"""
        return self._client

    def bq_dataset_id(self, db_name):
        """Public wrapper for use in BigQuery testing API"""
        return self._bq_dataset_id(db_name)

    def check_backend_supporting_objects(self, orchestration_options):
        results = []

        if self._kms_key_name:
            try:
                key = self._kms_client.get_crypto_key(
                    request={"name": self._kms_key_name}
                )
                if key.purpose == kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT:
                    if (
                        key.primary.state
                        == kms.CryptoKeyVersion.CryptoKeyVersionState.ENABLED
                    ):
                        results.append(
                            {
                                CONNECT_TEST: "Test KMS key",
                                CONNECT_STATUS: True,
                                CONNECT_DETAIL: "KMS key: %s is an %s key and is %s"
                                % (
                                    key.name,
                                    kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT.name,
                                    kms.CryptoKeyVersion.CryptoKeyVersionState.ENABLED.name,
                                ),
                            }
                        )
                    else:
                        results.append(
                            {
                                CONNECT_TEST: "Test KMS key",
                                CONNECT_STATUS: False,
                                CONNECT_DETAIL: "KMS key: %s has incorrect state: %s, expected key state is: %s"
                                % (
                                    key.name,
                                    key.primary.state.name,
                                    kms.CryptoKeyVersion.CryptoKeyVersionState.ENABLED.name,
                                ),
                            }
                        )
                else:
                    results.append(
                        {
                            CONNECT_TEST: "Test KMS key",
                            CONNECT_STATUS: False,
                            CONNECT_DETAIL: "KMS key: %s has incorrect purpose: %s, expected key purpose is: %s"
                            % (
                                key.name,
                                key.purpose.name,
                                kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT.name,
                            ),
                        }
                    )
            except Exception:
                self._log(traceback.format_exc(), detail=VVERBOSE)
                results.append(
                    {
                        CONNECT_TEST: "Test KMS key",
                        CONNECT_STATUS: False,
                        CONNECT_DETAIL: "Unable to obtain KMS key details\nPlease verify that the Cloud KMS API is enabled, the key details are correct and the service account has the required permissions",
                    }
                )

        try:
            user = self.get_user_name()
            results.append(
                {
                    CONNECT_TEST: "Test backend user",
                    CONNECT_STATUS: True,
                    CONNECT_DETAIL: "Backend User: %s" % user,
                }
            )
        except Exception:
            self._log(traceback.format_exc(), detail=VVERBOSE)
            results.append(
                {
                    CONNECT_TEST: "Test backend user",
                    CONNECT_STATUS: False,
                    CONNECT_DETAIL: "Unable to obtain backend user\nPlease verify that the BigQuery API is enabled",
                }
            )

        return results

    def close(self):
        if self._client:
            self._client.close()

    def compute_stats(
        self,
        db_name,
        table_name,
        incremental=None,
        for_columns=False,
        partition_tuples=None,
        sync=None,
    ):
        raise NotImplementedError("Compute statistics does not apply for BigQuery")

    def create_database(
        self, db_name, comment=None, properties=None, with_terminator=False
    ):
        """Create a BigQuery dataset using SQL.

        properties: Allows properties["location"] to specify a BigQuery location, e.g. "us-west"
        """
        assert db_name
        if properties:
            assert isinstance(properties, dict)
        if comment:
            assert isinstance(comment, str)

        if self.database_exists(db_name):
            self._log(
                "Dataset already exists, not attempting to create: %s" % db_name,
                detail=VVERBOSE,
            )
            return []
        sql = "CREATE SCHEMA {}".format(
            self.enclose_identifier(self._bq_dataset_id(db_name))
        )
        options = []
        if comment:
            options.append(f"description='{comment}'")
        if properties and properties.get("location"):
            options.append("location='{}'".format(properties["location"]))
        if options:
            sql += " OPTIONS({})".format(",".join(options))
        if with_terminator:
            sql += ";"
        cmds = self.execute_ddl(sql)
        return cmds

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
        """Create a BigQuery table.

        sort_column_names: Only applicable for partitioned tables
        storage_format: Only used for external table otherwise FILE_STORAGE_FORMAT_BIGTABLE
        location: Only used for external table
        without_db_name: Ignored for BigQuery
        See abstract method for more description
        """
        if external:
            return self._create_external_table(
                db_name,
                table_name,
                storage_format,
                location=location,
                with_terminator=with_terminator,
            )

        # Normal (non-external) table
        sql = self._gen_bigquery_create_table_sql_text(
            db_name,
            table_name,
            column_list,
            partition_column_names,
            table_properties=table_properties,
            sort_column_names=sort_column_names,
        )
        if with_terminator:
            sql += ";"
        cmds = self.execute_ddl(sql, sync=sync)

        # Check table was created with KMS encryption if requested
        if self._kms_key_name and not self._dry_run:
            table_id = "{}.{}.{}".format(
                self._backend_project_name(), db_name, table_name
            )
            table = self._client.get_table(table_id)
            self._check_kms_key_name(
                table.encryption_configuration.kms_key_name, key_type="table"
            )
        return cmds

    def create_view(
        self,
        db_name,
        view_name,
        column_tuples,
        ansi_joined_tables,
        filter_clauses=None,
        sync=None,
    ):
        """Create a BigQuery view
        See create_view() description for parameter descriptions.
        """
        projection = self._format_select_projection(column_tuples)
        where_clause = (
            "\nWHERE  " + "\nAND    ".join(filter_clauses) if filter_clauses else ""
        )
        sql = """CREATE VIEW %(db_view)s AS
SELECT %(projection)s
FROM   %(from_tables)s%(where_clause)s""" % {
            "db_view": self.enclose_object_reference(db_name, view_name),
            "projection": projection,
            "from_tables": ansi_joined_tables,
            "where_clause": where_clause,
        }
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
        """Create a BigQuery SQL UDF"""

        def format_parameter_tuples(parameter_tuples):
            if not parameter_tuples:
                return ""
            return ",".join("{} {}".format(_[0], _[1]) for _ in parameter_tuples)

        assert db_name
        assert udf_name
        assert udf_body
        if parameter_tuples:
            assert isinstance(parameter_tuples, list)
            assert isinstance(parameter_tuples[0], tuple)

        udf_param_clause = spec_as_string or format_parameter_tuples(parameter_tuples)
        or_replace_clause = " OR REPLACE" if or_replace else ""
        sql = "CREATE%s FUNCTION %s(%s) RETURNS %s AS (%s)" % (
            or_replace_clause,
            self.enclose_object_reference(db_name, udf_name),
            udf_param_clause,
            return_data_type,
            udf_body,
        )
        return self.execute_ddl(sql, sync=sync, log_level=log_level)

    def current_date_sql_expression(self):
        return "CURRENT_DATE()"

    def data_type_accepts_length(self, data_type):
        return False

    def database_exists(self, db_name):
        if not self._client:
            return False

        try:
            dataset = self._client.get_dataset(self._bq_dataset_id(db_name))
            return True
        except NotFound:
            return False

    def db_name_label(self, initcap=False):
        label = "dataset"
        return label.capitalize() if initcap else label

    def default_date_based_partition_granularity(self):
        return PART_COL_GRANULARITY_MONTH

    def default_sort_columns_to_primary_key(self) -> bool:
        """Rule of thumb on BigQuery is to cluster by primary key if no better alternative."""
        return True

    def default_storage_compression(self, user_requested_codec, user_requested_format):
        if user_requested_codec in ["HIGH", "MED", "NONE", None]:
            return None
        return user_requested_codec

    @staticmethod
    def default_storage_format():
        return None

    def derive_native_partition_info(self, db_name, table_name, column, position):
        """Return a ColumnPartitionInfo object (or None) based on native partition settings.
        BigQuery only allows a single partition column so we could, in theory, ignore
        both column and position parameters. We don't do this though and use column as a
        sanity check and position because then we're behaving like other backends.
        """
        assert db_name and table_name
        assert column
        column_name = (
            column.name if isinstance(column, ColumnMetadataInterface) else column
        )
        table = self._get_bq_table(db_name, table_name)

        if not self._table_is_partitioned(db_name, table_name):
            return None

        if (
            self._get_bq_table_partition_type(table)
            in SUPPORTED_BQ_DATE_PARTITIONING_TYPES
        ):
            granularity = self._get_goe_granularity_for_bq_partitioning_type(table)
            # column_name should always be table.time_partitioning.field on BigQuery
            if self._get_bq_table_partition_field(table).upper() != column_name.upper():
                raise BackendApiException(
                    "Partition columns do not match: %s != %s"
                    % (table.time_partitioning.field.upper(), column_name.upper())
                )
            return ColumnPartitionInfo(
                position=position,
                granularity=granularity,
                digits=1,
                source_column_name=column_name,
            )
        elif (
            self._get_bq_table_partition_type(table)
            == SUPPORTED_BQ_RANGE_PARTITIONING_TYPE
        ):
            table_json = table.to_api_repr()
            if table_json.get("rangePartitioning"):
                range_field = table_json["rangePartitioning"]["field"]
                if str(range_field).upper() != str(column_name).upper():
                    raise BackendApiException(
                        "Partition columns do not match: %s != %s"
                        % (range_field.upper(), column_name.upper())
                    )
                try:
                    range_start = int(table_json["rangePartitioning"]["range"]["start"])
                    range_end = int(table_json["rangePartitioning"]["range"]["end"])
                    range_interval = int(
                        table_json["rangePartitioning"]["range"]["interval"]
                    )
                    return ColumnPartitionInfo(
                        position=position,
                        source_column_name=column_name,
                        granularity=range_interval,
                        range_start=range_start,
                        range_end=range_end,
                    )
                except ValueError:
                    self._log(
                        "Incorrectly formatted rangePartitioning document: %s"
                        % str(table_json["rangePartitioning"])
                    )
                    raise

    def detect_column_has_fractional_seconds(self, db_name, table_name, column):
        """BigQuery EXTRACT() only goes to MICROSECOND granularity"""
        assert db_name and table_name
        assert isinstance(column, BigQueryColumn)
        if column.data_type not in [BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_TIMESTAMP]:
            return False
        sql = """SELECT %(col)s
FROM %(db_table)s
WHERE EXTRACT(MICROSECOND FROM %(col)s) != 0
LIMIT 1""" % {
            "db_table": self.enclose_object_reference(db_name, table_name),
            "col": self.enclose_identifier(column.name),
        }
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return True if row else False

    def drop_state(self):
        """Drop any cached state so values are refreshed on next request."""
        self._cached_bq_tables = {}

    def drop_table(self, db_name, table_name, purge=False, if_exists=True, sync=None):
        """Drop a BigQuery table. purge and sync are not applicable."""
        sql = "DROP TABLE %s" % self.enclose_object_reference(db_name, table_name)
        if if_exists:
            try:
                cmds = self.execute_ddl(sql, sync=sync)
            except NotFound:
                self._debug("Ignoring NotFound exception")
                cmds = [sql]
        else:
            cmds = self.execute_ddl(sql, sync=sync)
        self._forget_bq_table(db_name, table_name)
        return cmds

    def drop_view(self, db_name, view_name, if_exists=True, sync=None):
        """Drop a BigQuery view. sync is not applicable."""
        sql = "DROP VIEW %s" % self.enclose_object_reference(db_name, view_name)
        if if_exists:
            try:
                cmds = self.execute_ddl(sql, sync=sync)
            except NotFound:
                self._debug("Ignoring NotFound exception")
                cmds = [sql]
        else:
            cmds = self.execute_ddl(sql, sync=sync)
        self._forget_bq_table(db_name, view_name)
        return cmds

    def enclose_identifier(self, identifier):
        if identifier is None:
            return None
        return backtick_sandwich(identifier, ch=self.enclosure_character())

    def enclose_object_reference(self, db_name, object_name):
        return self.enclose_identifier(self._bq_table_id(db_name, object_name))

    def enclosure_character(self):
        return "`"

    def execute_query_fetch_all(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
        as_dict=False,
    ):
        """For parameter descriptions see: _execute_query_fetch_x"""
        rows = self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_ALL,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            profile=profile,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            as_dict=as_dict,
        )
        return rows

    def execute_query_fetch_one(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
        profile=None,
        as_dict=False,
    ):
        """For parameter descriptions see: _execute_query_fetch_x"""
        row = self._execute_query_fetch_x(
            sql,
            fetch_action=FETCH_ACTION_ONE,
            query_options=query_options,
            log_level=log_level,
            time_sql=time_sql,
            profile=profile,
            not_when_dry_running=not_when_dry_running,
            query_params=query_params,
            as_dict=as_dict,
        )
        return row

    def execute_query_get_cursor(
        self,
        sql,
        query_options=None,
        log_level=None,
        time_sql=False,
        not_when_dry_running=False,
        query_params=None,
    ):
        """execute_query_get_cursor() exposes too lower a level of detail to higher level code.
        Strongly advise not to implement/use this method
        """
        raise NotImplementedError(
            "execute_query_get_cursor() not supported for BigQuery"
        )

    def exists(self, db_name, object_name):
        return self._object_exists(db_name, object_name)

    def format_query_parameter(self, param_name):
        assert param_name
        return "@{}".format(param_name)

    def gen_column_object(self, column_name, **kwargs):
        return BigQueryColumn(column_name, **kwargs)

    def gen_copy_into_sql_text(
        self,
        db_name,
        table_name,
        from_object_clause,
        select_expr_tuples,
        filter_clauses=None,
    ):
        raise NotImplementedError("COPY INTO does not exist in BigQuery SQL")

    def gen_default_numeric_column(self, column_name, data_scale=None):
        """Return a default best-fit numeric column for generic usage.
        Used when we can't really know or influence a type, e.g. AVG() output in AAPD.
        data_scale=0 can be used if the outcome is known to be integral.
        On BigQuery data_scale=0 means use BIGQUERY_TYPE_INT64, otherwise data_scale influences whether
        we use NUMERIC or BIGNUMERIC.
        """
        if data_scale == 0:
            return BigQueryColumn(
                column_name,
                data_type=BIGQUERY_TYPE_INT64,
                nullable=True,
                safe_mapping=False,
            )
        elif data_scale and data_scale <= 9:
            return BigQueryColumn(
                column_name,
                data_type=BIGQUERY_TYPE_NUMERIC,
                data_precision=38,
                data_scale=9,
                nullable=True,
                safe_mapping=False,
            )
        else:
            return BigQueryColumn(
                column_name,
                data_type=BIGQUERY_TYPE_BIGNUMERIC,
                data_precision=self.max_decimal_precision(),
                data_scale=self.max_decimal_scale(),
                nullable=True,
                safe_mapping=False,
            )

    def gen_ctas_sql_text(
        self,
        db_name,
        table_name,
        storage_format,
        column_tuples,
        from_db_name=None,
        from_table_name=None,
        row_limit=None,
        external=False,
        table_properties=None,
    ):
        """See interface description for parameter descriptions
        Ignoring storage_format on BigQuery
        """
        assert db_name and table_name
        assert column_tuples
        assert isinstance(column_tuples, list), "%s is not list" % type(column_tuples)
        assert isinstance(column_tuples[0], (tuple, list))

        projection = self._format_select_projection(column_tuples)
        from_clause = (
            "\nFROM   {}".format(
                self.enclose_object_reference(from_db_name, from_table_name)
            )
            if from_db_name and from_table_name
            else ""
        )
        limit_clause = "\nLIMIT  {}".format(row_limit) if row_limit is not None else ""

        table_prop_clause = self._create_table_properties_clause(table_properties)

        sql = (
            dedent(
                """\
            CREATE TABLE %(db_table)s%(table_prop_clause)s
            AS
            SELECT %(projection)s%(from_clause)s%(limit_clause)s"""
            )
            % {
                "db_table": self.enclose_object_reference(db_name, table_name),
                "table_prop_clause": table_prop_clause,
                "projection": projection,
                "from_clause": from_clause,
                "limit_clause": limit_clause,
            }
        )
        return sql

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
        """Format INSERT from SELECT statement for BigQuery
        Ignores insert_hint, sort_expr_list and distribute_columns
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

        projected_expressions = select_expr_tuples + (partition_expr_tuples or [])
        projection = self._format_select_projection(projected_expressions)
        from_db_table = from_object_override or self.enclose_object_reference(
            from_db_name, from_table_name
        )

        where_clause = ""
        if filter_clauses:
            where_clause = "\nWHERE  " + "\nAND    ".join(filter_clauses)

        insert_sql = """INSERT INTO %(db_table)s
SELECT %(proj)s
FROM   %(from_db_table)s%(where)s""" % {
            "db_table": self.enclose_object_reference(db_name, table_name),
            "proj": projection,
            "from_db_table": from_db_table,
            "where": where_clause,
        }
        return insert_sql

    def gen_native_range_partition_key_cast(self, partition_column):
        """Generates the CAST to be used to appropriately range partition a table"""
        if partition_column.data_type in [
            BIGQUERY_TYPE_DATE,
            BIGQUERY_TYPE_DATETIME,
            BIGQUERY_TYPE_TIMESTAMP,
        ]:
            if partition_column.partition_info.granularity == PART_COL_GRANULARITY_YEAR:
                part_col_expr = "%s_TRUNC(%s,YEAR)" % (
                    partition_column.data_type.upper(),
                    self.enclose_identifier(partition_column.name),
                )
            elif (
                partition_column.partition_info.granularity
                == PART_COL_GRANULARITY_MONTH
            ):
                part_col_expr = "%s_TRUNC(%s,MONTH)" % (
                    partition_column.data_type.upper(),
                    self.enclose_identifier(partition_column.name),
                )
            elif (
                partition_column.partition_info.granularity == PART_COL_GRANULARITY_DAY
            ):
                if partition_column.data_type == BIGQUERY_TYPE_DATE:
                    part_col_expr = self.enclose_identifier(partition_column.name)
                else:
                    part_col_expr = "%s_TRUNC(%s,DAY)" % (
                        partition_column.data_type.upper(),
                        self.enclose_identifier(partition_column.name),
                    )
            else:
                raise NotImplementedError(
                    "BigQuery table partitioning not implemented for data type/granularity: %s/%s"
                    % (
                        partition_column.data_type,
                        partition_column.partition_info.granularity,
                    )
                )
        elif partition_column.data_type == BIGQUERY_TYPE_INT64:
            if not partition_column.partition_info:
                raise BackendApiException(
                    "Partition information not provided for column: %s"
                    % partition_column.name
                )
            part_col_expr = "RANGE_BUCKET(%s, GENERATE_ARRAY(%s, %s, %s))" % (
                self.enclose_identifier(partition_column.name),
                partition_column.partition_info.range_start,
                partition_column.partition_info.range_end,
                partition_column.partition_info.granularity,
            )
        else:
            raise NotImplementedError(
                "BigQuery table partitioning not implemented for data type:"
                "%s" % partition_column.data_type
            )
        return part_col_expr

    def gen_sql_text(
        self,
        db_name,
        table_name,
        column_names=None,
        filter_clauses=None,
        measures=None,
        agg_fns=None,
    ):
        return self._gen_sql_text_common(
            db_name,
            table_name,
            column_names=column_names,
            filter_clauses=filter_clauses,
            measures=measures,
            agg_fns=agg_fns,
        )

    def generic_string_data_type(self):
        return BIGQUERY_TYPE_STRING

    def get_columns(self, db_name, table_name):
        assert db_name and table_name
        table = self._get_bq_table(db_name, table_name)
        backend_columns = []
        for col in table.schema:
            # Using str() on attributes from BigQuery below because in Python2 unicode() is preventing comparisons
            backend_columns.append(
                BigQueryColumn(
                    str(col.name),
                    data_type=str(col.field_type),
                    nullable=str(col.is_nullable),
                )
            )
        return backend_columns

    def get_distinct_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        order_results=False,
        not_when_dry_running=False,
    ):
        """Run SQL to get distinct values for a list of columns.
        See interface description for parameter details.
        """

        def add_sql_cast(col):
            return (
                ("CAST(%s as STRING)" % col)
                if col in (columns_to_cast_to_string or [])
                else col
            )

        assert column_name_list and isinstance(column_name_list, list)
        expression_list = [
            add_sql_cast(self.enclose_identifier(_)) for _ in column_name_list
        ]
        return self.get_distinct_expressions(
            db_name,
            table_name,
            expression_list,
            order_results=order_results,
            not_when_dry_running=not_when_dry_running,
        )

    def get_job_details(self, job_id):
        """BigQuery specific method.
        Return job details for a given job id.
        """
        assert job_id
        return self._get_bq_job(job_id)

    def get_max_column_values(
        self,
        db_name,
        table_name,
        column_name_list,
        columns_to_cast_to_string=None,
        optimistic_prune_clause=None,
        not_when_dry_running=False,
    ):
        """Run SQL to get max value for a set of columns.
        See interface description for parameter details.
        """
        return self._get_max_column_values_common(
            db_name,
            table_name,
            column_name_list,
            columns_to_cast_to_string=columns_to_cast_to_string,
            optimistic_prune_clause=optimistic_prune_clause,
            not_when_dry_running=not_when_dry_running,
        )

    def get_partition_columns(self, db_name, table_name):
        """Gets a list of column objects for partition column.
        In BigQuery there can only be a single column in the list.
        As of bigquery client version 1.24.0 integer range partitioning
        is not visible, therefore we're falling back on the raw JSON until
        the partition scheme moves past beta and presumably becomes visible.
        """
        assert db_name and table_name
        table = self._get_bq_table(db_name, table_name)
        if self._get_bq_table_partition_type(table) in (
            SUPPORTED_BQ_DATE_PARTITIONING_TYPES
            + [SUPPORTED_BQ_RANGE_PARTITIONING_TYPE]
        ):
            part_col = match_table_column(
                self._get_bq_table_partition_field(table),
                self.get_columns(db_name, table_name),
            )
        else:
            # Not partitioned
            return None
        # BigQuery only supports a single partition column but GOE supports a list, therefore return a list:
        return [part_col]

    def get_session_option(self, option_name):
        """Not applicable to BigQuery, returns None."""
        return None

    def get_table_ddl(
        self, db_name, table_name, as_list=False, terminate_sql=False, for_replace=False
    ):
        ddl_str = self._get_table_ddl(db_name, table_name, terminate_sql=terminate_sql)
        if for_replace:
            ddl_str = re.sub(r"^CREATE TABLE", "CREATE OR REPLACE TABLE", ddl_str, re.I)
        self._debug("Table DDL: %s" % ddl_str)
        if as_list:
            return ddl_str.split("\n")
        else:
            return ddl_str

    def get_table_location(self, db_name, table_name):
        """get_table_location not applicable on BigQuery"""
        if self._is_external_table(db_name, table_name):
            table = self._get_bq_table(db_name, table_name)
            return (
                table.external_data_configuration.source_uris[0]
                if table.external_data_configuration.source_uris
                else None
            )
        else:
            return None

    def get_table_partition_count(self, db_name, table_name):
        """Get partition count from __PARTITIONS_SUMMARY__ using legacy SQL.
        TODO It is possible to get this information from INFORMATION_SCHEMA, not clear why we used legacy SQL.
        """
        assert db_name and table_name
        if not self._table_is_partitioned(db_name, table_name):
            return 0
        query_options = {"use_legacy_sql": True}
        sql = "SELECT COUNT(*) FROM [%s$__PARTITIONS_SUMMARY__]" % self._bq_table_id(
            db_name, table_name
        )
        row = self.execute_query_fetch_one(
            sql, query_options=query_options, log_level=VVERBOSE
        )
        return row[0] if row else None

    def get_table_partitions(self, db_name, table_name):
        """Get partition info from __PARTITIONS_SUMMARY__ using legacy SQL.
        TODO It is possible to get this information from INFORMATION_SCHEMA, not clear why we used legacy SQL.
        """
        if not self._table_is_partitioned(db_name, table_name):
            return None
        query_options = {"use_legacy_sql": True}
        sql = (
            "SELECT partition_id FROM [%s$__PARTITIONS_SUMMARY__]"
            % self._bq_table_id(db_name, table_name)
        )
        rows = self.execute_query_fetch_all(
            sql, query_options=query_options, log_level=VVERBOSE
        )
        part_list = {
            str(_[0]): self._table_partition_info(partition_id=_[0]) for _ in rows
        }
        return part_list

    def get_table_row_count(
        self,
        db_name,
        table_name,
        filter_clause=None,
        not_when_dry_running=False,
        log_level=VVERBOSE,
    ):
        if self._dry_run and not_when_dry_running:
            return None
        if (
            filter_clause
            or self.is_view(db_name, table_name)
            or self._is_external_table(db_name, table_name)
            or self._has_outstanding_streaming_inserts(db_name, table_name)
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
        else:
            # Forget the table before getting a count because metadata is cached
            self._forget_bq_table(db_name, table_name)
            self._log_or_not(
                "%s call: %s.num_rows"
                % (self._sql_engine_name, self._bq_table_id(db_name, table_name)),
                log_level=log_level,
            )
            return self.get_table_row_count_from_metadata(db_name, table_name)

    def get_table_row_count_from_metadata(self, db_name, table_name):
        assert db_name and table_name
        self._debug("Fetching table row count from metadata")
        table = self._get_bq_table(db_name, table_name)
        return table.num_rows

    def get_table_size(self, db_name, table_name, no_cache=False):
        assert db_name and table_name
        if no_cache:
            self._forget_bq_table(db_name, table_name)
        table = self._get_bq_table(db_name, table_name)
        return table.num_bytes

    def get_table_size_and_row_count(self, db_name, table_name):
        assert db_name and table_name
        table = self._get_bq_table(db_name, table_name)
        return table.num_bytes, table.num_rows

    def get_table_sort_columns(self, db_name, table_name, as_csv=True):
        assert db_name and table_name
        table = self._get_bq_table(db_name, table_name)
        if table.clustering_fields:
            assert isinstance(table.clustering_fields, list)
            assert isinstance(table.clustering_fields[0], str)
            return (
                ",".join(table.clustering_fields) if as_csv else table.clustering_fields
            )
        else:
            return []

    def get_table_stats(self, db_name, table_name, as_dict=False):
        assert db_name and table_name

        if self.table_exists(db_name, table_name):
            table = self._get_bq_table(db_name, table_name)
            tab_stats = {
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "avg_row_len": 0,
            }
        else:
            tab_stats = EMPTY_BACKEND_TABLE_STATS_DICT

        if as_dict:
            return tab_stats, EMPTY_BACKEND_COLUMN_STATS_DICT
        else:
            stats_tuple = (
                tab_stats["num_rows"],
                tab_stats["num_bytes"],
                tab_stats["avg_row_len"],
            )
            return stats_tuple, EMPTY_BACKEND_COLUMN_STATS_LIST

    def get_table_and_partition_stats(self, db_name, table_name, as_dict=False):
        tab_stats, _ = self.get_table_stats(db_name, table_name, as_dict=as_dict)
        if as_dict:
            return tab_stats, {}, EMPTY_BACKEND_COLUMN_STATS_DICT
        else:
            return tab_stats, [], EMPTY_BACKEND_COLUMN_STATS_LIST

    def get_table_stats_partitions(self, db_name, table_name):
        raise NotImplementedError("Get statistics does not apply for BigQuery")

    def get_user_name(self):
        row = self.execute_query_fetch_one("SELECT SESSION_USER()", log_level=None)
        return row[0] if row else None

    def insert_literal_values(
        self,
        db_name,
        table_name,
        literal_list,
        column_list=None,
        max_rows_per_insert=250,
        split_by_cr=True,
    ):
        """Insert an array of literals into a table using INSERT...VALUES(...),(...),(...).
        We've done this for simplicity due to two issues:
        1) BigQuery issue: https://github.com/googleapis/google-cloud-python/issues/5539
            "The issue is eventual consistency with the backend. Replacing the table, while it has the
             same table_id, represents a new table in terms of it's internal UUID and thus backends may
             deliver to the "old" table for a short period (typically a few minutes)."
           This means we can have problems with this method of inserting rows, it is likely only suitable for testing.
        2) Python does not support encoding Decimal to JSON until Python 3.10:
            https://bugs.python.org/issue16535
           Which gives us issue with this API:
            site-packages/google/cloud/bigquery/client.py", line 3421, in insert_rows
                return self.insert_rows_json(table, json_rows, **kwargs)
                ...
            json/encoder.py", line 179, in default
                raise TypeError(f'Object of type {o.__class__.__name__} '
            TypeError: Object of type Decimal is not JSON serializable
        """

        def gen_literal(py_val, data_type):
            return str(self.to_backend_literal(py_val, data_type))

        assert db_name
        assert table_name
        assert literal_list and isinstance(literal_list, list)
        assert isinstance(literal_list[0], list)

        column_list = column_list or self.get_columns(db_name, table_name)
        column_names = [_.name for _ in column_list]
        data_type_strs = [_.format_data_type() for _ in column_list]

        cmds = []
        remaining_rows = literal_list[:]
        while remaining_rows:
            this_chunk = remaining_rows[:max_rows_per_insert]
            remaining_rows = remaining_rows[max_rows_per_insert:]
            formatted_rows = []
            for row in this_chunk:
                formatted_rows.append(
                    ",".join(
                        gen_literal(py_val, data_type)
                        for py_val, data_type in zip(row, data_type_strs)
                    )
                )
            sql = self._insert_literals_using_insert_values_sql_text(
                db_name,
                table_name,
                column_names,
                formatted_rows,
                split_by_cr=split_by_cr,
            )
            cmds.extend(self.execute_dml(sql, log_level=VVERBOSE))
        return cmds

    def is_nan_sql_expression(self, column_expr):
        """is_nan for BigQuery"""
        return "is_nan(%s)" % column_expr

    def is_valid_partitioning_data_type(self, data_type):
        if not data_type:
            return False
        return bool(
            data_type.upper()
            in [
                BIGQUERY_TYPE_DATE,
                BIGQUERY_TYPE_DATETIME,
                BIGQUERY_TYPE_INT64,
                BIGQUERY_TYPE_BIGNUMERIC,
                BIGQUERY_TYPE_NUMERIC,
                BIGQUERY_TYPE_TIMESTAMP,
            ]
        )

    def is_valid_sort_data_type(self, data_type):
        # As of 2021-01-29 DATETIME is supported for clustering
        return True

    def is_valid_storage_compression(self, user_requested_codec, user_requested_format):
        return bool(user_requested_codec in ["NONE", None])

    def is_valid_storage_format(self, storage_format):
        return bool(storage_format in [FILE_STORAGE_FORMAT_BIGTABLE, None])

    def is_view(self, db_name, object_name):
        assert db_name and object_name
        return self._object_exists(db_name, object_name, table_type="VIEW")

    def kms_key_name(self):
        if self._kms_client:
            return self._kms_client.crypto_key_path(
                self._google_kms_key_ring_project or self._backend_project_name(),
                self._google_kms_key_ring_location,
                self._google_kms_key_ring_name,
                self._google_kms_key_name,
            )
        else:
            return None

    def length_sql_expression(self, column_expression):
        assert column_expression
        return "LENGTH(%s)" % column_expression

    def list_databases(self, db_name_filter=None, case_sensitive=True):
        self._debug("list_databases(%s)" % db_name_filter)
        datasets = list(self._client.list_datasets())
        if not datasets:
            return []
        if db_name_filter:
            ds_filter = "^" + db_name_filter.replace("*", ".*") + "$"
            self._debug("list_databases re filter: %s" % ds_filter)
            if case_sensitive:
                ds_filter_re = re.compile(ds_filter)
            else:
                ds_filter_re = re.compile(ds_filter, re.I)
            return [_.dataset_id for _ in datasets if ds_filter_re.search(_.dataset_id)]
        else:
            return [_.dataset_id for _ in datasets]

    def list_tables(self, db_name, table_name_filter=None, case_sensitive=True):
        assert db_name
        self._debug("list_tables(%s)" % table_name_filter)
        try:
            # Use list() below to consume all rows straight away
            tables = list(self._client.list_tables(self._bq_dataset_id(db_name)))
        except NotFound as exc:
            self._log("NotFound exception: {}".format(str(exc)))
            raise BackendApiException(
                "BigQuery dataset not found: %s" % db_name
            ) from None
        if table_name_filter:
            tbl_filter = r"^" + table_name_filter.replace("*", ".*") + r"$"
            self._debug("list_tables re filter: %s" % tbl_filter)
            if case_sensitive:
                tbl_filter_re = re.compile(tbl_filter)
            else:
                tbl_filter_re = re.compile(tbl_filter, re.I)

            return [_.table_id for _ in tables if tbl_filter_re.search(_.table_id)]
        else:
            return [_.table_id for _ in tables]

    def list_udfs(self, db_name, udf_name_filter=None, case_sensitive=True):
        """List UDFs using INFORMATION_SCHEMA.
        Python client has a list_routines() method but it is beta and arguments attribute is None even
        when the UDF has a return argument/input parameter.
        """
        assert db_name
        self._debug("list_udfs(%s)" % udf_name_filter)
        sql = (
            dedent(
                """\
            SELECT routine_name,data_type
            FROM   `%s`.INFORMATION_SCHEMA.ROUTINES
            WHERE  routine_catalog = @project
            AND    routine_schema = @db_name"""
            )
            % self._bq_dataset_id(db_name)
        )
        query_params = [
            ("project", BIGQUERY_TYPE_STRING, self._backend_project_name()),
            ("db_name", BIGQUERY_TYPE_STRING, db_name),
        ]
        if udf_name_filter:
            query_params.append(("udf_name", BIGQUERY_TYPE_STRING, udf_name_filter))
            if case_sensitive:
                sql += "\nAND    routine_name LIKE @udf_name"
            else:
                sql += "\nAND    UPPER(routine_name) LIKE UPPER(@udf_name)"
        return self.execute_query_fetch_all(
            sql, query_params=query_params, log_level=VVERBOSE
        )

    def list_views(self, db_name, view_name_filter=None, case_sensitive=True):
        sql = (
            dedent(
                """\
            SELECT table_name
            FROM   `%s`.INFORMATION_SCHEMA.VIEWS
            WHERE  table_catalog = @project
            AND    table_schema = @db_name"""
            )
            % self._bq_dataset_id(db_name)
        )
        query_params = [
            ("project", BIGQUERY_TYPE_STRING, self._backend_project_name()),
            ("db_name", BIGQUERY_TYPE_STRING, db_name),
        ]
        rows = self.execute_query_fetch_all(
            sql, log_level=VVERBOSE, query_params=query_params
        )
        if not rows:
            return []
        tables = [_[0] for _ in rows]
        if view_name_filter:
            vw_filter = r"^" + view_name_filter.replace("*", ".*") + r"$"
            self._debug("list_tables re filter: %s" % vw_filter)
            if case_sensitive:
                vw_filter_re = re.compile(vw_filter)
            else:
                vw_filter_re = re.compile(vw_filter, re.I)

            return [_ for _ in tables if vw_filter_re.search(_)]
        else:
            return tables

    def max_column_name_length(self):
        return 300

    def max_decimal_integral_magnitude(self):
        return 38

    def max_decimal_precision(self):
        return 76

    def max_decimal_scale(self, data_type=None):
        if data_type == BIGQUERY_TYPE_NUMERIC:
            return 9
        else:
            return 38

    def max_datetime_scale(self):
        """BigQuery supports microseconds"""
        return 6

    def max_datetime_value(self):
        return datetime64("9999-12-31T23:59:59")

    def max_partition_columns(self):
        return 1

    def max_sort_columns(self):
        return 4

    def max_table_name_length(self):
        return 1024

    def min_datetime_value(self):
        return datetime64("0001-01-01")

    def native_integer_types(self):
        return [BIGQUERY_TYPE_INT64]

    def partition_column_requires_synthetic_column(self, backend_column, granularity):
        # There's no native NUMERIC partition so it requires a synthetic column.
        # It should be noted that NUMERIC can hold up to 29 integral digits which exceeds INT64 at 18.
        return bool(
            backend_column.data_type
            in [BIGQUERY_TYPE_BIGNUMERIC, BIGQUERY_TYPE_NUMERIC]
        )

    def partition_range_max(self):
        """Maximum value for INT64
        From https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_type
        INT64	-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
        """
        return (2**63) - 1

    def partition_range_min(self):
        """Minimum value for INT64
        From https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_type
        INT64	-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
        """
        return -(2**63)

    def populate_sequence_table(
        self, db_name, table_name, starting_seq, target_seq, split_by_cr=False
    ):
        raise NotImplementedError("Sequence table does not apply for BigQuery")

    def refresh_table_files(self, db_name, table_name, sync=None):
        """No requirement to re-scan files for a table on BigQuery but drop from cache because that will be stale"""
        self.drop_state()

    def regexp_extract_sql_expression(self, subject, pattern):
        return "REGEXP_EXTRACT(%s, r'%s')" % (subject, pattern)

    def rename_table(
        self, from_db_name, from_table_name, to_db_name, to_table_name, sync=None
    ):
        """Rename a BigQuery table, implemented in native BQ SQL"""
        assert from_db_name and from_table_name
        assert to_db_name and to_table_name

        if not self._dry_run and not self.table_exists(from_db_name, from_table_name):
            raise BackendApiException(
                "Source table does not exist, cannot rename table: %s.%s"
                % (from_db_name, from_table_name)
            )

        if not self._dry_run and self.exists(to_db_name, to_table_name):
            raise BackendApiException(
                "Target table already exists, cannot rename table to: %s.%s"
                % (to_db_name, to_table_name)
            )

        sql = "ALTER TABLE IF EXISTS %s RENAME TO %s" % (
            self.enclose_object_reference(from_db_name, from_table_name),
            self.enclose_identifier(to_table_name),
        )

        return self.execute_ddl(sql, sync=sync)

    def role_exists(self, role_name):
        """No roles in BigQuery"""
        pass

    def sequence_table_max(self, db_name, table_name):
        raise NotImplementedError("Sequence table does not apply for BigQuery")

    def set_column_stats(
        self, db_name, table_name, new_column_stats, ndv_cap, num_null_factor
    ):
        raise NotImplementedError("Set statistics does not apply for BigQuery")

    def set_partition_stats(
        self, db_name, table_name, new_partition_stats, additive_stats
    ):
        raise NotImplementedError("Set statistics does not apply for BigQuery")

    def set_session_db(self, db_name, log_level=VERBOSE):
        raise NotImplementedError(
            "Set session dataset has not been implemented for BigQuery"
        )

    def set_table_stats(self, db_name, table_name, new_table_stats, additive_stats):
        raise NotImplementedError("Set statistics does not apply for BigQuery")

    def supported_backend_data_types(self):
        return [
            BIGQUERY_TYPE_BIGNUMERIC,
            BIGQUERY_TYPE_BOOLEAN,
            BIGQUERY_TYPE_BYTES,
            BIGQUERY_TYPE_DATE,
            BIGQUERY_TYPE_DATETIME,
            BIGQUERY_TYPE_FLOAT64,
            BIGQUERY_TYPE_INT64,
            BIGQUERY_TYPE_NUMERIC,
            BIGQUERY_TYPE_STRING,
            BIGQUERY_TYPE_TIME,
            BIGQUERY_TYPE_TIMESTAMP,
        ]

    def supported_date_based_partition_granularities(self):
        return [
            PART_COL_GRANULARITY_DAY,
            PART_COL_GRANULARITY_MONTH,
            PART_COL_GRANULARITY_YEAR,
        ]

    def supported_partition_function_parameter_data_types(self):
        return [
            BIGQUERY_TYPE_BIGNUMERIC,
            BIGQUERY_TYPE_INT64,
            BIGQUERY_TYPE_NUMERIC,
            BIGQUERY_TYPE_STRING,
        ]

    def supported_partition_function_return_data_types(self):
        return [BIGQUERY_TYPE_INT64]

    def synthetic_partition_numbers_are_string(self):
        return False

    def table_distribution(self, db_name, table_name):
        return None

    def table_exists(self, db_name: str, table_name: str) -> bool:
        return self._object_exists(db_name, table_name, table_type="TABLE")

    def table_has_rows(self, db_name: str, table_name: str) -> bool:
        """Return bool depending whether the table has rows or not."""
        sql = f"SELECT 1 FROM {self.enclose_object_reference(db_name, table_name)} LIMIT 1"
        row = self.execute_query_fetch_one(sql, log_level=VVERBOSE)
        return bool(row)

    def target_version(self):
        """No version available via SQL or API for BigQuery"""
        return None

    def to_backend_literal(self, py_val, data_type=None):
        """Translate a Python value to a BigQuery literal
        See link below for more detail:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#literals
        """
        self._debug("Formatting %s literal: %s" % (type(py_val), repr(py_val)))
        self._debug("For backend datatype: %s" % data_type)
        new_py_val = BigQueryLiteral.format_literal(py_val, data_type=data_type)
        self._debug("Final %s literal: %s" % (type(py_val), str(new_py_val)))
        return new_py_val

    def transform_encrypt_data_type(self):
        return BIGQUERY_TYPE_STRING

    def transform_null_cast(self, backend_column):
        assert isinstance(backend_column, BigQueryColumn)
        return "CAST(NULL AS %s)" % (backend_column.format_data_type())

    def transform_tokenize_data_type(self):
        return BIGQUERY_TYPE_STRING

    def transform_regexp_replace_expression(
        self, backend_column, regexp_replace_pattern, regexp_replace_string
    ):
        return "REGEXP_REPLACE(%s, %s, %s)" % (
            self.enclose_identifier(backend_column.name),
            regexp_replace_pattern,
            regexp_replace_string,
        )

    def transform_translate_expression(self, backend_column, from_string, to_string):
        """BigQuery does not have an equivalent to TRANSLATE() so this method remains unimplemented.
        This is only used in transformation code which is a hidden feature. We can worry about
        this at some point in the future - perhaps never.
        """
        raise NotImplementedError("Translation function is not supported on BigQuery")

    def udf_details(self, db_name, udf_name):
        """Get details of a BigQuery UDF"""
        sql = (
            dedent(
                """\
                SELECT ordinal_position, parameter_name, data_type
                FROM   `%s`.INFORMATION_SCHEMA.PARAMETERS
                WHERE  specific_catalog = @project
                AND    specific_schema = @db_name
                AND    specific_name = @udf_name
                ORDER BY ordinal_position"""
            )
            % self._bq_dataset_id(db_name)
        )
        query_params = [
            ("project", BIGQUERY_TYPE_STRING, self._backend_project_name()),
            ("db_name", BIGQUERY_TYPE_STRING, db_name),
            ("udf_name", BIGQUERY_TYPE_STRING, udf_name),
        ]
        try:
            rows = self.execute_query_fetch_all(
                sql, log_level=VVERBOSE, query_params=query_params
            )
        except NotFound as exc:
            self._log("NotFound exception: {}".format(str(exc)))
            raise BackendApiException(
                "BigQuery dataset not found: %s" % self._bq_dataset_id(db_name)
            ) from None
        if not rows:
            # Most of the time we expect the UDF to exist and suit GOE so check parameters first, only go for an
            # extra round trip if things aren't looking good.
            if not self.udf_exists(db_name, udf_name):
                return []
        parameters = []
        return_type = None
        for row in rows:
            if row[0] == 0:
                # ordinal_position 0 is the return data type
                return_type = row[2]
            else:
                parameters.append(UdfParameter(row[1], row[2]))
        return [UdfDetails(db_name, udf_name, return_type, parameters)]

    def udf_exists(self, db_name, udf_name):
        try:
            return bool(self.list_udfs(db_name, udf_name))
        except NotFound:
            return False

    def udf_installation_os(self, user_udf_version):
        raise NotImplementedError("GOE UDFs are not supported on BigQuery")

    def udf_installation_sql(self, create_udf_db, udf_db=None):
        raise NotImplementedError("GOE UDFs are not supported on BigQuery")

    def udf_installation_test(self, udf_db=None):
        raise NotImplementedError("GOE UDFs are not supported on BigQuery")

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, BigQueryColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.is_number_based():
            if column.data_type == BIGQUERY_TYPE_FLOAT64:
                return bool(target_type in [GOE_TYPE_DECIMAL, GOE_TYPE_DOUBLE])
            else:
                return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based():
            return bool(target_type in DATE_CANONICAL_TYPES)
        elif column.is_string_based():
            return bool(
                target_type in STRING_CANONICAL_TYPES
                or target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY]
                or target_type in [GOE_TYPE_INTERVAL_DS, GOE_TYPE_INTERVAL_YM]
            )
        elif target_type not in ALL_CANONICAL_TYPES:
            self._log(
                "Unknown canonical type in mapping: %s" % target_type, detail=VVERBOSE
            )
            return False
        elif column.data_type not in self.supported_backend_data_types():
            return False
        elif column.data_type == BIGQUERY_TYPE_BOOLEAN:
            return bool(target_type == GOE_TYPE_BOOLEAN)
        elif column.data_type == BIGQUERY_TYPE_BYTES:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        elif column.data_type == BIGQUERY_TYPE_TIME:
            return bool(target_type == GOE_TYPE_TIME)
        return False

    def valid_staging_formats(self):
        return [FILE_STORAGE_FORMAT_AVRO, FILE_STORAGE_FORMAT_PARQUET]

    def view_exists(self, db_name, view_name):
        return self._object_exists(db_name, view_name, table_type="VIEW")

    def to_canonical_column(self, column: BigQueryColumn) -> CanonicalColumn:
        """Translate a BigQuery column to an internal GOE column"""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name..."""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return CanonicalColumn(
                name=col.name,
                data_type=data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                char_length=data_length,
                char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
            )

        assert column
        assert isinstance(column, BigQueryColumn)

        if column.data_type == BIGQUERY_TYPE_BOOLEAN:
            return new_column(column, GOE_TYPE_BOOLEAN)
        elif column.data_type == BIGQUERY_TYPE_STRING:
            return new_column(
                column, GOE_TYPE_VARIABLE_STRING, data_length=column.data_length
            )
        elif column.data_type == BIGQUERY_TYPE_BYTES:
            return new_column(column, GOE_TYPE_BINARY)
        elif column.data_type == BIGQUERY_TYPE_INT64:
            return new_column(column, GOE_TYPE_INTEGER_8)
        elif column.data_type == BIGQUERY_TYPE_NUMERIC:
            data_precision = (
                column.data_precision if column.data_precision is not None else 38
            )
            data_scale = column.data_scale if column.data_scale is not None else 9
            return new_column(
                column,
                GOE_TYPE_DECIMAL,
                data_precision=data_precision,
                data_scale=data_scale,
            )
        elif column.data_type == BIGQUERY_TYPE_BIGNUMERIC:
            if column.data_precision is not None:
                data_precision = column.data_precision
            else:
                data_precision = self.max_decimal_precision()
            data_scale = (
                column.data_scale
                if column.data_scale is not None
                else self.max_decimal_scale()
            )
            return new_column(
                column,
                GOE_TYPE_DECIMAL,
                data_precision=data_precision,
                data_scale=data_scale,
            )
        elif column.data_type == BIGQUERY_TYPE_FLOAT64:
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == BIGQUERY_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE, data_scale=0)
        elif column.data_type == BIGQUERY_TYPE_DATETIME:
            return new_column(
                column, GOE_TYPE_TIMESTAMP, data_scale=self.max_datetime_scale()
            )
        elif column.data_type == BIGQUERY_TYPE_TIME:
            return new_column(column, GOE_TYPE_TIME)
        elif column.data_type == BIGQUERY_TYPE_TIMESTAMP:
            return new_column(
                column, GOE_TYPE_TIMESTAMP_TZ, data_scale=self.max_datetime_scale()
            )
        else:
            raise NotImplementedError(
                "Unsupported backend data type: %s" % column.data_type
            )

    def from_canonical_column(
        self, column: CanonicalColumn, decimal_padding_digits=0
    ) -> BigQueryColumn:
        """Translate an internal GOE column to a BigQuery column."""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            char_length=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable & data_default forward from RDBMS"""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return BigQueryColumn(
                col.name,
                data_type=data_type,
                data_length=data_length,
                char_length=char_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
            )

        assert column
        assert isinstance(
            column, CanonicalColumn
        ), "%s is not instance of CanonicalColumn" % type(column)

        if column.data_type == GOE_TYPE_BOOLEAN:
            return new_column(column, BIGQUERY_TYPE_BOOLEAN, safe_mapping=True)
        elif column.data_type in (
            GOE_TYPE_FIXED_STRING,
            GOE_TYPE_LARGE_STRING,
            GOE_TYPE_VARIABLE_STRING,
        ):
            return new_column(
                column,
                BIGQUERY_TYPE_STRING,
                char_length=column.char_length or column.data_length,
                safe_mapping=True,
            )
        elif column.data_type in (GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY):
            return new_column(
                column,
                BIGQUERY_TYPE_BYTES,
                data_length=column.data_length,
                safe_mapping=True,
            )
        elif column.data_type in (
            GOE_TYPE_INTEGER_1,
            GOE_TYPE_INTEGER_2,
            GOE_TYPE_INTEGER_4,
            GOE_TYPE_INTEGER_8,
        ):
            # On BigQuery all 4 native integer types map to BIGINT
            return new_column(column, BIGQUERY_TYPE_INT64, safe_mapping=True)
        elif column.data_type == GOE_TYPE_INTEGER_38:
            # On BigQuery there is no integral type > INT64 but BIGNUMERIC can hold 38 integral digits
            if column.data_precision and column.data_precision <= 29:
                return new_column(
                    column,
                    BIGQUERY_TYPE_NUMERIC,
                    data_precision=column.data_precision,
                    data_scale=0,
                    safe_mapping=True,
                )
            else:
                return new_column(
                    column,
                    BIGQUERY_TYPE_BIGNUMERIC,
                    data_precision=38,
                    data_scale=0,
                    safe_mapping=True,
                )
        elif column.data_type == GOE_TYPE_DECIMAL:
            if column.data_precision is not None:
                integral_magnitude = column.data_precision - (column.data_scale or 0)
            else:
                integral_magnitude = None
            if (
                integral_magnitude
                and integral_magnitude <= 29
                and (column.data_scale or 0) <= 9
            ):
                self._debug(
                    "Integral magnitude/scale is valid for NUMERIC: %s/%s"
                    % (integral_magnitude, column.data_scale)
                )
                new_data_type = BIGQUERY_TYPE_NUMERIC
                new_precision = column.data_precision
                new_scale = column.data_scale
                if not column.safe_mapping:
                    # We should round an unsafe mapping up to the max for NUMERIC.
                    # We can do this by removing the precision and scale settings.
                    self._log(
                        f"Switching unsafe NUMERIC mapping to BIGNUMERIC: {column.name}",
                        detail=VVERBOSE,
                    )
                    new_data_type = BIGQUERY_TYPE_BIGNUMERIC
                    new_precision = None
                    new_scale = None
                return new_column(
                    column,
                    new_data_type,
                    data_precision=new_precision,
                    data_scale=new_scale,
                    safe_mapping=True,
                )
            else:
                new_precision = column.data_precision
                new_scale = column.data_scale
                if not column.safe_mapping:
                    # We should round an unsafe mapping up to the max for BIGNUMERIC.
                    # We can do this by removing the precision and scale settings.
                    self._log(
                        f"Removing precision/scale decorators for unsafe BIGNUMERIC mapping: {column.name}",
                        detail=VVERBOSE,
                    )
                    new_precision = None
                    new_scale = None
                return new_column(
                    column,
                    BIGQUERY_TYPE_BIGNUMERIC,
                    data_precision=new_precision,
                    data_scale=new_scale,
                    safe_mapping=False,
                )
        elif column.data_type in (GOE_TYPE_FLOAT, GOE_TYPE_DOUBLE):
            return new_column(column, BIGQUERY_TYPE_FLOAT64, safe_mapping=True)
        elif column.data_type == GOE_TYPE_DATE and not self.canonical_date_supported():
            return new_column(column, BIGQUERY_TYPE_TIMESTAMP)
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, BIGQUERY_TYPE_DATE)
        elif column.data_type == GOE_TYPE_TIME:
            safe_mapping = bool(
                column.data_scale is None
                or column.data_scale <= self.max_datetime_scale()
            )
            return new_column(column, BIGQUERY_TYPE_TIME, safe_mapping=safe_mapping)
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            safe_mapping = bool(
                column.data_scale is None
                or column.data_scale <= self.max_datetime_scale()
            )
            return new_column(column, BIGQUERY_TYPE_DATETIME, safe_mapping=safe_mapping)
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            return new_column(column, BIGQUERY_TYPE_TIMESTAMP, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_DS:
            return new_column(column, BIGQUERY_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_YM:
            return new_column(column, BIGQUERY_TYPE_STRING, safe_mapping=False)
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )
