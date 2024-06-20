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

""" OffloadSourceTable: Library for logic/interaction with source of an offload
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime
import math
import logging
from string import Template
from textwrap import dedent
from typing import Union

from numpy import datetime64

from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    ColumnPartitionInfo,
    get_column_names,
    get_partition_columns,
    match_table_column,
    valid_column_list,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
)
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_constants import INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.util.misc_functions import get_integral_part_magnitude


class OffloadSourceTableException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

DATA_SAMPLE_SIZE_AUTO = -1

COLUMN_INTEGRAL_MAGNITUDE_MESSAGE_TEXT = Template(
    "Data in column $column has integral magnitude beyond backend capability"
)
COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT = (
    "One or more columns have data issues, see warnings for details"
)
COLUMN_SAMPLE_SCALE_MESSAGE_TEXT = Template(
    "Data in column $column has scale beyond backend capability"
)
DATETIME_STATS_SAMPLING_OPT_ACTION_TEXT = "checking column optimizer statistics"

OFFLOAD_PARTITION_TYPE_RANGE = "RANGE"
OFFLOAD_PARTITION_TYPE_LIST = "LIST"
OFFLOAD_PARTITION_TYPE_HASH = "HASH"

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def convert_high_values_to_python(
    partition_columns, hvs_individual, partition_type, source_table, strict=True
):
    """Wrapper for OffloadSourceTable.decode_partition_high_values_with_literals allowing calls from goe.py
    based on offload metadata rather than an OffloadSourceTable object
    """
    assert isinstance(
        source_table, OffloadSourceTableInterface
    ), "%s is not of type OffloadSourceTable" % str(type(source_table))
    assert valid_column_list(partition_columns)
    assert not hvs_individual or (
        hvs_individual and type(hvs_individual) in (list, tuple)
    )
    hv_list = []
    if partition_type == OFFLOAD_PARTITION_TYPE_LIST:
        # LIST partition tables can have multiple values per partition key and only one partition column
        zip_part_cols = [partition_columns[0] for _ in hvs_individual]
    else:
        zip_part_cols = partition_columns
    for part_col, hv in zip(zip_part_cols, hvs_individual):
        py_hv = source_table.rdbms_literal_to_python(
            part_col, hv, partition_type, strict=strict
        )
        hv_list.append(py_hv)
    return hv_list


def char_literal_to_python(rdbms_literal, quote_type="'"):
    return OffloadSourceTableInterface.char_literal_to_python(
        rdbms_literal, quote_type=quote_type
    )


###########################################################################
# CLASSES
###########################################################################


class RdbmsPartition:
    """Holds RDBMS partition details for a single partition"""

    def __init__(self):
        self.partition_name = None
        self.partition_count = None
        self.partition_position = None
        self.subpartition_count = None
        self.subpartition_name = None
        self.subpartition_names = None
        self.subpartition_position = None
        self.high_values_csv = None
        self.high_values_python = None
        self.partition_size = None
        self.num_rows = None
        self.high_values_individual = None

    def __str__(self):
        return (
            "RdbmsPartition(partition_name=%s, partition_count=%s, partition_position=%s, subpartition_count=%s, subpartition_name=%s, subpartition_names=%s, subpartition_position=%s, high_values_csv=%s, high_values_python=%s, partition_size=%s, num_rows=%s, high_values_individual=%s)"
            % (
                self.partition_name,
                self.partition_count,
                self.partition_position,
                self.subpartition_count,
                self.subpartition_name,
                (
                    ",".join(n for n in self.subpartition_names)
                    if self.subpartition_names is not None
                    else None
                ),
                self.subpartition_position,
                self.high_values_csv,
                self.high_values_python,
                self.partition_size,
                self.num_rows,
                self.high_values_individual,
            )
        )

    @staticmethod
    def by_name(
        partition_name=None,
        partition_count=None,
        partition_position=None,
        subpartition_count=None,
        subpartition_name=None,
        subpartition_names=None,
        subpartition_position=None,
        high_values_csv=None,
        high_values_python=None,
        partition_size=None,
        num_rows=None,
        high_values_individual=None,
    ):
        """Accepts named attributes and returns object based on them"""
        partition = RdbmsPartition()
        partition.partition_name = partition_name
        partition.partition_count = partition_count
        partition.partition_position = partition_position
        partition.subpartition_count = subpartition_count
        partition.subpartition_name = subpartition_name
        partition.subpartition_names = subpartition_names
        partition.subpartition_position = subpartition_position
        partition.high_values_csv = high_values_csv
        partition.high_values_python = high_values_python
        partition.partition_size = partition_size
        partition.num_rows = num_rows
        partition.high_values_individual = high_values_individual
        return partition


class OffloadSourceTableInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for DB specific sub-classes
    Attributes such as columns/size_in_bytes/etc are defined as methods so they can be enforced in
    subclasses, the subclasses should use @property to allow simplified usage of these
    """

    def __init__(
        self,
        schema_name,
        table_name,
        connection_options,
        messages,
        dry_run=False,
        conn=None,
        do_not_connect=False,
    ):
        self._messages = messages
        self.owner = schema_name
        self.table_name = table_name
        self._dry_run = dry_run
        self._db_type = connection_options.db_type
        self._db_api = frontend_api_factory(
            connection_options.db_type,
            connection_options,
            messages,
            existing_connection=conn,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=f"OffloadSourceTable({self.owner}.{self.table_name})",
        )
        self._table_exists = None
        self._table_is_view = None
        self._columns = None
        self._columns_with_partition_info = None
        self._primary_key_columns = None
        self._primary_index_columns = None
        self._size_in_bytes = None
        self._stats_num_rows = None
        self._timer = None

    def __del__(self):
        self.close()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _columns_getter(self):
        """Oracle implementation has an override for this."""
        if self._is_view():
            return self._columns
        else:
            return self._columns_with_partition_info

    def _columns_setter(self, new_columns, skip_exists_check=False):
        """When running in verification mode we might need to fake the columns in order to continue processing.
        Oracle implementation has an override for this.
        """
        if not skip_exists_check and self.exists():
            raise OffloadSourceTableException(
                "Set of columns is only supported when the table does NOT exist"
            )
        self._columns = new_columns
        self._columns_with_partition_info = self._get_columns_with_partition_info()

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def _extra_data_type_supported_checks(self):
        """Extra frontend specific data type checks.
        Returns True by default but some frontends may override.
        """
        return True

    def _frontend_decimal_to_integral_type(
        self, data_precision, data_scale, safe_mapping=True
    ):
        if data_scale == 0:
            # Integral numbers
            if 1 <= (data_precision or 0) <= 2:
                return GOE_TYPE_INTEGER_1
            elif 3 <= (data_precision or 0) <= 4:
                return GOE_TYPE_INTEGER_2
            elif 5 <= (data_precision or 0) <= 9:
                return GOE_TYPE_INTEGER_4
            elif (17 <= (data_precision or 0) <= 18) and not safe_mapping:
                # An unsafe mapping predicted a precision right on the edge of INT_8, round up to next threshold.
                self._log(
                    f"Switching unsafe INT8({data_precision}) mapping to INT38",
                    detail=VVERBOSE,
                )
                return GOE_TYPE_INTEGER_38
            elif 10 <= (data_precision or 0) <= 18:
                return GOE_TYPE_INTEGER_8
            elif 19 <= (data_precision or 0) <= 38:
                return GOE_TYPE_INTEGER_38
        return None

    def _get_column_details(self):
        logger.debug("_get_column_details: %s, %s" % (self.owner, self.table_name))
        return self._db_api.get_columns(self.owner, self.table_name)

    @abstractmethod
    def _get_column_low_high_values(self, column_name, from_stats=True, sample_perc=1):
        """Return low/high values for a specific column either from stats or from sampling data in the table"""

    def _get_columns_with_partition_info(self, part_col_names_override=None):
        """Process self._columns and return a list of objects with partition_info set for partition columns."""
        logger.debug(
            "_get_columns_with_partition_info: %s, %s" % (self.owner, self.table_name)
        )
        if part_col_names_override:
            part_cols = part_col_names_override
        else:
            part_cols = self._db_api.get_partition_column_names(
                self.owner, self.table_name
            )
        if part_cols:
            logger.debug("Found part_cols: %s" % str(part_cols))
        new_columns = []
        for col in self._columns:
            new_col = col.clone()
            if part_cols and col.name in part_cols:
                part_col_position = part_cols.index(new_col.name)
                new_col.partition_info = ColumnPartitionInfo(position=part_col_position)
            new_columns.append(new_col)
        return new_columns

    def _get_now(self):
        t = datetime.now()
        t = t.replace(microsecond=0)
        return t

    def _get_primary_index_columns(self) -> list:
        """
        Return a list of primary index column objects for applicable frontends.
        This interface level method returns nothing but individual frontends may override.
        """
        return []

    def _get_table_size(self):
        return self._db_api.get_table_size(self.owner, self.table_name)

    @abstractmethod
    def _get_table_stats(self):
        """Get optimizer stats from RDBMS data dictionary at table, partition and column level
        Return a dict of the form:
            table_stats = {
                'num_rows', 'num_bytes', 'avg_row_len',
                'column_stats': {name: {ndv, num_nulls, avg_col_len, low_value, high_value},},
                'partition_stats': {name: {num_rows, num_bytes, avg_row_len},}
            }
        """

    @abstractmethod
    def _is_compression_enabled(self) -> bool:
        """Return True/False is the source table has compression enabled. This is broad brush because
        some frontends may have a mix of compressed/uncompressed segments.
        """

    def _is_view(self) -> bool:
        if self._table_is_view is None:
            self._table_is_view = self._db_api.is_view(self.owner, self.table_name)
        return self._table_is_view

    def _log(self, msg, detail=None, ansi_code=None):
        """Write to offload log file"""
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _log_cols(self, columns):
        for col in columns:
            self._messages.log(
                "%s %s" % (col.name, col.format_data_type()), detail=VVERBOSE
            )

    def _reset_timer(self):
        self._timer = self._get_now()

    def _read_timer(self):
        t = self._get_now()
        return t - self._timer

    @abstractmethod
    def _sample_data_types_data_sample_parallelism(self, data_sample_parallelism):
        """Rationalise the user controlled sampling parallelism to a value suitable for the frontend system"""

    @abstractmethod
    def _sample_data_types_data_sample_pct(self, data_sample_pct):
        """Rationalise the user controlled sampling percentage to a value suitable for the frontend system"""

    @abstractmethod
    def _sample_data_types_date_as_string_column(self, column_name):
        """Frontend column object to store a datetime value as a string"""

    @abstractmethod
    def _sample_data_types_decimal_column(self, column, data_precision, data_scale):
        """Frontend column object for a DECIMAL(p,s) column"""

    @abstractmethod
    def _sample_data_types_compression_factor(self):
        """The factor by which we expect a table to reduce in size if compressed.
        This is total guesswork as variable by data shape and also some partitions may be compressed and some
        uncompressed. It's just a finger in the air.
        Should be > 0 and <= 1.
        """

    def _sample_data_types_execute_query(self, sample_sql):
        """Execute a query allowing for individual backends to override certain inputs. Oracle has an override."""
        return self._db_api.execute_query_fetch_one(
            sample_sql, log_level=VERBOSE, profile=True
        )

    @abstractmethod
    def _sample_data_types_min_gb(self):
        """The minimum threshold at which we sample 100% of a table"""

    @abstractmethod
    def _sample_data_types_max_pct(self):
        """The maximum value for sampling percentage for the backend"""

    @abstractmethod
    def _sample_data_types_min_pct(self):
        """The minimum value for sampling percentage for the backend"""

    def _sample_data_types_parallel_query_hint_block(self, query_parallelism):
        if query_parallelism is None:
            return ""
        return self._db_api.enclose_query_hints(
            self.parallel_query_hint(query_parallelism)
        )

    def _sample_data_types_v2_query_hint_block(self):
        """No hint by default. Oracle implementation has an override"""
        return ""

    @abstractmethod
    def _sample_perc_sql_clause(self, data_sample_pct) -> str:
        pass

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def check_nan_offload_allowed(
        self, backend_nan_supported, allow_floating_point_conversions=None
    ):
        nan_cols = [
            _ for _ in self.columns if _.data_type in self.nan_capable_data_types()
        ]
        if nan_cols and not backend_nan_supported:
            if allow_floating_point_conversions:
                self._messages.warning(
                    "Offloading Nan values as NULL in columns capable of NaN values because backend does not support them"
                )
                self._log_cols(nan_cols)
            else:
                self._messages.log(
                    "WARNING: Not offloading columns capable of NaN values because backend does not support them",
                    ansi_code="red",
                )
                self._messages.log(
                    "         Include --allow-floating-point-conversions option to proceed with the offload",
                    ansi_code="red",
                )
                self._log_cols(nan_cols)
                return False
        return True

    @abstractmethod
    def check_nanosecond_offload_allowed(
        self, backend_max_datetime_scale, allow_nanosecond_timestamp_columns=None
    ):
        pass

    def close(self):
        """Free up any resources currently held"""
        if getattr(self, "_db_api", None):
            self._db_api.close()

    @property
    def columns(self):
        return self._columns_getter()

    @columns.setter
    def columns(self, new_columns):
        self._columns_setter(new_columns)

    def data_types_in_use(self):
        return set(tc.data_type for tc in self.columns)

    def decode_partition_high_values(self, hv_csv, strict=True) -> tuple:
        """Takes partition high value and rationalise to Python values"""

    def enclose_identifier(self, identifier):
        return self._db_api.enclose_identifier(identifier)

    def enclose_query_hints(self, hint_contents):
        return self._db_api.enclose_query_hints(hint_contents)

    def enclosure_character(self):
        """Superceded by enclose_identifier(), ideally we shouldn't use enclosure_character() any longer."""
        return self._db_api.enclosure_character()

    def exists(self):
        return self._table_exists

    def fetchmany_takes_fetch_size(self):
        return self._db_api.fetchmany_takes_fetch_size()

    def format_query_parameter(self, param_name):
        return self._db_api.format_query_parameter(param_name)

    def frontend_db_name(self):
        return self._db_api.frontend_db_name()

    def get_canonical_columns(self):
        """Get canonical columns for the table"""
        return [self.to_canonical_column(_) for _ in self.columns]

    def get_column_names(self, conv_fn=None):
        return get_column_names(self.columns, conv_fn=conv_fn)

    def get_column(self, column_name):
        match = [col for col in self.columns if col.name.upper() == column_name.upper()]
        if not match:
            return None
        else:
            return match[0]

    @abstractmethod
    def get_current_scn(self):
        pass

    def get_partition_column_data_types(self):
        return [col.data_type for col in self.partition_columns]

    def get_partition_column_names(self):
        return [col.name for col in self.partition_columns]

    def get_distinct_column(self, column, partition_name=None):
        """Select distinct values of column from source table.
        column can be a column object or column_name.
        Optional partition_name to select from.
        """
        assert isinstance(column, (ColumnMetadataInterface, str))
        column_name = (
            column.name if isinstance(column, ColumnMetadataInterface) else column
        )
        return self._db_api.get_distinct_column_values(
            self.owner, self.table_name, column_name, partition_name=partition_name
        )

    def get_frontend_api(self):
        """Sometimes we just want to pass on access to the frontend api and not
        have wrappers for the sake of wrappers.
        """
        return self._db_api

    @abstractmethod
    def get_hash_bucket_candidate(self):
        """Return the column name of a selective column suitable for use as a bucket/hash column.
        Techniques may differ by frontend but in general we are looking for a column with high distinct values
        and low number of NULLs.
        """

    def get_hash_bucket_last_resort(self):
        cols = [
            col.name
            for col in self.columns
            if col.data_type not in self.hash_bucket_unsuitable_data_types()
        ]
        if cols:
            return cols[0]

    @abstractmethod
    def get_max_partition_size(self):
        pass

    @abstractmethod
    def get_partitions(self):
        pass

    def get_primary_index_columns(self):
        if self._primary_index_columns is None:
            self._primary_index_columns = self._get_primary_index_columns()
        return self._primary_index_columns

    def get_primary_key_columns(self):
        if self._primary_key_columns is None:
            self._primary_key_columns = self._db_api.get_primary_key_column_names(
                self.owner, self.table_name
            )
        return self._primary_key_columns

    def get_session_option(self, option_name):
        return self._db_api.get_session_option(option_name)

    def get_suitable_sample_size(self, bytes_override=None) -> int:
        """Sample size based on table size as we'll always have that in Oracle. Optimizer stats are less reliable.
        Decreasing size of data we plan to scan by factor of 10 as total size increases by factor of 10
        If a table has compressed segments then we should factor that in, this is guesswork though because
        we don't know which segments will be sampled.
        """
        size_in_bytes = bytes_override or self.size_in_bytes
        gb = (size_in_bytes / (1024**3)) if size_in_bytes else None
        if not gb or gb <= self._sample_data_types_min_gb():
            return 100
        magnitude = math.log10(gb)
        plan_to_scan_gb = self._sample_data_types_min_gb() * magnitude
        self._messages.log(
            "Sampling plans to scan %sG out of %sG" % (plan_to_scan_gb, gb),
            detail=VVERBOSE,
        )
        pct = round(plan_to_scan_gb * 100 / gb, 6)
        if self._is_compression_enabled():
            # Factor in reduced sample size if segments may be compressed
            self._messages.log(
                "Applying factor %s to sample percent due to table compression"
                % self._sample_data_types_compression_factor(),
                detail=VVERBOSE,
            )
            pct = pct * self._sample_data_types_compression_factor()
        return max(pct, self._sample_data_types_min_pct())

    @staticmethod
    @abstractmethod
    def hash_bucket_unsuitable_data_types():
        pass

    @abstractmethod
    def is_iot(self):
        pass

    @abstractmethod
    def is_partitioned(self):
        pass

    @abstractmethod
    def is_subpartitioned(self):
        pass

    @abstractmethod
    def offload_by_subpartition(self):
        pass

    @property
    @abstractmethod
    def offload_partition_level(self) -> Union[int, None]:
        """The partition level we've taken for OffloadSourcePartitions"""

    def has_rowdependencies(self):
        """Row dependencies are an Oracle only attribute. False by default and let Oracle override this method"""
        return False

    @property
    def parallelism(self):
        """Default table level parallelism, may not apply to all RDBMSs. Some frontends may override."""
        return None

    @property
    def partition_columns(self):
        return get_partition_columns(self.columns)

    @abstractmethod
    def partition_type(self):
        pass

    def split_partition_high_value_string(self, hv_string):
        return self._db_api.split_partition_high_value_string(hv_string)

    def sample_rdbms_data_types(
        self,
        columns_to_sample,
        data_sample_pct,
        data_sample_parallelism,
        backend_min_possible_date,
        backend_max_decimal_integral_magnitude,
        backend_max_decimal_scale,
        allow_decimal_scale_rounding,
    ):
        """Sample data for a list of columns and return a list of new RDBMS columns based on the results.
            columns_to_sample: A list of OracleColumn objects
            data_sample_pct: % of data in the source table to sample
            data_sample_parallelism: degree of parallelism for sample query. 0 = no parallel
            backend_min_possible_date: The minimum date that the backend can handle
            backend_max_decimal_integral_magnitude: The most digits to the left of the decimal place that the backend can handle
            backend_max_decimal_scale: The most digits to the right of the decimal place that the backend can handle
        This code will generate a sampling SQL something along the lines of:
            SELECT COUNT(*)
            ,      MAX(CASE WHEN "GOE_SAMP_COL_0" = 0 THEN LENGTH("PROD_ID") ELSE "GOE_SAMP_COL_0"-1 END)
            ,      MAX(CASE WHEN "GOE_SAMP_COL_0" = 0 THEN 0 ELSE LENGTH("PROD_ID")-"GOE_SAMP_COL_0" END)
            ,      MAX(CASE WHEN INSTR("PROD_ID",'E') > 0 THEN 1 ELSE NULL END)
            ,      MAX(CASE WHEN "GOE_SAMP_COL_1" = 0 THEN LENGTH("CUST_ID") ELSE "GOE_SAMP_COL_1"-1 END)
            ,      MAX(CASE WHEN "GOE_SAMP_COL_1" = 0 THEN 0 ELSE LENGTH("CUST_ID")-"GOE_SAMP_COL_1" END)
            ,      MAX(CASE WHEN INSTR("CUST_ID",'E') > 0 THEN 1 ELSE NULL END)
            FROM   (
                SELECT /*+ NO_MERGE */
                       "PROD_ID"
                ,      INSTR("PROD_ID",'.') AS "GOE_SAMP_COL_0"
                ,      "CUST_ID"
                ,      INSTR("CUST_ID",'.') AS "GOE_SAMP_COL_1"
                FROM   (
                    SELECT /*+ NO_PARALLEL */
                           TO_CHAR(ABS("PROD_ID"),'TM') AS "PROD_ID"
                    ,      TO_CHAR(ABS("CUST_ID"),'TM') AS "CUST_ID"
                    FROM   "SH"."SALES" SAMPLE BLOCK (99.999999)
                )
            )
        Each numeric column sampled will produce 3 columns:
            MAX(CASE WHEN "GOE_SAMP_COL_0" = 0 THEN LENGTH("PROD_ID") ELSE "GOE_SAMP_COL_0"-1 END)
            MAX(CASE WHEN "GOE_SAMP_COL_0" = 0 THEN 0 ELSE LENGTH("PROD_ID")-"GOE_SAMP_COL_0" END)
            MAX(CASE WHEN INSTR("PROD_ID",'E') > 0 THEN 1 ELSE NULL END)
        These are:
            Integral magnitude
            Scale
            Suspect value identifier: 1 if the value contains 'E' which would break the previous 2 values
        We're using string manipulation to get the magnitude and scale, when testing this was faster than
        using arithmetic functions.
        """

        def check_backend_max_decimal_integral_magnitude(
            col_name, num_value=None, magnitude=None
        ):
            if (
                num_value
                and get_integral_part_magnitude(num_value)
                > backend_max_decimal_integral_magnitude
            ):
                # This data won't fit and we need to reject the offload
                self._messages.warning(
                    "%s: %s > %s"
                    % (
                        COLUMN_INTEGRAL_MAGNITUDE_MESSAGE_TEXT.substitute(
                            column=col_name
                        ),
                        get_integral_part_magnitude(num_value),
                        backend_max_decimal_integral_magnitude,
                    ),
                    ansi_code="red",
                )
                return False
            elif (magnitude or 0) > backend_max_decimal_integral_magnitude:
                self._messages.warning(
                    "%s: %s > %s"
                    % (
                        COLUMN_INTEGRAL_MAGNITUDE_MESSAGE_TEXT.substitute(
                            column=col_name
                        ),
                        magnitude,
                        backend_max_decimal_integral_magnitude,
                    ),
                    ansi_code="red",
                )
                return False
            return True

        def check_backend_max_decimal_scale(
            col_name, scale, allow_decimal_scale_rounding
        ):
            if (
                not allow_decimal_scale_rounding
                and (scale or 0) > backend_max_decimal_scale
            ):
                # This data won't fit and we need to reject the offload
                self._messages.warning(
                    "%s: %s > %s"
                    % (
                        COLUMN_SAMPLE_SCALE_MESSAGE_TEXT.substitute(column=col_name),
                        scale,
                        backend_max_decimal_scale,
                    ),
                    ansi_code="red",
                )
                return False
            return True

        def format_sampling_query(
            columns_to_sample,
            data_sample_pct,
            data_sample_parallelism,
            columns_affected_by_sampling,
            columns_in_results,
        ):
            """Format and run an RDBMS data type sampling query.
            Also attempts to get representative values from RDBMS optimizer statistics to avoid sampling.
            columns_affected_by_sampling: This list is appended to during the method call.
            Fortunately for GOE so far a very similar SQL format works on both Oracle and Teradata. We will need
            to tease this apart when we choose to implement a 3rd frontend (AKA kicking the can down the road).
            """
            assert isinstance(columns_affected_by_sampling, list)
            proj = ["COUNT(*)"]
            cols_abs, cols_with_instr = [], []
            abort = False
            for i, col in enumerate(columns_to_sample):
                subs = {"col": col.name.upper(), "col_alias": "GOE_SAMP_COL_%s" % i}
                if col.is_date_based():
                    min_value = None
                    if self._db_api.low_high_value_from_stats_supported():
                        # For min value detection RDBMS optimizer stats will be more accurate than our sampling
                        min_value, _ = self._get_column_low_high_values(
                            col.name, from_stats=True
                        )
                    if min_value:
                        self._messages.log(
                            "%s min_value (%s): %s"
                            % (col.name, type(min_value), min_value),
                            detail=VVERBOSE,
                        )
                        if min_value < backend_min_possible_date:
                            columns_affected_by_sampling.append(
                                self._sample_data_types_date_as_string_column(col.name)
                            )
                            self._messages.notice(
                                'Detected incompatible value "%s" for %s after %s, offloading via %s'
                                % (
                                    min_value,
                                    col.name,
                                    DATETIME_STATS_SAMPLING_OPT_ACTION_TEXT,
                                    self._db_api.generic_string_data_type(),
                                )
                            )
                        continue
                    proj.append('MIN("%(col_alias)s")' % subs)
                    cols_abs.append('"%(col)s"' % subs)
                    cols_with_instr.append('"%(col)s" AS "%(col_alias)s"' % subs)
                else:
                    if (
                        col.data_precision is not None
                        and (col.data_precision - max(col.data_scale, 0))
                        > backend_max_decimal_integral_magnitude
                    ):
                        # We have max() on scale above because scales can be negative which effectively means 0
                        # The backend can only cope with a smaller number of digits to left of decimal place than frontend
                        # Check to see if stats give a short-cut to finding a bad value
                        min_value = max_value = None
                        if self._db_api.low_high_value_from_stats_supported():
                            min_value, max_value = self._get_column_low_high_values(
                                col.name, from_stats=True
                            )
                        if min_value or max_value:
                            self._messages.log(
                                "%s min_value/max_value: %s (%s)/%s (%s)"
                                % (
                                    col.name,
                                    min_value,
                                    type(min_value),
                                    max_value,
                                    type(max_value),
                                ),
                                detail=VVERBOSE,
                            )
                            if not check_backend_max_decimal_integral_magnitude(
                                col.name, num_value=min_value
                            ):
                                abort = True
                                continue
                            if not check_backend_max_decimal_integral_magnitude(
                                col.name, num_value=max_value
                            ):
                                abort = True
                                continue
                    proj.append(
                        """MAX(CASE WHEN "%(col_alias)s" = 0 THEN LENGTH("%(col)s") ELSE "%(col_alias)s"-1 END)"""
                        % subs
                    )
                    proj.append(
                        """MAX(CASE WHEN "%(col_alias)s" = 0 THEN 0 ELSE LENGTH("%(col)s")-"%(col_alias)s" END)"""
                        % subs
                    )
                    proj.append(
                        """MAX(CASE WHEN INSTR("%(col)s",'E') > 0 THEN 1 ELSE NULL END)"""
                        % subs
                    )
                    cols_abs.append(
                        """TO_CHAR(ABS("%(col)s"),'TM') AS "%(col)s" """ % subs
                    )
                    cols_with_instr.append('"%(col)s"' % subs)
                    cols_with_instr.append(
                        """INSTR("%(col)s",'.') AS "%(col_alias)s" """ % subs
                    )
                columns_in_results.append(col)

            if abort:
                raise OffloadSourceTableException(
                    COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT
                )

            sample_sql = (
                dedent(
                    """\
                SELECT %(projection)s
                FROM   (
                    SELECT %(v2_qb_hint_block)s
                           %(cols_with_instr)s
                    FROM   (
                        SELECT %(v1_qb_hint_block)s
                               %(inner_cols)s
                        FROM   "%(owner)s"."%(table)s" %(sample_clause)s
                    ) v1
                ) v2"""
                )
                % {
                    "projection": "\n,      ".join(proj),
                    "cols_with_instr": "\n    ,      ".join(cols_with_instr),
                    "inner_cols": "\n        ,      ".join(cols_abs),
                    "owner": self.owner.upper(),
                    "table": self.table_name.upper(),
                    "sample_clause": self._sample_perc_sql_clause(data_sample_pct),
                    "v1_qb_hint_block": self._sample_data_types_parallel_query_hint_block(
                        data_sample_parallelism
                    ),
                    "v2_qb_hint_block": self._sample_data_types_v2_query_hint_block(),
                }
            )

            return sample_sql

        def int_if_float(some_value):
            return int(some_value) if isinstance(some_value, float) else some_value

        assert isinstance(columns_to_sample, list)
        if not columns_to_sample:
            return
        assert isinstance(columns_to_sample[0], ColumnMetadataInterface)

        local_data_sample_pct = self._sample_data_types_data_sample_pct(data_sample_pct)
        local_data_sample_parallelism = self._sample_data_types_data_sample_parallelism(
            data_sample_parallelism
        )

        self._messages.log(
            "Minimum valid date for backend: %s" % str(backend_min_possible_date),
            detail=VVERBOSE,
        )

        columns_affected_by_sampling = []
        columns_in_results = []
        sample_sql = format_sampling_query(
            columns_to_sample,
            local_data_sample_pct,
            local_data_sample_parallelism,
            columns_affected_by_sampling,
            columns_in_results,
        )

        # It's possible while formatting the SQL we removed all sampling requirements
        if not columns_in_results:
            return columns_affected_by_sampling

        self._reset_timer()
        sample_output_row = self._sample_data_types_execute_query(sample_sql)
        self._messages.log("SQL elapsed time: %s" % self._read_timer(), VVERBOSE)

        if not sample_output_row or sample_output_row[0] == 0:
            self._messages.warning(
                "Unable to sample column data (row count %s), using default data types"
                % (sample_output_row[0] if sample_output_row else None),
                ansi_code="red",
            )
            return

        # From here on we reduce sample_output_row as we use each column's data.

        # Remove count star first column
        self._messages.log("Sampled %s rows" % str(sample_output_row[0]), VVERBOSE)
        sample_output_row = list(sample_output_row[1:])
        self._messages.log(
            "Sampled precision/scale raw data: %s" % str(sample_output_row), VVERBOSE
        )

        # There are 3 columns per RDBMS column, loop through them using the information as appropriate
        abort = False
        for col in columns_in_results:
            if col.is_date_based():
                min_value = sample_output_row.pop(0)
                if min_value and datetime64(min_value) < backend_min_possible_date:
                    columns_affected_by_sampling.append(
                        self._sample_data_types_date_as_string_column(col.name)
                    )
                    self._messages.notice(
                        'Detected incompatible value "%s" for %s after sampling column data, offloading via %s'
                        % (min_value, col.name, self._db_api.generic_string_data_type())
                    )
            else:
                int_part, scale, suspect = sample_output_row[:3]
                sample_output_row = sample_output_row[3:]
                int_part = int_if_float(int_part)
                scale = int_if_float(scale)
                if not check_backend_max_decimal_integral_magnitude(
                    col.name, magnitude=int_part
                ):
                    abort = True
                    continue
                if not check_backend_max_decimal_scale(
                    col.name, scale, allow_decimal_scale_rounding
                ):
                    abort = True
                    continue
                if suspect:
                    self._messages.warning(
                        "Unable to sample column data for %s due to suspect data"
                        % col.name,
                        ansi_code="red",
                    )
                    continue
                precision = (
                    None if int_part is None and scale is None else (int_part + scale)
                )
                self._messages.log(
                    "Detected precision=%s, scale=%s for %s after sampling column data"
                    % (precision, scale, col.name),
                    VERBOSE,
                )
                # Create a fake column object with the sampled precision and scale and then run it through auto detection routine
                columns_affected_by_sampling.append(
                    self._sample_data_types_decimal_column(col, precision, scale)
                )

        if abort:
            raise OffloadSourceTableException(COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT)

        return columns_affected_by_sampling

    @property
    def size_in_bytes(self):
        return self._size_in_bytes

    @property
    def stats_num_rows(self):
        return self._stats_num_rows

    @property
    def table_stats(self):
        return self._get_table_stats()

    @staticmethod
    @abstractmethod
    def supported_data_types() -> set:
        """Returns a set of column data types supported by GOE on the frontend system"""

    def check_data_types_supported(
        self,
        backend_max_datetime_scale,
        backend_nan_supported,
        allow_nanosecond_timestamp_columns=None,
        allow_floating_point_conversions=None,
    ):
        source_data_types = self.data_types_in_use()
        if not source_data_types.issubset(self.supported_data_types()):
            self._messages.log(
                "Data types not supported: %s"
                % list(source_data_types - self.supported_data_types())
            )
            return False

        if self.max_datetime_scale() > 6 and not self.check_nanosecond_offload_allowed(
            backend_max_datetime_scale,
            allow_nanosecond_timestamp_columns=allow_nanosecond_timestamp_columns,
        ):
            return False

        if self.nan_capable_data_types() and not self.check_nan_offload_allowed(
            backend_nan_supported,
            allow_floating_point_conversions=allow_floating_point_conversions,
        ):
            return False

        if not self._extra_data_type_supported_checks():
            return False

        return True

    @abstractmethod
    def supported_range_partition_data_type(self, data_type):
        pass

    @abstractmethod
    def supported_list_partition_data_type(self, data_type):
        pass

    def unsupported_partition_data_types(self, partition_type_override=None):
        partition_column_types = [_.data_type for _ in self.partition_columns]
        check_partition_type = partition_type_override or self.partition_type
        if check_partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
            bad_data_types = [
                _
                for _ in partition_column_types
                if not self.supported_range_partition_data_type(_)
            ]
        else:
            bad_data_types = [
                _
                for _ in partition_column_types
                if not self.supported_list_partition_data_type(_)
            ]
        return bad_data_types

    @abstractmethod
    def supported_partition_data_types(self):
        pass

    @staticmethod
    @abstractmethod
    def nan_capable_data_types():
        pass

    @staticmethod
    @abstractmethod
    def datetime_literal_to_python(rdbms_literal, strict=True):
        pass

    @staticmethod
    @abstractmethod
    def numeric_literal_to_python(rdbms_literal):
        pass

    @abstractmethod
    def rdbms_literal_to_python(
        self, rdbms_column, rdbms_literal, partition_type, strict=True
    ):
        pass

    @abstractmethod
    def get_minimum_partition_key_data(self):
        pass

    def max_datetime_scale(self):
        return self._db_api.max_datetime_scale()

    def max_datetime_value(self):
        return self._db_api.max_datetime_value()

    def max_column_name_length(self):
        return self.max_table_name_length()

    def max_schema_name_length(self):
        return self.max_table_name_length()

    def max_table_name_length(self):
        return self._db_api.max_table_name_length()

    def min_datetime_value(self):
        return self._db_api.min_datetime_value()

    def parallel_query_hint(self, query_parallelism):
        if query_parallelism is None:
            return ""
        return self._db_api.parallel_query_hint(query_parallelism)

    @abstractmethod
    def to_rdbms_literal_with_sql_conv_fn(self, py_val, rdbms_data_type):
        pass

    @abstractmethod
    def enable_offload_by_subpartition(self, desired_state=True):
        pass

    @abstractmethod
    def offload_by_subpartition_capable(self):
        pass

    @abstractmethod
    def partition_has_rows(self, partition_name):
        pass

    @abstractmethod
    def predicate_has_rows(self, predicate):
        pass

    @abstractmethod
    def predicate_to_where_clause(self, predicate, columns_override=None):
        pass

    @abstractmethod
    def predicate_to_where_clause_with_binds(self, predicate):
        pass

    @staticmethod
    def char_literal_to_python(rdbms_literal, quote_type="'"):
        """Convert an RDBMS string literal, such as 'ABC', to a Python variable
        Basically strip unwanted quotes
        """
        return rdbms_literal.strip(quote_type)

    def to_canonical_column_with_overrides(
        self,
        rdbms_column,
        canonical_overrides=None,
        auto_detect_num=True,
        auto_detect_dates=True,
    ):
        """Translate an RDBMS column to an internal GOE column but only after considering user overrides.
        canonical_overrides: Brings user defined overrides into play, they take precedence over default rules.
        auto_detect_num: Can be set to false to ignore default rules and go for a default data type.
        auto_detect_dates: Can be set to false to ignore default rules and go for a default data type.
        """
        assert rdbms_column
        assert isinstance(
            rdbms_column, ColumnMetadataInterface
        ), "%s is not an instance of ColumnMetadataInterface" % type(rdbms_column)
        if canonical_overrides:
            assert valid_column_list(canonical_overrides)

        if match_table_column(rdbms_column.name, canonical_overrides or []):
            # Use the user defined override
            new_col = match_table_column(rdbms_column.name, canonical_overrides)
            # Validate the data type of the source column and override are compatible
            if not self.valid_canonical_override(rdbms_column, new_col):
                raise OffloadSourceTableException(
                    "%s %s: %s -> %s"
                    % (
                        INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
                        rdbms_column.name,
                        rdbms_column.data_type,
                        new_col.data_type,
                    )
                )
            new_col.nullable = rdbms_column.nullable
        elif rdbms_column.is_number_based() and not auto_detect_num:
            tmp_col = self.gen_default_numeric_column(rdbms_column.name)
            tmp_col.nullable = rdbms_column.nullable
            new_col = self.to_canonical_column(tmp_col)
        elif rdbms_column.is_date_based() and not auto_detect_dates:
            tmp_col = self.gen_default_date_column(rdbms_column.name)
            tmp_col.nullable = rdbms_column.nullable
            new_col = self.to_canonical_column(tmp_col)
        else:
            new_col = self.to_canonical_column(rdbms_column)
        return new_col

    @abstractmethod
    def to_canonical_column(self, column):
        """Translate an RDBMS column to an internal GOE column"""

    @abstractmethod
    def from_canonical_column(self, column):
        """Translate an internal GOE column to an RDBMS column"""

    @abstractmethod
    def gen_column(
        self,
        name,
        data_type,
        data_length=None,
        data_precision=None,
        data_scale=None,
        nullable=None,
        data_default=None,
        hidden=None,
        char_semantics=None,
        char_length=None,
    ):
        """Create a column object appropriate to the RDBMS"""

    @abstractmethod
    def gen_default_numeric_column(self, column_name):
        """Generate a generic column object suitable as a catch-all numeric column"""

    @abstractmethod
    def gen_default_date_column(self, column_name):
        """Generate a generic column object suitable as a catch-all date column"""

    @abstractmethod
    def valid_canonical_override(self, column, canonical_override):
        """Offload has a number of options for overriding the default canonical mapping in to_canonical_column().
        This method validates the override.
        column: the source RDBMS column object.
        canonical_override: either a canonical column object or a GOE_TYPE_... data type.
        """

    def goe_join_pushdown_supported(self):
        return self._db_api.goe_join_pushdown_supported()

    def goe_offload_status_report_supported(self):
        return self._db_api.goe_offload_status_report_supported()

    @abstractmethod
    def transform_encrypt_data_type(self):
        """Return a backend data type matching the output of an encryption expression"""

    @abstractmethod
    def transform_null_cast(self, rdbms_column):
        """Return a SQL expression to cast a NULL value in place of a column"""

    @abstractmethod
    def transform_tokenize_data_type(self):
        """Return a backend data type matching the output of a tokenization expression"""

    @abstractmethod
    def transform_regexp_replace_expression(
        self, rdbms_column, regexp_replace_pattern, regexp_replace_string
    ):
        pass

    @abstractmethod
    def transform_translate_expression(self, rdbms_column, from_string, to_string):
        pass
