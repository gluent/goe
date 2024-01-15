#! /usr/bin/env python3
""" NetezzaSourceTable: Library for logic/interaction with Netezza source of an offload
    LICENSE_TEXT
"""

import inspect
import logging
from typing import Union

from goe.offload.column_metadata import (
    CanonicalColumn,
    is_safe_mapping,
    GOE_TYPE_FIXED_STRING,
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
    GOE_TYPE_BOOLEAN,
    ALL_CANONICAL_TYPES,
    NUMERIC_CANONICAL_TYPES,
    STRING_CANONICAL_TYPES,
)
from goe.offload.netezza.netezza_column import (
    NetezzaColumn,
    NETEZZA_TYPE_BOOLEAN,
    NETEZZA_TYPE_BYTEINT,
    NETEZZA_TYPE_SMALLINT,
    NETEZZA_TYPE_INTEGER,
    NETEZZA_TYPE_BIGINT,
    NETEZZA_TYPE_NUMERIC,
    NETEZZA_TYPE_REAL,
    NETEZZA_TYPE_DOUBLE_PRECISION,
    NETEZZA_TYPE_DATE,
    NETEZZA_TYPE_TIME,
    NETEZZA_TYPE_TIME_WITH_TIME_ZONE,
    NETEZZA_TYPE_TIMESTAMP,
    NETEZZA_TYPE_INTERVAL,
    NETEZZA_TYPE_CHARACTER,
    NETEZZA_TYPE_CHARACTER_VARYING,
    NETEZZA_TYPE_NATIONAL_CHARACTER,
    NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING,
    NETEZZA_TYPE_BINARY_VARYING,
    NETEZZA_TYPE_ST_GEOMETRY,
)
from goe.offload.offload_source_table import OffloadSourceTableInterface


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# CONSTANTS
###########################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# CLASSES
###########################################################################


class NetezzaSourceTable(OffloadSourceTableInterface):
    """IBM Netezza source table details and methods"""

    def __init__(
        self,
        schema_name,
        table_name,
        connection_options,
        messages,
        dry_run=False,
        do_not_connect=False,
    ):
        assert schema_name and table_name and connection_options
        assert hasattr(connection_options, "rdbms_app_user")
        assert hasattr(connection_options, "rdbms_app_pass")
        assert hasattr(connection_options, "rdbms_dsn")

        super(NetezzaSourceTable, self).__init__(
            schema_name,
            table_name,
            connection_options,
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
        )

        logger.info(
            "NetezzaSourceTable setup: (%s, %s, %s)"
            % (schema_name, table_name, connection_options.rdbms_app_user)
        )
        if dry_run:
            logger.info("* Dry run *")

        # attributes below are specified as private so they can be enforced
        # (where relevant) as properties via base class
        self._iot_type = None
        self._partitioned = None
        self._partition_type = None
        self._subpartition_type = None
        self._partitions = None
        self._dependencies = None
        self._parallelism = None

        if not do_not_connect:
            self._get_table_details()
            self._columns_setter(self._get_column_details(), skip_exists_check=True)
            if self._table_exists:
                self._size_in_bytes = self._get_table_size()
                self._hash_bucket_candidate = self._get_hash_bucket_candidate()
                # TODO SS@2017-09-04 Implement table stats when we pick up Netezza work again
                self._table_stats = None
            else:
                self._hash_bucket_candidate = None
                self._table_stats = None

    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _get_column_low_high_values(self, column_name, from_stats=True, sample_perc=1):
        raise NotImplementedError(
            "Netezza _get_column_low_high_values not implemented."
        )

    def _get_table_details(self):
        logger.debug("_get_table_details: %s, %s" % (self.owner, self.table_name))
        # order of columns important as referenced in helper functions
        q = """
            SELECT CASE
                     WHEN reltuples < 0
                     THEN ((2^32) * relrefs) + ((2^32) + reltuples)
                     ELSE ((2^32) * relrefs) + (reltuples)
                   END                                              AS num_rows
            ,      NULL                                             AS partitioned
            ,      NULL                                             AS partitioning_type
            FROM   _t_class
                   INNER JOIN
                   _t_object
                   ON (_t_class.oid = _t_object.objid)
                   INNER JOIN
                   _t_object_classes
                   ON (_t_object_classes.objclass = _t_object.objclass)
                   INNER JOIN
                   _t_user
                   ON (_t_user.usesysid = _t_object.objowner)
            WHERE  _t_object_classes.objrefclass = 4905
            AND    _t_class.relname = UPPER(?)
            AND    _t_user.usename = UPPER(?)"""

        row = self._db_api.execute_query_fetch_one(
            q, query_params=[self.table_name, self.owner]
        )
        if row:
            self._stats_num_rows, self._partitioned, self._partition_type = [
                _.encode("utf-8") if type(_) == str else _ for _ in row
            ]
            self._table_exists = True if self._stats_num_rows is not None else False
        else:
            self._table_exists = False

    def _get_columns_with_partition_info(self, part_col_names_override=None):
        raise NotImplementedError(
            "Netezza _get_columns_with_partition_info not implemented."
        )

    def _get_hash_bucket_candidate(self):
        logger.debug(
            "_get_hash_bucket_candidate: %s, %s" % (self.owner, self.table_name)
        )
        q = """
            SELECT v.column_name
            ,      v.num_distinct * ((v.num_rows - v.num_nulls) / v.num_rows) AS ndv_null_factor
            FROM (
            SELECT _v_relation_column.attname                           AS column_name
            ,      CASE WHEN _v_relation_column.attdispersion = -1
                     THEN _v_table.reltuples
                     ELSE 1.0/_v_relation_column.attdispersion
                   END                                                  AS num_distinct
            ,      _v_table.reltuples                                   AS num_rows
            ,      CASE WHEN _v_statistic.nullfrac = 0
                     THEN 0
                     ELSE _v_table.reltuples * _v_statistic.nullfrac
                   END                                                  AS num_nulls
            FROM   _v_relation_column
                   LEFT OUTER JOIN _v_statistic ON
                   (    _v_relation_column.objid  = _v_statistic.objid
                    AND _v_relation_column.attnum = _v_statistic.attnum)
                   INNER JOIN _v_table ON
                   (    _v_table.owner     = _v_statistic.owner
                    AND _v_table.tablename = _v_statistic.tablename)
            WHERE  _v_relation_column.owner = UPPER(?)
            AND    _v_relation_column.name = UPPER(?)
            AND    _v_relation_column.format_type IN (%s)
            AND    _v_relation_column.attdispersion <> 0
            AND    _v_table.reltuples > 0) AS v
            ORDER BY
                   ndv_null_factor DESC
            LIMIT 1""" % ",".join(
            ["'%s'" % dtype for dtype in self.supported_data_types()]
        )
        rows = self._db_api.execute_query_fetch_one(
            q, query_params=[self.owner, self.table_name]
        )
        return rows[0] if rows else None

    def _get_table_stats(self):
        # TODO SS@2017-09-04 When we revisit Netezza support we need to decide if having the stats is required
        raise NotImplementedError("Netezza _get_table_stats not implemented.")
        # return self._table_stats

    def _is_compression_enabled(self) -> bool:
        raise NotImplementedError("Netezza _is_compression_enabled not implemented.")

    def _sample_data_types_compression_factor(self):
        raise NotImplementedError(
            "Netezza _sample_data_types_compression_factor not implemented."
        )

    def _sample_data_types_data_sample_parallelism(self, data_sample_parallelism):
        raise NotImplementedError(
            "Netezza _sample_data_types_compression_factor not implemented."
        )

    def _sample_data_types_data_sample_pct(self, data_sample_pct):
        raise NotImplementedError(
            "Netezza _sample_data_types_data_sample_pct not implemented."
        )

    def _sample_data_types_date_as_string_column(self, column_name):
        raise NotImplementedError(
            "Netezza _sample_data_types_date_as_string_column not implemented."
        )

    def _sample_data_types_decimal_column(self, column, data_precision, data_scale):
        return NetezzaColumn(
            column.name,
            NETEZZA_TYPE_NUMERIC,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=column.nullable,
            safe_mapping=False,
        )

    def _sample_data_types_min_gb(self):
        """1GB seems like a good target"""
        return 1

    def _sample_data_types_max_pct(self):
        raise NotImplementedError("Netezza _sample_data_types_max_pct not implemented.")

    def _sample_data_types_min_pct(self):
        raise NotImplementedError("Netezza _sample_data_types_min_pct not implemented.")

    def _sample_perc_sql_clause(self, data_sample_pct) -> str:
        return ""

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @property
    def partition_type(self):
        return self._partition_type

    @property
    def offload_by_subpartition(self):
        return False

    @property
    def offload_partition_level(self) -> Union[int, None]:
        return None

    @property
    def parallelism(self):
        return self._parallelism

    def decode_partition_high_values(self, hv_csv, strict=True) -> tuple:
        raise NotImplementedError(
            "Netezza decode_partition_high_values not implemented."
        )

    def get_current_scn(self, return_none_on_failure=False):
        raise NotImplementedError("Netezza get_current_scn not implemented.")

    def get_hash_bucket_candidate(self):
        return self._hash_bucket_candidate

    def get_partitions(self, strict=True, populate_hvs=True):
        """Fetch list of partitions for table. This can be time consuming therefore
        executed on demand and not during instantiation
        """
        if self._partitions is None and self.is_partitioned():
            raise NotImplementedError("Netezza get_partitions not implemented.")
        return self._partitions

    def get_session_option(self, option_name):
        raise NotImplementedError("Netezza get_session_option not implemented.")

    def get_max_partition_size(self):
        """Return the size of the largest partition"""
        if self.is_partitioned():
            raise NotImplementedError("Netezza get_max_partition_size not implemented.")
        return None

    def is_iot(self):
        return self._iot_type == "IOT"

    def is_partitioned(self):
        return self._partitioned == "YES"

    def is_subpartitioned(self):
        return False

    @staticmethod
    def supported_data_types() -> set:
        return set(
            [
                NETEZZA_TYPE_BIGINT,
                NETEZZA_TYPE_INTEGER,
                NETEZZA_TYPE_SMALLINT,
                NETEZZA_TYPE_BYTEINT,
                NETEZZA_TYPE_DOUBLE_PRECISION,
                NETEZZA_TYPE_REAL,
                NETEZZA_TYPE_NUMERIC,
                NETEZZA_TYPE_TIME,
                NETEZZA_TYPE_TIMESTAMP,
                NETEZZA_TYPE_DATE,
                NETEZZA_TYPE_INTERVAL,
                NETEZZA_TYPE_CHARACTER_VARYING,
                NETEZZA_TYPE_CHARACTER,
                NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING,
                NETEZZA_TYPE_NATIONAL_CHARACTER,
                NETEZZA_TYPE_BOOLEAN,
                NETEZZA_TYPE_TIME_WITH_TIME_ZONE,
                NETEZZA_TYPE_BINARY_VARYING,
                NETEZZA_TYPE_ST_GEOMETRY,
            ]
        )

    def check_nanosecond_offload_allowed(
        self, backend_max_datetime_scale, allow_nanosecond_timestamp_columns=None
    ):
        raise NotImplementedError(
            "Netezza check_nanosecond_offload_allowed() not implemented."
        )

    def supported_range_partition_data_type(self, rdbms_data_type):
        raise NotImplementedError(
            "Netezza supported_range_partition_data_type() not implemented."
        )

    def supported_list_partition_data_type(self, rdbms_data_type):
        raise NotImplementedError(
            "Netezza supported_list_partition_data_type() not implemented."
        )

    @staticmethod
    def hash_bucket_unsuitable_data_types():
        return set([])

    @staticmethod
    def nan_capable_data_types():
        # TODO SS@2017-09-08 Added REAL and DOUBLE PRECISION as they seem NaN capable  (https://www.ibm.com/support/knowledgecenter/en/SSULQD_7.2.0/com.ibm.nz.dbu.doc/r_dbuser_summary_casting.html). We need to check how Netezza orders NaN in a sort and see if it is the same as Impala or the same as Oracle. If it is the same as Impala we don't these here.
        return set([NETEZZA_TYPE_DOUBLE_PRECISION, NETEZZA_TYPE_REAL])

    @staticmethod
    def datetime_literal_to_python(rdbms_literal, strict=True):
        raise NotImplementedError(
            "Netezza datetime_literal_to_python() not implemented."
        )

    @staticmethod
    def numeric_literal_to_python(rdbms_literal):
        raise NotImplementedError(
            "Netezza numeric_literal_to_python() not implemented."
        )

    def rdbms_literal_to_python(
        self, rdbms_column, rdbms_literal, partition_type, strict=True
    ):
        raise NotImplementedError("Netezza rdbms_literal_to_python() not implemented.")

    def enable_offload_by_subpartition(self, desired_state=True):
        raise NotImplementedError(
            "Netezza enable_offload_by_subpartition() not implemented."
        )

    def get_minimum_partition_key_data(self):
        """Returns lowest point of data stored in source Netezza table"""
        assert self.partition_columns
        return []

    def to_canonical_column(self, column):
        """Translate an Netezza column to an internal GOE column"""

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable & data_default forward from RDBMS
            Not carrying partition information formward to the canonical column because that will
            be defined by operational logic.
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return CanonicalColumn(
                col.name,
                data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                partition_info=None,
                bucket_info=None,
            )

        assert column
        assert isinstance(column, NetezzaColumn)

        if column.data_type in (
            NETEZZA_TYPE_CHARACTER,
            NETEZZA_TYPE_NATIONAL_CHARACTER,
        ):
            return new_column(
                column,
                GOE_TYPE_FIXED_STRING,
                data_length=column.data_length,
                safe_mapping=True,
            )
        elif column.data_type in (
            NETEZZA_TYPE_CHARACTER_VARYING,
            NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING,
            NETEZZA_TYPE_INTERVAL,
        ):
            return new_column(
                column, GOE_TYPE_VARIABLE_STRING, data_length=column.data_length
            )
        elif column.data_type in (
            NETEZZA_TYPE_BINARY_VARYING,
            NETEZZA_TYPE_ST_GEOMETRY,
        ):
            return new_column(column, GOE_TYPE_BINARY, data_length=column.data_length)
        elif column.data_type == NETEZZA_TYPE_BOOLEAN:
            return new_column(column, GOE_TYPE_BOOLEAN)
        elif column.data_type == NETEZZA_TYPE_BYTEINT:
            return new_column(column, GOE_TYPE_INTEGER_1)
        elif column.data_type == NETEZZA_TYPE_SMALLINT:
            return new_column(column, GOE_TYPE_INTEGER_2)
        elif column.data_type == NETEZZA_TYPE_INTEGER:
            return new_column(column, GOE_TYPE_INTEGER_4)
        elif column.data_type == NETEZZA_TYPE_BIGINT:
            return new_column(column, GOE_TYPE_INTEGER_8)
        elif column.data_type == NETEZZA_TYPE_NUMERIC:
            data_precision = column.data_precision
            data_scale = column.data_scale
            if data_precision is not None and data_scale is not None:
                # Process a couple of edge cases
                if data_scale > data_precision:
                    # e.g. NUMBER(3,5) scale > precision
                    data_precision = data_scale
                elif data_scale < 0:
                    # e.g. NUMBER(10,-5)
                    data_scale = 0
            if data_scale == 0:
                # Integral numbers
                if data_precision >= 1 and data_precision <= 2:
                    integral_type = GOE_TYPE_INTEGER_1
                elif data_precision >= 3 and data_precision <= 4:
                    integral_type = GOE_TYPE_INTEGER_2
                elif data_precision >= 5 and data_precision <= 9:
                    integral_type = GOE_TYPE_INTEGER_4
                elif data_precision >= 10 and data_precision <= 18:
                    integral_type = GOE_TYPE_INTEGER_8
                elif data_precision >= 19 and data_precision <= 38:
                    integral_type = GOE_TYPE_INTEGER_38
                else:
                    # The precision overflows our canonical integral types so store as a decimal
                    integral_type = GOE_TYPE_DECIMAL
                return new_column(
                    column, integral_type, data_precision=data_precision, data_scale=0
                )
            else:
                # If precision & scale are None then this is unsafe, otherwise leave it None to let new_column() logic take over
                safe_mapping = (
                    False if data_precision is None and data_scale is None else None
                )
                return new_column(
                    column,
                    GOE_TYPE_DECIMAL,
                    data_precision=data_precision,
                    data_scale=data_scale,
                    safe_mapping=safe_mapping,
                )
        elif column.data_type == NETEZZA_TYPE_REAL:
            return new_column(column, GOE_TYPE_FLOAT)
        elif column.data_type == NETEZZA_TYPE_DOUBLE_PRECISION:
            return new_column(column, GOE_TYPE_DOUBLE)
        elif column.data_type == NETEZZA_TYPE_DATE:
            return new_column(column, GOE_TYPE_DATE)
        elif column.data_type == NETEZZA_TYPE_TIME:
            return new_column(column, GOE_TYPE_TIME)
        elif column.data_type == NETEZZA_TYPE_TIMESTAMP:
            return new_column(column, GOE_TYPE_TIMESTAMP)
        elif column.data_type == NETEZZA_TYPE_TIME_WITH_TIME_ZONE:
            return new_column(column, GOE_TYPE_TIMESTAMP_TZ)
        else:
            raise NotImplementedError(
                "Unsupported Netezza data type: %s" % column.data_type
            )

    def from_canonical_column(self, column):
        # Present is not yet in scope
        raise NotImplementedError("Netezza from_canonical_column() not implemented.")

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
        return NetezzaColumn(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            hidden=hidden,
            char_semantics=char_semantics,
            char_length=char_length,
        )

    def to_rdbms_literal_with_sql_conv_fn(self, py_val, rdbms_data_type):
        raise NotImplementedError(
            "Netezza to_rdbms_literal_with_sql_conv_fn() not implemented."
        )

    def get_suitable_sample_size(self, bytes_override=None) -> int:
        # Return something even though sampling is not implemented
        return 0

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
        # Return something even though sampling is not implemented
        return []

    def supported_partition_data_types(self):
        raise NotImplementedError(
            "Netezza supported_partition_data_types() not implemented."
        )

    def offload_by_subpartition_capable(self, valid_for_auto_enable=False):
        return False

    def partition_has_rows(self, partition_name):
        raise NotImplementedError("Netezza partition_has_rows() not implemented.")

    def predicate_has_rows(self, predicate):
        raise NotImplementedError(
            self.__class__.__name__ + "." + inspect.currentframe().f_code.co_names
        )

    def predicate_to_where_clause(self, predicate, columns_override=None):
        raise NotImplementedError(
            self.__class__.__name__ + "." + inspect.currentframe().f_code.co_names
        )

    def predicate_to_where_clause_with_binds(self, predicate):
        raise NotImplementedError(
            self.__class__.__name__ + "." + inspect.currentframe().f_code.co_names
        )

    def valid_canonical_override(self, column, canonical_override):
        assert isinstance(column, NetezzaColumn)
        if isinstance(canonical_override, CanonicalColumn):
            target_type = canonical_override.data_type
        else:
            target_type = canonical_override
        if column.data_type == NETEZZA_TYPE_BOOLEAN:
            return bool(target_type == GOE_TYPE_BOOLEAN)
        elif column.data_type in [
            NETEZZA_TYPE_CHARACTER,
            NETEZZA_TYPE_NATIONAL_CHARACTER,
        ]:
            return bool(target_type == GOE_TYPE_FIXED_STRING)
        elif column.data_type in [
            NETEZZA_TYPE_CHARACTER_VARYING,
            NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING,
            NETEZZA_TYPE_INTERVAL,
        ]:
            return bool(target_type == GOE_TYPE_VARIABLE_STRING)
        elif column.data_type in [
            NETEZZA_TYPE_BINARY_VARYING,
            NETEZZA_TYPE_ST_GEOMETRY,
        ]:
            return bool(target_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])
        elif column.data_type == NETEZZA_TYPE_DOUBLE_PRECISION:
            return bool(target_type == GOE_TYPE_DOUBLE)
        elif column.data_type == NETEZZA_TYPE_REAL:
            return bool(target_type == GOE_TYPE_FLOAT)
        elif column.is_number_based():
            return target_type in NUMERIC_CANONICAL_TYPES
        elif column.is_date_based() and column.is_time_zone_based():
            return bool(target_type == GOE_TYPE_TIMESTAMP_TZ)
        elif column.is_date_based():
            return bool(
                target_type in [GOE_TYPE_DATE, GOE_TYPE_TIMESTAMP]
                or target_type in STRING_CANONICAL_TYPES
            )
        elif column.data_type == NETEZZA_TYPE_TIME:
            return bool(target_type == GOE_TYPE_TIME)
        elif target_type not in ALL_CANONICAL_TYPES:
            # Ideally we would log something here but this class has no messages object
            # self._log('Unknown canonical type in mapping: %s' % target_type, detail=VVERBOSE)
            return False
        return False

    def gen_default_numeric_column(self, column_name):
        return self.gen_column(column_name, NETEZZA_TYPE_NUMERIC)

    def gen_default_date_column(self, column_name):
        return self.gen_column(column_name, NETEZZA_TYPE_DATE)

    def transform_encrypt_data_type(self):
        return NETEZZA_TYPE_CHARACTER_VARYING

    def transform_null_cast(self, rdbms_column):
        assert isinstance(rdbms_column, NetezzaColumn)
        return "CAST(NULL AS %s)" % (rdbms_column.format_data_type())

    def transform_tokenize_data_type(self):
        return NETEZZA_TYPE_CHARACTER_VARYING

    def transform_regexp_replace_expression(
        self, backend_column, regexp_replace_pattern, regexp_replace_string
    ):
        raise NotImplementedError(
            "Netezza transform_regexp_replace_expression() not implemented."
        )

    def transform_translate_expression(self, backend_column, from_string, to_string):
        raise NotImplementedError(
            "Netezza transform_translate_expression() not implemented."
        )
