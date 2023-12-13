#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" MSSQLFrontendTestingApi: An extension of (not yet created) FrontendApi used purely for code relating to the setup,
    processing and verification of integration tests.
    LICENSE_TEXT
"""

import datetime
import logging
from typing import Optional, Union

from goe.offload.column_metadata import (
    CanonicalColumn,
    GLUENT_TYPE_BINARY,
    GLUENT_TYPE_DATE,
    GLUENT_TYPE_DECIMAL,
    GLUENT_TYPE_DOUBLE,
    GLUENT_TYPE_FIXED_STRING,
    GLUENT_TYPE_FLOAT,
    GLUENT_TYPE_INTEGER_1,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_INTEGER_4,
    GLUENT_TYPE_INTEGER_8,
    GLUENT_TYPE_INTEGER_38,
    GLUENT_TYPE_INTERVAL_DS,
    GLUENT_TYPE_INTERVAL_YM,
    GLUENT_TYPE_LARGE_BINARY,
    GLUENT_TYPE_LARGE_STRING,
    GLUENT_TYPE_TIME,
    GLUENT_TYPE_TIMESTAMP,
    GLUENT_TYPE_TIMESTAMP_TZ,
    GLUENT_TYPE_VARIABLE_STRING,
)
from goe.offload.microsoft.mssql_column import (
    MSSQLColumn,
    MSSQL_TYPE_BIGINT,
    MSSQL_TYPE_DATE,
    MSSQL_TYPE_DATETIMEOFFSET,
    MSSQL_TYPE_DATETIME2,
    MSSQL_TYPE_VARCHAR,
    MSSQL_TYPE_DECIMAL,
    MSSQL_TYPE_NUMERIC,
)
from goe.offload.offload_messages import VERBOSE
from tests.testlib.test_framework.frontend_testing_api import (
    FrontendTestingApiInterface,
)
from tests.testlib.test_framework.test_value_generators import TestDecimal


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# MSSQLFrontendTestingApi
###########################################################################


class MSSQLFrontendTestingApi(FrontendTestingApiInterface):
    def __init__(
        self,
        frontend_type,
        connection_options,
        messages,
        existing_connection=None,
        dry_run=False,
        do_not_connect=False,
        trace_action=None,
    ):
        """CONSTRUCTOR"""
        super().__init__(
            frontend_type,
            connection_options,
            messages,
            existing_connection=existing_connection,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            trace_action=trace_action,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _create_new_testing_client(
        self, existing_connection, trace_action_override=None
    ) -> FrontendTestingApiInterface:
        return MSSQLFrontendTestingApi(
            self._frontend_type,
            self._connection_options,
            self._messages,
            existing_connection=existing_connection,
            dry_run=self._dry_run,
            trace_action=trace_action_override,
        )

    def _data_type_supports_precision_and_scale(self, column_or_type):
        if isinstance(column_or_type, MSSQLColumn):
            data_type = column_or_type.data_type
        else:
            data_type = column_or_type
        return bool(data_type in (MSSQL_TYPE_DECIMAL, MSSQL_TYPE_NUMERIC))

    def _gl_chars_column_definitions(
        self, ascii_only=False, all_chars_notnull=False, supported_canonical_types=None
    ) -> list:
        raise NotImplementedError(
            "MSSQL _gl_chars_column_definitions() not yet implemented"
        )

    def _gl_type_mapping_column_definitions(
        self,
        max_backend_precision,
        max_backend_scale,
        max_decimal_integral_magnitude,
        filter_column=None,
    ):
        raise NotImplementedError(
            "MSSQL _gl_type_mapping_column_definitions() not yet implemented"
        )

    def _gl_types_column_definitions(
        self,
        all_chars_ascii7=False,
        all_chars_notnull=False,
        backend_supports_canonical_float=True,
        include_interval_columns=True,
    ) -> list:
        raise NotImplementedError(
            "MSSQL _gl_types_column_definitions() not yet implemented"
        )

    def _gl_wide_column_definitions(
        self,
        ascii_only=False,
        all_chars_notnull=False,
        supported_canonical_types=None,
        backend_max_test_column_count=None,
    ) -> list:
        raise NotImplementedError(
            "MSSQL _gl_wide_column_definitions() not yet implemented"
        )

    def _populate_generated_test_table(
        self, schema, table_name, columns, rows, fastexecute=False
    ):
        raise NotImplementedError(
            "MSSQL _populate_generated_test_table() not yet implemented"
        )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def collect_table_stats_sql_text(self, schema, table_name) -> str:
        raise NotImplementedError(
            "MSSQL collect_table_stats_sql_text() not yet implemented"
        )

    def remove_table_stats_sql_text(self, schema, table_name) -> str:
        raise NotImplementedError(
            "MSSQL remove_table_stats_sql_text() not yet implemented"
        )

    def drop_table(self, schema, table_name):
        """Obviously this is dangerous, that's why it is in this TestingApi only."""
        try:
            return self._db_api.execute_ddl(
                f"DROP TABLE {schema}.{table_name}", log_level=VERBOSE
            )
        except Exception as exc:
            if "not exist" in str(exc):
                # Nothing to drop
                pass
            else:
                self._log("Drop table exception: {}".format(str(exc)), detail=VERBOSE)
                raise

    def expected_channels_offload_predicates(self):
        """Return a list of tuples of Gluent offload predicates and expected frontend predicate"""
        # TODO this has been lifted from one of other frontends so may need adjusting.
        return [
            (
                "(column(CHANNEL_ID) = numeric(10)) AND (column(CHANNEL_ID) < numeric(2.2))",
                '("CHANNEL_ID" = 10 AND "CHANNEL_ID" < 2.2)',
            ),
            (
                "(column(CHANNEL_ID) IS NULL) AND (column(CHANNEL_ID) IS NOT NULL)",
                '("CHANNEL_ID" IS NULL AND "CHANNEL_ID" IS NOT NULL)',
            ),
            (
                "(column(CHANNEL_ID) is null) AND (column(CHANNEL_ID) is not null)",
                '("CHANNEL_ID" IS NULL AND "CHANNEL_ID" IS NOT NULL)',
            ),
            (
                "(column(CHANNEL_ID) = numeric(1234567890123456789012345))",
                '"CHANNEL_ID" = 1234567890123456789012345',
            ),
            (
                "(column(CHANNEL_ID) = numeric(-1234567890123456789012345))",
                '"CHANNEL_ID" = -1234567890123456789012345',
            ),
            (
                "(column(channel_id) = numeric(0.00000000000000000001))",
                '"CHANNEL_ID" = 0.00000000000000000001',
            ),
            (
                "(column(CHANNEL_ID) = numeric(-0.00000000000000000001))",
                '"CHANNEL_ID" = -0.00000000000000000001',
            ),
            (
                "(column(CHANNEL_ID) in (numeric(-10),numeric(0),numeric(10)))",
                '"CHANNEL_ID" IN (-10, 0, 10)',
            ),
            (
                'column(CHANNEL_DESC) = string("Internet")',
                "\"CHANNEL_DESC\" = 'Internet'",
            ),
            (
                '(column(CHANNEL_DESC) = string("Internet"))',
                "\"CHANNEL_DESC\" = 'Internet'",
            ),
            (
                '(column(ALIAS.CHANNEL_DESC) = string("Internet"))',
                '"ALIAS"."CHANNEL_DESC" = \'Internet\'',
            ),
            (
                'column(CHANNEL_DESC) = string("column(CHANNEL_DESC)")',
                "\"CHANNEL_DESC\" = 'column(CHANNEL_DESC)'",
            ),
            (
                'column(CHANNEL_DESC) NOT IN (string("A"),string("B"),string("C"))',
                "\"CHANNEL_DESC\" NOT IN ('A', 'B', 'C')",
            ),
            (
                '(column(CHANNEL_DESC) = string("Internet"))',
                "\"CHANNEL_DESC\" = 'Internet'",
            ),
        ]

    def expected_sales_offload_predicates(self):
        raise NotImplementedError(
            "MSSQL expected_sales_offload_predicates() not yet implemented"
        )

    def gen_ctas_from_subquery(
        self,
        schema: str,
        table_name: str,
        subquery: str,
        pk_col_name: Optional[str] = None,
        table_parallelism: Optional[str] = None,
        with_drop: bool = True,
        with_stats_collection: bool = False,
    ) -> list:
        raise NotImplementedError("MSSQL gen_ctas_from_subquery() not yet implemented")

    def get_test_table_owner(self, expected_schema: str, table_name: str) -> str:
        raise NotImplementedError("MSSQL get_test_table_owner() not yet implemented")

    def gl_type_mapping_generated_table_col_specs(
        self,
        max_backend_precision,
        max_backend_scale,
        max_decimal_integral_magnitude,
        supported_canonical_types,
        ascii_only=False,
    ):
        raise NotImplementedError(
            "MSSQL gl_type_mapping_generated_table_col_specs() not yet implemented"
        )

    def host_compare_sql_projection(self, column_list: list) -> str:
        """Return a SQL projection (CSV of column expressions) used to validate offloaded data.
        Because of systems variations all date based values must be normalised to:
            'YYYY-MM-DD HH24:MI:SS.FFF TZH:TZM'.
        """
        assert isinstance(column_list, list)
        projection = []
        for column in column_list:
            if column.data_type == MSSQL_TYPE_DATETIMEOFFSET:
                projection.append(
                    "REPLACE(CONVERT({},{},127),'Z',' ')".format(
                        MSSQL_TYPE_VARCHAR, self._db_api.enclose_identifier(column.name)
                    )
                )
            elif column.is_date_based():
                # CONVERT() style 21:
                #   yyyy-mm-dd hh:mi:ss.mmm (24h)
                projection.append(
                    "CONVERT({},{},21)".format(
                        MSSQL_TYPE_VARCHAR, self._db_api.enclose_identifier(column.name)
                    )
                )
            elif column.is_number_based():
                # CONVERT() style 3:
                #   Always 17 digits. Use for lossless conversion.  With this style, every distinct float
                #   or real value is guaranteed to convert to a distinct character string.
                projection.append(
                    "CONVERT({},{},3)".format(
                        MSSQL_TYPE_VARCHAR, self._db_api.enclose_identifier(column.name)
                    )
                )
            else:
                projection.append(self._db_api.enclose_identifier(column.name))
        return ",".join(projection)

    def run_sql_in_file(self, local_path):
        raise NotImplementedError("MSSQL run_sql_in_file() not yet implemented")

    def sales_based_fact_create_ddl(
        self,
        schema: str,
        table_name: str,
        maxval_partition: bool = False,
        extra_pred: Optional[str] = None,
        degree: Optional[int] = None,
        subpartitions: int = 0,
        enable_row_movement: bool = False,
        noseg_partition: bool = True,
        part_key_type: Optional[str] = None,
        time_id_column_name: Optional[str] = None,
        extra_col_tuples: Optional[list] = None,
        simple_partition_names: bool = False,
        with_drop: bool = True,
    ) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_fact_create_ddl() pending implementation"
        )

    def sales_based_fact_add_partition_ddl(self, schema: str, table_name: str) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_fact_add_partition_ddl() pending implementation"
        )

    def sales_based_fact_drop_partition_ddl(
        self, schema: str, table_name: str, hv_string_list: Optional[list] = None
    ) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_fact_drop_partition_ddl() pending implementation"
        )

    def sales_based_fact_truncate_partition_ddl(
        self,
        schema: str,
        table_name: str,
        hv_string_list: Optional[list] = None,
        dropping_oldest: Optional[bool] = None,
    ) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_fact_truncate_partition_ddl() pending implementation"
        )

    def sales_based_fact_hwm_literal(self, sales_literal: str, data_type: str) -> tuple:
        raise NotImplementedError(
            "MSSQL sales_based_fact_hwm_literal() pending implementation"
        )

    def sales_based_fact_late_arriving_data_sql(
        self, schema: str, table_name: str, time_id_literal: str
    ):
        raise NotImplementedError(
            "MSSQL sales_based_fact_late_arriving_data_sql() pending implementation"
        )

    def sales_based_list_fact_create_ddl(
        self,
        schema: str,
        table_name: str,
        default_partition: bool = False,
        extra_pred: Optional[str] = None,
        part_key_type: Optional[str] = None,
        out_of_sequence: bool = False,
        include_older_partition: bool = False,
        yrmon_column_name: Optional[str] = None,
        extra_col_tuples: Optional[list] = None,
        with_drop: bool = True,
    ) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_list_fact_create_ddl() pending implementation"
        )

    def sales_based_list_fact_add_partition_ddl(
        self, schema: str, table_name: str, next_ym_override: Optional[tuple] = None
    ) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_list_fact_add_partition_ddl() pending implementation"
        )

    def sales_based_list_fact_late_arriving_data_sql(
        self, schema: str, table_name: str, time_id_literal: str, yrmon_string: str
    ) -> list:
        raise NotImplementedError(
            "MSSQL sales_based_list_fact_late_arriving_data_sql() pending implementation"
        )

    def select_grant_exists(
        self,
        schema: str,
        table_name: str,
        to_user: str,
        grantable: Optional[bool] = None,
    ) -> bool:
        raise NotImplementedError("MSSQL select_grant_exists() not yet implemented")

    def table_row_count_from_stats(
        self, schema: str, table_name: str
    ) -> Union[int, None]:
        raise NotImplementedError(
            "MSSQL table_row_count_from_stats() not yet implemented"
        )

    def test_type_canonical_int_8(self) -> str:
        return MSSQL_TYPE_BIGINT

    def test_type_canonical_date(self) -> str:
        return MSSQL_TYPE_DATE

    def test_type_canonical_decimal(self) -> str:
        return MSSQL_TYPE_DECIMAL

    def test_type_canonical_string(self) -> str:
        return MSSQL_TYPE_VARCHAR

    def test_type_canonical_timestamp(self) -> str:
        return MSSQL_TYPE_DATETIME2

    def test_time_zone_query_option(self, tz) -> dict:
        raise NotImplementedError(
            "MSSQL test_time_zone_query_option() not yet implemented"
        )

    def unit_test_query_options(self):
        # TODO nj@2021-07-23 If we properly implement MSSQL then we need to fill this out
        return None
