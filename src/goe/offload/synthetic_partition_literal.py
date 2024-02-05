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

""" SyntheticPartitionLiteral class for generating Python string literals for synthetic partition columns

    The methods will give the same outcome as BackendTableInterface implementations do via SQL except this class is
    focused on Python variables, not backend SQL engines.

    This initially started out as a collection of global functions but became a class because I wanted some methods to
    be private and using underscore naming for "global" functions seemed wrong.

"""

import logging
from datetime import date
import decimal
import re

from numpy import datetime64

from goe.offload.column_metadata import ColumnMetadataInterface, match_table_column
from goe.offload.offload_constants import (
    PART_COL_DATE_GRANULARITIES,
    PART_COL_GRANULARITY_MONTH,
    PART_COL_GRANULARITY_YEAR,
)
from goe.util.misc_functions import MAX_SUPPORTED_PRECISION


class SyntheticPartitionLiteralException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


def gen_number_integral_floor_literal(number_value, granularity):
    """Python literal matching the outcome of BackendTableInterface._gen_synthetic_part_number_floor_sql_expr()
    for an integral numeric synthetic column.
    """
    assert granularity
    assert isinstance(granularity, int), "Granularity must be integral not: %s" % type(
        granularity
    )
    if not isinstance(number_value, decimal.Decimal):
        decimal.getcontext().prec = MAX_SUPPORTED_PRECISION
        number_value = decimal.Decimal(str(number_value))
    floored_value = (number_value / granularity).to_integral_value(
        rounding=decimal.ROUND_FLOOR
    )
    return (floored_value * granularity).to_integral_value()


###########################################################################
# SyntheticPartitionLiteral
###########################################################################


class SyntheticPartitionLiteral(object):
    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    @staticmethod
    def _gen_date_as_string_literal(date_value, granularity):
        """Python literal matching the outcome of BackendTableInterface._gen_synthetic_part_date_as_string_sql_expr()"""
        assert granularity in PART_COL_DATE_GRANULARITIES, (
            "Unexpected granularity: %s" % granularity
        )

        if date_value is None:
            # Compare to None because Unix epoch is False in datetime64
            return None

        assert isinstance(date_value, (date, datetime64))
        date_string = str(date_value)[:10]
        assert re.match(r"^\d{4}-\d\d?-\d\d?$", date_string)
        yyyy, mm, dd = date_string.split("-")
        component_fn_list = [["YEAR", yyyy], ["MONTH", mm], ["DAY", dd]]

        partition_expr = []
        for component_fn, size_or_literal in component_fn_list:
            partition_expr.append(size_or_literal)
            if granularity == component_fn[0]:
                break
        return "-".join(partition_expr)

    @staticmethod
    def _gen_date_truncated_literal(date_value, granularity):
        """Python literal matching the outcome of
        BackendTableInterface._gen_synthetic_partition_date_truncated_sql_expr()
        """
        assert isinstance(
            date_value, (date, datetime64)
        ), "%s is not of type (date, datetime64)" % type(date_value)
        assert granularity in PART_COL_DATE_GRANULARITIES, (
            "Unexpected granularity: %s" % granularity
        )

        if isinstance(date_value, date):
            date_truncated = date_value.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            if granularity == PART_COL_GRANULARITY_MONTH:
                date_truncated = date_truncated.replace(day=1)
            elif granularity == PART_COL_GRANULARITY_YEAR:
                date_truncated = date_truncated.replace(month=1, day=1)
            return date_truncated
        elif isinstance(date_value, datetime64):
            # Use astype() to truncate to granularity and then switch back to day granularity
            date_truncated = date_value.astype(
                "datetime64[{}]".format(granularity)
            ).astype("datetime64[D]")
            return date_truncated

    @staticmethod
    def _gen_number_integral_literal(number_value, granularity):
        """Python literal matching the outcome of BackendTableInterface._gen_synthetic_part_number_floor_sql_expr()
        for an integral numeric synthetic column.
        """
        return int(gen_number_integral_floor_literal(number_value, granularity))

    @staticmethod
    def _gen_number_string_literal(
        number_value, granularity, synthetic_partition_digits
    ):
        """Python literal matching the outcome of BackendTableInterface._gen_synthetic_part_number_floor_sql_expr()
        for a string synthetic column
        """
        floored_value = gen_number_integral_floor_literal(number_value, granularity)
        return "{:0>{width}}".format(
            str(floored_value), width=synthetic_partition_digits
        )

    @staticmethod
    def _gen_string_literal(string_value, granularity):
        """Python literal matching the outcome of BackendTableInterface._gen_synthetic_part_string_sql_expr()"""
        assert granularity
        assert isinstance(
            granularity, int
        ), "Granularity must be integral not: %s" % type(granularity)
        if isinstance(string_value, datetime64):
            string_value = str(string_value).replace("T", " ")
        elif isinstance(string_value, date):
            string_value = str(string_value)
        return string_value[:granularity]

    @staticmethod
    def _gen_synthetic_literal_function(partition_column, source_column):
        """Returns a Python function to achieve the same outcome as BackendTableInterface implementations would in SQL.
        For example a month granularity fn that converts:
            datetime64('2011-01-01 12:12:12.1234') -> '2011-01'
        """
        assert isinstance(partition_column, ColumnMetadataInterface)
        assert isinstance(source_column, ColumnMetadataInterface)

        source_column_name = partition_column.partition_info.source_column_name
        if source_column.name.upper() != (source_column_name or "").upper():
            raise SyntheticPartitionLiteralException(
                "Source column mismatch: %s != %s"
                % (source_column.name, source_column_name)
            )

        digits = partition_column.partition_info.digits
        granularity = partition_column.partition_info.granularity
        partition_fn = None
        if source_column.is_date_based() and partition_column.is_date_based():
            partition_fn = (
                lambda x: SyntheticPartitionLiteral._gen_date_truncated_literal(
                    x, granularity
                )
            )
        elif source_column.is_date_based() and partition_column.is_string_based():
            partition_fn = (
                lambda x: SyntheticPartitionLiteral._gen_date_as_string_literal(
                    x, granularity
                )
            )
        elif source_column.is_string_based():
            g = int(granularity)
            partition_fn = lambda x: SyntheticPartitionLiteral._gen_string_literal(x, g)
        elif source_column.is_number_based() and partition_column.is_string_based():
            g = int(granularity)
            partition_fn = (
                lambda x: SyntheticPartitionLiteral._gen_number_string_literal(
                    x, g, digits
                )
            )
        elif source_column.is_number_based() and partition_column.is_number_based():
            g = int(granularity)
            partition_fn = (
                lambda x: SyntheticPartitionLiteral._gen_number_integral_literal(x, g)
            )
        else:
            raise NotImplementedError(
                "Unsupported synthetic partition column data type: %s"
                % source_column.data_type
            )
        return partition_fn

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @staticmethod
    def gen_synthetic_literal_function(partition_column, table_columns):
        """Generate a Python function which can be used to create a synthetic partition literal
        partition_column can be a column name, which must exist in table_columns, or a column object.
        """
        if isinstance(partition_column, ColumnMetadataInterface):
            part_col_obj = partition_column
        else:
            part_col_obj = match_table_column(partition_column, table_columns)
        source_column_name = part_col_obj.partition_info.source_column_name
        source_column = match_table_column(source_column_name, table_columns)
        return SyntheticPartitionLiteral._gen_synthetic_literal_function(
            part_col_obj, source_column
        )

    @staticmethod
    def gen_synthetic_literal(partition_column, table_columns, source_value):
        conv_fn = SyntheticPartitionLiteral.gen_synthetic_literal_function(
            partition_column, table_columns
        )
        return conv_fn(source_value)
