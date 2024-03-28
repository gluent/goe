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

""" Functions for validating backend partition controls in Offload
"""

from optparse import OptionValueError
import re

from goe.offload.column_metadata import (
    ColumnPartitionInfo,
    is_synthetic_partition_column,
    match_table_column,
)
from goe.offload.offload_constants import PART_COL_GRANULARITY_DAY
from goe.offload.offload_functions import expand_columns_csv
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_source_table import (
    OFFLOAD_PARTITION_TYPE_HASH,
    OFFLOAD_PARTITION_TYPE_LIST,
)
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_INTERVAL_DS,
    ORACLE_TYPE_INTERVAL_YM,
)
from goe.util.misc_functions import case_insensitive_in, csv_split


class OffloadPartitionControlsException(Exception):
    pass


MISSING_PARTITION_GRANULARITY_EXCEPTION_TEXT = (
    "Partition granularity (--partition-granularity) mandatory for column/data type"
)
OFFLOAD_CHUNK_COLUMN_PART_COL_EXCEPTION_TEXT = (
    "Unknown partition column supplied as --offload-chunk-column"
)
PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT = (
    "String columns cannot be used to partition the backend table"
)
PARTITION_BY_STRING_PARTITION_FUNCTION_SUFFIX = (
    " without corresponding partition functions"
)
PARTITION_FUNCTIONS_ELEMENT_EXCEPTION_TEXT = "elements to match partition columns"
PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT = (
    "Offload partition functions are not supported"
)
TOO_MANY_BACKEND_PART_COLS_EXCEPTION_TEXT = (
    "Too many partition columns for backend system"
)


def empty_strings_to_none(list_of_strings):
    return [None if _ == "" else _ for _ in list_of_strings]


def default_offload_partition_columns_from_rdbms(
    rdbms_partition_columns,
    rdbms_partition_type,
    offload_partition_functions,
    backend_table,
    messages,
):
    """Default partition columns from RDBMS partition columns"""

    def seed_notice(data_type_desc, return_cols, messages, middle_text=None):
        seed_message = f"{data_type_desc} RDBMS partition columns cannot be used to partition the backend table"
        if middle_text:
            seed_message += middle_text
        if not return_cols:
            seed_message += ". Table will be offloaded to a non-partitioned table."
        messages.notice(seed_message, detail=VERBOSE)

    if not rdbms_partition_columns or not backend_table.partition_by_column_supported():
        return []
    if rdbms_partition_columns and rdbms_partition_type == OFFLOAD_PARTITION_TYPE_HASH:
        messages.notice(
            "Backend partition columns not seeded by RDBMS partition columns for %s partitioned table"
            % OFFLOAD_PARTITION_TYPE_HASH,
            detail=VERBOSE,
        )
        return []
    return_cols = [
        _.name
        for _ in rdbms_partition_columns
        if _.data_type not in (ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM)
    ]
    if rdbms_partition_columns and [
        _
        for _ in rdbms_partition_columns
        if _.data_type in (ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM)
    ]:
        seed_notice("INTERVAL", return_cols, messages)
    if (
        not backend_table.partition_by_string_supported()
        and return_cols
        and not offload_partition_functions
    ):
        # If offload_partition_functions are being specified then we cannot reject string partitioning just yet.
        # If a function is being used on the string column its type may change to a supported type.
        return_cols = [
            _.name for _ in rdbms_partition_columns if not _.is_string_based()
        ]
        pf_text = (
            PARTITION_BY_STRING_PARTITION_FUNCTION_SUFFIX
            if backend_table.goe_partition_functions_supported()
            else ""
        )
        if [_ for _ in rdbms_partition_columns if _.is_string_based()]:
            seed_notice("Character-based", return_cols, messages, middle_text=pf_text)
    return return_cols


def default_granularity_for_matching_rdbms_list_partition_key(
    rdbms_col, rdbms_partition_columns, rdbms_partition_type, messages, date_as_string
):
    """RDBMS list partition keys should be safe to have a granularity resulting in value parity"""
    new_granularity = None
    if rdbms_partition_type == OFFLOAD_PARTITION_TYPE_LIST and match_table_column(
        rdbms_col.name, rdbms_partition_columns
    ):
        if rdbms_col.is_number_based():
            new_granularity = "1"
        elif rdbms_col.is_string_based():
            # use data_length (not char length) so we don't shorten multi-byte values
            new_granularity = str(rdbms_col.data_length)
        elif rdbms_col.is_date_based() and not date_as_string:
            # No default if date based column is being offloaded to a string
            new_granularity = PART_COL_GRANULARITY_DAY
    if new_granularity:
        messages.notice(
            "Defaulting granularity to %s for %s partition column: %s"
            % (new_granularity, OFFLOAD_PARTITION_TYPE_LIST, rdbms_col.name),
            detail=VERBOSE,
        )
    return new_granularity


def derive_partition_digits(backend_table):
    """Find common partition digits used across a set of partition columns."""
    if not backend_table.get_partition_columns():
        return None
    digits_in_use = []
    for partition_column in backend_table.get_partition_columns():
        if partition_column.partition_info:
            source_column = match_table_column(
                partition_column.partition_info.source_column_name,
                backend_table.get_columns(),
            )
            if source_column.is_number_based():
                digits_in_use.append(partition_column.partition_info.digits)
    digits_in_use = list(set(digits_in_use))
    assert len(digits_in_use) <= 1, "Incompatible padding in synthetic partition cols"
    return digits_in_use[0] if len(digits_in_use) == 1 else None


def offload_options_to_partition_info(
    offload_partition_columns,
    offload_partition_functions,
    offload_partition_granularity,
    offload_partition_lower_value,
    offload_partition_upper_value,
    synthetic_partition_digits,
    rdbms_column,
    backend_table,
):
    if not offload_partition_columns:
        return None
    assert isinstance(offload_partition_columns, list)
    if offload_partition_functions:
        assert isinstance(offload_partition_functions, list)
    if offload_partition_granularity:
        assert isinstance(offload_partition_granularity, list)

    partition_info = None
    partition_column_match = case_insensitive_in(
        rdbms_column.name, offload_partition_columns
    )
    if partition_column_match and backend_table.partition_by_column_supported():
        position = offload_partition_columns.index(partition_column_match)
        if rdbms_column.is_date_based():
            digits = 1
        elif (
            rdbms_column.is_number_based()
            and not backend_table.synthetic_partition_numbers_are_string()
        ):
            digits = None
        else:
            digits = synthetic_partition_digits
        granularity = (
            offload_partition_granularity[position]
            if offload_partition_granularity
            else None
        )
        partition_function = (
            offload_partition_functions[position]
            if offload_partition_functions
            else None
        )
        partition_info = ColumnPartitionInfo(
            position=position,
            source_column_name=rdbms_column.name,
            granularity=granularity,
            function=partition_function,
            digits=digits,
            range_start=offload_partition_lower_value,
            range_end=offload_partition_upper_value,
        )
    return partition_info


def offloading_date_as_string(
    rdbms_column, backend_column, variable_string_columns_csv
):
    """Return True if we are offloading an RDBMS date to a backend string"""
    if rdbms_column.is_date_based():
        if backend_column:
            if backend_column.is_string_based():
                return True
        elif variable_string_columns_csv and case_insensitive_in(
            rdbms_column.name, csv_split(variable_string_columns_csv)
        ):
            return True
    return False


def validate_offload_partition_columns(
    offload_partition_columns,
    rdbms_columns,
    rdbms_partition_columns,
    rdbms_partition_type,
    offload_partition_functions,
    backend_table,
    messages,
    offload_chunk_column,
):
    """Default and validate requested backend partition columns"""
    if offload_partition_columns:
        # User defined partition columns
        new_partition_columns = expand_columns_csv(
            offload_partition_columns.upper(),
            rdbms_columns,
            retain_non_matching_names=True,
        )
    else:
        new_partition_columns = default_offload_partition_columns_from_rdbms(
            rdbms_partition_columns,
            rdbms_partition_type,
            offload_partition_functions,
            backend_table,
            messages,
        )

    if offload_chunk_column:
        if not [
            pcol
            for pcol in (new_partition_columns or [])
            if pcol.upper() == offload_chunk_column
        ]:
            raise OptionValueError(
                "%s: %s"
                % (OFFLOAD_CHUNK_COLUMN_PART_COL_EXCEPTION_TEXT, offload_chunk_column)
            )

    if not new_partition_columns:
        return

    if len(new_partition_columns) > backend_table.max_partition_columns():
        raise OffloadPartitionControlsException(
            "%s: %s"
            % (
                TOO_MANY_BACKEND_PART_COLS_EXCEPTION_TEXT,
                ",".join(new_partition_columns),
            )
        )
    return new_partition_columns


def validate_offload_partition_functions(
    offload_partition_functions, offload_partition_columns, backend_table, messages
):
    """Validate partition functions offload option has valid contents.
    Cannot check functions are compatible with the partition source column(s) because this will be called too early,
    but we can check it exists and has a valid structure.
    Returns a normalised value for offload_partition_functions, e.g. it may comes in as a string and leave as a list
    """
    if offload_partition_columns:
        assert isinstance(
            offload_partition_columns, list
        ), "Type {} is not list".format(type(offload_partition_columns))
    if offload_partition_functions:
        assert isinstance(
            offload_partition_functions, (str, list)
        ), "Type {} is not str or list".format(type(offload_partition_functions))

    if (
        offload_partition_functions
        and not backend_table.goe_partition_functions_supported()
    ):
        raise OffloadPartitionControlsException(
            "%s on %s"
            % (
                PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT,
                backend_table.backend_db_name(),
            )
        )

    if not offload_partition_columns:
        if offload_partition_functions:
            messages.log(
                "Ignoring partition functions because there are no partition columns",
                detail=VVERBOSE,
            )
        return []
    elif not offload_partition_functions:
        return [None for _ in range(len(offload_partition_columns))]

    if isinstance(offload_partition_functions, str):
        # For a first time offload this will be a CSV string and need splitting.
        offload_partition_functions = csv_split(offload_partition_functions)

    offload_partition_functions = empty_strings_to_none(offload_partition_functions)

    if len(offload_partition_columns) != len(offload_partition_functions):
        raise OffloadPartitionControlsException(
            'Offload partition functions "%s" should have %d %s: %s'
            % (
                ",".join(offload_partition_functions),
                len(offload_partition_columns),
                PARTITION_FUNCTIONS_ELEMENT_EXCEPTION_TEXT,
                ",".join(offload_partition_columns),
            )
        )

    new_partition_functions = []
    for part_function in offload_partition_functions:
        # Validate the partition function
        if part_function:
            if "." in part_function:
                func_db, func_name = part_function.split(".")
            else:
                func_db = backend_table.default_udf_db_name()
                if not func_db:
                    raise OffloadPartitionControlsException(
                        "Offload partition function is missing %s name: %s"
                        % (backend_table.db_name_label(initcap=True), part_function)
                    )
                func_name = part_function
            backend_table.check_partition_function(func_db, func_name)
            new_partition_functions.append(func_db + "." + func_name)
        else:
            new_partition_functions.append(None)

    return new_partition_functions


def validate_offload_partition_granularity(
    offload_partition_granularity,
    offload_partition_columns,
    offload_partition_functions,
    rdbms_columns,
    rdbms_partition_columns,
    rdbms_partition_type,
    backend_table,
    messages,
    variable_string_columns_csv,
    backend_columns=None,
):
    if offload_partition_columns:
        assert isinstance(offload_partition_columns, list)
        assert isinstance(offload_partition_functions, list)
    else:
        return None
    if offload_partition_granularity:
        assert isinstance(offload_partition_granularity, str)

    if offload_partition_granularity:
        granularity_list = csv_split(offload_partition_granularity)
        # Check user specified granularity matches partition column count
        if len(offload_partition_columns) != len(granularity_list):
            raise OffloadPartitionControlsException(
                'Backend partition granularity "%s" should have %d elements (%s)'
                % (
                    offload_partition_granularity,
                    len(offload_partition_columns),
                    ",".join(offload_partition_columns),
                )
            )
        granularity_list = empty_strings_to_none(granularity_list)
    else:
        granularity_list = [None for _ in range(len(offload_partition_columns))]

    # Validate/default granularity list contents
    new_partition_granularity = []
    for col_name, granularity, part_fn in zip(
        offload_partition_columns, granularity_list, offload_partition_functions
    ):
        rdbms_col = match_table_column(col_name, rdbms_columns)
        if not rdbms_col:
            raise OffloadPartitionControlsException(
                "Offload partitioning column not found: %s" % col_name
            )
        backend_column = match_table_column(col_name, backend_columns or [])

        date_as_string = False
        if not part_fn:
            # If there's a partition function we don't yet know what data type the backend column will be
            date_as_string = offloading_date_as_string(
                rdbms_col, backend_column, variable_string_columns_csv
            )
            pf_text = (
                PARTITION_BY_STRING_PARTITION_FUNCTION_SUFFIX
                if backend_table.goe_partition_functions_supported()
                else ""
            )
            if (
                rdbms_col.is_string_based() or date_as_string
            ) and not backend_table.partition_by_string_supported():
                raise OffloadPartitionControlsException(
                    "%s%s: %s"
                    % (
                        PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT,
                        pf_text,
                        col_name,
                    )
                )
            if (
                backend_column
                and backend_column.is_string_based()
                and not backend_table.partition_by_string_supported()
            ):
                raise OffloadPartitionControlsException(
                    "%s%s: %s"
                    % (
                        PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT,
                        pf_text,
                        col_name,
                    )
                )

        if not granularity:
            granularity = default_granularity_for_matching_rdbms_list_partition_key(
                rdbms_col,
                rdbms_partition_columns,
                rdbms_partition_type,
                messages,
                date_as_string,
            )

        if rdbms_col.is_date_based() and not granularity:
            # No default if date based column is being offloaded to a string
            if not date_as_string:
                granularity = backend_table.default_date_based_partition_granularity()

        if not granularity and (
            backend_table.partition_function_requires_granularity() or not part_fn
        ):
            raise OffloadPartitionControlsException(
                "%s: %s/%s%s"
                % (
                    MISSING_PARTITION_GRANULARITY_EXCEPTION_TEXT,
                    rdbms_col.name,
                    rdbms_col.data_type,
                    " (as variable string)" if date_as_string else "",
                )
            )

        # Check that specified granularity matches the column data type
        if (
            rdbms_col.is_date_based()
            and granularity
            not in backend_table.supported_date_based_partition_granularities()
            and not date_as_string
        ):
            raise OffloadPartitionControlsException(
                "Partition granularity %s not valid for column/data type: %s/%s"
                % (granularity, rdbms_col.name, rdbms_col.data_type)
            )
        elif (
            rdbms_col.is_number_based() or rdbms_col.is_string_based() or date_as_string
        ) and not re.search(r"^\d+$", granularity):
            raise OffloadPartitionControlsException(
                "Partition granularity %s not valid for column/data type: %s/%s"
                % (granularity, rdbms_col.name, rdbms_col.data_type)
            )

        new_partition_granularity.append(granularity)
    return new_partition_granularity


def part_cols_have_matching_synthetic_expression(left_part_col, right_part_col):
    """Identify when two synthetic columns can be a direct comparison in join pushdown.
    Doesn't use source_column_name because that could be different, we have to trust the user when they
    chose the source columns as join columns.
    """
    if not left_part_col or not right_part_col:
        return False
    if not is_synthetic_partition_column(
        left_part_col
    ) or not is_synthetic_partition_column(right_part_col):
        return False
    # We have partition columns on both sides of the join clause, check for detail match...
    return bool(
        left_part_col.partition_info.granularity
        == right_part_col.partition_info.granularity
        and left_part_col.partition_info.digits == right_part_col.partition_info.digits
        and left_part_col.partition_info.function
        == right_part_col.partition_info.function
    )
