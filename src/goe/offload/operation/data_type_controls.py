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

"""data_type_controls: Library of functions used in GOE to process data type control options/structures."""

from goe.offload.column_metadata import (
    CanonicalColumn,
    CANONICAL_TYPE_OPTION_NAMES,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_FLOAT,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_VARIABLE_STRING,
    get_column_names,
    match_table_column,
)
from goe.offload.offload_functions import expand_columns_csv
from goe.offload.offload_constants import (
    INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
)
from goe.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from goe.offload.offload_source_table import DATA_SAMPLE_SIZE_AUTO
from goe.offload.operation.not_null_columns import apply_not_null_columns_csv
from goe.offload.oracle.oracle_column import ORACLE_TYPE_BINARY_FLOAT


class OffloadDataTypeControlsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

BACKEND_DATA_TYPE_CONTROL_OPTIONS = {
    GOE_TYPE_INTEGER_1: "--integer-1-columns",
    GOE_TYPE_INTEGER_2: "--integer-2-columns",
    GOE_TYPE_INTEGER_4: "--integer-4-columns",
    GOE_TYPE_INTEGER_8: "--integer-8-columns",
    GOE_TYPE_INTEGER_38: "--integer-38-columns",
    GOE_TYPE_DOUBLE: "--double-columns",
    GOE_TYPE_VARIABLE_STRING: "--variable-string-columns",
}

CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT = "Data type conflict for columns"

DECIMAL_COL_TYPE_SYNTAX_TEMPLATE = 'must be of format "precision,scale" where 1<=precision<={p} and 0<=scale<={s} and scale<=precision'

###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def canonical_columns_from_columns_csv(
    data_type,
    column_list_csv,
    existing_canonical_list,
    reference_columns,
    precision=None,
    scale=None,
):
    """Return a list of CanonicalColumn objects based on an incoming data type and CSV of column names"""
    if not column_list_csv:
        return []
    column_list = expand_columns_csv(column_list_csv, reference_columns)
    conflicting_columns = [
        _.name for _ in existing_canonical_list if _.name in column_list
    ]
    if conflicting_columns:
        raise OffloadDataTypeControlsException(
            "%s %s when assigning type with %s"
            % (
                CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT,
                conflicting_columns,
                CANONICAL_TYPE_OPTION_NAMES[data_type],
            )
        )
    canonical_list = [
        CanonicalColumn(
            _, data_type, data_precision=precision, data_scale=scale, from_override=True
        )
        for _ in column_list
    ]
    if "*" in column_list_csv and not canonical_list:
        raise OffloadDataTypeControlsException(
            f"No columns match pattern: {column_list_csv}"
        )
    return canonical_list


def char_semantics_override_map(unicode_string_columns_csv, reference_columns):
    """Return a dictionary map of char semantics overrides"""
    if not unicode_string_columns_csv:
        return {}
    unicode_string_columns_list = expand_columns_csv(
        unicode_string_columns_csv, reference_columns
    )
    if "*" in unicode_string_columns_csv and not unicode_string_columns_list:
        raise OffloadDataTypeControlsException(
            f"No columns match pattern: {unicode_string_columns_csv}"
        )
    for col in unicode_string_columns_list:
        rdbms_column = match_table_column(col, reference_columns)
        if not rdbms_column.is_string_based():
            raise OffloadDataTypeControlsException(
                "%s %s: %s is not string based"
                % (
                    INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
                    rdbms_column.name,
                    rdbms_column.data_type,
                )
            )
    return dict(
        zip(
            unicode_string_columns_list,
            [CANONICAL_CHAR_SEMANTICS_UNICODE] * len(unicode_string_columns_list),
        )
    )


def date_min_max_incompatible(offload_source_table, offload_target_table):
    """Return True if it's possible for the source data to hold dates that are incompatible
    with the target system
    Currently only looking at the min when checking this. We could add max checks to this
    in the future
    """
    if (
        offload_source_table.min_datetime_value()
        < offload_target_table.min_datetime_value()
    ):
        return True
    return False


def offload_source_to_canonical_mappings(
    offload_source_table,
    offload_target_table,
    offload_operation,
    not_null_propagation,
    messages: OffloadMessages,
):
    """Take source/RDBMS columns and translate them into intermediate canonical columns.
    This could be from:
        User specified options: canonical_overrides.
        Source schema attributes: offload_source_table.to_canonical_column().
        Data type sampling: offload_source_table.sample_rdbms_data_types().
    """
    canonical_overrides = offload_operation.gen_canonical_overrides(
        offload_target_table, columns_override=offload_source_table.columns
    )
    sample_candidate_columns = []
    messages.log(
        "Canonical overrides: {}".format(
            str([(_.name, _.data_type) for _ in canonical_overrides])
        ),
        detail=VVERBOSE,
    )
    canonical_mappings = {}
    for tab_col in offload_source_table.columns:
        new_col = offload_source_table.to_canonical_column_with_overrides(
            tab_col, canonical_overrides
        )
        canonical_mappings[tab_col.name] = new_col

        if not offload_target_table.canonical_float_supported() and (
            tab_col.data_type == ORACLE_TYPE_BINARY_FLOAT
            or new_col.data_type == GOE_TYPE_FLOAT
        ):
            raise OffloadDataTypeControlsException(
                "4 byte binary floating point data cannot be offloaded to this backend system: %s"
                % tab_col.name
            )

        if (
            new_col.is_number_based() or new_col.is_date_based()
        ) and new_col.safe_mapping is False:
            messages.log(
                "Sampling column because of unsafe mapping: %s" % tab_col.name,
                detail=VVERBOSE,
            )
            sample_candidate_columns.append(tab_col)

        elif (
            new_col.is_number_based()
            and tab_col.data_precision is not None
            and (tab_col.data_precision - tab_col.data_scale)
            > offload_target_table.max_decimal_integral_magnitude()
        ):
            # precision - scale allows data beyond that supported by the backend system, fall back on sampling
            messages.log(
                "Sampling column because precision-scale > %s: %s"
                % (offload_target_table.max_decimal_integral_magnitude(), tab_col.name),
                detail=VVERBOSE,
            )
            sample_candidate_columns.append(tab_col)

        elif (
            new_col.is_number_based()
            and new_col.data_scale
            and new_col.data_scale > offload_target_table.max_decimal_scale()
        ):
            # Scale allows data beyond that supported by the backend system, fall back on sampling.
            # Use new_col above so that any user override of the scale is taken into account.
            messages.log(
                "Sampling column because scale > %s: %s"
                % (offload_target_table.max_decimal_scale(), tab_col.name),
                detail=VVERBOSE,
            )
            sample_candidate_columns.append(tab_col)

        elif new_col.is_date_based() and date_min_max_incompatible(
            offload_source_table, offload_target_table
        ):
            messages.log(
                "Sampling column because of system date boundary incompatibility: %s"
                % tab_col.name,
                detail=VVERBOSE,
            )
            sample_candidate_columns.append(tab_col)

    if sample_candidate_columns and offload_operation.data_sample_pct:
        # Some columns require sampling to determine a data type
        if offload_operation.data_sample_pct == DATA_SAMPLE_SIZE_AUTO:
            data_sample_pct = offload_source_table.get_suitable_sample_size()
            messages.log("Sampling %s%% of table" % data_sample_pct, detail=VERBOSE)
        else:
            data_sample_pct = offload_operation.data_sample_pct
        messages.log(
            "Sampling columns: %s" % str(get_column_names(sample_candidate_columns)),
            detail=VERBOSE,
        )
        sampled_rdbms_cols = offload_source_table.sample_rdbms_data_types(
            sample_candidate_columns,
            data_sample_pct,
            offload_operation.data_sample_parallelism,
            offload_target_table.min_datetime_value(),
            offload_target_table.max_decimal_integral_magnitude(),
            offload_target_table.max_decimal_scale(),
            offload_operation.allow_decimal_scale_rounding,
        )
        if sampled_rdbms_cols:
            for tab_col in sampled_rdbms_cols:
                # Overwrite any previous canonical mapping with the post sampled version
                canonical_mappings[
                    tab_col.name
                ] = offload_source_table.to_canonical_column(tab_col)

        report_data_type_control_options_to_simulate_sampling(
            sampled_rdbms_cols, canonical_mappings, messages
        )

    # Process any char semantics overrides
    for col_name, char_semantics in char_semantics_override_map(
        offload_operation.unicode_string_columns_csv, offload_source_table.columns
    ).items():
        canonical_mappings[col_name].char_semantics = char_semantics
        canonical_mappings[col_name].from_override = True

    # Get a fresh list of canonical columns in order to maintain column order
    canonical_columns = [
        canonical_mappings[_.name] for _ in offload_source_table.columns
    ]

    canonical_columns = apply_not_null_columns_csv(
        canonical_columns,
        offload_operation.not_null_columns_csv,
        not_null_propagation,
        offload_source_table.get_column_names(),
        messages,
    )

    messages.log("Canonical columns:", detail=VVERBOSE)
    [messages.log(str(_), detail=VVERBOSE) for _ in canonical_columns]
    return canonical_columns


def report_data_type_control_options_to_simulate_sampling(
    sampled_rdbms_cols, canonical_mappings, messages: OffloadMessages
):
    """Users requested the outcome of sampling should be logged to avoid resampling if an offload is re-run
    An example of when this is useful is running an offload in preview mode and then running in execute mode,
    this will run sampling twice and in some cases can be frustrating
    """
    if not sampled_rdbms_cols:
        return

    sampled_canonical_columns = [canonical_mappings[_.name] for _ in sampled_rdbms_cols]
    canonical_types_used = sorted(
        list(set(_.data_type for _ in sampled_canonical_columns))
    )

    messages.notice(
        "Data types were identified by sampling data for columns: "
        + ", ".join(get_column_names(sampled_canonical_columns))
    )
    messages.debug("Canonical types used: %s" % str(canonical_types_used))
    messages.log(
        "Sampled column/data type detail, use explicit data type controls (--*-columns) to override these values:"
    )
    for used_type in canonical_types_used:
        if used_type == GOE_TYPE_DECIMAL:
            # Need to use a combination of options for each precision/scale combo
            ps_tuples = list(
                set(
                    [
                        (_.data_precision, _.data_scale)
                        for _ in sampled_canonical_columns
                        if _.data_type == used_type
                    ]
                )
            )
            messages.debug(
                "Decimal precision/scale combinations used: %s" % str(ps_tuples)
            )
            for p, s in ps_tuples:
                column_name_csv = ",".join(
                    sorted(
                        [
                            _.name
                            for _ in sampled_canonical_columns
                            if _.data_type == used_type
                            and _.data_precision == p
                            and _.data_scale == s
                        ]
                    )
                )
                messages.log(
                    "--decimal-columns=%s --decimal-columns-type=%s,%s"
                    % (column_name_csv, p, s)
                )
        else:
            column_name_csv = ",".join(
                sorted(
                    [
                        _.name
                        for _ in sampled_canonical_columns
                        if _.data_type == used_type
                    ]
                )
            )
            if used_type in BACKEND_DATA_TYPE_CONTROL_OPTIONS:
                messages.log(
                    "%s=%s"
                    % (BACKEND_DATA_TYPE_CONTROL_OPTIONS[used_type], column_name_csv)
                )
            else:
                messages.log("%s: %s" % (used_type, column_name_csv))
