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

""" offload_functions: Library of functions used in goe.py and other offload related modules
"""

# NOTE: the idea of this module is to avoid dependency loops so be careful when importing
#       from other goelib modules

from datetime import datetime, date
import inspect
import math
from numpy import datetime64
import re

from goe.config.orchestration_defaults import get_load_db_pattern
from goe.offload import offload_constants
from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    get_column_names,
    valid_column_list,
    invalid_column_list_message,
)
from goe.offload.offload_metadata_functions import (
    flatten_lpa_individual_high_values,
    incremental_hv_csv_from_list,
)
from goe.offload.predicate_offload import GenericPredicate, create_or_relation_predicate
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
)
from goe.util.misc_functions import (
    backtick_sandwich,
    case_insensitive_in,
    csv_split,
    substitute_in_same_case,
    wildcard_matches_in_list,
)


class OffloadFunctionException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Valid re for --older-than-date. Matches against human input as well as data
# therefore needs to match both single and double digit MM & DD
OLDER_THAN_DATE_PATTERN_RE = r"^\d{4}-\d\d?-\d\d?$"
# Captures any string starting in a valid date ymd
STARTS_WITH_DATE_PATTERN_RE = r"^\d{4}-\d\d-\d\d.*"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def data_db_name(owner, opts):
    """Generates a data db name from the target backend db name.
    Tries to keep case consistent, i.e. if the target name is all in upper
    case then we try to have the data db name all in upper case.
    """
    new_db_name = substitute_in_same_case(opts.db_name_pattern, owner)
    return new_db_name


def load_db_name(target_owner, offload_options=None):
    """Generates a load db name from the target backend db name.
    Tries to keep case consistent, i.e. if the target name is all in upper
    case then we try to have the load db name all in upper case.
    Mostly offload_options.load_db_name_pattern is the same as load_db_name_pattern_default()
    but for some tests different values are assigned to offload_options.load_db_name_pattern.
    """
    assert target_owner
    load_db_pattern = (
        offload_options.load_db_name_pattern
        if offload_options
        else get_load_db_pattern()
    )
    load_db_name = substitute_in_same_case(load_db_pattern, target_owner)
    return load_db_name


def convert_backend_identifier_case(case_option, *args):
    """Convert the case of a number of backend identifiers to match BACKEND_IDENTIFIER_CASE.
    Any number of identifiers can be passed in allowing for combinations such as:
        convert_backend_identifier_case(options, db_name)
        convert_backend_identifier_case(options, db_name, table_name)
        convert_backend_identifier_case(options, db_name, load_db_name)
    The first option can be an options object or the attribute itself:
        convert_backend_identifier_case(options.backend_identifier_case, db_name, load_db_name)
    """
    assert args
    if hasattr(case_option, "backend_identifier_case"):
        case_option = case_option.backend_identifier_case
    if case_option and case_option.upper() == "LOWER":
        conv_fn = lambda x: x.lower()
    elif case_option and case_option.upper() == "UPPER":
        conv_fn = lambda x: x.upper()
    else:
        conv_fn = lambda x: x
    if len(args) == 1:
        return conv_fn(args[0])
    else:
        return tuple(conv_fn(_) for _ in args)


def expand_columns_csv(columns_csv, reference_columns, retain_non_matching_names=False):
    """Splits a data type column csv into a list and expands any * wildcards in column_csv based on matches in columns.
    Wildcards use case insensitive matching.
    retain_non_matching_names: Setting this to True will not match against the reference list for tokens with no
                               wildcard. An example of where this is useful is --measures in present where we
                               include NULL as a measure and don't want to lose it. It is also useful if you want
                               to retain invalid input in the list to later throw an exception about it.
    Returns a list of column names.
    """
    if columns_csv is None:
        return []
    column_names = csv_split(columns_csv)
    if not reference_columns:
        return column_names
    if isinstance(reference_columns[0], ColumnMetadataInterface):
        reference_names = get_column_names(reference_columns)
    else:
        reference_names = reference_columns
    new_columns = []
    for pattern in column_names:
        if retain_non_matching_names and "*" not in pattern:
            if pattern in reference_names:
                new_columns.append(pattern)
            elif case_insensitive_in(pattern, reference_names):
                # There is a column match but with a different case so take the reference name.
                match_list = [
                    _ for _ in reference_names if _.upper() == pattern.upper()
                ]
                new_columns.append(match_list[0])
            else:
                new_columns.append(pattern)
        else:
            matches = wildcard_matches_in_list(
                pattern, reference_names, case_sensitive=False
            )
            if not matches and retain_non_matching_names:
                new_columns.append(pattern)
            else:
                new_columns.extend(matches)
    return new_columns


def hvs_to_backend_sql_literals(
    threshold_cols, threshold_values, ipa_predicate_type, to_literal_fn
):
    """For HVs that will be used as literals (not binds) in SQL we need to convert them
    to an appropriate value, for example strings should be quoted as appropriate for the backend.
    The data type from threshold_cols is used as a second parameter to the to_literal_fn
    This function does that while also maintaining the outer structure of the HVs,
    i.e. LIST table HVs are a list of tuples. RANGE table HVs are just a tuple.
    """
    assert to_literal_fn
    assert inspect.isfunction(to_literal_fn) or inspect.ismethod(
        to_literal_fn
    ), "Invalid function type %s" % type(to_literal_fn)
    new_threshold_values = []
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        data_type = threshold_cols[0].data_type if threshold_cols else None
        for val_tuple in threshold_values:
            backend_tuple = tuple([to_literal_fn(val, data_type) for val in val_tuple])
            new_threshold_values.append(backend_tuple)
    else:
        new_threshold_values = tuple(
            [
                to_literal_fn(val, col.data_type)
                for col, val in zip(threshold_cols, threshold_values)
            ]
        )
    return new_threshold_values


def hybrid_threshold_clauses(
    lower_threshold_cols,
    upper_threshold_cols,
    offload_high_values,
    equality_with_gt=True,
    col_quoting_char='"',
    as_predicate_dsl=False,
    col_enclosure_fn=None,
):
    """Return predicates to be used in the hybrid view UNION ALL when it is based on a single high value (RANGE partitioning)
    Unit test in tests/offload/test_offload_functions.py
    col_quoting_char: Characters used to enclose column identifiers
    col_enclosure_fn: Function used to enclose column identifiers, supercedes col_quoting_char
    Return tuple:
        (predicate for lower part of UNION, predicate for upper part of UNION)
    """

    def quote_fn(col_name):
        if as_predicate_dsl:
            return col_name
        elif col_enclosure_fn:
            return col_enclosure_fn(col_name)
        elif col_quoting_char:
            return backtick_sandwich(col_name, col_quoting_char)
        else:
            return col_name

    def get_dsl_literal_name(col):
        if col.is_number_based():
            return "numeric"
        elif col.is_date_based():
            return "datetime"
        else:
            return "string"

    def format_dsl_hv(col, hv):
        if col.is_date_based():
            return str(hv).replace("T", " ")
        elif col.is_string_based():
            return backtick_sandwich(hv, '"')
        else:
            return str(hv)

    def format_predicate(col, op, hv):
        if as_predicate_dsl:
            return "(column(%s) %s %s(%s))" % (
                col.name,
                op,
                get_dsl_literal_name(col),
                format_dsl_hv(col, hv),
            )
        else:
            return "%s %s %s" % (quote_fn(col.name.upper()), op, hv)

    def dsl_and_join(exp, join_term="AND"):
        """Recursively nest ANDed (or ORed) expressions in ()"""
        if len(exp) > 1:
            return "({} {} {})".format(
                dsl_and_join(exp[:-1], join_term=join_term), join_term, exp[-1]
            )
        else:
            return exp[-1]

    def join_expression_strings(expressions, join_term="AND"):
        assert expressions
        assert isinstance(expressions, list)
        assert isinstance(expressions[0], str)
        if as_predicate_dsl:
            clause = dsl_and_join(expressions, join_term=join_term)
        else:
            clause = " {} ".format(join_term).join(expressions)
            if len(expressions) > 1:
                # Bracket any ANDed expressions into a single expression
                clause = "(" + clause + ")"
        return clause

    def join_expression_tuples(expressions):
        assert expressions
        assert isinstance(expressions, list)
        assert isinstance(expressions[0], tuple)
        return join_expression_strings(
            [format_predicate(col, cmp, hv) for (col, cmp, hv) in expressions]
        )

    assert valid_column_list(lower_threshold_cols), invalid_column_list_message(
        lower_threshold_cols
    )
    assert valid_column_list(upper_threshold_cols), invalid_column_list_message(
        upper_threshold_cols
    )
    assert isinstance(offload_high_values, (list, tuple))
    assert len(lower_threshold_cols) == len(upper_threshold_cols)
    assert len(lower_threshold_cols) == len(offload_high_values)

    # the top half of the UNION ALL is of format:
    #   (
    #       col1 > hv1
    #   OR (col1 = hv1 AND col2 > hv2)
    #   OR (col1 = hv1 AND col2 = hv2 AND col3 > hv3)
    #   OR (col1 = hv1 AND col2 = hv2 AND col3 = hv3)
    #   )
    # the equality line "col1 = hv1 AND col2 = hv2 AND col3 = hv3" can move to bottom half if
    # hv2 or hv3 are MAXVALUE or if equality_with_gt == False

    union_inclusivity_top = equality_with_gt

    if offload_constants.PART_OUT_OF_RANGE in offload_high_values:
        # if MAXVALUE present (multi-col partitioning) then the equality moves to lower part of UNION
        offload_high_values = offload_high_values[
            : offload_high_values.index(offload_constants.PART_OUT_OF_RANGE)
        ]
        union_inclusivity_top = False

    view_pcol_list = [
        (col, hv) for col, hv in zip(upper_threshold_cols, list(offload_high_values))
    ]

    if len(view_pcol_list) == 1:
        col, hv = view_pcol_list[0]
        op = ">=" if union_inclusivity_top else ">"
        view_threshold_clause = format_predicate(col, op, hv)
    else:
        view_threshold_clauses = []

        for i in range(len(view_pcol_list)):
            eq_list = [(col, "=", hv) for (col, hv) in view_pcol_list[0:i] if i > 0]
            gt_list = [(col, ">", hv) for (col, hv) in [view_pcol_list[i]]]
            view_threshold_clauses.append(join_expression_tuples(eq_list + gt_list))

        if union_inclusivity_top:
            view_threshold_clauses.append(
                join_expression_tuples([(col, "=", hv) for (col, hv) in view_pcol_list])
            )

        view_threshold_clause = join_expression_strings(
            view_threshold_clauses, join_term="OR"
        )
        if len(view_threshold_clauses) > 1 and not as_predicate_dsl:
            view_threshold_clause = "(" + view_threshold_clause + ")"

    # by default (i.e. no MAXVALUE complications) the bottom half of the UNION ALL is of format:
    #   (
    #       col1 < hv1
    #   OR (col1 = hv1 AND col2 < hv2)
    #   OR (col1 = hv1 AND col2 = hv2 AND col3 < hv3)
    #   )

    ext_pcol_list = [
        (col, hv) for col, hv in zip(lower_threshold_cols, list(offload_high_values))
    ]

    if len(ext_pcol_list) == 1:
        col, hv = ext_pcol_list[0]
        op = "<" if union_inclusivity_top else "<="
        ext_threshold_clause = format_predicate(col, op, hv)
    else:
        ext_threshold_clauses = []

        for i in range(len(ext_pcol_list)):
            eq_list = [(col, "=", hv) for (col, hv) in ext_pcol_list[0:i] if i > 0]
            lt_list = [(col, "<", hv) for (col, hv) in [ext_pcol_list[i]]]
            ext_threshold_clauses.append(join_expression_tuples(eq_list + lt_list))

        if not union_inclusivity_top:
            ext_threshold_clauses.append(
                join_expression_tuples([(col, "=", hv) for (col, hv) in ext_pcol_list])
            )

        ext_threshold_clause = join_expression_strings(
            ext_threshold_clauses, join_term="OR"
        )
        if len(ext_threshold_clauses) > 1 and not as_predicate_dsl:
            ext_threshold_clause = "(" + ext_threshold_clause + ")"

    return ext_threshold_clause, view_threshold_clause


def hybrid_view_list_clauses(
    lower_threshold_cols,
    upper_threshold_cols,
    incremental_high_values,
    col_quoting_char='"',
    in_list_chunk_size=1000,
    col_enclosure_fn=None,
):
    """Return predicates to be used in the hybrid view UNION ALL when it is based on a list of values (LIST partitioning)
    Unit test in tests/offload/test_offload_functions.py
    Return tuple:
        (predicate for the RDBMS side, predicate for the external table side)
    """

    def quote_fn(col_name):
        if col_enclosure_fn:
            return col_enclosure_fn(col_name)
        elif col_quoting_char:
            return backtick_sandwich(col_name, col_quoting_char)
        else:
            return col_name

    assert valid_column_list(lower_threshold_cols), invalid_column_list_message(
        lower_threshold_cols
    )
    assert valid_column_list(upper_threshold_cols), invalid_column_list_message(
        upper_threshold_cols
    )
    assert len(lower_threshold_cols) == len(upper_threshold_cols)
    assert (
        len(lower_threshold_cols) == 1
    ), "Multi column LIST partitioning is not supported"
    # incremental_high_values should always be a list of tuples
    assert isinstance(incremental_high_values, list), "Type %s is not list" % type(
        incremental_high_values
    )
    if incremental_high_values:
        assert isinstance(
            incremental_high_values[0], tuple
        ), "Type %s is not tuple" % type(incremental_high_values[0])

    num_chunks = int(
        math.ceil(len(incremental_high_values) / float(in_list_chunk_size))
    )
    sub_in_lists = []
    for chunk in range(num_chunks):
        chunk_index = chunk * in_list_chunk_size
        hv_subset = incremental_high_values[
            chunk_index : chunk_index + in_list_chunk_size
        ]
        hv_csv = incremental_hv_csv_from_list(
            flatten_lpa_individual_high_values(hv_subset)
        )
        sub_in_lists.append(hv_csv)

    # the top half of the UNION ALL is of format:
    #   (col NOT IN (val1, val2, valn))
    upper_threshold_clause = " AND ".join(
        "%s NOT IN (%s)" % (quote_fn(upper_threshold_cols[0].name.upper()), _)
        for _ in sub_in_lists
    )
    upper_threshold_clause = "(" + upper_threshold_clause + ")"

    # the lower half of the UNION ALL is of format:
    #   (col IN (val1, val2, valn))
    lower_threshold_clause = " OR ".join(
        "%s IN (%s)" % (quote_fn(lower_threshold_cols[0].name.upper()), _)
        for _ in sub_in_lists
    )
    lower_threshold_clause = "(" + lower_threshold_clause + ")"

    return lower_threshold_clause, upper_threshold_clause


def get_hybrid_threshold_clauses(
    lower_cols,
    upper_cols,
    ipa_predicate_type,
    incremental_high_values,
    col_quoting_char='"',
    col_enclosure_fn=None,
):
    if ipa_predicate_type in (
        INCREMENTAL_PREDICATE_TYPE_RANGE,
        INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
    ):
        return hybrid_threshold_clauses(
            lower_cols,
            upper_cols,
            incremental_high_values,
            col_quoting_char=col_quoting_char,
            col_enclosure_fn=col_enclosure_fn,
        )
    elif ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        return hybrid_view_list_clauses(
            lower_cols,
            upper_cols,
            incremental_high_values,
            col_quoting_char=col_quoting_char,
            col_enclosure_fn=col_enclosure_fn,
        )
    elif ipa_predicate_type in (
        INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
    ):
        return hybrid_threshold_clauses(
            lower_cols,
            upper_cols,
            incremental_high_values,
            equality_with_gt=False,
            col_quoting_char=col_quoting_char,
            col_enclosure_fn=col_enclosure_fn,
        )
    else:
        return "", ""


def get_dsl_threshold_clauses(
    cols, ipa_predicate_type, incremental_high_values, equality_with_gt=None
):
    if ipa_predicate_type in (
        INCREMENTAL_PREDICATE_TYPE_RANGE,
        INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
        INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
    ):
        if ipa_predicate_type in (
            INCREMENTAL_PREDICATE_TYPE_RANGE,
            INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
        ):
            # With no override equality_with_gt should be True for less-than predicate
            equality_with_gt = True if equality_with_gt is None else equality_with_gt
        else:
            # With no override equality_with_gt should be False for less-than-or-equal predicate
            equality_with_gt = False if equality_with_gt is None else equality_with_gt
        return hybrid_threshold_clauses(
            cols,
            cols,
            incremental_high_values,
            as_predicate_dsl=True,
            equality_with_gt=equality_with_gt,
        )
    else:
        raise NotImplementedError(
            "get_dsl_threshold_clauses() is not implemented for INCREMENTAL_PREDICATE_TYPE: %s"
            % ipa_predicate_type
        )


def get_hybrid_predicate_clauses(
    offload_predicates, table_object, columns_override=None
):
    """Return predicates to be used in the hybrid view UNION ALL when --offload-predicates are in use.
    columns_override allows an alternative column object list to be used (join pushdown)
    Return tuple:
        (predicate for lower (external table) part of UNION, predicate for upper (RDBMS source) part of UNION)
    """
    assert hasattr(table_object, "predicate_to_where_clause")
    assert isinstance(offload_predicates, list)
    if offload_predicates:
        assert isinstance(offload_predicates[0], GenericPredicate)

    if offload_predicates:
        hybrid_predicate = table_object.predicate_to_where_clause(
            create_or_relation_predicate(offload_predicates),
            columns_override=columns_override,
        )
        exclude_clause = "NOT (%s)" % hybrid_predicate
        remote_clause = "(%s)" % hybrid_predicate
        return remote_clause, exclude_clause
    else:
        return "", ""


def hybrid_view_combine_hv_and_pred_clauses(hv_clause, pred_clause, join_term):
    """Trivial function to combine hyrbid view threshold clauses with offload predicate
    clauses but gives consistent formatting for the few places that do this.
    """
    assert join_term in ["AND", "OR"]
    assert isinstance(hv_clause, str)
    assert isinstance(pred_clause, str)
    combined = hv_clause if hv_clause else ""
    combined += ("\n" + join_term + " ") if hv_clause and pred_clause else ""
    combined += pred_clause if pred_clause else ""
    if hv_clause and pred_clause:
        combined = "(" + combined + ")"
    return combined


def datetime_literal_to_python(dt_literal):
    """Take a human/RDBMS supplied datetime literal/value and convert to a Python value"""
    if isinstance(dt_literal, datetime64):
        return dt_literal
    elif isinstance(dt_literal, date):
        return datetime64(dt_literal)
    elif dt_literal.upper() == offload_constants.PART_OUT_OF_RANGE:
        return datetime64(datetime.max)
    elif dt_literal.upper() == offload_constants.PART_OUT_OF_LIST:
        return dt_literal
    elif re.match(OLDER_THAN_DATE_PATTERN_RE, dt_literal):
        return datetime64(dt_literal)
    elif re.match(STARTS_WITH_DATE_PATTERN_RE, dt_literal):
        try:
            return datetime64(dt_literal)
        except ValueError:
            raise OffloadFunctionException(
                "Failed to parse datetime value: %s (%s)"
                % (dt_literal, type(dt_literal))
            )
    else:
        raise OffloadFunctionException(
            "Failed to parse datetime value: %s (%s)" % (dt_literal, type(dt_literal))
        )
