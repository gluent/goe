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

""" TeradataPartitionExpression: Parse a Teradata partition check constraint into it's component parts.
    We only support offload of top-level RANGE_N() schemes so CASE_N() is only in the grammer in order to ignore it.
"""

import logging

import lark
from lark.exceptions import UnexpectedInput

from goe.offload.offload_source_table import (
    OFFLOAD_PARTITION_TYPE_LIST,
    OFFLOAD_PARTITION_TYPE_RANGE,
)


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# CONSTANTS
###########################################################################

PARTITION_TYPE_COLUMNAR = "COLUMNAR"

GRAMMAR = r"""
    %import common.WS
    %import common.NUMBER
    %import common.INT
    %ignore WS

    check_constraint: "CHECK" "(" _COMMENT? _supported_check ( "AND" _supported_check )* ")"
    _supported_check: _range_n_check | _range_n_check_in_parentheses | _columnar_check | _case_n_check_in_parentheses

    match_range_n: "RANGE_N" "(" _column_name "BETWEEN" _range_n_range ( "," _range_n_out_of_range )? ")"
    _range_n_check: match_range_n _COMMENT? _range_n_end?
    _range_n_check_in_parentheses: "(" match_range_n ")" _COMMENT? _range_n_end?
    _range_n_range: ( range_n_datetime_range ( "," range_n_datetime_range )* ) | ( range_n_number_range ( "," range_n_number_range )* )

    _columnar_check: match_columnar _COMMENT _END_COLUMNAR
    match_columnar: COLUMNAR_PSEUDO_COLUMN

    _column_name: "\""? COLUMN_NAME "\""? | COLUMN_NAME
    _start_datetime: DATE | TIMESTAMP | TIMESTAMP_TZ | TIMESTAMP_FRACTIONAL | TIMESTAMP_TZ_FRACTIONAL
    _end_datetime: DATE | TIMESTAMP | TIMESTAMP_TZ | TIMESTAMP_FRACTIONAL | TIMESTAMP_TZ_FRACTIONAL
    _interval_literal: INTERVAL_LITERAL

    range_n_datetime_range: _start_datetime "AND" _end_datetime "EACH" _interval_literal
    _range_n_out_of_range: "NO RANGE" | "UNKNOWN" | ( "NO RANGE" "OR" "UNKNOWN" ) | ( "NO RANGE" "," "UNKNOWN" )
    _range_n_end: "IS NOT NULL" | "BETWEEN" _INT "AND" _INT

    _start_number: INT
    _end_number: INT
    _step_number: INT
    range_n_number_range: _start_number "AND" _end_number "EACH" _step_number

    match_case_n: "CASE_N" "(" _case_n_supported_comparisons ( "," _case_n_supported_comparisons )* ( "," _case_n_unknown )? ")"
    _case_n_supported_comparisons: _case_n_literal_comparison | _case_n_between_datetime
    _case_n_literal_comparison: _column_name _OPERATOR _case_n_literal
    _case_n_literal: NUMBER | DATE | TIMESTAMP
    _case_n_between_datetime: _column_name "BETWEEN" case_n_datetime_range ( "," case_n_datetime_range )*
    case_n_datetime_range: _start_datetime "AND" _end_datetime
    _case_n_unknown: "UNKNOWN"
    _case_n_check_in_parentheses: "(" match_case_n ")" _case_n_end?
    _case_n_end: "BETWEEN" _INT "AND" _INT

    _COMMENT: /\/\*.*?\*\//
    _INT: INT
    QUOTED_INTEGER: "'" INT "'"
    DATE: /DATE '\d{4}-\d{2}-\d{2}'/
    TIMESTAMP: /TIMESTAMP '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d'/
    TIMESTAMP_TZ: /TIMESTAMP '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d[+-]00:00'/
    TIMESTAMP_FRACTIONAL: /TIMESTAMP '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d{1,9}'/
    TIMESTAMP_TZ_FRACTIONAL: /TIMESTAMP '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d{1,9}[+-]00:00'/
    INTERVAL_LITERAL: /INTERVAL '\d' (MONTH|DAY|HOUR)/
    INTERVAL: "INTERVAL"
    MONTH: "MONTH"
    COLUMN_NAME: /[a-zA-Z0-9\.\$\#_]+/i
    COLUMNAR_PSEUDO_COLUMN: /PARTITION\#L[1-5]/
    _END_COLUMNAR: "=1"
    _OPERATOR: /[=<>]/
"""

# These constants need to match token in above grammar
RANGE_NODE = "match_range_n"
CASE_NODE = "match_case_n"
COLUMNAR_NODE = "match_columnar"

NODE_PARTITION_TYPES = {
    RANGE_NODE: OFFLOAD_PARTITION_TYPE_RANGE,
    CASE_NODE: OFFLOAD_PARTITION_TYPE_LIST,
    COLUMNAR_NODE: PARTITION_TYPE_COLUMNAR,
}


class UnsupportedCaseNPartitionExpression(Exception):
    pass


class UnsupportedPartitionExpression(Exception):
    pass


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def get_parser():
    return lark.Lark(GRAMMAR, start="check_constraint")


###########################################################################
# TeradataPartitionExpression
###########################################################################


class TeradataPartitionExpression:
    def __init__(self, constraint_text: str):
        """Take DBC.PartitioningConstraintsV.ConstraintText, parse to AST and expsose via attributes:
        partition_type: RANGE, LIST. COLUMNAR is skipped and not exposed via the class.
        column: The column used as source for partition expression.
        pseudo_column: 'PARTIITON#Ln'.
        ranges: list[str] of ranges for a RANGE_N expression:
            [start SQL literal, end SQL literal, interval SQL literal]
        """
        assert isinstance(constraint_text, str)
        try:
            self._ast = get_parser().parse(constraint_text)
        except UnexpectedInput as exc:
            raise UnsupportedPartitionExpression(
                f"Unsupported partition expression: {constraint_text}"
            ) from exc

        self.column = None
        self.partition_type = None
        self.pseudo_column = None
        self.ranges = None

        matched_nodes = [_.data for _ in self._ast.children]
        if not matched_nodes or not set(matched_nodes).issubset(
            {RANGE_NODE, COLUMNAR_NODE}
        ):
            # Currently we only support RANGE_N or RANGE_N combined with columnar.
            if COLUMNAR_NODE in matched_nodes:
                raise UnsupportedPartitionExpression(
                    f"Only single RANGE_N partition expressions combined with columnar partitioning are supported: {constraint_text}"
                )
            elif CASE_NODE in matched_nodes:
                raise UnsupportedCaseNPartitionExpression(
                    f"CASE_N partition expressions are not currently supported ({matched_nodes}): {constraint_text}"
                )
            else:
                raise UnsupportedPartitionExpression(
                    f"Only single RANGE_N partition expressions are supported ({matched_nodes}): {constraint_text}"
                )

        if RANGE_NODE not in matched_nodes and COLUMNAR_NODE in matched_nodes:
            # Columnar alone so we can drop out here without ranges.
            self.partition_type = PARTITION_TYPE_COLUMNAR
            return

        # Pick up the first RANGE_N node.
        range_node_index = matched_nodes.index(RANGE_NODE)
        fn_node = self._ast.children[range_node_index]
        self.partition_type = NODE_PARTITION_TYPES[fn_node.data]
        self.pseudo_column = "PARTITION#L" + str(range_node_index + 1)

        # The RANGE_N node only has 2 children: a column and a list of between expressions
        self.column = fn_node.children[0].value

        ranges = []
        for range_node in fn_node.children[1:]:
            if range_node.data not in (
                "range_n_datetime_range",
                "range_n_number_range",
            ):
                continue
            # We expect 3 children of ranges nodes
            if len(range_node.children) != 3:
                raise UnsupportedPartitionExpression(
                    "RANGE_N BETWEEN expression does not contain 3 elements: {}".format(
                        [_.value for _ in range_node.children]
                    )
                )
            ranges.append([_.value for _ in range_node.children])

        self.ranges = ranges
