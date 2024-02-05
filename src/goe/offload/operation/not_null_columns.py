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

"""
not_null_columns: Library of functions used in GOE to process NOT NULL control options/structures.
"""

from typing import TYPE_CHECKING, Union

from goe.offload.offload_constants import NOT_NULL_PROPAGATION_NONE
from goe.offload.offload_functions import expand_columns_csv
from goe.offload.offload_messages import VVERBOSE
from goe.util.misc_functions import case_insensitive_in

if TYPE_CHECKING:
    from goe.offload.column_metadata import ColumnMetadataInterface
    from goe.offload.offload_messages import OffloadMessages


class OffloadNotNullControlsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

UNKNOWN_NOT_NULL_COLUMN_EXCEPTION_TEXT = "Unknown columns specified for NOT NULL"


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def apply_not_null_columns_csv(
    canonical_columns: list,
    not_null_columns_csv: Union[str, None],
    not_null_propagation: str,
    rdbms_column_names: list,
    messages: "OffloadMessages",
) -> "list[ColumnMetadataInterface]":
    """
    Applies --not-null-columns to a list of canonical columns and returns a new list, does not mutate original.
    """
    if not_null_columns_csv:
        # Set nullable to True on all columns and then set it to False for specified list.
        new_columns = []
        not_null_names = not_null_columns_csv_to_not_null_columns(
            not_null_columns_csv, rdbms_column_names
        )
        for column in canonical_columns:
            new_column = column.clone(nullable=True)
            if case_insensitive_in(column.name, not_null_names):
                new_column.nullable = False
            new_columns.append(new_column)
        return new_columns
    elif not_null_propagation == NOT_NULL_PROPAGATION_NONE:
        # Set nullable to True on all columns.
        messages.log(
            f"Setting all columns nullable=True due to not_null_columns: {NOT_NULL_PROPAGATION_NONE}",
            detail=VVERBOSE,
        )
        return [_.clone(nullable=True) for _ in canonical_columns]
    else:
        # Do nothing and let nullable pass through unhindered.
        return canonical_columns


def not_null_columns_csv_to_not_null_columns(
    not_null_columns_csv: str, rdbms_column_names: list
) -> Union[list, str]:
    assert isinstance(not_null_columns_csv, (str, type(None)))
    nn_columns = []
    if not_null_columns_csv:
        # The user gave us a list so use that and ensure all stated columns exist.
        nn_columns = expand_columns_csv(
            not_null_columns_csv, rdbms_column_names, retain_non_matching_names=True
        )
        validate_not_null_columns_exist(nn_columns, rdbms_column_names)
    return nn_columns


def validate_not_null_columns_exist(not_null_columns: list, rdbms_column_names: list):
    assert isinstance(not_null_columns, list)
    assert isinstance(rdbms_column_names, list)
    bad_cols = list(
        set([_.upper() for _ in not_null_columns])
        - set([_.upper() for _ in rdbms_column_names])
    )
    if bad_cols:
        raise OffloadNotNullControlsException(
            "%s: %s" % (UNKNOWN_NOT_NULL_COLUMN_EXCEPTION_TEXT, bad_cols)
        )
