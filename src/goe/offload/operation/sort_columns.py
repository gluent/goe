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

""" sort_columns: Library of functions used in GOE to process sort column controls
"""

from typing import Optional, TYPE_CHECKING

from goe.offload.column_metadata import match_table_column
from goe.offload import offload_constants
from goe.offload.offload_functions import expand_columns_csv
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_metadata_functions import offload_sort_columns_to_csv
from goe.util.misc_functions import csv_split

if TYPE_CHECKING:
    from goe.persistence.orchestration_metadata import OrchestrationMetadata
    from goe.offload.backend_api import BackendApiInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.offload.offload_source_table import OffloadSourceTableInterface


class OffloadSortColumnsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

SORT_COLUMN_INVALID_EXCEPTION_TEXT = "Column is not valid for backend sorting"
SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT = (
    "Too many sort columns specified for backend system"
)
SORT_COLUMN_NO_MODIFY_EXCEPTION_TEXT = "Changing column sorting is not supported"
UNKNOWN_SORT_COLUMN_EXCEPTION_TEXT = "Unknown columns specified for backend sorting"


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def default_sort_columns_from_metadata(
    hybrid_metadata: "OrchestrationMetadata",
):
    sort_columns = None
    if hybrid_metadata and hybrid_metadata.offload_sort_columns:
        sort_columns = csv_split(hybrid_metadata.offload_sort_columns)
    return sort_columns


def validate_sort_columns_exist(sort_columns: list, rdbms_column_names: list):
    assert isinstance(sort_columns, list)
    assert isinstance(rdbms_column_names, list)
    bad_cols = list(set(sort_columns) - set(rdbms_column_names))
    if bad_cols:
        raise OffloadSortColumnsException(
            "%s: %s" % (UNKNOWN_SORT_COLUMN_EXCEPTION_TEXT, bad_cols)
        )


def validate_sort_column_types(
    sort_columns: list, backend_cols: list, backend_api: "BackendApiInterface"
):
    assert isinstance(sort_columns, list)
    assert isinstance(backend_cols, list)
    for sort_col in sort_columns:
        backend_col = match_table_column(sort_col, backend_cols)
        if not backend_api.is_valid_sort_data_type(backend_col.data_type):
            raise OffloadSortColumnsException(
                "%s: %s/%s"
                % (
                    SORT_COLUMN_INVALID_EXCEPTION_TEXT,
                    backend_col.name,
                    backend_col.data_type,
                )
            )


def sort_columns_csv_to_sort_columns(
    sort_columns_csv: str,
    hybrid_metadata: Optional["OrchestrationMetadata"],
    offload_source_table: "OffloadSourceTableInterface",
    backend_cols: list,
    backend_api: "BackendApiInterface",
    messages: "OffloadMessages",
) -> Optional[list]:
    assert isinstance(sort_columns_csv, (str, type(None)))
    sort_columns = None
    rdbms_column_names = offload_source_table.get_column_names()
    if sort_columns_csv == offload_constants.SORT_COLUMNS_NO_CHANGE:
        if hybrid_metadata and hybrid_metadata.offload_sort_columns:
            # Use existing metadata to continue with existing configuration
            sort_columns = default_sort_columns_from_metadata(hybrid_metadata)
            messages.log(
                "Retaining SORT BY columns from previous offload: %s" % sort_columns,
                detail=VVERBOSE,
            )
        elif not hybrid_metadata:
            # First time Offload with default options, we might want to set a default
            # based on the frontend/backend combination.
            if backend_api.default_sort_columns_to_primary_key():
                sort_columns = offload_source_table.get_primary_key_columns()
    elif sort_columns_csv == offload_constants.SORT_COLUMNS_NONE:
        # The user requested no sorting.
        sort_columns = None
    elif sort_columns_csv:
        # The user gave us a list so use that and ensure all stated SORT BY columns exist.
        sort_columns = expand_columns_csv(
            sort_columns_csv, rdbms_column_names, retain_non_matching_names=True
        )
        validate_sort_columns_exist(sort_columns, rdbms_column_names)
        validate_sort_column_types(sort_columns, backend_cols, backend_api)
        if hybrid_metadata and hybrid_metadata.offload_sort_columns:
            messages.log(
                "New OFFLOAD_SORT_COLUMNS value specified: %s" % ",".join(sort_columns),
                detail=VVERBOSE,
            )
    if sort_columns and len(sort_columns) > backend_api.max_sort_columns():
        raise OffloadSortColumnsException(
            "%s: %s > %s"
            % (
                SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT,
                len(sort_columns),
                backend_api.max_sort_columns(),
            )
        )
    return sort_columns


def sort_columns_have_changed(
    offload_source_table: "OffloadSourceTableInterface", offload_operation
):
    if not offload_source_table.sorted_table_supported():
        return False
    existing_metadata = offload_operation.pre_offload_hybrid_metadata
    prior_sort_columns = (
        existing_metadata.offload_sort_columns if existing_metadata else None
    )
    new_sort_columns = offload_sort_columns_to_csv(offload_operation.sort_columns)
    return bool(prior_sort_columns != new_sort_columns)


def check_and_alter_backend_sort_columns(offload_target_table, offload_operation):
    if sort_columns_have_changed(offload_target_table, offload_operation):
        if not offload_target_table.sorted_table_modify_supported():
            raise OffloadSortColumnsException(SORT_COLUMN_NO_MODIFY_EXCEPTION_TEXT)
        offload_target_table.alter_table_sort_columns_step()
