# Copyright 2024 The GOE Authors. All rights reserved.
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

from textwrap import dedent
from typing import TYPE_CHECKING

from goe.exceptions import OffloadException
from goe.offload.column_metadata import (
    get_column_names,
    match_table_column,
)
from goe.offload.offload_messages import OffloadMessages
from goe.util.misc_functions import format_list_for_logging

if TYPE_CHECKING:
    from goe.offload.backend_table import BackendTableInterface
    from goe.offload.offload_source_table import OffloadSourceTableInterface


OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT = "Column mismatch detected between the source and backend table. Resolve before offloading"


def check_table_structure(
    frontend_table: "OffloadSourceTableInterface",
    backend_table: "BackendTableInterface",
    messages: OffloadMessages,
):
    """Compare frontend and backend columns are compatible to allow Offload to proceed.

    Throws an exception if there is a mismatch.

    Checks:
      - Check names match, case insensitive.
      - Check data types are compatible via canonical classes, e.g. is_numeric().
      - Check data types are compatible across valid remappings, e.g. frontend date to backend string.
    """
    frontend_cols = frontend_table.columns
    backend_cols = backend_table.get_non_synthetic_columns()

    # Check case insensitive names match.
    new_frontend_cols, missing_frontend_cols = check_table_columns_by_name(
        frontend_cols, backend_cols
    )
    if new_frontend_cols or missing_frontend_cols:
        check_table_columns_by_name_logging(
            frontend_table,
            backend_table,
            new_frontend_cols,
            missing_frontend_cols,
            messages,
        )
        raise OffloadException(
            "{}: {}.{}".format(
                OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT,
                frontend_table.owner,
                frontend_table.table_name,
            )
        )

    # Check data types are compatible via canonical classes.
    invalid_combinations = check_table_columns_by_type(frontend_table, backend_table)
    if invalid_combinations:
        check_table_columns_by_type_logging(
            frontend_table, backend_table, invalid_combinations, messages
        )
        raise OffloadException(
            "{}: {}.{}".format(
                OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT,
                frontend_table.owner,
                frontend_table.table_name,
            )
        )


def check_table_columns_by_name(frontend_cols: list, backend_cols: list) -> tuple:
    """Check case insensitive names match.

    Returns a tuples of lists of column names:
        (new_frontend_names: list[str], missing_frontend_names: list[str])
    """
    frontend_names = get_column_names(frontend_cols, conv_fn=str.upper)
    backend_names = get_column_names(backend_cols, conv_fn=str.upper)
    new_frontend_names = sorted([_ for _ in frontend_names if _ not in backend_names])
    missing_frontend_names = sorted(
        [_ for _ in backend_names if _ not in frontend_names]
    )
    return new_frontend_names, missing_frontend_names


def check_table_columns_by_name_logging(
    frontend_table: "OffloadSourceTableInterface",
    backend_table: "BackendTableInterface",
    new_frontend_cols: list,
    missing_frontend_cols: list,
    messages: OffloadMessages,
):
    if not new_frontend_cols and not missing_frontend_cols:
        return
    column_table = [
        (frontend_table.frontend_db_name(), backend_table.backend_db_name())
    ]
    column_table.extend([(_, "-") for _ in new_frontend_cols])
    column_table.extend([("-", _) for _ in missing_frontend_cols])
    messages.warning(
        "The following column mismatches were detected between the source and backend table:\n{}".format(
            format_list_for_logging(column_table, underline_char="-")
        ),
        ansi_code="red",
    )


def check_table_columns_by_type(
    frontend_table: "OffloadSourceTableInterface",
    backend_table: "BackendTableInterface",
) -> dict:
    """Check data types are compatible via canonical classes.

    Returns:
        A dict of frontend column names with the incompatible backend data type.
    """
    invalid_combinations = {}
    target_canonical_cols = backend_table.get_canonical_columns()
    for rdbms_col in frontend_table.columns:
        target_canonical_col = match_table_column(rdbms_col.name, target_canonical_cols)
        if (
            (rdbms_col.is_number_based() and target_canonical_col.is_number_based())
            or (rdbms_col.is_string_based() and target_canonical_col.is_string_based())
            or (rdbms_col.is_date_based() and target_canonical_col.is_date_based())
        ):
            # The types are close enough.
            continue
        if frontend_table.valid_canonical_override(rdbms_col, target_canonical_col):
            # The types are a valid offload combination.
            continue
        # If we get here then we have an invalid combination
        backend_col = match_table_column(rdbms_col.name, backend_table.get_columns())
        invalid_combinations[rdbms_col.name] = backend_col.data_type
    return invalid_combinations


def check_table_columns_by_type_logging(
    frontend_table: "OffloadSourceTableInterface",
    backend_table: "BackendTableInterface",
    invalid_combinations: dict,
    messages: OffloadMessages,
):
    if not invalid_combinations:
        return
    column_table = [
        (
            f"{frontend_table.frontend_db_name()} Column",
            f"{frontend_table.frontend_db_name()} Type",
            f"{backend_table.backend_db_name()} Type",
        )
    ]
    for col_name, backend_type in invalid_combinations.items():
        frontend_col = frontend_table.get_column(col_name)
        column_table.append(
            (
                col_name,
                frontend_col.data_type,
                backend_type,
            )
        )
    messages.warning(
        "The following column mismatches were detected between the source and backend table:\n{}".format(
            format_list_for_logging(column_table, underline_char="-")
        ),
        ansi_code="red",
    )
