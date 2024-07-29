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

""" Library for logic/interaction with hybrid metadata
    For now a handy location for like minded functions, in the future this might become a class
"""

from typing import TYPE_CHECKING

from goe.offload.column_metadata import (
    invalid_column_list_message,
    match_table_column,
    valid_column_list,
)
from goe.offload.offload_source_table import (
    OffloadSourceTableInterface,
    convert_high_values_to_python,
    OFFLOAD_PARTITION_TYPE_LIST,
    OFFLOAD_PARTITION_TYPE_RANGE,
)
from goe.orchestration import command_steps
from goe.persistence.orchestration_metadata import (
    OrchestrationMetadata,
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.util.misc_functions import csv_split, nvl, unsurround

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.backend_table import BackendTableInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


class OffloadMetadataException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

OFFLOAD_TYPE_FULL = "FULL"
OFFLOAD_TYPE_INCREMENTAL = "INCREMENTAL"

HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_BEGIN = "--BEGINRDBMSHWM"
HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_END = "--ENDRDBMSHWM"
HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_BEGIN = "--BEGINREMOTEHWM"
HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_END = "--ENDREMOTEHWM"

METADATA_HYBRID_VIEW = "GOE_OFFLOAD_HYBRID_VIEW"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def gen_offload_metadata(
    repo_client: "OrchestrationRepoClientInterface",
    offload_operation,
    frontend_owner: str,
    frontend_table_name: str,
    backend_owner: str,
    backend_table_name: str,
    threshold_cols,
    offload_high_values,
    incremental_predicate_values,
    offload_snapshot,
    pre_offload_metadata: OrchestrationMetadata,
):
    """pre_offload_metadata is used to carry certain metadata attributes forward to another operation,
    e.g. from an offload to a present. Also used to carry forward attributes from one offload to another which would
    not otherwise be set, e.g. Incremental Update attributes.
    Backend owner and table names should NOT have their case interfered with. Some backends are case sensitive so
    we cannot uppercase all values for consistency (which is what we used to do).
    """
    if offload_operation.sort_columns:
        assert isinstance(
            offload_operation.sort_columns, list
        ), "{} is not of type list".format(
            type(offload_operation.offload_partition_functions)
        )
    if offload_operation.offload_partition_functions:
        assert isinstance(
            offload_operation.offload_partition_functions, list
        ), "{} is not of type list".format(
            type(offload_operation.offload_partition_functions)
        )

    incremental_predicate_type = None
    incremental_range = None
    offload_partition_functions = None
    if pre_offload_metadata:
        # Some attributes should not be changed for data append operations.
        bucket_hash_column = pre_offload_metadata.offload_bucket_column
        offload_snapshot = pre_offload_metadata.offload_snapshot
        incremental_range = pre_offload_metadata.incremental_range
        incremental_predicate_type = pre_offload_metadata.incremental_predicate_type
        if pre_offload_metadata.offload_partition_functions:
            offload_partition_functions = csv_split(
                pre_offload_metadata.offload_partition_functions
            )
    else:
        bucket_hash_column = offload_operation.bucket_hash_col

    # Predicate type might be changing so need to take it from the currently-executing operation...
    incremental_predicate_type = (
        offload_operation.ipa_predicate_type or incremental_predicate_type
    )

    if not incremental_range:
        if offload_operation.offload_by_subpartition:
            incremental_range = "SUBPARTITION"
        elif (
            threshold_cols
            and incremental_predicate_type != INCREMENTAL_PREDICATE_TYPE_PREDICATE
        ):
            incremental_range = "PARTITION"

    offload_partition_functions = (
        offload_partition_functions or offload_operation.offload_partition_functions
    )

    if (
        offload_operation.hwm_in_hybrid_view
        and incremental_predicate_type != INCREMENTAL_PREDICATE_TYPE_PREDICATE
    ):
        # incremental_predicate_type PREDICATE has a UNION ALL hybrid view but no incremental keys
        incremental_key_csv = incremental_key_csv_from_part_keys(threshold_cols)
        incremental_hv_csv = incremental_hv_csv_from_list(offload_high_values)
    else:
        incremental_key_csv = None
        incremental_hv_csv = None

    offload_sort_csv = column_name_list_to_csv(offload_operation.sort_columns)
    offload_partition_functions_csv = offload_partition_functions_to_csv(
        offload_partition_functions
    )
    incremental_predicate_value = (
        [p.dsl for p in incremental_predicate_values]
        if incremental_predicate_values
        else None
    )

    return OrchestrationMetadata.from_attributes(
        client=repo_client,
        backend_owner=backend_owner,
        backend_table=backend_table_name,
        offload_type=offload_operation.offload_type,
        offloaded_owner=frontend_owner,
        offloaded_table=frontend_table_name,
        incremental_key=incremental_key_csv,
        incremental_high_value=incremental_hv_csv,
        incremental_range=incremental_range,
        incremental_predicate_type=incremental_predicate_type,
        incremental_predicate_value=incremental_predicate_value,
        offload_bucket_column=bucket_hash_column,
        offload_sort_columns=offload_sort_csv,
        offload_snapshot=offload_snapshot,
        offload_partition_functions=offload_partition_functions_csv,
        command_execution=offload_operation.execution_id,
    )


def gen_and_save_offload_metadata(
    repo_client: "OrchestrationRepoClientInterface",
    messages: "OffloadMessages",
    offload_operation,
    config: "OrchestrationConfig",
    offload_source_table: "OffloadSourceTableInterface",
    offload_target_table: "BackendTableInterface",
    incremental_key_columns,
    incremental_high_values,
    incremental_predicate_values,
    pre_offload_snapshot,
    pre_offload_metadata: OrchestrationMetadata,
):
    """Simple wrapper over generation and saving of metadata.
    Returns the new metadata object for convenience.
    """
    goe_metadata = gen_offload_metadata(
        repo_client,
        offload_operation,
        offload_source_table.owner,
        offload_source_table.table_name,
        offload_target_table.db_name,
        offload_target_table.table_name,
        incremental_key_columns,
        incremental_high_values,
        incremental_predicate_values,
        pre_offload_snapshot,
        pre_offload_metadata,
    )

    messages.offload_step(
        command_steps.STEP_SAVE_METADATA,
        lambda: goe_metadata.save(),
        execute=offload_operation.execute,
    )
    return goe_metadata


def partition_columns_from_metadata(inc_key_string, rdbms_columns):
    """Creates a list of tuples matching OffloadSourceTable.partition_columns from metadata."""
    assert valid_column_list(rdbms_columns), invalid_column_list_message(rdbms_columns)
    if not inc_key_string:
        return []
    inc_keys = csv_split(inc_key_string.upper())
    partition_columns = []
    for inc_key in inc_keys:
        col = match_table_column(inc_key, rdbms_columns)
        if not col:
            raise OffloadMetadataException("Column %s not found in table" % inc_key)
        partition_columns.append(col)
    return partition_columns


def incremental_key_csv_from_part_keys(incremental_keys):
    """Takes partition columns list of tuples and formats metadata CSV.
    Broken out as a function as formatting should be consistent.
    """
    assert valid_column_list(incremental_keys), invalid_column_list_message(
        incremental_keys
    )
    return ", ".join([_.name for _ in incremental_keys]) if incremental_keys else None


def unknown_to_str(ch):
    """Convert anything to str but not if it's already str.
    Basically: don't downgrade unicode data to str.
    """
    if not isinstance(ch, str):
        return str(ch)
    else:
        return ch


def incremental_hv_csv_from_list(incremental_high_values):
    """Takes partition HWM literals (for Oracle as stored in data dictionary) and formats them.
    Broken out as a function because formatting should be consistent.
    For RANGE this gives us a simple CSV with spaces after commas.
    For LIST this gives is a CSV of tuples with spaces after commas, this format is decoded in
    incremental_hv_list_from_csv so should not be modified lightly.
    """

    def hv_to_str(hv):
        if type(hv) in (tuple, list):
            return "(%s)" % ", ".join(unknown_to_str(_) for _ in hv)
        else:
            return unknown_to_str(hv)

    return (
        ", ".join(hv_to_str(_) for _ in incremental_high_values)
        if incremental_high_values
        else None
    )


def incremental_hv_list_from_csv(metadata_high_value_string, ipa_predicate_type):
    """Accepts hybrid metadata INCREMENTAL_HIGH_VALUE and decodes it based on the ipa_predicate_type.
    For RANGE this is just a pass through because metadata only stores a single HV list (implied tuple).
    For LIST the metadata is a list of tuples so we need to pre-process the string to get a list of the
    individual tuples.
    """

    def metadata_high_value_split(metadata_str):
        """Splits a string on commas when not nested inside parentheses or quotes"""
        tokens = []
        current_token = ""
        paren_level = 0
        gobble_until_next_char = None
        for c in metadata_str:
            if gobble_until_next_char:
                current_token += c
                if c == gobble_until_next_char:
                    gobble_until_next_char = None
            elif c == "," and paren_level == 0:
                tokens.append(current_token.strip())
                current_token = ""
            else:
                if c in ("'", '"'):
                    gobble_until_next_char = c
                elif c == "(":
                    paren_level += 1
                elif c == ")":
                    paren_level -= 1
                current_token += c
        # add the final token
        if current_token:
            tokens.append(current_token.strip())
        return tokens

    if not metadata_high_value_string:
        return None
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        tokens = metadata_high_value_split(metadata_high_value_string)
        # for list each token is surrounded by brackets so we should remove those
        tokens = [unsurround(_, "(", ")") for _ in tokens]
        return tokens
    else:
        return metadata_high_value_string


def decode_metadata_incremental_high_values_from_metadata(
    base_metadata: OrchestrationMetadata, rdbms_base_table: OffloadSourceTableInterface
):
    """Equivalent of OffloadSourceTable.decode_partition_high_values_with_literals but for metadata based values
    This function subverts ipa_predicate_type by using it as partition_type. This works currently but is not
    ideal.
    """
    assert isinstance(base_metadata, OrchestrationMetadata)
    assert isinstance(
        rdbms_base_table, OffloadSourceTableInterface
    ), "%s is not of type OffloadSourceTableInterface" % str(type(rdbms_base_table))
    if not base_metadata or not base_metadata.incremental_high_value:
        return [], [], []
    return decode_metadata_incremental_high_values(
        base_metadata.incremental_predicate_type,
        base_metadata.incremental_key,
        base_metadata.incremental_high_value,
        rdbms_base_table,
    )


def decode_metadata_incremental_high_values(
    incremental_predicate_type,
    incremental_key,
    incremental_high_value,
    rdbms_base_table: OffloadSourceTableInterface,
):
    """Equivalent of OffloadSourceTable.decode_partition_high_values_with_literals but for metadata based values
    This function subverts ipa_predicate_type by using it as partition_type. This works currently but is not
    ideal.
    """

    def hvs_to_python(hv_csv, partition_columns, rdbms_base_table, partition_type):
        hv_literals = rdbms_base_table.split_partition_high_value_string(hv_csv)
        partition_type = (
            OFFLOAD_PARTITION_TYPE_RANGE
            if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_RANGE
            else OFFLOAD_PARTITION_TYPE_LIST
        )
        hv_python = convert_high_values_to_python(
            partition_columns, hv_literals, partition_type, rdbms_base_table
        )
        return tuple(hv_python), tuple(hv_literals)

    ipa_predicate_type = incremental_predicate_type or INCREMENTAL_PREDICATE_TYPE_RANGE
    partition_columns = partition_columns_from_metadata(
        incremental_key, rdbms_base_table.columns
    )
    partitionwise_hvs = incremental_hv_list_from_csv(
        incremental_high_value, ipa_predicate_type
    )
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        hv_real_list, hv_indiv_list = [], []
        for partitionwise_hv in partitionwise_hvs:
            hv_real_vals, hv_indiv_vals = hvs_to_python(
                partitionwise_hv,
                partition_columns,
                rdbms_base_table,
                ipa_predicate_type,
            )
            hv_real_list.append(hv_real_vals)
            hv_indiv_list.append(hv_indiv_vals)
        return partition_columns, hv_real_list, hv_indiv_list
    else:
        hv_real_vals, hv_indiv_vals = hvs_to_python(
            partitionwise_hvs, partition_columns, rdbms_base_table, ipa_predicate_type
        )
        return partition_columns, hv_real_vals, hv_indiv_vals


def split_metadata_incremental_high_values(
    base_metadata: OrchestrationMetadata, frontend_api
):
    """Equivalent of decode_metadata_incremental_high_values() above but for when we don't have access
    to an RDBMS base table.
    This means we can only decode as far as string HVs, no conversion to real Python values.
    """
    if not base_metadata or not base_metadata.incremental_high_value:
        return None
    ipa_predicate_type = (
        base_metadata.incremental_predicate_type or INCREMENTAL_PREDICATE_TYPE_RANGE
    )
    partitionwise_hvs = incremental_hv_list_from_csv(
        base_metadata.incremental_high_value, ipa_predicate_type
    )
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        return [
            tuple(frontend_api.split_partition_high_value_string(_))
            for _ in partitionwise_hvs
        ]
    else:
        return frontend_api.split_partition_high_value_string(partitionwise_hvs)


def flatten_lpa_individual_high_values(incremental_high_values, to_str=True):
    """Takes a set of list partition high values, as produced by split_metadata_incremental_high_values() or
    decode_metadata_incremental_high_values():

    [('A', 'B'), ('C')]

    and flattens them out for use in SQL:

    ['A', 'B', 'C']
    """
    assert type(incremental_high_values) is list, "Type %s is not list" % type(
        incremental_high_values
    )
    if to_str:
        return [unknown_to_str(v) for hv in incremental_high_values for v in hv]
    else:
        return [v for hv in incremental_high_values for v in hv]


def flatten_lpa_high_values(incremental_high_values):
    """Wrapper for flatten_lpa_individual_high_values but does retains
    literal data types
    """
    return flatten_lpa_individual_high_values(incremental_high_values, to_str=False)


def column_name_list_to_csv(columns, delimiter=","):
    """Based on original implementation of offload_sort_columns_to_csv...
    Seems pretty trivial but need to be sure we do this consistently in a few different places
    """
    if not columns:
        return None
    return "%s" % delimiter.join(columns)


def offload_sort_columns_to_csv(offload_sort_columns):
    return column_name_list_to_csv(offload_sort_columns)


def offload_partition_functions_to_csv(offload_partition_functions):
    """Copied behaviour from offload_sort_columns_to_csv above"""
    if offload_partition_functions:
        assert isinstance(offload_partition_functions, list)
    else:
        return None
    return ",".join(nvl(_, "") for _ in offload_partition_functions)
