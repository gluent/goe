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

""" OffloadSourceData: Library for logic/interaction with data that is the source of an offload
    Goes hand in hand with OffloadSourceTable but is more about interacting with the source data,
    applying filters to it etc
    Any RDBMS specifics should be catered for in OffloadSourceTable, this module should be generic
    offload logic
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime, date
import logging
import re
from functools import partial

from numpy import datetime64

from goe.offload import offload_constants, predicate_offload
from goe.offload.column_metadata import valid_column_list
from goe.offload.offload_functions import (
    get_dsl_threshold_clauses,
    datetime_literal_to_python,
)
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_metadata_functions import (
    incremental_hv_list_from_csv,
    decode_metadata_incremental_high_values_from_metadata,
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_source_table import (
    OffloadSourceTableInterface,
    OFFLOAD_PARTITION_TYPE_RANGE,
    OFFLOAD_PARTITION_TYPE_LIST,
)
from goe.offload.backend_table import BackendTableInterface
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TYPE_TIMESTAMP_TZ,
)
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
)


class OffloadSourceDataException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

OFFLOAD_PARTITION_APPEND_CAPABLE_TYPES = [
    OFFLOAD_PARTITION_TYPE_RANGE,
    OFFLOAD_PARTITION_TYPE_LIST,
]

OFFLOAD_SOURCE_CLIENT_OFFLOAD = "offload"
OFFLOAD_OP_TERMS = {
    OFFLOAD_SOURCE_CLIENT_OFFLOAD: {
        "name": "offload",
        "now": "offloading",
        "past": "offloaded",
    },
}

LAPBO_TYPE_90_10 = "90/10"
LAPBO_TYPE_100_10 = "100/10"
LAPBO_TYPE_100_0 = "100/0"

# Text used in exceptions that can be matched in tests
NO_DEFAULT_PARTITION_NOTICE_TEXT = (
    f"with high_value of {offload_constants.PART_OUT_OF_LIST}"
)
NO_MATCHING_PARTITION_EXCEPTION_TEXT = "No partition found matching name"
NO_MAXVALUE_PARTITION_NOTICE_TEXT = (
    f"with high_value of {offload_constants.PART_OUT_OF_RANGE}"
)
INCREMENTAL_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT = (
    "Offload type invalid when DEFAULT partition has been offloaded"
)
INVALID_HV_FOR_LIST_AS_RANGE_EXCEPTION_TEXT = (
    "Partitions have key values which are incompatible with LIST_AS_RANGE offloading"
)
IPA_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT = (
    f"Cannot offload {offload_constants.PART_OUT_OF_LIST} partition"
)
OFFLOAD_TYPE_CHANGE_FOR_PBO_EXCEPTION_TEXT = "Switching between offload type FULL/INCREMENTAL is not supported for Predicate-Based Offload"
PREDICATE_APPEND_HWM_MESSAGE_TEXT = "Appended IPA high value to offload predicate"
PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT = (
    "--offload-predicate is incompatible with INCREMENTAL_PREDICATE_TYPE"
)
PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT = (
    "Option --no-modify-hybrid-view cannot be used with INCREMENTAL_PREDICATE_TYPE"
)
PREDICATE_TYPE_NO_MODIFY_RESET_EXCEPTION_TEXT = (
    "Options --no-modify-hybrid-view and --reset-hybrid-view are not compatible"
)
PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT = (
    "OFFLOAD_TYPE FULL is not compatible with INCREMENTAL_PREDICATE_TYPE"
)
PREDICATE_TYPE_REQUIRED_EXCEPTION_TEXT = (
    "--offload-predicate-type required for existing predicate type"
)
RANGE_AND_PREDICATE_WITHOUT_PART_KEY_EXCEPTION_TEXT = (
    "Offload predicate must contain a predicate for all existing partition key columns"
)
TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT = "Only one partition name can be included"

MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE = "max_query_optimistic_prune_clause"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def offload_source_data_factory(
    offload_source_table,
    offload_target_table,
    offload_operation,
    hybrid_metadata,
    messages,
    source_client_type,
    rdbms_partition_columns_override=None,
    col_offload_source_table_override=None,
):
    """Constructs and returns an appropriate data source object based on user inputs and the RDBMS table type"""
    assert source_client_type in OFFLOAD_OP_TERMS
    terms = OFFLOAD_OP_TERMS[source_client_type]
    if (
        offload_operation.offload_predicate
        or offload_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_PREDICATE
    ):
        messages.log(
            "Offload source data type: OffloadSourceDataPredicate", detail=VVERBOSE
        )
        return OffloadSourceDataPredicate(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )
    elif not offload_source_table.partition_type:
        messages.log("Offload source data type: OffloadSourceDataFull", detail=VVERBOSE)
        return OffloadSourceDataFull(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
        )
    elif (
        offload_source_table.partition_type
        not in OFFLOAD_PARTITION_APPEND_CAPABLE_TYPES
    ):
        messages.log(
            "Offload source data type: OffloadSourceDataFullPartitioned",
            detail=VVERBOSE,
        )
        return OffloadSourceDataFullPartitioned(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )
    elif offload_source_table.partition_type in (
        OFFLOAD_PARTITION_TYPE_RANGE,
        OFFLOAD_PARTITION_TYPE_LIST,
    ):
        bad_data_types = offload_source_table.unsupported_partition_data_types()
        if bad_data_types:
            messages.log(
                "Table %s.%s is %s partitioned by %s (not %s), incremental %s is not supported"
                % (
                    offload_source_table.owner,
                    offload_source_table.table_name,
                    offload_source_table.partition_type,
                    ", ".join(bad_data_types),
                    ", ".join(offload_source_table.supported_partition_data_types()),
                    terms["name"],
                ),
                detail=VERBOSE,
            )
            messages.log(
                "Offload source data type: OffloadSourceDataFullPartitioned",
                detail=VVERBOSE,
            )
            return OffloadSourceDataFullPartitioned(
                offload_source_table,
                offload_target_table,
                offload_operation,
                hybrid_metadata,
                messages,
                source_client_type=source_client_type,
                rdbms_partition_columns_override=rdbms_partition_columns_override,
                col_offload_source_table_override=col_offload_source_table_override,
            )

        if offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
            messages.log(
                "Offload source data type: OffloadSourceDataIpaRange", detail=VVERBOSE
            )
            return OffloadSourceDataIpaRange(
                offload_source_table,
                offload_target_table,
                offload_operation,
                hybrid_metadata,
                messages,
                source_client_type=source_client_type,
                rdbms_partition_columns_override=rdbms_partition_columns_override,
                col_offload_source_table_override=col_offload_source_table_override,
            )
        elif (
            offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_LIST
            and offload_operation.ipa_predicate_type
            in (
                INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
            )
        ):
            messages.log(
                "Offload source data type: OffloadSourceDataIpaListAsRange",
                detail=VVERBOSE,
            )
            return OffloadSourceDataIpaListAsRange(
                offload_source_table,
                offload_target_table,
                offload_operation,
                hybrid_metadata,
                messages,
                source_client_type=source_client_type,
                rdbms_partition_columns_override=rdbms_partition_columns_override,
                col_offload_source_table_override=col_offload_source_table_override,
            )
        elif offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_LIST:
            messages.log(
                "Offload source data type: OffloadSourceDataIpaList", detail=VVERBOSE
            )
            return OffloadSourceDataIpaList(
                offload_source_table,
                offload_target_table,
                offload_operation,
                hybrid_metadata,
                messages,
                source_client_type=source_client_type,
                rdbms_partition_columns_override=rdbms_partition_columns_override,
                col_offload_source_table_override=col_offload_source_table_override,
            )
        else:
            raise NotImplementedError(
                "Offload source data method not implemented for partition type: %s"
                % offload_source_table.partition_type
            )
    else:
        raise NotImplementedError(
            "Offload source data method not implemented for partition type: %s"
            % offload_source_table.partition_type
        )


def get_offload_type_for_config(
    owner,
    table_name,
    user_requested_offload_type,
    incr_append_capable,
    ida_options_specified,
    hybrid_metadata,
    messages,
    with_messages=True,
):
    offload_type = OFFLOAD_TYPE_FULL
    pred_for_90_10_in_hybrid_view = False

    if user_requested_offload_type:
        # The user has specified which OT they want
        offload_type = user_requested_offload_type
        if offload_type == OFFLOAD_TYPE_INCREMENTAL and not incr_append_capable:
            if with_messages:
                messages.warning(
                    'Offload type %s incompatible with table %s.%s, continuing with type "FULL"'
                    % (offload_type, owner, table_name),
                    ansi_type="red",
                )
            offload_type = OFFLOAD_TYPE_FULL
    elif hybrid_metadata:
        # The user has not specified an override but hybrid objects already exist - derive settings
        offload_type = hybrid_metadata.offload_type
        if (
            hybrid_metadata.incremental_high_value
            or hybrid_metadata.incremental_predicate_value
        ):
            if with_messages:
                if hybrid_metadata.incremental_high_value:
                    messages.log(
                        "Including HWM in hybrid view due to existing INCREMENTAL_HIGH_VALUE metadata",
                        detail=VVERBOSE,
                    )
                else:
                    messages.log(
                        "Including HWM in hybrid view due to existing INCREMENTAL_PREDICATE_VALUE metadata",
                        detail=VVERBOSE,
                    )
            pred_for_90_10_in_hybrid_view = True
    elif incr_append_capable and ida_options_specified:
        # The user has implied a 90/10 or 100/10 in the hybrid view
        offload_type = OFFLOAD_TYPE_INCREMENTAL

    if not pred_for_90_10_in_hybrid_view:
        if offload_type == OFFLOAD_TYPE_INCREMENTAL:
            pred_for_90_10_in_hybrid_view = True
        elif ida_options_specified:
            if with_messages:
                messages.log(
                    "Including HWM in hybrid view due to command options",
                    detail=VVERBOSE,
                )
            pred_for_90_10_in_hybrid_view = True

    return offload_type, pred_for_90_10_in_hybrid_view


def is_default_partition(partition):
    """Return True/False is the partition is a DEFAULT partition
    Global fn because shared by both partition and source data classes
    """
    upper_fn = lambda s: s.upper() if isinstance(s, str) else s
    return (
        True
        if [upper_fn(_) for _ in partition.partition_values_python]
        == [offload_constants.PART_OUT_OF_LIST]
        else False
    )


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###########################################################################
# OffloadSourcePartitions
###########################################################################


class OffloadSourcePartition(object):
    """Holds a list of partitions for an RDBMS table:
    Basis is this is format Oracle uses, this should be refined in future to be more generic
    [
      partition name
    , high value CSV string (for Oracle dba_tab_partitions.high_value)
    , high values as Python values
    , partition size in bytes
    , number of rows in partition (according to stats)
    , HVs as individual strings
    , is the boundary a common boundary (True or False and only applies when subpartition range offloads are in play)
    , [ is a list of subpartitions for the partition (only applies for partition range offloads) ]
    ]
    """

    def __init__(
        self,
        partition_name,
        partition_literal,
        partition_values_python,
        partition_values_individual,
        size_in_bytes,
        row_count,
        common_partition_literal,
        subpartitions,
    ):
        self.partition_name = partition_name
        self.partition_literal = partition_literal
        self.partition_values_python = partition_values_python
        self.partition_values_individual = partition_values_individual
        self.size_in_bytes = size_in_bytes
        self.row_count = row_count
        self.common_partition_literal = common_partition_literal
        self.subpartitions = subpartitions

    def __str__(self):
        return "OffloadSourcePartition(partition_name=%s, partition_literal=%r)" % (
            self.partition_name,
            self.partition_literal,
        )

    def as_legacy_tuple(self):
        return (
            self.partition_name,
            self.partition_literal,
            self.partition_values_python,
            self.size_in_bytes,
            self.row_count,
            self.partition_values_individual,
            self.common_partition_literal,
            self.subpartitions,
        )


class OffloadSourcePartitions(object):
    """Holds partitions in scope for offload"""

    def __init__(self, partitions=[]):
        self._partitions = partitions

    def __str__(self):
        if self._partitions:
            return "OffloadSourcePartitions(partitions=%s, partition range=%s-%s)" % (
                len(self._partitions),
                self._partitions[0].partition_name,
                self._partitions[-1].partition_name,
            )
        else:
            return "OffloadSourcePartitions(None)"

    @staticmethod
    def from_source_table(offload_source_table, partition_append_capable):
        """Constructor to return partitions from an OffloadSourceTable"""
        if offload_source_table.offload_by_subpartition:
            rdbms_partitions = offload_source_table.get_subpartitions(
                strict=partition_append_capable
            )
            name_fn = lambda x: x.subpartition_name
            hwm_info = offload_source_table.get_subpartition_boundary_info()
            common_hwm_fn = lambda x: hwm_info[x.high_values_python]["common"]
        else:
            rdbms_partitions = offload_source_table.get_partitions(
                strict=partition_append_capable
            )
            name_fn = lambda x: x.partition_name
            common_hwm_fn = lambda x: True

        partitions = []
        if rdbms_partitions:
            for ora_partition in rdbms_partitions:
                partitions.append(
                    OffloadSourcePartition(
                        name_fn(ora_partition),
                        ora_partition.high_values_csv,
                        ora_partition.high_values_python,
                        ora_partition.high_values_individual,
                        ora_partition.partition_size,
                        ora_partition.num_rows,
                        common_hwm_fn(ora_partition),
                        ora_partition.subpartition_names,
                    )
                )
        return OffloadSourcePartitions(partitions)

    def as_legacy_list(self):
        """For (hopefully short term) use in goe.py matching goe.py constants:
        pc_name, pc_hv, pc_hv_values, pc_size, pc_rows, pc_hv_literals, pc_common_hwm, subpartitions
        """
        return [_.as_legacy_tuple() for _ in self._partitions]

    def count(self):
        return len(self._partitions)

    def get_partitions(self):
        return self._partitions

    def partition_names(self):
        return [_.partition_name for _ in self._partitions]

    def row_count(self):
        return sum(_.row_count for _ in self._partitions)

    def set_partitions(self, partitions):
        self._partitions = partitions

    def size_in_bytes(self):
        return sum(_.size_in_bytes for _ in self._partitions)

    def sort_partitions(self, sort_fn, reverse=False):
        if self._partitions:
            # When sorting partitions we need to be sure any DEFAULT partition remains the "latest"
            default_partition = self.remove_default_partition()
            self._partitions.sort(key=sort_fn, reverse=reverse)
            if default_partition:
                # Insert the open ended partition back on the latest end of the list
                if reverse:
                    self._partitions.insert(0, default_partition)
                else:
                    self._partitions.append(default_partition)

    def subpartition_names(self):
        return [
            s
            for p in self._partitions
            if p.subpartitions is not None
            for s in p.subpartitions
        ]

    def report_partitions(self, offload_by_subpartition, messages, include_stats=True):
        """Reports partitions in a standard format, used in offload stdout"""

        def partition_attr_report_value(list_of_attrs):
            if any(_ is None for _ in list_of_attrs):
                return "Unknown"
            else:
                return sum(list_of_attrs)

        part_description = "subpartitions" if offload_by_subpartition else "partitions"
        if not self._partitions:
            messages.log("0 %s" % part_description)
            return
        size = partition_attr_report_value([_.size_in_bytes for _ in self._partitions])
        rows = partition_attr_report_value([_.row_count for _ in self._partitions])
        if include_stats:
            messages.log(
                "%s %s (%s - %s), total size %s, total rows (from stats) %s"
                % (
                    len(self._partitions),
                    part_description,
                    self._partitions[-1].partition_name,
                    self._partitions[0].partition_name,
                    size,
                    rows,
                )
            )
        else:
            messages.log("%s %s" % (len(self._partitions), part_description))
        if messages.debug_enabled():
            for p in self._partitions:
                messages.log("%s" % str(p), detail=VVERBOSE)

    def get_partition(self, partition_name=None, partition_values_python=None):
        """Get a single partition based on either name or partition_values_python"""
        assert partition_name or partition_values_python
        if partition_values_python:
            assert isinstance(partition_values_python, (list, tuple))

        if partition_name:
            filter_fn = lambda x: x.partition_name == partition_name
        else:
            filter_fn = lambda x: x.partition_values_python == tuple(
                partition_values_python
            )
        filtered_partitions = self.search_by_filter(filter_fn)
        if not filtered_partitions:
            return None
        elif partition_name and len(filtered_partitions) > 1:
            raise OffloadSourceDataException(
                "Unexpected partition count for partition name %s: %s"
                % (partition_name, len(filtered_partitions))
            )
        else:
            # It's possible to match multiple partitions by value due to sub-partitioning
            # Return the first in the list
            return filtered_partitions[0]

    def get_partition_by_index(self, index):
        return self._partitions[index] if self._partitions else None

    def get_prior_partition(
        self, partition=None, partition_name=None, partition_values_python=None
    ):
        """Get a single partition based on finding a particular partition_values_python and then
        finding the partition_values_python before the parameter.
        We can use any of a whole partition, partition name or HV as a jumping off point.
        This is not as simple as index-1 because of sub-partitioned tables with repeating HVs.
        This operation only really makes sense for RANGE based partition sets.
        """
        logger.debug("get_prior_partition")
        assert partition or partition_name or partition_values_python
        if partition_values_python:
            assert isinstance(partition_values_python, (list, tuple))
        if partition:
            assert isinstance(partition, OffloadSourcePartition)

        start_partition = partition or self.get_partition(
            partition_name=partition_name,
            partition_values_python=partition_values_python,
        )
        if not start_partition:
            if partition_name:
                raise OffloadSourceDataException(
                    "%s: %s" % (NO_MATCHING_PARTITION_EXCEPTION_TEXT, partition_name)
                )
            else:
                raise OffloadSourceDataException(
                    "No partition found matching high value: %s"
                    % str(partition_values_python)
                )
        logger.debug(
            "get_prior_partition filter partition: %s" % start_partition.partition_name
        )

        def filter_fn(p):
            if is_default_partition(p):
                return False
            return bool(
                tuple(p.partition_values_python)
                < tuple(start_partition.partition_values_python)
            )

        true_part_list, _ = self.split_partitions(filter_fn)
        return (
            true_part_list.get_partitions()[0] if true_part_list.count() > 0 else None
        )

    def has_maxvalue_partition(self):
        """Partitions are sorted new to old therefore, if there's an OUT-OF-RANGE partition it will be first
        For convenience return the partition name/None rather than True/False
        """
        if (not self._partitions) or (self._partitions[0].partition_literal is None):
            return None
        elif (
            offload_constants.PART_OUT_OF_RANGE
            in self._partitions[0].partition_values_individual
        ):
            # TODO need to review this for Teradata when there could be 2 open (MAXVALUE) partitions
            return self._partitions[0].partition_name
        else:
            return None

    def remove_maxvalue_partition(self):
        """Partitions are sorted new to old therefore, if there's a MAXVALUE partition it will be first
        Returns the removed partition for convenience
        """
        if self.has_maxvalue_partition():
            # TODO need to review this for Teradata when there could be 2 open (MAXVALUE) partitions
            return self._partitions.pop(0)
        return None

    def remove_default_partition(self):
        """Remove the DEFAULT partition from the list of partitions and return the partition object
        for convenience.
        """
        default_partition = self.get_default_partition()
        if default_partition:
            self._partitions.remove(default_partition)
        return default_partition

    def get_default_partition(self):
        """Return partition object for default partition."""
        return self.has_default_partition(return_partition_object=True)

    def has_default_partition(self, return_partition_object=False):
        """For convenience returns a matching partition name/None instead of True/False"""
        if self._partitions:
            default_partitions = [
                _ for _ in self._partitions if is_default_partition(_)
            ]
            if default_partitions:
                return (
                    default_partitions[0]
                    if return_partition_object
                    else default_partitions[0].partition_name
                )
        return None

    def search_by_filter(self, filter_fn):
        """Use filter_fn() on each partition and keep/reject them based on truthyness of filter_fn return"""
        filtered_partitions = []
        for p in self._partitions:
            if filter_fn(p):
                filtered_partitions.append(p)
        return filtered_partitions

    def apply_filter(self, filter_fn):
        """Apply filtered partitions to the object state"""
        self._partitions = self.search_by_filter(filter_fn)

    def split_partitions(self, split_fn):
        """Divvy up _partitions based on split_fn
        Return the two parts:
            (partitions where split_fn(), partitions where not split_fn())
        Does not change the underlying _partitions list
        """
        true_partitions, false_partitions = [], []
        for p in self._partitions:
            true_partitions.append(p) if split_fn(p) else false_partitions.append(p)
        return OffloadSourcePartitions(true_partitions), OffloadSourcePartitions(
            false_partitions
        )


###########################################################################
# OffloadSourceDataInterface
###########################################################################


class OffloadSourceDataInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for "offload type" specific sub-classes.
    Library for logic/interaction with data that is the source of an offload.
    Goes hand in hand with OffloadSourceTable but is more about interacting with the source data,
    applying filters to it etc.
    Any RDBMS specifics should be catered for in OffloadSourceTable, this module should be generic
    offload logic.
    """

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        assert offload_source_table and isinstance(
            offload_source_table, OffloadSourceTableInterface
        )
        assert offload_target_table and isinstance(
            offload_target_table, BackendTableInterface
        )
        if col_offload_source_table_override:
            assert col_offload_source_table_override and isinstance(
                col_offload_source_table_override, OffloadSourceTableInterface
            )
        if rdbms_partition_columns_override:
            assert valid_column_list(rdbms_partition_columns_override)

        self._messages = messages
        self.debug("OffloadSourceData setup")
        # We have two offload_source_table objects, one for partition logic and one for column access
        # This is because materialized joins need to work from base offloaded table for partitions but
        # have different column names
        self._part_offload_source_table = offload_source_table
        if col_offload_source_table_override:
            self.log(
                "Overriding source RDBMS columns with: %s.%s.columns"
                % (
                    col_offload_source_table_override.owner,
                    col_offload_source_table_override.table_name,
                ),
                detail=VVERBOSE,
            )
        self._col_offload_source_table = (
            col_offload_source_table_override or offload_source_table
        )
        self._offload_target_table = offload_target_table
        self._pre_offload_hybrid_metadata = hybrid_metadata
        self.debug("Pre-offload metadata: %s" % str(hybrid_metadata))
        # Caching partition_columns independently because join materialization requires an override.
        if rdbms_partition_columns_override:
            self.log(
                "Overriding source data partition columns with: %s"
                % str(rdbms_partition_columns_override),
                detail=VVERBOSE,
            )
        self._partition_columns = (
            rdbms_partition_columns_override or offload_source_table.partition_columns
        )
        # This informs us whether we are offloading, materializing a join, or presenting
        self._source_client_type = source_client_type
        self._terms = OFFLOAD_OP_TERMS[source_client_type]
        # Cache attributes from offload_operation/options that we are interested in
        self._offload_predicate = offload_operation.offload_predicate
        self._inflight_offload_predicate = self._offload_predicate
        self._offload_predicate_modify_hybrid_view = (
            offload_operation.offload_predicate_modify_hybrid_view
        )
        self._post_offload_predicates = None
        # For joins we need a version of the predicate with adjusted column names
        self._col_offload_predicate = self._offload_predicate
        self._user_requested_offload_type = offload_operation.offload_type
        self._user_requested_table_reset = offload_operation.reset_backend_table
        self._user_requested_hybrid_view_reset = offload_operation.reset_hybrid_view
        self._user_requested_predicate_type = offload_operation.ipa_predicate_type
        self._user_requested_partition_names = offload_operation.partition_names
        self._user_requested_max_offload_chunk_size = (
            offload_operation.max_offload_chunk_size
        )
        self._user_requested_max_offload_chunk_count = (
            offload_operation.max_offload_chunk_count
        )
        # cache any offload_source_table attributes that are used frequently - just for convenience
        self._offload_by_subpartition = offload_source_table.offload_by_subpartition

        self._target_table_max = None
        self._offload_type = None
        self._pred_for_90_10_in_hybrid_view = None
        self._backend_synth_part_expr = None
        # Ideally we should initialize the attributes below here but the constructors reference them in a mish mash
        # of ways and setting them here (or re-orging the constructor calls) is proving a risky business.
        # self._incremental_append_capable = None
        # self._partition_append_capable = None
        # self._partition_append_predicate_type = None
        # Having nothing to offload is not always a bad thing, this attribute lets a calling program
        # check this via self.nothing_to_offload()
        self._nothing_to_offload = False

        self._set_offload_type()

        # variables to hold assorted lists of partitions, these can consume significant
        # memory and have associated free_* methods to blat them if required
        self._source_partitions = None
        self._offloaded_partitions = None
        self._partitions_to_offload = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_backend_synth_part_expr(self):
        """Return a list defining how the backend is partitioned. Importantly, because of as_python_fns=True,
        this includes Python functions that convert values to any synthetic values.
        """
        if not self._target_table_exists():
            self._backend_synth_part_expr = []
        elif not self._backend_synth_part_expr:
            self._backend_synth_part_expr = (
                self._offload_target_table.gen_synthetic_partition_col_expressions(
                    as_python_fns=True
                )
            )
        return self._backend_synth_part_expr

    def _is_synthetic_partition_column(self, partition_column):
        return self._offload_target_table.is_synthetic_partition_column(
            partition_column
        )

    def _target_table_exists(self):
        if self._user_requested_table_reset:
            return False
        return self._offload_target_table.table_exists()

    def _set_offload_type(self):
        (
            self._offload_type,
            self._pred_for_90_10_in_hybrid_view,
        ) = get_offload_type_for_config(
            self._part_offload_source_table.owner,
            self._part_offload_source_table.table_name,
            self._user_requested_offload_type,
            self._incremental_append_capable,
            self._ida_options_have_been_specified(),
            self._pre_offload_hybrid_metadata,
            self._messages,
            with_messages=True,
        )

    def _populate_partitions_to_offload(self):
        """Get and store the remaining partitions after filtering offloaded_partitions from source_partitions"""
        logger.debug("_populate_partitions_to_offload")
        assert self._source_partitions
        assert self._offloaded_partitions
        if not self._partitions_to_offload:
            partition_delta = [
                _
                for _ in self._source_partitions.get_partitions()
                if _.partition_name not in self._offloaded_partitions.partition_names()
            ]
            self._partitions_to_offload = OffloadSourcePartitions(partition_delta)
            self.debug(
                "Partitions to Offload initial count: %s"
                % self._partitions_to_offload.count()
            )
        return self._partitions_to_offload

    def _datetime_literal_to_python(self, dt_literal):
        logger.debug("_datetime_literal_to_python")
        return datetime_literal_to_python(dt_literal)

    @abstractmethod
    def _ida_options_have_been_specified(self):
        pass

    def _partitions_of_matching_hwm(self, more_of_this_hwm, remaining_partitions):
        """Return a list of partitions with the same hwm as more_of_this_hwm
        Used when collecting more sub-partitions of the same HWM in a range offload
        This is not checking for contiguous partitions, just collecting them into a single list
        """
        if not more_of_this_hwm:
            return []
        return [
            subp
            for subp in remaining_partitions
            if subp.partition_values_python == more_of_this_hwm
        ]

    def _make_rpa_hwm_gte_clause(self):
        """If this is a partition append incremental offload then we can attempt to build a where clause to optimise
        the backend scan to get the current data HWM.
        Only returns a clause if:
          - RDBMS table has a single partition column (i.e. not multi-column-partitioning, nothing to do
            with sub-partitioning). This is because the hwm clause for multi-col would be excessively complex.
          - There is previous metadata.
          - We're not resetting.
          - The relevant backend partition column is a synthetic one, otherwise we trust backend engine to optimise.
        """
        if self._user_requested_table_reset:
            return None

        if (
            self._source_partitions.count() == 0
            or not self._pre_offload_hybrid_metadata
        ):
            # Not an IPA offload
            return None

        if not self._partition_columns or len(self._partition_columns) > 1:
            return None

        partition_column = self._partition_columns[0]

        backend_synth_part_expr = self._get_backend_synth_part_expr()

        assert type(backend_synth_part_expr) is list
        if backend_synth_part_expr:
            assert (
                len(backend_synth_part_expr[0]) == 4
            ), "_backend_synth_part_expr is malformed"

        matching_backend_part_col = None
        synthetic_conv_fn = None
        for part_expr in backend_synth_part_expr:
            if part_expr[3].upper() == partition_column.name.upper():
                matching_backend_part_col = self._offload_target_table.get_column(
                    part_expr[0]
                )
                synthetic_conv_fn = part_expr[2]
                break

        if not matching_backend_part_col:
            # RDBMS part col not used as a synthetic partition column in the backend
            return None

        if not self._is_synthetic_partition_column(matching_backend_part_col.name):
            self.debug(
                "Partition column is native therefore no extra predicate is required"
            )
            return None

        prior_offload_partition = self._get_prior_partition_to_offload_by_hv_literal()
        prior_rdbms_hv = (
            prior_offload_partition.partition_literal
            if prior_offload_partition
            else None
        )
        hv_tuple = (
            self._part_offload_source_table.decode_partition_high_values(prior_rdbms_hv)
            if prior_rdbms_hv
            else None
        )
        self.debug("Prior partition HV tuple: %s" % str(hv_tuple))

        if (
            hv_tuple
            and self._partition_columns[0].is_number_based()
            and hv_tuple[0] < 0
        ):
            # Workaround referenced in GOE-1572, we cannot trust synthetic partition values for negative numbers
            self.debug(
                "No synthetic partition predicate for negative numeric: %s"
                % str(hv_tuple[0])
            )
            return None

        if hv_tuple:
            converted_hv = synthetic_conv_fn(hv_tuple[0])
            backend_literal = self._offload_target_table.to_backend_literal(
                converted_hv, data_type=matching_backend_part_col.data_type
            )
            return "%s >= %s" % (matching_backend_part_col.name, backend_literal)

    def _combine_part_cols_with_values(self, row_values):
        """Helper function for Oracle min values row & backend max values row"""
        if not row_values:
            return None
        new_row = [
            (
                self._datetime_literal_to_python(row_val)
                if part_col.is_date_based()
                else row_val
            )
            for part_col, row_val in zip(self._partition_columns, row_values)
        ]
        return tuple(new_row)

    def _get_pre_offload_offload_type(self):
        if self._pre_offload_hybrid_metadata:
            return self._pre_offload_hybrid_metadata.offload_type
        else:
            return None

    def _get_pre_offload_incremental_key(self):
        if self._pre_offload_hybrid_metadata:
            return self._pre_offload_hybrid_metadata.incremental_key
        else:
            return None

    def _get_prior_partition_to_offload_by_hv_literal(self):
        """Find a particular RDBMS hwm in the list of candidate partitions and return the prior offloaded partition"""
        hv = (
            self._pre_offload_hybrid_metadata.incremental_high_value
            if self._pre_offload_hybrid_metadata
            else None
        )
        self.debug("Finding partition prior to: %s" % str(hv))
        source_partitions = self._source_partitions.get_partitions()

        def match_fn(p):
            match = bool(p.partition_literal == hv)
            if match:
                self.debug(
                    "Matched %s: %s == %s" % (p.partition_name, p.partition_literal, hv)
                )
            return match

        hv_partition_index = [i for i, p in enumerate(source_partitions) if match_fn(p)]
        if not hv_partition_index:
            return None
        # Use pop() below to take last partition with matching HWM
        # This only matters when multiple partitions have the same HWM (offload_by_subpartition)
        hv_partition_index = hv_partition_index.pop()
        if hv_partition_index >= len(source_partitions) - 1:
            return None
        return source_partitions[hv_partition_index + 1]

    def _select_target_table_max(self):
        """Get highest point of data stored in target table and store in state
        Impyla/cx_Oracle both truncate nanoseconds hence TIMESTAMP columns are returned as string
        optimistic_prune_clause is used to limit the partitions scanned when looking for existing rows
            If no data is returned then we resort to scanning without the clause
            optimistic_prune_clause is only applicable for single column partition schemes
        Also returns the max value for convenience
        """
        if not self._target_table_exists():
            return None

        column_names = [_.name for _ in self._partition_columns]
        # TODO Ideally we should check frontend system as part of data type check below.
        ts_columns = [
            _.name
            for _ in self._partition_columns
            if _.data_type in (ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ)
        ]
        prune_clause = self._make_rpa_hwm_gte_clause()
        if prune_clause:
            self._messages.event(MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE)
        t1 = datetime.now().replace(microsecond=0)
        try:
            max_row = self._offload_target_table.get_max_column_values(
                column_names,
                columns_to_cast_to_string=ts_columns,
                optimistic_prune_clause=prune_clause,
            )
        finally:
            t2 = datetime.now().replace(microsecond=0)
            self.log(
                "Elapsed time to retrieve maximum backend partition key data: %s"
                % (t2 - t1),
                detail=VVERBOSE,
            )
        return self._combine_part_cols_with_values(max_row)

    # Methods for RDBMS source partitions

    def _report_source_partitions(self):
        logger.debug("_report_source_partitions")
        assert self._source_partitions
        self.log("In {}:".format(self._col_offload_source_table.frontend_db_name()))
        self._source_partitions.report_partitions(
            self._part_offload_source_table.offload_by_subpartition, self._messages
        )

    # Methods for partitions that have already been offloaded

    def _report_offloaded_partitions(self):
        logger.debug("_report_offloaded_partitions")
        assert self._offloaded_partitions
        self.log("Already %s:" % self._terms["past"])
        self._offloaded_partitions.report_partitions(
            self._part_offload_source_table.offload_by_subpartition, self._messages
        )

    # Methods for partitions that are to be offloaded

    def _report_partitions_to_offload(self):
        logger.debug("_report_partitions_to_offload")
        assert self._offloaded_partitions
        self.log("To %s:" % self._terms["name"])
        self._partitions_to_offload.report_partitions(
            self._part_offload_source_table.offload_by_subpartition, self._messages
        )

    def _override_partitions_to_offload(self, new_partitions):
        logger.debug("_override_partitions_to_offload")
        if isinstance(new_partitions, OffloadSourcePartitions):
            self._partitions_to_offload = new_partitions
            self._nothing_to_offload = bool(self._partitions_to_offload.count() == 0)
        elif isinstance(new_partitions, list):
            self._partitions_to_offload.set_partitions(new_partitions)
        else:
            raise NotImplementedError(
                "Unsupported partition override of type: %s" % type(new_partitions)
            )
        return self._partitions_to_offload

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def log(self, msg, detail=None):
        self._messages.log(msg, detail=detail)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def get_offload_type(self):
        return self._offload_type

    def nothing_to_offload(self):
        return self._nothing_to_offload

    def pred_for_90_10_in_hybrid_view(self):
        return self._pred_for_90_10_in_hybrid_view

    @abstractmethod
    def offload_data_detection(self):
        pass

    @abstractmethod
    def get_incremental_high_values(self):
        pass

    def get_pre_offload_predicates(self, as_dsl=False):
        if self._pre_offload_hybrid_metadata:
            return (
                self._pre_offload_hybrid_metadata.decode_incremental_predicate_values(
                    as_dsl=as_dsl
                )
            )
        else:
            return None

    def get_post_offload_predicates(self):
        return self._post_offload_predicates

    def get_offload_predicate(self):
        return self._offload_predicate

    def get_pre_offload_predicate_type(self):
        if self._pre_offload_hybrid_metadata:
            return self._pre_offload_hybrid_metadata.incremental_predicate_type
        else:
            return None

    def get_inflight_offload_predicate(self):
        return self._inflight_offload_predicate

    def is_incremental_append_capable(self):
        return self._incremental_append_capable

    def is_partition_append_capable(self):
        return self._partition_append_capable

    def get_partition_append_predicate_type(self):
        return self._partition_append_predicate_type

    def incremental_hv_list_from_csv(self, metadata_high_value_string):
        logger.debug("incremental_hv_list_from_csv")
        return incremental_hv_list_from_csv(
            metadata_high_value_string, self._partition_append_predicate_type
        )

    # Methods for RDBMS source partitions

    @property
    def source_partitions(self):
        return self._source_partitions

    def populate_source_partitions(self, sort_by_hv=False):
        logger.debug("populate_source_partitions")
        if not self._source_partitions:
            source_partitions = OffloadSourcePartitions.from_source_table(
                self._part_offload_source_table,
                partition_append_capable=self._partition_append_capable,
            )
            if sort_by_hv:
                self.log(
                    "Sorting %s partitions by partition value"
                    % source_partitions.count(),
                    detail=VVERBOSE,
                )
                source_partitions.sort_partitions(
                    sort_fn=lambda x: x.partition_values_python, reverse=True
                )
            self.debug(
                "populate_source_partitions, partition count: %s"
                % source_partitions.count()
            )
            self._source_partitions = source_partitions
        return self._source_partitions

    def override_source_partitions(self, new_partitions):
        logger.debug("set_source_partitions")
        if isinstance(new_partitions, OffloadSourcePartitions):
            self._source_partitions = new_partitions
        elif isinstance(new_partitions, list):
            self._source_partitions.set_partitions(new_partitions)
        else:
            raise NotImplementedError(
                "Unsupported partition override of type: %s" % type(new_partitions)
            )
        return self._source_partitions

    def free_source_partitions(self):
        logger.debug("free_source_partitions")
        self._source_partitions = None

    # Methods for partitions that have already been offloaded

    @property
    def offloaded_partitions(self):
        return self._offloaded_partitions

    def free_offloaded_partitions(self):
        logger.debug("free_offloaded_partitions")
        self._offloaded_partitions = None

    # Methods for partitions that are to be offloaded

    @property
    def partitions_to_offload(self):
        return self._partitions_to_offload

    def split_partitions_to_offload_by_no_segment(self):
        """Split the partitions to offload on whether they have a segment or not"""
        logger.debug("split_partitions_to_offload_by_no_segment")
        filter_fn = lambda p: bool(p.size_in_bytes != 0)
        return self._partitions_to_offload.split_partitions(filter_fn)

    def discard_partitions_to_offload_by_no_segment(self):
        """Remove no-segment partitions from the to-offload list"""
        logger.debug("discard_partitions_to_offload_by_no_segment")
        filter_fn = lambda p: bool(p.size_in_bytes != 0)
        self.partitions_to_offload.apply_filter(filter_fn)

    @abstractmethod
    def offloading_open_ended_partition(self):
        pass

    def get_partitions_to_offload_chunks(self):
        """Break a list of offloadable partitions up in to chunks
        Allows user to control volume of data processed in one pass (smaller chunks = less Impala memory)
        """
        # TODO nj@2019-06-07 Don't like the double reverse but didn't want to dig into why right now

        remaining = self.partitions_to_offload.get_partitions()[:]
        remaining.reverse()
        while remaining:
            chunk = [remaining.pop(0)]
            chunk_size = chunk[0].size_in_bytes
            while (
                remaining
                and (chunk_size + remaining[0].size_in_bytes)
                < self._user_requested_max_offload_chunk_size
                and len(chunk) < self._user_requested_max_offload_chunk_count
            ):
                p = remaining.pop(0)
                chunk.append(p)
                chunk_size += p.size_in_bytes
            with_matching_hwm = self._partitions_of_matching_hwm(
                chunk[-1].partition_values_python, remaining
            )
            if with_matching_hwm:
                # If there are partitions with a matching HWM then they should be included in the chunk, overriding
                # max chunk size/count. This prevents failed offloads from believing a HWM is completely offloaded
                # (important for offload by subpartition)
                chunk.extend(with_matching_hwm)
                chunk_size += sum(p.size_in_bytes for p in with_matching_hwm)
                remaining = remaining[len(with_matching_hwm) :]
            # Remaining was reversed before we started so let's reverse the chunk before yielding it
            chunk.reverse()
            yield (
                OffloadSourcePartitions(chunk),
                OffloadSourcePartitions(remaining[:]),
            )


###########################################################################
# OffloadSourceDataPredicate
###########################################################################


class OffloadSourceDataPredicate(OffloadSourceDataInterface):
    """Offload data selected by predicate only"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        """CONSTRUCTOR"""
        self._partition_append_capable = False
        self._incremental_append_capable = True
        self._partition_append_predicate_type = None

        super(OffloadSourceDataPredicate, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )

        self._incremental_hvs = []

        self._messages.log(
            "Predicates (%s) supplied, %s selected rows from %s.%s"
            % (
                offload_operation.offload_predicate.dsl,
                self._terms["now"],
                offload_source_table.owner,
                offload_source_table.table_name,
            ),
            detail=VERBOSE,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _ida_options_have_been_specified(self):
        return bool(self._offload_predicate and not self._offload_predicate_case_2())

    def _no_offload_type_full(self, predicate_type):
        if (
            self._user_requested_offload_type == OFFLOAD_TYPE_FULL
            or self._offload_type == OFFLOAD_TYPE_FULL
        ):
            raise OffloadSourceDataException(
                "%s: %s"
                % (PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT, predicate_type)
            )

    def _offload_predicate_case_1(self):
        case_1 = bool(
            not self._pre_offload_hybrid_metadata
            or self.get_pre_offload_predicate_type()
            == INCREMENTAL_PREDICATE_TYPE_PREDICATE
        )
        if case_1:
            self.log(
                "Pre-offload predicate type is case 1: %s"
                % self.get_pre_offload_predicate_type(),
                detail=VVERBOSE,
            )
        return case_1

    def _offload_predicate_case_2_type(self):
        case_2_predicate_types = (
            INCREMENTAL_PREDICATE_TYPE_RANGE,
            INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        )
        # Take OFFLOAD_TYPE from metadata and not self._offload_type because this method is used to set it.
        pre_offload_offload_type = self._get_pre_offload_offload_type()
        pre_offload_inc_key = self._get_pre_offload_incremental_key()

        if not self._pre_offload_hybrid_metadata:
            # Cannot have LAPBO if there's no pre-offload metadata
            return None
        elif (
            pre_offload_offload_type == OFFLOAD_TYPE_INCREMENTAL
            and self.get_pre_offload_predicate_type() in case_2_predicate_types
            # If the predicate type remains the same
            and self._user_requested_predicate_type
            == self.get_pre_offload_predicate_type()
        ):
            return LAPBO_TYPE_90_10
        elif (
            pre_offload_offload_type == OFFLOAD_TYPE_FULL
            and pre_offload_inc_key is not None
            and self.get_pre_offload_predicate_type() in case_2_predicate_types
            # If the predicate type remains the same
            and self._user_requested_predicate_type
            == self.get_pre_offload_predicate_type()
        ):
            return LAPBO_TYPE_100_10
        elif (
            pre_offload_offload_type == OFFLOAD_TYPE_FULL
            and pre_offload_inc_key is None
        ):
            return LAPBO_TYPE_100_0
        return None

    def _offload_predicate_case_2(self):
        lapbo_type = self._offload_predicate_case_2_type()
        case_2 = bool(lapbo_type)
        if case_2:
            pre_offload_offload_type = self._get_pre_offload_offload_type()
            if (
                self._user_requested_offload_type
                and pre_offload_offload_type
                and self._user_requested_offload_type != pre_offload_offload_type
            ):
                # In theory we could support switching OFFLOAD_TYPE during PBO but in practice we choose not to.
                raise OffloadSourceDataException(
                    OFFLOAD_TYPE_CHANGE_FOR_PBO_EXCEPTION_TEXT
                )

            self.log(
                "Pre-offload/user requested predicate type is case 2 (%s late arriving data): %s/%s"
                % (
                    lapbo_type,
                    self.get_pre_offload_predicate_type(),
                    self._user_requested_predicate_type,
                ),
                detail=VVERBOSE,
            )
        return case_2

    def _offload_predicate_case_3(self):
        case_3 = bool(
            (
                self.get_pre_offload_predicate_type()
                in (
                    INCREMENTAL_PREDICATE_TYPE_RANGE,
                    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                )
                and self._user_requested_predicate_type
                in (
                    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
                    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
                )
            )
            or self.get_pre_offload_predicate_type()
            in (
                INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
                INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
            )
        )
        if case_3:
            self.log(
                "Pre-offload/user requested predicate type is case 3 (intra day): %s/%s"
                % (
                    self.get_pre_offload_predicate_type(),
                    self._user_requested_predicate_type,
                ),
                detail=VVERBOSE,
            )
        return case_3

    def _offload_predicate_contains_hwm_cols(self):
        def is_column_predicate(column_name, tree):
            if len(tree.children) == 2:
                lhs, rhs = tree.children
                if lhs.data == "column" and lhs.children[-1] == column_name.upper():
                    return True
                if rhs.data == "column" and rhs.children[-1] == column_name.upper():
                    return True
            return False

        (
            hv_columns,
            hv_values,
            hv_sql_exprs,
        ) = decode_metadata_incremental_high_values_from_metadata(
            self._pre_offload_hybrid_metadata, self._col_offload_source_table
        )
        # all HWM columns must have a corresponding predicate, and optionally the value must be < hwm
        return all(
            list(
                self._offload_predicate.ast.find_pred(
                    partial(is_column_predicate, hv_col.name)
                )
            )
            for hv_col, hv in zip(hv_columns, hv_values)
        )

    def _offload_predicate_in_pre_offload_metadata(self):
        return bool(
            self._offload_predicate.dsl
            in (self.get_pre_offload_predicates(as_dsl=True) or [])
        )

    def _predicate_has_rows_check(self, offload_predicate):
        """Returns True if the predicate matches data to Offload"""
        source_has_rows = self._part_offload_source_table.predicate_has_rows(
            offload_predicate
        )
        source_where_clause = self._part_offload_source_table.predicate_to_where_clause(
            offload_predicate
        )

        if self._user_requested_table_reset:
            dest_has_rows = False
        else:
            dest_has_rows = self._offload_target_table.predicate_has_rows(
                offload_predicate
            )

        if not source_has_rows:
            self._nothing_to_offload = True
            self.log(
                "No rows in source:\n%s\nNo data to offload." % source_where_clause
            )
        elif dest_has_rows:
            self._nothing_to_offload = True
            if self._offload_predicate_in_pre_offload_metadata():
                self.log("Already %s:\n%s" % (self._terms["past"], source_where_clause))
            else:
                # This is a new predicate but it matches previously offloaded data
                self.log(
                    "Already %s (overlap):\n%s"
                    % (self._terms["past"], source_where_clause)
                )
        else:
            self.log("To %s:\n%s" % (self._terms["name"], source_where_clause))
        return source_has_rows

    def _set_post_offload_predicates(self, source_has_rows):
        if self._user_requested_hybrid_view_reset:
            self._post_offload_predicates = [self._offload_predicate]
        elif (
            self.nothing_to_offload() and not source_has_rows
        ) or not self._offload_predicate_modify_hybrid_view:
            # Keep the same values we had before
            self._post_offload_predicates = self.get_pre_offload_predicates()
        elif not self._offload_predicate_in_pre_offload_metadata():
            self._post_offload_predicates = (
                self.get_pre_offload_predicates() or []
            ) + [self._offload_predicate]
        else:
            # Keep the same values we had before
            self._post_offload_predicates = self.get_pre_offload_predicates()

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload_data_detection(self):
        self._partitions_to_offload = OffloadSourcePartitions()

        pre_offload_predicate_type = self.get_pre_offload_predicate_type()

        self.debug(
            "pre_offload_predicate_type: %s" % self.get_pre_offload_predicate_type()
        )
        self.debug(
            "user_requested_predicate_type: %s" % self._user_requested_predicate_type
        )

        if self._offload_predicate_case_1():
            if not self._offload_predicate_modify_hybrid_view:
                raise OffloadSourceDataException(
                    "%s: %s"
                    % (
                        PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT,
                        INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                    )
                )
            if self._user_requested_predicate_type not in (
                None,
                INCREMENTAL_PREDICATE_TYPE_PREDICATE,
            ):
                raise OffloadSourceDataException(
                    "%s: %s"
                    % (
                        PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
                        self._user_requested_predicate_type,
                    )
                )

            source_has_rows = self._predicate_has_rows_check(self._offload_predicate)

            self._no_offload_type_full(INCREMENTAL_PREDICATE_TYPE_PREDICATE)

            self._set_post_offload_predicates(source_has_rows)

            self._partition_append_predicate_type = INCREMENTAL_PREDICATE_TYPE_PREDICATE

        elif self._offload_predicate_case_2():
            if self._user_requested_hybrid_view_reset:
                raise OffloadSourceDataException(
                    PREDICATE_TYPE_NO_MODIFY_RESET_EXCEPTION_TEXT
                )

            if self._offload_predicate_modify_hybrid_view:
                self._offload_predicate_modify_hybrid_view = False
                self.log(
                    "Setting --no-modify-hybrid-view for late arriving predicate offload",
                    detail=VVERBOSE,
                )

            self._post_offload_predicates = self.get_pre_offload_predicates()
            self._partition_append_predicate_type = pre_offload_predicate_type

            (
                hv_columns,
                _,
                self._incremental_hvs,
            ) = decode_metadata_incremental_high_values_from_metadata(
                self._pre_offload_hybrid_metadata, self._col_offload_source_table
            )

            if self._offload_predicate_case_2_type() in (
                LAPBO_TYPE_90_10,
                LAPBO_TYPE_100_10,
            ):
                self.populate_source_partitions()
                target_table_max = self._select_target_table_max()
                max_lt_dsl, _ = get_dsl_threshold_clauses(
                    hv_columns,
                    pre_offload_predicate_type,
                    target_table_max,
                    equality_with_gt=False,
                )
                self._messages.notice(
                    "%s: %s" % (PREDICATE_APPEND_HWM_MESSAGE_TEXT, str(max_lt_dsl))
                )
                hv_col_predicates = [predicate_offload.GenericPredicate(max_lt_dsl)]
                self._inflight_offload_predicate = (
                    predicate_offload.create_and_relation_predicate(
                        hv_col_predicates + [self._offload_predicate]
                    )
                )
                _ = self._predicate_has_rows_check(self._inflight_offload_predicate)
            else:
                _ = self._predicate_has_rows_check(self._offload_predicate)

        elif self._offload_predicate_case_3():
            if pre_offload_predicate_type == INCREMENTAL_PREDICATE_TYPE_RANGE:
                self._partition_append_predicate_type = (
                    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE
                )
            elif pre_offload_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE:
                self._partition_append_predicate_type = (
                    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE
                )
            else:
                self._partition_append_predicate_type = pre_offload_predicate_type

            source_has_rows = self._predicate_has_rows_check(self._offload_predicate)

            self._no_offload_type_full(self._partition_append_predicate_type)

            if not self._offload_predicate_contains_hwm_cols():
                raise OffloadSourceDataException(
                    RANGE_AND_PREDICATE_WITHOUT_PART_KEY_EXCEPTION_TEXT
                )

            self._set_post_offload_predicates(source_has_rows)

            # Case 3 needs to retain RANGE HVs
            (
                _,
                _,
                self._incremental_hvs,
            ) = decode_metadata_incremental_high_values_from_metadata(
                self._pre_offload_hybrid_metadata, self._col_offload_source_table
            )

        elif pre_offload_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
            raise OffloadSourceDataException(
                "%s: %s"
                % (
                    PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
                    pre_offload_predicate_type,
                )
            )

        else:
            self.log(
                "pre_offload_predicate_type: %s" % pre_offload_predicate_type,
                detail=VVERBOSE,
            )
            self.log(
                "offload_predicate_modify_hybrid_view: %s"
                % self._offload_predicate_modify_hybrid_view,
                detail=VVERBOSE,
            )
            if pre_offload_predicate_type in (
                INCREMENTAL_PREDICATE_TYPE_RANGE,
                INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
            ):
                raise OffloadSourceDataException(
                    f"{PREDICATE_TYPE_REQUIRED_EXCEPTION_TEXT}: {pre_offload_predicate_type}"
                )
            else:
                raise OffloadSourceDataException(
                    "Incompatible --offload-predicate options"
                )

    def get_incremental_high_values(self):
        return self._incremental_hvs

    def offloading_open_ended_partition(self):
        return False


###########################################################################
# OffloadSourceDataFull
###########################################################################


class OffloadSourceDataFull(OffloadSourceDataInterface):
    """Full non-partitioned 100/0 offloads"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
    ):
        """CONSTRUCTOR"""
        self._partition_append_capable = False
        self._incremental_append_capable = False
        self._partition_append_predicate_type = None

        super(OffloadSourceDataFull, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
        )

        self._messages.notice(
            "Table %s.%s is not partitioned, %s entire table"
            % (
                offload_source_table.owner,
                offload_source_table.table_name,
                self._terms["now"],
            )
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _ida_options_have_been_specified(self):
        return False

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload_data_detection(self):
        self.populate_source_partitions()
        self._offloaded_partitions = OffloadSourcePartitions()
        self._populate_partitions_to_offload()
        # For a heap table there's always something to offload - even if it's empty
        self._nothing_to_offload = False

    def get_incremental_high_values(self):
        return []

    def offloading_open_ended_partition(self):
        return False


###########################################################################
# OffloadSourceDataFullPartitioned
###########################################################################


class OffloadSourceDataFullPartitioned(OffloadSourceDataInterface):
    """Full partitioned 100/0 offloads"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        """CONSTRUCTOR"""
        self._partition_append_capable = False
        self._incremental_append_capable = False
        self._partition_append_predicate_type = None

        super(OffloadSourceDataFullPartitioned, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )

        messages.notice(
            "Table %s.%s is %s partitioned, %s entire table"
            % (
                offload_source_table.owner,
                offload_source_table.table_name,
                offload_source_table.partition_type,
                self._terms["now"],
            )
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _ida_options_have_been_specified(self):
        if self._user_requested_partition_names:
            raise NotImplementedError(
                "Partition name filter is not valid for this offload"
            )
        return False

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload_data_detection(self):
        self.populate_source_partitions()
        self._report_source_partitions()
        self._offloaded_partitions = OffloadSourcePartitions()
        self._populate_partitions_to_offload()
        self._nothing_to_offload = bool(self._partitions_to_offload.count() == 0)

    def get_incremental_high_values(self):
        return []

    def offloading_open_ended_partition(self):
        if not self.partitions_to_offload:
            return False
        return bool(
            self.partitions_to_offload.has_maxvalue_partition()
            or self.partitions_to_offload.has_default_partition()
        )


###########################################################################
# OffloadSourceDataIpaRange
###########################################################################


class OffloadSourceDataIpaRange(OffloadSourceDataInterface):
    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        """CONSTRUCTOR"""
        # cache attributes from offload_operation that we are interested in
        self._user_requested_rpa_filter = offload_operation.less_than_value

        self._partition_append_capable = True
        self._incremental_append_capable = True
        self._partition_append_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE

        super(OffloadSourceDataIpaRange, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )

        self._rdbms_min = None
        self._rpa_filter = None
        self._rpa_hwm_partition = None

        self.log(
            "Table %s.%s is %s partitioned by %s (%s)"
            % (
                offload_source_table.owner,
                offload_source_table.table_name,
                offload_source_table.partition_type,
                ", ".join([_.data_type for _ in self._partition_columns]),
                ", ".join([_.name for _ in self._partition_columns]),
            ),
            detail=VERBOSE,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _check_number_of_partition_append_filters(self, check_tokens):
        assert type(check_tokens) in (list, tuple)
        if len(check_tokens) != len(self._partition_columns):
            raise OffloadSourceDataException(
                "Partition filter %s has incorrect value. %d partition values required"
                % (str(check_tokens), len(self._partition_columns))
            )

    def _convert_string_literal_for_rpa(self, part_col, token):
        return self._col_offload_source_table.char_literal_to_python(token)

    def _normalise_rpa_partition_append_csv(self, csv_str):
        """Breaks up a user specified partition append string and returns a tuple of correctly
        typed Python values ready for comparison with partition_values_python.
        """
        assert isinstance(csv_str, str)
        logger.debug("_normalise_rpa_partition_append_csv")
        tokens = self._col_offload_source_table.split_partition_high_value_string(
            csv_str
        )
        pa_list = []
        self._check_number_of_partition_append_filters(tokens)
        for part_col, token in zip(self._partition_columns, tokens):
            try:
                if part_col.is_date_based():
                    pa_list.append(self._datetime_literal_to_python(token))
                elif part_col.is_number_based():
                    converted_token = (
                        self._col_offload_source_table.numeric_literal_to_python(token)
                    )
                    pa_list.append(converted_token)
                elif part_col.is_string_based():
                    converted_token = self._convert_string_literal_for_rpa(
                        part_col, token
                    )
                    pa_list.append(converted_token)
                else:
                    raise OffloadSourceDataException(
                        "Unsupported partition key type: %s" % part_col.data_type
                    )
            except ValueError:
                self.log("Failed to parse %s value: %s" % (part_col.data_type, token))
                raise
        return tuple(pa_list)

    def _normalise_rpa_partition_filter(self, user_requested_less_than_value):
        """Create a standard comparison tuple that can be compared with partition high values
        from the user requested input.
        Also validates that the items in the tuple match the RDBMS columns in both number
        and type.
        """
        logger.debug("_normalise_rpa_partition_filter")
        if isinstance(user_requested_less_than_value, str):
            rpa_filter_tuple = self._normalise_rpa_partition_append_csv(
                user_requested_less_than_value
            )
        elif isinstance(user_requested_less_than_value, (list, tuple)):
            rpa_filter_tuple = tuple(user_requested_less_than_value)
        elif isinstance(user_requested_less_than_value, date):
            rpa_filter_tuple = (datetime64(user_requested_less_than_value),)
        else:
            rpa_filter_tuple = (user_requested_less_than_value,)

        self._check_number_of_partition_append_filters(rpa_filter_tuple)

        for part_col, col_val in zip(self._partition_columns, rpa_filter_tuple):
            if part_col.is_date_based() != isinstance(col_val, (datetime64, date)):
                raise OffloadSourceDataException(
                    "Filter %s is incompatible with partition key %s of type %s"
                    % (str(col_val), part_col.name, part_col.data_type)
                )

        logger.debug("final filter: %s" % str(rpa_filter_tuple))
        return rpa_filter_tuple

    def _get_rpa_filter(self):
        logger.debug("_get_ipa_filter")
        if self._rpa_filter is None:
            self._normalise_rpa_filter()
        return self._rpa_filter

    def _normalise_rpa_partition_name(self):
        """Convert a partition name to a HV and then convert that HV to a tuple used for partition filtering
        Locates the partition in _source_partitions so this must be populated
        Doesn't validate the number of partition columns or data types because HV is coming directly from RDBMS data dictionary
        """
        logger.debug("_normalise_rpa_partition_name")
        if len(self._user_requested_partition_names) != 1:
            raise OffloadSourceDataException(
                "%s for %s offloaded table"
                % (
                    TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT,
                    self._partition_append_predicate_type,
                )
            )

        partition_name = self._user_requested_partition_names[0]
        matched_partition = self._source_partitions.get_partition(partition_name)
        if matched_partition:
            return matched_partition.partition_values_python
        else:
            raise OffloadSourceDataException(
                "%s: %s" % (NO_MATCHING_PARTITION_EXCEPTION_TEXT, partition_name)
            )

    def _ida_options_have_been_specified(self):
        if self._user_requested_rpa_filter or self._user_requested_partition_names:
            return True
        return False

    def _normalise_rpa_filter(self):
        if self._user_requested_rpa_filter:
            self._rpa_filter = self._normalise_rpa_partition_filter(
                self._user_requested_rpa_filter
            )
        elif self._user_requested_partition_names:
            self._rpa_filter = self._normalise_rpa_partition_name()
        else:
            self._rpa_filter = tuple()
        return self._rpa_filter

    def _rpa_hv_not_offloaded(self, partition):
        match = bool(tuple(partition.partition_values_python) > self._target_table_max)
        self.debug(
            "Partition %s not offloaded check: %s: %s > %s"
            % (
                partition.partition_name,
                match,
                tuple(partition.partition_values_python),
                self._target_table_max,
            )
        )
        return match

    def _rpa_partition_already_offloaded(
        self, partition, partition_already_marked_offloaded
    ):
        """This method is used to differentiate between true RANGE and LIST_AS_RANGE.
        rdbms_min <= backend_max: If there's data in Oracle older than backend max then current partition must
                                  be offloaded and we'll stop iterating when we find a HWM beyond backend max
        p.partition_values_python <= backend_max: If the HV for current partition is older than backend max then it must
                                                  be offloaded this latter check is required for truncated/empty partitions
        partition_already_marked_offloaded: When looping through partitions the next partition beyond the one with the HV
                                            matching backend data is also offloaded, due to less-than nature of RANGE HV.
        """

        def rpa_hv_already_offloaded(partition):
            match = bool(
                (
                    self._rdbms_min is not None
                    and self._rdbms_min <= self._target_table_max
                )
                or tuple(partition.partition_values_python) <= self._target_table_max
            )
            self.debug(
                "Partition %s already offloaded check: %s: %s <= %s"
                % (
                    partition.partition_name,
                    bool(
                        tuple(partition.partition_values_python)
                        <= self._target_table_max
                    ),
                    tuple(partition.partition_values_python),
                    self._target_table_max,
                )
            )
            self.log(
                "Partition %s already offloaded check: %s"
                % (partition.partition_name, match),
                detail=VVERBOSE,
            )
            return match

        if partition_already_marked_offloaded:
            self.debug(
                "Partition %s already marked as offloaded: %s"
                % (partition.partition_name, partition_already_marked_offloaded)
            )
        return bool(
            partition_already_marked_offloaded or rpa_hv_already_offloaded(partition)
        )

    def _rpa_partitions_already_offloaded(self):
        """Return partitions already offloaded based on a HWM and min/max values
        i.e. this is not comparing all partitions but looking for the HWM in the backend and assuming
             all older RDBMS partitions have been offloaded
        """
        assert self._source_partitions
        already_offloaded = []

        if self._set_target_table_max():
            # We only need oracle_min if there is already data in the backend
            self._set_rdbms_min()

            self.log("backend_max: " + str(self._target_table_max), detail=VVERBOSE)
            self.log("rdbms_min: " + str(self._rdbms_min), detail=VVERBOSE)

            reversed_offload_partitions = list(
                reversed(self._source_partitions.get_partitions())
            )
            for i, p in enumerate(reversed_offload_partitions):  # traverse old to new
                if self._rpa_partition_already_offloaded(p, bool(already_offloaded)):
                    already_offloaded.append(p)
                if self._rpa_hv_not_offloaded(p):
                    # Reached a partition with a HWM beyond the backend max, therefore time to break out of the loop
                    if (
                        self._part_offload_source_table.offload_by_subpartition
                        and i < len(reversed_offload_partitions)
                    ):
                        # When offloading by subpartition we may find repeating HWM, gobble up any other partitions of the same HWM
                        with_matching_hwm = self._partitions_of_matching_hwm(
                            p.partition_values_python,
                            reversed_offload_partitions[i + 1 :],
                        )
                        if with_matching_hwm:
                            # There is an assumption here that partitions of matching HWM are contiguous. This will be
                            # true of Oracle, we need to be careful when introducing more RDBMSs
                            self.log(
                                "Skipping %s partitions with matching HWM"
                                % len(with_matching_hwm),
                                detail=VVERBOSE,
                            )
                            already_offloaded.extend(with_matching_hwm)
                    break
            already_offloaded.reverse()
        return already_offloaded

    def _set_incremental_high_values(self):
        """Identify a HWM partition for a RANGE 90/10 config.
        HWM is filtered HWM or highest partition to be offloaded or what's already been offloaded.
        Filtered HWM is checked against source partitions because the HWM may be positioned anywhere (100/10).
        """
        logger.debug("_set_incremental_high_values")
        if self._rpa_hwm_partition:
            return self._rpa_hwm_partition

        self._rpa_hwm_partition = None
        if self._ida_options_have_been_specified():
            filter_fn = self._get_less_than_rpa_filter_fn()
            filtered_partitions = self._source_partitions.search_by_filter(filter_fn)
            if filtered_partitions:
                self.log(
                    "HWM partition %s derived from HWM filter"
                    % filtered_partitions[0].partition_name,
                    detail=VVERBOSE,
                )
                self._rpa_hwm_partition = filtered_partitions[0]

        if not self._rpa_hwm_partition:
            if self._partitions_to_offload.count() > 0:
                logger.debug("get_hwm_partition from partitions_to_offload")
                self._rpa_hwm_partition = (
                    self._partitions_to_offload.get_partition_by_index(0)
                )
            elif self._offloaded_partitions.count() > 0:
                logger.debug("get_hwm_partition from offloaded_partitions")
                self._rpa_hwm_partition = (
                    self._offloaded_partitions.get_partition_by_index(0)
                )

        return self._rpa_hwm_partition

    def _check_hwm_is_a_valid_boundary(
        self, hwm_partition, retained_partitions, offload_type
    ):
        """When RANGE offloading at subpartition level we need to ensure that any HWM for offload is a valid common boundary"""
        logger.debug("_check_hwm_is_a_valid_boundary")

        def is_valid_common_boundary(check_partition, retained_partitions_ascending):
            """A valid common boundary is one that is:
            a) a common boundary
            b) not the last HWM in the table (i.e. there are HWMs in the retain set)...
            """
            return bool(
                check_partition.common_partition_literal
                and retained_partitions_ascending
                and check_partition.partition_values_python
                != retained_partitions_ascending[-1].partition_values_python
            )

        def more_human_readable_python_hwm(python_hwm):
            # convert to str to remove datatype info and chop off any redundant trailing fractional seconds
            str_fn = lambda x: (
                re.sub(r"\.000000$", "", str(x)) if type(x) is datetime64 else str(x)
            )
            return str([str_fn(hv) for hv in python_hwm])

        hwm_check_retained = (
            list(reversed(retained_partitions)) if retained_partitions else []
        )
        if is_valid_common_boundary(hwm_partition, hwm_check_retained):
            # It's a valid HWM and the subpartition offload list is good to be used.
            self.log(
                "Usable common boundary found: %s"
                % more_human_readable_python_hwm(hwm_partition.partition_values_python),
                detail=VVERBOSE,
            )
        else:
            next_common_boundary = None
            for p in hwm_check_retained:
                if p.common_partition_literal:
                    next_common_boundary = p
                    break
            if next_common_boundary and is_valid_common_boundary(
                next_common_boundary, hwm_check_retained
            ):
                self._messages.warning(
                    "Minimum recommended common boundary is: --less-than-values=%s"
                    % more_human_readable_python_hwm(
                        next_common_boundary.partition_values_python
                    )
                )
            elif offload_type == OFFLOAD_TYPE_INCREMENTAL:
                # It's the last HWM in the table...
                raise OffloadSourceDataException(
                    "Table is not valid for range subpartition offloading due to incompatible high value boundaries. Offload with --offload-type=FULL"
                )
            if offload_type == OFFLOAD_TYPE_INCREMENTAL:
                # regardless of the messages and tests above, we don't have a valid HWM so need to stop here
                if self._get_rpa_filter():
                    raise OffloadSourceDataException(
                        "No common boundary at HWM filter: %s"
                        % more_human_readable_python_hwm(self._get_rpa_filter())
                    )
                else:
                    raise OffloadSourceDataException(
                        "No common boundary at HWM: %s"
                        % more_human_readable_python_hwm(
                            hwm_partition.partition_values_python
                        )
                    )

    def _maxvalue_partition_has_been_offloaded(self):
        logger.debug("_maxvalue_partition_has_been_offloaded")
        assert self._offloaded_partitions
        return self._offloaded_partitions.has_maxvalue_partition()

    def _do_not_offload_maxvalue_partition(self):
        """If there's a MAXVALUE partition to offload then remove it and return the partition object"""
        logger.debug("_do_not_offload_maxvalue_partition")
        if (
            self._part_offload_source_table.partition_type
            == OFFLOAD_PARTITION_TYPE_RANGE
        ):
            return self._partitions_to_offload.remove_maxvalue_partition()
        else:
            return None

    def _get_less_than_rpa_filter_fn(self):
        logger.debug("_get_less_than_rpa_filter_fn")
        less_than_list = self._get_rpa_filter()

        def filter_fn(p):
            try:
                match = bool(tuple(p.partition_values_python) <= tuple(less_than_list))
            except TypeError as exc:
                self.log(
                    "{} <= {} throws: {}".format(
                        str(p.partition_values_python), str(less_than_list), str(exc)
                    ),
                    detail=VERBOSE,
                )
                raise
            self.debug(
                "Matching partition %s (%s <= %s): %s"
                % (
                    p.partition_name,
                    str(p.partition_values_python),
                    str(less_than_list),
                    match,
                )
            )
            return match

        return filter_fn

    def _split_partitions_to_offload_by_rpa_filter(self):
        """Split the partitions to offload by the user filter returning both those to keep and those to discard"""
        logger.debug("_split_partitions_to_offload_by_rpa_filter")
        filter_fn = self._get_less_than_rpa_filter_fn()
        return self._partitions_to_offload.split_partitions(filter_fn)

    def _set_target_table_max(self):
        if not self._target_table_max:
            self._target_table_max = self._select_target_table_max()
        return self._target_table_max

    def _set_rdbms_min(self):
        """Get the RDBMS max values for partition columns and store in state
        Also return the value for convenience
        """
        t1 = datetime.now().replace(microsecond=0)
        self._rdbms_min = None
        try:
            min_row = self._part_offload_source_table.get_minimum_partition_key_data()
        finally:
            t2 = datetime.now().replace(microsecond=0)
            self.log(
                "Elapsed time to retrieve minimum RDBMS partition key data: %s"
                % (t2 - t1),
                detail=VVERBOSE,
            )
        self._rdbms_min = self._combine_part_cols_with_values(min_row)
        return self._rdbms_min

    def _populate_offloaded_partitions(self):
        """Inspects _source_partitions and populates a new list of offloaded partitions"""
        logger.debug("_populate_offloaded_partitions")
        if not self._offloaded_partitions:
            if not self._target_table_exists():
                self._offloaded_partitions = OffloadSourcePartitions()
            else:
                self._offloaded_partitions = OffloadSourcePartitions(
                    self._rpa_partitions_already_offloaded()
                )

            if (
                self._offload_type == OFFLOAD_TYPE_INCREMENTAL
                and self._maxvalue_partition_has_been_offloaded()
            ):
                raise OffloadSourceDataException(
                    f"Offload type {self._offload_type} invalid when {offload_constants.PART_OUT_OF_RANGE} partition has been offloaded"
                )

        return self._offloaded_partitions

    def _populate_source_partitions_rpa(self):
        self.populate_source_partitions()

    def _do_not_offload_open_ended_partition(self):
        """For INCREMENTAL RANGE do not offload a MAXVALUE partition."""
        removed_maxval_partition = None
        if (
            self._offload_type == OFFLOAD_TYPE_INCREMENTAL
            and self.partitions_to_offload.has_maxvalue_partition()
        ):
            removed_maxval_partition = self._do_not_offload_maxvalue_partition()
        return removed_maxval_partition

    def _do_not_offload_open_ended_partition_notice(self):
        return NO_MAXVALUE_PARTITION_NOTICE_TEXT

    def _discard_partitions_using_filters(self):
        """Apply RPA filters to the list of partitions to be offloaded"""
        logger.debug("_discard_partitions_using_filters")

        removed_open_partition = self._do_not_offload_open_ended_partition()

        retained_partition_list = []
        if self._ida_options_have_been_specified():
            (
                filtered_partitions,
                discarded_partitions,
            ) = self._split_partitions_to_offload_by_rpa_filter()
            self.debug("Filter retained %s partitions" % filtered_partitions.count())
            self.debug("Filter discarded %s partitions" % discarded_partitions.count())
            filtered_partition_list = filtered_partitions.as_legacy_list()
            if self._offload_type == OFFLOAD_TYPE_INCREMENTAL:
                self._override_partitions_to_offload(filtered_partitions)
                retained_partition_list = discarded_partitions.get_partitions()
            else:
                # ignore filtering and offload all partitions, however we still use filter to define view HWM (100/10 pattern)
                if filtered_partition_list:
                    self.log(
                        "Offloading all partitions when OFFLOAD_TYPE is %s"
                        % self._offload_type,
                        detail=VERBOSE,
                    )

        if removed_open_partition:
            # Add the MAXVALUE/DEFAULT partition to the retained list
            # TODO need to review this for Teradata when there could be 2 open (MAXVALUE) partitions
            self._messages.notice(
                "Not %s partition %s %s"
                % (
                    self._terms["now"],
                    removed_open_partition.partition_name,
                    self._do_not_offload_open_ended_partition_notice(),
                )
            )
            retained_partition_list.append(removed_open_partition)

        # This call stores the HWM partition in state
        hwm_partition = self._set_incremental_high_values()

        if self.partitions_to_offload.count() > 0 and self._offload_by_subpartition:
            self._check_hwm_is_a_valid_boundary(
                hwm_partition, retained_partition_list, self._offload_type
            )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload_data_detection(self):
        """Review candidate partitions and filter them by what's already been offloaded and any user requested options
        Report the different partition categories as appropriate and any warnings/notices
        Partitions in each category (offloaded, to offload, etc) are held in state - not returned
        """
        self._populate_source_partitions_rpa()
        self._report_source_partitions()
        if not self.is_partition_append_capable():
            return

        self._populate_offloaded_partitions()
        if self.offloaded_partitions.count() > 0:
            self._report_offloaded_partitions()

        self._populate_partitions_to_offload()
        self._discard_partitions_using_filters()
        self._nothing_to_offload = bool(self._partitions_to_offload.count() == 0)

        self._set_incremental_high_values()

        if self.partitions_to_offload.count() == 0:
            self.log("No partitions to %s" % self._terms["name"])
        else:
            # Keep and discard lists are just for reporting. We need to continue with the to-offload partition set after reporting
            (
                keep_partitions,
                discard_partitions,
            ) = self.split_partitions_to_offload_by_no_segment()

            self.log("To %s:" % self._terms["name"])
            keep_partitions.report_partitions(
                self._part_offload_source_table.offload_by_subpartition, self._messages
            )

            if discard_partitions.count() > 0:
                self.log("To skip (no segments):")
                discard_partitions.report_partitions(
                    self._part_offload_source_table.offload_by_subpartition,
                    self._messages,
                )

            if self.partitions_to_offload.has_maxvalue_partition():
                self._messages.notice(
                    "%s partition %s with high_value of %s"
                    % (
                        self._terms["now"].capitalize(),
                        self.partitions_to_offload.has_maxvalue_partition(),
                        offload_constants.PART_OUT_OF_RANGE,
                    )
                )

            if self._target_table_max and (
                not self._rdbms_min or self._target_table_max < self._rdbms_min
            ):
                # this indicates that all offloaded partitions have been removed from the RDBMS, we may get verification errors
                self._messages.warning(
                    "Offloaded data is less than RDBMS data, data verification may fail due to purged historical RDBMS data"
                )

    def get_incremental_high_values(self):
        logger.debug("get_incremental_high_values")
        if self._rpa_hwm_partition:
            return self._rpa_hwm_partition.partition_values_individual
        else:
            return []

    def offloading_open_ended_partition(self):
        if not self.partitions_to_offload:
            return False
        return self.partitions_to_offload.has_maxvalue_partition()


###########################################################################
# OffloadSourceDataIpaList
###########################################################################


class OffloadSourceDataIpaList(OffloadSourceDataInterface):
    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        """CONSTRUCTOR"""
        # cache attributes from offload_operation that we are interested in
        self._user_requested_lpa_filters = offload_operation.equal_to_values

        self._partition_append_capable = True
        self._incremental_append_capable = True
        self._partition_append_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST

        super(OffloadSourceDataIpaList, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )

        self._lpa_incremental_high_values = None
        self._lpa_partition_filters = None

        self.log(
            "Table %s.%s is %s partitioned by %s (%s)"
            % (
                offload_source_table.owner,
                offload_source_table.table_name,
                offload_source_table.partition_type,
                ", ".join([_.data_type for _ in self._partition_columns]),
                ", ".join([_.name for _ in self._partition_columns]),
            ),
            detail=VERBOSE,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _default_partition_has_been_offloaded(self):
        logger.debug("_default_partition_has_been_offloaded")
        assert self._offloaded_partitions
        return self._offloaded_partitions.has_default_partition()

    def _default_partition_has_matching_keys(self, list_of_keys):
        """Return True if an RDBMS DEFAULT partition has keys in list_of_keys."""
        if not list_of_keys:
            return False
        default_partition = self._source_partitions.has_default_partition()
        self.log(
            f"LPA table has {offload_constants.PART_OUT_OF_LIST} partition, fetching partition keys from {offload_constants.PART_OUT_OF_LIST} partition: {default_partition}",
            detail=VVERBOSE,
        )
        keys_in_default = self._get_lpa_keys_for_default_partition(default_partition)
        self.log(
            "LPA keys in open partition: %s" % str(keys_in_default), detail=VVERBOSE
        )
        matching_keys = [_ for _ in keys_in_default if _ in list_of_keys]
        if matching_keys:
            self.log(
                "LPA keys indicating open partition has been offloaded: %s"
                % str(matching_keys),
                detail=VVERBOSE,
            )
            return True
        return False

    def _normalise_lpa_partition_append_csv(self, csv_str):
        """Breaks up a user specified partition append string and returns a tuple of correctly
        typed Python values ready for comparison with partition_values_python
        """
        assert isinstance(csv_str, str)
        logger.debug("_normalise_lpa_partition_append_csv")
        pa_list = []
        if self._partition_columns[0].is_date_based():
            # For dates check for human input date formats, don't use RDBMS literal checking
            tokens = self._col_offload_source_table.split_partition_high_value_string(
                csv_str
            )
            pa_list.extend(
                [self._datetime_literal_to_python(token) for token in tokens]
            )
        else:
            # For all other data types use RDBMS literal checking
            pa_list = self._part_offload_source_table.decode_partition_high_values(
                csv_str
            )
        return tuple(pa_list)

    def _get_lpa_partition_filters(self):
        """Create a standard list of comparison tuples that can be compared with partition high values
        from the user requested input
        Also validates that the items in the tuple match the RDBMS column in both number
        and type
        """
        logger.debug("_get_lpa_partition_filters")

        if self._lpa_partition_filters is None:
            lpa_filter_list = []
            if self._user_requested_lpa_filters:
                logger.debug(
                    "filtering by user_requested_lpa_filters: %s"
                    % (self._user_requested_lpa_filters)
                )
                # the user request should be a list of csvs
                if type(self._user_requested_lpa_filters) not in (list, tuple):
                    raise OffloadSourceDataException(
                        "Invalid type for partition append filter: %s"
                        % type(self._user_requested_lpa_filters)
                    )
                for lpa_filter in self._user_requested_lpa_filters:
                    if isinstance(lpa_filter, str):
                        lpa_filter_list.append(
                            self._normalise_lpa_partition_append_csv(lpa_filter)
                        )
                    elif isinstance(lpa_filter, tuple):
                        lpa_filter_list.append(lpa_filter)
                    else:
                        raise OffloadSourceDataException(
                            "Invalid type for partition append filter: %s"
                            % type(lpa_filter)
                        )
            elif self._user_requested_partition_names:
                logger.debug("filtering by user_requested_partition_names")
                # find the named partitions and construct a list for comparison with p.partition_values_python
                for partition_name in self._user_requested_partition_names:
                    matched_partition = self._source_partitions.get_partition(
                        partition_name
                    )
                    if matched_partition:
                        lpa_filter_list.append(
                            matched_partition.partition_values_python
                        )
                    else:
                        raise OffloadSourceDataException(
                            "%s: %s"
                            % (NO_MATCHING_PARTITION_EXCEPTION_TEXT, partition_name)
                        )
            self.debug("Final filters: {}".format(lpa_filter_list))
            self._lpa_partition_filters = lpa_filter_list

        return self._lpa_partition_filters

    def _get_lpa_keys_from_target(self):
        """Runs a query getting a list of all backend values for RDBMS partition key"""
        logger.debug("_get_lpa_keys_from_target")
        if not self._partition_columns:
            return []

        column_names = [_.name for _ in self._partition_columns]
        ts_columns = [
            _.name
            for _ in self._partition_columns
            if _.data_type in (ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ)
        ]
        rows = self._offload_target_table.get_distinct_column_values(
            column_names, columns_to_cast_to_string=ts_columns
        )
        return self._get_lpa_keys_normalise_rows(self._partition_columns[0], rows)

    def _get_lpa_keys_for_default_partition(self, default_partition_name=None):
        """Gets a list of distinct partition keys in a default partition which we
        use to identify whether the partition has been offloaded or not.
        """
        if not default_partition_name:
            default_partition_name = self._source_partitions.has_default_partition()
            if not default_partition_name:
                return []

        # Use partition_column from _part_offload_source_table and not self._partition_columns because, for
        # join pushdown, it will not contain the correct column name.
        partition_column = self._part_offload_source_table.partition_columns[0]
        t1 = datetime.now().replace(microsecond=0)
        try:
            rows = self._part_offload_source_table.get_distinct_column(
                partition_column.name, default_partition_name
            )
        finally:
            t2 = datetime.now().replace(microsecond=0)
            self.log(
                "Elapsed time to retrieve distinct RDBMS partition key data: %s"
                % (t2 - t1),
                detail=VVERBOSE,
            )
        return self._get_lpa_keys_normalise_rows(partition_column, rows)

    def _get_lpa_keys_normalise_rows(self, partition_column, rows):
        to_unicode_fn = lambda s: s.decode("utf_8") if isinstance(s, bytes) else s
        assert partition_column
        if not rows:
            return []
        if partition_column.is_string_based():
            keys = [to_unicode_fn(_[0]) for _ in rows]
        else:
            keys = [_[0] for _ in rows]
        return keys

    def _get_source_partitions_matching_lpa_filters(self):
        """Inspects _source_partitions and returns a new list of partitions matching the user inputs
        This is required for metadata/hybrid view predicates
        """
        logger.debug("_get_source_partitions_matching_lpa_filters")
        lpa_filters = self._get_lpa_partition_filters()
        self.debug(
            "_get_source_partitions_matching_lpa_filters lpa_filters: %s"
            % str(lpa_filters)
        )
        filter_fn = self._lpa_partition_filter_fn(lpa_filters)
        matching_partitions = self._source_partitions.search_by_filter(filter_fn)
        return matching_partitions

    def _ida_options_have_been_specified(self):
        logger.debug("_ida_options_have_been_specified")
        if self._user_requested_lpa_filters or self._user_requested_partition_names:
            return True
        return False

    def _set_incremental_high_values(self):
        """Identify incremental high values for this offload for a 90/10 config
        Incremental high values means:
          - partitions matching the user requested filters
          - plus what hybrid metadata thinks has been offloaded (in case RDBMS partitions have been dropped)
        Does not add:
          - partitions in process of being offloaded. For FULL it is possible to offload partitions the user
            didn't specify. That's fine but we don't want them in the metadata
        Uses str() around each literal in order to be sure no mix of str/unicode
        De-dupes the final list before sorting
        """

        def get_metadata_list():
            if self._pre_offload_hybrid_metadata:
                (
                    _,
                    _,
                    hv_individual_list,
                ) = decode_metadata_incremental_high_values_from_metadata(
                    self._pre_offload_hybrid_metadata, self._col_offload_source_table
                )
                return hv_individual_list
            else:
                return []

        logger.debug("_set_incremental_high_values")
        if self._lpa_incremental_high_values is not None:
            self.debug(
                "Retaining LPA incremental high values: %s"
                % str(self._lpa_incremental_high_values)
            )
            return self._lpa_incremental_high_values

        if self._pred_for_90_10_in_hybrid_view:
            # 90/10 and 100/10 - we need to use whatever the user has requested and also metadata
            if self._get_lpa_partition_filters():
                self.debug("new HVs from filtered source partitions")
                matching_list_hvs = [
                    _.partition_values_individual
                    for _ in self._get_source_partitions_matching_lpa_filters()
                ]
            elif self._partitions_to_offload.count() > 0:
                self.debug("new HVs from partitions to offload")
                matching_list_hvs = [
                    _.partition_values_individual
                    for _ in self._partitions_to_offload.get_partitions()
                ]
            else:
                matching_list_hvs = []
            self.debug("matching_list_hvs: %s" % str(matching_list_hvs))

            if self._user_requested_hybrid_view_reset:
                if not matching_list_hvs:
                    # This is not good, we have a UNION ALL so MUST have predicates for the IN/NOT IN lists
                    raise OffloadSourceDataException(
                        "No matching partitions for hybrid view reset"
                    )
                metadata_hvs = []
            else:
                metadata_hvs = get_metadata_list()
                self.debug("metadata_hvs: %s" % str(metadata_hvs))

            # The order shouldn't really matter but we do have processes that check for changed metadata
            # and this at least ensures a no-op update of metadata does not flag up as a change in what's offloaded
            self._lpa_incremental_high_values = sorted(
                list(set(matching_list_hvs + metadata_hvs))
            )
        else:
            # 100/0 - no incremental values in views or metadata, also ignore any reset hybrid view request
            self._lpa_incremental_high_values = []

    def _populate_offloaded_partitions(self):
        """Inspects _source_partitions and populates a new list of offloaded partitions"""
        logger.debug("_populate_offloaded_partitions")
        if not self._offloaded_partitions:
            if not self._target_table_exists():
                self._offloaded_partitions = OffloadSourcePartitions()
            else:
                target_list_keys = self._get_lpa_keys_from_target()
                self.log(
                    "LPA keys in backend: %s" % str(target_list_keys), detail=VVERBOSE
                )
                if self._source_partitions.has_default_partition():
                    if self._default_partition_has_matching_keys(target_list_keys):
                        # Add DEFAULT to target_list_keys to indicate it has been offloaded
                        default_partition = (
                            self._source_partitions.get_default_partition()
                        )
                        target_list_keys.append(
                            default_partition.partition_values_python[0]
                        )

                def filter_fn(p):
                    # Using any() below because some key values may not exists but if "any" of
                    # them are in the backend then the partition has been offloaded
                    if any(_ in target_list_keys for _ in p.partition_values_python):
                        self.debug(
                            "Partition has been offloaded (i.e. is in backend keys): %s"
                            % p.partition_literal
                        )
                        return True
                    return False

                offloaded_partitions = self._source_partitions.search_by_filter(
                    filter_fn
                )
                self._offloaded_partitions = OffloadSourcePartitions(
                    offloaded_partitions
                )

            if (
                self._offload_type == OFFLOAD_TYPE_INCREMENTAL
                and self._default_partition_has_been_offloaded()
            ):
                raise OffloadSourceDataException(
                    "%s: %s"
                    % (
                        INCREMENTAL_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT,
                        self._offload_type,
                    )
                )

        return self._offloaded_partitions

    def _lpa_partition_filter_fn(self, lpa_filters):
        """Returns a function to be used when filtering partitions with p.apply_filter()
        The function first checks if the user is offloading a DEFAULT partition before
        going on to check for other partitions they may have requested. We do it that way
        around to avoid duck typing issues when comparing offload_constants.PART_OUT_OF_LIST with other types
        """

        def filter_fn(p):
            # Check if p is a requested DEFAULT partition
            if is_default_partition(p):
                upper_fn = lambda s: s.upper() if isinstance(s, str) else s
                if any(
                    upper_fn(list_item) == offload_constants.PART_OUT_OF_LIST
                    for lpa_filter in lpa_filters
                    for list_item in lpa_filter
                ):
                    # User has requested a default partition
                    return True
                else:
                    return False
            # Check if partition p is a requested partition
            if p.partition_values_python in lpa_filters:
                return True
            # Partition p isn't in the requested list
            return False

        return filter_fn

    def _partitions_to_offload_apply_lpa_filter(self, lpa_filters):
        """Reduce the list of partitions to offload by the user filter
        Default partition processing is a bit gnarly because:
            1) It can be any case
            2) It is a string value but valid in a non-string column
        """
        filter_fn = self._lpa_partition_filter_fn(lpa_filters)
        if lpa_filters:
            self.log("lpa_filters: %s" % str(lpa_filters), detail=VVERBOSE)
            self._partitions_to_offload.apply_filter(filter_fn)

    def _discard_partitions_using_filters(self):
        """Apply LPA filters to the list of partitions to be offloaded"""
        logger.debug("_discard_partitions_using_filters")
        lpa_filters = self._get_lpa_partition_filters()
        self._partitions_to_offload_apply_lpa_filter(lpa_filters)
        if (
            self._offload_type == OFFLOAD_TYPE_INCREMENTAL
            and self.partitions_to_offload.has_default_partition()
        ):
            raise OffloadSourceDataException(
                "%s %s for offload type %s"
                % (
                    IPA_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT,
                    self.partitions_to_offload.has_default_partition(),
                    self._offload_type,
                )
            )
        if self._offload_by_subpartition:
            raise NotImplementedError(
                "LIST subpartition level offload is not supported"
            )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload_data_detection(self):
        """Review candidate partitions and filter them by what's already been offloaded and any user requested options
        Report the different partition categories as appropriate and any warnings/notices
        Partitions in each category (offloaded, to offload, etc) are held in state - not returned
        """
        self.populate_source_partitions()
        self._report_source_partitions()
        if not self.is_partition_append_capable():
            return

        self._populate_offloaded_partitions()
        if self.offloaded_partitions.count() > 0:
            self._report_offloaded_partitions()

        self._populate_partitions_to_offload()
        self._discard_partitions_using_filters()
        self._nothing_to_offload = bool(self._partitions_to_offload.count() == 0)

        self._set_incremental_high_values()

        if self.partitions_to_offload.count() == 0:
            self.log("No partitions to %s" % self._terms["name"])
        else:
            # Keep and discard lists are just for reporting. We need to continue with the to-offload partition set after reporting
            (
                keep_partitions,
                discard_partitions,
            ) = self.split_partitions_to_offload_by_no_segment()

            self.log("To %s:" % self._terms["name"])
            keep_partitions.report_partitions(
                self._part_offload_source_table.offload_by_subpartition, self._messages
            )

            if discard_partitions.count() > 0:
                self.log("To skip (no segments):")
                discard_partitions.report_partitions(
                    self._part_offload_source_table.offload_by_subpartition,
                    self._messages,
                )

            if self.partitions_to_offload.has_default_partition():
                self._messages.notice(
                    "%s partition %s with high_value of %s"
                    % (
                        self._terms["now"].capitalize(),
                        self.partitions_to_offload.has_default_partition(),
                        offload_constants.PART_OUT_OF_LIST,
                    )
                )

    def get_incremental_high_values(self):
        logger.debug("get_incremental_high_values")
        if self._lpa_incremental_high_values:
            return self._lpa_incremental_high_values
        else:
            return []

    def offloading_open_ended_partition(self):
        if not self.partitions_to_offload:
            return False
        return self.partitions_to_offload.has_default_partition()


###########################################################################
# OffloadSourceDataIpaListAsRange
###########################################################################


class OffloadSourceDataIpaListAsRange(OffloadSourceDataIpaRange):
    """This is a limited extension of OffloadSourceDataIpaRange used when customers have partitioned Oracle
    by LIST but to mimic RANGE partitioning.
    This class inherits most functionality from Range but with these additions:
        1) RDBMS partitions are sorted by HIGH_VALUE in order to ensure matched RANGE behaviour
        2) HIGH_VALUE logic is changed from <= to < to match RANGE logic
    """

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        """CONSTRUCTOR"""
        # Cache attributes from offload_operation that we are interested in
        self._user_requested_list_rpa_lte_filter = offload_operation.less_or_equal_value

        super(OffloadSourceDataIpaListAsRange, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )

        self._partition_append_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
        self._list_rpa_lte_filter = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _check_for_unsupported_partitions(self):
        """For LIST_AS_RANGE we cannot allow any partitions with multiple HVs on a single partition.
        This should be called after populating self._source_partitions.
        """
        assert self._source_partitions

        def filter_fn(p):
            if len(p.partition_values_python) > 1:
                self.debug(
                    "Partition has multiple multiple high values: %s"
                    % p.partition_literal
                )
                return True
            return False

        matching_partitions = self._source_partitions.search_by_filter(filter_fn)
        if matching_partitions:
            raise OffloadSourceDataException(
                "%s: %s"
                % (
                    INVALID_HV_FOR_LIST_AS_RANGE_EXCEPTION_TEXT,
                    [_.partition_name for _ in matching_partitions],
                )
            )

    def _do_not_offload_default_partition(self):
        """If there's a DEFAULT partition to offload then remove it and return the partition name"""
        logger.debug("_do_not_offload_default_partition")
        return self._partitions_to_offload.remove_default_partition()

    def _do_not_offload_open_ended_partition(self):
        """For INCREMENTAL LIST_AS_RANGE do not offload a DEFAULT partition."""
        removed_default_partition = None
        if (
            self._offload_type == OFFLOAD_TYPE_INCREMENTAL
            and self.partitions_to_offload.has_default_partition()
        ):
            removed_default_partition = self._do_not_offload_default_partition()
        return removed_default_partition

    def _do_not_offload_open_ended_partition_notice(self):
        return NO_DEFAULT_PARTITION_NOTICE_TEXT

    def _get_less_than_rpa_filter_fn(self):
        logger.debug("_get_less_than_rpa_filter_fn")
        if self._get_list_rpa_lte_filter():
            lte_list = self._get_list_rpa_lte_filter()
            less_than_list = None
        else:
            lte_list = None
            less_than_list = self._get_rpa_filter()

        def filter_fn(p):
            if is_default_partition(p):
                self.log(
                    f"Rejecting {offload_constants.PART_OUT_OF_LIST} partition {p.partition_name}",
                    detail=VVERBOSE,
                )
                return False
            if less_than_list:
                match = bool(tuple(p.partition_values_python) < tuple(less_than_list))
                self.debug(
                    "Matching partition %s (%s < %s): %s"
                    % (
                        p.partition_name,
                        str(p.partition_values_python),
                        str(less_than_list),
                        match,
                    )
                )
            else:
                match = bool(tuple(p.partition_values_python) <= tuple(lte_list))
                self.debug(
                    "Matching partition %s (%s <= %s): %s"
                    % (
                        p.partition_name,
                        str(p.partition_values_python),
                        str(lte_list),
                        match,
                    )
                )
            return match

        return filter_fn

    def _get_list_rpa_lte_filter(self):
        logger.debug("_list_rpa_lte_filter")
        if self._list_rpa_lte_filter is None:
            self._normalise_list_rpa_lte_filter()
        return self._list_rpa_lte_filter

    def _ida_options_have_been_specified(self):
        if (
            self._user_requested_rpa_filter
            or self._user_requested_partition_names
            or self._user_requested_list_rpa_lte_filter
        ):
            return True
        return False

    def _normalise_list_rpa_lte_filter(self):
        if self._user_requested_list_rpa_lte_filter:
            self._list_rpa_lte_filter = self._normalise_rpa_partition_filter(
                self._user_requested_list_rpa_lte_filter
            )
        else:
            self._list_rpa_lte_filter = tuple()
        return self._list_rpa_lte_filter

    def _populate_source_partitions_rpa(self):
        """Populate source_partitions from the source RDBMS.
        For LIST_AS_RANGE we cannot allow any partitions with multiple HVs on a single partitions.
        """
        self.populate_source_partitions(sort_by_hv=True)
        self._check_for_unsupported_partitions()

    def _populate_offloaded_partitions(self):
        """Inspects _source_partitions and populates a new list of offloaded partitions"""
        logger.debug("_populate_offloaded_partitions")
        if not self._offloaded_partitions:
            if self._user_requested_table_reset or not self._target_table_exists():
                self._offloaded_partitions = OffloadSourcePartitions()
            else:
                self._offloaded_partitions = OffloadSourcePartitions(
                    self._rpa_partitions_already_offloaded()
                )

            if (
                self._offload_type == OFFLOAD_TYPE_INCREMENTAL
                and self._maxvalue_partition_has_been_offloaded()
            ):
                raise OffloadSourceDataException(
                    f"Offload type {self._offload_type} invalid when {offload_constants.PART_OUT_OF_RANGE} partition has been offloaded"
                )

        return self._offloaded_partitions

    def _rpa_partition_already_offloaded(
        self, partition, partition_already_marked_offloaded
    ):
        """This method is used to differentiate between true RANGE and LIST_AS_RANGE.
        We can ignore partition_already_marked_offloaded because LIST_AS_RANGE HVs are like-for-like.
        """
        match = bool(tuple(partition.partition_values_python) <= self._target_table_max)
        self.debug(
            "Partition %s already offloaded check: %s"
            % (partition.partition_name, match)
        )
        return match

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################


###########################################################################
# OffloadSourceDataIUPartitioned
###########################################################################


class OffloadSourceDataIUPartitioned(OffloadSourceDataInterface):
    """Get source partitions for an Incremental Update extraction"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        hybrid_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=None,
        col_offload_source_table_override=None,
    ):
        """CONSTRUCTOR"""
        self._partition_append_capable = False
        self._incremental_append_capable = False
        self._partition_append_predicate_type = None

        super(OffloadSourceDataIUPartitioned, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            hybrid_metadata,
            messages,
            source_client_type=source_client_type,
            rdbms_partition_columns_override=rdbms_partition_columns_override,
            col_offload_source_table_override=col_offload_source_table_override,
        )

        messages.notice(
            "Table %s.%s is %s partitioned, %s across entire table"
            % (
                offload_source_table.owner,
                offload_source_table.table_name,
                offload_source_table.partition_type,
                self._terms["now"],
            )
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _ida_options_have_been_specified(self):
        return False

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def offload_data_detection(self):
        self.populate_source_partitions()
        self._report_source_partitions()
        # All partitions are in scope for extraction
        self._offloaded_partitions = OffloadSourcePartitions()
        self._populate_partitions_to_offload()
        self._nothing_to_offload = bool(self._partitions_to_offload.count() == 0)

    def get_incremental_high_values(self):
        return []

    def offloading_open_ended_partition(self):
        if not self.partitions_to_offload:
            return False
        return bool(
            self.partitions_to_offload.has_maxvalue_partition()
            or self.partitions_to_offload.has_default_partition()
        )
