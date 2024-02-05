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

""" OrchestrationMetadata: Object containing orchestration metadata for a single hybrid object.
"""

import logging
from typing import Optional, Union, TYPE_CHECKING

from goe.offload.predicate_offload import GenericPredicate
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.util.misc_functions import csv_split

if TYPE_CHECKING:
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


###############################################################################
# CONSTANTS
###############################################################################

# Metadata key constants
HADOOP_OWNER = "HADOOP_OWNER"
HADOOP_TABLE = "HADOOP_TABLE"
OFFLOAD_TYPE = "OFFLOAD_TYPE"
OFFLOADED_OWNER = "OFFLOADED_OWNER"
OFFLOADED_TABLE = "OFFLOADED_TABLE"
INCREMENTAL_KEY = "INCREMENTAL_KEY"
INCREMENTAL_HIGH_VALUE = "INCREMENTAL_HIGH_VALUE"
INCREMENTAL_RANGE = "INCREMENTAL_RANGE"
INCREMENTAL_PREDICATE_TYPE = "INCREMENTAL_PREDICATE_TYPE"
INCREMENTAL_PREDICATE_VALUE = "INCREMENTAL_PREDICATE_VALUE"
OFFLOAD_BUCKET_COLUMN = "OFFLOAD_BUCKET_COLUMN"
OFFLOAD_SORT_COLUMNS = "OFFLOAD_SORT_COLUMNS"
OFFLOAD_SNAPSHOT = "OFFLOAD_SNAPSHOT"
OFFLOAD_PARTITION_FUNCTIONS = "OFFLOAD_PARTITION_FUNCTIONS"
COMMAND_EXECUTION = "COMMAND_EXECUTION"

ALL_METADATA_ATTRIBUTES = [
    HADOOP_OWNER,
    HADOOP_TABLE,
    OFFLOAD_TYPE,
    OFFLOADED_OWNER,
    OFFLOADED_TABLE,
    INCREMENTAL_KEY,
    INCREMENTAL_HIGH_VALUE,
    INCREMENTAL_RANGE,
    INCREMENTAL_PREDICATE_TYPE,
    INCREMENTAL_PREDICATE_VALUE,
    OFFLOAD_BUCKET_COLUMN,
    OFFLOAD_SORT_COLUMNS,
    OFFLOAD_SNAPSHOT,
    OFFLOAD_PARTITION_FUNCTIONS,
    COMMAND_EXECUTION,
]

# Map of object attribute names and their metadata key
METADATA_ATTRIBUTES = {
    "backend_owner": HADOOP_OWNER,
    "backend_table": HADOOP_TABLE,
    "offload_type": OFFLOAD_TYPE,
    "offloaded_owner": OFFLOADED_OWNER,
    "offloaded_table": OFFLOADED_TABLE,
    "incremental_key": INCREMENTAL_KEY,
    "incremental_high_value": INCREMENTAL_HIGH_VALUE,
    "incremental_range": INCREMENTAL_RANGE,
    "incremental_predicate_type": INCREMENTAL_PREDICATE_TYPE,
    "incremental_predicate_value": INCREMENTAL_PREDICATE_VALUE,
    "offload_bucket_column": OFFLOAD_BUCKET_COLUMN,
    "offload_sort_columns": OFFLOAD_SORT_COLUMNS,
    "offload_snapshot": OFFLOAD_SNAPSHOT,
    "offload_partition_functions": OFFLOAD_PARTITION_FUNCTIONS,
    "command_execution": COMMAND_EXECUTION,
}

INCREMENTAL_PREDICATE_TYPE_PREDICATE = "PREDICATE"
INCREMENTAL_PREDICATE_TYPE_LIST = "LIST"
INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE = "LIST_AS_RANGE"
INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE = "LIST_AS_RANGE_AND_PREDICATE"
INCREMENTAL_PREDICATE_TYPE_RANGE = "RANGE"
INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE = "RANGE_AND_PREDICATE"
INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV = [
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
]
INCREMENTAL_PREDICATE_TYPES = [
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
]


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def add_missing_metadata_keys(metadata_dict):
    assert isinstance(metadata_dict, dict)
    for key_name in METADATA_ATTRIBUTES.values():
        if key_name not in metadata_dict:
            metadata_dict[key_name] = None


def build_client(connection_options=None, messages=None, client=None, dry_run=False):
    if client:
        return client
    else:
        assert connection_options
        assert messages
        return orchestration_repo_client_factory(
            connection_options,
            messages,
            dry_run=dry_run,
            trace_action="repo_client(OrchestrationMetadata)",
        )


def hwm_column_names_from_predicates(offload_predicates):
    """Look at offload_predicates and form a list which can be compared to
    metadata result of OrchestrationMetadata.hwm_column_names().
    Column names are upper cased for ease of comparison.
    """
    if offload_predicates:
        assert isinstance(offload_predicates, list)
        assert isinstance(offload_predicates[0], GenericPredicate)

    col_list = []
    for pred_cols in [_.column_names() for _ in (offload_predicates or [])]:
        col_list.extend([_.upper() for _ in pred_cols])
    col_list = sorted(list(set(col_list)))
    return col_list


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OrchestrationMetadata
###########################################################################


class OrchestrationMetadata:
    """Object containing orchestration metadata for a single hybrid object.
    There are three expected paths for creating these objects:
    1) Generated using an OrchestrationMetadataClient.get_offload_metadata() call.
       from_name() is a wrapper for this path.
    2) Instantiated from a metadata dict.
    3) Instantiated from a list of attributes.
    Once instantiated the object has a save() method which can be used to persist the current state.
    If the object was created via paths 2 or 3 then it has the capability to generate its own client,
    assuming connection_options and messages are set.
    """

    def __init__(
        self,
        metadata: Union[dict, "OrchestrationMetadata"],
        connection_options=None,
        messages=None,
        client=None,
        dry_run=False,
    ):
        """Object containing orchestration metadata for a single hybrid object.
        metadata: Either a dict or metadata object to seed the new metadata object.
        client: Used to interact with metadata repo, only required if drop or save will be used.
        connection_options/messages: Alternative to client, builds a client.
                                     Only required if drop or save will be used.
        dry_run: Read only mode
        """
        logger.debug("Instantiating OrchestrationMetadata from metadata")
        assert isinstance(metadata, (dict, OrchestrationMetadata))
        if client:
            self._client = build_client(
                connection_options, messages, client, dry_run=dry_run
            )
        else:
            # We'll be lazy with client and only create it if we need it
            self._client = None
        if isinstance(metadata, OrchestrationMetadata):
            self._metadata = metadata.as_dict()
        else:
            self._metadata = metadata
        self._connection_options = connection_options
        self._messages = messages
        self._dry_run = dry_run

    @staticmethod
    def from_name(
        frontend_owner: str,
        frontend_name: str,
        connection_options=None,
        messages=None,
        client=None,
        dry_run=False,
    ):
        """Instantiate a metadata object by owner/name"""
        logger.debug(
            f"Instantiating OrchestrationMetadata from name: {frontend_owner}.{frontend_name}"
        )
        client = build_client(connection_options, messages, client, dry_run=dry_run)
        metadata = client.get_offload_metadata(frontend_owner, frontend_name)
        if metadata:
            return OrchestrationMetadata(
                metadata,
                connection_options=connection_options,
                messages=messages,
                client=client,
                dry_run=dry_run,
            )
        else:
            return None

    @staticmethod
    def from_attributes(
        connection_options=None, messages=None, client=None, dry_run=False, **kwargs
    ):
        """Instantiate a metadata object from a set of attributes"""
        logger.debug("Instantiating OrchestrationMetadata from attributes")
        metadata_dict = {}
        for k, v in kwargs.items():
            if k in METADATA_ATTRIBUTES:
                metadata_dict[METADATA_ATTRIBUTES[k]] = v
        add_missing_metadata_keys(metadata_dict)
        return OrchestrationMetadata(
            metadata_dict,
            connection_options=connection_options,
            messages=messages,
            client=client,
            dry_run=dry_run,
        )

    def __repr__(self):
        return str(self._metadata)

    def __str__(self):
        return str(self._metadata)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_client(self) -> "OrchestrationRepoClientInterface":
        if not self._client:
            self._client = build_client(
                self._connection_options, self._messages, None, dry_run=self._dry_run
            )
        return self._client

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def as_dict(self):
        return self._metadata

    @property
    def client(self) -> "OrchestrationRepoClientInterface":
        return self._get_client()

    def drop(self):
        """Persist metadata"""
        self._get_client().drop_offload_metadata(
            self.offloaded_owner, self.offloaded_table
        )

    def save(self):
        """Persist metadata"""
        self._get_client().set_offload_metadata(self._metadata)

    def decode_incremental_predicate_values(self, as_dsl=False):
        """Take incremental_predicate_value metadata and turn it back into the structure used during Offload."""
        if not self._metadata or not self.incremental_predicate_value:
            return None
        if as_dsl:
            # We've converted from DSL to GenericPredicate and back again just in case formatting changes, we want to
            # let GenericPredicate.dsl return a consistent format.
            return [GenericPredicate(_).dsl for _ in self.incremental_predicate_value]
        else:
            return [GenericPredicate(_) for _ in self.incremental_predicate_value]

    def hwm_column_names(self) -> list:
        """Look at metadata and return a unique list of column names in the hybrid view HWM predicate
        Column names are upper cased for ease of comparison.
        Do not change without reviewing hwm_column_names_from_predicates().
        """
        if not self.is_hwm_in_hybrid_view():
            return []
        inc_keys = (
            csv_split(self.incremental_key.upper()) if self.incremental_key else []
        )
        pred_cols = hwm_column_names_from_predicates(
            self.decode_incremental_predicate_values()
        )
        inc_keys = sorted(list(set(inc_keys + pred_cols)))
        return inc_keys

    def incremental_data_append_feature(self) -> Optional[str]:
        """Return the feature name for an INCREMENTAL_PREDICATE_TYPE"""
        if not self._metadata:
            return None

        ida_features = []
        if self.incremental_range:
            ida_features.append(self.incremental_range.title())
        if (
            self.incremental_predicate_type
            in INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV
        ):
            ida_features.append("Predicate")

        if ida_features:
            return " and ".join(ida_features) + "-Based Offload"
        else:
            return "Full Offload"

    def is_hwm_in_hybrid_view(self) -> bool:
        """Look at metadata and return True if we expect the hybrid view to contain a UNION ALL.
        i.e. 90/10 or 100/10.
        """
        return bool(
            (self.incremental_key and self.incremental_high_value)
            or self.incremental_predicate_value
        )

    def is_subpartition_offload(self) -> bool:
        """Does metadata indicate this is a subpartition offloaded table. Returns True or False"""
        return bool(self._metadata and self.incremental_range == "SUBPARTITION")

    def update_offloaded_attributes_for_join(self, offloaded_owner, offloaded_table):
        """Ordinarily we shouldn't be able to set offloaded_* attributes but joins have that requirement.
        Instead of adding setters I've added this method.
        """
        self._metadata["OFFLOADED_OWNER"] = offloaded_owner
        self._metadata["OFFLOADED_TABLE"] = offloaded_table

    ###########################################################################
    # PUBLIC ATTRIBUTES. Most are readonly.
    ###########################################################################

    @property
    def backend_owner(self):
        return self._metadata[HADOOP_OWNER]

    @property
    def backend_table(self):
        return self._metadata[HADOOP_TABLE]

    @property
    def offload_type(self):
        return self._metadata[OFFLOAD_TYPE]

    @offload_type.setter
    def offload_type(self, new_value):
        self._metadata[OFFLOAD_TYPE] = new_value

    @property
    def offloaded_owner(self):
        return self._metadata[OFFLOADED_OWNER]

    @property
    def offloaded_table(self):
        return self._metadata[OFFLOADED_TABLE]

    @property
    def incremental_key(self):
        return self._metadata[INCREMENTAL_KEY]

    @property
    def incremental_high_value(self):
        return self._metadata[INCREMENTAL_HIGH_VALUE]

    @incremental_high_value.setter
    def incremental_high_value(self, new_value):
        self._metadata[INCREMENTAL_HIGH_VALUE] = new_value

    @property
    def incremental_range(self):
        return self._metadata[INCREMENTAL_RANGE]

    @property
    def incremental_predicate_type(self):
        return self._metadata[INCREMENTAL_PREDICATE_TYPE]

    @property
    def incremental_predicate_value(self):
        return self._metadata[INCREMENTAL_PREDICATE_VALUE]

    @incremental_predicate_value.setter
    def incremental_predicate_value(self, new_value):
        self._metadata[INCREMENTAL_PREDICATE_VALUE] = new_value

    @property
    def offload_bucket_column(self):
        return self._metadata[OFFLOAD_BUCKET_COLUMN]

    @property
    def offload_sort_columns(self):
        return self._metadata[OFFLOAD_SORT_COLUMNS]

    @offload_sort_columns.setter
    def offload_sort_columns(self, new_value):
        self._metadata[OFFLOAD_SORT_COLUMNS] = new_value

    @property
    def offload_snapshot(self):
        return self._metadata[OFFLOAD_SNAPSHOT]

    @offload_snapshot.setter
    def offload_snapshot(self, new_value):
        self._metadata[OFFLOAD_SNAPSHOT] = new_value

    @property
    def offload_partition_functions(self):
        return self._metadata[OFFLOAD_PARTITION_FUNCTIONS]

    @property
    def command_execution(self):
        return self._metadata[COMMAND_EXECUTION]

    @command_execution.setter
    def command_execution(self, new_value):
        self._metadata[COMMAND_EXECUTION] = new_value

    @property
    def repo_client(self):
        return self._get_client()
