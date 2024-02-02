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

""" HadoopDataGovernanceInterface: Interface to Hadoop data governance APIs
"""

from abc import ABCMeta, abstractmethod
from goe.data_governance.hadoop_data_governance_constants import *
from goe.offload.offload_messages import VERBOSE, VVERBOSE

###############################################################################
# EXCEPTIONS
###############################################################################


class HadoopDataGovernanceException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# STANDALONE FUNCTIONS
###############################################################################


class HadoopDataGovernanceInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for Hadoop distro specific sub-classes"""

    def __init__(self):
        # state to cache values that may be used across multiple API calls
        self._auto_tags_csv = None
        self._custom_tags_csv = None
        self._auto_properties_csv = None
        self._custom_properties = None
        self._rdbms_name = None
        self._rdbms_schema = None
        self._source_rdbms_object_name = None
        self._target_rdbms_object_name = None

    # mandatory attributes (properties)
    @abstractmethod
    def pre_or_post_registration(self):
        pass

    # mandatory methods
    @abstractmethod
    def healthcheck_api(self):
        pass

    @abstractmethod
    def new_custom_metadata(self, description=None, tags=[], properties={}):
        pass

    @abstractmethod
    def get_custom_metadata(self, entity_id, ignore_missing_metadata=False):
        pass

    @abstractmethod
    def get_hive_object_metadata(
        self, db, object_name, ignore_missing_metadata=False, object_type=None
    ):
        pass

    @abstractmethod
    def get_hive_table_metadata(self, db, table_name, ignore_missing_metadata=False):
        pass

    @abstractmethod
    def get_hive_view_metadata(self, db, view_name, ignore_missing_metadata=False):
        pass

    @abstractmethod
    def get_hive_db_metadata(self, db, ignore_missing_metadata=False):
        pass

    @abstractmethod
    def update_custom_metadata(self, entity_id, new_metadata):
        pass

    @abstractmethod
    def register_custom_object_metadata(self, db, table_name, custom_metadata):
        pass

    @abstractmethod
    def register_custom_db_metadata(self, db, table_name, custom_metadata):
        pass

    @abstractmethod
    def get_custom_metadata_from_full_metadata(self, full_metadata):
        pass

    @abstractmethod
    def get_tags_from_metadata(self, metadata):
        pass

    @abstractmethod
    def get_properties_from_metadata(self, metadata):
        pass

    # common (non backend specific) methods
    def cache_property_values(
        self,
        auto_tags_csv,
        custom_tags_csv,
        auto_properties_csv,
        custom_properties,
        rdbms_name=None,
        rdbms_schema=None,
        source_rdbms_object_name=None,
        target_rdbms_object_name=None,
    ):
        """Caches property values that will be useful across multiple API calls"""
        self._auto_tags_csv = auto_tags_csv
        self._custom_tags_csv = custom_tags_csv
        self._auto_properties_csv = auto_properties_csv
        self._custom_properties = custom_properties
        self._rdbms_name = rdbms_name
        self._rdbms_schema = rdbms_schema
        self._source_rdbms_object_name = source_rdbms_object_name
        self._target_rdbms_object_name = target_rdbms_object_name

    def get_hive_object_custom_metadata(
        self, db, object_name, ignore_missing_metadata=False, object_type=None
    ):
        assert db and object_name
        metadata = self.get_hive_object_metadata(
            db,
            object_name,
            ignore_missing_metadata=ignore_missing_metadata,
            object_type=object_type,
        )
        if metadata:
            return self.get_custom_metadata_from_full_metadata(metadata)

    def get_hive_table_custom_metadata(
        self, db, table_name, ignore_missing_metadata=False
    ):
        """Get just the custom metadata dict for a hive table
        These are the fields we can change
        """
        assert db and table_name
        metadata = self.get_hive_table_metadata(
            db, table_name, ignore_missing_metadata=ignore_missing_metadata
        )
        if metadata:
            return self.get_custom_metadata_from_full_metadata(metadata)

    @property
    def auto_tags_csv(self):
        return self._auto_tags_csv

    @property
    def custom_tags_csv(self):
        return self._custom_tags_csv

    @property
    def auto_properties_csv(self):
        return self._auto_properties_csv

    @property
    def custom_properties(self):
        return self._custom_properties

    @property
    def rdbms_name(self):
        return self._rdbms_name

    @property
    def rdbms_schema(self):
        return self._rdbms_schema

    @property
    def source_rdbms_object_name(self):
        return self._source_rdbms_object_name

    @property
    def target_rdbms_object_name(self):
        return self._target_rdbms_object_name

    def expand_dynamic_data_governance_tag(self, tag):
        """Expands a dynamic tag to the runtime value"""
        if tag[0] == DATA_GOVERNANCE_DYNAMIC_TAG_PREFIX:
            dyn_tag = tag[1:].upper()
            if dyn_tag == DATA_GOVERNANCE_DYNAMIC_TAG_RDBMS_NAME:
                assert self.rdbms_name, "Rdbms name must be cached for dynamic tag"
                return self.rdbms_name.upper()
            else:
                return tag
        elif tag[0] == DATA_GOVERNANCE_DYNAMIC_TAG_EXCLUSION_PREFIX:
            # allow a minus sign to signify skipping a tag
            return None
        else:
            return tag
