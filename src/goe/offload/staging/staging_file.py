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

""" OffloadStagingFile: Library for logic/interaction with a staging file/table that will be populated during Offload Transport
"""

from abc import ABCMeta, abstractmethod
import logging

from goe.offload.column_metadata import valid_column_list
from goe.offload.offload_constants import (
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.offload.offload_messages import VVERBOSE


###############################################################################
# CONSTANTS
###############################################################################

JAVA_PRIMITIVE_BOOLEAN = "Boolean"
JAVA_PRIMITIVE_DOUBLE = "Double"
JAVA_PRIMITIVE_FLOAT = "Float"
JAVA_PRIMITIVE_INTEGER = "Integer"
JAVA_PRIMITIVE_LONG = "Long"
JAVA_PRIMITIVE_SHORT = "Short"
JAVA_PRIMITIVE_STRING = "String"

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OffloadStagingFileInterface
###########################################################################


class OffloadStagingFileInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for format specific sub-classes"""

    def __init__(
        self,
        load_db_name,
        table_name,
        staging_file_format,
        canonical_columns,
        binary_data_as_base64,
        messages,
        dry_run=False,
    ):
        assert load_db_name and table_name
        assert staging_file_format in (
            FILE_STORAGE_FORMAT_AVRO,
            FILE_STORAGE_FORMAT_PARQUET,
        ), "%s not in %s" % (
            staging_file_format,
            [FILE_STORAGE_FORMAT_AVRO, FILE_STORAGE_FORMAT_PARQUET],
        )
        assert canonical_columns
        assert valid_column_list(canonical_columns)

        # cache parameters in state
        self.load_db_name = load_db_name.lower()
        self.table_name = table_name.lower()
        self.file_format = staging_file_format
        self._canonical_columns = canonical_columns
        self._binary_data_as_base64 = binary_data_as_base64
        self._messages = messages
        self._dry_run = dry_run

        self._staging_columns = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _log(self, msg, detail=None):
        self._messages.log(msg, detail=detail)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _debug(self, msg):
        self._messages.debug(msg)
        logger.debug(msg)

    def _map_from_canonical_columns(self):
        staging_columns = []
        for canonical_column in self._canonical_columns:
            staging_columns.append(self.from_canonical_column(canonical_column))
        for i, c in enumerate(staging_columns):
            c.set_simplified_staging_column_name(i)
        self._log("Staging columns:", detail=VVERBOSE)
        [self._log(str(_), detail=VVERBOSE) for _ in staging_columns]
        return staging_columns

    # Enforced private methods

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def get_staging_columns(self):
        """Return the list of columns appropriate for the staging file format, e.g. AvroColumn()s or ParquetColumn()s"""
        if self._staging_columns is None:
            self._staging_columns = self._map_from_canonical_columns()
        return self._staging_columns

    def get_canonical_staging_columns(self, use_staging_file_names=False):
        """Return a list of canonical columns generated from staging columns.
        This is different to the canonical columns used outside of offload transport as many columns are staged
        as strings.
        """
        return [
            self.to_canonical_column(
                staging_column, use_staging_file_name=use_staging_file_names
            )
            for staging_column in self.get_staging_columns()
        ]

    # Enforced methods/properties

    @abstractmethod
    def from_canonical_column(self, column):
        """Translate an internal GOE column to a staging file column"""

    @abstractmethod
    def to_canonical_column(self, column):
        """Translate a staging file column to an internal GOE column"""

    @abstractmethod
    def get_java_primitive(self, staging_column):
        """Return a java primitive for a staging column which will be used by Sqoop/Spark
        when unloading data.
        """

    @abstractmethod
    def get_file_schema_json(self, as_string=True):
        pass
