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

""" ParquetColumn: Parquet implementation of ColumnMetadataInterface
"""

from goe.offload.column_metadata import ColumnMetadataInterface

###############################################################################
# CONSTANTS
###############################################################################

PARQUET_TYPE_BINARY = "BINARY"
PARQUET_TYPE_BOOLEAN = "BOOLEAN"
PARQUET_TYPE_DOUBLE = "DOUBLE"
PARQUET_TYPE_FLOAT = "FLOAT"
PARQUET_TYPE_INT32 = "INT32"
PARQUET_TYPE_INT64 = "INT64"
PARQUET_TYPE_STRING = "STRING"


###########################################################################
# CLASSES
###########################################################################


class StagingParquetColumn(ColumnMetadataInterface):
    """Holds details for a single table column
    Table objects hold a list of objects of this class
    """

    def __init__(
        self,
        name,
        data_type,
        data_length=None,
        data_precision=None,
        data_scale=None,
        nullable=None,
        data_default=None,
        safe_mapping=True,
        char_semantics=None,
    ):
        super(StagingParquetColumn, self).__init__(
            name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            data_default,
            safe_mapping,
            char_semantics=char_semantics,
        )

    def format_data_type(self):
        # Currently no staging data types have a length/precision/scale so no formatting required
        return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        # We should not need to this for a staging column
        raise NotImplementedError(
            "has_time_element() is not applicable to a StagingParquetColumn"
        )

    def is_binary(self):
        return bool(self.data_type == PARQUET_TYPE_BINARY)

    def is_nan_capable(self):
        # We should not need to this for a staging column
        raise NotImplementedError(
            "is_nan_capable() is not applicable to a StagingParquetColumn"
        )

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in (
                PARQUET_TYPE_DOUBLE,
                PARQUET_TYPE_FLOAT,
                PARQUET_TYPE_INT32,
                PARQUET_TYPE_INT64,
            )
        )

    def is_date_based(self):
        """Is the column date in class"""
        return False

    def is_interval(self):
        return False

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(self.data_type == PARQUET_TYPE_STRING)

    def is_time_zone_based(self):
        return False
