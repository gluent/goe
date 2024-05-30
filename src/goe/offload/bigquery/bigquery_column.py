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

""" BigQueryColumn: Oracle implementation of ColumnMetadataInterface
"""

from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
)

###############################################################################
# CONSTANTS
###############################################################################

BIGQUERY_TYPE_BIGNUMERIC = "BIGNUMERIC"
BIGQUERY_TYPE_BOOLEAN = "BOOLEAN"
BIGQUERY_TYPE_BYTES = "BYTES"
BIGQUERY_TYPE_DATE = "DATE"
BIGQUERY_TYPE_DATETIME = "DATETIME"
# BIGQUERY_TYPE_FLOAT for translation purposes only, not a supported type
BIGQUERY_TYPE_FLOAT = "FLOAT"
BIGQUERY_TYPE_FLOAT64 = "FLOAT64"
BIGQUERY_TYPE_INT64 = "INT64"
# BIGQUERY_TYPE_INTEGER for translation purposes only, not a supported type
BIGQUERY_TYPE_INTEGER = "INTEGER"
BIGQUERY_TYPE_NUMERIC = "NUMERIC"
BIGQUERY_TYPE_STRING = "STRING"
BIGQUERY_TYPE_TIME = "TIME"
BIGQUERY_TYPE_TIMESTAMP = "TIMESTAMP"


###########################################################################
# CLASSES
###########################################################################


class BigQueryColumn(ColumnMetadataInterface):
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
        partition_info=None,
        char_length=None,
    ):
        if data_length and char_length is None:
            char_length = data_length
        # Confusingly the API uses BIGQUERY_TYPE_FLOAT/BIGQUERY_TYPE_INTEGER but SQL uses the *64 names.
        # We have standardised on the SQL names.
        if data_type == BIGQUERY_TYPE_FLOAT:
            data_type = BIGQUERY_TYPE_FLOAT64
        elif data_type == BIGQUERY_TYPE_INTEGER:
            data_type = BIGQUERY_TYPE_INT64
        elif data_type == BIGQUERY_TYPE_STRING and data_length:
            char_length = data_length
        super(BigQueryColumn, self).__init__(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            safe_mapping=safe_mapping,
            partition_info=partition_info,
            char_length=char_length,
            char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
        )

    def format_data_type(self):
        if self.data_type in [BIGQUERY_TYPE_BIGNUMERIC, BIGQUERY_TYPE_NUMERIC]:
            if self.data_precision and self.data_scale is not None:
                return "%s(%s,%s)" % (
                    self.data_type,
                    self.data_precision,
                    self.data_scale,
                )
            elif self.data_precision:
                return "%s(%s)" % (self.data_type, self.data_precision)
            else:
                return self.data_type
        elif self.data_type == BIGQUERY_TYPE_STRING:
            if self.char_length:
                return "%s(%s)" % (self.data_type, self.char_length)
            else:
                return self.data_type
        elif self.data_type == BIGQUERY_TYPE_BYTES:
            if self.data_length:
                return "%s(%s)" % (self.data_type, self.data_length)
            else:
                return self.data_type
        else:
            return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(
            self.data_type
            in [BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_TIME, BIGQUERY_TYPE_TIMESTAMP]
        )

    def is_binary(self):
        return bool(self.data_type == BIGQUERY_TYPE_BYTES)

    def is_nan_capable(self):
        return bool(self.data_type == BIGQUERY_TYPE_FLOAT64)

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in (
                BIGQUERY_TYPE_BIGNUMERIC,
                BIGQUERY_TYPE_FLOAT64,
                BIGQUERY_TYPE_INT64,
                BIGQUERY_TYPE_NUMERIC,
            )
        )

    def is_date_based(self):
        """Is the column date in class"""
        return bool(
            self.data_type
            in (BIGQUERY_TYPE_DATE, BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_TIMESTAMP)
        )

    def is_interval(self):
        return False

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(self.data_type == BIGQUERY_TYPE_STRING)

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return bool(self.data_type == BIGQUERY_TYPE_TIMESTAMP)

    def valid_for_offload_predicate(self):
        return bool(
            self.is_number_based()
            or (self.is_date_based() and not self.is_time_zone_based())
            or self.is_string_based()
        )
