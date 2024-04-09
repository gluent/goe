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

""" HadoopColumn: Hadoop implementation of ColumnMetadataInterface
"""

from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    CANONICAL_CHAR_SEMANTICS_BYTE,
)


###############################################################################
# CONSTANTS
###############################################################################

# Data types
HADOOP_TYPE_BOOLEAN = "BOOLEAN"
HADOOP_TYPE_CHAR = "CHAR"
HADOOP_TYPE_STRING = "STRING"
HADOOP_TYPE_VARCHAR = "VARCHAR"
HADOOP_TYPE_BINARY = "BINARY"
HADOOP_TYPE_TINYINT = "TINYINT"
HADOOP_TYPE_SMALLINT = "SMALLINT"
HADOOP_TYPE_INT = "INT"
HADOOP_TYPE_BIGINT = "BIGINT"
HADOOP_TYPE_DATE = "DATE"
HADOOP_TYPE_DECIMAL = "DECIMAL"
HADOOP_TYPE_FLOAT = "FLOAT"
HADOOP_TYPE_DOUBLE = "DOUBLE"
HADOOP_TYPE_DOUBLE_PRECISION = "DOUBLE_PRECISION"
HADOOP_TYPE_REAL = "REAL"
HADOOP_TYPE_DATE = "DATE"
HADOOP_TYPE_TIMESTAMP = "TIMESTAMP"
HADOOP_TYPE_INTERVAL_DS = "INTERVAL_DAY_TIME"
HADOOP_TYPE_INTERVAL_YM = "INTERVAL_YEAR_MONTH"


###########################################################################
# CLASSES
###########################################################################


class HadoopColumn(ColumnMetadataInterface):
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
    ):
        super(HadoopColumn, self).__init__(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            safe_mapping=safe_mapping,
            partition_info=partition_info,
            char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
        )

    def format_data_type(self):
        if self.data_type == HADOOP_TYPE_DECIMAL:
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
        elif self.data_type in (HADOOP_TYPE_VARCHAR, HADOOP_TYPE_CHAR):
            return "%s(%s)" % (self.data_type, self.data_length)
        else:
            return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(self.data_type == HADOOP_TYPE_TIMESTAMP)

    def is_binary(self):
        return bool(self.data_type in [HADOOP_TYPE_BINARY])

    def is_nan_capable(self):
        return bool(self.data_type in [HADOOP_TYPE_DOUBLE, HADOOP_TYPE_FLOAT])

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in (
                HADOOP_TYPE_BIGINT,
                HADOOP_TYPE_DECIMAL,
                HADOOP_TYPE_DOUBLE,
                HADOOP_TYPE_FLOAT,
                HADOOP_TYPE_INT,
                HADOOP_TYPE_REAL,
                HADOOP_TYPE_SMALLINT,
                HADOOP_TYPE_TINYINT,
            )
        )

    def is_date_based(self):
        """Is the column date in class"""
        return bool(self.data_type in (HADOOP_TYPE_TIMESTAMP, HADOOP_TYPE_DATE))

    def is_interval(self):
        return False

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(
            self.data_type
            in (HADOOP_TYPE_CHAR, HADOOP_TYPE_VARCHAR, HADOOP_TYPE_STRING)
        )

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return False

    def valid_for_offload_predicate(self):
        return bool(
            self.is_number_based() or self.is_date_based() or self.is_string_based()
        )
