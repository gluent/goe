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

""" MSSQLColumn: MSSQL implementation of ColumnMetadataInterface
"""

from goe.offload.column_metadata import ColumnMetadataInterface

###############################################################################
# CONSTANTS
###############################################################################

# Data type constants, these must match the types stored in the data dictionary

MSSQL_TYPE_BIGINT = "bigint"
MSSQL_TYPE_BIT = "bit"
MSSQL_TYPE_DECIMAL = "decimal"
MSSQL_TYPE_INT = "int"
MSSQL_TYPE_MONEY = "money"
MSSQL_TYPE_NUMERIC = "numeric"
MSSQL_TYPE_SMALLINT = "smallint"
MSSQL_TYPE_SMALLMONEY = "smallmoney"
MSSQL_TYPE_TINYINT = "tinyint"
MSSQL_TYPE_FLOAT = "float"
MSSQL_TYPE_REAL = "real"
MSSQL_TYPE_DATE = "date"
MSSQL_TYPE_DATETIME2 = "datetime2"
MSSQL_TYPE_DATETIME = "datetime"
MSSQL_TYPE_DATETIMEOFFSET = "datetimeoffset"
MSSQL_TYPE_SMALLDATETIME = "smalldatetime"
MSSQL_TYPE_TIME = "time"
MSSQL_TYPE_CHAR = "char"
MSSQL_TYPE_VARCHAR = "varchar"
MSSQL_TYPE_NCHAR = "nchar"
MSSQL_TYPE_NVARCHAR = "nvarchar"
MSSQL_TYPE_UNIQUEIDENTIFIER = "uniqueidentifier"
MSSQL_TYPE_TEXT = "text"
MSSQL_TYPE_NTEXT = "ntext"
MSSQL_TYPE_BINARY = "binary"
MSSQL_TYPE_VARBINARY = "varbinary"
MSSQL_TYPE_IMAGE = "image"


###########################################################################
# CLASSES
###########################################################################


class MSSQLColumn(ColumnMetadataInterface):
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
        hidden=None,
        char_length=None,
        safe_mapping=True,
        partition_info=None,
        char_semantics=None,
    ):
        super(MSSQLColumn, self).__init__(
            name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            data_default,
            safe_mapping=safe_mapping,
            partition_info=partition_info,
            char_length=char_length,
            char_semantics=char_semantics,
        )
        self.hidden = hidden

    @staticmethod
    def from_mssql(attribute_list):
        """Accepts a Microsoft SQL Server specific list of attributes for a column and returns object based on them"""
        (
            name,
            _,
            nullable,
            data_type,
            data_precision,
            data_scale,
            data_length,
            data_default,
            hidden,
            char_length,
        ) = attribute_list
        col = MSSQLColumn(
            name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            data_default,
            hidden,
            char_length,
        )
        return col

    def format_data_type(self):
        # TODO not been through all types and format possibilities
        if self.data_type in [MSSQL_TYPE_DECIMAL, MSSQL_TYPE_NUMERIC]:
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
        elif self.data_type in [
            MSSQL_TYPE_CHAR,
            MSSQL_TYPE_VARCHAR,
            MSSQL_TYPE_NCHAR,
            MSSQL_TYPE_NVARCHAR,
        ]:
            return "%s(%s)" % (self.data_type, self.data_length)
        else:
            return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(
            self.data_type
            in [
                MSSQL_TYPE_DATETIME2,
                MSSQL_TYPE_DATETIME,
                MSSQL_TYPE_SMALLDATETIME,
                MSSQL_TYPE_TIME,
            ]
        )

    def is_binary(self):
        return bool(
            self.data_type.lower()
            in [MSSQL_TYPE_BINARY, MSSQL_TYPE_VARBINARY, MSSQL_TYPE_IMAGE]
        )

    def is_nan_capable(self):
        return False

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type.lower()
            in [
                MSSQL_TYPE_BIGINT,
                MSSQL_TYPE_BIT,
                MSSQL_TYPE_DECIMAL,
                MSSQL_TYPE_INT,
                MSSQL_TYPE_MONEY,
                MSSQL_TYPE_NUMERIC,
                MSSQL_TYPE_SMALLINT,
                MSSQL_TYPE_SMALLMONEY,
                MSSQL_TYPE_TINYINT,
                MSSQL_TYPE_FLOAT,
                MSSQL_TYPE_REAL,
            ]
        )

    def is_date_based(self):
        """Is the column date in class"""
        return any(
            dt in self.data_type
            for dt in [
                MSSQL_TYPE_DATE,
                MSSQL_TYPE_DATETIME2,
                MSSQL_TYPE_DATETIME,
                MSSQL_TYPE_DATETIMEOFFSET,
                MSSQL_TYPE_SMALLDATETIME,
            ]
        )

    def is_interval(self):
        return False

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(
            self.data_type.lower()
            in [
                MSSQL_TYPE_CHAR,
                MSSQL_TYPE_VARCHAR,
                MSSQL_TYPE_NCHAR,
                MSSQL_TYPE_NVARCHAR,
            ]
        )

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return bool(self.data_type.lower() == MSSQL_TYPE_DATETIMEOFFSET)

    def is_hidden(self):
        return self.hidden

    def valid_for_offload_predicate(self):
        return bool(
            self.is_number_based()
            or (self.is_date_based() and not self.is_time_zone_based())
            or self.is_string_based()
        )
