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

""" SynapseColumn: Synapse implementation of ColumnMetadataInterface
"""

from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
)

###############################################################################
# CONSTANTS
###############################################################################

# Eventually these may converge with MSSQL types into e.g. MICROSOFT_TYPE_X
SYNAPSE_TYPE_BIGINT = "bigint"
SYNAPSE_TYPE_BINARY = "binary"
SYNAPSE_TYPE_BIT = "bit"
SYNAPSE_TYPE_CHAR = "char"
SYNAPSE_TYPE_DATE = "date"
SYNAPSE_TYPE_DATETIME = "datetime"
SYNAPSE_TYPE_DATETIME2 = "datetime2"
SYNAPSE_TYPE_DATETIMEOFFSET = "datetimeoffset"
SYNAPSE_TYPE_DECIMAL = "decimal"
SYNAPSE_TYPE_FLOAT = "float"
SYNAPSE_TYPE_INT = "int"
SYNAPSE_TYPE_MONEY = "money"
SYNAPSE_TYPE_NCHAR = "nchar"
SYNAPSE_TYPE_NUMERIC = "numeric"
SYNAPSE_TYPE_NVARCHAR = "nvarchar"
SYNAPSE_TYPE_REAL = "real"
SYNAPSE_TYPE_SMALLDATETIME = "smalldatetime"
SYNAPSE_TYPE_SMALLINT = "smallint"
SYNAPSE_TYPE_SMALLMONEY = "smallmoney"
SYNAPSE_TYPE_TIME = "time"
SYNAPSE_TYPE_TINYINT = "tinyint"
SYNAPSE_TYPE_UNIQUEIDENTIFIER = "uniqueidentifier"
SYNAPSE_TYPE_VARBINARY = "varbinary"
SYNAPSE_TYPE_VARCHAR = "varchar"
SYNAPSE_TYPE_MAX_TOKEN = "max"


###########################################################################
# CLASSES
###########################################################################


class SynapseColumn(ColumnMetadataInterface):
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
        char_semantics=None,
        collation=None,
    ):
        super(SynapseColumn, self).__init__(
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
            char_semantics=char_semantics,
        )
        # Collation is a Microsoft only attribute and not present in ColumnMetadataInterface.
        self.collation = collation

    def format_data_type(self):
        if self.data_type in [SYNAPSE_TYPE_DECIMAL, SYNAPSE_TYPE_NUMERIC]:
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
            SYNAPSE_TYPE_CHAR,
            SYNAPSE_TYPE_VARCHAR,
            SYNAPSE_TYPE_BINARY,
            SYNAPSE_TYPE_VARBINARY,
            SYNAPSE_TYPE_NCHAR,
            SYNAPSE_TYPE_NVARCHAR,
        ]:
            if (
                self.data_type
                in [SYNAPSE_TYPE_VARCHAR, SYNAPSE_TYPE_VARBINARY, SYNAPSE_TYPE_NVARCHAR]
                and not self.data_length
            ):
                return "%s(%s)" % (self.data_type, SYNAPSE_TYPE_MAX_TOKEN)
            elif self.char_length and self.char_semantics in [
                CANONICAL_CHAR_SEMANTICS_CHAR,
                CANONICAL_CHAR_SEMANTICS_UNICODE,
            ]:
                return "%s(%s)" % (self.data_type, self.char_length)
            elif self.data_length:
                return "%s(%s)" % (self.data_type, self.data_length)
            else:
                return self.data_type
        elif (
            self.is_date_based() or self.data_type == SYNAPSE_TYPE_TIME
        ) and self.data_scale is not None:
            return "%s(%s)" % (self.data_type, self.data_scale)
        else:
            return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(
            self.data_type
            in [
                SYNAPSE_TYPE_DATETIME,
                SYNAPSE_TYPE_DATETIME2,
                SYNAPSE_TYPE_DATETIMEOFFSET,
                SYNAPSE_TYPE_SMALLDATETIME,
                SYNAPSE_TYPE_TIME,
            ]
        )

    def is_binary(self):
        return bool(self.data_type in [SYNAPSE_TYPE_BINARY, SYNAPSE_TYPE_VARBINARY])

    def is_nan_capable(self):
        """No documentation stating that this is the case, however testing showed the presence of Nan data
        threw an "Arithmetic overflow error converting float to data type REAL." error.
        """
        return False

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in [
                SYNAPSE_TYPE_BIT,
                SYNAPSE_TYPE_DECIMAL,
                SYNAPSE_TYPE_NUMERIC,
                SYNAPSE_TYPE_INT,
                SYNAPSE_TYPE_BIGINT,
                SYNAPSE_TYPE_SMALLINT,
                SYNAPSE_TYPE_TINYINT,
                SYNAPSE_TYPE_MONEY,
                SYNAPSE_TYPE_SMALLMONEY,
                SYNAPSE_TYPE_FLOAT,
                SYNAPSE_TYPE_REAL,
            ]
        )

    def is_date_based(self):
        """Is the column date in class"""
        return bool(
            self.data_type
            in [
                SYNAPSE_TYPE_DATE,
                SYNAPSE_TYPE_DATETIME,
                SYNAPSE_TYPE_DATETIME2,
                SYNAPSE_TYPE_DATETIMEOFFSET,
                SYNAPSE_TYPE_SMALLDATETIME,
            ]
        )

    def is_interval(self):
        return False

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(
            self.data_type
            in [
                SYNAPSE_TYPE_CHAR,
                SYNAPSE_TYPE_VARCHAR,
                SYNAPSE_TYPE_NCHAR,
                SYNAPSE_TYPE_NVARCHAR,
            ]
        )

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return bool(self.data_type == SYNAPSE_TYPE_DATETIMEOFFSET)

    def valid_for_offload_predicate(self):
        """Is the column valid for use in an Offload Predicate"""
        return bool(
            self.is_number_based()
            or (self.is_date_based() and not self.is_time_zone_based())
            or self.is_string_based()
        )
