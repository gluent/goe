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

""" TeradataColumn: Teradata implementation of ColumnMetadataInterface
"""

import re

from goe.offload.column_metadata import ColumnMetadataInterface

###############################################################################
# CONSTANTS
###############################################################################

# Data type constants, these must match the types stored in the data dictionary
# We haven't added Geospatial, JSON, PERIOD, UDT or XML data types
TERADATA_TYPE_BIGINT = "I8"
TERADATA_TYPE_BLOB = "BO"
TERADATA_TYPE_BYTE = "BF"
TERADATA_TYPE_BYTEINT = "I1"
TERADATA_TYPE_CHAR = "CF"
# TERADATA_TYPE_CHARACTER not required because internal type is the same as CHAR
# TERADATA_TYPE_CHAR_VARYING not required because internal type is the same as VARCHAR
TERADATA_TYPE_CLOB = "CO"
TERADATA_TYPE_DATE = "DA"
TERADATA_TYPE_DECIMAL = "D "
TERADATA_TYPE_DOUBLE = "F "
# TERADATA_TYPE_FLOAT not required because internal type is the same as DOUBLE PRECISION
TERADATA_TYPE_INTEGER = "I "
TERADATA_TYPE_INTERVAL_YR = "YR"
TERADATA_TYPE_INTERVAL_YM = "YM"
TERADATA_TYPE_INTERVAL_MO = "MO"
TERADATA_TYPE_INTERVAL_DY = "DY"
TERADATA_TYPE_INTERVAL_DH = "DH"
TERADATA_TYPE_INTERVAL_DM = "DM"
TERADATA_TYPE_INTERVAL_DS = "DS"
TERADATA_TYPE_INTERVAL_HR = "HR"
TERADATA_TYPE_INTERVAL_HM = "HM"
TERADATA_TYPE_INTERVAL_HS = "HS"
TERADATA_TYPE_INTERVAL_MI = "MI"
TERADATA_TYPE_INTERVAL_MS = "MS"
TERADATA_TYPE_INTERVAL_SC = "SC"
# TERADATA_TYPE_LONG_VARCHAR not required because internal type is the same as VARCHAR
TERADATA_TYPE_NUMBER = "N "
# TERADATA_TYPE_NUMERIC not required because internal type is the same as DECIMAL
# TERADATA_TYPE_REAL not required because internal type is the same as DOUBLE PRECISION
TERADATA_TYPE_SMALLINT = "I2"
TERADATA_TYPE_TIME = "AT"
TERADATA_TYPE_TIMESTAMP = "TS"
TERADATA_TYPE_TIME_TZ = "TZ"
TERADATA_TYPE_TIMESTAMP_TZ = "SZ"
TERADATA_TYPE_VARBYTE = "BV"
TERADATA_TYPE_VARCHAR = "CV"

TERADATA_TYPE_TO_SQL_NAME_MAP = {
    TERADATA_TYPE_BIGINT: "BIGINT",
    TERADATA_TYPE_BLOB: "BLOB",
    TERADATA_TYPE_BYTE: "BYTE",
    TERADATA_TYPE_BYTEINT: "BYTEINT",
    TERADATA_TYPE_CHAR: "CHAR",
    TERADATA_TYPE_CLOB: "CLOB",
    TERADATA_TYPE_DATE: "DATE",
    TERADATA_TYPE_DECIMAL: "DECIMAL",
    TERADATA_TYPE_DOUBLE: "DOUBLE PRECISION",
    TERADATA_TYPE_INTEGER: "INTEGER",
    TERADATA_TYPE_INTERVAL_YR: "INTERVAL YEAR",
    TERADATA_TYPE_INTERVAL_YM: "INTERVAL YEAR TO MONTH",
    TERADATA_TYPE_INTERVAL_MO: "INTERVAL MONTH",
    TERADATA_TYPE_INTERVAL_DY: "INTERVAL DAY",
    TERADATA_TYPE_INTERVAL_DH: "INTERVAL DAY TO HOUR",
    TERADATA_TYPE_INTERVAL_DM: "INTERVAL DAY TO MINUTE",
    TERADATA_TYPE_INTERVAL_DS: "INTERVAL DAY TO SECOND",
    TERADATA_TYPE_INTERVAL_HR: "INTERVAL HOUR",
    TERADATA_TYPE_INTERVAL_HM: "INTERVAL HOUR TO MINUTE ",
    TERADATA_TYPE_INTERVAL_HS: "INTERVAL HOUR TO SECOND",
    TERADATA_TYPE_INTERVAL_MI: "INTERVAL MINUTE",
    TERADATA_TYPE_INTERVAL_MS: "INTERVAL MINUTE TO SECOND",
    TERADATA_TYPE_INTERVAL_SC: "INTERVAL SECOND",
    TERADATA_TYPE_NUMBER: "NUMBER",
    TERADATA_TYPE_SMALLINT: "SMALLINT",
    TERADATA_TYPE_TIME: "TIME",
    TERADATA_TYPE_TIMESTAMP: "TIMESTAMP",
    TERADATA_TYPE_TIME_TZ: "TIME WITH TIME ZONE",
    TERADATA_TYPE_TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
    TERADATA_TYPE_VARBYTE: "VARBYTE",
    TERADATA_TYPE_VARCHAR: "VARCHAR",
}

TERADATA_INTERVAL_DS_SUB_PATTERN = "INTERVAL DAY(%s) TO SECOND(%s)"
TERADATA_INTERVAL_YM_SUB_PATTERN = "INTERVAL YEAR(%s) TO MONTH"
TERADATA_TIME_SUB_PATTERN = "TIME(%s)"
TERADATA_TIMESTAMP_SUB_PATTERN = "TIMESTAMP(%s)"

TERADATA_TIMESTAMP_RE = re.compile(r"^TIMESTAMP\(([0-6])\)$", re.I)

TERADATA_STRING_DATA_TYPES = [
    TERADATA_TYPE_CHAR,
    TERADATA_TYPE_CLOB,
    TERADATA_TYPE_VARCHAR,
]

TERADATA_INTERVAL_DATA_TYPES = [
    TERADATA_TYPE_INTERVAL_YR,
    TERADATA_TYPE_INTERVAL_YM,
    TERADATA_TYPE_INTERVAL_MO,
    TERADATA_TYPE_INTERVAL_DY,
    TERADATA_TYPE_INTERVAL_DH,
    TERADATA_TYPE_INTERVAL_DM,
    TERADATA_TYPE_INTERVAL_DS,
    TERADATA_TYPE_INTERVAL_HR,
    TERADATA_TYPE_INTERVAL_HM,
    TERADATA_TYPE_INTERVAL_HS,
    TERADATA_TYPE_INTERVAL_MI,
    TERADATA_TYPE_INTERVAL_MS,
    TERADATA_TYPE_INTERVAL_SC,
]


###########################################################################
# CLASSES
###########################################################################


class TeradataColumn(ColumnMetadataInterface):
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
        char_length=None,
        safe_mapping=True,
        partition_info=None,
        char_semantics=None,
    ):
        super().__init__(
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

    @staticmethod
    def from_teradata(attribute_list):
        """Accepts a Teradata specific list of attributes for a column and returns object based on them"""
        (
            _,
            name,
            nullable,
            data_type,
            data_precision,
            data_scale,
            data_length,
            data_default,
        ) = attribute_list
        if data_precision == -128:
            data_precision = None
        if data_scale == -128:
            data_scale = None
        char_length = None
        if data_type in TERADATA_STRING_DATA_TYPES:
            char_length = data_length
        col = TeradataColumn(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            char_length=char_length,
        )
        return col

    def format_data_type(self):
        assert (
            self.data_type in TERADATA_TYPE_TO_SQL_NAME_MAP
        ), f"Data type name unknown for: {self.data_type}"
        data_type_name = TERADATA_TYPE_TO_SQL_NAME_MAP[self.data_type]
        if self.data_type in [TERADATA_TYPE_DECIMAL, TERADATA_TYPE_NUMBER]:
            if self.data_precision and self.data_scale is not None:
                return "%s(%s,%s)" % (
                    data_type_name,
                    self.data_precision,
                    self.data_scale,
                )
            elif self.data_precision:
                return "%s(%s)" % (data_type_name, self.data_precision)
            else:
                return data_type_name
        elif self.data_length is not None and self.data_type in [
            TERADATA_TYPE_BLOB,
            TERADATA_TYPE_BYTE,
            TERADATA_TYPE_VARBYTE,
            TERADATA_TYPE_CHAR,
            TERADATA_TYPE_VARCHAR,
        ]:
            return "%s(%s)" % (data_type_name, self.data_length)
        elif self.data_scale is not None and self.data_type == TERADATA_TYPE_TIMESTAMP:
            return TERADATA_TIMESTAMP_SUB_PATTERN % self.data_scale
        elif self.data_scale is not None and self.data_type == TERADATA_TYPE_TIME:
            return TERADATA_TIME_SUB_PATTERN % self.data_scale
        elif (
            self.data_type == TERADATA_TYPE_INTERVAL_DS
            and self.data_precision is not None
            and self.data_scale is not None
        ):
            return TERADATA_INTERVAL_DS_SUB_PATTERN % (
                self.data_precision or 2,
                self.data_scale or 6,
            )
        elif (
            self.data_type == TERADATA_TYPE_INTERVAL_YM
            and self.data_precision is not None
        ):
            return TERADATA_INTERVAL_YM_SUB_PATTERN % self.data_precision
        else:
            return data_type_name

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(
            self.data_type
            in [
                TERADATA_TYPE_TIME,
                TERADATA_TYPE_TIME_TZ,
                TERADATA_TYPE_TIMESTAMP,
                TERADATA_TYPE_TIMESTAMP_TZ,
            ]
        )

    def is_binary(self):
        return bool(
            self.data_type
            in [TERADATA_TYPE_BLOB, TERADATA_TYPE_BYTE, TERADATA_TYPE_VARBYTE]
        )

    def is_nan_capable(self):
        return bool(self.data_type == TERADATA_TYPE_DOUBLE)

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in [
                TERADATA_TYPE_DECIMAL,
                TERADATA_TYPE_NUMBER,
                TERADATA_TYPE_BIGINT,
                TERADATA_TYPE_INTEGER,
                TERADATA_TYPE_SMALLINT,
                TERADATA_TYPE_BYTEINT,
                TERADATA_TYPE_DOUBLE,
            ]
        )

    def is_date_based(self):
        """Is the column date in class"""
        return bool(
            self.data_type
            in [TERADATA_TYPE_DATE, TERADATA_TYPE_TIMESTAMP, TERADATA_TYPE_TIMESTAMP_TZ]
        )

    def is_interval(self):
        return bool(self.data_type in TERADATA_INTERVAL_DATA_TYPES)

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(self.data_type in TERADATA_STRING_DATA_TYPES)

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return bool(
            self.data_type.upper()
            in (TERADATA_TYPE_TIME_TZ, TERADATA_TYPE_TIMESTAMP_TZ)
        )

    def valid_for_offload_predicate(self):
        return bool(
            self.is_number_based()
            or (self.is_date_based() and not self.is_time_zone_based())
            or (self.is_string_based() and self.data_type not in [TERADATA_TYPE_CLOB])
        )
