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

""" OracleColumn: Oracle implementation of ColumnMetadataInterface
"""

import re

from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    CANONICAL_CHAR_SEMANTICS_BYTE,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
)

###############################################################################
# CONSTANTS
###############################################################################

# Data type constants, these must match the types stored in the data dictionary
ORACLE_TYPE_CHAR = "CHAR"
ORACLE_TYPE_NCHAR = "NCHAR"
ORACLE_TYPE_CLOB = "CLOB"
ORACLE_TYPE_NCLOB = "NCLOB"
ORACLE_TYPE_LONG = "LONG"
ORACLE_TYPE_VARCHAR = "VARCHAR"
ORACLE_TYPE_VARCHAR2 = "VARCHAR2"
ORACLE_TYPE_NVARCHAR2 = "NVARCHAR2"
ORACLE_TYPE_RAW = "RAW"
ORACLE_TYPE_BLOB = "BLOB"
ORACLE_TYPE_LONG_RAW = "LONG RAW"
ORACLE_TYPE_NUMBER = "NUMBER"
ORACLE_TYPE_FLOAT = "FLOAT"
ORACLE_TYPE_BINARY_FLOAT = "BINARY_FLOAT"
ORACLE_TYPE_BINARY_DOUBLE = "BINARY_DOUBLE"
ORACLE_TYPE_DATE = "DATE"
ORACLE_TYPE_TIMESTAMP = "TIMESTAMP"
ORACLE_TYPE_TIMESTAMP_TZ = "TIMESTAMP WITH TIME ZONE"
ORACLE_TYPE_TIMESTAMP_LOCAL_TZ = "TIMESTAMP WITH LOCAL TIME ZONE"
ORACLE_TYPE_INTERVAL_DS = "INTERVAL DAY TO SECOND"
ORACLE_TYPE_INTERVAL_YM = "INTERVAL YEAR TO MONTH"
ORACLE_TYPE_XMLTYPE = "XMLTYPE"
ORACLE_TIMESTAMP_RE = re.compile(r"^TIMESTAMP\(([0-9])\)$", re.I)
ORACLE_TIMESTAMP_SUB_PATTERN = "TIMESTAMP(%s)"
ORACLE_SPLIT_HIGH_VALUE_RE = re.compile(r"( *TO_DATE\([^)]+\)|[^,]+)+")
ORACLE_TIMESTAMP_TZ_RE = re.compile(r"^TIMESTAMP\(([0-9])\) WITH TIME ZONE$", re.I)
ORACLE_TIMESTAMP_TZ_SUB_PATTERN = "TIMESTAMP(%s) WITH TIME ZONE"
ORACLE_TIMESTAMP_LOCAL_TZ_RE = re.compile(
    r"^TIMESTAMP\(([0-9])\) WITH LOCAL TIME ZONE$", re.I
)
ORACLE_TIMESTAMP_LOCAL_TZ_SUB_PATTERN = "TIMESTAMP(%s) WITH LOCAL TIME ZONE"
ORACLE_INTERVAL_DS_RE = re.compile(
    r"^INTERVAL DAY(\([0-9]\))? TO SECOND(\([0-9]\))?$", re.I
)
ORACLE_INTERVAL_DS_SUB_PATTERN = "INTERVAL DAY(%s) TO SECOND(%s)"
ORACLE_INTERVAL_YM_RE = re.compile(
    r"^INTERVAL YEAR(\([0-9]\))? TO MONTH(\([0-9]\))?$", re.I
)
ORACLE_INTERVAL_YM_SUB_PATTERN = "INTERVAL YEAR(%s) TO MONTH"


###########################################################################
# CLASSES
###########################################################################


class OracleColumn(ColumnMetadataInterface):
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
        char_semantics=None,
        char_length=None,
        safe_mapping=True,
        partition_info=None,
    ):
        super(OracleColumn, self).__init__(
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
        self.hidden = self._optional_boolean(hidden)
        # Oracle holds precision.scale information inside some data_type values, we remove them here
        if ORACLE_TIMESTAMP_RE.match(self.data_type):
            self.data_type = ORACLE_TYPE_TIMESTAMP
        elif ORACLE_TIMESTAMP_TZ_RE.match(self.data_type):
            self.data_type = ORACLE_TYPE_TIMESTAMP_TZ
        elif ORACLE_TIMESTAMP_LOCAL_TZ_RE.match(self.data_type):
            self.data_type = ORACLE_TYPE_TIMESTAMP_LOCAL_TZ
        elif ORACLE_INTERVAL_DS_RE.match(self.data_type):
            self.data_type = ORACLE_TYPE_INTERVAL_DS
        elif ORACLE_INTERVAL_YM_RE.match(self.data_type):
            self.data_type = ORACLE_TYPE_INTERVAL_YM
        if self.char_semantics is None and self.data_type in [
            ORACLE_TYPE_NCHAR,
            ORACLE_TYPE_NCLOB,
            ORACLE_TYPE_NVARCHAR2,
        ]:
            self.char_semantics = CANONICAL_CHAR_SEMANTICS_UNICODE

    @staticmethod
    def from_oracle(attribute_list):
        """Accepts an Oracle specific list of attributes for a column and returns object based on them"""
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
            char_used,
            char_length,
        ) = attribute_list
        if data_type == ORACLE_TYPE_FLOAT and data_precision:
            # Precision for FLOAT does not match with our use of precision for NUMBER
            data_precision = None
        if char_used:
            if data_type in [
                ORACLE_TYPE_NCHAR,
                ORACLE_TYPE_NCLOB,
                ORACLE_TYPE_NVARCHAR2,
            ]:
                char_semantics = CANONICAL_CHAR_SEMANTICS_UNICODE
            else:
                char_semantics = (
                    CANONICAL_CHAR_SEMANTICS_BYTE
                    if char_used == "B"
                    else CANONICAL_CHAR_SEMANTICS_CHAR
                )
        else:
            char_semantics = None
        col = OracleColumn(
            name,
            data_type,
            data_length=data_length,
            data_precision=data_precision,
            data_scale=data_scale,
            nullable=nullable,
            data_default=data_default,
            hidden=hidden,
            char_semantics=char_semantics,
            char_length=char_length,
        )
        return col

    def format_data_type(self):
        def char_semantic(char_semantics):
            return "CHAR" if char_semantics == CANONICAL_CHAR_SEMANTICS_CHAR else "BYTE"

        if self.data_type in (ORACLE_TYPE_NUMBER, ORACLE_TYPE_FLOAT):
            if self.data_precision and self.data_scale is not None:
                return "%s(%s,%s)" % (
                    self.data_type,
                    self.data_precision,
                    self.data_scale,
                )
            elif self.data_precision:
                return "%s(%s)" % (self.data_type, self.data_precision)
            elif self.data_scale is not None:
                return "%s(*,%s)" % (self.data_type, self.data_scale)
            else:
                return self.data_type
        elif self.data_type in (ORACLE_TYPE_CHAR, ORACLE_TYPE_VARCHAR2):
            if self.char_length:
                return "%s(%s %s)" % (
                    self.data_type,
                    self.char_length,
                    char_semantic(self.char_semantics),
                )
            else:
                # Oracle -> Oracle will always have char_length so this must be Backend -> Oracle
                return "%s(%s %s)" % (
                    self.data_type,
                    self.data_length,
                    char_semantic(CANONICAL_CHAR_SEMANTICS_BYTE),
                )
        elif self.data_type in (ORACLE_TYPE_NCHAR, ORACLE_TYPE_NVARCHAR2):
            return "%s(%s)" % (self.data_type, self.char_length)
        elif self.data_type == ORACLE_TYPE_TIMESTAMP:
            # this is a bit of a fudge, if the data type is just TIMESTAMP then Oracle has a default of TIMESTAMP(6)
            return ORACLE_TIMESTAMP_SUB_PATTERN % (
                6 if self.data_scale is None else self.data_scale
            )
        elif self.data_type == ORACLE_TYPE_TIMESTAMP_TZ:
            # this is a bit of a fudge, if the data type is just TIMESTAMP then Oracle has a default of TIMESTAMP(6)
            return ORACLE_TIMESTAMP_TZ_SUB_PATTERN % (
                6 if self.data_scale is None else self.data_scale
            )
        elif self.data_type == ORACLE_TYPE_TIMESTAMP_LOCAL_TZ:
            return ORACLE_TIMESTAMP_LOCAL_TZ_SUB_PATTERN % (
                6 if self.data_scale is None else self.data_scale
            )
        elif self.data_type == ORACLE_TYPE_RAW and self.data_length:
            return "%s(%s)" % (self.data_type, self.data_length)
        elif self.data_type == ORACLE_TYPE_INTERVAL_DS:
            return ORACLE_INTERVAL_DS_SUB_PATTERN % (
                9 if self.data_precision is None else self.data_precision,
                9 if self.data_scale is None else self.data_scale,
            )
        elif self.data_type == ORACLE_TYPE_INTERVAL_YM:
            return ORACLE_INTERVAL_YM_SUB_PATTERN % (
                9 if self.data_precision is None else self.data_precision
            )
        else:
            return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(
            self.data_type
            in [ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_TIMESTAMP_TZ]
        )

    def is_binary(self):
        return bool(
            self.data_type
            in [
                ORACLE_TYPE_BLOB,
                ORACLE_TYPE_RAW,
                ORACLE_TYPE_LONG,
                ORACLE_TYPE_LONG_RAW,
            ]
        )

    def is_nan_capable(self):
        return bool(
            self.data_type in [ORACLE_TYPE_BINARY_DOUBLE, ORACLE_TYPE_BINARY_FLOAT]
        )

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in [
                ORACLE_TYPE_NUMBER,
                ORACLE_TYPE_FLOAT,
                ORACLE_TYPE_BINARY_DOUBLE,
                ORACLE_TYPE_BINARY_FLOAT,
            ]
        )

    def is_date_based(self):
        """Is the column date in class"""
        return bool(
            self.data_type
            in [
                ORACLE_TYPE_DATE,
                ORACLE_TYPE_TIMESTAMP,
                ORACLE_TYPE_TIMESTAMP_TZ,
                ORACLE_TYPE_TIMESTAMP_LOCAL_TZ,
            ]
        )

    def is_interval(self):
        return bool(
            self.data_type in [ORACLE_TYPE_INTERVAL_DS, ORACLE_TYPE_INTERVAL_YM]
        )

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(
            self.data_type
            in [
                ORACLE_TYPE_VARCHAR,
                ORACLE_TYPE_VARCHAR2,
                ORACLE_TYPE_NVARCHAR2,
                ORACLE_TYPE_CHAR,
                ORACLE_TYPE_NCHAR,
                ORACLE_TYPE_CLOB,
                ORACLE_TYPE_NCLOB,
            ]
        )

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return bool(
            self.data_type.upper()
            in (ORACLE_TYPE_TIMESTAMP_TZ, ORACLE_TYPE_TIMESTAMP_LOCAL_TZ)
        )

    def is_hidden(self):
        return self.hidden

    def valid_for_offload_predicate(self):
        return bool(
            self.is_number_based()
            or (self.is_date_based() and not self.is_time_zone_based())
            or (
                self.is_string_based()
                and self.data_type not in [ORACLE_TYPE_CLOB, ORACLE_TYPE_NCLOB]
            )
        )
