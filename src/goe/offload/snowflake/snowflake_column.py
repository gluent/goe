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

"""
"""
from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    CANONICAL_CHAR_SEMANTICS_CHAR,
)

SNOWFLAKE_TYPE_BINARY = "BINARY"
SNOWFLAKE_TYPE_BOOLEAN = "BOOLEAN"
SNOWFLAKE_TYPE_DATE = "DATE"
SNOWFLAKE_TYPE_FLOAT = "FLOAT"
# SNOWFLAKE_TYPE_INTEGER because certain test activities expect access to an integer data type
SNOWFLAKE_TYPE_INTEGER = "INTEGER"
SNOWFLAKE_TYPE_NUMBER = "NUMBER"
SNOWFLAKE_TYPE_TEXT = "TEXT"
SNOWFLAKE_TYPE_TIME = "TIME"
SNOWFLAKE_TYPE_TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
SNOWFLAKE_TYPE_TIMESTAMP_TZ = "TIMESTAMP_TZ"

TEXT_LENGTH_UPPER_LIMIT = 16777216


class SnowflakeColumn(ColumnMetadataInterface):
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
        super(SnowflakeColumn, self).__init__(
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
        if self.data_type == SNOWFLAKE_TYPE_NUMBER:
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
        elif self.data_type == SNOWFLAKE_TYPE_TEXT:
            if self.char_length:
                return "%s(%s)" % (self.data_type, self.char_length)
            else:
                return self.data_type
        elif self.data_type == SNOWFLAKE_TYPE_BINARY:
            if self.data_length:
                return "%s(%s)" % (self.data_type, self.data_length)
            else:
                return self.data_type
        elif (
            self.is_date_based() or self.data_type == SNOWFLAKE_TYPE_TIME
        ) and self.data_scale is not None:
            return "%s(%s)" % (self.data_type, self.data_scale)
        else:
            return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        return bool(
            self.data_type
            in [
                SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                SNOWFLAKE_TYPE_TIMESTAMP_TZ,
                SNOWFLAKE_TYPE_TIME,
            ]
        )

    def is_binary(self):
        return bool(self.data_type == SNOWFLAKE_TYPE_BINARY)

    def is_nan_capable(self):
        return bool(self.data_type == SNOWFLAKE_TYPE_FLOAT)

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(self.data_type in [SNOWFLAKE_TYPE_FLOAT, SNOWFLAKE_TYPE_NUMBER])

    def is_date_based(self):
        """Is the column date in class"""
        return bool(
            self.data_type
            in [
                SNOWFLAKE_TYPE_DATE,
                SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
                SNOWFLAKE_TYPE_TIMESTAMP_TZ,
            ]
        )

    def is_interval(self):
        return False

    def is_string_based(self):
        return bool(self.data_type == SNOWFLAKE_TYPE_TEXT)

    def is_time_zone_based(self):
        return bool(self.data_type == SNOWFLAKE_TYPE_TIMESTAMP_TZ)

    def is_unbound_string(self):
        """Is the column string based and has no user specified size.
        e.g. STRING is generally unbound and VARCHAR generally has a size. But some systems may allow VARCHAR
             without a limit and fall back to being unbound.
        If TEXT with no length is used to specify a column we get a default of TEXT_LENGTH_UPPER_LIMIT. Assuming
        this means unbound and not that the user specifically expected data of this size.
        """
        if (
            self.data_type == SNOWFLAKE_TYPE_TEXT
            and self.data_length == TEXT_LENGTH_UPPER_LIMIT
        ):
            return True
        else:
            return bool(
                self.is_string_based()
                and self.data_length is None
                and self.char_length is None
            )

    def valid_for_offload_predicate(self):
        return bool(
            self.is_number_based()
            or (self.is_date_based() and not self.is_time_zone_based())
            or self.is_string_based()
        )
