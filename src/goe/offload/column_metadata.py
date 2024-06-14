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

""" ColumnMetadataInterface: Base interface for GOE column metadata.
    Other classes will build upon this basic model.
"""

from abc import ABCMeta, abstractmethod
from copy import copy
import logging
import re
from typing import Optional

from goe.util.misc_functions import str_summary_of_self


class ColumnMetadataException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

GOE_TYPE_FIXED_STRING = "FIXED_STRING"
GOE_TYPE_LARGE_STRING = "LARGE_STRING"
GOE_TYPE_VARIABLE_STRING = "VARIABLE_STRING"
GOE_TYPE_BINARY = "BINARY"
GOE_TYPE_LARGE_BINARY = "LARGE_BINARY"
GOE_TYPE_INTEGER_1 = "INTEGER_1"
GOE_TYPE_INTEGER_2 = "INTEGER_2"
GOE_TYPE_INTEGER_4 = "INTEGER_4"
GOE_TYPE_INTEGER_8 = "INTEGER_8"
GOE_TYPE_INTEGER_38 = "INTEGER_38"
GOE_TYPE_DECIMAL = "DECIMAL"
GOE_TYPE_FLOAT = "FLOAT"
GOE_TYPE_DOUBLE = "DOUBLE"
GOE_TYPE_DATE = "DATE"
GOE_TYPE_TIME = "TIME"
GOE_TYPE_TIMESTAMP = "TIMESTAMP"
GOE_TYPE_TIMESTAMP_TZ = "TIMESTAMP_TZ"
GOE_TYPE_INTERVAL_DS = "INTERVAL_DS"
GOE_TYPE_INTERVAL_YM = "INTERVAL_YM"
GOE_TYPE_BOOLEAN = "BOOLEAN"

ALL_CANONICAL_TYPES = [
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_VARIABLE_STRING,
    GOE_TYPE_BINARY,
    GOE_TYPE_LARGE_BINARY,
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_FLOAT,
    GOE_TYPE_DOUBLE,
    GOE_TYPE_DATE,
    GOE_TYPE_TIME,
    GOE_TYPE_TIMESTAMP,
    GOE_TYPE_TIMESTAMP_TZ,
    GOE_TYPE_INTERVAL_DS,
    GOE_TYPE_INTERVAL_YM,
    GOE_TYPE_BOOLEAN,
]

DATE_CANONICAL_TYPES = [GOE_TYPE_DATE, GOE_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP_TZ]
NUMERIC_CANONICAL_TYPES = [
    GOE_TYPE_INTEGER_1,
    GOE_TYPE_INTEGER_2,
    GOE_TYPE_INTEGER_4,
    GOE_TYPE_INTEGER_8,
    GOE_TYPE_INTEGER_38,
    GOE_TYPE_DECIMAL,
    GOE_TYPE_FLOAT,
    GOE_TYPE_DOUBLE,
]
STRING_CANONICAL_TYPES = [
    GOE_TYPE_FIXED_STRING,
    GOE_TYPE_LARGE_STRING,
    GOE_TYPE_VARIABLE_STRING,
]

CANONICAL_TYPE_OPTION_NAMES = {
    GOE_TYPE_BINARY: "--binary-columns",
    GOE_TYPE_DATE: "--date-columns",
    GOE_TYPE_DECIMAL: "--decimal-columns",
    GOE_TYPE_DOUBLE: "--double-columns",
    GOE_TYPE_INTEGER_1: "--integer-1-columns",
    GOE_TYPE_INTEGER_2: "--integer-2-columns",
    GOE_TYPE_INTEGER_4: "--integer-4-columns",
    GOE_TYPE_INTEGER_8: "--integer-8-columns",
    GOE_TYPE_INTEGER_38: "--integer-38-columns",
    GOE_TYPE_LARGE_STRING: "--large-string-columns",
    GOE_TYPE_LARGE_BINARY: "--large-binary-columns",
    GOE_TYPE_VARIABLE_STRING: "--variable-string-columns",
    GOE_TYPE_INTERVAL_DS: "--interval-ds-columns",
    GOE_TYPE_INTERVAL_YM: "--interval-ym-columns",
    GOE_TYPE_TIMESTAMP_TZ: "--timestamp-tz-columns",
}

CANONICAL_TYPE_BYTE_LENGTHS = {
    GOE_TYPE_INTEGER_1: 1,
    GOE_TYPE_INTEGER_2: 2,
    GOE_TYPE_INTEGER_4: 4,
    GOE_TYPE_INTEGER_8: 8,
    GOE_TYPE_FLOAT: 4,
    GOE_TYPE_DOUBLE: 8,
}

CANONICAL_CHAR_SEMANTICS_BYTE = "BYTE"
CANONICAL_CHAR_SEMANTICS_CHAR = "CHAR"
CANONICAL_CHAR_SEMANTICS_UNICODE = "UNICODE"

# Substitution template for GOE_PART_ column names
SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE = "GOE_PART_%s_%s"

# Regular expression extracting granularity/source name from synthetic partition column name.
SYNTHETIC_PARTITION_COLUMN_NAME_RE = re.compile(
    r"(^GOE_PART_)(Y|M|D|\d+|U\d+)_([A-Z0-9$#_]+$)", re.I
)

# A column name replacement for when the source columns contain special characters.
STAGING_FILE_SIMPLIFIED_NAME_TOKEN = "_GOE_SPECIAL_CHAR_COLUMN"
STAGING_FILE_UNSUPPORTED_NAME_CHARACTERS = [" ", "-", "#"]


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

# TODO nj@2020-03-09 The number and type of "helper" functions here indicates that further refactoring is required.
#                    Columns are currently a simple list of column objects but should be reworked as a top level
#                    Columns object which has the list of columns as an attribute. That way we all of these global
#                    functions will have access to the column and not need a column_list parameter.
#                    The top-level object could implement __iter__ to attempt backward compatibility?


def get_column_names(column_list, conv_fn=None):
    """Simple helper function to get column names from a list of column objects.
    Doesn't save typing but is easier on the eye.
    conv_fn can be used to upper/lower the names, e.g.:
        get_column_names(table.get_columns(), conv_fn=str.upper)
    """
    if conv_fn:
        return [conv_fn(_.name) for _ in column_list or []]
    else:
        return [_.name for _ in column_list or []]


def get_partition_columns(column_list):
    """Simple helper function to get sorted list of columns where partition_info is set.
    Return bucket columns before partition columns to maintain historical outcome.
    """
    if not column_list:
        return []
    part_cols = [_ for _ in column_list if _.partition_info]
    # Sort part_cols by partition_info.position
    return sorted(part_cols, key=lambda x: x.partition_info.position)


def get_partition_source_column_names(column_list, conv_fn=None):
    """Simple helper function to get source column names from partition columns.
    conv_fn can be used to upper/lower the names, e.g.:
        get_partition_source_column_names(table.get_columns(), conv_fn=str.upper)
    """

    def partition_info(col):
        if col.partition_info:
            return col.partition_info

    columns = get_partition_columns(column_list)
    if conv_fn:
        return [conv_fn(partition_info(_).source_column_name) for _ in columns or []]
    else:
        return [partition_info(_).source_column_name for _ in columns or []]


def is_safe_mapping(prior_safe_mapping, new_safe_mapping) -> bool:
    """Ensure we don't overwrite an unsafe mapping with a safe one."""
    if new_safe_mapping is None:
        # No action
        return prior_safe_mapping
    if not new_safe_mapping:
        # We're saying a mapping is unsafe which overrides all other states
        return new_safe_mapping
    if new_safe_mapping and prior_safe_mapping is None:
        # We can say it's safe if there was no previous state
        return new_safe_mapping
    return prior_safe_mapping


def match_partition_column_by_source(source_column_name, column_list):
    """Looks for, and returns, a partition column with a specific source_column_name in a list of columns"""
    assert source_column_name
    assert isinstance(column_list, list)
    if column_list:
        assert isinstance(column_list[0], ColumnMetadataInterface)
    part_cols_with_source = [
        _
        for _ in column_list
        if _.partition_info and _.partition_info.source_column_name
    ]
    match_cols = [
        _
        for _ in part_cols_with_source
        if _.partition_info.source_column_name.lower() == source_column_name.lower()
    ]
    if match_cols:
        return match_cols[0]
    return None


def match_table_column(
    search_name: str, column_list: list
) -> Optional["ColumnMetadataInterface"]:
    """Looks for, and returns, a column with name search_name in a list of table columns.

    Case insensitive matching."""
    assert search_name
    assert isinstance(column_list, list)
    if column_list:
        assert isinstance(column_list[0], ColumnMetadataInterface)
    match_cols = [_ for _ in column_list if _.name.lower() == search_name.lower()]
    if match_cols:
        return match_cols[0]
    return None


def match_table_column_position(search_name, column_list):
    """Looks for a column with name search_name in a list of table columns and returns the position."""
    assert search_name
    assert isinstance(column_list, list)
    if column_list:
        assert isinstance(column_list[0], ColumnMetadataInterface)
    match_cols = [
        i for i, _ in enumerate(column_list) if _.name.lower() == search_name.lower()
    ]
    if match_cols:
        return match_cols[0]
    return None


def valid_column_list(column_list):
    """Helper function checking a column list is the expected list of expected type."""
    reject_message = invalid_column_list_message(column_list)
    return bool(not reject_message)


def invalid_column_list_message(column_list):
    """Helper function checking a column list is the expected list of expected type.
    Returns string message/None rather than True/False to help with assertion messages.
    Message/None are still truthy so behaviour will be reliable.
    """
    if not isinstance(column_list, list):
        return "Type %s is not list" % type(column_list)
    if not column_list:
        return None
    if isinstance(column_list[0], ColumnMetadataInterface):
        return None
    else:
        return "Type %s is not instance of column" % type(column_list[0])


def is_synthetic_partition_column(column):
    assert column
    column_name = column.name if isinstance(column, ColumnMetadataInterface) else column
    return bool(SYNTHETIC_PARTITION_COLUMN_NAME_RE.search(column_name))


def regex_real_column_from_part_column(synthetic_part_col):
    assert synthetic_part_col
    return SYNTHETIC_PARTITION_COLUMN_NAME_RE.search(synthetic_part_col.upper())


def str_list_of_columns(column_list, with_cr=True):
    """Simple helper function to get a str() of each column in a list
    and optionally split them by \\n rather than leave as a list.
    Mainly helpful when debugging.
    """
    if with_cr:
        return "\n".join(str(_) for _ in column_list or [])
    else:
        return [str(_) for _ in column_list or []]


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###########################################################################
# ColumnPartitionInfo
###########################################################################


class ColumnPartitionInfo:
    """Holds partition information for a single table column.
    The partition_info attribute of ColumnMetadataInterface will hold an object
    of this class when the column is used for partitioning.
    """

    def __init__(
        self,
        position,
        source_column_name=None,
        granularity=None,
        digits=None,
        range_start=None,
        range_end=None,
        function=None,
    ):
        """Construct ColumnPartitionInfo with mandatory position arrtibute and any number of optional attributes.
        position: For system supporting multiple keys tells us the order, starting from 0.
        source_column_name: If another column is source for this one then this is how we know.
        granularity: Granularity when generating data for the column, can be a string (Y, M, D) or a number.
        digits: The number of digits for padding numeric values with zeros when converting to string.
        range_start: Start of any range partition span of permitted  values.
        range_end: End of any range partition span of permitted  values.
        function: UDF name for converting source_column_name into synthetic value.
        """
        assert position is not None
        self.position = position
        self.source_column_name = source_column_name
        self.function = function
        self.granularity = granularity
        self.digits = digits
        self.range_start = range_start
        self.range_end = range_end

    def __str__(self):
        return str_summary_of_self(self)

    def __repr__(self):
        return str_summary_of_self(self)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def synthetic_name(
        self, full_column_list, source_column_name_override=None, position_override=None
    ):
        """Generate a synthetic column name from partition attributes.
        source_column_name_override: Pass in a specific name to use in the resulting synthetic name
                                     This is used for Join Pushdown when an aliased (and
                                     potentially hashed column name is used)
        position_override: As above, used for Join Pushdown.
        """
        assert full_column_list
        assert valid_column_list(full_column_list), invalid_column_list_message(
            full_column_list
        )
        assert self.source_column_name
        source_column = match_table_column(self.source_column_name, full_column_list)
        if self.function:
            position = (
                position_override if position_override is not None else self.position
            )
            synth_desc = "U" + str(position)
        elif source_column.is_number_based() and self.digits:
            synth_desc = ("{:0%sd}" % self.digits).format(int(self.granularity))
        else:
            synth_desc = str(self.granularity)
        return (
            SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE
            % (synth_desc, source_column_name_override or self.source_column_name)
        ).upper()


###########################################################################
# ColumnMetadataInterface
###########################################################################


class ColumnMetadataInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for backend/frontend specific sub-classes.
    Holds details for a single table column.
    Table objects hold a list of objects of this class.
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
    ):
        self.name = self._required_string(name)
        self.data_type = self._required_string(data_type)
        self.data_length = self._optional_integer(data_length)
        self.char_length = self._optional_integer(char_length)
        self.char_semantics = self._optional_upper_in(
            char_semantics,
            [
                CANONICAL_CHAR_SEMANTICS_BYTE,
                CANONICAL_CHAR_SEMANTICS_CHAR,
                CANONICAL_CHAR_SEMANTICS_UNICODE,
            ],
        )
        self.data_precision = self._optional_integer(data_precision)
        self.data_scale = self._optional_integer(data_scale)
        self.nullable = self._optional_boolean(nullable)
        # data_default could be of any type so cannot be validated
        self.data_default = data_default
        self.safe_mapping = self._optional_boolean(safe_mapping)
        self.partition_info = self._optional_object(partition_info, ColumnPartitionInfo)
        # Only used for staging file columns.
        self.staging_file_column_name = None

    def __str__(self):
        return str_summary_of_self(self)

    def __repr__(self):
        return str_summary_of_self(self)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _required_string(self, column_attribute):
        assert column_attribute
        assert isinstance(column_attribute, str)
        return column_attribute

    def _optional_integer(self, column_attribute):
        if column_attribute:
            assert isinstance(column_attribute, int), "{} is not int".format(
                type(column_attribute)
            )
        return column_attribute

    def _optional_boolean(self, column_attribute):
        if column_attribute is not None:
            if isinstance(column_attribute, str):
                if column_attribute.upper() in ("Y", "YES", "TRUE"):
                    return True
                elif column_attribute.upper() in ("N", "NO", "FALSE"):
                    return False
                else:
                    raise ColumnMetadataException(
                        "Invalid boolean value: %s" % column_attribute
                    )
            else:
                assert isinstance(column_attribute, bool)
                return column_attribute
        return None

    def _required_boolean(self, column_attribute):
        assert column_attribute is not None
        return self._optional_boolean(column_attribute)

    def _optional_object(self, column_attribute, object_type):
        if column_attribute:
            assert isinstance(column_attribute, object_type)
        return column_attribute

    def _optional_upper_in(self, column_attribute, valid_list):
        if column_attribute:
            column_attribute = column_attribute.upper()
            assert column_attribute in valid_list
        return column_attribute

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @abstractmethod
    def format_data_type(self):
        """Take the attributes of the column and format a data type spec for use in SQL"""
        pass

    @abstractmethod
    def has_time_element(self):
        """Does the column data contain a time element"""
        pass

    @abstractmethod
    def is_binary(self):
        """Does the column hold binary data"""
        pass

    @abstractmethod
    def is_nan_capable(self):
        """Is the column capable of holding NaN (not-a-number) data"""
        pass

    @abstractmethod
    def is_number_based(self):
        """Is the column numeric in class"""
        pass

    @abstractmethod
    def is_date_based(self):
        """Is the column date in class"""
        pass

    @abstractmethod
    def is_interval(self):
        """Is the column of an interval type"""
        pass

    @abstractmethod
    def is_string_based(self):
        """Is the column string based in class"""
        pass

    @abstractmethod
    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        pass

    def valid_for_offload_predicate(self):
        """Is the column valid for use in an Offload Predicate.
        By default this is False which automatically gives us False for things like Canonical and Staging columns.
        Individual implementations can override it where necessary.
        """
        return False

    def is_nullable(self):
        return self.nullable

    def is_unbound_string(self):
        """Is the column string based and has no user specified size.
        e.g. STRING is generally unbound and VARCHAR generally has a size. But some systems may allow VARCHAR
             without a limit and fall back to being unbound.
        Default logic below may be overridden in specific implementations.
        """
        return bool(
            self.is_string_based()
            and self.data_length is None
            and self.char_length is None
        )

    def clone(
        self,
        name=None,
        data_type=None,
        data_length=None,
        nullable=None,
        partition_info=None,
        char_length=None,
        char_semantics=None,
    ):
        """Makes a copy of the current column and returns it with a new name."""
        clone_col = copy(self)
        if name:
            clone_col.name = name
        if data_type:
            clone_col.data_type = data_type
        if data_length is not None:
            clone_col.data_length = data_length
        if nullable is not None:
            clone_col.nullable = nullable
        if char_length is not None:
            clone_col.char_length = char_length
        if char_semantics is not None:
            clone_col.char_semantics = char_semantics
        if partition_info is not None:
            clone_col.partition_info = partition_info
        return clone_col

    def set_simplified_staging_column_name(self, column_position: int) -> str:
        """Simplified column name when the original name contains special characters. Used for staging columns."""
        if any(_ in self.name for _ in STAGING_FILE_UNSUPPORTED_NAME_CHARACTERS):
            self.staging_file_column_name = (
                f"{STAGING_FILE_SIMPLIFIED_NAME_TOKEN}_{column_position}"
            )
        else:
            self.staging_file_column_name = self.name


class CanonicalColumn(ColumnMetadataInterface):
    """Holds details for a single canonical column."""

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
        from_override=False,
        char_length=None,
        char_semantics=None,
    ):
        """CONSTRUCTOR"""
        super(CanonicalColumn, self).__init__(
            name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            data_default=data_default,
            safe_mapping=safe_mapping,
            partition_info=partition_info,
            char_length=char_length,
            char_semantics=char_semantics,
        )
        self.from_override = from_override
        if self.data_length is None and self.data_type in CANONICAL_TYPE_BYTE_LENGTHS:
            self.data_length = CANONICAL_TYPE_BYTE_LENGTHS[data_type]

    def format_data_type(self):
        # We should never need to format a canonical column
        raise NotImplementedError(
            "format_data_type() is not applicable to a CanonicalColumn"
        )

    def has_time_element(self):
        """Does the column data contain a time element"""
        return bool(
            self.data_type in [GOE_TYPE_TIME, GOE_TYPE_TIMESTAMP, GOE_TYPE_TIMESTAMP_TZ]
        )

    def is_binary(self):
        return bool(self.data_type in [GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY])

    def is_nan_capable(self):
        # We should never need to this for a canonical column
        raise NotImplementedError(
            "is_nan_capable() is not applicable to a CanonicalColumn"
        )

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(self.data_type in NUMERIC_CANONICAL_TYPES)

    def is_date_based(self):
        """Is the column date in class"""
        return bool(self.data_type in DATE_CANONICAL_TYPES)

    def is_interval(self):
        return bool(self.data_type in [GOE_TYPE_INTERVAL_DS, GOE_TYPE_INTERVAL_YM])

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(self.data_type in STRING_CANONICAL_TYPES)

    def is_time_zone_based(self):
        """Does the column contain time zone data"""
        return bool(self.data_type == GOE_TYPE_TIMESTAMP_TZ)
