#! /usr/bin/env python3
""" NetezzaColumn: Netezza implementation of ColumnMetadataInterface
    LICENSE_TEXT
"""

from gluentlib.offload.column_metadata import ColumnMetadataInterface

###############################################################################
# CONSTANTS
###############################################################################

# Data type constants, these must match the types stored in the data dictionary

NETEZZA_TYPE_BOOLEAN = 'BOOLEAN'
NETEZZA_TYPE_BYTEINT = 'BYTEINT'
NETEZZA_TYPE_SMALLINT = 'SMALLINT'
NETEZZA_TYPE_INTEGER = 'INTEGER'
NETEZZA_TYPE_BIGINT = 'BIGINT'
NETEZZA_TYPE_NUMERIC = 'NUMERIC'
NETEZZA_TYPE_REAL = 'REAL'
NETEZZA_TYPE_DOUBLE_PRECISION = 'DOUBLE PRECISION'
NETEZZA_TYPE_DATE = 'DATE'
NETEZZA_TYPE_TIME = 'TIME'
NETEZZA_TYPE_TIME_WITH_TIME_ZONE = 'TIME WITH TIME ZONE'
NETEZZA_TYPE_TIMESTAMP = 'TIMESTAMP'
NETEZZA_TYPE_INTERVAL = 'INTERVAL'
NETEZZA_TYPE_CHARACTER = 'CHARACTER'
NETEZZA_TYPE_CHARACTER_VARYING = 'CHARACTER VARYING'
NETEZZA_TYPE_NATIONAL_CHARACTER = 'NATIONAL CHARACTER'
NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING = 'NATIONAL CHARACTER VARYING'
NETEZZA_TYPE_BINARY_VARYING = 'BINARY VARYING'
NETEZZA_TYPE_ST_GEOMETRY = 'ST_GEOMETRY'


###########################################################################
# CLASSES
###########################################################################

class NetezzaColumn(ColumnMetadataInterface):
    """ Holds details for a single table column
        Table objects hold a list of objects of this class
    """

    def __init__(self, name, data_type, data_length=None, data_precision=None, data_scale=None, nullable=None,
                 data_default=None, hidden=None, char_length=None, safe_mapping=True, partition_info=None,
                 char_semantics=None):
        super(NetezzaColumn, self).__init__(name, data_type, data_length, data_precision, data_scale, nullable,
                                            data_default, safe_mapping=safe_mapping, partition_info=partition_info,
                                            char_length=None, char_semantics=char_semantics)
        self.hidden = hidden
        #TODO NJ@2020-01-07 Does Netezza catalog store data types with () in the names like Oracle does?
        #                   If so then we need to trim them here

    @staticmethod
    def from_netezza(attribute_list):
        """ Accepts an IBM Netezza specific list of attributes for a column and returns object based on them
        """
        name, _, nullable, data_type, data_precision, data_scale, data_length, data_default, hidden, char_length = attribute_list
        col = NetezzaColumn(name, data_type, data_length, data_precision, data_scale,
                            nullable, data_default, hidden, char_length)
        return col

    def format_data_type(self):
        raise NotImplementedError('Netezza NetezzaColumn.format_data_type() not implemented.')

    def has_time_element(self):
        """ Does the column data contain a time """
        return bool(self.data_type in [NETEZZA_TYPE_TIME, NETEZZA_TYPE_TIME_WITH_TIME_ZONE, NETEZZA_TYPE_TIMESTAMP])

    def is_binary(self):
        return bool(self.data_type == NETEZZA_TYPE_BINARY_VARYING)

    def is_nan_capable(self):
        return bool(self.data_type in [NETEZZA_TYPE_DOUBLE_PRECISION, NETEZZA_TYPE_REAL])

    def is_number_based(self):
        """ Is the column numeric in class
        """
        return bool(self.data_type in [NETEZZA_TYPE_BIGINT, NETEZZA_TYPE_INTEGER, NETEZZA_TYPE_SMALLINT,
                                       NETEZZA_TYPE_BYTEINT, NETEZZA_TYPE_DOUBLE_PRECISION, NETEZZA_TYPE_REAL,
                                       NETEZZA_TYPE_NUMERIC])

    def is_date_based(self):
        """ Is the column date in class
        """
        #TODO SS@2017-09-04 Ignoring TIME ZONE to prevent code changes in avro_to_hadoop_casts for now. Revisit later.
        return any(dt in self.data_type for dt in [NETEZZA_TYPE_DATE, NETEZZA_TYPE_TIMESTAMP])

    def is_interval(self):
        return bool(self.data_type == NETEZZA_TYPE_INTERVAL)

    def is_string_based(self):
        """ Is the column string based in class
        """
        return bool(self.data_type in [NETEZZA_TYPE_CHARACTER_VARYING, NETEZZA_TYPE_CHARACTER,
                                       NETEZZA_TYPE_NATIONAL_CHARACTER_VARYING, NETEZZA_TYPE_NATIONAL_CHARACTER])

    def is_time_zone_based(self):
        """ Does the column contain time zone data
        """
        return bool(self.data_type.upper() == NETEZZA_TYPE_TIME_WITH_TIME_ZONE)

    def is_hidden(self):
        return self.hidden

    def valid_for_offload_predicate(self):
        return bool(self.is_number_based()
                    or (self.is_date_based() and not self.is_time_zone_based())
                    or self.is_string_based())
