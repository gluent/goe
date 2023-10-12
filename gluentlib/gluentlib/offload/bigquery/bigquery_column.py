#! /usr/bin/env python3
""" BigQueryColumn: Oracle implementation of ColumnMetadataInterface
    LICENSE_TEXT
"""

from gluentlib.offload.column_metadata import ColumnMetadataInterface, CANONICAL_CHAR_SEMANTICS_CHAR

###############################################################################
# CONSTANTS
###############################################################################

BIGQUERY_TYPE_BIGNUMERIC = 'BIGNUMERIC'
BIGQUERY_TYPE_BOOLEAN = 'BOOLEAN'
BIGQUERY_TYPE_BYTES = 'BYTES'
BIGQUERY_TYPE_DATE = 'DATE'
BIGQUERY_TYPE_DATETIME = 'DATETIME'
# BIGQUERY_TYPE_FLOAT for translation purposes only, not a supported type
BIGQUERY_TYPE_FLOAT = 'FLOAT'
BIGQUERY_TYPE_FLOAT64 = 'FLOAT64'
BIGQUERY_TYPE_INT64 = 'INT64'
# BIGQUERY_TYPE_INTEGER for translation purposes only, not a supported type
BIGQUERY_TYPE_INTEGER = 'INTEGER'
BIGQUERY_TYPE_NUMERIC = 'NUMERIC'
BIGQUERY_TYPE_STRING = 'STRING'
BIGQUERY_TYPE_TIME = 'TIME'
BIGQUERY_TYPE_TIMESTAMP = 'TIMESTAMP'


###########################################################################
# CLASSES
###########################################################################

class BigQueryColumn(ColumnMetadataInterface):
    """ Holds details for a single table column
        Table objects hold a list of objects of this class
    """
    def __init__(self, name, data_type, data_length=None, data_precision=None, data_scale=None, nullable=None,
                 data_default=None, safe_mapping=True, partition_info=None, bucket_info=None):
        char_length = None
        # Confusingly the API uses BIGQUERY_TYPE_FLOAT/BIGQUERY_TYPE_INTEGER but SQL uses the *64 names.
        # We have standardised on the SQL names.
        if data_type == BIGQUERY_TYPE_FLOAT:
            data_type = BIGQUERY_TYPE_FLOAT64
        elif data_type == BIGQUERY_TYPE_INTEGER:
            data_type = BIGQUERY_TYPE_INT64
        elif data_type == BIGQUERY_TYPE_STRING and data_length:
            char_length = data_length
        super(BigQueryColumn, self).__init__(name, data_type, data_length=data_length, data_precision=data_precision,
                                             data_scale=data_scale, nullable=nullable, data_default=data_default,
                                             safe_mapping=safe_mapping, partition_info=partition_info,
                                             bucket_info=bucket_info, char_length=char_length,
                                             char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR)

    def format_data_type(self):
        return self.data_type

    def has_time_element(self):
        """ Does the column data contain a time """
        return bool(self.data_type in [BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_TIME, BIGQUERY_TYPE_TIMESTAMP])

    def is_binary(self):
        return bool(self.data_type == BIGQUERY_TYPE_BYTES)

    def is_nan_capable(self):
        return bool(self.data_type == BIGQUERY_TYPE_FLOAT64)

    def is_number_based(self):
        """ Is the column numeric in class """
        return bool(self.data_type in (BIGQUERY_TYPE_BIGNUMERIC, BIGQUERY_TYPE_FLOAT64,
                                       BIGQUERY_TYPE_INT64, BIGQUERY_TYPE_NUMERIC))

    def is_date_based(self):
        """ Is the column date in class """
        return bool(self.data_type in (BIGQUERY_TYPE_DATE, BIGQUERY_TYPE_DATETIME, BIGQUERY_TYPE_TIMESTAMP))

    def is_interval(self):
        return False

    def is_string_based(self):
        """ Is the column string based in class """
        return bool(self.data_type == BIGQUERY_TYPE_STRING)

    def is_time_zone_based(self):
        """ Does the column contain time zone data """
        return bool(self.data_type == BIGQUERY_TYPE_TIMESTAMP)

    def valid_for_offload_predicate(self):
        return bool(self.is_number_based()
                    or (self.is_date_based() and not self.is_time_zone_based())
                    or self.is_string_based())
