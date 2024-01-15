#! /usr/bin/env python3
""" AvroColumn: Avro implementation of ColumnMetadataInterface
    LICENSE_TEXT
"""

from goe.offload.column_metadata import ColumnMetadataInterface

###############################################################################
# CONSTANTS
###############################################################################

AVRO_TYPE_BOOLEAN = "BOOLEAN"
AVRO_TYPE_BYTES = "BYTES"
AVRO_TYPE_DOUBLE = "DOUBLE"
AVRO_TYPE_FLOAT = "FLOAT"
AVRO_TYPE_INT = "INT"
AVRO_TYPE_LONG = "LONG"
AVRO_TYPE_STRING = "STRING"


###########################################################################
# CLASSES
###########################################################################


class StagingAvroColumn(ColumnMetadataInterface):
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
        char_semantics=None,
    ):
        super(StagingAvroColumn, self).__init__(
            name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            data_default,
            safe_mapping,
            char_semantics=char_semantics,
        )

    def format_data_type(self):
        # Currently no staging data types have a length/precision/scale so no formatting required
        return self.data_type

    def has_time_element(self):
        """Does the column data contain a time"""
        # We should not need to this for a staging column
        raise NotImplementedError(
            "has_time_element() is not applicable to a StagingAvroColumn"
        )

    def is_binary(self):
        return bool(self.data_type == AVRO_TYPE_BYTES)

    def is_nan_capable(self):
        # We should not need to this for a staging column
        raise NotImplementedError(
            "is_nan_capable() is not applicable to a StagingAvroColumn"
        )

    def is_number_based(self):
        """Is the column numeric in class"""
        return bool(
            self.data_type
            in (AVRO_TYPE_DOUBLE, AVRO_TYPE_FLOAT, AVRO_TYPE_INT, AVRO_TYPE_LONG)
        )

    def is_date_based(self):
        """Is the column date in class"""
        return False

    def is_interval(self):
        return False

    def is_string_based(self):
        """Is the column string based in class"""
        return bool(self.data_type == AVRO_TYPE_STRING)

    def is_time_zone_based(self):
        return False
