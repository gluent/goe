#! /usr/bin/env python3
""" OffloadStagingAvroFile, OffloadStagingAvroImpalaFile: OffloadStagingFile implementations for Avro
    Gluent Inc (c) 2015-2021
"""

import logging

import json

from goe.offload.staging.staging_file import OffloadStagingFileInterface, \
    JAVA_PRIMITIVE_FLOAT, JAVA_PRIMITIVE_STRING, JAVA_PRIMITIVE_DOUBLE, JAVA_PRIMITIVE_INTEGER, \
    JAVA_PRIMITIVE_LONG, JAVA_PRIMITIVE_BOOLEAN
from goe.offload.staging.avro.avro_column import StagingAvroColumn, \
    AVRO_TYPE_STRING, AVRO_TYPE_LONG, AVRO_TYPE_BYTES, AVRO_TYPE_INT, AVRO_TYPE_BOOLEAN, \
    AVRO_TYPE_FLOAT, AVRO_TYPE_DOUBLE
from goe.offload.column_metadata import CanonicalColumn, \
    is_safe_mapping, match_table_column,\
    GLUENT_TYPE_FIXED_STRING, GLUENT_TYPE_LARGE_STRING, GLUENT_TYPE_VARIABLE_STRING, GLUENT_TYPE_BINARY,\
    GLUENT_TYPE_LARGE_BINARY, GLUENT_TYPE_INTEGER_1, GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4,\
    GLUENT_TYPE_INTEGER_8, GLUENT_TYPE_INTEGER_38, GLUENT_TYPE_DECIMAL, GLUENT_TYPE_FLOAT, GLUENT_TYPE_DOUBLE,\
    GLUENT_TYPE_DATE, GLUENT_TYPE_TIME, GLUENT_TYPE_TIMESTAMP, GLUENT_TYPE_TIMESTAMP_TZ,\
    GLUENT_TYPE_INTERVAL_DS, GLUENT_TYPE_INTERVAL_YM, GLUENT_TYPE_BOOLEAN


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OffloadStagingAvroFile
###########################################################################

class OffloadStagingAvroFile(OffloadStagingFileInterface):
    """ AVRO implementation
    """

    def __init__(self, load_db_name, table_name, staging_file_format, canonical_columns, binary_data_as_base64,
                 messages, staging_incremental_update=False, dry_run=False):
        """ CONSTRUCTOR
        """
        super(OffloadStagingAvroFile, self).__init__(
            load_db_name, table_name, staging_file_format, canonical_columns, binary_data_as_base64, messages,
            staging_incremental_update=staging_incremental_update, dry_run=dry_run
        )

        logger.info('OffloadStagingAvroFile setup: (%s, %s)' % (load_db_name, table_name))
        if dry_run:
            logger.info('* Dry run *')

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _from_canonical_column_to_avro(self, column):
        """ Translate an internal Gluent column to an Avro column
            This is the basic translations, each specific backend may override individual translations
            before calling this method.
        """
        def new_column(col, data_type, data_length=None, data_precision=None, data_scale=None, safe_mapping=None):
            """ Wrapper that carries name, nullable, data_default & char_semantics forward from RDBMS
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return StagingAvroColumn(col.name, data_type=data_type, data_length=data_length,
                                     data_precision=data_precision, data_scale=data_scale, nullable=col.nullable,
                                     data_default=col.data_default, safe_mapping=safe_mapping,
                                     char_semantics=col.char_semantics)

        assert column
        assert isinstance(column, CanonicalColumn)

        if column.data_type == GLUENT_TYPE_FIXED_STRING:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GLUENT_TYPE_LARGE_STRING:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GLUENT_TYPE_VARIABLE_STRING:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GLUENT_TYPE_BINARY:
            data_type = AVRO_TYPE_STRING if self._binary_data_as_base64 else AVRO_TYPE_BYTES
            return new_column(column, data_type, safe_mapping=True)
        elif column.data_type == GLUENT_TYPE_LARGE_BINARY:
            data_type = AVRO_TYPE_STRING if self._binary_data_as_base64 else AVRO_TYPE_BYTES
            return new_column(column, data_type, safe_mapping=True)
        elif column.data_type in (GLUENT_TYPE_INTEGER_1, GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4):
            if column.safe_mapping:
                return new_column(column, AVRO_TYPE_INT, safe_mapping=True)
            else:
                return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_INTEGER_8:
            if column.safe_mapping:
                return new_column(column, AVRO_TYPE_LONG, safe_mapping=True)
            else:
                return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_INTEGER_38:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_DECIMAL:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_FLOAT:
            if column.safe_mapping:
                return new_column(column, AVRO_TYPE_FLOAT, safe_mapping=True)
            else:
                return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_DOUBLE:
            if column.safe_mapping:
                return new_column(column, AVRO_TYPE_DOUBLE, safe_mapping=True)
            else:
                return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_DATE:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_TIME:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_TIMESTAMP:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_TIMESTAMP_TZ:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_INTERVAL_DS:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_INTERVAL_YM:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GLUENT_TYPE_BOOLEAN:
            return new_column(column, AVRO_TYPE_BOOLEAN, safe_mapping=True)
        else:
            raise NotImplementedError('Unsupported Gluent data type: %s' % column.data_type)

    def _from_avro_to_canonical_column(self, column, use_staging_file_name=False):
        """ Translate an Avro column to an internal Gluent column
            This is the basic translations, each specific backend may override individual translations
            before calling this method.
        """

        def new_column(col, data_type, data_length=None, data_precision=None, data_scale=None, safe_mapping=None):
            """ Wrapper that carries name, nullable, data_default & char_semantics forward
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            name = col.staging_file_column_name if use_staging_file_name else col.name
            return CanonicalColumn(name, data_type=data_type, data_length=data_length,
                                   data_precision=data_precision, data_scale=data_scale, nullable=col.nullable,
                                   data_default=col.data_default, safe_mapping=safe_mapping,
                                   char_semantics=col.char_semantics)

        assert column
        assert isinstance(column, StagingAvroColumn)

        if column.data_type == AVRO_TYPE_BOOLEAN:
            return new_column(column, GLUENT_TYPE_BOOLEAN, safe_mapping=True)
        elif column.data_type == AVRO_TYPE_BYTES:
            data_type = GLUENT_TYPE_VARIABLE_STRING if self._binary_data_as_base64 else GLUENT_TYPE_BINARY
            return new_column(column, data_type, safe_mapping=True)
        elif column.data_type == AVRO_TYPE_DOUBLE:
            return new_column(column, GLUENT_TYPE_DOUBLE, safe_mapping=True)
        elif column.data_type == AVRO_TYPE_FLOAT:
            return new_column(column, GLUENT_TYPE_FLOAT, safe_mapping=True)
        elif column.data_type == AVRO_TYPE_INT:
            return new_column(column, GLUENT_TYPE_INTEGER_4, safe_mapping=True)
        elif column.data_type == AVRO_TYPE_LONG:
            return new_column(column, GLUENT_TYPE_INTEGER_8, safe_mapping=True)
        elif column.data_type == AVRO_TYPE_STRING:
            return new_column(column, GLUENT_TYPE_VARIABLE_STRING, safe_mapping=True)
        else:
            raise NotImplementedError('Unsupported Avro data type: %s' % column.data_type)

    def _get_avro_schema_json_string(self):
        col_schemas = []
        for col in self.get_staging_columns():
            avro_field_type = ('["%s","null"]' if col.nullable else '"%s"') % col.data_type.lower()
            col_schemas.append("""{"name":"%s","type":%s}""" % (col.staging_file_column_name, avro_field_type))

        avro_schema = """{
  "type" : "record",
  "name" : "%(table_name)s",
  "namespace" : "%(load_db)s",
  "fields" : [%(fields)s],
  "tableName" : "%(load_db)s.%(table_name)s"
}""" % {'load_db': self.load_db_name, 'table_name': self.table_name, 'fields': ', '.join(col_schemas)}

        return avro_schema

    def _get_avro_java_primitive(self, staging_column):
        canonical_column = match_table_column(staging_column.name, self._canonical_columns)
        if staging_column.data_type == AVRO_TYPE_BOOLEAN:
            return JAVA_PRIMITIVE_BOOLEAN
        elif staging_column.data_type == AVRO_TYPE_DOUBLE:
            return JAVA_PRIMITIVE_DOUBLE
        elif staging_column.data_type == AVRO_TYPE_FLOAT:
            return JAVA_PRIMITIVE_FLOAT
        elif staging_column.data_type == AVRO_TYPE_INT:
            return JAVA_PRIMITIVE_INTEGER
        elif staging_column.data_type == AVRO_TYPE_LONG:
            return JAVA_PRIMITIVE_LONG
        elif canonical_column.data_type not in (GLUENT_TYPE_BINARY, GLUENT_TYPE_LARGE_BINARY):
            return JAVA_PRIMITIVE_STRING
        # Let the calling program use implicit conversion
        return None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def from_canonical_column(self, column):
        """ Translate an internal Gluent column to an Avro column
        """
        return self._from_canonical_column_to_avro(column)

    def to_canonical_column(self, column, use_staging_file_name=False):
        """ Translate an Avro column to an internal Gluent column
        """
        return self._from_avro_to_canonical_column(column, use_staging_file_name=use_staging_file_name)

    def get_file_schema_json(self, as_string=True):
        json_string = self._get_avro_schema_json_string()
        return json_string if as_string else json.loads(json_string)

    def get_java_primitive(self, staging_column):
        return self._get_avro_java_primitive(staging_column)


###########################################################################
# OffloadStagingAvroImpalaFile
###########################################################################

class OffloadStagingAvroImpalaFile(OffloadStagingAvroFile):
    """ AVRO implementation on Impala (inherits from Avro)
    """

    def __init__(self, load_db_name, table_name, staging_file_format, canonical_columns, binary_data_as_base64,
                 messages, staging_incremental_update=False, dry_run=False):
        """ CONSTRUCTOR
        """
        super(OffloadStagingAvroImpalaFile, self).__init__(
            load_db_name, table_name, staging_file_format, canonical_columns, binary_data_as_base64, messages,
            staging_incremental_update=staging_incremental_update, dry_run=dry_run
        )

        logger.info('OffloadStagingAvroImpalaFile setup: (%s, %s)' % (load_db_name, table_name))
        if dry_run:
            logger.info('* Dry run *')

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def from_canonical_column(self, column):
        """ Translate an internal Gluent column to an Avro/Impala column
        """
        def new_column(col, data_type, data_length=None, data_precision=None, data_scale=None, safe_mapping=None):
            """ Wrapper that carries name, nullable & data_default forward from RDBMS
            """
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return StagingAvroColumn(col.name, data_type=data_type, data_length=data_length,
                                     data_precision=data_precision, data_scale=data_scale, nullable=col.nullable,
                                     data_default=col.data_default, safe_mapping=safe_mapping)

        assert column
        assert isinstance(column, CanonicalColumn)

        if column.data_type == GLUENT_TYPE_BINARY:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GLUENT_TYPE_LARGE_BINARY:
            return new_column(column, AVRO_TYPE_STRING, safe_mapping=True)
        else:
            return self._from_canonical_column_to_avro(column)

    def get_file_schema_json(self, as_string=True):
        json_string = self._get_avro_schema_json_string()
        return json_string if as_string else json.loads(json_string)

    def get_java_primitive(self, staging_column):
        return self._get_avro_java_primitive(staging_column)
