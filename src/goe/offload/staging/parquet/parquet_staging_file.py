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

""" OffloadStagingParquetFile: OffloadStagingFile implementation for Parquet
"""

import logging

from goe.offload.staging.staging_file import (
    OffloadStagingFileInterface,
    JAVA_PRIMITIVE_FLOAT,
    JAVA_PRIMITIVE_STRING,
    JAVA_PRIMITIVE_DOUBLE,
    JAVA_PRIMITIVE_INTEGER,
    JAVA_PRIMITIVE_LONG,
    JAVA_PRIMITIVE_BOOLEAN,
)
from goe.offload.staging.parquet.parquet_column import (
    StagingParquetColumn,
    PARQUET_TYPE_STRING,
    PARQUET_TYPE_FLOAT,
    PARQUET_TYPE_INT32,
    PARQUET_TYPE_BINARY,
    PARQUET_TYPE_BOOLEAN,
    PARQUET_TYPE_DOUBLE,
    PARQUET_TYPE_INT64,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    is_safe_mapping,
    match_table_column,
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
)


###############################################################################
# CONSTANTS
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OffloadStagingParquetFile
###########################################################################


class OffloadStagingParquetFile(OffloadStagingFileInterface):
    """Parquet implementation"""

    def __init__(
        self,
        load_db_name,
        table_name,
        staging_file_format,
        canonical_columns,
        binary_data_as_base64,
        messages,
        dry_run=False,
    ):
        """CONSTRUCTOR"""
        super(OffloadStagingParquetFile, self).__init__(
            load_db_name,
            table_name,
            staging_file_format,
            canonical_columns,
            binary_data_as_base64,
            messages,
            dry_run=dry_run,
        )

        logger.info(
            "OffloadStagingParquetFile setup: (%s, %s)" % (load_db_name, table_name)
        )
        if dry_run:
            logger.info("* Dry run *")

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _from_canonical_column_to_parquet(self, column):
        """Translate an internal GOE column to a Parquet column
        This is the basic translations, each specific backend may override individual translations
        before calling this method.
        """

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable, data_default & char_semantics forward from RDBMS"""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            return StagingParquetColumn(
                col.name,
                data_type=data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                char_semantics=col.char_semantics,
            )

        assert column
        assert isinstance(column, CanonicalColumn)

        if column.data_type == GOE_TYPE_FIXED_STRING:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_LARGE_STRING:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_VARIABLE_STRING:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=True)
        elif column.data_type == GOE_TYPE_BINARY:
            data_type = (
                PARQUET_TYPE_STRING
                if self._binary_data_as_base64
                else PARQUET_TYPE_BINARY
            )
            return new_column(column, data_type, safe_mapping=True)
        elif column.data_type == GOE_TYPE_LARGE_BINARY:
            data_type = (
                PARQUET_TYPE_STRING
                if self._binary_data_as_base64
                else PARQUET_TYPE_BINARY
            )
            return new_column(column, data_type, safe_mapping=True)
        elif column.data_type in (
            GOE_TYPE_INTEGER_1,
            GOE_TYPE_INTEGER_2,
            GOE_TYPE_INTEGER_4,
        ):
            if column.safe_mapping:
                return new_column(column, PARQUET_TYPE_INT32, safe_mapping=True)
            else:
                return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTEGER_8:
            if column.safe_mapping:
                return new_column(column, PARQUET_TYPE_INT64, safe_mapping=True)
            else:
                return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTEGER_38:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_DECIMAL:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_FLOAT:
            if column.safe_mapping:
                return new_column(column, PARQUET_TYPE_FLOAT, safe_mapping=True)
            else:
                return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_DOUBLE:
            if column.safe_mapping:
                return new_column(column, PARQUET_TYPE_DOUBLE, safe_mapping=True)
            else:
                return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_DATE:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_TIME:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_TIMESTAMP:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_TIMESTAMP_TZ:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_DS:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_INTERVAL_YM:
            return new_column(column, PARQUET_TYPE_STRING, safe_mapping=False)
        elif column.data_type == GOE_TYPE_BOOLEAN:
            return new_column(column, PARQUET_TYPE_BOOLEAN, safe_mapping=True)
        else:
            raise NotImplementedError(
                "Unsupported GOE data type: %s" % column.data_type
            )

    def _from_parquet_to_canonical_column(self, column, use_staging_file_name=False):
        """Translate a Parquet column to an internal GOE column
        This is the basic translations, each specific backend may override individual translations
        before calling this method.
        """

        def new_column(
            col,
            data_type,
            data_length=None,
            data_precision=None,
            data_scale=None,
            safe_mapping=None,
        ):
            """Wrapper that carries name, nullable, data_default & char_semantics forward"""
            safe_mapping = is_safe_mapping(col.safe_mapping, safe_mapping)
            name = col.staging_file_column_name if use_staging_file_name else col.name
            return CanonicalColumn(
                name,
                data_type=data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=col.nullable,
                data_default=col.data_default,
                safe_mapping=safe_mapping,
                char_semantics=col.char_semantics,
            )

        assert column
        assert isinstance(column, StagingParquetColumn)

        if column.data_type == PARQUET_TYPE_BOOLEAN:
            return new_column(column, GOE_TYPE_BOOLEAN, safe_mapping=True)
        elif column.data_type == PARQUET_TYPE_BINARY:
            data_type = (
                GOE_TYPE_VARIABLE_STRING
                if self._binary_data_as_base64
                else GOE_TYPE_BINARY
            )
            return new_column(column, data_type, safe_mapping=True)
        elif column.data_type == PARQUET_TYPE_DOUBLE:
            return new_column(column, GOE_TYPE_DOUBLE, safe_mapping=True)
        elif column.data_type == PARQUET_TYPE_FLOAT:
            return new_column(column, GOE_TYPE_FLOAT, safe_mapping=True)
        elif column.data_type == PARQUET_TYPE_INT32:
            return new_column(column, GOE_TYPE_INTEGER_4, safe_mapping=True)
        elif column.data_type == PARQUET_TYPE_INT64:
            return new_column(column, GOE_TYPE_INTEGER_8, safe_mapping=True)
        elif column.data_type == PARQUET_TYPE_STRING:
            return new_column(column, GOE_TYPE_VARIABLE_STRING, safe_mapping=True)
        else:
            raise NotImplementedError(
                "Unsupported Parquet data type: %s" % column.data_type
            )

    def _get_parquet_java_primitive(self, staging_column):
        canonical_column = match_table_column(
            staging_column.name, self._canonical_columns
        )
        if staging_column.data_type == PARQUET_TYPE_BOOLEAN:
            return JAVA_PRIMITIVE_BOOLEAN
        elif staging_column.data_type == PARQUET_TYPE_DOUBLE:
            return JAVA_PRIMITIVE_DOUBLE
        elif staging_column.data_type == PARQUET_TYPE_FLOAT:
            return JAVA_PRIMITIVE_FLOAT
        elif staging_column.data_type == PARQUET_TYPE_INT32:
            return JAVA_PRIMITIVE_INTEGER
        elif staging_column.data_type == PARQUET_TYPE_INT64:
            return JAVA_PRIMITIVE_LONG
        elif canonical_column.data_type not in (GOE_TYPE_BINARY, GOE_TYPE_LARGE_BINARY):
            return JAVA_PRIMITIVE_STRING
        # Let the calling program use implicit conversion
        return None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def from_canonical_column(self, column):
        """Translate an internal GOE column to a Parquet column"""
        return self._from_canonical_column_to_parquet(column)

    def to_canonical_column(self, column, use_staging_file_name=False):
        """Translate a Parquet column to an internal GOE column"""
        return self._from_parquet_to_canonical_column(
            column, use_staging_file_name=use_staging_file_name
        )

    def get_java_primitive(self, staging_column):
        return self._get_parquet_java_primitive(staging_column)

    def get_file_schema_json(self, as_string=True):
        schema = [
            (_.staging_file_column_name, _.format_data_type(), bool(_.nullable))
            for _ in self.get_staging_columns()
        ]
        return repr(schema) if as_string else schema
