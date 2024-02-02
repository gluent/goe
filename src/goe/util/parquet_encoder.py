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

""" ParquetEncoder: Library for encoding and writing data in Parquet during Offload Transport.
"""

import time

import pyarrow
from pyarrow import parquet

from goe.offload.column_metadata import match_table_column
from goe.offload.query_import_interface import QueryImportInterface
from goe.offload.offload_messages import VVERBOSE
from goe.offload.oracle.oracle_column import ORACLE_TYPE_TIMESTAMP_LOCAL_TZ
from goe.offload.staging.parquet.parquet_staging_file import (
    PARQUET_TYPE_BINARY,
    PARQUET_TYPE_BOOLEAN,
    PARQUET_TYPE_DOUBLE,
    PARQUET_TYPE_FLOAT,
    PARQUET_TYPE_INT32,
    PARQUET_TYPE_INT64,
    PARQUET_TYPE_STRING,
)


###########################################################################
# ParquetEncoder
###########################################################################


class ParquetEncoder(QueryImportInterface):
    """This is not a general purpose Parquet encoder, but handles the schema types we use in Offload Transport."""

    def __init__(self, schema, messages, compression=False, base64_columns=None):
        super(ParquetEncoder, self).__init__(
            schema, messages, compression=compression, base64_columns=base64_columns
        )

        self.schema = self._schema_to_pyarrow(schema)
        self._codec = "SNAPPY" if compression else "NONE"
        # Below can be '1.0' or '2.0', according to documentation:
        #   1.0 is more portable
        #   2.0 introduced a new serialized data page format
        # Cloudera say: "Data using the version 2.0 of Parquet writer might not be consumable
        #                by Impala, due to use of the RLE_DICTIONARY encoding."
        self._parquet_version = "1.0"
        self._buffer_bytes = 1024 * 1024 * 20

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _extract_rows(self, extraction_cursor, fetch_size):
        def do_fetch(cursor):
            if fetch_size:
                return cursor.fetchmany(fetch_size)
            else:
                return cursor.fetchmany()

        batch = 1
        while True:
            self._debug(f"Fetching row batch: {batch}")
            rows = do_fetch(extraction_cursor)
            if not rows:
                break
            batch += 1
            yield rows

    def _wrangle_data(self, columnar, source_columns):
        """Wrangle data to fit with Offload process.
        Binary columns: LOBs need read() applying and some binary columns need base64 encoding.
        Timestamp columns: If being offloaded to string then we cannot tolerate a trailing '.'.
        Populates new_columnar using labels of columnar contents for non-binary columns which minimizes overhead.
        """
        new_columnar = {}
        for column_name, data in columnar.items():
            source_column = match_table_column(column_name, source_columns)
            if match_table_column(column_name, self._base64_columns):
                base64_fn = self._get_base64_encode_fn(source_column.data_type)
                new_data = [base64_fn(_) if _ is not None else _ for _ in data]
                new_columnar[column_name] = new_data
            elif source_column.data_type in self._source_data_types_requiring_read:
                read_fn = self._get_encode_read_fn()
                new_data = [read_fn(_) if _ is not None else _ for _ in data]
                new_columnar[column_name] = new_data
            elif source_column.data_type == ORACLE_TYPE_TIMESTAMP_LOCAL_TZ:
                tsltz_fn = self._get_tsltz_encode_fn()
                new_data = [tsltz_fn(_) if _ is not None else _ for _ in data]
                new_columnar[column_name] = new_data
            else:
                new_columnar[column_name] = columnar[column_name]
        return new_columnar

    def _rowbased_to_columnar(self, column_names, row_batch):
        """Convert row orientated data (list of tuples) to column orientated (dict of lists).
        Iterates through each column and pulls out the relevant slice. This outperformed numpy.rot90 and other
        techniques we tested.
        Returns the columnar structure.
        """
        columnar = {}
        for i, column_name in enumerate(column_names):
            columnar[column_name] = [_[i] for _ in row_batch]
        return columnar

    def _schema_to_pyarrow(self, schema):
        fields = [
            (name, self._schema_type_to_pyarrow(data_type), nullable)
            for name, data_type, nullable in schema
        ]
        return pyarrow.schema(fields)

    def _schema_type_to_pyarrow(self, schema_type):
        """Returns a PyArrow type for the Parquet schema type.
        https://arrow.apache.org/docs/python/api/datatypes.html
        """
        type_map = {
            PARQUET_TYPE_BINARY: pyarrow.binary(),
            PARQUET_TYPE_BOOLEAN: pyarrow.bool_(),
            PARQUET_TYPE_DOUBLE: pyarrow.float64(),
            PARQUET_TYPE_FLOAT: pyarrow.float32(),
            PARQUET_TYPE_INT32: pyarrow.int32(),
            PARQUET_TYPE_INT64: pyarrow.int64(),
            PARQUET_TYPE_STRING: pyarrow.string(),
        }
        return type_map[schema_type]

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def write_from_cursor(
        self, local_output_path, extraction_cursor, source_columns, fetch_size=None
    ):
        """fetch_size optional because not all frontends take a parameter to fetchmany()."""

        def write_as_parquet(writer, table):
            writer.write_table(table=table)
            self._log(
                "Writing buffered rows/MBs: %s/%.1f"
                % (table.num_rows, float(table.nbytes) / 1024 / 1024),
                detail=VVERBOSE,
            )

        assert local_output_path
        assert isinstance(local_output_path, str)

        ts1 = time.time()
        column_names = [_[0] for _ in extraction_cursor.description]

        # buffer_table builds up to a size threshold at which point we write to Parquet and start again
        buffer_table = None
        self._log(
            "Writing Parquet(version=%s, compression=%s)"
            % (self._parquet_version, self._codec),
            detail=VVERBOSE,
        )
        self._debug(
            "extraction_cursor format: {}".format(str(extraction_cursor.description))
        )
        writer = parquet.ParquetWriter(
            local_output_path,
            schema=self.schema,
            version=self._parquet_version,
            compression=self._codec,
        )
        try:
            for row_batch in self._extract_rows(
                extraction_cursor, fetch_size=fetch_size
            ):
                columnar = self._rowbased_to_columnar(column_names, row_batch)
                columnar = self._wrangle_data(columnar, source_columns)
                if buffer_table is None:
                    buffer_table = pyarrow.Table.from_pydict(
                        columnar, schema=self.schema
                    )
                else:
                    table = pyarrow.Table.from_pydict(columnar, schema=self.schema)
                    buffer_table = pyarrow.concat_tables([buffer_table, table])
                if buffer_table.nbytes > self._buffer_bytes:
                    # Write full PyArrow buffer
                    write_as_parquet(writer, buffer_table)
                    buffer_table = None
            if buffer_table:
                # Write remaining contents of PyArrow buffer
                write_as_parquet(writer, buffer_table)
        finally:
            try:
                writer.close()
            except:
                pass

        ts2 = time.time()
        self._log("Extract & write elapsed: %.1fs" % (ts2 - ts1), detail=VVERBOSE)
        return extraction_cursor.rowcount
