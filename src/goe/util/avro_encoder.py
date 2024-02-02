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

import os
import time
import zlib

from io import BytesIO
import avro
from avro import io

from goe.offload.column_metadata import match_table_column
from goe.offload.query_import_interface import QueryImportInterface
from goe.offload.offload_messages import VVERBOSE
from goe.offload.staging.avro.avro_staging_file import (
    AVRO_TYPE_BOOLEAN,
    AVRO_TYPE_BYTES,
    AVRO_TYPE_DOUBLE,
    AVRO_TYPE_FLOAT,
    AVRO_TYPE_INT,
    AVRO_TYPE_LONG,
    AVRO_TYPE_STRING,
)
from goe.offload.oracle.oracle_column import ORACLE_TYPE_TIMESTAMP_LOCAL_TZ


###########################################################################
# AvroEncoder
###########################################################################


class AvroEncoder(QueryImportInterface):
    """
    This is not a general purpose Avro encoder, but should handle the schema types we use.
    It takes about half the CPU time to encode cf the standard avro library.
    Further, the encode_from_cursor function is a generator yielding chunks suitable for incrementally appending
    to an existing HDFS file. This should allow us to support streaming to HDFS in the future.

    Writing to AVRO_TYPE_INT and AVRO_TYPE_LONG first converts the value with int()/long() respectively. The reason
    for this is that cx_Oracle does not recognise NUMBER with negative scale as an integral value. It is safe for us
    to coerce the type because we will only stage to INT/LONG if we trust the source schema enforces scale=0.

    This is not library code - it contains some fairly strong coupling to data formats in the offload process.
    """

    def __init__(self, schema, messages, compression=False, base64_columns=None):
        super(AvroEncoder, self).__init__(
            schema, messages, compression=compression, base64_columns=base64_columns
        )

        self.schema = avro.schema.parse(schema)
        self._codec = b"deflate" if compression else b"null"
        self._buffer = BytesIO()
        self._encoder = io.BinaryEncoder(self._buffer)
        self._sync_marker = os.urandom(16)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _column_encode_fn(
        self, projection_index, rdbms_column, avro_data_type, write_as_base64=False
    ):
        null_index, value_index = None, None
        if type(avro_data_type) == avro.schema.UnionSchema:
            null_index = [str(_) for _ in avro_data_type.schemas].index('"null"')
            value_index = len(avro_data_type.schemas) - 1 - null_index
            avro_type = [str(s) for s in avro_data_type.schemas if str(s) != '"null"'][
                0
            ].strip('"')
        else:
            avro_type = avro_data_type.fullname

        avro_type = avro_type.upper()
        rdbms_type = rdbms_column.data_type

        basic_encode = None

        if write_as_base64:
            base64_fn = self._get_base64_encode_fn(rdbms_type)
            basic_encode = lambda encoder, val: encoder.write_bytes(base64_fn(val))

        elif avro_type == AVRO_TYPE_BOOLEAN:
            basic_encode = io.BinaryEncoder.write_boolean

        elif avro_type == AVRO_TYPE_DOUBLE:
            basic_encode = io.BinaryEncoder.write_double

        elif avro_type == AVRO_TYPE_FLOAT:
            basic_encode = io.BinaryEncoder.write_float

        elif avro_type == AVRO_TYPE_INT:
            basic_encode = lambda encoder, val: encoder.write_long(int(val))

        elif avro_type == AVRO_TYPE_LONG:
            basic_encode = lambda encoder, val: encoder.write_long(int(val))

        elif rdbms_type in self._source_data_types_requiring_read:
            if rdbms_column.is_string_based():
                read_fn = self._get_encode_read_fn()
                basic_encode = lambda encoder, val: encoder.write_utf8(read_fn(val))
            else:
                read_fn = self._get_encode_read_fn()
                basic_encode = lambda encoder, val: encoder.write_bytes(read_fn(val))

        elif rdbms_type == ORACLE_TYPE_TIMESTAMP_LOCAL_TZ:
            tsltz_fn = self._get_tsltz_encode_fn()
            basic_encode = lambda encoder, val: encoder.write_utf8(tsltz_fn(val))

        elif rdbms_column.is_string_based():
            basic_encode = io.BinaryEncoder.write_utf8

        elif rdbms_column.is_number_based():
            # Writing a number that is not being mapped to an Avro primitive, we expect it to already be a str.
            basic_encode = io.BinaryEncoder.write_utf8

            def basic_encode(encoder, val):
                return io.BinaryEncoder.write_utf8(encoder, val)

        elif rdbms_column.is_binary():
            # Raw/binary should not undergo any character conversion therefore avoiding write_utf8
            basic_encode = io.BinaryEncoder.write_bytes

        elif avro_type == AVRO_TYPE_BYTES:
            # Doing this after LOB types to allow them to use val.read()
            basic_encode = io.BinaryEncoder.write_bytes

        elif avro_type == AVRO_TYPE_STRING:
            # If we are casting to string then it's a synthetic value and safe for str()
            basic_encode = lambda encoder, val: encoder.write_utf8(str(val))
            # Now that we include a number in FF of NLS_TIMESTAMP_FORMAT we don't need to look for trailing dot
            # if rdbms_type == ORACLE_TYPE_TIMESTAMP:
            #     # When offloading TIMESTAMP(0) we end up with a string with a trailing ".", e.g. "2024-04-29 05:55:58."
            #     basic_encode = lambda encoder, val: encoder.write_utf8(self._strip_trailing_dot(str(val)))
            # else:

        if null_index is None:
            return lambda encoder, row: basic_encode(encoder, row[projection_index])
        else:

            def union_encode(encoder, row):
                val = row[projection_index]
                if val is None:
                    encoder.write_long(null_index)
                else:
                    encoder.write_long(value_index)
                    basic_encode(encoder, val)

            return union_encode

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def encode_from_cursor(self, extraction_cursor, source_columns, fetch_size=None):
        """fetch_size optional because not all frontends take a parameter to fetchmany()."""

        def do_fetch(cursor):
            if fetch_size:
                return cursor.fetchmany(fetch_size)
            else:
                return cursor.fetchmany()

        # Encode header
        self._encoder.write(b"Obj" + bytes([1]))
        self._encoder.write_long(2)
        self._encoder.write_utf8("avro.schema")
        self._encoder.write_bytes(str(self.schema).encode("utf8"))
        self._encoder.write_utf8("avro.codec")
        self._encoder.write_bytes(self._codec)
        self._encoder.write_long(0)

        self._encoder.write(self._sync_marker)

        yield self._buffer.getvalue()
        self._buffer.truncate(0)
        self._buffer.seek(0)

        avro_conv_fns = []

        self._debug(
            "extraction_cursor format: {}".format(str(extraction_cursor.description))
        )
        for f in self.schema.fields:
            projection_index = [col[0] for col in extraction_cursor.description].index(
                f.name
            )
            col = source_columns[projection_index]
            avro_conv_fns.append(
                self._column_encode_fn(
                    projection_index,
                    col,
                    f.type,
                    write_as_base64=bool(
                        match_table_column(col.name, self._base64_columns)
                    ),
                )
            )

        # encode rows
        batch = 1
        self._debug(f"Fetching row batch: {batch}")
        rows = do_fetch(extraction_cursor)
        while rows:
            record_count = 0
            for row in rows:
                # write records to buffer
                for encode_fn in avro_conv_fns:
                    encode_fn(self._encoder, row)
                record_count += 1

            uncompressed_data = self._buffer.getvalue()
            self._buffer.truncate(0)
            self._buffer.seek(0)

            if self._codec == b"deflate":
                compressed_data = zlib.compress(uncompressed_data)[2:-1]
            else:
                compressed_data = uncompressed_data

            self._encoder.write_long(record_count)
            self._encoder.write_long(len(compressed_data))

            yield self._buffer.getvalue() + compressed_data + self._sync_marker
            self._buffer.truncate(0)
            self._buffer.seek(0)

            batch += 1
            self._debug(f"Fetching row batch: {batch}")
            rows = do_fetch(extraction_cursor)

    def write_from_cursor(
        self, local_output_path, extraction_cursor, source_columns, fetch_size=None
    ):
        """fetch_size optional because not all frontends take a parameter to fetchmany()."""
        assert local_output_path
        assert isinstance(local_output_path, str)
        ts1 = time.time()
        self._log("Writing Avro(compression=%s)" % self._codec, detail=VVERBOSE)
        with open(local_output_path, "wb") as writer:
            for chunk in self.encode_from_cursor(
                extraction_cursor, source_columns, fetch_size=fetch_size
            ):
                writer.write(chunk)
        ts2 = time.time()
        self._log("Extract & write elapsed: %.1fs" % (ts2 - ts1), detail=VVERBOSE)
        return extraction_cursor.rowcount
