#! /usr/bin/env python3
""" LICENSE_TEXT
"""

from goe.offload.offload_constants import (
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.util.avro_encoder import AvroEncoder
from goe.util.parquet_encoder import ParquetEncoder


def query_import_factory(
    staging_file, messages, compression=False, base64_columns=None
):
    if staging_file.file_format == FILE_STORAGE_FORMAT_AVRO:
        return AvroEncoder(
            staging_file.get_file_schema_json(),
            messages,
            compression=compression,
            base64_columns=base64_columns,
        )
    elif staging_file.file_format == FILE_STORAGE_FORMAT_PARQUET:
        return ParquetEncoder(
            staging_file.get_file_schema_json(as_string=False),
            messages,
            compression=compression,
            base64_columns=base64_columns,
        )
    else:
        raise NotImplementedError(
            "Unsupported file format: %s" % staging_file.file_format
        )
