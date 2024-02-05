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
