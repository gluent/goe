# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" TestParquetEncoder: Unit test library to test parquet_encoder module.
"""
from unittest import TestCase, main
import os.path
from pyarrow import parquet

from goe.offload.offload_messages import OffloadMessages
from goe.offload.oracle.oracle_column import OracleColumn, ORACLE_TYPE_VARCHAR2
from goe.util.parquet_encoder import ParquetEncoder, PARQUET_TYPE_STRING
from goe.util.misc_functions import get_temp_path

from tests.unit.util.test_avro_encoder import FakeDb, ROW_COUNT


class TestParquetEncoder(TestCase):
    def test_parquet_encoder(self):
        source_columns = [
            OracleColumn("COLUMN_NAME", ORACLE_TYPE_VARCHAR2, data_length=5)
        ]
        parquet_schema = [
            (_.name, PARQUET_TYPE_STRING, _.nullable) for _ in source_columns
        ]
        messages = OffloadMessages()
        encoder = ParquetEncoder(parquet_schema, messages)
        extraction_cursor = FakeDb(ROW_COUNT)
        local_staging_path = get_temp_path(prefix="goe-unittest", suffix=".parquet")
        rows_imported = encoder.write_from_cursor(
            local_staging_path, extraction_cursor, source_columns
        )
        # Check output file exists
        self.assertTrue(os.path.exists(local_staging_path))
        # Check correct number of rows written
        self.assertEqual(rows_imported, ROW_COUNT)
        # Open the file to prove it is valid Parquet
        parquet_file = parquet.ParquetFile(local_staging_path)
        metadata = parquet_file.metadata.to_dict()
        self.assertEqual(metadata["row_groups"][0]["num_rows"], ROW_COUNT)


if __name__ == "__main__":
    main()
