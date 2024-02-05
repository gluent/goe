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

""" TestAvroEncoder: Unit test library to test avro_encoder module.
"""
from unittest import TestCase, main
import os.path
from textwrap import dedent

from avro.datafile import DataFileReader
from avro.io import DatumReader

from goe.offload.offload_messages import OffloadMessages
from goe.offload.oracle.oracle_column import OracleColumn, ORACLE_TYPE_VARCHAR2
from goe.util.avro_encoder import AvroEncoder
from goe.util.misc_functions import get_temp_path


FETCH_SIZE = 5
# Row count enough to cause two batches and stop mid batch
ROW_COUNT = 8
SCHEMA_JSON = dedent(
    """\
    { "type" : "record",
      "name" : "no_table",
      "namespace" : "sh_test",
      "fields" : [{"name":"COLUMN_NAME","type":"string"}],
      "tableName" : "sh_test.no_table"
    }"""
)


class FakeDb(object):
    """Pretends to be a cx_Oracle cursor over a single string column table so we can test without needing a database"""

    def __init__(self, row_count):
        assert row_count
        self._rows = [(str(_),) for _ in range(row_count)]
        self.description = [("COLUMN_NAME",)]
        self.rowcount = 0

    def fetch(self):
        if self._rows:
            self.rowcount += 1
            return self._rows.pop()
        else:
            return []

    def fetchmany(self):
        if self._rows:
            rows = self._rows[:FETCH_SIZE]
            self.rowcount += len(self._rows[:FETCH_SIZE])
            self._rows = self._rows[FETCH_SIZE:]
            return rows
        else:
            return []


class TestAvroEncoder(TestCase):
    def test_avro_encoder(self):
        source_columns = [OracleColumn("COLUMN_NAME", ORACLE_TYPE_VARCHAR2)]
        messages = OffloadMessages()
        encoder = AvroEncoder(SCHEMA_JSON, messages)
        extraction_cursor = FakeDb(ROW_COUNT)
        local_staging_path = get_temp_path(prefix="goe-unittest", suffix=".avro")
        rows_imported = encoder.write_from_cursor(
            local_staging_path, extraction_cursor, source_columns
        )
        # Check output file exists
        self.assertTrue(os.path.exists(local_staging_path))
        # Check correct number of rows written
        self.assertEqual(rows_imported, ROW_COUNT)
        # Open the file to prove it is valid Avro
        with DataFileReader(open(local_staging_path, "rb"), DatumReader()) as reader:
            # Check correct number of rows in file
            rows_read = len([_ for _ in reader])
            self.assertEqual(rows_read, ROW_COUNT)


if __name__ == "__main__":
    main()
