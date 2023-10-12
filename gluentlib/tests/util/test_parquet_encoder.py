""" TestParquetEncoder: Unit test library to test parquet_encoder module.
"""
from unittest import TestCase, main
import os.path
from pyarrow import parquet

from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.offload.oracle.oracle_column import OracleColumn, ORACLE_TYPE_VARCHAR2
from gluentlib.util.parquet_encoder import ParquetEncoder, PARQUET_TYPE_STRING
from gluentlib.util.misc_functions import get_temp_path

from tests.util.test_avro_encoder import FakeDb, ROW_COUNT


class TestParquetEncoder(TestCase):

    def test_parquet_encoder(self):
        source_columns = [OracleColumn('COLUMN_NAME', ORACLE_TYPE_VARCHAR2, data_length=5)]
        parquet_schema = [(_.name, PARQUET_TYPE_STRING, _.nullable) for _ in source_columns]
        messages = OffloadMessages()
        encoder = ParquetEncoder(parquet_schema, messages)
        extraction_cursor = FakeDb(ROW_COUNT)
        local_staging_path = get_temp_path(prefix='gluent-unittest', suffix='.parquet')
        rows_imported = encoder.write_from_cursor(local_staging_path, extraction_cursor, source_columns)
        # Check output file exists
        self.assertTrue(os.path.exists(local_staging_path))
        # Check correct number of rows written
        self.assertEqual(rows_imported, ROW_COUNT)
        # Open the file to prove it is valid Parquet
        parquet_file = parquet.ParquetFile(local_staging_path)
        metadata = parquet_file.metadata.to_dict()
        self.assertEqual(metadata['row_groups'][0]['num_rows'], ROW_COUNT)


if __name__ == '__main__':
    main()
