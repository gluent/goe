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
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SNOWFLAKE,
    DBTYPE_SYNAPSE,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
)
from goe.offload.staging.avro.avro_staging_file import (
    OffloadStagingAvroFile,
    OffloadStagingAvroImpalaFile,
)
from goe.offload.staging.parquet.parquet_staging_file import OffloadStagingParquetFile


def staging_file_factory(
    load_db_name,
    table_name,
    staging_file_format,
    canonical_columns,
    orchestration_options,
    binary_data_as_base64,
    messages,
    dry_run=False,
):
    """Construct the correct Staging File class based on the format and offload transport orchestration query engine.
    "offload transport orchestration query engine" - quite a mouthful. I'm just trying to convey that this is not
    the engine used to write to the Avro file but the engine used to validate it and insert into the final table.
    """
    if (
        staging_file_format == FILE_STORAGE_FORMAT_AVRO
        and orchestration_options.target == DBTYPE_IMPALA
    ):
        return OffloadStagingAvroImpalaFile(
            load_db_name,
            table_name,
            staging_file_format,
            canonical_columns,
            binary_data_as_base64,
            messages,
            dry_run=dry_run,
        )
    elif (
        staging_file_format == FILE_STORAGE_FORMAT_AVRO
        and orchestration_options.target
        in [DBTYPE_HIVE, DBTYPE_BIGQUERY, DBTYPE_SNOWFLAKE]
    ):
        return OffloadStagingAvroFile(
            load_db_name,
            table_name,
            staging_file_format,
            canonical_columns,
            binary_data_as_base64,
            messages,
            dry_run=dry_run,
        )
    elif (
        staging_file_format == FILE_STORAGE_FORMAT_PARQUET
        and orchestration_options.target
        in [DBTYPE_BIGQUERY, DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE]
    ):
        return OffloadStagingParquetFile(
            load_db_name,
            table_name,
            staging_file_format,
            canonical_columns,
            binary_data_as_base64,
            messages,
            dry_run=dry_run,
        )
    else:
        raise NotImplementedError(
            "Unsupported staging file format: %s on %s"
            % (staging_file_format, orchestration_options.target)
        )
