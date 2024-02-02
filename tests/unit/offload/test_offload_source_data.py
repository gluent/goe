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

import pytest

from goe.offload.bigquery.bigquery_backend_table import BackendBigQueryTable
from goe.offload.offload_constants import DBTYPE_BIGQUERY
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_source_data import (
    OffloadSourceDataIpaRange,
    OFFLOAD_SOURCE_CLIENT_OFFLOAD,
    OffloadSourcePartitions,
)

from tests.unit.test_functions import (
    build_mock_options,
    build_mock_offload_operation,
    build_fake_oracle_table,
    FAKE_ORACLE_BQ_ENV,
)


@pytest.fixture(scope="module")
def config():
    return build_mock_options(FAKE_ORACLE_BQ_ENV)


@pytest.fixture(scope="module")
def messages():
    return OffloadMessages()


@pytest.fixture
def oracle_table(config, messages):
    return build_fake_oracle_table(config, messages)


@pytest.fixture(scope="module")
def bigquery_table(config, messages):
    fake_operation = build_mock_offload_operation()
    test_table_object = BackendBigQueryTable(
        "no_user",
        "no_table",
        DBTYPE_BIGQUERY,
        config,
        messages,
        fake_operation,
        None,  # metadata
        dry_run=True,
        do_not_connect=True,
    )
    return test_table_object


# There are 4 input partitions each of 1 million bytes in size.
# The parameters below simulate different user inputs and the expected number of partition chunks.
@pytest.mark.parametrize(
    "max_offload_chunk_count,max_offload_chunk_size,expected_chunks",
    [
        # Tests by partition count.
        (1, 1_000_000_000, 4),
        (3, 1_000_000_000, 2),
        (4, 1_000_000_000, 1),
        # Tests by partition size.
        (100, 1_000, 4),
        (100, 1_100_000, 4),
        (100, 2_000_001, 2),
        (100, 2_100_000, 2),
        (100, 4_000_001, 1),
    ],
)
def test_partition_chunking(
    messages,
    oracle_table,
    bigquery_table,
    max_offload_chunk_count,
    max_offload_chunk_size,
    expected_chunks,
):
    fake_operation = build_mock_offload_operation()
    fake_operation.max_offload_chunk_count = max_offload_chunk_count
    fake_operation.max_offload_chunk_size = max_offload_chunk_size

    client = OffloadSourceDataIpaRange(
        oracle_table,
        bigquery_table,
        fake_operation,
        {},
        messages,
        OFFLOAD_SOURCE_CLIENT_OFFLOAD,
    )
    partitions_to_offload = OffloadSourcePartitions.from_source_table(
        oracle_table, True
    )
    client._override_partitions_to_offload(partitions_to_offload)
    chunks = list(client.get_partitions_to_offload_chunks())
    assert (
        len(chunks) == expected_chunks
    ), f"Partition chunk count should be {expected_chunks}, not {len(chunks)}"
    # Ensure all partitions are in the to-offload and remaining lists.
    total_p_count = sum(_[0].count() for _ in chunks)
    assert total_p_count == 4, f"Total partition count should be 4, not {total_p_count}"
    # Ensure partitions are in the same order, only split into multiple lists.
    assert chunks[0][0].get_partitions()[-1].partition_name == "P1"
    assert chunks[-1][0].get_partitions()[0].partition_name == "P4"
