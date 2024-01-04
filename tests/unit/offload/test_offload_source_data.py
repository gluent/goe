import numpy
import pytest

from goe.offload.column_metadata import ColumnPartitionInfo
from goe.offload.bigquery.bigquery_backend_table import BackendBigQueryTable
from goe.offload.offload_constants import DBTYPE_BIGQUERY
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_source_data import (
    OffloadSourceDataIpaRange,
    OFFLOAD_SOURCE_CLIENT_OFFLOAD,
    OffloadSourcePartitions,
)
from goe.offload.offload_source_table import RdbmsPartition
from goe.offload.oracle.oracle_column import (
    OracleColumn,
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_NUMBER,
)
from goe.offload.oracle.oracle_offload_source_table import OracleSourceTable

from tests.unit.test_functions import (
    build_mock_options,
    build_mock_offload_operation,
    FAKE_ORACLE_BQ_ENV,
)

FRONTEND_COLUMNS = [
    OracleColumn(
        "PROD_ID",
        ORACLE_TYPE_NUMBER,
        data_precision=4,
        data_scale=0,
    ),
    OracleColumn(
        "TIME_ID",
        ORACLE_TYPE_DATE,
        partition_info=ColumnPartitionInfo(
            position=0, range_end=None, range_start=None, source_column_name=None
        ),
    ),
]


@pytest.fixture(scope="module")
def config():
    return build_mock_options(FAKE_ORACLE_BQ_ENV)


@pytest.fixture(scope="module")
def messages():
    return OffloadMessages()


@pytest.fixture
def oracle_table(config, messages):
    test_table_object = OracleSourceTable(
        "no_user",
        "no_table",
        config,
        messages,
        dry_run=True,
        do_not_connect=True,
    )
    test_table_object._columns = FRONTEND_COLUMNS
    test_table_object._columns_with_partition_info = FRONTEND_COLUMNS
    test_table_object._partitions = [
        RdbmsPartition.by_name(
            partition_name="P4",
            partition_count=1,
            partition_position=8,
            subpartition_count=0,
            subpartition_name=None,
            subpartition_names=None,
            subpartition_position=None,
            high_values_csv="TO_DATE(' 2012-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            high_values_python=(numpy.datetime64("2012-04-01T00:00:00"),),
            partition_size=1_000_000,
            num_rows=2,
            high_values_individual=(
                "TO_DATE(' 2012-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            ),
        ),
        RdbmsPartition.by_name(
            partition_name="P3",
            partition_count=1,
            partition_position=3,
            subpartition_count=0,
            subpartition_name=None,
            subpartition_names=None,
            subpartition_position=None,
            high_values_csv="TO_DATE(' 2012-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            high_values_python=(numpy.datetime64("2012-03-01T00:00:00"),),
            partition_size=1_000_000,
            num_rows=2,
            high_values_individual=(
                "TO_DATE(' 2012-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            ),
        ),
        RdbmsPartition.by_name(
            partition_name="P2",
            partition_count=1,
            partition_position=2,
            subpartition_count=0,
            subpartition_name=None,
            subpartition_names=None,
            subpartition_position=None,
            high_values_csv="TO_DATE(' 2012-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            high_values_python=(numpy.datetime64("2012-02-01T00:00:00"),),
            partition_size=1_000_000,
            num_rows=2,
            high_values_individual=(
                "TO_DATE(' 2012-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            ),
        ),
        RdbmsPartition.by_name(
            partition_name="P1",
            partition_count=1,
            partition_position=1,
            subpartition_count=0,
            subpartition_name=None,
            subpartition_names=None,
            subpartition_position=None,
            high_values_csv="TO_DATE(' 2012-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            high_values_python=(numpy.datetime64("2012-01-01T00:00:00"),),
            partition_size=1_000_000,
            num_rows=2,
            high_values_individual=(
                "TO_DATE(' 2012-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
            ),
        ),
    ]
    return test_table_object


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
