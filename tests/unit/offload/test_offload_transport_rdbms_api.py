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

""" TestOffloadTransportRdbmsApi: Unit tests for each Offload Transport RDBMS API.
"""
import pytest
from unittest import mock

from goe.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_source_data import (
    OffloadSourcePartition,
    OffloadSourcePartitions,
)
from goe.offload.offload_transport_rdbms_api import (
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
)
from goe.offload.predicate_offload import GenericPredicate
from tests.unit.test_functions import (
    build_mock_options,
    build_fake_oracle_table,
    build_fake_oracle_subpartitioned_table,
    FAKE_MSSQL_ENV,
    FAKE_NETEZZA_ENV,
    FAKE_ORACLE_ENV,
    FAKE_TERADATA_ENV,
    FAKE_ORACLE_PARTITIONS,
    FAKE_ORACLE_LIST_RANGE_PARTITIONS,
)


@pytest.fixture
def oracle_config():
    return build_mock_options(FAKE_ORACLE_ENV)


@pytest.fixture
def messages():
    return OffloadMessages()


@pytest.fixture
def fake_oracle_table(oracle_config, messages):
    return build_fake_oracle_table(oracle_config, messages)


@pytest.fixture
def fake_oracle_subpartitioned_table(oracle_config, messages):
    return build_fake_oracle_subpartitioned_table(oracle_config, messages)


def non_connecting_tests(api):
    def is_list_of_strings(v):
        assert isinstance(v, list)
        if v:
            assert isinstance(v[0], str)

    assert isinstance(api.generate_transport_action(), str)
    assert isinstance(api.get_rdbms_canary_query(), str)
    is_list_of_strings(api.sqoop_rdbms_specific_jvm_overrides([]))
    is_list_of_strings(api.sqoop_rdbms_specific_jvm_overrides(["abc=123"]))
    is_list_of_strings(api.sqoop_rdbms_specific_jvm_table_options(None))
    is_list_of_strings(api.sqoop_rdbms_specific_options())
    is_list_of_strings(api.sqoop_rdbms_specific_table_options("owner", "table-name"))
    try:
        assert isinstance(api.get_transport_row_source_query_hint_block(), str)
    except NotImplementedError:
        pass
    assert isinstance(api.jdbc_url(), str)
    assert isinstance(api.jdbc_driver_name(), (str, type(None)))


def test_mssql_ot_rdbms_api(messages):
    """Simple check that we can instantiate the MSSQL API"""
    config = build_mock_options(FAKE_MSSQL_ENV)
    rdbms_owner = "SH_TEST"
    rdbms_table = "SALES"
    api = offload_transport_rdbms_api_factory(
        rdbms_owner, rdbms_table, config, messages, dry_run=True
    )
    non_connecting_tests(api)


def test_netezza_ot_rdbms_api(messages):
    """Simple check that we can instantiate the Netezza API"""
    config = build_mock_options(FAKE_NETEZZA_ENV)
    rdbms_owner = "SH_TEST"
    rdbms_table = "SALES"
    api = offload_transport_rdbms_api_factory(
        rdbms_owner, rdbms_table, config, messages, dry_run=True
    )
    non_connecting_tests(api)


def test_oracle_ot_rdbms_api(oracle_config, messages):
    """Simple check that we can instantiate the Oracle API"""
    oracle_config = build_mock_options(FAKE_ORACLE_ENV)
    rdbms_owner = "SH_TEST"
    rdbms_table = "SALES"
    api = offload_transport_rdbms_api_factory(
        rdbms_owner, rdbms_table, oracle_config, messages, dry_run=True
    )
    non_connecting_tests(api)


def test_teradata_ot_rdbms_api(messages):
    """Simple check that we can instantiate the Teradata API"""
    config = build_mock_options(FAKE_TERADATA_ENV)
    rdbms_owner = "SH_TEST"
    rdbms_table = "SALES"
    api = offload_transport_rdbms_api_factory(
        rdbms_owner, rdbms_table, config, messages, dry_run=True
    )
    non_connecting_tests(api)


def offload_partitions_from_rdbms_partitions(rdbms_partitions):
    partitions = [
        OffloadSourcePartition(
            _.partition_name,
            _.high_values_csv,
            _.high_values_python,
            _.high_values_individual,
            _.partition_size,
            _.num_rows,
            None,  # common_partition_literal
            _.subpartition_names,
        )
        for _ in rdbms_partitions
    ]
    return OffloadSourcePartitions(partitions)


@pytest.mark.parametrize(
    "parallelism,partition_type,offload_by_subpartition,partition_chunk,expected_split_type,expected_parallelism",
    [
        (
            1,
            None,
            False,
            None,
            # Non-partitioned tables should always split by ROWID range.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            1,
        ),
        (
            10,
            None,
            False,
            None,
            # Non-partitioned tables should always split by ROWID range.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            10,
        ),
    ],
)
def test_get_transport_split_type_oracle_heap(
    oracle_config,
    messages,
    fake_oracle_table,
    parallelism,
    partition_type,
    offload_by_subpartition,
    partition_chunk,
    expected_split_type,
    expected_parallelism,
):
    api = offload_transport_rdbms_api_factory(
        fake_oracle_table.owner,
        fake_oracle_table.table_name,
        oracle_config,
        messages,
        dry_run=True,
    )
    predicate_offload_clause = None

    split_return = api.get_transport_split_type(
        partition_chunk,
        fake_oracle_table,
        parallelism,
        partition_type,
        offload_by_subpartition,
        predicate_offload_clause,
    )

    assert isinstance(split_return, tuple)
    assert split_return[0] == expected_split_type
    assert split_return[1] == expected_parallelism


@pytest.mark.parametrize(
    "parallelism,partition_chunk,expected_split_type,expected_parallelism",
    [
        # Offload 2 partitions with parallelism of 2.
        (
            2,
            offload_partitions_from_rdbms_partitions(FAKE_ORACLE_PARTITIONS[:2]),
            # 2 partitions with parallel 2 should split by partition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            2,
        ),
        # Offload 2 partitions with parallelism of 1.
        (
            1,
            offload_partitions_from_rdbms_partitions(FAKE_ORACLE_PARTITIONS[:2]),
            # 2 partitions with parallel 1 should split by partition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            1,
        ),
        # Offload 2 partitions with parallelism of 3.
        (
            3,
            offload_partitions_from_rdbms_partitions(FAKE_ORACLE_PARTITIONS[:2]),
            # 2 partitions with parallel 3 should split by ROWID range.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            3,
        ),
        # Offload 1 partition with parallelism of 2.
        (
            2,
            offload_partitions_from_rdbms_partitions(FAKE_ORACLE_PARTITIONS[:1]),
            # 1 partitions should split by ROWID range.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            2,
        ),
    ],
)
def test_get_transport_split_type_oracle_partitioned(
    oracle_config,
    messages,
    fake_oracle_table,
    parallelism,
    partition_chunk,
    expected_split_type,
    expected_parallelism,
):
    api = offload_transport_rdbms_api_factory(
        fake_oracle_table.owner,
        fake_oracle_table.table_name,
        oracle_config,
        messages,
        dry_run=True,
    )
    predicate_offload_clause = None
    offload_by_subpartition = False

    split_return = api.get_transport_split_type(
        partition_chunk,
        fake_oracle_table,
        parallelism,
        fake_oracle_table.partition_type,
        offload_by_subpartition,
        predicate_offload_clause,
    )

    assert isinstance(split_return, tuple)
    assert split_return[0] == expected_split_type
    assert split_return[1] == expected_parallelism


@pytest.mark.parametrize(
    "parallelism,partition_chunk,offload_by_subpartition,expected_split_type,expected_parallelism",
    [
        # Offload 1 top level partition with parallelism of 2.
        (
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:1]
            ),
            False,
            # 4 subpartitions with parallel 2 should split by subpartition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
            2,
        ),
        # Offload 1 top level partition with parallelism of 5, greater than 4 subpartitions.
        (
            5,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:1]
            ),
            False,
            # 4 subpartitions with parallel 2 should split by subpartition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            5,
        ),
        # Offload 2 top level partitions with parallelism of 2.
        (
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:2]
            ),
            False,
            # 2 is enough for top-level partitions, no need to split by subpartition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            2,
        ),
        # Offload 2 top level partitions with parallelism of 2.
        (
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:2]
            ),
            True,
            # 2 is enough for top-level partitions but we asked for subpartition therefore split by subpartition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
            2,
        ),
    ],
)
def test_get_transport_split_type_oracle_subpartitioned(
    oracle_config,
    messages,
    fake_oracle_subpartitioned_table,
    parallelism,
    partition_chunk,
    offload_by_subpartition,
    expected_split_type,
    expected_parallelism,
):
    api = offload_transport_rdbms_api_factory(
        fake_oracle_subpartitioned_table.owner,
        fake_oracle_subpartitioned_table.table_name,
        oracle_config,
        messages,
        dry_run=True,
    )
    predicate_offload_clause = None

    split_return = api.get_transport_split_type(
        partition_chunk,
        fake_oracle_subpartitioned_table,
        parallelism,
        fake_oracle_subpartitioned_table.partition_type,
        offload_by_subpartition,
        predicate_offload_clause,
    )

    assert isinstance(split_return, tuple)
    assert split_return[0] == expected_split_type
    assert split_return[1] == expected_parallelism


@pytest.mark.parametrize(
    "parallelism,pk_cols,offload_by_subpartition,partition_chunk,expected_split_type,expected_parallelism",
    [
        (
            2,
            [
                "ID",
            ],
            False,
            None,
            # PBO with no known partitions should always split by ID range if there's a suitable PK.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
            2,
        ),
        (
            2,
            None,
            False,
            None,
            # PBO with no known partitions should always split by ID range if there's NO suitable PK.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
            2,
        ),
        (
            3,
            [
                "ID",
            ],
            False,
            offload_partitions_from_rdbms_partitions(FAKE_ORACLE_PARTITIONS[:2]),
            # PBO with 2 known partitions and parallel 3 should split by ROWID range.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
            3,
        ),
        (
            2,
            [
                "ID",
            ],
            False,
            offload_partitions_from_rdbms_partitions(FAKE_ORACLE_PARTITIONS[:2]),
            # PBO with 2 known partitions and parallel 2 should split by partition.
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            2,
        ),
    ],
)
def test_get_transport_split_type_oracle_pbo(
    oracle_config,
    messages,
    fake_oracle_table,
    parallelism,
    pk_cols,
    offload_by_subpartition,
    partition_chunk,
    expected_split_type,
    expected_parallelism,
):
    fake_oracle_table._primary_key_columns = pk_cols
    api = offload_transport_rdbms_api_factory(
        fake_oracle_table.owner,
        fake_oracle_table.table_name,
        oracle_config,
        messages,
        dry_run=True,
    )
    predicate_offload_clause = GenericPredicate("column(ID) > numeric(1)")
    offload_by_subpartition = False

    split_return = api.get_transport_split_type(
        partition_chunk,
        fake_oracle_table,
        parallelism,
        fake_oracle_table.partition_type,
        offload_by_subpartition,
        predicate_offload_clause,
    )

    assert isinstance(split_return, tuple)
    assert split_return[0] == expected_split_type
    assert split_return[1] == expected_parallelism


@pytest.mark.parametrize(
    "partition_by_prm,parallelism,partition_chunk,pad",
    [
        (
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:2]
            ),
            None,
        ),
        (
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:2]
            ),
            12,
        ),
        (TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT, 2, None, None),
        (TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT, 4, None, None),
        (TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE, 2, None, None),
        (
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_ID_RANGE,
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:1]
            ),
            None,
        ),
        (TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE, 2, None, None),
        (
            TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
            2,
            offload_partitions_from_rdbms_partitions(
                FAKE_ORACLE_LIST_RANGE_PARTITIONS[:2]
            ),
            None,
        ),
        (TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD, 2, None, None),
    ],
)
def test_get_transport_row_source_query_oracle(
    oracle_config,
    messages,
    fake_oracle_table,
    partition_by_prm: str,
    parallelism: int,
    partition_chunk,
    pad: int,
):
    api = offload_transport_rdbms_api_factory(
        fake_oracle_table.owner,
        fake_oracle_table.table_name,
        oracle_config,
        messages,
        dry_run=True,
    )
    # Prevent DB connection by patching the scn method.
    api.get_rdbms_scn = lambda: 123

    consistent_read = True
    offload_by_subpartition = False
    mod_column = "SOME_MOD_COLUMN"
    predicate_offload_clause = fake_oracle_table.predicate_to_where_clause(
        GenericPredicate("column(ID) > numeric(1)")
    )
    query = api.get_transport_row_source_query(
        partition_by_prm,
        fake_oracle_table,
        consistent_read,
        parallelism,
        offload_by_subpartition,
        mod_column,
        predicate_offload_clause,
        partition_chunk,
        pad=pad,
        id_col_min=0,
        id_col_max=101,
    )
    if partition_by_prm in (
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_NATIVE_RANGE,
        TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_MOD,
    ):
        expected_union_alls = 0
    else:
        expected_union_alls = parallelism - 1
    assert query.count("UNION ALL") == expected_union_alls
