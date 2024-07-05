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

import goe.offload.offload_transport_functions as module_under_test
from goe.util.misc_functions import get_os_username, LINUX_FILE_NAME_LENGTH_LIMIT

from tests.unit.test_functions import (
    build_mock_options,
    FAKE_ORACLE_BQ_ENV,
)


def test_running_as_same_user_and_host():
    assert module_under_test.running_as_same_user_and_host(
        get_os_username(), "localhost"
    )
    assert not module_under_test.running_as_same_user_and_host(
        get_os_username(), "other-host"
    )
    assert not module_under_test.running_as_same_user_and_host(
        "other-user", "localhost"
    )


def test_offload_transport_file_name():
    std_prefix = "db_name.table_name"
    assert module_under_test.offload_transport_file_name(std_prefix) == std_prefix
    assert (
        len(module_under_test.offload_transport_file_name(std_prefix, extension=".dat"))
        < LINUX_FILE_NAME_LENGTH_LIMIT
    )

    long_prefix = "db_name".ljust(128, "x") + "." + "table_name".ljust(128, "x")
    # long_prefix would need to be truncated
    assert module_under_test.offload_transport_file_name(long_prefix) != long_prefix
    assert module_under_test.offload_transport_file_name(long_prefix).startswith(
        long_prefix[:200]
    )
    # long_prefix would need to be truncated but extension still honoured
    assert module_under_test.offload_transport_file_name(long_prefix).startswith(
        long_prefix[:200]
    )
    assert not module_under_test.offload_transport_file_name(
        long_prefix, extension=""
    ).endswith(".dat")
    assert (
        len(module_under_test.offload_transport_file_name(long_prefix, extension=""))
        == LINUX_FILE_NAME_LENGTH_LIMIT
    )
    assert module_under_test.offload_transport_file_name(
        long_prefix, extension=".dat"
    ).endswith(".dat")
    assert (
        len(
            module_under_test.offload_transport_file_name(long_prefix, extension=".dat")
        )
        == LINUX_FILE_NAME_LENGTH_LIMIT
    )
    with pytest.raises(AssertionError) as _:
        module_under_test.offload_transport_file_name("file/name")
    with pytest.raises(AssertionError) as _:
        module_under_test.offload_transport_file_name(123)
    with pytest.raises(AssertionError) as _:
        module_under_test.offload_transport_file_name(None)
    with pytest.raises(AssertionError) as _:
        module_under_test.offload_transport_file_name("file_name", extension=None)
    with pytest.raises(AssertionError) as _:
        module_under_test.offload_transport_file_name("file_name", extension=123)


@pytest.mark.parametrize(
    "inputs,expected_result",
    [
        # Simple range of 1-10 with assorted parallelism
        ((1, 10, 1), [(1, 11)]),
        ((1, 10, 2), [(1, 6), (6, 11)]),
        ((1, 10, 5), [(1, 3), (3, 5), (5, 7), (7, 9), (9, 11)]),
        # Range with min == max and assorted parallelism
        ((1, 1, 1), [(1, 2)]),
        ((1, 1, 2), [(1, 1.5), (1.5, 2)]),
        ((0, 0, 1), [(0, 1)]),
        ((0, 0, 4), [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1)]),
        # Range smaller than parallelism - what should we do?
        ((1, 2, 4), [(1, 1.5), (1.5, 2), (2, 2.5), (2.5, 3)]),
        # Bigger inputs that overflow float
        (
            (9999999999999991, 9999999999999999, 1),
            [(9999999999999991, 10000000000000000)],
        ),
        (
            (int(("9" * 30) + "000"), int(("9" * 30) + "999"), 4),
            [
                (int(("9" * 30) + "000"), int(("9" * 30) + "250")),
                (int(("9" * 30) + "250"), int(("9" * 30) + "500")),
                (int(("9" * 30) + "500"), int(("9" * 30) + "750")),
                (int(("9" * 30) + "750"), int("1" + ("0" * 30) + "000")),
            ],
        ),
    ],
)
def test_split_ranges_for_id_range(inputs, expected_result):
    result = module_under_test.split_ranges_for_id_range(*inputs)
    assert (
        result == expected_result
    ), f"Inputs: min={inputs[0]}, max={inputs[1]}, parallel={inputs[2]}"
    for i in range(inputs[0], inputs[1] + 1):
        # Assert that for each integer between min/max there is only a single capturing range
        capturing_ranges = [_ for _ in result if _[0] <= i < _[1]]
        assert (
            len(capturing_ranges) == 1
        ), f"Value {i} in capturing ranges = {capturing_ranges}"


@pytest.mark.parametrize(
    "input_list, parallelism, round_robin, csv, expected_result",
    [
        # Round robin, CSV=False
        ([8, 4, 1, 3, 6, 9], 1, True, False, [[8, 4, 1, 3, 6, 9]]),
        ([8, 4, 1, 3, 6, 9], 2, True, False, [[8, 1, 6], [4, 3, 9]]),
        ([8, 4, 1, 3, 6, 9], 3, True, False, [[8, 3], [4, 6], [1, 9]]),
        ([8, 4, 1, 3, 6], 1, True, False, [[8, 4, 1, 3, 6]]),
        ([8, 4, 1, 3, 6], 2, True, False, [[8, 1, 6], [4, 3]]),
        ([8, 4, 1, 3, 6], 3, True, False, [[8, 3], [4, 6], [1]]),
        ([8], 1, True, False, [[8]]),
        ([8], 2, True, False, [[8], []]),
        ([8], 3, True, False, [[8], [], []]),
        # Round robin, CSV=True
        ([8, 4, 1, 3, 6, 9], 1, True, True, ["8,4,1,3,6,9"]),
        ([8, 4, 1, 3, 6, 9], 2, True, True, ["8,1,6", "4,3,9"]),
        ([8, 4, 1, 3, 6, 9], 3, True, True, ["8,3", "4,6", "1,9"]),
        ([8, 4, 1, 3, 6], 1, True, True, ["8,4,1,3,6"]),
        ([8, 4, 1, 3, 6], 2, True, True, ["8,1,6", "4,3"]),
        ([8, 4, 1, 3, 6], 3, True, True, ["8,3", "4,6", "1"]),
        ([8], 1, True, True, ["8"]),
        ([8], 2, True, True, ["8", None]),
        ([8], 3, True, True, ["8", None, None]),
        # Contiguous, CSV=False
        ([8, 4, 1, 3, 6, 9], 1, False, False, [[8, 4, 1, 3, 6, 9]]),
        ([8, 4, 1, 3, 6, 9], 2, False, False, [[8, 4, 1], [3, 6, 9]]),
        ([8, 4, 1, 3, 6, 9], 3, False, False, [[8, 4], [1, 3], [6, 9]]),
        ([8, 4, 1, 3, 6], 1, False, False, [[8, 4, 1, 3, 6]]),
        ([8, 4, 1, 3, 6], 2, False, False, [[8, 4, 1], [3, 6]]),
        ([8, 4, 1, 3, 6], 3, False, False, [[8, 4], [1, 3], [6]]),
        ([8], 1, False, False, [[8]]),
        ([8], 2, False, False, [[8], []]),
        ([8], 3, False, False, [[8], [], []]),
    ],
)
def test_split_lists_for_id_list(
    input_list, parallelism, round_robin, csv, expected_result
):
    result = module_under_test.split_lists_for_id_list(
        input_list, parallelism, round_robin=round_robin, as_csvs=csv
    )
    assert (
        result == expected_result
    ), f"Input: ids={input_list}, parallel={parallelism}, round_robin={round_robin}, csv={csv}"


def test_get_local_staging_path_local():
    target_owner = "owner"
    table_name = "username"
    extension = ".ext"
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    config.log_path = "/this/that/tother"
    f = module_under_test.get_local_staging_path(
        target_owner, table_name, config, extension
    )
    assert target_owner in f
    assert table_name in f
    assert config.log_path in f
    assert "/tmp" not in f
    assert f.endswith(extension)


def test_get_local_staging_path_gcs():
    target_owner = "owner"
    table_name = "username"
    extension = ".ext"
    config = build_mock_options(FAKE_ORACLE_BQ_ENV)
    config.log_path = "gs://bucket/this/that/tother"
    f = module_under_test.get_local_staging_path(
        target_owner, table_name, config, extension
    )
    assert target_owner in f
    assert table_name in f
    assert config.log_path not in f
    assert "/tmp" in f
    assert f.endswith(extension)
