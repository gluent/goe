# Copyright 2024 The GOE Authors. All rights reserved.
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

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from goe.offload import offload_constants
from goe.offload.operation import ddl_file as module_under_test
from goe.offload.offload_messages import OffloadMessages
from goe.util.misc_functions import get_temp_path

from tests.unit.test_functions import (
    build_mock_offload_operation,
    build_mock_options,
    FAKE_ORACLE_BQ_ENV,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig


@pytest.fixture(scope="module")
def config() -> "OrchestrationConfig":
    return build_mock_options(FAKE_ORACLE_BQ_ENV)


@pytest.mark.parametrize(
    "schema,table_name",
    [
        ("my_user", "my_table123"),
        ("MY-USER-123", "MY-TABLE"),
    ],
)
def test_generate_ddl_file_path(
    schema: str, table_name: str, config: "OrchestrationConfig"
):
    path = module_under_test.generate_ddl_file_path(schema, table_name, config)
    assert schema in path
    assert table_name in path
    offload_log = FAKE_ORACLE_BQ_ENV["OFFLOAD_LOG"]
    assert path.startswith(offload_log)
    assert path.endswith(".sql")


def test_normalise_ddl_file_auto(config: "OrchestrationConfig"):
    fake_messages = OffloadMessages()
    fake_operation = build_mock_offload_operation()
    fake_operation.ddl_file = offload_constants.DDL_FILE_AUTO
    module_under_test.normalise_ddl_file(fake_operation, config, fake_messages)
    assert isinstance(fake_operation.ddl_file, str)


def test_ddl_file_header():
    h = module_under_test.ddl_file_header()
    assert h
    assert isinstance(h, str)


@pytest.mark.parametrize(
    "path,expect_exception",
    [
        ("/tmp", True),
        ("/tmp/", True),
        ("/tmp/ddl.sql", False),
        # Should fail because "not-a-dir" should not exist.
        ("/tmp/not-a-dir/not-a-file.sql", True),
        # Cloud storage paths will pass as long as the scheme is valid.
        ("gs://bucket/path/ddl.sql", False),
        ("s3://bucket/path/ddl.sql", False),
        ("unknown-scheme://bucket/path/ddl.sql", True),
    ],
)
def test_normalise_ddl_file_path(
    path: str, expect_exception: bool, config: "OrchestrationConfig"
):
    fake_messages = OffloadMessages()
    fake_operation = build_mock_offload_operation()
    fake_operation.ddl_file = path
    if expect_exception:
        with pytest.raises(Exception):
            _ = module_under_test.normalise_ddl_file(
                fake_operation, config, fake_messages
            )
    else:
        # No exception expected.
        _ = module_under_test.normalise_ddl_file(fake_operation, config, fake_messages)


@pytest.mark.parametrize(
    "ddl_list",
    [
        [
            "CREATE TABLE foo (bar INT);",
        ],
        [
            "DROP TABLE foo;",
            "CREATE TABLE foo (bar INT);",
        ],
        [],
    ],
)
def test_write_ddl_to_ddl_file(ddl_list: list, config):
    fake_messages = OffloadMessages()
    ddl_file = get_temp_path(prefix="test_write_ddl_to_ddl_file", suffix=".sql")
    m = mock.mock_open()
    with mock.patch("goe.offload.operation.ddl_file.open", m):
        module_under_test.write_ddl_to_ddl_file(
            ddl_file, ddl_list, config, fake_messages
        )
    fh = m()
    assert fh.write.mock_calls
    write_arg = fh.write.mock_calls[0].args[0]
    # Check the header is included in the write call.
    assert module_under_test.DDL_FILE_HEADER in write_arg
    # Check all lines of the DDL are in the write call.
    assert all(_ in write_arg for _ in ddl_list)
