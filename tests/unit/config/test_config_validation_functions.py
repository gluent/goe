# Copyright 2024 The GOE Authors. All rights reserved.
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
from unittest.mock import Mock

import goe.config.config_validation_functions as module_under_test
from goe.offload import offload_constants

from tests.unit.test_functions import (
    build_mock_options,
    FAKE_ORACLE_BQ_ENV,
)


@pytest.fixture
def bq_config():
    return build_mock_options(FAKE_ORACLE_BQ_ENV)


@pytest.mark.parametrize(
    "input,expected_status",
    [
        ("1.0", True),
        ("1.2", True),
        ("2.1", True),
        ("2.2", True),
        ("1", False),
        ("0", False),
        ("99.1", False),
        ("2/1", False),
        ("a", False),
        ("a.b", False),
    ],
)
def test_normalise_bigquery_options_google_dataproc_batches_version(
    bq_config, input: str, expected_status: bool
):
    bq_config.backend_distribution = offload_constants.BACKEND_DISTRO_GCP
    bq_config.google_dataproc_batches_version = input
    if expected_status:
        module_under_test.normalise_bigquery_options(bq_config), f"For input: {input}"
    else:
        with pytest.raises(Exception) as _:
            module_under_test.normalise_bigquery_options(
                bq_config
            ), f"For input: {input}"


@pytest.mark.parametrize(
    "input,expected_status",
    [
        ("1d", True),
        ("120m", True),
        ("21h", True),
        ("1", False),
        ("22", False),
        ("a", False),
        ("9.1h", False),
        ("2/1", False),
        ("am", False),
        (".h", False),
    ],
)
def test_normalise_bigquery_options_google_dataproc_batches_ttl(
    bq_config, input: str, expected_status: bool
):
    bq_config.backend_distribution = offload_constants.BACKEND_DISTRO_GCP
    bq_config.google_dataproc_batches_ttl = input
    if expected_status:
        module_under_test.normalise_bigquery_options(bq_config), f"For input: {input}"
    else:
        with pytest.raises(Exception) as _:
            module_under_test.normalise_bigquery_options(
                bq_config
            ), f"For input: {input}"
