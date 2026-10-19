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


@pytest.mark.parametrize(
    "attribute,env_var,value,expected",
    [
        (
            "bigquery_api_endpoint",
            "BIGQUERY_API_ENDPOINT",
            "bigquery-myep.p.googleapis.com",
            "https://bigquery-myep.p.googleapis.com",
        ),
        ("bigquery_api_endpoint", "BIGQUERY_API_ENDPOINT", "", None),
        (
            "bigquery_api_endpoint",
            "BIGQUERY_API_ENDPOINT",
            "https://host/bigquery/v2",
            module_under_test.OrchestrationConfigException,
        ),
        (
            "google_kms_api_endpoint",
            "GOOGLE_KMS_API_ENDPOINT",
            "https://cloudkms-myep.p.googleapis.com",
            "cloudkms-myep.p.googleapis.com",
        ),
        ("google_kms_api_endpoint", "GOOGLE_KMS_API_ENDPOINT", None, None),
        (
            "google_kms_api_endpoint",
            "GOOGLE_KMS_API_ENDPOINT",
            "http://cloudkms-myep.p.googleapis.com",
            module_under_test.OrchestrationConfigException,
        ),
    ],
)
def test_normalise_bigquery_options_api_endpoints(
    bq_config, attribute: str, env_var: str, value, expected
):
    bq_config.backend_distribution = offload_constants.BACKEND_DISTRO_GCP
    setattr(bq_config, attribute, value)
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected, match=env_var):
            module_under_test.normalise_bigquery_options(bq_config)
    else:
        module_under_test.normalise_bigquery_options(bq_config)
        assert getattr(bq_config, attribute) == expected


def test_normalise_bigquery_options_api_endpoints_non_gcp(bq_config):
    """BigQuery/KMS endpoints are not touched for non-GCP backends."""
    bq_config.backend_distribution = offload_constants.BACKEND_DISTRO_SNOWFLAKE
    bq_config.bigquery_api_endpoint = "not-normalised"
    bq_config.google_kms_api_endpoint = "http://not-validated"
    module_under_test.normalise_bigquery_options(bq_config)
    assert bq_config.bigquery_api_endpoint == "not-normalised"
    assert bq_config.google_kms_api_endpoint == "http://not-validated"


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            "storage-myep.p.googleapis.com",
            "https://storage-myep.p.googleapis.com",
        ),
        ("", None),
        (None, None),
        ("https://host/a/path", module_under_test.OrchestrationConfigException),
    ],
)
def test_normalise_filesystem_options_storage_api_endpoint(bq_config, value, expected):
    """GOOGLE_STORAGE_API_ENDPOINT is normalised for all backends because GCS is not BigQuery specific."""
    for backend_distribution in (
        offload_constants.BACKEND_DISTRO_GCP,
        offload_constants.BACKEND_DISTRO_SNOWFLAKE,
    ):
        bq_config.backend_distribution = backend_distribution
        bq_config.google_storage_api_endpoint = value
        if isinstance(expected, type) and issubclass(expected, Exception):
            with pytest.raises(expected, match="GOOGLE_STORAGE_API_ENDPOINT"):
                module_under_test.normalise_filesystem_options(bq_config)
        else:
            module_under_test.normalise_filesystem_options(bq_config)
            assert bq_config.google_storage_api_endpoint == expected
