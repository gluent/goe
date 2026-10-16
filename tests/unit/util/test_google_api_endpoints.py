# Copyright 2026 The GOE Authors. All rights reserved.
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

import logging

import pytest

from goe.util.google_api_endpoints import (
    normalise_grpc_api_endpoint,
    normalise_rest_api_endpoint,
)


@pytest.mark.parametrize(
    "input,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("bigquery-myep.p.googleapis.com", "https://bigquery-myep.p.googleapis.com"),
        (
            "  bigquery-myep.p.googleapis.com  ",
            "https://bigquery-myep.p.googleapis.com",
        ),
        ("bigquery-myep.p.googleapis.com/", "https://bigquery-myep.p.googleapis.com"),
        (
            "https://bigquery-myep.p.googleapis.com",
            "https://bigquery-myep.p.googleapis.com",
        ),
        (
            "https://bigquery-myep.p.googleapis.com/",
            "https://bigquery-myep.p.googleapis.com",
        ),
        (
            "HTTPS://bigquery-myep.p.googleapis.com",
            "https://bigquery-myep.p.googleapis.com",
        ),
        ("http://localhost:9050", "http://localhost:9050"),
        (
            "bigquery-myep.p.googleapis.com:8443",
            "https://bigquery-myep.p.googleapis.com:8443",
        ),
        (
            "https://my-bq-proxy.internal.corp:8443/",
            "https://my-bq-proxy.internal.corp:8443",
        ),
        ("10.1.2.3", "https://10.1.2.3"),
        ("[::1]:8443", "https://[::1]:8443"),
    ],
)
def test_normalise_rest_api_endpoint(input, expected):
    assert normalise_rest_api_endpoint(input) == expected
    # Normalisation must be idempotent.
    assert normalise_rest_api_endpoint(expected) == expected


@pytest.mark.parametrize(
    "input",
    [
        "ftp://host",
        "grpc://host",
        "https://host/bigquery/v2",
        "host/path",
        "https://host?x=1",
        "https://host#frag",
        "https://user:pw@host",
        "https://user@host",
        "https://host:abc",
        "https://host:99999",
        "host:",
        "https://",
        "://host",
    ],
)
def test_normalise_rest_api_endpoint_invalid(input):
    with pytest.raises(ValueError):
        normalise_rest_api_endpoint(input)


def test_normalise_rest_api_endpoint_http_warning(caplog):
    with caplog.at_level(logging.WARNING):
        normalise_rest_api_endpoint("http://localhost:9050")
    assert "cleartext" in caplog.text

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        normalise_rest_api_endpoint("https://localhost:9050")
    assert "cleartext" not in caplog.text


@pytest.mark.parametrize(
    "input,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("https://cloudkms-myep.p.googleapis.com", "cloudkms-myep.p.googleapis.com"),
        (
            "https://cloudkms-myep.p.googleapis.com:8443",
            "cloudkms-myep.p.googleapis.com:8443",
        ),
        ("cloudkms-myep.p.googleapis.com", "cloudkms-myep.p.googleapis.com"),
        ("cloudkms-myep.p.googleapis.com:443", "cloudkms-myep.p.googleapis.com:443"),
        ("cloudkms-myep.p.googleapis.com/", "cloudkms-myep.p.googleapis.com"),
        ("  cloudkms-myep.p.googleapis.com  ", "cloudkms-myep.p.googleapis.com"),
        ("[::1]:8443", "[::1]:8443"),
    ],
)
def test_normalise_grpc_api_endpoint(input, expected):
    assert normalise_grpc_api_endpoint(input) == expected
    # Normalisation must be idempotent.
    assert normalise_grpc_api_endpoint(expected) == expected


@pytest.mark.parametrize(
    "input",
    [
        "http://cloudkms-myep.p.googleapis.com",
        "http://cloudkms-myep.p.googleapis.com:8443",
        "grpc://host",
        "host/path",
        "https://host/path",
        "host:abc",
        "https://user:pw@host",
        "https://host?x=1",
        "https://",
    ],
)
def test_normalise_grpc_api_endpoint_invalid(input):
    with pytest.raises(ValueError):
        normalise_grpc_api_endpoint(input)
