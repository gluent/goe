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

import os
from unittest import mock

import pytest

from goe.util import goe_log_fh
from goe.util.goe_log_fh import GOELogFileHandle, GOOGLE_STORAGE_API_ENDPOINT_ENV


def _get_fs_kwargs(path: str, env: dict) -> dict:
    """Call GOELogFileHandle._get_fs() with fsspec.filesystem patched and return the kwargs used."""
    # Nested rather than parenthesised context managers for Python 3.8 compatibility.
    with mock.patch.dict(os.environ, env):
        with mock.patch.object(goe_log_fh.fsspec, "filesystem") as mock_filesystem:
            # Bypass __init__ so we don't open a file.
            fh = GOELogFileHandle.__new__(GOELogFileHandle)
            fh._get_fs(path)
    mock_filesystem.assert_called_once()
    return mock_filesystem.call_args


@pytest.mark.parametrize(
    "endpoint,expected",
    [
        ("storage-myep.p.googleapis.com", "https://storage-myep.p.googleapis.com"),
        (
            "https://storage-myep.p.googleapis.com/",
            "https://storage-myep.p.googleapis.com",
        ),
        (
            "https://my-gcs-proxy.internal.corp:8443",
            "https://my-gcs-proxy.internal.corp:8443",
        ),
    ],
)
def test_get_fs_gcs_custom_endpoint(endpoint, expected):
    call = _get_fs_kwargs(
        "gs://bucket/logs/a.log", {GOOGLE_STORAGE_API_ENDPOINT_ENV: endpoint}
    )
    assert call.args == ("gs",)
    assert call.kwargs["endpoint_url"] == expected


@pytest.mark.parametrize("endpoint", ["", "   "])
def test_get_fs_gcs_empty_endpoint(endpoint):
    call = _get_fs_kwargs(
        "gs://bucket/logs/a.log", {GOOGLE_STORAGE_API_ENDPOINT_ENV: endpoint}
    )
    assert call.args == ("gs",)
    assert "endpoint_url" not in call.kwargs


def test_get_fs_gcs_no_endpoint():
    with mock.patch.dict(os.environ):
        os.environ.pop(GOOGLE_STORAGE_API_ENDPOINT_ENV, None)
        call = _get_fs_kwargs("gs://bucket/logs/a.log", {})
    assert call.args == ("gs",)
    assert "endpoint_url" not in call.kwargs


def test_get_fs_local_path_ignores_endpoint():
    call = _get_fs_kwargs(
        "/tmp/logs/a.log",
        {GOOGLE_STORAGE_API_ENDPOINT_ENV: "storage-myep.p.googleapis.com"},
    )
    assert call.args == ("file",)
    assert "endpoint_url" not in call.kwargs


def test_get_fs_gcs_invalid_endpoint():
    with pytest.raises(ValueError, match=GOOGLE_STORAGE_API_ENDPOINT_ENV):
        _get_fs_kwargs(
            "gs://bucket/logs/a.log",
            {GOOGLE_STORAGE_API_ENDPOINT_ENV: "https://host/a/path"},
        )
