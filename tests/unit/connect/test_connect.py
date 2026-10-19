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

from pathlib import Path

import pytest

from goe.connect.connect import (
    _os_release_file_exists,
    _os_version_from_file_content,
    dict_from_environment_file,
    OS_RELEASE_FILE_DEBIAN,
    OS_RELEASE_FILE_REDHAT,
    OS_RELEASE_FILE_SUSE,
    upgrade_environment_file,
)

BIGQUERY_TEMPLATE_FILE = (
    Path(__file__).resolve().parents[3]
    / "templates"
    / "conf"
    / "offload.env.template.bigquery"
)


EXAMPLE_REDHAT_RELEASE_CONTENT = "CentOS Linux release 1.2.1234 (Core)\n"

EXAMPLE_SUSE_RELEASE_CONTENT = """openSUSE 42.2 (x86_64)
VERSION = 42.2
CODENAME = SomeName
"""
EXAMPLE_OS_RELEASE_CONTENT1 = """PRETTY_NAME="Debian GNU/Linux wibble"
NAME="Debian GNU/Linux wibble"
VERSION_CODENAME=wibble
ID=debian
HOME_URL="https://this/that"
SUPPORT_URL="https://this/that"
BUG_REPORT_URL="https://this/that"
"""

EXAMPLE_OS_RELEASE_CONTENT2 = """PRETTY_NAME="Debian GNU/Linux 99 (wibble)"
NAME="Debian GNU/Linux"
VERSION_ID="99"
VERSION="99 (wibble)"
VERSION_CODENAME=wibble
ID=debian
HOME_URL="https://www.this.org/"
SUPPORT_URL="https://www.this.org/support"
BUG_REPORT_URL="https://bugs.this.org/"
"""


def test__os_release_file_exists():
    assert isinstance(_os_release_file_exists(), (str, type(None)))


@pytest.mark.parametrize(
    "path,content,expected_version",
    [
        (
            OS_RELEASE_FILE_DEBIAN,
            EXAMPLE_OS_RELEASE_CONTENT1,
            "Debian GNU/Linux wibble",
        ),
        (
            OS_RELEASE_FILE_DEBIAN,
            EXAMPLE_OS_RELEASE_CONTENT2,
            "Debian GNU/Linux 99 (wibble)",
        ),
        (
            OS_RELEASE_FILE_REDHAT,
            EXAMPLE_REDHAT_RELEASE_CONTENT,
            EXAMPLE_REDHAT_RELEASE_CONTENT,
        ),
        (
            OS_RELEASE_FILE_SUSE,
            EXAMPLE_SUSE_RELEASE_CONTENT,
            "openSUSE 42.2 (x86_64) (VERSION = 42.2, CODENAME = SomeName)",
        ),
    ],
)
def test__os_version_from_file_content(path, content, expected_version):
    assert _os_version_from_file_content(path, content) == expected_version


def test_upgrade_environment_file(tmpdir):
    """Test that upgrade_environment_file correctly appends missing variables.

    Ensures that missing variables from a template are successfully appended
    to an existing environment configuration file without stripping keys.
    """
    env_file = tmpdir.join("offload.env")
    env_file.write("FRONTEND_DISTRIBUTION=ORACLE\n")

    template_file = tmpdir.join("offload.env.template")
    template_file.write("FRONTEND_DISTRIBUTION=ORACLE\nDATA_SAMPLE_PARALLELISM=0\n")

    upgrade_environment_file(str(env_file), str(template_file))

    content = env_file.read()
    assert "DATA_SAMPLE_PARALLELISM=0\n" in content


def test_bigquery_template_google_api_endpoints():
    """Check custom Google API endpoint keys in the BigQuery template as seen by connect.

    CLOUDSDK_API_ENDPOINT_OVERRIDES_DATAPROC must NOT be detected as a template key, otherwise
    connect --upgrade-environment-file would append it with an empty value which breaks gcloud.
    """
    template = dict_from_environment_file(str(BIGQUERY_TEMPLATE_FILE))
    for key in (
        "BIGQUERY_API_ENDPOINT",
        "GOOGLE_STORAGE_API_ENDPOINT",
        "GOOGLE_KMS_API_ENDPOINT",
    ):
        assert key in template, f"{key} missing from {BIGQUERY_TEMPLATE_FILE.name}"
        assert template[key] == "", f"{key} should have an empty default value"
    assert "CLOUDSDK_API_ENDPOINT_OVERRIDES_DATAPROC" not in template
    # The guidance must still be present in the template as a comment.
    assert "# export CLOUDSDK_API_ENDPOINT_OVERRIDES_DATAPROC=" in (
        BIGQUERY_TEMPLATE_FILE.read_text()
    )


def test_upgrade_environment_file_google_api_endpoints(tmpdir):
    """upgrade_environment_file appends the new endpoint keys but not the gcloud Dataproc override."""
    env_file = tmpdir.join("offload.env")
    env_file.write("BACKEND_DISTRIBUTION=GCP\n")

    upgrade_environment_file(str(env_file), str(BIGQUERY_TEMPLATE_FILE))

    content = env_file.read()
    assert "BIGQUERY_API_ENDPOINT=\n" in content
    assert "GOOGLE_STORAGE_API_ENDPOINT=\n" in content
    assert "GOOGLE_KMS_API_ENDPOINT=\n" in content
    assert "CLOUDSDK_API_ENDPOINT_OVERRIDES_DATAPROC" not in content
