import pytest

from goe.connect.connect import (
    _os_release_file_exists,
    _os_version_from_file_content,
    OS_RELEASE_FILE_DEBIAN,
    OS_RELEASE_FILE_REDHAT,
    OS_RELEASE_FILE_SUSE,
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
