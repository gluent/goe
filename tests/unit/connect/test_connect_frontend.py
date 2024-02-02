import pytest

from goe.connect.connect_frontend import (
    _oracle_version_supported,
    GOE_MINIMUM_ORACLE_VERSION,
)


@pytest.mark.parametrize(
    "oracle_version,expected_result",
    [
        (GOE_MINIMUM_ORACLE_VERSION, True),
        ("9.2.0.5", False),
        ("10.1.0.2", False),
        ("10.2.0.1.0", True),
        ("11.1.0.7", True),
        ("11.2.0.4", True),
        ("18.9.0", True),
        ("19.6.0.0", True),
        ("21.0.0.0.0", True),
    ],
)
def test__oracle_version_supported(oracle_version, expected_result):
    assert _oracle_version_supported(oracle_version) == expected_result
