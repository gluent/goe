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
