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


import pytest

from goe.offload import option_validation as module_under_test


@pytest.mark.parametrize(
    "input,expect_exception",
    [
        ("s", True),
        (None, True),
        (-1, True),
        (0, True),
        (1.1, True),
        (0.1, True),
        (123456789012345.1, True),
        (1, False),
        (123456789012345, False),
    ],
)
def test_check_opt_is_posint(input: str, expect_exception: bool):
    if expect_exception:
        with pytest.raises(Exception):
            _ = module_under_test.check_opt_is_posint("fake-option", input)
    else:
        output = module_under_test.check_opt_is_posint("fake-option", input)
        assert output == input
