# Copyright 2016 The GOE Authors. All rights reserved.
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

from goe.util.goe_version import GOEVersion


def test_goe_version():
    assert GOEVersion("3.0") == GOEVersion("3.0.0")
    assert GOEVersion("3.0") == GOEVersion("3.0")
    assert GOEVersion("3.0") == GOEVersion("3")

    assert GOEVersion("3.0") != GOEVersion("3.0.1")
    assert GOEVersion("3.1") != GOEVersion("3.0")
    assert GOEVersion("3.1") != GOEVersion("3")

    assert GOEVersion("3.1") > GOEVersion("3.0.1")
    assert GOEVersion("3.1") > GOEVersion("3.0")
    assert GOEVersion("3.1") > GOEVersion("3")

    assert GOEVersion("3.0") < GOEVersion("3.0.1")
    assert GOEVersion("3.0") < GOEVersion("3.1")
    assert GOEVersion("3.0") < GOEVersion("4")

    assert GOEVersion("3.0.0") == GOEVersion("3.0.0")
    assert GOEVersion("3.0.0") == GOEVersion("3.0")
    assert GOEVersion("3.0.0") == GOEVersion("3")

    assert GOEVersion("3.0.0") != GOEVersion("3.0.1")
    assert GOEVersion("3.0.1") != GOEVersion("3.0")
    assert GOEVersion("3.0.1") != GOEVersion("3")

    assert GOEVersion("3.0.2") > GOEVersion("3.0.1")
    assert GOEVersion("3.0.1") > GOEVersion("3.0")
    assert GOEVersion("3.0.1") > GOEVersion("3")

    assert GOEVersion("3.0.0") < GOEVersion("3.0.1")
    assert GOEVersion("3.0.0") < GOEVersion("3.1")
    assert GOEVersion("3.0.0") < GOEVersion("4")


def test_goe_version_with_alpha():
    assert GOEVersion("3.1.2-RC") == GOEVersion("3.1.2-RC")
    assert GOEVersion("3.1.2-DEV") == GOEVersion("3.1.2-DEV")
    assert GOEVersion("3.1.2-RC") != GOEVersion("3.1.2-DEV")

    assert GOEVersion("3.1.0-RC") > GOEVersion("3.0.0-RC")
    assert GOEVersion("3.1-RC") > GOEVersion("3.0-RC")
    assert GOEVersion("3.1.0-RC") > GOEVersion("3.0.0-DEV")
