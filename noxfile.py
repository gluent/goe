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

import nox


PYTHON_VERSIONS = ["3.7", "3.8", "3.9", "3.10", "3.11"]


def _setup_session_requirements(session, extra_packages=[]):
    session.install("--upgrade", "pip", "wheel")
    session.install("-e", ".[dev]")

    if extra_packages:
        session.install(*extra_packages)


@nox.session(python=PYTHON_VERSIONS, venv_backend="venv")
def unit(session):
    _setup_session_requirements(session)
    session.run("pytest", "tests/unit")
