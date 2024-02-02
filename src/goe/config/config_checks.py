#! /usr/bin/env python3

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

""" Function calls checking orchestration configuration.
"""

import os
import sys


def check_cli_path():
    """Check OFFLOAD_HOME in top level command wrappers
    This should be imported and called as the first GOE import, for example:

    import os

    from goe.config.config_checks import check_cli_path
    check_cli_path()

    import goe.other.libraries.if.required
    """
    if not os.environ.get("OFFLOAD_HOME"):
        print("OFFLOAD_HOME environment variable missing")
        print(
            "You should source environment variables first, eg: . ../conf/offload.env"
        )
        sys.exit(1)
