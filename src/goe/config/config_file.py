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

import os
import re
import sys
from typing import Optional

from dotenv import load_dotenv


CONFIG_FILE_NAME = "offload.env"
KEY_VALUE_PATTERN = re.compile(r"#?[ ]*([A-Z_0-9]+)=(.*)")


def check_config_path():
    """Check OFFLOAD_HOME in top level command wrappers"""
    if not os.environ.get("OFFLOAD_HOME"):
        print("OFFLOAD_HOME environment variable missing")
        sys.exit(1)


def get_environment_file_path():
    return os.path.join(os.environ.get("OFFLOAD_HOME"), "conf", CONFIG_FILE_NAME)


def load_env(path: str = None):
    """Load GOE environment from a configuration file.

    By default this is a fixed location: $OFFLOAD_HOME/conf/offload.env.
    In time this will become a parameter and support cloud storage locations.
    """
    if not path:
        path = get_environment_file_path()

    load_dotenv(path)


def env_key_value_pair(line_from_file: str) -> Optional[tuple]:
    """Used by connect to get the key names from a configuration file"""
    m = KEY_VALUE_PATTERN.match(line_from_file)
    return m.groups() if m else None
