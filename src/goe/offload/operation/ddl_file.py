#! /usr/bin/env python3

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
from typing import TYPE_CHECKING

from goe.exceptions import OffloadOptionError
from goe.filesystem.goe_dfs import get_scheme_from_location_uri
from goe.offload import offload_constants
from goe.util.misc_functions import standard_file_name

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.offload_messages import OffloadMessages


DDL_FILE_HEADER = """-- TODO
"""


def generate_ddl_file_path(
    owner: str, table_name: str, config: "OrchestrationConfig"
) -> str:
    """Generates a default path when DDL file option == AUTO."""
    file_name = standard_file_name(
        f"{owner}.{table_name}", extension=".sql", with_datetime=True
    )
    log_path = os.path.join(config.log_path, file_name)
    return log_path


def validate_ddl_file(ddl_file: str):
    """Simple validation that a value supplied via ddl_file looks good.

    Only local paths are fully validated at this point because paths to cloud storage are
    prefixes and may not exist until the object is created."""
    # Simplistic check that the file path looks like a cloud storage one.
    if ":" in ddl_file:
        # We don't need to know the scheme right now, just validation that it is supported.
        _ = get_scheme_from_location_uri(ddl_file)
        return

    # Assume local filesystem, we can validate the path.
    if os.path.exists(ddl_file):
        raise OffloadOptionError(f"DDL path already exists: {ddl_file}")

    if "/" in ddl_file[1:]:
        dirname = os.path.dirname(ddl_file)
        if not os.path.isdir(dirname):
            raise OffloadOptionError(f"DDL file directory does not exist: {dirname}")


def normalise_ddl_file(
    options, config: "OrchestrationConfig", messages: "OffloadMessages"
):
    """Validates path pointed to by ddl_file and generates a new path if AUTO. Mutates options."""
    if options.ddl_file:
        options.ddl_file = options.ddl_file.strip()
    else:
        return options.ddl_file

    if options.execute and options.ddl_file:
        messages.notice(offload_constants.DDL_FILE_EXECUTE_MESSAGE_TEXT)
        options.execute = False

    if options.ddl_file.upper() == offload_constants.DDL_FILE_AUTO:
        # Use an auto-generated path.
        options.ddl_file = generate_ddl_file_path(
            options.owner, options.table_name, config
        )
        return

    validate_ddl_file(options.ddl_file)


def write_ddl_to_ddl_file(ddl_file: str, ddl: list):
    """Take a list of DDL strings and write them to a file"""
    assert ddl_file
    ddl_str = "\n".join(ddl)
    ddl_file_contents = f"{DDL_FILE_HEADER}\n\n{ddl_str}"
    if ":" in ddl_file:
        # Cloud storage.
        pass
    else:
        # Local filesystem.
        with open(ddl_file, "w") as f:
            f.write(ddl_file_contents)