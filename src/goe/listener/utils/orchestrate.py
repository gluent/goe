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

"""
Simple Orchestration utilities.
"""

from goe.listener import exceptions
from goe.orchestration.orchestration_lock import (
    OrchestrationLockTimeout,
    orchestration_lock_for_table,
)


def check_for_running_command(owner_table: str) -> bool:
    """


    Args:
        owner_table (str): _description_

    Returns:
        bool: _description_
    """
    owner_name, table_name = owner_table.split(".")
    if owner_name and table_name:
        try:
            table_lock = orchestration_lock_for_table(owner_name, table_name)
            table_lock.acquire()
            table_lock.release()
        except OrchestrationLockTimeout:
            raise exceptions.ApplicationError(
                status_code=500,
                message=f"Another job has locked the table {owner_name}.{table_name}.",
            )
        return False

    raise exceptions.ApplicationError(
        status_code=500,
        message="Could not determine owner and table from supplied input.",
    )
