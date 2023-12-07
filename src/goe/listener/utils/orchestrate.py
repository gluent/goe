"""
Simple Orchestration utilities.
LICENSE_TEXT
"""

from goe.listener import exceptions
from goe.util.orchestration_lock import (
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
