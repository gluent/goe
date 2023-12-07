# Standard Library
import enum


class CommandStatus(str, enum.Enum):
    """Enum for command status"""

    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    EXECUTING = "EXECUTING"
