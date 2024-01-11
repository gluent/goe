# Third Party Libraries
from fastapi import status
from pydantic import UUID4

# GOE
from goe.listener import schemas
from goe.listener.exceptions.base import BaseApplicationError


class DatabaseConnectivityError(BaseApplicationError):
    """General Databaase Connectivity Error"""

    def __init__(self):
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        message = "Could not connect to the backend database."
        BaseApplicationError.__init__(  # noqa: WPS609
            self,
            status_code,
            content=schemas.ErrorMessage(code=status_code, message=message).dict(
                exclude_none=True,
            ),
        )


class HybridViewMetadataNotFoundError(BaseApplicationError):
    """Hybrid View Not Found Error"""

    def __init__(self, hybrid_view: str):
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        message = f"No metadata found for {hybrid_view}"
        BaseApplicationError.__init__(  # noqa: WPS609
            self,
            status_code,
            content=schemas.ErrorMessage(code=status_code, message=message).dict(
                exclude_none=True,
            ),
        )


class CredentialValidationError(BaseApplicationError):
    """Token Validation Failed"""

    def __init__(self):
        """Item is not public and requires auth"""
        status_code = status.HTTP_401_UNAUTHORIZED
        message = "Could not validate credentials"
        BaseApplicationError.__init__(  # noqa: WPS609
            self,
            status_code,
            content=schemas.ErrorMessage(code=status_code, message=message).dict(
                exclude_none=True,
            ),
        )


class LogFileNotFoundError(BaseApplicationError):
    """File Not Found Error"""

    def __init__(self, file_name: str):
        """Item is not public and requires auth"""
        status_code = status.HTTP_404_NOT_FOUND
        message = f"Log File {file_name} not found"
        BaseApplicationError.__init__(  # noqa: WPS609
            self,
            status_code,
            content=schemas.ErrorMessage(code=status_code, message=message).dict(
                exclude_none=True,
            ),
        )


class CommandExecutionNotFound(BaseApplicationError):
    """File Not Found Error"""

    def __init__(self, execution_id: UUID4):
        """Item is not public and requires auth"""
        status_code = status.HTTP_404_NOT_FOUND
        message = f"Command Execution {execution_id} was not found"
        BaseApplicationError.__init__(  # noqa: WPS609
            self,
            status_code,
            content=schemas.ErrorMessage(code=status_code, message=message).dict(
                exclude_none=True,
            ),
        )
