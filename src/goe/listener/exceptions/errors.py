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
