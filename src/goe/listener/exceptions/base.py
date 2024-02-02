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

"""HTTP exception and handler for FastAPI."""
# Standard Library
from typing import Any, Dict, Optional, Union

# Third Party Libraries
from cx_Oracle import DatabaseError as OracleDatabaseError
from fastapi import Request, status
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import ORJSONResponse
from pydantic import ValidationError
from redis.exceptions import ConnectionError, RedisError, TimeoutError

# GOE
from goe.listener import schemas


class BaseApplicationError(Exception):
    """Custom HTTPException class definition.

    This exception combined with exception_handler method allows you to use it
    the same manner as you'd use FastAPI.HTTPException with one difference. You
    have freedom to define returned response body, whereas in
    FastAPI.HTTPException content is returned under "detail" JSON key.

    FastAPI.HTTPException source:
    https://github.com/tiangolo/fastapi/blob/master/fastapi/exceptions.py

    """

    def __init__(
        self,
        status_code: int,
        content: Any = None,
        headers: Optional[Dict[str, Any]] = None,
        *kwargs,
    ) -> None:
        """Initialize HTTPException class object instance.

        Args:
            status_code(int): HTTP error status code.
            content(Any): Response body.
            headers(Optional[Dict[str, Any]]): Additional response headers.

        """
        self.status_code = status_code
        self.content = content
        self.headers = headers

    def __repr__(self):
        """Class custom __repr__ method implementation.

        Returns:
            str: HTTPException string object.

        """
        kwargs = []

        for key, value in self.__dict__.items():
            if not key.startswith("_"):
                kwargs.append("{key}={value}".format(key=key, value=repr(value)))

        return "{name}({kwargs})".format(
            name=self.__class__.__name__,
            kwargs=", ".join(kwargs),
        )

    def __str__(self):
        return f"{self.__class__.__name__}({self.__repr__()})"


class ApplicationError(BaseApplicationError):
    """General Application Error"""

    def __init__(
        self,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        message: Union[str, schemas.ErrorMessage] = "A general exception occurred.",
    ):
        BaseApplicationError.__init__(  # noqa: WPS609
            self,
            status_code,
            content=schemas.ErrorMessage(code=status_code, message=message).dict(
                exclude_none=True,
            )
            if isinstance(message, str)
            else message,
        )


async def http_error_handler(_: Request, exception: HTTPException):
    """Handle HTTPException globally.

    In this application custom handler is added in asgi.py while initializing
    FastAPI application. This is needed in order to handle custom HTTException
    globally.

    More details:
    https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers

    Args:
        request(starlette.requests.Request): Request class object instance.
        - More details: https://www.starlette.io/requests/
        exception(HTTPException): Custom HTTPException class object instance.

    Returns:
        FastAPI.response.JSONResponse class object instance initialized with
            kwargs from custom HTTPException.

    """
    return ORJSONResponse(
        status_code=exception.status_code,
        content=exception.detail,
        headers=getattr(exception, "headers", None),
    )


async def http422_error_handler(
    _: Request,
    exception: Union[RequestValidationError, ValidationError],
):
    """Handle Http validation error globally.

    In this application custom handler is added in asgi.py while initializing
    FastAPI application. This is needed in order to handle custom HTTException
    globally.

    More details:
    https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers

    Args:
        request(starlette.requests.Request): Request class object instance.
        - More details: https://www.starlette.io/requests/
        exception(HTTPException): Custom HTTPException class object instance.

    Returns:
        FastAPI.response.JSONResponse class object instance initialized with
            kwargs from custom HTTPException.

    """

    details = {error.get("loc")[-1]: error.get("msg") for error in exception.errors()}
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    return ORJSONResponse(
        status_code=status_code,
        content=schemas.ErrorMessage(
            code=status_code, message="Validation Error", details=details
        ).dict(
            exclude_none=True,
        ),
        headers=getattr(exception, "headers", None),
    )


async def system_error_exception_handler(request: Request, exception: ValueError):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    msg = f"{exception.__class__.__name__} - {exception}"
    details = {}
    return ORJSONResponse(
        status_code=status_code,
        content=schemas.ErrorMessage(
            code=status_code, message=msg, details=details
        ).dict(
            exclude_none=True,
        ),
        headers=getattr(exception, "headers", None),
    )


async def app_error_handler(_: Request, exception: BaseApplicationError):
    """Handle Http validation error globally.

    In this application custom handler is added in asgi.py while initializing
    FastAPI application. This is needed in order to handle custom HTTException
    globally.

    More details:
    https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers

    Args:
        request(starlette.requests.Request): Request class object instance.
        - More details: https://www.starlette.io/requests/
        exception(HTTPException): Custom HTTPException class object instance.

    Returns:
        FastAPI.response.JSONResponse class object instance initialized with
            kwargs from custom HTTPException.

    """
    return ORJSONResponse(
        status_code=exception.status_code,
        content=exception.content,
        headers=getattr(exception, "headers", None),
    )


async def cache_connectivity_error(
    _: Request,
    exception: Union[TimeoutError, ConnectionError, RedisError],
):
    """Handle cache connection error globally.

    In this application custom handler is added in asgi.py while initializing
    FastAPI application. This is needed in order to handle custom HTTException
    globally.

    More details:
    https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers

    Args:
        request(starlette.requests.Request): Request class object instance.
        - More details: https://www.starlette.io/requests/
        exception(HTTPException): Custom HTTPException class object instance.

    Returns:
        FastAPI.response.JSONResponse class object instance initialized with
            kwargs from custom HTTPException.

    """
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    message = "Failed to connect to the cache backend.  Please check your cache configuration."
    return ORJSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=schemas.ErrorMessage(code=status_code, message=message).dict(
            exclude_none=True,
        ),
        headers=getattr(exception, "headers", None),
    )


async def database_connectivity_error(
    _: Request,
    exception: OracleDatabaseError,
):
    """Handle database connection error globally.

    In this application custom handler is added in asgi.py while initializing
    FastAPI application. This is needed in order to handle custom HTTException
    globally.

    More details:
    https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers

    Args:
        request(starlette.requests.Request): Request class object instance.
        - More details: https://www.starlette.io/requests/
        exception(HTTPException): Custom HTTPException class object instance.

    Returns:
        FastAPI.response.JSONResponse class object instance initialized with
            kwargs from custom HTTPException.

    """
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    if isinstance(exception, OracleDatabaseError):
        message = f"{exception.__class__.__name__} - {exception}"
    else:
        message = "Failed to connect to the backend database.  Please check your database configuration."
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ORJSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=schemas.ErrorMessage(code=status_code, message=message).dict(
            exclude_none=True,
        ),
        headers=getattr(exception, "headers", None),
    )
